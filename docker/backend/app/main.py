import json
import os
import subprocess
import threading
import time
import sqlite3
import re
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

APP_ROOT = Path(__file__).resolve().parents[1]
VENDOR_ROOT = APP_ROOT / "vendor" / "GoogleFindMyTools-main"
SCRIPTS_DIR = APP_ROOT / "scripts"

SECRETS_MOUNT_PATH = Path(os.environ.get("SECRETS_PATH", "/data/secrets.json"))
# Where GoogleFindMyTools expects it by default
GFM_SECRETS_PATH = VENDOR_ROOT / "Auth" / "secrets.json"

DB_PATH = Path(os.environ.get("DB_PATH", "/data/db/fmd_history.db"))

# Background polling (optional)
POLL_ENABLED = os.environ.get("LOCATE_POLL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y", "on")
POLL_INTERVAL_MINUTES = int(os.environ.get("LOCATE_POLL_INTERVAL_MINUTES", "5"))
POLL_LOCATE_TIMEOUT_SECONDS = int(os.environ.get("LOCATE_POLL_TIMEOUT_SECONDS", "60"))

_stop_event = threading.Event()
_poll_thread: Optional[threading.Thread] = None

def _ensure_secrets() -> None:
    if not SECRETS_MOUNT_PATH.exists():
        raise RuntimeError(f"secrets.json not found at {SECRETS_MOUNT_PATH}. Mount it as a volume.")
    GFM_SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Copy only if changed to reduce writes on vendor dir
    try:
        src = SECRETS_MOUNT_PATH.read_bytes()
        dst = GFM_SECRETS_PATH.read_bytes() if GFM_SECRETS_PATH.exists() else None
        if dst != src:
            GFM_SECRETS_PATH.write_bytes(src)
    except Exception as e:
        raise RuntimeError(f"Failed to provision secrets.json: {e}")

def _run_script(script_name: str, args: List[str], timeout: int) -> Any:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(VENDOR_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    cmd = ["python", str(SCRIPTS_DIR / script_name)] + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail=f"Timed out after {timeout}s")
    if p.returncode != 0:
        # include stderr (trim) to help debugging, but don't leak secrets
        err = (p.stderr or p.stdout or "").strip()
        err = err[-2000:]
        raise HTTPException(status_code=500, detail=f"Locate backend error (code {p.returncode}): {err}")
    out = (p.stdout or "").strip()
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        # last resort: return raw
        return {"raw": out}

def _connect_db() -> sqlite3.Connection:
    # Ensure folder exists (bind-mounted from host via docker-compose)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Pre-create the db file if it doesn't exist yet. This makes first-run behavior reliable,
    # and surfaces permission issues as a clear exception.
    try:
        DB_PATH.touch(exist_ok=True)
    except Exception as e:
        raise RuntimeError(
            f"Cannot create DB file at {DB_PATH}. "
            f"Check that the host folder mounted to {DB_PATH.parent} exists and is writable. "
            f"Original error: {e}"
        )

    try:
        # Use rwc so SQLite creates the DB if needed.
        con = sqlite3.connect(f"file:{DB_PATH}?mode=rwc", uri=True, check_same_thread=False)
    except Exception as e:
        raise RuntimeError(
            f"Cannot open SQLite DB at {DB_PATH}. "
            f"Check mount/permissions for {DB_PATH.parent}. Original error: {e}"
        )
    con.row_factory = sqlite3.Row
    return con

def _init_db() -> None:
    con = _connect_db()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fmd_location_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonic_id TEXT NOT NULL,
                published_at_ms INTEGER NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                inserted_at_ms INTEGER NOT NULL
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_fmd_reports_cid_ts ON fmd_location_reports(canonic_id, timestamp_ms);")
        con.commit()
    finally:
        con.close()

def _normalize_unix_ms(value: Any) -> int:
    """Normalize various time formats into UNIX epoch milliseconds.

    Accepts:
      - int/float seconds or milliseconds
      - numeric strings (seconds or milliseconds)
      - datetime strings like '2026-01-29 20:45:41' or ISO-8601
    """
    if value is None:
        raise ValueError("time value is None")

    # Numbers
    if isinstance(value, (int, float)):
        v = int(value)
        # Heuristic: seconds are ~1e9..1e10, ms are ~1e12..1e13
        if v < 10_000_000_000:  # < year ~2286 in seconds
            return v * 1000
        return v

    # Strings: try numeric first, then datetime parse
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            raise ValueError("empty time string")

        # Numeric string
        if re.fullmatch(r"-?\d+(?:\.\d+)?", s):
            v = int(float(s))
            if abs(v) < 10_000_000_000:
                return v * 1000
            return v

        # Datetime string
        from datetime import datetime, timezone
        try:
            # Handles 'YYYY-MM-DD HH:MM:SS' and many ISO formats
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            # Last-resort: common format
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    raise ValueError(f"Unsupported time type: {type(value)}")

def _extract_locate_report(canonic_id: str, locate_resp: Any) -> Optional[Tuple[int, int, float, float]]:
    """Returns (published_at_ms, timestamp_ms, lat, lon) if ok + coordinates exist."""
    if not isinstance(locate_resp, dict):
        return None
    ok = locate_resp.get("ok")
    lat = locate_resp.get("latitude")
    lon = locate_resp.get("longitude")
    t = locate_resp.get("time") or locate_resp.get("timestamp") or locate_resp.get("publishedAt")
    if ok is not True:
        return None
    if lat is None or lon is None:
        return None
    ts_ms = _normalize_unix_ms(t) if t is not None else int(time.time() * 1000)
    published_ms = int(time.time() * 1000)  # when the server queried FMD
    return (published_ms, ts_ms, float(lat), float(lon))


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points in meters."""
    # Earth radius in meters
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c

def _store_location(canonic_id: str, published_at_ms: int, timestamp_ms: int, latitude: float, longitude: float) -> None:
    """Persist a location report for a device, but only if it differs from the last stored location.

    Rules:
      - Skip if coordinates are (effectively) identical to last point.
      - Skip if the new point is within HISTORY_MIN_DISTANCE_METERS of the last point.
    """
    now_ms = int(time.time() * 1000)

    lat = float(latitude)
    lon = float(longitude)

    # Treat tiny float noise as equal
    epsilon = float(os.getenv("HISTORY_DEDUP_EPSILON", "0.0"))
    if epsilon == 0.0:
        epsilon = 1e-7

    # Minimum distance in meters required to store a new point (default 0 = disabled)
    min_dist_m = float(os.getenv("HISTORY_MIN_DISTANCE_METERS", "0"))

    con = _connect_db()
    try:
        row = con.execute(
            """
            SELECT latitude, longitude
            FROM fmd_location_reports
            WHERE canonic_id = ?
            ORDER BY published_at_ms DESC
            LIMIT 1
            """,
            (canonic_id,),
        ).fetchone()

        if row is not None:
            last_lat, last_lon = float(row[0]), float(row[1])

            # Exact-ish match
            if abs(lat - last_lat) <= epsilon and abs(lon - last_lon) <= epsilon:
                return

            # Distance threshold
            if min_dist_m > 0:
                d = _haversine_meters(last_lat, last_lon, lat, lon)
                if d < min_dist_m:
                    return

        con.execute(
            """
            INSERT INTO fmd_location_reports(canonic_id, published_at_ms, timestamp_ms, latitude, longitude, inserted_at_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (canonic_id, int(published_at_ms), int(timestamp_ms), lat, lon, now_ms),
        )
        con.commit()
    finally:
        con.close()

def _poll_loop() -> None:
    # Run periodically to build our own location history
    # (since upstream doesn't provide it like Apple does).
    while not _stop_event.is_set():
        try:
            _ensure_secrets()
            devices = _run_script("gfmt_devices.py", [], timeout=30)
            if isinstance(devices, list):
                for d in devices:
                    if _stop_event.is_set():
                        break
                    cid = None
                    if isinstance(d, dict):
                        cid = d.get("canonic_id") or d.get("canonicId") or d.get("id")
                    if not cid:
                        continue
                    try:
                        locate = _run_script("gfmt_locate.py", [str(cid)], timeout=POLL_LOCATE_TIMEOUT_SECONDS)
                        rep = _extract_locate_report(str(cid), locate)
                        if rep:
                            _store_location(str(cid), *rep)
                    except HTTPException:
                        # ignore per-device failures; next cycle will retry
                        continue
        except Exception:
            # keep running; this is best-effort
            pass

        # Sleep in small chunks so shutdown is responsive
        interval_s = max(1, POLL_INTERVAL_MINUTES) * 60
        slept = 0
        while slept < interval_s and not _stop_event.is_set():
            time.sleep(min(1, interval_s - slept))
            slept += 1

class LocateRequest(BaseModel):
    canonic_id: str = Field(..., description="Tracker canonic_id")
    timeout_seconds: int = Field(60, ge=5, le=300)

class HistoryRequest(BaseModel):
    canonic_id: str = Field(..., description="Tracker canonic_id")
    startTimeUnixMS: int = Field(..., ge=0, description="Start time (inclusive) UNIX timestamp in milliseconds")
    endTimeUnixMS: int = Field(..., ge=0, description="End time (inclusive) UNIX timestamp in milliseconds")
    limit: int = Field(5000, ge=1, le=20000, description="Max number of points to return")

class LocationReportOut(BaseModel):
    publishedAt: int = Field(..., description="UNIX timestamp MS when this report was published by a device.")
    timestamp: int = Field(..., description="UNIX timestamp MS when this report was recorded by a device.")
    latitude: float
    longitude: float

app = FastAPI(title="Google Find My Bridge", version="0.2.0")

@app.on_event("startup")
def startup() -> None:
    _ensure_secrets()
    _init_db()

    global _poll_thread
    if POLL_ENABLED and _poll_thread is None:
        _stop_event.clear()
        _poll_thread = threading.Thread(target=_poll_loop, name="fmd-locate-poller", daemon=True)
        _poll_thread.start()

@app.on_event("shutdown")
def shutdown() -> None:
    _stop_event.set()

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.get("/google/devices")
def google_devices(timeout_seconds: int = 30) -> Any:
    _ensure_secrets()
    return _run_script("gfmt_devices.py", [], timeout=timeout_seconds)

@app.post("/google/locate")
def google_locate(req: LocateRequest) -> Any:
    _ensure_secrets()
    resp = _run_script("gfmt_locate.py", [req.canonic_id], timeout=req.timeout_seconds)

    # Persist successful locations so we can serve history later
    rep = _extract_locate_report(req.canonic_id, resp)
    if rep:
        _store_location(req.canonic_id, *rep)

    return resp

@app.post("/google/history", response_model=List[LocationReportOut])
def google_history(req: HistoryRequest) -> List[LocationReportOut]:
    if req.endTimeUnixMS < req.startTimeUnixMS:
        raise HTTPException(status_code=400, detail="endTimeUnixMS must be >= startTimeUnixMS")

    con = _connect_db()
    try:
        rows = con.execute(
            """
            SELECT published_at_ms, timestamp_ms, latitude, longitude
            FROM fmd_location_reports
            WHERE canonic_id = ?
              AND timestamp_ms BETWEEN ? AND ?
            ORDER BY timestamp_ms ASC
            LIMIT ?
            """,
            (req.canonic_id, int(req.startTimeUnixMS), int(req.endTimeUnixMS), int(req.limit)),
        ).fetchall()
    finally:
        con.close()

    return [
        LocationReportOut(
            publishedAt=int(r["published_at_ms"]),
            timestamp=int(r["timestamp_ms"]),
            latitude=float(r["latitude"]),
            longitude=float(r["longitude"]),
        )
        for r in rows
    ]
