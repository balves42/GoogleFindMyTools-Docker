import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

APP_ROOT = Path(__file__).resolve().parents[1]
VENDOR_ROOT = APP_ROOT / "vendor" / "GoogleFindMyTools-main"
SCRIPTS_DIR = APP_ROOT / "scripts"

SECRETS_MOUNT_PATH = Path(os.environ.get("SECRETS_PATH", "/data/secrets.json"))
# Where GoogleFindMyTools expects it by default
GFM_SECRETS_PATH = VENDOR_ROOT / "Auth" / "secrets.json"

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

class LocateRequest(BaseModel):
    canonic_id: str = Field(..., description="Tracker canonic_id")
    timeout_seconds: int = Field(60, ge=5, le=300)

app = FastAPI(title="Google Find My Bridge", version="0.1.0")

@app.on_event("startup")
def startup() -> None:
    _ensure_secrets()

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
    return _run_script("gfmt_locate.py", [req.canonic_id], timeout=req.timeout_seconds)
