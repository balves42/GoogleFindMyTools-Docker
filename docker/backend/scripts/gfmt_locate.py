import json
import re
import sys
import io
from contextlib import redirect_stdout, redirect_stderr

from NovaApi.ExecuteAction.LocateTracker.location_request import get_location_data_for_device

LAT_RE = re.compile(r"Latitude:\s*([-0-9.]+)")
LON_RE = re.compile(r"Longitude:\s*([-0-9.]+)")
MAP_RE = re.compile(r"Google Maps Link:\s*(\S+)")
TIME_RE = re.compile(r"Time:\s*([0-9\-: ]+)")

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error":"missing canonic_id"}))
        sys.exit(2)
    canonic_id = sys.argv[1]
    buf = io.StringIO()
    # Run in-process but capture output. If vendor calls exit(1), it will kill process (ok).
    with redirect_stdout(buf), redirect_stderr(buf):
        get_location_data_for_device(canonic_id, "Tracker")
    s = buf.getvalue()
    # Parse last occurrence
    lats = LAT_RE.findall(s)
    lons = LON_RE.findall(s)
    maps = MAP_RE.findall(s)
    times = TIME_RE.findall(s)
    if not lats or not lons:
        print(json.dumps({"ok": False, "output": s[-4000:]}))
        return
    lat = float(lats[-1])
    lon = float(lons[-1])
    out = {
        "ok": True,
        "latitude": lat,
        "longitude": lon,
        "google_maps": maps[-1] if maps else None,
        "time": times[-1] if times else None,
    }
    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
