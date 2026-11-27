"""Quality gate: exit non-zero when the latest run failed, so the scheduler can alert.

The raw snapshot is still kept either way; this only decides whether someone gets paged.
"""
import json, os, pathlib, sys

ROOT = pathlib.Path(os.environ.get("FOP_DATA") or pathlib.Path(__file__).resolve().parents[1] / "data")
dq = json.loads((ROOT / "gold" / "dq.json").read_text())
last = dq["recent"][0] if dq["recent"] else None
if last is None:
    sys.exit("no runs recorded yet")
print(f"latest run {last['fetched_at']}: ok={last['ok']} issues={last['issues'] or 'none'}")
if not last["ok"] or any(i.startswith("schema changed") for i in last["issues"]):
    sys.exit(1)
