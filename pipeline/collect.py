"""Bronze: fetch one snapshot of Animal Kingdom wait times and store it exactly as received.

Data: Queue-Times.com (https://queue-times.com), free API with attribution.
"""
import json, pathlib, sys, urllib.request
from datetime import datetime, timezone

PARK_ID = 8  # Disney's Animal Kingdom
URL = f"https://queue-times.com/parks/{PARK_ID}/queue_times.json"
ROOT = pathlib.Path(__file__).resolve().parents[1] / "data" / "bronze"


def main() -> int:
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
    req = urllib.request.Request(URL, headers={"User-Agent": "flight-of-passage-live (portfolio project)"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, body = r.status, r.read().decode("utf-8")
    except Exception as e:  # keep the failure as data: a missing snapshot is itself a quality signal
        status, body = 0, json.dumps({"error": str(e)})
    out = ROOT / fetched_at.strftime("%Y-%m-%d") / (fetched_at.strftime("%H%M%S") + ".json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
                               "http_status": status, "payload": body}))
    print(f"bronze: {out.relative_to(ROOT.parents[1])} status={status} bytes={len(body)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
