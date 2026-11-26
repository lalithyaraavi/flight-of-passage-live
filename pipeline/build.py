"""Silver + gold: clean every bronze snapshot, run quality checks, publish small JSON tables for the page.

bronze/YYYY-MM-DD/HHMMSS.json   raw snapshots, never edited
silver/waits/YYYY-MM-DD.csv     one row per ride reading, deduplicated, typed, flagged
gold/*.json                     what the page reads
"""
import csv, json, os, pathlib, statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(os.environ.get("FOP_DATA") or pathlib.Path(__file__).resolve().parents[1] / "data")
BRONZE, SILVER, GOLD = ROOT / "bronze", ROOT / "silver" / "waits", ROOT / "gold"
PARK_TZ = ZoneInfo("America/New_York")
FOP = 4439  # Avatar Flight of Passage
KEY_RIDES = [4439, 4438, 110, 113, 112]  # Flight of Passage, Na'vi River Journey, Everest, Safaris, Kali
STALE_AFTER = timedelta(minutes=20)  # feed timestamp older than this at fetch time = stale
LATE_AFTER = timedelta(minutes=15)   # gap between runs longer than this = late run (schedule is every 10)
JUMP_LIMIT = 60                      # minutes of change between consecutive readings that we flag
SILVER_COLS = ["reading_id", "fetched_at", "source_updated_at", "park_date", "local_time", "land",
               "ride_id", "ride", "status", "wait_min", "is_stale", "is_jump"]


def utc(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_bronze():
    snaps = []
    for f in sorted(BRONZE.glob("*/*.json")):
        try:
            rec = json.loads(f.read_text())
            snaps.append(rec | {"file": f"{f.parent.name}/{f.name}"})
        except json.JSONDecodeError:
            snaps.append({"file": f"{f.parent.name}/{f.name}", "fetched_at": None, "http_status": -1, "payload": ""})
    return [s for s in snaps if s.get("fetched_at")]


def build():
    snaps = load_bronze()
    runs, rows, seen = [], [], set()
    last_wait = {}
    prev_fetch, prev_source, prev_count = None, None, None
    for s in snaps:
        fetched = utc(s["fetched_at"])
        run = {"fetched_at": s["fetched_at"], "gap_min": None, "ok": True, "issues": [], "rows_new": 0}
        if prev_fetch:
            gap = fetched - prev_fetch
            run["gap_min"] = round(gap.total_seconds() / 60, 1)
            if gap > LATE_AFTER:
                run["issues"].append(f"late run: {run['gap_min']} min since the last one")
        prev_fetch = fetched
        try:
            data = json.loads(s["payload"]) if s.get("http_status") == 200 else None
        except json.JSONDecodeError:
            data = None
        if not data or "lands" not in data:
            run["ok"] = False
            run["issues"].append(f"fetch failed (HTTP {s.get('http_status')})")
            runs.append(run)
            continue
        rides = [(land["name"], r) for land in data["lands"] for r in land.get("rides", [])]
        if not rides or any(k not in r for _, r in rides for k in ("id", "name", "is_open", "wait_time", "last_updated")):
            run["ok"] = False
            run["issues"].append("schema changed: expected ride fields missing")
            runs.append(run)
            continue
        if prev_count and abs(len(rides) - prev_count) > 0.3 * prev_count:
            run["issues"].append(f"ride count moved from {prev_count} to {len(rides)}")
        prev_count = len(rides)
        source_ts = max(utc(r["last_updated"]) for _, r in rides)
        any_open = any(r["is_open"] for _, r in rides)
        stale = fetched - source_ts > STALE_AFTER and any_open  # a quiet feed overnight is expected
        if stale:
            run["issues"].append("feed stale: newest reading is " + str(int((fetched - source_ts).total_seconds() // 60)) + " min old")
        if prev_source and source_ts == prev_source:
            run["issues"].append("duplicate: feed had not refreshed since the last run")
        prev_source = source_ts
        for land, r in rides:
            src = utc(r["last_updated"])
            key = (r["id"], src.isoformat())
            if key in seen:  # same reading fetched twice: keep the first copy
                continue
            seen.add(key)
            local = src.astimezone(PARK_TZ)
            if not r["is_open"]:
                status, wait = "closed", None
            elif r["wait_time"] == 0:
                status, wait = "open_no_wait", 0  # shows, trails, walk-ons: open, nothing posted
            else:
                status, wait = "open", int(r["wait_time"])
            jump = False
            if wait is not None and status == "open":
                pw = last_wait.get(r["id"])
                if pw is not None and abs(wait - pw) > JUMP_LIMIT:
                    jump = True  # held out of gold; the baseline stays on the last believable reading
                    run["issues"].append(f"jump: {r['name']} {pw} -> {wait} min")
                else:
                    last_wait[r["id"]] = wait
            rows.append({"reading_id": f"{r['id']}-{int(src.timestamp())}", "fetched_at": s["fetched_at"],
                         "source_updated_at": src.isoformat().replace("+00:00", "Z"), "park_date": local.date().isoformat(),
                         "local_time": local.strftime("%H:%M"), "land": land, "ride_id": r["id"], "ride": r["name"],
                         "status": status, "wait_min": wait, "is_stale": int(stale), "is_jump": int(jump)})
            run["rows_new"] += 1
        runs.append(run)

    # silver partitions
    SILVER.mkdir(parents=True, exist_ok=True)
    by_day = defaultdict(list)
    for r in rows:
        by_day[r["park_date"]].append(r)
    for day, rs in by_day.items():
        with open(SILVER / f"{day}.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=SILVER_COLS)
            w.writeheader()
            w.writerows(rs)

    # gold
    GOLD.mkdir(parents=True, exist_ok=True)
    good = [r for r in rows if not r["is_stale"] and not r["is_jump"]]
    write = lambda name, obj: (GOLD / name).write_text(json.dumps(obj, separators=(",", ":")))

    last_ok = next((s for s in reversed(snaps) if s.get("http_status") == 200), None)
    latest_rows = []
    if last_ok:
        data = json.loads(last_ok["payload"])
        for land in data["lands"]:
            for r in land["rides"]:
                st = "closed" if not r["is_open"] else ("open_no_wait" if r["wait_time"] == 0 else "open")
                latest_rows.append({"land": land["name"], "ride_id": r["id"], "ride": r["name"], "status": st,
                                    "wait": r["wait_time"] if st == "open" else None, "updated": r["last_updated"]})
    write("latest.json", {"fetched_at": last_ok["fetched_at"] if last_ok else None, "rides": latest_rows})

    days = sorted(by_day)
    today = days[-1] if days else None
    series = {}
    for rid in KEY_RIDES:
        pts = [(r["local_time"], r["wait_min"]) for r in good if r["park_date"] == today and r["ride_id"] == rid and r["status"] == "open"]
        name = next((r["ride"] for r in rows if r["ride_id"] == rid), str(rid))
        series[str(rid)] = {"ride": name, "points": pts}
    write("today.json", {"park_date": today, "series": series})

    def minutes(hhmm):
        h, m = map(int, hhmm.split(":"))
        return h * 60 + m

    day_stats = []
    for d in days:
        pts = sorted((minutes(r["local_time"]), r["wait_min"], r["local_time"]) for r in good
                     if r["park_date"] == d and r["ride_id"] == FOP and r["status"] == "open")
        if len(pts) < 3:
            continue
        peak = max(pts, key=lambda p: p[1])
        best, best_avg = None, None
        for i, p in enumerate(pts):  # lowest average wait across any 60-minute window
            win = [q[1] for q in pts[i:] if q[0] - p[0] <= 60]
            if len(win) >= 3 and (best_avg is None or statistics.mean(win) < best_avg):
                best, best_avg = p, statistics.mean(win)
        day_stats.append({"date": d, "readings": len(pts), "mean": round(statistics.mean(q[1] for q in pts), 1),
                          "peak": peak[1], "peak_at": peak[2], "best_start": best[2] if best else None,
                          "best_avg": round(best_avg, 1) if best_avg is not None else None})
    write("days.json", day_stats)

    cells = defaultdict(list)
    for r in good:
        if r["ride_id"] == FOP and r["status"] == "open":
            dt = datetime.fromisoformat(r["park_date"])
            slot = minutes(r["local_time"]) // 30
            cells[(dt.weekday(), slot)].append(r["wait_min"])
    write("heatmap.json", [{"dow": k[0], "slot": k[1], "mean": round(statistics.mean(v), 1), "n": len(v)} for k, v in sorted(cells.items())])

    totals = {"runs": len(runs), "failed": sum(not r["ok"] for r in runs),
              "late": sum(any(i.startswith("late") for i in r["issues"]) for r in runs),
              "stale": sum(any(i.startswith("feed stale") for i in r["issues"]) for r in runs),
              "duplicates": sum(any(i.startswith("duplicate") for i in r["issues"]) for r in runs),
              "jumps": sum(r["is_jump"] for r in rows), "readings": len(rows), "readings_used": len(good),
              "first_fetch": snaps[0]["fetched_at"] if snaps else None, "days": len(days)}
    write("dq.json", {"totals": totals, "recent": runs[-48:][::-1]})
    print(f"silver: {len(rows)} readings in {len(days)} partitions | gold: {len(good)} used | runs: {len(runs)}")


if __name__ == "__main__":
    build()
