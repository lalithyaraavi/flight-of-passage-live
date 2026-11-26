# Flight of Passage, Live

I once waited three hours for Avatar Flight of Passage, so I mapped the wait times and cut my next wait in half. This repo is that project rebuilt as a small, real data pipeline.

Every 10 minutes a GitHub Actions job reads the posted wait for every ride in Disney's Animal Kingdom, stores the raw snapshot, cleans it, runs quality checks and publishes small JSON tables that the page in `site/` reads live.

| Layer | Path | What it holds |
| --- | --- | --- |
| Bronze | `data/bronze/YYYY-MM-DD/HHMMSS.json` | The feed's JSON exactly as received, with fetch time and HTTP status. Never edited. |
| Silver | `data/silver/waits/YYYY-MM-DD.csv` | One row per ride reading: deduplicated on ride + feed timestamp, park-local time, `closed` vs `open_no_wait` vs `open`, stale and jump flags. |
| Gold | `data/gold/*.json` | Latest board, today's curves, daily best hour, weekday x half-hour heatmap, run log with quality checks. |

Quality checks on every run: late runs (scheduler gaps over 15 min), feed not refreshed (duplicate snapshot), stale feed while rides are open, failed fetches, schema changes, ride count swings, and impossible jumps (over 60 min between readings), which are held out of gold.

Run locally (Python 3.10+, standard library only):

```
python pipeline/collect.py   # one bronze snapshot
python pipeline/build.py     # rebuild silver and gold
```

Wait times [Powered by Queue-Times.com](https://queue-times.com/). Posted waits are the park's estimates. Independent project, not affiliated with or endorsed by The Walt Disney Company.

Lalithya Raavi · Systems Design Engineering, University of Waterloo
