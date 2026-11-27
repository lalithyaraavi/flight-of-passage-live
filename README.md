# Flight of Passage, Live

I once waited three hours for Avatar Flight of Passage, so I mapped the wait times and cut my next wait in half. This repo is that project rebuilt as a small, real data pipeline.

Every 10 minutes a GitHub Actions job reads the posted wait for every ride in Disney's Animal Kingdom, stores the raw snapshot, cleans it, runs quality checks and publishes small JSON tables that the page in `site/` reads live.

| Layer | Path | What it holds |
| --- | --- | --- |
| Bronze | `data/bronze/YYYY-MM-DD/HHMMSS.json` | The feed's JSON exactly as received, with fetch time and HTTP status. Never edited. |
| Silver | `data/silver/waits/YYYY-MM-DD.csv` | One row per ride reading: deduplicated on ride + feed timestamp, park-local time, `closed` vs `open_no_wait` vs `open`, stale and jump flags. |
| Gold | `data/gold/*.json` | Today's curves, daily best hour and the weekday x half-hour heatmap are SQL (DuckDB) in `pipeline/sql/`. The latest board and the run log with quality checks come from `build.py`. |

Quality checks on every run: late runs (scheduler gaps over 15 min), feed not refreshed (duplicate snapshot), stale feed while rides are open, failed fetches, schema changes, ride count swings, and impossible jumps (over 60 min between readings), which are held out of gold.

## Also in the repo

| File | What it is |
| --- | --- |
| `pipeline/sql/*.sql` | The gold tables as SQL: window functions (`RANGE BETWEEN CURRENT ROW AND 60 FOLLOWING`) for the best hour, `arg_min`/`arg_max`, weekday x half-hour aggregation. |
| `spark/silver_spark.py` | The silver step as a PySpark job: parse, explode, dedupe with a window, stale and jump flags, Parquet partitioned by park day. Checked against `build.py` on three days of test data and it matches row for row. |
| `dags/fop_dag.py` | The same pipeline as an Airflow DAG (`fetch_bronze >> build_silver_gold >> [quality_gate, publish]`), every 10 minutes, retries with backoff, no backfill. Loads in Airflow 3.4. |
| `pipeline/check.py` | Quality gate: exits non-zero when the latest run failed or the schema changed, so a scheduler can alert. |

## Run locally (Python 3.10+)

```
pip install -r requirements.txt
python pipeline/collect.py     # one bronze snapshot
python pipeline/build.py       # silver in Python, gold in SQL
python pipeline/check.py       # quality gate
spark-submit spark/silver_spark.py data/bronze out/silver   # optional, needs PySpark
```

Wait times [Powered by Queue-Times.com](https://queue-times.com/). Posted waits are the park's estimates. Independent project, not affiliated with or endorsed by The Walt Disney Company.

Lalithya Raavi · Systems Design Engineering, University of Waterloo
