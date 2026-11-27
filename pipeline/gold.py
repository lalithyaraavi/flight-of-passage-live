"""Gold layer in SQL: load the silver partitions into DuckDB and run the queries in pipeline/sql/.

silver  = every cleaned reading (one CSV per park day)
good    = silver minus readings flagged stale or impossible (held out of every table)
"""
import json, pathlib
import duckdb

SQL = pathlib.Path(__file__).resolve().parent / "sql"


def rows(con, name):
    cur = con.execute((SQL / f"{name}.sql").read_text())
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def run(silver_dir, gold_dir, key_rides):
    con = duckdb.connect()
    files = sorted(pathlib.Path(silver_dir).glob("*.csv"))
    if files:
        paths = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
        con.execute(f"""CREATE VIEW silver AS SELECT * FROM read_csv([{paths}], header = true,
            types = {{'park_date': 'VARCHAR', 'local_time': 'VARCHAR', 'wait_min': 'INTEGER', 'ride_id': 'INTEGER',
                     'is_stale': 'INTEGER', 'is_jump': 'INTEGER'}})""")
    else:  # nothing collected yet: empty tables, same columns
        con.execute("CREATE VIEW silver AS SELECT * FROM (VALUES (NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::INTEGER, NULL::VARCHAR, NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER, NULL::INTEGER)) t(reading_id, fetched_at, source_updated_at, park_date, local_time, land, ride_id, ride, status, wait_min, is_stale, is_jump) WHERE false")
    con.execute("CREATE VIEW good AS SELECT * FROM silver WHERE is_stale = 0 AND is_jump = 0")

    gold = pathlib.Path(gold_dir)
    write = lambda name, obj: (gold / name).write_text(json.dumps(obj, separators=(",", ":")))

    today = con.execute("SELECT max(park_date) FROM silver").fetchone()[0]
    names = dict(con.execute("SELECT ride_id, any_value(ride) FROM silver GROUP BY ride_id").fetchall())
    series = {str(r): {"ride": names.get(r, str(r)), "points": []} for r in key_rides}
    for r in rows(con, "today"):
        series[str(r["ride_id"])]["points"].append([r["local_time"], r["wait_min"]])
    write("today.json", {"park_date": today, "series": series})
    write("days.json", rows(con, "days"))
    write("heatmap.json", rows(con, "heatmap"))
