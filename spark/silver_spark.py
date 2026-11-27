"""The silver step in PySpark: the same cleaning build.py does, written the way it would run on a cluster.

bronze JSON snapshots -> exploded ride readings -> typed, deduplicated, flagged -> Parquet partitioned by park day.

Run:  spark-submit spark/silver_spark.py data/bronze out/silver_parquet
At Disney scale the input is a stream of playback events instead of ride snapshots,
but the moves are the same: parse, explode, dedupe on a natural key, flag, partition by day.
"""
import sys
from pyspark.sql import SparkSession, Window, functions as F, types as T

STALE_AFTER_S = 20 * 60
JUMP_LIMIT = 60

RIDE = T.StructType([T.StructField("id", T.LongType()), T.StructField("name", T.StringType()),
                     T.StructField("is_open", T.BooleanType()), T.StructField("wait_time", T.LongType()),
                     T.StructField("last_updated", T.StringType())])
PAYLOAD = T.StructType([T.StructField("lands", T.ArrayType(T.StructType([
    T.StructField("name", T.StringType()), T.StructField("rides", T.ArrayType(RIDE))])))])


def silver(spark, bronze_path):
    raw = (spark.read.option("multiLine", True).json(f"{bronze_path}/*/*.json")
           .where(F.col("http_status") == 200)
           .withColumn("p", F.from_json("payload", PAYLOAD))
           .where(F.col("p.lands").isNotNull()))

    rides = (raw.select("fetched_at", F.explode("p.lands").alias("land"))
             .select("fetched_at", F.col("land.name").alias("land"), F.explode("land.rides").alias("r"))
             .select(F.to_timestamp("fetched_at").alias("fetched_ts"), "fetched_at", "land",
                     F.col("r.id").alias("ride_id"), F.col("r.name").alias("ride"),
                     F.col("r.is_open").alias("is_open"), F.col("r.wait_time").alias("wait_time"),
                     F.to_timestamp("r.last_updated").alias("src_ts")))

    # Stale = the newest reading in the snapshot is old while something is open (quiet overnight is fine).
    snap = (rides.groupBy("fetched_at").agg(F.max("src_ts").alias("newest"), F.max(F.col("is_open").cast("int")).alias("any_open")))
    rides = (rides.join(snap, "fetched_at")
             .withColumn("is_stale", ((F.col("fetched_ts").cast("long") - F.col("newest").cast("long") > STALE_AFTER_S)
                                      & (F.col("any_open") == 1)).cast("int")))

    # Same reading fetched twice: keep the first copy.
    first = Window.partitionBy("ride_id", "src_ts").orderBy("fetched_ts")
    rides = rides.withColumn("k", F.row_number().over(first)).where("k = 1").drop("k")

    local = F.from_utc_timestamp("src_ts", "America/New_York")
    rides = (rides
             .withColumn("status", F.when(~F.col("is_open"), "closed").when(F.col("wait_time") == 0, "open_no_wait").otherwise("open"))
             .withColumn("wait_min", F.when(F.col("status") == "closed", None).otherwise(F.col("wait_time")).cast("int"))
             .withColumn("park_date", F.date_format(local, "yyyy-MM-dd"))
             .withColumn("local_time", F.date_format(local, "HH:mm")))

    # Impossible jump: an open wait more than 60 min away from the ride's last believable open wait.
    # Pass 1 compares with the previous reading; pass 2 re-checks against the last reading pass 1 trusted,
    # so the reading right after a spike isn't flagged too. (build.py walks the readings one by one;
    # the two can only disagree on several jumps in a row.)
    prev = Window.partitionBy("ride_id").orderBy("src_ts")
    before = prev.rowsBetween(Window.unboundedPreceding, -1)
    opens = rides.where("status = 'open'").withColumn("j1", F.abs(F.col("wait_min") - F.lag("wait_min").over(prev)) > JUMP_LIMIT)
    opens = opens.withColumn("baseline", F.last(F.when(~F.coalesce("j1", F.lit(False)), F.col("wait_min")), ignorenulls=True).over(before))
    opens = opens.withColumn("is_jump", F.coalesce((F.abs(F.col("wait_min") - F.col("baseline")) > JUMP_LIMIT).cast("int"), F.lit(0))).select("ride_id", "src_ts", "is_jump")
    rides = rides.join(opens, ["ride_id", "src_ts"], "left").withColumn("is_jump", F.coalesce("is_jump", F.lit(0)))

    return rides.select(
        F.concat_ws("-", "ride_id", F.col("src_ts").cast("long")).alias("reading_id"), "fetched_at",
        F.date_format("src_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("source_updated_at"), "park_date", "local_time",
        "land", "ride_id", "ride", "status", "wait_min", "is_stale", "is_jump")


if __name__ == "__main__":
    bronze_path, out = sys.argv[1], sys.argv[2]
    spark = SparkSession.builder.appName("fop-silver").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    df = silver(spark, bronze_path)
    df.write.mode("overwrite").partitionBy("park_date").parquet(out)
    print(f"silver rows: {df.count()}")
    spark.stop()
