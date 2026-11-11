"""Step 5b: latest position of every running trip -> PostgreSQL table vehicle_positions_latest (for the map).

No windows and no aggregation over time here: the job only keeps a "current state" table fresh.

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/positions_to_postgres.py
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import CHECKPOINT_ROOT, create_spark, pg_connect, read_positions

KEEP_HOURS = 2  # rows of trips that have not reported for this long are deleted

COLUMNS = ["line", "route_type", "headsign", "vehicle_id", "lat", "lon", "bearing", "delay_s", "last_stop_id"]

# Upsert by trip_id. The WHERE makes the statement safe against old data: a position is written only
# if it is newer than the one already in the table (matters when a batch is replayed after a crash).
UPSERT_SQL = f"""
    INSERT INTO vehicle_positions_latest (trip_id, event_time, {", ".join(COLUMNS)})
    VALUES (%s, to_timestamp(%s), {", ".join(["%s"] * len(COLUMNS))})
    ON CONFLICT (trip_id) DO UPDATE SET
        event_time = EXCLUDED.event_time,
        {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS)},
        updated_at = now()
    WHERE vehicle_positions_latest.event_time < EXCLUDED.event_time
"""

CLEANUP_SQL = f"DELETE FROM vehicle_positions_latest WHERE event_time < now() - interval '{KEEP_HOURS} hours'"


def latest_per_trip(batch: DataFrame) -> DataFrame:
    """A micro-batch can hold several positions of one trip; keep the newest one of each."""
    return (
        batch.where(F.col("trip_id").isNotNull() & F.col("event_time").isNotNull()
                    & F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .groupBy("trip_id")
        # max_by(x, y): the value of x in the row where y is the biggest. Packing the columns into
        # a struct lets us take the whole newest row with one aggregate.
        .agg(F.max_by(F.struct("event_time", *COLUMNS), "event_time").alias("newest"))
        .select("trip_id", F.col("newest.event_time").cast("double").alias("event_ts"),
                *[f"newest.{c}" for c in COLUMNS])
    )


def upsert_batch(batch: DataFrame, batch_id: int) -> None:
    rows = latest_per_trip(batch).collect()  # at most one row per running trip (~2000), safe to collect
    with pg_connect() as conn:  # one transaction: upsert + cleanup
        with conn.cursor() as cur:
            if rows:
                cur.executemany(UPSERT_SQL, [tuple(row) for row in rows])
            cur.execute(CLEANUP_SQL)
            removed = cur.rowcount
    print(f"[positions_to_postgres] batch {batch_id}: {len(rows)} trips updated, {removed} stale trips removed")


def start(spark: SparkSession) -> StreamingQuery:
    """Start the streaming query and return it without waiting (run_all.py starts several of them)."""
    # "latest": on its first start the job skips the history in the topic. A map of *current* positions
    # has no use for last week's data, unlike the aggregation jobs that read the topic from "earliest".
    positions = read_positions(spark, starting_offsets="latest")
    return (
        positions.writeStream
        .queryName("positions_to_postgres")
        # No aggregation in the streaming part, so there is no state and no watermark:
        # every row is handed to foreachBatch exactly as it arrived (append mode).
        .outputMode("append")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/positions_to_postgres")
        .trigger(processingTime="20 seconds")
        .start()
    )


def main() -> None:
    start(create_spark("positions_to_postgres")).awaitTermination()


if __name__ == "__main__":
    main()
