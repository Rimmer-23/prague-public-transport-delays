"""Step 4b: average delay per line in 15-minute windows -> PostgreSQL table line_delays_15m.

Same aggregation as delays_by_line.py, but every micro-batch is upserted into PostgreSQL instead of printed.

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/delays_to_postgres.py
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import CHECKPOINT_ROOT, create_spark, pg_connect, read_positions
from delays_by_line import aggregate_delays

# Upsert: insert the row, or overwrite it if this (window, line) is already in the table.
# In update mode Spark sends the full current numbers of a window (not a difference), so overwriting is correct.
UPSERT_SQL = """
    INSERT INTO line_delays_15m
        (window_start, window_end, line, route_type, positions, trips, avg_delay_s, max_delay_s)
    VALUES (to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s)
    ON CONFLICT (window_start, line, route_type) DO UPDATE SET
        window_end  = EXCLUDED.window_end,
        positions   = EXCLUDED.positions,
        trips       = EXCLUDED.trips,
        avg_delay_s = EXCLUDED.avg_delay_s,
        max_delay_s = EXCLUDED.max_delay_s,
        updated_at  = now()
"""


def upsert_batch(batch: DataFrame, batch_id: int) -> None:
    """Called for every micro-batch with the (window, line) rows that changed in it."""
    rows = (
        batch.select(
            # Seconds since 1970 (UTC): an unambiguous instant, no time zone guessing between Spark, Python and SQL
            F.col("window.start").cast("long"),
            F.col("window.end").cast("long"),
            "line",
            F.coalesce("route_type", F.lit(-1)),  # part of the primary key, must not be NULL
            "positions",
            "trips",
            "avg_delay_s",
            "max_delay_s",
        )
        # collect() brings all rows to the driver. Fine for aggregates (hundreds of rows per batch),
        # never do this with raw positions.
        .collect()
    )
    if not rows:
        return

    # One transaction per micro-batch: the whole batch is in the table or nothing is.
    # Spark may run the same batch again after a crash (at-least-once); the upsert makes a re-run harmless.
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, [tuple(row) for row in rows])

    print(f"[delays_to_postgres] batch {batch_id}: {len(rows)} (window, line) rows upserted into line_delays_15m")


def start(spark: SparkSession) -> StreamingQuery:
    """Start the streaming query and return it without waiting (run_all.py starts several of them)."""
    delays = aggregate_delays(read_positions(spark))
    return (
        delays.writeStream
        .queryName("delays_to_postgres")
        .outputMode("update")  # only windows whose numbers changed in this batch
        .foreachBatch(upsert_batch)
        # Own checkpoint: this job reads Kafka independently of delays_by_line and raw_to_parquet
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/delays_to_postgres")
        .trigger(processingTime="20 seconds")
        .start()
    )


def main() -> None:
    start(create_spark("delays_to_postgres")).awaitTermination()


if __name__ == "__main__":
    main()
