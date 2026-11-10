"""Step 3b: average delay per line in 15-minute event-time windows.

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/delays_by_line.py
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import CHECKPOINT_ROOT, create_spark, read_positions

WINDOW = "15 minutes"
WATERMARK = "10 minutes"  # positions arrive up to ~5 min late (see docs/decisions.md)
TOP_N = 15

ROUTE_TYPES = {0: "tram", 1: "metro", 2: "train", 3: "bus", 4: "ferry", 7: "funicular", 11: "trolleybus"}


def show_batch(batch: DataFrame, batch_id: int) -> None:
    """Called for every micro-batch with the rows that changed in it (a normal, non-streaming DataFrame)."""
    rows = batch.count()
    print(f"\n=== Batch {batch_id}: {rows} (window, line) rows updated; top {TOP_N} by avg delay ===")
    if rows == 0:
        return
    (
        batch.withColumn("window_start", F.date_format("window.start", "yyyy-MM-dd HH:mm"))
        .withColumn("window_end", F.date_format("window.end", "HH:mm"))
        .withColumn("type", F.element_at(F.create_map(*[F.lit(x) for kv in ROUTE_TYPES.items() for x in kv]), F.col("route_type")))
        .orderBy(F.desc("avg_delay_s"))
        .select("window_start", "window_end", "line", "type", "positions", "trips", "avg_delay_s", "max_delay_s")
        .show(TOP_N, truncate=False)
    )


def aggregate_delays(positions: DataFrame) -> DataFrame:
    """Streaming aggregation shared by the console job (this file) and delays_to_postgres.py."""
    return (
        positions.where(F.col("delay_s").isNotNull() & F.col("line").isNotNull())
        # Watermark: how long we wait for late positions. Spark keeps a window's state until
        # (max event_time seen) - 10 min passes the window end, then finalises and forgets it.
        .withWatermark("event_time", WATERMARK)
        # Tumbling window: 14:00-14:15, 14:15-14:30, ... by the time the vehicle reported, not when we got it
        .groupBy(F.window("event_time", WINDOW), "line", "route_type")
        .agg(
            F.count("*").alias("positions"),
            F.approx_count_distinct("trip_id").alias("trips"),
            F.round(F.avg("delay_s")).cast("int").alias("avg_delay_s"),
            F.max("delay_s").alias("max_delay_s"),
        )
    )


def main() -> None:
    spark = create_spark("delays_by_line")
    delays = aggregate_delays(read_positions(spark))

    query = (
        delays.writeStream
        # update: in each batch emit only windows whose numbers changed (append would wait until a window is final)
        .outputMode("update")
        .foreachBatch(show_batch)
        # Checkpoint: Kafka offsets already processed + state of open windows. Restart continues from here.
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/delays_by_line")
        .trigger(processingTime="20 seconds")  # producer polls every 20 s
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
