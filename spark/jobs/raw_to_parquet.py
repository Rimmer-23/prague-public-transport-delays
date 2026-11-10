"""Step 4a: archive raw vehicle positions from Kafka into Parquet files, one folder per date.

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/raw_to_parquet.py
"""

from pyspark.sql import functions as F

from common import CHECKPOINT_ROOT, RAW_POSITIONS_PATH, create_spark, read_positions

TRIGGER = "5 minutes"  # one file per date every 5 min instead of several every 20 s (see docs/decisions.md)


def main() -> None:
    spark = create_spark("raw_to_parquet")

    positions = (
        read_positions(spark)
        # Partition column: the calendar day (Prague time) when the vehicle reported the position.
        # Rows that failed JSON parsing have no event_time and land in date=__HIVE_DEFAULT_PARTITION__.
        .withColumn("date", F.to_date("event_time"))
    )

    query = (
        positions
        # A micro-batch has one task per Kafka partition (3) and each task writes its own file.
        # coalesce(1) merges them into one task -> one file per date per batch.
        .coalesce(1)
        .writeStream.format("parquet")
        # No aggregation here: every row is new, so append is the only mode that makes sense
        # (and the only one the file sink supports).
        .outputMode("append")
        # Folder layout: <path>/date=2026-11-10/part-....parquet. The value lives in the folder name,
        # not in the files, and a query with WHERE date = ... reads only that folder.
        .partitionBy("date")
        .option("path", RAW_POSITIONS_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw_to_parquet")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    # The file sink prints nothing, so report each finished micro-batch ourselves.
    print(f"Writing {RAW_POSITIONS_PATH}, a batch every {TRIGGER}. Ctrl+C to stop.")
    last_batch = -1
    while not query.awaitTermination(10):  # False = still running after 10 s
        progress = query.lastProgress
        if progress and progress["batchId"] != last_batch:
            last_batch = progress["batchId"]
            print(f"{progress['timestamp']} batch {last_batch}: {progress['numInputRows']} rows written")


if __name__ == "__main__":
    main()
