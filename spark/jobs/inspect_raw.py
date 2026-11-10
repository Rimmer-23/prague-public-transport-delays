"""Step 4a check: read the Parquet archive back with a batch (non-streaming) Spark job.

Run (no Kafka connector needed, we only read files):
    docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/jobs/inspect_raw.py
"""

from pyspark.sql import functions as F

from common import RAW_POSITIONS_PATH, create_spark


def main() -> None:
    spark = create_spark("inspect_raw")

    # spark.read (not readStream): a normal DataFrame over all files that exist right now.
    # The schema comes from the Parquet files themselves; `date` is restored from folder names.
    positions = spark.read.parquet(RAW_POSITIONS_PATH)
    positions.printSchema()

    print("Rows per date:")
    (
        # _metadata is a hidden column every file source has: which file a row was read from
        positions.withColumn("file", F.col("_metadata.file_path"))
        .groupBy("date")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("file").alias("files"),
            F.countDistinct("trip_id").alias("trips"),
            F.min("event_time").alias("first_event"),
            F.max("event_time").alias("last_event"),
        )
        .orderBy("date")
        .show(truncate=False)
    )

    # (partition, offset) identifies a Kafka message. Equal numbers = nothing was written twice.
    total = positions.count()
    unique = positions.select("partition", "offset").distinct().count()
    print(f"Rows: {total}, distinct Kafka (partition, offset): {unique}")

    print("Rows per Kafka partition (max_offset + 1 should match kafka-get-offsets.sh while the job keeps up):")
    (
        positions.groupBy("partition")
        .agg(F.count("*").alias("rows"), F.max("offset").alias("max_offset"))
        .orderBy("partition")
        .show()
    )

    print("Sample:")
    (
        positions.where(F.col("delay_s").isNotNull())
        .select("date", "event_time", "line", "route_type", "trip_id", "delay_s", "lat", "lon")
        .orderBy(F.desc("event_time"))
        .show(5, truncate=False)
    )


if __name__ == "__main__":
    main()
