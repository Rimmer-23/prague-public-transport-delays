"""Step 7: disruptions from Kafka topic pid.disruptions -> PostgreSQL table disruptions.

The topic carries the newest version of every disruption (see producers/disruptions.py); the job keeps
one row per disruption up to date, so Grafana can show disruptions next to the delays.

Run (see docs/commands.md):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/disruptions_to_postgres.py
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import CHECKPOINT_ROOT, DISRUPTIONS_TOPIC, create_spark, parse_disruptions, pg_connect, read_kafka_stream

COLUMNS = ["kind", "title", "link", "lines", "time_from", "time_to", "priority", "description", "first_seen", "removed_at"]
TIMESTAMPS = {"time_from", "time_to", "first_seen", "removed_at"}  # ISO strings with an offset -> timestamptz

UPSERT_SQL = f"""
    INSERT INTO disruptions (id, {", ".join(COLUMNS)})
    VALUES (%s, {", ".join("%s::timestamptz" if c in TIMESTAMPS else "%s" for c in COLUMNS)})
    ON CONFLICT (id) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS)},
        updated_at = now()
"""


def upsert_batch(batch: DataFrame, batch_id: int) -> None:
    rows = (
        batch.groupBy("id")
        # Several versions of one disruption can sit in one micro-batch. The topic has a single partition,
        # so a bigger offset always means a newer version: keep the row with the biggest offset.
        .agg(F.max_by(F.struct(*COLUMNS), "offset").alias("newest"))
        .select("id", *[f"newest.{c}" for c in COLUMNS])
        .collect()
    )
    if not rows:
        return
    with pg_connect() as conn:
        with conn.cursor() as cur:
            # psycopg turns a Python list into a PostgreSQL array (column `lines`)
            cur.executemany(UPSERT_SQL, [tuple(row) for row in rows])
    print(f"[disruptions_to_postgres] batch {batch_id}: {len(rows)} disruptions upserted")


def start(spark: SparkSession) -> StreamingQuery:
    """Start the streaming query and return it without waiting (run_all.py starts several of them)."""
    disruptions = parse_disruptions(read_kafka_stream(spark, DISRUPTIONS_TOPIC))
    return (
        disruptions.writeStream
        .queryName("disruptions_to_postgres")
        .outputMode("append")  # no streaming aggregation: rows go to foreachBatch as they arrive
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/disruptions_to_postgres")
        .trigger(processingTime="1 minute")  # the producer polls the feeds every 2 minutes
        .start()
    )


def main() -> None:
    start(create_spark("disruptions_to_postgres")).awaitTermination()


if __name__ == "__main__":
    main()
