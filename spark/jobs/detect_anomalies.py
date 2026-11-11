"""Step 6: find lines that are much later than usual right now and publish alerts to Kafka topic pid.alerts.

    pid.vehicle_positions -> sliding windows per line -> compare with the line's baseline
                          -> new alerts: PostgreSQL line_alerts (memory + history) and Kafka pid.alerts

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/detect_anomalies.py
"""

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import ALERTS_TOPIC, CHECKPOINT_ROOT, KAFKA_BOOTSTRAP, ROUTE_TYPES, create_spark, pg_connect, read_positions

# Sliding window: 10 minutes long, a new one starts every 5 minutes (12:00-12:10, 12:05-12:15, ...),
# so every position falls into two windows. With tumbling windows a line is "blind" right after a window
# starts (too few trips yet); here there is always a window that already holds at least 5 minutes of data.
WINDOW = "10 minutes"
SLIDE = "5 minutes"
WATERMARK = "10 minutes"

MIN_TRIPS = 3               # fewer trips in a window = one late vehicle, not a problem of the line
MIN_ALERT_DELAY_S = 180     # never alert below 3 minutes, however punctual the line usually is
FALLBACK_THRESHOLD_S = 300  # for lines that have no baseline yet
SIGMAS = 3.0                # "much later than usual" = baseline average + 3 standard deviations
COOLDOWN_MINUTES = 30       # a line that stays late is reported again only after this long

# Insert the alert unless the line already has one within the cooldown (the same window included).
# RETURNING gives a row back only when something was inserted - that is how we know the alert is new.
INSERT_ALERT_SQL = f"""
    INSERT INTO line_alerts
        (line, route_type, window_start, window_end, avg_delay_s, max_delay_s, trips, positions, threshold_s, baseline_avg_s)
    SELECT %(line)s::text, %(route_type)s::smallint, to_timestamp(%(window_start)s::bigint), to_timestamp(%(window_end)s::bigint),
           %(avg_delay_s)s::int, %(max_delay_s)s::int, %(trips)s::int, %(positions)s::int,
           %(threshold_s)s::int, %(baseline_avg_s)s::int
    WHERE NOT EXISTS (
        SELECT 1 FROM line_alerts
        WHERE line = %(line)s::text AND route_type = %(route_type)s::smallint
          AND window_start > to_timestamp(%(window_start)s::bigint) - interval '{COOLDOWN_MINUTES} minutes'
          AND window_start < to_timestamp(%(window_start)s::bigint) + interval '{COOLDOWN_MINUTES} minutes'
    )
    ON CONFLICT DO NOTHING
    RETURNING 1
"""

BASELINE_SCHEMA = "line string, route_type int, baseline_avg_s double, baseline_std_s double"


def aggregate_sliding(positions: DataFrame) -> DataFrame:
    return (
        positions.where(F.col("delay_s").isNotNull() & F.col("line").isNotNull())
        .withWatermark("event_time", WATERMARK)
        # Third argument = slide. Without it the window would be tumbling, as in delays_by_line.py.
        .groupBy(F.window("event_time", WINDOW, SLIDE), "line", F.coalesce("route_type", F.lit(-1)).alias("route_type"))
        .agg(
            F.count("*").alias("positions"),
            F.approx_count_distinct("trip_id").alias("trips"),
            F.round(F.avg("delay_s")).cast("int").alias("avg_delay_s"),
            F.max("delay_s").alias("max_delay_s"),
        )
    )


def iso(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).isoformat(timespec="seconds")


def detect_batch(batch: DataFrame, batch_id: int) -> None:
    """Called for every micro-batch with the (window, line) rows that changed in it."""
    spark = batch.sparkSession
    # Cheap filter first: most windows are nowhere near an alert and need no baseline lookup at all
    suspects = batch.where((F.col("trips") >= MIN_TRIPS) & (F.col("avg_delay_s") >= MIN_ALERT_DELAY_S))

    with pg_connect() as conn:  # one transaction for the whole batch, committed at the end of the block
        with conn.cursor() as cur:
            # The baseline is read again on every batch: it is tiny (one row per line) and this way the job
            # picks up the numbers of the nightly batch job without a restart.
            cur.execute("SELECT line, route_type::int, avg_delay_s, std_delay_s FROM line_delay_baseline")
            baseline = spark.createDataFrame(cur.fetchall(), BASELINE_SCHEMA)

            candidates = (
                # broadcast: the small table is copied to every task, so the join needs no shuffle
                suspects.join(F.broadcast(baseline), ["line", "route_type"], "left")
                .withColumn(
                    "threshold_s",
                    # An explicit when(): greatest() skips NULLs, so for a line without a baseline row
                    # greatest(180, NULL) would quietly give 180 instead of "unknown".
                    F.when(
                        F.col("baseline_avg_s").isNotNull(),
                        F.greatest(F.lit(MIN_ALERT_DELAY_S), F.ceil(F.col("baseline_avg_s") + SIGMAS * F.col("baseline_std_s"))),
                    ).otherwise(F.lit(FALLBACK_THRESHOLD_S)).cast("int"),
                )
                .where(F.col("avg_delay_s") >= F.col("threshold_s"))
                .select(
                    "line", "route_type",
                    F.col("window.start").cast("long").alias("window_start"),
                    F.col("window.end").cast("long").alias("window_end"),
                    "avg_delay_s", "max_delay_s", "trips", "positions", "threshold_s",
                    F.round("baseline_avg_s").cast("int").alias("baseline_avg_s"),
                )
                .orderBy("window_start")  # oldest first, so the cooldown works inside one batch too
                .collect()
            )

            new_alerts = []
            for row in candidates:
                alert = row.asDict()
                cur.execute(INSERT_ALERT_SQL, alert)
                if cur.fetchone() is None:
                    continue  # already reported, or the line is in its cooldown
                new_alerts.append({
                    "alert_id": f"{alert['line']}:{alert['route_type']}:{alert['window_start']}",
                    **alert,
                    "vehicle_type": ROUTE_TYPES.get(alert["route_type"], "other"),
                    "window_start": iso(alert["window_start"]),
                    "window_end": iso(alert["window_end"]),
                    "detected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                })

            if new_alerts:
                # Spark as a Kafka *producer*: a DataFrame with columns key and value written to a topic.
                # Done before the PostgreSQL commit: if Kafka is down the batch fails, the inserts are rolled
                # back and Spark repeats the batch later - an alert may be sent twice, but is never lost.
                (
                    spark.createDataFrame(
                        [(a["line"], json.dumps(a, ensure_ascii=False)) for a in new_alerts], "key string, value string"
                    )
                    .write.format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                    .option("topic", ALERTS_TOPIC)
                    .save()
                )

    if candidates:
        print(f"[detect_anomalies] batch {batch_id}: {len(candidates)} windows over the threshold, {len(new_alerts)} new alerts"
              + "".join(f"\n    line {a['line']} ({a['vehicle_type']}): avg {a['avg_delay_s']} s, threshold {a['threshold_s']} s, "
                        f"{a['trips']} trips, window from {a['window_start']}" for a in new_alerts))


def start(spark: SparkSession) -> StreamingQuery:
    """Start the streaming query and return it without waiting (run_all.py starts several of them)."""
    windows = aggregate_sliding(read_positions(spark))
    return (
        windows.writeStream
        .queryName("detect_anomalies")
        # update: a window is checked every time its numbers change, i.e. while it is still open.
        # append would hand us a window only after it is final (window end + watermark) - too late for an alert.
        .outputMode("update")
        .foreachBatch(detect_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/detect_anomalies")
        .trigger(processingTime="20 seconds")
        .start()
    )


def main() -> None:
    start(create_spark("detect_anomalies")).awaitTermination()


if __name__ == "__main__":
    main()
