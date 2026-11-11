"""Step 8: daily batch job over the Parquet archive.

For every requested day it computes
    daily_line_stats         which lines were late (average, median, 90th percentile, share of late positions)
    hourly_delay_stats       delay by hour and vehicle type: rush hours, trams vs buses
    daily_segment_delays     between which stops trips lose time
    daily_disruption_impact  delay of a line during a disruption vs the rest of its day
and, from the last 14 days, the "usual" delay of every line (line_delay_baseline) used by detect_anomalies.py.

Run (see CLAUDE.md, "Команды"):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/daily_stats.py [--date 2026-11-10 | --all] [--explain]

Without arguments: yesterday (Prague time). The job is idempotent - run it again for the same day and the
day's rows are replaced, not doubled.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    DISRUPTIONS_TOPIC, GTFS_PATH, KAFKA_BOOTSTRAP, RAW_POSITIONS_PATH,
    create_spark, parse_disruptions, pg_connect, route_type_name,
)
from detect_anomalies import MIN_TRIPS, SLIDE, WINDOW

LATE_THRESHOLD_S = 180     # "late" = 3 minutes or more behind the timetable
MIN_SEGMENT_TRIPS = 5      # a segment with fewer trips a day says little; skipping them also keeps the table small
BASELINE_DAYS = 14         # history used for the usual delay of a line
BASELINE_MIN_WINDOWS = 12  # = at least 1 hour of data, otherwise the line gets no baseline
ISO = "yyyy-MM-dd'T'HH:mm:ssXXX"  # timestamp with offset as text: unambiguous on its way to PostgreSQL


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=date.fromisoformat, help="day to process, YYYY-MM-DD (default: yesterday)")
    group.add_argument("--all", action="store_true", help="process every day found in the archive")
    parser.add_argument("--explain", action="store_true", help="print the physical plan of the line statistics")
    return parser.parse_args()


def late_share() -> F.Column:
    return F.avg((F.col("delay_s") >= LATE_THRESHOLD_S).cast("int"))


def clean(raw: DataFrame) -> DataFrame:
    """Rows usable for statistics, only the columns we need."""
    return (
        raw.where(F.col("delay_s").isNotNull() & F.col("line").isNotNull() & F.col("trip_id").isNotNull())
        # After a producer restart the first poll resends positions that are already in Kafka
        .dropDuplicates(["trip_id", "event_time"])
        .select(
            "date", "event_time", "trip_id", "line", "route_id",
            F.coalesce("route_type", F.lit(-1)).alias("route_type"),
            "delay_s", "last_stop_id", "last_stop_sequence",
        )
    )


def read_gtfs(spark: SparkSession, name: str) -> DataFrame:
    path = f"{GTFS_PATH}/{name}"
    if not os.path.exists(path):  # local mode: the driver sees the same files as the tasks
        sys.exit(f"{path} not found. Load the GTFS reference data first: python scripts/load_gtfs.py")
    return spark.read.csv(path, header=True)


def line_stats(positions: DataFrame, routes: DataFrame) -> DataFrame:
    return (
        # Broadcast join: routes.txt is tiny (~900 rows), so Spark ships a copy of it to every task and
        # looks names up locally. The big side (millions of positions) is not shuffled for the join at all.
        positions.join(F.broadcast(routes.select("route_id", "route_long_name")), "route_id", "left")
        .groupBy("date", "line", "route_type")
        .agg(
            F.first("route_long_name", ignorenulls=True).alias("route_long_name"),
            F.countDistinct("trip_id").alias("trips"),
            F.count("*").alias("positions"),
            F.round(F.avg("delay_s")).cast("int").alias("avg_delay_s"),
            # Percentiles: unlike the average they are not dragged up by a few extreme vehicles.
            # "approx" because an exact percentile needs all values sorted; this one works in one pass.
            F.percentile_approx("delay_s", 0.5).alias("p50_delay_s"),
            F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
            F.max("delay_s").alias("max_delay_s"),
            late_share().alias("late_share"),
        )
    )


def hourly_stats(positions: DataFrame) -> DataFrame:
    return (
        positions.groupBy("date", F.hour("event_time").alias("hour"), "route_type")  # hour in Prague time
        .agg(
            F.countDistinct("trip_id").alias("trips"),
            F.count("*").alias("positions"),
            F.round(F.avg("delay_s")).cast("int").alias("avg_delay_s"),
            F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
            late_share().alias("late_share"),
        )
    )


def segment_delays(positions: DataFrame, stops: DataFrame) -> DataFrame:
    """How much delay trips gain between two consecutive stops."""
    # 1. One row per (trip, stop): the delay in the first position reported after the trip reached the stop
    at_stop = (
        positions.where(F.col("last_stop_id").isNotNull() & F.col("last_stop_sequence").isNotNull())
        .groupBy("date", "trip_id", "line", "route_type", "last_stop_sequence", "last_stop_id")
        .agg(F.min_by("delay_s", "event_time").alias("delay_s"))
    )
    # 2. Window function: unlike groupBy it keeps every row and lets a row look at its neighbours.
    #    Within one trip, ordered by stop sequence, lag() is "the same column in the previous stop's row".
    #    (This "window" has nothing to do with the time windows of streaming.)
    trip = Window.partitionBy("date", "trip_id").orderBy("last_stop_sequence")
    hops = (
        at_stop.withColumn("from_stop_id", F.lag("last_stop_id").over(trip))
        .withColumn("from_sequence", F.lag("last_stop_sequence").over(trip))
        .withColumn("delay_gain_s", F.col("delay_s") - F.lag("delay_s").over(trip))
        # only neighbouring stops: if we missed a stop in between, the gain cannot be assigned to one segment
        .where(F.col("last_stop_sequence") - F.col("from_sequence") == 1)
        .withColumnRenamed("last_stop_id", "to_stop_id")
    )
    # 3. Aggregate all trips of the day over the segment
    segments = (
        hops.groupBy("date", "from_stop_id", "to_stop_id", "route_type")
        .agg(
            F.count("*").alias("trips"),
            F.avg("delay_gain_s").alias("avg_delay_gain_s"),
            F.percentile_approx("delay_gain_s", 0.9).alias("p90_delay_gain_s"),
            F.concat_ws(", ", F.sort_array(F.collect_set("line"))).alias("lines"),
        )
        .where(F.col("trips") >= MIN_SEGMENT_TRIPS)
    )
    # 4. Names and coordinates from GTFS stops.txt (joined twice: once for each end of the segment)
    stop = stops.select(
        "stop_id", "stop_name", F.col("stop_lat").cast("double").alias("lat"), F.col("stop_lon").cast("double").alias("lon")
    )
    from_stop = F.broadcast(stop.select(F.col("stop_id").alias("from_stop_id"), F.col("stop_name").alias("from_stop_name")))
    to_stop = F.broadcast(stop.select(F.col("stop_id").alias("to_stop_id"), F.col("stop_name").alias("to_stop_name"), "lat", "lon"))
    return segments.join(from_stop, "from_stop_id", "left").join(to_stop, "to_stop_id", "left")


def read_disruptions(spark: SparkSession) -> DataFrame:
    """Latest version of every disruption, one row per affected line: (id, kind, title, line, time_from, active_to)."""
    # spark.read (not readStream) on Kafka: a batch query over everything that is in the topic right now.
    # The topic is compacted, so "everything" is roughly one message per disruption, however old.
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", DISRUPTIONS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    columns = ["kind", "title", "lines", "time_from", "time_to", "removed_at"]
    return (
        parse_disruptions(raw)
        .groupBy("id")
        .agg(F.max_by(F.struct(*columns), "offset").alias("newest"))  # compaction may not have run yet
        .select("id", "newest.*")
        .withColumn("time_from", F.to_timestamp("time_from"))
        .withColumn("time_to", F.to_timestamp("time_to"))
        # no end time ("until further notice"): it ended when it left the feed, or is still going on
        .withColumn("active_to", F.coalesce("time_to", F.to_timestamp("removed_at"), F.current_timestamp()))
        .where(F.col("time_from").isNotNull())
        .withColumn("line", F.explode("lines"))  # one row per element of the array
        .select(F.col("id").alias("disruption_id"), "kind", "title", "line", "time_from", "time_to", "active_to")
    )


def disruption_impact(positions: DataFrame, disruptions: DataFrame) -> DataFrame:
    """Delay of a line while a disruption was active vs the same line, same day, with no disruption."""
    windows = (
        positions.groupBy("date", F.window("event_time", "15 minutes").alias("w"), "line")
        .agg(F.count("*").alias("positions"), F.sum("delay_s").alias("delay_sum"))
        .select("date", "line", F.col("w.start").alias("w_start"), F.col("w.end").alias("w_end"), "positions", "delay_sum")
    )
    d = F.broadcast(disruptions).alias("d")
    w = windows.alias("w")
    # Not an equality join only: same line AND the time intervals overlap
    overlaps = (F.col("w.line") == F.col("d.line")) & (F.col("w.w_start") < F.col("d.active_to")) & (F.col("w.w_end") > F.col("d.time_from"))

    during = (
        w.join(d, overlaps, "inner")
        .groupBy("w.date", "d.disruption_id", "w.line", "d.kind", "d.title", "d.time_from", "d.time_to")
        .agg(
            F.sum("w.positions").cast("int").alias("positions_during"),
            F.round(F.sum("w.delay_sum") / F.sum("w.positions")).cast("int").alias("avg_delay_during_s"),
        )
    )
    other = (
        w.join(d, overlaps, "left_anti")  # windows of the line that no disruption touches
        .groupBy("date", "line")
        .agg(
            F.sum("positions").cast("int").alias("positions_other"),
            F.round(F.sum("delay_sum") / F.sum("positions")).cast("int").alias("avg_delay_other_s"),
        )
    )
    return (
        during.join(other, ["date", "line"], "left")
        .withColumn("time_from", F.date_format("time_from", ISO))
        .withColumn("time_to", F.date_format("time_to", ISO))
    )


def baseline(recent: DataFrame) -> DataFrame:
    """Usual delay of a line: mean and standard deviation of its window averages (same windows as the detector)."""
    windows = (
        recent.groupBy(F.window("event_time", WINDOW, SLIDE), "line", "route_type")
        .agg(F.countDistinct("trip_id").alias("trips"), F.avg("delay_s").alias("avg_delay_s"), F.min("date").alias("date"))
        .where(F.col("trips") >= MIN_TRIPS)
    )
    return (
        windows.groupBy("line", "route_type")
        .agg(
            F.count("*").cast("int").alias("windows"),
            F.avg("avg_delay_s").alias("avg_delay_s"),
            F.stddev_pop("avg_delay_s").alias("std_delay_s"),
            F.min("date").alias("date_from"),
            F.max("date").alias("date_to"),
        )
        .where(F.col("windows") >= BASELINE_MIN_WINDOWS)
    )


TABLES = {
    "daily_line_stats": ["date", "line", "route_type", "route_long_name", "trips", "positions", "avg_delay_s",
                         "p50_delay_s", "p90_delay_s", "max_delay_s", "late_share"],
    "hourly_delay_stats": ["date", "hour", "route_type", "trips", "positions", "avg_delay_s", "p90_delay_s", "late_share"],
    "daily_segment_delays": ["date", "from_stop_id", "to_stop_id", "route_type", "from_stop_name", "to_stop_name",
                             "lat", "lon", "lines", "trips", "avg_delay_gain_s", "p90_delay_gain_s"],
    "daily_disruption_impact": ["date", "disruption_id", "line", "kind", "title", "time_from", "time_to",
                                "positions_during", "avg_delay_during_s", "positions_other", "avg_delay_other_s"],
}
BASELINE_COLUMNS = ["line", "route_type", "windows", "avg_delay_s", "std_delay_s", "date_from", "date_to"]


def copy_rows(cur, table: str, columns: list[str], rows: list) -> None:
    # COPY: PostgreSQL's bulk load - one stream of rows instead of one INSERT per row
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(tuple(row))


def main() -> None:
    args = parse_args()
    spark = create_spark("daily_stats")

    # Nothing is read yet: Spark only lists the folders and takes the schema from the Parquet footers.
    raw = spark.read.parquet(RAW_POSITIONS_PATH)
    archive_dates = sorted(r["date"] for r in raw.select("date").distinct().collect() if r["date"] is not None)
    if args.all:
        dates = archive_dates
    else:
        dates = [args.date or datetime.now(ZoneInfo("Europe/Prague")).date() - timedelta(days=1)]
    if not archive_dates:
        sys.exit("The archive is empty: let raw_to_parquet.py run for a while first.")
    requested, dates = dates, [d for d in dates if d in archive_dates]
    if not dates:
        # e.g. the very first day of collecting: yesterday does not exist yet. Daily tables get nothing,
        # but the baseline below is still computed from whatever the archive has.
        print(f"No data for {', '.join(map(str, requested))}. Days in the archive: {', '.join(map(str, archive_dates))}")
    else:
        print(f"Processing {len(dates)} day(s): {', '.join(map(str, dates))}")

    # Partition pruning: `date` is the folder name (date=2026-11-10/), so this filter makes Spark open
    # only the folders of the requested days instead of the whole archive.
    positions = clean(raw.where(F.col("date").isin(dates)))
    # Four results are computed from the same rows. cache() keeps them after the first computation,
    # otherwise each result would read and deduplicate the Parquet files again.
    positions.cache()

    lines = line_stats(positions, read_gtfs(spark, "routes.txt"))
    if args.explain:
        lines.explain()  # look for "PartitionFilters: [... date ...]" and "BroadcastHashJoin"

    recent_from = max(archive_dates) - timedelta(days=BASELINE_DAYS - 1)
    results = {
        "daily_line_stats": lines,
        "hourly_delay_stats": hourly_stats(positions),
        "daily_segment_delays": segment_delays(positions, read_gtfs(spark, "stops.txt")),
        "daily_disruption_impact": disruption_impact(positions, read_disruptions(spark)),
    }
    # Up to here everything was a plan. collect() is the action that makes Spark actually run it.
    collected = {table: df.select(*TABLES[table]).collect() for table, df in results.items()} if dates else {}
    baseline_rows = baseline(clean(raw.where(F.col("date") >= F.lit(recent_from)))).select(*BASELINE_COLUMNS).collect()

    # One transaction for everything: after a crash the database has either the old numbers or the new ones.
    with pg_connect() as conn:
        with conn.cursor() as cur:
            for table, rows in collected.items():
                # delete + insert of the same days = the job can be repeated safely (idempotent)
                cur.execute(f"DELETE FROM {table} WHERE date = ANY(%s)", (dates,))
                copy_rows(cur, table, TABLES[table], rows)
                print(f"{table}: {len(rows)} rows written")
            cur.execute("DELETE FROM line_delay_baseline")
            copy_rows(cur, "line_delay_baseline", BASELINE_COLUMNS, baseline_rows)
            print(f"line_delay_baseline: {len(baseline_rows)} lines (data since {recent_from})")

    if not dates:
        spark.stop()
        return

    # A short report in the console, from the already computed (cached) data
    last = dates[-1]
    print(f"\n=== {last}: most delayed lines (at least 20 trips) ===")
    (
        lines.where((F.col("date") == F.lit(last)) & (F.col("trips") >= 20))
        .withColumn("type", route_type_name())
        .withColumn("late_%", F.round(F.col("late_share") * 100, 1))
        .orderBy(F.desc("avg_delay_s"))
        .select("line", "type", "route_long_name", "trips", "avg_delay_s", "p50_delay_s", "p90_delay_s", "late_%")
        .show(10, truncate=40)
    )
    print(f"=== {last}: average delay (s) by hour and vehicle type ===")
    (
        positions.where(F.col("date") == F.lit(last))
        .withColumn("type", route_type_name())
        .groupBy(F.hour("event_time").alias("hour"))
        .pivot("type", ["tram", "bus", "train", "metro", "trolleybus"])  # rows -> columns
        .agg(F.round(F.avg("delay_s")).cast("int"))
        .orderBy("hour")
        .show(24)
    )
    spark.stop()


if __name__ == "__main__":
    main()
