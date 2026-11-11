"""Shared pieces of Spark jobs: Kafka sources, message schemas, PostgreSQL connection."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = "kafka:19092"  # INTERNAL listener: we are inside the compose network
POSITIONS_TOPIC = "pid.vehicle_positions"
DISRUPTIONS_TOPIC = "pid.disruptions"
ALERTS_TOPIC = "pid.alerts"

CHECKPOINT_ROOT = "/opt/spark/checkpoints"
DATA_ROOT = "/opt/spark/data"  # ./data on the host
RAW_POSITIONS_PATH = f"{DATA_ROOT}/raw/vehicle_positions"  # Parquet, one folder per date
GTFS_PATH = f"{DATA_ROOT}/gtfs"  # routes.txt and stops.txt, put there by scripts/load_gtfs.py

# Upper limit of Kafka messages one micro-batch may take. In normal operation a batch is ~1500 messages
# and the limit is never reached. It matters when a job starts with a big backlog (new checkpoint, or the
# job was off for hours): instead of one giant batch Spark catches up in several bounded ones.
MAX_OFFSETS_PER_TRIGGER = 200_000

ROUTE_TYPES = {0: "tram", 1: "metro", 2: "train", 3: "bus", 4: "ferry", 7: "funicular", 11: "trolleybus"}

# Schema of the JSON value written by producers/vehicle_positions.py.
# Spark cannot infer a schema from a stream, so we describe it ourselves.
POSITION_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("line", StringType()),
    StructField("route_id", StringType()),
    StructField("route_type", IntegerType()),
    StructField("headsign", StringType()),
    StructField("vehicle_id", LongType()),
    StructField("agency", StringType()),
    StructField("delay_s", IntegerType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("bearing", IntegerType()),
    StructField("speed", DoubleType()),
    StructField("state_position", StringType()),
    StructField("is_canceled", BooleanType()),
    StructField("last_stop_id", StringType()),
    StructField("last_stop_sequence", IntegerType()),
    StructField("next_stop_id", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),
])

# Schema of the JSON value written by producers/disruptions.py
DISRUPTION_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("kind", StringType()),
    StructField("title", StringType()),
    StructField("link", StringType()),
    StructField("lines", ArrayType(StringType())),
    StructField("time_from", StringType()),
    StructField("time_to", StringType()),
    StructField("priority", IntegerType()),
    StructField("description", StringType()),
    StructField("first_seen", StringType()),
    StructField("removed_at", StringType()),
])


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        # After a groupBy Spark splits the data into this many partitions = tasks (and, in streaming,
        # this many pieces of window state). The default 200 suits a cluster and gigabytes; here it made
        # every 20-second micro-batch run 200 nearly empty tasks and take longer than 20 seconds.
        # Note: a streaming query remembers the value in its checkpoint and ignores later changes.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "Europe/Prague")  # show timestamps in Prague time
    return spark


def route_type_name(column: str = "route_type"):
    """Column expression: GTFS route_type number -> 'tram', 'bus', ..."""
    mapping = F.create_map(*[F.lit(x) for kv in ROUTE_TYPES.items() for x in kv])
    return F.coalesce(F.element_at(mapping, F.col(column).cast("int")), F.lit("other"))


def read_kafka_stream(spark: SparkSession, topic: str, starting_offsets: str = "earliest") -> DataFrame:
    """Streaming DataFrame over a Kafka topic: columns key, value (bytes), partition, offset, timestamp, ..."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        # Used only on the very first start; afterwards the checkpoint says where to continue.
        # "earliest" = the whole topic, "latest" = only messages that arrive after the job started.
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )


def parse_positions(raw: DataFrame) -> DataFrame:
    """Kafka rows (streaming or batch) -> parsed vehicle positions, plus Kafka partition/offset."""
    return (
        raw.select(
            F.col("partition"),
            F.col("offset"),
            F.from_json(F.col("value").cast("string"), POSITION_SCHEMA).alias("p"),
        )
        .select("partition", "offset", "p.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time", F.to_timestamp("ingest_time"))
    )


def read_positions(spark: SparkSession, starting_offsets: str = "earliest") -> DataFrame:
    """Streaming DataFrame of parsed vehicle positions."""
    return parse_positions(read_kafka_stream(spark, POSITIONS_TOPIC, starting_offsets))


def parse_disruptions(raw: DataFrame) -> DataFrame:
    """Kafka rows (streaming or batch) of pid.disruptions -> parsed disruptions, plus the Kafka offset."""
    return (
        raw.select(
            F.col("offset"),
            F.from_json(F.col("value").cast("string"), DISRUPTION_SCHEMA).alias("d"),
        )
        .select("offset", "d.*")
        .where(F.col("id").isNotNull())
    )


def pg_connect():
    """Connection to PostgreSQL; settings come from the container environment (docker-compose.yml <- .env)."""
    import psycopg  # imported here so that jobs that never touch PostgreSQL do not need the library

    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
