"""Shared pieces of Spark jobs: Kafka source and the schema of pid.vehicle_positions messages."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = "kafka:19092"  # INTERNAL listener: we are inside the compose network
POSITIONS_TOPIC = "pid.vehicle_positions"
CHECKPOINT_ROOT = "/opt/spark/checkpoints"

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


def create_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "Europe/Prague")  # show timestamps in Prague time
    return spark


def read_positions(spark: SparkSession) -> DataFrame:
    """Streaming DataFrame of parsed vehicle positions, plus Kafka partition/offset."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", POSITIONS_TOPIC)
        # Used only on the very first start; afterwards the checkpoint says where to continue.
        .option("startingOffsets", "earliest")
        .load()
    )
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
