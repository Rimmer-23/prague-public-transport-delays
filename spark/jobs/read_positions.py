"""Step 3a: read pid.vehicle_positions from Kafka with Spark Structured Streaming and print parsed rows.

Run (see docs/commands.md):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/read_positions.py
"""

from common import create_spark, read_positions


def main() -> None:
    spark = create_spark("read_positions")
    positions = read_positions(spark)

    query = (
        positions.select("partition", "offset", "trip_id", "line", "route_type", "delay_s", "event_time")
        .writeStream.format("console")
        .outputMode("append")  # only new rows of each micro-batch
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")  # start a micro-batch every 10 s
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
