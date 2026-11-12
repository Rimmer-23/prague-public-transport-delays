"""All streaming queries of the project in one Spark application.

Each job in this folder can be started on its own (good for learning and debugging). Running them all that
way would mean five JVMs; here one driver runs five independent streaming queries. Every query still has
its own checkpoint, its own Kafka offsets and its own trigger - they only share the process.

Run (see docs/commands.md):
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 \
        --conf spark.jars.ivy=/opt/spark/ivy \
        /opt/spark/jobs/run_all.py

Do not start a job separately while it is running here: two queries on one checkpoint corrupt it.
"""

import delays_to_postgres
import detect_anomalies
import disruptions_to_postgres
import positions_to_postgres
import raw_to_parquet
from common import create_spark

JOBS = [raw_to_parquet, delays_to_postgres, positions_to_postgres, disruptions_to_postgres, detect_anomalies]


def main() -> None:
    spark = create_spark("pid_streaming")
    for job in JOBS:
        query = job.start(spark)
        print(f"Started query {query.name} (id {query.id})")

    # Blocks until any query stops. A query stops only with an error, and then the exception is raised
    # here and ends the whole application - better a visible crash (and a restart by Docker)
    # than four queries running while the fifth is silently dead.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
