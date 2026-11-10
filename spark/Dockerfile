# Spark image for this project: the official image plus Python libraries our jobs import.
# Job code is not copied here, it is mounted from spark/jobs (see docker-compose.yml).
FROM apache/spark:4.2.1-java21-python3

USER root
# psycopg: PostgreSQL client used by delays_to_postgres.py to upsert aggregates
RUN pip3 install --no-cache-dir "psycopg[binary]==3.3.8"
USER spark
