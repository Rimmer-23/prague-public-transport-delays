# Commands

The whole pipeline runs with one command (see README, "Running it"). Below is every part on its own, to study it
in isolation. **While the `pipeline` profile is running, do not start the same jobs or producers by hand:** two
queries on one checkpoint corrupt it, two producers double the data, two bots with one `group.id` split the alerts.

Examples use PowerShell. In Git Bash run `export MSYS_NO_PATHCONV=1` first, otherwise paths like `/opt/...` get mangled.

## Environment

- Python venv for producers and scripts: `py -3.14 -m venv .venv` and `.venv/Scripts/python -m pip install -r requirements.txt`
- Snapshot of vehicle positions (to see the data structure): `.venv/Scripts/python scripts/explore.py` → `data/explore/`

## Whole pipeline

- Start (producers, bot, GTFS loader, all streaming queries, daily batch; services restart automatically):
  `docker compose --profile pipeline up -d --build`
- Logs: `docker compose --profile pipeline logs -f --tail 20` (or `docker logs -f pid-spark-streaming`)
- Stop everything, keep data: `docker compose --profile pipeline down`
- Stop only the pipeline services, keep the infrastructure:
  `docker compose --profile pipeline stop producer-positions producer-disruptions telegram-bot gtfs-loader spark-streaming spark-daily`
- Free memory but keep collecting (Kafka buffers positions, Spark catches up from its checkpoints later):
  `docker compose --profile pipeline stop spark-streaming spark-daily grafana`, then
  `docker compose --profile pipeline up -d`. Services stopped by hand do not come back after a reboot by themselves.
- Infrastructure only (Kafka, PostgreSQL, Grafana, Spark container): `docker compose up -d` / `docker compose down`
  (`down -v` also deletes the volumes with Kafka and PostgreSQL data)

## Kafka

- Topics are created by the one-shot `kafka-init` container. By hand:
  `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --topic pid.vehicle_positions --partitions 3 --replication-factor 1`
- List / describe topics: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list` (or `--describe --topic <name>`)
- Read a topic:
  `docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true`
  (same with `--topic pid.alerts` or `--topic pid.disruptions`)
- Write to a topic by hand (`key:value`):
  `docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --reader-property parse.key=true --reader-property key.separator=: --reader-property ignore.error=true`
- Last offset per partition: `docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions`
- Where the bot stopped (consumer group, committed offset, lag):
  `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group pid-telegram-bot`

## Producers, loaders, bot

- Vehicle positions (API → Kafka, Ctrl+C to stop): `.venv/Scripts/python producers/vehicle_positions.py`
- Disruptions (RSS → Kafka): `.venv/Scripts/python producers/disruptions.py`
- GTFS (download if updated, load into PostgreSQL and `data/gtfs/`): `.venv/Scripts/python scripts/load_gtfs.py` (`--force` to download again)
- Telegram bot: `.venv/Scripts/python bot/telegram_bot.py`. Token in `TELEGRAM_BOT_TOKEN` (`.env`); without it the bot
  prints alerts to the console. Chat commands: `/subscribe 22`, `/subscribe all`, `/unsubscribe 22`, `/list`, `/top`.

## Spark jobs

Every job runs with the same command, only the file differs:

```
docker exec -it spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 --conf spark.jars.ivy=/opt/spark/ivy /opt/spark/jobs/<job>.py
```

| Job | What it does |
|---|---|
| `read_positions.py` | Reads the topic and prints it to the console |
| `delays_by_line.py` | Average delay per line in 15-minute windows, console |
| `raw_to_parquet.py` | Raw positions → Parquet by date (`data/raw/vehicle_positions/date=YYYY-MM-DD/`), batch every 5 min |
| `delays_to_postgres.py` | Per-line 15-minute aggregates → table `line_delays_15m` |
| `positions_to_postgres.py` | Latest position of each trip → table `vehicle_positions_latest` (map) |
| `disruptions_to_postgres.py` | Disruptions from Kafka → table `disruptions` |
| `detect_anomalies.py` | Anomalies → table `line_alerts` and topic `pid.alerts` |
| `run_all.py` | All five streaming queries in one Spark application |
| `daily_stats.py` | Daily batch job (yesterday by default); `--date 2026-11-10`, `--all` (every day in the archive), `--explain` (physical plan). Re-running a day replaces its rows |

- Check the Parquet archive (batch, no Kafka connector needed): `docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/jobs/inspect_raw.py`
- Spark UI: http://localhost:4040 for a job started by hand, http://localhost:4041 for the pipeline service.
- A job's checkpoint is `checkpoints/<job>`. Deleting it makes the job start again from the beginning of the topic.

### Resets

- Reset the Parquet archive: delete **both** `data/raw/vehicle_positions` and `checkpoints/raw_to_parquet`
  (only one of the two = duplicates or gaps).
- Recompute `line_delays_15m` from the beginning of the topic: delete `checkpoints/delays_to_postgres` and run the job
  (upsert overwrites rows, no duplicates).
- Recompute alerts: `TRUNCATE line_alerts`, delete `checkpoints/detect_anomalies`, run the job
  (the bot skips old alerts by age).

## PostgreSQL

- SQL console: `docker exec -it pid-postgres psql -U pid -d pid`
  (e.g. `select * from line_delays_15m where trips >= 3 order by window_start desc, avg_delay_s desc limit 20;`)
- Apply the schema to an existing database (after adding a file to `postgres/init/`):
  `docker exec pid-postgres sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"; done'`
- Create / update the read-only `grafana_reader` role (or change its password):
  `docker exec pid-postgres psql -U pid -d pid -f /docker-entrypoint-initdb.d/02_grafana_reader.sql`
- From Windows connect to `127.0.0.1`, not `localhost`: the port is published on IPv4 only, and `localhost` is tried
  as IPv6 first.

## Grafana

- http://localhost:3001: "PID delays by line" (home), "PID live map", "PID daily analytics"; links at the top switch between them.
  Viewing needs no login; editing — `admin` / `GRAFANA_ADMIN_PASSWORD` from `.env`.
- To change a dashboard, edit `grafana/dashboards/<name>.json` (Grafana re-reads the folder every 30 s), or edit it in
  the UI, then Export → JSON and save it to that file (provisioned dashboards cannot be saved from the UI).
