# Decisions

Short log of design decisions and why they were made.

## 2026-11-09 — Kafka: single node in KRaft mode

- **Decision:** one container `apache/kafka:4.3.2` acting as both broker and controller, no ZooKeeper.
- **Why:** ZooKeeper was removed in Kafka 4.x; KRaft stores cluster metadata in Kafka itself.
  One node is enough for a single-machine learning project. Consequence: replication factor is 1 everywhere,
  so there is no fault tolerance — acceptable here, would not be in production (3+ brokers, RF=3).

## 2026-11-09 — Kafka: two listeners (INTERNAL / EXTERNAL)

- **Decision:** `INTERNAL kafka:19092` for containers, `EXTERNAL localhost:9092` for programs on Windows.
- **Why:** a client first connects to any address, then the broker tells it the *advertised* address to use.
  `localhost` inside a container means the container itself, and the hostname `kafka` does not resolve on Windows,
  so one address cannot serve both the Python producer (host) and Spark (container).

## 2026-11-09 — Topics are created explicitly, auto-create is off

- **Decision:** `auto.create.topics.enable=false`; topics are created with `kafka-topics.sh`.
- **Why:** with auto-create a typo in a topic name silently creates a new empty topic with default settings.

## 2026-11-09 — `pid.vehicle_positions`: 3 partitions

- **Decision:** 3 partitions, replication factor 1.
- **Why:** the partition count is the upper limit of parallelism for consumers in one group and for Spark tasks
  reading the topic. 3 lets us see parallel consumption on a laptop; data volume (~1800 vehicles every 10–30 s) is small.
  Partitions can be added later but not removed, and adding them changes which partition a key goes to.

## 2026-11-09 — Message key: `trip_id`

- **Decision:** messages in `pid.vehicle_positions` are keyed by GTFS `trip_id`.
- **Why:** Kafka keeps order only within a partition, and the same key always goes to the same partition,
  so all positions of one trip arrive in order. ~1800 active trips spread evenly over partitions
  (first run: 1996 / 1757 / 1963 messages).
- **Rejected:** line number — Spark re-shuffles data for `groupBy(line)` anyway, and big lines would skew partitions;
  vehicle registration number — not unique across operators (1565 numbers for 1776 vehicles) and null for trains.

## 2026-11-09 — Deduplication in the producer

- **Decision:** the producer remembers the last `origin_timestamp` per `trip_id` and does not resend unchanged positions.
- **Why:** a vehicle reports less often than we poll (every 20 s), so 25–40 % of positions in a poll are repeats.
  Without dedup they would inflate Kafka/Parquet and bias averages towards slowly reporting vehicles.
- **Limitation:** the memory is in-process; after a producer restart the first poll resends everything once.
  Spark can add `dropDuplicates` with a watermark as a second line of defence.

## 2026-11-09 — Message format: flat JSON

- **Decision:** one flat JSON object per vehicle position (trip_id, line, route_type, delay_s, lat/lon, stops,
  `event_time` = `origin_timestamp` from the vehicle, `ingest_time` = when the producer fetched it).
- **Why:** the raw GeoJSON is deeply nested and has fields we never use; a flat object is easy to describe as a
  Spark schema. JSON (not Avro/Protobuf) keeps messages human-readable in the console consumer;
  a Schema Registry would be the next step for a production system.
- **Event time vs processing time:** both timestamps are kept, windows will be built on `event_time`.

## 2026-11-09 — Kafka client: confluent-kafka

- **Decision:** `confluent-kafka` (Python wrapper over librdkafka), producer with `acks=all`, idempotence, lz4.
- **Why:** de-facto standard client, fast, has Python 3.14 wheels for Windows.

## 2026-11-09 — Spark: local mode in one container, jobs via `docker exec`

- **Decision:** one container `apache/spark:4.2.1-java21-python3` that just stays alive; jobs are started with
  `docker exec spark spark-submit ...` in local mode (driver and executors in one JVM, `local[*]`).
- **Why:** the data volume (~1500 messages / 20 s) fits a single machine easily; a standalone cluster
  (master + workers) adds containers and networking without teaching anything about the streaming API itself.
  Job code is mounted from the repo, so editing a job does not require rebuilding an image.
- **Version:** 4.2.1 (patch release) rather than 4.3.0, which was published a few days before.
- **Kafka connector:** `spark-sql-kafka-0-10` is not in the image, it is downloaded by `--packages` from Maven Central
  and cached in `.cache/ivy` (bind mount: the image user has no home dir, a named volume would be owned by root).

## 2026-11-10 — Delays by line: 15-min tumbling windows on event time, watermark 10 min

- **Window:** tumbling (non-overlapping) 15 minutes on `event_time` (when the vehicle reported), grouped by line.
  15 min is long enough to contain several positions of most lines and short enough to see a rush hour build up.
  Event time, not processing time: a position that arrives late still lands in the window it belongs to,
  and re-reading old data gives the same result.
- **Watermark 10 min:** in one API response positions were up to ~5 min old, so 10 min leaves a margin.
  Trade-off: bigger watermark = fewer dropped late rows, but windows stay in memory and become final later.
- **Output mode `update`:** each micro-batch emits only windows whose numbers changed, so the console shows
  the current window growing. `append` would print a window only once it is final (window end + watermark,
  i.e. ~25 min after it starts); `complete` would re-print all windows ever seen on every batch.
- **foreachBatch for output:** sorting is not allowed on a streaming aggregation in update mode, but inside
  `foreachBatch` each batch is a normal DataFrame. The same hook will write to PostgreSQL in stage 4.
- **Checkpoint:** `checkpoints/delays_by_line` keeps processed Kafka offsets and the state of open windows.
  Verified: after a restart the job continued with batch 5 and only new data instead of re-reading the topic.
- **Known limitation:** the average is over positions, not trips — a trip that reports more often weighs more.
  Lines with 1–2 trips in a window produce noisy extremes; a minimum sample (e.g. ≥ 3 trips) is needed before
  ranking or alerting.

## 2026-11-10 — Raw archive: Parquet partitioned by event date, one batch per 5 minutes

- **Decision:** a separate streaming job `raw_to_parquet` writes every message of `pid.vehicle_positions` to
  `data/raw/vehicle_positions/date=YYYY-MM-DD/` (Parquet, snappy), all parsed fields plus Kafka `partition`/`offset`.
- **Why Parquet:** columnar and compressed (35k positions = 1.5 MB), stores the schema, and a batch query that
  needs `line` and `delay_s` reads only those columns. Kafka is a buffer with limited retention, not an archive.
- **Why a separate job, not a second sink in `delays_by_line`:** each streaming query has its own checkpoint and
  reads Kafka independently, so the archive keeps working when the aggregation job is stopped, changed or reset.
- **Partition by `date` = day of `event_time` in Prague time:** daily batch jobs read one folder (partition pruning).
  Event date, not ingest date: a position reported at 23:59 and fetched at 00:01 belongs to the day it happened.
  Not partitioned by line: hundreds of lines x days would give thousands of tiny files.
- **Trigger 5 min + `coalesce(1)`:** every micro-batch writes at least one file per date. With the 20 s trigger of
  the console job and 3 Kafka partitions that would be ~13 000 small files a day; now it is ~288.
  Price: the archive lags up to 5 minutes behind Kafka, which is fine for batch analytics.
- **Exactly-once:** the checkpoint stores the Kafka offsets of each batch, and the file sink lists the files of each
  committed batch in `_spark_metadata`. After a crash the batch is re-run with the same offsets and Spark readers
  ignore files that are not in the log. Verified on the first run: 34 972 rows = sum of the topic's end offsets,
  34 972 distinct (partition, offset). Restart from the checkpoint is not verified yet for this job.
- **Limitations:** tools that do not know `_spark_metadata` (pandas, DuckDB) may see leftovers of a failed batch;
  the data folder and the checkpoint must be deleted together; no deduplication here — the archive is a copy of
  the topic, duplicates after a producer restart are removed by the jobs that read it.

## 2026-11-10 — Aggregates in PostgreSQL: upsert from `foreachBatch`

- **Decision:** job `delays_to_postgres` runs the same aggregation as `delays_by_line` (shared `aggregate_delays()`)
  and upserts every micro-batch into `line_delays_15m` with `INSERT ... ON CONFLICT (window_start, line, route_type)
  DO UPDATE`. The primary key is exactly the key Spark groups by.
- **Why PostgreSQL next to Parquet:** Parquet is the cheap full history for batch jobs; a dashboard needs fast
  queries on small ready-made aggregates and a table that can be updated in place.
- **Why upsert:** in `update` mode Spark emits an open window again on every batch that changes it, each time
  with the full current numbers (not a difference). A plain `INSERT` would add a new row every 20 s.
  The alternative, `append` mode, writes each window once but only after it is final (~25 min after it starts).
- **Idempotency instead of exactly-once:** `foreachBatch` is at-least-once — after a crash Spark may run the last
  batch again. With an upsert a repeated batch just overwrites the same rows, so the table ends up correct.
  One transaction per batch: a batch is in the table completely or not at all.
  Verified: checkpoint deleted, whole topic replayed — still 2061 rows / 34 972 positions, only `updated_at` changed;
  per-window numbers equal the same aggregation computed in batch from the Parquet archive.
- **Why psycopg and `collect()`, not Spark's JDBC writer:** the JDBC sink can only append or overwrite a whole
  table, it has no upsert. A batch of aggregates is a few hundred rows, so collecting it to the driver and running
  `executemany` is simple and cheap. This would not scale to raw data; there the usual pattern is a JDBC write
  to a staging table followed by one `INSERT ... SELECT ... ON CONFLICT`.
- **Custom Spark image:** `spark/Dockerfile` adds `psycopg[binary]` to the official image. In local mode only the
  driver needs it; on a real cluster the library would have to be on every executor that imports it.
- **Timestamps:** window bounds travel as epoch seconds and become `timestamptz` via `to_timestamp()`.
  `collect()` returns naive datetimes in the time zone of the Python process, which is easy to get wrong.
- **Credentials:** `POSTGRES_*` in `.env`; Compose substitutes them into both the `postgres` and `spark` services.
- **Limitations:** a new connection per batch (fine at one batch per 20 s); late rows beyond the watermark are
  dropped, so a finished window in PostgreSQL can differ slightly from a batch recount over Parquet;
  `route_type` NULL is stored as -1 because a primary key column cannot be NULL (none seen so far).

## 2026-11-10 — Grafana: everything provisioned from files, read-only database role

- **Decision:** `grafana/grafana:13.2.5` on `localhost:3001`; the data source and the dashboard are files in
  `grafana/` that Grafana loads at startup (provisioning), not objects clicked together in the UI.
- **Why provisioning:** `docker compose up -d` on a clean machine gives a working dashboard, and dashboard changes
  are visible in git diffs. Price: a provisioned dashboard cannot be saved from the UI, it has to be exported to
  JSON and written back to the file.
- **Read-only role `grafana_reader`:** a dashboard query is arbitrary SQL sent to the database, and anyone who can
  open a dashboard can send their own query through Grafana. So Grafana gets a role that can only `SELECT`.
  The role is created by `postgres/init/02_grafana_reader.sql`; its password comes from the environment via psql
  `\getenv`, so no password is stored in git.
- **Anonymous Viewer:** dashboards open without a login because the port is published on 127.0.0.1 only.
  Would be turned off on any shared or public host.
- **Dashboard reads only `line_delays_15m`:** Grafana does no heavy work, all aggregation was done by Spark.
  The queries only re-aggregate small data (e.g. average by vehicle type weighted by `positions`).
- **Weighted averages:** an average of per-line averages would give a line with 3 positions the same weight as a
  line with 300, so the dashboard uses `sum(avg_delay_s * positions) / sum(positions)`.
- **Minimum sample:** rankings ("most delayed lines", "worst line") require `trips >= 3` in a window, otherwise a
  single late bus on a rare line tops the list.
- **Gaps:** lines on charts break when there is no data for more than 20 minutes (collection was off) instead of
  drawing a straight line across the gap.
- **Limitations:** vehicle type names are a `CASE` in the queries (GTFS has names of lines and stops, not of
  vehicle types). The map came later, see the next entry.

## 2026-11-11 — Map: a "current state" table next to the history table

- **Decision:** job `positions_to_postgres` keeps table `vehicle_positions_latest` — one row per running trip,
  overwritten on every new position (upsert by `trip_id`, only if the new `event_time` is newer). Rows of trips
  that have been silent for 2 hours are deleted in the same transaction. Grafana's Geomap panel reads the table.
- **Why a separate table:** `line_delays_15m` answers "how was it" (history, aggregated), the map asks "where is
  everything now". Raw positions do not belong in PostgreSQL (millions of rows a day), the latest one per trip does
  (~2000 rows in total).
- **Stateless streaming:** no window, no watermark, no state store — every micro-batch is reduced to the newest
  row per trip (`max_by(struct(...), event_time)`) inside `foreachBatch` and upserted.
- **`startingOffsets=latest`:** the only job that skips the history on its first start; a map of current
  positions has no use for last week's data. The aggregation jobs keep `earliest`.
- **"Now" = newest position in the table**, not the clock: when collection is off the map still shows the last
  known picture, and the "Newest position" tile shows how old it is.

## 2026-11-11 — `spark.sql.shuffle.partitions` = 8, `maxOffsetsPerTrigger` = 200 000

- **Problem:** with five streaming queries in one application micro-batches took 22–38 s for a 20 s trigger
  ("Current batch is falling behind"). Each aggregation ran 200 tasks per batch over a few hundred rows.
- **Decision:** `spark.sql.shuffle.partitions=8` in `create_spark()`. 200 is a default for clusters; here it only
  multiplied scheduling overhead and state store files (200 per stateful query).
- **Catch:** a streaming query stores the number of state partitions in its checkpoint and ignores later changes,
  so the checkpoints of the stateful queries (`delays_to_postgres`, `delays_by_line`, `detect_anomalies`) were
  deleted and the topic replayed. Harmless thanks to the upsert.
- **`maxOffsetsPerTrigger`:** one micro-batch takes at most 200 000 Kafka messages. Normally a batch is ~1500
  messages; the limit matters after a checkpoint reset or a long stop, when the backlog would otherwise arrive
  as one batch (and `collect()` in `foreachBatch` would pull a huge result to the driver).

## 2026-11-11 — GTFS reference data: only routes and stops, loaded with COPY

- **Decision:** `scripts/load_gtfs.py` downloads `PID_GTFS.zip` (47 MB), extracts `routes.txt` and `stops.txt` to
  `data/gtfs/` (for Spark) and loads them into `gtfs_routes` / `gtfs_stops` (for Grafana and the bot) with
  `TRUNCATE` + `COPY` in one transaction.
- **Why not the whole timetable:** delay is already computed by PID, so `trips`/`stop_times` (hundreds of MB) are
  not needed; names and coordinates are. Checked on live data: every `route_id` and `last_stop_id` from the
  positions has a match.
- **Conditional download:** `If-Modified-Since` with the saved `Last-Modified`; the server answers 304 and the
  script skips the download, so it can run daily.
- **In Spark:** the reference tables are joined as broadcast joins — the small table is shipped to every task and
  the big side is not shuffled.

## 2026-11-11 — Disruptions: compacted topic keyed by disruption id

- **Source:** two PID RSS feeds — `rss-mimoradnosti` (incidents, unplanned) and `rss-vyluky` (planned closures).
  Besides standard RSS they carry `<lines>`, `<dateFrom>`, `<dateTo>` (epoch seconds; empty = until further notice).
- **Decision:** topic `pid.disruptions`, 1 partition, `cleanup.policy=compact`, key = RSS `guid`. The producer
  sends a message only when a disruption appears, changes, or disappears from the feed (`removed_at` is set).
- **Why compaction:** the feed shows only what is active now, and an incident may be edited several times. We need
  the *latest version of every disruption forever*, not a 7-day log of edits. Compaction keeps at least the newest
  message per key, so the topic behaves like a table and a new consumer can rebuild the full state from it.
- **The topic is the producer's state store:** on start the producer reads the topic from the beginning
  (`assign` at offset 0, no consumer group) and rebuilds `{id: last message}`. No local file, no database, and a
  restart neither resends 190 unchanged items nor misses that something ended while it was off.
- **1 partition:** ordering across all keys, trivial restore; the volume is a few messages per hour.
- **`segment.ms=1h`, `min.cleanable.dirty.ratio=0.1`:** the cleaner never touches the active segment, which by
  default rolls once a week — on this tiny topic compaction would never be observed.
- **To PostgreSQL:** `disruptions_to_postgres` upserts the newest version per id (`max_by(..., offset)` within a
  batch; later batches carry later offsets). Lines are a `text[]` column with a GIN index.
- **Limitations:** the end of an open-ended incident is known only to the poll interval (2 min); if a feed cannot
  be fetched its items are *not* marked as removed; the XML is parsed with the standard library (trusted source).

## 2026-11-11 — Anomalies: sliding windows, baseline per line, alerts deduplicated in PostgreSQL

- **Window:** sliding, 10 minutes long, sliding by 5 (`F.window("event_time", "10 minutes", "5 minutes")`), update
  mode. With tumbling windows a line is unobservable right after a window starts (fewer than 3 trips yet); with
  sliding windows there is always a window holding at least the last 5 minutes. Price: every position is counted
  in two windows, state is twice as big.
- **Rule:** a (window, line) is an alert when it has ≥ 3 trips and its average delay ≥ threshold, where
  threshold = `max(180 s, baseline_avg + 3·baseline_std)` for a line with a baseline, 300 s otherwise.
- **Baseline:** mean and standard deviation of the line's window averages over the last 14 days, computed by the
  nightly batch job with the same window definition and stored in `line_delay_baseline`. The streaming job reads
  the table on every micro-batch (one row per line) and broadcast-joins it inside `foreachBatch`, so new numbers
  are picked up without a restart. A stream–static join outside `foreachBatch` would read the table once.
- **Why state in PostgreSQL:** in update mode the same window is emitted on every batch that changes it. Table
  `line_alerts` with primary key (line, route_type, window_start) plus a `NOT EXISTS` cooldown check (30 min per
  line) turns "condition is true" into "alert once". `INSERT ... RETURNING` tells which alerts are new.
  The Spark-native alternative is arbitrary stateful processing (`transformWithState`), which needs pandas/Arrow
  in the image and is harder to inspect than a table.
- **Kafka sink:** new alerts are written to `pid.alerts` (key = line) with the DataFrame Kafka writer, *before*
  the PostgreSQL transaction commits. If Kafka fails, the inserts roll back and Spark retries the batch; if the
  commit fails after the Kafka write, the alert is sent again on retry. At-least-once: a duplicate is possible,
  a lost alert is not.
- **Bug found on the way:** `greatest(180, NULL)` is 180 in Spark (NULLs are skipped), so lines without a baseline
  silently got the 180 s threshold instead of 300 s. Replaced with an explicit `when(...).otherwise(...)`.
- **Limitations:** one baseline per line for the whole day (rush hour and night are not distinguished — needs more
  history); outliers inflate the standard deviation; no "back to normal" message.

## 2026-11-11 — Telegram bot: Kafka consumer group with manual commits

- **Decision:** `bot/telegram_bot.py` — one process, one loop: `consumer.poll(1 s)` on `pid.alerts`, then
  Telegram `getUpdates` (long polling, 2 s). Plain `requests` against the Bot API, no bot framework.
- **Consumer group `pid-telegram-bot`:** Kafka stores the group's committed offset, so a restarted bot continues
  where it stopped. `enable.auto.commit=false`, commit after the alert is handled → at-least-once.
- **`auto.offset.reset=earliest`** applies only when the group has no committed offset (first start). Old alerts
  are then dropped by age (window ended more than 30 min ago), so replaying the topic never spams anybody.
- **Subscriptions** (`/subscribe 22`, `/subscribe all`, `/unsubscribe`, `/list`) live in PostgreSQL, table
  `bot.telegram_subscriptions`. A separate schema because chat ids are personal data: the Grafana role
  `grafana_reader` has rights on schema `public` only.
- **Without a token** the bot runs in console mode and prints alerts — the Kafka part can be studied without
  creating a Telegram bot.
- **Limitations:** Telegram updates are confirmed before the reply is sent (a crash in between loses one command);
  one process handles both loops, so a reply can take up to ~3 s.

## 2026-11-11 — Daily batch job: idempotent, one transaction, results in PostgreSQL

- **Decision:** `spark/jobs/daily_stats.py` reads the Parquet archive for the requested day(s) and writes four
  tables (`daily_line_stats`, `hourly_delay_stats`, `daily_segment_delays`, `daily_disruption_impact`) plus the
  baseline for the anomaly detector.
- **Idempotent:** for every table `DELETE WHERE date = ANY(days)` + `COPY`, all tables in one transaction. The job
  can be re-run for any day; a crash leaves the old numbers.
- **Partition pruning:** the filter on `date` (the folder name) makes Spark read only the requested days.
- **`cache()`:** four results are computed from the same cleaned rows; without it the Parquet files would be read
  and deduplicated four times.
- **Deduplication** by (`trip_id`, `event_time`): the producer resends everything once after a restart.
- **Percentiles next to the average:** median and 90th percentile (`percentile_approx`) show whether a whole line
  is late or a few vehicles pull the average up. "Late" = 180 s or more.
- **Segments:** for every trip the delay at each stop (first position after reaching it), then `lag()` over a
  window partitioned by trip and ordered by stop sequence gives the delay gained between two consecutive stops.
  Only neighbouring stops are counted, and only segments with ≥ 5 trips a day are stored.
- **Disruptions are read from Kafka in batch mode** (`spark.read.format("kafka")`): the compacted topic is the
  full current state. Join: same line and overlapping time interval (non-equi join with a broadcast side).
  "Impact" = average delay of the line during the disruption vs the same line the same day outside any disruption.
- **Limitations:** correlation, not causation (a closure lasting all day has nothing to compare with; rush hour
  and an incident can coincide); trips crossing midnight are split between two days; results are collected to the
  driver (a few thousand rows per day).

