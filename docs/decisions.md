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
