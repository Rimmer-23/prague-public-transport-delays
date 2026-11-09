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
