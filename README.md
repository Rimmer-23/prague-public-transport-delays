# Prague public transport delays

Real-time monitoring of delays in Prague Integrated Transport (PID) built on **Apache Kafka** and
**Apache Spark** (Structured Streaming + batch), with PostgreSQL, Grafana and a Telegram bot on top.

## The problem

Official apps show the delay of a single trip. Nobody sees the whole picture: which lines, segments and hours
are systematically late, and when delays suddenly grow. This project collects the position of every vehicle
(up to ~1800 trams, buses and trains at a time, each reporting every 10–30 seconds) and turns the stream into

- a live map of all vehicles coloured by delay,
- per-line delay statistics in 15-minute windows,
- alerts when a line is much later than it usually is (Kafka topic → Telegram),
- a daily report: worst lines, rush hours, trams vs buses, segments where trips lose time,
  and whether disruptions announced by PID show up in the delays.

## Architecture

```
Golemio API ──► producer ──► Kafka: pid.vehicle_positions ──┐
(vehicle positions,           3 partitions, key = trip_id   │
 delay computed by PID)                                     ▼
                                              Spark Structured Streaming (5 queries, 1 application)
PID RSS feeds ─► producer ──► Kafka:           ├─ raw_to_parquet ────────► Parquet by date (data lake)
(incidents,                   pid.disruptions  ├─ delays_to_postgres ────► PostgreSQL: delay per line, 15-min windows
 closures)                    compacted,       ├─ positions_to_postgres ─► PostgreSQL: latest position per trip
                              key = id ───────►├─ disruptions_to_postgres► PostgreSQL: disruptions
                                               └─ detect_anomalies ──────► PostgreSQL: alerts
                                                        │                        │
                                                        ▼                        ▼
                                              Kafka: pid.alerts              Grafana
                                                        │              (3 dashboards, incl. maps)
                                                        ▼                        ▲
                                                  Telegram bot                   │
                                              (subscribe to a line)              │
                                                                                 │
GTFS timetable ─► load_gtfs.py ─► routes, stops ─┐                               │
                                                 ▼                               │
Parquet + pid.disruptions ──► Spark batch, daily ──► PostgreSQL: daily stats, ───┘
                                                     baseline for the anomaly detector
```

Everything runs in Docker Compose on one machine: Kafka in KRaft mode (no ZooKeeper), Spark in local mode.

## What is inside

| Part | File | What it does | Concepts it demonstrates |
|---|---|---|---|
| Positions producer | [producers/vehicle_positions.py](producers/vehicle_positions.py) | Polls the Golemio API every 20 s, one flat JSON message per vehicle | message key and partitioning, idempotent producer, `acks=all`, deduplication at the source |
| Disruptions producer | [producers/disruptions.py](producers/disruptions.py) | Polls two RSS feeds, sends a message when a disruption appears, changes or ends | log compaction, a topic as the producer's own state store (rebuilt by reading the topic on start) |
| Raw archive | [spark/jobs/raw_to_parquet.py](spark/jobs/raw_to_parquet.py) | Every message → Parquet partitioned by event date | file sink, exactly-once with checkpoint + `_spark_metadata`, small-files problem |
| Delays per line | [spark/jobs/delays_to_postgres.py](spark/jobs/delays_to_postgres.py) | Average / max delay per line in 15-minute windows → PostgreSQL | event time, tumbling windows, watermark, update mode, `foreachBatch`, idempotent upsert |
| Live positions | [spark/jobs/positions_to_postgres.py](spark/jobs/positions_to_postgres.py) | Latest position of every trip → PostgreSQL (for the map) | stateless streaming, "current state" table, `startingOffsets=latest` |
| Disruptions | [spark/jobs/disruptions_to_postgres.py](spark/jobs/disruptions_to_postgres.py) | Latest version of every disruption → PostgreSQL | ordering within a partition, upsert by key |
| Anomaly detector | [spark/jobs/detect_anomalies.py](spark/jobs/detect_anomalies.py) | Lines much later than their baseline → `pid.alerts` | sliding windows, broadcast join with a baseline, Spark as a Kafka producer, at-least-once delivery |
| All streams together | [spark/jobs/run_all.py](spark/jobs/run_all.py) | The five queries above in one Spark application | several queries per application, one checkpoint per query |
| Daily batch | [spark/jobs/daily_stats.py](spark/jobs/daily_stats.py) | Daily statistics and the per-line baseline from the Parquet archive | partition pruning, caching, percentiles, window functions (`lag`), non-equi join, batch read from Kafka, idempotent output |
| Reference data | [scripts/load_gtfs.py](scripts/load_gtfs.py) | Routes and stops from the GTFS timetable → PostgreSQL and files for Spark | conditional HTTP download, bulk load with `COPY` |
| Telegram bot | [bot/telegram_bot.py](bot/telegram_bot.py) | Reads `pid.alerts`, sends alerts to chats subscribed to the line | consumer group, committed offsets, manual commit, `auto.offset.reset` |
| Dashboards | [grafana/dashboards/](grafana/dashboards/) | Delays by line, live map, daily analytics — provisioned from files | read-only database role, Grafana provisioning |

Design decisions and the reasons behind them (window size, watermark, message key, compaction, how alerts are
deduplicated, what was rejected) are logged in [docs/decisions.md](docs/decisions.md).

## Dashboards

Grafana at http://localhost:3001, no login needed.

- **PID delays by line** — average delay by vehicle type and for selected lines, most delayed lines, alerts and
  PID disruptions; alerts and incidents of the selected lines are drawn as marks on the chart.
- **PID live map** — every running vehicle as an arrow pointing in its direction, coloured by delay; filters by
  vehicle type and minimum delay.
- **PID daily analytics** — results of the batch job for a chosen day: worst lines (average, median, 90th
  percentile, share of late positions), delay by hour and vehicle type, a map of segments where trips lose
  time, and delay during disruptions compared with the rest of the day.

## First numbers

> Preliminary. Collection started on 9 November 2026 and so far covers only a few hours on three days
> (about 54 000 vehicle positions, 7 100 trips). The pipeline now runs continuously; these numbers should be
> replaced once it has a week or two of full days.

| Vehicle type | Positions | Average delay | Positions late by 3+ min |
|---|---:|---:|---:|
| Bus | 39 495 | 142 s | 28 % |
| Tram | 6 259 | 111 s | 18 % |
| Train | 4 658 | 91 s | 16 % |
| Trolleybus | 729 | 94 s | 12 % |
| Metro | 2 529 | 9 s | 0 % |

- Buses are late most often, the metro practically never — as expected for vehicles that share roads with cars
  versus vehicles with their own track.
- The average hides a lot: on the most delayed lines roughly half of the positions are 3+ minutes late
  (bus 133 Florenc – Sídliště Malešice: average 468 s, 65 % late; bus 136: 349 s, 47 %), while most lines
  stay under 2 minutes.
- Delay is created at specific places. In one evening the segments where trams gained the most time were all
  around the city centre: Anděl → Bertramka (+76 s per trip), Karlovo náměstí → Moráň (+74 s),
  Karlovo náměstí → Štěpánská (+71 s), Náměstí Míru → Jana Masaryka (+63 s) — 5–6 trips each, so a hint, not a result.
- Not answered yet: rush hours and the effect of disruptions need full days of data.

## Running it

You need Docker (Compose v2) and a free Golemio API token from https://api.golemio.cz/api-keys.

```bash
cp .env.example .env    # then fill in GOLEMIO_API_TOKEN and the passwords
```

```bash
docker compose --profile pipeline up -d --build
```

That starts Kafka, PostgreSQL, Grafana, both producers, the GTFS loader, the Spark streaming application, the
daily batch job and the bot. Open http://localhost:3001 — the map fills within a minute, per-line charts within
a few minutes; the daily analytics need at least one batch run (one happens right at start, then every day at
05:00 Prague time).

- Telegram alerts are optional: create a bot with @BotFather, put its token into `TELEGRAM_BOT_TOKEN` in `.env`
  and restart the `telegram-bot` service; then send the bot `/subscribe 22` or `/subscribe all`.
  Without a token the bot prints alerts to its log.
- Follow the logs: `docker compose --profile pipeline logs -f --tail 20`
- Stop everything (data is kept): `docker compose --profile pipeline down`
- Spark UI of the streaming application: http://localhost:4041

Every part can also be started by hand, one command each, to study it in isolation — the commands are listed
in [CLAUDE.md](CLAUDE.md) (section "Команды", in Russian).

## Stack

Python 3.14 · Apache Kafka 4.3 (KRaft) · Apache Spark 4.2 (PySpark, Structured Streaming) · PostgreSQL 18 ·
Grafana 13 · Docker Compose

## Limitations

- One Kafka broker and Spark in local mode: no fault tolerance and no real parallelism across machines.
  Replication factor is 1 everywhere.
- Messages are JSON without a schema registry; a schema change would have to be coordinated by hand.
- Delay is taken as computed by PID; the project aggregates it and does not verify it against the timetable.
- The anomaly baseline is one number per line for the whole day (rush hour and night are not told apart yet),
  and there is no "back to normal" message.
- Disruption impact is a correlation on the same line and time, not a proof of cause.

## Data sources

- Vehicle positions: [Golemio](https://golemio.cz), the Prague open data platform (API keys: https://api.golemio.cz/api-keys)
- Timetable (GTFS): PID open data, https://data.pid.cz/PID_GTFS.zip
- Disruptions: PID RSS feeds (`pid.cz/feed/rss-mimoradnosti`, `pid.cz/feed/rss-vyluky`)
