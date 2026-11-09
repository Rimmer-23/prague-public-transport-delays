"""Poll Golemio API for PID vehicle positions and publish them to Kafka.

Usage:
    .venv\\Scripts\\python producers\\vehicle_positions.py

Each vehicle becomes one flat JSON message in topic `pid.vehicle_positions`,
keyed by trip_id. Positions that did not change since the previous poll
(same trip_id and origin_timestamp) are skipped.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

API_URL = "https://api.golemio.cz/v2/vehiclepositions"
TOPIC = "pid.vehicle_positions"


def fetch_positions(token: str) -> list[dict]:
    response = requests.get(
        API_URL,
        headers={"X-Access-Token": token},
        params={"limit": 10000},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("features", [])


def to_message(feature: dict, ingest_time: str) -> dict | None:
    """Flatten one GeoJSON feature into the message we store in Kafka."""
    props = feature.get("properties", {})
    pos = props.get("last_position", {})
    trip = props.get("trip", {})
    gtfs = trip.get("gtfs", {})
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]

    trip_id = gtfs.get("trip_id")
    event_time = pos.get("origin_timestamp")
    if not trip_id or not event_time:
        return None  # without these we can neither key nor window the message

    return {
        "trip_id": trip_id,
        "line": gtfs.get("route_short_name"),
        "route_id": gtfs.get("route_id"),
        "route_type": gtfs.get("route_type"),
        "headsign": gtfs.get("trip_headsign"),
        "vehicle_id": trip.get("vehicle_registration_number"),
        "agency": (trip.get("agency_name") or {}).get("real"),
        "delay_s": (pos.get("delay") or {}).get("actual"),
        "lon": coords[0],
        "lat": coords[1],
        "bearing": pos.get("bearing"),
        "speed": pos.get("speed"),
        "state_position": pos.get("state_position"),
        "is_canceled": pos.get("is_canceled"),
        "last_stop_id": (pos.get("last_stop") or {}).get("id"),
        "last_stop_sequence": (pos.get("last_stop") or {}).get("sequence"),
        "next_stop_id": (pos.get("next_stop") or {}).get("id"),
        "event_time": event_time,  # when the vehicle reported the position
        "ingest_time": ingest_time,  # when we fetched it
    }


def main() -> None:
    load_dotenv()
    token = os.getenv("GOLEMIO_API_TOKEN")
    if not token or token == "your-token-here":
        sys.exit("GOLEMIO_API_TOKEN is not set. Copy .env.example to .env and put your token there.")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    interval = int(os.getenv("POLL_INTERVAL_S", "20"))

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",                # broker confirms only after the message is safely written
        "enable.idempotence": True,   # retries never create duplicates inside Kafka
        "compression.type": "lz4",    # messages are similar JSON, compress well
        "linger.ms": 100,             # wait up to 100 ms to send messages in batches
    })

    delivery_errors = 0

    def on_delivery(err, msg):
        nonlocal delivery_errors
        if err is not None:
            delivery_errors += 1
            print(f"Delivery failed for key {msg.key()}: {err}", file=sys.stderr)

    last_seen: dict[str, str] = {}  # trip_id -> last sent event_time
    print(f"Polling every {interval}s, sending to {TOPIC} at {bootstrap}. Ctrl+C to stop.")

    try:
        while True:
            started = time.monotonic()
            ingest_time = datetime.now(timezone.utc).isoformat(timespec="seconds")

            try:
                features = fetch_positions(token)
            except requests.RequestException as e:
                print(f"{ingest_time} API error: {e}", file=sys.stderr)
                time.sleep(interval)
                continue

            sent = skipped = invalid = 0
            current: dict[str, str] = {}
            for feature in features:
                msg = to_message(feature, ingest_time)
                if msg is None:
                    invalid += 1
                    continue
                trip_id, event_time = msg["trip_id"], msg["event_time"]
                current[trip_id] = event_time
                if last_seen.get(trip_id) == event_time:
                    skipped += 1
                    continue
                producer.produce(
                    TOPIC,
                    key=trip_id,
                    value=json.dumps(msg, ensure_ascii=False),
                    on_delivery=on_delivery,
                )
                producer.poll(0)  # serve delivery callbacks
                sent += 1

            producer.flush(10)
            last_seen = current  # forget trips that are no longer in the feed

            print(
                f"{ingest_time} vehicles: {len(features)}, sent: {sent}, "
                f"unchanged: {skipped}, invalid: {invalid}, delivery errors total: {delivery_errors}"
            )
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
