"""Poll the PID RSS feeds with traffic disruptions and publish them to Kafka.

Usage:
    .venv\\Scripts\\python producers\\disruptions.py

Topic `pid.disruptions` is *compacted* and keyed by the disruption id: Kafka keeps (at least) the newest
message of every key forever, so the topic is not a log of events but the current state of every disruption.
A message is sent only when something changed:
    - a disruption appeared in a feed;
    - its title, lines or times were edited;
    - it disappeared from the feed -> the same message again with `removed_at` filled in.

The producer has to remember what it already sent. It keeps that in memory and, on start, rebuilds it by
reading the topic from the beginning - the topic itself is the producer's database.
"""

import html
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from dotenv import load_dotenv

TOPIC = "pid.disruptions"
FEEDS = {
    "incident": "https://pid.cz/feed/rss-mimoradnosti/",  # mimořádnosti: unplanned, happening now
    "closure": "https://pid.cz/feed/rss-vyluky/",          # výluky: planned closures and diversions
}
CONTENT_NS = "{http://purl.org/rss/1.0/modules/content/}"
DESCRIPTION_MAX = 500
# Fields that describe the disruption itself; first_seen / removed_at are added by us
CONTENT_FIELDS = ["id", "kind", "title", "link", "lines", "time_from", "time_to", "priority", "description"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def epoch_to_iso(text: str | None) -> str | None:
    """'1794425880' -> '2026-11-11T19:38:00+00:00'; empty (= until further notice) -> None."""
    text = (text or "").strip()
    return datetime.fromtimestamp(int(text), timezone.utc).isoformat(timespec="seconds") if text.isdigit() else None


def clean_text(text: str | None) -> str | None:
    """HTML from the feed -> one line of plain text."""
    if not text:
        return None
    text = re.sub(r"<[^>]+>", " ", text)       # drop tags
    text = html.unescape(text)                  # &nbsp; &amp; ...
    text = re.sub(r"\s+", " ", text).strip()
    return text[:DESCRIPTION_MAX] or None


def fetch_feed(kind: str, url: str) -> dict[str, dict]:
    """Download one RSS feed and return {disruption id: content}."""
    response = requests.get(url, headers={"User-Agent": "pid-delays-monitor"}, timeout=30)
    response.raise_for_status()
    root = ET.fromstring(response.content)

    items = {}
    for item in root.iterfind("./channel/item"):
        guid = (item.findtext("guid") or "").strip()
        if not guid:
            continue
        priority = (item.findtext("priority") or "").strip()
        items[guid] = {
            "id": guid,
            "kind": kind,
            "title": clean_text(item.findtext("title")),
            "link": (item.findtext("link") or "").strip() or None,
            # sorted: the order of <line> elements is not guaranteed and must not look like a change
            "lines": sorted({(line.text or "").strip() for line in item.iterfind("./lines/line")} - {""}),
            "time_from": epoch_to_iso(item.findtext("dateFrom")),
            "time_to": epoch_to_iso(item.findtext("dateTo")),
            "priority": int(priority) if priority.isdigit() else None,
            "description": clean_text(item.findtext(f"{CONTENT_NS}encoded") or item.findtext("description")),
        }
    return items


def restore_state(bootstrap: str) -> dict[str, dict]:
    """Read the whole topic and return the newest message of every key: {id: message}."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        # A group id is mandatory for the client, but we never commit offsets and we choose the partition
        # ourselves (assign), so no consumer group is really formed.
        "group.id": "pid-disruptions-producer-restore",
        "enable.auto.commit": False,
        "enable.partition.eof": True,  # tell us when we reach the end of the partition
    })
    state: dict[str, dict] = {}
    try:
        metadata = consumer.list_topics(TOPIC, timeout=10)
        partitions = metadata.topics[TOPIC].partitions
        if metadata.topics[TOPIC].error is not None or not partitions:
            sys.exit(f"Topic {TOPIC} does not exist. Create it first (see docs/commands.md).")
        # offset 0 = read from the very beginning, regardless of any committed offsets
        consumer.assign([TopicPartition(TOPIC, p, 0) for p in partitions])
        remaining = len(partitions)
        while remaining > 0:
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == msg.error()._PARTITION_EOF:
                    remaining -= 1
                    continue
                raise KafkaException(msg.error())
            if msg.value() is not None:
                # later messages overwrite earlier ones: exactly what compaction does inside Kafka
                state[msg.key().decode()] = json.loads(msg.value())
    finally:
        consumer.close()
    return state


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Czech titles on a Windows console
    load_dotenv()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    interval = int(os.getenv("DISRUPTIONS_POLL_INTERVAL_S", "120"))

    state = restore_state(bootstrap)
    active = sum(1 for m in state.values() if not m.get("removed_at"))
    print(f"Restored {len(state)} disruptions from {TOPIC} ({active} still listed).")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
    })

    def on_delivery(err, msg):
        if err is not None:
            print(f"Delivery failed for key {msg.key()}: {err}", file=sys.stderr)

    def send(message: dict) -> None:
        state[message["id"]] = message
        producer.produce(TOPIC, key=message["id"], value=json.dumps(message, ensure_ascii=False), on_delivery=on_delivery)

    print(f"Polling {len(FEEDS)} feeds every {interval}s, sending to {TOPIC} at {bootstrap}. Ctrl+C to stop.")
    try:
        while True:
            started = time.monotonic()
            now = now_iso()
            new = changed = removed = 0

            for kind, url in FEEDS.items():
                try:
                    items = fetch_feed(kind, url)
                except (requests.RequestException, ET.ParseError) as e:
                    # A feed we could not read tells us nothing: do not mark its disruptions as removed
                    print(f"{now} {kind} feed error: {e}", file=sys.stderr)
                    continue

                for guid, content in items.items():
                    previous = state.get(guid)
                    if previous is None:
                        send({**content, "first_seen": now, "removed_at": None})
                        new += 1
                        print(f"{now} new {kind}: {content['title']} | lines: {', '.join(content['lines']) or '-'}")
                    elif previous.get("removed_at") or any(previous.get(f) != content[f] for f in CONTENT_FIELDS):
                        send({**content, "first_seen": previous.get("first_seen") or now, "removed_at": None})
                        changed += 1

                for guid, previous in list(state.items()):
                    if previous.get("kind") == kind and not previous.get("removed_at") and guid not in items:
                        send({**previous, "removed_at": now})
                        removed += 1
                        print(f"{now} ended {kind}: {previous.get('title')}")

            producer.flush(10)
            listed = sum(1 for m in state.values() if not m.get("removed_at"))
            print(f"{now} listed: {listed}, new: {new}, changed: {changed}, ended: {removed}")
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
