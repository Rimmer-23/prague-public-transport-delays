"""Telegram bot: reads alerts from Kafka topic pid.alerts and sends them to chats subscribed to the line.

Usage:
    .venv\\Scripts\\python bot\\telegram_bot.py

Without TELEGRAM_BOT_TOKEN in .env the bot runs in console mode: alerts are printed instead of sent,
which is enough to see the Kafka consumer work. To get a token, talk to @BotFather in Telegram (/newbot).

Commands in the chat with the bot:
    /subscribe 22      alerts for line 22        /subscribe all    alerts for every line
    /unsubscribe 22    stop them                 /unsubscribe all  remove all subscriptions
    /list              my subscriptions          /top              most delayed lines right now
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

TOPIC = "pid.alerts"
GROUP_ID = "pid-telegram-bot"
ALL_LINES = "*"
MAX_ALERT_AGE = timedelta(minutes=30)  # older alerts (e.g. after the bot was off) are not worth sending
PRAGUE = ZoneInfo("Europe/Prague")

HELP = (
    "I watch delays of Prague public transport (PID) and tell you when a line is much later than usual.\n\n"
    "/subscribe 22 - alerts for line 22\n"
    "/subscribe all - alerts for every line\n"
    "/unsubscribe 22 - stop alerts for line 22\n"
    "/unsubscribe all - remove all subscriptions\n"
    "/list - my subscriptions\n"
    "/top - most delayed lines right now"
)


def minutes(seconds: int | float) -> str:
    return f"{seconds / 60:.0f} min" if abs(seconds) >= 90 else f"{seconds:.0f} s"


def format_alert(alert: dict) -> str:
    start = datetime.fromisoformat(alert["window_start"]).astimezone(PRAGUE)
    end = datetime.fromisoformat(alert["window_end"]).astimezone(PRAGUE)
    usual = (
        f"Usual delay of this line: {minutes(alert['baseline_avg_s'])}."
        if alert.get("baseline_avg_s") is not None
        else "No history for this line yet, fixed threshold used."
    )
    return (
        f"⚠️ Line {alert['line']} ({alert.get('vehicle_type', 'vehicle')}) is running late\n"
        f"Average delay {minutes(alert['avg_delay_s'])}, worst vehicle {minutes(alert['max_delay_s'])} "
        f"({alert['trips']} trips, {start:%H:%M}-{end:%H:%M}).\n"
        f"{usual}"
    )


class Database:
    """Subscriptions live in PostgreSQL (schema bot), so they survive a restart of the bot."""

    def __init__(self) -> None:
        self.settings = dict(
            # 127.0.0.1, not "localhost": the port is published on IPv4 only, and on Windows "localhost"
            # is tried as IPv6 (::1) first, where the connection attempt can hang.
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "pid"),
            user=os.getenv("POSTGRES_USER", "pid"),
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=10,
        )

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        # A connection per query: a handful of queries a minute, not worth keeping one open (and reconnecting)
        with psycopg.connect(**self.settings) as conn:
            cur = conn.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def subscribers(self, line: str) -> list[int]:
        rows = self.query(
            "SELECT DISTINCT chat_id FROM bot.telegram_subscriptions WHERE line IN (%s, %s)", (line, ALL_LINES)
        )
        return [chat_id for (chat_id,) in rows]


class Telegram:
    def __init__(self, token: str) -> None:
        self.url = f"https://api.telegram.org/bot{token}"
        self.next_update_id = None  # Telegram's own "offset": id of the first update we have not handled yet

    def send(self, chat_id: int, text: str) -> bool:
        for attempt in range(3):
            try:
                response = requests.post(f"{self.url}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=15)
            except requests.RequestException as e:
                print(f"Telegram is unreachable ({type(e).__name__}), attempt {attempt + 1}", file=sys.stderr)
                time.sleep(2)
                continue
            if response.status_code == 429:  # too many requests: Telegram says how long to wait
                time.sleep(response.json().get("parameters", {}).get("retry_after", 1))
                continue
            if not response.ok:
                # e.g. 403: the user blocked the bot. Never print the URL here, it contains the token.
                print(f"Telegram refused a message to chat {chat_id}: {response.status_code} {response.text}", file=sys.stderr)
            return response.ok
        return False

    def updates(self) -> list[dict]:
        """New messages written to the bot. Long polling: the request waits up to 2 s for something to arrive."""
        try:
            response = requests.get(
                f"{self.url}/getUpdates",
                params={"timeout": 2, "offset": self.next_update_id, "allowed_updates": '["message"]'},
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Telegram getUpdates failed: {type(e).__name__}", file=sys.stderr)
            time.sleep(2)
            return []
        result = response.json().get("result", [])
        if result:
            # Asking for offset N confirms everything before N, so Telegram will not send it again
            self.next_update_id = result[-1]["update_id"] + 1
        return result


def handle_command(db: Database, chat_id: int, text: str) -> str:
    parts = text.split()
    command = parts[0].split("@")[0].lower()  # "/subscribe@my_bot" in group chats
    arg = parts[1].upper() if len(parts) > 1 else None  # lines are "22", "S9", "A"
    line = ALL_LINES if arg == "ALL" else arg

    if command in ("/start", "/help"):
        return HELP

    if command == "/subscribe":
        if not line:
            return "Which line? For example: /subscribe 22"
        if line != ALL_LINES and not db.query("SELECT 1 FROM gtfs_routes WHERE line = %s LIMIT 1", (line,)):
            known = db.query("SELECT 1 FROM gtfs_routes LIMIT 1")
            if known:  # the reference table is loaded and has no such line
                return f"I do not know line {line}. Check the name, e.g. 22, 136, S9, A."
        db.query(
            "INSERT INTO bot.telegram_subscriptions (chat_id, line) VALUES (%s, %s) ON CONFLICT DO NOTHING", (chat_id, line)
        )
        return "Subscribed to all lines." if line == ALL_LINES else f"Subscribed to line {line}."

    if command == "/unsubscribe":
        if not line:
            return "Which line? For example: /unsubscribe 22 (or /unsubscribe all)"
        if line == ALL_LINES:
            db.query("DELETE FROM bot.telegram_subscriptions WHERE chat_id = %s", (chat_id,))
            return "All subscriptions removed."
        db.query("DELETE FROM bot.telegram_subscriptions WHERE chat_id = %s AND line = %s", (chat_id, line))
        return f"Unsubscribed from line {line}."

    if command == "/list":
        lines = [l for (l,) in db.query("SELECT line FROM bot.telegram_subscriptions WHERE chat_id = %s ORDER BY line", (chat_id,))]
        if not lines:
            return "No subscriptions yet. Try /subscribe 22"
        return "Your subscriptions: " + ", ".join("all lines" if l == ALL_LINES else l for l in lines)

    if command == "/top":
        rows = db.query(
            """
            SELECT line, avg_delay_s, trips, window_start
            FROM line_delays_15m
            WHERE window_start = (SELECT max(window_start) FROM line_delays_15m) AND trips >= 3
            ORDER BY avg_delay_s DESC
            LIMIT 10
            """
        )
        if not rows:
            return "No data yet."
        window = rows[0][3].astimezone(PRAGUE)
        body = "\n".join(f"{line}: {minutes(avg)} ({trips} trips)" for line, avg, trips, _ in rows)
        return f"Most delayed lines, 15 minutes from {window:%d.%m. %H:%M}:\n{body}"

    return "I do not understand. /help shows what I can do."


def handle_alert(db: Database, telegram: Telegram | None, alert: dict) -> None:
    age = datetime.now(timezone.utc) - datetime.fromisoformat(alert["window_end"])
    if age > MAX_ALERT_AGE:
        print(f"skipped old alert {alert['alert_id']} (window ended {age.total_seconds() / 60:.0f} min ago)")
        return
    text = format_alert(alert)
    if telegram is None:
        print(f"[console mode] {text}")
        return
    chats = db.subscribers(alert["line"])
    sent = sum(telegram.send(chat_id, text) for chat_id in chats)
    print(f"alert {alert['alert_id']}: sent to {sent} of {len(chats)} subscribed chats")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    load_dotenv()
    if not os.getenv("POSTGRES_PASSWORD"):
        sys.exit("POSTGRES_PASSWORD is not set. Copy .env.example to .env and fill it in.")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    db = Database()
    telegram = Telegram(token) if token else None

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        # Consumer group: Kafka stores "how far this group has read" (the committed offset) per partition.
        # A restarted bot continues after the last alert it handled; a second copy of the bot with the same
        # group.id would share the partitions with the first one instead of getting every alert twice.
        "group.id": GROUP_ID,
        # We commit ourselves, after the alert is handled. If the bot dies in between, the alert is read
        # again after a restart: at-least-once (a repeated message is better than a lost one).
        "enable.auto.commit": False,
        # Only for a group with no committed offset yet (the very first start): begin with the oldest alert.
        # Old alerts are then dropped by MAX_ALERT_AGE, so nobody is flooded with history.
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    mode = "Telegram" if telegram else "console mode (TELEGRAM_BOT_TOKEN is not set)"
    print(f"Listening to {TOPIC} at {bootstrap} as group {GROUP_ID}, {mode}. Ctrl+C to stop.")
    try:
        while True:
            # 1. Kafka: wait up to 1 s for an alert
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}", file=sys.stderr)
                else:
                    try:
                        handle_alert(db, telegram, json.loads(msg.value()))
                    except (ValueError, KeyError) as e:
                        print(f"Bad message at offset {msg.offset()}: {e!r}", file=sys.stderr)  # skip it, do not get stuck
                    # "Everything up to and including this message is done"
                    consumer.commit(message=msg, asynchronous=False)

            # 2. Telegram: commands from users
            if telegram is not None:
                for update in telegram.updates():
                    message = update.get("message") or {}
                    text = (message.get("text") or "").strip()
                    if text.startswith("/"):
                        chat_id = message["chat"]["id"]
                        try:
                            reply = handle_command(db, chat_id, text)
                        except psycopg.Error as e:
                            print(f"Database error: {e}", file=sys.stderr)
                            reply = "Something went wrong on my side, try again later."
                        telegram.send(chat_id, reply)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()  # leave the group cleanly, so Kafka does not wait for a session timeout


if __name__ == "__main__":
    main()
