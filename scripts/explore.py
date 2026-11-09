"""Fetch current PID vehicle positions from Golemio API and print their structure.

Usage:
    .venv\\Scripts\\python scripts\\explore.py [--limit N]

The raw response is saved to data/explore/ so it can be studied later.
"""

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

API_URL = "https://api.golemio.cz/v2/vehiclepositions"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "explore"


def fetch_positions(token: str, limit: int) -> dict:
    response = requests.get(
        API_URL,
        headers={"X-Access-Token": token},
        params={"limit": limit},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10000, help="max vehicles to fetch")
    args = parser.parse_args()

    load_dotenv()
    token = os.getenv("GOLEMIO_API_TOKEN")
    if not token or token == "your-token-here":
        sys.exit("GOLEMIO_API_TOKEN is not set. Copy .env.example to .env and put your token there.")

    data = fetch_positions(token, args.limit)

    # Save raw response for later study
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / f"vehiclepositions_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    features = data.get("features", [])
    print(f"Top-level keys: {list(data.keys())}")
    print(f"Vehicles in response: {len(features)}")
    print(f"Raw response saved to: {out_file}\n")

    if not features:
        return

    print("=== First vehicle (full JSON) ===")
    print(json.dumps(features[0], ensure_ascii=False, indent=2))

    # Quick overview: route types and delays
    # .get() everywhere: we are still learning the structure, missing fields must not crash the script
    route_types = Counter(
        f.get("properties", {}).get("trip", {}).get("gtfs", {}).get("route_type") for f in features
    )
    print(f"\n=== Vehicles by GTFS route_type === {dict(route_types)}")
    print("(0 = tram, 1 = metro, 2 = train, 3 = bus, 4 = ferry, 7 = funicular, 11 = trolleybus)")

    delays = [
        (f.get("properties", {}).get("last_position", {}).get("delay") or {}).get("actual")
        for f in features
    ]
    delays = [d for d in delays if d is not None]
    if delays:
        print(
            f"\n=== Delay (seconds) === vehicles with delay: {len(delays)}, "
            f"min: {min(delays)}, max: {max(delays)}, avg: {sum(delays) / len(delays):.0f}"
        )


if __name__ == "__main__":
    main()
