"""Download the static GTFS timetable of PID and load the reference tables we need: routes and stops.

Usage:
    .venv\\Scripts\\python scripts\\load_gtfs.py [--force]

Result:
    data/gtfs/routes.txt, data/gtfs/stops.txt   - read by the Spark batch job (spark/jobs/daily_stats.py)
    PostgreSQL tables gtfs_routes, gtfs_stops    - read by Grafana and the Telegram bot

PID publishes a new archive every night (~4:00), so the script is safe to run daily:
it asks the server whether the file changed and skips the download if it did not.
"""

import argparse
import csv
import io
import os
import sys
import zipfile
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

GTFS_URL = "https://data.pid.cz/PID_GTFS.zip"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "gtfs"
ZIP_PATH = OUT_DIR / "PID_GTFS.zip"
STAMP_PATH = OUT_DIR / "last_modified.txt"  # Last-Modified header of the archive we have
FILES = ["routes.txt", "stops.txt"]  # the archive also has trips, stop_times, shapes... (hundreds of MB) - not needed


def download(force: bool) -> bool:
    """Download the archive if the server has a newer one. Returns True if a new file was downloaded."""
    headers = {}
    if not force and ZIP_PATH.exists() and STAMP_PATH.exists():
        # Conditional request: the server answers 304 Not Modified (no body) if nothing changed
        headers["If-Modified-Since"] = STAMP_PATH.read_text(encoding="ascii").strip()

    with requests.get(GTFS_URL, headers=headers, stream=True, timeout=60) as response:
        if response.status_code == 304:
            print(f"GTFS archive is up to date ({headers['If-Modified-Since']}), download skipped.")
            return False
        response.raise_for_status()
        size_mb = int(response.headers.get("Content-Length", 0)) / 1e6
        print(f"Downloading {GTFS_URL} ({size_mb:.0f} MB)...")
        tmp_path = ZIP_PATH.with_suffix(".part")
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1 << 20):  # stream to disk, 1 MB at a time
                f.write(chunk)
        tmp_path.replace(ZIP_PATH)  # the old archive is replaced only after a complete download
        if "Last-Modified" in response.headers:
            STAMP_PATH.write_text(response.headers["Last-Modified"], encoding="ascii")
    return True


def extract() -> None:
    """Copy the files we need out of the archive (under fixed names, whatever the archive contains)."""
    with zipfile.ZipFile(ZIP_PATH) as archive:
        for name in FILES:
            (OUT_DIR / name).write_bytes(archive.read(name))
            print(f"Extracted {OUT_DIR / name}")


def read_csv(name: str) -> list[dict]:
    # utf-8-sig: GTFS files may start with a BOM, which would otherwise stick to the first column name
    with open(OUT_DIR / name, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def none_if_empty(value: str | None):
    return value if value not in (None, "") else None


def to_bool(value: str | None):
    return None if value in (None, "") else value == "1"


def load_to_postgres() -> None:
    routes = [
        (
            r["route_id"],
            r["route_short_name"],
            none_if_empty(r.get("route_long_name")),
            none_if_empty(r.get("route_type")),
            none_if_empty(r.get("route_color")),
            to_bool(r.get("is_night")),
            to_bool(r.get("is_regional")),
        )
        for r in read_csv("routes.txt")
    ]
    stops = [
        (
            s["stop_id"],
            none_if_empty(s.get("stop_name")),
            none_if_empty(s.get("stop_lat")),
            none_if_empty(s.get("stop_lon")),
            none_if_empty(s.get("zone_id")),
            none_if_empty(s.get("location_type")) or 0,  # empty means 0 (stop / platform) in GTFS
            none_if_empty(s.get("parent_station")),
            none_if_empty(s.get("platform_code")),
        )
        for s in read_csv("stops.txt")
    ]

    with psycopg.connect(
        # 127.0.0.1, not "localhost": the port is published on IPv4 only, and on Windows "localhost"
        # is tried as IPv6 (::1) first, where the connection attempt can hang.
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "pid"),
        user=os.getenv("POSTGRES_USER", "pid"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    ) as conn:
        # One transaction: readers see either the old tables or the new ones, never an empty table.
        with conn.cursor() as cur:
            cur.execute("TRUNCATE gtfs_routes, gtfs_stops")
            # COPY: PostgreSQL's bulk load, one stream of rows instead of an INSERT per row
            with cur.copy(
                "COPY gtfs_routes (route_id, line, route_long_name, route_type, route_color, is_night, is_regional) FROM STDIN"
            ) as copy:
                for row in routes:
                    copy.write_row(row)
            with cur.copy(
                "COPY gtfs_stops (stop_id, stop_name, lat, lon, zone_id, location_type, parent_station, platform_code) FROM STDIN"
            ) as copy:
                for row in stops:
                    copy.write_row(row)
    print(f"Loaded into PostgreSQL: {len(routes)} routes, {len(stops)} stops.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="download and load even if the archive did not change")
    args = parser.parse_args()

    load_dotenv()
    if not os.getenv("POSTGRES_PASSWORD"):
        sys.exit("POSTGRES_PASSWORD is not set. Copy .env.example to .env and fill it in.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    changed = download(args.force)
    if changed or not all((OUT_DIR / name).exists() for name in FILES):
        extract()
    load_to_postgres()


if __name__ == "__main__":
    main()
