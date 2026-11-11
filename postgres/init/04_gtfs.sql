-- Stage 7. Reference data from the static GTFS timetable (https://data.pid.cz/PID_GTFS.zip).
-- Filled by scripts/load_gtfs.py (TRUNCATE + COPY in one transaction). Empty until the script runs,
-- so every query that uses these tables must LEFT JOIN them.

-- routes.txt: one row per line
CREATE TABLE IF NOT EXISTS gtfs_routes (
    route_id        text PRIMARY KEY,   -- "L991"; vehicle positions carry the same id
    line            text NOT NULL,      -- route_short_name: "A", "22", "S9"
    route_long_name text,               -- "Bílá Hora - Nádraží Hostivař"
    route_type      smallint,           -- 0 tram, 1 metro, 2 train, 3 bus, 4 ferry, 7 funicular, 11 trolleybus
    route_color     text,               -- hex without '#'
    is_night        boolean,
    is_regional     boolean
);
CREATE INDEX IF NOT EXISTS gtfs_routes_line_idx ON gtfs_routes (line);

-- stops.txt: one row per platform / stop post
CREATE TABLE IF NOT EXISTS gtfs_stops (
    stop_id        text PRIMARY KEY,    -- "U693Z1P"; last_stop_id / next_stop_id of a vehicle position
    stop_name      text,
    lat            double precision,
    lon            double precision,
    zone_id        text,
    location_type  smallint,            -- 0 stop/platform, 1 station, 2 entrance, ...
    parent_station text,
    platform_code  text
);
