-- Runs automatically, but only when the database is created for the first time (empty volume).
-- To apply changes later: run the SQL by hand, or `docker compose down -v` and start again.

-- One row per (15-minute window, line): what spark/jobs/delays_to_postgres.py computes.
CREATE TABLE IF NOT EXISTS line_delays_15m (
    window_start timestamptz NOT NULL,
    window_end   timestamptz NOT NULL,
    line         text        NOT NULL,
    route_type   smallint    NOT NULL,  -- GTFS: 0 tram, 1 metro, 2 train, 3 bus, ...; -1 unknown
    positions    integer     NOT NULL,  -- vehicle positions in the window
    trips        integer     NOT NULL,  -- approximate number of distinct trips
    avg_delay_s  integer     NOT NULL,
    max_delay_s  integer     NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    -- The key Spark groups by. Spark sends the same window many times while it is open,
    -- the primary key lets us overwrite the row (INSERT ... ON CONFLICT) instead of adding a new one.
    PRIMARY KEY (window_start, line, route_type)
);

-- Dashboards ask "one line over time"; the primary key index starts with window_start and does not help here.
CREATE INDEX IF NOT EXISTS line_delays_15m_line_idx ON line_delays_15m (line, window_start);
