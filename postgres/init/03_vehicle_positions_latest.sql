-- Stage 5b (map). One row per trip: the most recent position we know about.
-- Written by spark/jobs/positions_to_postgres.py, read by the Grafana map.
--
-- Unlike line_delays_15m this is not history but *current state*: the row of a trip is overwritten
-- every time the vehicle reports again, and rows of finished trips are deleted by the job.
CREATE TABLE IF NOT EXISTS vehicle_positions_latest (
    trip_id      text        PRIMARY KEY,
    line         text,
    route_type   smallint,
    headsign     text,
    vehicle_id   bigint,
    lat          double precision NOT NULL,
    lon          double precision NOT NULL,
    bearing      integer,
    delay_s      integer,
    last_stop_id text,
    event_time   timestamptz NOT NULL,   -- when the vehicle reported this position
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- The map asks for "positions of the last few minutes"
CREATE INDEX IF NOT EXISTS vehicle_positions_latest_event_time_idx ON vehicle_positions_latest (event_time);
