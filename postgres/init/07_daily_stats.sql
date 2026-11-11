-- Stage 8. Results of the daily batch job spark/jobs/daily_stats.py (computed from the Parquet archive).
-- Every table has a `date` column (Prague calendar day). The job is idempotent: re-running it for a date
-- deletes that date's rows and inserts them again in one transaction.

-- Ranking of lines: which lines were late on a given day
CREATE TABLE IF NOT EXISTS daily_line_stats (
    date            date     NOT NULL,
    line            text     NOT NULL,
    route_type      smallint NOT NULL,
    route_long_name text,                 -- from GTFS routes.txt
    trips           integer  NOT NULL,    -- distinct trips seen
    positions       integer  NOT NULL,
    avg_delay_s     integer  NOT NULL,
    p50_delay_s     integer  NOT NULL,    -- median: half of the positions were later than this
    p90_delay_s     integer  NOT NULL,    -- 10 % of the positions were later than this
    max_delay_s     integer  NOT NULL,
    late_share      real     NOT NULL,    -- share of positions with a delay of 3 minutes or more, 0..1
    PRIMARY KEY (date, line, route_type)
);

-- Rush hours, trams vs buses: delay by hour of day and vehicle type
CREATE TABLE IF NOT EXISTS hourly_delay_stats (
    date        date     NOT NULL,
    hour        smallint NOT NULL,        -- 0..23, Prague time
    route_type  smallint NOT NULL,
    trips       integer  NOT NULL,
    positions   integer  NOT NULL,
    avg_delay_s integer  NOT NULL,
    p90_delay_s integer  NOT NULL,
    late_share  real     NOT NULL,
    PRIMARY KEY (date, hour, route_type)
);

-- Where delay is *created*: how many seconds trips lose between two consecutive stops
CREATE TABLE IF NOT EXISTS daily_segment_delays (
    date             date     NOT NULL,
    from_stop_id     text     NOT NULL,
    to_stop_id       text     NOT NULL,
    route_type       smallint NOT NULL,
    from_stop_name   text,                -- from GTFS stops.txt
    to_stop_name     text,
    lat              double precision,    -- coordinates of to_stop, for the map
    lon              double precision,
    lines            text,                -- "9, 22, 98": lines seen on the segment
    trips            integer  NOT NULL,
    avg_delay_gain_s real     NOT NULL,   -- > 0: trips leave the segment later than they entered it
    p90_delay_gain_s integer  NOT NULL,
    PRIMARY KEY (date, from_stop_id, to_stop_id, route_type)
);
CREATE INDEX IF NOT EXISTS daily_segment_delays_gain_idx ON daily_segment_delays (date, avg_delay_gain_s DESC);

-- Did a disruption show up in the delays? Delay of an affected line while the disruption was active
-- vs the same line on the same day outside of any disruption.
CREATE TABLE IF NOT EXISTS daily_disruption_impact (
    date               date    NOT NULL,
    disruption_id      text    NOT NULL,
    line               text    NOT NULL,
    kind               text    NOT NULL,
    title              text,
    time_from          timestamptz,
    time_to            timestamptz,
    positions_during   integer NOT NULL,
    avg_delay_during_s integer NOT NULL,
    positions_other    integer,           -- NULL: the disruption covered the whole observed day
    avg_delay_other_s  integer,
    PRIMARY KEY (date, disruption_id, line)
);
