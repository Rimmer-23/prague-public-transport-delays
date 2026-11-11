-- Stage 6. Anomalies: lines that are much later than usual right now.

-- "Usual" delay of a line. Recomputed by the daily batch job (spark/jobs/daily_stats.py) from the Parquet
-- archive, read by the streaming job spark/jobs/detect_anomalies.py. Empty until the batch job has run once;
-- a line without a baseline is checked against a fixed threshold instead.
CREATE TABLE IF NOT EXISTS line_delay_baseline (
    line         text     NOT NULL,
    route_type   smallint NOT NULL,
    windows      integer  NOT NULL,          -- how many 15-min windows the numbers are based on
    avg_delay_s  double precision NOT NULL,  -- mean of the window averages
    std_delay_s  double precision NOT NULL,  -- their standard deviation
    date_from    date     NOT NULL,
    date_to      date     NOT NULL,
    computed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (line, route_type)
);

-- Every alert that was sent to Kafka topic pid.alerts. The table has two jobs:
--   1. memory of the detector: the primary key and the cooldown query make sure a line is reported once,
--      not on every micro-batch while it stays late;
--   2. history for Grafana (annotations on charts, table of recent alerts).
CREATE TABLE IF NOT EXISTS line_alerts (
    line           text        NOT NULL,
    route_type     smallint    NOT NULL,
    window_start   timestamptz NOT NULL,
    window_end     timestamptz NOT NULL,
    avg_delay_s    integer     NOT NULL,
    max_delay_s    integer     NOT NULL,
    trips          integer     NOT NULL,
    positions      integer     NOT NULL,
    threshold_s    integer     NOT NULL,   -- the limit the line crossed
    baseline_avg_s integer,                -- usual delay of the line; NULL = no baseline, fixed threshold used
    detected_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (line, route_type, window_start)
);
CREATE INDEX IF NOT EXISTS line_alerts_window_start_idx ON line_alerts (window_start);

-- Who wants to hear about which line. Written and read by bot/telegram_bot.py.
-- Chat ids are personal data and no dashboard needs them, so the table lives in its own schema:
-- the read-only role grafana_reader has access to schema public only (see 02_grafana_reader.sql).
CREATE SCHEMA IF NOT EXISTS bot;

CREATE TABLE IF NOT EXISTS bot.telegram_subscriptions (
    chat_id    bigint NOT NULL,
    line       text   NOT NULL,            -- line name, or '*' for all lines
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, line)
);
CREATE INDEX IF NOT EXISTS telegram_subscriptions_line_idx ON bot.telegram_subscriptions (line);
