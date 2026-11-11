-- Stage 7. Traffic disruptions from the PID RSS feeds, latest known version of each one.
-- Kafka topic pid.disruptions -> spark/jobs/disruptions_to_postgres.py -> this table.
CREATE TABLE IF NOT EXISTS disruptions (
    id           text PRIMARY KEY,       -- <guid> of the RSS item, also the Kafka message key
    kind         text NOT NULL,          -- 'incident' (mimořádnost, unplanned) | 'closure' (výluka, planned)
    title        text,
    link         text,
    lines        text[] NOT NULL DEFAULT '{}',  -- affected lines: {"136"} or {"S1","S22","S9"}
    time_from    timestamptz,
    time_to      timestamptz,            -- NULL = "until further notice"
    priority     smallint,
    description  text,
    first_seen   timestamptz,            -- when our producer saw the item for the first time
    removed_at   timestamptz,            -- when it disappeared from the feed (NULL = still listed)
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- "Which disruptions touch line X": GIN index makes `lines @> ARRAY['136']` / `'136' = ANY(lines)` fast
CREATE INDEX IF NOT EXISTS disruptions_lines_idx ON disruptions USING gin (lines);
CREATE INDEX IF NOT EXISTS disruptions_time_from_idx ON disruptions (time_from);
