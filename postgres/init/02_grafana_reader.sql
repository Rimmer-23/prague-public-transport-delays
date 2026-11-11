-- Read-only role for Grafana. Any dashboard query is plain SQL sent to the database,
-- so Grafana connects as a user that can only SELECT.
--
-- Runs automatically when the database is created for the first time. On an existing database
-- (or to change the password) run it by hand, it is safe to repeat:
--   docker exec pid-postgres psql -U pid -d pid -f /docker-entrypoint-initdb.d/02_grafana_reader.sql

-- psql command: copy the environment variable of the container into the psql variable grafana_password
\getenv grafana_password GRAFANA_DB_PASSWORD

-- CREATE ROLE has no IF NOT EXISTS: build the statement only when the role is missing and run it (\gexec)
SELECT 'CREATE ROLE grafana_reader LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') \gexec

ALTER ROLE grafana_reader PASSWORD :'grafana_password';

GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
-- ...and on tables that will be created later by the current user
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
