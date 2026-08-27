-- Scenario runs and their scores (spec 009). On an existing volume apply
-- manually: docker exec coldbore-timescaledb psql -U coldbore -d coldbore
--   -f - < infra/timescale/init/002_scenarios.sql
CREATE TABLE IF NOT EXISTS scenario_runs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario    TEXT        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    score       JSONB
);
CREATE INDEX IF NOT EXISTS scenario_runs_started_idx ON scenario_runs (started_at DESC);
