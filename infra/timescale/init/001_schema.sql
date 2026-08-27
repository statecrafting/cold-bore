-- cold-bore schema. Runs once on a fresh volume via docker-entrypoint-initdb.d;
-- `docker compose -f infra/docker-compose.yml down -v` resets.
-- Spec: 003-infra (see docs/design/architecture.md §6 for rationale).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── frames: the telemetry hypertable ────────────────────────────────────────
CREATE TABLE frames (
    time         TIMESTAMPTZ NOT NULL,          -- event time (frame t_ms)
    pad_id       SMALLINT    NOT NULL,
    well_id      SMALLINT    NOT NULL,
    epoch        BIGINT      NOT NULL,          -- producer generation (edge start ms)
    seq          BIGINT      NOT NULL,
    pressure_psi REAL        NOT NULL,
    rate_bpm     REAL        NOT NULL,
    proppant_ppa REAL        NOT NULL,
    temp_f       REAL        NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('frames', 'time', chunk_time_interval => INTERVAL '15 minutes');

-- Idempotency backstop: the sink inserts with ON CONFLICT DO NOTHING against
-- this index. `epoch` (producer generation) makes seq reuse across edge
-- restarts non-colliding; `time` participates because a hypertable's unique
-- indexes must include the partitioning column; a duplicate frame carries an
-- identical `time`, so the constraint still fires on real duplicates.
CREATE UNIQUE INDEX frames_identity ON frames (pad_id, well_id, epoch, seq, time);

-- Columnar compression on cold chunks; the achieved ratio is a reported
-- number. Segment by well so per-well reads stay cheap after compression.
ALTER TABLE frames SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pad_id, well_id',
    timescaledb.compress_orderby   = 'time, seq'
);
SELECT add_compression_policy('frames', INTERVAL '1 hour');

-- ── frames_1s: continuous aggregate backing dashboard history ───────────────
-- materialized_only=false (real-time aggregation): queries union the
-- materialized buckets with a live scan of the unmaterialized tail, so the
-- dashboard sees current data even if the refresh policy lags (TimescaleDB
-- >= 2.13 defaults to materialized_only=true, which would hide the tail).
CREATE MATERIALIZED VIEW frames_1s
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 second', time) AS bucket,
    pad_id,
    well_id,
    count(*)          AS n,
    avg(pressure_psi) AS pressure_avg,
    max(pressure_psi) AS pressure_max,
    avg(rate_bpm)     AS rate_avg,
    avg(proppant_ppa) AS proppant_avg,
    avg(temp_f)       AS temp_avg,
    min(seq)          AS seq_min,
    max(seq)          AS seq_max
FROM frames
GROUP BY bucket, pad_id, well_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('frames_1s',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '2 seconds',
    schedule_interval => INTERVAL '2 seconds');

-- ── stream_offsets: transactional consumer offsets (stream mode) ───────────
-- The committed offset is updated in the SAME transaction as the batch
-- insert, so a restarted stream consumer resumes exactly where the data
-- actually ends: no re-read window beyond the crashed batch, no gap.
-- Server-side offset tracking is also updated (best effort) for
-- observability, but this row is the truth.
CREATE TABLE stream_offsets (
    consumer TEXT PRIMARY KEY,
    committed_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── events: faults, gaps, heals, lifecycle ──────────────────────────────────
CREATE TABLE events (
    id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind    TEXT        NOT NULL,
    service TEXT        NOT NULL,
    payload JSONB       NOT NULL
);
CREATE INDEX events_time_idx ON events (time DESC);

-- ── service_metrics: 1 Hz snapshots from every service ─────────────────────
CREATE TABLE service_metrics (
    time    TIMESTAMPTZ NOT NULL,
    service TEXT        NOT NULL,
    payload JSONB       NOT NULL
);
SELECT create_hypertable('service_metrics', 'time', chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX service_metrics_service_idx ON service_metrics (service, time DESC);
