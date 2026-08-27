"""asyncpg access: telemetry persistence and the read queries behind the
REST surface. All reads that back charts go to the frames_1s continuous
aggregate, never raw frames (architecture doc §6)."""

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg


async def connect(pg_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(pg_url, min_size=2, max_size=8)


def _ms_to_dt(t_ms: int) -> datetime:
    return datetime.fromtimestamp(t_ms / 1000.0, tz=UTC)


async def insert_metric(pool: asyncpg.Pool, service: str, t_ms: int, payload: dict) -> None:
    await pool.execute(
        "INSERT INTO service_metrics (time, service, payload) VALUES ($1, $2, $3::jsonb)",
        _ms_to_dt(t_ms),
        service,
        json.dumps(payload),
    )


async def insert_event(
    pool: asyncpg.Pool, kind: str, service: str, t_ms: int, payload: dict
) -> None:
    await pool.execute(
        "INSERT INTO events (time, kind, service, payload) VALUES ($1, $2, $3, $4::jsonb)",
        _ms_to_dt(t_ms),
        kind,
        service,
        json.dumps(payload),
    )


async def recent_events(pool: asyncpg.Pool, limit: int = 100) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        "SELECT time, kind, service, payload FROM events ORDER BY time DESC LIMIT $1",
        limit,
    )
    return [
        {
            "time": r["time"].isoformat(),
            "kind": r["kind"],
            "service": r["service"],
            "payload": json.loads(r["payload"]),
        }
        for r in rows
    ]


async def history(pool: asyncpg.Pool, seconds: int) -> list[dict[str, Any]]:
    """Per-second pipeline totals from the continuous aggregate."""
    rows = await pool.fetch(
        """
        SELECT bucket,
               sum(n)::bigint          AS frames,
               avg(pressure_avg)::real AS pressure_avg,
               avg(rate_avg)::real     AS rate_avg,
               avg(proppant_avg)::real AS proppant_avg
        FROM frames_1s
        WHERE bucket > now() - make_interval(secs => $1)
        GROUP BY bucket
        ORDER BY bucket
        """,
        float(seconds),
    )
    return [
        {
            "bucket": r["bucket"].isoformat(),
            "frames": r["frames"],
            "pressure_avg": r["pressure_avg"],
            "rate_avg": r["rate_avg"],
            "proppant_avg": r["proppant_avg"],
        }
        for r in rows
    ]


async def latest_wells(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Most recent frame per well over the last 10 s (live grid)."""
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (pad_id, well_id)
               pad_id, well_id, seq, time,
               pressure_psi, rate_bpm, proppant_ppa, temp_f
        FROM frames
        WHERE time > now() - interval '10 seconds'
        ORDER BY pad_id, well_id, time DESC, seq DESC
        """
    )
    return [
        {
            "pad": r["pad_id"],
            "well": r["well_id"],
            "seq": r["seq"],
            "time": r["time"].isoformat(),
            "pressure_psi": r["pressure_psi"],
            "rate_bpm": r["rate_bpm"],
            "proppant_ppa": r["proppant_ppa"],
            "temp_f": r["temp_f"],
        }
        for r in rows
    ]


async def completeness(pool: asyncpg.Pool, seconds: int) -> list[dict[str, Any]]:
    """Per-(well, epoch) integrity over a window: seq span vs rows landed.
    `missing` counts frames the span implies but the table lacks (open gaps,
    in-window approximation; store-and-forward heals shrink it
    retroactively). Grouped by epoch so a producer restart mid-window does
    not corrupt the span arithmetic."""
    rows = await pool.fetch(
        """
        SELECT pad_id, well_id, epoch,
               count(*)::bigint                    AS n,
               (max(seq) - min(seq) + 1)::bigint   AS span
        FROM frames
        WHERE time > now() - make_interval(secs => $1)
        GROUP BY pad_id, well_id, epoch
        ORDER BY pad_id, well_id, epoch
        """,
        float(seconds),
    )
    return [
        {
            "pad": r["pad_id"],
            "well": r["well_id"],
            "epoch": r["epoch"],
            "rows": r["n"],
            "span": r["span"],
            "missing": r["span"] - r["n"],
        }
        for r in rows
    ]
