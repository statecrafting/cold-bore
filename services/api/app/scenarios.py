"""The game layer (spec 009): scenarios are YAML timelines of control
commands with SLO objectives and scoring weights. The engine fires the
timeline, lets the pipeline settle, then computes the score from SQL over
what actually landed: frames (completeness), service_metrics (latency,
recovery), events (the fault record). Every scenario is a named, repeatable
experiment; the score is only as good as the pipeline's accounting."""

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import asyncpg
import yaml
from pydantic import TypeAdapter

from .control import ControlCommand

log = logging.getLogger("coldbore.scenarios")

SCENARIOS_DIR = Path(__file__).resolve().parents[3] / "scenarios"
SETTLE_S = 12.0
CONTROL_ADAPTER: TypeAdapter = TypeAdapter(ControlCommand)


@dataclass
class Step:
    at: float
    cmd: dict


@dataclass
class Scenario:
    id: str
    title: str
    tagline: str
    duration_s: float
    objectives: dict[str, float]
    timeline: list[Step]
    scoring: dict[str, float]  # weight per component, sums to 100

    @classmethod
    def load(cls, path: Path) -> "Scenario":
        raw = yaml.safe_load(path.read_text())
        steps = []
        for entry in raw.get("timeline", []):
            cmd = entry["cmd"]
            CONTROL_ADAPTER.validate_python(cmd)  # refuse bad scenarios at load
            steps.append(Step(at=float(entry["at"]), cmd=cmd))
        steps.sort(key=lambda s: s.at)
        return cls(
            id=raw["id"],
            title=raw["title"],
            tagline=raw.get("tagline", ""),
            duration_s=float(raw["duration_s"]),
            objectives=dict(raw.get("objectives", {})),
            timeline=steps,
            scoring=dict(raw.get("scoring", {})),
        )


def load_all() -> dict[str, Scenario]:
    scenarios: dict[str, Scenario] = {}
    if SCENARIOS_DIR.is_dir():
        for path in sorted(SCENARIOS_DIR.glob("*.yaml")):
            try:
                s = Scenario.load(path)
                scenarios[s.id] = s
            except Exception:
                log.exception("scenario %s failed to load", path.name)
    return scenarios


def grade(total: float) -> str:
    if total >= 95:
        return "S"
    if total >= 85:
        return "A"
    if total >= 70:
        return "B"
    if total >= 50:
        return "C"
    return "F"


@dataclass
class Engine:
    pool_getter: Any  # () -> asyncpg.Pool | None
    publish_control: Any  # async (dict) -> None
    broadcast: Any  # (str) -> None
    scenarios: dict[str, Scenario] = field(default_factory=load_all)
    active: dict | None = None
    _task: asyncio.Task | None = None

    def listing(self) -> list[dict]:
        return [
            {
                "id": s.id,
                "title": s.title,
                "tagline": s.tagline,
                "duration_s": s.duration_s,
                "objectives": s.objectives,
                "steps": len(s.timeline),
            }
            for s in self.scenarios.values()
        ]

    async def start(self, scenario_id: str) -> dict:
        if self.active is not None:
            raise RuntimeError(f"scenario {self.active['scenario']} already running")
        scenario = self.scenarios.get(scenario_id)
        if scenario is None:
            raise KeyError(scenario_id)
        self.active = {
            "scenario": scenario.id,
            "title": scenario.title,
            "started_at": time.time(),
            "duration_s": scenario.duration_s + SETTLE_S,
            "steps_fired": 0,
        }
        self._task = asyncio.create_task(self._run(scenario))
        return self.active

    def _emit(self, kind: str, payload: dict) -> None:
        self.broadcast(
            json.dumps(
                {
                    "type": "event",
                    "kind": kind,
                    "service": "scenario",
                    "t_ms": int(time.time() * 1000),
                    "data": payload,
                }
            )
        )

    async def _run(self, scenario: Scenario) -> None:
        started = time.time()
        self._emit("scenario_started", {"scenario": scenario.id, "title": scenario.title})
        try:
            for step in scenario.timeline:
                delay = started + step.at - time.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                await self.publish_control(step.cmd)
                if self.active:
                    self.active["steps_fired"] += 1
                self._emit(
                    "scenario_step",
                    {"scenario": scenario.id, "at": step.at, "cmd": step.cmd},
                )
            remaining = started + scenario.duration_s - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
            # End of shift: clear every fault, let the pipeline settle, score.
            await self.publish_control({"cmd": "reset"})
            self._emit("scenario_settling", {"scenario": scenario.id, "settle_s": SETTLE_S})
            await asyncio.sleep(SETTLE_S)
            score = await self._score(scenario, started, time.time())
            await self._persist(scenario, started, score)
            self._emit("scenario_scored", {"scenario": scenario.id, **score})
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await self.publish_control({"cmd": "reset"})
            raise
        except Exception:
            log.exception("scenario %s failed", scenario.id)
            self._emit("scenario_failed", {"scenario": scenario.id})
        finally:
            self.active = None

    async def _score(self, scenario: Scenario, started: float, ended: float) -> dict:
        pool: asyncpg.Pool | None = self.pool_getter()
        if pool is None:
            return {"error": "database not connected"}
        window_s = ended - started
        components: dict[str, float] = {}
        detail: dict[str, Any] = {}

        # Completeness: rows landed vs seq spans, per (well, epoch), over the
        # whole run including settle (late fills count: that is the point).
        rows = await pool.fetch(
            """
            SELECT sum(n)::bigint AS rows, sum(span)::bigint AS span FROM (
                SELECT count(*) AS n, max(seq) - min(seq) + 1 AS span
                FROM frames
                WHERE time > to_timestamp($1) AND time <= to_timestamp($2)
                GROUP BY pad_id, well_id, epoch
            ) per_well
            """,
            started,
            ended,
        )
        landed = rows[0]["rows"] or 0
        span = rows[0]["span"] or 0
        completeness_pct = 100.0 * landed / span if span else 0.0
        target_pct = float(scenario.objectives.get("completeness_pct", 100.0))
        components["completeness"] = max(0.0, min(1.0, completeness_pct / target_pct))
        detail["completeness_pct"] = round(completeness_pct, 3)

        # Latency: fraction of ingest snapshots inside the run whose p99 met
        # the objective (snapshots with no traffic are excluded).
        max_p99 = float(scenario.objectives.get("max_p99_ms", 1500.0))
        lat = await pool.fetch(
            """
            SELECT (payload -> 'e2e' ->> 'p99_ms')::float AS p99
            FROM service_metrics
            WHERE service = 'ingest'
              AND time > to_timestamp($1) AND time <= to_timestamp($2)
              AND payload -> 'e2e' IS NOT NULL
            """,
            started,
            ended,
        )
        p99s = [r["p99"] for r in lat if r["p99"] is not None]
        ok = sum(1 for p in p99s if p <= max_p99)
        components["latency"] = ok / len(p99s) if p99s else 0.0
        detail["p99_within_slo_pct"] = round(100.0 * components["latency"], 1)
        detail["p99_worst_ms"] = max(p99s) if p99s else None

        # Recovery: seconds from the last scenario fault command to the first
        # in-SLO ingest snapshot after it. Full credit within the objective,
        # linear falloff to zero at 3x.
        recovery_target = float(scenario.objectives.get("recovery_s", 30.0))
        last_fault = started + max((s.at for s in scenario.timeline), default=0.0)
        rec = await pool.fetch(
            """
            SELECT extract(epoch FROM time)::float8 AS t,
                   (payload -> 'e2e' ->> 'p99_ms')::float AS p99
            FROM service_metrics
            WHERE service = 'ingest' AND time > to_timestamp($1)
              AND payload -> 'e2e' IS NOT NULL
            ORDER BY time
            """,
            last_fault,
        )
        recovered_at = next(
            (r["t"] for r in rec if r["p99"] is not None and r["p99"] <= max_p99), None
        )
        if recovered_at is None:
            components["recovery"] = 0.0
            detail["recovery_s"] = None
        else:
            recovery_s = max(0.0, recovered_at - last_fault)
            detail["recovery_s"] = round(recovery_s, 1)
            if recovery_s <= recovery_target:
                components["recovery"] = 1.0
            else:
                overshoot = (recovery_s - recovery_target) / (2 * recovery_target)
                components["recovery"] = max(0.0, 1.0 - overshoot)

        weights = scenario.scoring or {"completeness": 50, "latency": 25, "recovery": 25}
        total = sum(components.get(k, 0.0) * w for k, w in weights.items())
        return {
            "total": round(total, 1),
            "grade": grade(total),
            "components": {k: round(v * weights.get(k, 0), 1) for k, v in components.items()},
            "weights": weights,
            "detail": detail,
            "window_s": round(window_s, 1),
        }

    async def _persist(self, scenario: Scenario, started: float, score: dict) -> None:
        pool: asyncpg.Pool | None = self.pool_getter()
        if pool is None:
            return
        await pool.execute(
            """
            INSERT INTO scenario_runs (scenario, started_at, finished_at, score)
            VALUES ($1, to_timestamp($2), now(), $3::jsonb)
            """,
            scenario.id,
            started,
            json.dumps(score),
        )

    async def runs(self, limit: int = 20) -> list[dict]:
        pool: asyncpg.Pool | None = self.pool_getter()
        if pool is None:
            return []
        rows = await pool.fetch(
            "SELECT scenario, started_at, finished_at, score FROM scenario_runs "
            "ORDER BY started_at DESC LIMIT $1",
            limit,
        )
        return [
            {
                "scenario": r["scenario"],
                "started_at": r["started_at"].isoformat(),
                "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
                "score": json.loads(r["score"]),
            }
            for r in rows
        ]
