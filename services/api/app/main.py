"""cold-bore api: egress (REST + WebSocket) and control-plane entry point.

Startup order tolerates missing infrastructure: the app comes up, retries the
broker and database in the background, and reports what it can see. During a
drill the api keeps serving with whatever planes are still alive.
"""

import asyncio
import contextlib
import json
import logging
import time
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.staticfiles import StaticFiles
from pydantic import TypeAdapter, ValidationError

from . import db
from .broker import Broker
from .config import settings
from .control import ControlCommand
from .mgmt import MgmtPoller
from .scenarios import Engine
from .ws import Hub

log = logging.getLogger("coldbore.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

DASHBOARD_DIR = Path(__file__).resolve().parents[3] / "dashboard"
CONTROL_ADAPTER: TypeAdapter = TypeAdapter(ControlCommand)

started_at = time.time()
hub = Hub()
broker = Broker(settings)
mgmt = MgmtPoller(settings)
pool: asyncpg.Pool | None = None
latest_broker: dict = {}
_tasks: list[asyncio.Task] = []
engine = Engine(
    pool_getter=lambda: pool,
    publish_control=lambda cmd: broker.publish_control(cmd),
    broadcast=lambda msg: hub.broadcast(msg),
    snapshots_getter=lambda: broker.latest,
)


async def _on_metric(service: str, payload: dict) -> None:
    hub.broadcast(json.dumps({"type": "metrics", "service": service, "data": payload}))
    if pool is not None:
        await db.insert_metric(pool, service, int(payload.get("t_ms", 0)), payload)


async def _on_event(kind: str, service: str, t_ms: int, payload: dict) -> None:
    hub.broadcast(
        json.dumps(
            {"type": "event", "kind": kind, "service": service, "t_ms": t_ms, "data": payload}
        )
    )
    if pool is not None:
        await db.insert_event(pool, kind, service, t_ms, payload)


async def _connect_with_retry() -> None:
    global pool
    while pool is None:
        try:
            pool = await db.connect(settings.pg_url)
            log.info("database pool ready")
        except Exception as exc:
            log.warning("database unavailable (%s); retrying", exc)
            await asyncio.sleep(2.0)
    while True:
        try:
            await broker.start(_on_metric, _on_event)
            log.info("broker connected")
            return
        except Exception as exc:
            log.warning("broker unavailable (%s); retrying", exc)
            await asyncio.sleep(2.0)


async def _poll_broker_stats() -> None:
    global latest_broker
    while True:
        stats = await mgmt.all_stats()
        if stats:
            latest_broker = stats
            hub.broadcast(json.dumps({"type": "broker", "data": stats}))
        await asyncio.sleep(settings.poll_interval_s)


async def _poll_wells() -> None:
    while True:
        if pool is not None:
            try:
                wells = await db.latest_wells(pool)
                if wells:
                    hub.broadcast(json.dumps({"type": "wells", "data": wells}))
            except Exception as exc:
                log.debug("wells poll failed: %s", exc)
        await asyncio.sleep(settings.poll_interval_s)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    _tasks.append(asyncio.create_task(_connect_with_retry()))
    _tasks.append(asyncio.create_task(_poll_broker_stats()))
    _tasks.append(asyncio.create_task(_poll_wells()))
    yield
    for task in _tasks:
        task.cancel()
    for task in _tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await broker.close()
    await mgmt.close()
    if pool is not None:
        await pool.close()


app = FastAPI(title="cold-bore api", version="0.1.0", lifespan=lifespan)


@app.get("/api/status")
async def status() -> dict:
    return {
        "uptime_s": round(time.time() - started_at, 1),
        "services": broker.latest,
        "broker": latest_broker,
        "ws_clients": hub.client_count,
        "ws_dropped": hub.total_dropped,
        "db_connected": pool is not None,
        # Empty list = the substrate is pulsing and scenarios may run.
        "substrate_problems": engine.substrate_problems(),
    }


@app.get("/api/history")
async def get_history(seconds: int = Query(default=300, ge=1, le=86400)) -> list:
    if pool is None:
        raise HTTPException(503, "database not connected")
    return await db.history(pool, seconds)


@app.get("/api/wells")
async def get_wells() -> list:
    if pool is None:
        raise HTTPException(503, "database not connected")
    return await db.latest_wells(pool)


@app.get("/api/completeness")
async def get_completeness(seconds: int = Query(default=60, ge=1, le=86400)) -> list:
    if pool is None:
        raise HTTPException(503, "database not connected")
    return await db.completeness(pool, seconds)


@app.get("/api/events")
async def get_events(limit: int = Query(default=100, ge=1, le=1000)) -> list:
    if pool is None:
        raise HTTPException(503, "database not connected")
    return await db.recent_events(pool, limit)


@app.get("/api/scenarios")
async def list_scenarios() -> dict:
    return {"scenarios": engine.listing(), "active": engine.active}


@app.post("/api/scenarios/{scenario_id}/start")
async def start_scenario(scenario_id: str) -> dict:
    try:
        active = await engine.start(scenario_id)
    except KeyError as exc:
        raise HTTPException(404, f"unknown scenario {scenario_id}") from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True, "active": active}


@app.get("/api/runs")
async def list_runs(limit: int = Query(default=20, ge=1, le=200)) -> list:
    return await engine.runs(limit)


@app.post("/api/debug/poison")
async def inject_poison() -> dict:
    try:
        await broker.publish_poison()
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True}


@app.post("/api/control")
async def post_control(command: dict) -> dict:
    try:
        validated = CONTROL_ADAPTER.validate_python(command)
    except ValidationError as exc:
        raise HTTPException(422, detail=json.loads(exc.json())) from exc
    try:
        await broker.publish_control(validated.model_dump())
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "command": validated.model_dump()}


@app.websocket("/ws")
async def websocket(ws: WebSocket) -> None:
    await hub.serve(
        ws,
        hello=json.dumps({"type": "hello", "services": broker.latest, "broker": latest_broker}),
    )


if DASHBOARD_DIR.is_dir():
    app.mount("/", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")
