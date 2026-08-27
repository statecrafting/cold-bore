"""aio-pika side: consume the telemetry plane, publish control commands.

The api is a consumer like any other: it binds its own queue on the
telemetry exchange, persists what it sees, and forwards it to the WebSocket
hub. Losing the api loses nothing upstream."""

import asyncio
import json
import logging
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from .config import Settings

log = logging.getLogger("coldbore.broker")

FRAMES_QUEUE = "cb.frames.q"
CONTROL_EXCHANGE = "cb.control.x"
TELEMETRY_EXCHANGE = "cb.telemetry.x"
TELEMETRY_API_QUEUE = "cb.telemetry.api.q"


class Broker:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: AbstractRobustConnection | None = None
        self._control_exchange: aio_pika.abc.AbstractExchange | None = None
        # Latest snapshot per service, served by /api/status and used as the
        # WS hello for late-joining dashboards.
        self.latest: dict[str, Any] = {}
        self._on_metric = None
        self._on_event = None

    async def start(self, on_metric, on_event) -> None:
        """`on_metric(service, payload)` / `on_event(kind, service, t_ms,
        payload)` are async callbacks (persist + broadcast)."""
        self._on_metric = on_metric
        self._on_event = on_event
        self._connection = await aio_pika.connect_robust(
            self._settings.amqp_url, client_properties={"connection_name": "coldbore-api"}
        )
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=256)

        telemetry = await channel.declare_exchange(
            TELEMETRY_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        self._control_exchange = await channel.declare_exchange(
            CONTROL_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
        )
        queue = await channel.declare_queue(TELEMETRY_API_QUEUE, auto_delete=True)
        await queue.bind(telemetry, routing_key="metrics.#")
        await queue.bind(telemetry, routing_key="events.#")
        await queue.consume(self._handle)
        log.info("telemetry consumer bound to %s", TELEMETRY_API_QUEUE)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            try:
                body = json.loads(message.body)
            except json.JSONDecodeError:
                log.warning("unparseable telemetry message dropped")
                return
            key = message.routing_key or ""
            try:
                if key.startswith("metrics."):
                    service = body.get("service", key.removeprefix("metrics."))
                    self.latest[service] = body
                    if self._on_metric is not None:
                        await self._on_metric(service, body)
                elif key.startswith("events.") and self._on_event is not None:
                    await self._on_event(
                        body.get("kind", "unknown"),
                        body.get("service", "unknown"),
                        int(body.get("t_ms", 0)),
                        body.get("payload", {}),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("telemetry handler failed")

    async def publish_poison(self) -> None:
        """Debug injector: publish one malformed frame to the frames
        exchange (the DLQ drill in classic mode; skip-and-count in stream
        mode). Never touches the edge: this is broker-side garbage."""
        if self._connection is None:
            raise RuntimeError("broker not started")
        channel = await self._connection.channel()
        try:
            exchange = await channel.get_exchange("cb.frames.x", ensure=False)
            await exchange.publish(
                aio_pika.Message(
                    body=b'{"v": 99, "garbage": true}',
                    content_type="application/json",
                ),
                routing_key="frames.pad0.well0",
            )
        finally:
            await channel.close()

    async def publish_control(self, command: dict) -> None:
        if self._control_exchange is None:
            raise RuntimeError("broker not started")
        await self._control_exchange.publish(
            aio_pika.Message(
                body=json.dumps(command).encode(),
                content_type="application/json",
            ),
            routing_key="",
        )
