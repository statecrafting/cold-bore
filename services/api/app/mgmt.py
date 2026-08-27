"""RabbitMQ management API poller: the broker-side view of consumer lag.

Queue depth + unacked from the broker itself is the authoritative lag signal
in classic mode (the consumer cannot see what it has not been delivered)."""

import logging
from typing import Any

import httpx

from .config import Settings

log = logging.getLogger("coldbore.mgmt")


class MgmtPoller:
    def __init__(self, settings: Settings) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.mgmt_url,
            auth=(settings.mgmt_user, settings.mgmt_password),
            timeout=5.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def all_stats(self) -> dict[str, Any]:
        """Classic queue stats plus the stream's, under a `stream` key. The
        stream appears in the queues API too; its `messages` is total
        retained records (offsets are dense from 0 in a lab session, so
        `messages - 1 - committed_offset` is the consumer's lag)."""
        stats = await self.queue_stats("cb.frames.q")
        stream = await self.queue_stats("cb.frames.s")
        if stream:
            stats = stats or {}
            stats["stream"] = {
                "messages": stream.get("depth", 0),
                "publish_rate": stream.get("publish_rate", 0.0),
                "consumers": stream.get("consumers", 0),
            }
        return stats

    async def queue_stats(self, queue: str = "cb.frames.q", vhost: str = "%2F") -> dict[str, Any]:
        """One poll; {} when the queue does not exist yet or the broker is
        unreachable (both normal during startup and drills)."""
        try:
            resp = await self._client.get(f"/api/queues/{vhost}/{queue}")
            if resp.status_code == 404:
                return {}
            resp.raise_for_status()
            body = resp.json()
        except httpx.HTTPError as exc:
            log.debug("mgmt poll failed: %s", exc)
            return {}
        stats = body.get("message_stats", {})
        return {
            "queue": queue,
            "depth": body.get("messages", 0),
            "ready": body.get("messages_ready", 0),
            "unacked": body.get("messages_unacknowledged", 0),
            "consumers": body.get("consumers", 0),
            "publish_rate": stats.get("publish_details", {}).get("rate", 0.0),
            "deliver_rate": stats.get("deliver_get_details", {}).get("rate", 0.0),
            "ack_rate": stats.get("ack_details", {}).get("rate", 0.0),
        }
