"""WebSocket hub: fan live pipeline telemetry out to every connected
dashboard.

Slow-consumer policy (the WS leg of the backpressure story): each client gets
a bounded queue; when it overflows, the *oldest* message drops and a counter
increments. A stalled browser tab degrades its own feed, never the hub, and
the drop count is visible in /api/status.
"""

import asyncio
import contextlib
from dataclasses import dataclass, field

from fastapi import WebSocket

QUEUE_CAP = 500


@dataclass(eq=False)  # identity semantics: usable in the hub's set
class Client:
    ws: WebSocket
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=QUEUE_CAP))
    dropped: int = 0


class Hub:
    def __init__(self) -> None:
        self._clients: set[Client] = set()

    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def total_dropped(self) -> int:
        return sum(c.dropped for c in self._clients)

    def broadcast(self, message: str) -> None:
        """Non-blocking: enqueue for every client, dropping oldest on overflow."""
        for client in self._clients:
            try:
                client.queue.put_nowait(message)
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    client.queue.get_nowait()
                    client.dropped += 1
                with contextlib.suppress(asyncio.QueueFull):
                    client.queue.put_nowait(message)

    async def serve(self, ws: WebSocket, hello: str | None = None) -> None:
        """Own one client connection until it closes. `hello` is sent first
        so a fresh dashboard renders current state immediately."""
        await ws.accept()
        client = Client(ws=ws)
        if hello is not None:
            client.queue.put_nowait(hello)
        self._clients.add(client)
        sender = asyncio.create_task(self._send_loop(client))
        try:
            # Inbound messages are ignored (the dashboard controls the
            # pipeline via POST /api/control); receiving detects disconnect.
            while True:
                await ws.receive_text()
        except Exception:
            pass
        finally:
            self._clients.discard(client)
            sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender

    async def _send_loop(self, client: Client) -> None:
        while True:
            message = await client.queue.get()
            await client.ws.send_text(message)
