"""Hub slow-consumer policy: a stalled client drops its own oldest messages,
never blocks the broadcaster, and the drops are counted."""

from app.ws import QUEUE_CAP, Client, Hub


class FakeWs:
    pass


def test_broadcast_drops_oldest_on_overflow():
    hub = Hub()
    client = Client(ws=FakeWs())
    hub._clients.add(client)

    for i in range(QUEUE_CAP + 10):
        hub.broadcast(f"m{i}")

    assert client.dropped == 10
    assert client.queue.qsize() == QUEUE_CAP
    # Oldest were dropped: the head of the queue is m10, the tail m509.
    assert client.queue.get_nowait() == "m10"


def test_broadcast_without_clients_is_noop():
    hub = Hub()
    hub.broadcast("nobody home")
    assert hub.client_count == 0
    assert hub.total_dropped == 0
