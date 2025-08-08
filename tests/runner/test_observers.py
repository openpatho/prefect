from __future__ import annotations

from websockets.exceptions import ConnectionClosedError

from prefect.runner._observers import FlowRunCancellingObserver


class _RaisingSubscriber:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise ConnectionClosedError(1000, "closed")


async def test_observer_handles_connection_closed_error():
    observer = FlowRunCancellingObserver(lambda _: None)
    observer._events_subscriber = _RaisingSubscriber()

    # Should complete without raising
    await observer._consume_events()

