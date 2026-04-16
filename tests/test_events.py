"""PRD §5.1: event-bus behavior — multiple subscribers, failure isolation."""

from __future__ import annotations

from hidock_direct.events import (
    DownloadProgress,
    Event,
    EventBus,
    IdleWaiting,
)


def test_event_bus_multiple_subscribers_all_receive():
    bus = EventBus()
    hits_a: list[Event] = []
    hits_b: list[Event] = []
    bus.subscribe(hits_a.append)
    bus.subscribe(hits_b.append)

    evt = IdleWaiting(state="CONNECTED_IDLE")
    bus.publish(evt)

    assert hits_a == [evt]
    assert hits_b == [evt]


def test_event_bus_failing_subscriber_does_not_break_others():
    bus = EventBus()
    tripped: list[BaseException] = []

    def boom(_evt):
        raise RuntimeError("subscriber blew up")

    good_hits: list[Event] = []
    bus.subscribe(boom)
    bus.subscribe(good_hits.append)
    bus._on_subscriber_error = lambda exc, _sub: tripped.append(exc)  # type: ignore[assignment]

    evt = DownloadProgress(device_filename="X", bytes_done=1, bytes_total=10)
    bus.publish(evt)
    bus.publish(evt)

    # Good subscriber got both events.
    assert good_hits == [evt, evt]
    # Error hook captured both crashes.
    assert len(tripped) == 2


def test_event_bus_unsubscribe():
    bus = EventBus()
    hits: list[Event] = []
    fn = hits.append
    bus.subscribe(fn)
    bus.publish(IdleWaiting(state="A"))
    bus.unsubscribe(fn)
    bus.publish(IdleWaiting(state="B"))
    assert len(hits) == 1
