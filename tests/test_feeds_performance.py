import time

from trading_system.core.events import EventPublisher
from trading_system.data.feeds import HistoricalFeed


def test_replay_under_200ms(feed_config, session_day):
    publisher = EventPublisher()
    feed = HistoricalFeed(feed_config, publisher=publisher)
    start = time.perf_counter()
    feed.start(session_day, "historical")
    duration_ms = (time.perf_counter() - start) * 1000
    assert duration_ms < 200, f"Replay took {duration_ms:.2f} ms"
