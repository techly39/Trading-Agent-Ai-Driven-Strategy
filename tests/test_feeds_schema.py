from trading_system.core.events import Topic
from trading_system.data.feeds import HistoricalFeed


def test_bar_schema(feed_config, publisher, session_day):
    feed = HistoricalFeed(feed_config, publisher=publisher)
    received = []

    feed.subscribe(["SPY", "QQQ", "IWM", "DIA", "VIX"], lambda bar: received.append(bar))
    feed.start(session_day, "historical")

    assert received, "Expected bars to be published"
    for bar in received:
        payload = bar.as_dict()
        assert payload["ts"].endswith("Z")
        assert payload["symbol"] in {"SPY", "QQQ", "IWM", "DIA", "VIX"}
        assert payload["source"] == "historical"
        assert isinstance(payload["seq"], int) and payload["seq"] > 0
    assert Topic.HEALTH_HEARTBEAT in publisher.history
    assert publisher.history[Topic.HEALTH_HEARTBEAT], "Heartbeat events expected"
