import json
from datetime import datetime, timezone
from pathlib import Path

from trading_system.data.feeds import HistoricalFeed


def test_rth_filter(feed_config, publisher, session_day, record_writer):
    spy_path = Path(feed_config["paths"]["spy"])
    records = json.loads(spy_path.read_text(encoding="utf-8"))
    records.append(
        {
            "ts": "2024-01-03T14:25:00Z",
            "open": 468.0,
            "high": 468.5,
            "low": 467.9,
            "close": 468.2,
            "volume": 1200000,
        }
    )
    record_writer(spy_path, records)

    feed = HistoricalFeed(feed_config, publisher=publisher)
    collected = []
    feed.subscribe(["SPY"], lambda bar: collected.append(bar))
    feed.start(session_day, "historical")

    assert collected
    for bar in collected:
        ts = datetime.fromisoformat(bar.ts.replace("Z", "+00:00"))
        ts = ts.astimezone(timezone.utc)
        assert ts.hour >= 14
        assert ts.hour <= 20
        assert (ts.hour, ts.minute) >= (14, 30)
        assert (ts.hour, ts.minute) <= (20, 55)
