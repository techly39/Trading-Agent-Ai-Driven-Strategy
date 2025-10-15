from datetime import datetime

from trading_system.data.feeds import HistoricalFeed, align_context
from trading_system.utils.session import SessionCalendar


def test_context_alignment(feed_config, publisher, session_day):
    feed = HistoricalFeed(feed_config, publisher=publisher)
    calendar = SessionCalendar()
    aligned_payloads = []

    def on_spy(bar):
        ctx = align_context(bar)
        aligned_payloads.append((bar, ctx))

    feed.subscribe(["SPY"], on_spy)
    feed.start(session_day, "historical")

    assert aligned_payloads
    for spy_bar, ctx in aligned_payloads:
        assert set(ctx.keys()) == {"QQQ", "IWM", "DIA", "VIX"}
        spy_ts = calendar.bucketize(datetime.fromisoformat(spy_bar.ts.replace("Z", "+00:00")))
        for symbol, context_bar in ctx.items():
            if context_bar is None:
                continue
            ctx_ts = calendar.bucketize(datetime.fromisoformat(context_bar.ts.replace("Z", "+00:00")))
            assert ctx_ts == spy_ts
            assert context_bar.symbol == symbol
