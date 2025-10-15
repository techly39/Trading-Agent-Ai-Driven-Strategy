import json
from pathlib import Path

from trading_system.core.events import EventPublisher, OpsCode, Topic
from trading_system.data.feeds import HistoricalFeed


def test_gap_and_duplicate_detection(feed_config, publisher, session_day, record_writer):
    spy_path = Path(feed_config["paths"]["spy"])
    records = json.loads(spy_path.read_text(encoding="utf-8"))
    # Introduce a duplicate bucket by repeating an existing timestamp
    duplicate = dict(records[5])
    records.insert(6, duplicate)
    # Remove a bucket to create a gap
    records.pop(20)
    # Swap two entries to simulate out-of-order arrival
    records[30], records[31] = records[31], records[30]
    record_writer(spy_path, records)

    feed = HistoricalFeed(feed_config, publisher=publisher)
    feed.start(session_day, "historical")

    codes = [event.code for event in publisher.history[Topic.OPS_EVENT]]
    assert OpsCode.DUPLICATE in codes
    assert OpsCode.GAP in codes
    assert OpsCode.OUT_OF_ORDER in codes
    assert OpsCode.SESSION_CLOSE in codes



def test_replay_determinism(feed_config, session_day):
    def run_once():
        pub = EventPublisher()
        feed = HistoricalFeed(feed_config, publisher=pub)
        seqs = []
        feed.subscribe(["SPY"], lambda bar: seqs.append(bar.seq))
        feed.start(session_day, "historical")
        return seqs

    first = run_once()
    second = run_once()
    assert first == second
