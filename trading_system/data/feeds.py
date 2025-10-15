"""Market data feed implementations for historical and paper/live modes."""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import json

from trading_system.core.events import (
    Bar,
    EventPublisher,
    HealthHeartbeat,
    OpsCode,
    OpsEvent,
    Topic,
)
from trading_system.utils.session import DEFAULT_CALENDAR, SessionCalendar

SubscriptionCallback = Callable[[Bar], None]


def _parse_utc(ts: str | datetime) -> datetime:
    if isinstance(ts, datetime):
        dt = ts
    else:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ensure_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")


class Subscription:
    """Subscription handle allowing clients to cancel their callbacks."""

    def __init__(self, symbols: Iterable[str], callback: SubscriptionCallback, registry: List["Subscription"]):
        self.symbols = tuple(symbols)
        self.callback = callback
        self._registry = registry

    def cancel(self) -> None:
        if self in self._registry:
            self._registry.remove(self)


class _ContextManager:
    def __init__(self) -> None:
        self._calendar: SessionCalendar = DEFAULT_CALENDAR
        self._context_symbols: Tuple[str, ...] = ("QQQ", "IWM", "DIA", "VIX")
        self._store: Dict[Tuple[str, date], Dict[str, Bar]] = defaultdict(dict)

    def reset(self, calendar: SessionCalendar, context_symbols: Iterable[str]) -> None:
        self._calendar = calendar
        self._context_symbols = tuple(context_symbols)
        self._store.clear()

    def update(self, bar: Bar) -> None:
        ts = _parse_utc(bar.ts)
        session = self._calendar.session_date_from_ts(ts)
        if session is None:
            return
        key = (bar.source, session)
        self._store[key][bar.symbol] = bar

    def aligned(self, spy_bar: Bar) -> Dict[str, Optional[Bar]]:
        ts = _parse_utc(spy_bar.ts)
        session = self._calendar.session_date_from_ts(ts)
        if session is None:
            return {}
        key = (spy_bar.source, session)
        store = self._store.get(key, {})
        bucket = self._calendar.bucketize(ts)
        aligned: Dict[str, Optional[Bar]] = {}
        for symbol in self._context_symbols:
            candidate = store.get(symbol)
            if candidate is None:
                aligned[symbol] = None
                continue
            candidate_ts = _parse_utc(candidate.ts)
            if self._calendar.bucketize(candidate_ts) <= bucket:
                aligned[symbol] = candidate
            else:
                aligned[symbol] = None
        return aligned


_CONTEXT_MANAGER = _ContextManager()


def align_context(spy_bar: Bar) -> Dict[str, Optional[Bar]]:
    """Return the latest context bars aligned to the provided SPY bar."""

    return _CONTEXT_MANAGER.aligned(spy_bar)


class BaseFeed:
    """Base functionality shared across feed implementations."""

    def __init__(
        self,
        config: Dict[str, any],
        *,
        session_calendar: SessionCalendar | None = None,
        publisher: EventPublisher | None = None,
    ) -> None:
        self.config = config
        self.calendar = session_calendar or DEFAULT_CALENDAR
        self.publisher = publisher or EventPublisher()
        self._subscriptions: List[Subscription] = []
        self._last_bar: Dict[str, Bar] = {}
        self._running: bool = False
        self._seq: Dict[Tuple[str, date], int] = defaultdict(int)
        context_symbols = config.get("context_symbols", ["QQQ", "IWM", "DIA", "VIX"])
        _CONTEXT_MANAGER.reset(self.calendar, context_symbols)

    # --- subscription management -------------------------------------------------
    def subscribe(self, symbols: Iterable[str], on_bar: SubscriptionCallback) -> Subscription:
        sub = Subscription(symbols, on_bar, self._subscriptions)
        self._subscriptions.append(sub)
        return sub

    def _dispatch(self, bar: Bar) -> None:
        if bar.symbol in {"QQQ", "IWM", "DIA", "VIX"}:
            _CONTEXT_MANAGER.update(bar)
        self._last_bar[bar.symbol] = bar
        for sub in list(self._subscriptions):
            if not sub.symbols or bar.symbol in sub.symbols:
                sub.callback(bar)
        self.publisher.publish(Topic.BAR_UPDATE, bar)

    def _publish_bar(
        self,
        bucket: datetime,
        symbol: str,
        row: Dict[str, Any],
        session_date: date,
    ) -> Bar:
        seq_key = (symbol, session_date)
        current_seq = self._seq.get(seq_key, 0) + 1
        self._seq[seq_key] = current_seq
        bar = Bar.from_raw(
            bucket,
            symbol,
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            self.source,
            current_seq,
        )
        self._dispatch(bar)
        if symbol == "SPY":
            _CONTEXT_MANAGER.update(bar)
        return bar

    def get_last_bar(self, symbol: str) -> Optional[Bar]:
        return self._last_bar.get(symbol)

    def stop(self) -> None:
        self._running = False

    # --- hooks to be implemented by subclasses -----------------------------------
    def start(self, session_date: date, mode: str) -> None:  # pragma: no cover - interface only
        raise NotImplementedError

    def replay(
        self,
        session_date: date,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
    ) -> None:  # pragma: no cover - interface only
        raise NotImplementedError


class HistoricalFeed(BaseFeed):
    """Historical replay feed sourced from parquet files."""

    def __init__(
        self,
        config: Dict[str, any],
        *,
        session_calendar: SessionCalendar | None = None,
        publisher: EventPublisher | None = None,
    ) -> None:
        super().__init__(config, session_calendar=session_calendar, publisher=publisher)
        self.source = "historical"

    def start(self, session_date: date, mode: str) -> None:
        if mode != "historical":
            raise ValueError("HistoricalFeed supports mode='historical' only")
        self._running = True
        self.replay(session_date)

    def replay(
        self,
        session_date: date,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
    ) -> None:
        self._running = True
        if from_ts and from_ts.tzinfo is None:
            from_ts = from_ts.replace(tzinfo=timezone.utc)
        if to_ts and to_ts.tzinfo is None:
            to_ts = to_ts.replace(tzinfo=timezone.utc)
        symbols_cfg = self.config.get("symbols", ["SPY"])
        primary_symbol = symbols_cfg[0]
        all_symbols = [*symbols_cfg]
        context = self.config.get("context_symbols", [])
        for sym in context:
            if sym not in all_symbols:
                all_symbols.append(sym)
        frames = {symbol: self._load_symbol(symbol, session_date) for symbol in all_symbols}
        expected_buckets = self.calendar.buckets(session_date)
        session_key = (self.source, session_date)
        self._seq[session_key] = 0
        last_heartbeat_at: Optional[datetime] = None

        context_symbols = [sym for sym in all_symbols if sym != primary_symbol]

        for bucket in expected_buckets:
            if not self._running:
                break
            if from_ts and bucket < from_ts:
                continue
            if to_ts and bucket > to_ts:
                break
            # Publish context symbols first to ensure alignment for SPY consumers.
            for symbol in context_symbols:
                frame = frames.get(symbol, {})
                row = frame.get(bucket)
                if row is None:
                    continue
                self._publish_bar(bucket, symbol, row, session_date)
            # Publish the primary symbol last so align_context can surface context bars in the same bucket.
            primary_frame = frames.get(primary_symbol, {})
            primary_row = primary_frame.get(bucket)
            if primary_row is None:
                continue
            bar = self._publish_bar(bucket, primary_symbol, primary_row, session_date)
            if last_heartbeat_at is None or (bucket - last_heartbeat_at).total_seconds() >= 60:
                self.publisher.publish(
                    Topic.HEALTH_HEARTBEAT,
                    HealthHeartbeat(
                        ts=bar.ts,
                        seq_per_symbol={sym: self._seq.get((sym, session_date), 0) for sym in all_symbols},
                    ),
                )
                last_heartbeat_at = bucket

        self.publisher.publish(
            Topic.OPS_EVENT,
            OpsEvent(
                ts=_parse_utc(expected_buckets[-1]).isoformat().replace("+00:00", "Z"),
                code=OpsCode.SESSION_CLOSE,
                message="Session replay completed",
                metadata={"session_date": session_date.isoformat(), "source": self.source},
            ),
        )

    # --- helpers -----------------------------------------------------------------
    def _load_symbol(self, symbol: str, session_date: date) -> Dict[datetime, Dict[str, float]]:
        paths = self.config.get("paths", {})
        paths_lower = {k.lower(): v for k, v in paths.items()}
        match = paths.get(symbol)
        if match is None:
            match = paths.get(symbol.lower())
        if match is None:
            match = paths_lower.get(symbol.lower())
        if match is None:
            raise FileNotFoundError(f"No path configured for symbol {symbol}")
        path = Path(match)
        if not path.exists():
            raise FileNotFoundError(path)
        records_raw = self._read_records(path)
        if not isinstance(records_raw, list):
            raise ValueError(f"File {path} must contain a list of records")
        parsed: List[Dict[str, Any]] = []
        last_ts: Optional[datetime] = None
        monotonic = True
        for record in records_raw:
            if "ts" not in record:
                raise ValueError(f"Record in {path} missing 'ts'")
            ts = _parse_utc(record["ts"])
            if last_ts and ts < last_ts:
                monotonic = False
            last_ts = ts
            parsed.append(
                {
                    "ts": ts,
                    "open": float(record["open"]),
                    "high": float(record["high"]),
                    "low": float(record["low"]),
                    "close": float(record["close"]),
                    "volume": None if record.get("volume") is None else float(record["volume"]),
                }
            )
        if parsed and not monotonic:
            self.publisher.publish(
                Topic.OPS_EVENT,
                OpsEvent(
                    ts=_ensure_iso(parsed[-1]["ts"]),
                    code=OpsCode.OUT_OF_ORDER,
                    message=f"Out-of-order bars detected for {symbol}",
                ),
            )
        parsed.sort(key=lambda row: row["ts"])
        filtered = [
            row
            for row in parsed
            if self.calendar.in_rth(row["ts"]) and self.calendar.session_date_from_ts(row["ts"]) == session_date
        ]
        bucketed: Dict[datetime, Dict[str, Any]] = {}
        duplicates: Dict[datetime, int] = defaultdict(int)
        for row in filtered:
            bucket = self.calendar.bucketize(row["ts"])
            duplicates[bucket] += 1
            bucketed[bucket] = {
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
        duplicate_buckets = [bucket for bucket, count in duplicates.items() if count > 1]
        if duplicate_buckets:
            self.publisher.publish(
                Topic.OPS_EVENT,
                OpsEvent(
                    ts=_ensure_iso(max(duplicate_buckets)),
                    code=OpsCode.DUPLICATE,
                    message=f"Duplicate bars detected for {symbol}",
                    metadata={"count": len(duplicate_buckets)},
                ),
            )
        if bucketed:
            expected = set(self.calendar.buckets(session_date))
            missing = sorted(expected - set(bucketed.keys()))
            if missing:
                self.publisher.publish(
                    Topic.OPS_EVENT,
                    OpsEvent(
                        ts=_ensure_iso(missing[0]),
                        code=OpsCode.GAP,
                        message=f"Missing bars for {symbol}",
                        metadata={"missing": [_ensure_iso(m) for m in missing]},
                    ),
                )
        return bucketed

    def _read_records(self, path: Path) -> List[Dict[str, Any]]:
        try:
            import pandas as pd  # type: ignore

            df = pd.read_parquet(path)
            return df.to_dict("records")
        except ModuleNotFoundError:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            return data


class PaperLiveFeed(BaseFeed):
    """Stub feed for paper/live modes that validates configuration."""

    def __init__(
        self,
        config: Dict[str, any],
        *,
        session_calendar: SessionCalendar | None = None,
        publisher: EventPublisher | None = None,
    ) -> None:
        super().__init__(config, session_calendar=session_calendar, publisher=publisher)
        self.source = "paper"
        live_config = config.get("live", {})
        self._enabled = bool(live_config.get("enabled"))
        self._provider = live_config.get("provider", "")
        self._env_keys = live_config.get("env_keys", [])

    def start(self, session_date: date, mode: str) -> None:
        if mode not in {"paper", "live"}:
            raise ValueError("PaperLiveFeed supports mode 'paper' or 'live'")
        self.source = mode
        self._running = True
        if not self._enabled:
            self.publisher.publish(
                Topic.OPS_EVENT,
                OpsEvent(
                    ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    code=OpsCode.PROVIDER_ERROR,
                    message="Live provider disabled",
                    metadata={"provider": self._provider, "enabled": False},
                ),
            )
            self._running = False
            return
        missing_env = [key for key in self._env_keys if not os.getenv(key)]
        if missing_env:
            self.publisher.publish(
                Topic.OPS_EVENT,
                OpsEvent(
                    ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    code=OpsCode.PROVIDER_ERROR,
                    message="Live provider credentials missing",
                    metadata={"missing": missing_env},
                ),
            )
            self._running = False
            return
        # In DF1 the live path is a stub; a heartbeat is emitted so the pipeline can observe health.
        self.publisher.publish(
            Topic.HEALTH_HEARTBEAT,
            HealthHeartbeat(ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), seq_per_symbol={}),
        )

    def replay(
        self,
        session_date: date,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
    ) -> None:
        raise NotImplementedError("PaperLiveFeed does not support replay in DF1")


__all__ = [
    "HistoricalFeed",
    "PaperLiveFeed",
    "align_context",
    "BaseFeed",
]
