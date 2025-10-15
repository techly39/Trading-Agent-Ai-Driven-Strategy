"""Event schemas and publisher utilities for the market data feed."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, DefaultDict, Dict, List, Optional


def _ensure_utc_iso(ts: datetime | str) -> str:
    if isinstance(ts, str):
        return ts
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")


class Topic(str, Enum):
    """Well-known pub/sub topics for the market data feed."""

    BAR_UPDATE = "bar.update"
    OPS_EVENT = "ops.event"
    HEALTH_HEARTBEAT = "health.heartbeat"


@dataclass(frozen=True)
class Bar:
    """Normalized bar payload used across feeds."""

    ts: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    source: str
    seq: int

    @classmethod
    def from_raw(
        cls,
        ts: datetime,
        symbol: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Optional[float],
        source: str,
        seq: int,
    ) -> "Bar":
        return cls(
            ts=_ensure_utc_iso(ts),
            symbol=symbol,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=None if volume is None else float(volume),
            source=source,
            seq=int(seq),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source": self.source,
            "seq": self.seq,
        }


class OpsCode(str, Enum):
    """Operations event codes emitted by the feed."""

    GAP = "GAP"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    CLOCK_DRIFT = "CLOCK_DRIFT"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    SESSION_CLOSE = "SESSION_CLOSE"


@dataclass(frozen=True)
class OpsEvent:
    """Operations event payload."""

    ts: str
    code: OpsCode
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthHeartbeat:
    """Heartbeat payload publishing the latest seq per symbol."""

    ts: str
    seq_per_symbol: Dict[str, int]


class EventPublisher:
    """Simple synchronous publisher used by the feed for testing purposes."""

    def __init__(self) -> None:
        self._listeners: DefaultDict[Topic, List[Callable[[Any], None]]] = DefaultDict(list)
        self.history: DefaultDict[Topic, List[Any]] = DefaultDict(list)

    def subscribe(self, topic: Topic, callback: Callable[[Any], None]) -> None:
        self._listeners[topic].append(callback)

    def publish(self, topic: Topic, payload: Any) -> None:
        self.history[topic].append(payload)
        for listener in self._listeners.get(topic, []):
            listener(payload)


__all__ = [
    "Bar",
    "EventPublisher",
    "HealthHeartbeat",
    "OpsCode",
    "OpsEvent",
    "Topic",
]
