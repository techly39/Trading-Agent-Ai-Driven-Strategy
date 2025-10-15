# DF1 Market Data Feed

This repository contains the DF1 deliverable for the AI-orchestrated SPY trading system: a market data feed that publishes
normalized 5-minute bars for SPY with aligned context feeds (VIX, QQQ, IWM, DIA). The module supports historical replay and
paper/live stubs behind one stable API.

## Module Scope
- Load historical bars for SPY and context indices from the paths declared in `config/settings.yaml`.
- Enforce US regular trading hours (09:30–16:00 ET / 14:30–21:00 UTC) with deterministic UTC timestamps and per-session
  monotonic sequences.
- Publish on topics `bar.update`, `ops.event`, and `health.heartbeat` using the schemas defined in
  `trading_system/core/events.py`.
- Provide `HistoricalFeed` and `PaperLiveFeed` classes that expose the public interface in `task.txt`.
- Support deterministic `align_context(spy_bar)` lookups for downstream breadth and volatility gates.

## Configuration
Configuration is stored in `config/settings.yaml` and can be overridden with environment variables that start with `TS_` and
use `__` to express nesting (e.g., `TS_PATHS__SPY=/alt/path/spy_5m.parquet`). The default configuration points to JSON-backed
`.parquet` files under `data/` which contain synthetic DF1 fixtures.

Key sections:
- `symbols`: primary trading symbols (default `SPY`).
- `context_symbols`: context feeds aligned with the primary stream.
- `paths`: file locations for each symbol (historical replay source).
- `live`: provider configuration stubs for paper/live mode. The DF1 implementation disables live mode unless explicitly
  enabled via configuration and environment credentials.

## Usage
```python
from datetime import date

from trading_system.config import load_settings
from trading_system.data.feeds import HistoricalFeed

settings = load_settings()
feed = HistoricalFeed(settings)

feed.subscribe(["SPY"], lambda bar: print(bar.as_dict()))
feed.start(date(2024, 1, 3), "historical")
```

The `PaperLiveFeed` shares the same interface. In DF1 it validates configuration and emits health/ops events while reporting
missing credentials gracefully.

## Tests & Performance
Tests rely on pytest. Run the full suite (including schema, alignment, integrity, and performance checks) via:

```bash
python -m pytest -q
```

The performance sanity test asserts that replaying one US RTH session for SPY + four context symbols completes in under 200 ms.

## Smoke Replay
A one-day replay smoke test is included in `tests/test_feeds_performance.py`. It exercises the full publication path and
validates the performance gate. The same harness can be used manually by calling `HistoricalFeed.start(session_date, "historical")`.

## Limitations & Follow-ups
- Historical data is stored as JSON for portability and read via a pandas-compatible shim; production deployments should use
  vendor parquet exports.
- Paper/live mode is stubbed pending provider integration. Ops events communicate disabled or missing credentials conditions.
- Session calendar implements a minimal US holiday set sufficient for DF1 tests; production should replace with an exchange
  calendar library.
