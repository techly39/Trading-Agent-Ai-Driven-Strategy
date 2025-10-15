import json
from datetime import date
from pathlib import Path
from typing import Dict

import pytest

from trading_system.config import load_settings
from trading_system.core.events import EventPublisher
from trading_system.utils.session import SessionCalendar


@pytest.fixture
def session_calendar() -> SessionCalendar:
    return SessionCalendar()


@pytest.fixture
def feed_config(tmp_path) -> Dict:
    settings = load_settings("config/settings.yaml")
    new_paths = {}
    for key, path_str in settings.get("paths", {}).items():
        source = Path(path_str)
        data = source.read_text(encoding="utf-8")
        dest = tmp_path / source.name
        dest.write_text(data, encoding="utf-8")
        new_paths[key] = str(dest)
    settings["paths"] = new_paths
    return settings


@pytest.fixture
def publisher() -> EventPublisher:
    return EventPublisher()


@pytest.fixture
def session_day() -> date:
    return date(2024, 1, 3)


def write_records(path: Path, records) -> None:
    path.write_text(json.dumps(records), encoding="utf-8")


@pytest.fixture
def record_writer():
    return write_records
