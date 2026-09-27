"""Render the Pages payload from the append-only log. The page does not invent a day."""
from __future__ import annotations

import json
from pathlib import Path

from research.hot_n4_clean_v4.forward.ledger import HERE, load

ROOT = HERE.parents[2]
PAGE = ROOT / "dashboard" / "holdup"
LOG_JSON = PAGE / "log.json"
STATUS_JSON = PAGE / "status.json"


def write_page(records: list[dict] | None = None, status: dict | None = None) -> None:
    rows = load() if records is None else records
    PAGE.mkdir(parents=True, exist_ok=True)
    LOG_JSON.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if status is not None:
        STATUS_JSON.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
