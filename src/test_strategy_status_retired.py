import json
from pathlib import Path

from src.strategy_status_publish import WEBULL_DAYS, from_webull_sim


def _write(root: Path, rows):
    p = root / WEBULL_DAYS
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_retired_excel_all_not_missing(tmp_path):
    _write(tmp_path, [
        {"name": "excel_all_webull_sim", "date": "2026-10-06", "fills": []},
        {"name": "h1_webull_sim", "date": "2026-10-13", "fills": []},
    ])
    rows = from_webull_sim(tmp_path, "2026-10-13")
    assert not any("excel_all_webull_sim" in json.dumps(r) for r in rows)
    assert any("h1_webull_sim" in json.dumps(r) for r in rows)


def test_other_missing_names_still_missing(tmp_path):
    _write(tmp_path, [
        {"name": "foo_webull_sim", "date": "2026-10-12", "fills": []},
        {"name": "h1_webull_sim", "date": "2026-10-13", "fills": []},
    ])
    rows = from_webull_sim(tmp_path, "2026-10-13")
    assert any("foo_webull_sim" in json.dumps(r) and "MISSING" in json.dumps(r) for r in rows)
