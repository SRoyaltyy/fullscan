"""Weather MD fallback + write. No live Yahoo.

Run: python -m src.test_weather
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import weather


def test_run_from_sector_md() -> None:
    text = """# Sector Prediction — Energy — 2026-09-02
- predicted_direction: **up**
- predicted_magnitude_band: **flat**
- total_score: **2.7** (mult 0.9)
- confidence_score: 0.55
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "energy_predict.md"
        p.write_text(text, encoding="utf-8")
        run = weather._run_from_predict_md(p)
    assert run is not None
    assert run["predicted_direction"] == "up"
    assert run["total_score"] == 2.7
    assert run["confidence_score"] == 0.55


def test_run_from_general_md_footer() -> None:
    text = """# Premarket Prediction — 2026-09-02
> **Prediction: DOWN** | total score -3.825
---
## Pipeline-computed decision (deterministic)
- total_score: **-3.825** (multiplier 0.9)
- predicted_direction: **down**
- confidence_score: 0.52
"""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "predict.md"
        p.write_text(text, encoding="utf-8")
        run = weather._run_from_predict_md(p)
    assert run is not None
    assert run["predicted_direction"] == "down"
    assert run["total_score"] == -3.825
    assert run["confidence_score"] == 0.52


def test_load_runs_fills_missing_scoreboard_from_md() -> None:
    orig_daily = weather.DAILY
    orig_board = weather.SCOREBOARD
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        weather.DAILY = root
        weather.SCOREBOARD = root / "empty_scoreboard.json"
        weather.SCOREBOARD.write_text(json.dumps({"runs": []}), encoding="utf-8")
        (root / "general").mkdir()
        (root / "general" / "2026-09-02_predict.md").write_text(
            "- total_score: **-3.825**\n- predicted_direction: **down**\n"
            "- confidence_score: 0.52\n",
            encoding="utf-8",
        )
        sec = root / "sectors" / "2026-09-02"
        sec.mkdir(parents=True)
        (sec / "energy_predict.md").write_text(
            "- predicted_direction: **up**\n- total_score: **2.7**\n",
            encoding="utf-8",
        )
        general, sectors = weather.load_runs("2026-09-02")
        assert general is not None
        assert general["total_score"] == -3.825
        assert sectors["Energy"]["total_score"] == 2.7
    weather.DAILY = orig_daily
    weather.SCOREBOARD = orig_board


def test_offline_derive_uses_disk() -> None:
    rules = weather._load_json(weather.RULES_PATH) or {}
    th = rules.get("thresholds", {})
    sig, gaps = weather.derive_signals("2026-09-03", th, live=False)
    assert isinstance(sig.get("sectors"), dict)
    assert len(sig["sectors"]) >= 5


if __name__ == "__main__":
    test_run_from_sector_md()
    test_run_from_general_md_footer()
    test_offline_derive_uses_disk()
    test_load_runs_fills_missing_scoreboard_from_md()
    print("4 tests passed")
