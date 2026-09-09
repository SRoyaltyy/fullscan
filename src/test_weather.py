"""Weather MD fallback + write. No live Yahoo.

Run: python -m src.test_weather
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

from src import fetch_channel1, weather


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
            "SCORES_BEGIN\nHORIZON_3D: down:mild:0.52\nSCORES_END\n"
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
        assert (general.get("horizon_calls") or {}).get("HORIZON_3D", {}).get(
            "direction") == "down"
    weather.DAILY = orig_daily
    weather.SCOREBOARD = orig_board


def test_build_skip_news_does_not_touch_db() -> None:
    with mock.patch.object(fetch_channel1, "fetch_vix", return_value={"vix": {}}), \
            mock.patch.object(fetch_channel1, "fetch_commodities_fx", return_value={}), \
            mock.patch.object(fetch_channel1, "fetch_fred_block", return_value={}), \
            mock.patch.object(fetch_channel1, "fetch_futures", return_value={}), \
            mock.patch.object(fetch_channel1, "fetch_finviz_tape",
                              return_value={"available": False, "rows": []}), \
            mock.patch.object(fetch_channel1, "fetch_fear_greed",
                              return_value={"available": False}), \
            mock.patch.object(fetch_channel1, "fetch_yield_spx_corr",
                              return_value={"available": False}), \
            mock.patch.object(fetch_channel1, "fetch_global_sessions",
                              return_value={}), \
            mock.patch.object(
                fetch_channel1, "fetch_news_block",
                side_effect=AssertionError("news must be skipped")):
        data = fetch_channel1.build(
            "predict", "2026-09-09", budget_s=30, skip_news=True)
    assert data["news_24h"].get("skipped") == "weather"


def test_live_channel1_is_budgeted() -> None:
    src = Path(__file__).resolve().parent / "weather.py"
    text = src.read_text(encoding="utf-8")
    assert "budget_s=45" in text
    assert "skip_news=True" in text
    ch1 = (Path(__file__).resolve().parent / "fetch_channel1.py").read_text(
        encoding="utf-8")
    assert "skip remaining FRED" in ch1
    assert "NewsDbError" in ch1


def test_offline_derive_uses_disk() -> None:
    rules = weather._load_json(weather.RULES_PATH) or {}
    th = rules.get("thresholds", {})
    sig, gaps = weather.derive_signals("2026-09-03", th, live=False)
    assert isinstance(sig.get("sectors"), dict)
    assert len(sig["sectors"]) >= 5


def test_weather_ok_requires_vix_and_yields() -> None:
    from src import packet_gates
    with tempfile.TemporaryDirectory() as d:
        fat_unknown = Path(d) / "wx.json"
        fat_unknown.write_text(json.dumps({
            "date": "2026-09-09",
            "signals": {
                "sectors": {s: {"dir": "flat"} for s in (
                    "Technology", "Energy", "Financial", "Healthcare",
                    "Utilities",
                )},
                "vix": "unknown",
                "yields": "unknown",
            },
            "pad": "x" * 900,
        }), encoding="utf-8")
        ok, reason = packet_gates.weather_ok(fat_unknown)
        assert ok is False
        assert "vix" in reason

        stub = Path(d) / "stub.json"
        stub.write_text('{"ok":true}', encoding="utf-8")
        ok, reason = packet_gates.weather_ok(stub)
        assert ok is False
        assert reason.startswith("too_small")

        good = Path(d) / "good.json"
        good.write_text(json.dumps({
            "date": "2026-09-09",
            "signals": {
                "sectors": {s: {"dir": "flat"} for s in (
                    "Technology", "Energy", "Financial", "Healthcare",
                    "Utilities",
                )},
                "vix": "falling",
                "vix_spot": 15.8,
                "yields": "flat",
                "dgs10_current": 4.05,
            },
            "pad": "x" * 900,
        }), encoding="utf-8")
        ok, reason = packet_gates.weather_ok(good)
        assert ok is True, reason


if __name__ == "__main__":
    test_run_from_sector_md()
    test_run_from_general_md_footer()
    test_offline_derive_uses_disk()
    test_load_runs_fills_missing_scoreboard_from_md()
    test_build_skip_news_does_not_touch_db()
    test_live_channel1_is_budgeted()
    test_weather_ok_requires_vix_and_yields()
    print("7 tests passed")
