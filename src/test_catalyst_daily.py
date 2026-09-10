"""Unit tests for catalyst_daily selector + action merge (no LLM)."""
from __future__ import annotations

import json
from pathlib import Path

from src import catalyst_daily as cd


def test_signal_weight_scales():
    assert cd.signal_weight("Strong Bullish", 100) == 3.0
    assert cd.signal_weight("Bullish", 50) == 0.9
    assert cd.signal_weight("Neutral", 80) == 0.0
    assert cd.signal_weight("Bearish", 100) == -1.8
    assert cd.signal_weight("Strong Bearish", 50) == -1.5


def test_select_priority(tmp_path, monkeypatch):
    heat_dir = tmp_path / "map_heat"
    news_dir = tmp_path / "news"
    heat_dir.mkdir()
    news_dir.mkdir()
    date = "2026-08-26"
    (heat_dir / f"{date}_map_heat.json").write_text(json.dumps({
        "overrides": [{
            "industry": "Uranium",
            "spx_leaders": [{"ticker": "CCJ"}],
            "rut_leaders": [{"ticker": "UEC"}],
        }],
        "earnings": [{"ticker": "NVDA", "session": "AMC"}],
    }), encoding="utf-8")
    (heat_dir / f"{date}_research.json").write_text(json.dumps({
        "cards": [{
            "action": "OVERRIDE",
            "industry": "Uranium",
            "subsector_dir": "up",
            "captains": [{"ticker": "CCJ", "sent": "pos"}],
        }],
        "opportunities": [{
            "id": "U1", "side": "long", "tickers": ["LEU"],
            "why": "nested override the energy ETF misses",
        }],
        "earnings": [{"ticker": "NVDA", "session": "AMC"}],
    }), encoding="utf-8")
    (news_dir / f"{date}_actions.json").write_text(json.dumps({
        "ticker_actions": [
            {"ticker": "COP", "side": "buy", "net": 6.8, "buy_score": 6.8,
             "sell_score": 0, "events": []},
            {"ticker": "CCJ", "side": "sell", "net": -2.0, "buy_score": 0,
             "sell_score": 2.0, "events": []},
        ]
    }), encoding="utf-8")

    monkeypatch.setattr(cd, "HEAT_DIR", heat_dir)
    monkeypatch.setattr(cd, "NEWS_DIR", news_dir)

    picked = cd.select_targets(date, max_n=6)
    tickers = [p["ticker"] for p in picked]
    roles = {p["ticker"]: p["role"] for p in picked}
    # Researched opportunities and binary earnings get seats before the
    # broader override-captain fill.
    assert tickers[0] == "LEU"
    assert "CCJ" in tickers
    assert "UEC" in tickers
    assert "LEU" in tickers
    assert "NVDA" in tickers
    assert roles["CCJ"] == "override_captain"
    assert "COP" in tickers
    assert roles["COP"] == "action_top"


def test_apply_to_actions(tmp_path, monkeypatch):
    news_dir = tmp_path / "news"
    news_dir.mkdir()
    date = "2026-08-26"
    (news_dir / f"{date}_actions.json").write_text(json.dumps({
        "ticker_actions": [
            {"ticker": "CCJ", "side": "sell", "net": -2.0,
             "buy_score": 0.0, "sell_score": 2.0, "events": []},
        ]
    }), encoding="utf-8")
    (news_dir / f"{date}_actions.md").write_text("# News → Actions\n", encoding="utf-8")
    monkeypatch.setattr(cd, "NEWS_DIR", news_dir)

    dossiers = [{
        "ticker": "CCJ",
        "role": "override_captain",
        "why": "OVERRIDE Uranium",
        "net_signal": "Bullish",
        "conviction": 80,
        "search_backend": "deepseek_fallback",
        "catalyst_stack": "Cameco contract + uranium price.",
    }]
    report = cd.apply_to_actions(date, dossiers)
    rec = next(r for r in report["ticker_actions"] if r["ticker"] == "CCJ")
    assert rec["buy_score"] > 0
    assert any(e.get("event") == "catalyst" for e in rec["events"])
    assert rec["catalyst_signal"] == "Bullish"
    md = (news_dir / f"{date}_actions.md").read_text(encoding="utf-8")
    assert "## Catalyst dossiers" in md
    assert "CCJ" in md


def test_ticker_boosts(tmp_path, monkeypatch):
    out = tmp_path / "catalyst"
    out.mkdir()
    date = "2026-08-26"
    (out / f"{date}_dossiers.json").write_text(json.dumps({
        "dossiers": [
            {"ticker": "CCJ", "net_signal": "Strong Bullish", "conviction": 100,
             "search_backend": "deepseek_fallback"},
            {"ticker": "COP", "error": "fail"},
        ]
    }), encoding="utf-8")
    monkeypatch.setattr(cd, "OUT_DIR", out)
    boosts = cd.ticker_boosts(date)
    assert boosts["CCJ"] == 3.0
    assert "COP" not in boosts


def test_deepseek_dossier_is_usable():
    assert cd.usable_dossier({
        "ticker": "CCJ",
        "net_signal": "Bullish",
        "search_backend": "deepseek_fallback",
    })
    assert not cd.usable_dossier({
        "ticker": "CCJ",
        "net_signal": "Bullish",
        "search_backend": "unknown",
    })


def test_dossier_loop_honours_step_deadline_and_lands_interim(tmp_path, monkeypatch):
    """1800s ceiling with 8 unbounded tickers = SIGKILL and no file.
    Each ticker gets a slice of what is left; when the slice is too thin
    the rest are marked skipped, and every finished dossier is already on
    disk via the progress callback."""
    import os
    import time

    from src import step_deadline

    monkeypatch.setattr(cd, "DATA_CATALYST", tmp_path / "data")
    monkeypatch.setattr(cd.config, "DATABASE_URL", "")
    monkeypatch.setattr(cd.config, "require_llm", lambda: None)

    seen_budgets: list[float | None] = []
    clock = {"rem": None}          # fake step clock; each dossier burns 100s

    def _fake_remaining():
        return clock["rem"]

    monkeypatch.setattr(cd.step_deadline, "remaining_s", _fake_remaining)

    class _Engine:
        CUTOFF_DATE = None
        TODAY = ""
        LOOKBACK_START = ""

        @staticmethod
        def analyze_stock(ticker, snapshot, searxng):
            raw = os.environ.get(step_deadline.ENV)
            seen_budgets.append(None if raw is None else float(raw) - time.time())
            if clock["rem"] is not None:
                clock["rem"] -= 100
            return {"net_signal": "Bullish", "conviction": 70,
                    "search_backend": "deepseek_fallback", "catalyst_grid": []}

    monkeypatch.setattr(cd, "_prepare_engine", lambda skip: _Engine)
    targets = [{"ticker": t, "role": "opportunity", "why": "test"}
               for t in ("AAA", "BBB", "CCC", "DDD")]

    # Unbounded step: every ticker runs, no env deadline leaks.
    monkeypatch.delenv(step_deadline.ENV, raising=False)
    progress: list[int] = []
    out = cd.run_dossiers("2026-09-10", targets, skip_gemini=False,
                          on_progress=lambda rows: progress.append(len(rows)))
    assert [r["ticker"] for r in out] == ["AAA", "BBB", "CCC", "DDD"]
    assert all(b is None for b in seen_budgets)
    assert progress == [1, 2, 3, 4]
    assert step_deadline.ENV not in os.environ

    # 300s left: one ticker fits (300 - 45 reserve = 255 >= 240); after it
    # burns 100s the rest are skipped rather than started into SIGKILL.
    seen_budgets.clear()
    clock["rem"] = 300
    monkeypatch.setenv(step_deadline.ENV, f"{time.time() + 300:.0f}")
    out = cd.run_dossiers("2026-09-10", [dict(t, ticker=t["ticker"] + "2")
                                         for t in targets], skip_gemini=False)
    assert len(seen_budgets) == 1
    assert 200 <= seen_budgets[0] <= 255            # narrowed per-ticker slice
    assert out[0]["net_signal"] == "Bullish"
    assert [r.get("error") for r in out[1:]] == ["skipped: step deadline"] * 3
    assert os.environ[step_deadline.ENV]           # parent's deadline restored
    assert abs(float(os.environ[step_deadline.ENV]) - (time.time() + 300)) < 5

    # Plenty left: slices are bounded by MAX_TICKER_S, all run.
    seen_budgets.clear()
    clock["rem"] = 6000
    monkeypatch.setenv(step_deadline.ENV, f"{time.time() + 6000:.0f}")
    out = cd.run_dossiers("2026-09-10", [dict(t, ticker=t["ticker"] + "3")
                                         for t in targets], skip_gemini=False)
    assert len(out) == 4 and all(not r.get("error") for r in out)
    assert all(b <= cd.MAX_TICKER_S + 1 for b in seen_budgets)
