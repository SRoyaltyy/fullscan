"""Idiosyncratic ranker for hard-red sit mornings."""
from src.hard_red_exceptions import (
    idio_score, long_ok, rank_day, run, short_ok,
)


def _card(**kw):
    base = {
        "cond_good": 0, "cond_bad": 0, "boxes": {}, "news": {"tone": "missing"},
        "e_pol": "missing", "r_pol": "missing", "on_list": True,
        "sources": ["yday_gainer"],
    }
    base.update(kw)
    return base


def test_e_beat_green_cameras_scores_long():
    card = _card(
        cond_good=6, cond_bad=1,
        boxes={"join": "good", "news": "good", "gen": "bad", "sect": "bad"},
        e_pol="good", e_label="beat · EPS surprise +9.3%",
        news={"tone": "good", "title": "wins contract"},
        last_green=True, yday_ret=4.2, rsi=55, macd_up=True,
    )
    pack = idio_score(card)
    assert pack["score"] >= 3
    assert long_ok(card, pack)
    # Weather cameras listed, not the whole score.
    assert any("gen" in w or "sect" in w for w in pack["weather"])


def test_alarm_and_rsi_ob_not_long():
    card = _card(cond_good=5, cond_bad=0, alarm=True, rsi=82, rsi_ob=True)
    pack = idio_score(card)
    assert not long_ok(card, pack)


def test_red_tape_is_short_ok():
    card = _card(
        cond_good=0, cond_bad=6,
        e_pol="bad", news={"tone": "bad"},
        last_red=True, yday_ret=-8.0,
        boxes={"news": "bad", "join": "bad"},
    )
    pack = idio_score(card)
    assert pack["score"] <= -3
    assert short_ok(card, pack)


def test_rank_day_splits_sides():
    cards = {
        "AAA": _card(cond_good=7, cond_bad=0, e_pol="good", last_green=True,
                     yday_ret=3, rsi=50),
        "BBB": _card(cond_good=0, cond_bad=6, e_pol="bad", last_red=True,
                     yday_ret=-6, rsi=40),
        "ZZZ": _card(cond_good=1, cond_bad=1, rsi=50),
    }
    day = rank_day("2026-09-15", cards, s=-3.84, hard=True, cal=[], bars=None)
    assert day["hard_red"] is True
    assert day["n_cards"] == 3
    assert any(r["ticker"] == "AAA" for r in day["longs"])
    assert any(r["ticker"] == "BBB" for r in day["shorts"])
    assert "Weather S=" in (day["longs"][0]["why"][0])


def test_run_without_panel_is_ok_false(tmp_path, monkeypatch):
    import src.factor_mine as fm
    monkeypatch.setattr(fm, "PANEL_PATH", tmp_path / "missing.json")
    doc = run()
    assert doc["ok"] is False
