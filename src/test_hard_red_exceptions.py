"""Idiosyncratic ranker for hard-red sit mornings."""
from src.hard_red_exceptions import (
    build_history, horizon_pack, idio_score, long_ok, open_camera_setup,
    rank_day, run, scan_open_camera_setups, short_ok,
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


def test_horizon_pack_uses_open_to_later_close():
    cal = ["2026-08-13", "2026-08-14", "2026-08-17"]
    book = {"AAA": {
        "2026-08-13": {"o": 10.0, "c": 11.0},
        "2026-08-14": {"o": 11.0, "c": 12.0},
        "2026-08-17": {"o": 12.0, "c": 9.0},
    }}
    h1 = horizon_pack(book, cal, "AAA", "2026-08-13", 1, "long")
    assert h1["pct"] == 20.0
    assert h1["px"] == 12.0
    assert h1["date"] == "2026-08-14"
    h3 = horizon_pack(book, cal, "AAA", "2026-08-13", 2, "long")
    assert h3["pct"] == -10.0
    sh = horizon_pack(book, cal, "AAA", "2026-08-13", 1, "short")
    assert sh["pct"] == -20.0


def test_history_has_camera_tally_every_session():
    cal = ["2026-08-13", "2026-08-14"]
    probe = {
        "2026-08-13": {
            "AAA": _card(cond_good=5, cond_bad=2, e_pol="good",
                         boxes={"join": "good", "gen": "bad"},
                         files=["data/factor_mine/panel.json"]),
        },
    }
    mornings = {"2026-08-13": {"s": -3.8, "hard_red": True},
                "2026-08-14": {"s": 1.0, "hard_red": False}}
    px = {"AAA": {
        "2026-08-13": {"o": 10, "c": 10.5},
        "2026-08-14": {"o": 10.5, "c": 11},
    }}
    hist = build_history(probe, mornings, cal, px)
    rows = hist["AAA"]
    assert len(rows) == 2
    assert rows[0]["cams"] == "+5 −2"
    assert rows[0]["hard_red"] is True
    assert rows[0]["n_pos"] == 5
    assert rows[1]["on_list"] is False
    assert rows[0]["day_pct"] == 5.0
    assert rows[0]["h1"] == 10.0


def test_reconstructed_off_list_day_paints_cameras():
    cal = ["2026-08-13", "2026-08-14"]
    probe = {
        "2026-08-14": {
            "AAA": _card(cond_good=4, cond_bad=0,
                         boxes={"join": "good", "gen": "good"}),
        },
    }
    mornings = {"2026-08-13": {"s": 8.5, "hard_red": False},
                "2026-08-14": {"s": 5.5, "hard_red": False}}
    px = {"AAA": {
        "2026-08-13": {"o": 10, "c": 10.4},
        "2026-08-14": {"o": 10.4, "c": 11},
    }}
    lookback = {
        ("2026-08-13", "AAA"): {
            "date": "2026-08-13",
            "boxes": {"join": "good", "gen": "good", "sector": "neutral",
                      "vol": "good"},
            "condition": {"good": 3, "bad": 0},
            "sources": ["data/join/2026-08-13_ranked.csv",
                        "01_daily/weather/2026-08-13_weather.json"],
            "e_pol": "missing",
        },
    }
    hist = build_history(probe, mornings, cal, px, lookback=lookback)
    r0, r1 = hist["AAA"]
    assert r0["on_list"] is False
    assert r0["reconstructed"] is True
    assert r0["cams"] == "+3 −0"
    assert r0["boxes"]["join"] == "good"
    assert "join" in " ".join(r0["files"])
    assert r1["on_list"] is True
    assert r1["reconstructed"] is False


def _hist_row(open_, n_pos, n_neg, **kw):
    row = {"open": open_, "n_pos": n_pos, "n_neg": n_neg, "hard_red": False,
           "h1": None, "h3": None, "h5": None}
    row.update(kw)
    return row


def test_open_camera_setup_explore_long_example():
    # yesterday +1 −1 (net 0) → today +4 −2 (net +2); cheapest open.
    rows = [
        _hist_row(12, 2, 2),
        _hist_row(11, 2, 1),
        _hist_row(13, 1, 1),
        _hist_row(14, 1, 1),
        _hist_row(10, 4, 2),
    ]
    s = open_camera_setup(rows, 4)
    assert s is not None
    assert s["side"] == "long"
    assert s["quality"] == "explore"
    assert s["open_rank"] == "lowest"
    assert s["net_delta"] == 2
    assert s["tight_long"] is False  # explore, not clean


def test_open_camera_setup_clean_long_and_tight():
    rows = [
        _hist_row(12, 1, 1),
        _hist_row(11, 1, 1),
        _hist_row(13, 1, 1),
        _hist_row(14, 1, 1),
        _hist_row(10, 4, 1),  # n_pos up, n_neg same, net 0→+3, lowest
    ]
    s = open_camera_setup(rows, 4)
    assert s["side"] == "long"
    assert s["quality"] == "clean"
    assert s["open_rank"] == "lowest"
    assert s["net_delta"] == 3
    assert s["tight_long"] is True


def test_open_camera_setup_second_lowest_is_still_long():
    rows = [
        _hist_row(10, 1, 1),
        _hist_row(13, 1, 1),
        _hist_row(14, 1, 1),
        _hist_row(15, 1, 1),
        _hist_row(11, 3, 1),
    ]
    s = open_camera_setup(rows, 4)
    assert s["side"] == "long"
    assert s["open_rank"] == "second_lowest"
    assert s["quality"] == "clean"
    assert s["tight_long"] is False


def test_open_camera_setup_short_clean_and_explore():
    rich = [
        _hist_row(10, 2, 2),
        _hist_row(11, 2, 2),
        _hist_row(12, 2, 2),
        _hist_row(13, 4, 2),
        _hist_row(15, 1, 2),  # richest, n_pos down, n_neg same → clean short
    ]
    clean = open_camera_setup(rich, 4)
    assert clean["side"] == "short"
    assert clean["quality"] == "clean"
    assert clean["open_rank"] == "highest"
    assert clean["net_delta"] == -3

    explore_rows = [
        _hist_row(10, 2, 2),
        _hist_row(11, 2, 2),
        _hist_row(12, 2, 2),
        _hist_row(13, 4, 2),
        _hist_row(15, 5, 4),  # net 2→1 down, but n_pos rose → explore short
    ]
    exp = open_camera_setup(explore_rows, 4)
    assert exp["side"] == "short"
    assert exp["quality"] == "explore"
    assert exp["net_delta"] == -1


def test_open_camera_setup_skips_thin_window_or_missing_cams():
    thin = [_hist_row(10 + i, 1, 1) for i in range(4)]
    assert open_camera_setup(thin, 3) is None
    missing_prev = [
        _hist_row(12, 1, 1),
        _hist_row(11, 1, 1),
        _hist_row(13, 1, 1),
        _hist_row(14, None, None),
        _hist_row(10, 4, 0),
    ]
    assert open_camera_setup(missing_prev, 4) is None
    mid = [
        _hist_row(10, 1, 1),
        _hist_row(11, 1, 1),
        _hist_row(12, 1, 1),
        _hist_row(13, 1, 1),
        _hist_row(12, 5, 0),  # 3rd of 5 opens — not cheap/rich
    ]
    assert open_camera_setup(mid, 4) is None
    flat = [
        _hist_row(12, 2, 1),
        _hist_row(11, 2, 1),
        _hist_row(13, 2, 1),
        _hist_row(14, 3, 1),
        _hist_row(10, 3, 1),  # cheapest but net unchanged
    ]
    assert open_camera_setup(flat, 4) is None


def test_open_camera_setup_skips_rows_without_open_in_window():
    rows = [
        _hist_row(12, 1, 1),
        _hist_row(None, 9, 9),
        _hist_row(11, 1, 1),
        _hist_row(13, 1, 1),
        _hist_row(14, 1, 1),
        _hist_row(10, 3, 1),
    ]
    s = open_camera_setup(rows, 5)
    assert s is not None
    assert s["side"] == "long"
    assert s["open_rank"] == "lowest"


def test_scan_open_camera_setups_splits_all_vs_hard_red(tmp_path):
    rows = [
        _hist_row(12, 1, 1, h1=1.0),
        _hist_row(11, 1, 1, h1=-2.0),
        _hist_row(13, 1, 1, h1=0.5),
        _hist_row(14, 1, 1, h1=0.4),
        _hist_row(10, 4, 1, h1=3.0, hard_red=True),  # tight long, H1 +3
        _hist_row(16, 1, 2, h1=4.0, hard_red=True),  # richest, net 3→-1, short
    ]
    (tmp_path / "AAA.json").write_text(
        __import__("json").dumps({"ticker": "AAA", "from_date": "2026-08-13",
                                  "to_date": "2026-08-20", "rows": rows}),
        encoding="utf-8",
    )
    doc = scan_open_camera_setups(tmp_path)
    assert doc["n_histories"] == 1
    assert doc["all"]["long_all"]["n"] == 1
    assert doc["all"]["long_tight"]["n"] == 1
    assert doc["all"]["short_all"]["n"] == 1
    # short H1 is −stored long = −4
    assert doc["all"]["short_all"]["h1"]["mean"] == -4.0
    assert doc["hard_red"]["long_all"]["n"] == 1
    assert doc["hard_red"]["short_all"]["n"] == 1


def test_backfill_dates_skip_sealed_and_missing_source():
    from src.hard_red_exceptions import backfill_dates
    got = backfill_dates(
        ["2026-10-29", "2026-10-30"],
        ["2026-10-30", "2026-11-02"],
        today="2026-11-04",
    )
    assert got == ["2026-11-02"]
    assert "2026-11-03" not in got


def test_sealed_buy_sell_rows_are_byte_identical(tmp_path, monkeypatch):
    """A recompute that rewrites longs/shorts and ticker rows does not land."""
    import json
    from src import day_movers as dm
    from src import hard_red_exceptions as hre

    sealed_long = {"ticker": "AAA", "idio": 4, "side": "long"}
    sealed_short = {"ticker": "BBB", "idio": -3, "side": "short"}
    day_old = {
        "date": "2026-10-30",
        "longs": [sealed_long],
        "shorts": [sealed_short],
        "n_long_ok": 1,
    }
    row_old = {
        "date": "2026-10-30",
        "open": 10.0,
        "close": 11.0,
        "why": ["sealed buy"],
    }
    dash = tmp_path / "dash"
    (dash / "t").mkdir(parents=True)
    (dash / "t" / "AAA.json").write_text(json.dumps({
        "ticker": "AAA",
        "from_date": "2026-10-30",
        "to_date": "2026-10-30",
        "cam_order": ["join"],
        "cam_short": {"join": "join"},
        "rows": [row_old],
    }), encoding="utf-8")
    latest = {
        "ok": True,
        "from_date": "2026-10-30",
        "to_date": "2026-10-30",
        "n_days": 1,
        "tickers": ["AAA"],
        "days": [day_old],
        "latest": day_old,
    }
    (dash / "latest.json").write_text(
        json.dumps(latest, indent=2, default=str), encoding="utf-8",
    )
    before_row = json.dumps(row_old, default=str)
    before_day = json.dumps(day_old, default=str)
    monkeypatch.setattr(hre, "DASH_DIR", dash)
    monkeypatch.setattr(hre, "OUT_DIR", tmp_path / "out")
    monkeypatch.setattr(hre, "OUT_MD", tmp_path / "HARD.md")
    monkeypatch.setattr(dm, "write_from_hre", lambda *a, **k: dash / "days.json")

    mutated = {
        "date": "2026-10-30",
        "longs": [{"ticker": "ZZZ", "idio": 99, "side": "long"}],
        "shorts": [{"ticker": "QQQ", "idio": -99, "side": "short"}],
        "n_long_ok": 9,
    }
    new_day = {
        "date": "2026-11-02",
        "longs": [{"ticker": "CCC", "idio": 2, "side": "long"}],
        "shorts": [],
        "n_long_ok": 1,
    }
    doc = {
        "ok": True,
        "from_date": "2026-10-01",
        "to_date": "2026-11-02",
        "n_days": 2,
        "tickers": ["AAA", "CCC"],
        "cam_order": ["join"],
        "cam_short": {"join": "join"},
        "latest": new_day,
        "days": [mutated, new_day],
        "history": {
            "AAA": [{
                "date": "2026-10-30",
                "open": 1.0,
                "close": 2.0,
                "why": ["rewritten"],
            }, {
                "date": "2026-11-02",
                "open": 12.0,
                "close": 13.0,
                "why": ["new"],
            }],
        },
    }
    hre.write_json(doc, today="2026-11-04")
    ticker = json.loads((dash / "t" / "AAA.json").read_text(encoding="utf-8"))
    assert json.dumps(ticker["rows"][0], default=str) == before_row
    assert before_row in (dash / "t" / "AAA.json").read_text(encoding="utf-8")
    assert ticker["rows"][1]["date"] == "2026-11-02"
    assert "rewritten" not in json.dumps(ticker["rows"][0])
    saved = json.loads((dash / "latest.json").read_text(encoding="utf-8"))
    assert json.dumps(saved["days"][0], default=str) == before_day
    assert saved["days"][0]["longs"][0]["ticker"] == "AAA"
    assert saved["days"][0]["shorts"][0]["ticker"] == "BBB"
    assert saved["days"][1]["date"] == "2026-11-02"
    assert saved["days"][1]["longs"][0]["ticker"] == "CCC"
    assert "ZZZ" not in json.dumps(saved["days"][0])
    assert saved["from_date"] == "2026-10-30"
    assert saved["to_date"] == "2026-11-02"


def test_hard_red_workflow_schedule_and_loud_skip():
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    text = (root / ".github" / "workflows" / "hard_red_exceptions.yml").read_text(
        encoding="utf-8")
    assert 'cron: "45 21 * * 1-5"' in text
    assert "ubuntu-latest" in text
    assert "A skipped hard-red run is a failure" in text
    assert "cancel-in-progress: false" in text
