"""Time-cut as-of remine — path fork + holdout, no live Pages writes."""
from __future__ import annotations

from pathlib import Path

from src import factor_mine as fm
from src import factor_mine_asof as fma
from src import factor_mine_book as fmb


ZERO_FEES = {
    "commission_per_share": 0.0,
    "commission_min_per_order": 0.0,
    "commission_max_pct_of_amount": 1.0,
    "platform_per_share": 0.0,
    "platform_min_per_order": 0.0,
    "platform_max_pct_of_amount": 1.0,
    "settlement_per_share": 0.0,
    "regulatory_pct_of_amount_sell_only": 0.0,
    "regulatory_min_per_order": 0.0,
    "taf_per_share_sell_only": 0.0,
    "taf_min_per_order": 0.0,
    "taf_max_per_order": 0.0,
}


def _row(date, ticker, **kw):
    r = {
        "date": date, "ticker": ticker, "sources": ["union"],
        "boxes": {}, "alarm": False, "zero_red": True,
        "last_green": True, "src_rank": 0,
    }
    r.update(kw)
    return r


def _panel(cal, rows):
    by_date: dict[str, list] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    return {
        "session_dates": list(cal),
        "rows": rows,
        "by_date": by_date,
        "from_date": cal[0],
        "to_date": cal[-1],
        "n_sessions": len(cal),
        "n_rows": len(rows),
    }


def test_publish_paths_default_is_live() -> None:
    p = fm.publish_paths()
    assert p["json"] == fm.OUT_JSON
    assert p["md"] == fm.OUT_MD
    assert p["dash"] == fm.DASH_DIR
    assert p["persist_panel"] is True
    assert p["write_actions"] is True
    side = fm.publish_paths("03_scoreboard/factor_mine_asof_0909",
                            "dashboard/factor-mine-asof-0909")
    assert side["json"] == (
        fm.ROOT / "03_scoreboard" / "factor_mine_asof_0909" / "factor_mine.json")
    assert side["dash"] == fm.ROOT / "dashboard" / "factor-mine-asof-0909"
    assert side["persist_panel"] is False
    assert side["write_actions"] is False
    assert side["daily_md"] is None
    assert side["json"] != fm.OUT_JSON
    assert side["dash"] != fm.DASH_DIR


def test_slice_panel_drops_later_sessions() -> None:
    cal = ["2026-09-08", "2026-09-09", "2026-09-10"]
    rows = [_row(d, "AAA") for d in cal]
    panel = _panel(cal, rows)
    cut = fm.slice_panel(panel, "2026-09-08", "2026-09-09")
    assert cut["session_dates"] == ["2026-09-08", "2026-09-09"]
    assert cut["to_date"] == "2026-09-09"
    assert all(r["date"] <= "2026-09-09" for r in cut["rows"])
    assert "2026-09-10" not in cut["by_date"]


def test_out_root_write_skips_live_paths(tmp_path) -> None:
    live_json = fm.OUT_JSON.read_bytes() if fm.OUT_JSON.is_file() else None
    live_md = fm.OUT_MD.read_bytes() if fm.OUT_MD.is_file() else None
    live_dash = None
    dash_index = fm.DASH_DIR / "index.html"
    if dash_index.is_file():
        live_dash = dash_index.stat().st_mtime_ns
    live_panel = fm.PANEL_PATH.read_bytes() if fm.PANEL_PATH.is_file() else None
    live_start = fm.OUT_START.read_bytes() if fm.OUT_START.is_file() else None
    live_combo = None
    combo = fm.ROOT / "03_scoreboard" / "factor_mine_combos.json"
    if combo.is_file():
        live_combo = combo.read_bytes()
    live_action = None
    action = fm.ROOT / "03_scoreboard" / "FACTOR_MINE_ACTION.md"
    if action.is_file():
        live_action = action.read_bytes()

    rec = fm.make_recipe("demo_h1", hold=1, top_n=1)
    payload = {
        "generated_at": "2026-09-09T16:00:00-04:00",
        "from_date": "2026-09-08",
        "to_date": "2026-09-09",
        "n_recipes": 1,
        "n_rows": 2,
        "fill": "test",
        "stats": [{
            "name": "demo_h1", "side": "long", "hold": 1,
            "size": "leftover", "sell": "list", "s_boost": "none",
            "win_rate": 0.6, "profitable_day_rate": 0.5,
            "start_green": 1, "start_n": 2, "start_rate": 0.5,
            "total_ret_pct": 1.2, "effectiveness": 10,
            "reliable": True, "audit_ok": True,
        }],
        "recipes": [rec],
        "series": {"demo_h1": [10000, 10100]},
        "daily": {},
        "starts": {},
        "books": {},
        "featured": ["demo_h1"],
    }
    paths = fm.publish_paths(tmp_path, tmp_path / "dash")
    fm.write_outputs(payload, payload["stats"], books=None, paths=paths)
    assert (tmp_path / "factor_mine.json").is_file()
    assert (tmp_path / "FACTOR_MINE.md").is_file()
    assert (tmp_path / "dash" / "index.html").is_file()
    assert not (tmp_path / "action").exists()
    live = fm.publish_paths()
    assert Path(paths["json"]) != Path(live["json"])
    assert Path(paths["dash"]) != Path(live["dash"])

    if live_json is not None:
        assert fm.OUT_JSON.read_bytes() == live_json
    if live_md is not None:
        assert fm.OUT_MD.read_bytes() == live_md
    if live_dash is not None:
        assert dash_index.stat().st_mtime_ns == live_dash
    if live_panel is not None:
        assert fm.PANEL_PATH.read_bytes() == live_panel
    if live_start is not None:
        assert fm.OUT_START.read_bytes() == live_start
    if live_combo is not None:
        assert combo.read_bytes() == live_combo
    if live_action is not None:
        assert action.read_bytes() == live_action


def test_cli_rejects_out_root_with_land_closed() -> None:
    try:
        fm.main(["--out-root", "tmp/asof", "--land-closed"])
    except SystemExit as e:
        assert "--out-root" in str(e)
    else:
        raise AssertionError("expected SystemExit")
    try:
        fm.main(["--holdout"])
    except SystemExit as e:
        assert "--holdout" in str(e)
    else:
        raise AssertionError("expected SystemExit")


def test_holdout_replays_frozen_and_does_not_repick() -> None:
    cal = ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11"]
    rows = [_row(d, "WIN") for d in cal]
    full = _panel(cal, rows)
    bars = {
        ("WIN", "2026-09-08"): {"open": 10, "close": 12},
        ("WIN", "2026-09-09"): {"open": 12, "close": 14},
        ("WIN", "2026-09-10"): {"open": 14, "close": 7},
        ("WIN", "2026-09-11"): {"open": 7, "close": 6},
    }
    rec = fm.make_recipe("union_hot_n4_holdup", hold=1, top_n=1,
                         s_boost="holdup")
    is_panel = fm.slice_panel(full, end="2026-09-09")
    fees = ZERO_FEES
    is_book = fmb.simulate_book(
        is_panel, rec, bars=bars, fees=fees, regime={})
    is_stat = {
        "name": "union_hot_n4_holdup",
        "side": "long",
        "hold": 1,
        "win_rate": 0.80,
        "total_ret_pct": max(5.0, float(is_book.get("total_ret_pct") or 0)),
        "profitable_day_rate": 0.80,
        "start_rate": 0.80,
        "start_green": 2,
        "start_n": 2,
        "book_n_trades": 40,
        "audit_ok": True,
    }
    payload = {
        "from_date": "2026-09-08",
        "to_date": "2026-09-09",
        "n_sessions": 2,
        "n_rows": 2,
        "n_recipes": 1,
        "stats": [is_stat],
        "recipes": [rec],
    }
    assert fm.is_workable_stat(is_stat) is True
    rows_out = fma.score_holdout(
        payload, full, cutoff="2026-09-09", oos_start="2026-09-10",
        names=["union_hot_n4_holdup"],
        bars=bars, fees=fees, regime={})
    by = {r["name"]: r for r in rows_out}
    holdup = by["union_hot_n4_holdup"]
    assert holdup["selected_is"] is True
    assert holdup["is_book_pct"] == is_stat["total_ret_pct"]
    assert holdup["oos_eq_is"] is not None
    assert holdup["fresh_book_pct"] is not None
    assert holdup["oos_book_pct_continued"] is not None
    assert (holdup.get("oos_n_days") or 0) >= 1
    md = fma.render_asof_md(
        payload, rows_out, cutoff="2026-09-09",
        oos_start="2026-09-10", oos_end="2026-09-11")
    assert "union_hot_n4_holdup" in md
    assert "Selected 9/9" in md
    assert "Verdict" in md
    # A loser that fails the 9/9 bar is not selected even if OOS rips.
    loser = {
        "name": "ghost_rip",
        "side": "long",
        "hold": 1,
        "win_rate": 0.10,
        "total_ret_pct": -20.0,
        "profitable_day_rate": 0.10,
        "start_rate": 0.10,
        "book_n_trades": 40,
        "audit_ok": True,
    }
    payload["stats"] = [is_stat, loser]
    names = fma.report_names(payload)
    assert "ghost_rip" not in names
    assert "union_hot_n4_holdup" in names


def test_always_extras_marked_even_when_failing_bar() -> None:
    payload = {
        "stats": [{
            "name": "flatten_h5",
            "win_rate": 0.40,
            "total_ret_pct": 1.0,
            "profitable_day_rate": 0.20,
            "start_rate": 0.10,
            "book_n_trades": 10,
            "audit_ok": True,
        }],
        "recipes": [fm.make_recipe("flatten_h5", universe="flatten", hold=5)],
    }
    names = fma.report_names(payload)
    assert "flatten_h5" in names
    assert fm.is_workable_stat(payload["stats"][0]) is False


if __name__ == "__main__":
    test_publish_paths_default_is_live()
    test_slice_panel_drops_later_sessions()
    test_cli_rejects_out_root_with_land_closed()
    test_always_extras_marked_even_when_failing_bar()
    print("asof unit tests (no write) passed")
