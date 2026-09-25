"""Append-only factor-mine snapshots, ledgers, and the price hold."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_freeze as fmf
from src import map_heat as mh
from src import ticker_lookback as tl


def _redirect(tmp: Path):
    return (
        fmf.SNAP_DIR, fmf.LEDGER_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH,
    ), (
        tmp / "snapshots", tmp / "ledgers", tmp / "prices",
        tmp / "freeze_manifest.json",
    )


def _use(tmp: Path):
    old, new = _redirect(tmp)
    fmf.SNAP_DIR, fmf.LEDGER_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH = new
    for p in new[:3]:
        p.mkdir(parents=True, exist_ok=True)
    return old


def _restore(old) -> None:
    fmf.SNAP_DIR, fmf.LEDGER_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH = old


def test_snapshot_is_write_once_and_restate_logs_previous_hash() -> None:
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            snap = {
                "date": "2026-09-25",
                "rows": [{"ticker": "AAA", "open": 10.0, "ohlc_hot_score": 1.2,
                          "heat_vintage": "2026-09-24"}],
            }
            first = fmf.write_snapshot("2026-09-25", snap, restate=False)
            try:
                fmf.write_snapshot("2026-09-25", snap, restate=False)
                raised = False
            except fmf.FrozenHistory:
                raised = True
            assert raised
            changed = dict(snap, rows=[{"ticker": "BBB", "open": 11.0,
                                        "ohlc_hot_score": 2.0,
                                        "heat_vintage": "2026-09-25"}])
            second = fmf.write_snapshot("2026-09-25", changed, restate=True)
            assert first != second
            man = fmf.load_manifest()
            assert man["snapshots"]["2026-09-25"]["sha256"] == second
            assert man["restatements"][-1]["prev_sha256"] == first
            assert man["restatements"][-1]["date"] == "2026-09-25"
            assert man["first_frozen"] == "2026-09-25"
        finally:
            _restore(old)


def test_guard_fails_when_an_earlier_hash_changes() -> None:
    old = {
        "snapshots": {"2026-09-25": {"sha256": "aaa"}},
        "ledgers": {"2026-09-25": {"sha256": "bbb"}},
    }
    new = {
        "snapshots": {
            "2026-09-25": {"sha256": "aaa"},
            "2026-09-26": {"sha256": "ccc"},
        },
        "ledgers": {
            "2026-09-25": {"sha256": "bbb"},
            "2026-09-26": {"sha256": "ddd"},
        },
    }
    # File bytes are checked only for dates present on disk. Point the
    # folders at an empty temp dir and skip that half by using dates
    # whose files we write to match.
    with tempfile.TemporaryDirectory() as d:
        saved = _use(Path(d))
        try:
            for date, body, slot in (
                ("2026-09-25", {"k": 1}, "snapshots"),
                ("2026-09-26", {"k": 2}, "snapshots"),
                ("2026-09-25", {"k": 3}, "ledgers"),
                ("2026-09-26", {"k": 4}, "ledgers"),
            ):
                raw = fmf.canonical_bytes(body)
                folder = fmf.SNAP_DIR if slot == "snapshots" else fmf.LEDGER_DIR
                (folder / f"{date}.json").write_bytes(raw)
                new.setdefault(slot, {})[date] = {"sha256": fmf.sha256_bytes(raw)}
            old["snapshots"]["2026-09-25"]["sha256"] = new["snapshots"]["2026-09-25"]["sha256"]
            old["ledgers"]["2026-09-25"]["sha256"] = new["ledgers"]["2026-09-25"]["sha256"]
            fmf.guard_manifest(old, new, restate=[])
            broken = json.loads(json.dumps(new))
            broken["ledgers"]["2026-09-25"] = {"sha256": "changed"}
            try:
                fmf.guard_manifest(old, broken, restate=[])
                failed = False
            except SystemExit:
                failed = True
            assert failed
            try:
                fmf.guard_manifest(old, broken, restate=["2026-09-25"])
                mismatched = False
            except SystemExit:
                mismatched = True
            assert mismatched
            restated = {"k": 99}
            raw = fmf.canonical_bytes(restated)
            (fmf.LEDGER_DIR / "2026-09-25.json").write_bytes(raw)
            broken["ledgers"]["2026-09-25"] = {"sha256": fmf.sha256_bytes(raw)}
            fmf.guard_manifest(old, broken, restate=["2026-09-25"])
        finally:
            _restore(saved)


def test_missing_bars_hold_the_day_and_do_not_write_hot_zero() -> None:
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            def _no_bars(ticker, date, n=1):
                return []

            with mock.patch.object(fmf, "ensure_candidate_bars", wraps=None):
                pass
            with mock.patch("src.price_store.ensure_through", return_value=None), \
                    mock.patch("src.ohlc_ripper.prior_bars", side_effect=_no_bars):
                try:
                    fmf.ensure_candidate_bars("2026-09-25", ["AAA", "BBB"])
                    held = False
                except fmf.HoldDay as e:
                    held = True
                    assert "AAA" in e.missing or e.missing
            assert held
            assert not fmf.snapshot_path("2026-09-25").exists()
        finally:
            _restore(old)


def test_build_panel_fetches_bars_before_candidates() -> None:
    order: list[str] = []

    def fake_universe(date, cal, plan, movers):
        order.append("universe")
        return ["AAA"]

    def fake_ensure(date, tickers):
        order.append("ensure")
        assert tickers == ["AAA"]

    def fake_candidates(date, cal, plan, movers):
        order.append("candidates")
        return {"flatten": ["AAA"]}

    def fake_attach(date, ticker, sources, src_rank, sess, prev, prior, df):
        order.append("attach")
        return {
            "date": date, "ticker": ticker, "sources": sources,
            "src_rank": src_rank, "open": 10.0, "close": 11.0,
            "ohlc_hot_score": 1.5,
        }

    with mock.patch.object(fm, "live_panel_end", return_value="2026-09-25"), \
            mock.patch.object(fm.sm, "load_payload", return_value={}), \
            mock.patch.object(fm.sm, "list_books", return_value=[]), \
            mock.patch.object(fm.sm, "session_calendar",
                              return_value=["2026-09-24", "2026-09-25"]), \
            mock.patch.object(fm.gc, "lookback_calendar",
                              side_effect=lambda c: list(c)), \
            mock.patch.object(fm, "_session_map",
                              return_value=({"2026-09-25": {"date": "2026-09-25"}}, [])), \
            mock.patch.object(fm.fla, "collect_mover_buys",
                              return_value={"by_date": {}}), \
            mock.patch.object(fm.fla, "flatten_day_targets",
                              return_value={"tickers": ["AAA"]}), \
            mock.patch.object(fmf, "ranking_universe", side_effect=fake_universe), \
            mock.patch.object(fmf, "ensure_candidate_bars", side_effect=fake_ensure), \
            mock.patch.object(fm, "_candidates", side_effect=fake_candidates), \
            mock.patch.object(fm, "_attach_row", side_effect=fake_attach), \
            mock.patch.object(fmf, "row_price_problem", return_value=None):
        panel = fm.build_panel("2026-09-25", "2026-09-25", fail_closed=True)
    assert order[:3] == ["universe", "ensure", "candidates"]
    assert panel["rows"][0]["ohlc_hot_score"] == 1.5
    assert panel["rows"][0]["open"] == 10.0


def test_build_panel_refuses_unresolved_hot_score() -> None:
    def fake_attach(date, ticker, sources, src_rank, sess, prev, prior, df):
        return {
            "date": date, "ticker": ticker, "sources": list(sources),
            "src_rank": src_rank, "open": None, "ohlc_hot_score": 0.0,
        }

    with mock.patch.object(fm, "live_panel_end", return_value="2026-09-25"), \
            mock.patch.object(fm.sm, "load_payload", return_value={}), \
            mock.patch.object(fm.sm, "list_books", return_value=[]), \
            mock.patch.object(fm.sm, "session_calendar",
                              return_value=["2026-09-25"]), \
            mock.patch.object(fm.gc, "lookback_calendar",
                              side_effect=lambda c: list(c)), \
            mock.patch.object(fm, "_session_map",
                              return_value=({"2026-09-25": {"date": "2026-09-25"}}, [])), \
            mock.patch.object(fm.fla, "collect_mover_buys",
                              return_value={"by_date": {}}), \
            mock.patch.object(fm.fla, "flatten_day_targets",
                              return_value={"tickers": ["AAA"]}), \
            mock.patch.object(fmf, "ranking_universe", return_value=["AAA"]), \
            mock.patch.object(fmf, "ensure_candidate_bars", return_value=None), \
            mock.patch.object(fm, "_candidates",
                              return_value={"flatten": ["AAA"]}), \
            mock.patch.object(fm, "_attach_row", side_effect=fake_attach), \
            mock.patch.object(fmf, "row_price_problem",
                              return_value="hot_score unresolved"):
        try:
            fm.build_panel("2026-09-25", "2026-09-25", fail_closed=True)
            refused = False
        except fmf.HoldDay as e:
            refused = True
            assert "hot_score" in e.reason
    assert refused


def test_morning_map_heat_is_not_replaced_by_postclose() -> None:
    orig = mh.OUT_DIR
    with tempfile.TemporaryDirectory() as d:
        mh.OUT_DIR = Path(d)
        tl.MAP_HEAT_DIR = Path(d)
        try:
            morning = {
                "date": "2026-09-25",
                "phase": "morning_overlay",
                "overlay_at": "2026-09-25T08:00:00-04:00",
                "generated_at": "2026-09-25T08:00:00-04:00",
                "tape": [{"ticker": "ES", "label": "ES", "last": 1, "change": 0.1}],
                "sectors": [{"sector": f"S{i}", "d1": 0.1, "w1": 0, "rvol": 1}
                            for i in range(11)],
                "industries": [{"industry": "Semis", "d1": 1.0}] * 60,
                "hot": [], "cold": [], "overrides": [], "themes": [],
                "theme_tape": [], "ticker_news": [],
                "econ": [], "earnings": [], "event_options": [],
                "export": "finviz_2026-09-24.csv",
                "n_tickers": 100,
            }
            mh.write("2026-09-25", morning)
            morning_path = Path(d) / "2026-09-25_map_heat_morning.json"
            assert morning_path.is_file()
            frozen = morning_path.read_bytes()
            post = dict(morning)
            post["phase"] = "postclose_baseline"
            post.pop("overlay_at", None)
            post["generated_at"] = "2026-09-26T02:09:00-04:00"
            post["tape"] = [{"ticker": "ES", "label": "ES", "last": 9, "change": 3}]
            # write() refuses a morning header mismatch; postclose is not
            # morning_overlay so it rewrites the live board only.
            mh.write("2026-09-25", post)
            assert morning_path.read_bytes() == frozen
            board, vintage = tl._map_heat_board("2026-09-25", "2026-09-24")
            assert vintage == "2026-09-25"
            assert board.get("phase") == "morning_overlay"
            assert board["tape"][0]["last"] == 1
        finally:
            mh.OUT_DIR = orig
            from src.ticker_lookback import ROOT as tl_root
            tl.MAP_HEAT_DIR = tl_root / "01_daily" / "map_heat"


def test_resume_appends_one_day_and_keeps_prior_trades() -> None:
    cal = ["2026-09-24", "2026-09-25"]
    rows = []
    for d, px in (("2026-09-24", 10.0), ("2026-09-25", 12.0)):
        rows.append({
            "date": d, "ticker": "AAA", "sources": ["union"],
            "src_rank": 0, "boxes": {}, "alarm": False, "open": px, "close": px + 1,
        })
    by = {}
    for r in rows:
        by.setdefault(r["date"], []).append(r)
    panel = {
        "session_dates": cal, "rows": rows, "by_date": by,
        "from_date": cal[0], "to_date": cal[-1],
    }
    bars = {
        ("AAA", "2026-09-24"): {"open": 10.0, "close": 11.0},
        ("AAA", "2026-09-25"): {"open": 12.0, "close": 13.0},
    }
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    regime = {
        "2026-09-24": {"predict_score": 0.0},
        "2026-09-25": {"predict_score": 0.0},
    }
    with mock.patch.object(fm, "session_has_closed", return_value=True), \
            mock.patch.object(fm, "ensure_sim_fields", side_effect=lambda p, rec=None: p):
        full = fmb.simulate_book(panel, rec, bars=bars, fees=fm.pt_fees(), regime=regime)
        first_trades = [t for t in full["trades"] if t["date"] == "2026-09-24"]
        # pos at end of day 1 is the lot still open (hold=1 sells the next day).
        day1 = fmb.simulate_book(
            {**panel, "session_dates": ["2026-09-24"], "to_date": "2026-09-24",
             "rows": [r for r in rows if r["date"] == "2026-09-24"],
             "by_date": {"2026-09-24": by["2026-09-24"]}},
            rec, bars=bars, fees=fm.pt_fees(), regime=regime,
        )
        resume = {
            "cash": day1["cash"],
            "yday_equity": day1["daily"][-1]["equity"],
            "pos": day1["pos"],
            "after": "2026-09-24",
        }
        nxt = fmb.simulate_book(
            panel, rec, bars=bars, fees=fm.pt_fees(), regime=regime, resume=resume,
        )
    assert [t["date"] for t in nxt["trades"]] == ["2026-09-25"] * len(nxt["trades"])
    assert all(t["date"] != "2026-09-24" for t in nxt["trades"])
    assert first_trades
    # Prior day's buy ticker is unchanged by the append.
    assert {t["ticker"] for t in first_trades if t["side"] == "BUY"} == {"AAA"}


def test_reconstructed_label_and_append_does_not_rebuild_old_rows() -> None:
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        old = _use(tmp)
        orig_panel = fm.PANEL_PATH
        orig_out = fm.OUT_JSON
        try:
            fm.PANEL_PATH = tmp / "panel.json"
            fm.OUT_JSON = tmp / "out.json"
            panel = {
                "from_date": "2026-09-24",
                "to_date": "2026-09-24",
                "session_dates": ["2026-09-24"],
                "n_rows": 1,
                "rows": [{
                    "date": "2026-09-24", "ticker": "OLD", "sources": ["flatten"],
                    "src_rank": 0, "open": 5.0, "close": 5.5, "ohlc_hot_score": 1.1,
                    "boxes": {}, "alarm": False,
                }],
            }
            fm.PANEL_PATH.write_text(json.dumps(panel), encoding="utf-8")
            built = []

            def fake_build(date, to_date=None, fail_closed=False):
                built.append(date)
                row = {
                    "date": date, "ticker": "NEW", "sources": ["flatten"],
                    "src_rank": 0, "open": 8.0, "close": 9.0,
                    "ohlc_hot_score": 1.4, "boxes": {}, "alarm": False,
                }
                return {
                    "from_date": date, "to_date": date,
                    "session_dates": [date], "rows": [row],
                    "by_date": {date: [row]}, "n_rows": 1,
                }

            payload = {
                "from_date": "2026-09-24",
                "to_date": "2026-09-24",
                "dates": ["2026-09-24"],
                "n_sessions": 1,
                "capital": 10000,
                "daily": {"union_h1": [{
                    "date": "2026-09-24", "cash": 9000.0, "equity": 10050.0,
                    "bought": ["OLD"], "sold": [], "lots": [{
                        "ticker": "OLD", "shares": 10, "entry_px": 5.0,
                        "entry_date": "2026-09-24",
                    }],
                }]},
                "books": {"union_h1": {"trades": [{
                    "date": "2026-09-24", "ticker": "OLD", "side": "BUY",
                    "shares": 10, "price": 5.0,
                }]}},
                "series": {"union_h1": [10050.0]},
                "starts": {"union_h1": [{
                    "start": "2026-09-24", "days": [{
                        "date": "2026-09-24", "bought": ["OLD"], "sold": [],
                        "equity": 10050.0, "cash": 9000.0,
                    }],
                    "final_equity": 10050.0, "return_pct": 0.5,
                }]},
                "stats": [{"name": "union_h1", "total_ret_pct": 0.5}],
                "mornings": {"2026-09-24": {"s": 1.0}},
                "recipes": [fm.make_recipe("union_h1", hold=1, top_n=1)],
            }
            fm.OUT_JSON.write_text(json.dumps(payload), encoding="utf-8")
            with mock.patch.object(fm, "write_outputs"), \
                    mock.patch.object(fm, "build_panel", side_effect=fake_build), \
                    mock.patch.object(fm, "panel_lookback_calendar",
                                      return_value=["2026-09-24", "2026-09-25"]), \
                    mock.patch.object(fm, "session_has_closed", return_value=True), \
                    mock.patch.object(fmf, "pin_prices", return_value={
                        "date": "2026-09-25",
                        "names": {"NEW": {"prior": [], "open": 8.0, "close": 9.0}},
                    }), \
                    mock.patch.object(fmf, "heat_record", return_value={
                        "vintage": "2026-09-25", "phase": "morning_overlay",
                        "board_date": "2026-09-25", "source": None, "sha256": "abc",
                    }), \
                    mock.patch.object(fmb, "load_regime", return_value={}), \
                    mock.patch.object(fmf, "build_ledger", return_value={
                        "date": "2026-09-25",
                        "origin": "frozen",
                        "recipes": {"union_h1": {
                            "primary": {
                                "buys": [{"ticker": "NEW", "side": "BUY",
                                          "shares": 1, "price": 8.0}],
                                "sells": [],
                                "skips": [],
                                "daily": {
                                    "date": "2026-09-25", "cash": 8000.0,
                                    "equity": 10100.0, "yday_equity": 10050.0,
                                    "bought": ["NEW"], "sold": ["OLD"],
                                    "open_cash": 9000.0, "made_money": True,
                                    "s": 1.0, "hard_red": False,
                                },
                                "trades": [{
                                    "date": "2026-09-25", "ticker": "NEW",
                                    "side": "BUY", "shares": 1, "price": 8.0,
                                }],
                                "state": {"cash": 8000.0, "yday_equity": 10100.0,
                                          "pos": {}, "after": "2026-09-25"},
                            },
                            "starts": {
                                "2026-09-24": {
                                    "buys": [],
                                    "sells": [],
                                    "skips": [],
                                    "daily": {
                                        "date": "2026-09-25", "cash": 8000.0,
                                        "equity": 10100.0, "yday_equity": 10050.0,
                                        "bought": ["NEW"], "sold": [],
                                        "open_cash": 9000.0, "made_money": True,
                                    },
                                    "trades": [],
                                    "state": {"after": "2026-09-25"},
                                },
                            },
                        }},
                    }):
                out = fmf.append_land(
                    "2026-09-24", "2026-09-25", write=True,
                    recipes=payload["recipes"], payload=payload,
                )
            assert built == ["2026-09-25"]
            saved = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
            old_rows = [r for r in saved["rows"] if r["date"] == "2026-09-24"]
            assert old_rows[0]["ticker"] == "OLD"
            assert old_rows[0]["open"] == 5.0
            assert any(r["date"] == "2026-09-25" and r["ticker"] == "NEW"
                       for r in saved["rows"])
            assert fmf.snapshot_path("2026-09-25").is_file()
            assert fmf.ledger_path("2026-09-25").is_file()
            assert out["freeze"]["first_frozen"] == "2026-09-25"
            assert "2026-09-24" in out["reconstructed_dates"]
            assert "2026-09-25" not in out["reconstructed_dates"]
            prior = out["daily"]["union_h1"][0]
            assert prior["date"] == "2026-09-24"
            assert prior["bought"] == ["OLD"]
            assert out["daily"]["union_h1"][-1]["date"] == "2026-09-25"
        finally:
            fm.PANEL_PATH = orig_panel
            fm.OUT_JSON = orig_out
            _restore(old)


def test_load_or_build_does_not_rebuild_landed_dates() -> None:
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        orig = fm.PANEL_PATH
        fm.PANEL_PATH = tmp / "panel.json"
        try:
            fm.PANEL_PATH.write_text(json.dumps({
                "from_date": "2026-09-24",
                "to_date": "2026-09-24",
                "session_dates": ["2026-09-24"],
                "rows": [{"date": "2026-09-24", "ticker": "KEEP", "src_rank": 0}],
            }), encoding="utf-8")
            seen = []

            def fake_build(date, to_date=None, fail_closed=False):
                seen.append((date, fail_closed))
                row = {"date": date, "ticker": "NEW", "src_rank": 0}
                return {
                    "session_dates": [date], "rows": [row],
                    "by_date": {date: [row]},
                }

            with mock.patch.object(fm, "build_panel", side_effect=fake_build), \
                    mock.patch.object(fm, "panel_lookback_calendar",
                                      return_value=["2026-09-24", "2026-09-25"]), \
                    mock.patch.object(fm, "live_panel_end",
                                      return_value="2026-09-25"):
                out = fm.load_or_build_panel("2026-09-24", "2026-09-25")
            assert seen == [("2026-09-25", False)]
            tickers = {(r["date"], r["ticker"]) for r in out["rows"]}
            assert ("2026-09-24", "KEEP") in tickers
            assert ("2026-09-25", "NEW") in tickers
        finally:
            fm.PANEL_PATH = orig


def test_partial_ledger_is_not_frozen() -> None:
    panel = {
        "session_dates": ["2026-09-25"],
        "rows": [],
        "by_date": {"2026-09-25": []},
    }
    recipes = [fm.make_recipe("union_h1", hold=1, top_n=1)]
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            with mock.patch.object(fmf, "_simulate_single", side_effect=RuntimeError("boom")):
                try:
                    fmf.build_ledger(panel, {}, recipes, "2026-09-25", {})
                    held = False
                except fmf.HoldDay as e:
                    held = True
                    assert "union_h1" in e.missing
            assert held
            assert not fmf.ledger_path("2026-09-25").exists()
        finally:
            _restore(old)


if __name__ == "__main__":
    test_snapshot_is_write_once_and_restate_logs_previous_hash()
    test_guard_fails_when_an_earlier_hash_changes()
    test_missing_bars_hold_the_day_and_do_not_write_hot_zero()
    test_build_panel_fetches_bars_before_candidates()
    test_build_panel_refuses_unresolved_hot_score()
    test_morning_map_heat_is_not_replaced_by_postclose()
    test_resume_appends_one_day_and_keeps_prior_trades()
    test_reconstructed_label_and_append_does_not_rebuild_old_rows()
    test_load_or_build_does_not_rebuild_landed_dates()
    test_partial_ledger_is_not_frozen()
    print("factor-mine freeze tests passed")
