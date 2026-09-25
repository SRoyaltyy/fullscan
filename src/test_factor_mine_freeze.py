"""Append-only factor-mine snapshots, ledgers, and the price hold."""
from __future__ import annotations

import gzip
import json
import os
import sys
import tempfile
from contextlib import ExitStack
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
        fmf.LINEUP_DIR, fmf.CANDIDATE_DIR,
    ), (
        tmp / "snapshots", tmp / "ledgers", tmp / "prices",
        tmp / "freeze_manifest.json", tmp / "lineups", tmp / "candidates",
    )


def _use(tmp: Path):
    old, new = _redirect(tmp)
    (fmf.SNAP_DIR, fmf.LEDGER_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH,
     fmf.LINEUP_DIR, fmf.CANDIDATE_DIR) = new
    for p in (new[0], new[1], new[2], new[4], new[5]):
        p.mkdir(parents=True, exist_ok=True)
    return old


def _restore(old) -> None:
    (fmf.SNAP_DIR, fmf.LEDGER_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH,
     fmf.LINEUP_DIR, fmf.CANDIDATE_DIR) = old


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
                raw = fmf.encode_frozen(slot, body)
                path = fmf.snapshot_path(date) if slot == "snapshots" else fmf.ledger_path(date)
                path.write_bytes(raw)
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
            raw = fmf.encode_frozen("ledgers", restated)
            fmf.ledger_path("2026-09-25").write_bytes(raw)
            broken["ledgers"]["2026-09-25"] = {"sha256": fmf.sha256_bytes(raw)}
            fmf.guard_manifest(old, broken, restate=["2026-09-25"])
        finally:
            _restore(saved)


def test_missing_bars_hold_the_day_and_do_not_write_hot_zero() -> None:
    """Every name lacking a Yahoo bar is a hard failure, and is not scored."""
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            with mock.patch("src.price_store.ensure_through", return_value=None), \
                    mock.patch.object(fmf, "_raw_bars", return_value=[]), \
                    mock.patch.object(tl, "_official_ohlc",
                                      return_value={"open": None, "close": None,
                                                    "high": None, "low": None}):
                try:
                    fmf.ensure_candidate_bars("2026-09-25", [])
                    empty = False
                except fmf.HoldDay as e:
                    empty = True
                    assert e.status == "held_incomplete"
                    assert "entire universe or panel is missing" in e.reason
                assert empty
                try:
                    fmf.ensure_candidate_bars("2026-09-25", ["BBB", "AAA"])
                    failed = False
                except fmf.HoldDay as e:
                    failed = True
                    assert e.status == "held_incomplete"
                    assert "entire universe missing yahoo bars" in e.reason
                    assert e.missing == ["AAA", "BBB"]
                assert failed

            gaps = [
                {"ticker": "AAA", "missing": ["missing 09:30 open"],
                 "reason": "missing 09:30 open"},
                {"ticker": "BBB", "missing": ["missing 09:30 open"],
                 "reason": "missing 09:30 open"},
            ]

            def fake_attach(*_a, **_k):
                raise AssertionError("a dropped name must not be scored")

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
                                      return_value={"tickers": ["AAA", "BBB"]}), \
                    mock.patch.object(fmf, "ranking_universe",
                                      return_value=["AAA", "BBB"]), \
                    mock.patch.object(fmf, "ensure_candidate_bars", return_value=gaps), \
                    mock.patch.object(fm, "_candidates",
                                      return_value={"flatten": ["AAA", "BBB"]}), \
                    mock.patch.object(fm, "_attach_row", side_effect=fake_attach):
                try:
                    fm.build_panel("2026-09-25", "2026-09-25", fail_closed=True)
                    skipped = False
                except fmf.SkipDay as e:
                    skipped = True
                    assert "no rankable" in e.reason
            assert skipped
            assert not fmf.snapshot_path("2026-09-25").exists()
        finally:
            _restore(old)


def _bars(n: int, close: float = 10.0) -> list[dict]:
    out = []
    for i in range(n):
        day = f"2026-07-{i+1:02d}" if i < 28 else f"2026-08-{i-27:02d}"
        out.append({
            "date": day, "open": close, "high": close, "low": close,
            "close": close, "volume": 100.0,
        })
    return out


def test_completeness_gate_lists_every_hole_and_refuses_adjusted_bars() -> None:
    need = 35
    full = _bars(need)
    short = _bars(4)

    def raw(ticker):
        return {"AAA": full, "BBB": short, "CCC": full}.get(ticker, [])

    def official(ticker, date, bars=None):
        if ticker == "CCC":
            return {"open": None, "high": None, "low": None, "close": None}
        return {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5}

    with mock.patch("src.price_store.ensure_through", return_value=None), \
            mock.patch.object(fmf, "_raw_bars", side_effect=raw), \
            mock.patch.object(tl, "_official_ohlc", side_effect=official):
        gaps = fmf.ensure_candidate_bars("2026-09-25", ["AAA", "BBB", "CCC"])
    by = {g["ticker"]: g["missing"] for g in gaps}
    assert "AAA" not in by
    assert any(item.startswith("indicator bars") for item in by["BBB"])
    assert "missing 09:30 open" in by["CCC"]
    assert "indicator bars" in next(g["reason"] for g in gaps if g["ticker"] == "BBB")
    from src import price_store as ps
    old = ps.AUTO_ADJUST
    ps.AUTO_ADJUST = True
    try:
        try:
            fmf.ensure_candidate_bars("2026-09-25", ["AAA"])
            adjusted = False
        except fmf.HoldDay as e:
            adjusted = True
            assert e.status == "held_incomplete"
            assert "adjusted" in e.reason
        assert adjusted
        try:
            fmf.make_snapshot("2026-09-25", [], None, None)
            snapped = False
        except fmf.HoldDay:
            snapped = True
        assert snapped
    finally:
        ps.AUTO_ADJUST = old


def test_one_gapped_name_is_dropped_and_the_rerun_matches() -> None:
    """One hole drops that name and the day locks. A later fill stays dropped."""
    names = ["AAA", "GAP"]
    gap = {
        "ticker": "GAP",
        "missing": ["missing 09:30 open", "indicator bars 0/35"],
        "reason": "missing 09:30 open; indicator bars 0/35",
    }
    ensures = {"n": 0}

    def fake_ensure(date, tickers):
        ensures["n"] += 1
        assert "GAP" in tickers
        return [dict(gap)]

    def fake_attach(date, ticker, sources, src_rank, sess, prev, prior, df):
        return {
            "date": date, "ticker": ticker, "sources": sources,
            "src_rank": src_rank, "open": 10.0, "close": 11.0,
            "ohlc_hot_score": 1.5,
        }

    def boom(*_a, **_k):
        raise RuntimeError("download down")

    with mock.patch("src.price_store.ensure_through", side_effect=boom):
        try:
            fmf.ensure_candidate_bars("2026-09-25", ["AAA", "BBB"])
            fetched = False
        except fmf.HoldDay as e:
            fetched = True
            assert e.status == "held_incomplete"
            assert "price fetch failed" in e.reason
    assert fetched

    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
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
                                      return_value={"tickers": names}), \
                    mock.patch.object(fmf, "ranking_universe", return_value=list(names)), \
                    mock.patch.object(fmf, "ensure_candidate_bars", side_effect=fake_ensure), \
                    mock.patch.object(fm, "_candidates",
                                      return_value={"flatten": list(names)}), \
                    mock.patch.object(fm, "_attach_row", side_effect=fake_attach), \
                    mock.patch.object(fmf, "row_price_problem", return_value=None), \
                    mock.patch.object(fmf, "heat_record", return_value={
                        "vintage": "2026-09-24", "phase": "morning_overlay",
                        "board_date": "2026-09-25", "source": None, "sha256": "abc",
                    }), mock.patch.object(fmf, "code_sha", return_value="cafebabe"):
                panel = fm.build_panel("2026-09-25", "2026-09-25", fail_closed=True)
                snap = fmf.make_snapshot(
                    "2026-09-25", panel["rows"], "2026-09-24", "pricesha",
                    dropped=panel["dropped"],
                )
                digest = fmf.write_snapshot("2026-09-25", snap, restate=False)
                locked = fmf.snapshot_path("2026-09-25").read_bytes()
                assert fmf.sha256_bytes(locked) == digest
                # Yahoo later fills GAP. The frozen list still drops it.
                panel2 = fm.build_panel("2026-09-25", "2026-09-25", fail_closed=True)
                snap2 = fmf.make_snapshot(
                    "2026-09-25", panel2["rows"], "2026-09-24", "pricesha",
                    dropped=panel2["dropped"],
                )
                try:
                    fmf.write_snapshot("2026-09-25", snap2, restate=False)
                    rewrote = True
                except fmf.FrozenHistory:
                    rewrote = False
        finally:
            _restore(old)
    assert ensures["n"] == 1
    assert [r["ticker"] for r in panel["rows"]] == ["AAA"]
    assert panel["rows"][0]["ohlc_hot_score"] == 1.5
    assert snap["n_dropped"] == 1
    assert snap["dropped"][0]["ticker"] == "GAP"
    assert "missing 09:30 open" in snap["dropped"][0]["reason"]
    assert snap["auto_adjust"] is False
    assert [r["ticker"] for r in panel2["rows"]] == ["AAA"]
    assert snap2["dropped"] == snap["dropped"]
    assert fmf.canonical_bytes(snap2) == locked
    assert not rewrote
    assert fmf.sha256_bytes(locked) == digest


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
        except fmf.SkipDay as e:
            refused = True
            assert "hot_score" in e.reason
            assert "no rankable" in e.reason
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
                    mock.patch.object(fmf, "prepare_lock", return_value={
                        "date": "2026-09-25",
                        "n": 1,
                        "prior_export": "2026-09-24",
                        "excluded": ["flatten", "mover_buy"],
                        "names": [{
                            "ticker": "NEW",
                            "sources": [{"source": "yday_gainer", "rank": 1}],
                        }],
                    }), \
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
            assert fmf.candidate_path("2026-09-25").is_file()
            frozen = json.loads(fmf.snapshot_path("2026-09-25").read_text(encoding="utf-8"))
            assert frozen["candidates"]["n"] == 1
            assert frozen["candidates"]["names"][0]["ticker"] == "NEW"
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
        old = _use(tmp)
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
            _restore(old)


def test_published_0922_pin_beats_the_postclose_file() -> None:
    """The 09-22 post-close land must not be the 09:30 digest input."""
    pin = tl._preopen_digest_path("2026-09-22")
    live_path = tl.NEWS_DIR / "2026-09-22_finviz_digest.json"
    assert pin.is_file()
    pinned = json.loads(pin.read_text(encoding="utf-8"))
    live = json.loads(live_path.read_text(encoding="utf-8"))
    assert tl._digest_is_preopen(pinned, "2026-09-22")
    assert not tl._digest_is_preopen(live, "2026-09-22")
    assert not tl._preopen_digest_path("2026-08-29").is_file()
    data, vintage = tl.load_digest_asof("2026-09-22")
    assert vintage == "2026-09-22"
    assert data.get("generated_at") == pinned.get("generated_at")


def test_late_digest_does_not_replace_preopen_tones() -> None:
    with tempfile.TemporaryDirectory() as d:
        news = Path(d)
        date = "2026-09-22"
        prior = "2026-09-21"
        morning = {
            "date": date,
            "generated_at": "2026-09-22T07:09:51-04:00",
            "top_signal": [{"ticker": "AAA", "digest": "AAA beats and raises guidance"}],
        }
        late = {
            "date": date,
            "generated_at": "2026-09-23T01:54:14-04:00",
            "top_signal": [{"ticker": "BBB", "digest": "BBB plunges after a miss"}],
        }
        (news / f"{date}_finviz_digest_preopen.json").write_text(
            json.dumps(morning), encoding="utf-8")
        (news / f"{date}_finviz_digest.json").write_text(
            json.dumps(late), encoding="utf-8")
        orig = tl.NEWS_DIR
        tl.NEWS_DIR = news
        try:
            data, vintage = tl.load_digest_asof(date)
            assert vintage == date
            assert data["top_signal"][0]["ticker"] == "AAA"
            tones = tl._digest_tones(date)
            assert "AAA" in tones
            assert "BBB" not in tones
            # No pin and a post-close stamp walks to the prior pre-open file.
            (news / f"{date}_finviz_digest_preopen.json").unlink()
            (news / f"{prior}_finviz_digest_preopen.json").write_text(
                json.dumps({
                    "date": prior,
                    "generated_at": "2026-09-21T06:53:43-04:00",
                    "top_signal": [{"ticker": "CCC", "digest": "CCC beats estimates"}],
                }),
                encoding="utf-8",
            )
            prior_sess = {"date": prior, "prior": None}
            cur = {"date": date, "prior": prior_sess}
            tl._INDEX = {"sessions": [prior_sess, cur]}
            data, vintage = tl.load_digest_asof(date)
            assert vintage == prior
            assert data["top_signal"][0]["ticker"] == "CCC"
        finally:
            tl.NEWS_DIR = orig
            tl._INDEX = None


def test_save_report_does_not_overwrite_preopen_pin() -> None:
    with tempfile.TemporaryDirectory() as d:
        from src import finviz_digest as fd
        orig = fd.NEWS_DIR
        fd.NEWS_DIR = Path(d)
        try:
            first = {
                "date": "2026-09-25",
                "generated_at": "2026-09-25T07:05:00-04:00",
                "export_used": "x",
                "ticker_digest_count": 1,
                "signal_count": 1,
                "index_digests": [],
                "top_signal": [{"ticker": "AAA", "digest": "beats"}],
                "by_sector": {},
                "all_ticker_digests": [],
            }
            fd.save_report(dict(first))
            pin = fd.preopen_digest_path("2026-09-25")
            frozen = pin.read_bytes()
            late = dict(first)
            late["generated_at"] = "2026-09-26T01:54:00-04:00"
            late["top_signal"] = [{"ticker": "ZZZ", "digest": "plunges"}]
            fd.save_report(late)
            assert pin.read_bytes() == frozen
            live = json.loads((Path(d) / "2026-09-25_finviz_digest.json").read_text())
            assert live["top_signal"][0]["ticker"] == "ZZZ"
        finally:
            fd.NEWS_DIR = orig


def test_price_store_keeps_the_first_bar() -> None:
    import pandas as pd
    from src import price_store as ps
    with tempfile.TemporaryDirectory() as d:
        orig = (ps.PRICE_DIR, ps.STORE_PATH, ps.META_PATH)
        root = Path(d)
        ps.PRICE_DIR = root
        ps.STORE_PATH = root / "ohlc.parquet"
        ps.META_PATH = root / "meta.json"
        try:
            first = pd.DataFrame([{
                "date": "2026-09-24", "ticker": "AAA",
                "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 100,
            }])
            ps._save_store(first)
            second = pd.DataFrame([{
                "date": "2026-09-24", "ticker": "AAA",
                "open": 99.0, "high": 99.0, "low": 99.0, "close": 99.0, "volume": 1,
            }, {
                "date": "2026-09-25", "ticker": "AAA",
                "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.5, "volume": 50,
            }])
            ps._save_store(pd.concat([ps._load_store(), second], ignore_index=True))
            stored = ps._load_store()
            old = stored[stored["date"] == pd.Timestamp("2026-09-24")].iloc[0]
            assert float(old["open"]) == 10.0
            assert (stored["date"] == pd.Timestamp("2026-09-25")).any()
        finally:
            ps.PRICE_DIR, ps.STORE_PATH, ps.META_PATH = orig


def test_holdup_created_on_is_the_first_session_after_the_commit() -> None:
    rec = fm.make_recipe(
        "union_hot_n4_holdup", s_boost="holdup", rank="hot_score", top_n=4)
    assert rec["created_on"] == "2026-09-21"
    white = fm.make_recipe("union_white_both_n4_h1")
    assert white["created_on"] == "2026-09-14"
    base = fm.make_recipe("union_h1")
    assert base["created_on"] == fm.START
    combo = fm.recipe_created_on("combo_oh_5050_shared", {
        "created_on": fm.START,
        "members": ["overnight_mega_h1", "union_hot_n4_holdup"],
    })
    assert combo == "2026-09-21"


def test_prune_does_not_use_full_window_stats() -> None:
    payload = {
        "from_date": "2026-08-13",
        "to_date": "2026-09-24",
        "dates": ["2026-09-20", "2026-09-21", "2026-09-24"],
        "freeze": {"first_frozen": None},
        "capital": 10000,
        "recipes": [
            fm.make_recipe("lucky_h1"),
            fm.make_recipe("union_h1"),
        ],
        "stats": [
            {"name": "lucky_h1", "win_rate": 0.9, "total_ret_pct": 80.0,
             "start_rate": 0.9, "profitable_day_rate": 0.9,
             "book_n_trades": 40, "audit_ok": True, "universe": "union"},
            {"name": "union_h1", "win_rate": 0.2, "total_ret_pct": -5.0,
             "start_rate": 0.2, "profitable_day_rate": 0.2,
             "book_n_trades": 40, "audit_ok": True, "universe": "union"},
        ],
    }
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            with mock.patch.object(fm, "_baked_recipe_names", return_value={"union_h1"}):
                out = fm.prune_payload_workable(payload)
        finally:
            _restore(old)
    names = {r["name"] for r in out["recipes"]}
    assert "union_h1" in names
    assert "lucky_h1" not in names
    assert "full-window" in (out.get("workable") or {}).get("note", "").lower() or \
        "Full-window" in (out.get("workable") or {}).get("note", "")


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


def _mini_panel(date: str = "2026-08-17"):
    row = {
        "date": date, "ticker": "AAA", "sources": ["union"],
        "boxes": {"vol": "good"}, "blue": False, "alarm": False,
        "zero_red": True, "last_green": True, "last_red": False,
        "ohlc_ret_5": 3.0, "ohlc_ret_1": 1.0, "ohlc_rvol": 1.0,
        "ohlc_hot_score": 1.0, "src_rank": 0, "cond_good": 1, "cond_bad": 0,
        "e_pol": "none", "rsi": 50, "open": 10.0, "close": 11.0,
    }
    rows = [row]
    return {
        "from_date": date,
        "to_date": date,
        "session_dates": [date],
        "rows": rows,
        "by_date": {date: rows},
        "n_rows": 1,
        "n_sessions": 1,
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }


def _emit_once(root: Path, date: str = "2026-08-17") -> dict[str, bytes]:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt

    panel = _mini_panel(date)
    date = panel["to_date"]
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    bars = {("AAA", date): {"open": 10.0, "close": 11.0}}
    old = _use(root)
    try:
        with mock.patch.object(fmf, "code_sha", return_value="determinism-test"), \
                mock.patch.object(fmf, "heat_record", return_value={
                    "vintage": date, "phase": "morning_overlay",
                    "board_date": date, "source": None, "sha256": "abc",
                }), \
                mock.patch.object(fm, "flatten_plan", return_value={
                    "route": "", "flatten_ok": False,
                }), \
                mock.patch.object(fmb, "load_regime", return_value={}):
            snap = fmf.make_snapshot(date, panel["rows"], None, "prices-sha")
            fmf.write_snapshot(date, snap, restate=False)
            ledger = fmf.build_ledger(
                panel, {"recipes": [rec]}, [rec], date, bars,
                fees=pt.load_fees(), regime={},
            )
            fmf.write_ledger(date, ledger, restate=False)
            payload = fmf.splice_payload({"recipes": [rec], "stats": []}, date, ledger)
            payload["to_date"] = date
            payload["from_date"] = date
            payload["dates"] = [date]
            payload = fmf.label_payload(payload)
            # Each session is an independent replay. A shared scoreboard
            # file would look like a locked day being dropped.
            dest = root / date / "factor_mine.json"
            fm.write_scoreboard(payload, dest)
        files = {
            f"{date}/snapshot": fmf.snapshot_path(date).read_bytes(),
            f"{date}/ledger": fmf.ledger_path(date).read_bytes(),
            f"{date}/scoreboard": dest.read_bytes(),
        }
        shard_dir = dest.parent / "factor_mine" / "shards"
        for path in sorted(shard_dir.glob("*.json.gz")):
            files[f"{date}/shard/{path.name}"] = path.read_bytes()
        return files
    finally:
        _restore(old)


def test_replay_twice_is_byte_identical() -> None:
    """Same commit and the same frozen inputs land the same bytes.

    Every retro session is replayed, not a single sample day.
    ``python -m src.test_factor_mine_freeze`` restarts under
    PYTHONHASHSEED=0. Recipe ranks break ties on ticker and use a
    stable sort.
    """
    from src.factor_mine_retro import SESSIONS

    if __name__ == "__main__":
        assert os.environ.get("PYTHONHASHSEED") == "0"

    def run(root: Path) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for date in SESSIONS:
            out.update(_emit_once(root, date))
        return out

    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        first = run(Path(a))
        second = run(Path(b))
    assert first.keys() == second.keys()
    assert len(SESSIONS) == 30
    for key in first:
        assert first[key] == second[key], key
    for date in SESSIONS:
        snap = json.loads(first[f"{date}/snapshot"])
        assert snap["date"] == date
        assert snap["code_sha"] == "determinism-test"
        assert snap["tape"] == "raw"
        assert snap["auto_adjust"] is False
        ledger = json.loads(gzip.decompress(first[f"{date}/ledger"]))
        assert ledger["code_sha"] == "determinism-test"


def test_corrupt_frozen_input_fails_the_hash_guard() -> None:
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            fmf.write_snapshot("2026-09-25", {
                "date": "2026-09-25",
                "rows": [{"ticker": "AAA", "open": 10.0}],
            }, restate=False)
            man = fmf.load_manifest()
            path = fmf.snapshot_path("2026-09-25")
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 0xFF
            path.write_bytes(bytes(raw))
            try:
                fmf.guard_manifest(man, man, restate=[])
                caught = False
            except SystemExit as e:
                caught = True
                assert "sha does not match" in str(e)
            assert caught
        finally:
            _restore(old)


def test_lineup_is_append_only_and_prune_keeps_it() -> None:
    early = fm.make_recipe("union_h1")
    later = fm.make_recipe("union_hot_n4_holdup")
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            with mock.patch.object(fmf, "code_sha", return_value="lineup-test"):
                first = fmf.record_lineup("2026-08-13", [early, later])
                second = fmf.record_lineup(
                    "2026-08-13", [early, later, fm.make_recipe("overnight_mega_h1")],
                )
            assert [r["name"] for r in first["recipes"]] == ["union_h1"]
            assert second == first
            assert first["recipes"][0]["created_on"] == "2026-08-13"
            payload = {
                "to_date": "2026-08-13",
                "dates": ["2026-08-13"],
                "freeze": {"first_frozen": "2026-08-13"},
                "recipes": [early, later],
                "stats": [
                    {"name": "union_h1", "win_rate": 0.1, "total_ret_pct": -1,
                     "start_rate": 0, "profitable_day_rate": 0,
                     "book_n_trades": 1, "audit_ok": True},
                    {"name": "union_hot_n4_holdup", "win_rate": 0.99,
                     "total_ret_pct": 50, "start_rate": 1,
                     "profitable_day_rate": 1, "book_n_trades": 40,
                     "audit_ok": True},
                ],
            }
            out = fm.prune_payload_workable(payload)
            assert [r["name"] for r in out["recipes"]] == ["union_h1"]
            assert "not re-chosen" in out["workable"]["note"]
        finally:
            _restore(old)


def test_recipe_creation_date_cannot_move() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "recipe_created_on.json"
        rec = fm.make_recipe("union_h1")
        fmf.record_recipe_dates([rec], path)
        moved = dict(rec)
        moved["created_on"] = "2026-09-21"
        try:
            fmf.record_recipe_dates([moved], path)
            refused = False
        except fmf.FrozenHistory:
            refused = True
        assert refused
        try:
            fmf.guard_recipe_catalog(
                {"union_h1": "2026-08-13"},
                {"union_h1": "2026-09-21", "overnight_h1": "2026-08-13"},
            )
            caught = False
        except SystemExit:
            caught = True
        assert caught
        fmf.guard_recipe_catalog(
            {"union_h1": "2026-08-13"},
            {"union_h1": "2026-08-13", "overnight_h1": "2026-08-13"},
        )


def _csv(path: Path, rows: list[dict]) -> None:
    cols = ["Ticker", "Open", "High", "Low", "Price", "Prev Close"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _with_exports(tmp: Path):
    """Point Finviz reads at ``tmp`` and drop the process-wide bar cache."""
    old = tl.EXPORT_DIR
    tl.EXPORT_DIR = tmp
    tl._FINVIZ_BARS.clear()
    return old


def _prints(book):
    def official(ticker, date, bars=None):
        row = book.get(ticker) or {}
        return {
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
        }
    return official


def test_open_close_cross_check_sources_and_tolerances() -> None:
    """Post-close Finviz Price, raw.csv Open when the tape accepts it.

    Close agrees inside max(0.5%, $0.02). Open agrees inside max(1%, $0.02).
    A raw Open tape does not call Stooq. A morning Price is not the close.
    A bare float from the snapshot reader is a close and no open.
    """
    assert fmf.PRICE_CHECK["close"] == {"pct": 0.005, "abs": 0.02}
    assert fmf.PRICE_CHECK["open"] == {"pct": 0.01, "abs": 0.02}
    day = "2026-09-25"
    book = {
        "AAA": {"open": 10.0, "close": 10.0},
        "BBB": {"open": 1.0, "close": 1.0},
        "CCC": {"open": 100.0, "close": 100.40},
        "DDD": {"open": 100.0, "close": 100.60},
        "FFF": {"open": 10.0, "close": 10.0},
    }
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        _csv(root / f"finviz_{day}.csv", [
            {"Ticker": "AAA", "Open": 10.02, "High": 11, "Low": 9, "Price": 10.02},
            {"Ticker": "BBB", "Open": 1.02, "High": 1.1, "Low": 0.9, "Price": 1.02},
            {"Ticker": "CCC", "Open": 100.0, "High": 101, "Low": 99, "Price": 100.00},
            {"Ticker": "DDD", "Open": 100.0, "High": 101, "Low": 99, "Price": 100.00},
            {"Ticker": "FFF", "Open": 10.15, "High": 11, "Low": 9, "Price": 10.00},
        ])
        (root / f"finviz_{day}.scraped_at").write_text(
            "2026-09-25T16:05:00-04:00\n", encoding="utf-8")
        old_export = _with_exports(root)
        old_paper = fmf.PAPER_OPEN_DIR
        fmf.PAPER_OPEN_DIR = root / "no-fills"
        try:
            calls: list[str] = []

            def stooq(ticker, date):
                calls.append(ticker)
                return {"open": None, "high": None, "low": None, "close": None}

            def radar(date):
                calls.append("radar")
                return {}

            def tape(date):
                calls.append("tape")
                return {
                    "source": "finviz_raw",
                    "commit_sha": "a" * 40,
                    "commit_time": "2026-09-25T20:05:00+00:00",
                    "scrape_ts": "2026-09-25T20:05:00+00:00",
                    "opens": {
                        "AAA": 10.02, "BBB": 1.02, "CCC": 100.0,
                        "DDD": 100.0, "FFF": 10.15,
                    },
                    "note": "latest raw.csv commit before the next session 09:30 ET",
                }

            with mock.patch.object(tl, "_official_ohlc", side_effect=_prints(book)), \
                    mock.patch.object(fmf, "stooq_bar", side_effect=stooq), \
                    mock.patch.object(fmf, "theme_radar_prices", side_effect=radar), \
                    mock.patch.object(fmf, "day_open_tape", side_effect=tape):
                assert fmf.export_is_postclose(day)
                assert fmf.finviz_export_has_open(day)
                assert fmf.session_cross_check(day, ["AAA", "BBB", "CCC"]) == []
                assert calls == ["tape"]
                close_gap = fmf.session_cross_check(day, ["DDD"])
                open_gap = fmf.session_cross_check(day, ["FFF"])
                missing = fmf.session_cross_check(day, ["EEE"])
            assert len(close_gap) == 1 and close_gap[0]["field"] == "close"
            assert close_gap[0]["source"] == f"finviz_{day}.csv Price"
            assert close_gap[0]["ref"] == 100.0
            assert len(open_gap) == 1 and open_gap[0]["field"] == "open"
            assert open_gap[0]["source"] == "finviz_raw"
            assert open_gap[0]["ref"] == 10.15
            assert {item for g in missing for item in g["missing"]} == {
                "missing session close", "missing 09:30 open reference",
            }
            # Morning scrape: Price is not the close. Theme-radar then Stooq.
            (root / f"finviz_{day}.scraped_at").write_text(
                "2026-09-25T08:00:00-04:00\n", encoding="utf-8")
            tl._FINVIZ_BARS.clear()
            assert not fmf.export_is_postclose(day)
            morning_tape = {
                "source": "stooq",
                "scrape_ts": "2026-09-25T12:00:00+00:00",
                "opens": {},
                "note": "raw.csv commit at or after the next session 09:30 ET",
            }
            with mock.patch.object(tl, "_official_ohlc", side_effect=_prints(book)), \
                    mock.patch.object(fmf, "theme_radar_prices", return_value={"AAA": 10.0}), \
                    mock.patch.object(fmf, "day_open_tape", return_value=morning_tape), \
                    mock.patch.object(fmf, "stooq_bar", return_value={
                        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
                    }):
                assert fmf.session_cross_check(day, ["AAA"]) == []
                with mock.patch.object(fmf, "theme_radar_prices", return_value={"AAA": 12.0}):
                    held = fmf.session_cross_check(day, ["AAA"])
            assert any(g.get("field") == "close" and "theme-radar" in g["source"] for g in held)
            assert all("finviz" not in str(g.get("source")) for g in held if g.get("field") == "close")
        finally:
            fmf.PAPER_OPEN_DIR = old_paper
            tl.EXPORT_DIR = old_export
            tl._FINVIZ_BARS.clear()


def test_cross_check_hold_writes_nothing() -> None:
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        old = _use(tmp)
        orig_panel = fm.PANEL_PATH
        try:
            fm.PANEL_PATH = tmp / "panel.json"

            def fake_build(date, to_date=None, fail_closed=False):
                row = {
                    "date": date, "ticker": "AAA", "sources": ["yday_gainer"],
                    "src_rank": 0, "open": 10.0, "close": 11.0,
                    "ohlc_hot_score": 1.0, "boxes": {}, "alarm": False,
                }
                return {
                    "session_dates": [date], "rows": [row],
                    "by_date": {date: [row]},
                }

            def disagree(date):
                raise fmf.HoldDay(
                    date, ["AAA"],
                    "finviz cross-check: AAA close ours=10.0 finviz=12.0",
                    status="held_review",
                    gaps=[{
                        "ticker": "AAA", "field": "close",
                        "ours": 10.0, "finviz": 12.0,
                        "source": "finviz_2026-09-25.csv Prev Close",
                    }],
                )

            with mock.patch.object(fm, "build_panel", side_effect=fake_build), \
                    mock.patch.object(fm, "panel_lookback_calendar",
                                      return_value=["2026-09-24", "2026-09-25"]), \
                    mock.patch.object(fm, "session_has_closed", return_value=True), \
                    mock.patch.object(fmf, "prepare_lock", side_effect=disagree):
                out = fmf.append_land(
                    "2026-09-24", "2026-09-25", write=True, payload={
                        "dates": [], "recipes": [], "stats": [],
                    }, panel={"session_dates": [], "rows": []},
                )
            assert not fmf.snapshot_path("2026-09-25").exists()
            assert not fmf.price_path("2026-09-25").exists()
            assert not fmf.candidate_path("2026-09-25").exists()
            assert "2026-09-25" not in (out.get("dates") or [])
        finally:
            fm.PANEL_PATH = orig_panel
            _restore(old)


def test_candidate_log_uses_morning_files_only() -> None:
    from src import flatten_lookback_action as fla
    from src import gainer_asof as ga
    from src import gainer_capture as gc
    from src import oppset_clock_b as opp

    def refuse(*_a, **_k):
        raise AssertionError("prior-night holdings are not a candidate source")

    def overnight(*_a, **kwargs):
        if kwargs.get("min_mcap_m"):
            return ["DDD"]
        return []

    with mock.patch.object(gc, "lookback_calendar",
                           side_effect=lambda c: ["2026-09-24", "2026-09-25"]), \
            mock.patch.object(gc, "knowable_export_date", return_value="2026-09-24"), \
            mock.patch.object(gc, "next_session", return_value="2026-09-28"), \
            mock.patch.object(ga, "load_finviz", return_value=object()), \
            mock.patch.object(ga, "_liquid_tape", return_value=[
                {"ticker": "BBB", "change_pct": 1.0},
                {"ticker": "AAA", "change_pct": 4.0},
                {"ticker": "AAA", "change_pct": 4.0},
            ]), \
            mock.patch.object(gc, "yesterday_gainers", return_value=["BBB", "AAA"]), \
            mock.patch.object(gc, "yesterday_movers", return_value=[]), \
            mock.patch.object(gc, "earnings_reaction", return_value=["CCC"]), \
            mock.patch.object(gc, "overnight_scheduled", side_effect=overnight), \
            mock.patch.object(opp, "union_enabled", return_value=True), \
            mock.patch.object(opp, "flagged_tickers", return_value=["CCC", "AAA"]), \
            mock.patch.object(fla, "flatten_day_targets", side_effect=refuse), \
            mock.patch.object(fla, "collect_mover_buys", side_effect=refuse), \
            mock.patch.object(fmf, "code_sha", return_value="prov-test"):
        doc = fmf.candidate_provenance("2026-09-25")
    assert doc["n"] == 4
    assert doc["prior_export"] == "2026-09-24"
    assert doc["excluded"] == ["flatten", "mover_buy"]
    by = {row["ticker"]: row["sources"] for row in doc["names"]}
    assert [row["ticker"] for row in doc["names"]] == ["AAA", "BBB", "CCC", "DDD"]
    assert {"source": "liquid_tape", "rank": 1} in by["AAA"]
    assert {"source": "yday_gainer", "rank": 2} in by["AAA"]
    assert {"source": "oppset", "rank": 2} in by["AAA"]
    assert {"source": "liquid_tape", "rank": 2} in by["BBB"]
    assert {"source": "earn_react", "rank": 1} in by["CCC"]
    assert {"source": "overnight_mega", "rank": 1} in by["DDD"]
    blob = json.dumps(doc)
    assert "flatten" not in blob or "excluded" in blob
    assert "mover_buy" not in blob or "excluded" in blob
    for row in doc["names"]:
        assert all(s["source"] != "flatten" for s in row["sources"])
        assert all(s["source"] != "mover_buy" for s in row["sources"])
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            digest = fmf.write_candidates("2026-09-25", doc, restate=False)
            man = fmf.load_manifest()
            assert man["candidates"]["2026-09-25"]["sha256"] == digest
            fmf.guard_manifest(man, man, restate=[])
            path = fmf.candidate_path("2026-09-25")
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 0xFF
            path.write_bytes(bytes(raw))
            try:
                fmf.guard_manifest(man, man, restate=[])
                caught = False
            except SystemExit as e:
                caught = True
                assert "candidates" in str(e)
            assert caught
        finally:
            _restore(old)


def test_stooq_fills_open_when_the_export_has_no_open_column() -> None:
    day = "2026-09-25"
    text = (
        "Date,Open,High,Low,Close,Volume\n"
        "20260925,10.10,11,9,10.00,100\n"
    )
    assert fmf.parse_stooq_bar(text, day)["open"] == 10.10
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        path = root / f"finviz_{day}.csv"
        path.write_text(
            "Ticker,Price,Prev Close\nAAA,10.00,9\n", encoding="utf-8")
        (root / f"finviz_{day}.scraped_at").write_text(
            "2026-09-25T16:30:00-04:00\n", encoding="utf-8")
        old_export = _with_exports(root)
        old_paper = fmf.PAPER_OPEN_DIR
        fmf.PAPER_OPEN_DIR = root / "empty"
        try:
            assert fmf.export_is_postclose(day)
            assert not fmf.finviz_export_has_open(day)
            with mock.patch.object(tl, "_official_ohlc", side_effect=_prints({
                "AAA": {"open": 10.10, "close": 10.0},
            })), mock.patch.object(fmf, "stooq_bar", return_value={
                "open": 10.10, "high": 11.0, "low": 9.0, "close": 10.0,
            }), mock.patch.object(fmf, "day_open_tape", return_value={
                "source": "stooq", "scrape_ts": None, "opens": {},
                "commit_sha": "", "commit_time": "",
                "note": "no raw export",
            }):
                gaps = fmf.session_cross_check(day, ["AAA"])
            assert gaps == []
        finally:
            fmf.PAPER_OPEN_DIR = old_paper
            tl.EXPORT_DIR = old_export
            tl._FINVIZ_BARS.clear()


def test_theme_radar_dated_file_rejects_current_and_a_bad_hash() -> None:
    day = "2026-09-25"
    body = "Ticker,Price,scrape_ts\nAAA,10.50,2026-09-25 16:30:00\n"
    parsed = fmf.parse_theme_radar_prices(body)
    assert parsed == {"AAA": {"close": 10.50, "open": None}}
    opened = "Ticker,Price,scrape_ts,Open\nAAA,10.50,2026-09-25 16:30:00,10.25\n"
    assert fmf.parse_theme_radar_prices(opened) == {
        "AAA": {"close": 10.50, "open": 10.25},
    }
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "current.csv").write_text(
            "Ticker,Price\nAAA,999\n", encoding="utf-8")
        (root / f"{day}.csv").write_text(body, encoding="utf-8")
        old = fmf.THEME_RADAR_SNAP_DIR
        fmf.THEME_RADAR_SNAP_DIR = root
        try:
            assert fmf._dated_snapshot_name("current") is None
            assert fmf.theme_radar_prices(day) == {"AAA": {"close": 10.50, "open": None}}
            digest = fmf.sha256_bytes(body.encode("utf-8"))
            with mock.patch.object(fmf, "_theme_radar_expected_hash", return_value="0" * 64):
                assert fmf.theme_radar_prices(day) == {}
            with mock.patch.object(fmf, "_theme_radar_expected_hash", return_value=digest):
                assert fmf.theme_radar_prices(day) == {"AAA": {"close": 10.50, "open": None}}
            (root / f"{day}.csv").unlink()
            assert fmf.theme_radar_prices(day) == {}
        finally:
            fmf.THEME_RADAR_SNAP_DIR = old


def test_webull_fill_outside_open_tolerance_holds() -> None:
    day = "2026-09-25"
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        _csv(root / f"finviz_{day}.csv", [{
            "Ticker": "AAA", "Open": 10.0, "High": 11, "Low": 9, "Price": 10.5,
        }])
        (root / f"finviz_{day}.scraped_at").write_text(
            "2026-09-25T20:10:00-04:00\n", encoding="utf-8")
        (root / f"{day}_status.json").write_text(json.dumps({
            "date": day,
            "sent": [
                {"ticker": "AAA", "side": "BUY", "status": "filled",
                 "broker_status": "FILLED", "avg_fill_px": 10.05, "filled_qty": 10},
                {"ticker": "BBB", "side": "BUY", "status": "working",
                 "broker_status": "SUBMITTED", "avg_fill_px": None, "filled_qty": 0},
            ],
            "card": {"tickets": [{"ticker": "AAA", "status": "plan", "px": 50}]},
        }), encoding="utf-8")
        old_export = _with_exports(root)
        old_paper = fmf.PAPER_OPEN_DIR
        old_log = fmf.OPEN_SOURCE_LOG
        fmf.PAPER_OPEN_DIR = root
        fmf.OPEN_SOURCE_LOG = root / "open_source_log.csv"
        raw_tape = {
            "source": "finviz_raw",
            "commit_sha": "b" * 40,
            "commit_time": "2026-09-25T20:10:00+00:00",
            "scrape_ts": "2026-09-25T20:10:00+00:00",
            "opens": {"AAA": 10.0},
            "note": "latest raw.csv commit before the next session 09:30 ET",
        }
        try:
            fills = fmf.paper_fills(day)
            assert fills == {"AAA": [10.05]}
            with mock.patch.object(tl, "_official_ohlc", side_effect=_prints({
                "AAA": {"open": 10.0, "close": 10.5},
            })), mock.patch.object(fmf, "stooq_bar", side_effect=AssertionError), \
                    mock.patch.object(fmf, "day_open_tape", return_value=raw_tape):
                assert fmf.session_cross_check(day, ["AAA"]) == []
            (root / f"{day}_status.json").write_text(json.dumps({
                "sent": [{
                    "ticker": "AAA", "side": "BUY", "status": "filled",
                    "avg_fill_px": 10.20, "filled_qty": 10,
                }],
            }), encoding="utf-8")
            with mock.patch.object(tl, "_official_ohlc", side_effect=_prints({
                "AAA": {"open": 10.0, "close": 10.5},
            })), mock.patch.object(fmf, "candidate_provenance", return_value={
                "date": day, "n": 1, "names": [{"ticker": "AAA", "sources": []}],
                "excluded": ["flatten", "mover_buy"],
            }), mock.patch.object(fmf, "stooq_bar", side_effect=AssertionError), \
                    mock.patch.object(fmf, "day_open_tape", return_value=raw_tape):
                try:
                    fmf.prepare_lock(day)
                    held = False
                    err = None
                except fmf.HoldDay as e:
                    held = True
                    err = e
            assert held and err is not None
            assert err.status == "held_review"
            assert err.gaps[0]["field"] == "fill"
            assert err.gaps[0]["ref"] == 10.20
            assert "webull paper" in err.gaps[0]["source"]
            assert not fmf.snapshot_path(day).exists()
        finally:
            fmf.PAPER_OPEN_DIR = old_paper
            fmf.OPEN_SOURCE_LOG = old_log
            tl.EXPORT_DIR = old_export
            tl._FINVIZ_BARS.clear()


def test_raw_open_uses_latest_commit_before_the_next_open() -> None:
    """raw.csv Open follows the latest theme-radar commit, not a lower bound.

    Accept the file when that commit is before the next session's 09:30 ET.
    A scrape_ts, once the column exists, must clear the same upper bound.
    A missing stamp does not reject the day. Labor Day 2026-09-07 makes
    the next session after 09-04 Tuesday 09-08. 2026-08-27 has no raw
    export, so it is Stooq. From 09-25 a slim Open is finviz_snapshot.
    A bad raw hash is not used. current.csv is not read.
    """
    assert fmf.before_next_open("2026-09-24T13:30:00+00:00", "2026-09-24") is True
    assert fmf.before_next_open("2026-09-24T12:00:00+00:00", "2026-09-24") is True
    assert fmf.before_next_open("2026-09-25T13:29:00+00:00", "2026-09-24") is True
    assert fmf.before_next_open("2026-09-25T13:30:00+00:00", "2026-09-24") is False
    assert fmf.before_next_open("2026-09-08T13:29:00+00:00", "2026-09-04") is True
    assert fmf.before_next_open("2026-09-08T13:30:00+00:00", "2026-09-04") is False
    assert fmf.before_next_open(None, "2026-09-24") is False
    early = "2026-09-24"
    late = "2026-09-25"
    missing_day = "2026-08-27"
    raw_sha = "a" * 40
    slim_sha = "b" * 40
    state = {
        "raw": {"sha": raw_sha, "committed_at": "2026-09-25T14:00:00+00:00"},
        "slim": {"sha": slim_sha, "committed_at": "2026-09-25T20:00:00+00:00"},
    }

    def commits(path):
        name = str(path)
        if name.endswith(f"{missing_day}.raw.csv") or "current" in name:
            return None
        if name.endswith(f"{early}.raw.csv"):
            return state["raw"]
        if name.endswith(f"{late}.raw.csv"):
            return None
        if name.endswith(f"{late}.csv"):
            return state["slim"]
        return None

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "current.csv").write_text(
            "Ticker,Price,Open\nAAA,1,1\n", encoding="utf-8")
        (root / "current.raw.csv").write_text(
            "Ticker,Open\nAAA,1\n", encoding="utf-8")
        (root / f"{early}.csv").write_text(
            "Ticker,Price,scrape_ts\nAAA,10.0,2026-09-24T20:00:00+00:00\n",
            encoding="utf-8")
        (root / f"{early}.raw.csv").write_text(
            "Ticker,Open\nAAA,10.0\n", encoding="utf-8")
        (root / f"{late}.csv").write_text(
            "Ticker,Price,scrape_ts,Open\nAAA,10.0,2026-09-25T20:00:00+00:00,10.25\n",
            encoding="utf-8")
        old = fmf.THEME_RADAR_SNAP_DIR
        fmf.THEME_RADAR_SNAP_DIR = root
        try:
            assert fmf._dated_raw_name("current") is None
            assert fmf.theme_radar_latest_commit(
                "data/snapshots/current.raw.csv") is None
            with mock.patch.object(fmf, "theme_radar_latest_commit", side_effect=commits):
                late_commit = fmf.day_open_tape(early)
                assert late_commit["source"] == "stooq"
                assert late_commit["commit_sha"] == raw_sha
                assert "next session" in late_commit["note"]
                assert late_commit["opens"] == {}
                state["raw"] = {
                    "sha": raw_sha, "committed_at": "2026-09-24T13:30:00+00:00",
                }
                (root / f"{early}.csv").write_text(
                    "Ticker,Price\nAAA,10.0\n", encoding="utf-8")
                no_stamp = fmf.day_open_tape(early)
                assert no_stamp["source"] == "finviz_raw"
                assert no_stamp["opens"]["AAA"] == 10.0
                assert no_stamp["scrape_ts"] is None
                assert no_stamp["commit_time"] == "2026-09-24T13:30:00+00:00"
                (root / f"{early}.raw.csv").write_text(
                    "Ticker,Open,scrape_ts\nAAA,10.0,2026-09-25T14:00:00+00:00\n",
                    encoding="utf-8")
                ignored = fmf.day_open_tape(early)
                assert ignored["source"] == "finviz_raw"
                assert ignored["scrape_ts"] is None
                (root / "manifest.json").write_text(json.dumps({
                    "runs": [{
                        "date": early,
                        "scrape_ts_utc": "2026-09-24T20:00:00+00:00",
                    }],
                }), encoding="utf-8")
                from_manifest = fmf.day_open_tape(early)
                assert from_manifest["source"] == "finviz_raw"
                assert from_manifest["scrape_ts"] == "2026-09-24T20:00:00+00:00"
                (root / "manifest.json").write_text(json.dumps({
                    "runs": [{
                        "date": early,
                        "scrape_ts_utc": "2026-09-25T14:00:00+00:00",
                    }],
                }), encoding="utf-8")
                late_manifest = fmf.day_open_tape(early)
                assert late_manifest["source"] == "stooq"
                assert late_manifest["scrape_ts"] == "2026-09-25T14:00:00+00:00"
                assert "scrape_ts" in late_manifest["note"]
                absent = fmf.day_open_tape(missing_day)
                assert absent["source"] == "stooq"
                assert absent["note"] == "no raw export"
                assert absent["commit_sha"] == ""
                state["raw"] = {
                    "sha": raw_sha, "committed_at": "2026-09-24T20:00:00+00:00",
                }
                (root / f"{early}.csv").write_text(
                    "Ticker,Price,scrape_ts\nAAA,10.0,2026-09-25T14:00:00+00:00\n",
                    encoding="utf-8")
                late_scrape = fmf.day_open_tape(early)
                assert late_scrape["source"] == "stooq"
                assert "scrape_ts" in late_scrape["note"]
                assert late_scrape["commit_sha"] == raw_sha
                (root / f"{early}.csv").write_text(
                    "Ticker,Price,scrape_ts\nAAA,10.0,2026-09-24T20:00:00+00:00\n",
                    encoding="utf-8")
                accepted = fmf.day_open_tape(early)
                assert accepted["source"] == "finviz_raw"
                assert accepted["opens"]["AAA"] == 10.0
                assert accepted["commit_sha"] == raw_sha
                assert accepted["scrape_ts"] == "2026-09-24T20:00:00+00:00"
                fmf.write_open_source_row(early, accepted, path=root / "log.csv")
                logged = (root / "log.csv").read_text(encoding="utf-8").splitlines()
                assert logged[0] == "date,source,commit_sha,commit_time,scrape_ts,note"
                assert logged[1].startswith(f"{early},finviz_raw,{raw_sha},")
                digest = fmf.sha256_bytes((root / f"{early}.raw.csv").read_bytes())
                with mock.patch.object(
                    fmf, "_theme_radar_raw_expected_hash", return_value="0" * 64,
                ):
                    rejected = fmf.day_open_tape(early)
                assert rejected["source"] == "stooq"
                assert "hash" in rejected["note"]
                with mock.patch.object(
                    fmf, "_theme_radar_raw_expected_hash", return_value=digest,
                ):
                    assert fmf.theme_radar_raw_opens(early)["AAA"] == 10.0
                (root / f"{early}.raw.csv").unlink()
                slim = fmf.day_open_tape(late)
                assert slim["source"] == "finviz_snapshot"
                assert slim["opens"]["AAA"] == 10.25
                assert slim["commit_sha"] == slim_sha
        finally:
            fmf.THEME_RADAR_SNAP_DIR = old


def test_excel_signal_pin_is_the_last_commit_before_the_open() -> None:
    from datetime import datetime

    sug = "excel_bot/suggestions/suggestions.csv"
    day = "excel_bot/daily/2026-09-23_excel_bot.md"
    other = "excel_bot/strategies/L1/card.json"
    before = datetime.fromisoformat("2026-09-24T08:00:00-04:00")
    at_open = datetime.fromisoformat("2026-09-24T09:30:00-04:00")
    after = datetime.fromisoformat("2026-09-24T11:00:00-04:00")
    earlier = datetime.fromisoformat("2026-09-23T17:28:00-04:00")
    history = {
        sug: [
            (earlier, "a" * 40, earlier.isoformat()),
            (before, "b" * 40, before.isoformat()),
            (at_open, "c" * 40, at_open.isoformat()),
            (after, "d" * 40, after.isoformat()),
        ],
        day: [
            (after, "e" * 40, after.isoformat()),
        ],
        other: [
            (earlier, "f" * 40, earlier.isoformat()),
        ],
    }
    assert fmf.is_excel_signal_path(sug)
    assert fmf.is_excel_signal_path(day)
    assert not fmf.is_excel_signal_path(other)
    pinned = fmf.pin_excel_signals(["2026-09-24"], history)
    files = pinned["2026-09-24"]["files"]
    assert files[sug]["sha"] == "b" * 40
    assert day not in files
    assert other not in files
    assert pinned["2026-09-24"]["cutoff"].startswith("2026-09-24T09:30:00")
    with tempfile.TemporaryDirectory() as tmp:
        old = _use(Path(tmp))
        try:
            snap = {"date": "2026-09-24", "rows": []}
            digest = fmf.write_snapshot("2026-09-24", snap)
            first = fmf.record_excel_signals(
                ["2026-09-24"], history,
                now=datetime.fromisoformat("2026-09-24T12:00:00-04:00"),
            )
            assert first["2026-09-24"]["files"][sug]["sha"] == "b" * 40
            again = fmf.record_excel_signals(
                ["2026-09-24"], history,
                now=datetime.fromisoformat("2026-09-24T12:00:00-04:00"),
            )
            assert again == first
            man = fmf.load_manifest()
            assert man["snapshots"]["2026-09-24"]["sha256"] == digest
            moved = {
                sug: [(before, "9" * 40, before.isoformat())],
            }
            try:
                fmf.record_excel_signals(
                    ["2026-09-24"], moved,
                    now=datetime.fromisoformat("2026-09-24T12:00:00-04:00"),
                )
                raised = False
            except fmf.FrozenHistory:
                raised = True
            assert raised
            assert (
                fmf.load_manifest()["excel_signals"]["2026-09-24"]["files"][sug]["sha"]
                == "b" * 40
            )
        finally:
            _restore(old)


def test_finviz_stooq_disagreement_does_not_hold_the_day() -> None:
    """Finviz/Stooq disagreement is a warning. The lock is not refused."""
    fmf.LAST_OPEN_SOURCE.clear()
    gap = {
        "ticker": "AAA", "field": "close",
        "ours": 10.0, "ref": 12.0,
        "source": "finviz_2026-09-25.csv Price",
    }
    with mock.patch.object(fmf, "candidate_provenance", return_value={
                "date": "2026-09-25", "n": 1,
                "names": [{"ticker": "AAA", "sources": []}],
                "excluded": ["flatten", "mover_buy"],
            }), mock.patch.object(fmf, "session_cross_check", return_value=[gap]):
        doc = fmf.prepare_lock("2026-09-25")
    assert doc["names"][0]["ticker"] == "AAA"


def _aaa_session_bars() -> list[dict]:
    bars = _bars(40)
    bars.append({
        "date": "2026-09-25", "open": 10.0, "high": 11.0, "low": 9.0,
        "close": 10.5, "volume": 100.0,
    })
    return bars


def test_missing_yahoo_bars_drop_and_the_day_locks() -> None:
    """HYAC-U and TBCVU have no Yahoo bar. They are dropped, the day locks,
    a second run matches, days through 09-24 stay put, and a future file
    does not change the lock.
    """
    from src import factor_mine_sequential as seq

    day = "2026-09-25"
    history = ["2026-09-22", "2026-09-23", "2026-09-24"]
    future = "2026-09-28"
    dropped = ["HYAC-U", "TBCVU"]
    candidates = {
        "date": day,
        "n": 1,
        "prior_export": "2026-09-24",
        "excluded": ["flatten", "mover_buy"],
        "names": [{
            "ticker": "AAA",
            "sources": [{"source": "yday_gainer", "rank": 1}],
        }],
    }
    pinned = {
        "date": day,
        "tape": "raw",
        "auto_adjust": False,
        "names": {"AAA": {"prior": [], "open": 10.0, "close": 10.5}},
    }
    ledger = {
        "date": day,
        "origin": "frozen",
        "code_sha": "drop-bars",
        "recipes": {"union_h1": {"primary": {
            "buys": [], "sells": [], "skips": [], "trades": [],
            "daily": {
                "date": day, "cash": 10000.0, "equity": 10000.0,
                "yday_equity": 10000.0, "bought": [], "sold": [],
                "open_cash": 10000.0, "made_money": False,
            },
            "state": {"cash": 10000.0, "pos": {}, "after": day},
        }, "starts": {}}},
    }
    fetches = {"n": 0}
    seen = {}

    def raw(ticker):
        if str(ticker).upper() == "AAA":
            return _aaa_session_bars()
        return []

    def official(ticker, date, bars=None):
        if str(ticker).upper() == "AAA" and str(date)[:10] == day:
            return {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5}
        return {"open": None, "high": None, "low": None, "close": None}

    def fake_ensure(*_a, **_k):
        fetches["n"] += 1
        return None

    def fake_attach(date, ticker, sources, src_rank, sess, prev, prior, df):
        assert ticker == "AAA"
        return {
            "date": date, "ticker": ticker, "sources": list(sources),
            "src_rank": src_rank, "open": 10.0, "close": 10.5,
            "ohlc_hot_score": 1.5, "boxes": {}, "alarm": False,
        }

    def fake_ledger(panel, payload, recipes, date, bars, fees=None, regime=None):
        seen["dates"] = list(panel.get("session_dates") or [])
        seen["tickers"] = sorted({
            r.get("ticker") for r in (panel.get("rows") or []) if r.get("ticker")
        })
        return json.loads(json.dumps(ledger))

    def seed(tmp: Path, *, plant_future: bool) -> dict:
        state = tmp / "state"
        for recipe in (seq.HOT4_RECIPE, "union_hot_n4_holdup"):
            for date in history:
                seq.write_state(recipe, date, {
                    "date": date, "cash": 10000.0, "recipe": recipe,
                }, state)
        for date in history:
            fmf.write_snapshot(date, {
                "date": date,
                "rows": [{
                    "date": date, "ticker": "KEEP", "open": 5.0,
                    "ohlc_hot_score": 1.0,
                }],
                "dropped": [],
                "dropped_missing_bars": [],
            }, restate=False)
            fmf.write_ledger(date, {
                "date": date, "origin": "frozen", "recipes": {},
            }, restate=False)
        if plant_future:
            (fmf.SNAP_DIR / f"{future}.json").write_text(json.dumps({
                "date": future,
                "rows": [{"date": future, "ticker": "FUTUREONLY", "open": 1.0}],
                "dropped_missing_bars": ["FUTUREONLY"],
            }), encoding="utf-8")
            (fmf.PRICE_DIR / f"{future}.json").write_text(
                '{"date":"future"}\n', encoding="utf-8")
            for recipe in (seq.HOT4_RECIPE, "union_hot_n4_holdup"):
                seq.write_state(recipe, future, {
                    "date": future, "cash": 1.0, "recipe": recipe,
                }, state)
        files = {}
        for date in history:
            files[f"snap:{date}"] = fmf.snapshot_path(date).read_bytes()
            files[f"ledger:{date}"] = fmf.ledger_path(date).read_bytes()
        for recipe in (seq.HOT4_RECIPE, "union_hot_n4_holdup"):
            for date in history + ([future] if plant_future else []):
                files[f"state:{recipe}:{date}"] = (
                    seq.state_path(recipe, date, state).read_bytes()
                )
        if plant_future:
            files["future-snap"] = (fmf.SNAP_DIR / f"{future}.json").read_bytes()
            files["future-price"] = (fmf.PRICE_DIR / f"{future}.json").read_bytes()
        man = fmf.load_manifest()
        entries = {
            "snapshots": {
                date: json.loads(json.dumps(man["snapshots"][date]))
                for date in history
            },
            "ledgers": {
                date: json.loads(json.dumps(man["ledgers"][date]))
                for date in history
            },
        }
        return {"files": files, "manifest": entries, "state": state}

    def run(tmp: Path, *, plant_future: bool) -> dict:
        old = _use(tmp)
        old_panel = fm.PANEL_PATH
        old_state = seq.STATE_DIR
        seq.STATE_DIR = tmp / "state"
        fm.PANEL_PATH = tmp / "panel.json"
        try:
            frozen = seed(tmp, plant_future=plant_future)
            payload = {
                "dates": list(history),
                "to_date": history[-1],
                "recipes": [fm.make_recipe("union_h1", hold=1, top_n=1)],
                "stats": [],
                "capital": 10000,
                "mornings": {day: {"s": None, "freeze": "appended"}},
            }
            panel = {
                "from_date": history[0],
                "to_date": history[-1],
                "session_dates": list(history),
                "rows": [{
                    "date": date, "ticker": "KEEP", "open": 5.0, "src_rank": 0,
                } for date in history],
            }
            patches = [
                mock.patch("src.price_store.ensure_through", side_effect=fake_ensure),
                mock.patch.object(fmf, "_raw_bars", side_effect=raw),
                mock.patch.object(tl, "_official_ohlc", side_effect=official),
                mock.patch.object(fm, "live_panel_end", return_value=day),
                mock.patch.object(fm.sm, "load_payload", return_value={}),
                mock.patch.object(fm.sm, "list_books", return_value=[]),
                mock.patch.object(fm.sm, "session_calendar",
                                  return_value=history + [day]),
                mock.patch.object(fm.gc, "lookback_calendar",
                                  side_effect=lambda c: list(c)),
                mock.patch.object(fm, "_session_map",
                                  return_value=({day: {"date": day}}, [])),
                mock.patch.object(fm.fla, "collect_mover_buys",
                                  return_value={"by_date": {}}),
                mock.patch.object(fm.fla, "flatten_day_targets",
                                  return_value={"tickers": ["AAA", *dropped]}),
                mock.patch.object(fmf, "ranking_universe",
                                  return_value=["AAA", *dropped]),
                mock.patch.object(fm, "_candidates",
                                  return_value={"flatten": ["AAA", *dropped]}),
                mock.patch.object(fm, "_attach_row", side_effect=fake_attach),
                mock.patch.object(fmf, "row_price_problem", return_value=None),
                mock.patch.object(fm, "panel_lookback_calendar",
                                  return_value=history + [day]),
                mock.patch.object(fm, "session_has_closed", return_value=True),
                mock.patch.object(fm, "write_outputs"),
                mock.patch("src.factor_mine_rules.lock_recipe_rules"),
                mock.patch.object(fmf, "prepare_lock", return_value=candidates),
                mock.patch.object(fmf, "pin_prices", return_value=pinned),
                mock.patch.object(fmf, "heat_record", return_value={
                    "vintage": "2026-09-24", "phase": "morning_overlay",
                    "board_date": day, "source": None, "sha256": "abc",
                }),
                mock.patch.object(fmf, "code_sha", return_value="drop-bars"),
                mock.patch.object(fmf, "build_ledger", side_effect=fake_ledger),
            ]
            stack = ExitStack()
            for patch in patches:
                stack.enter_context(patch)
            with stack:
                fmf.append_land(
                    "2026-08-13", day, write=True,
                    recipes=payload["recipes"], payload=payload, panel=panel,
                )
                locked = fmf.snapshot_path(day).read_bytes()
                digest = fmf.sha256_bytes(locked)
                ledger_bytes = fmf.ledger_path(day).read_bytes()
                panel2 = fm.build_panel(day, day, fail_closed=True)
                snap_doc = json.loads(locked)
                snap2 = fmf.make_snapshot(
                    day, panel2["rows"], history[-1], snap_doc["prices_sha256"],
                    candidates=candidates, dropped=panel2["dropped"],
                )
                assert fmf.canonical_bytes(snap2) == locked
                try:
                    fmf.write_snapshot(day, snap2, restate=False)
                    rewrote = True
                except fmf.FrozenHistory:
                    rewrote = False
                fmf.append_land(
                    "2026-08-13", day, write=True,
                    recipes=payload["recipes"], payload=dict(payload),
                    panel=json.loads(json.dumps(panel)),
                )
            man = fmf.load_manifest()
            for date in history:
                assert man["snapshots"][date] == frozen["manifest"]["snapshots"][date]
                assert man["ledgers"][date] == frozen["manifest"]["ledgers"][date]
                assert fmf.snapshot_path(date).read_bytes() == frozen["files"][f"snap:{date}"]
                assert fmf.ledger_path(date).read_bytes() == frozen["files"][f"ledger:{date}"]
            for key, blob in frozen["files"].items():
                if key.startswith("state:"):
                    _, recipe, date = key.split(":", 2)
                    assert seq.state_path(recipe, date, frozen["state"]).read_bytes() == blob
                elif key == "future-snap":
                    assert (fmf.SNAP_DIR / f"{future}.json").read_bytes() == blob
                elif key == "future-price":
                    assert (fmf.PRICE_DIR / f"{future}.json").read_bytes() == blob
            assert fmf.snapshot_path(day).read_bytes() == locked
            assert fmf.sha256_bytes(fmf.snapshot_path(day).read_bytes()) == digest
            assert man["snapshots"][day]["sha256"] == digest
            assert man["snapshots"][day]["dropped_missing_bars"] == dropped
            assert man["ledgers"][day]["sha256"] == fmf.sha256_bytes(ledger_bytes)
            return {
                "snap": locked,
                "digest": digest,
                "ledger": ledger_bytes,
                "seen": json.loads(json.dumps(seen)),
                "rewrote": rewrote,
                "snap_doc": snap_doc,
            }
        finally:
            fm.PANEL_PATH = old_panel
            seq.STATE_DIR = old_state
            _restore(old)

    fetches["n"] = 0
    seen.clear()
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        clean = run(Path(a), plant_future=False)
        planted = run(Path(b), plant_future=True)
    assert fetches["n"] == 2
    assert not clean["rewrote"]
    assert clean["digest"] == planted["digest"]
    assert clean["snap"] == planted["snap"]
    assert clean["ledger"] == planted["ledger"]
    assert clean["seen"]["tickers"] == planted["seen"]["tickers"]
    assert clean["seen"]["dates"] == planted["seen"]["dates"]
    assert future not in clean["seen"]["dates"]
    assert "FUTUREONLY" not in planted["seen"]["tickers"]
    assert dropped[0] not in planted["seen"]["tickers"]
    assert dropped[1] not in planted["seen"]["tickers"]
    doc = clean["snap_doc"]
    assert doc["dropped_missing_bars"] == dropped
    assert [row["ticker"] for row in doc["rows"]] == ["AAA"]
    assert doc["rows"][0]["ohlc_hot_score"] == 1.5
    assert [gap["ticker"] for gap in doc["dropped"]
            if fmf.MISSING_YAHOO_BARS in (gap.get("missing") or [])] == dropped
    assert "KEEP" in clean["seen"]["tickers"]
    assert "AAA" in clean["seen"]["tickers"]


if __name__ == "__main__":
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable, "-m", "src.test_factor_mine_freeze"])
    test_snapshot_is_write_once_and_restate_logs_previous_hash()
    test_guard_fails_when_an_earlier_hash_changes()
    test_missing_bars_hold_the_day_and_do_not_write_hot_zero()
    test_finviz_stooq_disagreement_does_not_hold_the_day()
    test_missing_yahoo_bars_drop_and_the_day_locks()
    test_completeness_gate_lists_every_hole_and_refuses_adjusted_bars()
    test_one_gapped_name_is_dropped_and_the_rerun_matches()
    test_build_panel_fetches_bars_before_candidates()
    test_build_panel_refuses_unresolved_hot_score()
    test_morning_map_heat_is_not_replaced_by_postclose()
    test_resume_appends_one_day_and_keeps_prior_trades()
    test_reconstructed_label_and_append_does_not_rebuild_old_rows()
    test_load_or_build_does_not_rebuild_landed_dates()
    test_published_0922_pin_beats_the_postclose_file()
    test_late_digest_does_not_replace_preopen_tones()
    test_save_report_does_not_overwrite_preopen_pin()
    test_price_store_keeps_the_first_bar()
    test_holdup_created_on_is_the_first_session_after_the_commit()
    test_prune_does_not_use_full_window_stats()
    test_partial_ledger_is_not_frozen()
    test_replay_twice_is_byte_identical()
    test_corrupt_frozen_input_fails_the_hash_guard()
    test_lineup_is_append_only_and_prune_keeps_it()
    test_recipe_creation_date_cannot_move()
    test_open_close_cross_check_sources_and_tolerances()
    test_cross_check_hold_writes_nothing()
    test_candidate_log_uses_morning_files_only()
    test_stooq_fills_open_when_the_export_has_no_open_column()
    test_theme_radar_dated_file_rejects_current_and_a_bad_hash()
    test_webull_fill_outside_open_tolerance_holds()
    test_raw_open_uses_latest_commit_before_the_next_open()
    test_excel_signal_pin_is_the_last_commit_before_the_open()
    print("factor-mine freeze tests passed")
