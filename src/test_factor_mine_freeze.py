"""Append-only factor-mine snapshots and the price hold."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

from src import factor_mine as fm
from src import factor_mine_freeze as fmf
from src import map_heat as mh
from src import ticker_lookback as tl


def _redirect(tmp: Path):
    return (
        fmf.SNAP_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH,
    ), (
        tmp / "snapshots", tmp / "prices",
        tmp / "freeze_manifest.json",
    )


def _use(tmp: Path):
    old, new = _redirect(tmp)
    fmf.SNAP_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH = new
    for p in new[:2]:
        p.mkdir(parents=True, exist_ok=True)
    return old


def _restore(old) -> None:
    fmf.SNAP_DIR, fmf.PRICE_DIR, fmf.MANIFEST_PATH = old


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
    }
    new = {
        "snapshots": {
            "2026-09-25": {"sha256": "aaa"},
            "2026-09-26": {"sha256": "ccc"},
        },
    }
    with tempfile.TemporaryDirectory() as d:
        saved = _use(Path(d))
        try:
            for date, body in (
                ("2026-09-25", {"k": 1}),
                ("2026-09-26", {"k": 2}),
            ):
                raw = fmf.canonical_bytes(body)
                (fmf.SNAP_DIR / f"{date}.json").write_bytes(raw)
                new["snapshots"][date] = {"sha256": fmf.sha256_bytes(raw)}
            old["snapshots"]["2026-09-25"]["sha256"] = new["snapshots"]["2026-09-25"]["sha256"]
            fmf.guard_manifest(old, new, restate=[])
            broken = json.loads(json.dumps(new))
            broken["snapshots"]["2026-09-25"] = {"sha256": "changed"}
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
            (fmf.SNAP_DIR / "2026-09-25.json").write_bytes(raw)
            broken["snapshots"]["2026-09-25"] = {"sha256": fmf.sha256_bytes(raw)}
            fmf.guard_manifest(old, broken, restate=["2026-09-25"])
        finally:
            _restore(saved)


def test_missing_bars_hold_the_day_and_do_not_write_hot_zero() -> None:
    """Every candidate gapped means nobody is rankable, so the day is skipped."""
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            with mock.patch("src.price_store.ensure_through", return_value=None), \
                    mock.patch.object(fmf, "_raw_bars", return_value=[]), \
                    mock.patch.object(tl, "_official_ohlc",
                                      return_value={"open": None, "close": None}):
                gaps = fmf.ensure_candidate_bars("2026-09-25", ["BBB", "AAA"])
            assert [g["ticker"] for g in gaps] == ["AAA", "BBB"]
            assert all(g["reason"] for g in gaps)

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


def test_append_freezes_the_new_day_and_keeps_old_rows() -> None:
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
                    mock.patch.object(fmf, "code_sha", return_value="abc123freeze"):
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
            snap = json.loads(fmf.snapshot_path("2026-09-25").read_text())
            assert snap["code_sha"] == "abc123freeze"
            assert snap["rows"][0]["ticker"] == "NEW"
            assert out["freeze"]["first_frozen"] == "2026-09-25"
            assert set(out["freeze"]) == {"first_frozen", "n_snapshots"}
            assert out["_frozen_dates"] == ["2026-09-25"]
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


def test_snapshot_stamps_the_building_code_sha() -> None:
    with mock.patch.object(fmf, "heat_record", return_value={
        "vintage": "2026-09-25", "phase": "morning_overlay",
        "board_date": "2026-09-25", "source": None, "sha256": "abc",
    }), mock.patch.object(fmf, "code_sha", return_value="deadbeef"):
        snap = fmf.make_snapshot(
            "2026-09-25",
            [{"date": "2026-09-25", "ticker": "AAA", "src_rank": 0, "open": 10.0}],
            "2026-09-24",
            "pricesha",
        )
    assert snap["code_sha"] == "deadbeef"
    assert snap["tape"] == "raw"
    assert snap["auto_adjust"] is False
    assert snap["rows"][0]["open_0930"] == 10.0


if __name__ == "__main__":
    test_snapshot_is_write_once_and_restate_logs_previous_hash()
    test_guard_fails_when_an_earlier_hash_changes()
    test_missing_bars_hold_the_day_and_do_not_write_hot_zero()
    test_completeness_gate_lists_every_hole_and_refuses_adjusted_bars()
    test_one_gapped_name_is_dropped_and_the_rerun_matches()
    test_build_panel_fetches_bars_before_candidates()
    test_build_panel_refuses_unresolved_hot_score()
    test_morning_map_heat_is_not_replaced_by_postclose()
    test_append_freezes_the_new_day_and_keeps_old_rows()
    test_load_or_build_does_not_rebuild_landed_dates()
    test_published_0922_pin_beats_the_postclose_file()
    test_late_digest_does_not_replace_preopen_tones()
    test_save_report_does_not_overwrite_preopen_pin()
    test_price_store_keeps_the_first_bar()
    test_snapshot_stamps_the_building_code_sha()
    print("factor-mine freeze tests passed")
