"""OOS-0914 guards: one test for each enforceable IRONCLAD rule."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src import factor_mine as fm
from src import factor_mine_book as book
from src import factor_mine_freeze as fmf
from src import factor_mine_oos0914 as oos
from src import factor_mine_retro as retro
from src import factor_mine_rules as fmr
from src import factor_mine_sequential as seq
from src import paper_trade as pt

IRONCLAD_SHA256 = "9dade507949485d4c0e65a2595a25a07c18018f509bafcce421300408d4e6f95"


def test_preregister_stays_small() -> None:
    doc = oos.load_preregister()
    assert doc["cutoff"] == "2026-09-14"
    assert doc["designed_after"] == "2026-09-14"
    assert doc["selection"]["random4"]["seed"] == 20260813
    assert doc["selection"]["random4"]["draws"] == 1000
    assert doc["max_candidates"] == 50
    assert doc["trim"]["luck_test_n"] == 37
    assert doc["trim"]["before_mining"] is True
    cands = oos.expand_candidates(doc)
    assert len(cands) <= 50
    assert len(cands) == 37
    own = [c for c in cands if c["family"] == "own"]
    war = [c for c in cands if c["family"] != "own"]
    assert len(own) == 20
    assert len(war) == 17
    assert cands[0]["id"] == "lg_hot_h1_sx"
    assert cands[1]["id"] == "lg_hot_h2_sx"
    assert cands[1]["stop_pct"] is None
    assert all(c["stop_pct"] is None for c in own)
    assert all(c["created_on"] == "2026-09-14" for c in own)
    assert all(c["created_on"] == "2026-09-28" for c in war)
    assert cands[-1]["id"] == "tr12_dfpe_first_top_hammer_h3"
    assert not any("short" in str(c["family"]) for c in cands)
    for spec in doc["war_room_files"].values():
        raw = (oos.ROOT / spec["path"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == spec["sha256"]


def test_no_future_read(tmp_path: Path) -> None:
    folder = tmp_path / "snapshots"
    folder.mkdir(parents=True)
    (folder / "2026-09-11.json").write_text(json.dumps({
        "date": "2026-09-11",
        "rows": [{"date": "2026-09-11", "ticker": "AAA", "ohlc_hot_score": 1}],
    }), encoding="utf-8")
    future = folder / "2026-09-14.json"
    future.write_text(json.dumps({
        "date": "2026-09-14",
        "canary": "FUTURE",
        "rows": [{"date": "2026-09-14", "ticker": "ZZZ"}],
    }), encoding="utf-8")
    opened: list[str] = []

    def reader(path: Path) -> str:
        opened.append(path.name)
        if path.name.startswith("2026-09-14"):
            raise AssertionError("opened a file dated on or after the cutoff")
        return path.read_text(encoding="utf-8")

    found = oos.load_snapshot_dir(
        folder, start="2026-08-13", end="2026-09-11", read_text=reader,
    )
    assert opened == ["2026-09-11.json"]
    assert list(found) == ["2026-09-11"]
    assert "ZZZ" not in json.dumps(found)
    assert "FUTURE" not in json.dumps(found)

    store = tmp_path / "prices"
    store.mkdir(parents=True)
    (store / "meta.json").write_text(
        json.dumps({"last_date": "2026-09-14"}), encoding="utf-8")
    parquet = store / "ohlc.parquet"
    parquet.write_bytes(b"not-read")
    try:
        oos.load_session_bars(
            parquet, ["2026-09-11"], {"AAA"}, max_date="2026-09-11",
            allow_test=False,
        )
        raised = False
    except oos.FutureLeak:
        raised = True
    assert raised
    assert parquet.read_bytes() == b"not-read"


def _toy():
    dates = ["2026-08-13", "2026-08-14"]
    rec = fm.make_recipe(
        "toy_oos", hold=1, top_n=1, rank="hot_score", sell="time",
    )

    def rows_for(date: str):
        ticker = "BBB" if date == dates[-1] else "AAA"
        return [{
            "date": date, "ticker": ticker, "sources": ["union"],
            "ohlc_hot_score": 3.0, "alarm": False, "e_pol": False,
            "rsi": 40.0,
        }]

    bars = {}
    for date in dates:
        bars[("AAA", date)] = {"open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5}
        bars[("BBB", date)] = {"open": 8.0, "high": 8.5, "low": 7.5, "close": 8.2}
    return dates, rec, rows_for, bars


def test_resume_byte_identical(tmp_path: Path) -> None:
    dates, rec, rows_for, bars = _toy()
    full_root = tmp_path / "full"
    history: dict[str, list] = {}
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=full_root, persist=True, fees=fm.pt_fees(), regime={},
        history=history,
    )
    full_bytes = {
        date: (full_root / rec["name"] / f"{date}.json").read_bytes()
        for date in dates
    }
    resumed = tmp_path / "resumed"
    src = full_root / rec["name"] / f"{dates[0]}.json"
    dest = resumed / rec["name"] / f"{dates[0]}.json"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(src.read_bytes())
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=resumed, persist=True, fees=fm.pt_fees(), regime={},
    )
    for date in dates:
        assert (resumed / rec["name"] / f"{date}.json").read_bytes() == full_bytes[date]


def test_append_refusal(tmp_path: Path) -> None:
    dates, rec, rows_for, bars = _toy()
    root = tmp_path / "state"
    seq.walk(
        dates[:1], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], dates[0], root)
    doc = {
        "date": dates[0],
        "recipes": {
            rec["name"]: {
                "buys": state.get("buys") or [],
                "sells": state.get("sells") or [],
                "trades": state.get("fills") or [],
                "equity": state.get("equity"),
                "mean": state.get("mean"),
            },
        },
    }
    path = tmp_path / "ledgers" / f"{dates[0]}.json"
    oos.write_oos_ledger(dates[0], doc, path)
    first = path.read_bytes()
    oos.write_oos_ledger(dates[0], json.loads(json.dumps(doc)), path)
    assert path.read_bytes() == first
    changed = json.loads(json.dumps(doc))
    changed["recipes"][rec["name"]]["buys"] = [
        {"ticker": "ZZZ", "side": "BUY", "shares": 1, "price": 1.0},
    ]
    try:
        oos.write_oos_ledger(dates[0], changed, path)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert path.read_bytes() == first
    try:
        seq.write_state(rec["name"], dates[0], {"date": dates[0], "cash": 1}, root)
        state_raised = False
    except Exception:
        state_raised = True
    assert state_raised


def test_ironclad_linked_from_scoreboard() -> None:
    path = oos.ROOT / "IRONCLAD_RULES.md"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == IRONCLAD_SHA256
    text = raw.decode("utf-8")
    for number in range(1, 24):
        assert f"\n{number}. " in text
    board = oos.SCOREBOARD.read_text(encoding="utf-8")
    assert "../IRONCLAD_RULES.md" in board
    assert "../IRONCLAD_RULES.md" in oos.render_hold_scoreboard()
    sample = oos.render_scoreboard(
        {"n_candidates": 0, "random4": {}, "iwm": {}, "rows": [], "train": {"sessions": []}},
        None,
    )
    assert "../IRONCLAD_RULES.md" in sample


def _ledger_slot(state: dict) -> dict:
    return {
        "buys": state.get("buys") or [],
        "sells": state.get("sells") or [],
        "trades": state.get("fills") or [],
        "equity": state.get("equity"),
        "mean": state.get("mean"),
    }


def test_rule_01_buys_never_change(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dates, rec, rows_for, bars = _toy()
    root = tmp_path / "state"
    seq.walk(
        dates[:1], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], dates[0], root)
    doc = {"date": dates[0], "recipes": {rec["name"]: _ledger_slot(state)}}
    path = tmp_path / "ledgers" / f"{dates[0]}.json"
    oos.write_oos_ledger(dates[0], doc, path)
    first = path.read_bytes()
    side = path.with_name(path.name + ".sha256").read_bytes()
    changed = json.loads(json.dumps(doc))
    changed["recipes"][rec["name"]]["buys"] = [
        {"ticker": "ZZZ", "side": "BUY", "shares": 1, "price": 1.0},
    ]
    try:
        oos.write_oos_ledger(dates[0], changed, path)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert path.read_bytes() == first
    assert path.with_name(path.name + ".sha256").read_bytes() == side


def test_rule_02_append_only(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dates, rec, rows_for, bars = _toy()
    root = tmp_path / "state"
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    paths = {}
    for date in dates:
        state = seq.read_state(rec["name"], date, root)
        doc = {"date": date, "recipes": {rec["name"]: _ledger_slot(state)}}
        paths[date] = oos.write_oos_ledger(date, doc, tmp_path / f"{date}.json")
    first = paths[dates[0]].read_bytes()
    assert paths[dates[1]].is_file()
    assert paths[dates[0]].read_bytes() == first
    dropped = json.loads(paths[dates[0]].read_text(encoding="utf-8"))
    dropped["recipes"].pop(rec["name"])
    try:
        oos.write_oos_ledger(dates[0], dropped, paths[dates[0]])
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert paths[dates[0]].read_bytes() == first


def test_rule_03_fingerprint_recheck(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    date = "2026-08-13"
    doc = {"date": date, "recipes": {"toy": {
        "buys": [{"ticker": "AAA", "side": "BUY", "shares": 1, "price": 10.0}],
        "sells": [], "trades": [], "equity": 10000, "mean": 0,
    }}}
    path = tmp_path / f"{date}.json"
    oos.write_oos_ledger(date, doc, path)
    locked = path.read_bytes()
    flipped = bytearray(locked)
    flipped[20] = flipped[20] ^ 1
    path.write_bytes(flipped)
    try:
        oos.write_oos_ledger(date, doc, path)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert path.read_bytes() == bytes(flipped)


def test_rule_04_rule_change_is_a_new_name(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dest = tmp_path / "freeze_manifest.json"
    dest.write_bytes(fmf.MANIFEST_PATH.read_bytes())
    before = fmf.MANIFEST_PATH.read_bytes()
    study = oos.expand_candidates()[0]
    rec = oos._frozen_recipe(study)
    old = fmf.MANIFEST_PATH
    fmf.MANIFEST_PATH = dest
    try:
        fmr.lock_recipe_rules([rec], write=True, locked_on="2026-09-14")
        locked = json.loads(dest.read_text(encoding="utf-8"))
        sha = locked["recipe_rules"][rec["name"]]["sha256"]
        changed = dict(rec)
        changed["stop_pct"] = 0.12
        try:
            fmr.lock_recipe_rules([changed], write=True, locked_on="2026-09-14")
            raised = False
        except fmr.RuleDrift:
            raised = True
        assert raised
        again = json.loads(dest.read_text(encoding="utf-8"))
        assert again["recipe_rules"][rec["name"]]["sha256"] == sha
        assert sha == fmr.recipe_fingerprint(rec)
    finally:
        fmf.MANIFEST_PATH = old
    assert fmf.MANIFEST_PATH.read_bytes() == before


def test_rule_05_designed_after() -> None:
    rec = oos._frozen_recipe(oos.expand_candidates()[0])
    assert rec["created_on"] == "2026-09-14"
    assert fmr.session_label(rec["name"], "2026-09-11", rec) == "designed_after"
    assert fmr.session_label(rec["name"], "2026-09-14", rec) == "real"
    split = fmr.split_means(rec["name"], [
        {"date": "2026-09-11", "mean": 40.0},
        {"date": "2026-09-14", "mean": 1.0},
    ], rec)
    assert split["n_designed_after"] == 1
    assert split["n_real"] == 1
    assert split["real_compound_pct"] == 1.0
    assert split["designed_after_compound_pct"] == 40.0


def test_rule_06_locked_ticket_untouched(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    ticket = tmp_path / "day" / "2026-08-13_strategy_tickets.json"
    ticket.parent.mkdir()
    ticket.write_text('{"ticket_lock": true}\n', encoding="utf-8")
    before = ticket.read_bytes()
    doc = {"date": "2026-08-13", "recipes": {"toy": {
        "buys": [], "sells": [], "trades": [], "equity": 10000, "mean": 0,
    }}}
    oos.write_oos_ledger("2026-08-13", doc, tmp_path / "oos" / "2026-08-13.json")
    assert ticket.read_bytes() == before
    assert not (tmp_path / "day" / "2026-08-13_strategy_tickets_draft.json").exists()
    assert oos.STATE_ROOT != seq.STATE_DIR
    assert "oos0914" in oos.STATE_ROOT.parts


def test_rule_07_day_n_uses_prior_close(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dates, rec, rows_for, bars = _toy()
    chained = tmp_path / "chained"
    seq.walk(
        dates, [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=chained, persist=True, fees=fm.pt_fees(), regime={},
    )
    alone = tmp_path / "alone"
    seq.walk(
        dates[1:], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=alone, persist=True, fees=fm.pt_fees(), regime={},
    )
    chained_cash = seq.read_state(rec["name"], dates[1], chained)["cash"]
    alone_cash = seq.read_state(rec["name"], dates[1], alone)["cash"]
    assert chained_cash != alone_cash
    clipped = seq.bars_through({
        ("AAA", dates[0]): {"open": 1},
        ("AAA", dates[1]): {"open": 2},
    }, dates[0])
    assert ("AAA", dates[1]) not in clipped
    assert ("AAA", dates[0]) in clipped


def test_rule_08_restart_and_future_file(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    test_resume_byte_identical(tmp_path / "resume")
    test_no_future_read(tmp_path / "future")


def test_rule_09_frozen_input_hash(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    morning = {"rows": [{"ticker": "AAA", "ohlc_hot_score": 1}]}
    later = {"rows": [{"ticker": "AAA", "ohlc_hot_score": 1}, {"ticker": "LATE", "ohlc_hot_score": 9}]}
    assert oos.snapshot_input_sha(morning) != oos.snapshot_input_sha(later)
    doc = {
        "date": "2026-08-13",
        "input_sha256": oos.snapshot_input_sha(morning),
        "dropped": ["ZZZ"],
        "recipes": {"toy": {
            "buys": [{"ticker": "AAA", "side": "BUY", "shares": 1, "price": 10}],
            "sells": [], "trades": [], "equity": 9000, "mean": 0,
        }},
    }
    path = tmp_path / "2026-08-13.json"
    oos.write_oos_ledger("2026-08-13", doc, path)
    locked = path.read_bytes()
    nxt = json.loads(json.dumps(doc))
    nxt["input_sha256"] = oos.snapshot_input_sha(later)
    nxt["dropped"] = ["ZZZ", "LATE"]
    try:
        oos.write_oos_ledger("2026-08-13", nxt, path)
        raised = False
    except fmr.AppendDrift:
        raised = True
    assert raised
    assert path.read_bytes() == locked


def test_rule_10_empty_morning_sits(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    stamped = oos.snapshot_rows({
        "rows": [{"date": "2026-08-13", "ticker": "AAA", "e_pol": True}],
    }, "2026-08-13")
    assert stamped[0]["e_pol"] is False
    dates, rec, _rows, bars = _toy()

    def empty(_date):
        return []

    root = tmp_path / "state"
    seq.walk(
        dates[:1], [rec], rows_for=empty, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], dates[0], root)
    assert state["buys"] == []
    path = oos.write_oos_ledger(dates[0], {
        "date": dates[0],
        "recipes": {rec["name"]: _ledger_slot(state)},
    }, tmp_path / f"{dates[0]}.json")
    assert path.is_file()

    from src import factor_mine_probe as fmp
    called = {"n": 0}

    def boom(*_a, **_k):
        called["n"] += 1
        raise AssertionError("live lookup")

    old = fmp.attach_erd_polarity
    fmp.attach_erd_polarity = boom
    try:
        seq.walk(
            dates[:1], [rec],
            rows_for=lambda date: oos.snapshot_rows({
                "rows": [{
                    "date": date, "ticker": "AAA", "sources": ["union"],
                    "ohlc_hot_score": 3.0, "e_pol": True,
                }],
            }, date),
            bars_for=lambda _d: bars,
            root=tmp_path / "stamped", persist=True, fees=fm.pt_fees(), regime={},
        )
    finally:
        fmp.attach_erd_polarity = old
    assert called["n"] == 0


def test_rule_11_yahoo_missing_name_still_locks(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    store = tmp_path / "prices"
    store.mkdir()
    (store / "meta.json").write_text(
        json.dumps({"last_date": "2026-09-11"}), encoding="utf-8")
    frame = pd.DataFrame([{
        "date": "2026-09-11", "ticker": "AAA",
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.4,
    }])
    path = store / "ohlc.parquet"
    frame.to_parquet(path)

    def boom(*_a, **_k):
        raise AssertionError("price source left Yahoo")

    old_stooq = fmf.stooq_bar
    fmf.stooq_bar = boom
    try:
        bars = oos.load_session_bars(
            path, ["2026-09-11"], {"AAA"}, max_date="2026-09-11",
        )
    finally:
        fmf.stooq_bar = old_stooq
    assert bars[("AAA", "2026-09-11")]["open"] == 10.0

    import yfinance as yf
    calls = {}

    def fake_download(*args, **kwargs):
        calls["auto_adjust"] = kwargs.get("auto_adjust")
        return None

    old_dl = yf.download
    yf.download = fake_download
    try:
        try:
            oos._ensure_iwm(["2026-09-14"], {}, allow_test=True)
        except Exception:
            pass
    finally:
        yf.download = old_dl
    assert calls.get("auto_adjust") is False

    day = "2026-08-13"
    snaps = tmp_path / "snaps"
    snaps.mkdir()
    (snaps / f"{day}.json").write_text(json.dumps({
        "date": day,
        "dropped": ["ZZZ"],
        "rows": [
            {"date": day, "ticker": "AAA", "sources": ["union"], "ohlc_hot_score": 1},
            {"date": day, "ticker": "MISS", "sources": ["union"], "ohlc_hot_score": 9},
        ],
    }), encoding="utf-8")
    saved_snap, saved_ledger = oos.SNAP_DIR, oos.LEDGER_DIR
    oos.SNAP_DIR = snaps
    oos.LEDGER_DIR = tmp_path / "ledgers"

    def only_aaa(_dates, _tickers, *, allow_test=False):
        return {("AAA", day): {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5}}

    old_bars = oos._bars_for_window
    oos._bars_for_window = only_aaa
    rec = fm.make_recipe("toy_miss", hold=1, top_n=2, rank="hot_score", sell="time")
    try:
        oos.walk_test([day], [rec], root=tmp_path / "state")
    finally:
        oos.SNAP_DIR = saved_snap
        oos.LEDGER_DIR = saved_ledger
        oos._bars_for_window = old_bars
    ledger = json.loads((tmp_path / "ledgers" / f"{day}.json").read_text(encoding="utf-8"))
    assert ledger["dropped"] == ["ZZZ"]
    buys = ledger["recipes"][rec["name"]]["buys"]
    assert [b["ticker"] for b in buys] == ["AAA"]
    assert (tmp_path / "ledgers" / f"{day}.json").is_file()


def test_rule_12_fills(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    day = "2026-08-13"
    rec = fm.make_recipe(
        "toy_mark", hold=2, top_n=1, rank="hot_score", sell="time",
    )

    def rows_for(date):
        return [{
            "date": date, "ticker": "AAA", "sources": ["union"],
            "ohlc_hot_score": 3.0, "alarm": False, "e_pol": False,
        }]

    bars = {("AAA", day): {"open": 10.0, "high": 10.2, "low": 9.8, "close": 12.0}}
    root = tmp_path / "mark"
    seq.walk(
        [day], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], day, root)
    assert state["buys"][0]["price"] == 10.0
    lot = state["state"]["pos"]["AAA"]
    assert float(lot["close_px"]) == 12.0
    assert abs(float(state["equity"]) - (float(state["cash"]) + lot["shares"] * 12.0)) < 0.02

    level = book.same_bar_stop(
        {"entry_px": 10.0},
        {"open": 10.0, "high": 10.2, "low": 9.0, "close": 9.5},
        side="long", stop_pct=0.08,
    )
    assert level[0] == 10.0 * 0.92
    assert level[1] == "stop_same_bar"
    gap = book.same_bar_stop(
        {"entry_px": 10.0},
        {"open": 8.0, "high": 8.5, "low": 7.5, "close": 8.2},
        side="long", stop_pct=0.08,
    )
    assert gap[0] == 8.0
    assert gap[1] == "stop_gap_open"


def test_rule_13_stop_fills_first(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    both = book.same_bar_stop(
        {"entry_px": 10.0},
        {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0},
        side="long", stop_pct=0.08, take_pct=0.05,
    )
    assert both[1] == "stop_first_same_bar"
    assert both[0] == 10.0 * 0.92
    assert both[0] != 10.0 * 1.05

    day = "2026-08-13"
    nxt = "2026-08-14"
    rec = fm.make_recipe(
        "toy_stop", hold=2, top_n=1, rank="hot_score", sell="time",
        stop_pct=0.08, take_pct=0.05,
    )

    def rows_for(date):
        return [{
            "date": date, "ticker": "AAA", "sources": ["union"],
            "ohlc_hot_score": 3.0, "alarm": False, "e_pol": False,
        }]

    bars = {}
    bars[("AAA", day)] = {"open": 10.0, "high": 10.4, "low": 9.9, "close": 10.2}
    bars[("AAA", nxt)] = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0}
    root = tmp_path / "stop"
    seq.walk(
        [day, nxt], [rec], rows_for=rows_for, bars_for=lambda _d: bars,
        root=root, persist=True, fees=fm.pt_fees(), regime={},
    )
    state = seq.read_state(rec["name"], nxt, root)
    sells = state["sells"]
    assert sells and sells[0]["price"] == round(9.2, 4)
    assert sells[0]["fill_rule"] == "stop_first_same_bar"


def test_rule_14_futubull_plus_flat_15bp() -> None:
    fees = fm.pt_fees()
    base = pt.order_fees(100, 10.0, "buy", fees)

    def charge():
        return pt.order_fees(100, 10.0, "buy", fees)

    plus = oos._with_extra_fee(charge)
    extra = round(100 * 10.0 * (oos.FLAT_RT / 2.0), 4)
    assert plus == round(float(base) + extra, 4)
    assert pt.order_fees(100, 10.0, "buy", fees) == base
    text = oos.render_scoreboard(_judge_train(), _judge_test())
    assert "Futubull + 15 bp" in text
    assert "RANDOM4 mean" in text


def _judge_train() -> dict:
    return {
        "n_candidates": 40,
        "random4": {
            "mean": -1.0, "seed": 20260813, "draws": 1000,
            "best_of_n_null": 1.0, "n_candidates": 40, "p5": -2.0, "p95": 2.0,
        },
        "iwm": {"after_fees_return": -0.5},
        "train": {"sessions": ["2026-08-13"]},
        "rows": [
            {
                "id": "live_h1", "after_fees_return": 1.2,
                "start_day_win_rate": 0.5, "fires": 4, "win_rate": 0.5,
                "asymmetric_payoff": 1.5, "best_stock": "AAA",
                "without_best_stock_return": 0.2, "pass": True,
                "untestable": False,
            },
            {
                "id": "dead_h1", "untestable": True, "pass": False,
                "after_fees_return": 0, "start_day_win_rate": 0, "fires": 0,
            },
        ],
    }


def _judge_test() -> dict:
    return {
        "sessions": ["2026-09-14", "2026-09-15"],
        "rules": [{
            "name": "oos0914_live_h1",
            "after_fees_return": 1.0,
            "best_stock": "AAA",
            "without_best_stock_return": 0.4,
            "days": [{
                "date": "2026-09-14", "buys": ["AAA"], "sells": [],
                "fees": 1.0, "cash": 100, "equity": 10100, "mean": 1.0,
            }],
        }],
        "baselines": {
            "random4": {
                "futubull": {"mean": -2.0},
                "futubull_plus_15bp": {"mean": -2.5},
            },
            "iwm": {
                "futubull": {"after_fees_return": -1.0},
                "futubull_plus_15bp": {"after_fees_return": -1.2},
            },
        },
    }


def test_rule_15_no_future_load(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    test_no_future_read(tmp_path)


def test_rule_16_list_committed_before_mining(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    doc = json.loads(json.dumps(oos.load_preregister()))
    assert int(doc["max_candidates"]) <= 50
    assert "selection" in doc
    doc["bases"] = list(doc["bases"]) * 3
    try:
        oos.expand_candidates(doc)
        raised = False
    except RuntimeError:
        raised = True
    assert raised
    saved = (oos.TRAIN_REPORT, oos.FROZEN_PATH, oos.OUT_DIR)
    before = fmf.MANIFEST_PATH.read_bytes()
    oos.OUT_DIR = tmp_path
    oos.TRAIN_REPORT = tmp_path / "train_report.json"
    oos.FROZEN_PATH = tmp_path / "frozen_rules.json"
    oos.TRAIN_REPORT.write_text(
        json.dumps({"freeze": ["not_in_the_list"]}), encoding="utf-8")
    try:
        try:
            oos.freeze()
            refused = False
        except SystemExit:
            refused = True
        assert refused
        assert not oos.FROZEN_PATH.exists()
    finally:
        oos.TRAIN_REPORT, oos.FROZEN_PATH, oos.OUT_DIR = saved
    assert fmf.MANIFEST_PATH.read_bytes() == before


def test_rule_17_freeze_before_test_score(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dest = tmp_path / "freeze_manifest.json"
    dest.write_bytes(fmf.MANIFEST_PATH.read_bytes())
    before = fmf.MANIFEST_PATH.read_bytes()
    saved = (
        oos.TRAIN_REPORT, oos.FROZEN_PATH, oos.OUT_DIR,
        oos.TEST_REPORT, oos.SCOREBOARD, fmf.MANIFEST_PATH,
    )
    oos.OUT_DIR = tmp_path
    oos.TRAIN_REPORT = tmp_path / "train_report.json"
    oos.FROZEN_PATH = tmp_path / "frozen_rules.json"
    oos.TEST_REPORT = tmp_path / "test_report.json"
    oos.SCOREBOARD = tmp_path / "board.md"
    fmf.MANIFEST_PATH = dest
    cid = oos.expand_candidates()[0]["id"]
    oos.TRAIN_REPORT.write_text(json.dumps({"freeze": [cid]}), encoding="utf-8")
    calls = {"n": 0}

    def no_score():
        calls["n"] += 1
        raise AssertionError("test window scored during freeze")

    old_score = oos.score
    oos.score = no_score
    try:
        payload = oos.freeze()
        assert calls["n"] == 0
        assert not oos.TEST_REPORT.exists()
        assert payload["fingerprints"]
        assert "after_fees_return" not in json.dumps(payload)
    finally:
        oos.score = old_score
        (
            oos.TRAIN_REPORT, oos.FROZEN_PATH, oos.OUT_DIR,
            oos.TEST_REPORT, oos.SCOREBOARD, fmf.MANIFEST_PATH,
        ) = saved
    assert fmf.MANIFEST_PATH.read_bytes() == before


def test_rule_18_small_list_and_luck_null() -> None:
    assert len(oos.expand_candidates()) <= 50
    day = "2026-08-13"
    snaps = {day: {"rows": [
        {"date": day, "ticker": ticker, "ohlc_hot_score": 1}
        for ticker in ("AAA", "BBB", "CCC", "DDD", "EEE")
    ]}}

    def fake(panel, rec, bars, flat_15bp=False, fees=None, regime=None):
        return {"total_ret_pct": 1.0}

    old = retro.score_panel
    retro.score_panel = fake
    try:
        out = oos.score_random4([day], snaps, {})
    finally:
        retro.score_panel = old
    assert out["draws"] == 1000
    assert out["seed"] == 20260813
    assert out["n_candidates"] <= 50
    assert out["best_of_n_null"] is not None


def test_rule_19_baselines() -> None:
    text = oos.render_scoreboard(_judge_train(), _judge_test())
    assert "RANDOM4" in text
    assert "IWM" in text
    assert "seed 20260813" in text
    assert "1,000" in text or "1000" in text


def test_rule_20_without_best_stock() -> None:
    text = oos.render_scoreboard(_judge_train(), _judge_test())
    assert "Without its best stock (AAA)" in text
    assert "+0.40%" in text


def test_rule_21_keep_bar_reported() -> None:
    records = []
    for i in range(31):
        records.append({
            "buys": [{"ticker": "AAA"}],
            "fills": [{"side": "SELL", "pnl": 2.0 if i < 20 else -1.0}],
            "mean": 1.0,
        })
    assert oos.trade_fires(records) == 31
    assert oos.closed_win_rate(records) > 0.55
    assert oos.keep_bar_met(records)
    assert oos.asymmetric_payoff(records) == 2.0
    assert oos.start_day_win_rate(records) == 1.0
    assert not oos.keep_bar_met(records[:29])
    weak = [{
        "buys": [{"ticker": "AAA"}],
        "fills": [{"side": "SELL", "pnl": 1.0 if i < 10 else -1.0}],
    } for i in range(31)]
    assert not oos.keep_bar_met(weak)
    text = oos.render_scoreboard(_judge_train(), _judge_test())
    assert "asymmetric" in text
    assert "start-day win rate" in text


def test_rule_22_untestable_out_of_rankings() -> None:
    assert oos.is_untestable([{"buys": [], "fills": []}])
    assert not oos.is_untestable([{"buys": [{"ticker": "AAA"}]}])
    ranked = oos.ranked_pass_ids([
        {"id": "b", "pass": True, "untestable": False,
         "after_fees_return": 1.0, "start_day_win_rate": 1.0},
        {"id": "a", "pass": True, "untestable": True,
         "after_fees_return": 9.0, "start_day_win_rate": 1.0},
        {"id": "c", "pass": True, "untestable": False,
         "after_fees_return": 1.0, "start_day_win_rate": 1.0},
    ])
    assert ranked == ["b", "c"]
    text = oos.render_scoreboard(_judge_train(), None)
    assert "dead_h1" not in text
    assert "live_h1" in text


def test_rule_23_no_real_money_yet() -> None:
    assert oos.real_money_allowed(
        locked_sessions=19, beats_random4=True, beats_iwm=True,
        without_best_beats=True,
    ) is False
    assert oos.real_money_allowed(
        locked_sessions=20, beats_random4=True, beats_iwm=True,
        without_best_beats=False,
    ) is False
    assert oos.real_money_allowed(
        locked_sessions=20, beats_random4=True, beats_iwm=True,
        without_best_beats=True,
    ) is True
    source = Path(oos.__file__).read_text(encoding="utf-8").lower()
    assert "webull" not in source
    assert "supabase" not in source
    assert oos.STATE_ROOT != seq.STATE_DIR


def test_excel_feature_uses_prior_close() -> None:
    import pandas as pd
    from src.factor_mine_oos0914_excel import build_features

    rows = []
    for i, close in enumerate((10.0, 11.0, 12.0)):
        rows.append({
            "date": f"2026-08-{10 + i:02d}",
            "ticker": "AAA",
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": 1000.0,
        })
    feat = build_features(pd.DataFrame(rows))
    last = feat[feat["date"] == "2026-08-12"].iloc[0]
    assert abs(float(last["ret1"]) - 0.1) < 1e-9


def test_theme_train_does_not_open_future_snapshot(tmp_path: Path) -> None:
    import pandas as pd
    from src.factor_mine_oos0914_theme import (
        _top_quintile, read_snapshot, snapshot_dates,
    )

    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "2026-09-11.csv").write_text(
        "Ticker,Forward P/E,Market Cap,Analyst Recom\nAAA,1,1,1\n",
        encoding="utf-8",
    )
    future = tmp_path / "2026-09-14.csv"
    future.write_text("CANARY\n", encoding="utf-8")
    assert snapshot_dates(tmp_path, allow_test=False) == ["2026-09-11"]
    try:
        read_snapshot(tmp_path, "2026-09-14", allow_test=False)
        raised = False
    except oos.FutureLeak:
        raised = True
    assert raised
    assert future.read_text(encoding="utf-8") == "CANARY\n"
    top = _top_quintile(pd.Series({f"T{i}": float(i) for i in range(5)}))
    assert top == {"T4"}


def main() -> None:
    import tempfile
    manifest = fmf.MANIFEST_PATH.read_bytes()
    test_ironclad_linked_from_scoreboard()
    test_preregister_stays_small()
    test_excel_feature_uses_prior_close()
    test_rule_05_designed_after()
    test_rule_14_futubull_plus_flat_15bp()
    test_rule_18_small_list_and_luck_null()
    test_rule_19_baselines()
    test_rule_20_without_best_stock()
    test_rule_21_keep_bar_reported()
    test_rule_22_untestable_out_of_rankings()
    test_rule_23_no_real_money_yet()
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        test_no_future_read(root / "future")
        test_resume_byte_identical(root / "resume")
        test_append_refusal(root / "append")
        test_rule_01_buys_never_change(root / "r01")
        test_rule_02_append_only(root / "r02")
        test_rule_03_fingerprint_recheck(root / "r03")
        test_rule_04_rule_change_is_a_new_name(root / "r04")
        test_rule_06_locked_ticket_untouched(root / "r06")
        test_rule_07_day_n_uses_prior_close(root / "r07")
        test_rule_08_restart_and_future_file(root / "r08")
        test_rule_09_frozen_input_hash(root / "r09")
        test_rule_10_empty_morning_sits(root / "r10")
        test_rule_11_yahoo_missing_name_still_locks(root / "r11")
        test_rule_12_fills(root / "r12")
        test_rule_13_stop_fills_first(root / "r13")
        test_rule_15_no_future_load(root / "r15")
        test_rule_16_list_committed_before_mining(root / "r16")
        test_rule_17_freeze_before_test_score(root / "r17")
        test_theme_train_does_not_open_future_snapshot(root / "theme")
    assert fmf.MANIFEST_PATH.read_bytes() == manifest
    print("oos0914 tests passed")


if __name__ == "__main__":
    main()
