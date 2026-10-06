"""Webull sim: fees, whole shares, stop-first, lock split, provenance."""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from src import past_day_lock
from src.webull_sim import (
    live_fetch_allowed,
    FEES_PATH,
    FIRST_LOCKED,
    START_CASH,
    Account,
    Pick,
    Schedule,
    apply_session,
    before_open,
    book_name,
    classify,
    fee_for,
    final_row,
    in_write_freeze,
    load_schedule,
    make_row,
    parse_excel,
    parse_theme_log,
    q,
    run_book,
    sandbox_label,
    seal_row,
    section_for,
    whole_shares,
    write_books,
)

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def schedule() -> Schedule:
    return load_schedule()


def test_published_fees_match_the_page() -> None:
    raw = json.loads(FEES_PATH.read_text(encoding="utf-8"))
    assert raw["source_url"] == "https://www.webull.com/pricing"
    assert raw["retrieved"] == "2026-10-06"
    assert raw["commission_per_trade"] == 0
    fees = schedule()
    # 100 shares sold at $10. SEC is per sale dollar. CAT is per share. TAF is 0.
    sell = fee_for(fees, "sell", 100, Decimal("10"))
    assert sell == Decimal("0.0000206") * Decimal("1000") + Decimal("0.000003") * 100
    buy = fee_for(fees, "buy", 100, Decimal("10"))
    assert buy == Decimal("0.000003") * 100
    assert "stock_borrow" in raw["left_out"]
    assert fees.taf_per_share == 0


def test_whole_shares_do_not_spend_past_cash() -> None:
    fees = schedule()
    shares = whole_shares(Decimal("100"), Decimal("30"), fees, "buy")
    assert shares == 3
    cost = Decimal("30") * 3 + fee_for(fees, "buy", 3, Decimal("30"))
    assert cost <= Decimal("100")
    assert whole_shares(Decimal("29"), Decimal("30"), fees, "buy") == 0
    assert whole_shares(Decimal("100"), Decimal("40"), fees, "sell") == 2


def test_stop_fills_before_target_on_the_same_bar() -> None:
    fees = schedule()
    account = Account(cash=Decimal("1000"))
    account.lots.append(__import__("src.webull_sim", fromlist=["Lot"]).Lot(
        ticker="AAA", side="long", shares=10, entry_px=Decimal("10"),
        entry_date="2026-10-06", stop=Decimal("9"), target=Decimal("11"),
    ))
    fills = apply_session(
        account, [], [],
        {"AAA": {"open": 10, "high": 12, "low": 8, "close": 10.5}},
        fees, "2026-10-07", 1,
    )
    assert len(fills) == 1
    assert fills[0]["reason"] == "stop"
    assert fills[0]["price"] == Decimal("9")
    assert account.lots == []


def test_gap_through_the_stop_fills_at_the_open() -> None:
    fees = schedule()
    account = Account(cash=Decimal("0"))
    account.lots.append(__import__("src.webull_sim", fromlist=["Lot"]).Lot(
        ticker="AAA", side="long", shares=10, entry_px=Decimal("10"),
        entry_date="2026-10-06", stop=Decimal("9"), target=Decimal("11"),
    ))
    fills = apply_session(
        account, [], [],
        {"AAA": {"open": 8, "high": 12, "low": 7, "close": 9}},
        fees, "2026-10-07", 1,
    )
    assert fills[0]["reason"] == "stop"
    assert fills[0]["price"] == Decimal("8")


def test_cash_carries_to_the_next_day() -> None:
    fees = schedule()
    bars = {
        ("AAA", "2026-10-01"): {"open": 10, "high": 10, "low": 10, "close": 10},
        ("AAA", "2026-10-02"): {"open": 12, "high": 12, "low": 12, "close": 12},
    }

    def bars_for(day, tickers):
        return {ticker: bars[(ticker, day)] for ticker in tickers if (ticker, day) in bars}

    early = datetime(2026, 10, 1, 8, 0, tzinfo=ET)
    plans = [
        {
            "date": "2026-10-01",
            "source": "plan.json",
            "commit": "abc",
            "commit_et": early.isoformat(),
            "picks": [Pick("AAA", "long", hold_sessions=1)],
            "exits": [],
            "reason": "",
            "tradable": True,
            "note": "",
            "borrow": "",
        },
        {
            "date": "2026-10-02",
            "source": "plan.json",
            "commit": "abc",
            "commit_et": early.isoformat(),
            "picks": [],
            "exits": ["AAA"],
            "reason": "",
            "tradable": True,
            "note": "",
            "borrow": "",
        },
    ]
    rows = run_book(
        "toy_webull_sim", plans, ["2026-10-01", "2026-10-02"],
        bars_for, fees, datetime(2026, 10, 6, 12, tzinfo=ET),
    )
    assert rows[0]["section"] == "built_after"
    assert rows[0]["locked_trade"] is False
    assert rows[1]["section"] == "built_after"
    assert Decimal(rows[1]["cash"]) > START_CASH
    assert rows[0]["cash"] != rows[1]["cash"]


def test_locked_section_does_not_inherit_built_after_pnl() -> None:
    fees = schedule()
    bars = {
        ("AAA", "2026-10-02"): {"open": 10, "high": 10, "low": 10, "close": 10},
        ("AAA", "2026-10-06"): {"open": 10, "high": 10, "low": 10, "close": 11},
    }

    def bars_for(day, tickers):
        return {ticker: bars[(ticker, day)] for ticker in tickers if (ticker, day) in bars}

    when = datetime(2026, 10, 2, 8, tzinfo=ET)
    plans = []
    for day in ("2026-10-02", "2026-10-06"):
        plans.append({
            "date": day,
            "source": "p",
            "commit": "abc",
            "commit_et": when.isoformat(),
            "picks": [Pick("AAA", "long")],
            "exits": [],
            "reason": "",
            "tradable": True,
            "note": "",
            "borrow": "",
        })
    rows = run_book(
        "toy_webull_sim", plans, ["2026-10-02", "2026-10-06"],
        bars_for, fees, datetime(2026, 10, 6, 12, tzinfo=ET),
    )
    assert rows[0]["section"] == "built_after"
    assert rows[1]["section"] == "locked"
    shares = whole_shares(START_CASH, Decimal("10"), fees, "buy")
    spent = Decimal("10") * shares + fee_for(fees, "buy", shares, Decimal("10"))
    assert Decimal(rows[1]["cash"]) == START_CASH - spent
    assert rows[1]["positions"][0]["shares"] == shares
    assert Decimal(rows[1]["equity"]) > Decimal(rows[0]["equity"])


def test_sit_and_late_commit_are_visible_and_not_locked_trades() -> None:
    when = datetime(2026, 10, 6, 9, 41, tzinfo=ET)
    reason, tradable = classify("2026-10-06", False, when, [Pick("AAA", "long")], [])
    assert reason == "plan committed 09:41 ET"
    assert tradable is False
    sit, ok = classify("2026-10-06", True, datetime(2026, 10, 6, 8, tzinfo=ET), [], [])
    assert sit == "sat out, 0 picks"
    assert ok is False
    missing, _ok = classify("2026-10-06", False, None, [], [])
    assert missing == "no pre-09:30 plan"
    assert section_for("2026-10-06", False) == "not_a_locked_trade"
    assert section_for("2026-10-05", True) == "built_after"
    assert before_open(datetime(2026, 10, 5, 22, 29, tzinfo=ET), "2026-10-06")


def test_past_day_lock_rejects_a_sealed_edit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(past_day_lock, "WEBULL_DIR", tmp_path)
    manifest = tmp_path / "manifest.jsonl"
    row = make_row(
        name="toy_webull_sim", day=FIRST_LOCKED, source="p", commit="abc",
        commit_et="2026-10-06T08:00:00-04:00", reason="sat out, 0 picks",
        note="", section="not_a_locked_trade", locked_trade=False, final=True,
        cash=START_CASH, fees=Decimal("0"), equity=START_CASH, fills=[],
        positions=[], picks=0,
    )
    write_books([row], seal=False, path=tmp_path / "days.jsonl", manifest=manifest)
    seal_row(row, manifest=manifest)
    changed = dict(row)
    changed["reason"] = "traded"
    try:
        write_books([changed], seal=True, path=tmp_path / "days.jsonl", manifest=manifest)
    except past_day_lock.PastDayLockError as exc:
        assert "toy_webull_sim" in str(exc)
    else:
        raise AssertionError("sealed edit was accepted")
    # A day before the watermark is not fingerprinted.
    early = dict(row)
    early["date"] = "2026-10-02"
    early["section"] = "built_after"
    seal_row(early, manifest=manifest)
    days = [item.get("date") for item in past_day_lock.load_manifest(manifest) if item.get("kind") == "day"]
    assert days == [FIRST_LOCKED]


def test_missed_day_stays_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(past_day_lock, "WEBULL_DIR", tmp_path)
    manifest = tmp_path / "manifest.jsonl"
    first = make_row(
        name="toy_webull_sim", day="2026-10-07", source="p", commit="abc",
        commit_et="2026-10-07T08:00:00-04:00", reason="sat out, 0 picks",
        note="", section="not_a_locked_trade", locked_trade=False, final=True,
        cash=START_CASH, fees=Decimal("0"), equity=START_CASH, fills=[],
        positions=[], picks=0,
    )
    write_books([first], seal=False, path=tmp_path / "days.jsonl", manifest=manifest)
    seal_row(first, manifest=manifest)
    missed = dict(first)
    missed["date"] = "2026-10-06"
    try:
        seal_row(missed, manifest=manifest)
    except past_day_lock.PastDayLockError as exc:
        assert "missed" in str(exc).lower() or "before" in str(exc).lower()
    else:
        raise AssertionError("a missed day was sealed")


def test_excel_final_file_counts_and_is_not_the_card() -> None:
    text = (ROOT / "excel_bot/daily/2026-10-05_excel_bot.md").read_text(encoding="utf-8")
    grouped = parse_excel(text)
    assert sum(len(picks) for picks in grouped.values()) == 66
    assert len(grouped["L1_long_green_tp8_lowvol"]) == 12
    assert len(grouped["L2_long_green_tp3_lowvol"]) == 12
    assert len(grouped["L3_long_green_hold2_midcap"]) == 34
    assert len(grouped["L5_long_green_hold2_midhibeta"]) == 8
    assert "L4_long_green_hold8_bbailike" not in grouped
    assert grouped["L1_long_green_tp8_lowvol"][0].target_pct == Decimal("0.08")
    assert grouped["L3_long_green_hold2_midcap"][0].hold_sessions == 2
    card = json.loads((ROOT / "excel_bot/strategies/L1_long_green_tp8_lowvol/card.json").read_text())
    assert "CLOSE" in card["spec"]["entry_rule"]


def test_theme_radar_borrow_is_not_invented() -> None:
    text = (
        "cell,date,ticker,entry,hold_days,exit_date,short_ret,short_ret_fee_only,"
        "short_ret_fee_borrow,tape,feature_date,source,meta\n"
        "cell_a,2026-10-05,AAA,2026-10-05,2,2026-10-07,0.1,0.09,0.08,down,2026-10-02,shadow,\n"
    )
    when = datetime(2026, 10, 4, 22, tzinfo=ET)
    books = parse_theme_log(text, "abc123", when)
    name = book_name("theme_radar_cell_a")
    plan = books[name][0]
    assert plan["borrow"] == "borrow not modeled"
    assert plan["picks"][0].side == "short"
    assert plan["picks"][0].hold_sessions == 2
    assert plan["tradable"] is True
    assert "paper account" in plan["note"]


def test_h1_sandbox_fill_not_observed() -> None:
    payload = json.loads((ROOT / "data/paper_open/2026-10-05_status.json").read_text())
    assert sandbox_label(payload) == "not observed"
    assert sandbox_label(None) == "not observed"
    assert sandbox_label({"fill_status": "filled", "sent": []}) == "not observed"


def test_freeze_window() -> None:
    assert in_write_freeze(datetime(2026, 10, 6, 9, 0, tzinfo=ET))
    assert in_write_freeze(datetime(2026, 10, 6, 3, 0, tzinfo=ET))
    assert not in_write_freeze(datetime(2026, 10, 6, 9, 40, tzinfo=ET))
    assert not in_write_freeze(datetime(2026, 10, 6, 2, 59, tzinfo=ET))
    assert final_row("2026-10-06", datetime(2026, 10, 6, 9, 0, tzinfo=ET), True, True) is False
    assert final_row("2026-10-06", datetime(2026, 10, 6, 9, 45, tzinfo=ET), False, True) is True
    assert live_fetch_allowed(datetime(2026, 10, 6, 9, 0, tzinfo=ET), "2026-10-06") is False
    assert live_fetch_allowed(datetime(2026, 10, 6, 9, 36, tzinfo=ET), "2026-10-06") is True


def test_same_day_rerun_matches(tmp_path: Path) -> None:
    row = make_row(
        name="toy_webull_sim", day="2026-10-02", source="p", commit="abc",
        commit_et="2026-10-02T08:00:00-04:00", reason="sat out, 0 picks",
        note="", section="built_after", locked_trade=False, final=True,
        cash=START_CASH, fees=Decimal("0"), equity=START_CASH, fills=[],
        positions=[], picks=0,
    )
    path = tmp_path / "days.jsonl"
    write_books([row], seal=False, path=path)
    changed = dict(row)
    changed["cash"] = q(Decimal("1"))
    write_books([changed], seal=False, path=path)
    kept = json.loads(path.read_text().splitlines()[0])
    assert kept["cash"] == q(START_CASH)


def test_workflow_is_after_the_freeze_and_on_ubuntu() -> None:
    text = (ROOT / ".github/workflows/webull_sim.yml").read_text(encoding="utf-8")
    assert "ubuntu-latest" in text
    assert "workflow_dispatch" in text
    assert "45 13 * * 1-5" in text
    assert "OpenClaw" not in text
    assert "webull_sim" in text


if __name__ == "__main__":
    import tempfile

    class _Patch:
        def setattr(self, obj, name, value):
            setattr(obj, name, value)

    test_published_fees_match_the_page()
    test_whole_shares_do_not_spend_past_cash()
    test_stop_fills_before_target_on_the_same_bar()
    test_gap_through_the_stop_fills_at_the_open()
    test_cash_carries_to_the_next_day()
    test_locked_section_does_not_inherit_built_after_pnl()
    test_sit_and_late_commit_are_visible_and_not_locked_trades()
    test_excel_final_file_counts_and_is_not_the_card()
    test_theme_radar_borrow_is_not_invented()
    test_h1_sandbox_fill_not_observed()
    test_freeze_window()
    test_workflow_is_after_the_freeze_and_on_ubuntu()
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        test_past_day_lock_rejects_a_sealed_edit(folder, _Patch())
    with tempfile.TemporaryDirectory() as tmp:
        test_missed_day_stays_missing(Path(tmp), _Patch())
    with tempfile.TemporaryDirectory() as tmp:
        test_same_day_rerun_matches(Path(tmp))
    print("ok")
