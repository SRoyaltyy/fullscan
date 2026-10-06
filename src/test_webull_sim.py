"""Webull sim: fees, whole shares, stop-first, lock split, provenance."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from src import past_day_lock
from unittest import mock

from src.webull_sim import (
    live_fetch_allowed,
    FEES_PATH,
    FIRST_LOCKED,
    START_CASH,
    THEME_BORROW_NOTE,
    THEME_LOG_NOTE,
    Account,
    Pick,
    Schedule,
    apply_session,
    add_trading_days,
    before_open,
    book_name,
    classify,
    fee_for,
    final_row,
    hold_continuations,
    in_write_freeze,
    load_h1_plans,
    load_schedule,
    MISSING_OPEN_RULE,
    canon,
    github_api,
    is_final_run,
    load_theme_plans,
    official_opens,
    unsent_orders_label,
    make_row,
    group_theme_rows,
    parse_excel,
    parse_theme_log,
    q,
    render_html,
    render_md,
    resolve_sizing,
    run_book,
    sandbox_label,
    seal_row,
    section_for,
    simulate,
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
    slot = START_CASH / Decimal(20)
    shares = whole_shares(slot, Decimal("10"), fees, "buy")
    spent = Decimal("10") * shares + fee_for(fees, "buy", shares, Decimal("10"))
    assert Decimal(rows[1]["cash"]) == START_CASH - spent
    assert rows[1]["positions"][0]["shares"] == shares
    assert shares < whole_shares(START_CASH, Decimal("10"), fees, "buy")
    assert Decimal(rows[1]["equity"]) > Decimal(rows[0]["equity"])
    assert "max(20, 1)" in rows[1]["sizing"]
    assert "plan sets no priority" in rows[1]["sizing"]


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


def _flat_bars(tickers: list[str], day: str, price: str) -> dict:
    px = float(price)
    return {ticker: {"open": px, "high": px, "low": px, "close": px} for ticker in tickers}


def test_slot_is_equity_over_max_20_n() -> None:
    fees = schedule()
    account = Account()
    picks = [Pick(f"N{i}", "long") for i in range(7)]
    fills = apply_session(
        account, picks, [], _flat_bars([p.ticker for p in picks], "2026-10-01", "10"),
        fees, "2026-10-01", 0,
    )
    slot = START_CASH / Decimal(20)
    shares = whole_shares(slot, Decimal("10"), fees, "buy")
    assert shares == 49
    assert [fill["reason"] for fill in fills] == ["open"] * 7
    assert [fill["shares"] for fill in fills] == [shares] * 7
    assert [fill["ticker"] for fill in fills] == [p.ticker for p in picks]
    # Seven names do not split the book seven ways, and one name does not take it all.
    assert shares != whole_shares(START_CASH / Decimal(7), Decimal("10"), fees, "buy")
    assert account.cash > START_CASH - slot * 7


def test_thirty_four_picks_get_thirty_four_equal_slots() -> None:
    fees = schedule()
    # Listed Z-to-A so a ticker sort would disagree with the plan.
    picks = [Pick(f"T{i:02d}", "long") for i in range(33, -1, -1)]
    assert len(picks) == 34
    account = Account()
    fills = apply_session(
        account, picks, [], _flat_bars([p.ticker for p in picks], "2026-10-01", "10"),
        fees, "2026-10-01", 0,
    )
    slot = START_CASH / Decimal(34)
    shares = whole_shares(slot, Decimal("10"), fees, "buy")
    assert [fill["ticker"] for fill in fills] == [p.ticker for p in picks]
    assert [fill["reason"] for fill in fills] == ["open"] * 34
    assert [fill["shares"] for fill in fills] == [shares] * 34
    assert shares == whole_shares(START_CASH / Decimal(max(20, 34)), Decimal("10"), fees, "buy")
    reversed_picks = list(reversed(picks))
    other = Account()
    other_fills = apply_session(
        other, reversed_picks, [],
        _flat_bars([p.ticker for p in reversed_picks], "2026-10-01", "10"),
        fees, "2026-10-01", 0,
    )
    by_ticker = {fill["ticker"]: fill["shares"] for fill in fills}
    assert {fill["ticker"]: fill["shares"] for fill in other_fills} == by_ticker


def test_missing_open_does_not_drop_the_other_picks() -> None:
    fees = schedule()
    picks = [Pick("AAA", "long"), Pick("MISS", "long"), Pick("BBB", "long")]
    bars = _flat_bars(["AAA", "BBB"], "2026-10-01", "10")
    fills = apply_session(Account(), picks, [], bars, fees, "2026-10-01", 0)
    slot = START_CASH / Decimal(20)
    shares = whole_shares(slot, Decimal("10"), fees, "buy")
    assert [fill["ticker"] for fill in fills] == ["AAA", "MISS", "BBB"]
    assert fills[0]["reason"] == "open" and fills[0]["shares"] == shares
    assert fills[1]["reason"] == "open not observed" and fills[1]["shares"] == 0
    assert fills[2]["reason"] == "open" and fills[2]["shares"] == shares


def test_no_cash_only_from_held_lots() -> None:
    fees = schedule()
    from src.webull_sim import Lot
    held = Account(cash=Decimal("5"))
    held.lots.append(Lot(
        ticker="HELD", side="long", shares=100, entry_px=Decimal("100"),
        entry_date="2026-09-30",
    ))
    picks = [Pick("BBB", "long"), Pick("AAA", "long")]
    bars = _flat_bars(["HELD", "BBB", "AAA"], "2026-10-01", "10")
    bars["HELD"] = {"open": 100, "high": 100, "low": 100, "close": 100}
    fills = apply_session(held, picks, [], bars, fees, "2026-10-01", 1)
    assert [fill["ticker"] for fill in fills] == ["BBB", "AAA"]
    assert [fill["reason"] for fill in fills] == ["no cash", "no cash"]
    assert [fill["mark"] for fill in fills] == ["plan sets no priority", "plan sets no priority"]
    # A flat book whose slot cannot buy one share is not "no cash".
    flat = Account()
    pricey = apply_session(
        flat, [Pick("ZZZ", "long")], [],
        _flat_bars(["ZZZ"], "2026-10-01", "600"),
        fees, "2026-10-01", 0,
    )
    assert pricey[0]["reason"] == "no whole share"
    assert "mark" not in pricey[0]
    assert flat.lots == []


def test_own_count_precedence() -> None:
    fees = schedule()
    picks = [Pick(f"H{i}", "long") for i in range(4)]
    bars = _flat_bars([p.ticker for p in picks], "2026-10-01", "10")
    own = Account()
    fills = apply_session(own, picks, [], bars, fees, "2026-10-01", 0, sizing="own_count")
    slot = START_CASH / Decimal(4)
    shares = whole_shares(slot, Decimal("10"), fees, "buy")
    default_shares = whole_shares(START_CASH / Decimal(20), Decimal("10"), fees, "buy")
    assert shares > default_shares
    assert [fill["shares"] for fill in fills] == [shares] * 4
    assert resolve_sizing(picks, "own_count") == "own_count"
    weighted = [
        Pick("W1", "long", weight=Decimal("1")),
        Pick("W3", "long", weight=Decimal("3")),
    ]
    weighted_fills = apply_session(
        Account(), weighted, [], _flat_bars(["W1", "W3"], "2026-10-01", "10"),
        fees, "2026-10-01", 0,
    )
    assert weighted_fills[0]["shares"] == whole_shares(START_CASH / Decimal(4), Decimal("10"), fees, "buy")
    assert weighted_fills[1]["shares"] == whole_shares(START_CASH * Decimal(3) / Decimal(4), Decimal("10"), fees, "buy")
    assert resolve_sizing(weighted, "slot") == "own_weights"
    counted = [Pick("S1", "long", shares=10), Pick("S2", "long", shares=10)]
    counted_fills = apply_session(
        Account(), counted, [], _flat_bars(["S1", "S2"], "2026-10-01", "10"),
        fees, "2026-10-01", 0,
    )
    assert [fill["shares"] for fill in counted_fills] == [10, 10]
    assert resolve_sizing(counted, "slot") == "own_shares"


def test_theme_radar_late_first_appearance_is_not_a_fill() -> None:
    text = (
        "cell,date,ticker,entry,hold_days,exit_date,short_ret,short_ret_fee_only,"
        "short_ret_fee_borrow,tape,feature_date,source,meta\n"
        "cell_a,2026-10-05,AAA,2026-10-05,2,2026-10-07,0.1,0.09,0.08,down,2026-10-02,shadow,\n"
    )
    late = datetime(2026, 10, 5, 17, 0, tzinfo=ET)
    books = parse_theme_log(text, "latecommit", late)
    name = book_name("theme_radar_cell_a")
    plan = books[name][0]
    assert plan["reason"] == "no pre-09:30 plan"
    assert plan["tradable"] is False
    assert plan["commit"] == "latecommit"
    assert [pick.ticker for pick in plan["picks"]] == ["AAA"]
    early = datetime(2026, 10, 5, 8, 0, tzinfo=ET)
    early_plan = group_theme_rows([{
        "cell": "cell_a", "ticker": "BBB", "entry": "2026-10-05", "hold_days": "3",
        "_sha": "earlycommit", "_when": early,
    }])[name][0]
    assert early_plan["tradable"] is True
    assert early_plan["commit"] == "earlycommit"
    assert early_plan["picks"][0].hold_sessions == 3

    def bars_for(day, tickers):
        return {ticker: {"open": 10, "high": 10, "low": 10, "close": 10} for ticker in tickers}

    rows = run_book(
        name, [plan], ["2026-10-05"], bars_for, schedule(),
        datetime(2026, 10, 6, 12, tzinfo=ET),
    )
    assert rows[0]["section"] == "built_after"
    assert rows[0]["reason"] == "no pre-09:30 plan"
    assert rows[0]["fills"] == []
    assert Decimal(rows[0]["cash"]) == START_CASH
    assert rows[0]["picks"] == 1


EARLY_SHA = "ecdb627a7d3df2b8fd9470f9bab1aa4a77732336"
LATE_SHA = "ffffffffffffffffffffffffffffffffffffffff"
PLAN_PATH = "research/shadow_log/plans/plan_2026-10-06.csv"
FPE_10_06 = ["AXIL", "DAL", "FBK", "JPM", "LEVI", "LW", "NEOG", "PENG", "RELL", "UNH", "WFC", "WS"]
PLAN_CSV = "cell,signal_date,ticker,entry_date,hold_days,rules_sha256,source\n" + "".join(
    f"fpe_delta_t3_earn_today_3d,2026-10-05,{ticker},2026-10-06,3,abc,preopen_plan_2026-10-06\n"
    for ticker in FPE_10_06
) + """fresh_dcp_t1_ep_ge03_2d,2026-10-05,AIB,2026-10-06,2,abc,preopen_plan_2026-10-06
fresh_dcp_t1_ep_ge03_2d,2026-10-05,RIVN,2026-10-06,2,abc,preopen_plan_2026-10-06
fresh_dcp_t1_avoid_ah_3d,2026-10-05,AIB,2026-10-06,3,abc,preopen_plan_2026-10-06
fresh_dcp_t1_avoid_ah_3d,2026-10-05,RIVN,2026-10-06,3,abc,preopen_plan_2026-10-06
# status=fires rows=16 fpe_delta_t3_earn_today_3d=12 fresh_dcp_t1_ep_ge03_2d=2 fresh_dcp_t1_avoid_ah_3d=2
# built_at_utc=2026-10-06T10:20:11Z signal_date=2026-10-05 entry_date=2026-10-06
"""
NO_FIRES_CSV = """# status=no_fires
# built_at_utc=2026-10-07T10:20:11Z signal_date=2026-10-06 entry_date=2026-10-07
"""


def _commit(sha: str, when: str) -> dict:
    return {"sha": sha, "commit": {"committer": {"date": when}}}


def _theme_github(commits: list[dict], *, listing: list[dict] | None = None, text: str = PLAN_CSV):
    """Stand-in for the GitHub API. Newest commit is first, matching the API."""
    if listing is None:
        listing = [{"name": "plan_2026-10-06.csv", "type": "file"}]

    def github_api(path: str, accept: str = "") -> tuple[int, str]:
        if "log.csv" in path:
            raise AssertionError(path)
        if "commits?" in path:
            return 0, json.dumps(commits)
        if "?ref=" in path:
            return 0, text
        if path.endswith("research/shadow_log/plans"):
            return 0, json.dumps(listing)
        raise AssertionError(path)

    return github_api


def _flat_day(price: float) -> dict:
    return {"open": price, "high": price, "low": price, "close": price}


def test_theme_plan_first_commit_before_open_is_locked() -> None:
    # Newest-first. A later rewrite after the open must not hide the 06:20 ET commit.
    commits = [
        _commit(LATE_SHA, "2026-10-06T15:00:00Z"),
        _commit(EARLY_SHA, "2026-10-06T10:20:12Z"),
    ]
    with mock.patch("src.webull_sim.github_api", side_effect=_theme_github(commits)):
        books = load_theme_plans()
    name = book_name("theme_radar_fpe_delta_t3_earn_today_3d")
    plan = books[name][0]
    assert plan["tradable"] is True
    assert plan["reason"] == ""
    assert plan["commit"] == EARLY_SHA
    assert plan["commit_et"] == "2026-10-06T06:20:12-04:00"
    assert plan["source"] == f"SRoyaltyy/theme-radar:{PLAN_PATH}"
    assert [pick.ticker for pick in plan["picks"]] == FPE_10_06
    assert all(pick.side == "short" and pick.hold_sessions == 3 for pick in plan["picks"])
    assert plan["picks"][0].exit_on == "2026-10-09"
    ep = books[book_name("theme_radar_fresh_dcp_t1_ep_ge03_2d")][0]
    ah = books[book_name("theme_radar_fresh_dcp_t1_avoid_ah_3d")][0]
    assert [pick.ticker for pick in ep["picks"]] == ["AIB", "RIVN"]
    assert [pick.hold_sessions for pick in ep["picks"]] == [2, 2]
    assert [pick.ticker for pick in ah["picks"]] == ["AIB", "RIVN"]
    assert [pick.hold_sessions for pick in ah["picks"]] == [3, 3]
    assert len(plan["picks"]) == 12 and len(ep["picks"]) == 2 and len(ah["picks"]) == 2
    assert add_trading_days("2026-09-18", 3) == "2026-09-23"
    assert add_trading_days("2026-09-04", 1) == "2026-09-08"

    def bars_for(day, tickers):
        return {ticker: _flat_day(10) for ticker in tickers}

    rows = run_book(
        name, [plan], ["2026-10-06"], bars_for, schedule(),
        datetime(2026, 10, 6, 12, tzinfo=ET),
    )
    assert rows[0]["section"] == "locked"
    assert rows[0]["locked_trade"] is True
    assert rows[0]["final"] is False
    assert rows[0]["commit"] == EARLY_SHA
    assert rows[0]["commit_et"] == "2026-10-06T06:20:12-04:00"
    assert rows[0]["source"] == f"SRoyaltyy/theme-radar:{PLAN_PATH}"
    assert rows[0]["fills"][0]["side"] == "sell"
    assert rows[0]["fills"][0]["reason"] == "open"
    assert rows[0]["positions"][0]["side"] == "short"


def test_theme_plan_at_or_after_open_or_missing_is_not_a_fill() -> None:
    def run_at(when: str) -> dict:
        with mock.patch(
            "src.webull_sim.github_api",
            side_effect=_theme_github([_commit(LATE_SHA, when)]),
        ):
            books = load_theme_plans()
        name = book_name("theme_radar_fpe_delta_t3_earn_today_3d")
        plan = books[name][0]
        assert plan["reason"] == "no pre-09:30 plan"
        assert plan["tradable"] is False
        assert plan["commit"] == LATE_SHA
        assert plan["source"].endswith(PLAN_PATH)

        def bars_for(day, tickers):
            return {ticker: _flat_day(10) for ticker in tickers}

        rows = run_book(
            name, [plan], ["2026-10-06"], bars_for, schedule(),
            datetime(2026, 10, 6, 16, 15, tzinfo=ET),
        )
        assert rows[0]["section"] == "not_a_locked_trade"
        assert rows[0]["reason"] == "no pre-09:30 plan"
        assert rows[0]["fills"] == []
        assert Decimal(rows[0]["cash"]) == START_CASH
        assert rows[0]["picks"] == 12
        return rows[0]

    # 13:30 UTC is 09:30 ET. The lock is strict: at the open is not before it.
    run_at("2026-10-06T13:30:00Z")
    run_at("2026-10-06T13:41:00Z")

    listing = [
        {"name": "README.md", "type": "file"},
        {"name": "letters_2026-10-06.csv", "type": "file"},
    ]
    with mock.patch(
        "src.webull_sim.github_api",
        side_effect=_theme_github([], listing=listing),
    ):
        missing = load_theme_plans()
    for cell in (
        "fpe_delta_t3_earn_today_3d",
        "fresh_dcp_t1_ep_ge03_2d",
        "fresh_dcp_t1_avoid_ah_3d",
    ):
        plan = missing[book_name(f"theme_radar_{cell}")][0]
        assert plan["date"] == "2026-10-06"
        assert plan["reason"] == "no pre-09:30 plan"
        assert plan["tradable"] is False
        assert plan["commit"] == ""
        assert plan["picks"] == []


def test_aib_stays_in_both_theme_radar_books() -> None:
    commits = [_commit(EARLY_SHA, "2026-10-06T10:20:12Z")]
    with mock.patch("src.webull_sim.github_api", side_effect=_theme_github(commits)):
        books = load_theme_plans()
    ep = book_name("theme_radar_fresh_dcp_t1_ep_ge03_2d")
    ah = book_name("theme_radar_fresh_dcp_t1_avoid_ah_3d")
    ep_names = [pick.ticker for pick in books[ep][0]["picks"]]
    ah_names = [pick.ticker for pick in books[ah][0]["picks"]]
    assert ep_names == ["AIB", "RIVN"]
    assert ah_names == ["AIB", "RIVN"]
    assert books[ep][0]["picks"][0].exit_on == "2026-10-08"
    assert books[ah][0]["picks"][0].exit_on == "2026-10-09"

    def bars_for(day, tickers):
        return {ticker: _flat_day(10) for ticker in tickers}

    now = datetime(2026, 10, 6, 12, tzinfo=ET)
    ep_rows = run_book(ep, books[ep], ["2026-10-06"], bars_for, schedule(), now)
    ah_rows = run_book(ah, books[ah], ["2026-10-06"], bars_for, schedule(), now)
    assert [row["ticker"] for row in ep_rows[0]["positions"]] == ["AIB", "RIVN"]
    assert [row["ticker"] for row in ah_rows[0]["positions"]] == ["AIB", "RIVN"]
    assert ep_rows[0]["positions"][0]["shares"] == ah_rows[0]["positions"][0]["shares"] > 0
    assert ep_rows[0]["positions"][0]["side"] == "short"
    assert ah_rows[0]["positions"][0]["side"] == "short"


def test_short_pnl_sign_and_fees() -> None:
    fees = schedule()
    entry = "2026-10-06"
    exit_day = add_trading_days(entry, 2)
    assert exit_day == "2026-10-08"
    pick = Pick("AAA", "short", hold_sessions=2, exit_on=exit_day)
    plan = {
        "date": entry,
        "source": f"SRoyaltyy/theme-radar:{PLAN_PATH}",
        "commit": EARLY_SHA,
        "commit_et": "2026-10-06T06:20:12-04:00",
        "picks": [pick],
        "exits": [],
        "reason": "",
        "tradable": True,
        "note": "",
        "borrow": "borrow not modeled",
    }
    plans = hold_continuations([plan], exit_day)
    assert [item["date"] for item in plans] == [entry, "2026-10-07", exit_day]
    sessions = [item["date"] for item in plans]

    def down(day, tickers):
        close = {"2026-10-06": 10, "2026-10-07": 9, "2026-10-08": 8}[day]
        opened = 10 if day == entry else 9
        return {ticker: {"open": opened, "high": opened, "low": close, "close": close} for ticker in tickers}

    noon = datetime(2026, 10, 8, 12, tzinfo=ET)
    still = run_book("theme_radar_cell_webull_sim", plans, sessions, down, fees, noon)
    assert still[-1]["positions"]
    assert still[-1]["positions"][0]["side"] == "short"
    assert all(fill["reason"] != "hold" for row in still for fill in row["fills"])

    done = run_book(
        "theme_radar_cell_webull_sim", plans, sessions, down, fees,
        datetime(2026, 10, 8, 16, 15, tzinfo=ET),
    )
    opened = [fill for fill in done[0]["fills"] if fill["reason"] == "open"]
    covered = [fill for fill in done[-1]["fills"] if fill["reason"] == "hold"]
    assert len(opened) == 1 and len(covered) == 1
    shares = opened[0]["shares"]
    assert opened[0]["side"] == "sell"
    assert covered[0]["side"] == "buy"
    assert Decimal(covered[0]["price"]) == Decimal("8")
    entry_fee = fee_for(fees, "sell", shares, Decimal("10"))
    cover_fee = fee_for(fees, "buy", shares, Decimal("8"))
    assert Decimal(opened[0]["fee"]) == entry_fee
    assert Decimal(covered[0]["fee"]) == cover_fee
    # The sale is the entry, so the SEC fee is on that leg only. CAT is on both.
    assert entry_fee - cover_fee == fees.sec_per_dollar * Decimal("10") * shares
    profit = (Decimal("10") - Decimal("8")) * shares - entry_fee - cover_fee
    assert profit > 0
    assert Decimal(done[-1]["cash"]) == START_CASH + profit
    assert Decimal(done[-1]["fees"]) == entry_fee + cover_fee
    assert done[-1]["positions"] == []

    def up(day, tickers):
        close = 12 if day == exit_day else 10
        opened_px = 10
        return {ticker: {"open": opened_px, "high": close, "low": opened_px, "close": close} for ticker in tickers}

    lost = run_book(
        "theme_radar_cell_webull_sim", plans, sessions, up, fees,
        datetime(2026, 10, 8, 16, 15, tzinfo=ET),
    )
    loss_cover = [fill for fill in lost[-1]["fills"] if fill["reason"] == "hold"][0]
    assert Decimal(loss_cover["price"]) == Decimal("12")
    loss_fee = fee_for(fees, "buy", shares, Decimal("12"))
    loss = (Decimal("10") - Decimal("12")) * shares - entry_fee - loss_fee
    assert loss < 0
    assert Decimal(lost[-1]["cash"]) == START_CASH + loss

    pricey = apply_session(
        Account(), [Pick("ZZZ", "short")], [],
        {"ZZZ": _flat_day(600)}, fees, entry, 0,
    )
    assert pricey[0]["reason"] == "no whole share"
    assert pricey[0]["side"] == "sell"


def test_theme_hold_days_do_not_move_other_books() -> None:
    fees = schedule()
    toy = [
        {
            "date": "2026-10-06",
            "source": "toy.json",
            "commit": "abc",
            "commit_et": "2026-10-06T08:00:00-04:00",
            "picks": [Pick("AAA", "long", hold_sessions=3)],
            "exits": [],
            "reason": "",
            "tradable": True,
            "note": "",
            "borrow": "",
        },
        {
            "date": "2026-10-09",
            "source": "toy.json",
            "commit": "abc",
            "commit_et": "2026-10-09T08:00:00-04:00",
            "picks": [],
            "exits": [],
            "reason": "",
            "tradable": True,
            "note": "",
            "borrow": "",
        },
    ]
    theme = [{
        "date": "2026-10-06",
        "source": f"SRoyaltyy/theme-radar:{PLAN_PATH}",
        "commit": EARLY_SHA,
        "commit_et": "2026-10-06T06:20:12-04:00",
        "picks": [Pick("AIB", "short", hold_sessions=3, exit_on="2026-10-09")],
        "exits": [],
        "reason": "",
        "tradable": True,
        "note": "",
        "borrow": "borrow not modeled",
    }]
    bars = {}
    for day, px in (("2026-10-06", 10), ("2026-10-07", 10), ("2026-10-08", 10), ("2026-10-09", 11)):
        bars[("AAA", day)] = _flat_day(px)
    bars[("AIB", "2026-10-06")] = _flat_day(20)
    bars[("AIB", "2026-10-07")] = _flat_day(19)
    bars[("AIB", "2026-10-08")] = _flat_day(18)
    bars[("AIB", "2026-10-09")] = {"open": 18, "high": 18, "low": 16, "close": 16}
    now = datetime(2026, 10, 9, 16, 30, tzinfo=ET)
    alone = simulate({"toy_webull_sim": toy}, now, fees, bars)
    both = simulate(
        {"toy_webull_sim": toy, "theme_radar_fresh_dcp_t1_avoid_ah_3d_webull_sim": theme},
        now, fees, bars,
    )
    toy_alone = [row for row in alone if row["name"] == "toy_webull_sim"]
    toy_both = [row for row in both if row["name"] == "toy_webull_sim"]
    assert toy_alone == toy_both
    assert toy_both[-1]["positions"][0]["ticker"] == "AAA"
    theme_rows = [row for row in both if row["name"].startswith("theme_radar_")]
    cover = [fill for fill in theme_rows[-1]["fills"] if fill["reason"] == "hold"]
    assert Decimal(cover[0]["price"]) == Decimal("16")
    assert cover[0]["side"] == "buy"


def test_theme_radar_page_states_the_borrow_lines_under_each_book() -> None:
    def row(name: str) -> dict:
        return make_row(
            name=name, day=FIRST_LOCKED, source=f"SRoyaltyy/theme-radar:{PLAN_PATH}",
            commit=EARLY_SHA, commit_et="2026-10-06T06:20:12-04:00",
            reason="", note="", section="locked", locked_trade=True, final=False,
            cash=START_CASH, fees=Decimal("0"), equity=START_CASH, fills=[],
            positions=[], picks=1,
        )

    other = row("h1_webull_sim")
    plain = render_md([other], schedule())
    plain_html = render_html([other], schedule())
    assert THEME_BORROW_NOTE not in plain
    assert THEME_LOG_NOTE not in plain
    assert THEME_BORROW_NOTE not in plain_html
    ep = book_name("theme_radar_fresh_dcp_t1_ep_ge03_2d")
    ah = book_name("theme_radar_fresh_dcp_t1_avoid_ah_3d")
    md = render_md([other, row(ep), row(ah)], schedule())
    html = render_html([other, row(ep), row(ah)], schedule())
    head, tail = md.split("## Theme Radar short books", 1)
    assert "h1_webull_sim" in head
    assert ep not in head and ah not in head
    assert md.count(THEME_BORROW_NOTE) == 2
    assert md.count(THEME_LOG_NOTE) == 2
    for name in sorted((ep, ah)):
        at = tail.index(f"### {name}")
        borrow_at = tail.index(THEME_BORROW_NOTE, at)
        log_at = tail.index(THEME_LOG_NOTE, at)
        assert at < borrow_at < log_at
        rest = tail[log_at + len(THEME_LOG_NOTE):]
        if "### " in rest:
            assert borrow_at < tail.index("### ", at + 4)
    assert html.count(THEME_BORROW_NOTE) == 2
    assert html.count(THEME_LOG_NOTE) == 2
    for name in (ep, ah):
        at = html.index(f"<h2>{name}</h2>")
        assert at < html.index(THEME_BORROW_NOTE, at) < html.index(THEME_LOG_NOTE, at)


def test_low_ticker_does_not_drop_the_yahoo_open() -> None:
    """LOW/HIGH/OPEN are real tickers and also OHLC field names."""
    import pandas as pd
    from src.price_store import _flatten_yf

    index = pd.to_datetime(["2026-10-06"])
    columns = pd.MultiIndex.from_tuples([
        ("Open", "AAPL"), ("High", "AAPL"), ("Low", "AAPL"), ("Close", "AAPL"), ("Volume", "AAPL"),
        ("Open", "LOW"), ("High", "LOW"), ("Low", "LOW"), ("Close", "LOW"), ("Volume", "LOW"),
    ])
    raw = pd.DataFrame(
        [[332.3, 334.0, 330.0, 333.0, 100, 20.0, 21.0, 19.0, 20.5, 50]],
        index=index, columns=columns,
    )
    flat = _flatten_yf(raw, ["AAPL", "LOW"])
    assert set(flat["ticker"]) == {"AAPL", "LOW"}
    apple = flat[flat["ticker"] == "AAPL"].iloc[0]
    assert float(apple["open"]) == 332.3
    # group_by="ticker" puts the name on level 0. LOW must not flip that either.
    grouped = pd.DataFrame(
        [[332.3, 334.0, 330.0, 333.0, 100, 20.0, 21.0, 19.0, 20.5, 50]],
        index=index,
        columns=pd.MultiIndex.from_tuples([
            ("AAPL", "Open"), ("AAPL", "High"), ("AAPL", "Low"), ("AAPL", "Close"), ("AAPL", "Volume"),
            ("LOW", "Open"), ("LOW", "High"), ("LOW", "Low"), ("LOW", "Close"), ("LOW", "Volume"),
        ]),
    )
    again = _flatten_yf(grouped, ["AAPL", "LOW"])
    assert float(again[again["ticker"] == "AAPL"].iloc[0]["open"]) == 332.3


def test_unexplained_jump_is_not_used_and_a_missing_open_is_not_guessed() -> None:
    import pandas as pd

    flat = pd.DataFrame([
        {"date": "2026-10-03", "ticker": "SDEV", "open": 3.0, "high": 3.2, "low": 2.9, "close": 3.1, "volume": 1},
        {"date": "2026-10-06", "ticker": "SDEV", "open": 3.48, "high": 3.8, "low": 3.2, "close": 3.54, "volume": 1},
        {"date": "2026-10-03", "ticker": "JUMP", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1},
        {"date": "2026-10-06", "ticker": "JUMP", "open": 40.0, "high": 40.0, "low": 40.0, "close": 40.0, "volume": 1},
        {"date": "2026-10-06", "ticker": "NEW", "open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0, "volume": 1},
        {"date": "2026-10-03", "ticker": "SPLIT", "open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0, "volume": 1},
        {"date": "2026-10-06", "ticker": "SPLIT", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1},
    ])
    actions = pd.DataFrame([
        {"date": "2026-10-06", "ticker": "SPLIT", "split": 2.0},
    ])
    got = official_opens(flat, actions, "2026-10-06")
    assert got[("SDEV", "2026-10-06")]["open"] == 3.48
    assert got[("NEW", "2026-10-06")]["open"] == 5.0
    assert ("JUMP", "2026-10-06") not in got
    assert got[("SPLIT", "2026-10-06")]["open"] == 10.0
    assert "GONE" not in {ticker for ticker, _day in got}
    # The jump is not a price. The 16:15 run logs it as not filled
    # (test_final_run_locks_the_other_picks_when_one_open_is_missing).
    close = datetime(2026, 10, 6, 16, 15, tzinfo=ET)
    assert final_row("2026-10-06", close, True, False) is False
    assert final_row("2026-10-06", close, True, True) is True


def test_h1_ledger_plan_fills_at_the_open_and_flatten_h1_sat_out() -> None:
    plans = [plan for plan in load_h1_plans() if plan["date"] == "2026-10-06"]
    assert len(plans) == 1
    plan = plans[0]
    assert [pick.ticker for pick in plan["picks"]] == ["SDEV", "PACB", "DNA", "QSI"]
    assert plan["exits"] == ["FEAM", "GLND", "NAUT"]
    assert plan["commit"].startswith("dcec598c3")
    assert plan["tradable"] is True
    assert plan["commit_et"].startswith("2026-10-06T08:41")
    tickets = json.loads((ROOT / "data/day_board/2026-10-06_strategy_tickets.json").read_text())
    flatten = tickets["strategies"]["flatten_h1"]
    assert flatten["buy"] == [] and flatten["sell"] == []
    assert "h1" not in tickets["strategies"]
    bars = {}
    for ticker, price in (("SDEV", 3.48), ("PACB", 2.10), ("DNA", 4.00), ("QSI", 1.50)):
        bars[(ticker, "2026-10-06")] = {
            "open": price, "high": price, "low": price, "close": price,
        }
    # No open for a fifth name, and the research sells have no sim lot.
    rows = simulate(
        {"h1_webull_sim": [plan]},
        datetime(2026, 10, 6, 10, 15, tzinfo=ET),
        schedule(),
        bars,
    )
    row = rows[0]
    assert row["name"] == "h1_webull_sim"
    assert row["section"] == "locked"
    assert row["reason"] == ""
    filled = {fill["ticker"]: fill for fill in row["fills"] if fill.get("shares")}
    assert set(filled) == {"SDEV", "PACB", "DNA", "QSI"}
    assert all(fill["side"] == "buy" for fill in filled.values())
    assert all(fill["reason"] == "open" for fill in filled.values())
    assert "FEAM" not in filled and "GLND" not in filled and "NAUT" not in filled
    # The live 10-06 journal has acknowledged order ids, so the column does
    # not claim that no orders went out. The missed-deadline sentence is the
    # overlay for a status file that actually says that.
    live = json.loads((ROOT / "data/paper_open/2026-10-06_status.json").read_text())
    assert any(str(order.get("order_id") or "").strip() for order in live.get("sent") or [])
    assert unsent_orders_label(live) == ""
    sentence = (
        "No Webull orders sent on 2026-10-06: "
        "send started after the 09:30 open (missed deadline)"
    )
    missed = {"date": "2026-10-06", "status": "missed_deadline"}
    assert unsent_orders_label(missed) == sentence
    late_send = {
        "date": "2026-10-06",
        "status": "failed",
        "sent": [{"status": "missed_deadline", "ok": False, "order_id": ""}],
    }
    assert unsent_orders_label(late_send) == sentence
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        (folder / "2026-10-06_status.json").write_text(json.dumps(missed), encoding="utf-8")
        page = render_md(
            [row], schedule(),
            manifest={"lock_from": "2026-10-06", "entries": []},
            paper_open=folder,
        )
    assert "h1_webull_sim" in page
    assert sentence in page
    stored = json.dumps(row, sort_keys=True)
    quiet = render_md(
        [row], schedule(),
        manifest={"lock_from": "2026-10-06", "entries": []},
        paper_open=ROOT / "data/paper_open",
    )
    assert "No Webull orders sent" not in quiet
    assert json.dumps(row, sort_keys=True) == stored


def test_excel_pre_lock_label_and_pages_ship_webull_sim() -> None:
    row = make_row(
        name="L1_long_green_tp8_lowvol_webull_sim", day="2026-10-06",
        source="excel_bot/daily/2026-10-05_excel_bot.md", commit="caf155612ba7",
        commit_et="2026-10-05T18:29:10-04:00", reason="open not observed", note="",
        section="not_a_locked_trade", locked_trade=False, final=False,
        cash=START_CASH, fees=Decimal("0"), equity=START_CASH, fills=[],
        positions=[], picks=12,
    )
    before = json.dumps(row, sort_keys=True)
    phrase = "sealed by git commit time (pre-lock)"
    md = render_md([row], schedule(), manifest={"lock_from": "2026-10-06", "entries": []},
                   paper_open=Path("/no/such/paper"))
    assert f"excel_bot/daily/2026-10-05_excel_bot.md — {phrase}" in md
    listed = {"lock_from": "2026-10-06", "entries": [{"signal_date": "2026-10-06"}]}
    later = dict(row)
    later["source"] = "excel_bot/daily/2026-10-06_excel_bot.md"
    later["date"] = "2026-10-07"
    sealed = render_md([later], schedule(), manifest=listed, paper_open=Path("/no/such/paper"))
    assert "sealed by git commit time + excel_bot freeze manifest" in sealed
    quiet = render_md([later], schedule(), manifest={"lock_from": "2026-10-06", "entries": []},
                      paper_open=Path("/no/such/paper"))
    assert "freeze manifest" not in quiet
    assert json.dumps(row, sort_keys=True) == before
    deploy = (ROOT / ".github/workflows/deploy-dashboard.yml").read_text(encoding="utf-8")
    publish = (ROOT / "scripts/publish_dashboard.sh").read_text(encoding="utf-8")
    assert "webull-sim" in deploy
    assert "webull-sim" in publish


def _session_plan(day: str, picks: list[Pick], exits: list[str] | None = None) -> dict:
    return {
        "date": day,
        "source": "data/day_board/x.json",
        "commit": "abc",
        "commit_et": f"{day}T08:00:00-04:00",
        "picks": picks,
        "exits": exits or [],
        "reason": "",
        "tradable": True,
        "note": "",
        "borrow": "",
        "sizing": "slot",
    }


def test_final_run_locks_the_other_picks_when_one_open_is_missing() -> None:
    """16:15 ET is the final run. 10:15 still retries. A 3x jump is a missing open."""
    import pandas as pd

    assert is_final_run(datetime(2026, 10, 6, 9, 45, tzinfo=ET), "2026-10-06") is False
    assert is_final_run(datetime(2026, 10, 6, 10, 15, tzinfo=ET), "2026-10-06") is False
    assert is_final_run(datetime(2026, 10, 6, 16, 15, tzinfo=ET), "2026-10-06") is True
    picks = [Pick(f"N{i:02d}", "long") for i in range(15)]
    missing = "N07"

    def bars_for(day, tickers):
        return {ticker: _flat_day(10) for ticker in tickers if ticker != missing}

    plan = _session_plan("2026-10-06", picks)
    early = run_book(
        "1d_top_webull_sim", [plan], ["2026-10-06"], bars_for, schedule(),
        datetime(2026, 10, 6, 10, 15, tzinfo=ET),
    )[0]
    assert early["section"] == "not_a_locked_trade"
    assert early["final"] is False
    assert early["locked_trade"] is False
    missed = next(fill for fill in early["fills"] if fill["ticker"] == missing)
    assert missed["reason"] == "open not observed" and missed["shares"] == 0
    filled = [fill for fill in early["fills"] if fill.get("shares")]
    assert len(filled) == 14

    late = run_book(
        "1d_top_webull_sim", [plan], ["2026-10-06"], bars_for, schedule(),
        datetime(2026, 10, 6, 16, 15, tzinfo=ET),
    )[0]
    assert late["section"] == "locked"
    assert late["final"] is True
    assert late["locked_trade"] is True
    assert late["reason"] == ""
    missed = next(fill for fill in late["fills"] if fill["ticker"] == missing)
    assert missed["reason"] == "no open, not filled" and missed["shares"] == 0
    assert len([fill for fill in late["fills"] if fill.get("shares")]) == 14
    assert "no open, not filled: N07" in late["note"]
    assert missing not in {row["ticker"] for row in late["positions"]}
    page = render_md([late], schedule(), manifest={"lock_from": "2026-10-06", "entries": []},
                     paper_open=Path("/no/such/paper"))
    html = render_html([late], schedule(), manifest={"lock_from": "2026-10-06", "entries": []},
                       paper_open=Path("/no/such/paper"))
    assert MISSING_OPEN_RULE in page and MISSING_OPEN_RULE in html
    assert "no open, not filled: N07" in page and "no open, not filled: N07" in html

    # A held lot whose exit open is missing stays held and is logged.
    held = [Pick("HELD", "long")]
    nxt = [Pick("BBB", "long")]

    def two_days(day, tickers):
        if day == "2026-10-06":
            return {ticker: _flat_day(10) for ticker in tickers}
        return {ticker: _flat_day(10) for ticker in tickers if ticker != "HELD"}

    rows = run_book(
        "demo_webull_sim",
        [
            _session_plan("2026-10-06", held),
            _session_plan("2026-10-07", nxt, ["HELD"]),
        ],
        ["2026-10-06", "2026-10-07"],
        two_days, schedule(),
        datetime(2026, 10, 7, 16, 15, tzinfo=ET),
    )
    second = rows[1]
    assert second["section"] == "locked" and second["final"] is True
    assert any(row["ticker"] == "HELD" and row["shares"] > 0 for row in second["positions"])
    logged = next(fill for fill in second["fills"] if fill["ticker"] == "HELD")
    assert logged["reason"] == "no open, not filled" and logged["shares"] == 0
    assert "no open, not filled: HELD" in second["note"]
    bought = next(fill for fill in second["fills"] if fill["ticker"] == "BBB")
    assert bought["reason"] == "open" and bought["shares"] > 0

    flat = pd.DataFrame([
        {"date": "2026-10-03", "ticker": "AAA", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1},
        {"date": "2026-10-06", "ticker": "AAA", "open": 10.2, "high": 10.2, "low": 10.2, "close": 10.2, "volume": 1},
        {"date": "2026-10-03", "ticker": "JUMP", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1},
        {"date": "2026-10-06", "ticker": "JUMP", "open": 40.0, "high": 40.0, "low": 40.0, "close": 40.0, "volume": 1},
    ])
    opens = official_opens(flat, None, "2026-10-06")
    assert ("JUMP", "2026-10-06") not in opens

    def jump_bars(day, tickers):
        return {ticker: opens[(ticker, day)] for ticker in tickers if (ticker, day) in opens}

    jump = run_book(
        "1d_top_webull_sim",
        [_session_plan("2026-10-06", [Pick("AAA", "long"), Pick("JUMP", "long")])],
        ["2026-10-06"], jump_bars, schedule(),
        datetime(2026, 10, 6, 16, 15, tzinfo=ET),
    )[0]
    assert jump["section"] == "locked"
    assert next(fill for fill in jump["fills"] if fill["ticker"] == "JUMP")["reason"] == "no open, not filled"
    assert next(fill for fill in jump["fills"] if fill["ticker"] == "AAA")["shares"] > 0

    # A book whose opens all printed does not change between 10:15 and 16:15.
    whole = _session_plan("2026-10-06", [Pick("AAA", "long"), Pick("BBB", "long")])

    def all_bars(day, tickers):
        return {ticker: _flat_day(10) for ticker in tickers}

    at_1015 = run_book(
        "h1_webull_sim", [whole], ["2026-10-06"], all_bars, schedule(),
        datetime(2026, 10, 6, 10, 15, tzinfo=ET),
    )[0]
    at_1615 = run_book(
        "h1_webull_sim", [whole], ["2026-10-06"], all_bars, schedule(),
        datetime(2026, 10, 6, 16, 15, tzinfo=ET),
    )[0]
    assert canon(at_1015) == canon(at_1615)
    assert at_1615["locked_trade"] is True and at_1615["final"] is True


def test_no_fires_sits_out_and_history_starts_on_10_06() -> None:
    import os
    import subprocess

    fires = _theme_github([_commit(EARLY_SHA, "2026-10-06T10:20:12Z")], text=NO_FIRES_CSV)
    with mock.patch("src.webull_sim.github_api", side_effect=fires):
        books = load_theme_plans()
    for cell in (
        "fpe_delta_t3_earn_today_3d",
        "fresh_dcp_t1_ep_ge03_2d",
        "fresh_dcp_t1_avoid_ah_3d",
    ):
        plan = books[book_name(f"theme_radar_{cell}")][0]
        assert plan["date"] == "2026-10-06"
        assert plan["reason"] == "sat out, 0 picks"
        assert plan["tradable"] is False
        assert plan["picks"] == []
        assert plan["commit"] == EARLY_SHA
        assert "status=no_fires" in plan["note"]
    late = _theme_github([_commit(LATE_SHA, "2026-10-06T15:00:00Z")], text=NO_FIRES_CSV)
    with mock.patch("src.webull_sim.github_api", side_effect=late):
        late_books = load_theme_plans()
    late_plan = late_books[book_name("theme_radar_fpe_delta_t3_earn_today_3d")][0]
    assert late_plan["reason"] == "no pre-09:30 plan"
    assert late_plan["tradable"] is False

    listing = [
        {"name": "plan_2026-10-03.csv", "type": "file"},
        {"name": "plan_2026-10-06.csv", "type": "file"},
    ]

    def github_api(path: str, accept: str = "") -> tuple[int, str]:
        if "plan_2026-10-03.csv" in path:
            raise AssertionError(path)
        return _theme_github([_commit(EARLY_SHA, "2026-10-06T10:20:12Z")], listing=listing)(path, accept)

    with mock.patch("src.webull_sim.github_api", side_effect=github_api):
        dated = load_theme_plans()
    days = {plan["date"] for plans in dated.values() for plan in plans}
    assert "2026-10-03" not in days
    assert "2026-10-06" in days

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
        folder = root / "research" / "shadow_log" / "plans"
        folder.mkdir(parents=True)
        (folder / "plan_2026-10-03.csv").write_text(PLAN_CSV, encoding="utf-8")
        (folder / "plan_2026-10-06.csv").write_text(PLAN_CSV, encoding="utf-8")
        (folder / "plan_2026-10-07.csv").write_text(NO_FIRES_CSV, encoding="utf-8")
        env = os.environ.copy()
        env.update({
            "GIT_AUTHOR_DATE": "2026-10-06T10:20:12Z",
            "GIT_COMMITTER_DATE": "2026-10-06T10:20:12Z",
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        })
        subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True, env=env)
        subprocess.run(["git", "commit", "-m", "plans"], cwd=root, check=True, capture_output=True, env=env)
        with mock.patch.dict(os.environ, {"THEME_RADAR_DIR": str(root)}):
            local = load_theme_plans()
    fpe = local[book_name("theme_radar_fpe_delta_t3_earn_today_3d")]
    assert [plan["date"] for plan in fpe] == ["2026-10-06", "2026-10-07"]
    assert [pick.ticker for pick in fpe[0]["picks"]] == FPE_10_06
    assert fpe[1]["reason"] == "sat out, 0 picks"
    assert "2026-10-03" not in {plan["date"] for plans in local.values() for plan in plans}


def test_theme_radar_fetch_does_not_send_the_actions_token() -> None:
    captured = {}

    def run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")

        class _Proc:
            returncode = 0
            stdout = "[]\n200"
            stderr = ""

        return _Proc()

    with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "secret", "GH_TOKEN": "secret"}):
        with mock.patch("src.webull_sim.subprocess.run", side_effect=run):
            github_api("repos/SRoyaltyy/theme-radar/contents/research/shadow_log/plans")
    assert captured["cmd"][0] == "curl"
    assert "secret" not in " ".join(captured["cmd"])
    assert captured["env"] is not None
    assert "GITHUB_TOKEN" not in captured["env"]
    assert "GH_TOKEN" not in captured["env"]


def test_a_final_row_is_copied_when_the_close_moves() -> None:
    """A sealed row stays byte-identical after a new field and a new close."""
    picks = [Pick("AAA", "long", target_pct=Decimal("0.08"))]
    plan = _session_plan("2026-10-06", picks)
    plan["note"] = "original rule"

    def bars_for(day, names):
        return {ticker: {"open": 10, "high": 10, "low": 10, "close": 10} for ticker in names}

    early = run_book(
        "toy_webull_sim", [plan], ["2026-10-06"], bars_for, schedule(),
        datetime(2026, 10, 6, 16, 15, tzinfo=ET),
    )[0]
    assert early["final"] is True
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        path = folder / "days.jsonl"
        manifest = folder / "manifest.jsonl"
        write_books([early], seal=False, path=path, manifest=manifest)
        seal_row(early, manifest=manifest)
        original = path.read_text(encoding="utf-8").strip()
        later = _session_plan("2026-10-07", [Pick("BBB", "long")])
        changed = dict(plan)
        changed["note"] = "new rule note"
        bars = {}
        for day in ("2026-10-06", "2026-10-07"):
            for ticker in ("AAA", "BBB"):
                # 10-07 stays under the 8% target so the carried lot is still held.
                # 10-06's close is a different print; the sealed row must ignore it.
                bars[(ticker, day)] = {
                    "open": 10, "high": 10, "low": 10,
                    "close": 80 if day == "2026-10-06" else 10,
                }
        real_make = make_row

        def extra_field(**kwargs):
            row = real_make(**kwargs)
            row["rule_note"] = "added later"
            return row

        with mock.patch("src.webull_sim.make_row", side_effect=extra_field):
            rows = simulate(
                {"toy_webull_sim": [changed, later]},
                datetime(2026, 10, 7, 10, 15, tzinfo=ET),
                schedule(), bars, books_path=path,
            )
        kept = next(row for row in rows if row["date"] == "2026-10-06")
        nxt = next(row for row in rows if row["date"] == "2026-10-07")
        assert "rule_note" not in kept
        assert kept["note"] == "original rule"
        assert kept["equity"] == early["equity"]
        assert canon(kept) == original
        assert nxt.get("rule_note") == "added later"
        assert any(pos["ticker"] == "AAA" for pos in nxt["positions"])
        assert Decimal(nxt["cash"]) < Decimal(kept["cash"])
        write_books(rows, seal=True, path=path, manifest=manifest)
        assert original in path.read_text(encoding="utf-8")


def test_final_run_keeps_sealed_rows_and_locks_blfs_and_theme() -> None:
    """16:15 ET on the current books: the seal holds, BLFS locks, shorts fill."""
    import shutil
    import subprocess
    from src.webull_sim import collect_books, needed_universe

    ten = [
        "1d_top_webull_sim", "1m_top_webull_sim", "1w_top_webull_sim",
        "2w_top_webull_sim", "3d_top_webull_sim",
        "stock_book_1d_webull_sim", "stock_book_1m_webull_sim",
        "stock_book_1w_webull_sim", "stock_book_2w_webull_sim",
        "stock_book_3d_webull_sim",
    ]
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        days = folder / "days.jsonl"
        manifest = folder / "manifest.jsonl"
        shutil.copy(ROOT / "data/webull_sim/days.jsonl", days)
        shutil.copy(ROOT / "data/past_day_lock/manifest.jsonl", manifest)
        sealed_lines = {}
        for line in days.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row.get("final") and row.get("date") == "2026-10-06":
                sealed_lines[row["name"]] = line
        assert "L1_long_green_tp8_lowvol_webull_sim" in sealed_lines
        radar = folder / "theme-radar"
        radar.mkdir(parents=True)
        subprocess.run(["git", "init"], cwd=radar, check=True, capture_output=True)
        plan_dir = radar / "research" / "shadow_log" / "plans"
        plan_dir.mkdir(parents=True)
        (plan_dir / "plan_2026-10-06.csv").write_text(PLAN_CSV, encoding="utf-8")
        env = os.environ.copy()
        env.update({
            "GIT_AUTHOR_DATE": "2026-10-06T10:20:12Z",
            "GIT_COMMITTER_DATE": "2026-10-06T10:20:12Z",
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        })
        subprocess.run(["git", "add", "."], cwd=radar, check=True, capture_output=True, env=env)
        subprocess.run(["git", "commit", "-m", "plan"], cwd=radar, check=True, capture_output=True, env=env)
        with mock.patch.dict(os.environ, {"THEME_RADAR_DIR": str(radar)}):
            books = collect_books()
        tickers, _days = needed_universe(books)
        bars = {}
        for ticker in tickers:
            if ticker == "BLFS":
                continue
            bars[(ticker, "2026-10-06")] = {"open": 10, "high": 10, "low": 10, "close": 10}
        rows = simulate(
            books, datetime(2026, 10, 6, 16, 15, tzinfo=ET),
            schedule(), bars, books_path=days,
        )
        written = write_books(rows, seal=True, path=days, manifest=manifest)
        text = days.read_text(encoding="utf-8")
        for name, line in sealed_lines.items():
            assert line in text, name
        l1 = json.loads(sealed_lines["L1_long_green_tp8_lowvol_webull_sim"])
        assert l1["equity"] == "10010.820817"
        sealed_now = {
            (row.get("name"), row.get("date"))
            for row in past_day_lock.load_manifest(manifest)
            if row.get("record") == "webull_sim" and row.get("kind") == "day"
        }
        by_name = {row["name"]: row for row in written if row["date"] == "2026-10-06"}
        for name in ten:
            row = by_name[name]
            assert row["section"] == "locked" and row["final"] is True and row["locked_trade"] is True
            assert "no open, not filled: BLFS" in row["note"]
            assert sum(1 for fill in row["fills"] if fill.get("shares")) == 14
            assert next(fill for fill in row["fills"] if fill["ticker"] == "BLFS")["reason"] == "no open, not filled"
            assert (name, "2026-10-06") in sealed_now
        theme = {
            "theme_radar_fpe_delta_t3_earn_today_3d_webull_sim": (FPE_10_06, 3),
            "theme_radar_fresh_dcp_t1_ep_ge03_2d_webull_sim": (["AIB", "RIVN"], 2),
            "theme_radar_fresh_dcp_t1_avoid_ah_3d_webull_sim": (["AIB", "RIVN"], 3),
        }
        for name, (tickers, hold) in theme.items():
            row = by_name[name]
            assert row["section"] == "locked" and row["final"] is True and row["locked_trade"] is True
            assert row["reason"] == ""
            assert [fill["ticker"] for fill in row["fills"] if fill.get("shares")] == tickers
            assert all(fill["side"] == "sell" for fill in row["fills"] if fill.get("shares"))
            assert all(pos["side"] == "short" for pos in row["positions"])
            assert row["picks"] == len(tickers)
            assert (name, "2026-10-06") in sealed_now


def test_workflow_is_after_the_freeze_and_on_ubuntu() -> None:
    text = (ROOT / ".github/workflows/webull_sim.yml").read_text(encoding="utf-8")
    assert "ubuntu-latest" in text
    assert "workflow_dispatch" in text
    assert "45 13 * * 1-5" in text
    assert "15 20 * * 1-5" in text
    assert "OpenClaw" not in text
    assert "webull_sim" in text
    assert "https://github.com/SRoyaltyy/theme-radar.git" in text
    assert "THEME_RADAR_DIR" in text


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
    test_theme_radar_late_first_appearance_is_not_a_fill()
    test_theme_plan_first_commit_before_open_is_locked()
    test_theme_plan_at_or_after_open_or_missing_is_not_a_fill()
    test_aib_stays_in_both_theme_radar_books()
    test_short_pnl_sign_and_fees()
    test_theme_hold_days_do_not_move_other_books()
    test_theme_radar_page_states_the_borrow_lines_under_each_book()
    test_slot_is_equity_over_max_20_n()
    test_thirty_four_picks_get_thirty_four_equal_slots()
    test_missing_open_does_not_drop_the_other_picks()
    test_no_cash_only_from_held_lots()
    test_own_count_precedence()
    test_h1_sandbox_fill_not_observed()
    test_low_ticker_does_not_drop_the_yahoo_open()
    test_unexplained_jump_is_not_used_and_a_missing_open_is_not_guessed()
    test_h1_ledger_plan_fills_at_the_open_and_flatten_h1_sat_out()
    test_excel_pre_lock_label_and_pages_ship_webull_sim()
    test_final_run_locks_the_other_picks_when_one_open_is_missing()
    test_no_fires_sits_out_and_history_starts_on_10_06()
    test_theme_radar_fetch_does_not_send_the_actions_token()
    test_a_final_row_is_copied_when_the_close_moves()
    test_final_run_keeps_sealed_rows_and_locks_blfs_and_theme()
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
