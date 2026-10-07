"""Past buy/sell certificate. The baseline is the first seal, not this tree.

Run: python -m src.test_record_certificate
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from src.record_certificate import (
    EXCEL_LOCK_PICKS,
    EXCEL_LOCK_SHA,
    BEGIN,
    certify_excel,
    certify_factor_mine,
    certify_h1,
    certify_paper,
    certify_webull,
    leg,
    norm_side,
    scoreboard_fill_legs,
    stamp_html,
    Certificate,
)

ROOT = Path(__file__).resolve().parents[1]


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)


def _repo(tmp: Path) -> None:
    _git(tmp, "init")
    _git(tmp, "config", "user.email", "cert@example.com")
    _git(tmp, "config", "user.name", "Certificate")


def _commit(tmp: Path, msg: str) -> None:
    _git(tmp, "add", "-A")
    _git(tmp, "commit", "-m", msg)


def _commit_at(tmp: Path, msg: str, when: str) -> None:
    env = os.environ.copy()
    env["GIT_AUTHOR_DATE"] = when
    env["GIT_COMMITTER_DATE"] = when
    subprocess.run(
        ["git", "add", "-A"], cwd=tmp, check=True, capture_output=True, text=True,
    )
    subprocess.run(
        ["git", "commit", "-m", msg], cwd=tmp, check=True,
        capture_output=True, text=True, env=env,
    )


def _scoreboard(day_row: str, fill: str, *, close: str = "$9,990.00") -> str:
    row = day_row.replace("$9,990.00", close)
    return "\n".join([
        "# toy",
        "",
        "| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | "
        "Intraday $ | Bought | Sold | Close cash | Close equity | Close held |",
        "|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|",
        row,
        "",
        "## Fills",
        "",
        "| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | "
        "Equity change (sells only) | Why | Cameras |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        fill,
        "",
    ])


DAY = (
    "| 2026-10-01 | +0.10 | $10,000.00 | — | $10,000.00 | +0.00 | -10.00 | "
    "AAA | — | $9,990.00 | $9,990.00 | AAA×1 |"
)
BUY = (
    "| 2026-10-01 09:30 ET | **BUY** | `AAA` | 20 | $10.00 | $1.00 | — | "
    "$9,989.00 | — | list | — |"
)


def test_fingerprint_ignores_marks() -> None:
    assert norm_side("LONG") == "buy"
    assert norm_side("short") == "sell"
    assert norm_side("COVER") == "buy"
    same = scoreboard_fill_legs(_scoreboard(DAY, BUY))
    marked = scoreboard_fill_legs(_scoreboard(DAY, BUY, close="$1.00"))
    assert same == marked
    assert same["2026-10-01"] == (leg("2026-10-01", "buy", "AAA", "20"),)
    changed = BUY.replace("| 20 |", "| 9 |")
    assert scoreboard_fill_legs(_scoreboard(DAY, changed)) != same


def test_factor_mine_uses_prime_print_not_this_tree(tmp: Path) -> None:
    _repo(tmp)
    folder = tmp / "03_scoreboard" / "factor_mine"
    folder.mkdir(parents=True)
    path = folder / "toy.md"
    path.write_text(_scoreboard(DAY, BUY), encoding="utf-8")
    _commit(tmp, "prime")
    path.write_text(_scoreboard(DAY, BUY, close="$12,000.00"), encoding="utf-8")
    _commit(tmp, "equity mark only")
    cert = certify_factor_mine(tmp)
    assert cert.status == "pass", cert.failed
    assert cert.checked == 1
    path.write_text(_scoreboard(DAY, BUY.replace("| 20 |", "| 7 |")), encoding="utf-8")
    bad = certify_factor_mine(tmp)
    assert bad.status == "fail"
    assert "toy 2026-10-01" in bad.failed[0]
    assert not (tmp / "data" / "past_day_lock").exists()


def test_excel_lock_matches_suggestions_and_does_not_invent_a_final() -> None:
    import csv
    import sys
    engine = str(ROOT / "excel_bot" / "engine")
    if engine not in sys.path:
        sys.path.insert(0, engine)
    import signal_freeze as sf
    rows = list(csv.DictReader(open(sf.SUGG_PATH, encoding="utf-8")))
    picks = sf.canonical_picks(rows)["2026-10-06"]
    assert len(picks) == EXCEL_LOCK_PICKS
    assert sf.fingerprint(picks) == EXCEL_LOCK_SHA
    manifest = json.loads((ROOT / "excel_bot" / "freeze_manifest.json").read_text())
    lock = next(
        entry for entry in manifest["entries"]
        if entry["signal_date"] == "2026-10-06" and entry["kind"] == "lock"
    )
    assert lock["sha256"] == EXCEL_LOCK_SHA
    assert lock["n_picks"] == EXCEL_LOCK_PICKS
    final = ROOT / "excel_bot" / "daily" / "2026-10-06_excel_bot.md"
    assert not final.exists()
    before = (ROOT / "excel_bot" / "freeze_manifest.json").read_bytes()
    cert = certify_excel(ROOT)
    assert (ROOT / "excel_bot" / "freeze_manifest.json").read_bytes() == before
    assert not final.exists()
    # The kept 10-06 draft lock still matches. Earlier finals that gained
    # names after their first commit fail the page. A 10-07 sit-out does not.
    assert not any(line.startswith("2026-10-06") for line in cert.failed)
    assert cert.status == "fail"
    assert cert.failed
    assert any("2026-10-07" in note for note in cert.notes)


def test_excel_prelock_is_the_first_commit(tmp: Path) -> None:
    _repo(tmp)
    daily = tmp / "excel_bot" / "daily"
    daily.mkdir(parents=True)
    md = daily / "2026-10-01_excel_bot.md"
    first = (
        "# day\n\n| ticker | side | strategy | exit | ref close | signal colors |\n"
        "|---|---|---|---|---|---|\n"
        "| AAA | LONG | L1 | tp8 | 1.00 | white |\n\n"
        "## Live strategy scoreboard\n\n| strategy | n |\n|---|---|\n| L1 | 1 |\n"
    )
    md.write_text(first, encoding="utf-8")
    _commit(tmp, "first print")
    md.write_text(first.replace("| AAA |", "| BBB |"), encoding="utf-8")
    _commit(tmp, "rewritten names")
    sugg = tmp / "excel_bot" / "suggestions"
    sugg.mkdir()
    (sugg / "suggestions.csv").write_text(
        "signal_date,ticker\n", encoding="utf-8",
    )
    (tmp / "excel_bot" / "freeze_manifest.json").write_text(
        json.dumps({"entries": []}), encoding="utf-8",
    )
    cert = certify_excel(tmp)
    assert cert.status == "fail"
    assert any("2026-10-01" in line for line in cert.failed)
    md.write_text(first, encoding="utf-8")
    again = certify_excel(tmp)
    assert again.status == "pass", again.failed


def test_h1_first_plan_keeps_sell_size_and_ignores_marks(tmp: Path) -> None:
    _repo(tmp)
    rel = Path("research/hot_n4_clean_v4/forward_h1/h1_log.jsonl")
    path = tmp / rel
    path.parent.mkdir(parents=True)
    plan = {
        "kind": "plan", "date": "2026-10-02",
        "picks": [{"ticker": "AAA"}],
        "planned_sells": [{"ticker": "BBB", "shares": 10}],
    }
    mark = {"kind": "mark", "date": "2026-10-02", "equity_primary": 9229.32}
    path.write_text(json.dumps(plan) + "\n" + json.dumps(mark) + "\n", encoding="utf-8")
    page = tmp / "dashboard" / "h1"
    page.mkdir(parents=True)
    (page / "log.json").write_text(
        json.dumps([plan, mark], indent=2) + "\n", encoding="utf-8",
    )
    _commit(tmp, "seal")
    moved = dict(mark)
    moved["equity_primary"] = 1.0
    path.write_text(json.dumps(plan) + "\n" + json.dumps(moved) + "\n", encoding="utf-8")
    (page / "log.json").write_text(json.dumps([plan, moved]) + "\n", encoding="utf-8")
    ok = certify_h1(tmp)
    assert ok.status == "pass", ok.failed
    plan2 = json.loads(json.dumps(plan))
    plan2["planned_sells"][0]["shares"] = 11
    path.write_text(json.dumps(plan2) + "\n", encoding="utf-8")
    (page / "log.json").write_text(json.dumps([plan2]) + "\n", encoding="utf-8")
    bad = certify_h1(tmp)
    assert bad.status == "fail"
    assert "2026-10-02" in bad.failed[0]


def test_paper_lock_beats_an_earlier_draft(tmp: Path) -> None:
    _repo(tmp)
    folder = tmp / "data" / "day_board"
    folder.mkdir(parents=True)
    path = folder / "2026-10-06_strategy_tickets.json"

    def doc(ticker: str) -> dict:
        return {
            "date": "2026-10-06",
            "generated_at": "draft",
            "strategies": {
                "stock_book_1d": {
                    "family": "stock_book",
                    "buy": [{"ticker": ticker, "side": "long", "px": 1.5}],
                    "sell": [],
                },
                "excel_only": {
                    "family": "excel",
                    "buy": [{"ticker": "ZZZ", "side": "long"}],
                    "sell": [],
                },
            },
        }

    path.write_text(json.dumps(doc("AAA")), encoding="utf-8")
    _commit(tmp, "draft")
    sealed = json.dumps(doc("BBB"))
    path.write_text(sealed, encoding="utf-8")
    _commit(tmp, "lock")
    from src.record_certificate import sha256_text
    digest = sha256_text(sealed)
    manifest = tmp / "data" / "past_day_lock" / "manifest.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({
        "date": "2026-10-06", "kind": "day", "name": "",
        "record": "strategy_tickets", "sha256": digest,
    }) + "\n", encoding="utf-8")
    _commit(tmp, "manifest")
    # A price-only edit is not a buy/sell change. Excel family is not paper.
    edited = doc("BBB")
    edited["generated_at"] = "later"
    edited["strategies"]["stock_book_1d"]["buy"][0]["px"] = 99
    edited["strategies"]["excel_only"]["buy"][0]["ticker"] = "QQQ"
    path.write_text(json.dumps(edited), encoding="utf-8")
    ok = certify_paper(tmp)
    assert ok.status == "pass", ok.failed
    edited["strategies"]["stock_book_1d"]["buy"][0]["ticker"] = "CCC"
    path.write_text(json.dumps(edited), encoding="utf-8")
    bad = certify_paper(tmp)
    assert bad.status == "fail"
    assert manifest.read_text(encoding="utf-8").count("\n") == 1


def test_paper_seal_is_the_preopen_plan_not_the_first_draft(tmp: Path) -> None:
    _repo(tmp)
    folder = tmp / "data" / "day_board"
    folder.mkdir(parents=True)
    path = folder / "2026-09-11_strategy_tickets.json"

    def doc(ticker: str) -> dict:
        return {
            "date": "2026-09-11",
            "strategies": {
                "stock_book_1d": {
                    "family": "stock_book",
                    "buy": [{"ticker": ticker, "side": "long", "shares": 10}],
                    "sell": [],
                },
            },
        }

    path.write_text(json.dumps(doc("AAA")), encoding="utf-8")
    _commit_at(tmp, "morning draft", "2026-09-11T07:00:00-04:00")
    path.write_text(json.dumps(doc("BBB")), encoding="utf-8")
    _commit_at(tmp, "send-time plan", "2026-09-11T08:30:00-04:00")
    path.write_text(json.dumps(doc("CCC")), encoding="utf-8")
    bad = certify_paper(tmp)
    assert bad.status == "fail", bad.failed
    assert any("2026-09-11" in line for line in bad.failed)
    path.write_text(json.dumps(doc("BBB")), encoding="utf-8")
    ok = certify_paper(tmp)
    assert ok.status == "pass", ok.failed


def test_theme_radar_compares_first_plan_and_stores_nothing(tmp: Path) -> None:
    _repo(tmp)
    books = tmp / "data" / "webull_sim"
    books.mkdir(parents=True)
    row = {
        "name": "theme_radar_fpe_delta_t3_earn_today_3d_webull_sim",
        "date": "2026-10-06",
        "section": "locked",
        "reason": "",
        "fills": [
            {"ticker": "AAA", "side": "sell", "shares": 81, "reason": "open"},
        ],
        "equity": "9229.32",
        "note": "sat note",
    }
    (books / "days.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    _commit(tmp, "sim")

    def plans(_root, day):
        assert day == "2026-10-06"
        return {
            row["name"]: (leg(day, "short", "AAA", ""),),
        }

    ok = certify_webull(tmp, theme_plans=plans)
    assert ok.status == "pass", ok.failed
    assert not (tmp / "research" / "shadow_log").exists()

    def drifted(_root, day):
        return {row["name"]: (leg(day, "short", "BBB", ""),)}

    bad = certify_webull(tmp, theme_plans=drifted)
    assert bad.status == "fail"

    def unread(_root, day):
        return None

    missing = certify_webull(tmp, theme_plans=unread)
    assert missing.status == "incomplete"
    assert missing.failed == []
    assert not (tmp / "research" / "shadow_log").exists()


def test_sit_out_is_not_a_buy_sell_change(tmp: Path) -> None:
    _repo(tmp)
    books = tmp / "data" / "webull_sim"
    books.mkdir(parents=True)
    row = {
        "name": "stock_book_1d_webull_sim",
        "date": "2026-10-07",
        "section": "not_a_locked_trade",
        "reason": "sat out, 0 picks",
        "fills": [],
        "note": "sitting out",
    }
    (books / "days.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    cert = certify_webull(tmp, theme_plans=lambda _r, _d: {})
    assert cert.status == "pass", cert.failed
    assert any("sat out" in note for note in cert.notes)


def test_fail_paints_the_page_red_and_restamp_replaces() -> None:
    cert = Certificate(
        board="paper", title="Paper", status="fail", checked=1,
        failed=["2026-10-01 AAA changed"], baseline="first seal",
    )
    html = stamp_html("<html><body><h1>Book</h1></body></html>", cert)
    assert 'data-record-certificate="fail"' in html
    assert "PAST BUY/SELL CERTIFICATE — FAIL" in html
    assert "#7f1d1d" in html
    assert "last_px" not in html
    assert BEGIN in html
    passed = Certificate(
        board="paper", title="Paper", status="pass", checked=2,
        baseline="first seal",
    )
    again = stamp_html(html, passed)
    assert again.count(BEGIN) == 1
    assert 'data-record-certificate="pass"' in again
    assert "FAIL" not in again.split(BEGIN, 1)[1].split("PAST", 1)[0]
    assert "— PASS" in again


def main() -> None:
    import tempfile
    tests = [
        test_fingerprint_ignores_marks,
        test_excel_lock_matches_suggestions_and_does_not_invent_a_final,
        test_fail_paints_the_page_red_and_restamp_replaces,
    ]
    tmp_tests = [
        test_factor_mine_uses_prime_print_not_this_tree,
        test_excel_prelock_is_the_first_commit,
        test_h1_first_plan_keeps_sell_size_and_ignores_marks,
        test_paper_lock_beats_an_earlier_draft,
        test_paper_seal_is_the_preopen_plan_not_the_first_draft,
        test_theme_radar_compares_first_plan_and_stores_nothing,
        test_sit_out_is_not_a_buy_sell_change,
    ]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    for fn in tmp_tests:
        with tempfile.TemporaryDirectory() as raw:
            fn(Path(raw))
        print("ok", fn.__name__)
    print(f"{len(tests) + len(tmp_tests)} tests passed")


if __name__ == "__main__":
    main()
