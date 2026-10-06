"""Honest scorecard rules: prime first print, day percent, and the win bar."""
from __future__ import annotations

import subprocess
from pathlib import Path

from src.honest_scorecard import (
    before_0930,
    compound,
    day_percent,
    describe_baselines,
    live_dates,
    parse_flatten_card,
    parse_scoreboard,
    prime_from_versions,
    random4_compounded,
    split_sections,
    verdict,
)

ROOT = Path(__file__).resolve().parents[1]

HEADER = (
    "| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | "
    "Bought | Sold | Close cash | Close equity | Close held | 09:30 why |"
)
SEP = "|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|"
FILL_HEADER = (
    "| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | "
    "Equity change (sells only) | Why | Cameras |"
)


def _board(close: str, *, lots: str = "", sells: str = "") -> str:
    session = (
        f"| 2026-08-13 | +1.00 | $10,000.00 | — | $10,000.00 | +0.00 | "
        f"AAA | — | $9,000.00 | {close} | AAA×1 | — |"
    )
    parts = [
        "# toy",
        "",
        "Cash book **+1.00%** ($10,100). Fills 2 · realized $10.00.",
        "",
        "## Each session (cash + holdings state)",
        "",
        HEADER,
        SEP,
        session,
        "",
        "## Fills",
        "",
        FILL_HEADER,
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    if sells:
        parts.append(sells)
    if lots:
        parts += [
            "",
            "## Every lot",
            "",
            "| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | "
            "Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            lots,
        ]
    return "\n".join(parts) + "\n"


def test_day_percent_excludes_the_overnight_gap() -> None:
    # Prior close 50, morning 100, close 110. The gap to 100 is not the day.
    assert day_percent(100.0, 110.0) == 0.1
    assert day_percent(10000.0, 10153.12) == (10153.12 - 10000.0) / 10000.0
    assert day_percent(0.0, 10.0) is None
    assert compound([0.1, -0.1]) == (1.1 * 0.9) - 1


def test_win_bar() -> None:
    assert verdict(29, 29, 10.0).startswith("not enough trades yet")
    assert "$+10.00 after fees" in verdict(29, 29, 10.0)
    assert verdict(30, 16, -5.0).startswith("not cleared")
    assert "$-5.00 after fees" in verdict(30, 16, -5.0)
    # 16/30 = 53.3%. 17/30 is just over 55%. Exactly 55% is not a win.
    assert verdict(30, 17, 1.0).startswith("win")
    assert verdict(300, 165, 1.0).startswith("not cleared")
    assert verdict(300, 166, 2.5).startswith("win")
    assert "$+2.50 after fees" in verdict(300, 166, 2.5)
    assert "not printed" in verdict(10, 10, None)


def test_prime_print_ignores_later_rewrite() -> None:
    first = _board("$10,153.12", sells=(
        "| 2026-08-13 09:30 ET | **SELL** | `AAA` | 1 | $11.00 | $1.00 | "
        "$+5.00 | $10,000.00 | — | list | — |"
    ))
    later = _board("$9,000.00", lots="| 2026-08-13 | `AAA` | 1 | $10.00 | $10.00 | +0.00 | $11.00 | +10.00 | +4.00 | +0.00 | +1.00 |")
    # A single Equity column, from the earliest blotter, is not both prints.
    early = (
        "| Date | S | Route | Hard-red | Cash | Stock | Equity | Bought | Sold | Skipped | Why |\n"
        "| 2026-08-13 | +8.53 | io | no | $97.53 | $10.00 | $10,153.12 | AAA | — | — | buy |\n"
    )
    prime = prime_from_versions([
        ("aaa", early),
        ("bbb", first),
        ("ccc", later),
    ])
    assert prime["2026-08-13"]["eq_close"] == 10153.12
    assert prime["2026-08-13"]["eq_0930"] == 10000.0
    assert prime["2026-08-13"]["commit"] == "bbb"
    assert prime["2026-08-13"]["pnls"] == [5.0]
    assert prime["2026-08-13"]["lots"]["AAA"] == 4.0
    assert prime["2026-08-13"]["lots_commit"] == "ccc"
    # The rewritten working tree is not consulted.
    assert parse_scoreboard(later)["2026-08-13"]["eq_close"] == 9000.0


def test_open_close_marks_count_as_both_equities() -> None:
    text = "\n".join([
        FILL_HEADER,
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        "| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | "
        "$10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |",
        "| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | "
        "$97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +153.12) | — | — |",
    ])
    row = parse_scoreboard(text)["2026-08-13"]
    assert row["eq_0930"] == 10000.0
    assert row["eq_close"] == 10153.12


def test_live_is_not_blended_with_later_unsealed_days() -> None:
    days = {
        "2026-08-13": {"eq_0930": 100.0, "eq_close": 110.0, "pnls": [1.0]},
        "2026-09-28": {"eq_0930": 110.0, "eq_close": 121.0, "pnls": [2.0]},
        "2026-09-29": {"eq_0930": 130.0, "eq_close": 143.0, "pnls": [3.0]},
    }
    flags = {
        "2026-08-13": False,
        "2026-09-28": True,
        "2026-09-29": False,
    }
    first, live = live_dates(flags)
    assert first == "2026-09-28"
    assert live == {"2026-09-28"}
    sections = split_sections(days, live)
    by = {section.title: section for section in sections}
    assert [day["date"] for day in by["LIVE-LOCKED"].days] == ["2026-09-28"]
    assert abs(compound(by["LIVE-LOCKED"].percents) - day_percent(110.0, 121.0)) < 1e-12
    built_dates = [day["date"] for day in by["BUILT AFTER THE FACT"].days]
    assert built_dates == ["2026-08-13", "2026-09-29"]
    assert "2026-09-29" not in [day["date"] for day in by["LIVE-LOCKED"].days]


def test_clock() -> None:
    assert before_0930("2026-09-28T12:36:47Z", "2026-09-28")
    assert not before_0930("2026-10-02T04:45:16Z", "2026-09-29")
    assert not before_0930(None, "2026-09-28")


def test_flatten_card_line() -> None:
    text = (
        "- Prior close **$103,336.45** · 09:30 **$103,577.15** · "
        "overnight **$+240.70** · session **$-20.35** · 16:00 **$103,556.79**"
    )
    assert parse_flatten_card(text) == (103577.15, 103556.79)
    from src.honest_scorecard import parse_flatten_lots
    lots = parse_flatten_lots(
        "| Ticker | Sleeve | Held | Shares | 09:30 | Close | Overnight $ | Session $ | Day $ |\n"
        "|---|---|---|---:|---:|---:|---:|---:|---:|\n"
        "| COP | io_core | held | 10 | $1.00 | $2.00 | $+0.00 | $+10.00 | $+12.50 |\n"
    )
    assert lots == {"COP": 12.5}
    assert abs(day_percent(103577.15, 103556.79) - ((103556.79 - 103577.15) / 103577.15)) < 1e-12


def test_random4_uses_open_to_close_and_reports_rank() -> None:
    fees = {
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
    pools = {"2026-08-13": ["AAA", "BBB", "CCC", "DDD", "EEE"]}
    prices = {
        ("2026-08-13", "AAA"): (10.0, 12.0),
        ("2026-08-13", "BBB"): (10.0, 11.0),
        ("2026-08-13", "CCC"): (10.0, 10.0),
        ("2026-08-13", "DDD"): (10.0, 9.0),
        ("2026-08-13", "EEE"): (10.0, 8.0),
        ("2026-08-13", "IWM"): (100.0, 101.0),
    }
    packed = random4_compounded(
        ["2026-08-13"], pools, prices, 10000.0, fees, draws=20,
    )
    assert packed is not None
    draws, used = packed
    assert len(draws) == 20 and used == ["2026-08-13"]
    empty = {"2026-08-13": [], "2026-08-14": pools["2026-08-13"]}
    prices14 = dict(prices)
    for tick in empty["2026-08-14"]:
        prices14[("2026-08-14", tick)] = (10.0, 11.0)
    partial = random4_compounded(
        ["2026-08-13", "2026-08-14"], empty, prices14, 10000.0, fees, draws=5,
    )
    assert partial is not None and partial[1] == ["2026-08-14"]
    described = describe_baselines(0.05, draws, (0.01, 100.0))
    assert "median" in described["random4"]
    assert "percentile" in described["random4"]
    assert "session $" in described["iwm"]


def test_git_prime_ignores_the_working_tree(tmp: Path) -> None:
    repo = tmp / "repo"
    repo.mkdir()
    subprocess.check_call(["git", "init"], cwd=repo, stdout=subprocess.DEVNULL)
    subprocess.check_call(["git", "config", "user.email", "scorecard@example.com"], cwd=repo)
    subprocess.check_call(["git", "config", "user.name", "scorecard"], cwd=repo)
    dest = repo / "03_scoreboard" / "factor_mine"
    dest.mkdir(parents=True)
    path = dest / "toy.md"
    path.write_text(_board("$10,200.00"), encoding="utf-8")
    subprocess.check_call(["git", "add", "."], cwd=repo)
    subprocess.check_call(["git", "commit", "-m", "first"], cwd=repo, stdout=subprocess.DEVNULL)
    path.write_text(_board("$1.00"), encoding="utf-8")
    subprocess.check_call(["git", "add", "."], cwd=repo)
    subprocess.check_call(["git", "commit", "-m", "rewrite"], cwd=repo, stdout=subprocess.DEVNULL)
    from src.honest_scorecard import factor_mine_prime
    books = factor_mine_prime(repo)
    assert books["toy"]["2026-08-13"]["eq_close"] == 10200.0
    assert books["toy"]["2026-08-13"]["eq_0930"] == 10000.0


def test_workflow_is_github_hosted() -> None:
    text = (ROOT / ".github" / "workflows" / "honest_scorecard.yml").read_text(encoding="utf-8")
    assert "ubuntu-latest" in text
    assert "self-hosted" not in text
    assert "src.test_honest_scorecard" in text


def main() -> None:
    import tempfile
    tests = [
        test_day_percent_excludes_the_overnight_gap,
        test_win_bar,
        test_prime_print_ignores_later_rewrite,
        test_open_close_marks_count_as_both_equities,
        test_live_is_not_blended_with_later_unsealed_days,
        test_clock,
        test_flatten_card_line,
        test_random4_uses_open_to_close_and_reports_rank,
        test_git_prime_ignores_the_working_tree,
        test_workflow_is_github_hosted,
    ]
    failed = 0
    for fn in tests:
        try:
            if fn.__code__.co_argcount:
                with tempfile.TemporaryDirectory() as raw:
                    fn(Path(raw))
            else:
                fn()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        else:
            print(f"ok {fn.__name__}")
    if failed:
        raise SystemExit(f"{failed} failed")
    print(f"{len(tests)} passed")


if __name__ == "__main__":
    main()
