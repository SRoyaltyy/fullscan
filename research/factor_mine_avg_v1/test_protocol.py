"""Locks for factor_mine_avg_v1. No price file and no score."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_avg_v1.protocol import (  # noqa: E402
    ALLOW_PREFIXES,
    BEFORE,
    AFTER,
    PREREG,
    REPORT,
    best_ticker,
    compound,
    day_counts,
    ex_best_compound,
    load_drop,
    median,
    prereg_fingerprint,
    random4_rows,
    universe,
)

EXPECTED = (
    "union_candle_score_h1",
    "union_candle_score_h3",
    "union_cond_h1",
    "union_cond_h3",
    "union_cond_n4_h3",
    "union_hot_n12_h1",
    "union_hot_n4_h1",
    "union_hot_score_h1",
    "union_hot_score_h3",
    "union_ret_5_h1",
    "union_ret_5_h3",
    "union_w_hot_candle_h1",
    "union_w_hot_candle_h3",
    "union_w_hot_cond_h1",
    "union_w_hot_cond_h3",
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def test_fingerprint_matches_header() -> None:
    text = PREREG.read_text(encoding="utf-8")
    header = ""
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            header = line.split(":", 1)[1].strip()
    assert header == prereg_fingerprint(text)
    assert len(header) == 64


def test_universe_is_the_fifteen_long_rank_buys() -> None:
    recipes = universe()
    names = tuple(recipe["name"] for recipe in recipes)
    assert names == EXPECTED
    for recipe in recipes:
        assert recipe["side"] == "long"
        assert recipe["rank"]
        assert recipe["top_n"] in (4, 8, 12)


def test_windows_do_not_overlap_and_stop_where_locked() -> None:
    assert BEFORE[-1] == "2026-09-11"
    assert AFTER[0] == "2026-09-14"
    assert AFTER[-1] == "2026-09-25"
    assert set(BEFORE).isdisjoint(AFTER)
    assert list(BEFORE) == sorted(BEFORE)
    assert list(AFTER) == sorted(AFTER)


def test_drop_list_is_the_362_cleanup() -> None:
    payload = load_drop()
    assert payload["n_dropped"] == 77
    assert set(payload["matched_splits"]) <= set(payload["dropped"])
    assert set(payload["open_prev_close_52"]) <= set(payload["dropped"])
    assert len(set(payload["dropped"]) - set(payload["open_prev_close_52"])) == 25


def test_ex_best_removes_only_the_best_ticker() -> None:
    sessions = ["2026-08-13", "2026-08-14"]
    returns = [0.10, -0.05]
    pnl = {
        "2026-08-13": {"AAA": 1000.0, "BBB": 0.0},
        "2026-08-14": {"AAA": -100.0, "BBB": -400.0},
    }
    first = {"AAA": "2026-08-13", "BBB": "2026-08-13"}
    totals = {"AAA": 900.0, "BBB": -400.0}
    assert best_ticker(totals, first) == "AAA"
    # Before window is the first session only. End equity 11,000 minus 1,000.
    got = ex_best_compound(sessions, ["2026-08-13"], returns, pnl, "AAA")
    assert got == 0.0
    assert day_counts([0.1, 0.0, -0.2]) == (1, 1, 1)
    assert median([1.0, 3.0, 2.0, 4.0]) == 2.5
    assert abs(compound([0.10, 0.10]) - 0.21) < 1e-12


def test_random4_rows_are_stock_book_picks() -> None:
    rows = random4_rows("2026-08-13", ["AAA", "BBB"])
    assert [row["sources"] for row in rows] == [["stock_book"], ["stock_book"]]
    assert [row["ticker"] for row in rows] == ["AAA", "BBB"]
    assert [row["src_rank"] for row in rows] == [0, 1]


def test_diff_stays_inside_the_study() -> None:
    try:
        base = _git("merge-base", "HEAD", "origin/main").strip()
    except subprocess.CalledProcessError:
        base = _git("merge-base", "HEAD", "main").strip()
    changed = [line for line in _git("diff", "--name-only", base, "HEAD").splitlines() if line]
    status = _git("status", "--porcelain", "-uall")
    for line in status.splitlines():
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        changed.append(path)
    for path in changed:
        if not any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in ALLOW_PREFIXES):
            raise AssertionError(path)


def test_results_do_not_rewrite_the_prereg() -> None:
    log = _git("log", "--diff-filter=A", "--format=%H", "--", "research/factor_mine_avg_v1/returns/REPORT.md").splitlines()
    if not log:
        return
    first = log[-1]
    parent = _git("rev-parse", f"{first}^").strip()
    prereg_then = subprocess.check_output(["git", "show", f"{parent}:research/factor_mine_avg_v1/PREREG.md"], cwd=ROOT)
    assert prereg_then == PREREG.read_bytes()
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_avg_v1/returns/REPORT.md"],
        cwd=ROOT,
    )
    assert missing.returncode != 0
    for path in (REPORT, ROOT / "research/factor_mine_avg_v1/returns/summary.json"):
        rel = path.relative_to(ROOT).as_posix()
        added = _git("log", "--diff-filter=A", "--format=%H", "--", rel).splitlines()
        if not added or not path.is_file():
            continue
        committed = subprocess.check_output(["git", "show", f"{added[-1]}:{rel}"], cwd=ROOT)
        assert committed == path.read_bytes()


def main() -> None:
    test_fingerprint_matches_header()
    test_universe_is_the_fifteen_long_rank_buys()
    test_windows_do_not_overlap_and_stop_where_locked()
    test_drop_list_is_the_362_cleanup()
    test_ex_best_removes_only_the_best_ticker()
    test_random4_rows_are_stock_book_picks()
    test_diff_stays_inside_the_study()
    test_results_do_not_rewrite_the_prereg()
    print("factor_mine_avg_v1 protocol ok")


if __name__ == "__main__":
    main()
