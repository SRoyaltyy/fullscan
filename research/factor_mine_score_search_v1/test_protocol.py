"""Locks for factor_mine_score_search_v1. No price file and no score."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v1.protocol import (  # noqa: E402
    ALLOW_PREFIXES,
    BY_ID,
    FORWARD,
    FREEZE,
    GRID,
    INPUTS_SHA256,
    PREREG,
    REPORT,
    TUNE,
    file_sha256,
    load_drop,
    load_inputs,
    prereg_fingerprint,
    rank_formulas,
    raw_feature,
    simulate,
    top_names,
    zscores,
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def _fee(_shares: int, _px: float, _side: str) -> float:
    return 0.0


def test_fingerprint_and_inputs() -> None:
    text = PREREG.read_text(encoding="utf-8")
    header = ""
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            header = line.split(":", 1)[1].strip()
    assert header == prereg_fingerprint(text)
    assert file_sha256(ROOT / "research/factor_mine_score_search_v1/INPUTS.json") == INPUTS_SHA256
    assert "6016c796f50c7d2f4ba6fa10d69ba665e8b17d6705a7c6ad7ebefe65973cf1d3" in text
    payload = load_inputs()
    assert list(payload["dates"]) == list(TUNE + FORWARD)
    assert payload["dates"]["2026-08-27"]["s"] is None
    assert payload["dates"]["2026-09-16"]["n"] == 4


def test_grid_is_34_and_matches_the_prereg() -> None:
    assert len(GRID) == 34
    assert GRID[0]["id"] == "board_list"
    assert GRID[0]["kind"] == "board_list"
    found = []
    for line in PREREG.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| `"):
            continue
        found.append(line.split("`")[1])
    assert found == [row["id"] for row in GRID]
    assert raw_feature({"yday": 50}, "yday", BY_ID["cap_yday_10"]) == 10
    assert zscores([1.0, None, 3.0]) == [-1.0, 0.0, 1.0]


def test_board_list_buys_the_top_of_the_list() -> None:
    rows = [
        {"ticker": "BBB", "src_rank": 1, "open": 10, "close": 10},
        {"ticker": "AAA", "src_rank": 0, "open": 10, "close": 12},
        {"ticker": "CCC", "src_rank": 2, "open": None, "close": 10},
    ]
    assert top_names(rows, BY_ID["board_list"], 1) == ["AAA"]
    days = [{"session": "2026-08-13", "rows": rows, "hard_red": False}]
    book = simulate(days, {"2026-08-13": ["AAA"]}, _fee)
    assert abs(book["days"][0]["ret"] - 0.2) < 1e-12
    flat = simulate(days, {"2026-08-13": []}, _fee)
    assert flat["days"][0]["ret"] == 0.0


def test_rank_tie_uses_ex_best_then_id() -> None:
    order = rank_formulas([
        {"id": "b", "mean_compound": 0.1, "mean_ex": 0.0},
        {"id": "a", "mean_compound": 0.1, "mean_ex": 0.2},
        {"id": "c", "mean_compound": 0.2, "mean_ex": None},
    ])
    assert order == ["c", "a", "b"]


def test_windows_and_drop_list() -> None:
    assert TUNE[-1] == "2026-09-11"
    assert FORWARD[0] == "2026-09-14"
    assert set(TUNE).isdisjoint(FORWARD)
    payload = load_drop()
    assert payload["n_dropped"] == 77
    assert "YAAS" in payload["dropped"]
    source = json.loads((ROOT / "research/breadth_rank_v1c/bars/DROPPED.json").read_text(encoding="utf-8"))
    assert payload["dropped"] == source["dropped"]
    assert file_sha256(ROOT / "research/breadth_rank_v1c/bars/DROPPED.json") == (
        "4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad"
    )
    assert file_sha256(ROOT / "research/breadth_rank_v1c/JUMPS.md") == (
        "8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a"
    )
    jumps = (ROOT / "research/breadth_rank_v1c/JUMPS.md").read_text(encoding="utf-8")
    tickers = []
    for line in jumps.splitlines():
        if not line.startswith("| ") or line.startswith("| ---") or line.startswith("| ticker"):
            continue
        tickers.append(line.split("|")[1].strip())
    assert len(set(tickers) - {"YAAS"}) == 76
    assert set(payload["dropped"]) - {"YAAS"} == set(tickers) - {"YAAS"}
    assert payload["matched_splits"] == ["ALP", "NFE", "TNMG", "WCT"]
    cite = (ROOT / "research/factor_mine_score_search_v1/DROP_CITE.md").read_text(encoding="utf-8")
    assert "4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad" in cite
    assert "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2" in cite


def test_diff_stays_inside_the_study() -> None:
    try:
        base = _git("merge-base", "HEAD", "origin/main").strip()
    except subprocess.CalledProcessError:
        base = _git("merge-base", "HEAD", "main").strip()
    changed = [line for line in _git("diff", "--name-only", base, "HEAD").splitlines() if line]
    for line in _git("status", "--porcelain", "-uall").splitlines():
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        changed.append(path)
    for path in changed:
        if not any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in ALLOW_PREFIXES):
            raise AssertionError(path)


def test_freeze_precedes_the_forward_file() -> None:
    log = _git("log", "--diff-filter=A", "--format=%H", "--", "research/factor_mine_score_search_v1/returns/REPORT.md").splitlines()
    if not log:
        return
    first = log[-1]
    parent = _git("rev-parse", f"{first}^").strip()
    prereg_then = subprocess.check_output(["git", "show", f"{parent}:research/factor_mine_score_search_v1/PREREG.md"], cwd=ROOT)
    assert prereg_then == PREREG.read_bytes()
    freeze_then = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_score_search_v1/freeze/FREEZE.json"],
        cwd=ROOT,
    )
    assert freeze_then.returncode == 0
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_score_search_v1/returns/REPORT.md"],
        cwd=ROOT,
    )
    assert missing.returncode != 0
    for path in (REPORT, FREEZE):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT).as_posix()
        added = _git("log", "--diff-filter=A", "--format=%H", "--", rel).splitlines()
        if not added:
            continue
        committed = subprocess.check_output(["git", "show", f"{added[-1]}:{rel}"], cwd=ROOT)
        assert committed == path.read_bytes()


def main() -> None:
    test_fingerprint_and_inputs()
    test_grid_is_34_and_matches_the_prereg()
    test_board_list_buys_the_top_of_the_list()
    test_rank_tie_uses_ex_best_then_id()
    test_windows_and_drop_list()
    test_diff_stays_inside_the_study()
    test_freeze_precedes_the_forward_file()
    print("factor_mine_score_search_v1 protocol ok")


if __name__ == "__main__":
    main()
