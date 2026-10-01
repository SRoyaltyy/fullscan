"""Locks for factor_mine_score_search_v2. No price file and no score."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v2.protocol import (  # noqa: E402
    ALLOW_PREFIXES,
    FORWARD,
    FREEZE,
    GRID,
    INPUTS_SHA256,
    LUCK_N,
    NON_RED,
    PREREG,
    REPORT,
    TUNE,
    V1_GRID_N,
    file_sha256,
    hard_red,
    joint_metric,
    load_drop,
    load_inputs,
    prereg_fingerprint,
    rank_formulas,
    required_cells,
    score_formula,
    unexplained_jumps,
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def test_fingerprint_and_inputs() -> None:
    text = PREREG.read_text(encoding="utf-8")
    header = ""
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            header = line.split(":", 1)[1].strip()
    assert header == prereg_fingerprint(text)
    assert file_sha256(ROOT / "research/factor_mine_score_search_v2/INPUTS.json") == INPUTS_SHA256
    assert INPUTS_SHA256 in text
    assert "Luck N is 68" in text
    assert LUCK_N == V1_GRID_N + len(GRID)
    payload = load_inputs()
    assert list(payload["dates"]) == list(TUNE + FORWARD)
    assert payload["dates"]["2026-08-27"]["s"] is None
    assert hard_red(None) is True
    got = []
    for session in TUNE:
        score = payload["dates"][session]["s"]
        if not hard_red(score):
            got.append(session)
    assert tuple(got) == NON_RED


def test_grid_and_required_cells() -> None:
    assert len(GRID) == 34
    assert GRID[0]["id"] == "board_list"
    found = []
    for line in PREREG.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| `"):
            continue
        found.append(line.split("`")[1])
    assert found == [row["id"] for row in GRID]
    cells = required_cells()
    assert ("2026-08-20", 4) in cells
    assert ("2026-08-20", 2) not in cells
    assert all(top_n == 8 or (start == "2026-08-20" and top_n == 4) for start, top_n in cells)
    assert not any(start >= "2026-09-02" for start, _top in cells)
    line = next(row for row in PREREG.read_text(encoding="utf-8").splitlines() if row.startswith("Required cells:"))
    listed = []
    for part in line.split(":", 1)[1].split(";"):
        start, size = part.strip().rstrip(".").split(" X=")
        listed.append((start, int(size)))
    assert tuple(listed) == cells


def test_rank_uses_the_worse_metric() -> None:
    assert joint_metric(0.7, 0.4, 30) == 0.4
    assert joint_metric(0.7, 0.4, 29) is None
    required = required_cells()

    def cells(win: float, closed: int, miss: tuple | None = None) -> dict:
        out = {}
        for pair in required:
            if pair == miss:
                out[pair] = {"closed": 10, "up_share": win, "win_rate": win}
            else:
                out[pair] = {"closed": closed, "up_share": win, "win_rate": win}
        return out

    high = score_formula(cells(0.62, 40))
    low = score_formula(cells(0.51, 40))
    short = score_formula(cells(0.9, 40, miss=required[0]))
    high["id"] = "high"
    low["id"] = "low"
    short["id"] = "short"
    assert high["eligible"] is True
    assert short["eligible"] is False
    assert high["rank_key"] == 0.62
    order = rank_formulas([low, short, high])
    assert order == ["high", "low", "short"]


def test_jump_halt_predicate() -> None:
    bars = {
        "AAA": [
            {"date": "2026-08-13", "open": 10.0, "close": 10.0},
            {"date": "2026-08-14", "open": 40.0, "close": 40.0},
        ]
    }
    flags = unexplained_jumps(bars, {})
    assert len(flags) == 1
    assert flags[0]["leg"] == "open_over_prev_close"
    assert unexplained_jumps(bars, {("AAA", "2026-08-14"): 0.25}) == []
    intraday = {
        "BBB": [{"date": "2026-08-13", "open": 10.0, "close": 40.0}],
    }
    assert unexplained_jumps(intraday, {})[0]["leg"] == "close_over_open"


def test_windows_and_drop_list() -> None:
    assert TUNE[-1] == "2026-09-11"
    assert FORWARD[0] == "2026-09-14"
    assert set(TUNE).isdisjoint(FORWARD)
    payload = load_drop()
    assert payload["n_dropped"] == 77
    assert "YAAS" in payload["dropped"]
    source = json.loads((ROOT / "research/breadth_rank_v1c/bars/DROPPED.json").read_text(encoding="utf-8"))
    assert payload["dropped"] == source["dropped"]
    assert payload["matched_splits"] == ["ALP", "NFE", "TNMG", "WCT"]


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


def test_prereg_precedes_the_freeze() -> None:
    log = _git(
        "log", "--diff-filter=A", "--format=%H", "--",
        "research/factor_mine_score_search_v2/freeze/FREEZE.json",
    ).splitlines()
    if not log:
        return
    parent = _git("rev-parse", f"{log[-1]}^").strip()
    prereg_then = subprocess.check_output(
        ["git", "show", f"{parent}:research/factor_mine_score_search_v2/PREREG.md"], cwd=ROOT,
    )
    assert prereg_then == PREREG.read_bytes()
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_score_search_v2/freeze/FREEZE.json"],
        cwd=ROOT,
    )
    assert missing.returncode != 0


def test_freeze_precedes_the_forward_file() -> None:
    log = _git(
        "log", "--diff-filter=A", "--format=%H", "--",
        "research/factor_mine_score_search_v2/returns/REPORT.md",
    ).splitlines()
    if not log:
        return
    parent = _git("rev-parse", f"{log[-1]}^").strip()
    prereg_then = subprocess.check_output(
        ["git", "show", f"{parent}:research/factor_mine_score_search_v2/PREREG.md"], cwd=ROOT,
    )
    assert prereg_then == PREREG.read_bytes()
    freeze_then = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_score_search_v2/freeze/FREEZE.json"],
        cwd=ROOT,
    )
    assert freeze_then.returncode == 0
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_score_search_v2/returns/REPORT.md"],
        cwd=ROOT,
    )
    assert missing.returncode != 0
    if FREEZE.is_file() and REPORT.is_file():
        assert REPORT.is_file()


def main() -> None:
    test_fingerprint_and_inputs()
    test_grid_and_required_cells()
    test_rank_uses_the_worse_metric()
    test_jump_halt_predicate()
    test_windows_and_drop_list()
    test_diff_stays_inside_the_study()
    test_prereg_precedes_the_freeze()
    test_freeze_precedes_the_forward_file()
    print("factor_mine_score_search_v2 protocol ok")


if __name__ == "__main__":
    main()
