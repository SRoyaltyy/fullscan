"""Locks for factor_mine_diagnosis_v1. No price file and no score."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_diagnosis_v1.protocol import (  # noqa: E402
    ALLOW_PREFIXES,
    DROP_SHA256,
    FORWARD,
    HOLDUP_NAME,
    INPUTS_SHA256,
    MIN_TRADES,
    PREREG,
    RANKED_15,
    RECIPES_SHA256,
    REPORT,
    SUBJECTS,
    TUNE,
    active_parts,
    file_sha256,
    grow_tree,
    hard_red,
    interaction_gain,
    load_drop,
    load_inputs,
    load_recipes,
    pair_drops,
    prereg_fingerprint,
    prior_feature_dates,
    SameDayLeak,
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def test_fingerprint_and_pins() -> None:
    text = PREREG.read_text(encoding="utf-8")
    header = ""
    for line in text.splitlines():
        if line.startswith("- fingerprint_sha256:"):
            header = line.split(":", 1)[1].strip()
    assert header == prereg_fingerprint(text)
    assert file_sha256(ROOT / "research/factor_mine_diagnosis_v1/INPUTS.json") == INPUTS_SHA256
    assert file_sha256(ROOT / "research/factor_mine_diagnosis_v1/RECIPES.json") == RECIPES_SHA256
    assert file_sha256(ROOT / "research/factor_mine_diagnosis_v1/DROP_LIST.json") == DROP_SHA256
    assert INPUTS_SHA256 in text and RECIPES_SHA256 in text and DROP_SHA256 in text
    assert "not a recipe to trade" in text
    assert "Interaction gain" in text
    payload = load_inputs()
    assert list(payload["dates"]) == list(TUNE + FORWARD)
    assert payload["dates"]["2026-08-27"]["s"] is None
    assert hard_red(None) is False
    assert hard_red(-3) is True
    assert payload["dates"]["2026-09-16"]["n"] == 4
    recipes = load_recipes()
    assert [row["name"] for row in recipes] == list(SUBJECTS)
    assert len(RANKED_15) == 15
    holdup = recipes[-1]
    assert holdup["name"] == HOLDUP_NAME
    assert holdup["created_on"] == "2026-09-21"
    assert holdup["s_boost"] == "holdup"


def test_parts_pairs_and_leak() -> None:
    by_name = {row["name"]: row for row in load_recipes()}
    assert active_parts(by_name["probable_h3"]) == ["list_source", "weather", "hold_rule"]
    assert active_parts(by_name["union_hot_n4_h1"]) == ["must_not", "sort", "weather", "hold_rule"]
    assert active_parts(by_name["short_extended_h3"]) == ["must_have", "weather", "hold_rule"]
    assert len(pair_drops()) == 10
    assert interaction_gain(10, 8, 7, 6) == -1
    assert prior_feature_dates(["2026-08-12"], "2026-08-13") == ["2026-08-12"]
    try:
        prior_feature_dates(["2026-08-13"], "2026-08-13")
    except SameDayLeak:
        pass
    else:
        raise AssertionError("leak")
    drop = load_drop()
    assert drop["n_dropped"] == 76
    assert drop["matched_splits"] == ["ALP", "NFE", "TNMG", "WCT"]
    assert "YAAS" not in drop["dropped"]


def test_tree_stops_under_30() -> None:
    rows = []
    for index in range(20):
        rows.append({"win": True, "ret": 0.1, "bins": {"price_band": "lt3", "ret5_band": "neg",
            "cond_band": "le0", "heat": "missing", "candle": "no", "news": "missing",
            "earn": "no", "days_on_list": "1", "weather": "up", "alarm": "no",
            "blue": "no", "zero_red": "no", "vol": "missing"}})
    leaves = grow_tree(rows, 2, MIN_TRADES)
    assert len(leaves) == 1
    assert leaves[0]["flag_lt_30"] is True
    assert leaves[0]["n"] == 20


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


def test_prereg_precedes_the_report() -> None:
    log = _git(
        "log", "--diff-filter=A", "--format=%H", "--",
        "research/factor_mine_diagnosis_v1/returns/REPORT.md",
    ).splitlines()
    if not log:
        return
    first = log[-1]
    parent = _git("rev-parse", f"{first}^").strip()
    prereg_then = subprocess.check_output(
        ["git", "show", f"{parent}:research/factor_mine_diagnosis_v1/PREREG.md"], cwd=ROOT,
    )
    assert prereg_then == PREREG.read_bytes()
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{parent}:research/factor_mine_diagnosis_v1/returns/REPORT.md"],
        cwd=ROOT,
    )
    assert missing.returncode != 0
    assert REPORT.is_file()


def main() -> None:
    test_fingerprint_and_pins()
    test_parts_pairs_and_leak()
    test_tree_stops_under_30()
    test_diff_stays_inside_the_study()
    test_prereg_precedes_the_report()
    print("protocol ok")


if __name__ == "__main__":
    main()
