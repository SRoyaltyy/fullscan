"""Fingerprint, input pins, and row rules for the clean-tape study.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_clean
"""
from __future__ import annotations

import json
import os
import subprocess

from src.lever_search_bars import SameDayBarError, feature_bars
from src.lever_search_clean_protocol import (
    CHECK_CALENDAR,
    CLEAN_BAR_BLOB_SHA,
    CLEAN_BAR_COMMIT,
    CLEAN_BAR_PATH,
    FINGERPRINT,
    LUCK_N,
    MANIFEST_PATH,
    PREREG,
    RETURNS,
    RUN_WINDOW,
    SESSIONS,
    STUDY_LABEL,
    assert_prereg,
    fingerprint_sha256,
    group_rows,
    header_fingerprint,
    load_manifest,
    overall_counts,
)
from src.lever_search_labelled_score import assemble_row
from src.lever_search_proof import build_group3_recipes

# This branch stacks on PR #358, so the labelled study's files are also in
# the three-dot diff until #358 merges. They are allowed, not written here.
ALLOWED = (
    "research/lever_search_clean/",
    "src/lever_search_clean_protocol.py",
    "src/lever_search_clean_append.py",
    "src/lever_search_clean_score.py",
    "src/test_lever_search_clean.py",
    "src/test_lever_search_clean_append.py",
    ".github/workflows/lever_search_clean_append_only.yml",
    "research/lever_search_labelled/",
    "src/lever_search_labelled_protocol.py",
    "src/lever_search_labelled_append.py",
    "src/lever_search_labelled_score.py",
    "src/test_lever_search_labelled.py",
    "src/test_lever_search_labelled_append.py",
    ".github/workflows/lever_search_labelled_append_only.yml",
)


def test_fingerprint_matches_covered_bytes() -> None:
    text = PREREG.read_text(encoding="utf-8")
    digest = fingerprint_sha256(text)
    assert digest == header_fingerprint(text) == FINGERPRINT
    assert_prereg()


def test_manifest_covers_every_session_and_names_the_fallback() -> None:
    data, index = load_manifest()
    assert data["panel_fallback"] == "not used"
    assert data["label"] == STUDY_LABEL
    bars = data["bars"]
    assert bars["path"] == CLEAN_BAR_PATH
    assert bars["commit"] == CLEAN_BAR_COMMIT
    assert bars["blob_sha"] == CLEAN_BAR_BLOB_SHA
    for day in SESSIONS:
        row = index[(day, "panel")]
        assert row["n_rows"] >= 1
        assert len(row["commit"]) == 40
        assert len(row["blob_sha"]) == 40
    assert len(RUN_WINDOW) == 21
    assert len(CHECK_CALENDAR) == 16
    assert len(build_group3_recipes()) == 110
    assert LUCK_N == 9500


def test_group_counts_match_the_prereg_table() -> None:
    text = PREREG.read_text(encoding="utf-8")
    overall = overall_counts()
    assert overall["n"] == 110
    assert (overall["search_min"], overall["search_median"], overall["search_max"]) == (8, 17.0, 21)
    assert (overall["check_min"], overall["check_median"], overall["check_max"]) == (8, 12.0, 16)
    assert "search days min 8, median 17, max 21" in text
    assert "Check days min 8, median 12, max 16" in text
    labels = {
        (): "no fullscan role (price-only, or union with the price-only fallback)",
        ("stock_book",): "stock_book only",
        ("actions", "stock_book"): "actions and stock_book",
        ("export", "stock_book"): "export and stock_book",
        ("join", "stock_book"): "join and stock_book",
        ("ab", "actions", "stock_book"): "ab, actions, and stock_book",
        ("ab", "stock_book"): "ab and stock_book",
        ("actions",): "actions only",
        ("actions", "export", "stock_book"): "actions, export, and stock_book",
        ("actions", "join", "stock_book"): "actions, join, and stock_book",
        ("catalyst", "stock_book"): "catalyst and stock_book",
        ("export",): "export only",
    }
    rows = group_rows()
    assert len(rows) == len(labels)
    for row in rows:
        label = labels[row["roles"]]
        needle = f"| {label} | {row['n_recipes']} | {row['search_min']} | {row['check_min']} |"
        assert needle in text, needle
        assert row["search_min"] == row["search_max"]
        assert row["check_min"] == row["check_max"]


def test_assemble_row_drops_panel_open_and_close() -> None:
    panel = {
        "date": "2026-08-13",
        "ticker": "btsg",
        "sources": ["flatten"],
        "src_rank": 0,
        "boxes": {"join": "good", "vol": "bad", "news": "good", "ab": "good"},
        "blue": False,
        "alarm": True,
        "zero_red": True,
        "open": 59.8,
        "close": 60.23,
    }
    feat = {"ok": True, "ret_1": 1.0, "ret_5": 2.0, "ret_10": 3.0, "rvol": 2.0,
            "hot_score": 1.5, "break_10": False, "last_green": True, "last_red": False,
            "candle_score": 0.2, "candle_capture": False}
    row = assemble_row(panel, feat, {"BTSG": "bad"}, {}, None, 0.5)
    assert "open" not in row and "close" not in row
    assert row["ticker"] == "BTSG"
    assert row["boxes"]["vol"] == "good"
    assert row["boxes"]["news"] == "bad"
    assert row["boxes"]["ab"] == "missing"
    assert row["boxes"]["join"] == "good"
    assert row["alarm"] is True
    assert row["erd_earn_react"] is False
    assert row["rs_week"] == 1.5
    assert row["cond_good"] == 2  # join good, vol good; news bad; ab missing
    assert row["cond_bad"] == 1


def test_assemble_row_empty_spy_leaves_rs_week_empty() -> None:
    """SPY is absent from the clean tape, so rs_week stays empty."""
    panel = {
        "date": "2026-08-13",
        "ticker": "btsg",
        "sources": ["flatten"],
        "src_rank": 0,
        "boxes": {},
        "blue": False,
        "alarm": False,
        "zero_red": False,
    }
    feat = {"ok": True, "ret_1": 1.0, "ret_5": 2.0, "ret_10": 3.0, "rvol": 1.0,
            "hot_score": 0.5, "break_10": False, "last_green": False, "last_red": False,
            "candle_score": 0.0, "candle_capture": False}
    row = assemble_row(panel, feat, None, None, None, None)
    assert row["rs_week"] is None


def test_same_day_bar_is_refused() -> None:
    try:
        feature_bars([{"date": "2026-08-13", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}], "2026-08-13")
    except SameDayBarError:
        return
    raise AssertionError("same-day bar was accepted")


def test_pins_are_the_earliest_commits() -> None:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    wanted = {(row["date"], row["input"]): row for row in data["inputs"]}
    log = subprocess.check_output(
        ["git", "log", "--reverse", "--pretty=format:COMMIT %H", "--name-status",
         "--", "data/stock_book", "01_daily/news", "data/ab_checklist", "data/exports",
         "data/join", "01_daily/catalyst"],
        text=True,
    )
    commit = None
    first: dict[str, str] = {}
    for line in log.splitlines():
        if line.startswith("COMMIT "):
            commit = line.split()[1]
            continue
        if not line.strip() or commit is None:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        path = parts[-1]
        if path not in first and parts[0][:1] in ("A", "C", "R"):
            first[path] = commit
    for row in data["inputs"]:
        if row["input"] == "panel":
            continue
        assert first[row["path"]] == row["commit"], row["path"]
        blob = subprocess.check_output(
            ["git", "rev-parse", f"{row['commit']}:{row['path']}"],
            text=True,
        ).strip()
        assert blob == row["blob_sha"]
    panel_commits = subprocess.check_output(
        ["git", "log", "--reverse", "--format=%H", "--", "data/factor_mine/panel.json"],
        text=True,
    ).split()
    seen: dict[str, tuple[str, str, int]] = {}
    for rev in panel_commits:
        blob = subprocess.check_output(
            ["git", "rev-parse", f"{rev}:data/factor_mine/panel.json"],
            text=True,
        ).strip()
        raw = subprocess.check_output(["git", "cat-file", "-p", blob])
        parsed = json.loads(raw)
        counts: dict[str, int] = {}
        for item in parsed.get("rows") or []:
            if isinstance(item, dict) and item.get("date"):
                counts[item["date"]] = counts.get(item["date"], 0) + 1
        for day, count in counts.items():
            if day not in seen:
                seen[day] = (rev, blob, count)
    for day in SESSIONS:
        rev, blob, count = seen[day]
        pinned = wanted[(day, "panel")]
        assert pinned["commit"] == rev
        assert pinned["blob_sha"] == blob
        assert pinned["n_rows"] == count


def _pr_changed_files() -> list[str]:
    """Files this branch changes against the PR base.

    A two-dot diff from the original branch point also lists files main
    gained later. CI sees those when it checks out the pull-request merge.
    Three-dot from the PR base is this study's own diff.
    """
    base = os.environ.get("LEVER_CLEAN_DIFF_BASE", "").strip()
    if not base:
        for candidate in ("origin/main", "main"):
            probe = subprocess.run(
                ["git", "rev-parse", "--verify", "--quiet", candidate],
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0:
                base = candidate
                break
    if not base:
        raise AssertionError("PR base is not available")
    return subprocess.check_output(
        ["git", "diff", "--name-only", f"{base}...HEAD"],
        text=True,
    ).splitlines()


def test_diff_does_not_touch_an_existing_file() -> None:
    names = _pr_changed_files()
    assert names, "PR diff is empty"
    for name in names:
        assert name.startswith(ALLOWED) or name in ALLOWED, name


def test_report_keeps_the_study_label() -> None:
    text = (RETURNS / "REPORT.md").read_text(encoding="utf-8")
    assert STUDY_LABEL in text
    assert "9,500" in text
    assert "| struck |" not in text
    assert "rebuild_match` does not strike" in text


def main() -> None:
    tests = [
        test_fingerprint_matches_covered_bytes,
        test_manifest_covers_every_session_and_names_the_fallback,
        test_group_counts_match_the_prereg_table,
        test_assemble_row_drops_panel_open_and_close,
        test_assemble_row_empty_spy_leaves_rs_week_empty,
        test_same_day_bar_is_refused,
        test_pins_are_the_earliest_commits,
        test_diff_does_not_touch_an_existing_file,
        test_report_keeps_the_study_label,
    ]
    for test in tests:
        test()
        print("ok", test.__name__)


if __name__ == "__main__":
    main()
