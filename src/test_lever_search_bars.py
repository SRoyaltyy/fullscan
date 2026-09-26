"""Group 3 bar guard and pinned proof-day counts.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_bars

A feature read must not see a bar dated the session or later. The session
open is allowed only when the frozen recipe trades at the 09:30 open.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from src.lever_search_bars import (
    BAR_BLOB_SHA,
    BAR_COMMIT,
    BAR_SHA256,
    SameDayBarError,
    feature_bars,
    fill_open,
)
from src.lever_search_proof import (
    PROOF_BLOB_SHA,
    PROOF_COMMIT,
    PROOF_SHA256,
    PROVEN_DATES,
    build_group3_recipes,
    check_days,
    group_day_report,
    input_day_is_usable,
    overall_day_report,
    row_is_usable,
    search_days,
)

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "research" / "lever_search" / "PREREG.md"
MANIFEST = ROOT / "research" / "lever_search" / "INPUT_ASOF_MANIFEST.json"
WORKFLOW = ROOT / ".github" / "workflows" / "lever_search_append_only.yml"

GROUPS = (
    (("stock_book",), 48, 10, 7),
    (("actions", "stock_book"), 22, 4, 1),
    ((), 16, 25, 16),
    (("export", "stock_book"), 6, 8, 5),
    (("join", "stock_book"), 4, 0, 0),
    (("ab", "actions", "stock_book"), 2, 1, 1),
    (("ab", "stock_book"), 2, 5, 5),
    (("actions",), 2, 11, 6),
    (("actions", "export", "stock_book"), 2, 3, 0),
    (("actions", "join", "stock_book"), 2, 0, 0),
    (("catalyst", "stock_book"), 2, 0, 0),
    (("export",), 2, 14, 11),
)


def test_feature_bars_reject_same_day_and_later() -> None:
    session = "2026-08-13"
    prior = {"date": "2026-08-12", "open": 1, "high": 2, "low": 0.5, "close": 1.5}
    assert feature_bars([prior], session) == [prior]
    assert feature_bars([], session) == []
    for day in (session, "2026-08-14", "2026-09-11"):
        try:
            feature_bars([prior, {"date": day, "close": 9}], session)
        except SameDayBarError as exc:
            assert "same-day-or-later" in str(exc)
        else:
            raise AssertionError(day)


def test_fill_open_is_the_only_same_day_field() -> None:
    bar = {"date": "2026-08-13", "open": 4.25}
    assert fill_open(bar, trades_at_open=True) == 4.25
    try:
        fill_open(bar, trades_at_open=False)
    except SameDayBarError as exc:
        assert "session open" in str(exc)
    else:
        raise AssertionError("open was read without a 09:30 trade")
    for field in ("high", "low", "close", "volume"):
        leaked = dict(bar)
        leaked[field] = 1
        try:
            fill_open(leaked, trades_at_open=True)
        except SameDayBarError as exc:
            assert field in str(exc)
        else:
            raise AssertionError(field)


def test_proof_day_counts_and_usable_rule() -> None:
    assert PROOF_COMMIT == "1bf94d7dc3ce00c29667d0fae6fdb6bb8effb73c"
    assert PROOF_BLOB_SHA == "832eaa4fdc19d368d87a4dad5c3edd3c200b0a54"
    assert PROOF_SHA256 == "1bfafd9d4f9a512b9bc3ac33d6d087349028f57ec4d0fedffe76ee0998e48f75"
    assert len(PROVEN_DATES["stock_book"]) == 19
    assert len(PROVEN_DATES["S"]) == 27
    assert len(PROVEN_DATES["predict"]) == 27
    assert len(PROVEN_DATES["hard_red"]) == 27
    assert len(PROVEN_DATES["sector_predict"]) == 21
    assert len(PROVEN_DATES["ab_checklist"]) == 18
    assert len(PROVEN_DATES["ab_enriched"]) == 18
    both = set(PROVEN_DATES["ab_checklist"]) & set(PROVEN_DATES["ab_enriched"])
    assert len(both) == 16
    assert len(PROVEN_DATES["actions"]) == 12
    assert len(PROVEN_DATES["judge"]) == 6
    assert len(PROVEN_DATES["map_heat"]) == 4
    assert len(PROVEN_DATES["catalyst"]) == 2
    assert PROVEN_DATES["join"] == ()
    assert row_is_usable(proven="yes", stale_content="yes", headline=False)
    assert row_is_usable(proven="yes", stale_content="no", headline=True)
    assert not row_is_usable(proven="yes", stale_content="yes", headline=True)
    assert not row_is_usable(proven="no", stale_content="no", headline=False)
    assert not row_is_usable(proven="yes", stale_content="n/a", headline=True)
    assert input_day_is_usable(
        [{"path": "a.json", "proven": "yes", "stale_content": "no"}],
        headline=True,
    )
    assert not input_day_is_usable(
        [{"path": "01_daily/news/", "proven": "yes", "stale_content": "no"}],
        headline=True,
    )
    assert not input_day_is_usable([], headline=False)
    assert not input_day_is_usable(
        [
            {"path": "a.json", "proven": "yes", "stale_content": "n/a"},
            {"path": "b.json", "proven": "no", "stale_content": "n/a"},
        ],
        headline=False,
    )


def test_group3_search_and_check_days() -> None:
    recipes = build_group3_recipes()
    assert len(recipes) == 110
    assert all(recipe["trades_at_open"] is True for recipe in recipes)
    overall = overall_day_report()
    assert overall["search_min"] == 0
    assert overall["search_median"] == 10
    assert overall["search_max"] == 25
    assert overall["check_min"] == 0
    assert overall["check_median"] == 7
    assert overall["check_max"] == 16
    rows = group_day_report()
    found = {
        (row["roles"], row["n_recipes"], int(row["search_min"]), int(row["check_min"]))
        for row in rows
    }
    assert found == set(GROUPS)
    for row in rows:
        assert row["search_min"] == row["search_max"] == row["search_median"]
        assert row["check_min"] == row["check_max"] == row["check_median"]
    # A recipe with no fullscan role searches the whole run window.
    price_only = next(recipe for recipe in recipes if recipe["name"] == "union_h1")
    assert len(search_days(price_only)) == 25
    assert len(check_days(price_only)) == 16
    joined = next(recipe for recipe in recipes if recipe["name"] == "union_join_g_h1")
    assert search_days(joined) == ()
    assert check_days(joined) == ()


def test_prereg_pins_and_manifest_sha() -> None:
    text = PREREG.read_text(encoding="utf-8")
    raw = MANIFEST.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    assert f"sha256 `{digest}`" in text
    assert PROOF_COMMIT in text
    assert PROOF_BLOB_SHA in text
    assert PROOF_SHA256 in text
    assert BAR_COMMIT in text
    assert BAR_BLOB_SHA in text
    assert BAR_SHA256 in text
    assert "stale_content" in text
    assert "A1-A15" in text
    assert "0/3" in text
    assert "split-adjusted" in text
    assert "search days min 0, median 10, max 25" in text
    assert "Check days min 0, median 7, max 16" in text
    assert "trades at the 09:30 open" in text
    assert "src.test_lever_search_bars" in WORKFLOW.read_text(encoding="utf-8")
    import json
    manifest = json.loads(raw)
    days = manifest["group3_days"]
    assert days["stock_book"] == 19
    assert days["search_median"] == 10
    assert days["check_median"] == 7
    proof = manifest["fullscan_file_proof"]
    assert proof["commit"] == PROOF_COMMIT
    assert proof["blob_sha"] == PROOF_BLOB_SHA
    bar = next(item for item in manifest["pinned_files"] if item["path"].endswith("ohlc.parquet"))
    assert bar["blob_sha"] == BAR_BLOB_SHA
    assert bar["sha256"] == BAR_SHA256
    assert "split-adjusted" in bar["adjustment"]


def main() -> None:
    tests = [
        test_feature_bars_reject_same_day_and_later,
        test_fill_open_is_the_only_same_day_field,
        test_proof_day_counts_and_usable_rule,
        test_group3_search_and_check_days,
        test_prereg_pins_and_manifest_sha,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
