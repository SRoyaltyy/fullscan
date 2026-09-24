"""Cheapest SuperGrok model above 30B.

stdlib only — workflow_selfcheck has no pip. Do not import news_impact,
deepseek_client, or requests here.
"""
from __future__ import annotations

from src.openclaw_models import (
    DEFAULT_NEWS_MODEL,
    MIN_PARAMS_B,
    SECTOR_MODEL,
    above_30b,
    is_refused,
    pick_cheapest_above_30b,
    try_order,
)


def test_default_is_cheapest_general_above_30b() -> None:
    assert MIN_PARAMS_B == 30
    assert DEFAULT_NEWS_MODEL == "xai/grok-4.3"
    assert above_30b(DEFAULT_NEWS_MODEL)
    assert not is_refused(DEFAULT_NEWS_MODEL)
    assert pick_cheapest_above_30b() == DEFAULT_NEWS_MODEL
    assert SECTOR_MODEL == "xai/grok-4.6"
    assert DEFAULT_NEWS_MODEL != SECTOR_MODEL


def test_refuses_mini_and_coding() -> None:
    assert is_refused("xai/grok-3-mini")
    assert is_refused("grok-build-0.1")
    assert is_refused("openclaw/default")
    assert not above_30b("xai/grok-3-mini")


def test_live_list_prefers_legacy_fast_when_present() -> None:
    assert pick_cheapest_above_30b([
        "xai/grok-4.6", "xai/grok-4.1-fast", "xai/grok-4.7",
    ]) == "xai/grok-4.1-fast"
    assert pick_cheapest_above_30b([
        "grok-4.6", "grok-4.3",
    ]) == "xai/grok-4.3"


def test_try_order_includes_sector_fallback() -> None:
    order = try_order(None)
    assert order[0] == DEFAULT_NEWS_MODEL
    assert SECTOR_MODEL in order


def test_module_stays_stdlib_for_selfcheck() -> None:
    imports = [
        line for line in open(__file__, encoding="utf-8")
        if line.startswith("import ") or line.startswith("from ")
    ]
    blob = "".join(imports)
    assert "news_impact" not in blob
    assert "deepseek_client" not in blob
    assert "requests" not in blob


def main() -> None:
    tests = [
        test_default_is_cheapest_general_above_30b,
        test_refuses_mini_and_coding,
        test_live_list_prefers_legacy_fast_when_present,
        test_try_order_includes_sector_fallback,
        test_module_stays_stdlib_for_selfcheck,
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
