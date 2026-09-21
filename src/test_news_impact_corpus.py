"""Table-driven tests for corpus inventory, mix, and class horizons."""
from __future__ import annotations

from src.news_impact.corpus import (
    dedupe_titles,
    funnel_from_results,
    inventory,
    load_finviz_exports,
)
from src.news_impact.horizons import default_horizon, skips_short_window, window_grid
from src.news_impact.hygiene import is_reaction_title
from src.news_impact.mix import is_first_party, is_high_mass, mix_book, score_mix_books
from src.news_impact.pipeline import analyze_article


def test_inventory_lists_empty_and_unused() -> None:
    inv = inventory()
    names = {s["name"]: s for s in inv["sources"]}
    assert names["parsed_json"]["status"] == "used"
    assert names["finviz_export"]["status"] == "used"
    assert names["grok_automations"]["status"] == "empty"
    assert names["rss_dumps"]["status"] == "empty"
    assert names["supabase_dumps"]["status"] == "empty"
    assert names["theme_radar_snapshots"]["status"] == "unused_readonly"
    assert inv["window"]["june_2026_parse"] is False
    assert inv["theme_radar"]["readonly"] is True
    assert "Do not merge the repos" in inv["theme_radar"]["export_ask"]


def test_finviz_export_has_news_title_and_ticker() -> None:
    rows = load_finviz_exports("2026-09-21")
    assert rows, "expected News Title rows on the 09-21 export"
    assert any(r.get("ticker_hint") for r in rows)
    assert any(r.get("published_at") for r in rows)
    # ticker_hint maps a named dividend when extract_named would miss nothing
    agilent = [r for r in rows if r["ticker_hint"] == "A" and "dividend" in r["title"].lower()]
    assert agilent


def test_reaction_and_reaffirm_still_hold_on_finviz_title() -> None:
    assert is_reaction_title("Agilent stock falls Wednesday, still outperforms") is True
    row = analyze_article(
        {"title": "Cencora reaffirms fiscal 2026 adjusted EPS guidance",
         "ticker_hint": "COR"},
        persist=False,
    )
    assert row["classification"]["sign"] is None
    assert all(e.get("direction") == "not_determined" for e in row["entities"])


def test_class_horizons_defaults() -> None:
    assert default_horizon("inventory_print") == "0-1d"
    assert default_horizon("blast_ops") == "0-1d"
    assert default_horizon("guidance", sign="raise") == "1-4w"
    assert default_horizon("guidance", sign=None) == "1-4w"
    assert default_horizon("print_vs_priced") == "1-4w"
    assert default_horizon("gate") == "1-6m"
    assert default_horizon("capacity") == "1-6m"
    assert default_horizon("trial_readout") == "1-6m"
    assert skips_short_window("gate") is True
    assert skips_short_window("inventory_print") is False
    row = analyze_article(
        {"title": "Salesforce raises fiscal 2026 guidance after a strong quarter"},
        persist=False,
    )
    assert row["classification"]["event_class"] == "guidance"
    assert all(e.get("horizon") == "1-4w" for e in row["entities"] if e.get("ticker"))


def test_window_grid_skips_6m_as_01d_miss() -> None:
    gate = {
        "usable": True,
        "title": "Amgen gets FDA approval",
        "classification": {"event_class": "gate", "q5": "impulse", "sign": "open"},
        "performance": [{
            "ticker": "AMGN", "kind": "ticker", "direction": "up",
            "horizon": "1-6m", "tradeable_expression": "direct",
            "event_class": "gate", "q5": "impulse",
            "ret_1d": -5.0, "agree_1d": False,
            "ret_20d": 2.0, "agree_20d": True,
            "ret_63d": 4.0, "agree_63d": True,
        }],
    }
    inv = {
        "usable": True,
        "title": "EIA crude inventory build",
        "classification": {"event_class": "inventory_print", "q5": "impulse"},
        "performance": [{
            "ticker": "DVN", "kind": "ticker", "direction": "down",
            "horizon": "0-1d", "tradeable_expression": "direct",
            "event_class": "inventory_print", "q5": "impulse",
            "ret_1d": -1.0, "agree_1d": True,
            "ret_20d": None, "agree_20d": None,
        }],
    }
    grid = window_grid([gate, inv])
    assert grid["gate"]["n_1d"] == 0, grid  # not a 0-1d miss
    assert grid["gate"]["n_20d"] == 1
    assert grid["gate"]["n_63d"] == 1
    assert grid["inventory_print"]["n_1d"] == 1
    assert grid["inventory_print"]["hit_1d"] == 1


def test_mix_converge_conflict_singleton() -> None:
    def art(title, tick, direction, src, published="2026-09-17T08:00:00-04:00"):
        return {
            "title": title,
            "usable": True,
            "harvest_source": src,
            "source": src,
            "published_at": published,
            "classification": {"event_class": "print_vs_priced", "q5": "impulse"},
            "entities": [{
                "ticker": tick, "name": tick, "direction": direction,
                "tradeable_expression": "direct",
            }],
            "performance": [{
                "ticker": tick, "kind": "ticker", "direction": direction,
                "tradeable_expression": "direct",
                "event_class": "print_vs_priced", "q5": "impulse",
                "entry_date": "2026-09-17",
                "ret_1d": 1.0 if direction == "up" else -1.0,
                "agree_1d": True,
            }],
        }

    a = art("Salesforce raises fiscal 2026 guidance", "CRM", "up", "parsed")
    b = art("CRM lifts FY26 outlook at investor day", "CRM", "up", "events")
    c = art("Solo name prints a beat", "AAL", "up", "parsed")
    d = art("Name misses estimates", "X", "down", "parsed")
    e = art("Name beats estimates", "X", "up", "events")
    mix = mix_book([a, b, c, d, e])
    assert mix["converge_n"] == 1, mix
    assert mix["converge"][0]["ticker"] == "CRM"
    assert mix["converge"][0]["direction"] == "up"
    assert mix["conflict_n"] == 1
    assert mix["conflict"][0]["ticker"] == "X"
    assert mix["conflict"][0]["grade"] is False
    assert mix["singleton_n"] == 1
    scores = score_mix_books([a, b, c, d, e], mix)
    assert scores["converge"]["n_1d"] == 1
    assert scores["singleton"]["n_1d"] == 1
    assert scores["conflict_n"] == 1


def test_first_party_outranks_finviz_wrap() -> None:
    sec = {
        "title": "SEC issues order granting temporary exemptive relief to Tokenized Securities Venues",
        "usable": True,
        "harvest_source": "parsed",
        "source": "rss",
        "published_at": "2026-09-17T08:00:00-04:00",
        "classification": {"event_class": "market_structure", "q5": "impulse"},
        "entities": [{"ticker": "COIN", "direction": "up", "tradeable_expression": "direct"}],
        "performance": [{
            "ticker": "COIN", "kind": "ticker", "direction": "up",
            "tradeable_expression": "direct", "event_class": "market_structure",
            "q5": "impulse", "entry_date": "2026-09-17",
            "ret_1d": 2.0, "agree_1d": True,
        }],
    }
    wrap = {
        "title": "Tokenized stocks in focus after the SEC note",
        "usable": True,
        "harvest_source": "finviz_export",
        "source": "finviz_export",
        "published_at": "2026-09-17T08:00:00-04:00",
        "classification": {"event_class": "market_structure", "q5": "impulse"},
        "entities": [{"ticker": "COIN", "direction": "down", "tradeable_expression": "direct"}],
        "performance": [{
            "ticker": "COIN", "kind": "ticker", "direction": "down",
            "tradeable_expression": "direct", "event_class": "market_structure",
            "q5": "impulse", "entry_date": "2026-09-17",
            "ret_1d": -1.0, "agree_1d": False,
        }],
    }
    assert is_first_party(sec) is True
    assert is_high_mass(sec) is True
    mix = mix_book([sec, wrap])
    assert mix["conflict_n"] == 0, mix
    assert mix["converge_n"] + mix["singleton_n"] >= 1
    # wrap down is dropped; first-party up remains
    rec = (mix["converge"] or mix["singleton"])[0]
    assert rec["direction"] == "up"
    assert rec["high_mass"] is True


def test_funnel_does_not_invent_five_digits() -> None:
    unique = [{"title": "AAPL plunges 4%"}, {"title": "ASML nearly sold out of 2027 EUV capacity"}]
    results = [
        analyze_article(unique[0], persist=False),
        analyze_article(unique[1], persist=False),
    ]
    fun = funnel_from_results(100, unique, results)
    assert fun["raw_headlines"] == 100
    assert fun["unique_after_dedupe"] == 2
    assert fun["impulse_updown_listed"] <= 2
    assert fun["has_tape_graded"] == 0  # no performance overlay in this unit test


def test_dedupe_titles() -> None:
    rows = dedupe_titles([
        {"title": "Hello World"},
        {"title": "hello world"},
        {"title": "Other"},
    ])
    assert len(rows) == 2


def main() -> None:
    tests = [
        test_inventory_lists_empty_and_unused,
        test_finviz_export_has_news_title_and_ticker,
        test_reaction_and_reaffirm_still_hold_on_finviz_title,
        test_class_horizons_defaults,
        test_window_grid_skips_6m_as_01d_miss,
        test_mix_converge_conflict_singleton,
        test_first_party_outranks_finviz_wrap,
        test_funnel_does_not_invent_five_digits,
        test_dedupe_titles,
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
