"""Table-driven unit tests for news_impact harvest + label hygiene.

Every new filter has its own cases. Not a new taxonomy.
"""
from __future__ import annotations

from src.news_impact.classify import classify_text
from src.news_impact.grade import (
    is_gradeable,
    performance_rollup,
    ungraded_reason,
)
from src.news_impact.hygiene import (
    REACTION_TITLE_CASES,
    collapse_macro_stories,
    entry_clock_of,
    guidance_hygiene,
    is_reaction_title,
    majority_basket_agree,
    macro_story_key,
    skips_01d_horizon,
)
from src.news_impact.pipeline import analyze_article


def test_reaction_title_table() -> None:
    for title, want in REACTION_TITLE_CASES:
        got = is_reaction_title(title)
        assert got is want, (title, got, want)


def test_reaction_titles_are_discard_not_tradable() -> None:
    cases = [
        "AAPL plunges 7% after the print",
        "Salesforce’s stock surges as AI momentum fuels revenue growth",
        "Stocks rebound as crude oil weakens",
        "Gold falls 2% amid firmer dollar",
        "Update drives 4% drop in the shares",
    ]
    for title in cases:
        row = analyze_article({"title": title}, persist=False)
        cls = row["classification"]
        assert cls["event_class"] == "discard", (title, cls)
        assert cls["q5"] == "regime", (title, cls)
        assert row["entities"] == [], title
        assert row["usable"] is False, title
        assert row["tradable"] is False, title
        for g in [
            {
                "event_class": "discard",
                "q5": "regime",
                "direction": "up",
                "tradeable_expression": "direct",
                "kind": "ticker",
            }
        ]:
            assert is_gradeable(g, row) is False


def test_reaction_does_not_eat_real_impulse() -> None:
    keep = [
        "ASML nearly sold out of 2027 EUV capacity amid very strong AI-driven demand",
        "Boston Scientific says cyberattack will materially hit Q3",
        "Amgen gets FDA approval to update IMDELLTRA label",
        "Company raises fiscal 2026 guidance",
    ]
    for title in keep:
        assert is_reaction_title(title) is False, title
        row = analyze_article({"title": title}, persist=False)
        assert row["classification"]["event_class"] != "discard" or row["classification"]["q5"] != "regime" or row["usable"], (
            title, row["classification"]
        )


def test_guidance_reaffirm_is_not_raise() -> None:
    cases = [
        "Cencora reaffirms fiscal 2026 adjusted EPS guidance despite Walgreens shift",
        "Elevance Health reaffirmed its 2026 guidance for at least $27.00",
        "Intuit is reaffirming fiscal 2027 financial guidance",
        "Issuer maintains guidance for the full year",
    ]
    for title in cases:
        hy = guidance_hygiene(title)
        assert hy["sign"] is None, (title, hy)
        assert hy["direction"] == "not_determined", (title, hy)
        row = analyze_article({"title": title}, persist=False)
        assert row["classification"]["event_class"] == "guidance", (title, row["classification"])
        assert row["classification"]["sign"] is None, (title, row["classification"])
        for e in row["entities"]:
            assert e.get("direction") == "not_determined", (title, e)
        assert row["tradable"] is False, title


def test_guidance_raise_plus_miss_eps_is_mixed() -> None:
    title = "Salesforce raises guidance but misses EPS estimates"
    hy = guidance_hygiene(title)
    assert hy["direction"] == "mixed", hy
    assert hy["sign"] is None, hy
    assert hy["split"] is True, hy
    assert "print_vs_priced" in hy["split_facts"]
    row = analyze_article({"title": title}, persist=False)
    assert row["classification"]["event_class"] == "guidance"
    assert row["classification"]["sign"] is None
    assert row["classification"]["split"] is True
    dirs = {e.get("direction") for e in row["entities"]}
    assert "up" not in dirs
    assert dirs <= {"mixed", "not_determined"} or "mixed" in dirs


def test_guidance_raise_still_up() -> None:
    title = "Salesforce raises fiscal 2026 guidance after a strong quarter"
    hy = guidance_hygiene(title)
    assert hy["sign"] == "raise"
    assert hy["direction"] == "up"
    row = analyze_article({"title": title}, persist=False)
    assert row["classification"]["sign"] == "raise"
    ups = [e for e in row["entities"] if e.get("direction") == "up"]
    assert ups, row["entities"]


def test_guidance_cut_still_down() -> None:
    title = "Salesforce cuts full-year guidance after a soft outlook"
    hy = guidance_hygiene(title)
    assert hy["sign"] == "cut"
    assert hy["direction"] == "down"
    row = analyze_article({"title": title}, persist=False)
    assert row["classification"]["event_class"] == "guidance"
    assert row["classification"]["sign"] == "cut"


def test_entry_clock_published_vs_retrieved_only() -> None:
    pub = {
        "published_at": "Wed, 17 Sep 2026 08:00:00 -0400",
        "retrieved_at": "2026-09-17T04:17:45-04:00",
    }
    assert entry_clock_of(pub) == "published"
    missing = {"published_at": "", "retrieved_at": "2026-09-17T04:17:45-04:00"}
    assert entry_clock_of(missing) == "retrieved_only"
    none = {"retrieved_at": "2026-09-17T04:17:45-04:00"}
    assert entry_clock_of(none) == "retrieved_only"

    row = analyze_article(
        {
            "title": "Airlines Scramble for Jet Fuel as Hormuz Disruption Drags On",
            "published_at": "Wed, 17 Sep 2026 08:00:00 -0400",
            "retrieved_at": "2026-09-17T04:17:45-04:00",
        },
        persist=False,
    )
    assert row["entry_clock"] == "published"
    assert row["published_at"].startswith("Wed, 17 Sep 2026")

    row2 = analyze_article(
        {
            "title": "Airlines Scramble for Jet Fuel as Hormuz Disruption Drags On",
            "retrieved_at": "2026-09-17T04:17:45-04:00",
        },
        persist=False,
    )
    assert row2["entry_clock"] == "retrieved_only"
    assert row2["published_at"] == ""
    assert row2["retrieved_at"].startswith("2026-09-17")


def test_macro_collapse_one_row_per_factor_session_sign() -> None:
    def story(title, factor, sign, session, agrees):
        legs = []
        for tick, direction, agree in (
            ("QQQ", "up", agrees[0]),
            ("TLT", "up", agrees[1]),
            ("UUP", "down", agrees[2]),
            ("HYG", "up", agrees[3]),
            ("SPY", "up", agrees[4]),
        ):
            legs.append({
                "ticker": tick,
                "kind": "ticker",
                "direction": direction,
                "tradeable_expression": "proxy",
                "event_class": "factor_impulse",
                "q5": "impulse",
                "entry_date": session,
                "ret_1d": 1.0 if agree else -1.0,
                "ret_20d": 2.0 if agree else -2.0,
                "agree_1d": agree,
                "agree_20d": agree,
            })
        return {
            "title": title,
            "usable": True,
            "macro_factor": factor,
            "classification": {
                "event_class": "factor_impulse",
                "q5": "impulse",
                "sign": sign,
                "factor": factor,
            },
            "published_at": f"{session}T08:00:00-04:00",
            "performance": legs,
        }

    a = story("Fed holds rates — Reuters", "rates", "cut", "2026-09-17",
              [True, True, True, False, True])
    b = story("Fed holds rates — Bloomberg reprint", "rates", "cut", "2026-09-17",
              [False, False, False, False, False])
    c = story("Hot CPI print", "inflation", "raise", "2026-09-17",
              [True, True, True, True, False])
    bag = collapse_macro_stories([a, b, c])
    assert bag["n_stories"] == 2, bag
    assert bag["reprints_collapsed"] == 1, bag
    # reprint of the same FOMC hold does not add 5 extra legs
    assert bag["leg_n_1d"] == 10, bag  # 5 + 5 unique (ticker, session, sign)
    # headline: 2 stories with majority votes
    assert bag["headline_n_1d"] == 2, bag
    # first story majority True (4/5); second majority True (4/5)
    assert bag["headline_hit_1d"] == 2, bag
    assert majority_basket_agree(a["performance"], "agree_1d") is True
    assert macro_story_key(a) == macro_story_key(b)

    # Headline rates are not the graded 0-1d / 1-4w columns.
    roll = performance_rollup([a, b, c])
    assert roll["n_1d"] == 0, roll  # factor_impulse stays ungraded
    assert roll["macro_headline"]["headline_n_1d"] == 2
    assert roll["slices"]["factor_impulse"]["graded"] is False
    assert roll["slices"]["factor_impulse"]["n_1d"] == 10


def test_horizon_skips_01d_for_long_classes() -> None:
    cases = [
        ({"event_class": "gate", "horizon": "1-6m", "direction": "up",
          "q5": "impulse", "tradeable_expression": "direct", "kind": "ticker"},
         True),
        ({"event_class": "capacity", "horizon": "1-6m", "direction": "up",
          "q5": "impulse", "tradeable_expression": "direct", "kind": "ticker"},
         True),
        ({"event_class": "blast_cyber", "horizon": "1-4w", "direction": "down",
          "q5": "impulse", "tradeable_expression": "direct", "kind": "ticker"},
         True),
        ({"event_class": "input_cost", "horizon": "1-4w", "direction": "down",
          "q5": "impulse", "tradeable_expression": "direct", "kind": "ticker"},
         False),
        ({"event_class": "guidance", "horizon": "0-1d", "direction": "up",
          "q5": "impulse", "tradeable_expression": "direct", "kind": "ticker"},
         False),
    ]
    for target, want in cases:
        assert skips_01d_horizon(target) is want, (target, want)

    chips = analyze_article(
        {"title": "Foundry wins $2B CHIPS Act award for a new fab"},
        persist=False,
    )
    assert chips["classification"]["event_class"] == "gate"
    assert skips_01d_horizon(
        {"event_class": "gate", "horizon": "1-6m"}, chips,
    ) is True

    gate = {
        "usable": True,
        "title": "Amgen gets FDA approval to update IMDELLTRA label",
        "classification": {"event_class": "gate", "q5": "impulse"},
        "performance": [{
            "ticker": "AMGN",
            "kind": "ticker",
            "direction": "up",
            "horizon": "1-6m",
            "tradeable_expression": "direct",
            "event_class": "gate",
            "q5": "impulse",
            "ret_1d": -1.0,
            "ret_20d": 3.0,
            "agree_1d": False,
            "agree_20d": True,
        }],
    }
    cap = {
        "usable": True,
        "title": "ASML nearly sold out of 2027 EUV capacity",
        "classification": {"event_class": "capacity", "q5": "impulse"},
        "performance": [{
            "ticker": "ASML",
            "kind": "ticker",
            "direction": "up",
            "horizon": "1-6m",
            "tradeable_expression": "direct",
            "event_class": "capacity",
            "q5": "impulse",
            "ret_1d": -2.0,
            "ret_20d": 4.0,
            "agree_1d": False,
            "agree_20d": True,
        }],
    }
    cyber = {
        "usable": True,
        "title": "Boston Scientific says cyberattack will hit Q3",
        "classification": {"event_class": "blast_cyber", "q5": "impulse"},
        "performance": [{
            "ticker": "BSX",
            "kind": "ticker",
            "direction": "down",
            "horizon": "1-4w",
            "tradeable_expression": "direct",
            "event_class": "blast_cyber",
            "q5": "impulse",
            "ret_1d": 1.0,
            "ret_20d": -3.0,
            "agree_1d": False,
            "agree_20d": True,
        }],
    }
    cheap = {
        "usable": True,
        "title": "Jet fuel lifts airline costs",
        "classification": {"event_class": "input_cost", "q5": "impulse"},
        "performance": [{
            "ticker": "AAL",
            "kind": "ticker",
            "direction": "down",
            "horizon": "1-4w",
            "tradeable_expression": "direct",
            "event_class": "input_cost",
            "q5": "impulse",
            "ret_1d": -1.0,
            "ret_20d": -2.0,
            "agree_1d": True,
            "agree_20d": True,
        }],
    }
    roll = performance_rollup([gate, cap, cyber, cheap])
    # 0-1d skips gate/capacity/cyber; input_cost still counts
    assert roll["n_1d"] == 1, roll
    assert roll["hit_1d"] == 1
    # 1-4w still grades all four (impulse + up/down + direct)
    assert roll["n_20d"] == 4, roll
    assert roll["hit_20d"] == 4
    assert ungraded_reason(gate["performance"][0], gate) is None


def test_classify_text_reaction_and_guidance_helpers() -> None:
    d = classify_text("Shares plunged 11% on the tape")
    assert d.event_class == "discard"
    assert d.q5 == "regime"
    g = classify_text("Cencora reaffirms fiscal 2026 guidance")
    assert g.event_class == "guidance"
    assert g.sign is None


def main() -> None:
    tests = [
        test_reaction_title_table,
        test_reaction_titles_are_discard_not_tradable,
        test_reaction_does_not_eat_real_impulse,
        test_guidance_reaffirm_is_not_raise,
        test_guidance_raise_plus_miss_eps_is_mixed,
        test_guidance_raise_still_up,
        test_guidance_cut_still_down,
        test_entry_clock_published_vs_retrieved_only,
        test_macro_collapse_one_row_per_factor_session_sign,
        test_horizon_skips_01d_for_long_classes,
        test_classify_text_reaction_and_guidance_helpers,
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
