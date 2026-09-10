"""Combo morning tickets are the union of member pick_day lists.

Run: PYTHONPATH=. python3 -m src.test_strategy_tickets
"""
from __future__ import annotations

from src import strategy_tickets as st


class _FakeFM:
    def __init__(self, picks: dict):
        self.picks = picks

    def pick_day(self, rows, rec):
        return list(self.picks.get(rec["name"], []))


def test_combo_would_buy_unions_member_lists() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
        "union_hot_n4_h1": {"name": "union_hot_n4_h1", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "SPIR"}, {"ticker": "AEHR"}],
        "union_e_fresh_h3": [{"ticker": "AVO"}],
        "union_hot_n4_h1": [{"ticker": "INTC"}, {"ticker": "AEHR"}],
    })
    buys = st._combo_would_buy(
        [], rec_by,
        ["short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"],
        {"net": "priority"}, fm,
    )
    names = [b["ticker"] for b in buys]
    assert names == ["SPIR", "AEHR", "AVO", "INTC"]
    assert next(b for b in buys if b["ticker"] == "AEHR")["src"] == "short_news_r_h3"


def test_combo_skip_drops_long_and_short_clash() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "AEHR"}, {"ticker": "SPIR"}],
        "union_e_fresh_h3": [{"ticker": "AEHR"}, {"ticker": "AVO"}],
    })
    buys = st._combo_would_buy(
        [], rec_by, ["short_news_r_h3", "union_e_fresh_h3"],
        {"net": "skip"}, fm,
    )
    assert [b["ticker"] for b in buys] == ["SPIR", "AVO"]


def test_recipe_strats_never_emits_combo_needs_mine() -> None:
    """A combo is a union of member 09:30 lists — not a mine-only stub."""
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "combo_needs_mine" not in src
    assert "_combo_would_buy" in src
    rows = st.recipe_strats("2026-09-10")
    combos = [r for r in rows if r["name"].startswith("combo_")]
    assert combos, "combo_specs should be attached even without a blotter md"
    bad = [r["name"] for r in combos if r.get("status") == "combo_needs_mine"]
    assert not bad, bad
    # At least one mix should inherit a member name that pick_day already lists.
    with_names = [r for r in combos if r.get("buy_n")]
    assert with_names, "expected at least one combo shopping list from the panel"


def main() -> None:
    test_combo_would_buy_unions_member_lists()
    test_combo_skip_drops_long_and_short_clash()
    test_recipe_strats_never_emits_combo_needs_mine()
    print("ok")


if __name__ == "__main__":
    main()
