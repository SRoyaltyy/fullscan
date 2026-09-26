"""Walker locks. No cleaned tape and no 2026-09-14 session."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.engine import shares_to_keep, walk  # noqa: E402
from research.concentration_cap_v1.metrics import drop_compound, slice_book, window_row  # noqa: E402
from research.concentration_cap_v1.tune import assert_tune_only  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import walk as v4_walk  # noqa: E402
from research.factor_mine_recipe_search_v4.metrics import _drop_compound  # noqa: E402
from src.forward_shadow_v1 import initial_state, step_day  # noqa: E402
from src.paper_trade import load_fees  # noqa: E402


def _bars():
    sessions = ("2026-08-17", "2026-08-18", "2026-08-19")
    px = {
        ("AAA", "2026-08-17"): (10.0, 10.0),
        ("AAA", "2026-08-18"): (10.0, 12.0),
        ("AAA", "2026-08-19"): (12.0, 12.0),
        ("BBB", "2026-08-17"): (20.0, 20.0),
        ("BBB", "2026-08-18"): (20.0, 20.0),
        ("BBB", "2026-08-19"): (21.0, 21.0),
    }

    def price(ticker, session, which):
        bar = px.get((ticker, session))
        if not bar:
            return None
        value = bar[0] if which == "open" else bar[1]
        return float(value)

    def day(session, names, score):
        return {
            "session": session,
            "s": score,
            "rows": [{
                "ticker": name, "sources": ["union"], "src_rank": index,
                "ohlc_hot_score": 9 - index, "alarm": False, "boxes": {},
                "days_on_list": 2, "erd_earn_react": False,
            } for index, name in enumerate(names)],
        }

    days = [
        day("2026-08-17", ["AAA"], 1.0),
        day("2026-08-18", ["AAA"], 1.0),
        day("2026-08-19", ["AAA", "BBB"], 1.0),
    ]
    return days, price


def _recipe(**extra):
    row = {
        "id": "union_hot_n4_h1__w0__n4__cnone",
        "name": "union_hot_n4_h1__w0__n4__cnone",
        "universe": "union",
        "hold": 1,
        "top_n": 4,
        "rank": "hot_score",
        "forbid": {"alarm": True},
        "require": {},
        "sell": "list",
        "s_boost": "none",
        "weather": False,
        "exit_when": {},
        "weight_cap": None,
        "skip_first": False,
        "earn_news": False,
    }
    row.update(extra)
    return row


def test_uncapped_matches_v4() -> None:
    days, price = _bars()
    fees = load_fees()
    recipe = _recipe()
    ours = walk(days, recipe, fees, price)
    v4 = v4_walk(days, recipe, fees, price, "keep_held")
    if [round(day["equity"], 4) for day in ours["daily"]] != [round(day["equity"], 4) for day in v4["daily"]]:
        raise SystemExit(f"equity drift {ours['daily']} vs {v4['daily']}")
    if [day["bought"] for day in ours["daily"]] != [day["bought"] for day in v4["daily"]]:
        raise SystemExit("buys drifted")
    if [day["sold"] for day in ours["daily"]] != [day["sold"] for day in v4["daily"]]:
        raise SystemExit("sells drifted")
    if len(ours["closed"]) != len(v4["closed"]):
        raise SystemExit("closed drifted")


def test_trim_sits_and_does_not_top_up() -> None:
    days, price = _bars()
    fees = load_fees()
    capped = walk(days, _recipe(weight_cap=0.25, id="cap"), fees, price)
    # Day 1 buys AAA with the whole book. Day 2 is still only AAA, so the trim
    # cash has no new name to spend and the held share count must fall.
    second = capped["daily"][1]
    if second["bought"]:
        raise SystemExit(f"trim topped up {second}")
    if second["trimmed"] != ["AAA"]:
        raise SystemExit(f"trim missing {second}")
    if second["sold"]:
        raise SystemExit("trim counted as a full exit")
    # A new name on day 3 may be bought. AAA must not be bought again.
    third = capped["daily"][2]
    if "AAA" in third["bought"]:
        raise SystemExit("keep-held bought a held name")
    if "BBB" not in third["bought"]:
        raise SystemExit(f"new name was not funded {third}")


def test_shares_respect_the_cap_after_the_fee() -> None:
    fees = load_fees()
    equity = 10_000.0
    price = 10.0
    shares = 1_000
    keep = shares_to_keep(shares, price, equity, 0.25, fees)
    if keep >= shares or keep < 1:
        raise SystemExit(f"keep {keep}")
    sold = shares - keep
    from src.paper_trade import order_fees
    fee = float(order_fees(sold, price, "sell", fees))
    after = equity - fee
    if keep * price > 0.25 * after + 1e-6:
        raise SystemExit("still over the cap")
    if (keep + 1) * price <= 0.25 * (equity - float(order_fees(sold - 1, price, "sell", fees))) + 1e-6:
        raise SystemExit("kept fewer shares than the cap allows")


def test_drop_matches_v4_and_starts_from_prior_equity() -> None:
    book = {
        "closed": [],
        "daily": [
            {"equity": 21_000.0, "ret": 0.05, "ret_15": 0.05, "session": "2026-09-14",
             "bought": [], "sold": [], "trimmed": []},
            {"equity": 22_000.0, "ret": 22_000.0 / 21_000.0 - 1.0, "ret_15": 0.0,
             "session": "2026-09-15", "bought": [], "sold": [], "trimmed": []},
        ],
        "pnl_by_day": {
            "2026-09-14": {"BEST": 3_000.0, "OTHER": -2_000.0},
            "2026-09-15": {"BEST": 500.0, "OTHER": 500.0},
        },
        "start_equity": 20_000.0,
    }
    if abs(drop_compound(book, ["BEST"]) - _drop_compound(book, "BEST")) > 1e-12:
        raise SystemExit("single-name drop drifted from v4")
    both = drop_compound(book, ["BEST", "OTHER"])
    # Both names are the whole equity change, so the path stays at the prior close.
    if abs(both - 0.0) > 1e-12:
        raise SystemExit(f"top-2 drop {both} != 0")
    sliced = slice_book(book, ["2026-09-15"], 21_000.0)
    row = window_row(sliced)
    if abs(row["start_equity"] - 21_000.0) > 1e-9:
        raise SystemExit("slice restarted")


def test_tune_refuses_the_reject_window() -> None:
    try:
        assert_tune_only(["2026-09-14"])
    except SystemExit as exc:
        if "2026-09-14" not in str(exc):
            raise
        return
    raise SystemExit("tune accepted a forward session")


def test_forward_runner_trims_and_leaves_uncapped_books_alone() -> None:
    fees = load_fees()
    recipe = {
        "exit_when": {}, "forbid": {"alarm": True}, "hold": 1, "name": "fwd_ccap_example",
        "rank": "hot_score", "require": {}, "sell": "list", "side": "long", "top_n": 4,
        "trades_at_open": True, "universe": "union", "weather": False, "weight_cap": 0.25,
        "s_boost": "none",
    }
    state, first = step_day(
        recipe, initial_state(), "2026-09-28", ["2026-09-28"], ["AAA"],
        {}, {"AAA": 10.0}, {"AAA": 10.0}, fees, s=1.0,
    )
    shares = state["positions"][0]["shares"]
    _state, second = step_day(
        recipe, state, "2026-09-29", ["2026-09-28", "2026-09-29"], ["AAA"],
        {}, {"AAA": 10.0}, {"AAA": 10.0}, fees, s=1.0,
    )
    if second["buys"]:
        raise SystemExit("forward trim bought the held name")
    kept = _state["positions"][0]["shares"]
    if kept >= shares:
        raise SystemExit("forward runner did not trim")
    if not any(row.get("kind") == "trim" for row in second["sells"]):
        raise SystemExit("trim fee was not charged")
    plain = dict(recipe)
    plain.pop("weight_cap")
    again, _day = step_day(
        plain, state, "2026-09-29", ["2026-09-28", "2026-09-29"], ["AAA"],
        {}, {"AAA": 10.0}, {"AAA": 10.0}, fees, s=1.0,
    )
    if again["positions"][0]["shares"] != shares or again["cash_f"] != state["cash_f"]:
        raise SystemExit("uncapped forward book changed")


def main() -> None:
    test_uncapped_matches_v4()
    test_trim_sits_and_does_not_top_up()
    test_shares_respect_the_cap_after_the_fee()
    test_drop_matches_v4_and_starts_from_prior_equity()
    test_tune_refuses_the_reject_window()
    test_forward_runner_trims_and_leaves_uncapped_books_alone()
    print("ok")


if __name__ == "__main__":
    main()
