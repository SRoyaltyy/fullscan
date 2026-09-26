"""Walk the concentration screen and write the report. This file does not pick a recipe."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_screen_v1.metrics import lists_from, period_stats, slice_book  # noqa: E402
from research.concentration_screen_v1.protocol import (  # noqa: E402
    LUCK_N,
    LUCK_PRIOR,
    NAMED,
    N_ROWS,
    P2,
    assert_grid,
    screen_specs,
)
from research.concentration_screen_v1.short_book import walk_short  # noqa: E402
from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import walk  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import INPUTS, SESSIONS  # noqa: E402
from src.paper_trade import load_fees  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "RESULTS.json"
REPORT = HERE / "REPORT.md"


def _pct(value) -> str:
    if value is None:
        return ""
    return f"{100.0 * float(value):.2f}%"


def _num(value) -> str:
    if value is None:
        return ""
    return str(value)


def _flag(stat: dict) -> str:
    return "yes" if stat["lt30"] else ""


def _row_line(row: dict) -> str:
    p1 = row["p1"]
    p2 = row["p2"]
    return (
        "| `{id}` | {f} | {s} | {p1r} | {p1r15} | {p1e1} | {p1e3} | {p1e5} | {p1sh} | {p1tk} | {p1n} | {p1w} | {p1m} "
        "| {p2r} | {p2r15} | {p2e1} | {p2e3} | {p2e5} | {p2sh} | {p2tk} | {p2n} | {p2w} | {p2m} |"
    ).format(
        id=row["id"],
        f=row["family"],
        s=row["start"],
        p1r=_pct(p1["ret"]),
        p1r15=_pct(p1["ret_15"]),
        p1e1=_pct(p1["ex1"]),
        p1e3=_pct(p1["ex3"]),
        p1e5=_pct(p1["ex5"]),
        p1sh=_pct(p1["best_share"]),
        p1tk=p1["n_tickers"],
        p1n=p1["n_trades"],
        p1w=_pct(p1["win_rate"]),
        p1m=_flag(p1),
        p2r=_pct(p2["ret"]),
        p2r15=_pct(p2["ret_15"]),
        p2e1=_pct(p2["ex1"]),
        p2e3=_pct(p2["ex3"]),
        p2e5=_pct(p2["ex5"]),
        p2sh=_pct(p2["best_share"]),
        p2tk=p2["n_tickers"],
        p2n=p2["n_trades"],
        p2w=_pct(p2["win_rate"]),
        p2m=_flag(p2),
    )


def _header() -> str:
    return "\n".join([
        "| id | family | P1 start | P1 return | P1 15bp | P1 w/o top 1 | P1 w/o top 3 | P1 w/o top 5 | P1 best share | P1 names | P1 trades | P1 win | P1 <30 | P2 return | P2 15bp | P2 w/o top 1 | P2 w/o top 3 | P2 w/o top 5 | P2 best share | P2 names | P2 trades | P2 win | P2 <30 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ])


def _report(rows: list[dict], grouped: dict) -> str:
    named = next(row for row in rows if row["id"] == f"g3:{NAMED}")
    lines = [
        "# concentration_screen_v1",
        "",
        "Report only. This file does not pick a recipe and it does not freeze one.",
        "",
        "Cyrus's rule: not relying on one stock has to show up both before 2026-09-14 and after it. P1 is the sessions through 2026-09-11. P2 is 2026-09-14 through 2026-09-25. P2's without-stock returns start from the equity at the P1 close, using the fixed drop.",
        "",
        "The 110 Group 3 recipes walk every pinned board session from 2026-08-13 through 2026-09-11. v4's 150 rows are the 25 parts, weather on and off, and the three Monday starts. A v4 row's P1 starts on that Monday. Keep-held Futubull fees are the returns in the drop columns. Flat 15bp is the secondary total return beside them.",
        "",
        "Top stocks are ranked by their dollar contribution inside that period. A `<30` cell has fewer than 30 closed trades in that period. Those rows stay in the lists when the without-stock return is positive.",
        "",
        f"Luck N for a choice made from this screen is {LUCK_N}: the prior {LUCK_PRIOR}, plus these {N_ROWS} screen rows. Any recipe taken from list A or list B has already been seen in both periods, so this screen cannot prove it. It can only be proved going forward.",
        "",
        "The lists below are sorted for reading by the weaker period. That order is not a rank and not a selection.",
        "",
        "## List A — positive without the top stock in both periods",
        "",
        _header(),
    ]
    for row in grouped["A"]:
        lines.append(_row_line(row))
    if not grouped["A"]:
        lines.append("|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |")
    lines.extend([
        "",
        "## List B — positive without the top 3 in both periods",
        "",
        _header(),
    ])
    for row in grouped["B"]:
        lines.append(_row_line(row))
    if not grouped["B"]:
        lines.append("|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |")
    lines.extend([
        "",
        f"## `{NAMED}`",
        "",
        "Included on its own, whether or not it is on list A or list B.",
        "",
        _header(),
        _row_line(named),
        "",
        "## Full grid",
        "",
        "Family order, then id. Not a ranking.",
        "",
        _header(),
    ])
    for row in rows:
        lines.append(_row_line(row))
    lines.append("")
    return "\n".join(lines)


def _walk_one(spec: dict, days_by_session: dict, fees: dict, price) -> dict:
    sessions = list(spec["p1"]) + [day for day in P2 if day not in spec["p1"]]
    days = [days_by_session[session] for session in sessions]
    recipe = spec["recipe"]
    if recipe.get("side") == "short":
        book = walk_short(days, recipe, fees, price)
    else:
        book = walk(days, recipe, fees, price, "keep_held")
    p1_end = next(day["equity"] for day in book["daily"] if day["session"] == spec["p1"][-1])
    p1 = slice_book(book, spec["p1"], book["start_equity"])
    p2 = slice_book(book, P2, p1_end)
    if abs(p2["start_equity"] - p1_end) > 1e-9:
        raise SystemExit("P2 did not start at the P1 close")
    return {
        "family": spec["family"],
        "id": spec["id"],
        "p1": period_stats(p1),
        "p2": period_stats(p2),
        "side": recipe.get("side") or "long",
        "start": spec["start"],
    }


def main() -> None:
    assert_grid()
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    if list(payload["dates"]) != list(SESSIONS):
        raise SystemExit("pinned board sessions")
    store = CleanStore()

    def price(ticker: str, session: str, which: str):
        if which == "open":
            return store.session_open(ticker, session)
        if which == "close":
            return store.session_close(ticker, session)
        raise SystemExit(which)

    days_by_session = {
        session: {
            "session": session,
            "s": payload["dates"][session]["s"],
            "rows": payload["dates"][session]["rows"],
        }
        for session in SESSIONS
    }
    rows = []
    for spec in screen_specs():
        rows.append(_walk_one(spec, days_by_session, fees, price))
        print(spec["id"], flush=True)
    if len(rows) != N_ROWS:
        raise SystemExit("row count")
    grouped = lists_from(rows)
    body = {
        "lists": {
            "A": [row["id"] for row in grouped["A"]],
            "B": [row["id"] for row in grouped["B"]],
        },
        "luck_n": LUCK_N,
        "luck_prior": LUCK_PRIOR,
        "note": (
            "Report only. No recipe is picked or frozen. A recipe taken from this "
            "screen has been seen in both periods and can only prove itself going forward. "
            f"The choice counts toward luck N {LUCK_N}."
        ),
        "rows": rows,
    }
    RESULTS.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT.write_text(_report(rows, grouped), encoding="utf-8")
    print("screen wrote", len(rows), "A", len(grouped["A"]), "B", len(grouped["B"]), flush=True)


if __name__ == "__main__":
    main()
