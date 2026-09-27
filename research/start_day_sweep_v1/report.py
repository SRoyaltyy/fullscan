"""Plain-English report. This module does not walk a tape and does not pick a recipe."""
from __future__ import annotations

from research.start_day_sweep_v1.protocol import (
    END_SESSION,
    FIRST_START,
    LAST_START,
    LUCK_N,
    NEW_TRIES,
    P2,
    P2_OTHER_ANCHOR,
    P2_V4_ANCHOR,
    positive_phrase,
)


def _pct(value: float) -> str:
    return f"{100.0 * float(value):.2f}%"


def _diff(value: float) -> str:
    number = float(value)
    if number == 0.0:
        return "0"
    return f"{number:.3e}"


def _family(name: str) -> str:
    if name == "v4":
        return "v4"
    if name == "g3":
        return "Group 3"
    if name == "cap_v3":
        return "cap v3"
    return name


def _trades(p2: dict) -> str:
    return ", ".join(str(row["trades"]) for row in p2["sessions"])


def render(payload: dict) -> str:
    y = int(payload["n_starts"])
    lines = [
        "# start_day_sweep_v1",
        "",
        "Report only. No recipe is picked, frozen, or added to any forward hook.",
        "",
        f"Luck N is {LUCK_N:,}, unchanged. New tries: {NEW_TRIES}, because nothing is selected.",
        "",
        (
            f"Each book starts with $10,000 at the open of one pinned session and is carried, "
            f"day by day, through the {END_SESSION} close. The start days are the {y} pinned "
            f"sessions from {FIRST_START} through {LAST_START}. A start is positive when ending "
            f"equity is above the $10,000 it started with. The 15bp column is that same keep-held "
            f"book with a flat 15bp fee instead of the Futubull schedule. The median is the average "
            f"of the 15th and 16th ending returns after they are sorted. The worst is the lowest ending return."
        ),
        "",
        (
            "The walker, the pinned inputs, and the keep-held Futubull fee path are the ones in "
            "factor_mine_recipe_search_v4. Group 3 rows use that same walker on the Group 3 recipe "
            "(8 names, weather off), the path concentration_screen_v1 already used. The two cap rows "
            "use the concentration_cap_v3 book, which is the concentration_cap_v1 keep-held walker "
            "with a 20% weight cap and leftover cash left sitting."
        ),
        "",
        (
            f"P2 is {P2[0]} through {P2[-1]}, the same window as concentration_screen_v1. "
            f"For a v4 row the P2 days are taken from the book that started on {P2_V4_ANCHOR} and was "
            f"carried forward. For a Group 3 or cap row they are taken from the book that started on "
            f"{P2_OTHER_ANCHOR}. An up day finished above the prior close. A down day finished below it. "
            f"A flat or sat-out day finished even with it, including a day that made no trade. "
            f"Trades each P2 session are the buy, sell, and trim orders at that open, in this session order: "
            + ", ".join(P2)
            + "."
        ),
        "",
        (
            "The 2026-08-17, 2026-08-24, and 2026-08-31 starts reproduce the concentration_screen_v1 "
            f"P1 and P2 numbers for the v4 rows. The largest absolute difference on those returns, "
            f"including the 15bp returns, is {_diff(payload['sanity']['screen_monday_max_abs_diff'])}. "
            "The Group 3 books that start on 2026-08-13 reproduce that screen's P1 and P2 numbers. "
            "The two cap books reproduce the concentration_cap_v3 tune window and its P2 window. "
            f"The largest absolute difference on every reproduction check in this file is "
            f"{_diff(payload['sanity']['max_abs_diff'])}."
        ),
        "",
        "| Recipe | Family | Positive start days | Positive start days at 15bp | Median ending return | Worst ending return | P2 up days | P2 down days | P2 flat or sat-out days | Trades each P2 session |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in payload["recipes"]:
        if not row.get("computed"):
            lines.append(
                f"| `{row['id']}` | {_family(row['family'])} | not computed | not computed |  |  |  |  |  |  |"
            )
            continue
        p2 = row["p2"]
        lines.append(
            "| `{id}` | {family} | {pos} | {pos15} | {med} | {worst} | {up} | {down} | {flat} | {trades} |".format(
                id=row["id"],
                family=_family(row["family"]),
                pos=positive_phrase(row["positive"], row["n_starts"]),
                pos15=positive_phrase(row["positive_15"], row["n_starts"]),
                med=_pct(row["median_ret"]),
                worst=_pct(row["worst_ret"]),
                up=p2["up"],
                down=p2["down"],
                flat=p2["flat_sat"],
                trades=_trades(p2),
            )
        )
    lines.append("")
    return "\n".join(lines)
