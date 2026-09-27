"""Lock the report. Does not walk the tape."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v3.protocol import LUCK_N as CAP_LUCK  # noqa: E402
from research.start_day_sweep_v1.append_check import judge, parse_name_status  # noqa: E402
from research.start_day_sweep_v1.protocol import (  # noqa: E402
    CAPITAL,
    END_SESSION,
    LUCK_N,
    NEW_TRIES,
    P2,
    START_DAYS,
    assert_recipe_grid,
    day_kind,
    ending_return,
    order_trades,
    positive_phrase,
    summarize_p2,
    summarize_starts,
)
from research.start_day_sweep_v1.report import render  # noqa: E402

HERE = Path(__file__).resolve().parent


def test_grid() -> None:
    assert_recipe_grid()
    if LUCK_N != 22_009 or LUCK_N != CAP_LUCK or NEW_TRIES != 0:
        raise SystemExit("luck")
    if len(START_DAYS) != 30 or END_SESSION in START_DAYS:
        raise SystemExit("starts")
    if len(P2) != 10:
        raise SystemExit("p2")


def test_day_kind_and_orders() -> None:
    if day_kind(0.0) != "flat_sat" or day_kind(1e-13) != "flat_sat" or day_kind(-1e-13) != "flat_sat":
        raise SystemExit("flat band")
    if day_kind(1e-12) != "flat_sat" or day_kind(1e-12 + 1e-15) != "up":
        raise SystemExit("up edge")
    if day_kind(-1e-9) != "down":
        raise SystemExit("down")
    day = {"bought": ["A", "B"], "sold": ["C"], "trimmed": ["D"]}
    if order_trades(day) != 4:
        raise SystemExit("orders")
    if order_trades({"bought": [], "sold": []}) != 0:
        raise SystemExit("empty orders")
    book = {
        "daily": [
            {"bought": ["A"], "equity": 10100.0, "ret": 0.01, "ret_15": 0.01, "session": P2[0], "sold": [], "trimmed": []},
            {"bought": [], "equity": 10100.0, "ret": 0.0, "ret_15": 0.0, "session": P2[1], "sold": [], "trimmed": []},
        ],
    }
    # The helper requires the full P2 list. Build a stand-in through summarize_starts only here.
    starts = [
        {"end_equity": 11000.0, "end_equity_15": 9000.0, "ret": ending_return(11000.0), "ret_15": ending_return(9000.0)},
        {"end_equity": 10000.0, "end_equity_15": 12000.0, "ret": ending_return(10000.0), "ret_15": ending_return(12000.0)},
        {"end_equity": 8000.0, "end_equity_15": 8000.0, "ret": ending_return(8000.0), "ret_15": ending_return(8000.0)},
    ]
    summary = summarize_starts(starts)
    if summary["positive"] != 1 or summary["positive_15"] != 1 or summary["n_starts"] != 3:
        raise SystemExit("positive count")
    if summary["worst_ret"] != ending_return(8000.0):
        raise SystemExit("worst")
    if summary["median_ret"] != statistics.median([row["ret"] for row in starts]):
        raise SystemExit("median")
    if positive_phrase(1, 30) != "positive from 1 of 30 start days":
        raise SystemExit("phrase")
    if book["daily"][0]["session"] != "2026-09-14":
        raise SystemExit("p2 fixture")


def test_p2_partition() -> None:
    daily = []
    for index, session in enumerate(P2):
        if index == 0:
            ret = 0.01
            bought = ["A"]
        elif index == 1:
            ret = -0.02
            bought = []
        else:
            ret = 0.0
            bought = []
        daily.append({
            "bought": bought,
            "equity": CAPITAL,
            "ret": ret,
            "ret_15": ret,
            "session": session,
            "sold": ["B"] if index == 1 else [],
            "trimmed": [],
        })
    closed = [{"exit": P2[1], "ticker": "B", "win": False}]
    p2 = summarize_p2(daily, closed)
    if p2["up"] + p2["down"] + p2["flat_sat"] != len(P2):
        raise SystemExit("p2 partition")
    if p2["up"] != 1 or p2["down"] != 1 or p2["flat_sat"] != 8:
        raise SystemExit("p2 counts")
    if p2["sat_out"] != 8:
        raise SystemExit("sat out")
    if p2["sessions"][0]["trades"] != 1 or p2["sessions"][1]["trades"] != 1:
        raise SystemExit("p2 trades")
    if p2["sessions"][1]["closed"] != 1:
        raise SystemExit("closed exit")


def test_append_only_judge() -> None:
    good = parse_name_status(
        "A\tresearch/start_day_sweep_v1/REPORT.md\n"
        "A\tresearch/start_day_sweep_v1/RESULTS.json\n"
        "A\t.github/workflows/start_day_sweep_v1.yml\n"
    )
    judge(good)
    for sample in (
        "M\tresearch/start_day_sweep_v1/REPORT.md\n",
        "A\tsrc/paper_trade.py\n",
        "D\tresearch/factor_mine_recipe_search_v4/engine.py\n",
        "A\tresearch/forward_shadow_v1/recipes.json\n",
        "R100\told.py\tresearch/start_day_sweep_v1/new.py\n",
    ):
        try:
            if sample.startswith("R"):
                parse_name_status(sample)
            else:
                judge(parse_name_status(sample))
        except SystemExit:
            continue
        raise SystemExit(f"judge allowed {sample!r}")


def test_results_match_the_report() -> None:
    payload = json.loads((HERE / "RESULTS.json").read_text(encoding="utf-8"))
    text = (HERE / "REPORT.md").read_text(encoding="utf-8")
    if text != render(payload):
        raise SystemExit("report drifted from RESULTS.json")
    if payload["luck_n"] != LUCK_N or payload["new_tries"] != 0:
        raise SystemExit("results luck")
    if payload["picked"] is not None or payload["frozen"] or payload["forward_hook"]:
        raise SystemExit("selection")
    if payload["fill"] != "keep_held" or payload["fee_path"] != "keep_held_futubull":
        raise SystemExit("fee path")
    if payload["start_days"] != list(START_DAYS) or payload["p2_sessions"] != list(P2):
        raise SystemExit("calendar")
    if payload["end_session"] != END_SESSION or payload["n_starts"] != 30:
        raise SystemExit("window")
    ids = [row["id"] for row in payload["recipes"]]
    from research.start_day_sweep_v1.protocol import CAP_IDS, G3_IDS, V4_IDS
    if ids != list(V4_IDS) + list(G3_IDS) + list(CAP_IDS):
        raise SystemExit("recipe ids")
    monday = payload["sanity"]["screen_monday_max_abs_diff"]
    overall = payload["sanity"]["max_abs_diff"]
    if monday is None or overall is None or float(monday) > 1e-8 or float(overall) > 1e-8:
        raise SystemExit(f"sanity diff {monday} {overall}")
    kinds = {"return", "count", "equity"}
    for check in payload["sanity"]["checks"]:
        if check["kind"] not in kinds:
            raise SystemExit("check kind")
        if check["abs_diff"] is None or float(check["abs_diff"]) > 1e-8:
            raise SystemExit(f"check failed {check['recipe']} {check['field']}")
    for row in payload["recipes"]:
        if not row["computed"]:
            raise SystemExit(f"not computed {row['id']}")
        if row["n_starts"] != 30:
            raise SystemExit("n")
        again = summarize_starts(row["starts"])
        for key in ("positive", "positive_15", "median_ret", "worst_ret", "n_starts"):
            if again[key] != row[key]:
                raise SystemExit(f"summary {row['id']} {key}")
        if [item["session"] for item in row["starts"]] != list(START_DAYS):
            raise SystemExit("start order")
        for item in row["starts"]:
            if abs(item["ret"] - ending_return(item["end_equity"])) > 1e-12:
                raise SystemExit("ending return")
            if abs(item["ret_15"] - ending_return(item["end_equity_15"])) > 1e-12:
                raise SystemExit("ending return 15")
            if (item["end_equity"] > CAPITAL) != (item["ret"] > 0):
                raise SystemExit("positive edge")
        p2 = row["p2"]
        if p2["up"] + p2["down"] + p2["flat_sat"] != len(P2):
            raise SystemExit(f"p2 partition {row['id']}")
        if [item["session"] for item in p2["sessions"]] != list(P2):
            raise SystemExit("p2 order")
        for item in p2["sessions"]:
            if day_kind(item["ret"]) != item["kind"]:
                raise SystemExit("kind")
            if item["trades"] != item["bought"] + item["sold"] + item["trimmed"]:
                raise SystemExit("trade parts")
        phrase = positive_phrase(row["positive"], row["n_starts"])
        phrase_15 = positive_phrase(row["positive_15"], row["n_starts"])
        if phrase not in text or phrase_15 not in text:
            raise SystemExit(f"phrase missing {row['id']}")
        if f"`{row['id']}`" not in text:
            raise SystemExit(f"id missing {row['id']}")
    for phrase in (
        "No recipe is picked, frozen, or added to any forward hook.",
        "22,009",
        "New tries: 0, because nothing is selected.",
        "largest absolute difference",
        "concentration_screen_v1",
        "keep-held",
    ):
        if phrase not in text:
            raise SystemExit(f"wording {phrase}")
    if (HERE / "FREEZE.json").exists() or (HERE / "freeze").exists():
        raise SystemExit("freeze")


def main() -> None:
    test_grid()
    test_day_kind_and_orders()
    test_p2_partition()
    test_append_only_judge()
    test_results_match_the_report()
    print("start_day_sweep_v1 ok")


if __name__ == "__main__":
    main()
