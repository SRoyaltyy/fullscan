"""Walker and gate locks. No cleaned tape and no 2026-09-14 session."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1 import test_engine as v1_tests  # noqa: E402
from research.concentration_cap_v1.engine import walk as v1_walk  # noqa: E402
from research.concentration_cap_v2.forward import is_rejected  # noqa: E402
from research.concentration_cap_v2.protocol import SHARE_MAX, candidates  # noqa: E402
from research.concentration_cap_v2.tune import assert_tune_only, is_passer  # noqa: E402


def _starts(joints):
    return [
        {"joint": joint, "n": 30 if joint is not None else 10, "start": start}
        for joint, start in zip(joints, ("2026-08-17", "2026-08-24", "2026-08-31"))
    ]


def _tune(**extra):
    row = {
        "start_equity": 10_000.0,
        "end_equity": 11_000.0,
        "profit_share": 0.19,
        "ex_top3": 0.01,
    }
    row.update(extra)
    return row


def test_v2_uses_the_v1_walker() -> None:
    if walk_is_v1() is not True:
        raise SystemExit("v2 forked the walker")


def walk_is_v1() -> bool:
    from research.concentration_cap_v2 import tune as tune_mod
    return tune_mod.walk is v1_walk


def test_v2_tune_refuses_the_reject_window() -> None:
    try:
        assert_tune_only(["2026-09-14"])
    except SystemExit as exc:
        if "2026-09-14" not in str(exc):
            raise
        return
    raise SystemExit("tune accepted a forward session")


def test_passer_needs_share_profit_and_top3() -> None:
    good = _starts((0.6, 0.55, 0.5))
    if not is_passer(_tune(), good):
        raise SystemExit("clean passer rejected")
    if is_passer(_tune(profit_share=SHARE_MAX), good):
        raise SystemExit("20% share passed")
    if is_passer(_tune(end_equity=10_000.0, profit_share=None), good):
        raise SystemExit("flat profit passed")
    if is_passer(_tune(end_equity=9_000.0, profit_share=-0.1), good):
        raise SystemExit("negative profit passed")
    if is_passer(_tune(ex_top3=0.0), good):
        raise SystemExit("flat ex-top3 passed")
    if is_passer(_tune(ex_top3=-0.01), good):
        raise SystemExit("negative ex-top3 passed")
    if is_passer(_tune(), _starts((0.6, None, 0.5))):
        raise SystemExit("missing Monday joint passed")


def test_reject_gates() -> None:
    quiet = {
        "n": 10,
        "joint": None,
        "start_equity": 10_000.0,
        "end_equity": 10_100.0,
        "profit_share": 0.1,
        "ex_top3": 0.01,
    }
    if is_rejected(False, quiet):
        raise SystemExit("unfrozen row rejected")
    if is_rejected(True, quiet):
        raise SystemExit("quiet passer rejected")
    concentrated = dict(quiet)
    concentrated["profit_share"] = 0.20
    if not is_rejected(True, concentrated):
        raise SystemExit("20% P2 share kept")
    negative_book = dict(concentrated)
    negative_book["end_equity"] = 9_000.0
    negative_book["profit_share"] = 0.5
    if is_rejected(True, negative_book):
        raise SystemExit("share gate fired on a loss")
    top3 = dict(quiet)
    top3["ex_top3"] = -0.001
    if not is_rejected(True, top3):
        raise SystemExit("negative ex-top3 kept")
    flat_top3 = dict(quiet)
    flat_top3["ex_top3"] = 0.0
    if is_rejected(True, flat_top3):
        raise SystemExit("zero ex-top3 rejected")
    weak = dict(quiet)
    weak["n"] = 30
    weak["joint"] = 0.49
    if not is_rejected(True, weak):
        raise SystemExit("weak joint kept")
    short = dict(weak)
    short["n"] = 29
    short["joint"] = 0.49
    if is_rejected(True, short):
        raise SystemExit("joint gate fired under 30 trades")


def test_width_ten_is_a_candidate() -> None:
    rows = [row for row in candidates() if row["id"] == "union_ret_5_h3__w0__n10__c20"]
    if len(rows) != 1 or rows[0]["top_n"] != 10 or rows[0]["weight_cap"] != 0.20:
        raise SystemExit("width 10 missing")
    if rows[0]["rank"] != "ret_5" or rows[0]["weather"] is not False:
        raise SystemExit("ret_5 w0 drifted")


def main() -> None:
    v1_tests.main()
    test_v2_uses_the_v1_walker()
    test_v2_tune_refuses_the_reject_window()
    test_passer_needs_share_profit_and_top3()
    test_reject_gates()
    test_width_ten_is_a_candidate()
    print("ok v2")


if __name__ == "__main__":
    main()
