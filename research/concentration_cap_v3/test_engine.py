"""Gate locks. No cleaned tape and no 2026-09-14 session."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.engine import walk as v1_walk  # noqa: E402
from research.concentration_cap_v3.forward import is_rejected  # noqa: E402
from research.concentration_cap_v3.metrics import gross_share  # noqa: E402
from research.concentration_cap_v3.protocol import dependence  # noqa: E402
from research.concentration_cap_v3.tune import assert_tune_only, is_passer  # noqa: E402


def _starts(joints):
    return [{"joint": joint, "n": 30 if joint is not None else 10} for joint in joints]


def _tune(**extra):
    row = {"compound": 0.20, "ex_top1": 0.17, "ex_top3": 0.04}
    row.update(extra)
    return row


def test_walker_is_v1() -> None:
    from research.concentration_cap_v3 import tune as tune_mod
    if tune_mod.walk is not v1_walk:
        raise SystemExit("v3 forked the walker")


def test_tune_refuses_the_reject_window() -> None:
    try:
        assert_tune_only(["2026-09-14"])
    except SystemExit as exc:
        if "2026-09-14" not in str(exc):
            raise
        return
    raise SystemExit("tune accepted a forward session")


def test_passer_uses_dependence_not_dollar_share() -> None:
    good = _starts((0.6, 0.55, 0.5))
    if not is_passer(_tune(), good):
        raise SystemExit("clean passer rejected")
    # 33% to 13% is about 60% dependence. Dollar share is irrelevant.
    if is_passer(_tune(compound=0.33, ex_top1=0.13, ex_top3=0.05), good):
        raise SystemExit("cyrus 60% dependence passed")
    if is_passer(_tune(compound=0.0, ex_top1=0.0), good):
        raise SystemExit("flat R passed")
    if is_passer(_tune(compound=-0.04, ex_top1=-0.01), good):
        raise SystemExit("negative R passed")
    if is_passer(_tune(ex_top3=0.0), good):
        raise SystemExit("flat R_-3 passed")
    if is_passer(_tune(ex_top3=-0.01), good):
        raise SystemExit("negative R_-3 passed")
    if is_passer(_tune(), _starts((0.6, None, 0.5))):
        raise SystemExit("missing Monday joint passed")
    # A high dollar concentration can still clear the net line.
    wide = _tune(compound=0.10, ex_top1=0.09, ex_top3=0.02)
    if dependence(wide["compound"], wide["ex_top1"]) >= 0.20:
        raise SystemExit("fixture")
    if not is_passer(wide, good):
        raise SystemExit("low dependence rejected")


def test_reject_uses_the_same_line() -> None:
    quiet = {"n": 10, "joint": None, "compound": 0.12, "ex_top1": 0.11, "ex_top3": 0.02}
    if is_rejected(False, quiet) or is_rejected(True, quiet):
        raise SystemExit("quiet book rejected")
    concentrated = dict(quiet)
    concentrated["compound"] = 0.33
    concentrated["ex_top1"] = 0.13
    if not is_rejected(True, concentrated):
        raise SystemExit("60% dependence kept")
    flat = dict(quiet)
    flat["compound"] = 0.0
    flat["ex_top1"] = 0.0
    if not is_rejected(True, flat):
        raise SystemExit("flat P2 kept")
    top3 = dict(quiet)
    top3["ex_top3"] = 0.0
    if not is_rejected(True, top3):
        raise SystemExit("flat R_-3 kept")
    weak = dict(quiet)
    weak["n"] = 30
    weak["joint"] = 0.49
    if not is_rejected(True, weak):
        raise SystemExit("weak joint kept")
    short = dict(weak)
    short["n"] = 29
    if is_rejected(True, short):
        raise SystemExit("joint gate fired under 30 trades")


def test_gross_share_is_winners_only() -> None:
    book = {
        "daily": [{"session": "2026-08-13"}],
        "pnl_by_day": {"2026-08-13": {"AAA": 100.0, "BBB": 50.0, "CCC": -30.0}},
        "closed": [],
    }
    got = gross_share(book)
    if got is None or abs(got - (100.0 / 150.0)) > 1e-12:
        raise SystemExit(f"gross share {got}")
    losers = {
        "daily": [{"session": "2026-08-13"}],
        "pnl_by_day": {"2026-08-13": {"AAA": -5.0, "BBB": -1.0}},
        "closed": [],
    }
    if gross_share(losers) is not None:
        raise SystemExit("losers have a gross share")


def main() -> None:
    test_walker_is_v1()
    test_tune_refuses_the_reject_window()
    test_passer_uses_dependence_not_dollar_share()
    test_reject_uses_the_same_line()
    test_gross_share_is_winners_only()
    print("ok v3")


if __name__ == "__main__":
    main()
