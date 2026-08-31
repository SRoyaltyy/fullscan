"""Decision-lattice unit tests (no network, no LLM).

Run: python3 -m src.test_decision_lattice
"""
from __future__ import annotations

import pandas as pd

from src.decision_lattice import (
    attach_domains,
    evaluate_market,
    finalize_decisions,
)


def _market(state: str = "yellow") -> dict:
    tone = "bad" if state in ("red", "hard_red") else (
        "good" if state == "green" else "neutral"
    )
    return {
        "state": state,
        "tone": tone,
        "rationale": f"test {state}",
        "allowed_lanes": (
            ["catalyst_exception"] if state == "hard_red"
            else ["standard", "group_leader", "catalyst"]
        ),
    }


def _context(state: str = "yellow") -> dict:
    return {
        "market": _market(state),
        "heat": {
            "premarket": True,
            "sectors": {
                "Technology": {
                    "d1": 1.0, "w1": 2.0, "breadth": 0.6, "rvol": 1.0,
                }
            },
            "industries": {
                "Software - Infrastructure": {
                    "d1": 1.0, "w1": 5.0, "vs_parent_w1": 3.0,
                    "breadth": 0.7, "rvol": 1.1,
                }
            },
            "themes_by_industry": {
                "Software - Infrastructure": [{
                    "theme": "Artificial Intelligence",
                    "label": "Software infra",
                    "w1": 5.0,
                    "vs_parent_w1": 3.0,
                }]
            },
        },
        "actions": {},
        "digest": {},
        "judge_tickers": {},
        "judge_sectors": {},
        "catalysts": {},
    }


def _frame(ticker: str = "TEST") -> pd.DataFrame:
    return pd.DataFrame([{
        "Ticker": ticker,
        "sector": "Technology",
        "industry": "Software - Infrastructure",
        "s_sector": -0.30,
        "s_general": -0.10,
        "s_join": 0.40,
        "s_ab": 0.70,
        "s_ab_intrinsic": 0.60,
        "s_peer": 0.20,
        "s_news": 0.0,
        "s_heat": 0.05,
        "relvol": 0.10,
        "change_pct": 1.0,
        "gap_pct": 0.8,
        "lb_alarm": False,
        "lb_fade": False,
        "lb_cond": "good",
        "lb_region": "good",
    }])


def test_extreme_general_is_market_gate_not_eight_percent() -> None:
    run = {
        "predicted_direction": "down",
        "total_score": -5.85,
        "confidence_score": 0.55,
        "components": {
            "B0_ASIA": 0.0,
            "B0_EUROPE": 0.0,
            "B1_CATALYSTS": -1.0,
            "B2_BONDS": -0.5,
            "B3_FEDPATH": -0.5,
            "B4_VIX": -0.5,
            "B5_SENTIMENT": 0.5,
            "B6_FUTURES": -0.5,
            "B7_OIL_DOLLAR": -1.0,
        },
    }
    result = evaluate_market(
        "2099-01-01", run,
        {"signals": {"risk": "off", "general_score": -5.85}},
    )
    assert result["state"] == "hard_red"
    assert result["good_points"] == 0.5
    assert result["bad_points"] == -7.0
    assert len(result["red_pillars"]) >= 3
    assert result["allowed_lanes"] == ["catalyst_exception"]


def test_parent_conflict_survives_and_child_is_independent() -> None:
    ctx = _context("yellow")
    out = attach_domains(_frame(), ctx).iloc[0]
    # Measured sector tape green vs essay red: do not average to a fake call.
    assert out["d_parent"] == "neutral"
    assert bool(out["parent_conflict"]) is True
    # Software is independently green both absolute and vs parent.
    assert out["d_child"] == "good"
    assert out["child_rel_tone"] == "good"


def test_hard_red_blocks_group_but_allows_confirmed_direct_event() -> None:
    ctx = _context("hard_red")
    ctx["digest"]["ACMR"] = {
        "tone": "good",
        "text": "ACM Research beats EPS and revenue, raises FY outlook",
        "materiality": "high",
        "direct": True,
        "event_risk": False,
    }
    frame = _frame("ACMR")
    attached = attach_domains(frame, ctx)
    decided = finalize_decisions(attached, "2099-01-01", ctx).iloc[0]
    assert decided["d_company"] == "good"
    assert bool(decided["company_price_confirmed"]) is True
    assert decided["decision_lane"] == "catalyst_exception"
    assert bool(decided["bull_eligible"]) is True
    assert "BUY CATALYST_EXCEPTION" in decided["bull_decision"]

    # Same group/setup without the company event cannot sneak through.
    ctx2 = _context("hard_red")
    blocked = finalize_decisions(
        attach_domains(_frame("PLTR"), ctx2), "2099-01-01", ctx2,
    ).iloc[0]
    assert bool(blocked["bull_eligible"]) is False
    assert "HARD_RED" in blocked["bull_decision"]


def test_failed_dossier_does_not_erase_digest_event() -> None:
    ctx = _context("hard_red")
    ctx["digest"]["ACMR"] = {
        "tone": "good",
        "text": "ACM Research beats Q2 and raises 2026 outlook",
        "materiality": "high",
        "direct": True,
        "event_risk": False,
    }
    ctx["catalysts"]["ACMR"] = {
        "usable": False,
        "tone": "missing",
        "error": "Step 1 parse failure",
        "role": "override_captain",
    }
    out = attach_domains(_frame("ACMR"), ctx).iloc[0]
    assert out["d_company"] == "good"
    assert "dossier failed" in out["company_trace"]
    assert "finviz_digest" in out["company_sources"]


def main() -> None:
    tests = [
        test_extreme_general_is_market_gate_not_eight_percent,
        test_parent_conflict_survives_and_child_is_independent,
        test_hard_red_blocks_group_but_allows_confirmed_direct_event,
        test_failed_dossier_does_not_erase_digest_event,
    ]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
