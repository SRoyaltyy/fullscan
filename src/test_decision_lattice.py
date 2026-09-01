"""Decision-lattice unit tests (no network, no LLM).

Run: python3 -m src.test_decision_lattice
"""
from __future__ import annotations

import pandas as pd

from src.decision_lattice import (
    _digest_tone,
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
            ["catalyst_exception", "probable"] if state == "hard_red"
            else ["standard", "group_leader", "catalyst", "probable"]
        ),
        "position_scale": 0.25 if state == "hard_red" else 1.0,
    }


def _context(state: str = "yellow") -> dict:
    return {
        "date": "2099-01-01",
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
        "news_time": "2099-01-01 08:00:00",
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
    assert result["allowed_lanes"] == ["catalyst_exception", "probable"]
    assert result["max_long_slots"] == 10


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

    # Same group/setup without a company event is a probable long
    # (child/theme outperform), not a catalyst exception.
    ctx2 = _context("hard_red")
    group = finalize_decisions(
        attach_domains(_frame("PLTR"), ctx2), "2099-01-01", ctx2,
    ).iloc[0]
    assert group["decision_lane"] == "probable"
    assert bool(group["bull_eligible"]) is True
    assert "BUY PROBABLE" in group["bull_decision"]

    # No company clock and a cold child cannot sneak through.
    ctx3 = _context("hard_red")
    ctx3["heat"]["industries"] = {
        "Specialty Retail": {
            "d1": -3.0, "w1": -5.0, "vs_parent_w1": -4.0,
            "breadth": 0.2, "rvol": 0.8,
        }
    }
    sink = _frame("SINK")
    sink["industry"] = "Specialty Retail"
    sink["sector"] = "Consumer Cyclical"
    sink["s_sector"] = -0.60
    sink["change_pct"] = -1.2
    sink["gap_pct"] = -0.8
    blocked = finalize_decisions(
        attach_domains(sink, ctx3), "2099-01-01", ctx3,
    ).iloc[0]
    assert bool(blocked["bull_eligible"]) is False
    assert blocked["decision_lane"] == "blocked"


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


def test_stale_digest_cannot_be_hard_red_exception() -> None:
    ctx = _context("hard_red")
    ctx["digest"]["OLD"] = {
        "tone": "good",
        "text": "Old Corp beats earnings and raises guidance",
        "materiality": "high",
        "direct": True,
        "event_risk": False,
    }
    frame = _frame("OLD")
    frame["news_time"] = "2098-12-20 08:00:00"
    frame["industry"] = "Specialty Retail"
    frame["sector"] = "Consumer Cyclical"
    decided = finalize_decisions(
        attach_domains(frame, ctx), "2099-01-01", ctx,
    ).iloc[0]
    assert bool(decided["company_fresh"]) is False
    assert decided["d_company"] == "neutral"
    assert bool(decided["bull_eligible"]) is False


def test_probable_long_without_price_confirm() -> None:
    """Same-day company news still clocks a long when the tape does not confirm."""
    ctx = _context("hard_red")
    ctx["digest"]["CRM"] = {
        "tone": "good",
        "text": "Salesforce beats Q2 guidance and raises FY27 outlook",
        "materiality": "high",
        "direct": True,
        "event_risk": False,
    }
    frame = _frame("CRM")
    frame["change_pct"] = -0.8
    frame["gap_pct"] = -0.4
    frame["s_peer"] = -0.10
    decided = finalize_decisions(
        attach_domains(frame, ctx), "2099-01-01", ctx,
    ).iloc[0]
    assert decided["d_company"] == "good"
    assert bool(decided["company_price_confirmed"]) is False
    assert decided["decision_lane"] == "probable"
    assert bool(decided["bull_eligible"]) is True
    assert "company news" in decided["bull_decision"]


def test_alarm_still_blocks_probable() -> None:
    ctx = _context("hard_red")
    ctx["digest"]["FADE"] = {
        "tone": "good",
        "text": "Fade Co beats EPS and raises outlook",
        "materiality": "high",
        "direct": True,
        "event_risk": False,
    }
    frame = _frame("FADE")
    frame["lb_alarm"] = True
    frame["lb_cond"] = "bad"
    decided = finalize_decisions(
        attach_domains(frame, ctx), "2099-01-01", ctx,
    ).iloc[0]
    assert bool(decided["bull_eligible"]) is False
    assert "alarm" in decided["decision_blockers"]


def test_insider_sale_is_not_bullish_record_language() -> None:
    text = (
        "Cardinal Health CEO sold $29 million of company stock following "
        "the post-earnings share surge to record levels"
    )
    assert _digest_tone(text) == "bad"


def test_judge_company_names_survive_prose_parser() -> None:
    from src.judge_apply import parse_judge_md

    parsed = parse_judge_md(
        "SECTOR Technology: [bullish] Salesforce beat supports software\n"
        "SECTOR Healthcare: [bullish] Amgen Repatha Phase 3 succeeded\n"
    )
    assert parsed["tickers"]["CRM"] > 0
    assert parsed["tickers"]["AMGN"] > 0


def main() -> None:
    tests = [
        test_extreme_general_is_market_gate_not_eight_percent,
        test_parent_conflict_survives_and_child_is_independent,
        test_hard_red_blocks_group_but_allows_confirmed_direct_event,
        test_failed_dossier_does_not_erase_digest_event,
        test_stale_digest_cannot_be_hard_red_exception,
        test_probable_long_without_price_confirm,
        test_alarm_still_blocks_probable,
        test_insider_sale_is_not_bullish_record_language,
        test_judge_company_names_survive_prose_parser,
    ]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
