"""Gainer reverse-run + improve bars, no LLM."""
from __future__ import annotations

from src import gainer_reverse_audit as gra


def test_classify_captured_vs_never_targeted() -> None:
    hits = {
        "stock_book_1d": {"SDGR"},
        "keep_tickets": set(),
        "news": set(),
        "judge": set(),
        "catal_targets": {"NE", "RIG"},
        "catal_usable": set(),
        "any": {"SDGR"},
    }
    catal = {"targets": ["NE", "RIG"]}
    cap = gra.classify_gainer("SDGR", 1, hits, catal)
    assert cap["reason"] == "captured"
    assert cap["stock_book_1d"] is True
    miss = gra.classify_gainer("GNRC", 2, hits, catal)
    assert miss["reason"] == "never_targeted"
    empty = gra.classify_gainer("NE", 3, hits, catal)
    assert empty["reason"] == "targeted_empty"


def test_score_improve_fails_empty_dossiers_and_zero_recall() -> None:
    days = [
        {
            "recall": 0.0,
            "top5_hits": 0,
            "catalyst": {
                "n_ok": 0,
                "targets": ["NE", "RIG", "SLB", "BKR", "KGS", "WHD", "CNR", "BTU"],
            },
            "flatten_sit": True,
            "gainers": [{"ticker": "SDGR"}],
        },
        {
            "recall": 0.0,
            "top5_hits": 0,
            "catalyst": {
                "n_ok": 0,
                "targets": ["NE", "RIG", "SLB", "BKR", "KGS", "WHD", "CNR", "BTU"],
            },
            "flatten_sit": True,
            "gainers": [{"ticker": "GNRC"}],
        },
        {
            "recall": 0.0,
            "top5_hits": 0,
            "catalyst": {
                "n_ok": 0,
                "targets": ["NE", "RIG", "SLB", "BKR", "KGS", "WHD", "CNR", "BTU"],
            },
            "flatten_sit": True,
            "gainers": [{"ticker": "MSTR"}],
        },
    ]
    grades = gra.score_improve(days)
    assert grades["gainer_recall"]["pass"] is False
    assert grades["catalyst_targets_move"]["pass"] is False
    assert grades["catalyst_targets_move"]["stuck_captains"] is True
    assert grades["live_up_not_empty"]["pass"] is False


def test_render_lists_locked_bars() -> None:
    payload = {
        "generated_at": "t",
        "top_n": 15,
        "min_change": 5.0,
        "days": [],
        "grades": {},
    }
    md = gra.render_markdown(payload)
    assert "gainer_recall" in md
    assert "live_up_not_empty" in md
    assert "Book%" in md
    assert "flatten_robust" in md


def test_audit_0917_sdgr_not_on_leakfree_hot4() -> None:
    """Same-day rip cannot be on that morning's leak-free hot4.

    After the lookback-panel restamp it can sit on yday_gainer the *next*
    session, and on combo_ej if the event rifle already had a seat.
    """
    day = gra.audit_date("2026-09-17")
    assert day["coverage"]["status"] in {"full", "partial"}
    ticks = [r["ticker"] for r in day["gainers"]]
    assert "SDGR" in ticks
    assert day["catalyst"]["n_ok"] == 0
    assert "NE" in day["catalyst"]["targets"]
    assert "SDGR" not in (day.get("keep") or {}).get("union_hot_n4_h1", [])


if __name__ == "__main__":
    test_classify_captured_vs_never_targeted()
    test_score_improve_fails_empty_dossiers_and_zero_recall()
    test_render_lists_locked_bars()
    test_audit_0917_sdgr_not_on_leakfree_hot4()
    print("4 gainer-reverse-audit tests passed")
