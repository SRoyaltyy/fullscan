"""The audit mark is exact only on the same row, and only as the string exact."""
from __future__ import annotations

from pathlib import Path

from research.pricefull.inclusion import decide, load_audit, read_marks

ROOT = Path(__file__).resolve().parents[2]
AUDIT = ROOT / "research" / "audit" / "INPUT_PROVENANCE_336.md"


def test_exact_cell_marks_only_that_id():
    text = """
| Input | REBUILD_MATCH |
| --- | --- |
| ret_1 | exact |
| ret_5 | mismatch |
| hot score | exact |
"""
    marks = read_marks(text)
    assert marks["exact_ids"] == ["ret_1"]
    decision = decide(marks)
    assert "ret_1" in decision["exact_ids"]
    assert "ret_5" not in decision["exact_ids"]
    assert "hot_score" not in decision["exact_ids"]


def test_nearby_row_does_not_mark_the_next_id():
    text = """
| Input | REBUILD_MATCH | note |
| --- | --- | --- |
| ret_5 | exact | |
| rvol | mismatch (1/1, first 2026-09-10) | |
"""
    marks = read_marks(text)
    assert marks["exact_ids"] == ["ret_5"]
    decision = decide(marks)
    assert decision["hot_score_usable"] is False


def test_hot_score_needs_every_part():
    text = """
| Input | REBUILD_MATCH |
| --- | --- |
| hot_score | exact |
| ret_5 | exact |
| ret_10 | exact |
| rvol | exact |
| break_10 | mismatch |
| last_green | exact |
"""
    decision = decide(read_marks(text))
    assert decision["hot_score_usable"] is False
    assert decision["variants"]["pricefull_hot4"]["buys"] is False


def test_bullet_exact_and_a14_stays_out():
    text = """
- peer_rs REBUILD_MATCH=exact
- A14_profitable_oversold_setup REBUILD_MATCH=exact
- A01_rsi_value REBUILD_MATCH=mismatch
"""
    decision = decide(read_marks(text))
    assert decision["peer_survives"] is True
    assert decision["a14_audit_exact"] is True
    assert decision["usable_ab"] == []
    assert decision["variants"]["pricefull_w1d"]["buys"] is True
    assert "A14_profitable_oversold_setup" not in decision["usable_ab"]


def test_case_and_substring_do_not_count():
    text = """
| Input | REBUILD_MATCH |
| --- | --- |
| A07_rvol | Exact |
| rvol | exact |
"""
    marks = read_marks(text)
    assert "A07_rvol" not in marks["exact_ids"]
    assert marks["exact_ids"] == ["rvol"]


def test_missing_file_buys_nobody():
    decision = load_audit(Path("/tmp/pricefull-no-such-audit.md"))
    assert decision["file_present"] is False
    assert decision["exact_ids"] == []
    assert decision["variants"]["pricefull_w1d"]["buys"] is False
    assert decision["variants"]["pricefull_hot4"]["buys"] is False


def test_live_audit_drops_every_section4_id():
    decision = load_audit(AUDIT)
    assert decision["file_present"] is True
    assert decision["exact_ids"] == []
    assert decision["variants"]["pricefull_w1d"]["buys"] is False
    assert decision["variants"]["pricefull_hot4"]["buys"] is False
    found = {row["id"]: row for row in decision["ids"]}
    assert found["A06_volume_red_green_2day"]["marks"]
    assert found["A06_volume_red_green_2day"]["marks"][0].startswith("mismatch")
    assert found["A14_profitable_oversold_setup"]["exact"] is False
    assert found["hot_score"]["marks"] == []
    assert found["yday_gainer"]["marks"] == []
