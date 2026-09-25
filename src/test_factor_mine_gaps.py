"""The three audit gaps: old board, holdup ex-GLND, Webull input trace."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_old_board_versus_rebuild_totals() -> None:
    text = (ROOT / "03_scoreboard" / "FACTOR_MINE_OLD_VS_PIT.md").read_text(
        encoding="utf-8")
    assert "0eab52983" in text
    assert "Old cumulative 62.798% ($16,279.84)" in text
    assert "Rebuild cumulative 24.991% ($12,499.09)" in text
    assert "Old cumulative 80.971% ($18,097.07)" in text
    assert "Rebuild cumulative 52.196% ($15,219.62)" in text
    assert "| 2026-08-13 | IREN, TNDM, TPG, INO |" in text
    assert "| different |" in text
    assert "| same |" in text


def test_holdup_ex_glnd_timing_clean() -> None:
    text = (ROOT / "03_scoreboard" / "FACTOR_MINE_RETRO_PIT.md").read_text(
        encoding="utf-8")
    assert "Holdup without GLND on timing-clean days" in text
    assert "| `union_hot_n4_holdup` | futubull | without GLND | timing_clean | 17.986 | 15 |" in text
    assert "| `union_hot_n4_holdup` | flat_15bp | without GLND | timing_clean | 20.456 | 15 |" in text
    assert "| `union_hot_n4_h1` | futubull | without GLND | timing_clean | -1.339 | 15 |" in text


def test_webull_input_trace_has_one_row_per_day() -> None:
    text = (ROOT / "03_scoreboard" / "FACTOR_MINE_HOT4_PAPER.md").read_text(
        encoding="utf-8")
    assert "## Input trace" in text
    for day, commit in (
        ("2026-09-22", "18039b02cf"),
        ("2026-09-23", "409c73e31a"),
        ("2026-09-24", "36416dd8a9"),
    ):
        assert f"| {day} | `{commit}`" in text
    assert "SECZ is dropped: indicator bars 58/60" in text
    assert "c4199bbdd4" in text


if __name__ == "__main__":
    test_old_board_versus_rebuild_totals()
    test_holdup_ex_glnd_timing_clean()
    test_webull_input_trace_has_one_row_per_day()
    print("factor-mine gap tests passed")
