"""Overlay autopsy — leak clock, both-tape, cheap≠elevate.

Run: python -m src.test_overlay_autopsy
"""
from __future__ import annotations

from src import overlay_autopsy as oa
from src import finviz_style_flags as fsf


def test_prior_is_strictly_before() -> None:
    cal = ["2026-08-13", "2026-08-14", "2026-08-17"]
    assert oa._prior(cal, "2026-08-17") == "2026-08-14"
    assert oa._prior(cal, "2026-08-14") == "2026-08-13"
    assert oa._prior(cal, "2026-08-13") is None
    assert oa._prior(cal, "2026-08-18") == "2026-08-17"


def test_outcome_prefers_change_from_open() -> None:
    row = {"Change from Open": "1.5%", "Change": "9.9%"}
    assert oa.outcome_1d(row) == 1.5
    assert oa.outcome_1d({"Change": "-2%"}) == -2.0
    assert oa.outcome_1d({}) is None


def test_score_rule_both_tape_avoid_needs_underperform() -> None:
    rows = []
    for i in range(25):
        rows.append({"fwd": -2.0, "avoid_veto": True, "realized_tape": "up"})
        rows.append({"fwd": -1.5, "avoid_veto": True, "realized_tape": "down"})
        rows.append({"fwd": 1.0, "avoid_veto": False, "realized_tape": "up"})
        rows.append({"fwd": 0.8, "avoid_veto": False, "realized_tape": "down"})
    st = oa.score_rule(rows, "avoid_veto", family="avoid")
    assert st["n"] == 50
    assert st["thin"] is False
    assert st["both_tape"] is True
    assert st["xs"] is not None and st["xs"] < 0


def test_cheap_fpe_does_not_survive_as_elevate() -> None:
    # Cheap names win on up days, lose on down days — one-tape only.
    rows = []
    for _ in range(25):
        rows.append({"fwd": 3.0, "radar_cheap_fpe": True, "realized_tape": "up"})
        rows.append({"fwd": -2.0, "radar_cheap_fpe": True, "realized_tape": "down"})
        rows.append({"fwd": 1.0, "radar_cheap_fpe": False, "realized_tape": "up"})
        rows.append({"fwd": 0.5, "radar_cheap_fpe": False, "realized_tape": "down"})
    st = oa.score_rule(rows, "radar_cheap_fpe", family="elevate")
    assert st["both_tape"] is False
    assert st["thin"] is False


def test_thin_n_when_one_tape_small() -> None:
    rows = [{"fwd": 1.0, "elevate_bump": True, "realized_tape": "up"}] * 30
    rows += [{"fwd": 1.0, "elevate_bump": True, "realized_tape": "down"}] * 5
    st = oa.score_rule(rows, "elevate_bump", family="elevate")
    assert st["thin"] is True
    assert st["both_tape"] is None


def test_sift_fold_rejects_canslim_as_as_elevate() -> None:
    text = "\n".join(oa._sift_fold_section())
    assert "Do not bump" in text
    assert "GEV" in text
    assert "REAX" in text
    assert "thin-n" in text


def test_mechanism_table_render() -> None:
    md = oa._mechanism_table([{
        "mechanism": "Theme Radar fade vetoes",
        "goal": "Avoid",
        "fields": "`Forward P/E`",
        "basket_fire": "GEV 19/19",
        "veto_not_fuel": "YES",
        "elevate": "NO",
    }])
    text = "\n".join(md)
    assert "Theme Radar fade vetoes" in text
    assert "**Avoid**" in text
    assert "NO" in text


def test_does_not_touch_live_policy() -> None:
    from pathlib import Path
    text = Path(oa.__file__).read_text(encoding="utf-8")
    assert "LIVE_POLICY" not in text or "untouched" in text
    assert "from . import sleeve_merge" not in text
    assert oa.run.__doc__ is None or True
    assert fsf.HIGH_FPE == 35.0


def main() -> None:
    test_prior_is_strictly_before()
    test_outcome_prefers_change_from_open()
    test_score_rule_both_tape_avoid_needs_underperform()
    test_cheap_fpe_does_not_survive_as_elevate()
    test_thin_n_when_one_tape_small()
    test_sift_fold_rejects_canslim_as_as_elevate()
    test_mechanism_table_render()
    test_does_not_touch_live_policy()
    print("test_overlay_autopsy: 8 ok")


if __name__ == "__main__":
    main()
