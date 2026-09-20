"""Next Avoid — JAM leak clock, size gate, 🚨∧fade not OR FPE.

Run: python3 -m src.test_overlay_next_avoid
"""
from __future__ import annotations

from pathlib import Path

from src import overlay_horizon_bt as oh
from src import overlay_next_avoid as ona
from src.theme_radar_baskets import TS_TOP_Q


def test_prior_ab_never_same_day() -> None:
    cal = ["2026-08-18", "2026-08-19", "2026-08-20"]
    assert ona.prior_ab_date("2026-08-18", cal) is None
    assert ona.prior_ab_date("2026-08-19", cal) == "2026-08-18"
    assert ona.prior_ab_date("2026-08-20", cal) == "2026-08-19"
    assert ona.prior_ab_date("2026-08-14", cal) is None
    hit = ona.prior_ab_date("2026-08-20", cal)
    assert hit is not None and hit < "2026-08-20"


def test_ab_silent_missing_file_and_eps() -> None:
    assert ona.is_ab_silent(None, None) is True
    assert ona.is_ab_silent(None, "2026-08-18") is True
    assert ona.is_ab_silent({"s_ab": None}, "2026-08-18") is True
    assert ona.is_ab_silent({"s_ab": 0.04}, "2026-08-18") is True
    assert ona.is_ab_silent({"s_ab": 0.05}, "2026-08-18") is False
    assert ona.is_ab_silent({"s_ab": 0.8}, "2026-08-18") is False


def test_size_gate_includes_veri_aeva_excludes_acmr() -> None:
    assert ona.size_ok("micro") is True
    assert ona.size_ok("small") is True
    assert ona.size_ok("mid") is False
    assert ona.size_ok("large") is False
    # VERI + AEVA/RXT shape; ACMR mid stays out.
    veri = {"total_score": 8.0, "size": "micro"}
    aeva = {"total_score": 7.0, "size": "small"}
    acmr = {"total_score": 5.0, "size": "mid"}
    cut = 6.0
    assert ona.is_jam(veri, cut, None, None) is True
    assert ona.is_jam(aeva, cut, None, None) is True
    assert ona.is_jam(acmr, cut, None, None) is False


def test_join_hot_uses_p80_not_s_join_floor() -> None:
    # 8378/11586 would fire on raw s_join ≥ 0.30 — that is not this gate.
    rows = {}
    for i in range(100):
        rows[f"T{i}"] = {"total_score": float(i - 80), "size": "small"}
    cut = ona.join_high_cut(rows)
    assert cut is not None
    assert cut >= 0
    assert ona.is_join_hot({"total_score": cut}, cut) is True
    assert ona.is_join_hot({"total_score": cut - 0.01}, cut) is False
    # Degenerate session (08-18 p80 = -1) is not a fire.
    junk = {f"Z{i}": {"total_score": -1.0, "size": "small"} for i in range(30)}
    assert ona.join_high_cut(junk) is None
    assert ona.is_join_hot({"total_score": -1.0}, None) is False
    assert TS_TOP_Q == 0.80


def test_alarm_fade_not_or_with_fpe() -> None:
    assert ona.is_alarm_fade(None) is False
    assert ona.is_alarm_fade({"lb_alarm": True, "lb_fade": False}) is False
    assert ona.is_alarm_fade({"lb_alarm": False, "lb_fade": True}) is False
    assert ona.is_alarm_fade({"lb_alarm": True, "lb_fade": True}) is True
    # Missing columns look like False/False — not a fire, not an FPE OR.
    src = Path(ona.__file__).read_text(encoding="utf-8")
    assert "high_fpe" not in src
    assert "HIGH_FPE" not in src
    assert "not OR" in src or "Not OR" in src or "not_or_with_fpe" in src


def test_concentration_still_rejects_one_day() -> None:
    c = oh.concentration([100.0, 1.0, 1.0])
    assert c["reject"] is True
    gate = oh.decide(
        {"both_tape": True, "thin": False, "n": 40, "xs": -1.0,
         "mean_pnl": -2.0, "walk_forward": {"ok": True, "thin": False}},
        {"pnl": 50.0}, {"pnl": 80.0},
        [100.0, 1.0, 1.0], n_avoided=21, book_kind="leftover")
    assert gate["verdict"] == "FAIL"
    assert gate["concentration"]["reject"] is True


def test_veto_never_fired_is_fail() -> None:
    ic = {"both_tape": None, "thin": True, "n": 0, "xs": None,
          "mean_pnl": None, "walk_forward": {"ok": None, "thin": True}}
    gate = oh.decide(ic, {"pnl": 10.0}, {"pnl": 10.0},
                     [0.0] * 5, n_avoided=0, book_kind="leftover")
    assert gate["verdict"] == "FAIL"
    assert "never fired" in (gate["reasons"][0] or "")


def test_fpe_h5_stays_closed() -> None:
    assert oh.fpe_clock_allowed("flatten_h5") is False
    assert oh.fpe_clock_allowed("theme_radar_1d") is True
    src = Path(ona.__file__).read_text(encoding="utf-8")
    assert "flatten_h5" in src
    assert "remine FPE" in src


def test_does_not_import_live_policy() -> None:
    text = Path(ona.__file__).read_text(encoding="utf-8")
    assert "LIVE_POLICY" not in text or "Does not import" in text
    assert "from . import sleeve_merge" not in text
    assert "from src import sleeve_merge" not in text
    assert "flatten_robust" in text  # named as untouched


def test_named_autopsy_size_on_disk() -> None:
    j = ona.load_join_map("2026-08-14")
    assert j["VERI"]["size"] == "micro"
    assert j["AEVA"]["size"] == "small"
    assert j["RXT"]["size"] == "small"
    assert j["ACMR"]["size"] == "mid"
    cut = ona.join_high_cut(j)
    assert cut is not None and cut >= 0
    assert ona.is_join_hot(j["VERI"], cut)
    assert ona.is_join_hot(j["AEVA"], cut)
    assert not ona.is_jam(j["ACMR"], cut, None, None)


def main() -> None:
    test_prior_ab_never_same_day()
    test_ab_silent_missing_file_and_eps()
    test_size_gate_includes_veri_aeva_excludes_acmr()
    test_join_hot_uses_p80_not_s_join_floor()
    test_alarm_fade_not_or_with_fpe()
    test_concentration_still_rejects_one_day()
    test_veto_never_fired_is_fail()
    test_fpe_h5_stays_closed()
    test_does_not_import_live_policy()
    test_named_autopsy_size_on_disk()
    print("test_overlay_next_avoid: 10 ok")


if __name__ == "__main__":
    main()
