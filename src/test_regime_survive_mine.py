"""Leak-free unit tests for the regime-survival mine. No full asof scan."""
from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from src import regime_survive_mine as rsm


CAL = [
    "2026-08-12", "2026-08-13", "2026-08-14", "2026-08-17",
    "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21",
]


def test_feature_export_is_always_prior_session() -> None:
    for d in CAL[1:]:
        prior = rsm.prior_session(CAL, d)
        assert prior is not None
        assert prior < d
        assert prior in CAL
    assert rsm.prior_session(CAL, CAL[0]) is None


def test_hit_actuals_ignore_sector_hit_pct() -> None:
    got = rsm.load_hit_actuals()
    assert got.get("2026-08-12") == 0.26
    assert got.get("2026-08-13") == 0.65
    # Sector table Mag HIT% on 08-12 is 36.4 — must not become SPY.
    assert got.get("2026-08-12") != 36.4
    assert all(abs(v) < 5 for v in got.values())


def test_spy_prior_never_uses_selection_day() -> None:
    hit = {"2026-08-12": 0.26, "2026-08-13": 0.65}
    with tempfile.TemporaryDirectory() as tmp:
        rec = rsm.spy_prior_for("2026-08-13", CAL, hit, export_dir=Path(tmp))
    assert rec["prior_session"] == "2026-08-12"
    assert rec["prior_session"] != "2026-08-13"
    assert rec["spy_prior_pct"] == 0.26
    assert rec["spy_src"] == "hit_board_prior"


def test_spy_prior_walks_past_weekend_and_missing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        # Friday 08-14 has SPY. Sat/Sun dumps must not be used for Monday 08-17.
        _write_finviz(folder, "2026-08-14", [("SPY", "100", "0.41%")])
        _write_finviz(folder, "2026-08-15", [("SPY", "100", "9.99%")])
        _write_finviz(folder, "2026-08-16", [("SPY", "100", "-8.88%")])
        rec = rsm.spy_prior_for(
            "2026-08-17", hit_actuals={"2026-08-16": 9.99}, export_dir=folder,
        )
        assert rec["prior_session"] == "2026-08-14"
        assert rec["spy_prior_pct"] == 0.41
        assert rec["spy_src"] == "finviz_prior"
        # 08-26 has no tape and no HIT actual — walk to 08-25, never 08-27.
        _write_finviz(folder, "2026-08-25", [("SPY", "100", "0.32%")])
        rec2 = rsm.spy_prior_for("2026-08-27", hit_actuals={}, export_dir=folder)
        assert rec2["prior_session"] == "2026-08-25"
        assert rec2["prior_session"] != "2026-08-27"
        assert rec2["spy_prior_pct"] == 0.32


def test_spy_prior_rejects_sector_hit_pct_magnitude() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        rec = rsm.spy_prior_for(
            "2026-08-13",
            hit_actuals={"2026-08-12": 36.4, "2026-08-11": 0.22},
            export_dir=folder,
        )
        assert rec["spy_prior_pct"] == 0.22
        assert rec["prior_session"] == "2026-08-11"
        assert abs(rec["spy_prior_pct"]) < 20


def test_gen_s_and_weather_risk_from_morning_s() -> None:
    th = {"risk_on_score": 4.0, "risk_off_score": -4.0}
    assert rsm.gen_s_bucket(8.53) == "up"
    assert rsm.gen_s_bucket(0.75) == "flat"
    assert rsm.gen_s_bucket(-0.9) == "flat"
    assert rsm.gen_s_bucket(-6.2) == "down"
    assert rsm.gen_s_bucket(None) == "flat"
    assert rsm.weather_risk_from_s(8.53, th) == "on"
    assert rsm.weather_risk_from_s(2.25, th) == "mixed"
    assert rsm.weather_risk_from_s(-5.17, th) == "off"
    assert rsm.weather_risk_from_s(None, th) == "unknown"


def test_factor_spec_rejects_outcome_columns() -> None:
    row = {"blue": True, "ret_1d": 9.4, "join": "good"}
    try:
        rsm.factor_match(row, {"ret_1d": 1})
        raise AssertionError("ret_1d must be rejected")
    except ValueError as exc:
        assert "leak" in str(exc)
    try:
        rsm.factor_match(row, {"Change%": 1})
        raise AssertionError("Change% must be rejected")
    except ValueError as exc:
        assert "leak" in str(exc)
    assert rsm.factor_match(row, {"blue": True, "join": "good"}) is True
    assert rsm.factor_match(row, {"blue": True, "join": "bad"}) is False


def test_within_bucket_edge_vs_peers() -> None:
    rows = []
    for i in range(50):
        rows.append({
            "date": "2026-08-20", "ticker": f"A{i}",
            "blue": True, "join": "good",
            "gen_s": "up", "weather_risk": "mixed", "spy_prior": "down",
            "ret_1d": 2.0, "xs_1d": 1.5,
            "ret_3d": 3.0, "xs_3d": 1.0,
        })
    for i in range(50):
        rows.append({
            "date": "2026-08-20", "ticker": f"B{i}",
            "blue": False, "join": "good",
            "gen_s": "up", "weather_risk": "mixed", "spy_prior": "down",
            "ret_1d": -1.0, "xs_1d": -1.0,
            "ret_3d": -1.0, "xs_3d": -1.0,
        })
    for i in range(50):
        rows.append({
            "date": "2026-08-18", "ticker": f"C{i}",
            "blue": True, "join": "good",
            "gen_s": "down", "weather_risk": "off", "spy_prior": "down",
            "ret_1d": -2.0, "xs_1d": -0.5,
            "ret_3d": -2.0, "xs_3d": -0.5,
        })
    for i in range(50):
        rows.append({
            "date": "2026-08-18", "ticker": f"D{i}",
            "blue": False, "join": "good",
            "gen_s": "down", "weather_risk": "off", "spy_prior": "down",
            "ret_1d": 0.0, "xs_1d": 0.5,
            "ret_3d": 0.0, "xs_3d": 0.5,
        })
    rec = rsm.measure_factor(rows, {"blue": True}, "1d", min_n=20)
    assert rec["n"] == 100
    assert rec["fail"], "blue must fail at least one down-tape bucket"
    assert "gen_s=down" in rec["fail"]
    assert "gen_s=up" in rec["pass"]


def test_pass_requires_hit_and_excess() -> None:
    """Higher mean excess with a worse hit rate is not a WIN."""
    rows = []
    for i in range(40):
        rows.append({
            "date": "2026-08-20", "ticker": f"A{i}",
            "blue": True, "join": "good",
            "gen_s": "up", "weather_risk": "mixed", "spy_prior": "up",
            "ret_1d": 20.0 if i < 8 else -0.2,
            "xs_1d": 19.0 if i < 8 else -0.3,
        })
    for i in range(40):
        rows.append({
            "date": "2026-08-20", "ticker": f"B{i}",
            "blue": False, "join": "good",
            "gen_s": "up", "weather_risk": "mixed", "spy_prior": "up",
            "ret_1d": 0.5, "xs_1d": 0.3,
        })
    rec = rsm.measure_factor(rows, {"blue": True}, "1d", min_n=20)
    assert rec["edge"] is not None and rec["edge"] > 0
    assert rec["hit"] < rec["peer_hit"]
    assert "gen_s=up" in rec["fail"]


def test_ungated_io_buys_are_the_seven_sit_mornings() -> None:
    assert len(rsm.UNGATED_IO_BUYS) == 7
    assert "2026-08-20" not in rsm.UNGATED_IO_BUYS
    assert "2026-08-21" not in rsm.UNGATED_IO_BUYS
    assert "2026-08-18" not in rsm.UNGATED_IO_BUYS  # hard-red, both sit


def test_candidate_factors_are_pit_only() -> None:
    for fac in rsm.candidate_factors():
        assert not (set(fac["spec"]) & rsm.LEAK_COLS)
        rsm.factor_match(
            {"blue": True, "alarm": False, "join": "good", "gen": "bad",
             "sector": "good", "ab": "good", "peer": "good", "vol": "good",
             "heat": "good", "n_red": 0, "n_print": 6,
             "ab_good": True, "peer_good": True, "vol_good": True,
             "join_good": True, "catal": False, "ins_buy": False,
             "ins_sell": False, "qc_hot": False, "white": False,
             "fade": False, "first_crack": False, "steady": False,
             "fat": False, "relvol_b": "hot", "rsi_b": "oversold",
             "sma20_b": "below", "short_b": "high", "gap_b": "down",
             "perf_w_b": "washed", "earn_b": "missing",
             "cond": "good", "region": "bad"},
            fac["spec"],
        )


def _write_finviz(folder: Path, day: str, rows: list[tuple[str, str, str]]) -> None:
    path = folder / f"finviz_{day}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["Ticker", "Price", "Change"])
        w.writeheader()
        for t, px, chg in rows:
            w.writerow({"Ticker": t, "Price": px, "Change": chg})


def test_fill_returns_are_later_session_outcomes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        _write_finviz(folder, "2026-08-20", [("AAA", "100", "1.00%"), ("SPY", "500", "-0.50%")])
        _write_finviz(folder, "2026-08-21", [("AAA", "104", "4.00%"), ("SPY", "501", "0.20%")])
        _write_finviz(folder, "2026-08-24", [("AAA", "110", "5.77%"), ("SPY", "505", "0.80%")])
        rows = [
            {"ticker": "AAA", "date": "2026-08-20", "ret_1d": None, "ret_3d": None},
            {"ticker": "AAA", "date": "2026-08-21", "ret_1d": None, "ret_3d": None},
        ]
        cal = ["2026-08-20", "2026-08-21", "2026-08-24"]
        out = rsm.fill_returns_from_finviz(rows, export_dir=folder, session_cal=cal)
        # 1d for 08-20 = 104/100 - 1 = 4%. Must not use 08-20's own +1% Change.
        assert out[0]["ret_1d"] == 4.0
        assert out[0]["ret_3d"] is None  # no D+3 in cal
        # 1d for 08-21 = 110/104 - 1
        assert abs(out[1]["ret_1d"] - 100.0 * (110 / 104 - 1)) < 0.01
        # Existing finite return is kept.
        keep = [{"ticker": "AAA", "date": "2026-08-20", "ret_1d": 9.9, "ret_3d": None}]
        kept = rsm.fill_returns_from_finviz(keep, export_dir=folder, session_cal=cal)
        assert kept[0]["ret_1d"] == 9.9


if __name__ == "__main__":
    test_feature_export_is_always_prior_session()
    test_hit_actuals_ignore_sector_hit_pct()
    test_spy_prior_never_uses_selection_day()
    test_spy_prior_walks_past_weekend_and_missing()
    test_spy_prior_rejects_sector_hit_pct_magnitude()
    test_gen_s_and_weather_risk_from_morning_s()
    test_factor_spec_rejects_outcome_columns()
    test_within_bucket_edge_vs_peers()
    test_pass_requires_hit_and_excess()
    test_ungated_io_buys_are_the_seven_sit_mornings()
    test_candidate_factors_are_pit_only()
    test_fill_returns_are_later_session_outcomes()
    print("ok")
