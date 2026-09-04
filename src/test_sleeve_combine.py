"""Regime router + combine report. No network.

Run: python -m src.test_sleeve_combine
"""
from __future__ import annotations

from src.sleeve_combine import (
    BUCKET_CASH,
    BUCKET_IO,
    BUCKET_MOVER,
    IO_HARD_RED,
    MOVER_GATE,
    _parse_ret,
    compound,
    daily_returns,
    max_drawdown,
    render,
    route,
    route_empty_gap,
    route_fallback,
)


def test_route_green_is_mover() -> None:
    r = route(1.0)
    assert r["primary"] == BUCKET_MOVER
    assert r["excel_role"] == "confirm_only"
    assert r["score"] == 1.0
    r = route(5.5)
    assert r["primary"] == BUCKET_MOVER


def test_route_mid_is_io() -> None:
    r = route(0.75)
    assert r["primary"] == BUCKET_IO
    r = route(-3.0)
    assert r["primary"] == BUCKET_IO
    r = route(-0.9)
    assert r["primary"] == BUCKET_IO


def test_route_hard_red_is_cash() -> None:
    r = route(-3.01)
    assert r["primary"] == BUCKET_CASH
    assert r["excel_role"] == "shorts_only_unfunded"
    assert "no new 1d" in r["why"]
    r = route(-6.3)
    assert r["primary"] == BUCKET_CASH


def test_missing_predict_parks_in_io() -> None:
    r = route(None)
    assert r["primary"] == BUCKET_IO
    r = route(None, missing_is_io=False)
    assert r["primary"] == BUCKET_MOVER


def test_thresholds_match_the_user_rule() -> None:
    assert MOVER_GATE == 1.0
    assert IO_HARD_RED == -3.0


def test_route_fallback_skip_day_is_io_including_hard_red() -> None:
    assert route_fallback(-0.9)["primary"] == BUCKET_IO
    assert route_fallback(-2.9)["primary"] == BUCKET_IO
    assert route_fallback(0.0)["primary"] == BUCKET_IO
    assert route_fallback(0.75)["primary"] == BUCKET_IO
    assert route_fallback(-3.0)["primary"] == BUCKET_IO
    assert route_fallback(-6.2)["primary"] == BUCKET_IO
    assert route_fallback(2.25)["primary"] == BUCKET_MOVER
    assert route_fallback(1.0)["primary"] == BUCKET_MOVER
    assert route_fallback(None)["primary"] == BUCKET_MOVER


def test_route_empty_gap_only_when_list_empty_and_nonneg() -> None:
    assert route_empty_gap(5.5, 0)["kind"] == "empty_gap"
    assert route_empty_gap(5.5, 0)["primary"] == BUCKET_IO
    assert route_empty_gap(0.0, 0)["kind"] == "empty_gap"
    assert route_empty_gap(5.5, 1)["kind"] == "mover"
    assert route_empty_gap(5.5, 1)["primary"] == BUCKET_MOVER
    assert route_empty_gap(-0.9, 0)["kind"] == "skip"
    assert route_empty_gap(-0.9, 794)["primary"] == BUCKET_IO
    assert route_empty_gap(None, 0)["primary"] == BUCKET_MOVER


def test_stitch_skip_io_uses_live_2w_on_skip_days() -> None:
    from src.mover_paper import stitch_skip_io
    raw = {
        "capital": 100_000, "top_n": 10, "pct": 0.10,
        "trades": [], "skipped": [],
        "curve": [
            {"date": "2026-08-13", "score": 8.5, "n_mover_calls": 0,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-08-14", "score": 5.5, "n_mover_calls": 0,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-08-17", "score": 2.25, "n_mover_calls": 1,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-08-18", "score": -6.2, "n_mover_calls": 117,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-08-21", "score": 3.25, "n_mover_calls": 120,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-08-28", "score": 0.75, "n_mover_calls": 261,
             "cash": 100_000, "equity": 100_000, "open": 0},
            {"date": "2026-09-03", "score": -0.9, "n_mover_calls": 794,
             "cash": 100_000, "equity": 100_000, "open": 0},
        ],
        "final_equity": 100_000, "by_source": {},
    }
    sim, gates = stitch_skip_io(
        raw, {"regime": {}},
        io_rets={"2026-08-14": 0.0317, "2026-08-18": 0.0197,
                 "2026-09-03": 0.0199},
    )
    by = {g["date"]: g["decision"] for g in gates}
    assert by["2026-08-13"] == "IO-GAP"
    assert by["2026-08-14"] == "IO-GAP"
    assert by["2026-08-17"] == "MOVER"
    assert by["2026-08-18"] == "IO"
    assert by["2026-08-21"] == "MOVER"  # calls exist; 0 fills is not a gap
    assert by["2026-08-28"] == "IO"
    assert by["2026-09-03"] == "IO"
    assert "CASH" not in by.values()
    assert sim["io_fallback"] is True
    assert "2w_size" in sim["hold"]
    want = 100_000 * 1.0317 * 1.0197 * 1.0199
    assert abs(sim["final_equity"] - want) < 1.0
    day = {g["date"]: g.get("advisory") for g in gates}
    assert "3.17%" in (day["2026-08-14"] or "")
    assert "1.99%" in (day["2026-09-03"] or "")
    assert "gap" in (day["2026-08-28"] or "")


def test_excel_is_never_the_primary() -> None:
    for s in (8.5, 1.0, 0.0, -3.0, -6.2, None):
        r = route(s)
        assert r["primary"] != "excel"
        assert r["excel_role"] in ("confirm_only", "shorts_only_unfunded")


def test_excel_ret_is_a_fraction() -> None:
    assert abs(_parse_ret("-0.72%") + 0.0072) < 1e-12
    assert abs(_parse_ret("+85.09%") - 0.8509) < 1e-12
    assert _parse_ret("") is None


def test_daily_returns_and_dd() -> None:
    eq = {"d1": 100.0, "d2": 110.0, "d3": 99.0}
    rets = daily_returns(eq)
    assert abs(rets["d2"] - 0.10) < 1e-12
    assert abs(rets["d3"] - (99 / 110 - 1)) < 1e-12
    assert "d1" not in rets
    assert abs(max_drawdown(eq) - (99 / 110 - 1)) < 1e-12
    assert abs(compound([0.10, 99 / 110 - 1]) - (99 / 100 - 1)) < 1e-12


def test_render_has_the_three_jobs() -> None:
    md = render({
        "generated_at": "2026-09-04T00:00:00-04:00",
        "window": ["2026-08-13", "2026-09-03"],
        "policy": {"mover_gate": 1.0, "io_hard_red": -3.0,
                   "io_pref_sleeves": ["2w_size"], "excel_confirm": ["L3"]},
        "card": {"date": "2026-09-03", "score": -0.9, "primary": "io",
                 "why": "test"},
        "mover": {"final_equity": 109259, "ret": 0.0926, "max_dd": -0.0012,
                  "trades": 29, "win": 0.621, "gross_win": 9000,
                  "gross_loss": -100, "open_days": [], "gated_days": [],
                  "blocked_bad": [], "kept_good": []},
        "io": {"days": [], "stats": {"all": {"n": 1, "mean": 0.01,
                                             "vs_spy": 0.02, "win": 0.6}}},
        "excel": {"universe": 3603, "live_n": 10, "live_win": 40.0,
                  "scoreboard": {}},
        "overlap": {"same_day_excel_and_book_days": 0,
                    "same_day_excel_and_book_book_days": 0,
                    "all_three": [], "mover_and_book": []},
        "combine": {"final": {"router": 0.1, "hold_through": 0.12},
                    "max_dd": {"router": -0.02, "hold_through": -0.03}},
    })
    assert "Excel · mover · .io" in md
    assert "vast swaths" in md
    assert "highest hit-rate" in md
    assert "down days" in md
    assert "Do not average the three pick lists" in md
    assert "Today (2026-09-03)" in md
    assert "Hold-through" in md
    assert "no new 1d risk" in md


if __name__ == "__main__":
    test_route_green_is_mover()
    test_route_mid_is_io()
    test_route_hard_red_is_cash()
    test_missing_predict_parks_in_io()
    test_thresholds_match_the_user_rule()
    test_route_fallback_skip_day_is_io_including_hard_red()
    test_route_empty_gap_only_when_list_empty_and_nonneg()
    test_stitch_skip_io_uses_live_2w_on_skip_days()
    test_excel_is_never_the_primary()
    test_excel_ret_is_a_fraction()
    test_daily_returns_and_dd()
    test_render_has_the_three_jobs()
    print("ok")
