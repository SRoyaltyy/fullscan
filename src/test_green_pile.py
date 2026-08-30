"""Green-pile mask + status. No network.

Run: python -m src.test_green_pile
"""
from __future__ import annotations

import pandas as pd

from src.green_pile import EPS, GREEN_MIN, green_mask, pile_status


def _row(**kw):
    base = dict(s_join=0.2, s_general=0.2, s_ab=0.2, s_peer=0.2,
                s_sector=0.0, s_news=0.0, relvol=1.2)
    base.update(kw)
    return base


def test_all_green_passes() -> None:
    df = pd.DataFrame([_row() for _ in range(GREEN_MIN)])
    mask = green_mask(df)
    assert int(mask.sum()) == GREEN_MIN
    st = pile_status(df)
    assert st["used"] is True
    assert st["buy_mode"] == "green_pile"
    assert st["sell_mode"] in ("core_weights", "core_weights_ex_green")


def test_missing_ab_collapses() -> None:
    df = pd.DataFrame([_row(s_ab=0.0) for _ in range(20)])
    st = pile_status(df)
    assert st["used"] is False
    assert "AB" in st["missing_core"]
    assert st["buy_mode"] == "weighted_fallback"


def test_news_red_is_veto() -> None:
    df = pd.DataFrame([_row(s_news=-0.2), _row()])
    mask = green_mask(df)
    assert bool(mask.iloc[0]) is False
    assert bool(mask.iloc[1]) is True


def test_news_yellow_is_not_a_veto() -> None:
    df = pd.DataFrame([_row(s_news=0.0)])
    assert bool(green_mask(df).iloc[0]) is True


def test_dead_relvol_veto() -> None:
    df = pd.DataFrame([_row(relvol=0.4), _row(relvol=0.0)])
    mask = green_mask(df)
    assert bool(mask.iloc[0]) is False
    assert bool(mask.iloc[1]) is True  # 0 = no print, ignore


def test_thin_pile_fallback() -> None:
    df = pd.DataFrame([_row() for _ in range(GREEN_MIN - 1)])
    st = pile_status(df)
    assert st["n_pile"] == GREEN_MIN - 1
    assert st["used"] is False
    assert st["buy_mode"] == "weighted_fallback"


def test_core_eps() -> None:
    df = pd.DataFrame([_row(s_join=EPS - 0.001)])
    assert bool(green_mask(df).iloc[0]) is False
    df = pd.DataFrame([_row(s_join=EPS)])
    assert bool(green_mask(df).iloc[0]) is True


def test_green_rank_mean() -> None:
    from src.green_pile import attach_ranks
    df = attach_ranks(pd.DataFrame([_row(s_join=0.4, s_general=0.2, s_ab=0.6, s_peer=0.8)]))
    assert abs(float(df["green_rank"].iloc[0]) - 0.5) < 1e-9
    assert abs(float(df["s_tape"].iloc[0]) - 0.7) < 1e-9


def test_micro_not_liquid() -> None:
    rows = [_row() for _ in range(GREEN_MIN)]
    for r in rows:
        r["market_cap_m"] = 50.0
        r["size"] = "micro"
    st = pile_status(pd.DataFrame(rows))
    assert st["used"] is False
    assert st["n_pile_liquid"] == 0


def main() -> None:
    tests = [
        test_all_green_passes,
        test_missing_ab_collapses,
        test_news_red_is_veto,
        test_news_yellow_is_not_a_veto,
        test_dead_relvol_veto,
        test_thin_pile_fallback,
        test_core_eps,
        test_green_rank_mean,
        test_micro_not_liquid,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
