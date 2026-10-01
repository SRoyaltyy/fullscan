"""Offline MF / CANSLIM flags — exact Elite headers, no live book.

Run: python -m src.test_finviz_style_flags
"""
from __future__ import annotations

import io
from pathlib import Path

from src import finviz_style_flags as fsf

HEADERS = [
    fsf.H_TICKER, fsf.H_SECTOR, fsf.H_INCOME, fsf.H_EV, fsf.H_EV_EBITDA,
    fsf.H_PE, fsf.H_ROIC, fsf.H_MCAP, fsf.H_EPS_QOQ, fsf.H_EPS_SURP,
    fsf.H_EPS_THIS, fsf.H_EPS_P3, fsf.H_HIGH_52, fsf.H_RVOL, fsf.H_ADV,
    fsf.H_PERF_Q, fsf.H_INST_OWN, fsf.H_INST_TX,
    fsf.H_FPE, fsf.H_RSI,
]


def _row(**kw) -> dict:
    base = {h: "" for h in HEADERS}
    base.update(kw)
    return base


def test_to_float_percent_and_blank() -> None:
    assert fsf.to_float("72.08%") == 72.08
    assert fsf.to_float("128930.00") == 128930.0
    assert fsf.finite("-") is None
    assert fsf.finite("") is None
    assert fsf.finite("N/A") is None


def test_earnings_yield_prefers_income_over_ev() -> None:
    row = _row(**{fsf.H_INCOME: "100", fsf.H_EV: "1000",
                  fsf.H_EV_EBITDA: "10", fsf.H_PE: "20"})
    ey = fsf.earnings_yield(row)
    assert ey is not None
    assert abs(ey - 0.1) < 1e-9


def test_earnings_yield_falls_back_when_ev_blank() -> None:
    # Banks often have blank Enterprise Value (JPM on 2026-09-04).
    row = _row(**{fsf.H_INCOME: "63632", fsf.H_EV: "",
                  fsf.H_EV_EBITDA: "", fsf.H_PE: "15.51"})
    ey = fsf.earnings_yield(row)
    assert ey is not None
    assert abs(ey - 1 / 15.51) < 1e-9


def test_mf_ranks_cheap_high_roic_first() -> None:
    cheap = _row(**{
        fsf.H_TICKER: "CHEAP", fsf.H_SECTOR: "Technology",
        fsf.H_INCOME: "50", fsf.H_EV: "200", fsf.H_ROIC: "40%",
        fsf.H_MCAP: "5000",
    })
    rich = _row(**{
        fsf.H_TICKER: "RICH", fsf.H_SECTOR: "Technology",
        fsf.H_INCOME: "10", fsf.H_EV: "1000", fsf.H_ROIC: "5%",
        fsf.H_MCAP: "5000",
    })
    flags = fsf.flag_rows([cheap, rich], mf_top_n=1)
    by = {r["Ticker"]: r for r in flags}
    assert by["CHEAP"]["mf_place"] == "1"
    assert by["CHEAP"]["mf_flag"] == "1"
    assert by["RICH"]["mf_place"] == "2"
    assert by["RICH"]["mf_flag"] == "0"
    assert float(by["CHEAP"]["ey"]) > float(by["RICH"]["ey"])


def test_mf_drops_sub_100m_mcap() -> None:
    tiny = _row(**{
        fsf.H_TICKER: "TINY", fsf.H_SECTOR: "Technology",
        fsf.H_INCOME: "50", fsf.H_EV: "80", fsf.H_ROIC: "90%",
        fsf.H_MCAP: "40",
    })
    flags = fsf.flag_rows([tiny], mf_top_n=10)
    assert flags[0]["mf_place"] == "1"
    assert flags[0]["mf_flag"] == "0"


def test_mf_drops_financial_by_default() -> None:
    bank = _row(**{
        fsf.H_TICKER: "JPM", fsf.H_SECTOR: "Financial",
        fsf.H_INCOME: "100", fsf.H_EV: "200", fsf.H_ROIC: "80%",
        fsf.H_MCAP: "900000", fsf.H_PE: "15",
    })
    flags = fsf.flag_rows([bank], mf_top_n=10)
    assert flags[0]["mf_excluded"] == "1"
    assert flags[0]["mf_flag"] == "0"
    keep = fsf.flag_rows([bank], mf_top_n=10, exclude_fin_util=False)
    assert keep[0]["mf_flag"] == "1"


def test_canslim_needs_every_letter() -> None:
    good = _row(**{
        fsf.H_TICKER: "WIN", fsf.H_SECTOR: "Technology",
        fsf.H_EPS_QOQ: "29.13%", fsf.H_EPS_SURP: "6.77%",
        fsf.H_EPS_THIS: "30%", fsf.H_EPS_P3: "6.89%",
        fsf.H_HIGH_52: "-4.75%", fsf.H_RVOL: "1.2", fsf.H_ADV: "54616.42",
        fsf.H_PERF_Q: "5.46%", fsf.H_INST_OWN: "68.91%",
        fsf.H_INST_TX: "0.16%",
    })
    flags = fsf.flag_rows([good])
    assert flags[0]["canslim_flag"] == "1"
    assert flags[0]["c_qoq"] == "1"
    assert flags[0]["n_high"] == "1"

    miss = dict(good)
    miss[fsf.H_EPS_SURP] = "-1.00%"
    flags = fsf.flag_rows([miss])
    assert flags[0]["c_qoq"] == "0"
    assert flags[0]["canslim_flag"] == "0"


def test_n_high_uses_finviz_percent_below_high() -> None:
    far = _row(**{fsf.H_TICKER: "FAR", fsf.H_HIGH_52: "-40%"})
    near = _row(**{fsf.H_TICKER: "NEAR", fsf.H_HIGH_52: "-4.75%"})
    brk = _row(**{fsf.H_TICKER: "BRK", fsf.H_HIGH_52: "1.2%"})
    flags = {r["Ticker"]: r for r in fsf.flag_rows([far, near, brk])}
    assert flags["FAR"]["n_high"] == "0"
    assert flags["NEAR"]["n_high"] == "1"
    assert flags["BRK"]["n_high"] == "1"


def test_ab_join_is_ticker_and_p01_can_veto_leader() -> None:
    row = _row(**{
        fsf.H_TICKER: "LAG", fsf.H_PERF_Q: "8%",
        fsf.H_EPS_QOQ: "20%", fsf.H_EPS_SURP: "5%",
        fsf.H_EPS_THIS: "30%", fsf.H_HIGH_52: "-2%",
        fsf.H_RVOL: "1.1", fsf.H_ADV: "800",
        fsf.H_INST_OWN: "40%", fsf.H_INST_TX: "1%",
    })
    ab = {"LAG": {"ab_score": 12.0, "P01_peer_lead_week": -1}}
    flags = fsf.flag_rows([row], ab_map=ab)
    assert flags[0]["ab_score"] == "12.0"
    assert flags[0]["P01_peer_lead_week"] == "-1"
    assert flags[0]["l_leader"] == "0"
    assert flags[0]["canslim_flag"] == "0"


def test_load_and_cli_roundtrip(tmp_path: Path | None = None) -> None:
    from tempfile import TemporaryDirectory
    td = Path(tmp_path) if tmp_path is not None else None
    ctx = None
    if td is None:
        ctx = TemporaryDirectory()
        td = Path(ctx.name)
    try:
        src = td / "finviz_2026-09-04.csv"
        fields = HEADERS
        with src.open("w", encoding="utf-8", newline="") as fh:
            fh.write(",".join(fields) + "\n")
            fh.write(",".join([
                "AAA", "Technology", "50", "200", "8", "10", "40%", "5000",
                "29%", "7%", "30%", "10%", "-3%", "1.5", "2000",
                "6%", "50%", "0.2%", "22", "55",
            ]) + "\n")
        abp = td / "2026-09-04_ab_checklist_enriched.csv"
        with abp.open("w", encoding="utf-8", newline="") as fh:
            fh.write("Ticker,score_enriched,P01_peer_lead_week\n")
            fh.write("AAA,9,1\n")
        out = td / "flags.csv"
        rc = fsf.main(["--csv", str(src), "--ab", str(abp), "--out", str(out)])
        assert rc == 0
        text = out.read_text(encoding="utf-8")
        assert "Ticker" in text
        assert "AAA" in text
        assert "canslim_flag" in text
        line = [ln for ln in text.splitlines() if ln.startswith("AAA,")][0]
        assert ",1" in line
    finally:
        if ctx is not None:
            ctx.cleanup()


def test_theme_radar_high_fpe_is_avoid_and_not_elevate() -> None:
    row = _row(**{fsf.H_TICKER: "RICH", fsf.H_FPE: "48",
                  fsf.H_RSI: "62", fsf.H_MCAP: "5000"})
    prior = _row(**{fsf.H_TICKER: "RICH", fsf.H_RSI: "61", fsf.H_MCAP: "4980"})
    radar = fsf.theme_radar(row, prior)
    assert radar["radar_high_fpe"] is True
    assert radar["avoid_veto"] is True
    flag = {"canslim_flag": "1", "P01_peer_lead_week": "1", "ab_score": "12"}
    assert fsf.elevate_bump(flag, radar) is False


def test_cheap_fpe_is_not_auto_elevate() -> None:
    row = _row(**{fsf.H_TICKER: "CHEAP", fsf.H_FPE: "9",
                  fsf.H_RSI: "45", fsf.H_MCAP: "8000"})
    radar = fsf.theme_radar(row, None)
    assert radar["radar_cheap_fpe"] is True
    assert radar["avoid_veto"] is False
    # MF / cheap alone — no CANSLIM, no AB lead.
    assert fsf.elevate_bump({"canslim_flag": "0", "ab_score": ""}, radar) is False
    assert fsf.elevate_bump({"mf_flag": "1", "canslim_flag": "0"}, radar) is False


def test_rsi_and_mcap_up_is_avoid() -> None:
    row = _row(**{fsf.H_TICKER: "HOT", fsf.H_FPE: "18",
                  fsf.H_RSI: "68", fsf.H_MCAP: "1200"})
    prior = _row(**{fsf.H_TICKER: "HOT", fsf.H_RSI: "60", fsf.H_MCAP: "1100"})
    radar = fsf.theme_radar(row, prior)
    assert radar["d_rsi"] is not None and radar["d_rsi"] >= 5
    assert radar["d_mcap_pct"] is not None and radar["d_mcap_pct"] >= 3
    assert radar["radar_hot"] is True
    assert radar["radar_rsi_up"] is True
    assert radar["radar_mcap_up"] is True
    assert radar["avoid_veto"] is False  # combo failed both-tape; FPE-only veto
    flags = fsf.flag_rows([row], prior_by_ticker={"HOT": prior})
    assert flags[0]["radar_rsi_up"] == "1"
    assert flags[0]["radar_mcap_up"] == "1"
    assert flags[0]["avoid_veto"] == "0"


def test_elevate_needs_canslim_and_ab_or_p01() -> None:
    row = _row(**{
        fsf.H_TICKER: "WIN", fsf.H_SECTOR: "Technology", fsf.H_FPE: "22",
        fsf.H_EPS_QOQ: "29%", fsf.H_EPS_SURP: "7%", fsf.H_EPS_THIS: "30%",
        fsf.H_HIGH_52: "-3%", fsf.H_RVOL: "1.2", fsf.H_ADV: "2000",
        fsf.H_PERF_Q: "6%", fsf.H_INST_OWN: "50%", fsf.H_INST_TX: "0.2%",
        fsf.H_MCAP: "5000", fsf.H_RSI: "55",
    })
    flags = fsf.flag_rows(
        [row],
        ab_map={"WIN": {"ab_score": 9.0, "P01_peer_lead_week": 1}},
        prior_by_ticker={"WIN": _row(**{fsf.H_RSI: "54", fsf.H_MCAP: "4990"})},
    )
    assert flags[0]["canslim_flag"] == "1"
    assert flags[0]["avoid_veto"] == "0"
    assert flags[0]["elevate_bump"] == "1"


def test_does_not_import_live_policy() -> None:
    import src.finviz_style_flags as mod
    text = Path(mod.__file__).read_text(encoding="utf-8")
    assert not hasattr(mod, "LIVE_POLICY")
    assert "from . import sleeve_merge" not in text
    assert "from src import sleeve_merge" not in text
    assert "import sleeve_merge" not in text


def main() -> None:
    test_to_float_percent_and_blank()
    test_earnings_yield_prefers_income_over_ev()
    test_earnings_yield_falls_back_when_ev_blank()
    test_mf_ranks_cheap_high_roic_first()
    test_mf_drops_sub_100m_mcap()
    test_mf_drops_financial_by_default()
    test_canslim_needs_every_letter()
    test_n_high_uses_finviz_percent_below_high()
    test_ab_join_is_ticker_and_p01_can_veto_leader()
    test_load_and_cli_roundtrip()
    test_theme_radar_high_fpe_is_avoid_and_not_elevate()
    test_cheap_fpe_is_not_auto_elevate()
    test_rsi_and_mcap_up_is_avoid()
    test_elevate_needs_canslim_and_ab_or_p01()
    test_does_not_import_live_policy()
    print("test_finviz_style_flags: 15 ok")


if __name__ == "__main__":
    main()
