"""Offline Magic Formula + CANSLIM flags from one Finviz Elite CSV.

Research only. Does not touch flatten_robust, Yahoo, OpenBB, or the book.

A 09:30 recipe on session D must point this at the *prior* Elite file
(see factor_mine.feature_export_date). Running it on finviz_D.csv is a
snapshot of that file, not a live gate.

CLI:
  python -m src.finviz_style_flags --csv data/exports/finviz_2026-09-04.csv
  python -m src.finviz_style_flags --csv data/exports/finviz_2026-09-04.csv \\
      --ab data/ab_checklist/2026-09-04_ab_checklist_enriched.csv \\
      --out data/style_flags/2026-09-04_style_flags.csv
"""
from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Exact Elite headers — do not alias.
H_TICKER = "Ticker"
H_SECTOR = "Sector"
H_INCOME = "Income"
H_EV = "Enterprise Value"
H_EV_EBITDA = "EV/EBITDA"
H_PE = "P/E"
H_ROIC = "Return on Invested Capital"
H_MCAP = "Market Cap"
H_EPS_QOQ = "EPS Growth Quarter Over Quarter"
H_EPS_SURP = "EPS Surprise"
H_EPS_THIS = "EPS Growth This Year"
H_EPS_P3 = "EPS Growth Past 3 Years"
H_HIGH_52 = "52-Week High"
H_RVOL = "Relative Volume"
H_ADV = "Average Volume"
H_PERF_Q = "Performance (Quarter)"
H_INST_OWN = "Institutional Ownership"
H_INST_TX = "Institutional Transactions"

AB_TICKER = "Ticker"
AB_SCORE = ("score_enriched", "score", "checklist_score")
AB_P01 = "P01_peer_lead_week"

FINANCIAL_UTIL = frozenset({"Financial", "Utilities"})

# Research defaults (O'Neil / Greenblatt proxies, not vendor ratings).
MF_TOP_N = 50
MF_MIN_MCAP = 100.0  # $ millions — same floor as ticker_lookback.RANDOM_MIN_MCAP_M
C_QOQ_MIN = 18.0
A_ANNUAL_MIN = 25.0
N_NEAR_HIGH = -15.0  # Finviz % below the 52w high
S_RVOL_MIN = 1.0
S_ADV_MIN = 500.0  # thousands of shares
I_OWN_MIN = 20.0


def to_float(v: object) -> float:
    """Finviz Elite mixes floats, '19.55%', '-', and blanks."""
    if v is None:
        return math.nan
    if isinstance(v, bool):
        return math.nan
    if isinstance(v, (int, float)):
        x = float(v)
        return math.nan if math.isnan(x) else x
    s = str(v).replace("%", "").replace(",", "").strip()
    if s in ("", "-", "--", "N/A", "nan", "None", "—"):
        return math.nan
    try:
        return float(s)
    except ValueError:
        return math.nan


def finite(v: object) -> float | None:
    x = to_float(v)
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def earnings_yield(row: dict) -> float | None:
    """Income / Enterprise Value, else 1/EV/EBITDA, else 1/P/E."""
    income = finite(row.get(H_INCOME))
    ev = finite(row.get(H_EV))
    if income is not None and ev is not None and ev > 0 and income > 0:
        return income / ev
    ebitda_mult = finite(row.get(H_EV_EBITDA))
    if ebitda_mult is not None and ebitda_mult > 0:
        return 1.0 / ebitda_mult
    pe = finite(row.get(H_PE))
    if pe is not None and pe > 0:
        return 1.0 / pe
    return None


def load_finviz(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        rows = list(csv.DictReader(fh))
    out = []
    seen: set[str] = set()
    for raw in rows:
        t = str(raw.get(H_TICKER) or "").strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        raw[H_TICKER] = t
        out.append(raw)
    return out


def load_ab(path: Path | None) -> dict[str, dict]:
    if path is None or not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        rows = list(csv.DictReader(fh))
    out: dict[str, dict] = {}
    for raw in rows:
        t = str(raw.get(AB_TICKER) or "").strip().upper()
        if not t or t in out:
            continue
        score = None
        for col in AB_SCORE:
            if col in raw:
                score = finite(raw.get(col))
                if score is not None:
                    break
        p01 = finite(raw.get(AB_P01)) if AB_P01 in raw else None
        out[t] = {
            "ab_score": score,
            "P01_peer_lead_week": None if p01 is None else int(p01),
        }
    return out


def _rank_desc(values: list[float | None]) -> list[int | None]:
    """1 = best (largest). Ties share the min rank. None stays None."""
    order = sorted(
        ((i, v) for i, v in enumerate(values) if v is not None),
        key=lambda iv: iv[1],
        reverse=True,
    )
    ranks: list[int | None] = [None] * len(values)
    last_v = None
    last_rank = 0
    for n, (i, v) in enumerate(order, start=1):
        if last_v is None or v < last_v:
            last_rank = n
            last_v = v
        ranks[i] = last_rank
    return ranks


def _bit(ok: bool | None) -> str:
    if ok is None:
        return ""
    return "1" if ok else "0"


def flag_rows(rows: list[dict], ab_map: dict[str, dict] | None = None,
              mf_top_n: int = MF_TOP_N,
              exclude_fin_util: bool = True) -> list[dict]:
    ab_map = ab_map or {}
    n = len(rows)
    ey = [earnings_yield(r) for r in rows]
    roic = [finite(r.get(H_ROIC)) for r in rows]
    ey_rank = _rank_desc(ey)
    roc_rank = _rank_desc(roic)
    combo: list[int | None] = []
    for i in range(n):
        if ey_rank[i] is None or roc_rank[i] is None:
            combo.append(None)
        else:
            combo.append(int(ey_rank[i]) + int(roc_rank[i]))
    # Lower combo rank is better (Greenblatt). Rank those.
    combo_for_rank = [(-c if c is not None else None) for c in combo]
    mf_place = _rank_desc(combo_for_rank)  # 1 = smallest combo

    out = []
    for i, r in enumerate(rows):
        t = r[H_TICKER]
        sector = str(r.get(H_SECTOR) or "").strip()
        mcap = finite(r.get(H_MCAP))
        qoq = finite(r.get(H_EPS_QOQ))
        surp = finite(r.get(H_EPS_SURP))
        this_y = finite(r.get(H_EPS_THIS))
        past3 = finite(r.get(H_EPS_P3))
        high52 = finite(r.get(H_HIGH_52))
        rvol = finite(r.get(H_RVOL))
        adv = finite(r.get(H_ADV))
        perf_q = finite(r.get(H_PERF_Q))
        inst_own = finite(r.get(H_INST_OWN))
        inst_tx = finite(r.get(H_INST_TX))
        p01 = (ab_map.get(t) or {}).get("P01_peer_lead_week")
        ab_score = (ab_map.get(t) or {}).get("ab_score")

        c_ok = (qoq is not None and surp is not None
                and qoq >= C_QOQ_MIN and surp > 0)
        a_ok = (
            (this_y is not None and this_y >= A_ANNUAL_MIN)
            or (past3 is not None and past3 >= A_ANNUAL_MIN)
        )
        n_ok = high52 is not None and high52 > N_NEAR_HIGH
        s_ok = (rvol is not None and adv is not None
                and rvol >= S_RVOL_MIN and adv >= S_ADV_MIN)
        l_ok = perf_q is not None and perf_q > 0
        if p01 == 1:
            l_ok = bool(l_ok)
        elif p01 == -1:
            l_ok = False
        i_ok = (inst_own is not None and inst_tx is not None
                and inst_own >= I_OWN_MIN and inst_tx >= 0)
        letters = [c_ok, a_ok, n_ok, s_ok, l_ok, i_ok]
        canslim = all(letters)

        excluded = exclude_fin_util and sector in FINANCIAL_UTIL
        thin = mcap is not None and mcap < MF_MIN_MCAP
        mf_ok = (
            mf_place[i] is not None
            and mf_place[i] <= mf_top_n
            and not excluded
            and not thin
            and (mcap is None or mcap > 0)
        )

        rec = {
            "Ticker": t,
            "Sector": sector,
            "ey": "" if ey[i] is None else f"{ey[i]:.6f}",
            "roic": "" if roic[i] is None else f"{roic[i]:.4f}",
            "ey_rank": "" if ey_rank[i] is None else str(ey_rank[i]),
            "roc_rank": "" if roc_rank[i] is None else str(roc_rank[i]),
            "mf_combo_rank": "" if combo[i] is None else str(combo[i]),
            "mf_place": "" if mf_place[i] is None else str(mf_place[i]),
            "mf_flag": "1" if mf_ok else "0",
            "mf_excluded": "1" if excluded else "0",
            "c_qoq": _bit(None if qoq is None or surp is None else c_ok),
            "a_annual": _bit(None if this_y is None and past3 is None else a_ok),
            "n_high": _bit(None if high52 is None else n_ok),
            "s_demand": _bit(None if rvol is None or adv is None else s_ok),
            "l_leader": _bit(None if perf_q is None and p01 is None else l_ok),
            "i_inst": _bit(None if inst_own is None or inst_tx is None else i_ok),
            "canslim_flag": "1" if canslim else "0",
            "ab_score": "" if ab_score is None else str(ab_score),
            "P01_peer_lead_week": "" if p01 is None else str(p01),
        }
        out.append(rec)
    return out


def write_csv(rows: list[dict], dest) -> None:
    fields = list(rows[0].keys()) if rows else [
        "Ticker", "Sector", "ey", "roic", "ey_rank", "roc_rank",
        "mf_combo_rank", "mf_place", "mf_flag", "mf_excluded",
        "c_qoq", "a_annual", "n_high", "s_demand", "l_leader", "i_inst",
        "canslim_flag", "ab_score", "P01_peer_lead_week",
    ]
    dest.write(",".join(fields) + "\n")
    w = csv.DictWriter(dest, fieldnames=fields, lineterminator="\n")
    for r in rows:
        w.writerow(r)


def _date_from_name(path: Path) -> str | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    return m.group(1) if m else None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, help="One Finviz Elite export")
    ap.add_argument("--ab", default=None, help="Optional AB checklist CSV")
    ap.add_argument("--out", default=None, help="Output CSV (default stdout)")
    ap.add_argument("--top", type=int, default=MF_TOP_N,
                    help="Magic Formula keep first N by combo place")
    ap.add_argument("--keep-fin-util", action="store_true",
                    help="Do not drop Financial / Utilities from MF")
    args = ap.parse_args(argv)

    src = Path(args.csv)
    if not src.exists():
        raise SystemExit(f"[style-flags] missing {src}")
    rows = load_finviz(src)
    ab = load_ab(Path(args.ab) if args.ab else None)
    flags = flag_rows(
        rows, ab_map=ab, mf_top_n=max(1, args.top),
        exclude_fin_util=not args.keep_fin_util,
    )

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", encoding="utf-8", newline="") as fh:
            write_csv(flags, fh)
        n_mf = sum(1 for r in flags if r["mf_flag"] == "1")
        n_cs = sum(1 for r in flags if r["canslim_flag"] == "1")
        print(
            f"[style-flags] {_date_from_name(src) or src.name} "
            f"rows={len(flags)} mf={n_mf} canslim={n_cs} -> {outp}"
        )
    else:
        import sys
        write_csv(flags, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
