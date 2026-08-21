"""Enrich AB checklist with peer leadership + industry/sector regime.

Uses only on-disk artifacts (no LLM, no live web):
  - data/peers/correlations.csv  (preferred)
  - data/peers/Correlations.xlsx (fallback — same map peer_rs loads)
  - data/exports/finviz_*.csv
  - 01_daily/sectors/<date>/_BOARD.md
  - data/ab_checklist/<date>_ab_checklist*.csv

If --ticker is set and that name was filtered out of the liquid checklist
(mcap/ADV gate), we still build a one-row enrich from Finviz export + peer map
instead of failing.

CLI:
  python -m src.ab_enrich --date 2026-08-18
  python -m src.ab_enrich --date 2026-08-18 --ticker BB
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import peer_rs

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
AB_DIR = ROOT / "data" / "ab_checklist"
SECTOR_DIR = ROOT / "01_daily" / "sectors"
PEERS_DIR = ROOT / "data" / "peers"
OUT_DIR = AB_DIR
ET = ZoneInfo(config.TZ)

PERF_WEEK = "Performance (Week)"
PERF_MONTH = "Performance (Month)"
CHANGE = "Change"


def _pct(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).replace("%", "").replace(",", "").strip()
    if s in ("", "-", "nan", "None", "—"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _corr_paths_status() -> list[str]:
    lines = []
    for p in (
        PEERS_DIR / "correlations.csv",
        PEERS_DIR / "Correlations.xlsx",
        PEERS_DIR / "correlations.csv.gz",
        PEERS_DIR / "parts",
    ):
        if p.exists():
            extra = ""
            if p.is_file():
                extra = f" ({p.stat().st_size:,} bytes)"
            lines.append(f"FOUND {p.relative_to(ROOT)}{extra}")
        else:
            lines.append(f"missing {p.relative_to(ROOT)}")
    return lines


def _latest_ab_path(date: str | None) -> tuple[str, Path | None]:
    if not AB_DIR.exists():
        return date or "", None
    cands = sorted(AB_DIR.glob("*_ab_checklist*.csv"))
    # prefer base checklist over enriched for re-runs
    cands = [p for p in cands if "enriched" not in p.name]
    if not cands:
        cands = sorted(AB_DIR.glob("*_ab_checklist*.csv"))
    if not cands:
        return date or "", None

    def key(p: Path):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", p.name)
        return m.group(1) if m else p.name

    if date:
        for name in (
            f"{date}_ab_checklist.csv",
            f"{date}_ab_checklist_merged.csv",
        ):
            p = AB_DIR / name
            if p.exists():
                return date, p
        # any matching date prefix
        hit = [p for p in cands if p.name.startswith(date)]
        if hit:
            return date, hit[0]
        return date, None

    cands = sorted(cands, key=key)
    p = cands[-1]
    return key(p), p


def _resolve_export(date: str) -> Path:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        raise SystemExit("[ab_enrich] no finviz exports")
    older = [f for f in files if f.stem.replace("finviz_", "") <= date]
    return older[-1] if older else files[-1]


def _parse_sector_board(date: str) -> dict[str, dict]:
    if not SECTOR_DIR.exists():
        return {}
    boards = sorted(SECTOR_DIR.glob("*/_BOARD.md"))
    boards = [b for b in boards if b.parent.name <= date] or boards
    if not boards:
        return {}
    path = boards[-1]
    text = path.read_text(encoding="utf-8", errors="replace")
    out: dict[str, dict] = {}
    for line in text.splitlines():
        if not line.startswith("|") or line.startswith("|-"):
            continue
        parts = [c.strip() for c in line.strip("|").split("|")]
        if len(parts) < 6:
            continue
        name, etf, direction, mag, score_s, conf_s = parts[:6]
        if name.lower() in ("sector", ""):
            continue
        try:
            score = float(score_s)
        except ValueError:
            score = np.nan
        try:
            conf = float(conf_s)
        except ValueError:
            conf = np.nan
        out[name] = {
            "dir": direction.lower().strip(),
            "score": score,
            "conf": conf,
            "mag": mag,
            "etf": etf,
            "board_date": path.parent.name,
        }
    return out


def _industry_sector_stats(export: pd.DataFrame):
    tcol = "Ticker" if "Ticker" in export.columns else export.columns[0]
    export = export.copy()
    export["Ticker"] = export[tcol].astype(str).str.strip().str.upper()
    export["_week"] = export[PERF_WEEK].map(_pct) if PERF_WEEK in export.columns else np.nan

    ind = None
    if "Industry" in export.columns:
        g = export.dropna(subset=["_week"]).groupby(export["Industry"].astype(str))
        ind = g.agg(
            ind_med_week=("_week", "median"),
            ind_n=("_week", "count"),
            ind_pct_up=("_week", lambda s: float((s > 0).mean()) if len(s) else np.nan),
        ).reset_index().rename(columns={"Industry": "industry"})

    sec = None
    if "Sector" in export.columns:
        g = export.dropna(subset=["_week"]).groupby(export["Sector"].astype(str))
        sec = g.agg(
            sec_med_week=("_week", "median"),
            sec_n=("_week", "count"),
            sec_pct_up=("_week", lambda s: float((s > 0).mean()) if len(s) else np.nan),
        ).reset_index().rename(columns={"Sector": "sector"})
    return ind, sec


def _flags_row(r: pd.Series) -> dict:
    rs = r.get("rs_week")
    beat = r.get("beat_week_pct")
    peer_med = r.get("peer_med_week")
    ind_med = r.get("ind_med_week")
    sec_dir = str(r.get("sector_dir") or "").lower()
    sec_score = r.get("sector_score")

    p01 = 0
    if np.isfinite(rs) and np.isfinite(beat):
        if rs > 0 and beat >= 0.5:
            p01 = 1
        elif rs < 0 and beat <= 0.5:
            p01 = -1

    p02 = 0
    if np.isfinite(peer_med):
        p02 = 1 if peer_med > 0 else (-1 if peer_med < 0 else 0)

    p03 = 0
    if np.isfinite(ind_med):
        p03 = 1 if ind_med > 0 else (-1 if ind_med < 0 else 0)

    p04 = 0
    if sec_dir in ("up", "down"):
        p04 = 1 if sec_dir == "up" else -1
    elif np.isfinite(sec_score):
        p04 = 1 if sec_score > 0 else (-1 if sec_score < 0 else 0)

    return {
        "P01_peer_lead_week": p01,
        "P02_peers_advancing": p02,
        "P03_industry_advancing": p03,
        "P04_sector_supportive": p04,
    }


def _finviz_ab_proxy(export: pd.DataFrame) -> pd.DataFrame:
    """OHLC-free AB-like base score from the Elite export so the book always has AB."""
    tcol = "Ticker" if "Ticker" in export.columns else export.columns[0]
    out = pd.DataFrame({"Ticker": export[tcol].astype(str).str.strip().str.upper()})

    def num(col):
        if col not in export.columns:
            return pd.Series(np.nan, index=export.index)
        return pd.to_numeric(
            export[col].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False),
            errors="coerce",
        )

    s = pd.Series(0, index=export.index, dtype=int)
    eps = num("EPS Surprise")
    s += np.where(eps >= 20, 2, np.where(eps >= 5, 1, np.where(eps <= -20, -2, np.where(eps <= -5, -1, 0))))
    rec = num("Analyst Recom")
    s += np.where(rec <= 2.0, 2, np.where(rec <= 2.5, 1, np.where(rec >= 3.5, -1, 0)))
    sma50 = num("50-Day Simple Moving Average")
    sma200 = num("200-Day Simple Moving Average")
    s += np.where((sma50 > 0) & (sma200 > 0), 1, np.where((sma50 < 0) & (sma200 < 0), -1, 0))
    rsi = num("Relative Strength Index (14)")
    s += np.where((rsi >= 40) & (rsi <= 65), 1, np.where(rsi >= 75, -1, np.where(rsi <= 30, 1, 0)))
    pw = num("Performance (Week)")
    s += np.where(pw > 2, 1, np.where(pw < -2, -1, 0))
    pm = num("Profit Margin")
    s += np.where(pm > 10, 1, np.where(pm < 0, -1, 0))
    out["score"] = s.astype(int).values
    out["score_source"] = "finviz_ab_proxy"
    return out


def _stub_from_export(ticker: str, export: pd.DataFrame) -> pd.DataFrame:
    """Minimal checklist-like row when name failed liquid gate / missing from AB CSV."""
    t = ticker.upper()
    hit = export[export["Ticker"] == t]
    if hit.empty:
        # still allow peer-only row
        return pd.DataFrame([{"Ticker": t, "score": 0, "score_source": "stub_not_in_export"}])
    row = hit.iloc[0]
    return pd.DataFrame(
        [
            {
                "Ticker": t,
                "score": 0,
                "score_source": "stub_not_in_liquid_checklist",
                "Sector": row.get("Sector"),
                "Industry": row.get("Industry"),
                "Price": row.get("Price"),
                "Market Cap": row.get("Market Cap"),
                "Average Volume": row.get("Average Volume"),
            }
        ]
    )


def run(date: str | None = None, ticker: str | None = None) -> Path:
    print("[ab_enrich] correlations search paths:")
    for line in _corr_paths_status():
        print(f"  {line}")

    # Load peer map early so we can confirm ticker membership
    try:
        corr_map = peer_rs._load_correlations()
        print(f"[ab_enrich] peer map loaded: {len(corr_map):,} tickers")
    except SystemExit as e:
        print(f"[ab_enrich] WARN peer map: {e}")
        corr_map = {}

    if ticker:
        tU = ticker.upper()
        peers = corr_map.get(tU, [])
        print(
            f"[ab_enrich] ticker={tU} in_correlations={tU in corr_map} "
            f"n_peers={len(peers)} peers={peers[:8]}"
        )

    date, ab_path = _latest_ab_path(date)
    if not date:
        raise SystemExit("[ab_enrich] could not resolve as-of date")

    export_path = _resolve_export(date)
    export = pd.read_csv(export_path, low_memory=False)
    tcol = "Ticker" if "Ticker" in export.columns else export.columns[0]
    export["Ticker"] = export[tcol].astype(str).str.strip().str.upper()
    print(f"[ab_enrich] export={export_path.name} rows={len(export):,}")

    if ab_path is not None:
        print(f"[ab_enrich] base checklist={ab_path.name}")
        ab = pd.read_csv(ab_path, low_memory=False)
        if "Ticker" not in ab.columns:
            ab = ab.rename(columns={ab.columns[0]: "Ticker"})
        ab["Ticker"] = ab["Ticker"].astype(str).str.strip().str.upper()
    else:
        print("[ab_enrich] WARN: no checklist CSV — scoring from Finviz AB proxy")
        ab = _finviz_ab_proxy(export)

    # If checklist exists but scores are all ~0, still add the Finviz proxy
    if "score" in ab.columns and pd.to_numeric(ab["score"], errors="coerce").fillna(0).abs().sum() == 0:
        print("[ab_enrich] checklist scores empty — overlay Finviz AB proxy")
        proxy = _finviz_ab_proxy(export)
        ab = ab.drop(columns=["score"], errors="ignore").merge(proxy[["Ticker", "score", "score_source"]], on="Ticker", how="left")
        ab["score"] = ab["score"].fillna(0)

    if ticker:
        tU = ticker.upper()
        sub = ab[ab["Ticker"] == tU].copy()
        if sub.empty:
            in_export = tU in set(export["Ticker"])
            print(
                f"[ab_enrich] WARN: {tU} not in liquid checklist "
                f"(usually mcap/ADV gate). in_export={in_export}. "
                f"Falling back to Finviz+Correlations stub."
            )
            ab = _stub_from_export(tU, export)
        else:
            ab = sub

    # Peer RS file
    peer_path = PEERS_DIR / f"{date}_peer_rs.csv"
    if not peer_path.exists():
        print("[ab_enrich] computing peer_rs…")
        peer_path = peer_rs.run(date)
    peer = pd.read_csv(peer_path)
    peer["Ticker"] = peer["Ticker"].astype(str).str.strip().str.upper()

    if ticker:
        tU = ticker.upper()
        if tU not in set(peer["Ticker"]):
            print(
                f"[ab_enrich] WARN: {tU} missing from peer_rs file — "
                f"not in Correlations map or not in export that day"
            )

    meta_cols = [c for c in ("Sector", "Industry") if c in export.columns]
    meta = export[["Ticker"] + meta_cols].drop_duplicates("Ticker")

    ind_stats, sec_stats = _industry_sector_stats(export)
    board = _parse_sector_board(date)
    print(f"[ab_enrich] sector board entries={len(board)} peer_rows={len(peer):,}")

    m = ab.merge(peer, on="Ticker", how="left", suffixes=("", "_peer"))
    # don't duplicate Sector/Industry if already present from stub
    for c in meta_cols:
        if c in m.columns and m[c].notna().any():
            meta = meta.drop(columns=[c], errors="ignore")
    if len(meta.columns) > 1:
        m = m.merge(meta, on="Ticker", how="left")

    if ind_stats is not None and "Industry" in m.columns:
        m = m.merge(ind_stats, left_on="Industry", right_on="industry", how="left")
    if sec_stats is not None and "Sector" in m.columns:
        m = m.merge(sec_stats, left_on="Sector", right_on="sector", how="left")

    def board_lookup(sector_name):
        empty = pd.Series(
            {"sector_dir": None, "sector_score": np.nan, "sector_conf": np.nan, "sector_board_date": None}
        )
        if not isinstance(sector_name, str) or not sector_name:
            return empty
        info = board.get(sector_name)
        if info is None:
            for k, v in board.items():
                if k.lower() == sector_name.lower():
                    info = v
                    break
        if not info:
            return empty
        return pd.Series(
            {
                "sector_dir": info.get("dir"),
                "sector_score": info.get("score"),
                "sector_conf": info.get("conf"),
                "sector_board_date": info.get("board_date"),
            }
        )

    if "Sector" in m.columns and board:
        bcols = m["Sector"].apply(board_lookup)
        m = pd.concat([m, bcols], axis=1)
    else:
        m["sector_dir"] = None
        m["sector_score"] = np.nan
        m["sector_conf"] = np.nan
        m["sector_board_date"] = None

    flag_rows = m.apply(_flags_row, axis=1, result_type="expand")
    m = pd.concat([m, flag_rows], axis=1)

    base = pd.to_numeric(m.get("score"), errors="coerce").fillna(0)
    extra = (
        m["P01_peer_lead_week"].fillna(0)
        + m["P02_peers_advancing"].fillna(0)
        + m["P03_industry_advancing"].fillna(0)
        + m["P04_sector_supportive"].fillna(0)
    )
    m["score_base"] = base
    m["score_context"] = extra.astype(int)
    m["score_enriched"] = (base + extra).astype(int)

    def lead_label(r):
        bits = []
        if r.get("P01_peer_lead_week") == 1:
            bits.append("LEAD")
        elif r.get("P01_peer_lead_week") == -1:
            bits.append("LAG")
        if r.get("P02_peers_advancing") == 1:
            bits.append("peers↑")
        elif r.get("P02_peers_advancing") == -1:
            bits.append("peers↓")
        if r.get("P03_industry_advancing") == 1:
            bits.append("ind↑")
        elif r.get("P03_industry_advancing") == -1:
            bits.append("ind↓")
        if r.get("P04_sector_supportive") == 1:
            bits.append("sec↑")
        elif r.get("P04_sector_supportive") == -1:
            bits.append("sec↓")
        return ",".join(bits) if bits else "—"

    m["context_label"] = m.apply(lead_label, axis=1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"_{ticker.upper()}" if ticker else ""
    out_csv = OUT_DIR / f"{date}_ab_checklist_enriched{suffix}.csv"
    m_sorted = m.sort_values("score_enriched", ascending=False)
    m_sorted.to_csv(out_csv, index=False)

    lines = [
        f"# AB enriched — {date}" + (f" — {ticker.upper()}" if ticker else ""),
        "",
        f"- Base checklist: `{ab_path.name if ab_path else 'stub/export'}`",
        f"- Peer map: `data/peers/correlations.csv` or `Correlations.xlsx` ({len(corr_map):,} names)",
        f"- Peer RS: `{peer_path.name}`",
        f"- Export: `{export_path.name}`",
        "",
        "## Flag legend",
        "",
        "| Flag | +1 | −1 |",
        "|------|----|----|",
        "| P01 peer lead week | rs_week>0 & beats ≥50% peers | lags |",
        "| P02 peers advancing | peer median week > 0 | < 0 |",
        "| P03 industry advancing | industry median week > 0 | < 0 |",
        "| P04 sector supportive | board Dir=up | Dir=down |",
        "",
        "## Results",
        "",
        "| Ticker | enr | base | ctx | rs_w | beat% | ind_med_w | sector | board | label |",
        "|--------|----:|-----:|----:|-----:|------:|----------:|--------|-------|-------|",
    ]
    show = m_sorted.head(25 if not ticker else 5)
    for _, r in show.iterrows():
        rs = r.get("rs_week")
        beat = r.get("beat_week_pct")
        indm = r.get("ind_med_week")
        lines.append(
            f"| {r['Ticker']} | {int(r['score_enriched']):+d} | {int(r['score_base']):+d} | "
            f"{int(r['score_context']):+d} | "
            f"{(f'{rs:+.1f}' if np.isfinite(rs) else '—')} | "
            f"{(f'{100*beat:.0f}%' if np.isfinite(beat) else '—')} | "
            f"{(f'{indm:+.1f}' if np.isfinite(indm) else '—')} | "
            f"{str(r.get('Sector') or '')[:18]} | "
            f"{str(r.get('sector_dir') or '—')} | {r.get('context_label')} |"
        )

    md = OUT_DIR / f"{date}_ab_checklist_enriched{suffix}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    print(f"[ab_enrich] wrote {out_csv.name} rows={len(m):,}")
    print(f"[ab_enrich] wrote {md.name}")
    if len(m_sorted):
        top = m_sorted.iloc[0]
        print(
            f"[ab_enrich] top {top['Ticker']} enr={int(top['score_enriched']):+d} "
            f"label={top['context_label']}"
        )
    return out_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--ticker", default=None)
    args = ap.parse_args()
    run(date=args.date, ticker=args.ticker)


if __name__ == "__main__":
    main()
