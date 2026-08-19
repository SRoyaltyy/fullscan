"""Enrich AB checklist with peer leadership + industry/sector regime.

Uses only on-disk artifacts (no LLM, no live web):
  - data/peers/Correlations.xlsx|correlations.csv  → peer map
  - data/exports/finviz_*.csv                       → week/month perf, Industry, Sector
  - 01_daily/sectors/<date>/_BOARD.md               → sector Dir/Score (nearest <= asof)
  - data/ab_checklist/<date>_ab_checklist*.csv      → base scores

Flags (long-biased, +1 / 0 / -1):
  P01_peer_lead_week     rs_week > 0 and beat_week_pct >= 0.5
  P02_peers_advancing    peer median week > 0
  P03_industry_advancing industry median week > 0
  P04_sector_supportive  sector board Dir == up (score>0)

CLI:
  python -m src.ab_enrich --date 2026-08-18
  python -m src.ab_enrich --date 2026-08-18 --ticker AAPL
"""
from __future__ import annotations

import argparse
import re
from datetime import datetime
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


def _latest_ab_path(date: str | None) -> tuple[str, Path]:
    cands = sorted(AB_DIR.glob("*_ab_checklist*.csv"))
    cands = [p for p in cands if "merged" not in p.name or True]
    # prefer non-enriched base, then any
    if not cands:
        raise SystemExit("[ab_enrich] no data/ab_checklist/*_ab_checklist*.csv — run ab_checklist first")
    if date:
        exact = [
            AB_DIR / f"{date}_ab_checklist.csv",
            AB_DIR / f"{date}_ab_checklist_merged.csv",
            AB_DIR / f"{date}_ab_checklist_enriched.csv",
        ]
        for p in exact:
            if p.exists():
                return date, p
        raise SystemExit(f"[ab_enrich] no checklist for {date}")
    # newest by name date prefix
    def key(p: Path):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", p.name)
        return m.group(1) if m else p.name

    cands = sorted(cands, key=key)
    p = cands[-1]
    d = key(p)
    return d, p


def _resolve_export(date: str) -> Path:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        raise SystemExit("[ab_enrich] no finviz exports")
    older = [f for f in files if f.stem.replace("finviz_", "") <= date]
    return older[-1] if older else files[-1]


def _parse_sector_board(date: str) -> dict[str, dict]:
    """Nearest _BOARD.md on or before date → {Sector Name: {dir, score, conf}}."""
    if not SECTOR_DIR.exists():
        return {}
    boards = sorted(SECTOR_DIR.glob("*/_BOARD.md"))
    boards = [b for b in boards if b.parent.name <= date]
    if not boards:
        boards = sorted(SECTOR_DIR.glob("*/_BOARD.md"))
    if not boards:
        return {}
    path = boards[-1]
    text = path.read_text(encoding="utf-8", errors="replace")
    out: dict[str, dict] = {}
    # table rows: | Sector | ETF | Dir | Mag | Score | Conf |
    for line in text.splitlines():
        if not line.startswith("|") or line.startswith("|-") or "Sector" in line and "ETF" in line:
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
        direction = direction.lower().strip()
        out[name] = {
            "dir": direction,
            "score": score,
            "conf": conf,
            "mag": mag,
            "etf": etf,
            "board_date": path.parent.name,
        }
    return out


def _industry_sector_stats(export: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    tcol = "Ticker" if "Ticker" in export.columns else export.columns[0]
    export = export.copy()
    export["Ticker"] = export[tcol].astype(str).str.strip().str.upper()
    export["_week"] = export[PERF_WEEK].map(_pct) if PERF_WEEK in export.columns else np.nan
    export["_chg"] = export[CHANGE].map(_pct) if CHANGE in export.columns else np.nan

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


def run(date: str | None = None, ticker: str | None = None) -> Path:
    date, ab_path = _latest_ab_path(date)
    print(f"[ab_enrich] base={ab_path.name} asof={date}")

    ab = pd.read_csv(ab_path, low_memory=False)
    if "Ticker" not in ab.columns:
        ab = ab.rename(columns={ab.columns[0]: "Ticker"})
    ab["Ticker"] = ab["Ticker"].astype(str).str.strip().str.upper()
    if ticker:
        ab = ab[ab["Ticker"] == ticker.upper()].copy()
        if ab.empty:
            raise SystemExit(f"[ab_enrich] {ticker} not in checklist")

    # Peer RS (Finviz week/month vs Correlations peers)
    peer_path = ROOT / "data" / "peers" / f"{date}_peer_rs.csv"
    if not peer_path.exists():
        print("[ab_enrich] computing peer_rs…")
        peer_path = peer_rs.run(date)
    peer = pd.read_csv(peer_path)
    peer["Ticker"] = peer["Ticker"].astype(str).str.strip().str.upper()

    export = pd.read_csv(_resolve_export(date), low_memory=False)
    tcol = "Ticker" if "Ticker" in export.columns else export.columns[0]
    export["Ticker"] = export[tcol].astype(str).str.strip().str.upper()
    meta_cols = [c for c in ("Sector", "Industry") if c in export.columns]
    meta = export[["Ticker"] + meta_cols].drop_duplicates("Ticker")

    ind_stats, sec_stats = _industry_sector_stats(export)
    board = _parse_sector_board(date)
    print(f"[ab_enrich] sector board entries={len(board)} peer_rows={len(peer):,}")

    m = ab.merge(peer, on="Ticker", how="left", suffixes=("", "_peer"))
    m = m.merge(meta, on="Ticker", how="left")
    if ind_stats is not None and "Industry" in m.columns:
        m = m.merge(
            ind_stats,
            left_on="Industry",
            right_on="industry",
            how="left",
        )
    if sec_stats is not None and "Sector" in m.columns:
        m = m.merge(
            sec_stats,
            left_on="Sector",
            right_on="sector",
            how="left",
        )

    # map sector board
    def board_lookup(sector_name):
        if not isinstance(sector_name, str) or not sector_name:
            return pd.Series({"sector_dir": None, "sector_score": np.nan, "sector_conf": np.nan, "sector_board_date": None})
        # exact then fuzzy
        info = board.get(sector_name)
        if info is None:
            for k, v in board.items():
                if k.lower() == sector_name.lower():
                    info = v
                    break
        if not info:
            return pd.Series({"sector_dir": None, "sector_score": np.nan, "sector_conf": np.nan, "sector_board_date": None})
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

    # composite: base score + peer/regime flags
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

    # leadership label for humans
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
    out_csv = OUT_DIR / f"{date}_ab_checklist_enriched.csv"
    m_sorted = m.sort_values("score_enriched", ascending=False)
    m_sorted.to_csv(out_csv, index=False)

    # MD summary
    lines = [
        f"# AB enriched — {date}",
        "",
        f"- Base checklist: `{ab_path.name}`",
        f"- Peer RS: `{peer_path.name}` (Correlations × Finviz week/month)",
        f"- Sector board: nearest `01_daily/sectors/*/ _BOARD.md` ≤ {date}",
        "",
        "## Flag legend",
        "",
        "| Flag | +1 | −1 |",
        "|------|----|----|",
        "| P01 peer lead week | rs_week>0 and beats ≥50% of peers | lags peers |",
        "| P02 peers advancing | peer median week > 0 | peer median week < 0 |",
        "| P03 industry advancing | industry median week > 0 | industry median week < 0 |",
        "| P04 sector supportive | sector board Dir=up | Dir=down |",
        "",
        f"- score_enriched = score_base + sum(P01..P04)",
        "",
        "## Top 25 by score_enriched",
        "",
        "| Ticker | enr | base | ctx | rs_w | beat% | ind_med_w | sector | board | label |",
        "|--------|----:|-----:|----:|-----:|------:|----------:|--------|-------|-------|",
    ]
    for _, r in m_sorted.head(25).iterrows():
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
            f"{str(r.get('sector_dir') or '—')}/{r.get('sector_score') if np.isfinite(r.get('sector_score', np.nan)) else '—'} | "
            f"{r.get('context_label')} |"
        )

    # context combo rates among top half scores
    lines += ["", "## Context combo counts (full liquid set)", ""]
    for lab in ("LEAD,peers↑,ind↑", "LEAD", "LAG", "peers↑", "peers↓", "ind↑", "ind↓", "sec↑", "sec↓"):
        # approximate via label contains
        n = int(m["context_label"].astype(str).str.contains(lab.split(",")[0] if "," not in lab else lab, regex=False).sum()) if lab != "LEAD,peers↑,ind↑" else int(
            m["context_label"].astype(str).apply(lambda s: all(x in s for x in ["LEAD", "peers↑", "ind↑"])).sum()
        )
        lines.append(f"- `{lab}`: **{n}**")

    lines += ["", f"- csv: `{out_csv.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{date}_ab_checklist_enriched.md"
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
