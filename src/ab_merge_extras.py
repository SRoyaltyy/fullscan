"""Merge Form-4 insider monthly panel + Finviz quote colors into latest ab_checklist.

Does not recompute Part A/B1 — only enriches the existing CSV/MD with:
  B15 Form4 net_value / net_delta (completed prior month)
  B16 quote snapshot green−red / key_color_score

CLI:
  python -m src.ab_merge_extras
  python -m src.ab_merge_extras --date 2026-08-18
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
AB_DIR = ROOT / "data" / "ab_checklist"
INS_PANEL = ROOT / "data" / "insider" / "history" / "monthly_panel.parquet"
INS_PANEL_CSV = ROOT / "data" / "insider" / "history" / "monthly_panel.csv"
COLORS_DIR = ROOT / "data" / "quote_colors"
ET = ZoneInfo(config.TZ)


def _load_panel() -> pd.DataFrame:
    if INS_PANEL.exists():
        return pd.read_parquet(INS_PANEL)
    if INS_PANEL_CSV.exists():
        return pd.read_csv(INS_PANEL_CSV)
    return pd.DataFrame()


def _latest_completed_month(asof: str) -> str:
    # prior calendar month relative to asof (avoid partial current month)
    return (pd.Timestamp(asof).to_period("M") - 1).strftime("%Y-%m")


def _insider_features(panel: pd.DataFrame, asof: str) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame()
    month = _latest_completed_month(asof)
    p = panel.copy()
    p["ticker"] = p["ticker"].astype(str).str.upper()
    cur = p[p["month"] == month].copy()
    if cur.empty:
        # fall back to max month ≤ target
        p = p[p["month"] <= month]
        if p.empty:
            return pd.DataFrame()
        idx = p.groupby("ticker")["month"].idxmax()
        cur = p.loc[idx].copy()
        month = "mixed"

    out = pd.DataFrame({
        "Ticker": cur["ticker"].values,
        "B15_form4_month": cur["month"].values if "month" in cur else month,
        "B15_form4_net": cur["net_value"].values,
        "B15_form4_net_prev": cur["net_prev"].values if "net_prev" in cur else np.nan,
        "B15_form4_net_delta": cur["net_delta"].values if "net_delta" in cur else np.nan,
        "B15_form4_buys": cur["n_buys"].values if "n_buys" in cur else np.nan,
        "B15_form4_sells": cur["n_sells"].values if "n_sells" in cur else np.nan,
    })

    def flag_row(r):
        d = r["B15_form4_net_delta"]
        n = r["B15_form4_net"]
        if np.isfinite(d):
            if d > 0 and (not np.isfinite(n) or n >= 0):
                return 1
            if d < 0:
                return -1
        if np.isfinite(n):
            return 1 if n > 0 else (-1 if n < 0 else 0)
        return 0

    out["flag_B15_form4_insider"] = out.apply(flag_row, axis=1)
    out["status_B15_form4_insider"] = out["flag_B15_form4_insider"].map(
        {1: "GOOD", 0: "NEUTRAL", -1: "BAD"}
    )
    out["val_B15_form4_insider"] = out.apply(
        lambda r: (
            f"month={r['B15_form4_month']} net={r['B15_form4_net']:,.0f} "
            f"prev={r['B15_form4_net_prev']} Δ={r['B15_form4_net_delta']} "
            f"buys={r['B15_form4_buys']} sells={r['B15_form4_sells']}"
        ),
        axis=1,
    )
    return out


def _color_features(asof: str) -> pd.DataFrame:
    # prefer same-date file, else latest
    exact = COLORS_DIR / f"{asof}_quote_colors.csv"
    files = sorted(COLORS_DIR.glob("????-??-??_quote_colors.csv"))
    path = exact if exact.exists() else (files[-1] if files else None)
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path)
    tcol = "ticker" if "ticker" in df.columns else "Ticker"
    out = pd.DataFrame({
        "Ticker": df[tcol].astype(str).str.upper(),
        "B16_n_green": df.get("n_green"),
        "B16_n_red": df.get("n_red"),
        "B16_green_minus_red": df.get("green_minus_red"),
        "B16_key_color_score": df.get("key_color_score"),
        "flag_B16_quote_colors": df.get("flag_colors", 0),
        "status_B16_quote_colors": df.get("status_colors", "NEUTRAL"),
    })
    out["val_B16_quote_colors"] = out.apply(
        lambda r: (
            f"green={r['B16_n_green']} red={r['B16_n_red']} "
            f"Δ={r['B16_green_minus_red']} key_score={r['B16_key_color_score']} "
            f"(source={path.name})"
        ),
        axis=1,
    )
    # recompute flag if missing
    if out["flag_B16_quote_colors"].isna().all():
        def f(x):
            try:
                v = float(x)
            except Exception:
                return 0
            if v >= 5:
                return 1
            if v <= -5:
                return -1
            return 0
        out["flag_B16_quote_colors"] = out["B16_green_minus_red"].map(f)
        out["status_B16_quote_colors"] = out["flag_B16_quote_colors"].map(
            {1: "GOOD", 0: "NEUTRAL", -1: "BAD"}
        )
    return out


def run(date: str | None = None) -> pd.DataFrame:
    files = sorted(AB_DIR.glob("????-??-??_ab_checklist.csv"))
    if not files:
        raise SystemExit("[merge] no ab_checklist CSV — run ab_checklist first")
    if date:
        exact = AB_DIR / f"{date}_ab_checklist.csv"
        path = exact if exact.exists() else files[-1]
    else:
        path = files[-1]
    asof = path.name[:10]
    ab = pd.read_csv(path)
    ab["Ticker"] = ab["Ticker"].astype(str).str.upper()

    panel = _load_panel()
    ins = _insider_features(panel, asof)
    cols = _color_features(asof)

    print(f"[merge] ab={path.name} rows={len(ab)}")
    print(f"[merge] insider panel rows matched prep={len(ins)}")
    print(f"[merge] colors rows={len(cols)}")

    out = ab.copy()
    if len(ins):
        # drop prior merge cols if re-run
        drop = [c for c in out.columns if c.startswith("B15_") or c.startswith("flag_B15") or c.startswith("status_B15") or c.startswith("val_B15")]
        out = out.drop(columns=drop, errors="ignore")
        out = out.merge(ins, on="Ticker", how="left")
        out["flag_B15_form4_insider"] = out["flag_B15_form4_insider"].fillna(0).astype(int)
        out["status_B15_form4_insider"] = out["status_B15_form4_insider"].fillna("NEUTRAL")
    else:
        out["flag_B15_form4_insider"] = 0
        out["status_B15_form4_insider"] = "NEUTRAL"
        out["val_B15_form4_insider"] = "no Form4 panel yet — run insider_history"

    if len(cols):
        drop = [c for c in out.columns if c.startswith("B16_") or c.startswith("flag_B16") or c.startswith("status_B16") or c.startswith("val_B16")]
        out = out.drop(columns=drop, errors="ignore")
        out = out.merge(cols, on="Ticker", how="left")
        out["flag_B16_quote_colors"] = pd.to_numeric(out["flag_B16_quote_colors"], errors="coerce").fillna(0).astype(int)
        out["status_B16_quote_colors"] = out["status_B16_quote_colors"].fillna("NEUTRAL")
    else:
        out["flag_B16_quote_colors"] = 0
        out["status_B16_quote_colors"] = "NEUTRAL"
        out["val_B16_quote_colors"] = "no quote_colors file — run quote_colors"

    # recompute total score if base score present
    if "score" in out.columns:
        base = out["score"].astype(float)
        # avoid double-counting if already merged once: use flag columns only as add-on tracked separately
        out["score_with_b15_b16"] = (
            base
            + out["flag_B15_form4_insider"].astype(float)
            + out["flag_B16_quote_colors"].astype(float)
        )
        out = out.sort_values("score_with_b15_b16", ascending=False)

    out_path = AB_DIR / f"{asof}_ab_checklist_merged.csv"
    out.to_csv(out_path, index=False)

    lines = [
        f"# A+B1 + B15 Form4 + B16 colors — {asof}",
        "",
        f"- Base checklist: `{path.name}`",
        f"- Form4 panel: `{'yes' if len(ins) else 'no'}`",
        f"- Quote colors: `{'yes' if len(cols) else 'no'}`",
        "",
        "| Ticker | score | +B15/B16 | Form4 Δ | colors Δ | Industry |",
        "|--------|------:|---------:|--------:|---------:|----------|",
    ]
    for _, r in out.head(25).iterrows():
        lines.append(
            f"| {r['Ticker']} | {r.get('score')} | {r.get('score_with_b15_b16')} | "
            f"{r.get('B15_form4_net_delta', '')} | {r.get('B16_green_minus_red', '')} | "
            f"{str(r.get('Industry', ''))[:36]} |"
        )
    lines += [
        "",
        "### B15 (Form 4)",
        "Completed prior calendar month net open-market P/S from SEC. "
        "GOOD if net_delta>0 (or net>0).",
        "",
        "### B16 (Finviz colors)",
        "green_minus_red across ~84 snapshot fields; flag GOOD if Δ≥5, BAD if Δ≤−5.",
        f"",
        f"CSV: `{out_path.relative_to(ROOT)}`",
    ]
    md_path = AB_DIR / f"{asof}_ab_checklist_merged.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "asof": asof,
        "n": int(len(out)),
        "has_form4": bool(len(ins)),
        "has_colors": bool(len(cols)),
        "generated": datetime.now(ET).isoformat(),
    }
    (AB_DIR / f"{asof}_ab_checklist_merged.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[merge] wrote {out_path}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    run(date=args.date)


if __name__ == "__main__":
    main()
