"""Holdout × year table for the upper→lower headline flags.

Uses the correct short fee: −return − cost (shorts pay, they do not
collect the spread). Lean-load only the letters that matter.

  python engine/score_upper_stories.py --grids research/all_cols_grids
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mine_full_grid import attach_spy, harden, liquid, s2d, split_masks  # noqa: E402
from mine_hi_corr import cost_of, load_mcap, load_split, tstat  # noqa: E402
from mine_upper_lower import signed_nets  # noqa: E402
from signals import classify_fill  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "research" / "UPPER_STORIES.md"
WANT = {"CE", "CD", "AM", "JB", "HE", "AH", "CF"}


def lean_load(grids: Path):
    files = sorted(p for p in grids.glob("*.json") if not p.name.startswith("_"))
    frames, chunk = [], []
    for i, p in enumerate(files, 1):
        raw = json.loads(p.read_text())
        t = raw["ticker"]
        prev_close = prev_vol = None
        recs = []
        for d in raw["days"]:
            o, c = d.get("open"), d.get("close")
            if not isinstance(o, (int, float)) or not isinstance(c, (int, float)) or o == 0:
                continue
            cells = d.get("cells") or {}
            rec = {
                "ticker": t,
                "date": s2d(d["date"]),
                "open": float(o), "close": float(c),
                "volume": float(d["volume"]) if isinstance(
                    d.get("volume"), (int, float)) else 0.0,
                "H": (c - o) / o,
                "dvol_l1": (prev_close * prev_vol) if prev_close and prev_vol else np.nan,
                "px_l1": prev_close if prev_close else np.nan,
            }
            for let in WANT:
                cell = cells.get(let) or {}
                v = cell.get("v")
                if isinstance(v, bool):
                    rec[f"{let}_v"] = float(v)
                elif isinstance(v, (int, float)):
                    rec[f"{let}_v"] = float(v)
                f = cell.get("f")
                if f:
                    fam, _sc = classify_fill(f)
                    if fam in ("green", "red"):
                        rec[f"{let}_f"] = fam
            recs.append(rec)
            prev_close, prev_vol = c, rec["volume"]
        if len(recs) >= 30:
            chunk.append(pd.DataFrame.from_records(recs))
        if len(chunk) >= 250:
            frames.append(pd.concat(chunk, ignore_index=True))
            chunk = []
            print(f"  loaded {i}/{len(files)}", flush=True)
    if chunk:
        frames.append(pd.concat(chunk, ignore_index=True))
    if not frames:
        raise SystemExit(f"no dumps in {grids}")
    df = pd.concat(frames, ignore_index=True)
    return df.sort_values(["ticker", "date"]).drop_duplicates(
        ["ticker", "date"], keep="last").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="research/all_cols_grids")
    args = ap.parse_args()
    grids = Path(args.grids)
    if not grids.is_absolute():
        grids = ROOT / grids
    discovery, holdout = load_split()
    mcap = load_mcap()
    print(f"lean-load {grids}", flush=True)
    df = lean_load(grids)
    g = df.groupby("ticker", sort=False)
    df["y_h1"] = df["H"]
    df["y_2d"] = g["close"].shift(-1) / df["open"] - 1
    df["y_3d"] = g["close"].shift(-2) / df["open"] - 1
    df["y_1w"] = g["close"].shift(-4) / df["open"] - 1
    for let in WANT:
        if f"{let}_v" in df.columns:
            df[f"{let}_v_l1"] = g[f"{let}_v"].shift(1)
            df[f"{let}_v_l2"] = g[f"{let}_v"].shift(2)
        if f"{let}_f" in df.columns:
            df[f"{let}_f_l1"] = g[f"{let}_f"].shift(1)
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    print(f"  {df['ticker'].nunique()} / {len(df)} "
          f"{df['date'].min()}→{df['date'].max()}", flush=True)
    splits = split_masks(df, discovery, holdout)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]], dtype=np.float64)
    hold = splits["hold"]

    def flag(col, pred):
        if col not in df.columns:
            return np.zeros(len(df), dtype=bool)
        return pred(df[col]).fillna(False).to_numpy()

    stories = [
        ("CE=1 yesterday (fresh 24d+43d low) → UP",
         flag("CE_v_l1", lambda s: s == 1), "long",
         ("y_h1", "y_2d", "y_3d", "y_1w")),
        ("CD=1 yesterday (fresh 24d+43d high) → DOWN",
         flag("CD_v_l1", lambda s: s == 1), "short",
         ("y_h1", "y_2d", "y_3d", "y_1w")),
        ("AM≥1 yesterday (weekly range once ≥100% of the low) → DOWN",
         flag("AM_v_l1", lambda s: s >= 1), "short",
         ("y_h1", "y_2d", "y_3d", "y_1w")),
        ("JB=1 yesterday (7 of last 8 days |H|>3%) → DOWN",
         flag("JB_v_l1", lambda s: s == 1), "short",
         ("y_h1", "y_1w")),
        ("HE=1 yesterday (crash print) → DOWN",
         flag("HE_v_l1", lambda s: s == 1), "short",
         ("y_h1", "y_1w")),
        ("AH red yesterday (several recent H≤−5%) → DOWN",
         flag("AH_f_l1", lambda s: s == "red"), "short",
         ("y_h1", "y_1w")),
        ("CF=1 yesterday (any 24d low, no 43d) → UP",
         flag("CF_v_l1", lambda s: s == 1), "long",
         ("y_h1", "y_1w")),
        ("CE=1 two days ago → DOWN over ~1w (bounce then fade)",
         flag("CE_v_l2", lambda s: s == 1), "short",
         ("y_1w",)),
    ]

    lines = [
        "# Upper→lower headline stories (correct short fee)",
        "",
        f"_Generated {date.today().isoformat()}. Research only._",
        "",
        "Short PnL is **−return − fee**. Long is **return − fee**.",
        "",
        f"Window **{df['date'].min()} → {df['date'].max()}**. "
        f"Tickers **{df['ticker'].nunique()}**. Liquid days **{len(df)}**. "
        f"2025 days **{int(splits['y2025'].sum())}**.",
        "",
        "| story | horizon | slice | after fees | n | t | book |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    harden_rows = []
    for name, mask, side, labels in stories:
        for ycol in labels:
            long_n, short_n = signed_nets(df[ycol].to_numpy(), cost)
            signed = long_n if side == "long" else short_n
            book_hold = signed[np.isfinite(signed) & hold]
            for slice_name, sm in (
                ("holdout 2026", hold & splits["y2026"]),
                ("holdout 2025", hold & splits["y2025"]),
                ("holdout all", hold),
                ("discovery 2025", splits["disc"] & splits["y2025"]),
            ):
                yy = signed[mask & sm & np.isfinite(signed)]
                bm = signed[np.isfinite(signed) & sm]
                if yy.size < 30:
                    lines.append(
                        f"| {name} | {ycol} | {slice_name} | — | "
                        f"{int(yy.size)} | — | — |"
                    )
                    continue
                lines.append(
                    f"| {name} | {ycol} | {slice_name} | "
                    f"{float(yy.mean())*100:+.2f}% | {int(yy.size)} | "
                    f"{tstat(yy):.1f} | "
                    f"{(float(np.nanmean(bm)) if bm.size else float('nan'))*100:+.2f}% |"
                )
            df["_y"] = signed
            zero = np.zeros(len(df))
            v, why, parts, uncond, day_s, top5 = harden(
                df, mask, "_y", zero, splits, "open")
            holdp = parts.get("hold") or {}
            harden_rows.append(
                f"| {name} | {ycol} | {side} | **{v}** | "
                f"{(holdp.get('mean') or 0)*100:+.2f}% (n={holdp.get('n')}) | "
                f"{', '.join(why) or '—'} |"
            )
    lines += [
        "",
        "## Ship bar",
        "",
        "| story | horizon | side | verdict | holdout | why |",
        "|---|---|---|---|---:|---|",
        *harden_rows,
        "",
        "Research only. No cards.",
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
