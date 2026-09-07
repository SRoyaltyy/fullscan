"""Re-score the few families that matter on latest + prev tiles.

Used after capture_all_cols --tile prev so 2025 exists. Does not dump
thousands of gates. Research only.

  python engine/score_families.py --grids research/all_cols_grids
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
from mine_full_grid import (  # noqa: E402
    FIVE, add_labels, attach_spy, hyst_on, liquid, load_dumps, split_masks,
)
from mine_hi_corr import (  # noqa: E402
    MIN_N, Y2026, cost_of, load_mcap, load_split, tstat,
)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "research" / "FAMILY_2025.md"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="research/all_cols_grids")
    args = ap.parse_args()
    grids = Path(args.grids)
    if not grids.is_absolute():
        grids = ROOT / grids
    discovery, holdout = load_split()
    mcap = load_mcap()
    print(f"loading {grids}", flush=True)
    df, inv = load_dumps(grids)
    print(f"  {inv['n_tickers']} / {inv['n_rows']} {inv['date_min']}→{inv['date_max']}",
          flush=True)
    df = add_labels(df)
    if "CE_v" in df.columns:
        df["CE_v_l1"] = df.groupby("ticker", sort=False)["CE_v"].shift(1)
    if "CF_v" in df.columns:
        df["CF_v_l1"] = df.groupby("ticker", sort=False)["CF_v"].shift(1)
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    splits = split_masks(df, discovery, holdout)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    light5 = hyst_on(df, FIVE, 5.0, 2.0)
    o_green = (df["O_f"] == "green").fillna(False).to_numpy() if "O_f" in df.columns \
        else np.zeros(len(df), dtype=bool)
    families = {
        "CE yesterday = 1 (fresh 24d+43d low)": (
            (df["CE_v_l1"] == 1).fillna(False).to_numpy()
            if "CE_v_l1" in df.columns else np.zeros(len(df), dtype=bool),
            "y_h1",
        ),
        "CF yesterday = 1 (24d low)": (
            (df["CF_v_l1"] == 1).fillna(False).to_numpy()
            if "CF_v_l1" in df.columns else np.zeros(len(df), dtype=bool),
            "y_h1",
        ),
        "five-cell light + green O": (light5 & o_green, "y_h1"),
        "CE yesterday = 1 → 1w from open": (
            (df["CE_v_l1"] == 1).fillna(False).to_numpy()
            if "CE_v_l1" in df.columns else np.zeros(len(df), dtype=bool),
            "y_from_open_1w",
        ),
    }
    lines = [
        "# Family check on latest + prev tiles",
        "",
        f"_Generated {date.today().isoformat()}. Research only._",
        "",
        f"Window **{df['date'].min()} → {df['date'].max()}**. "
        f"Tickers **{df['ticker'].nunique()}**. Liquid days **{len(df)}**. "
        f"2025 days **{int(splits['y2025'].sum())}**.",
        "",
        "| family | slice | mean after fees | n | t | vs book |",
        "|---|---|---:|---:|---:|---:|",
    ]
    hold = splits["hold"]
    for name, (mask, ycol) in families.items():
        y = df[ycol].to_numpy() - cost
        book = y[np.isfinite(y) & hold]
        book_m = float(np.nanmean(book)) if book.size else float("nan")
        for slice_name, sm in (
            ("holdout 2026", hold & splits["y2026"]),
            ("holdout 2025", hold & splits["y2025"]),
            ("holdout all", hold),
            ("discovery 2025", splits["disc"] & splits["y2025"]),
        ):
            yy = y[mask & sm & np.isfinite(y)]
            if yy.size < 30:
                lines.append(f"| {name} | {slice_name} | — | {yy.size} | — | — |")
                continue
            mu = float(yy.mean())
            lines.append(
                f"| {name} | {slice_name} | {mu*100:+.2f}% | {int(yy.size)} | "
                f"{tstat(yy):.1f} | {book_m*100:+.2f}% |"
            )
    lines += [
        "",
        "CE must beat the book in **2025 holdout** before it is a research keep. "
        "Light+O is scored on this paint only.",
        "",
        "Research only. No cards.",
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
