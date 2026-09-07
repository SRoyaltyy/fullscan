"""Re-score the few families that matter on latest + prev tiles.

Lean load: only CE/CF/S/O and the five morning fill scores — not 275
columns. Used after `capture_all_cols --tile prev` so 2025 exists.

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
    FIVE, attach_spy, harden, hyst_on, liquid, s2d, split_masks,
)
from mine_hi_corr import cost_of, load_mcap, load_split, tstat  # noqa: E402
from signals import classify_fill  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "research" / "FAMILY_2025.md"
WANT = set(FIVE) | {"O", "CE", "CF", "S", "AH", "FR"}


def lean_load(grids: Path):
    files = sorted(p for p in grids.glob("*.json") if not p.name.startswith("_"))
    frames = []
    chunk = []
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
                "volume": float(d["volume"]) if isinstance(d.get("volume"), (int, float)) else 0.0,
                "H": (c - o) / o,
                "dvol_l1": (prev_close * prev_vol) if prev_close and prev_vol else np.nan,
                "px_l1": prev_close if prev_close else np.nan,
            }
            for let in WANT:
                cell = cells.get(let) or {}
                v = cell.get("v")
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    rec[f"{let}_v"] = float(v)
                f = cell.get("f")
                if f:
                    fam, sc = classify_fill(f)
                    rec[f"{let}_fs"] = sc
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
    df = df.sort_values(["ticker", "date"]).drop_duplicates(
        ["ticker", "date"], keep="last").reset_index(drop=True)
    return df


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
    print(f"  {df['ticker'].nunique()} / {len(df)} {df['date'].min()}→{df['date'].max()}",
          flush=True)
    g = df.groupby("ticker", sort=False)
    df["y_h1"] = df["H"]
    df["y_from_open_1w"] = g["close"].shift(-4) / df["open"] - 1
    if "CE_v" in df.columns:
        df["CE_v_l1"] = g["CE_v"].shift(1)
    if "CF_v" in df.columns:
        df["CF_v_l1"] = g["CF_v"].shift(1)
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    splits = split_masks(df, discovery, holdout)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    light5 = hyst_on(df, FIVE, 5.0, 2.0)
    o_green = (df["O_f"] == "green").fillna(False).to_numpy() if "O_f" in df.columns \
        else np.zeros(len(df), dtype=bool)
    families = {
        "CE yesterday = 1 (fresh 24d+43d low) → same-day H": (
            (df["CE_v_l1"] == 1).fillna(False).to_numpy()
            if "CE_v_l1" in df.columns else np.zeros(len(df), dtype=bool),
            "y_h1",
        ),
        "CF yesterday = 1 (24d low) → same-day H": (
            (df["CF_v_l1"] == 1).fillna(False).to_numpy()
            if "CF_v_l1" in df.columns else np.zeros(len(df), dtype=bool),
            "y_h1",
        ),
        "five-cell light + green O → same-day H": (light5 & o_green, "y_h1"),
        "CE yesterday = 1 → buy open, sell ~1w later": (
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
        "| family | slice | mean after fees | n | t | holdout book |",
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
                lines.append(f"| {name} | {slice_name} | — | {int(yy.size)} | — | — |")
                continue
            mu = float(yy.mean())
            bm = y[np.isfinite(y) & sm]
            bm = float(np.nanmean(bm)) if bm.size else book_m
            lines.append(
                f"| {name} | {slice_name} | {mu*100:+.2f}% | {int(yy.size)} | "
                f"{tstat(yy):.1f} | {bm*100:+.2f}% |"
            )
    lines += [
        "",
        "## Ship bar (same harden as the full mine)",
        "",
        "| family | label | verdict | holdout | why |",
        "|---|---|---|---:|---|",
    ]
    for name, (mask, ycol) in families.items():
        v, why, parts, uncond, day_s, top5 = harden(
            df, mask, ycol, cost, splits, "open")
        holdp = parts.get("hold") or {}
        lines.append(
            f"| {name} | {ycol} | **{v}** | "
            f"{(holdp.get('mean') or 0)*100:+.2f}% (n={holdp.get('n')}) | "
            f"{', '.join(why) or '—'} |"
        )
    lines += [
        "",
        "CE must beat the book in **2025 and 2026**, both ticker halves, "
        "both SPY tapes, fees, no lottery. Light+O is this ColorEngine paint.",
        "",
        "Research only. No cards.",
        "",
    ]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}")
    print("".join(open(OUT).read()))


if __name__ == "__main__":
    main()
