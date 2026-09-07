"""Upper rows → later rows: past cells forecast the next few days.

Only lag ≥ 1 (older Excel rows). Both directions. 1d / 2d / 3d / 1w
from the open after the feature is known. No same-row H/I. No same-row
fills — this is the “upper predicts lower” test, not the morning-paint
test.

  python engine/mine_upper_lower.py --grids research/all_cols_grids
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mine_full_grid import (  # noqa: E402
    SKIP_NUM, attach_spy, harden, liquid, s2d, split_masks,
)
from mine_hi_corr import (  # noqa: E402
    MIN_N, MIN_TICKERS, cost_of, load_mcap, load_split, tstat,
)
from signals import classify_fill  # noqa: E402

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT_MD = ROOT / "research" / "UPPER_LOWER.md"
OUT_JSON = ROOT / "research" / "upper_lower.json"

LABELS = (
    ("y_h1", "same-day leftover H (open→close)"),
    ("y_2d", "next ~2 sessions (buy open, sell close)"),
    ("y_3d", "next ~3 sessions (buy open, sell close)"),
    ("y_1w", "next ~1 week (buy open, sell close)"),
)


def lean_load(grids: Path, limit=0):
    files = sorted(p for p in grids.glob("*.json") if not p.name.startswith("_"))
    if limit:
        files = files[:limit]
    frames, chunk = [], []
    col_num, col_fill, col_text = Counter(), Counter(), Counter()
    text_tokens = defaultdict(Counter)
    letters = set()
    for i, p in enumerate(files, 1):
        raw = json.loads(p.read_text())
        t = raw["ticker"]
        prev_c = prev_v = None
        recs = []
        for d in raw["days"]:
            o, c = d.get("open"), d.get("close")
            if not isinstance(o, (int, float)) or not isinstance(c, (int, float)) or o == 0:
                continue
            cells = d.get("cells") or {}
            rec = {
                "ticker": t,
                "date": s2d(d["date"]),
                "open": np.float32(o),
                "close": np.float32(c),
                "volume": np.float32(d["volume"]) if isinstance(
                    d.get("volume"), (int, float)) else np.float32(0),
                "H": np.float32((c - o) / o),
                "dvol_l1": np.float32(prev_c * prev_v) if prev_c and prev_v else np.nan,
                "px_l1": np.float32(prev_c) if prev_c else np.nan,
            }
            for let, cell in cells.items():
                letters.add(let)
                v = cell.get("v")
                if isinstance(v, bool):
                    rec[f"{let}_v"] = np.float32(v)
                    col_num[let] += 1
                elif isinstance(v, (int, float)):
                    rec[f"{let}_v"] = np.float32(v)
                    col_num[let] += 1
                elif isinstance(v, str) and v:
                    rec[f"{let}_t"] = v[:32]
                    col_text[let] += 1
                    text_tokens[let][v[:32]] += 1
                f = cell.get("f")
                if f:
                    fam, _sc = classify_fill(f)
                    if fam in ("green", "red"):
                        rec[f"{let}_f"] = fam
                        col_fill[let] += 1
            recs.append(rec)
            prev_c, prev_v = c, float(rec["volume"])
        if len(recs) >= 30:
            chunk.append(pd.DataFrame.from_records(recs))
        if len(chunk) >= 200:
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
    inv = {
        "n_files": len(files),
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "letters": sorted(letters, key=lambda x: (len(x), x)),
        "numeric": sorted(col_num, key=lambda x: (len(x), x)),
        "fill": sorted(col_fill, key=lambda x: (len(x), x)),
        "text": sorted(col_text, key=lambda x: (len(x), x)),
        "text_tokens": {k: tok.most_common(5) for k, tok in text_tokens.items()},
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
    }
    return df, inv


def add_future(df):
    g = df.groupby("ticker", sort=False)
    df = df.copy()
    df["y_h1"] = df["H"]
    df["y_2d"] = g["close"].shift(-1) / df["open"] - 1
    df["y_3d"] = g["close"].shift(-2) / df["open"] - 1
    df["y_1w"] = g["close"].shift(-4) / df["open"] - 1
    return df


def add_past(df, letters):
    """Only upper-row (lag 1–2) copies. No same-row features."""
    g = df.groupby("ticker", sort=False)
    extra = {}
    for let in letters:
        if f"{let}_v" in df.columns:
            extra[f"{let}_v_l1"] = g[f"{let}_v"].shift(1)
            extra[f"{let}_v_l2"] = g[f"{let}_v"].shift(2)
        if f"{let}_f" in df.columns:
            extra[f"{let}_f_l1"] = g[f"{let}_f"].shift(1)
            extra[f"{let}_f_l2"] = g[f"{let}_f"].shift(2)
        if f"{let}_t" in df.columns:
            extra[f"{let}_t_l1"] = g[f"{let}_t"].shift(1)
    if extra:
        df = pd.concat([df, pd.DataFrame(extra, index=df.index)], axis=1)
    return df


def build_past_gates(df, inv, disc_mask):
    gates = {}
    for let in inv["letters"]:
        for lag, suf in ((1, "_l1"), (2, "_l2")):
            vk = f"{let}_v{suf}"
            if vk in df.columns and let not in SKIP_NUM:
                s = df[vk]
                for name, mask in (
                    (f"{let}{suf}_eq1", s == 1),
                    (f"{let}{suf}_eq0", s == 0),
                    (f"{let}{suf}_ge1", s >= 1),
                    (f"{let}{suf}_le-1", s <= -1),
                    (f"{let}{suf}_gt0", s > 0),
                    (f"{let}{suf}_lt0", s < 0),
                ):
                    gates[name] = (
                        mask.fillna(False).to_numpy(),
                        let,
                        f"{let} on the row {lag} day(s) above",
                    )
                if lag == 1 and disc_mask is not None and s.nunique(dropna=True) >= 8:
                    arr = s.to_numpy()
                    use = disc_mask & np.isfinite(arr)
                    if use.sum() >= 80:
                        hi = np.nanquantile(arr[use], 0.8)
                        lo = np.nanquantile(arr[use], 0.2)
                        if np.isfinite(hi) and np.isfinite(lo) and hi > lo:
                            gates[f"{let}{suf}_qhi"] = (
                                arr >= hi, let,
                                f"{let} high (top fifth) yesterday")
                            gates[f"{let}{suf}_qlo"] = (
                                arr <= lo, let,
                                f"{let} low (bottom fifth) yesterday")
            fk = f"{let}_f{suf}"
            if fk in df.columns:
                s = df[fk]
                gates[f"{let}{suf}_green"] = (
                    (s == "green").fillna(False).to_numpy(), let,
                    f"{let} was green {lag} day(s) ago")
                gates[f"{let}{suf}_red"] = (
                    (s == "red").fillna(False).to_numpy(), let,
                    f"{let} was red {lag} day(s) ago")
        tk = f"{let}_t_l1"
        if tk in df.columns:
            for tok, _n in (inv["text_tokens"].get(let) or [])[:4]:
                if not tok or tok in {"0", "None"}:
                    continue
                gates[f"{let}_l1_is_{tok[:14]}"] = (
                    (df[tk] == tok).fillna(False).to_numpy(), let,
                    f"{let} yesterday text is {tok[:20]}",
                )
    return gates


def cheap_either_way(mask, y, tick, hold, min_n=300):
    """Promote long or short if holdout |t|≥2 and mean has a sign."""
    m = np.asarray(mask, dtype=bool) & hold & np.isfinite(y)
    yy = y[m]
    if yy.size < min_n:
        return None
    if np.unique(tick[m]).size < MIN_TICKERS:
        return None
    mu = float(yy.mean())
    t = tstat(yy)
    if not np.isfinite(t) or abs(t) < 2:
        return None
    if mu > 0:
        return "long"
    if mu < 0:
        return "short"
    return None


def write_report(meta, rows):
    keeps = [r for r in rows if r["verdict"] == "KEEP"]
    # collapse eq1/ge1/gt0 twins
    seen = set()
    uniq = []
    for r in sorted(keeps, key=lambda x: -abs(x.get("hold_mean") or 0)):
        key = (r["letter"], r["label"], r["side"], r.get("hold_n"),
               round(r.get("hold_mean") or 0, 4))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    by_lab = defaultdict(list)
    for r in uniq:
        by_lab[r["label"]].append(r)
    lines = [
        "# Upper rows → later rows",
        "",
        f"_Generated {date.today().isoformat()}. Research only. "
        f"Live frozen. Past cells only (lag ≥ 1). Both directions._",
        "",
        "## What counts as a win",
        "",
        "A cell already printed on an **older row** that reliably says "
        "the next 1 / 2 / 3 / ~5 sessions are **generally up or generally "
        "down**. No peeking at the same row’s H or I. No same-row colors.",
        "",
        f"Dumps **{meta['n_tickers']}** names · **{meta['n_rows']}** liquid "
        f"days · **{meta['date_min']} → {meta['date_max']}** · 2025 days "
        f"**{meta['n_y2025']}** · gates **{meta['n_gates']}** · "
        f"hardened **{meta['n_hardened']}** · raw KEEP **{meta['n_keep']}** "
        f"· unique after twins **{len(uniq)}**.",
        "",
        f"**KEEP {len(uniq)}** unique past→future links after the ship bar "
        f"(ticker holdout, both years when present, both SPY tapes, fees, "
        f"not a lottery / name ghost)." if uniq else
        "**KEEP 0** unique past→future links after the ship bar.",
        "",
    ]
    for ycol, title in LABELS:
        chunk = by_lab.get(ycol, [])
        lines += [f"### {title}", ""]
        if not chunk:
            lines += ["None.", ""]
            continue
        lines += [
            "| when (older row) | side | holdout after fees | vs book | n |",
            "|---|---|---:|---:|---:|",
        ]
        for r in chunk[:20]:
            lines.append(
                f"| {r['plain']} | {r['side']} | "
                f"{r['hold_mean']*100:+.2f}% | "
                f"{(r.get('uncond') or 0)*100:+.2f}% | {r['hold_n']} |"
            )
        lines.append("")
    lines += [
        "Research only. No cards. No live wire.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="research/all_cols_grids")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    grids = Path(args.grids)
    if not grids.is_absolute():
        grids = ROOT / grids
    discovery, holdout = load_split()
    mcap = load_mcap()
    print(f"lean-load {grids}", flush=True)
    df, inv = lean_load(grids, limit=args.limit)
    print(f"  raw {inv['n_tickers']} / {inv['n_rows']} "
          f"{inv['date_min']}→{inv['date_max']} letters={len(inv['letters'])}",
          flush=True)
    df = add_future(df)
    df = add_past(df, inv["letters"])
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    print(f"  liquid {df['ticker'].nunique()} / {len(df)}", flush=True)
    splits = split_masks(df, discovery, holdout)
    gates = build_past_gates(df, inv, splits["disc"])
    print(f"  {len(gates)} past-only gates", flush=True)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    tick = df["ticker"].to_numpy()
    hold = splits["hold"]
    promoted = []
    for i, (name, (mask, let, plain)) in enumerate(gates.items(), 1):
        for ycol, _title in LABELS:
            y = df[ycol].to_numpy() - cost
            side = cheap_either_way(mask, y, tick, hold)
            if not side:
                continue
            promoted.append((name, ycol, mask, let, plain, side))
        if i % 1500 == 0:
            print(f"  screened {i}/{len(gates)} promoted={len(promoted)}",
                  flush=True)
    print(f"  promoted {len(promoted)}", flush=True)
    zero = np.zeros(len(df))
    rows = []
    df["_y"] = 0.0
    for j, (name, ycol, mask, let, plain, side) in enumerate(promoted):
        net = df[ycol].to_numpy() - cost
        df["_y"] = net if side == "long" else -net
        v, why, parts, uncond, day_s, top5 = harden(
            df, mask, "_y", zero, splits, "open")
        holdp = parts.get("hold") or {}
        rows.append({
            "gate": name, "letter": let, "plain": plain,
            "label": ycol, "side": side,
            "verdict": v, "why": why,
            "hold_mean": holdp.get("mean"),
            "hold_n": holdp.get("n"),
            "hold_t": holdp.get("t"),
            "hold_tickers": holdp.get("tickers"),
            "uncond": (uncond or {}).get("mean"),
            "day_share": day_s, "top5_share": top5,
        })
        if (j + 1) % 400 == 0:
            print(f"  hardened {j+1}/{len(promoted)}", flush=True)
    n2025 = int(splits["y2025"].sum())
    meta = {
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "n_gates": len(gates),
        "n_hardened": len(promoted),
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "n_y2025": n2025,
        "date_min": inv["date_min"],
        "date_max": inv["date_max"],
        "clock": "lag>=1 only",
        "sides": "long+short",
    }
    write_report(meta, rows)
    keeps = [r for r in rows if r["verdict"] == "KEEP"]
    OUT_JSON.write_text(json.dumps({
        "meta": meta,
        "keeps": keeps,
        "kills_sample": [r for r in rows if r["verdict"] == "KILL"][:80],
    }, indent=2, default=str))
    print(f"KEEP {meta['n_keep']} KILL {meta['n_kill']} → {OUT_MD}")


if __name__ == "__main__":
    main()
