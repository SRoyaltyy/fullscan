"""Upper rows → later rows: past cells forecast the next few days.

Only lag ≥ 1. Both directions. 1d / 2d / 3d / 1w from the open after
the feature is known. Compact per-letter arrays so the full dump fits.

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
from mine_full_grid import attach_spy, harden, liquid, s2d, split_masks  # noqa: E402
from mine_hi_corr import MIN_TICKERS, cost_of, load_mcap, load_split, tstat  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT_MD = ROOT / "research" / "UPPER_LOWER.md"
OUT_JSON = ROOT / "research" / "upper_lower.json"
SKIP_NUM = set("A IR P".split())

LABELS = (
    ("y_h1", "same-day leftover H (open→close)"),
    ("y_2d", "next ~2 sessions (buy open, sell close)"),
    ("y_3d", "next ~3 sessions (buy open, sell close)"),
    ("y_1w", "next ~1 week (buy open, sell close)"),
)


def load_ohlc(grids: Path, limit=0):
    files = sorted(p for p in grids.glob("*.json") if not p.name.startswith("_"))
    if limit:
        files = files[:limit]
    recs = []
    for i, p in enumerate(files, 1):
        raw = json.loads(p.read_text())
        t = raw["ticker"]
        prev_c = prev_v = None
        n = 0
        for d in raw["days"]:
            o, c = d.get("open"), d.get("close")
            if not isinstance(o, (int, float)) or not isinstance(c, (int, float)) or o == 0:
                continue
            recs.append({
                "ticker": t,
                "date": s2d(d["date"]),
                "open": np.float32(o),
                "close": np.float32(c),
                "volume": np.float32(d["volume"]) if isinstance(
                    d.get("volume"), (int, float)) else np.float32(0),
                "H": np.float32((c - o) / o),
                "dvol_l1": np.float32(prev_c * prev_v) if prev_c and prev_v else np.nan,
                "px_l1": np.float32(prev_c) if prev_c else np.nan,
            })
            prev_c = c
            prev_v = recs[-1]["volume"]
            n += 1
        if n < 30:
            del recs[-n:]
        if i % 800 == 0:
            print(f"  ohlc {i}/{len(files)}", flush=True)
    df = pd.DataFrame.from_records(recs)
    df = df.sort_values(["ticker", "date"]).drop_duplicates(
        ["ticker", "date"], keep="last").reset_index(drop=True)
    return df, files


def add_future(df):
    g = df.groupby("ticker", sort=False)
    df = df.copy()
    df["y_h1"] = df["H"]
    df["y_2d"] = g["close"].shift(-1) / df["open"] - 1
    df["y_3d"] = g["close"].shift(-2) / df["open"] - 1
    df["y_1w"] = g["close"].shift(-4) / df["open"] - 1
    return df


def fill_arrays(files, df):
    key = {(t, d): i for i, (t, d) in enumerate(zip(df["ticker"], df["date"]))}
    n = len(df)
    letters = []
    seen = {}
    values, fills = {}, {}
    text_raw = {}
    text_used = set()
    for i, p in enumerate(files, 1):
        raw = json.loads(p.read_text())
        t = raw["ticker"]
        for d in raw["days"]:
            idx = key.get((t, s2d(d["date"])))
            if idx is None:
                continue
            for let, cell in (d.get("cells") or {}).items():
                if let not in seen:
                    seen[let] = True
                    letters.append(let)
                    values[let] = np.full(n, np.nan, dtype=np.float32)
                    fills[let] = np.zeros(n, dtype=np.int8)
                v = cell.get("v")
                if isinstance(v, bool):
                    values[let][idx] = float(v)
                elif isinstance(v, (int, float)):
                    values[let][idx] = v
                elif isinstance(v, str) and v:
                    if let not in text_raw:
                        text_raw[let] = np.empty(n, dtype=object)
                    text_raw[let][idx] = v[:32]
                    text_used.add(let)
                f = cell.get("f")
                if f:
                    fam, _ = classify_fill(f)
                    if fam == "green":
                        fills[let][idx] = 1
                    elif fam == "red":
                        fills[let][idx] = 2
        if i % 800 == 0:
            print(f"  cells {i}/{len(files)} letters={len(letters)}", flush=True)
    tokens = {}
    for let in text_used:
        c = Counter(x for x in text_raw[let] if x)
        tokens[let] = [tok for tok, k in c.most_common(4) if tok and k >= 200]
    return letters, values, fills, text_raw, tokens


def lag_by_ticker(arr, ticker):
    s = pd.Series(arr)
    return s.groupby(ticker, sort=False).shift(1).to_numpy(), \
        s.groupby(ticker, sort=False).shift(2).to_numpy()


def letter_gates(let, l1, l2, f1, f2, t1, toks, disc):
    out = []
    if let not in SKIP_NUM:
        for suf, s, lag in (("_l1", l1, 1), ("_l2", l2, 2)):
            if s is None:
                continue
            s = np.asarray(s, dtype=float)
            out += [
                (f"{let}{suf}_eq1", s == 1, f"{let} = 1, {lag} day(s) ago"),
                (f"{let}{suf}_eq0", s == 0, f"{let} = 0, {lag} day(s) ago"),
                (f"{let}{suf}_ge1", s >= 1, f"{let} ≥ 1, {lag} day(s) ago"),
                (f"{let}{suf}_le-1", s <= -1, f"{let} ≤ −1, {lag} day(s) ago"),
                (f"{let}{suf}_gt0", s > 0, f"{let} > 0, {lag} day(s) ago"),
                (f"{let}{suf}_lt0", s < 0, f"{let} < 0, {lag} day(s) ago"),
            ]
            if lag == 1:
                use = disc & np.isfinite(s)
                if np.unique(s[np.isfinite(s)]).size >= 8 and use.sum() >= 80:
                    hi = np.nanquantile(s[use], 0.8)
                    lo = np.nanquantile(s[use], 0.2)
                    if np.isfinite(hi) and np.isfinite(lo) and hi > lo:
                        out.append((f"{let}_l1_qhi", s >= hi,
                                    f"{let} high (top fifth) yesterday"))
                        out.append((f"{let}_l1_qlo", s <= lo,
                                    f"{let} low (bottom fifth) yesterday"))
    if f1 is not None:
        out.append((f"{let}_l1_green", f1 == 1, f"{let} was green yesterday"))
        out.append((f"{let}_l1_red", f1 == 2, f"{let} was red yesterday"))
    if f2 is not None:
        out.append((f"{let}_l2_green", f2 == 1, f"{let} was green 2 days ago"))
        out.append((f"{let}_l2_red", f2 == 2, f"{let} was red 2 days ago"))
    if t1 is not None and toks:
        for tok in toks:
            out.append((f"{let}_l1_is_{tok[:12]}", t1 == tok,
                        f"{let} yesterday text is {tok[:20]}"))
    return out


def cheap_either_way(mask, y, tick, hold, min_n=300):
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


def add_past(df, letters):
    """Test helper: attach lag-1/2 columns for a tiny frame."""
    g = df.groupby("ticker", sort=False)
    extra = {}
    for let in letters:
        if f"{let}_v" in df.columns:
            extra[f"{let}_v_l1"] = g[f"{let}_v"].shift(1)
            extra[f"{let}_v_l2"] = g[f"{let}_v"].shift(2)
        if f"{let}_f" in df.columns:
            extra[f"{let}_f_l1"] = g[f"{let}_f"].shift(1)
    if extra:
        df = pd.concat([df, pd.DataFrame(extra, index=df.index)], axis=1)
    return df


def build_past_gates(df, inv, disc_mask):
    """Test helper: lag-only gates from a small frame with _l1 columns."""
    gates = {}
    for let in inv["letters"]:
        vk = f"{let}_v_l1"
        if vk in df.columns and let not in SKIP_NUM:
            s = df[vk]
            gates[f"{let}_l1_eq1"] = (s == 1).fillna(False).to_numpy()
            gates[f"{let}_l1_gt0"] = (s > 0).fillna(False).to_numpy()
        fk = f"{let}_f_l1"
        if fk in df.columns:
            gates[f"{let}_l1_green"] = (df[fk] == "green").fillna(False).to_numpy()
        if f"{let}_v" in df.columns:
            pass
    # wrap like the old tests expect tuples
    out = {}
    for k, m in gates.items():
        let = k.split("_")[0]
        out[k] = (m, let, k)
    return out


def write_report(meta, rows):
    keeps = [r for r in rows if r["verdict"] == "KEEP"]
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
    ]
    if uniq:
        lines.append(
            f"**KEEP {len(uniq)}** unique past→future links after the ship bar "
            f"(ticker holdout, both years when present, both SPY tapes, fees, "
            f"not a lottery / name ghost)."
        )
    else:
        lines.append("**KEEP 0** unique past→future links after the ship bar.")
    lines.append("")
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
        for r in chunk[:25]:
            lines.append(
                f"| {r['plain']} | {r['side']} | "
                f"{(r['hold_mean'] or 0)*100:+.2f}% | "
                f"{(r.get('uncond') or 0)*100:+.2f}% | {r['hold_n']} |"
            )
        lines.append("")
    lines += ["Research only. No cards. No live wire.", ""]
    OUT_MD.write_text("\n".join(lines))
    return uniq


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
    print(f"ohlc {grids}", flush=True)
    df, files = load_ohlc(grids, limit=args.limit)
    df = add_future(df)
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    print(f"  liquid {df['ticker'].nunique()} / {len(df)} "
          f"{df['date'].min()}→{df['date'].max()}", flush=True)
    print("cells…", flush=True)
    letters, values, fills, text_raw, tokens = fill_arrays(files, df)
    print(f"  letters={len(letters)} text={len(tokens)}", flush=True)
    splits = split_masks(df, discovery, holdout)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    tick = df["ticker"].to_numpy()
    hold = splits["hold"]
    disc = splits["disc"]
    n_gates = 0
    promoted = []
    for j, let in enumerate(letters, 1):
        l1, l2 = lag_by_ticker(values[let], df["ticker"])
        f1, f2 = lag_by_ticker(fills[let], df["ticker"])
        t1 = None
        if let in text_raw:
            t1, _ = lag_by_ticker(text_raw[let], df["ticker"])
        gates = letter_gates(let, l1, l2, f1, f2, t1, tokens.get(let), disc)
        n_gates += len(gates)
        for name, mask, plain in gates:
            mask = np.asarray(mask, dtype=bool)
            for ycol, _title in LABELS:
                y = df[ycol].to_numpy() - cost
                side = cheap_either_way(mask, y, tick, hold)
                if side:
                    promoted.append((name, ycol, mask.copy(), let, plain, side))
        if j % 40 == 0:
            print(f"  letter {j}/{len(letters)} promoted={len(promoted)}",
                  flush=True)
    print(f"  gates {n_gates} promoted {len(promoted)}", flush=True)
    zero = np.zeros(len(df))
    df["_y"] = 0.0
    rows = []
    for k, (name, ycol, mask, let, plain, side) in enumerate(promoted):
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
        if (k + 1) % 400 == 0:
            print(f"  hardened {k+1}/{len(promoted)}", flush=True)
    meta = {
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "n_gates": n_gates,
        "n_hardened": len(promoted),
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "n_y2025": int(splits["y2025"].sum()),
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
        "clock": "lag>=1 only",
        "sides": "long+short",
    }
    uniq = write_report(meta, rows)
    OUT_JSON.write_text(json.dumps({
        "meta": meta,
        "keeps": [r for r in rows if r["verdict"] == "KEEP"],
        "keeps_unique": uniq,
    }, indent=2, default=str))
    print(f"KEEP {meta['n_keep']} unique {len(uniq)} KILL {meta['n_kill']} → {OUT_MD}")


if __name__ == "__main__":
    main()
