"""Upper rows → later rows: past cells forecast the next few days.

Only lag ≥ 1. Both directions. 1d / 2d / 3d / 1w from the open after
the feature is known. Compact per-letter arrays so the full dump fits.

  python engine/mine_upper_lower.py --grids research/all_cols_grids
"""
from __future__ import annotations

import argparse
import gc
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
OUT_MD = ROOT / "research" / "UPPER_LOWER_RAW.md"
OUT_JSON = ROOT / "research" / "upper_lower.json"
SKIP_NUM = set("A IR P".split())
# Formula twins on daily rows — one sleeve, many letters.
LETTER_ALIAS = {
    "S": "DI", "DI": "DI",
    "V": "DK", "DK": "DK",
    "GH": "EC", "EC": "EC",
    "JD": "HO", "HO": "HO",
}
OP_ALIAS = {
    "eq1": "pos", "ge1": "pos", "gt0": "pos",
    "eq0": "zero",
    "lt0": "neg", "le-1": "neg1",
    "qhi": "qhi", "qlo": "qlo",
    "green": "green", "red": "red",
}

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


def cheap_positive(mask, y, tick, hold, min_n=300):
    """True when this signed PnL is fat and positive on holdout."""
    m = np.asarray(mask, dtype=bool) & hold & np.isfinite(y)
    yy = y[m]
    if yy.size < min_n:
        return False
    if np.unique(tick[m]).size < MIN_TICKERS:
        return False
    mu = float(yy.mean())
    t = tstat(yy)
    return bool(np.isfinite(t) and t >= 2 and mu > 0)


def cheap_either_way(mask, y, tick, hold, min_n=300):
    """Test helper: `y` is already the intended-side PnL (no extra fee)."""
    if cheap_positive(mask, y, tick, hold, min_n=min_n):
        return "long"
    if cheap_positive(mask, -y, tick, hold, min_n=min_n):
        return "short"
    return None


def signed_nets(y_label, cost):
    """Long and short both pay the fee: y−c and −y−c."""
    y_label = np.asarray(y_label, dtype=np.float64)
    cost = np.asarray(cost, dtype=np.float64)
    return y_label - cost, -y_label - cost


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


def op_family(gate: str) -> str:
    """CE_l1_eq1 → pos; O_l1_green → green; XX_l1_is_FOO → is_FOO."""
    tail = gate.split("_", 2)[-1] if "_" in gate else gate
    if tail.startswith("is_"):
        return tail
    return OP_ALIAS.get(tail, tail)


def letter_family(let: str) -> str:
    return LETTER_ALIAS.get(let, let)


def sleeve_key(r) -> tuple:
    """Same holdout n + mean + label + side = the same days, reprinted."""
    return (
        r["label"],
        r["side"],
        r.get("hold_n"),
        round(r.get("hold_mean") or 0, 4),
    )


def collapse_keeps(rows):
    """Drop eq1/ge1/gt0 copies and formula twins of one sleeve."""
    keeps = [r for r in rows if r["verdict"] == "KEEP"]
    by_sleeve = defaultdict(list)
    for r in keeps:
        by_sleeve[sleeve_key(r)].append(r)
    uniq = []
    for key, members in by_sleeve.items():
        members = sorted(members, key=lambda x: (x["letter"], x["gate"]))
        head = dict(members[0])
        letters = sorted({m["letter"] for m in members})
        head["twins"] = letters
        head["n_twins"] = len(members)
        uniq.append(head)
    uniq.sort(key=lambda x: -abs(x.get("hold_mean") or 0))
    return uniq


def _pct(x):
    if x is None:
        return "—"
    return f"{x*100:+.2f}%"


def write_report(meta, rows):
    uniq = collapse_keeps(rows)
    by_lab = defaultdict(list)
    for r in uniq:
        by_lab[r["label"]].append(r)
    n_keep = sum(1 for r in rows if r["verdict"] == "KEEP")
    n_kill = sum(1 for r in rows if r["verdict"] == "KILL")
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
        f"hardened **{meta['n_hardened']}** · raw KEEP **{n_keep}** "
        f"· unique sleeves **{len(uniq)}** · KILL **{n_kill}**.",
        "",
    ]
    if uniq:
        lines.append(
            f"**Yes — {len(uniq)} unique past→future sleeve(s)** cleared "
            f"the ship bar (ticker holdout, both years when present, both "
            f"SPY tapes, fees, not a lottery / name ghost). Raw KEEP "
            f"{n_keep} is not {n_keep} edges: `eq1`=`ge1`=`gt0` and "
            f"S/DI, V/DK, GH/EC, JD/HO are reprints."
        )
    else:
        lines.append(
            "**KEEP 0** unique past→future sleeves after the ship bar. "
            "Older cells still *describe* price (they are formulas on "
            "A–F). They did not forecast the next few days both years, "
            "both tapes, after fees, except reprints that died on harden."
        )
    lines.append("")
    for ycol, title in LABELS:
        chunk = by_lab.get(ycol, [])
        lines += [f"### {title}", ""]
        if not chunk:
            lines += ["None.", ""]
            continue
        lines += [
            "| when (older row) | side | holdout after fees | vs book | "
            "2025 | 2026 | n | twins |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
        for r in chunk[:30]:
            twins = ",".join(r.get("twins") or [r["letter"]])
            lines.append(
                f"| {r['plain']} | {r['side']} | "
                f"{_pct(r.get('hold_mean'))} | {_pct(r.get('uncond'))} | "
                f"{_pct(r.get('y2025_mean'))} | {_pct(r.get('y2026_mean'))} | "
                f"{r.get('hold_n')} | {twins} |"
            )
        if len(chunk) > 30:
            lines.append(f"| … | | | | | | | {len(chunk)-30} more |")
        lines.append("")
    near = [
        r for r in rows
        if r["verdict"] == "KILL"
        and (r.get("hold_t") or 0) >= 2
        and abs(r.get("hold_mean") or 0) >= 0.003
    ]
    if near:
        lines += [
            "### Near-misses (holdout looks real, harden said no)",
            "",
            "Usually a 2026 bounce that does not repeat in 2025, or one "
            "SPY tape only. First 20 unique sleeves.",
            "",
            "| when | side | label | holdout | why |",
            "|---|---|---|---:|---|",
        ]
        seen = set()
        shown = 0
        for r in sorted(near, key=lambda x: -abs(x.get("hold_mean") or 0)):
            k = sleeve_key(r)
            if k in seen:
                continue
            seen.add(k)
            why = ",".join(r.get("why") or [])[:80]
            lines.append(
                f"| {r['plain']} | {r['side']} | {r['label']} | "
                f"{_pct(r.get('hold_mean'))} | {why} |"
            )
            shown += 1
            if shown >= 20:
                break
        lines.append("")
    lines += ["Research only. No cards. No live wire.", ""]
    OUT_MD.write_text("\n".join(lines))
    return uniq


def _row_from_harden(name, ycol, let, plain, side, v, why, parts, uncond,
                     day_s, top5):
    holdp = parts.get("hold") or {}
    y25 = parts.get("y2025") or {}
    y26 = parts.get("y2026") or {}
    return {
        "gate": name, "letter": let, "plain": plain,
        "label": ycol, "side": side,
        "verdict": v, "why": why,
        "hold_mean": holdp.get("mean"),
        "hold_n": holdp.get("n"),
        "hold_t": holdp.get("t"),
        "hold_tickers": holdp.get("tickers"),
        "uncond": (uncond or {}).get("mean"),
        "y2025_mean": y25.get("mean"),
        "y2025_n": y25.get("n"),
        "y2026_mean": y26.get("mean"),
        "y2026_n": y26.get("n"),
        "day_share": day_s, "top5_share": top5,
    }


def dump_out(meta, rows):
    uniq = write_report(meta, rows)
    OUT_JSON.write_text(json.dumps({
        "meta": meta,
        "keeps": [r for r in rows if r["verdict"] == "KEEP"],
        "keeps_unique": uniq,
    }, indent=2, default=str))
    return uniq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="research/all_cols_grids")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--letters", default="",
                    help="comma letters to score (default: all)")
    args = ap.parse_args()
    grids = Path(args.grids)
    if not grids.is_absolute():
        grids = ROOT / grids
    want = {x.strip().upper() for x in args.letters.split(",") if x.strip()}
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
    if want:
        letters = [L for L in letters if L in want]
    print(f"  letters={len(letters)} text={len(tokens)}", flush=True)
    splits = split_masks(df, discovery, holdout)
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]], dtype=np.float32)
    tick = df["ticker"].to_numpy()
    hold = splits["hold"]
    disc = splits["disc"]
    y_raw = {ycol: df[ycol].to_numpy(dtype=np.float64) for ycol, _ in LABELS}
    y_long, y_short = {}, {}
    for ycol, _ in LABELS:
        y_long[ycol], y_short[ycol] = signed_nets(y_raw[ycol], cost)
    n_gates = 0
    n_promoted = 0
    rows = []
    zero = np.zeros(len(df), dtype=np.float64)
    df["_y"] = 0.0
    meta = {
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "n_gates": 0,
        "n_hardened": 0,
        "n_keep": 0,
        "n_kill": 0,
        "n_y2025": int(splits["y2025"].sum()),
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
        "clock": "lag>=1 only",
        "sides": "long+short",
        "partial": True,
    }
    for j, let in enumerate(letters, 1):
        l1, l2 = lag_by_ticker(values.pop(let), df["ticker"])
        f1, f2 = lag_by_ticker(fills.pop(let), df["ticker"])
        t1 = None
        if let in text_raw:
            t1, _ = lag_by_ticker(text_raw.pop(let), df["ticker"])
        gates = letter_gates(let, l1, l2, f1, f2, t1, tokens.get(let), disc)
        n_gates += len(gates)
        for name, mask, plain in gates:
            mask = np.asarray(mask, dtype=bool)
            for ycol, _title in LABELS:
                if cheap_positive(mask, y_long[ycol], tick, hold):
                    side, signed = "long", y_long[ycol]
                elif cheap_positive(mask, y_short[ycol], tick, hold):
                    side, signed = "short", y_short[ycol]
                else:
                    continue
                n_promoted += 1
                df["_y"] = signed
                v, why, parts, uncond, day_s, top5 = harden(
                    df, mask, "_y", zero, splits, "open")
                rows.append(_row_from_harden(
                    name, ycol, let, plain, side, v, why, parts, uncond,
                    day_s, top5))
        del l1, l2, f1, f2, t1, gates
        if j % 20 == 0 or j == len(letters):
            n_keep = sum(1 for r in rows if r["verdict"] == "KEEP")
            print(f"  letter {j}/{len(letters)} promoted={n_promoted} "
                  f"KEEP={n_keep}", flush=True)
            meta = {
                "n_tickers": int(df["ticker"].nunique()),
                "n_rows": int(len(df)),
                "n_gates": n_gates,
                "n_hardened": n_promoted,
                "n_keep": n_keep,
                "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
                "n_y2025": int(splits["y2025"].sum()),
                "date_min": str(df["date"].min()),
                "date_max": str(df["date"].max()),
                "clock": "lag>=1 only",
                "sides": "long+short",
                "partial": j < len(letters),
            }
            dump_out(meta, rows)
            gc.collect()
    meta["partial"] = False
    uniq = dump_out(meta, rows)
    print(f"KEEP {meta['n_keep']} unique {len(uniq)} KILL {meta['n_kill']} "
          f"→ {OUT_MD}")


if __name__ == "__main__":
    main()
