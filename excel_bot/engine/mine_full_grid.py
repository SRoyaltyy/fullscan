"""Mine EVERY A–JO column (value, text, fill) → future H/I.

Whole workbook, not A–O and not a hand-picked leftover list.
Same-row H/I are labels only. Lag ≥ 1 of any letter is fair at the
open. Lag 0 only if the clock map says open. Close-lag0 features may
only predict later sessions.

Also scores the standing five-cell light + green O family, and every
letter AND-ed with that morning cluster — so a second link cannot hide
as “only works next to O.”

  python engine/mine_full_grid.py --grids research/all_cols_grids
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mine_hi_corr import (  # noqa: E402
    COST_HI, COST_LO, DAY_CAP, MIN_EDGE, MIN_N, MIN_TICKERS, TOP5_CAP,
    WIDE_CAP, Y2026, Q1, Q3, cost_of, load_mcap, load_split, lottery,
    spearman, tstat,
)
from signals import classify_fill  # noqa: E402

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT_MD = ROOT / "research" / "FULL_GRID_MINE.md"
OUT_JSON = ROOT / "research" / "full_grid_mine.json"
INV_MD = ROOT / "research" / "FULL_GRID_INVENTORY.md"

VALUE_OPEN = set(
    "A C J Q Z AC AH BT BV CG CH DC DE EB EK EN EP EQ ER ES ET EU EV "
    "FQ FR FS FU GD GE GF HF HG HW II IR IT IY IZ JB JC JD JE JF JL".split()
)
FILL_OPEN = set("A B C G J K L M O IR IS IT".split())
NEVER_L0 = set("H I")  # labels
SKIP_NUM = set("A IR P")  # date serials / TODAY
FIVE = ("A", "B", "C", "G", "J")
NINE = ("A", "B", "C", "G", "J", "K", "L", "M", "O")

# Same-day I is gap + leftover H — not an open label.
OPEN_LABELS = ("y_h1", "y_from_open_2d", "y_from_open_1w")
CLOSE_LABELS = ("y_i_stack_1d", "y_i_stack_1w", "y_h_lead_1d")

STANDING = (
    "light5", "light5_O", "light5_O_AH", "light5_O_FR",
    "light9", "light9_O",
)

COMBO_SUFFIXES = (
    "_eq1", "_ge1", "_le-1", "_gt0", "_lt0", "_green", "_red",
    "_qhi", "_qlo",
)


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def fam_score(rgb):
    if not rgb:
        return "none", 0.0
    return classify_fill(rgb)


def load_dumps(grids: Path, limit=0):
    files = sorted(p for p in grids.glob("*.json") if not p.name.startswith("_"))
    if limit:
        files = files[:limit]
    frames = []
    col_seen = Counter()
    col_num = Counter()
    col_fill = Counter()
    col_text = Counter()
    text_tokens = defaultdict(Counter)
    for p in files:
        raw = json.loads(p.read_text())
        t = raw["ticker"]
        prev_close = None
        prev_vol = None
        recs = []
        for d in raw["days"]:
            cells = d.get("cells") or {}
            for let, rec in cells.items():
                col_seen[let] += 1
                v = rec.get("v")
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    col_num[let] += 1
                elif isinstance(v, str) and v:
                    col_text[let] += 1
                    text_tokens[let][v[:40]] += 1
                if rec.get("f"):
                    col_fill[let] += 1
            o, c = d.get("open"), d.get("close")
            if not isinstance(o, (int, float)) or not isinstance(c, (int, float)):
                continue
            if o == 0:
                continue
            H = (c - o) / o
            I = (c - prev_close) / prev_close if prev_close else None
            rec = {
                "ticker": t,
                "date": s2d(d["date"]),
                "open": np.float32(o), "close": np.float32(c),
                "volume": np.float32(d["volume"]) if isinstance(
                    d.get("volume"), (int, float)) else np.float32(0),
                "H": np.float32(H),
                "I": np.float32(I) if I is not None else np.nan,
                "dvol_l1": (np.float32(prev_close * prev_vol)
                            if prev_close and prev_vol else np.nan),
                "px_l1": np.float32(prev_close) if prev_close else np.nan,
            }
            for let, cell in cells.items():
                v = cell.get("v")
                if isinstance(v, bool):
                    rec[f"{let}_v"] = np.float32(v)
                elif isinstance(v, (int, float)):
                    rec[f"{let}_v"] = np.float32(v)
                elif isinstance(v, str) and v:
                    rec[f"{let}_t"] = v[:40]
                family, sc = fam_score(cell.get("f"))
                if sc:
                    rec[f"{let}_fs"] = np.float32(sc)
                if family in ("green", "red"):
                    rec[f"{let}_f"] = family
            recs.append(rec)
            prev_close, prev_vol = c, float(rec["volume"])
        if len(recs) < 30:
            continue
        frames.append(pd.DataFrame.from_records(recs))
    if not frames:
        raise SystemExit(f"no dumps in {grids}")
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["ticker", "date"]).drop_duplicates(
        ["ticker", "date"], keep="last").reset_index(drop=True)
    inv = {
        "n_files": len(files),
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "letters": sorted(col_seen, key=lambda x: (len(x), x)),
        "n_letters": len(col_seen),
        "numeric": sorted(col_num, key=lambda x: (len(x), x)),
        "fill": sorted(col_fill, key=lambda x: (len(x), x)),
        "text": sorted(col_text, key=lambda x: (len(x), x)),
        "text_tokens": {k: tok.most_common(8) for k, tok in text_tokens.items()},
        "counts": {k: {"seen": col_seen[k], "num": col_num[k],
                       "fill": col_fill[k], "text": col_text[k]}
                   for k in col_seen},
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
    }
    return df, inv


def add_labels(df):
    g = df.groupby("ticker", sort=False)
    df = df.copy()
    df["y_h1"] = df["H"]
    df["y_h_lead_1d"] = g["H"].shift(-1)
    df["y_from_open_2d"] = g["close"].shift(-1) / df["open"] - 1
    df["y_from_open_1w"] = g["close"].shift(-4) / df["open"] - 1
    df["y_i_stack_1d"] = g["close"].shift(-1) / df["close"] - 1
    df["y_i_stack_1w"] = g["close"].shift(-5) / df["close"] - 1
    return df


def add_lags(df, letters):
    g = df.groupby("ticker", sort=False)
    extra = {}
    for let in letters:
        vk, fk, tk = f"{let}_v", f"{let}_f", f"{let}_t"
        if vk in df.columns:
            extra[f"{let}_v_l1"] = g[vk].shift(1)
            extra[f"{let}_v_l2"] = g[vk].shift(2)
        if fk in df.columns:
            extra[f"{let}_f_l1"] = g[fk].shift(1)
        if tk in df.columns:
            extra[f"{let}_t_l1"] = g[tk].shift(1)
    if extra:
        df = pd.concat([df, pd.DataFrame(extra, index=df.index)], axis=1)
    return df


def attach_spy(df):
    spy_path = ROOT / "data" / "rows" / "SPY.json"
    if not spy_path.exists():
        df["spy_I"] = np.nan
        df["spy_I_l1"] = np.nan
        return df
    spy = pd.DataFrame(json.loads(spy_path.read_text()))
    spy["date"] = pd.to_datetime(spy["date"]).dt.date
    spy = spy.sort_values("date")
    spy["spy_I"] = spy["close"].pct_change()
    spy["spy_I_l1"] = spy["spy_I"].shift(1)
    return df.merge(spy[["date", "spy_I", "spy_I_l1"]], on="date", how="left")


def liquid(df):
    return (
        (df["px_l1"] >= 2)
        & (df["dvol_l1"] >= 1_000_000)
        & np.isfinite(df["H"])
    )


def split_masks(df, discovery, holdout):
    disc = df["ticker"].isin(discovery)
    hold = df["ticker"].isin(holdout)
    disc = disc | ~(disc | hold)
    return {
        "disc": disc.to_numpy(), "hold": hold.to_numpy(),
        "early": (df["date"] < Q3).to_numpy(),
        "late": (df["date"] >= Q3).to_numpy(),
        "q1": (df["date"] < Q1).to_numpy(),
        "y2025": (df["date"] < Y2026).to_numpy(),
        "y2026": (df["date"] >= Y2026).to_numpy(),
        "spy_up": (df["spy_I_l1"] > 0.0015).to_numpy(),
        "spy_dn": (df["spy_I_l1"] < -0.0015).to_numpy(),
        "spy_up_today": (df["spy_I"] > 0.0015).to_numpy(),
        "spy_dn_today": (df["spy_I"] < -0.0015).to_numpy(),
    }


def is_open_atom(let, lag, kind):
    """kind in {v, f, t}."""
    if lag >= 1:
        return True
    if let in NEVER_L0:
        return False
    if kind == "f":
        return let in FILL_OPEN
    return let in VALUE_OPEN


def hyst_on(df, letters, enter=5.0, off=2.0):
    """Per-ticker hysteresis on the sum of fill scores."""
    score = np.zeros(len(df), dtype=np.float32)
    for let in letters:
        col = f"{let}_fs"
        if col in df.columns:
            score += df[col].fillna(0).to_numpy(dtype=np.float32)
    out = np.zeros(len(df), dtype=bool)
    for _, idx in df.groupby("ticker", sort=False).indices.items():
        on = False
        sl = score[idx]
        flags = np.empty(len(idx), dtype=bool)
        for i, v in enumerate(sl):
            if (not on) and v >= enter:
                on = True
            elif on and v <= off:
                on = False
            flags[i] = on
        out[idx] = flags
    return out


def _add_gate(gates, name, mask, clock, let, plain):
    if isinstance(mask, pd.Series):
        mask = mask.fillna(False).to_numpy()
    else:
        mask = np.asarray(mask, dtype=bool)
    gates[name] = (mask, clock, let, plain)


def build_gates(df, inv, disc_mask=None):
    """Every letter → binary series + clock tag."""
    gates = {}
    letters = inv["letters"]
    n = len(df)
    for let in letters:
        for lag, suffix in ((0, ""), (1, "_l1"), (2, "_l2")):
            vk = f"{let}_v{suffix}"
            if vk in df.columns and let not in SKIP_NUM:
                if lag == 0 and let in NEVER_L0:
                    continue
                s = df[vk]
                clock = "open" if is_open_atom(let, lag, "v") else "close"
                for name, mask in (
                    (f"{let}{suffix}_eq1", s == 1),
                    (f"{let}{suffix}_eq0", s == 0),
                    (f"{let}{suffix}_le-1", s <= -1),
                    (f"{let}{suffix}_ge1", s >= 1),
                    (f"{let}{suffix}_gt0", s > 0),
                    (f"{let}{suffix}_lt0", s < 0),
                    (f"{let}{suffix}_ge2", s >= 2),
                    (f"{let}{suffix}_le-2", s <= -2),
                ):
                    _add_gate(gates, name, mask, clock, let,
                              f"{let} value{suffix}")
                if disc_mask is not None and s.nunique(dropna=True) >= 8:
                    base = s.to_numpy()
                    use = disc_mask & np.isfinite(base)
                    if use.sum() >= 80:
                        hi = np.nanquantile(base[use], 0.8)
                        lo = np.nanquantile(base[use], 0.2)
                        if np.isfinite(hi) and np.isfinite(lo) and hi > lo:
                            _add_gate(
                                gates, f"{let}{suffix}_qhi", base >= hi,
                                clock, let, f"{let} top-quintile{suffix}")
                            _add_gate(
                                gates, f"{let}{suffix}_qlo", base <= lo,
                                clock, let, f"{let} bottom-quintile{suffix}")
            if lag == 2:
                continue
            fk = f"{let}_f{suffix}"
            if fk in df.columns:
                if lag == 0 and let in NEVER_L0:
                    continue
                clock = "open" if is_open_atom(let, lag, "f") else "close"
                s = df[fk]
                _add_gate(gates, f"{let}{suffix}_green", s == "green",
                          clock, let, f"{let} highlight green{suffix}")
                _add_gate(gates, f"{let}{suffix}_red", s == "red",
                          clock, let, f"{let} highlight red{suffix}")
        tk = f"{let}_t"
        if tk in df.columns:
            if let in NEVER_L0:
                continue
            tokens = inv["text_tokens"].get(let) or []
            for tok, _n in tokens[:5]:
                if not tok or tok in {"0", "None"}:
                    continue
                clock = "open" if is_open_atom(let, 0, "t") else "close"
                _add_gate(gates, f"{let}_is_{tok[:16]}", df[tk] == tok,
                          clock, let, f"{let} text is {tok}")
                if f"{let}_t_l1" in df.columns:
                    _add_gate(gates, f"{let}_l1_is_{tok[:16]}",
                              df[f"{let}_t_l1"] == tok, "open", let,
                              f"{let} yesterday text is {tok}")
    # Standing morning family (fill scores, enter 5 / off 2).
    light5 = hyst_on(df, FIVE, 5.0, 2.0)
    light9 = hyst_on(df, NINE, 5.0, 2.0)
    o_green = (df["O_f"] == "green").fillna(False).to_numpy() if "O_f" in df.columns \
        else np.zeros(n, dtype=bool)
    ah = (df["AH_v"] >= 1).fillna(False).to_numpy() if "AH_v" in df.columns \
        else np.zeros(n, dtype=bool)
    fr = (df["FR_v"] >= 1).fillna(False).to_numpy() if "FR_v" in df.columns \
        else np.zeros(n, dtype=bool)
    _add_gate(gates, "light5", light5, "open", "ABC GJ",
              "five morning cells A,B,C,G,J stay in a green-light streak")
    _add_gate(gates, "light5_O", light5 & o_green, "open", "O",
              "five-cell morning light + green O")
    _add_gate(gates, "light5_O_AH", light5 & o_green & ah, "open", "AH",
              "five-cell morning light + green O + AH≥1")
    _add_gate(gates, "light5_O_FR", light5 & o_green & fr, "open", "FR",
              "five-cell morning light + green O + FR≥1")
    _add_gate(gates, "light9", light9, "open", "ABCGJKLMO",
              "nine morning cells A,B,C,G,J,K,L,M,O stay in a green-light streak")
    _add_gate(gates, "light9_O", light9 & o_green, "open", "O",
              "nine-cell morning light + green O")
    return gates


def combo_specs(gates, n):
    """Lazy combo list: (name, left, right, clock, letter, plain).

    Masks are AND-ed only when screened so we do not hold thousands of
    extra 500k-row bool arrays in RAM.
    """
    specs = []
    partners = [p for p in ("O_green", "light5", "light5_O") if p in gates]
    for gname, (mask, clock, let, plain) in gates.items():
        if gname in STANDING or gname in {"O_green", "light5", "light5_O"}:
            continue
        if not any(gname.endswith(suf) for suf in COMBO_SUFFIXES):
            continue
        for pname in partners:
            pclock = gates[pname][1]
            pplain = gates[pname][3]
            cclock = "open" if clock == "open" and pclock == "open" else "close"
            specs.append((
                f"{gname}__and_{pname}", gname, pname, cclock, let,
                f"{plain} AND {pplain}",
            ))
    opens = [c for c in "ABCGJKLMO" if f"{c}_green" in gates]
    for i, a in enumerate(opens):
        for b in opens[i + 1:]:
            specs.append((
                f"{a}_green__{b}_green", f"{a}_green", f"{b}_green",
                "open", a,
                f"{gates[f'{a}_green'][3]} AND {gates[f'{b}_green'][3]}",
            ))
    binary = []
    for gname, (mask, clock, let, plain) in gates.items():
        if gname in STANDING:
            continue
        if not (gname.endswith("_eq1") or gname.endswith("_green")):
            continue
        rate = float(mask.mean()) if n else 0.0
        if 0.03 <= rate <= 0.40:
            binary.append(gname)
    binary = binary[:80]
    for i, a in enumerate(binary):
        al, ap = gates[a][2], gates[a][3]
        ac = gates[a][1]
        for b in binary[i + 1:]:
            if gates[b][2] == al:
                continue
            bc, bp = gates[b][1], gates[b][3]
            specs.append((
                f"{a}__{b}", a, b,
                "open" if ac == "open" and bc == "open" else "close",
                al, f"{ap} AND {bp}",
            ))
    return specs


def combo_mask(gates, left, right):
    return gates[left][0] & gates[right][0]


def cheap_screen(mask, y, tick, hold, min_n=300):
    m = np.asarray(mask, dtype=bool) & hold & np.isfinite(y)
    yy = y[m]
    if yy.size < min_n:
        return None
    nt = np.unique(tick[m]).size
    if nt < MIN_TICKERS:
        return None
    mu = float(yy.mean())
    t = tstat(yy)
    if (not np.isfinite(t)) or t < 2 or mu <= 0:
        return None
    return {"n": int(yy.size), "mean": mu, "t": t, "tickers": int(nt)}


def harden(df, mask, ycol, cost, splits, clock):
    y = df[ycol].to_numpy() - cost
    tick = df["ticker"].to_numpy()
    dt = df["date"].to_numpy()
    base = np.asarray(mask, dtype=bool) & np.isfinite(y)
    parts = {}
    reasons = []
    for name, sm in (
        ("disc", splits["disc"]), ("hold", splits["hold"]),
        ("early", splits["early"]), ("late", splits["late"]),
        ("q1", splits["q1"]), ("y2025", splits["y2025"]), ("y2026", splits["y2026"]),
        ("spy_up", splits["spy_up"] if clock == "open" else splits["spy_up_today"]),
        ("spy_dn", splits["spy_dn"] if clock == "open" else splits["spy_dn_today"]),
    ):
        yy = y[base & sm]
        if yy.size < 20:
            parts[name] = None
            continue
        parts[name] = {
            "n": int(yy.size), "mean": float(yy.mean()), "t": tstat(yy),
            "tickers": int(np.unique(tick[base & sm]).size),
        }
    hold, disc = parts.get("hold"), parts.get("disc")
    uncond_y = y[np.isfinite(y) & splits["hold"]]
    uncond = {"n": int(uncond_y.size), "mean": float(np.nanmean(uncond_y))} if uncond_y.size else None
    if hold is None or disc is None:
        return "THIN", ["thin"], parts, uncond, 1, 1
    if hold["tickers"] < MIN_TICKERS or hold["n"] < MIN_N:
        return "THIN", ["thin"], parts, uncond, 1, 1
    if not (np.isfinite(disc["t"]) and disc["t"] >= 2):
        reasons.append("disc_t")
    if not (np.isfinite(hold["t"]) and hold["t"] >= 2):
        reasons.append("hold_t")
    if disc["mean"] <= 0:
        reasons.append("disc_sign")
    if hold["mean"] <= 0:
        reasons.append("hold_sign")
    su, sd = parts.get("spy_up"), parts.get("spy_dn")
    if su is None or sd is None or su["mean"] <= 0 or sd["mean"] <= 0:
        reasons.append("tape")
    if parts.get("early") and parts["early"]["mean"] <= 0:
        reasons.append("early")
    if parts.get("late") and parts["late"]["mean"] <= 0:
        reasons.append("late")
    if parts.get("q1") and parts["q1"]["mean"] <= 0:
        reasons.append("q1")
    if parts.get("y2025") and parts["y2025"]["mean"] <= 0:
        reasons.append("y2025")
    if parts.get("y2026") and parts["y2026"]["mean"] <= 0:
        reasons.append("y2026")
    if uncond and hold["mean"] - uncond["mean"] < MIN_EDGE:
        reasons.append("no_edge")
    if uncond and parts.get("y2026") and parts["y2026"]["mean"] < uncond["mean"]:
        reasons.append("y2026_no_edge")
    if uncond and parts.get("y2025") and parts["y2025"]["mean"] < uncond["mean"]:
        reasons.append("y2025_no_edge")
    if uncond and uncond["n"] and hold["n"] / uncond["n"] > WIDE_CAP:
        reasons.append("too_wide")
    day_s, top5 = lottery(y, tick, dt, base & splits["hold"])
    if day_s > DAY_CAP:
        reasons.append("lottery_day")
    if top5 > TOP5_CAP:
        reasons.append("ticker_ghost")
    verdict = "KEEP" if not reasons else "KILL"
    return verdict, reasons, parts, uncond, day_s, top5


def fmt_pct(x):
    if x is None or not (isinstance(x, (int, float)) and np.isfinite(x)):
        return "—"
    return f"{x*100:+.2f}%"


def write_inventory(inv):
    lines = [
        "# Full workbook inventory (A–JO dumps)",
        "",
        f"_Generated {date.today().isoformat()}. Research only._",
        "",
        f"Tickers **{inv['n_tickers']}**. Name-days **{inv['n_rows']}**. "
        f"Letters present **{inv['n_letters']}** / 275 "
        f"(formulas through JL; grid is A–JO).",
        "",
        f"Date window **{inv.get('date_min', '?')} → {inv.get('date_max', '?')}**.",
        "",
        f"- Numeric in at least one cell: **{len(inv['numeric'])}** "
        f"({', '.join(inv['numeric'][:40])}{'…' if len(inv['numeric'])>40 else ''})",
        f"- Fill/highlight in at least one cell: **{len(inv['fill'])}**",
        f"- Text in at least one cell: **{len(inv['text'])}** "
        f"({', '.join(inv['text']) or '—'})",
        "",
        "Every letter below is mined (value thresholds, quintiles, green/red "
        "fill, lags 1–2, frequent text tokens) and AND-ed with the morning "
        "light / green O cluster. H and I same-row are labels only.",
        "",
        "| col | seen | numeric | fill | text |",
        "|---|---:|---:|---:|---:|",
    ]
    for let in inv["letters"]:
        c = inv["counts"][let]
        lines.append(f"| {let} | {c['seen']} | {c['num']} | {c['fill']} | {c['text']} |")
    INV_MD.write_text("\n".join(lines) + "\n")


LABEL_PLAIN = {
    "y_h1": "same-day H (open→close)",
    "y_from_open_2d": "buy open, sell next close",
    "y_from_open_1w": "buy open, sell close ~1 week later",
    "y_i_stack_1d": "next session’s I (tomorrow’s daily %)",
    "y_i_stack_1w": "compound close-to-close over the next week",
    "y_h_lead_1d": "next session’s H",
}


def _is_standing(r):
    return r["gate"] in STANDING or r["gate"].startswith("light5") or r["gate"].startswith("light9")


def write_report(meta, rows, inv, spears):
    keeps = [r for r in rows if r["verdict"] == "KEEP"]
    kills = [r for r in rows if r["verdict"] == "KILL"]
    new_keeps = [r for r in keeps if not _is_standing(r)]
    standing_rows = [r for r in rows if r["gate"] in STANDING]
    window = f"{meta.get('date_min', '?')} → {meta.get('date_max', '?')}"
    n2025 = meta.get("n_y2025", 0)
    lines = [
        "# Full workbook (A–JO) → H/I mine",
        "",
        f"_Generated {date.today().isoformat()}. Research only. "
        f"Live `flatten_robust` frozen. Whole sheet, not A–O. "
        f"Not #144’s hand-picked leftover list._",
        "",
        "## Verdict",
        "",
    ]
    if new_keeps:
        lines.append(
            f"**KEEP {len(new_keeps)} new** gates on the full A–JO dump "
            f"survived the ship bar besides the standing light+O family. "
            f"Listed in plain English first."
        )
    else:
        lines.append(
            f"**No new KEEP** besides the standing morning family. "
            f"Hardened **{len(kills)}** promoted gate×label cells on "
            f"{meta['n_tickers']} tickers, {meta['n_rows']} liquid days, "
            f"{inv['n_letters']} letters present ({window}). "
            "Every letter the engine painted was screened (values, "
            "quintiles, green/red fills, lags, frequent text) and AND-ed "
            "with green O / the five-cell light. "
            "Nothing new cleared ticker-holdout + both available years vs "
            "the book + both SPY tapes + fees + no lottery."
        )
    lines += [
        "",
        f"Dumps: **{meta['n_tickers']}** names · letters **{inv['n_letters']}** "
        f"· numeric **{len(inv['numeric'])}** · fills **{len(inv['fill'])}** "
        f"· text **{len(inv['text'])}** · single-letter gates "
        f"**{meta['n_base_gates']}** · with combos **{meta['n_gates']}** "
        f"· hardened **{meta['n_hardened']}**.",
        "",
        f"Date window **{window}**. 2025 name-days **{n2025}**. "
        + ("Year-split is live." if n2025 >= 400
           else "Latest tile is mostly 2026 — y2025 harden is skipped when n<20, "
                "not treated as a pass."),
        "",
        "### Standing family (must rediscover, not a new find)",
        "",
        "| recipe | predicts | holdout | verdict | why |",
        "|---|---|---:|---|---|",
    ]
    if standing_rows:
        for r in standing_rows:
            lines.append(
                f"| {r['plain']} | {r['label_plain']} | "
                f"{fmt_pct(r['hold_mean'])} (n={r.get('hold_n') or '—'}) | "
                f"**{r['verdict']}** | {', '.join(r['why'][:5]) or '—'} |"
            )
    else:
        lines.append("| — | — | — | — | not on this dump |")
    lines += [
        "",
        "### What held (new, not light+O)",
        "",
    ]
    if new_keeps:
        for r in new_keeps:
            lines.append(
                f"- When **{r['plain']}** ({r['clock']} clock), "
                f"**{r['label_plain']}** averaged **{fmt_pct(r['hold_mean'])}** "
                f"after fees (n={r['hold_n']}, {r['hold_tickers']} names, "
                f"t={r['hold_t']:.2f}) vs everyone {fmt_pct(r['uncond'])}."
            )
    else:
        lines.append("No new full-sheet gate.")
    lines += [
        "",
        "### Near-misses (holdout t≥2, killed on harden, not standing)",
        "",
        "| when | predicts | holdout | why |",
        "|---|---|---:|---|",
    ]
    near = [r for r in kills if (r.get("hold_t") or 0) >= 2
            and (r.get("hold_mean") or 0) > 0.003
            and not _is_standing(r)]
    near.sort(key=lambda r: -(r["hold_mean"] or 0))
    for r in near[:30]:
        lines.append(
            f"| {r['plain']} | {r['label_plain']} | "
            f"{fmt_pct(r['hold_mean'])} (n={r['hold_n']}) | "
            f"{', '.join(r['why'][:5])} |"
        )
    if not near:
        lines.append("| — | — | — | — |")
    lines += [
        "",
        "### Strongest holdout Spearman (column number, lag-1 = always fair)",
        "",
        "| col | lag | predicts | ρ holdout | n |",
        "|---|---|---|---:|---:|",
    ]
    for r in spears[:20]:
        lines.append(
            f"| {r['col']} | {r['lag']} | {r['label']} | "
            f"{r['rho']:+.3f} | {r['n']} |"
        )
    if not spears:
        lines.append("| — | — | — | — | — |")
    lines += [
        "",
        "## Clock",
        "",
        "Same-row H/I never features. Lag ≥ 1 of any letter is open-fair. "
        "Lag 0 values only for the locked 44 `value_mine_open` letters. "
        "Lag 0 fills only for timing-tested A,B,C,G,J,K,L,M,O (+IR/IS/IT). "
        "Unknown → close, and close features only predict **later** H/I "
        "(not the same-day leftover).",
        "",
        "A–F seeded from Yahoo/rows. Inventory: `FULL_GRID_INVENTORY.md`.",
        "",
        "Research only. No cards. No live wire.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines))


def pack_row(name, ycol, clock, let, plain, v, why, parts, uncond, day_s, top5):
    holdp = parts.get("hold") or {}
    return {
        "gate": name, "letter": let, "clock": clock, "plain": plain,
        "label": ycol, "label_plain": LABEL_PLAIN.get(ycol, ycol),
        "verdict": v, "why": why,
        "hold_mean": holdp.get("mean"), "hold_n": holdp.get("n"),
        "hold_t": holdp.get("t"), "hold_tickers": holdp.get("tickers"),
        "uncond": (uncond or {}).get("mean"),
        "day_share": day_s, "top5_share": top5,
    }


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
    print(f"loading dumps {grids}", flush=True)
    df, inv = load_dumps(grids, limit=args.limit)
    print(f"  raw {inv['n_tickers']} / {inv['n_rows']} letters={inv['n_letters']} "
          f"{inv.get('date_min')}→{inv.get('date_max')}", flush=True)
    write_inventory(inv)
    df = add_labels(df)
    df = add_lags(df, inv["letters"])
    df = attach_spy(df)
    df = df.loc[liquid(df)].reset_index(drop=True)
    print(f"  liquid {df['ticker'].nunique()} / {len(df)}", flush=True)
    splits = split_masks(df, discovery, holdout)
    print("building gates…", flush=True)
    gates = build_gates(df, inv, disc_mask=splits["disc"])
    specs = combo_specs(gates, len(df))
    n_base = len(gates)
    print(f"  {n_base} single-letter + standing · {len(specs)} combo specs",
          flush=True)

    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    tick = df["ticker"].to_numpy()
    hold = splits["hold"]
    promoted = []
    standing_jobs = []
    for name, (mask, clock, let, plain) in gates.items():
        labels = OPEN_LABELS if clock == "open" else CLOSE_LABELS
        for ycol in labels:
            if name in STANDING:
                standing_jobs.append((name, ycol, mask, clock, let, plain, None))
                continue
            y = df[ycol].to_numpy()
            scr = cheap_screen(mask, y - cost, tick, hold)
            if scr:
                promoted.append((name, ycol, mask, clock, let, plain, scr))
    for name, left, right, clock, let, plain in specs:
        mask = combo_mask(gates, left, right)
        labels = OPEN_LABELS if clock == "open" else CLOSE_LABELS
        for ycol in labels:
            y = df[ycol].to_numpy()
            scr = cheap_screen(mask, y - cost, tick, hold)
            if scr:
                promoted.append((name, ycol, mask.copy(), clock, let, plain, scr))
    print(f"  promoted {len(promoted)} + standing {len(standing_jobs)}",
          flush=True)

    rows = []
    for job in standing_jobs + promoted:
        name, ycol, mask, clock, let, plain, _scr = job
        v, why, parts, uncond, day_s, top5 = harden(
            df, mask, ycol, cost, splits, clock)
        rows.append(pack_row(name, ycol, clock, let, plain,
                             v, why, parts, uncond, day_s, top5))

    spears = []
    holdm = splits["hold"]
    for let in inv["numeric"]:
        if let in SKIP_NUM or let in NEVER_L0:
            continue
        col = f"{let}_v_l1"
        if col not in df.columns:
            continue
        x = df[col].to_numpy()
        for ycol in ("y_h1", "y_i_stack_1d", "y_i_stack_1w"):
            rho, n = spearman(x[holdm], df[ycol].to_numpy()[holdm])
            if np.isfinite(rho) and n >= MIN_N:
                spears.append({"col": let, "lag": 1, "label": ycol,
                               "rho": rho, "n": n})
    spears.sort(key=lambda r: -abs(r["rho"]))

    n2025 = int(splits["y2025"].sum())
    meta = {
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "n_base_gates": n_base,
        "n_gates": n_base + len(specs),
        "n_hardened": len(standing_jobs) + len(promoted),
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "n_thin": sum(1 for r in rows if r["verdict"] == "THIN"),
        "n_keep_new": sum(1 for r in rows
                          if r["verdict"] == "KEEP" and not _is_standing(r)),
        "surface": "A-JO",
        "seed": "yahoo_rows_cache",
        "date_min": inv.get("date_min"),
        "date_max": inv.get("date_max"),
        "n_y2025": n2025,
    }
    write_report(meta, rows, inv, spears)
    OUT_JSON.write_text(json.dumps({
        "meta": meta, "inventory": {
            "n_letters": inv["n_letters"], "numeric": inv["numeric"],
            "fill": inv["fill"], "text": inv["text"],
            "date_min": inv.get("date_min"), "date_max": inv.get("date_max"),
        },
        "keeps": [r for r in rows if r["verdict"] == "KEEP"],
        "standing": [r for r in rows if r["gate"] in STANDING],
        "kills": rows,
        "spearman": spears[:60],
    }, indent=2, default=str))
    print(f"KEEP {meta['n_keep']} (new {meta['n_keep_new']}) "
          f"KILL {meta['n_kill']} → {OUT_MD}")


if __name__ == "__main__":
    main()
