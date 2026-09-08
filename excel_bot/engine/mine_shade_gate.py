"""M mid #95CA82 fill × Excel-locked open values / lags.

Parent feature is column M's FILL hex #95CA82 only (open-knowable).
M's number is close-only and never a same-row feature.

Gate — OPEN_SAME_ROW_LABELS.md + CLOCK_MAP.md, abort if drifted:
  shades/fills at open: A B C G J K L M O IR IS IT
  numbers/text at open: the 44 value_mine_open cols (never same-row H/I)
  lags: any letter from rows above is fair
  OUT: M/B/G/K/O numbers, D/E/F/H/I same-row, core_score

Computable 44-col stand-ins on the M-only expand (OHLCV + prior H/I):
  J, AH, ER, FQ, JB, JC. Other 44 letters need a full-sheet dump.

  python3 engine/mine_shade_gate.py
  python3 engine/mine_shade_gate.py --render-only

Live flatten_robust frozen. No cards under strategies/. No live push.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import COST_FUTU_LONG, annotate_days  # noqa: E402
from excel_clock_gate import (  # noqa: E402
    FILL_OPEN, VALUE_OPEN_44, VALUE_OUT_SAME_ROW, assert_excel_clock_gate,
    assert_feature_legal, gate_payload,
)
from harden_hyst_open import splice_md  # noqa: E402
from mine_hi_horizon import load_spy_regimes  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_shade_open import (  # noqa: E402
    FILL_IDX, _cell, _worst, classify, ghost_score, heat_cuts, jsonable,
    labels_at, norm_hex, push_hit, trailing_i,
)
from mine_unmined import (  # noqa: E402
    BEAT, HALF_CUT, Q3_CUT, _blk, pack_row,
)
from audit_af_seed import load_mine_grid  # noqa: E402
from harden_hyst_open import s2d  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "SHADE_GATE.md")
OUT_JSON = os.path.join(RESEARCH, "shade_gate.json")
CARDS = os.path.join(RESEARCH, "SHADE_KEEP_CARDS.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
SHADE_MD = os.path.join(RESEARCH, "SHADE_OPEN.md")
MARKER = "## Shade clock-gate pairs (M fill × open 44 / lags)"
HEX_MID = "95CA82"
PARENT = "M_hex_95CA82"
M_IDX = 12


def _cmp(op, left, th):
    if left is None:
        return False
    if op == "==":
        return left == th
    if op == "!=":
        return left != th
    if op == ">":
        return left > th
    if op == ">=":
        return left >= th
    if op == "<":
        return left < th
    if op == "<=":
        return left <= th
    raise ValueError(op)


def build_atoms():
    """Legal atoms only. Same-row nums ⊆ 44; fills ⊆ 12; lags any letter."""
    raw = [
        # parent — M FILL only
        ("M_hex_95CA82", "fill", "M", 0, "==", HEX_MID, "hex"),
        # same-row 44 (computable on this panel)
        ("J_l0_gt0", "num", "J", 0, ">", 0.0, "num"),
        ("J_l0_lt0", "num", "J", 0, "<", 0.0, "num"),
        ("J_l0_ge1", "num", "J", 0, ">=", 0.01, "num"),
        ("J_l0_le-1", "num", "J", 0, "<=", -0.01, "num"),
        ("AH_l0_eq0", "num", "AH", 0, "==", 0, "num"),
        ("AH_l0_ge1", "num", "AH", 0, ">=", 1, "num"),
        ("AH_l0_ge2", "num", "AH", 0, ">=", 2, "num"),
        ("FQ_l0_eq1", "num", "FQ", 0, "==", 1, "num"),
        ("ER_l0_eq1", "num", "ER", 0, "==", 1, "num"),
        ("ER_l0_eq-1", "num", "ER", 0, "==", -1, "num"),
        ("JB_l0_eq1", "num", "JB", 0, "==", 1, "num"),
        ("JC_l0_eq1", "num", "JC", 0, "==", 1, "num"),
        # lags — any letter, including close numbers from rows above
        ("I_l1_gt0", "num", "I", 1, ">", 0.0, "num"),
        ("I_l1_lt0", "num", "I", 1, "<", 0.0, "num"),
        ("H_l2_gt0", "num", "H", 2, ">", 0.0, "num"),
        ("H_l2_lt0", "num", "H", 2, "<", 0.0, "num"),
        ("J_l1_gt0", "num", "J", 1, ">", 0.0, "num"),
        ("J_l1_lt0", "num", "J", 1, "<", 0.0, "num"),
        ("M_l1_ge02", "num", "M", 1, ">=", 0.02, "num"),
        ("M_l1_ge05", "num", "M", 1, ">=", 0.05, "num"),
        ("K_l1_ge02", "num", "K", 1, ">=", 0.02, "num"),
        ("K_l1_ge05", "num", "K", 1, ">=", 0.05, "num"),
        ("G_l1_ge2", "num", "G", 1, ">=", 2.0, "num"),
        ("G_l1_ge3", "num", "G", 1, ">=", 3.0, "num"),
        ("B_l1_gtC", "num", "B", 1, "rel_gt_c", 0.0, "num"),
    ]
    atoms = []
    for name, kind, col, lag, op, th, fam in raw:
        if name == "B_l1_gtC":
            # skip — needs today's C vs yesterday B; today's C is legal
            # but this shape is easier as J (already covered)
            continue
        assert_feature_legal(kind, col, lag)
        if lag == 0 and kind == "num" and col in VALUE_OUT_SAME_ROW:
            raise ValueError(f"OUT same-row value slipped in: {name}")
        if lag == 0 and kind == "num" and col not in VALUE_OPEN_44:
            raise ValueError(f"same-row value not in 44: {name}")
        if lag == 0 and kind == "fill" and col not in FILL_OPEN:
            raise ValueError(f"same-row fill not in 12: {name}")
        if lag == 0 and col in ("H", "I"):
            raise ValueError(f"same-row H/I label leak: {name}")
        if "core_score" in name:
            raise ValueError("core_score is OUT")
        atoms.append({
            "name": name, "kind": kind, "col": col, "lag": lag,
            "op": op, "th": th, "fam": fam,
        })
    return atoms


ATOMS = None


def atoms():
    global ATOMS
    if ATOMS is None:
        ATOMS = build_atoms()
    return ATOMS


def pair_name(atom):
    return f"{PARENT}__and__{atom['name']}"


def meaning_of(name):
    if name == PARENT:
        return (
            "morning cell M is fill hex #95CA82 (mid green, open-knowable). "
            "Not M's number"
        )
    if name.startswith(f"{PARENT}__and__"):
        tail = name.split("__and__", 1)[1]
        return meaning_of(PARENT) + ", and " + _atom_plain(tail)
    return _atom_plain(name)


def _atom_plain(name):
    table = {
        "J_l0_gt0": "today's open-to-open J is positive (44, same-row open)",
        "J_l0_lt0": "today's open-to-open J is negative (44, same-row open)",
        "J_l0_ge1": "today's open-to-open J ≥ +1% (44, same-row open)",
        "J_l0_le-1": "today's open-to-open J ≤ −1% (44, same-row open)",
        "AH_l0_eq0": "AH = 0 — no prior-6 H print ≤ −5% (44, open walk)",
        "AH_l0_ge1": "AH ≥ 1 — at least one prior-6 H ≤ −5% (44, open walk)",
        "AH_l0_ge2": "AH ≥ 2 — two or more prior-6 H ≤ −5% (44, open walk)",
        "FQ_l0_eq1": "FQ = 1 — yesterday's H > +3% (44, prior H only)",
        "ER_l0_eq1": "ER = 1 — yesterday H or I was a +5% print (44)",
        "ER_l0_eq-1": "ER = −1 — yesterday H or I was a −3% print (44)",
        "JB_l0_eq1": "JB = 1 — ≥7 of prior-8 H were |H|>3% (44)",
        "JC_l0_eq1": "JC = 1 — none of prior-8 H were < −3% (44)",
        "I_l1_gt0": "yesterday's I (daily %) was positive — lag",
        "I_l1_lt0": "yesterday's I (daily %) was negative — lag",
        "H_l2_gt0": "H two sessions ago was positive — lag (t−1 H is IZ)",
        "H_l2_lt0": "H two sessions ago was negative — lag",
        "J_l1_gt0": "yesterday's J was positive — lag",
        "J_l1_lt0": "yesterday's J was negative — lag",
        "M_l1_ge02": "yesterday's M number (low wick) ≥ 2% — lag, not today",
        "M_l1_ge05": "yesterday's M number (low wick) ≥ 5% — lag, not today",
        "K_l1_ge02": "yesterday's K number (high wick) ≥ 2% — lag",
        "K_l1_ge05": "yesterday's K number (high wick) ≥ 5% — lag",
        "G_l1_ge2": "yesterday's G (vol ratio) ≥ 2 — lag",
        "G_l1_ge3": "yesterday's G (vol ratio) ≥ 3 — lag",
    }
    return table.get(name, name)


def series_bundle(days):
    """Open-legal series. Same-row H/I/M/K/G/B numbers are stored for LAGS only."""
    n = len(days)
    H = [d.get("h_ret") for d in days]
    I = [d.get("i_ret") for d in days]
    J = [d.get("j_ret") for d in days]
    M = []
    K = []
    G = []
    prev_v = None
    for d in days:
        o, h, l, v = d.get("open"), d.get("high"), d.get("low"), d.get("volume")
        M.append((-(l - o) / o) if (o and l is not None) else None)
        K.append(((h - o) / o) if (o and h is not None) else None)
        G.append((v / prev_v) if (v and prev_v) else None)
        prev_v = v if v else prev_v
    AH, FQ, ER, JB, JC = [], [], [], [], []
    for i in range(n):
        w6 = [H[j] for j in range(max(0, i - 6), i) if H[j] is not None]
        AH.append(sum(1 for x in w6 if x <= -0.05) if len(w6) >= 6 else None)
        if i == 0 or H[i - 1] is None:
            FQ.append(None)
        else:
            FQ.append(1 if H[i - 1] > 0.03 else 0)
        if i == 0:
            ER.append(None)
        else:
            h0, i0 = H[i - 1], I[i - 1]
            if h0 is None and i0 is None:
                ER.append(None)
            else:
                val = 0
                if h0 is not None and h0 < -0.03:
                    val = -1
                elif i0 is not None and i0 < -0.03:
                    val = -1
                elif h0 is not None and h0 > 0.05:
                    val = 1
                elif i0 is not None and i0 > 0.05:
                    val = 1
                ER.append(val)
        w8 = [H[j] for j in range(max(0, i - 8), i) if H[j] is not None]
        if len(w8) < 8:
            JB.append(None)
            JC.append(None)
        else:
            JB.append(1 if sum(1 for x in w8 if x > 0.03 or x < -0.03) >= 7 else 0)
            JC.append(1 if sum(1 for x in w8 if x < -0.03) == 0 else 0)
    fills = []
    for d in days:
        raw = [norm_hex(x) for x in (d.get("fills") or [])]
        fills.append((raw + [None] * 15)[:15])
    return {
        "H": H, "I": I, "J": J, "M": M, "K": K, "G": G,
        "AH": AH, "FQ": FQ, "ER": ER, "JB": JB, "JC": JC,
        "fills": fills,
    }


def atom_value(bundle, atom, ei):
    src = ei - atom["lag"]
    if src < 0:
        return None
    if atom["kind"] == "fill":
        row = bundle["fills"][src]
        li = FILL_IDX.get(atom["col"])
        if li is None:
            return None
        return row[li] if li < len(row) else None
    col = atom["col"]
    if col not in bundle:
        raise ValueError(f"no series for {col}")
    return bundle[col][src]


def fire_atom(bundle, atom, ei):
    val = atom_value(bundle, atom, ei)
    return _cmp(atom["op"], val, atom["th"])


def work_grid(path, discovery, holdout, spy):
    t = os.path.basename(path)[:-5].upper()
    if t.startswith("_"):
        return None
    split = ("discovery" if t in discovery
             else "holdout" if t in holdout else None)
    if split is None:
        return None
    try:
        blob = load_mine_grid(path)
        if blob is None:
            return None
        days = annotate_days(blob["days"])
    except Exception:
        return None
    if len(days) < 30:
        return None
    bundle = series_bundle(days)
    ivals = bundle["I"]
    hits, book, heats = [], [], []
    parent_atom = next(a for a in atoms() if a["name"] == PARENT)
    legs = [a for a in atoms() if a["name"] != PARENT]
    n = len(days)
    for ei in range(n):
        labs = labels_at(days, ei)
        raw = labs.get(("H", 1))
        if raw is None:
            continue
        iso = str(s2d(days[ei]["date"]))
        heat = trailing_i(ivals, ei)
        tape = spy.get(iso)
        if tape is None:
            tape = 0
        if split == "discovery" and heat is not None:
            heats.append(heat)
        net = raw - COST_FUTU_LONG
        book.append((t, iso, split, "H", 1, net, heat, tape))
        if not fire_atom(bundle, parent_atom, ei):
            continue
        hits.append((PARENT, "hex", "M", HEX_MID, "H", 1, net,
                     t, iso, split, heat, tape))
        for a in legs:
            if fire_atom(bundle, a, ei):
                hits.append((pair_name(a), "pair", a["col"], HEX_MID, "H", 1,
                             net, t, iso, split, heat, tape))
    return {"hits": hits, "book": book, "heats": heats, "n": n}


_G = {}


def _init_g(disc, hold, spy):
    _G.update(disc=disc, hold=hold, spy=spy)


def _work(path):
    return work_grid(path, _G["disc"], _G["hold"], _G["spy"])


def collect(files, discovery, holdout, spy, workers=4):
    hits, book, heats = [], [], []
    n_ok = 0
    _G.update(disc=discovery, hold=holdout, spy=spy)
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init_g,
                  initargs=(discovery, holdout, spy)) as pool:
            for i, rec in enumerate(pool.imap_unordered(_work, files,
                                                        chunksize=8), 1):
                if rec:
                    n_ok += 1
                    hits.extend(rec["hits"])
                    book.extend(rec["book"])
                    heats.extend(rec["heats"])
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} ok={n_ok} hits={len(hits)}",
                          flush=True)
    else:
        for i, f in enumerate(files, 1):
            rec = _work(f)
            if rec:
                n_ok += 1
                hits.extend(rec["hits"])
                book.extend(rec["book"])
                heats.extend(rec["heats"])
            if i % 400 == 0:
                print(f"  ... {i}/{len(files)} ok={n_ok} hits={len(hits)}",
                      flush=True)
    return hits, book, heats, n_ok


def pack_all(hits, book, heats, baselines):
    lo, hi = heat_cuts(heats)
    cells = defaultdict(_cell)
    book_cells = defaultdict(_cell)
    for name, _fam, _let, _hx, lab, hz, net, t, iso, split, heat, tape in hits:
        push_hit(cells[(name, lab, hz)], net, iso, split, tape, heat, t, lo, hi)
    for t, iso, split, lab, hz, net, heat, tape in book:
        push_hit(book_cells[(lab, hz)], net, iso, split, tape, heat, t, lo, hi)
    by_name = {}
    rows = []
    for (name, label, hz), cell in cells.items():
        row = pack_row(name, "open", "long", f"hold{hz}", cell, {})
        if not row:
            continue
        row["family"] = "parent" if name == PARENT else "pair"
        row["label"] = label
        row["horizon"] = hz
        row["plain"] = meaning_of(name)
        row["clock"] = "open"
        b = baselines.get(f"{label}_{hz}") or _blk(book_cells[(label, hz)]["hold"])
        if not b:
            b = _blk(book_cells[(label, hz)]["disc"])
        row["baseline"] = b
        reasons = [x for x in (row.get("fail_reasons") or [])
                   if x != "no_edge_vs_uncond"]
        d, h = row.get("discovery") or {}, row.get("holdout") or {}
        if b and (
            (d.get("avg_net") or 0) < (b.get("avg_net") or 0) + BEAT
            or (h and (h.get("avg_net") or 0) < (b.get("avg_net") or 0) + BEAT)
        ):
            reasons.append("no_edge_vs_uncond")
        row["fail_reasons"] = reasons
        deeper(row, cell)
        rows.append(row)
        by_name[(name, label, hz)] = row
    for r in rows:
        if r["def"] == PARENT:
            continue
        parent = by_name.get((PARENT, r["label"], r["horizon"]))
        if not parent:
            continue
        r["parent"] = PARENT
        ph = parent.get("holdout") or {}
        h = r.get("holdout") or {}
        if ph.get("avg_net") is not None and h.get("avg_net") is not None:
            r["vs_parent_pp"] = (h["avg_net"] - ph["avg_net"]) * 100
            if h["avg_net"] < ph["avg_net"] + BEAT:
                r["fail_reasons"] = list(dict.fromkeys(
                    list(r.get("fail_reasons") or []) + ["no_edge_vs_parent"]
                ))
        r["vs_book_pp"] = None
        b = r.get("baseline") or {}
        if h.get("avg_net") is not None and b.get("avg_net") is not None:
            r["vs_book_pp"] = (h["avg_net"] - b["avg_net"]) * 100
    for r in rows:
        if r["def"] == PARENT:
            h = r.get("holdout") or {}
            b = r.get("baseline") or {}
            if h.get("avg_net") is not None and b.get("avg_net") is not None:
                r["vs_book_pp"] = (h["avg_net"] - b["avg_net"]) * 100
        classify(r)
    return rows, lo, hi, book_cells


def family_verdict(rows):
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
          and r["def"] != PARENT]
    n_keep = sum(1 for r in h1 if r.get("keep") == "KEEP")
    n_kill = sum(1 for r in h1 if r.get("keep") == "KILL")
    n_thin = sum(1 for r in h1 if r.get("keep") == "THIN")
    if n_keep:
        return "KEEP", (
            f"Clock-gate pairs on M mid `#95CA82` fill: KEEP {n_keep} · "
            f"KILL {n_kill} · THIN {n_thin}."
        )
    return "null", (
        f"M mid `#95CA82` fill × locked open 44 / lags does not clear "
        f"the ship bar (KEEP 0 · KILL {n_kill} · THIN {n_thin}). "
        "Parent fill stays DEMOTE. Live frozen."
    )


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render(rows, n_grids, n_days, verd, why, ghost=None, panel=None):
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1]
    h1 = sorted(h1, key=lambda r: (
        0 if r.get("keep") == "KEEP" else 1 if r.get("keep") == "KILL" else 2,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    ))
    n_keep = sum(1 for r in h1 if r["def"] != PARENT and r.get("keep") == "KEEP")
    n_kill = sum(1 for r in h1 if r["def"] != PARENT and r.get("keep") == "KILL")
    n_thin = sum(1 for r in h1 if r["def"] != PARENT and r.get("keep") == "THIN")
    panel = panel or {}
    L = [
        "# Shade clock-gate pairs — M fill × open 44 / lags",
        "",
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        "frozen · research only · no live push._",
        "",
        "## Plain English",
        "",
        "Excel clock gate is the source of truth "
        "(`OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`). Mid-M `#95CA82` "
        "stays **open-fill only**. This beat pairs that fill with the "
        "locked 44 `value_mine_open` cols we can compute on the expand "
        "panel (J, AH, ER, FQ, JB, JC) and with **lags** of any letter "
        "(H/I/M/K/G/J from rows above). Same-row M/B/G/K/O numbers, "
        "D/E/F/H/I, and `core_score` are OUT.",
        "",
        f"**Family verdict: {verd}** — KEEP {n_keep} · KILL {n_kill} · "
        f"THIN {n_thin}.",
        "",
        why,
        "",
        "### Excel clock gate (enforced)",
        "",
        "- Shades/fills at open: **A B C G J K L M O IR IS IT**",
        "- Numbers/text at open: the **44 `value_mine_open` cols** "
        "(never same-row H/I)",
        "- Lags: any letter from rows above is fair",
        "- OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, "
        "`core_score`",
        "- Parent feature is M **fill** `#95CA82`. CF `IZ=1` is a lag. "
        "M’s number is never a feature.",
        "",
        f"Panel **{panel.get('n_tickers_with_rows') or n_grids}** tickers · "
        f"{panel.get('date_min') or '?'} → {panel.get('date_max') or '?'} · "
        f"{panel.get('name_days') or '?'} name-days. Calendar days "
        f"**{n_days}**. Futubull 0.15% long. Beat book and parent by ≥20 bp.",
        "",
    ]
    if ghost:
        L += [
            f"**Ghost / name check: {ghost.get('family')}**",
            "",
            ghost.get("why") or "",
            "",
        ]
    L += [
        "### Same-day H (open entry)",
        "",
        "| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | "
        "verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in h1:
        h = r.get("holdout") or {}
        q1 = r.get("q1") or {}
        vs_b = r.get("vs_book_pp")
        vs_p = r.get("vs_parent_pp")
        L.append(
            f"| {r.get('plain') or r['def']} | {_pct(h)} | "
            f"{'—' if vs_b is None else f'{vs_b:+.2f} pp'} | "
            f"{'—' if vs_p is None else f'{vs_p:+.2f} pp'} | "
            f"{_pct(q1)} | {_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"**{r.get('keep')}** | {','.join(r.get('fail_reasons') or []) or '—'} | "
            f"`{r['def']}` |"
        )
    near = [r for r in h1 if r["def"] != PARENT
            and (r.get("holdout") or {}).get("avg_net", -1) > 0
            and (r.get("vs_book_pp") or -9) >= 0.20
            and (r.get("vs_parent_pp") or -9) >= 0.20]
    if near:
        L += [
            "",
            "### Near-miss (green holdout, beats book and parent by ≥20 bp, not KEEP)",
            "",
        ]
        for r in near:
            h = r.get("holdout") or {}
            L.append(
                f"- `{r['def']}` holdout {_pct(h)}, vs book "
                f"{r.get('vs_book_pp'):+.2f} pp, vs parent "
                f"{r.get('vs_parent_pp'):+.2f} pp. Killed by "
                f"{','.join(r.get('fail_reasons') or []) or '—'}. "
                "Not a card."
            )
    L += [
        "",
        "### What this does not change",
        "",
        "- Live `flatten_robust` is frozen. No card under `strategies/`.",
        "- Standing M mid fill on the expand stays **DEMOTE** as a parent.",
        "- Pair+lag open on the smaller dump (KEEP 0 / KILL 4266) is not reopened.",
        "- Close-entry values stay out of the open clock.",
        "",
        "Research only. Live frozen.",
        "",
    ]
    return "\n".join(L)


def pair_ghost_family(scores):
    """Ghost bar across parent + pairs. Not the shade-hex M/O family helper."""
    parent = next((s for s in scores if s["def"] == PARENT), None)
    pairs = [s for s in scores if s["def"] != PARENT]
    keep_g = [s for s in pairs if s.get("ghost") == "PASS"]
    slots = [s["ghost"] for s in pairs if s.get("ghost")]
    family = "GHOST THIN"
    if slots:
        worst = _worst(*slots)
        family = f"GHOST {worst}"
        if keep_g and worst == "PASS":
            family = "GHOST PASS"
    bits = []
    if parent:
        bits.append(
            f"parent `{PARENT}` GHOST {parent.get('ghost')} "
            f"holdout {((parent.get('holdout') or {}).get('avg_net') or 0)*100:+.2f}%"
        )
    bits.append(f"pair PASS {len(keep_g)} / {len(pairs)}")
    return {
        "family": family,
        "why": "Clock-gate pair ghost: " + "; ".join(bits) + ". Live frozen.",
        "live_untouched": "flatten_robust",
        "recipes": scores,
        "n_pair_pass": len(keep_g),
        "n_pair": len(pairs),
        "parent_ghost": (parent or {}).get("ghost"),
    }


def ghost_all(rows, hits, book):
    want = [r["def"] for r in rows if r.get("label") == "H"
            and r.get("horizon") == 1]
    by = defaultdict(list)
    for name, _fam, _let, _hx, lab, hz, net, t, iso, split, heat, tape in hits:
        if lab == "H" and hz == 1:
            by[name].append((t, iso, split, net, tape))
    book_h = [(t, iso, split, net, tape)
              for t, iso, split, lab, hz, net, heat, tape in book
              if lab == "H" and hz == 1]
    parent = by.get(PARENT) or []
    scores = []
    for name in want:
        scores.append(ghost_score(name, by.get(name) or [], parent, book_h))
    return pair_ghost_family(scores) if scores else None


def splice_scoreboard(md, verd, n_keep, n_kill, ghost=None):
    g = ""
    if ghost:
        g = f" {ghost.get('family')}."
    block = (
        f"{MARKER}\n\n"
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        f"frozen. Excel clock gate (`OPEN_SAME_ROW_LABELS` + `CLOCK_MAP`). "
        f"M mid `#95CA82` fill × locked 44 / lags. Family **{verd}**.{g} "
        f"KEEP {n_keep} · KILL {n_kill}. See `SHADE_GATE.md`. "
        "Live frozen._\n"
    )
    if os.path.exists(SB_MD):
        return splice_md(SB_MD, MARKER, block, require="PASS 376")
    return block


def splice_shade_open(verd, n_keep, n_kill):
    """One pointer in SHADE_OPEN.md — do not rewrite the DEMOTE tables."""
    if not os.path.exists(SHADE_MD):
        return
    md = open(SHADE_MD, encoding="utf-8").read()
    mark = "### Excel clock gate (source of truth)"
    block = (
        f"{mark}\n\n"
        "Same-row open fills: **A B C G J K L M O IR IS IT**. "
        "Same-row numbers/text: the **44 `value_mine_open` cols** "
        "(never H/I). Lags of any letter are fair. OUT: M’s number, "
        "B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`. "
        "Docs: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`. "
        f"M `#95CA82` × 44/lag pairs: family **{verd}** "
        f"(KEEP {n_keep} · KILL {n_kill}) — `SHADE_GATE.md`.\n"
    )
    if mark in md:
        pre, rest = md.split(mark, 1)
        nxt = rest.find("\n### ")
        if nxt == -1:
            nxt = rest.find("\n## ")
        tail = rest[nxt:] if nxt != -1 else "\n"
        open(SHADE_MD, "w").write(pre + block + tail)
    else:
        # insert after Open-only gate section header block
        needle = "### Open-only gate (standing bar)"
        if needle in md:
            open(SHADE_MD, "w").write(md.replace(needle, block + "\n" + needle, 1))


def write_cards(rows, verd, ghost, panel):
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1]
    keeps = [r for r in h1 if r.get("keep") == "KEEP" and r["def"] != PARENT]
    tip = os.popen("git rev-parse --short HEAD").read().strip()
    L = [
        "# Research cards — shade clock-gate pairs (M fill × open 44 / lags)",
        "",
        f"_Generated {date.today().isoformat()} · tip `{tip}` · "
        "**research cards only** · live `flatten_robust` frozen · "
        f"family **{verd}**._",
        "",
        "## Plain English",
        "",
        "Excel clock gate enforced exactly. Mid-M `#95CA82` stays "
        "open-fill only. Pairs are locked 44 values we can compute on "
        "the expand (J/AH/ER/FQ/JB/JC) plus lags of any letter.",
        "",
        f"| panel | value |",
        f"|---|---|",
        f"| tickers | **{panel.get('n_tickers_with_rows')}** |",
        f"| dates | **{panel.get('date_min')} → {panel.get('date_max')}** |",
        f"| name-days | **{panel.get('name_days')}** |",
        "",
    ]
    if not keeps:
        L += [
            "No KEEP cards. Parent M fill remains **DEMOTE**. Pair family "
            f"**{verd}**. Ghost {((ghost or {}).get('family') or 'n/a')}.",
            "",
            "## Explicitly not a live wire",
            "",
            "Live `flatten_robust` stays frozen. No card under `strategies/`.",
            "",
            "Source: `SHADE_GATE.md` · `OPEN_SAME_ROW_LABELS.md` · "
            "`CLOCK_MAP.md`. Research only. Live frozen.",
            "",
        ]
        return "\n".join(L)
    for i, r in enumerate(keeps, 1):
        h = r.get("holdout") or {}
        L += [
            f"## Card {i} — `{r['def']}`",
            "",
            f"| field | value |",
            f"|---|---|",
            f"| recipe | {r.get('plain')} |",
            f"| entry | open |",
            f"| label | H (outcome, not a feature) |",
            f"| holdout | **{(h.get('avg_net') or 0)*100:+.2f}%** (n={h.get('n')}) |",
            f"| vs book | {r.get('vs_book_pp')} pp |",
            f"| vs parent | {r.get('vs_parent_pp')} pp |",
            f"| verdict | **{r.get('keep')}** |",
            f"| status | research card · not live |",
            "",
        ]
    L += [
        "## Explicitly not a live wire",
        "",
        "Live `flatten_robust` stays frozen. No card under `strategies/`.",
        "",
        "Source: `SHADE_GATE.md`. Research only. Live frozen.",
        "",
    ]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    clocks = assert_excel_clock_gate()
    built = atoms()
    for a in built:
        assert_feature_legal(a["kind"], a["col"], a["lag"])
    print("gate ok fills", "".join(FILL_OPEN), "values", len(VALUE_OPEN_44),
          "atoms", len(built), flush=True)
    stats_path = os.path.join(RESEARCH, "shade_panel_stats.json")
    panel = json.load(open(stats_path)) if os.path.exists(stats_path) else {}
    if args.render_only and os.path.exists(OUT_JSON):
        payload = json.load(open(OUT_JSON))
        rows = [classify(r) for r in payload.get("rows") or []]
        for r in rows:
            r["plain"] = meaning_of(r["def"])
        verd, why = family_verdict(rows)
        h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
              and r["def"] != PARENT]
        payload["rows"] = rows
        payload["family_verdict"] = verd
        payload["why"] = why
        payload["n_keep_h1"] = sum(1 for r in h1 if r.get("keep") == "KEEP")
        payload["n_kill_h1"] = sum(1 for r in h1 if r.get("keep") == "KILL")
        payload["n_thin_h1"] = sum(1 for r in h1 if r.get("keep") == "THIN")
        ghost = payload.get("ghost")
        md = render(rows, payload.get("n_grids") or 0,
                    payload.get("n_days") or 0, verd, why, ghost, panel)
        open(OUT_MD, "w").write(md)
        json.dump(payload, open(OUT_JSON, "w"), indent=2)
        sb = splice_scoreboard(
            md, verd, payload["n_keep_h1"], payload["n_kill_h1"], ghost)
        open(SB_MD, "w").write(sb)
        splice_shade_open(verd, payload["n_keep_h1"], payload["n_kill_h1"])
        open(os.path.join(RESEARCH, "SHADE_GATE_CARDS.md"), "w").write(
            write_cards(rows, verd, ghost, panel))
        print(f"render-only VERDICT {verd}", OUT_MD)
        return
    split = json.load(open(SPLIT_PATH))
    discovery, holdout = set(split["discovery"]), set(split["holdout"])
    spy = load_spy_regimes()
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"mine {len(files)} grids workers={args.workers}", flush=True)
    hits, book, heats, n_ok = collect(
        files, discovery, holdout, spy, workers=args.workers)
    baselines = {}
    by_lab = defaultdict(list)
    for _t, _iso, split_, lab, hz, net, _heat, _tape in book:
        if split_ == "holdout":
            by_lab[(lab, hz)].append(net)
    for k, vals in by_lab.items():
        if len(vals) >= 2:
            m = sum(vals) / len(vals)
            baselines[f"{k[0]}_{k[1]}"] = {
                "n": len(vals), "avg_net": m, "t": 0.0,
            }
    rows, lo, hi, _bc = pack_all(hits, book, heats, baselines)
    verd, why = family_verdict(rows)
    n_days = len({b[1] for b in book})
    print("ghost…", flush=True)
    ghost = ghost_all(rows, hits, book)
    md = render(rows, n_ok, n_days, verd, why, ghost, panel)
    open(OUT_MD, "w").write(md)
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
          and r["def"] != PARENT]
    payload = {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "entry": "open",
        "cost_model": "futubull",
        "family_verdict": verd,
        "why": why,
        "n_grids": n_ok,
        "n_hits": len(hits),
        "n_book": len(book),
        "n_keep_h1": sum(1 for r in h1 if r.get("keep") == "KEEP"),
        "n_kill_h1": sum(1 for r in h1 if r.get("keep") == "KILL"),
        "n_thin_h1": sum(1 for r in h1 if r.get("keep") == "THIN"),
        "n_days": n_days,
        "heat_lo": lo, "heat_hi": hi,
        "rows": jsonable(rows),
        "ghost": ghost,
        "atoms": [{k: a[k] for k in ("name", "kind", "col", "lag", "op", "th")}
                  for a in built],
        "source": "rows_cache",
        "panel": panel,
        **gate_payload(),
        "clock_generated": clocks.get("generated"),
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    sb = splice_scoreboard(
        md, verd, payload["n_keep_h1"], payload["n_kill_h1"], ghost)
    open(SB_MD, "w").write(sb)
    splice_shade_open(verd, payload["n_keep_h1"], payload["n_kill_h1"])
    cards = write_cards(rows, verd, ghost, panel)
    # Keep SHADE_KEEP_CARDS as the M-mid DEMOTE file; write pairs beside it.
    open(os.path.join(RESEARCH, "SHADE_GATE_CARDS.md"), "w").write(cards)
    print(f"VERDICT {verd} KEEP={payload['n_keep_h1']} "
          f"KILL={payload['n_kill_h1']} grids={n_ok}")
    print(OUT_MD)


if __name__ == "__main__":
    main()
# flatten_robust labeled untouched — this miner does not import or write live.
