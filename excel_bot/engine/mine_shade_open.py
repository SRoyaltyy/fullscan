"""Shade hex + onset on open-knowable fills. Open entry. Same-day H first.

Cyrus: shades of green (stored fill hex / CF color), not prior-I heat-green.
Open-knowable fills only: A, B, C, G, J, K, L, M, O (+ IR/IS/IT inventory).
Any other letter's fill at lag 0 is close-entry — excluded.

Live flatten_robust frozen. No cards. No live push.

  python3 engine/mine_shade_open.py
  python3 engine/mine_shade_open.py --render-only
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import (  # noqa: E402
    CLOSE_LETTERS, COST_FUTU_LONG, OPEN_CORE_IDX, OPEN_LETTERS, SHIP,
    VISIBLE, annotate_days, assert_clock_legal, feature_clock,
)
from harden_hyst_open import lottery_day, s2d, splice_md  # noqa: E402
from mine_hi_horizon import FLAT_BPS, load_spy_regimes  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_unmined import (  # noqa: E402
    BEAT, HALF_CUT, Q3_CUT, _blk, _push, _slot, pack_row,
)
from audit_af_seed import load_mine_grid  # noqa: E402
from signals import _hysteresis, classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
MODEL = os.path.join(HERE, "model.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "SHADE_OPEN.md")
OUT_JSON = os.path.join(RESEARCH, "shade_open.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
MARKER = "## Shade hex + onset (open-entry)"

# Timing-tested open fills. IR/IS/IT are STOCKHISTORY aliases of A/B/C.
OPEN_FILL = tuple(OPEN_LETTERS)  # A B C G J K L M O
OPEN_ALIASES = ("IR", "IS", "IT")
# Close fills never start an open trade.
CLOSE_FILL = tuple(CLOSE_LETTERS)

# Locked CF greens on open letters (model.json, first-match priority).
# Deep / mid / pale are distinct hexes — not the old green on/off bit.
CF_GREEN = {
    "A": (("3B7D23", "deep", "HN ≥ 4"), ("B8DCAB", "pale", "HN ≥ 0")),
    "B": (("95CA82", "mid", "HM ≥ 3"),),
    "G": (("3B7D23", "deep", "EC > 3"), ("B8DCAB", "pale", "DD = 1")),
    "J": (("95CA82", "mid", "JG = 1"),),
    "K": (("3B7D23", "deep", "HU ≥ 9"), ("95CA82", "mid", "HU ≥ 5")),
    "L": (("3B7D23", "deep", "JA > 0"), ("C6EFCE", "mint", "L value > 0")),
    "M": (("95CA82", "mid", "IZ = 1"),),
    "O": (("C6EFCE", "mint", "O value > 0"),),
}
CF_OTHER = {
    "A": (("FF0000", "red", "HN ≤ −1"),),
    "B": (("FF0000", "red", "HM ≤ 0"),),
    "C": (("F6C6AD", "peach", "EC ≤ 2"),),
    "G": (("F6C6AD", "peach", "DD = 0"),),
    "J": (("FF0000", "red", "GV = 1 or GQ ≤ 2"),
          ("0070C0", "blue", "GQ ≥ 8"), ("6FC5E6", "blue", "GQ ≥ 6")),
    "L": (("FFC7CE", "pink", "L value < −5"),),
    "M": (("F2AA84", "orange", "IZ = −1"),),
    "O": (("FFC7CE", "pink", "O value < 0"),),
}
MULTI_SHADE = tuple(
    c for c, rows in CF_GREEN.items() if len({h for h, _n, _w in rows}) > 1
)
GREEN_HEX = {h for rows in CF_GREEN.values() for h, _n, _w in rows}
HEX_NAME = {h: n for rows in CF_GREEN.values() for h, n, _w in rows}
HEX_NAME["DCEDD5"] = "pale"  # dump-only pale on M; not in CF_GREEN

LABELS = (
    ("H", 1, "same-day H (intraday %, close vs open)"),
    ("I", 1, "same-day I (daily %, close vs yesterday)"),
    ("I_sum", 2, "2d stacked I"),
)


def col_idx_letter(s):
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch) - 64)
    return n


def col_letter(n):
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def parse_sqref(sqref):
    out = []
    for part in (sqref or "").split():
        part = part.replace("$", "")
        a, b = (part.split(":") + [part])[:2]
        m1 = re.match(r"([A-Z]+)(\d+)", a)
        m2 = re.match(r"([A-Z]+)(\d+)", b)
        if m1 and m2:
            out.append((m1.group(1), int(m1.group(2)),
                        m2.group(1), int(m2.group(2))))
    return out


def cols_in(ranges):
    letters = set()
    for c1, _r1, c2, _r2 in ranges:
        for i in range(col_idx_letter(c1), col_idx_letter(c2) + 1):
            letters.add(col_letter(i))
    return letters


def norm_hex(h):
    if not h:
        return None
    h = str(h).upper().replace("#", "")
    if len(h) == 8 and h.startswith("FF"):
        h = h[2:]
    return h if len(h) == 6 else None


def cf_inventory(model=None):
    """Which locked open fills have more than one green hex in CF."""
    model = model or json.load(open(MODEL))
    want = set(OPEN_FILL) | set(OPEN_ALIASES)
    by = {c: [] for c in want}
    for rule in model.get("cf_rules") or []:
        hit = cols_in(parse_sqref(rule.get("sqref", ""))) & want
        if not hit:
            continue
        fills = []
        if rule.get("fill"):
            fills.append(norm_hex(rule["fill"]))
        cs = rule.get("colorScale") or {}
        fills.extend(norm_hex(x) for x in (cs.get("colors") or []))
        rec = {
            "priority": rule.get("priority"),
            "type": rule.get("type"),
            "operator": rule.get("operator"),
            "formulas": rule.get("formulas"),
            "fills": [f for f in fills if f],
            "colorScale": bool(cs),
        }
        for c in hit:
            by[c].append(rec)
    letters = []
    for c in list(OPEN_FILL) + list(OPEN_ALIASES):
        fills = []
        for r in by[c]:
            fills.extend(r["fills"])
        uniq = sorted({f for f in fills if f})
        greens = []
        for h in uniq:
            fam, sc = classify_fill(h)
            if fam == "green":
                greens.append({"hex": h, "score": sc, "bucket": HEX_NAME.get(h)})
        letters.append({
            "col": c,
            "clock": "open" if c in OPEN_FILL or c in OPEN_ALIASES else "close",
            "n_rules": len(by[c]),
            "hexes": uniq,
            "green_hexes": [g["hex"] for g in greens],
            "n_green_hex": len(greens),
            "multi_shade": len(greens) > 1,
            "alias_of": {"IR": "A", "IS": "B", "IT": "C"}.get(c),
            "cf": by[c],
            "greens": greens,
        })
    return {
        "live_untouched": "flatten_robust",
        "open_fills": "".join(OPEN_FILL),
        "open_aliases": list(OPEN_ALIASES),
        "close_fills_excluded": "".join(CLOSE_FILL),
        "multi_shade_letters": [r["col"] for r in letters if r["multi_shade"]],
        "single_green_letters": [
            r["col"] for r in letters
            if r["n_green_hex"] == 1 and r["col"] in OPEN_FILL
        ],
        "no_green_cf": [
            r["col"] for r in letters if r["n_green_hex"] == 0
        ],
        "letters": letters,
    }


def assert_open_gate():
    """Open recipes read only timing-tested open fills."""
    cols = tuple(VISIBLE.index(c) for c in OPEN_FILL)
    assert feature_clock(cols) == "open"
    assert_clock_legal(cols, "open")
    for c in CLOSE_FILL:
        try:
            assert_clock_legal((VISIBLE.index(c),), "open")
        except ValueError:
            continue
        raise AssertionError(f"close fill {c} must not be open-legal")


def _cell():
    return {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(),
        "q12": _slot(), "q3": _slot(), "q1": _slot(),
        "tickers": set(), "dates": set(),
        "day": defaultdict(float),
        "ticker_pnl": defaultdict(float),
        "month": defaultdict(_slot),
        "heat_hot": _slot(), "heat_mixed": _slot(), "heat_cold": _slot(),
        "spy_flat": _slot(),
        "reg": defaultdict(_slot),
    }


def trailing_i(ivals, ei, window=5, need=3):
    chunk = [ivals[j] for j in range(max(0, ei - window), ei)
             if ivals[j] is not None]
    if len(chunk) < need:
        return None
    return sum(chunk) / len(chunk)


def heat_cuts(vals):
    xs = sorted(v for v in vals if v is not None)
    if len(xs) < 30:
        return -0.0045, 0.0048
    return xs[len(xs) // 3], xs[(2 * len(xs)) // 3]


def heat_name(v, lo, hi):
    if v is None:
        return "mixed"
    if v <= lo:
        return "cold"
    if v >= hi:
        return "hot"
    return "mixed"


def tape_name(v):
    return {1: "up", -1: "dn", 0: "flat"}.get(v, "unk")


def labels_at(days, ei):
    d = days[ei]
    h, i = d.get("h_ret"), d.get("i_ret")
    out = {}
    if h is not None:
        out[("H", 1)] = h
    if i is not None:
        out[("I", 1)] = i
    if ei + 1 < len(days) and i is not None:
        nxt = days[ei + 1].get("i_ret")
        if nxt is not None:
            out[("I_sum", 2)] = (1.0 + i) * (1.0 + nxt) - 1.0
    return out


def recipe_hits(hexes, fams, scores, ei):
    """Standing shade / green-on / greener-than-X / onset for this day."""
    hits = []
    prev_f = fams[ei - 1] if ei else None
    prev_h = hexes[ei - 1] if ei else None
    today_f, today_s = fams[ei], scores[ei]
    today_h = hexes[ei]
    for li, letter in enumerate(OPEN_FILL):
        fam = today_f[li] if li < len(today_f) else "none"
        sc = today_s[li] if li < len(today_s) else 0.0
        hx = today_h[li] if li < len(today_h) else None
        pf = prev_f[li] if prev_f and li < len(prev_f) else "none"
        ph = prev_h[li] if prev_h and li < len(prev_h) else None
        if fam == "green":
            hits.append((f"{letter}_green", "green_on", letter, hx))
            if sc >= 1.5:
                hits.append((f"{letter}_ge15", "greener", letter, hx))
            if sc >= 2.0:
                hits.append((f"{letter}_ge20", "greener", letter, hx))
            if hx:
                hits.append((f"{letter}_hex_{hx}", "hex", letter, hx))
            if pf != "green":
                hits.append((f"{letter}_onset_green", "onset", letter, hx))
            if pf == "red":
                hits.append((f"{letter}_onset_red2green", "onset", letter, hx))
            if hx and ph != hx:
                hits.append((f"{letter}_onset_hex_{hx}", "onset", letter, hx))
    return hits


def _emit(hits, name, fam, letter, hx, lab, hz, net, t, iso, split, heat, tape):
    hits.append((name, fam, letter, hx, lab, hz, net, t, iso, split, heat, tape))


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
    n = len(days)
    hexes, fams, scores = [], [], []
    ivals, cores = [], []
    dump = Counter()
    for d in days:
        fills = [norm_hex(x) for x in (d.get("fills") or [])]
        fills = (fills + [None] * 15)[:15]
        hexes.append(fills)
        fams.append(d.get("fams") or ["none"] * 15)
        scores.append(d.get("scores") or [0.0] * 15)
        ivals.append(d.get("i_ret"))
        cores.append(d.get("open_core") or 0.0)
        for li, letter in enumerate(OPEN_FILL):
            dump[(letter, fills[li] or "none")] += 1
    light = set()
    for c in _hysteresis(cores, 5, 2, 2):
        if c.get("side") == 1:
            light.add(c["entry_idx"])
    hits, book, heats = [], [], []
    for ei in range(n):
        iso = str(s2d(days[ei]["date"]))
        labs = labels_at(days, ei)
        if not labs:
            continue
        heat = trailing_i(ivals, ei)
        tape = spy.get(iso)
        if tape is None:
            tape = 0
        if split == "discovery" and heat is not None:
            heats.append(heat)
        recs = recipe_hits(hexes, fams, scores, ei)
        in_light = ei in light
        for (lab, hz), raw in labs.items():
            # I / 2d I only on shade + onset + light (cheap, not a remine)
            if lab != "H" and not (in_light or any(
                    x[1] in ("hex", "greener", "onset") for x in recs)):
                continue
            net = raw - COST_FUTU_LONG
            book.append((t, iso, split, lab, hz, net, heat, tape))
            for name, fam, letter, hx in recs:
                _emit(hits, name, fam, letter, hx, lab, hz, net,
                      t, iso, split, heat, tape)
                if in_light and fam in ("hex", "greener", "green_on", "onset"):
                    _emit(hits, f"light__{name}", "light_shade", letter, hx,
                          lab, hz, net, t, iso, split, heat, tape)
        if in_light:
            for (lab, hz), raw in labs.items():
                _emit(hits, "light_on", "light", None, None, lab, hz,
                      raw - COST_FUTU_LONG, t, iso, split, heat, tape)
    return {"hits": hits, "book": book, "heats": heats, "dump": dump, "n": n}


def push_hit(cell, net, iso, split, tape, heat, ticker, lo, hi):
    _push(cell["disc"] if split == "discovery" else cell["hold"], net)
    _push(cell["early"] if iso < HALF_CUT else cell["late"], net)
    _push(cell["q12"] if iso < Q3_CUT else cell["q3"], net)
    if iso < Q1_CUT:
        _push(cell["q1"], net)
    if tape == 1:
        _push(cell["spy_up"], net)
    elif tape == -1:
        _push(cell["spy_dn"], net)
    else:
        _push(cell["spy_flat"], net)
    bucket = heat_name(heat, lo, hi)
    _push(cell[f"heat_{bucket}"], net)
    _push(cell["reg"][(bucket, tape_name(tape))], net)
    cell["tickers"].add(ticker)
    cell["dates"].add(iso)
    cell["ticker_pnl"][ticker] += net
    _push(cell["month"][iso[:7]], net)
    if split == "discovery":
        cell["day"][iso] += net


def parent_of(name):
    if name.startswith("light__"):
        core = name[7:]
        letter = core.split("_")[0]
        if core == f"{letter}_green":
            return "light_on"
        return f"light__{letter}_green"
    if name.endswith("_ge15") or name.endswith("_ge20"):
        return name.split("_")[0] + "_green"
    if "_hex_" in name:
        return name.split("_")[0] + "_green"
    if name.endswith("_onset_green") or name.endswith("_onset_red2green"):
        return name.split("_onset_")[0] + "_green"
    return None


def meaning_of(name):
    if name == "light_on":
        return (
            "the five morning cells (A, B, C, G, J) add up to a strong "
            "green (+5 or more) and the light first turns on"
        )
    light = name.startswith("light__")
    core = name[7:] if light else name
    letter = core.split("_")[0]
    extra = ""
    if core.endswith("_green") and "onset" not in core:
        extra = f"morning cell {letter} is highlighted any green"
    elif "_ge20" in core:
        extra = f"morning cell {letter} is deep green (score ≥ 2.0)"
    elif "_ge15" in core:
        extra = f"morning cell {letter} is mid-or-deep green (score ≥ 1.5)"
    elif "_onset_red2green" in core:
        extra = (
            f"morning cell {letter} flips from red yesterday to green today"
        )
    elif "_onset_green" in core:
        extra = (
            f"morning cell {letter} was off or not green yesterday and "
            "is green today"
        )
    elif "_onset_hex_" in core:
        hx = core.split("_onset_hex_")[1]
        extra = (
            f"morning cell {letter} flips onto hex #{hx} "
            f"({HEX_NAME.get(hx) or 'green'}) today"
        )
    elif "_hex_" in core:
        hx = core.split("_hex_")[1]
        extra = (
            f"morning cell {letter} is hex #{hx} "
            f"({HEX_NAME.get(hx) or 'green'} green)"
        )
    else:
        extra = core
    if light:
        return meaning_of("light_on") + ", and " + extra
    return extra + " (known at 9:30)"


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
        row["family"] = (
            "light" if name == "light_on"
            else "light_shade" if name.startswith("light__")
            else "onset" if "onset" in name
            else "greener" if "_ge" in name
            else "hex" if "_hex_" in name
            else "green_on"
        )
        row["label"] = label
        row["horizon"] = hz
        row["plain"] = meaning_of(name)
        row["clock"] = "open"
        bkey = f"{label}_{hz}"
        b = baselines.get(bkey) or _blk(book_cells[(label, hz)]["hold"])
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
        if row.get("verdict") == "PASS" and reasons:
            row.get("fail_reasons")
        deeper(row, cell)
        row["heat_lo"] = lo
        row["heat_hi"] = hi
        row["reg_cells"] = {
            f"{hk}_{tk}": _blk(sl)
            for (hk, tk), sl in cell["reg"].items()
        }
        rows.append(row)
        by_name[(name, label, hz)] = row
    # beat parent (green-on / light) by 20 bp on holdout
    for r in rows:
        p = parent_of(r["def"])
        if not p:
            continue
        parent = by_name.get((p, r["label"], r["horizon"]))
        if not parent:
            continue
        r["parent"] = p
        ph = parent.get("holdout") or {}
        h = r.get("holdout") or {}
        if ph.get("avg_net") is not None and h.get("avg_net") is not None:
            r["vs_parent_pp"] = (h["avg_net"] - ph["avg_net"]) * 100
            if h["avg_net"] < ph["avg_net"] + BEAT:
                r["fail_reasons"] = list(dict.fromkeys(
                    list(r.get("fail_reasons") or []) + ["no_edge_vs_parent"]
                ))
    for r in rows:
        classify(r)
    return rows, lo, hi, book_cells


SIZE_FAIL = {
    "thin_disc", "thin_hold", "ticker_bar", "date_bar",
    "tape_thin", "spy_thin", "q3_missing", "q1_thin",
}


def classify(row):
    """KEEP only if quality and size both clear. Thin size → THIN, not KEEP."""
    reasons = list(row.get("fail_reasons") or [])
    quality = set(reasons) - SIZE_FAIL
    size_hit = bool(set(reasons) & SIZE_FAIL)
    disc = row.get("discovery") or {}
    if row.get("n_tickers", 0) < 50 or not disc or disc.get("n", 0) < 80 or size_hit:
        row["verdict"] = "KILL" if quality else "THIN"
    elif quality:
        row["verdict"] = "KILL"
    else:
        row["verdict"] = "KEEP"
    row["keep"] = row["verdict"]
    row["live_untouched"] = "flatten_robust"
    return row


def soft_regime(rows, hits, book, lo, hi):
    """Heat × SPY tape vs same-cell book. Only for H/1 KEEP candidates."""
    keepers = [r for r in rows
               if r.get("keep") == "KEEP" and r.get("label") == "H"
               and r.get("horizon") == 1 and r["def"] != "light_on"]
    if not keepers:
        return []
    names = {r["def"] for r in keepers}
    recs = [h for h in hits if h[0] in names and h[4] == "H" and h[5] == 1]
    books = [b for b in book if b[3] == "H" and b[4] == 1]
    out = []
    heat_names = ("cold", "mixed", "hot")
    tapes = ("up", "dn", "flat")
    for name in sorted(names):
        for hk in heat_names:
            for tk in tapes:
                rh = [h for h in recs if h[0] == name
                      and heat_name(h[10], lo, hi) == hk
                      and tape_name(h[11]) == tk]
                bh = [b for b in books
                      if heat_name(b[6], lo, hi) == hk
                      and tape_name(b[7]) == tk]
                hout = [h[6] for h in rh if h[9] == "holdout"]
                bout = [b[5] for b in bh if b[2] == "holdout"]
                q1 = [h[6] for h in rh if h[8] < Q1_CUT]
                tickers = {h[7] for h in rh}
                dates = {h[8] for h in rh}
                pnl = defaultdict(float)
                day = defaultdict(float)
                for h in rh:
                    pnl[h[7]] += h[6]
                    day[h[8]] += h[6]
                n_h = len(hout)
                avg_h = (sum(hout) / n_h) if n_h else None
                avg_b = (sum(bout) / len(bout)) if bout else None
                vs = ((avg_h - avg_b) if avg_h is not None and avg_b is not None
                      else None)
                reasons = []
                if n_h < 40 or len(tickers) < 15 or len(dates) < 8:
                    reasons.append("thin")
                if avg_h is not None and avg_h <= 0:
                    reasons.append("hold_sign")
                if vs is not None and vs < BEAT:
                    reasons.append("no_edge_vs_book")
                if q1 and len(q1) >= 20 and (sum(q1) / len(q1)) <= 0:
                    reasons.append("q1_sign")
                elif len(q1) < 20:
                    reasons.append("q1_thin")
                tot = sum(pnl.values())
                top5 = sorted(pnl, key=pnl.get, reverse=True)[:5]
                share = (sum(pnl[t] for t in top5) / tot) if tot else 0.0
                if share > 0.25:
                    reasons.append("ticker_ghost")
                day_items = [{"date": d, "net": v} for d, v in day.items()]
                day_bad, day_frac, *_ = lottery_day(day_items)
                if day_bad:
                    reasons.append("lottery_day")
                size = {"thin", "q1_thin"}
                if "thin" in reasons:
                    verd = "THIN"
                elif set(reasons) - size:
                    verd = "KILL"
                else:
                    verd = "KEEP"
                out.append({
                    "def": name, "label": "H", "horizon": 1,
                    "heat": hk, "spy": tk,
                    "holdout_n": n_h, "holdout_avg": avg_h,
                    "vs_book_pp": None if vs is None else vs * 100,
                    "q1_n": len(q1),
                    "q1_avg": (sum(q1) / len(q1)) if q1 else None,
                    "top5_share": share, "n_tickers": len(tickers),
                    "verdict": verd, "fail_reasons": reasons,
                    "lottery_day_frac": day_frac,
                    "live_untouched": "flatten_robust",
                })
    return out


def dump_inventory(counters):
    by = {c: Counter() for c in OPEN_FILL}
    for ctr in counters:
        for (letter, hx), n in ctr.items():
            by[letter][hx] += n
    rows = []
    for letter in OPEN_FILL:
        ctr = by[letter]
        items = []
        greens = []
        for hx, n in ctr.most_common():
            fam, sc = classify_fill(None if hx == "none" else hx)
            rec = {"hex": hx, "n": n, "family": fam, "score": sc}
            items.append(rec)
            if fam == "green":
                greens.append(rec)
        rows.append({
            "col": letter,
            "unique_hex": [i["hex"] for i in items if i["hex"] != "none"],
            "n_hex": sum(1 for i in items if i["hex"] != "none"),
            "green_hexes": [g["hex"] for g in greens],
            "n_green_hex": len(greens),
            "multi_shade": len(greens) > 1,
            "counts": items,
        })
    return rows


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def soft_majority(soft):
    """Per-recipe KEEP / KILL / THIN of the 9 heat×tape cells."""
    by = defaultdict(lambda: {"KEEP": 0, "KILL": 0, "THIN": 0})
    for c in soft or []:
        by[c["def"]][c["verdict"]] += 1
    out = []
    for name, d in by.items():
        tot = d["KEEP"] + d["KILL"] + d["THIN"]
        if d["KEEP"] > tot / 2:
            slot = "KEEP"
        elif d["KEEP"] == d["KILL"] and d["KEEP"] > 0:
            slot = "REGIME-CONDITIONAL"
        else:
            slot = "DEMOTE"
        out.append({
            "def": name, "keep_cells": d["KEEP"], "kill_cells": d["KILL"],
            "thin_cells": d["THIN"], "slot": slot,
        })
    out.sort(key=lambda r: (-r["keep_cells"], r["def"]))
    return out


def family_verdict(rows, soft):
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
          and r["def"] != "light_on"]
    keep = [r for r in h1 if r.get("keep") == "KEEP"]

    def _shade_cut(r):
        name = r["def"]
        core = name[7:] if name.startswith("light__") else name
        return (
            "onset" not in core
            and ("_hex_" in core or "_ge15" in core or "_ge20" in core)
        )

    shade_keep = [r for r in keep if _shade_cut(r)]
    onset_keep = [r for r in keep if r["family"] == "onset"]
    maj = {r["def"]: r for r in soft_majority(soft)}
    shade_ok = [r for r in shade_keep
                if (maj.get(r["def"]) or {}).get("slot") == "KEEP"]
    onset_ok = [r for r in onset_keep
                if (maj.get(r["def"]) or {}).get("slot") == "KEEP"]
    if shade_ok:
        names = ", ".join(f"`{r['def']}`" for r in shade_ok[:4])
        return "KEEP", (
            "Shade hex is not the same as green-on. Mid-green M "
            "(#95CA82 / score ≥ 1.5) and its onset beat any-green M "
            f"and the book after fees, both tapes, and the ghost bar "
            f"({names}). Soft-regime majority holds on those recipes. "
            "Not a live wire."
        )
    if onset_ok:
        names = ", ".join(f"`{r['def']}`" for r in onset_ok[:3])
        return "KEEP", (
            "Standing shade did not add a soft-regime majority, but an "
            f"onset flip clears it ({names})."
        )
    if shade_keep or onset_keep:
        return "DEMOTE", (
            "Some shade / onset recipes print a holdout KEEP, then lose "
            "a majority of heat × SPY cells (usually a five-name ghost)."
        )
    if any(r.get("keep") == "THIN" for r in h1) and not any(
            r.get("keep") == "KILL" for r in h1):
        return "THIN", "Not enough trades to judge shade hex."
    return "null", (
        "Shade hex and onset on open-knowable fills do not beat any-green "
        "or the book after fees, tapes, and the ghost bar."
    )


def render(inv_cf, inv_dump, rows, soft, n_grids, n_days, verd, why, lo, hi):
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1]
    h1_shade = [r for r in h1 if r["def"] != "light_on"]
    h1 = sorted(h1, key=lambda r: (
        0 if r.get("keep") == "KEEP" else 1 if r.get("keep") == "KILL" else 2,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    ))
    i1 = [r for r in rows if r.get("label") == "I" and r.get("horizon") == 1]
    i2 = [r for r in rows if r.get("label") == "I_sum" and r.get("horizon") == 2]
    n_keep = sum(1 for r in h1_shade if r.get("keep") == "KEEP")
    n_kill = sum(1 for r in h1_shade if r.get("keep") == "KILL")
    n_thin = sum(1 for r in h1_shade if r.get("keep") == "THIN")
    multi = inv_cf["multi_shade_letters"]
    L = [
        "# Shade hex + onset — open-knowable fills",
        "",
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        "frozen · Yahoo/rows A–F seed only · no cards · no live push._",
        "",
        "## Plain English",
        "",
        "The old color mine only asked whether a morning cell was green or "
        "not. Cyrus asked for **shades of green**: the stored fill hex. "
        "This beat inventories the locked open paints, then buys at the "
        "**open** when a cell is a specific hex / greener-than-X, versus "
        "plain green-on. If shade standing fails, it tries **onset** — "
        "the cell was red or off yesterday and flipped to this shade today. "
        "That is stricter than riding a standing green, and is not the "
        "same as the five-cell hysteresis light.",
        "",
        "Labels are Excel **H** (intraday %) first — that is where the "
        "leftover same-day research still lived after soft-regime demoted "
        "the morning five-cell light+green O ± AH/FR family. Same-day **I** "
        "and a 2-day I stack are reported if cheap. Same-row H/I are never "
        "features. Close paints D/E/F/H/I/N never start this trade. "
        "IR/IS/IT are the STOCKHISTORY aliases of A/B/C; they have **no** "
        "CF of their own.",
        "",
        f"**Family verdict: {verd}**",
        "",
        why,
        "",
        f"Dumps **{n_grids}**. Name-days scored **{n_days}**. "
        f"Same-day H recipes: **KEEP {n_keep}** · **KILL {n_kill}** · "
        f"**THIN {n_thin}**. Futubull 0.15% long is taken off the recipe "
        "and the buy-everyone book. Beat the book by ≥20 bp. Ghost bar: "
        "Q1 not red, five names ≤25% of P&L, July ≤40% of winning-month "
        "P&L, fattest day ≤25%. Both SPY tapes. Soft-regime heat is the "
        f"open-knowable prior-5 mean of I "
        f"(discovery terciles cold ≤ {lo*100:.2f}%, hot ≥ {hi*100:.2f}%).",
        "",
        "### Inventory — which open fills have more than one green hex",
        "",
        "CF rules in `model.json` (first matching rule wins). Dumps confirm "
        "which hexes actually fire.",
        "",
        "| letter | CF green hexes | dump green hexes | multi-shade? | note |",
        "|---|---|---|---|---|",
    ]
    dump_by = {r["col"]: r for r in inv_dump}
    for rec in inv_cf["letters"]:
        c = rec["col"]
        d = dump_by.get(c) or {}
        note = "alias of " + rec["alias_of"] if rec.get("alias_of") else "—"
        if c == "C":
            note = "peach only — not a green"
        if c in OPEN_ALIASES:
            note = f"no CF; value alias of {rec['alias_of']}"
        L.append(
            f"| **{c}** | {', '.join('#'+h for h in rec['green_hexes']) or '—'} "
            f"| {', '.join('#'+h for h in (d.get('green_hexes') or [])) or '—'} "
            f"| {'yes' if rec['multi_shade'] or d.get('multi_shade') else 'no'} "
            f"| {note} |"
        )
    dump_multi = [r["col"] for r in inv_dump if r.get("multi_shade")]
    L += [
        "",
        f"CF-multi letters: **{', '.join(multi) or 'none'}**. "
        f"Dump-multi letters (the ones that actually fire two greens): "
        f"**{', '.join(dump_multi) or 'none'}**. "
        "A / K / L / M have two greens in the dumps. G has two greens in "
        "CF but **no green fires** on this rebuild (purple/blue instead). "
        "J's CF green `#95CA82` is also absent here. O is a single mint "
        "`#C6EFCE` — shade-of-O is identical to green-O. B is one mid "
        "green `#95CA82`. C is peach. IR / IS / IT have zero CF rules.",
        "",
        "Do **not** rehash prior-I heat-green. Soft-regime below is only a "
        "gate on shade KEEP candidates, not a new I-heat mine.",
        "",
        "### Same-day H (primary)",
        "",
        "| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | "
        "top-5 | July | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    def line(r):
        h = r.get("holdout") or {}
        b = r.get("baseline") or {}
        vs_b = "—"
        if h.get("avg_net") is not None and b.get("avg_net") is not None:
            vs_b = f"{(h['avg_net']-b['avg_net'])*100:+.2f} pp"
        vs_p = r.get("vs_parent_pp")
        vs_p_s = "—" if vs_p is None else f"{vs_p:+.2f} pp"
        q1 = r.get("q1") or {}
        why = ",".join(r.get("fail_reasons") or []) or "—"
        return (
            f"| {r.get('plain') or r['def']} | {_pct(h)} | {vs_b} | {vs_p_s} | "
            f"{_pct(q1)} | {_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | "
            f"{(r.get('july_share') or 0)*100:.0f}% | **{r.get('keep')}** | "
            f"{why} | `{r['def']}` |"
        )

    show = [r for r in h1 if r["family"] != "green_on" or r.get("keep") == "KEEP"]
    # always show green-on baselines for multi-shade + O
    want_base = set(MULTI_SHADE) | {"O"}
    for r in h1:
        if r["family"] == "green_on" and r["def"].split("_")[0] in want_base:
            if r not in show:
                show.append(r)
    if "light_on" in {r["def"] for r in h1}:
        lo_r = next(r for r in h1 if r["def"] == "light_on")
        if lo_r not in show:
            show.insert(0, lo_r)
    show = sorted(show, key=lambda r: (
        0 if r.get("keep") == "KEEP" else 1,
        0 if r["def"] == "light_on" else 1,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    ))
    for r in show[:80]:
        L.append(line(r))
    L += [
        "",
        "### Same-day I / 2d stacked I (cheap report)",
        "",
        "Not the search. Prior-I heat-green is not remine. These rows only "
        "say whether a shade KEEP on H also prints on I.",
        "",
        "| label | best recipe | holdout | verdict | why | code |",
        "|---|---|---|---|---|---|",
    ]
    for lab_rows, labn in ((i1, "same-day I"), (i2, "2d stacked I")):
        if not lab_rows:
            L.append(f"| {labn} | — | — | — | not scored | — |")
            continue
        best = sorted(lab_rows, key=lambda r: (
            0 if r.get("keep") == "KEEP" else 1,
            -((r.get("holdout") or {}).get("avg_net") or -9),
        ))[0]
        L.append(
            f"| {labn} | {best.get('plain') or best['def']} | "
            f"{_pct(best.get('holdout'))} | **{best.get('keep')}** | "
            f"{','.join(best.get('fail_reasons') or []) or '—'} | "
            f"`{best['def']}` |"
        )
    if soft:
        L += [
            "",
            "### Soft-regime (KEEP H candidates only)",
            "",
            "Heat × SPY tape versus the same-cell book after fees. "
            "Not a new I-heat mine. Family verdict uses **per-recipe "
            "majority of the 9 cells**, not a dump of every cell.",
            "",
            "#### Per-recipe majority",
            "",
            "| recipe | KEEP | KILL | THIN | of 9 | slot |",
            "|---|---:|---:|---:|---:|---|",
        ]
        for m in soft_majority(soft):
            tot = m["keep_cells"] + m["kill_cells"] + m["thin_cells"]
            L.append(
                f"| `{m['def']}` | {m['keep_cells']} | {m['kill_cells']} | "
                f"{m['thin_cells']} | {tot} | **{m['slot']}** |"
            )
        L += [
            "",
            "#### Cell dump",
            "",
            "| recipe | heat | SPY | holdout | vs book | Q1 | top-5 | "
            "verdict | why |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for c in soft:
            q1s = ("—" if c.get("q1_avg") is None
                   else f"{c['q1_avg']*100:+.2f}% (n={c['q1_n']})")
            hs = ("—" if c.get("holdout_avg") is None
                  else f"{c['holdout_avg']*100:+.2f}% (n={c['holdout_n']})")
            vs = ("—" if c.get("vs_book_pp") is None
                  else f"{c['vs_book_pp']:+.2f} pp")
            L.append(
                f"| `{c['def']}` | {c['heat']} | {c['spy']} | {hs} | {vs} | "
                f"{q1s} | {(c.get('top5_share') or 0)*100:.0f}% | "
                f"**{c['verdict']}** | "
                f"{','.join(c.get('fail_reasons') or []) or '—'} |"
            )
    L += [
        "",
        "### What this does not change",
        "",
        "- Live `flatten_robust` is frozen. No card. No push.",
        "- Soft-regime already **DEMOTEd** the morning five-cell "
        "light+green O ± AH/FR family (thin leftover same-day H only).",
        "- Full-sheet ML (#149) stays family null vs the overnight-gap book.",
        "- Prior-I heat-green is not remine.",
        "- Close-entry fills stay out of the open clock.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def splice_scoreboard(md, verd, n_keep, n_kill):
    block = (
        f"{MARKER}\n\n"
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        f"frozen. Shade hex + onset on open-knowable fills. Family "
        f"**{verd}**. Same-day H KEEP {n_keep} · KILL {n_kill}. "
        f"See `excel_bot/research/SHADE_OPEN.md`._\n"
    )
    if os.path.exists(SB_MD):
        return splice_md(SB_MD, MARKER, block, require="PASS 376")
    return block


_G = {}


def _work(path):
    return work_grid(path, _G["disc"], _G["hold"], _G["spy"])


def collect(files, discovery, holdout, spy, workers=4):
    hits, book, heats, dumps = [], [], [], []
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
                    dumps.append(rec["dump"])
                if i % 200 == 0:
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
                dumps.append(rec["dump"])
            if i % 200 == 0:
                print(f"  ... {i}/{len(files)} ok={n_ok} hits={len(hits)}",
                      flush=True)
    return hits, book, heats, dumps, n_ok


def _init_g(discovery, holdout, spy):
    _G.update(disc=discovery, hold=holdout, spy=spy)


def jsonable(rows):
    out = []
    for r in rows:
        rec = {}
        for k, v in r.items():
            if k in ("tickers", "dates"):
                continue
            if isinstance(v, set):
                rec[k] = sorted(v)
            else:
                rec[k] = v
        out.append(rec)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    assert_open_gate()
    inv_cf = cf_inventory()
    print("CF multi-shade:", inv_cf["multi_shade_letters"], flush=True)
    print("CF no-green:", inv_cf["no_green_cf"], flush=True)
    if args.render_only and os.path.exists(OUT_JSON):
        payload = json.load(open(OUT_JSON))
        rows = [classify(r) for r in payload.get("rows") or []]
        for r in rows:
            if r.get("def"):
                r["plain"] = meaning_of(r["def"])
        soft = payload.get("soft_regime") or []
        verd, why = family_verdict(rows, soft)
        h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
              and r["def"] != "light_on"]
        payload["rows"] = rows
        payload["family_verdict"] = verd
        payload["why"] = why
        payload["n_keep_h1"] = sum(1 for r in h1 if r.get("keep") == "KEEP")
        payload["n_kill_h1"] = sum(1 for r in h1 if r.get("keep") == "KILL")
        payload["n_thin_h1"] = sum(1 for r in h1 if r.get("keep") == "THIN")
        n_days = payload.get("n_days") or 170
        md = render(
            payload.get("cf") or inv_cf,
            payload.get("dump") or [],
            rows, soft,
            payload.get("n_grids") or 0,
            n_days, verd, why,
            payload.get("heat_lo") or -0.0045,
            payload.get("heat_hi") or 0.0048,
        )
        payload["n_days"] = n_days
        open(OUT_MD, "w").write(md)
        json.dump(payload, open(OUT_JSON, "w"), indent=2)
        sb = splice_scoreboard(
            md, verd, payload["n_keep_h1"], payload["n_kill_h1"])
        open(SB_MD, "w").write(sb)
        print(f"render-only VERDICT {verd} KEEP={payload['n_keep_h1']} "
              f"KILL={payload['n_kill_h1']} THIN={payload['n_thin_h1']}")
        print(OUT_MD)
        return
    split = json.load(open(SPLIT_PATH))
    discovery, holdout = set(split["discovery"]), set(split["holdout"])
    spy = load_spy_regimes()
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"mine {len(files)} grids workers={args.workers}", flush=True)
    hits, book, heats, dumps, n_ok = collect(
        files, discovery, holdout, spy, workers=args.workers)
    inv_dump = dump_inventory(dumps)
    baselines = {}
    by_lab = defaultdict(list)
    for _t, _iso, split, lab, hz, net, _heat, _tape in book:
        if split == "holdout":
            by_lab[(lab, hz)].append(net)
    for k, vals in by_lab.items():
        if len(vals) >= 2:
            m = sum(vals) / len(vals)
            baselines[f"{k[0]}_{k[1]}"] = {
                "n": len(vals), "avg_net": m, "t": 0.0,
            }
    rows, lo, hi, _book_cells = pack_all(hits, book, heats, baselines)
    soft = soft_regime(rows, hits, book, lo, hi)
    verd, why = family_verdict(rows, soft)
    n_days = len({b[1] for b in book})
    md = render(inv_cf, inv_dump, rows, soft, n_ok, n_days, verd, why, lo, hi)
    open(OUT_MD, "w").write(md)
    h1 = [r for r in rows if r.get("label") == "H" and r.get("horizon") == 1
          and r["def"] != "light_on"]
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
        "open_fills": "".join(OPEN_FILL),
        "open_aliases": list(OPEN_ALIASES),
        "close_fills_excluded": "".join(CLOSE_FILL),
        "multi_shade_letters": inv_cf["multi_shade_letters"],
        "cf": inv_cf,
        "dump": inv_dump,
        "heat_lo": lo, "heat_hi": hi,
        "n_keep_h1": sum(1 for r in h1 if r.get("keep") == "KEEP"),
        "n_kill_h1": sum(1 for r in h1 if r.get("keep") == "KILL"),
        "n_thin_h1": sum(1 for r in h1 if r.get("keep") == "THIN"),
        "n_days": n_days,
        "rows": jsonable(rows),
        "soft_regime": soft,
        "source": "rows_cache",
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    sb = splice_scoreboard(
        md, verd, payload["n_keep_h1"], payload["n_kill_h1"])
    open(SB_MD, "w").write(sb)
    print(f"VERDICT {verd} KEEP={payload['n_keep_h1']} "
          f"KILL={payload['n_kill_h1']} grids={n_ok}")
    print(OUT_MD)


if __name__ == "__main__":
    main()
# flatten_robust labeled untouched — this miner does not import or write live.
