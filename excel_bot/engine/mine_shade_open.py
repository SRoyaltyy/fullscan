"""Shade hex + onset on open-knowable fills. Open entry. Same-day H first.

Cyrus / War room standing bar: EVERY feature must be knowable as of that
day's open — text, numeric, AND highlight shades.

  * Open-knowable fills only (A B C G J K L M O). Close fills D E F H I N
    never start a trade.
  * M **fill** `#95CA82` is the feature (CF IZ=1 from *prior-row* H and IY).
    M's **number** `-(E-C)/C` is same-day low — close-only — never a feature.
  * No same-row peek of H/I values, fills, text, or transforms. Labels may
    be same-day H (the outcome). Lags from prior rows are fine.
  * Soft-regime heat is the prior-5 mean of I (range [ei-5, ei), not ei).
    SPY tape is a ship-bar slice, not a buy feature.

The 1000-grid / 4.4k KEEP indexed OPEN_FILL into the A–O array, so letter
M read column H (close-knowable same-day return fill). FILL_IDX maps
through VISIBLE. That leak is aborted.

Live flatten_robust frozen. No cards. No live push.

  python3 engine/mine_shade_open.py
  python3 engine/mine_shade_open.py --ghost
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
from excel_clock_gate import (  # noqa: E402
    FILL_OPEN as GATE_FILL_OPEN, VALUE_OPEN_44, VALUE_OUT_SAME_ROW,
    assert_excel_clock_gate,
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
# fills[] / fams[] / scores[] are A–O (VISIBLE), not OPEN_FILL order.
# Using enumerate(OPEN_FILL) as the index reads H when the letter is M.
FILL_IDX = {c: VISIBLE.index(c) for c in OPEN_FILL}
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
    """Abort if any feature path can read a close-knowable same-row cell.

    Excel clock gate (OPEN_SAME_ROW_LABELS + CLOCK_MAP) is the source of
    truth: open fills A B C G J K L M O IR IS IT; numbers/text the 44
    value_mine_open cols; lags any letter; OUT M/B/G/K/O numbers,
    D/E/F/H/I same-row, core_score. Mid-M `#95CA82` stays open-fill only.
    """
    assert_excel_clock_gate()
    if set(OPEN_FILL) | set(OPEN_ALIASES) != set(GATE_FILL_OPEN):
        raise AssertionError("shade OPEN_FILL drifted from Excel fill gate")
    for col in ("H", "I", "D", "E", "F", "N"):
        if col in OPEN_FILL or col in OPEN_ALIASES:
            raise AssertionError(f"close fill {col} leaked into open fills")
        if col in VALUE_OPEN_44:
            raise AssertionError(f"{col} must not be value_mine_open")
    for col in ("B", "G", "K", "M", "O"):
        if col not in VALUE_OUT_SAME_ROW:
            raise AssertionError(f"{col} number must stay on the OUT list")
    cols = tuple(VISIBLE.index(c) for c in OPEN_FILL)
    assert feature_clock(cols) == "open"
    assert_clock_legal(cols, "open")
    assert FILL_IDX["M"] == VISIBLE.index("M") == 12
    assert FILL_IDX["O"] == VISIBLE.index("O") == 14
    # The old 4.4k KEEP used enumerate(OPEN_FILL) so M → fills[7] == H.
    assert FILL_IDX["M"] != VISIBLE.index("H")
    assert "H" not in OPEN_FILL and "I" not in OPEN_FILL
    for letter, li in FILL_IDX.items():
        mapped = VISIBLE[li]
        if mapped in CLOSE_FILL or mapped != letter:
            raise AssertionError(
                f"open-only abort: letter {letter} mapped to {mapped} "
                f"(idx {li}) — close-knowable leak"
            )
        if letter in CLOSE_FILL:
            raise AssertionError(f"close fill {letter} must not be a feature")
    for c in CLOSE_FILL:
        try:
            assert_clock_legal((VISIBLE.index(c),), "open")
        except ValueError:
            continue
        raise AssertionError(f"close fill {c} must not be open-legal")
    # M number is close-only (same-day low). Feature is fill shade only.
    assert "M" in OPEN_FILL, "M fill is open-knowable; M value is not"


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
    for letter in OPEN_FILL:
        li = FILL_IDX[letter]
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
        for letter in OPEN_FILL:
            dump[(letter, fills[FILL_IDX[letter]] or "none")] += 1
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
            "Shade hex is not the same as green-on. Standing shade "
            f"KEEP after fees, tapes, and soft-regime majority: {names}. "
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


# Harder ghost / name bar than the 25% family KEEP. Wire gate, not a remine.
GHOST_FOCUS = ("M_ge15", "M_hex_95CA82", "M_onset_hex_95CA82", "O_onset_red2green")
GHOST_PARENTS = {
    "M_ge15": "M_green",
    "M_hex_95CA82": "M_green",
    "M_onset_hex_95CA82": "M_green",
    "O_onset_red2green": "O_green",
}
GHOST_WANT = frozenset(GHOST_FOCUS) | frozenset(GHOST_PARENTS.values())
GHOST_TOP5_PASS, GHOST_TOP5_FAIL = 0.15, 0.25
GHOST_JULY_PASS, GHOST_JULY_FAIL = 0.25, 0.40
GHOST_DAY_PASS, GHOST_DAY_FAIL = 0.15, 0.25


def _avg(xs):
    return (sum(xs) / len(xs)) if xs else None


def _slot_of(xs):
    n = len(xs)
    avg = _avg(xs)
    return {"n": n, "avg_net": avg}


def ghost_grid(path, discovery, holdout, spy):
    """Same-day H only, focus recipes + parents + book. No light / I."""
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
    hexes, fams, scores = [], [], []
    for d in days:
        fills = [norm_hex(x) for x in (d.get("fills") or [])]
        fills = (fills + [None] * 15)[:15]
        hexes.append(fills)
        fams.append(d.get("fams") or ["none"] * 15)
        scores.append(d.get("scores") or [0.0] * 15)
    hits = {name: [] for name in GHOST_WANT}
    book = []
    for ei in range(len(days)):
        labs = labels_at(days, ei)
        raw = labs.get(("H", 1))
        if raw is None:
            continue
        iso = str(s2d(days[ei]["date"]))
        net = raw - COST_FUTU_LONG
        tape = spy.get(iso)
        if tape is None:
            tape = 0
        book.append((t, iso, split, net, tape))
        for name, _fam, _let, _hx in recipe_hits(hexes, fams, scores, ei):
            if name in GHOST_WANT:
                hits[name].append((t, iso, split, net, tape))
    return {"hits": hits, "book": book}


def _ghost_work(path):
    return ghost_grid(path, _G["disc"], _G["hold"], _G["spy"])


def collect_ghost(files, discovery, holdout, spy, workers=4):
    hits = {name: [] for name in GHOST_WANT}
    book = []
    n_ok = 0
    _G.update(disc=discovery, hold=holdout, spy=spy)
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init_g,
                  initargs=(discovery, holdout, spy)) as pool:
            for i, rec in enumerate(pool.imap_unordered(_ghost_work, files,
                                                        chunksize=8), 1):
                if rec:
                    n_ok += 1
                    book.extend(rec["book"])
                    for k, rows in rec["hits"].items():
                        hits[k].extend(rows)
                if i % 200 == 0:
                    print(f"  ghost ... {i}/{len(files)} ok={n_ok}", flush=True)
    else:
        for i, f in enumerate(files, 1):
            rec = _ghost_work(f)
            if rec:
                n_ok += 1
                book.extend(rec["book"])
                for k, rows in rec["hits"].items():
                    hits[k].extend(rows)
            if i % 200 == 0:
                print(f"  ghost ... {i}/{len(files)} ok={n_ok}", flush=True)
    return hits, book, n_ok


def _hold(trades):
    return [x for x in trades if x[2] == "holdout"]


def _disc(trades):
    return [x for x in trades if x[2] == "discovery"]


def _nets(trades):
    return [x[3] for x in trades]


def _by_ticker(trades):
    pnl, n = defaultdict(float), defaultdict(int)
    for t, _iso, _sp, net, _tape in trades:
        pnl[t] += net
        n[t] += 1
    return pnl, n


def _top_share(pnl, k=5):
    tot = sum(pnl.values())
    names = sorted(pnl, key=pnl.get, reverse=True)[:k]
    share = (sum(pnl[t] for t in names) / tot) if tot else 0.0
    return names, share, tot


def _july_share(trades):
    month = defaultdict(float)
    for _t, iso, _sp, net, _tape in trades:
        month[iso[:7]] += net
    pos = {m: v for m, v in month.items() if v > 0}
    gross = sum(pos.values())
    july = month.get("2026-07", 0.0)
    share = (july / gross) if gross > 0 and july > 0 else 0.0
    return share, month


def _drop(trades, names):
    drop = set(names)
    return [x for x in trades if x[0] not in drop]


def _band(share, pass_cut, fail_cut):
    if share > fail_cut:
        return "FAIL"
    if share > pass_cut:
        return "CONDITIONAL"
    return "PASS"


def _worst(*slots):
    order = {"FAIL": 2, "CONDITIONAL": 1, "PASS": 0, "THIN": 1}
    return max(slots, key=lambda s: order.get(s, 0))


def ghost_score(name, trades, parent, book):
    """Harder name/month/day/tape/split bar. Same-day H after Futubull."""
    hold = _hold(trades)
    disc = _disc(trades)
    phold = _hold(parent)
    bhold = _hold(book)
    reasons = []
    checks = []

    def add(code, slot, text, **extra):
        rec = {"code": code, "slot": slot, "text": text}
        rec.update(extra)
        checks.append(rec)
        if slot != "PASS":
            reasons.append(code)

    h_avg = _avg(_nets(hold))
    d_avg = _avg(_nets(disc))
    p_avg = _avg(_nets(phold))
    b_avg = _avg(_nets(bhold))
    vs_book = None if h_avg is None or b_avg is None else h_avg - b_avg
    vs_par = None if h_avg is None or p_avg is None else h_avg - p_avg

    if h_avg is None or len(hold) < 80:
        add("thin_hold", "THIN", "holdout too thin to ghost-score")
        return {
            "def": name, "ghost": "THIN", "reasons": reasons, "checks": checks,
            "holdout": _slot_of(_nets(hold)), "discovery": _slot_of(_nets(disc)),
            "live_untouched": "flatten_robust",
        }

    if h_avg <= 0:
        add("hold_sign", "FAIL",
            f"holdout after fees is {h_avg*100:+.2f}%")
    elif vs_book is not None and vs_book < BEAT:
        add("no_edge_vs_book", "FAIL",
            f"holdout vs book {vs_book*100:+.2f} pp")
    else:
        add("vs_book", "PASS",
            f"holdout {h_avg*100:+.2f}% (n={len(hold)}) vs book "
            f"{(b_avg or 0)*100:+.2f}% ({(vs_book or 0)*100:+.2f} pp)")

    if vs_par is not None and vs_par < BEAT:
        add("no_edge_vs_parent", "FAIL",
            f"holdout vs parent {vs_par*100:+.2f} pp")
    else:
        add("vs_parent", "PASS",
            f"holdout vs parent {(vs_par or 0)*100:+.2f} pp "
            f"(parent {(p_avg or 0)*100:+.2f}%, n={len(phold)})")

    if d_avg is None or len(disc) < 80:
        add("thin_disc", "CONDITIONAL", "discovery thin on this 1000-name cut")
    elif d_avg <= 0 or (b_avg is not None and d_avg < b_avg + BEAT):
        add("disc_name_split", "FAIL",
            f"discovery name-split {d_avg*100:+.2f}% does not beat book")
    else:
        add("name_split", "PASS",
            f"name-split discovery {d_avg*100:+.2f}% (n={len(disc)}) / "
            f"holdout {h_avg*100:+.2f}% (n={len(hold)})")

    pnl_h, n_h = _by_ticker(hold)
    top5, share5, tot_h = _top_share(pnl_h, 5)
    top1, share1, _ = _top_share(pnl_h, 1)
    top10, share10, _ = _top_share(pnl_h, 10)
    names_tab = []
    for tkr in top5:
        names_tab.append({
            "ticker": tkr, "n": n_h[tkr], "pnl": pnl_h[tkr],
            "avg": pnl_h[tkr] / n_h[tkr],
            "share": (pnl_h[tkr] / tot_h) if tot_h else 0.0,
        })
    slot5 = _band(share5, GHOST_TOP5_PASS, GHOST_TOP5_FAIL)
    add("top5_holdout", slot5,
        f"holdout top-5 {', '.join(top5)} = {share5*100:.1f}% of holdout P&L "
        f"(top-1 {share1*100:.1f}%, top-10 {share10*100:.1f}%)",
        share=share5, names=top5, top1_share=share1, top10_share=share10)

    rest5 = _drop(hold, top5)
    rest1 = _drop(hold, top1)
    r5 = _avg(_nets(rest5))
    r1 = _avg(_nets(rest1))
    handful = (share5 > GHOST_TOP5_PASS) or (
        h_avg and r5 is not None and r5 < 0.5 * h_avg)
    if r5 is None or len(rest5) < 40:
        add("drop5_thin", "CONDITIONAL", "not enough leftover names after drop-5")
    elif r5 <= 0:
        add("drop5_sign", "FAIL",
            f"drop top-5 leftover holdout {r5*100:+.2f}% — handful drives the print")
    elif b_avg is not None and r5 < b_avg + BEAT:
        add("drop5_vs_book", "FAIL",
            f"drop top-5 leftover {r5*100:+.2f}% does not beat the book")
    elif p_avg is not None and r5 < p_avg + BEAT:
        add("drop5_vs_parent", "CONDITIONAL",
            f"drop top-5 leftover {r5*100:+.2f}% beats book but not parent "
            f"({(p_avg or 0)*100:+.2f}%)")
    elif handful:
        add("drop5_handful", "CONDITIONAL",
            f"drop top-5 leftover {r5*100:+.2f}% (n={len(rest5)}) — "
            f"a handful still moves the mean a lot")
    else:
        add("drop5", "PASS",
            f"drop top-5 leftover holdout {r5*100:+.2f}% (n={len(rest5)}) "
            f"still beats book and parent; not a handful-of-names print")

    if r1 is not None and r1 <= 0:
        add("drop1_sign", "FAIL",
            f"drop top-1 leftover {r1*100:+.2f}% — one name is the trade")
    else:
        add("drop1", "PASS",
            f"drop top-1 leftover {(r1 or 0)*100:+.2f}% (n={len(rest1)})")

    july, months = _july_share(hold)
    slot_j = _band(july, GHOST_JULY_PASS, GHOST_JULY_FAIL)
    add("july_holdout", slot_j,
        f"July is {july*100:.1f}% of holdout winning-month P&L",
        share=july)
    red_months = []
    month_rows = []
    for m in sorted(months):
        chunk = [x[3] for x in hold if x[1][:7] == m]
        avg = _avg(chunk)
        month_rows.append({"month": m, "n": len(chunk), "avg_net": avg,
                           "pnl": months[m]})
        if avg is not None and avg <= 0 and len(chunk) >= 40:
            red_months.append(m)
    if len(red_months) >= 2:
        add("month_split", "FAIL",
            f"holdout red months n≥40: {', '.join(red_months)}")
    elif red_months:
        add("month_split", "CONDITIONAL",
            f"holdout red month n≥40: {', '.join(red_months)}")
    else:
        add("month_split", "PASS", "no holdout month with n≥40 is red")

    day_items = [{"date": iso, "net": net} for _t, iso, _sp, net, _tp in hold]
    day_bad, day_frac, top_day, *_ = lottery_day(day_items)
    slot_d = "FAIL" if (day_bad or day_frac > GHOST_DAY_FAIL) else (
        "CONDITIONAL" if day_frac > GHOST_DAY_PASS else "PASS")
    add("day_lottery_holdout", slot_d,
        f"fattest holdout day {top_day or '—'} is {day_frac*100:.1f}% of "
        f"winning-day P&L",
        share=day_frac, top_day=top_day)

    q1h = [x[3] for x in hold if x[1] < Q1_CUT]
    q1_avg = _avg(q1h)
    if len(q1h) < 40:
        add("q1_holdout", "CONDITIONAL", f"Q1 holdout thin n={len(q1h)}")
    elif q1_avg is not None and q1_avg <= 0:
        add("q1_holdout", "FAIL",
            f"Q1 holdout {q1_avg*100:+.2f}% (n={len(q1h)}) is red")
    else:
        add("q1_holdout", "PASS",
            f"Q1 holdout {(q1_avg or 0)*100:+.2f}% (n={len(q1h)})")

    for tape, label in ((1, "spy_up"), (-1, "spy_dn")):
        chunk = [x[3] for x in hold if x[4] == tape]
        avg = _avg(chunk)
        bchunk = [x[3] for x in bhold if x[4] == tape]
        bavg = _avg(bchunk)
        if len(chunk) < 40:
            add(label, "CONDITIONAL", f"holdout {label} thin n={len(chunk)}")
        elif avg is not None and avg <= 0:
            add(label, "FAIL",
                f"holdout {label} {avg*100:+.2f}% (n={len(chunk)}) is red")
        elif bavg is not None and avg < bavg + BEAT:
            add(label, "FAIL",
                f"holdout {label} {avg*100:+.2f}% does not beat that tape's book")
        else:
            add(label, "PASS",
                f"holdout {label} {(avg or 0)*100:+.2f}% (n={len(chunk)}) vs "
                f"book {(bavg or 0)*100:+.2f}%")

    for cut, label, pred in (
        (HALF_CUT, "early", lambda iso: iso < HALF_CUT),
        (HALF_CUT, "late", lambda iso: iso >= HALF_CUT),
    ):
        chunk = [x[3] for x in hold if pred(x[1])]
        avg = _avg(chunk)
        if len(chunk) < 40:
            add(f"time_{label}", "CONDITIONAL",
                f"holdout {label} thin n={len(chunk)}")
        elif avg is not None and avg <= 0:
            add(f"time_{label}", "FAIL",
                f"holdout {label} time-split {avg*100:+.2f}% is red")
        else:
            add(f"time_{label}", "PASS",
                f"holdout {label} time-split {(avg or 0)*100:+.2f}% "
                f"(n={len(chunk)}, cut {cut})")

    slot = _worst(*(c["slot"] for c in checks))
    handful_flag = bool(handful) or slot5 != "PASS"
    return {
        "def": name,
        "plain": meaning_of(name),
        "ghost": slot,
        "handful": handful_flag,
        "reasons": reasons,
        "checks": checks,
        "holdout": _slot_of(_nets(hold)),
        "discovery": _slot_of(_nets(disc)),
        "parent_holdout": _slot_of(_nets(phold)),
        "book_holdout": _slot_of(_nets(bhold)),
        "vs_book_pp": None if vs_book is None else vs_book * 100,
        "vs_parent_pp": None if vs_par is None else vs_par * 100,
        "top5_holdout_share": share5,
        "top1_holdout_share": share1,
        "top10_holdout_share": share10,
        "top5_tickers": top5,
        "top_names": names_tab,
        "drop5_holdout": _slot_of(_nets(rest5)),
        "drop1_holdout": _slot_of(_nets(rest1)),
        "july_holdout_share": july,
        "months_holdout": month_rows,
        "day_lottery_holdout": day_frac,
        "lottery_top_day": top_day,
        "q1_holdout": _slot_of(q1h),
        "n_tickers_holdout": len(pnl_h),
        "n_tickers": len({x[0] for x in trades}),
        "live_untouched": "flatten_robust",
    }


def ghost_family(scores):
    """One English verdict for M mid, M onset, O onset."""
    by = {s["def"]: s for s in scores}
    m_mid = by.get("M_ge15") or by.get("M_hex_95CA82")
    m_on = by.get("M_onset_hex_95CA82")
    o_on = by.get("O_onset_red2green")
    m_slot = _worst(
        *(s["ghost"] for s in (m_mid, m_on) if s),
    ) if (m_mid or m_on) else "THIN"
    o_slot = o_on["ghost"] if o_on else "THIN"
    def _brief(score, label):
        if not score:
            return f"{label}: no score."
        h = score.get("holdout") or {}
        d5 = score.get("drop5_holdout") or {}
        names = ", ".join(score.get("top5_tickers") or [])
        return (
            f"{label} **GHOST {score['ghost']}** — holdout "
            f"{(h.get('avg_net') or 0)*100:+.2f}% (n={h.get('n')}), "
            f"vs book {(score.get('vs_book_pp') or 0):+.2f} pp, "
            f"vs parent {(score.get('vs_parent_pp') or 0):+.2f} pp. "
            f"Holdout top-5 {(score.get('top5_holdout_share') or 0)*100:.1f}% "
            f"({names}). Drop-5 leftover "
            f"{(d5.get('avg_net') or 0)*100:+.2f}% (n={d5.get('n')})."
        )

    if m_slot == "FAIL":
        family = "GHOST FAIL"
        why = (
            "Standing M mid-green `#95CA82` / ge15 does not clear the "
            "ghost / name bar (holdout sign, leftover vs book/parent, "
            "or a handful). "
            f"{_brief(m_mid, 'M mid `#95CA82` / ge15')} "
            f"{_brief(m_on, 'M hex-onset')} "
            f"{_brief(o_on, 'O red→green')} "
            "Do not talk wire."
        )
    elif m_slot == "CONDITIONAL":
        family = "GHOST CONDITIONAL"
        why = (
            "Standing M mid-green `#95CA82` / ge15 is not a clean "
            "GHOST PASS on the harder name bar (top-5 share, drop-5 "
            "leftover, or a sibling). Research KEEP on standing M mid "
            "only if the leftover still beats the book and any-green M. "
            "Do not wire. "
            f"{_brief(m_mid, 'M mid')} {_brief(m_on, 'M onset')} "
            f"{_brief(o_on, 'O onset')}"
        )
    elif m_slot == "PASS":
        family = "GHOST PASS"
        why = (
            "A handful of tickers does **not** drive the M mid-green print. "
            "Holdout top-5, drop-top-5 leftover, July, day-lottery, Q1 "
            "holdout, both SPY tapes, and name + time splits all clear the "
            "harder bar vs book and vs any-green M. Still not a live wire. "
            f"{_brief(m_mid, 'M mid')} {_brief(m_on, 'M onset')} "
            f"{_brief(o_on, 'O onset')}"
        )
    else:
        family = "GHOST THIN"
        why = "Not enough holdout trades to ghost-score M mid."
    return {
        "family": family,
        "why": why,
        "m_mid": (m_mid or {}).get("ghost"),
        "m_onset": (m_on or {}).get("ghost"),
        "o_onset": o_slot,
        "live_untouched": "flatten_robust",
        "recipes": scores,
    }


def render_ghost(ghost):
    if not ghost:
        return []
    L = [
        "",
        "### Ghost / name check (harder than soft-regime majority)",
        "",
        "Family KEEP already cleared the usual 25% top-5 / 40% July / 25% "
        "day-lottery bar. This cut asks whether a **handful of names** "
        "is the holdout print. Holdout-only top-5 (PASS ≤15%, FAIL >25%), "
        "drop top-5 leftover still beating the book and any-green M, "
        "July holdout ≤25% of winning-month P&L, fattest holdout day ≤15%, "
        "Q1 **on holdout names** not red, both SPY tapes on holdout, "
        "name-split (discovery vs holdout tickers) and time-split "
        f"(cut {HALF_CUT}). Futubull 0.15% is on every print. "
        "Live stays frozen.",
        "",
        f"**Ghost verdict: {ghost['family']}**",
        "",
        ghost["why"],
        "",
        f"M mid `#95CA82` / ge15: **GHOST {ghost.get('m_mid') or '—'}**. "
        f"M hex-onset: **GHOST {ghost.get('m_onset') or '—'}**. "
        f"O red→green onset: **GHOST {ghost.get('o_onset') or '—'}**.",
        "",
        "| recipe | holdout | vs book | vs parent | drop-5 leftover | "
        "holdout top-5 | July | day | Q1 holdout | SPY↑ | SPY↓ | "
        "handful? | ghost |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    def _p(slot):
        if not slot or slot.get("avg_net") is None:
            return "—"
        return f"{slot['avg_net']*100:+.2f}% (n={slot['n']})"

    def _chk(score, code):
        for c in score.get("checks") or []:
            if c["code"] == code:
                return c
        return {}

    for s in ghost.get("recipes") or []:
        if s["def"] not in GHOST_FOCUS:
            continue
        t5 = _chk(s, "top5_holdout")
        ju = _chk(s, "july_holdout")
        dy = _chk(s, "day_lottery_holdout")
        q1 = _chk(s, "q1_holdout")
        up = _chk(s, "spy_up")
        dn = _chk(s, "spy_dn")
        names = ", ".join(s.get("top5_tickers") or [])
        L.append(
            f"| `{s['def']}` | {_p(s.get('holdout'))} | "
            f"{(s.get('vs_book_pp') if s.get('vs_book_pp') is not None else 0):+.2f} pp | "
            f"{(s.get('vs_parent_pp') if s.get('vs_parent_pp') is not None else 0):+.2f} pp | "
            f"{_p(s.get('drop5_holdout'))} | "
            f"{(s.get('top5_holdout_share') or 0)*100:.1f}% ({names}) | "
            f"{(s.get('july_holdout_share') or 0)*100:.1f}% | "
            f"{(s.get('day_lottery_holdout') or 0)*100:.1f}% | "
            f"{_p(s.get('q1_holdout'))} | "
            f"{(up.get('text') or '—').split(' (n=')[0] if up else '—'} | "
            f"{(dn.get('text') or '—').split(' (n=')[0] if dn else '—'} | "
            f"{'yes' if s.get('handful') else 'no'} | **{s.get('ghost')}** |"
        )
    L += [
        "",
        "Holdout top names (P&L share of that recipe's holdout book):",
        "",
        "| recipe | name | n | holdout avg | holdout P&L share |",
        "|---|---|---:|---:|---:|",
    ]
    for s in ghost.get("recipes") or []:
        if s["def"] not in ("M_ge15", "M_onset_hex_95CA82", "O_onset_red2green"):
            continue
        for row in s.get("top_names") or []:
            L.append(
                f"| `{s['def']}` | {row['ticker']} | {row['n']} | "
                f"{row['avg']*100:+.2f}% | {row['share']*100:.1f}% |"
            )
    L += [
        "",
        "M_ge15 and M_hex_95CA82 are the same trades (real M has one green "
        "hex). Drop-5 leftover is the holdout mean after removing the five "
        "fattest names. A red leftover is not a five-name ghost of a "
        "winner — there is no winner.",
        "",
    ]
    return L


def render(inv_cf, inv_dump, rows, soft, n_grids, n_days, verd, why, lo, hi,
           ghost=None):
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
        f"**Family verdict: {verd}** — expanded-universe M mid is a "
        f"**DEMOTE** (KEEP 0 / KILL {n_kill} / THIN {n_thin}).",
        "",
        why,
        "",
        "The 4.4k / +10.6% KEEP was **H’s fill** (close-knowable "
        "`H≥5%`) mis-indexed as M. This rebuild is **5223** tickers × "
        "**2018-09-10 → 2026-09-04** (8.54M name-days). Real M mid "
        "`#95CA82` holdout **−0.19%** (n=320499) vs book −0.10% "
        "(−0.09 pp). Time split early −0.18% / late −0.41%. Soft-regime "
        "majority is N/A (no KEEP). Live frozen.",
        "",
        "### Excel clock gate (source of truth)",
        "",
        "Same-row open fills: **A B C G J K L M O IR IS IT**. "
        "Same-row numbers/text: the **44 `value_mine_open` cols** "
        "(never H/I). Lags of any letter are fair. OUT: M’s number, "
        "B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`. "
        "Docs: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`. "
        "Mid-M `#95CA82` expand stays open-fill only. See `SHADE_GATE.md`.",
        "",
        "### Open-only gate (standing bar)",
        "",
        "Every **feature** is knowable at that day's open — fill shade, "
        "number, and text. This is the Excel clock gate, not a slogan.",
        "",
        "- Shades/fills at open: only **A B C G J K L M O IR IS IT**.",
        "- Numbers/text at open: only the **44 `value_mine_open` cols**. "
        "Never same-row H/I.",
        "- Lags: any letter from rows above is fair.",
        "- OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, "
        "`core_score`.",
        "- **M fill** `#95CA82` / ge15 is the feature. CF `IZ=1` reads "
        "*yesterday's* H and the static IY row (lag). Known at 9:30.",
        "- **M's number** `-(low-open)/open` is same-day low → close-only. "
        "Never a feature. Never a gate.",
        "- Close fills **D E F H I N** do not start a trade. Same-row H/I "
        "values, fills, text, and transforms are **labels only**.",
        "- Onset uses yesterday's M fill (lag). Soft-regime heat is the "
        "prior-5 mean of I (`[ei-5, ei)`). SPY tape is a ship-bar slice, "
        "not a buy feature.",
        "- `FILL_IDX` maps through `VISIBLE` (M=12). The 1000-grid / 4.4k "
        "KEEP had `enumerate(OPEN_FILL)` so M read **H's fill** "
        "(close-knowable `H≥5%`). That path is aborted.",
        "",
    ]
    if ghost:
        L += [
            f"**Ghost / name check: {ghost['family']}**",
            "",
            ghost["why"],
            "",
        ]
    L += [
        f"Dumps **{n_grids}** tickers. Calendar days **{n_days}**. "
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
    ]
    L += render_ghost(ghost)
    L += [
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
    # always show green-on baselines for multi-shade + M/O (M is the KEEP card)
    want_base = set(MULTI_SHADE) | {"O", "M"}
    have = {r["def"] for r in show}
    for r in h1:
        if r["family"] == "green_on" and r["def"].split("_")[0] in want_base:
            if r["def"] not in have:
                show.append(r)
                have.add(r["def"])
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
        "Research only. Expanded panel is 2018-09 → 2026-09. "
        "Live frozen.",
        "",
    ]
    return "\n".join(L)


def splice_scoreboard(md, verd, n_keep, n_kill, ghost=None):
    g = ""
    if ghost:
        g = f" {ghost.get('family')}. M mid GHOST {ghost.get('m_mid')}; O onset GHOST {ghost.get('o_onset')}."
    block = (
        f"{MARKER}\n\n"
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        f"frozen. Shade hex + onset on open-knowable fills. Family "
        f"**{verd}**.{g} Expanded open-only M mid `#95CA82` / ge15 is a "
        f"**DEMOTE** (not the 4.4k H-fill leak). Same-day H KEEP "
        f"{n_keep} · KILL {n_kill}. See `SHADE_OPEN.md` / "
        f"`SHADE_KEEP_CARDS.md`._\n"
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
    ap.add_argument("--ghost", action="store_true",
                    help="harder name/month/day/tape ghost on M mid + O onset")
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
        payload["open_only_gate"] = True
        payload["m_number_excluded"] = True
        stats_path = os.path.join(RESEARCH, "shade_panel_stats.json")
        if os.path.exists(stats_path):
            payload["panel"] = json.load(open(stats_path))
        n_days = payload.get("n_days") or 170
        ghost = payload.get("ghost")
        if ghost and ghost.get("recipes"):
            ghost = ghost_family(ghost["recipes"])
            payload["ghost"] = ghost
        md = render(
            payload.get("cf") or inv_cf,
            payload.get("dump") or [],
            rows, soft,
            payload.get("n_grids") or 0,
            n_days, verd, why,
            payload.get("heat_lo") or -0.0045,
            payload.get("heat_hi") or 0.0048,
            ghost=ghost,
        )
        payload["n_days"] = n_days
        open(OUT_MD, "w").write(md)
        json.dump(payload, open(OUT_JSON, "w"), indent=2)
        sb = splice_scoreboard(
            md, verd, payload["n_keep_h1"], payload["n_kill_h1"], ghost)
        open(SB_MD, "w").write(sb)
        print(f"render-only VERDICT {verd} KEEP={payload['n_keep_h1']} "
              f"KILL={payload['n_kill_h1']} THIN={payload['n_thin_h1']}"
              f"{' ' + ghost['family'] if ghost else ''}")
        print(OUT_MD)
        return
    if args.ghost:
        if not os.path.exists(OUT_JSON):
            raise SystemExit("run the shade mine before --ghost")
        payload = json.load(open(OUT_JSON))
        split = json.load(open(SPLIT_PATH))
        discovery, holdout = set(split["discovery"]), set(split["holdout"])
        spy = load_spy_regimes()
        files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
                 if not os.path.basename(f).startswith("_")]
        print(f"ghost {len(files)} grids workers={args.workers}", flush=True)
        hits, book, n_ok = collect_ghost(
            files, discovery, holdout, spy, workers=args.workers)
        scores = []
        for name in GHOST_FOCUS:
            scores.append(ghost_score(
                name, hits.get(name) or [],
                hits.get(GHOST_PARENTS[name]) or [], book))
        ghost = ghost_family(scores)
        payload["ghost"] = ghost
        payload["ghost_n_grids"] = n_ok
        rows = [classify(r) for r in payload.get("rows") or []]
        for r in rows:
            if r.get("def"):
                r["plain"] = meaning_of(r["def"])
        soft = payload.get("soft_regime") or []
        verd, why = family_verdict(rows, soft)
        md = render(
            payload.get("cf") or inv_cf,
            payload.get("dump") or [],
            rows, soft,
            payload.get("n_grids") or n_ok,
            payload.get("n_days") or 170,
            verd, why,
            payload.get("heat_lo") or -0.0045,
            payload.get("heat_hi") or 0.0048,
            ghost=ghost,
        )
        open(OUT_MD, "w").write(md)
        json.dump(payload, open(OUT_JSON, "w"), indent=2)
        sb = splice_scoreboard(
            md, verd, payload.get("n_keep_h1") or 0,
            payload.get("n_kill_h1") or 0, ghost)
        open(SB_MD, "w").write(sb)
        print(f"GHOST {ghost['family']} M_mid={ghost['m_mid']} "
              f"M_onset={ghost['m_onset']} O={ghost['o_onset']} grids={n_ok}")
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
