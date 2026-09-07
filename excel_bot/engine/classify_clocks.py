"""Classify every A–JL column: open / close / unknown.

Sources (in order of authority):
  1. timing_test.py / NOTES.md measured fill clocks for A–O
  2. Formula-value caveat in clock.py (G/K/M values are close)
  3. Same-row formula + CF dependency walk on model.json
  4. Optional 1-ticker all-cols perturbation (--measure)

Unknown is not open. Mining treats unknown as CLOSE.
core_score-like (any feature that reads D,E,F,H,I on the same row) = CLOSE.

  python engine/classify_clocks.py
  python engine/classify_clocks.py --measure AAPL
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date

from openpyxl.utils import column_index_from_string, get_column_letter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xlparse import parse  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MODEL = os.path.join(HERE, "model.json")
OUT_JSON = os.path.join(ROOT, "research", "clock_map.json")
OUT_MD = os.path.join(ROOT, "research", "CLOCK_MAP.md")
FULL_COLS = 275

# timing_test / NOTES.md — fills
MEAS_FILL_OPEN = tuple("ABCGJKLMO")
MEAS_FILL_CLOSE = tuple("DEFHIN")
# clock.py value caveat + OHLCV meaning
MEAS_VALUE_OPEN = tuple("ACJ")          # date, open, open-to-open
MEAS_VALUE_CLOSE = tuple("BDEFHIGKMN")  # close/high/low/vol + derived

# STOCKHISTORY daily spill (A–F aliases)
SH_DAILY = {"IR": "A", "IS": "B", "IT": "C", "IU": "D", "IV": "E", "IW": "F"}


def _formula_text(spec):
    if isinstance(spec, dict):
        return spec.get("f")
    return spec


def walk_refs(ast, out):
    if not isinstance(ast, tuple) or not ast:
        return
    kind = ast[0]
    if kind == "ref":
        _sheet, ext, col, row, _ac, _ar = ast[1:]
        out.append((col, row, ext))
        return
    if kind == "range":
        walk_refs(ast[1], out)
        walk_refs(ast[2], out)
        return
    for x in ast[1:]:
        if isinstance(x, tuple):
            walk_refs(x, out)
        elif isinstance(x, list):
            for y in x:
                walk_refs(y, out)


def refs_of(formula):
    if not formula or not str(formula).startswith("="):
        return []
    try:
        ast = parse(formula)
    except Exception:
        return None  # unparsed
    out = []
    walk_refs(ast, out)
    return out


def _split_coord(coord):
    i = 0
    while i < len(coord) and coord[i].isalpha():
        i += 1
    return coord[:i], int(coord[i:])


def load_model(path=MODEL):
    m = json.load(open(path))
    formulas = m.get("formulas") or {}
    by_col = defaultdict(list)
    for coord, spec in formulas.items():
        col, row = _split_coord(coord)
        by_col[col].append((row, _formula_text(spec)))
    for col in by_col:
        by_col[col].sort()
    cf = []
    for rule in m.get("cf_rules") or []:
        f = rule.get("formula") or rule.get("f") or ""
        if not f and isinstance(rule.get("formulas"), list):
            f = rule["formulas"][0] if rule["formulas"] else ""
        cf.append({
            "sqref": rule.get("sqref") or "",
            "formula": f or rule.get("expression") or "",
            "type": rule.get("type"),
        })
    return m, dict(by_col), cf


def cf_cols(sqref):
    cols = set()
    for part in (sqref or "").replace("$", "").split():
        a, b = part.split(":")[0], part.split(":")[-1]
        c1, c2 = _split_coord(a)[0], _split_coord(b)[0]
        i1, i2 = column_index_from_string(c1), column_index_from_string(c2)
        for i in range(i1, i2 + 1):
            cols.add(get_column_letter(i))
    return cols


def representative_formula(rows):
    """Prefer a mid-sheet daily row (2–145) over header/aggregate row 1."""
    daily = [(r, f) for r, f in rows if 2 <= r <= 145 and f]
    if daily:
        return daily[min(8, len(daily) - 1)]
    nonempty = [(r, f) for r, f in rows if f]
    return nonempty[0] if nonempty else (None, None)


def seed_clocks():
    fill, value = {}, {}
    for c in MEAS_FILL_OPEN:
        fill[c] = "open"
    for c in MEAS_FILL_CLOSE:
        fill[c] = "close"
    for c in MEAS_VALUE_OPEN:
        value[c] = "open"
    for c in MEAS_VALUE_CLOSE:
        value[c] = "close"
    for sh, vis in SH_DAILY.items():
        fill[sh] = fill[vis]
        value[sh] = value[vis]
    return fill, value


def _worse(a, b):
    """close > unknown > open."""
    rank = {"open": 0, "unknown": 1, "close": 2}
    if a is None:
        return b
    if b is None:
        return a
    return a if rank[a] >= rank[b] else b


def propagate(by_col, cf_rules, fill, value, rounds=8):
    """Same-row deps inherit the worse clock. Prior rows are already known."""
    notes = defaultdict(list)
    for _ in range(rounds):
        changed = False
        for col, rows in by_col.items():
            r0, f = representative_formula(rows)
            if not f:
                continue
            refs = refs_of(f)
            if refs is None:
                if value.get(col) != "unknown":
                    value[col] = "unknown"
                    notes[col].append("unparsed_formula")
                    changed = True
                continue
            vclk = value.get(col, "open")
            for dcol, drow, ext in refs:
                if ext is not None:
                    continue  # static external cache
                if r0 is None:
                    continue
                if drow > r0:
                    vclk = _worse(vclk, "unknown")
                    notes[col].append(f"future_ref {dcol}{drow}")
                    continue
                if drow < r0:
                    continue  # prior session
                # same row
                dep = value.get(dcol) or fill.get(dcol)
                if dep is None:
                    vclk = _worse(vclk, "unknown")
                else:
                    vclk = _worse(vclk, dep)
            if "STOCKHISTORY" in (f or ""):
                # weekly/daily spill: high/low/vol live in the spill
                if col not in SH_DAILY or SH_DAILY[col] in MEAS_VALUE_CLOSE:
                    vclk = _worse(vclk, "close")
                    notes[col].append("STOCKHISTORY")
            if vclk and value.get(col) != vclk:
                value[col] = vclk
                changed = True
        # CF expressions on a column: fill inherits refs' clocks
        for rule in cf_rules:
            f = rule.get("formula") or ""
            refs = refs_of(f) if f.startswith("=") or (f and f[0].isalpha()) else []
            if refs is None:
                refs = []
            if f and not f.startswith("="):
                refs2 = refs_of("=" + f)
                if refs2:
                    refs = refs2
            clk = "open"
            for dcol, _drow, ext in refs or []:
                if ext is not None:
                    continue
                dep = _worse(value.get(dcol), fill.get(dcol))
                clk = _worse(clk, dep or "unknown")
            for col in cf_cols(rule.get("sqref")):
                if fill.get(col) != _worse(fill.get(col), clk):
                    fill[col] = _worse(fill.get(col), clk)
                    if clk != "open":
                        notes[col].append("cf_reads_" + clk)
                    changed = True
        if not changed:
            break
    return fill, value, notes


def lock_measured(fill, value):
    """timing_test / NOTES win over the dependency walk."""
    for c in MEAS_FILL_OPEN:
        fill[c] = "open"
    for c in MEAS_FILL_CLOSE:
        fill[c] = "close"
    for c in MEAS_VALUE_OPEN:
        value[c] = "open"
    for c in MEAS_VALUE_CLOSE:
        value[c] = "close"
    for sh, vis in SH_DAILY.items():
        fill[sh] = fill[vis]
        value[sh] = value[vis]
    return fill, value


MEASURED_FILL = set(MEAS_FILL_OPEN) | set(MEAS_FILL_CLOSE) | set(SH_DAILY)


def feature_clock(fill, value, col, kind):
    """kind = 'fill' or 'value'. Unknown → close.

    Unmeasured fills (anything past A–O / IR:IW) are CLOSE even if the
    CF walker said open — that walk is not a timing_test. Same-day
    +several-percent hold1 on those fills is the leak signature.
    """
    if kind == "fill" and col not in MEASURED_FILL:
        return "close"
    raw = (fill if kind == "fill" else value).get(col, "unknown")
    return "open" if raw == "open" else "close"


def mine_clock(fill, value, col):
    """Conservative column clock (mixed features). Unknown → close."""
    c = _worse(fill.get(col), value.get(col))
    if c is None or c == "unknown":
        return "close"
    return c


def groups(fill, value):
    letters = [get_column_letter(i) for i in range(1, FULL_COLS + 1)]
    g = {"fill_open": [], "fill_close": [], "fill_unknown": [],
         "value_open": [], "value_close": [], "value_unknown": [],
         "fill_mine_open": [], "fill_mine_close": [],
         "value_mine_open": [], "value_mine_close": [],
         "mine_open": [], "mine_close": []}
    for col in letters:
        g[f"fill_{fill.get(col, 'unknown')}"].append(col)
        g[f"value_{value.get(col, 'unknown')}"].append(col)
        fm = feature_clock(fill, value, col, "fill")
        vm = feature_clock(fill, value, col, "value")
        g[f"fill_mine_{fm}"].append(col)
        g[f"value_mine_{vm}"].append(col)
        g[f"mine_{mine_clock(fill, value, col)}"].append(col)
    return g


def build(measure=None):
    _m, by_col, cf = load_model()
    fill, value = seed_clocks()
    fill, value, notes = propagate(by_col, cf, fill, value)
    fill, value = lock_measured(fill, value)
    measured = {
        "source": "timing_test.py / NOTES.md (BBAI, 5 days)",
        "fill_open": list(MEAS_FILL_OPEN),
        "fill_close": list(MEAS_FILL_CLOSE),
        "value_open": list(MEAS_VALUE_OPEN),
        "value_close": list(MEAS_VALUE_CLOSE),
        "landmine": "core_score = A..J includes D,E,F,H,I → CLOSE entry only",
    }
    if measure:
        notes.setdefault("_measure", []).append(measure)
    g = groups(fill, value)
    letters = [get_column_letter(i) for i in range(1, FULL_COLS + 1)]
    cols = []
    for col in letters:
        cols.append({
            "col": col,
            "idx": column_index_from_string(col) - 1,
            "fill": fill.get(col, "unknown"),
            "value": value.get(col, "unknown"),
            "fill_mine": feature_clock(fill, value, col, "fill"),
            "value_mine": feature_clock(fill, value, col, "value"),
            "mine": mine_clock(fill, value, col),
            "notes": list(dict.fromkeys(notes.get(col, [])))[:6],
            "measured_fill": (
                "open" if col in MEAS_FILL_OPEN else
                "close" if col in MEAS_FILL_CLOSE else None
            ),
        })
    return {
        "generated": str(date.today()),
        "n_cols": FULL_COLS,
        "letters": "A..JL",
        "measured": measured,
        "counts": {k: len(v) for k, v in g.items()},
        "groups": g,
        "columns": cols,
        "live_untouched": "flatten_robust",
        "unknown_mined_as": "close",
        "core_score_entry": "close",
    }


def _join(xs, n=40):
    if not xs:
        return "—"
    s = ",".join(xs)
    return s if len(xs) <= n else ",".join(xs[:n]) + f" … (+{len(xs)-n})"


def render(inv):
    g, c = inv["groups"], inv["counts"]
    L = [
        "# A–JL column clocks (open / close / unknown)",
        "",
        f"_Generated {inv['generated']} · live `flatten_robust` is not changed._",
        "",
        "## Method",
        "",
        "1. **Measured fills A–O** — `timing_test.py` / NOTES.md (BBAI, 5 days, "
        "OHLCV ×0.4–4.0). Open fills: A,B,C,G,J,K,L,M,O. Close fills: D,E,F,H,I,N.",
        "2. **Measured values A–O** — formula inputs, not fills. Open values: "
        "A (date), C (open), J (open-to-open). Close values: B (close), D/E/F "
        "(high/low/vol), H/I (same-day ret), G/K/M (vol/wick — fill may be "
        "open, **value is close**), N.",
        "3. **Dependency walk** on `model.json` formulas + CF expressions. "
        "Same-row refs inherit the worse clock. Prior-row refs are already "
        "known. Unparsed / future-row → **unknown**.",
        "4. **STOCKHISTORY** spill IR:IW aliases A:F. Weekly AP:AU treated close "
        "(contains high/low/vol).",
        "",
        "**Unknown is not open.** Unmeasured fills (past A–O) mine at "
        "**CLOSE** even if CF deps look open — only `timing_test` licenses "
        "open entry on a fill. Value gates may be open when the formula "
        "walk proves no same-row close input. `core_score` / any def that "
        "reads D,E,F,H,I on the same row is CLOSE. That is the landmine.",
        "",
        "## Counts",
        "",
        "| kind | open | close | unknown |",
        "|---|---:|---:|---:|",
        f"| fill (raw) | {c['fill_open']} | {c['fill_close']} | {c['fill_unknown']} |",
        f"| value (raw) | {c['value_open']} | {c['value_close']} | {c['value_unknown']} |",
        f"| **fill mine** (unk→close) | {c['fill_mine_open']} | {c['fill_mine_close']} | 0 |",
        f"| **value mine** (unk→close) | {c['value_mine_open']} | {c['value_mine_close']} | 0 |",
        f"| mixed-feature conservative | {c['mine_open']} | {c['mine_close']} | 0 |",
        "",
        "## Groups",
        "",
        f"**Fill OPEN (timing or proven):** {_join(g['fill_mine_open'])}",
        "",
        f"**Value OPEN:** {_join(g['value_mine_open'])}",
        "",
        f"**Fill CLOSE or unknown→close:** {_join(g['fill_mine_close'], 60)}",
        "",
        f"**Value CLOSE or unknown→close:** {_join(g['value_mine_close'], 60)}",
        "",
        "## Landmine",
        "",
        inv["measured"]["landmine"],
        "",
        "A-keyed fill defs may enter at open. Mixing A with any close-mine "
        "column forces close. G/K/M *value* gates are close even though their "
        "*fills* tested open.",
        "",
        "## Per-column (first 80 + any measured)",
        "",
        "| col | fill | value | fill mine | value mine | source |",
        "|---|---|---|---|---|---|",
    ]
    for rec in inv["columns"]:
        if rec["idx"] >= 15 and rec.get("fill_mine") != "open" and rec.get("value_mine") != "open":
            continue
        src = "timing" if rec["measured_fill"] else (
            "dep" if rec["notes"] else "seed/dep")
        L.append(
            f"| {rec['col']} | {rec['fill']} | {rec['value']} | "
            f"**{rec.get('fill_mine', rec['mine'])}** | "
            f"**{rec.get('value_mine', rec['mine'])}** | {src} |"
        )
    L += [
        "",
        "Full machine map: `clock_map.json`. Research only.",
        "",
    ]
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--measure", default=None,
                    help="optional ticker for all-cols perturb (not required)")
    args = ap.parse_args()
    inv = build(measure=args.measure)
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    json.dump(inv, open(OUT_JSON, "w"), indent=1)
    open(OUT_MD, "w").write(render(inv))
    c = inv["counts"]
    print(f"fill open={c['fill_open']} close={c['fill_close']} "
          f"unk={c['fill_unknown']}")
    print(f"value open={c['value_open']} close={c['value_close']} "
          f"unk={c['value_unknown']}")
    print(f"mine open={c['mine_open']} close={c['mine_close']}")
    print(f"wrote {OUT_MD}")


if __name__ == "__main__":
    main()
