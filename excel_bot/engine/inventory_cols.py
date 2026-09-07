"""Inventory stored A–O grids vs the full A–JL workbook (model.json).

Writes excel_bot/research/grid_inventory.json. Safe to run without grids.
"""
from __future__ import annotations

import collections
import json
import os
import sys
from datetime import date

from openpyxl.utils import column_index_from_string, get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL = os.path.join(HERE, "model.json")
OUT = os.path.join(HERE, "..", "research", "grid_inventory.json")

STORED_FIELDS = ["date", "open", "close", "high", "low", "volume", "fills"]
STORED_FILL_COLS = 15  # A..O
FULL_COLS = 275        # A..JL


def _split_coord(coord):
    i = 0
    while i < len(coord) and coord[i].isalpha():
        i += 1
    return coord[:i], int(coord[i:])


def _formula_text(spec):
    if isinstance(spec, dict):
        return spec.get("f")
    return spec


def classify_column(letter, sample_f):
    """Human bucket for a column from a representative formula."""
    f = sample_f or ""
    if letter in list("ABCDEF"):
        return "ohlcv_visible"
    if "STOCKHISTORY" in f:
        return "stockhistory_anchor"
    if "[1]Change!" in f:
        return "external_change_lookup"
    if "[1]VIX!" in f:
        return "external_vix"
    if letter in list("GHIJKLMNO"):
        return "visible_derived"
    if f == "" or f == "=":
        return "spill_or_blank"
    return "deeper_formula"


def build(model_path=MODEL):
    m = json.load(open(model_path))
    meta = m.get("meta") or {}
    formulas = m.get("formulas") or {}
    cf_rules = m.get("cf_rules") or []

    col_rows = collections.defaultdict(set)
    col_sample = {}
    for coord, spec in formulas.items():
        col, row = _split_coord(coord)
        col_rows[col].add(row)
        f = _formula_text(spec)
        if f and (col not in col_sample or row < col_sample[col][0]):
            col_sample[col] = (row, f)

    cf_cols = set()
    for rule in cf_rules:
        for part in (rule.get("sqref") or "").split():
            part = part.replace("$", "")
            a, b = part.split(":")[0], part.split(":")[-1]
            c1, c2 = _split_coord(a)[0], _split_coord(b)[0]
            i1, i2 = column_index_from_string(c1), column_index_from_string(c2)
            cf_cols.update(range(i1, i2 + 1))

    visible = []
    for i, letter in enumerate("ABCDEFGHIJKLMNO"):
        row, f = col_sample.get(letter, (None, None))
        visible.append({
            "col": letter, "idx": i,
            "clock_fill": ("open" if letter in "ABCGJKLMO" else "close"),
            "n_formula_rows": len(col_rows.get(letter, ())),
            "sample": (f[:220] if f else None),
            "bucket": classify_column(letter, f),
            "in_stored_grid": True,
        })

    deeper = []
    for i in range(16, FULL_COLS + 1):
        letter = get_column_letter(i)
        row, f = col_sample.get(letter, (None, None))
        deeper.append({
            "col": letter, "idx": i - 1,
            "n_formula_rows": len(col_rows.get(letter, ())),
            "has_cf": i in cf_cols,
            "sample": (f[:180] if f else None),
            "bucket": classify_column(letter, f),
            "in_stored_grid": False,
        })

    buckets = collections.Counter(c["bucket"] for c in deeper)
    return {
        "generated": str(date.today()),
        "model_meta": meta,
        "n_formulas": len(formulas),
        "n_cf_rules": len(cf_rules),
        "n_cf_columns": len(cf_cols),
        "n_formula_columns": len(col_rows),
        "full_cols": FULL_COLS,
        "full_col_letters": "A..JL",
        "stored": {
            "source": "excel-state rows cache + daily A–O fill rebuild",
            "n_tickers": 3603,
            "fields": STORED_FIELDS,
            "fill_cols": STORED_FILL_COLS,
            "fill_letters": "A..O",
            "missing_vs_full": {
                "columns": FULL_COLS - STORED_FILL_COLS,
                "formula_values_G_to_O": True,
                "deeper_fills": sum(1 for c in deeper if c["has_cf"]),
                "note": (
                    "grids/<T>.json stores OHLCV + 15 fills. It does not "
                    "store G–O formula VALUES or any column past O. "
                    "excel-state itself stores Yahoo rows only — colors "
                    "are rebuilt each daily run and not persisted."
                ),
            },
        },
        "all_cols_mode": {
            "flag": "python engine/run.py --all-cols",
            "captures": "A..JL (275) values + fills, rows 1–364",
            "used_in_daily": False,
            "cost": "minutes per ticker; not the 3,603 daily path",
        },
        "visible_A_to_O": visible,
        "deeper_bucket_counts": dict(buckets),
        "deeper_cf_columns": [c["col"] for c in deeper if c["has_cf"]],
        "deeper_sample": [c for c in deeper if c["sample"] or c["has_cf"]][:80],
    }


def main():
    os.makedirs(os.path.dirname(os.path.abspath(OUT)), exist_ok=True)
    inv = build()
    json.dump(inv, open(OUT, "w"), indent=1)
    print(f"formulas={inv['n_formulas']} cf_cols={inv['n_cf_columns']} "
          f"formula_cols={inv['n_formula_columns']}")
    print(f"stored A–O fills; full A–JL = {inv['full_cols']} cols")
    print(f"deeper CF columns: {len(inv['deeper_cf_columns'])}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    sys.path.insert(0, HERE)
    main()
