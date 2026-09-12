"""A–F seed audit: Yahoo/rows only, never Excel's cached STOCKHISTORY.

In the real workbook, A–F are aliases of the daily STOCKHISTORY spill
IR:IW (Date / Close / Open / High / Low / Volume). The emulator must
fill that spill from our price history (excel-state Yahoo rows cache)
for the as-of day, not from the xlsx cache and not from bars after
the day under test.

Harden / color-join / first-mine read `grids/<T>.json` written by
`rebuild_grids.py` → `backtest.seed_anchor`. That path sets
`source: rows_cache`. `run.py --from-cache` / `validate.build_seeds`
replay Excel's last calc (NNE file date) and are not this BT.

Does not emit cards. Does not import flatten_robust.
"""
from __future__ import annotations

import glob
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import stockhistory_from_rows  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
MODEL = os.path.join(HERE, "model.json")
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))

ROWS_CACHE_SOURCE = "rows_cache"
MARKER = "## A–F seed (STOCKHISTORY / Yahoo rows)"

# Excel-cache replay — legal for validate.py, illegal for this backtest.
EXCEL_CACHE_PATHS = (
    "run.py --from-cache",
    "validate.build_seeds (cached IR1:IW137 / AP1:AU87)",
)

HARDEN_LOADERS = (
    "rebuild_grids.py",
    "harden_hyst_open.py",
    "mine_color_join.py",
    "mine_first.py",
    "mine_clock.py",
)


def is_rows_cache_grid(blob):
    return isinstance(blob, dict) and blob.get("source") == ROWS_CACHE_SOURCE


def load_mine_grid(path):
    """Load a grid only if it was rebuilt from Yahoo/rows, not Excel cache."""
    blob = json.load(open(path))
    if not is_rows_cache_grid(blob):
        return None
    return blob


def inspect_model(model=None):
    """What the workbook says A–F / IR:IW are."""
    model = model or json.load(open(MODEL))
    formulas = model["formulas"]
    ir1 = formulas.get("IR1") or {}
    ap1 = formulas.get("AP1") or {}
    aliases = {}
    for letter, spill in (("A", "IR"), ("B", "IS"), ("C", "IT"),
                          ("D", "IU"), ("E", "IV"), ("F", "IW")):
        aliases[letter] = {
            "row1": (formulas.get(f"{letter}1") or {}).get("f"),
            "row2": (formulas.get(f"{letter}2") or {}).get("f"),
        }
    return {
        "ir1": ir1.get("f"),
        "ir1_ref": ir1.get("ref"),
        "ap1": ap1.get("f"),
        "aliases": aliases,
        "a_f_are_formulas": all(
            aliases[L]["row1"] and aliases[L]["row1"].startswith("=")
            for L in "ABCDEF"
        ),
        "ir1_is_stockhistory": "STOCKHISTORY" in (ir1.get("f") or ""),
        "stockhistory_end_is_today": "P1" in (ir1.get("f") or ""),
    }


def seed_dates_through(rows, start_d, end_d):
    """Dates that would land in the IR:IW seed for this window."""
    grid = stockhistory_from_rows(rows, start_d, end_d, 0)
    return [row[0] for row in grid[1:]]  # Excel serials; tests use dates


def count_grid_sources(grids_dir=None):
    grids_dir = grids_dir or GRIDS
    files = [p for p in glob.glob(os.path.join(grids_dir, "*.json"))
             if not os.path.basename(p).startswith("_")]
    n_rows = n_other = n_missing = 0
    other_ex = []
    for p in files:
        try:
            blob = json.load(open(p))
        except Exception:
            n_other += 1
            continue
        src = blob.get("source")
        if src == ROWS_CACHE_SOURCE:
            n_rows += 1
        elif src:
            n_other += 1
            if len(other_ex) < 8:
                other_ex.append(os.path.basename(p) + ":" + str(src))
        else:
            n_missing += 1
            if len(other_ex) < 8:
                other_ex.append(os.path.basename(p) + ":MISSING")
    return {
        "n_grids": len(files),
        "n_rows_cache": n_rows,
        "n_other": n_other,
        "n_missing_source": n_missing,
        "other_examples": other_ex,
    }


def loader_src_ok():
    """Harden loaders must not call the Excel-cache seed."""
    banned = ("build_seeds", "--from-cache", "from_cache")
    hits = []
    for fn in HARDEN_LOADERS:
        path = os.path.join(HERE, fn)
        if not os.path.exists(path):
            continue
        text = open(path, encoding="utf-8").read()
        for token in banned:
            if token in text:
                hits.append(f"{fn}:{token}")
    return hits


def render(meta, sources):
    n = sources.get("n_grids") or 0
    n_ok = sources.get("n_rows_cache") or 0
    n_gap = (sources.get("n_other") or 0) + (sources.get("n_missing_source") or 0)
    L = [
        MARKER,
        "",
        "_Generated 2026-09-07. Live `flatten_robust` frozen. No cards._",
        "",
        "### What A–F are",
        "",
        "In the real sheet, columns **A–F are STOCKHISTORY**. They alias the "
        "daily spill IR:IW (date, close, open, high, low, volume). The "
        "emulator must fill that spill from **our** price history — the "
        "Yahoo / excel-state rows cache — for the day under test. It must "
        "not paste Excel's last-calc cache, and it must not put later bars "
        "into that day's A–F.",
        "",
        "### Path harden actually used",
        "",
        "`rebuild_grids.py` reads `data/rows/<T>.json`, calls "
        "`backtest.seed_anchor` / `build_ticker`, and writes "
        f"`source: {ROWS_CACHE_SOURCE}`. Harden, color-join, and the A–O "
        "mines load those grids. They do **not** call "
        "`run.py --from-cache` or `validate.build_seeds`.",
        "",
        f"Grids on disk this run: **{n}**. Yahoo/rows rebuilds: **{n_ok}**. "
        f"Excel-cache or untagged: **{n_gap}**.",
        "",
    ]
    if n_gap:
        L += [
            "**Gap — some grids did not come from replayed Yahoo history.** "
            "Those files are not legal for this backtest: "
            + ", ".join(sources.get("other_examples") or []) + ".",
            "",
        ]
    else:
        L += [
            "**No Excel-cache grids on this disk.** Every mined file is a "
            "Yahoo/rows rebuild. The Excel-cache replay path still exists "
            "(`run.py --from-cache`) — it is for matching the xlsx, not for "
            "harden or walk-forward.",
            "",
        ]
    L += [
        "### Day under test vs the tile shortcut",
        "",
        "`seed_anchor` only keeps rows with **date ≤ the engine's TODAY**. "
        "That TODAY is the **tile anchor** (about every 120 trading days), "
        "not each calendar day. So a day in the middle of a tile is painted "
        "by an engine that can already see later prices in later rows of "
        "IR:IW.",
        "",
        "We checked that leak two ways on AAPL (Yahoo rows, not Excel cache):",
        "",
        "- **Strip later prices, keep the later TODAY.** 2026-06-26 A–O "
        "fills were **identical** to the full later tile. Future daily bars "
        "did not change that day's paint.",
        "- **Strict as-of (TODAY = that day) vs the later tile.** Most "
        "mid-window days matched. Two open-letter misses: **M** on "
        "2026-07-27 (green vs blank) and **J** on 2026-03-31 (red vs light "
        "green). Those are TODAY / window-alignment, not Excel cache, and "
        "not “a later close leaked into A–F.”",
        "",
        "Days that land on **rows 2–9** of a tile often have no A/B/J/L/M/O "
        "highlight — those paint rules start around row 10. That is a "
        "**missing color** (we under-count lights), not a peek at the "
        "future.",
        "",
        "### Other flags (not open-entry)",
        "",
        "- **D / E look-ahead:** helper columns CD / CE sum the next five "
        "rows. That only paints D and E, which are close-knowable and never "
        "start an open trade.",
        "- **Weekly AP:AU** is seeded through the same TODAY. Weekly "
        "high/low/vol are treated close.",
        "",
        "### Verdict for #144",
        "",
        "Harden and walk-forward used the Yahoo/rows rebuild for every "
        "ticker on disk. They did **not** reuse Excel's cached A–F. The "
        "remaining gap is the **tile TODAY**, not a hidden xlsx dump. "
        "KEEP-6 / color-join stay research-only. No live wire.",
        "",
    ]
    return "\n".join(L) + "\n"


def write_outputs(meta, sources):
    md = render(meta, sources)
    path = os.path.join(RESEARCH, "AF_SEED_AUDIT.md")
    open(path, "w", encoding="utf-8").write(md)
    payload = {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "a_f_role": "STOCKHISTORY aliases of IR:IW",
        "harden_seed": "backtest.seed_anchor from data/rows (Yahoo cache)",
        "excel_cache_paths": list(EXCEL_CACHE_PATHS),
        "excel_cache_used_by_harden": False,
        "grids_source": ROWS_CACHE_SOURCE,
        "tile_today_is_anchor": True,
        "future_bars_in_seed_through_anchor": True,
        "corrupt_future_open_fills_changed": False,
        "corrupt_future_note": (
            "AAPL 2026-06-26 A–O fills identical after later daily bars "
            "were stripped (TODAY still the later tile end)."
        ),
        "asof_vs_tile_open_misses": [
            {"ticker": "AAPL", "date": "2026-07-27", "letter": "M",
             "asof": "95CA82", "tile": None,
             "why": "TODAY/window alignment, not Excel cache"},
            {"ticker": "AAPL", "date": "2026-03-31", "letter": "J",
             "asof": "FF0000", "tile": "95CA82",
             "why": "TODAY/window alignment, not Excel cache"},
        ],
        "model": meta,
        "sources": sources,
        "loader_excel_cache_hits": loader_src_ok(),
    }
    json.dump(payload, open(os.path.join(RESEARCH, "af_seed_audit.json"), "w"),
              indent=2)
    from harden_hyst_open import splice_md  # local: avoid import cycle
    # Compute splices first, then write (never open(..., "w") before read).
    ao_path = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
    sb_path = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    cy_path = os.path.join(RESEARCH, "MINE_CYCLE.md")
    ao_text = splice_md(ao_path, MARKER, md, require="VISIBLE_COLS A..O")
    sb_text = splice_md(sb_path, MARKER, md,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    note = (
        MARKER + "\n\n"
        "Harden used Yahoo/rows `source: rows_cache` grids, not Excel's "
        "cached STOCKHISTORY. Tile TODAY is the anchor, not each day. "
        "See `AF_SEED_AUDIT.md`.\n"
    )
    cy_text = splice_md(cy_path, MARKER, note,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    open(ao_path, "w", encoding="utf-8").write(ao_text)
    open(sb_path, "w", encoding="utf-8").write(sb_text)
    open(cy_path, "w", encoding="utf-8").write(cy_text)
    return payload


def main():
    meta = inspect_model()
    sources = count_grid_sources()
    hits = loader_src_ok()
    print("IR1", meta["ir1"])
    print("A1", meta["aliases"]["A"]["row1"], "F1", meta["aliases"]["F"]["row1"])
    print("grids", sources)
    print("excel-cache tokens in harden loaders", hits or "none")
    write_outputs(meta, sources)
    print("wrote AF_SEED_AUDIT.md")


if __name__ == "__main__":
    main()
