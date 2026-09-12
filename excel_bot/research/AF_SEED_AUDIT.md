## A–F seed (STOCKHISTORY / Yahoo rows)

_Generated 2026-09-07. Live `flatten_robust` frozen. No cards._

### What A–F are

In the real sheet, columns **A–F are STOCKHISTORY**. They alias the daily spill IR:IW (date, close, open, high, low, volume). The emulator must fill that spill from **our** price history — the Yahoo / excel-state rows cache — for the day under test. It must not paste Excel's last-calc cache, and it must not put later bars into that day's A–F.

### Path harden actually used

`rebuild_grids.py` reads `data/rows/<T>.json`, calls `backtest.seed_anchor` / `build_ticker`, and writes `source: rows_cache`. Harden, color-join, and the A–O mines load those grids. They do **not** call `run.py --from-cache` or `validate.build_seeds`.

Grids on disk this run: **3603**. Yahoo/rows rebuilds: **3603**. Excel-cache or untagged: **0**.

**No Excel-cache grids on this disk.** Every mined file is a Yahoo/rows rebuild. The Excel-cache replay path still exists (`run.py --from-cache`) — it is for matching the xlsx, not for harden or walk-forward.

### Day under test vs the tile shortcut

`seed_anchor` only keeps rows with **date ≤ the engine's TODAY**. That TODAY is the **tile anchor** (about every 120 trading days), not each calendar day. So a day in the middle of a tile is painted by an engine that can already see later prices in later rows of IR:IW.

We checked that leak two ways on AAPL (Yahoo rows, not Excel cache):

- **Strip later prices, keep the later TODAY.** 2026-06-26 A–O fills were **identical** to the full later tile. Future daily bars did not change that day's paint.
- **Strict as-of (TODAY = that day) vs the later tile.** Most mid-window days matched. Two open-letter misses: **M** on 2026-07-27 (green vs blank) and **J** on 2026-03-31 (red vs light green). Those are TODAY / window-alignment, not Excel cache, and not “a later close leaked into A–F.”

Days that land on **rows 2–9** of a tile often have no A/B/J/L/M/O highlight — those paint rules start around row 10. That is a **missing color** (we under-count lights), not a peek at the future.

### Other flags (not open-entry)

- **D / E look-ahead:** helper columns CD / CE sum the next five rows. That only paints D and E, which are close-knowable and never start an open trade.
- **Weekly AP:AU** is seeded through the same TODAY. Weekly high/low/vol are treated close.

### Verdict for #144

Harden and walk-forward used the Yahoo/rows rebuild for every ticker on disk. They did **not** reuse Excel's cached A–F. The remaining gap is the **tile TODAY**, not a hidden xlsx dump. KEEP-6 / color-join stay research-only. No live wire.

