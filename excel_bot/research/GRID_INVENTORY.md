# Stored grids vs full A–JL workbook

_Research inventory. Live `flatten_robust` is not changed._

## Verdict

Daily / excel-state state is an **A–O fill grid** (15 columns) plus Yahoo OHLCV.
The emulator can dump **A–JL (275 columns)** via `run.py --all-cols`, but that
mode is opt-in, minutes-per-ticker, and **not** what the daily job stores.
Formula *values* for G–O and every column past O are absent from stored grids.
Color mining on the ship-bar surface is therefore A–O fills + OHLCV-derived
formula states (H, I, J, gap). Deeper CF (97 columns past O) is unevaluated
until someone persists `--all-cols` grids.

## What excel-state actually keeps

| Artifact | Where | What |
|---|---|---|
| Yahoo rows | `excel-state` `state/rows.tar.gz` → `data/rows/<T>.json` | date, open, high, low, close, volume. **3,603** tickers. Span on 2026-09-04 cache: **2026-01-05 → 2026-09-04** (~105–169 sessions). |
| Color grids | rebuilt each daily run into `grids/<T>.json`, **not** pushed to excel-state | `{date, open, close, high, low, volume, fills[15]}` |
| Deep grids | `excel-deep-grids` referenced by `excel_mine.yml` | **branch absent** on origin (2026-09-07) |
| Full A–JL | `python engine/run.py --all-cols` (#139) | 275 cols × rows 1–364, values + fills. Not daily. |

`down_day_mine.excel_notes()` already recorded this: *excel-state ships Yahoo
row caches, not the 3,603 color grids.*

## Visible sheet (A–O) — this is the stored mining surface

Fill clocks from `timing_test.py` / NOTES.md (perturbation of day-t OHLCV):

| Col | Value (formula) | Fill clock | In stored grid |
|---|---|---|---|
| A | Date (`=IR`) | **open** | fills[0] + date serial |
| B | Close (`=IS`) | **open** | close + fills[1] |
| C | Open (`=IT`) | **open** | open + fills[2] |
| D | High (`=IU`) | **close** | high + fills[3] |
| E | Low (`=IV`) | **close** | low + fills[4] |
| F | Volume (`=IW`) | **close** | volume + fills[5] |
| G | `F_t/F_{t-1}` vol ratio | **open** (fill) | **fill only** |
| H | `(B-C)/C` same-day ret | **close** | **fill only** (value recoverable) |
| I | `(B-Bprev)/Bprev` | **close** | **fill only** (value recoverable) |
| J | `(C-Cprev)/Cprev` | **open** | **fill only** (value recoverable) |
| K | `(D-C)/C` upper wick | **open** (fill) | **fill only** |
| L | composite score (N/O/H/EL/DD/…) | **open** (fill) | **fill only** |
| M | `-(E-C)/C` lower wick | **open** (fill) | **fill only** |
| N | `EL * 1_{|H|≥3%}` | **close** | **fill only** |
| O | composite score (EL/H/F/N/DD/CP/…) | **open** (fill) | **fill only** — O1 is the ticker |

**Value vs fill caveat:** G, K, M *values* read same-day volume / high / low,
so a value-gate on those three is close-knowable even though the fill clock
is open. This mine clocks value-gates by formula inputs. Card-style mining
uses fills and the perturbation clocks.

`core_score = A..J` includes D,E,F,H,I → **close** entry only.
Open entry is legal only when the feature reads no close-knowable fill
(A-keyed defs; `open_score` / `open_core`; open color combos; lag combos
that use *yesterday's* close-knowable fill + today's A).

## Full workbook (A–JL) — `--all-cols` only

From `engine/model.json` (Sheet1, max_row 364, max_col 275):

| Count | What |
|---:|---|
| 35,769 | formulas |
| 173 | CF rules |
| 112 | columns with any CF (15 visible + **97** past O) |
| 270 | columns that have at least one formula |
| 246 | columns with ≥50 formula rows (daily-shaped) |

Deeper buckets (past column O):

- `IR:IW` — daily STOCKHISTORY spill (Date/Close/Open/High/Low/Volume). A–F are aliases.
- `AP:AU` — weekly STOCKHISTORY spill.
- `Q1`/`R1` — `[1]Change!` external lookup (static cache).
- `IY` — `[1]VIX!` external.
- `P1` — `TODAY()`.
- `S/T/U/V/X/Y/AA` — sheet-level aggregates / flags (V is a 0/1 stress flag).
- `L`/`O` already pull `EL`, `DD`, `CF`, `CP` — those live past O.
- `AI` — text state machine (`INTRADAY` / `BUY` / `SELL`).
- `JA` — another 0/1 composite (`HN/JB/JC/S/CP/AA/AJ/IB/IZ/JF/IP`).

None of those values or their fills are in `grids/<T>.json`.

## Existing cards vs this surface

L1–L5 / S1–S2 were mined on A–O fills, confirmation **close**, 0.1%/0.3% costs,
discovery/holdout, 6.5-month regime. They do **not** use `--all-cols`.
This cycle re-tests that surface with correct clocks, sleeve holds 1/2/3/5/8,
Futubull *and* mcap costs, tape/regime splits, and **new** patterns
(open scores, color combos, lag combos, gap/J/H value gates).

## What would exhaust the rest

Persist `--all-cols` (or a chosen subset: L/O values, EL/DD/CP, AI, JA, IY)
on the same 3,603 tickers and re-run `mine_clock.py`. Until then, claiming
the *workbook* is exhausted would be a lie; claiming the **stored A–O fill
surface** is exhausted is the honest bar.
