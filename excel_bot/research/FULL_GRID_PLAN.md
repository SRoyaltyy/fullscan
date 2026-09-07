# Full workbook mine (A–JO), not A–O

The first H/I pass reconstructed features from A–F only. That is not
what was asked. #144’s leftover / “all cols” sweep used a **hand-picked**
list (~20 value gates, ~16 fill letters). This pass **evaluates every
column the emulator paints** (model.json max_col 275 = A–JO; formulas
through JL) and scores each letter’s number, text, and fill — plus lags
— against future H and I.

It also AND-s every interesting letter-gate with the standing morning
cluster (five-cell light, green O, light+O) so a second link cannot hide
as “only works next to O.”

## Why this is different from “A–O + reconstructed OHLCV”

Even if every formula is a transform of STOCKHISTORY, the *specific*
transforms (candle text in DF/DG/DH, weekly AP:AU, VIX IY/IZ, composite
L/O/AD/IB, streaks, 0/1 flags) can encode nonlinear state that a gap/AH
reconstruction misses. The only way to settle “there must be more” is
to dump those cells and mine them.

## Surface

- Capture: `engine/capture_all_cols.py` — Yahoo/rows seed, rows 2–145,
  all 275 letters, value + fill. `--tile latest` (newest ~140 sessions)
  or `--tile prev` (prior STEP tile → `TICKER__tprev.json`) or `--tile all`.
- Mine: `engine/mine_full_grid.py` builds gates for **every letter
  present** (eq0/1, ≷0, ≥1/≤−1, ≥2, quintiles on discovery, green/red
  fill, lags 1–2, frequent text tokens).
- Standing family is always hardened: five-cell light (A,B,C,G,J fill
  scores, enter 5 / off 2) + green O ± AH≥1 ± FR≥1, and the nine-cell
  variant.
- Combos (lazy): every interesting atom × {O green, light5, light+O};
  pairwise green fills among timing-tested open letters; binary-ish
  eq1/green pairs (3–40% hit rate).

## Clock (same landmines)

- Same-row H/I = labels, never features.
- Lag ≥ 1 of any letter is fair at 09:30.
- Lag 0 values only for the locked 44 open letters.
- Lag 0 fills only for timing-tested A,B,C,G,J,K,L,M,O (+IR/IS/IT).
- Unknown / close lag-0 → predict **later** H/I only.
- Same-day I is not an open label (gap identity).

## Ship bar

Ticker discovery ≠ holdout, available years both beat buy-everyone
when that year exists on the dump, both SPY tapes, n + tickers, fees,
no lottery / name ghost, not a half-the-book sleeve.

Latest-tile dumps are ~140 engine days ending at the Yahoo anchor.
That window is often 2026-only. y2025 harden is skipped when n<20 —
it is **not** a free pass. Use `--tile prev` (or `--tile all`) when
you need 2025 on the same letters.

## Outcome

See `FULL_GRID_MINE.md` / `FULL_GRID_INVENTORY.md` after the dump+mine.
Research only. Live `flatten_robust` frozen.
