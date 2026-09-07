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

## Outcome (latest tile, 2026-09-07)

Dumped **3,604** names × 275 letters (Yahoo/rows). **268** letters
present. Liquid **3,251 / 332,341** days, **2026-01-08 → 2026-09-04**,
**0 days in 2025**.

Raw board KEEP 272 / KILL 3866 is **not 272 edges**. After twins:

- **CE yesterday = 1** (fresh 24-day low that is also the 43-day low)
  → next-day leftover H **+1.32%** after fees (n=2,563). Washout bounce.
  Open-fair. **2025 not tested.**
- Almost everything else is S/DI / V/DK / AN / O-low / CF “beaten-up
  → next week up” on a 2026 tape.
- Standing light+O **did not reprint +2% H** on this ColorEngine paint
  (H −0.12%). C is mostly orange; the light stays on too often. That is
  a paint/tile mismatch with #144’s xlsx grids, not a refutation.

See `FULL_GRID_MINE.md`. Prior tile (`--tile prev`) + `score_families.py`
is the 2025 check. Research only. Live frozen.
