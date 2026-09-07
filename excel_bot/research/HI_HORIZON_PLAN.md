# H / I multi-horizon harness

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

Cyrus + New Bot retargeted the mine. The label is no longer only Futubull open→close P&L. We predict **column H** (same-day close vs open — the intraday %) and **column I** (close vs yesterday’s close — the daily %) on a ticker train/test split so holdout moves stay visible.

Horizons: **1d, 2d, 3d, 1 week (~5d), 2 weeks (~10d)**. Each is reported. 1d is the H / I print on the feature row. Longer horizons use the H / I print that many sessions later, and the stacked daily I over that window so a two-week move is one number.

## Train / test

Same ticker split as every other beat (`holdout_split.json`). Discovery ≠ holdout. A KEEP has to show the move on **holdout** names, not only the names we tuned on.

## Features (clock-clean)

- Values, text, and fills with lags. Gate: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`.
- **Never** same-row H or I as an input on day X — those cells *are* the labels (and close-knowable).
- Prior-row H / I (yesterday and older) are fair features; they were already printed.
- Open landmines stay open. The locked 44 and open fills are not treated as close-today.
- AA today is legal on the **close-entry** side.
- Standing five-cell light + green O ± AH/FR is re-scored here as a check, not discarded and not remineed as a new search.

## Soft regimes

Not a perfect green/red coder. Each day is bucketed by:

- **Sheet heat:** morning five-cell sum hot (≥5), mixed, or cold (≤0).
- **Tape:** SPY up / down / flat (|SPY day| < 0.15% is flat).

Ask: does the factor help **inside** the bucket? A global KEEP needs SPY-up and SPY-down to agree on sign (flat may be thin). Otherwise the note is regime-conditional, in plain English.

n / effect / lottery / five-name / July / Q1 bars still apply.

## This beat

1. Wire this harness (this file + `mine_hi_horizon.py`).
2. Re-score standing light+O ± AH/FR on H/I horizons.
3. Bounded close-entry pair+lag (AA-today OK, no same-row H/I inputs) predicting H/I.
4. Soft regime tables on anything that clears.

Research only. Live frozen.
