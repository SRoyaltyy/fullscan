# Excel emulator mine — clock-aware cycle

_Generated 2026-09-07 · live `flatten_robust` is not changed._

## Protocol

document → rebuild A–O grids from excel-state rows → backtest → keep / iterate.
No live card emission. No merge without Cyrus.

## Ship bar

PASS needs **all** of: discovery n≥300 t≥3 avg>0; holdout n≥100 t≥2 avg>0
same sign; ≥50 tickers; ≥20 entry dates; no single trade >25% of gross wins;
trimmed mean (drop best trade) >0; early and late tape same sign when both
n≥40; SPY-up and SPY-down both positive when both n≥40; hold1 must also
PASS hold2. Clock labeled on every row. Open entry only when the feature
reads no close-knowable fill.

## Status

Inventory: see `GRID_INVENTORY.md`. Backtest results land in this file
after `python engine/mine_clock.py` (overwrites the keeper tables).

## What this cycle mines

- Existing card defs, A-keyed at **open** and again at **close**.
- New `open_score` / `open_core` (no D,E,F,H,I,N fills).
- Color combos and majority of open-knowable fills.
- Lag combos: yesterday H/I/N + today's A (open clock).
- Formula-state gates from stored OHLCV: gap, J, H.
- Sleeve holds 1/2/3/5/8. Costs: `mcap_bps` and `futubull`.

Deeper A–JL formula values are **not** in stored grids.
`--all-cols` is opt-in and unused daily.
