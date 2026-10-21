# Overlay horizon backtest — KEEP FPE avoid

Kid: We tried the expensive-sticker skip on every shopping list clock. Fees in. No peeking at today's homework. One lucky day does not count.

_Research only. Live `flatten_robust` / `LIVE_POLICY` untouched. No merge without Cyrus._

Fill = 09:30 `Open` · whole shares · [`futubull_fees.json`](../00_grounding/futubull_fees.json). Feature = last Elite `Forward P/E` with date **< D**. Theme Radar unit book = $1k notional per liquid name-day. Flatten leftover = $10k · sell first · min-hold · hard-red sit. `flatten_robust_shaped` = gated `flatten_live` hold-3 (not live wire).

## Bar

| check | rule |
|---|---|
| sleeve clock | tagged `hold_sessions` + leftover or 1d open→close |
| leak | prior Elite only |
| both-tape | avoided *peer-excess* < 0 on SPY-up **and** SPY-down, n≥20 each |
| book | leftover: $10k Δ vs baseline. unit: mean(kept)−mean(all), and avoided mean < 0. Universe-sum of skipped losers is rejected. |
| concentration | top-2 |ΔP&L| share ≥ 65% → FAIL |
| walk-forward | same-sign avoided-name $ on first/last half, else thin |

## Picked KEEP (default FPE≥35 · all regimes, sweep only if PASS)

| sleeve | hold | clock | n avoided | hit vs peer | xs $ | book Δ$ | max DD | top-2 day | both-tape | verdict |
|---|---:|---|---:|---|---:|---:|---:|---:|---|---|
| `theme_radar_1d` | 1 | 1d open→close + Futubull | 4535 | 31.5% / 31.4% | +0.09 | -0.01 | 0.0% | 31.6% | NO | **FAIL** |
| `flatten_h1` | 1 | 09:30 leftover · min-hold 1 + fees | 11 | 18.2% / 54.7% | -8.95 | +326.05 | -2.0% | 55.9% | thin-n | **THIN** |
| `flatten_h3` | 3 | 09:30 leftover · min-hold 3 + fees | 9 | 44.4% / 61.5% | -35.23 | +456.31 | -2.1% | 51.0% | thin-n | **THIN** |
| `flatten_h5` | 5 | 09:30 leftover · min-hold 5 + fees | 11 | 45.5% / 52.9% | -30.93 | +722.58 | -1.6% | 49.7% | thin-n | **FAIL** |
| `flatten_live_h5` | 5 | gated leftover · min-hold 5 + fees | 0 | — / 37.5% | — | +0.00 | -0.3% | 0.0% | thin-n | **FAIL** |
| `flatten_robust_shaped` | 3 | gated leftover · min-hold 3 (live-shaped, not LIVE) | 0 | — / 43.8% | — | +0.00 | -0.3% | 0.0% | thin-n | **FAIL** |

### Why each call

- `theme_radar_1d` **FAIL** — avoided names beat peers after fees (xs $+0.09) — not an avoid.
- `flatten_h1` **THIN** — thin-n — cannot claim both-tape.
- `flatten_h3` **THIN** — thin-n — cannot claim both-tape.
- `flatten_h5` **FAIL** — Theme Radar 5d FPE board: IC_up -0.033 Sign_up 40% (2/5) n=5; IC_down -0.131 Sign_down 100% n=10. Up-tape flips (Sign_up 40%, 2/5, n=5). Do not add both-tape 5d FPE Avoid to flatten_h5.; leftover $ was +722.58 (thin-n, not a rescue).
- `flatten_live_h5` **FAIL** — veto never fired.
- `flatten_robust_shaped` **FAIL** — veto never fired.

## Theme Radar 5d FPE board (local) — `flatten_h5` closed

Authoritative IC for a 5-session FPE Avoid. Leftover $ on `flatten_h5` is **not** a rescue. FPE is a **1d Theme Radar** signal; do not keep mining it on the 5d clock.

| factor | clock | IC_up | Sign_up | n_up | IC_down | Sign_down | n_down | both-tape | verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| Forward P/E ≥ 35 | 5d Theme Radar | -0.033 | **40%** (2/5) | 5 | -0.131 | 100% | 10 | **NO** (up flips) | **FAIL** |
| d_RSI | 5d Theme Radar | — | — | — | — | — | — | — | **INCONCLUSIVE** |
| d_Market Cap | 5d Theme Radar | — | — | — | — | — | — | — | **INCONCLUSIVE** |

Up-tape flips (Sign_up 40%, 2/5, n=5). Do not add both-tape 5d FPE Avoid to flatten_h5. `flatten_h5` × FPE-avoid = **FAIL / do not wire.**

## Full sweep (iterate knobs — do not cherrypick a FAIL default)

| sleeve | FPE≥ | regime | n | xs $ | book Δ$ | both-tape | top-2 | verdict |
|---|---:|---|---:|---:|---:|---|---:|---|
| `theme_radar_1d` | 35 | all | 4535 | +0.09 | -0.01 | NO | 31.6% | FAIL |
| `theme_radar_1d` | 35 | morn_up | 1530 | +0.90 | -0.03 | NO | 60.9% | FAIL |
| `theme_radar_1d` | 35 | s_nonneg | 1530 | +0.90 | -0.03 | NO | 60.9% | FAIL |
| `theme_radar_1d` | 40 | all | 3654 | +0.02 | -0.00 | NO | 30.7% | FAIL |
| `theme_radar_1d` | 40 | morn_up | 1222 | +0.38 | -0.01 | YES | 61.6% | FAIL |
| `theme_radar_1d` | 40 | s_nonneg | 1222 | +0.38 | -0.01 | YES | 61.6% | FAIL |
| `theme_radar_1d` | 50 | all | 2591 | -0.23 | +0.01 | NO | 31.8% | FAIL |
| `theme_radar_1d` | 50 | morn_up | 862 | +0.18 | -0.00 | YES | 58.2% | FAIL |
| `theme_radar_1d` | 50 | s_nonneg | 862 | +0.18 | -0.00 | YES | 58.2% | FAIL |
| `flatten_h1` | 35 | all | 11 | -8.95 | +326.05 | thin | 55.9% | THIN |
| `flatten_h1` | 35 | morn_up | 6 | -18.93 | +428.34 | thin | 61.8% | THIN |
| `flatten_h1` | 35 | s_nonneg | 7 | -16.68 | +311.42 | thin | 57.2% | THIN |
| `flatten_h1` | 40 | all | 11 | -8.95 | +326.05 | thin | 55.9% | THIN |
| `flatten_h1` | 40 | morn_up | 6 | -18.93 | +428.34 | thin | 61.8% | THIN |
| `flatten_h1` | 40 | s_nonneg | 7 | -16.68 | +311.42 | thin | 57.2% | THIN |
| `flatten_h1` | 50 | all | 7 | -2.74 | +120.21 | thin | 61.0% | THIN |
| `flatten_h1` | 50 | morn_up | 4 | -15.89 | +187.68 | thin | 56.0% | THIN |
| `flatten_h1` | 50 | s_nonneg | 4 | -14.08 | +110.01 | thin | 64.9% | THIN |
| `flatten_h3` | 35 | all | 9 | -35.23 | +456.31 | thin | 51.0% | THIN |
| `flatten_h3` | 35 | morn_up | 6 | -49.06 | +638.10 | thin | 62.6% | THIN |
| `flatten_h3` | 35 | s_nonneg | 7 | -42.89 | +490.85 | thin | 52.4% | THIN |
| `flatten_h3` | 40 | all | 9 | -35.23 | +456.31 | thin | 51.0% | THIN |
| `flatten_h3` | 40 | morn_up | 6 | -49.06 | +638.10 | thin | 62.6% | THIN |
| `flatten_h3` | 40 | s_nonneg | 7 | -42.89 | +490.85 | thin | 52.4% | THIN |
| `flatten_h3` | 50 | all | 6 | -34.07 | +230.10 | thin | 53.0% | THIN |
| `flatten_h3` | 50 | morn_up | 4 | -52.84 | +340.74 | thin | 66.1% | THIN |
| `flatten_h3` | 50 | s_nonneg | 4 | -47.60 | +269.23 | thin | 55.1% | THIN |
| `flatten_h5` | 35 | all | 11 | -30.93 | +722.58 | thin | 49.7% | FAIL |
| `flatten_h5` | 35 | morn_up | 6 | -28.85 | +772.04 | thin | 50.7% | FAIL |
| `flatten_h5` | 35 | s_nonneg | 7 | -29.17 | +771.06 | thin | 50.7% | FAIL |
| `flatten_h5` | 40 | all | 11 | -30.93 | +722.58 | thin | 49.7% | FAIL |
| `flatten_h5` | 40 | morn_up | 6 | -28.85 | +772.04 | thin | 50.7% | FAIL |
| `flatten_h5` | 40 | s_nonneg | 7 | -29.17 | +771.06 | thin | 50.7% | FAIL |
| `flatten_h5` | 50 | all | 7 | -26.19 | +570.80 | thin | 35.6% | FAIL |
| `flatten_h5` | 50 | morn_up | 4 | -32.15 | +491.30 | thin | 52.5% | FAIL |
| `flatten_h5` | 50 | s_nonneg | 4 | -29.72 | +614.38 | thin | 37.0% | FAIL |
| `flatten_live_h5` | 35 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 35 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 35 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 40 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 40 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 40 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 50 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 50 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_live_h5` | 50 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 35 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 35 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 35 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 40 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 40 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 40 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 50 | all | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 50 | morn_up | 0 | — | +0.00 | thin | 0.0% | FAIL |
| `flatten_robust_shaped` | 50 | s_nonneg | 0 | — | +0.00 | thin | 0.0% | FAIL |

## Flatten skips (FPE≥35 · wish-list top 8 · not live tickets)

IREN / HIMS / TNDM (08-13), BTBT (08-14), HNST (08-17), INSP / CRMD (08-24 hard-red sit; 08-25/26), ATRC (09-03/04). Zero avoided names on realized SPY-down. Live gate days 08-20/21 are gold (AEM/KGC/…) — high-FPE 0. Leftover $ lift is wish-list HOLD only; baselines matched published blotters (h1 ~+$1.57k vs +$1.55k, h3 ~+$1.17k vs +$1.18k, h5 ~+$2.31k vs +$2.28k).

## Next experiment — join-hot ∧ AB-silent micros (not FPE)

Kid: The expensive-sticker skip is done. Next we skip the tiny names the crowd is yelling about while the report card is blank. If that is just a handful of days, we stop.

FPE / d_RSI / d_mcap stay **closed** on `flatten_h5`. This Avoid is **not** OR'd with FPE. Elevate stays closed. No live wire.

Pre-register (09:30):

- **join-hot:** session-D `data/join/{D}_ranked.csv` `total_score` ≥ p80 (n≥20 scores and cut ≥ 0; 08-18 p80 = −1 is discarded, not a fire).
- **AB-silent:** last `ab_checklist_enriched.csv` (else plain) with date **< D** is missing, has no row, or `|s_ab|` < 0.05 (`s_ab` = clip(raw/12). `ticker_lookback.EPS`). 08-13/14 have no AB — everyone silent (honest).
- **size:** join/book `micro` **or** `small` (VERI + AEVA/RXT). `mid` (ACMR) excluded. Micro-only is a footnote, not a promote.
- **sleeves:** `flatten_h1` / `flatten_h3` leftover + unit; `book_1d` BUY leftover + unit. **Not** `flatten_h5`.
- Same bar: 09:30 open, Futubull, peer-excess both-tape, leftover Δ$, top-2 |ΔP&L| ≥ 65% → FAIL.

| sleeve | hold | clock | n avoided | hit vs peer | xs $ | leftover Δ$ | max DD | top-2 day | both-tape | verdict |
|---|---:|---|---:|---|---:|---:|---:|---:|---|---|
| `flatten_h1` | 1 | 09:30 leftover · min-hold 1 + fees | 6 | 16.7% / 53.0% | -13.24 | +212.44 | -2.1% | 56.2% | thin-n | **THIN** |
| `flatten_h3` | 3 | 09:30 leftover · min-hold 3 + fees | 6 | 33.3% / 61.7% | +5.37 | -479.53 | -1.8% | 56.8% | thin-n | **THIN** |
| `book_1d` | 1 | book 1d BUY leftover · min-hold 1 + fees | 21 | 38.1% / 48.5% | -9.27 | +52.55 | -3.2% | 63.2% | thin-n | **THIN** |

### Why JAM

- `flatten_h1` **THIN** — thin-n — cannot claim both-tape.
- `flatten_h3` **THIN** — thin-n — cannot claim both-tape.
- `book_1d` **THIN** — book $ up but both-tape thin — do not promote.

### JAM skips (wish-list / book BUY — not live tickets)

Size split among skipped picks: micro=10 small=23. Named autopsy VERI/AEVA/RXT were book-gap worst buys, **not** flatten top-8 or book 1d BUY — the mechanism is tested on the sleeves we can trade, not harvested as a name list.

- `flatten_h1`: INO (2026-08-13), TNDM (2026-08-13), LDI (2026-08-14), BTBT (2026-08-14), TMC (2026-08-17), HNST (2026-08-17).
- `flatten_h3`: INO (2026-08-13), TNDM (2026-08-13), LDI (2026-08-14), BTBT (2026-08-14), TMC (2026-08-17), HNST (2026-08-17).
- `book_1d`: INO (2026-08-13), TNDM (2026-08-13), AGEN (2026-08-13), ACHV (2026-08-13), VOR (2026-08-13), SGRY (2026-08-13), WW (2026-08-13), MBRX (2026-08-13), PROK (2026-08-13), IMNN (2026-08-13), ABEO (2026-08-13), NRXP (2026-08-13), FTRE (2026-08-13), UNCY (2026-08-13), SPRB (2026-08-13), FDMT (2026-08-13), TBCH (2026-08-14), WOLF (2026-08-14), PGY (2026-08-14), TMC (2026-08-17), HNST (2026-08-17).

## Soft 🚨∧fade (only because JAM did not PASS)

`lb_alarm` **and** `lb_fade` on **D's morning stock book**. Early books have no columns (honest empty). **Not** OR'd with FPE. Matching hold only.

| sleeve | hold | clock | n avoided | hit vs peer | xs $ | leftover Δ$ | max DD | top-2 day | both-tape | verdict |
|---|---:|---|---:|---|---:|---:|---:|---:|---|---|
| `flatten_h1` | 1 | 09:30 leftover · min-hold 1 + fees | 0 | — / 50.9% | — | +0.00 | -2.0% | 0.0% | thin-n | **FAIL** |
| `flatten_h3` | 3 | 09:30 leftover · min-hold 3 + fees | 0 | — / 60.0% | — | +0.00 | -1.3% | 0.0% | thin-n | **FAIL** |
| `book_1d` | 1 | book 1d BUY leftover · min-hold 1 + fees | 0 | — / 47.6% | — | +0.00 | -3.2% | 0.0% | thin-n | **FAIL** |

### Why 🚨∧fade

- `flatten_h1` **FAIL** — veto never fired.
- `flatten_h3` **FAIL** — veto never fired.
- `book_1d` **FAIL** — veto never fired.

Veto never fired on flatten top-8 or book 1d BUY.

## Clean stop / recommended pause

**Clean stop.** Join-hot ∧ AB-silent (micro/small) is honest thin-n / concentration / null on `flatten_h1`, `flatten_h3`, and `book_1d`. Soft 🚨∧fade never fired on those sleeves (columns empty early; later 🚨∧fade names are not on the BUY / flatten wish-list). Neither Avoid clears the ship bar.

**Recommended pause:** do not keep mining join p80 / AB-silent / 🚨∧fade knobs on these clocks, and do **not** reopen FPE / d_RSI / d_mcap on `flatten_h5`. Elevate stays closed. No live wire. No merge. Wait for a new 09:30 camera or a longer AB vintage — not another cut on the same thin fires.

## Elevate

**Keep did not clear.** Elevate mechanisms stay rejected. Do not bump CANSLIM, Magic Formula, cheap Forward P/E, or `total_score`.

## Null / FPE closed

FPE Avoid is a **clean null**. Theme Radar 1d percent fade (overlay xs −0.09) **does not survive** Futubull $ peer-excess (xs $+0.09; up-tape xs $+0.35). Local 5d FPE board **FAIL**s both-tape on `flatten_h5` (Sign_up **40%** 2/5 n=5; IC_down −0.131 n=10). Flatten leftover +$326 / +$456 / +$723 stays thin and is not a rescue. Live-shaped veto never fired. d_RSI / d_mcap 5d inconclusive. Sweep FPE 40/50 × morning-up / S≥0 did not clear the bar. **Stop mining FPE / d_RSI / d_mcap on flatten_h5.**

JAM + soft 🚨∧fade also failed the same bar (see Next experiment). **Clean stop + recommended pause.** Elevate stays closed. No live wire.

## Leak / live asserts

- Feature export date < session D.
- Same-day Change / Gap / RelVol never gate.
- `LIVE_POLICY` not imported, not written.
- `flatten_robust` not called.

