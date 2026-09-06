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
| `flatten_h5` | 5 | 09:30 leftover · min-hold 5 + fees | 11 | 45.5% / 52.9% | -30.93 | +722.58 | -1.6% | 49.7% | thin-n | **THIN** |
| `flatten_live_h5` | 5 | gated leftover · min-hold 5 + fees | 0 | — / 37.5% | — | +0.00 | -0.3% | 0.0% | thin-n | **FAIL** |
| `flatten_robust_shaped` | 3 | gated leftover · min-hold 3 (live-shaped, not LIVE) | 0 | — / 43.8% | — | +0.00 | -0.3% | 0.0% | thin-n | **FAIL** |

### Why each call

- `theme_radar_1d` **FAIL** — avoided names beat peers after fees (xs $+0.09) — not an avoid.
- `flatten_h1` **THIN** — thin-n — cannot claim both-tape.
- `flatten_h3` **THIN** — thin-n — cannot claim both-tape.
- `flatten_h5` **THIN** — thin-n — cannot claim both-tape.
- `flatten_live_h5` **FAIL** — veto never fired.
- `flatten_robust_shaped` **FAIL** — veto never fired.

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
| `flatten_h5` | 35 | all | 11 | -30.93 | +722.58 | thin | 49.7% | THIN |
| `flatten_h5` | 35 | morn_up | 6 | -28.85 | +772.04 | thin | 50.7% | THIN |
| `flatten_h5` | 35 | s_nonneg | 7 | -29.17 | +771.06 | thin | 50.7% | THIN |
| `flatten_h5` | 40 | all | 11 | -30.93 | +722.58 | thin | 49.7% | THIN |
| `flatten_h5` | 40 | morn_up | 6 | -28.85 | +772.04 | thin | 50.7% | THIN |
| `flatten_h5` | 40 | s_nonneg | 7 | -29.17 | +771.06 | thin | 50.7% | THIN |
| `flatten_h5` | 50 | all | 7 | -26.19 | +570.80 | thin | 35.6% | THIN |
| `flatten_h5` | 50 | morn_up | 4 | -32.15 | +491.30 | thin | 52.5% | THIN |
| `flatten_h5` | 50 | s_nonneg | 4 | -29.72 | +614.38 | thin | 37.0% | THIN |
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

## Elevate

**Keep did not clear.** Elevate mechanisms stay rejected. Do not bump CANSLIM, Magic Formula, cheap Forward P/E, or `total_score`.

## Null / next smallest experiment

Clean null. Theme Radar 1d percent fade (overlay xs −0.09) **does not survive** Futubull $ peer-excess (xs $+0.09; up-tape xs $+0.35). Flatten leftover books print +$326 / +$456 / +$723 vs matched $10k baselines, but n=9–11 avoided picks, **0 SPY-down** avoided entries, and every skip sits on io/HOLD mornings — live-shaped books never fired the veto (gold 08-20/21). Sweep FPE 40/50 × morning-up / S≥0 did not clear the bar.

**Next smallest experiment:** do **not** drop the FPE cut or harvest GEV/CCJ lists. Pre-register FPE≥35 on the `flatten_h1` wish-list unit clock; wait until avoided n≥20 on realized SPY-up **and** SPY-down (or morning-weather up **and** down) **before** looking at leftover $. If the next book-era still cannot fill both tapes, drop the patch. Do not paste 1d IC onto h5. Elevate stays closed.

## Leak / live asserts

- Feature export date < session D.
- Same-day Change / Gap / RelVol never gate.
- `LIVE_POLICY` not imported, not written.
- `flatten_robust` not called.

