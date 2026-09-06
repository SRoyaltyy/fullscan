# Rank residual mine — 2026-08-12 → 2026-09-04

Join's own ranked file, not an external screener. Live `flatten_robust` / `LIVE_POLICY` / `join_rules.json` are untouched. Grok Bot already rejected CANSLIM, Magic Formula, cheap Forward P/E, and a `total_score` re-sort — those stay out.

1d open→close = same-session Finviz Change from Open (join / stock-book clock). Parquet oc fills holes through 2026-08-21. Does not auto-apply to flatten_h5 / live 3d recycle.

leak-free 09:30 — Change / Gap / RelVol / printed book / today OHLC never pick. CFO and Price are outcomes. News Title only when News Time < 09:30 ET.

## Verdict

Join's own top-15 underperforms the mediocre 16–30 band on the 1d open→close clock across 18 sessions (134598 name-days with a Finviz Change-from-Open). Top-15 win 51.8% / mean +0.17% (n=270); ranks 16–30 win 57.4% / mean +0.48% (n=270). That is an inverted ranker, not a missing external formula. Grok Bot already rejected CANSLIM / Magic Formula / cheap Forward P/E / total_score re-sort — those stay out. Avoid keeper: `incomplete` inside ranks 1–15 (n=90, win 45.6%, lift -6.3pp vs the top-15 band, mean +0.05%). Elevate keeper: `hc_after_red` inside ranks 16–80 (n=58, win 75.9%, lift +24.2pp, mean +0.71%). Patched top-8 (skip incomplete / Consumer Defensive / China ADR, Healthcare first from ranks ≤80): raw8 mean +0.20% (win 51.4%, n=144) vs skip_junk +0.29% (n=144) vs elev_hc +0.66% (n=144) vs elev_hc_red +0.49% (n=144). Fee-aware $10k / 8 names: raw $-943.53 vs skip $-255.64 vs elev $370.17 vs elev_hc_red $62.77. Both-tape raw: up n=112 win=54.5% mean=+0.17%; down n=32 win=40.6% mean=+0.30%. elev_hc: up n=112 win=58.0% mean=+0.46%; down n=32 win=59.4% mean=+1.39%. The skip/elevate patch mostly fires before Elite labels exist (pre-Elite raw8 -0.16% vs elev_hc +1.27%). After 2026-08-20 the morning card is complete and raw8 +0.38% vs elev_hc +0.36% — often the same eight names. Healthcare already in top-15 is the good pocket; mid-band Healthcare as a whole is not an elevate. The mid-band keeper is Healthcare the morning after a red SPY day. Research overlay only. Do not change join_rules.json, LIVE_POLICY, or flatten_robust. Do not paste this 1d IC onto flatten_h5. Mid 16–30 is a diagnosis that the ranker is inverted — it is not a recipe that blindly buys ranks 16–30 every morning.

## Rank bands (1d open→close)

| Band | n | Win% | Mean oc | t |
|---|---:|---:|---:|---:|
| `all` | 134598 | 42.6% | -0.05% | -5.084 |
| `ranks_1_8` | 144 | 51.4% | +0.20% | 0.979 |
| `ranks_1_15` | 270 | 51.8% | +0.17% | 1.086 |
| `ranks_16_30` | 270 | 57.4% | +0.48% | 3.836 |
| `ranks_16_50` | 630 | 54.6% | +0.34% | 3.787 |
| `ranks_16_80` | 1170 | 51.6% | +0.30% | 4.609 |

## Avoid keepers (inside ranks 1–15)

A keeper needs n≥40, win-rate lift ≤ −3pp vs the top-15 band, and a worse mean than that band. Same-day tape never picks.

| Feature | Band | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `incomplete` | 1-15 | 90 | 45.6% | -6.3pp | +0.05% | 0.134 | Elite earnsurp/rsi/ext missing on the morning card |

## Elevate keepers (inside ranks 16–80)

A keeper needs n≥40, lift ≥ +3pp vs the 16–80 band, and a positive mean *and* mean edge. Mid 16–30 as a *band* is a diagnosis, not a buy-ranks-16–30 recipe.

| Feature | Band | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `hc_after_red` | 16-80 | 58 | 75.9% | +24.2pp | +0.71% | 4.138 | Healthcare the morning after a red SPY day |
| `news_good` | 16-80 | 51 | 56.9% | +5.2pp | +0.48% | 1.324 | pre-open News Title tone = good (09:30-knowable) |
| `last_red` | 16-80 | 265 | 56.2% | +4.6pp | +0.45% | 2.987 | prior bar open→close red (OHLC, leak-free) |

## Patched top-8 vs raw join top-8

`skip_junk` drops incomplete Elite cards, Consumer Defensive, and China ADRs, then refills from lower ranks. `elev_hc` does the same and then puts Healthcare names from ranks ≤80 first. `elev_hc_red` prefers Healthcare only the morning after a red SPY day (the mid-band keeper). Fee book is $10k / 8 names, whole shares, Futubull both sides. `mid1630` is diagnostic only.

| Book | n | Days | Win% | Mean oc | t | Fee $ | Both-tape |
|---|---:|---:|---:|---:|---:|---:|---|
| `raw8` | 144 | 18 | 51.4% | +0.20% | 0.979 | -943.53 | up n=112 win=54.5% mean=+0.17%; down n=32 win=40.6% mean=+0.30% |
| `skip_junk` | 144 | 18 | 52.1% | +0.29% | 1.433 | -255.64 | up n=112 win=51.8% mean=+0.10%; down n=32 win=53.1% mean=+0.95% |
| `elev_hc` | 144 | 18 | 58.3% | +0.66% | 3.056 | 370.17 | up n=112 win=58.0% mean=+0.46%; down n=32 win=59.4% mean=+1.39% |
| `elev_hc_cap3` | 144 | 18 | 54.2% | +0.42% | 1.972 | -58.52 | up n=112 win=53.6% mean=+0.14%; down n=32 win=56.2% mean=+1.40% |
| `elev_hc_red` | 144 | 18 | 61.1% | +0.49% | 2.467 | 62.77 | up n=112 win=62.5% mean=+0.36%; down n=32 win=56.2% mean=+0.95% |
| `mid1630` | 270 | 18 | 57.4% | +0.48% | 3.836 | 185.28 | up n=210 win=56.7% mean=+0.43%; down n=60 win=60.0% mean=+0.66% |

### Pre-Elite vs Elite-label days

Before 2026-08-20 the ranked file has no earnsurp/rsi/ext. That is when incomplete cards and China ADRs occupy #1–8. After Elite labels exist, skip_junk is often a no-op.

| Book | Window | n | Win% | Mean oc | Fee $ |
|---|---|---:|---:|---:|---:|
| `raw8` | pre_elite | 48 | 37.5% | -0.16% | -956.57 |
| `raw8` | elite | 96 | 58.3% | +0.38% | 13.04 |
| `skip_junk` | pre_elite | 48 | 43.8% | +0.24% | -198.42 |
| `skip_junk` | elite | 96 | 56.2% | +0.32% | -57.22 |
| `elev_hc` | pre_elite | 48 | 66.7% | +1.27% | 370.88 |
| `elev_hc` | elite | 96 | 54.2% | +0.36% | -0.7 |
| `elev_hc_red` | pre_elite | 48 | 64.6% | +0.73% | 66.25 |
| `elev_hc_red` | elite | 96 | 59.4% | +0.37% | -3.48 |

## Buried names the ranker parked mid-list

| Date | Ticker | Rank | oc | Sector | Complete |
|---|---|---:|---:|---|---|
| 2026-08-13 | `LVWR` | 69 | +16.90% | Consumer Cyclical | no |
| 2026-08-13 | `CREX` | 45 | +11.96% | Technology | no |
| 2026-08-17 | `FEDU` | 25 | +11.56% | Consumer Defensive | no |
| 2026-08-14 | `INO` | 50 | +10.75% | Healthcare | no |
| 2026-09-04 | `HOOD` | 51 | +9.60% | Financial | yes |
| 2026-08-28 | `CRWD` | 75 | +9.46% | Technology | yes |
| 2026-09-03 | `CABA` | 67 | +9.17% | Healthcare | yes |
| 2026-08-14 | `PROP` | 28 | +8.95% | Energy | no |
| 2026-08-14 | `KOPN` | 63 | +8.86% | Technology | no |
| 2026-08-28 | `FTNT` | 37 | +7.83% | Technology | yes |
| 2026-08-27 | `SMTC` | 21 | +7.56% | Technology | yes |
| 2026-08-14 | `AEVA` | 16 | +7.52% | Technology | no |

## Worst top-15 junk the ranker still printed

| Date | Ticker | Rank | oc | Why |
|---|---|---:|---:|---|
| 2026-08-17 | `GSUN` | 14 | -11.61% | incomplete, Consumer Defensive, China ADR |
| 2026-08-17 | `CANG` | 11 | -11.27% | incomplete, China ADR |
| 2026-08-14 | `GPRO` | 10 | -6.66% | incomplete |
| 2026-08-13 | `VERI` | 5 | -5.18% | incomplete |
| 2026-08-12 | `TRMB` | 5 | -5.15% | incomplete |
| 2026-08-17 | `DAO` | 4 | -4.86% | incomplete, Consumer Defensive, China ADR |
| 2026-08-14 | `VYX` | 14 | -3.58% | incomplete |
| 2026-08-17 | `RLX` | 6 | -3.51% | incomplete, Consumer Defensive, China ADR |
| 2026-08-14 | `TBCH` | 8 | -3.36% | incomplete |
| 2026-08-17 | `ZBAO` | 2 | -3.14% | incomplete, China ADR |

## How this is graded

1. Universe is the morning `data/join/YYYY-MM-DD_ranked.csv`.
2. Outcome is same-session Finviz Change from Open. Today's Change% / Gap / RelVol / OHLC never pick.
3. Incomplete = earnsurp / rsi / ext missing on that morning card.
4. News Title is used only when News Time is strictly before 09:30 ET.
5. Both-tape splits use that session's SPY close-to-close (robustness, not a 09:30 gate).
6. Do not change `join_rules.json` or live flatten unless a keeper clears the bar *and* Cyrus asks to wire it.
