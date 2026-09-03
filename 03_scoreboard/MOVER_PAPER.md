# Mover paper trading

_Generated 2026-09-03T11:34:38 — calls 2026-08-13 → 2026-09-03_

**Strategy:** LONG-only · top 10/day by cond · entry open (09:30 ET) · hold 1d (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $109,259.07 | **9.26%** | 0.12% | 29 | 3917 | 62.1% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 29 | 62.1% | $9,259.08 |
| SELL (short) | 0 | 0% | $0.00 |

## Day gate (per session)

| Date | Predict | Score | SPY streak | Gate | Advisory |
|---|---|---:|---:|---|---|
| 2026-08-14 | UP | 5.5 | 0 | **OPEN** — predict score +5.50 >= +1.00 | — |
| 2026-08-17 | UP | 2.25 | 1 | **OPEN** — predict score +2.25 >= +1.00 | — |
| 2026-08-18 | DOWN | -6.2 | 2 | **CLOSED** — predict DOWN score -6.20 < +1.00 | — |
| 2026-08-19 | DOWN | -7.2 | 3 | **CLOSED** — predict DOWN score -7.20 < +1.00 | — |
| 2026-08-20 | UP | 1.125 | 0 | **OPEN** — predict score +1.12 >= +1.00 | — |
| 2026-08-21 | UP | 3.25 | 1 | **OPEN** — predict score +3.25 >= +1.00 | events: high uncertainty |
| 2026-08-24 | DOWN | -5.175 | 0 | **CLOSED** — predict DOWN score -5.17 < +1.00 | news judge: hawkish/bearish top items |
| 2026-08-25 | UP | 1.8 | 1 | **OPEN** — predict score +1.80 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-08-26 | UP | 2.025 | 0 | **OPEN** — predict score +2.02 >= +1.00 | events: high uncertainty |
| 2026-08-27 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-08-28 | FLAT | 0.75 | 1 | **CLOSED** — predict FLAT score +0.75 < +1.00 | news judge: hawkish/bearish top items; events: high uncertainty |
| 2026-08-31 | DOWN | -5.85 | 0 | **CLOSED** — predict DOWN score -5.85 < +1.00 | — |
| 2026-09-01 | DOWN | -6.3 | 1 | **CLOSED** — predict DOWN score -6.30 < +1.00 | — |
| 2026-09-02 | DOWN | -3.825 | 2 | **CLOSED** — predict DOWN score -3.83 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-03 | FLAT | -0.9 | 3 | **CLOSED** — predict FLAT score -0.90 < +1.00 | news judge: hawkish/bearish top items |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-20 09:30 ET | `HDSN` | BUY | 1760 | $5.77 | 2026-08-21 16:00 ET | $5.63 | $-292.18 | -2.88% | 7/2/0 |
| 2026-08-20 09:30 ET | `IAG` | BUY | 515 | $19.63 | 2026-08-21 16:00 ET | $21.14 | $764.19 | 7.56% | 7/2/0 |
| 2026-08-20 09:30 ET | `KGC` | BUY | 342 | $29.63 | 2026-08-21 16:00 ET | $32.76 | $1,061.49 | 10.48% | 7/2/0 |
| 2026-08-20 09:30 ET | `NFGC` | BUY | 5842 | $1.75 | 2026-08-21 16:00 ET | $1.84 | $374.00 | 3.66% | 7/2/0 |
| 2026-08-20 09:30 ET | `WPM` | BUY | 70 | $144.54 | 2026-08-21 16:00 ET | $157.78 | $922.30 | 9.12% | 7/2/0 |
| 2026-08-20 09:30 ET | `ABUS` | BUY | 2084 | $4.92 | 2026-08-21 16:00 ET | $5.21 | $550.16 | 5.37% | 6/2/0 |
| 2026-08-21 09:30 ET | `AU` | BUY | 88 | $119.43 | 2026-08-24 16:00 ET | $118.66 | $-72.36 | -0.69% | 9/1/0 |
| 2026-08-21 09:30 ET | `AUPH` | BUY | 612 | $17.20 | 2026-08-24 16:00 ET | $16.60 | $-383.17 | -3.64% | 8/0/0 |
| 2026-08-21 09:30 ET | `AEM` | BUY | 48 | $216.30 | 2026-08-24 16:00 ET | $214.08 | $-110.92 | -1.07% | 7/1/0 |
| 2026-08-21 09:30 ET | `ARCT` | BUY | 943 | $11.13 | 2026-08-24 16:00 ET | $13.76 | $2,455.50 | 23.4% | 7/1/0 |
| 2026-08-21 09:30 ET | `AUTL` | BUY | 4338 | $2.47 | 2026-08-24 16:00 ET | $2.38 | $-503.14 | -4.7% | 7/1/0 |
| 2026-08-21 09:30 ET | `CRDL` | BUY | 5535 | $1.93 | 2026-08-24 16:00 ET | $1.80 | $-863.35 | -8.08% | 7/1/0 |
| 2026-08-21 09:30 ET | `CRSP` | BUY | 178 | $59.72 | 2026-08-24 16:00 ET | $56.91 | $-505.33 | -4.75% | 7/1/0 |
| 2026-08-21 09:30 ET | `CYPH` | BUY | 8056 | $1.32 | 2026-08-24 16:00 ET | $1.64 | $2,368.63 | 22.27% | 7/1/0 |
| 2026-08-21 09:30 ET | `FUTU` | BUY | 92 | $115.18 | 2026-08-24 16:00 ET | $116.49 | $115.88 | 1.09% | 7/1/0 |
| 2026-08-27 09:30 ET | `ACMR` | BUY | 132 | $80.97 | 2026-08-28 16:00 ET | $80.49 | $-68.24 | -0.64% | 8/2/1 |
| 2026-08-27 09:30 ET | `GGB` | BUY | 2430 | $4.42 | 2026-08-28 16:00 ET | $4.70 | $617.21 | 5.75% | 8/1/1 |
| 2026-08-27 09:30 ET | `MT` | BUY | 143 | $75.12 | 2026-08-28 16:00 ET | $74.63 | $-75.02 | -0.7% | 8/1/1 |
| 2026-08-27 09:30 ET | `MU` | BUY | 11 | $925.74 | 2026-08-28 16:00 ET | $935.39 | $102.01 | 1.0% | 8/2/1 |
| 2026-08-27 09:30 ET | `TX` | BUY | 194 | $55.20 | 2026-08-28 16:00 ET | $55.83 | $116.96 | 1.09% | 8/1/1 |
| 2026-08-27 09:30 ET | `ANET` | BUY | 56 | $190.90 | 2026-08-28 16:00 ET | $201.09 | $566.22 | 5.3% | 7/2/1 |
| 2026-08-27 09:30 ET | `ASML` | BUY | 6 | $1746.33 | 2026-08-28 16:00 ET | $1735.01 | $-72.03 | -0.69% | 7/3/1 |
| 2026-08-27 09:30 ET | `DLO` | BUY | 693 | $15.60 | 2026-08-28 16:00 ET | $15.14 | $-336.86 | -3.12% | 7/2/1 |
| 2026-08-27 09:30 ET | `GEN` | BUY | 373 | $28.89 | 2026-08-28 16:00 ET | $30.50 | $590.76 | 5.48% | 7/2/1 |
| 2026-08-27 09:30 ET | `LRCX` | BUY | 34 | $314.61 | 2026-08-28 16:00 ET | $318.58 | $130.70 | 1.22% | 7/3/1 |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

