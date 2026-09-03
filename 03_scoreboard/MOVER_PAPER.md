# Mover paper trading

_Generated 2026-09-03T09:31:54 — calls 2026-08-13 → 2026-09-03_

**Strategy:** LONG-only · top 10/day by cond · entry open (09:30 ET) · hold 1d (exit 16:00 ET) · 5% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $104,751.59 | **4.75%** | 0.13% | 31 | 3894 | 61.3% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 31 | 61.3% | $4,751.59 |
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
| 2026-09-03 | — | — | 3 | **OPEN** — no predict on file — allowed | — |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-20 09:30 ET | `KGC` | BUY | 170 | $29.63 | 2026-08-21 16:00 ET | $32.76 | $527.03 | 10.46% | 7/2/0 |
| 2026-08-20 09:30 ET | `NFGC` | BUY | 2888 | $1.75 | 2026-08-21 16:00 ET | $1.84 | $184.88 | 3.66% | 7/2/0 |
| 2026-08-20 09:30 ET | `WPM` | BUY | 34 | $144.54 | 2026-08-21 16:00 ET | $157.78 | $445.93 | 9.07% | 7/2/0 |
| 2026-08-20 09:30 ET | `ABUS` | BUY | 1029 | $4.92 | 2026-08-21 16:00 ET | $5.21 | $271.65 | 5.37% | 6/2/0 |
| 2026-08-20 09:30 ET | `AEM` | BUY | 24 | $204.45 | 2026-08-21 16:00 ET | $216.06 | $274.47 | 5.59% | 7/2/1 |
| 2026-08-21 09:30 ET | `AU` | BUY | 43 | $119.43 | 2026-08-24 16:00 ET | $118.66 | $-37.40 | -0.73% | 9/1/0 |
| 2026-08-21 09:30 ET | `AUPH` | BUY | 299 | $17.20 | 2026-08-24 16:00 ET | $16.60 | $-187.21 | -3.64% | 8/0/0 |
| 2026-08-21 09:30 ET | `AEM` | BUY | 23 | $216.30 | 2026-08-24 16:00 ET | $214.08 | $-55.23 | -1.11% | 7/1/0 |
| 2026-08-21 09:30 ET | `ARCT` | BUY | 461 | $11.13 | 2026-08-24 16:00 ET | $13.76 | $1,200.41 | 23.4% | 7/1/0 |
| 2026-08-21 09:30 ET | `AUTL` | BUY | 2101 | $2.47 | 2026-08-24 16:00 ET | $2.38 | $-243.68 | -4.7% | 7/1/0 |
| 2026-08-21 09:30 ET | `CRDL` | BUY | 2685 | $1.93 | 2026-08-24 16:00 ET | $1.80 | $-418.81 | -8.08% | 7/1/0 |
| 2026-08-21 09:30 ET | `CRSP` | BUY | 86 | $59.72 | 2026-08-24 16:00 ET | $56.91 | $-246.21 | -4.79% | 7/1/0 |
| 2026-08-21 09:30 ET | `CYPH` | BUY | 3916 | $1.32 | 2026-08-24 16:00 ET | $1.64 | $1,151.38 | 22.27% | 7/1/0 |
| 2026-08-21 09:30 ET | `FUTU` | BUY | 45 | $115.18 | 2026-08-24 16:00 ET | $116.49 | $54.65 | 1.05% | 7/1/0 |
| 2026-08-21 09:30 ET | `GMAB` | BUY | 156 | $33.36 | 2026-08-24 16:00 ET | $33.06 | $-51.79 | -1.0% | 7/1/0 |
| 2026-08-27 09:30 ET | `ACMR` | BUY | 64 | $80.97 | 2026-08-28 16:00 ET | $80.49 | $-35.13 | -0.68% | 8/2/1 |
| 2026-08-27 09:30 ET | `GGB` | BUY | 1175 | $4.42 | 2026-08-28 16:00 ET | $4.70 | $298.44 | 5.75% | 8/1/1 |
| 2026-08-27 09:30 ET | `MT` | BUY | 69 | $75.12 | 2026-08-28 16:00 ET | $74.63 | $-38.26 | -0.74% | 8/1/1 |
| 2026-08-27 09:30 ET | `MU` | BUY | 5 | $925.74 | 2026-08-28 16:00 ET | $935.39 | $44.20 | 0.95% | 8/2/1 |
| 2026-08-27 09:30 ET | `TX` | BUY | 94 | $55.20 | 2026-08-28 16:00 ET | $55.83 | $54.62 | 1.05% | 8/1/1 |
| 2026-08-27 09:30 ET | `ANET` | BUY | 27 | $190.90 | 2026-08-28 16:00 ET | $201.09 | $270.94 | 5.26% | 7/2/1 |
| 2026-08-27 09:30 ET | `ASML` | BUY | 2 | $1746.33 | 2026-08-28 16:00 ET | $1735.01 | $-26.67 | -0.76% | 7/3/1 |
| 2026-08-27 09:30 ET | `DLO` | BUY | 334 | $15.60 | 2026-08-28 16:00 ET | $15.14 | $-162.35 | -3.12% | 7/2/1 |
| 2026-08-27 09:30 ET | `GEN` | BUY | 180 | $28.89 | 2026-08-28 16:00 ET | $30.50 | $284.67 | 5.47% | 7/2/1 |
| 2026-08-27 09:30 ET | `LRCX` | BUY | 16 | $314.61 | 2026-08-28 16:00 ET | $318.58 | $59.39 | 1.18% | 7/3/1 |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

