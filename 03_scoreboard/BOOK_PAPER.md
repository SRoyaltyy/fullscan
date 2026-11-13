# Stock-book paper trading

_Generated 2026-11-13T03:48:29 — calls 2026-08-13 → 2026-11-12_

**Strategy:** LONG-only · top 10/day by book · entry close (16:00 ET) · hold 1w (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Selection: `1d` stock-book buy list (prints ~13:00-15:45 ET, hence close entry).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $106,063.23 | **6.06%** | 9.9% | 107 | 582 | 57.0% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 107 | 57.0% | $6,063.23 |
| SELL (short) | 0 | 0% | $0.00 |

## Day gate (per session)

| Date | Predict | Score | SPY streak | Book | Advisory |
|---|---|---:|---:|---|---|
| 2026-08-13 | UP | 8.525 | 0 | **OPEN** — predict score +8.53 >= +1.00 | — |
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
| 2026-08-28 | FLAT | 0.75 | 0 | **CLOSED** — predict FLAT score +0.75 < +1.00 | news judge: hawkish/bearish top items; events: high uncertainty |
| 2026-08-31 | DOWN | -5.85 | 1 | **CLOSED** — predict DOWN score -5.85 < +1.00 | — |
| 2026-09-01 | DOWN | -6.3 | 2 | **CLOSED** — predict DOWN score -6.30 < +1.00 | — |
| 2026-09-02 | DOWN | -3.825 | 3 | **CLOSED** — predict DOWN score -3.83 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-03 | FLAT | -0.9 | 0 | **CLOSED** — predict FLAT score -0.90 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-04 | UP | 2.25 | 0 | **OPEN** — predict score +2.25 >= +1.00 | — |
| 2026-09-08 | DOWN | -11.475 | 1 | **CLOSED** — predict DOWN score -11.47 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-09 | DOWN | -13.95 | 2 | **CLOSED** — predict DOWN score -13.95 < +1.00 | — |
| 2026-09-10 | DOWN | -13.275 | 2 | **CLOSED** — predict DOWN score -13.28 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-11 | FLAT | 0.5 | 2 | **CLOSED** — predict FLAT score +0.50 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-14 | DOWN | -11.002 | 3 | **CLOSED** — predict DOWN score -11.00 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-15 | DOWN | -3.836 | 3 | **CLOSED** — predict DOWN score -3.84 < +1.00 | news judge: hawkish/bearish top items; events: high uncertainty |
| 2026-09-16 | UP | 5.297 | 3 | **OPEN** — predict score +5.30 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-09-17 | UP | 7.383 | 3 | **OPEN** — predict score +7.38 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-09-18 | UP | 4.861 | 3 | **OPEN** — predict score +4.86 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-09-21 | UP | 12.871 | 4 | **OPEN** — predict score +12.87 >= +1.00 | — |
| 2026-09-22 | DOWN | -0.497 | 4 | **CLOSED** — predict DOWN score -0.50 < +1.00 | — |
| 2026-09-23 | UP | 2.293 | 4 | **OPEN** — predict score +2.29 >= +1.00 | — |
| 2026-09-24 | DOWN | -7.659 | 4 | **CLOSED** — predict DOWN score -7.66 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-25 | UP | 2.706 | 4 | **OPEN** — predict score +2.71 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-09-28 | DOWN | -6.14 | 0 | **CLOSED** — predict DOWN score -6.14 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-29 | UP | 1.292 | 0 | **OPEN** — predict score +1.29 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-09-30 | UP | 2.009 | 0 | **OPEN** — predict score +2.01 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-10-01 | DOWN | -1.343 | 0 | **CLOSED** — predict DOWN score -1.34 < +1.00 | news judge: hawkish/bearish top items |
| 2026-10-02 | UP | 4.365 | 0 | **OPEN** — predict score +4.37 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-10-05 | FLAT | 0.086 | 0 | **CLOSED** — predict FLAT score +0.09 < +1.00 | news judge: hawkish/bearish top items |
| 2026-10-06 | UP | 2.623 | 0 | **OPEN** — predict score +2.62 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-10-07 | DOWN | -5.211 | 0 | **CLOSED** — predict DOWN score -5.21 < +1.00 | news judge: hawkish/bearish top items |
| 2026-10-08 | DOWN | -6.787 | 0 | **CLOSED** — predict DOWN score -6.79 < +1.00 | news judge: hawkish/bearish top items |
| 2026-10-09 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-10-12 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-10-13 | DOWN | -6.767 | 0 | **CLOSED** — predict DOWN score -6.77 < +1.00 | news judge: hawkish/bearish top items |
| 2026-10-14 | DOWN | -1.645 | 0 | **CLOSED** — predict DOWN score -1.65 < +1.00 | — |
| 2026-10-15 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-10-16 | UP | 5.526 | 0 | **OPEN** — predict score +5.53 >= +1.00 | — |
| 2026-10-19 | UP | 2.125 | 1 | **OPEN** — predict score +2.12 >= +1.00 | — |
| 2026-10-20 | DOWN | -8.071 | 1 | **CLOSED** — predict DOWN score -8.07 < +1.00 | — |
| 2026-10-21 | UP | 4.644 | 1 | **OPEN** — predict score +4.64 >= +1.00 | — |
| 2026-10-22 | DOWN | -14.314 | 1 | **CLOSED** — predict DOWN score -14.31 < +1.00 | — |
| 2026-10-23 | DOWN | -7.538 | 1 | **CLOSED** — predict DOWN score -7.54 < +1.00 | — |
| 2026-10-26 | DOWN | -2.128 | 1 | **CLOSED** — predict DOWN score -2.13 < +1.00 | — |
| 2026-10-27 | UP | 5.515 | 1 | **OPEN** — predict score +5.51 >= +1.00 | — |
| 2026-10-28 | UP | 6.621 | 1 | **OPEN** — predict score +6.62 >= +1.00 | — |
| 2026-10-29 | DOWN | -6.946 | 1 | **CLOSED** — predict DOWN score -6.95 < +1.00 | events: high uncertainty |
| 2026-10-30 | DOWN | -12.921 | 1 | **CLOSED** — predict DOWN score -12.92 < +1.00 | — |
| 2026-11-02 | UP | 3.044 | 1 | **OPEN** — predict score +3.04 >= +1.00 | — |
| 2026-11-03 | DOWN | -5.632 | 1 | **CLOSED** — predict DOWN score -5.63 < +1.00 | — |
| 2026-11-04 | UP | 10.848 | 1 | **OPEN** — predict score +10.85 >= +1.00 | — |
| 2026-11-05 | DOWN | -2.495 | 1 | **CLOSED** — predict DOWN score -2.50 < +1.00 | — |
| 2026-11-06 | — | — | 1 | **OPEN** — no predict on file — allowed | — |
| 2026-11-09 | UP | 9.449 | 1 | **OPEN** — predict score +9.45 >= +1.00 | — |
| 2026-11-10 | UP | 6.883 | 1 | **OPEN** — predict score +6.88 >= +1.00 | — |
| 2026-11-11 | UP | 12.369 | 1 | **OPEN** — predict score +12.37 >= +1.00 | news judge: hawkish/bearish top items |
| 2026-11-12 | DOWN | -7.158 | 1 | **CLOSED** — predict DOWN score -7.16 < +1.00 | — |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-10-16 16:00 ET | `ETON` | BUY | 205 | $50.31 | 2026-10-23 16:00 ET | $50.00 | $-68.95 | -0.67% | small |
| 2026-10-16 16:00 ET | `WAY` | BUY | 377 | $27.36 | 2026-10-23 16:00 ET | $28.85 | $551.86 | 5.35% | mid |
| 2026-10-19 16:00 ET | `CDNA` | BUY | 155 | $67.08 | 2026-10-26 16:00 ET | $69.10 | $308.07 | 2.96% | mid |
| 2026-10-19 16:00 ET | `PRCH` | BUY | 592 | $17.65 | 2026-10-26 16:00 ET | $17.37 | $-181.22 | -1.73% | small |
| 2026-10-19 16:00 ET | `CBRL` | BUY | 151 | $69.02 | 2026-10-26 16:00 ET | $68.84 | $-32.17 | -0.31% | small |
| 2026-10-27 16:00 ET | `CDNA` | BUY | 148 | $70.53 | 2026-11-03 16:00 ET | $88.47 | $2,650.13 | 25.39% | mid |
| 2026-10-27 16:00 ET | `PACS` | BUY | 218 | $47.85 | 2026-11-03 16:00 ET | $46.68 | $-260.80 | -2.5% | mid |
| 2026-10-27 16:00 ET | `CBRL` | BUY | 153 | $68.19 | 2026-11-03 16:00 ET | $68.33 | $16.41 | 0.16% | small |
| 2026-10-27 16:00 ET | `SHOO` | BUY | 244 | $42.78 | 2026-11-03 16:00 ET | $44.03 | $298.58 | 2.86% | mid |
| 2026-10-27 16:00 ET | `FIGS` | BUY | 510 | $20.48 | 2026-11-03 16:00 ET | $20.82 | $160.07 | 1.53% | mid |
| 2026-10-27 16:00 ET | `LAUR` | BUY | 277 | $37.64 | 2026-11-03 16:00 ET | $35.49 | $-602.82 | -5.78% | mid |
| 2026-10-27 16:00 ET | `COP` | BUY | 70 | $147.96 | 2026-11-03 16:00 ET | $144.40 | $-253.69 | -2.45% | large |
| 2026-10-27 16:00 ET | `PEB` | BUY | 490 | $21.31 | 2026-11-03 16:00 ET | $21.95 | $300.79 | 2.88% | mid |
| 2026-10-27 16:00 ET | `CHEF` | BUY | 95 | $108.99 | 2026-11-03 16:00 ET | $100.95 | $-768.44 | -7.42% | mid |
| 2026-10-27 16:00 ET | `SN` | BUY | 52 | $197.53 | 2026-11-03 16:00 ET | $210.59 | $674.73 | 6.57% | large |
| 2026-11-04 16:00 ET | `CDNA` | BUY | 124 | $85.86 | 2026-11-11 16:00 ET | $81.17 | $-586.38 | -5.51% | mid |
| 2026-11-04 16:00 ET | `PRCH` | BUY | 495 | $21.56 | 2026-11-11 16:00 ET | $22.14 | $274.15 | 2.57% | small |
| 2026-11-04 16:00 ET | `CNK` | BUY | 264 | $40.38 | 2026-11-11 16:00 ET | $41.32 | $241.21 | 2.26% | mid |
| 2026-11-04 16:00 ET | `LAUR` | BUY | 303 | $35.15 | 2026-11-11 16:00 ET | $35.61 | $131.42 | 1.23% | mid |
| 2026-11-04 16:00 ET | `CHEF` | BUY | 99 | $106.84 | 2026-11-11 16:00 ET | $107.30 | $40.86 | 0.39% | mid |
| 2026-11-04 16:00 ET | `PEB` | BUY | 480 | $22.20 | 2026-11-11 16:00 ET | $22.05 | $-84.55 | -0.79% | mid |
| 2026-11-04 16:00 ET | `AVO` | BUY | 911 | $11.71 | 2026-11-11 16:00 ET | $11.78 | $40.03 | 0.38% | small |
| 2026-11-04 16:00 ET | `ABX` | BUY | 1179 | $9.05 | 2026-11-11 16:00 ET | $9.01 | $-77.86 | -0.73% | small |
| 2026-11-04 16:00 ET | `TRU` | BUY | 129 | $82.13 | 2026-11-11 16:00 ET | $81.96 | $-26.79 | -0.25% | large |
| 2026-11-04 16:00 ET | `ILMN` | BUY | 47 | $226.54 | 2026-11-11 16:00 ET | $212.93 | $-644.02 | -6.05% | large |

Full records: `data/book_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `BOOK_STRATEGY_SWEEP.md`. Dashboard: `dashboard/book-paper/index.html`.

