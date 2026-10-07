# Stock-book paper trading

_Generated 2026-10-07T04:47:39 — calls 2026-08-13 → 2026-10-06_

**Strategy:** LONG-only · top 10/day by book · entry close (16:00 ET) · hold 1w (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Selection: `1d` stock-book buy list (prints ~13:00-15:45 ET, hence close entry).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $98,355.53 | **-1.64%** | 9.9% | 59 | 381 | 49.2% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 59 | 49.2% | $-1,644.48 |
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

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-09-04 16:00 ET | `SONO` | BUY | 677 | $15.36 | 2026-09-14 16:00 ET | $15.58 | $131.28 | 1.26% | small |
| 2026-09-04 16:00 ET | `FUBO` | BUY | 925 | $11.25 | 2026-09-14 16:00 ET | $11.87 | $549.40 | 5.28% | small |
| 2026-09-04 16:00 ET | `VNT` | BUY | 312 | $33.30 | 2026-09-14 16:00 ET | $32.35 | $-304.58 | -2.93% | mid |
| 2026-09-04 16:00 ET | `NU` | BUY | 677 | $15.37 | 2026-09-14 16:00 ET | $14.41 | $-667.57 | -6.42% | large |
| 2026-09-04 16:00 ET | `HOOD` | BUY | 85 | $122.11 | 2026-09-14 16:00 ET | $114.33 | $-665.89 | -6.42% | large |
| 2026-09-16 16:00 ET | `RDNT` | BUY | 135 | $75.78 | 2026-09-23 16:00 ET | $73.21 | $-351.85 | -3.44% | mid |
| 2026-09-16 16:00 ET | `BLFS` | BUY | 284 | $36.11 | 2026-09-23 16:00 ET | $37.74 | $455.46 | 4.44% | small |
| 2026-09-16 16:00 ET | `AVAH` | BUY | 719 | $14.26 | 2026-09-23 16:00 ET | $12.79 | $-1,075.68 | -10.49% | mid |
| 2026-09-16 16:00 ET | `STGW` | BUY | 1231 | $8.33 | 2026-09-23 16:00 ET | $8.48 | $152.60 | 1.49% | mid |
| 2026-09-16 16:00 ET | `DBX` | BUY | 271 | $37.74 | 2026-09-23 16:00 ET | $34.22 | $-961.04 | -9.4% | mid |
| 2026-09-16 16:00 ET | `SPT` | BUY | 910 | $11.27 | 2026-09-23 16:00 ET | $9.96 | $-1,220.35 | -11.9% | small |
| 2026-09-16 16:00 ET | `TENB` | BUY | 282 | $36.37 | 2026-09-23 16:00 ET | $36.23 | $-46.89 | -0.46% | mid |
| 2026-09-16 16:00 ET | `NYT` | BUY | 141 | $72.41 | 2026-09-23 16:00 ET | $61.89 | $-1,488.24 | -14.58% | large |
| 2026-09-16 16:00 ET | `FOXA` | BUY | 155 | $66.16 | 2026-09-23 16:00 ET | $63.96 | $-346.02 | -3.37% | large |
| 2026-09-16 16:00 ET | `FOX` | BUY | 173 | $59.15 | 2026-09-23 16:00 ET | $56.96 | $-384.00 | -3.75% | large |
| 2026-09-23 16:00 ET | `ARQT` | BUY | 369 | $26.38 | 2026-09-30 16:00 ET | $26.43 | $8.79 | 0.09% | mid |
| 2026-09-23 16:00 ET | `PGEN` | BUY | 1308 | $7.44 | 2026-09-30 16:00 ET | $8.56 | $1,430.91 | 14.7% | mid |
| 2026-09-23 16:00 ET | `ADMA` | BUY | 1003 | $9.70 | 2026-09-30 16:00 ET | $10.03 | $304.86 | 3.13% | mid |
| 2026-09-23 16:00 ET | `TH` | BUY | 459 | $21.20 | 2026-09-30 16:00 ET | $19.72 | $-691.31 | -7.1% | mid |
| 2026-09-23 16:00 ET | `CTOS` | BUY | 1023 | $9.51 | 2026-09-30 16:00 ET | $9.11 | $-435.84 | -4.48% | mid |
| 2026-09-23 16:00 ET | `PUBM` | BUY | 557 | $17.45 | 2026-09-30 16:00 ET | $18.73 | $695.62 | 7.15% | small |
| 2026-09-23 16:00 ET | `APPS` | BUY | 843 | $11.54 | 2026-09-30 16:00 ET | $11.10 | $-392.88 | -4.04% | small |
| 2026-09-23 16:00 ET | `PAYS` | BUY | 767 | $12.67 | 2026-09-30 16:00 ET | $13.48 | $601.28 | 6.19% | small |
| 2026-09-23 16:00 ET | `ELF` | BUY | 96 | $100.78 | 2026-09-30 16:00 ET | $100.97 | $13.59 | 0.14% | mid |
| 2026-09-23 16:00 ET | `NICE` | BUY | 82 | $117.79 | 2026-09-30 16:00 ET | $111.27 | $-539.20 | -5.58% | mid |

Full records: `data/book_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `BOOK_STRATEGY_SWEEP.md`. Dashboard: `dashboard/book-paper/index.html`.

