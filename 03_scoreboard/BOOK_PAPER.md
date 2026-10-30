# Stock-book paper trading

_Generated 2026-10-30T02:46:36 — calls 2026-08-13 → 2026-10-29_

**Strategy:** LONG-only · top 10/day by book · entry close (16:00 ET) · hold 1w (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Selection: `1d` stock-book buy list (prints ~13:00-15:45 ET, hence close entry).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $104,204.90 | **4.2%** | 9.9% | 87 | 495 | 57.5% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 87 | 57.5% | $4,204.92 |
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

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-09-30 16:00 ET | `MD` | BUY | 382 | $25.73 | 2026-10-07 16:00 ET | $25.83 | $28.20 | 0.29% | mid |
| 2026-09-30 16:00 ET | `FTNT` | BUY | 55 | $178.76 | 2026-10-07 16:00 ET | $189.28 | $574.20 | 5.84% | large |
| 2026-10-02 16:00 ET | `CDNA` | BUY | 147 | $67.15 | 2026-10-09 16:00 ET | $63.67 | $-516.52 | -5.23% | mid |
| 2026-10-02 16:00 ET | `SHOO` | BUY | 215 | $45.97 | 2026-10-09 16:00 ET | $44.30 | $-364.71 | -3.69% | mid |
| 2026-10-02 16:00 ET | `SIG` | BUY | 98 | $100.58 | 2026-10-09 16:00 ET | $105.22 | $450.06 | 4.57% | mid |
| 2026-10-02 16:00 ET | `FIGS` | BUY | 731 | $13.52 | 2026-10-09 16:00 ET | $15.78 | $1,632.99 | 16.52% | mid |
| 2026-10-09 16:00 ET | `PACS` | BUY | 233 | $43.30 | 2026-10-16 16:00 ET | $46.91 | $834.99 | 8.28% | mid |
| 2026-10-09 16:00 ET | `MQ` | BUY | 558 | $18.11 | 2026-10-16 16:00 ET | $18.69 | $309.07 | 3.06% | small |
| 2026-10-09 16:00 ET | `CON` | BUY | 272 | $37.19 | 2026-10-16 16:00 ET | $38.54 | $360.05 | 3.56% | mid |
| 2026-10-09 16:00 ET | `AMN` | BUY | 277 | $36.52 | 2026-10-16 16:00 ET | $40.49 | $1,092.41 | 10.8% | small |
| 2026-10-09 16:00 ET | `IT` | BUY | 52 | $191.15 | 2026-10-16 16:00 ET | $205.35 | $734.01 | 7.38% | large |
| 2026-10-09 16:00 ET | `BAND` | BUY | 189 | $53.27 | 2026-10-16 16:00 ET | $43.34 | $-1,881.98 | -18.69% | small |
| 2026-10-09 16:00 ET | `NTAP` | BUY | 42 | $237.15 | 2026-10-16 16:00 ET | $203.80 | $-1,405.01 | -14.11% | large |
| 2026-10-12 16:00 ET | `PACS` | BUY | 229 | $44.30 | 2026-10-19 16:00 ET | $45.85 | $348.92 | 3.44% | mid |
| 2026-10-12 16:00 ET | `CON` | BUY | 270 | $37.54 | 2026-10-19 16:00 ET | $38.18 | $165.71 | 1.63% | mid |
| 2026-10-12 16:00 ET | `AMN` | BUY | 280 | $36.28 | 2026-10-19 16:00 ET | $40.50 | $1,174.24 | 11.56% | small |
| 2026-10-16 16:00 ET | `PDFS` | BUY | 263 | $39.22 | 2026-10-23 16:00 ET | $39.32 | $19.39 | 0.19% | mid |
| 2026-10-16 16:00 ET | `MQ` | BUY | 552 | $18.69 | 2026-10-23 16:00 ET | $18.85 | $73.90 | 0.72% | small |
| 2026-10-16 16:00 ET | `PRCH` | BUY | 589 | $17.50 | 2026-10-23 16:00 ET | $17.73 | $120.09 | 1.17% | small |
| 2026-10-16 16:00 ET | `ARQT` | BUY | 358 | $28.82 | 2026-10-23 16:00 ET | $31.20 | $842.65 | 8.17% | mid |
| 2026-10-16 16:00 ET | `ETON` | BUY | 205 | $50.31 | 2026-10-23 16:00 ET | $50.00 | $-68.95 | -0.67% | small |
| 2026-10-16 16:00 ET | `WAY` | BUY | 377 | $27.36 | 2026-10-23 16:00 ET | $28.85 | $551.86 | 5.35% | mid |
| 2026-10-19 16:00 ET | `CDNA` | BUY | 155 | $67.08 | 2026-10-26 16:00 ET | $69.10 | $308.07 | 2.96% | mid |
| 2026-10-19 16:00 ET | `CBRL` | BUY | 151 | $69.02 | 2026-10-26 16:00 ET | $68.84 | $-32.17 | -0.31% | small |
| 2026-10-19 16:00 ET | `SHOO` | BUY | 232 | $44.96 | 2026-10-26 16:00 ET | $42.76 | $-516.50 | -4.95% | mid |

Full records: `data/book_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `BOOK_STRATEGY_SWEEP.md`. Dashboard: `dashboard/book-paper/index.html`.

