# Stock-book paper trading

_Generated 2026-09-10T06:11:46 — calls 2026-08-13 → 2026-09-08_

**Strategy:** LONG-only · top 10/day by book · entry close (16:00 ET) · hold 1w (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Selection: `1d` stock-book buy list (prints ~13:00-15:45 ET, hence close entry).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $104,122.47 | **4.12%** | 5.71% | 29 | 225 | 58.6% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 29 | 58.6% | $4,122.48 |
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
| 2026-08-28 | FLAT | 0.75 | 1 | **CLOSED** — predict FLAT score +0.75 < +1.00 | news judge: hawkish/bearish top items; events: high uncertainty |
| 2026-08-31 | DOWN | -5.85 | 0 | **CLOSED** — predict DOWN score -5.85 < +1.00 | — |
| 2026-09-01 | DOWN | -6.3 | 1 | **CLOSED** — predict DOWN score -6.30 < +1.00 | — |
| 2026-09-02 | DOWN | -3.825 | 2 | **CLOSED** — predict DOWN score -3.83 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-03 | FLAT | -0.9 | 3 | **CLOSED** — predict FLAT score -0.90 < +1.00 | news judge: hawkish/bearish top items |
| 2026-09-04 | UP | 2.25 | 0 | **OPEN** — predict score +2.25 >= +1.00 | — |
| 2026-09-08 | DOWN | -11.475 | 1 | **CLOSED** — predict DOWN score -11.47 < +1.00 | — |
| 2026-09-09 | DOWN | -13.95 | 0 | **CLOSED** — predict DOWN score -13.95 < +1.00 | — |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-13 16:00 ET | `VOR` | BUY | 428 | $23.29 | 2026-08-20 16:00 ET | $23.07 | $-107.49 | -1.08% | small |
| 2026-08-13 16:00 ET | `SGRY` | BUY | 657 | $15.19 | 2026-08-20 16:00 ET | $13.94 | $-838.39 | -8.4% | small |
| 2026-08-13 16:00 ET | `WW` | BUY | 690 | $14.45 | 2026-08-20 16:00 ET | $15.00 | $361.50 | 3.63% | micro |
| 2026-08-13 16:00 ET | `TGTX` | BUY | 208 | $47.94 | 2026-08-20 16:00 ET | $50.56 | $539.48 | 5.41% | mid |
| 2026-08-13 16:00 ET | `MBRX` | BUY | 17478 | $0.57 | 2026-08-20 16:00 ET | $0.59 | $38.62 | 0.39% | micro |
| 2026-08-20 16:00 ET | `ELF` | BUY | 106 | $98.46 | 2026-08-27 16:00 ET | $106.17 | $812.53 | 7.79% | mid |
| 2026-08-20 16:00 ET | `MOS` | BUY | 447 | $23.35 | 2026-08-27 16:00 ET | $23.76 | $171.57 | 1.64% | mid |
| 2026-08-20 16:00 ET | `AUPH` | BUY | 605 | $17.27 | 2026-08-27 16:00 ET | $16.48 | $-496.76 | -4.75% | mid |
| 2026-08-20 16:00 ET | `CE` | BUY | 222 | $46.98 | 2026-08-27 16:00 ET | $44.87 | $-474.26 | -4.55% | mid |
| 2026-08-20 16:00 ET | `OCUL` | BUY | 942 | $11.09 | 2026-08-27 16:00 ET | $10.82 | $-278.88 | -2.67% | mid |
| 2026-08-20 16:00 ET | `EPAM` | BUY | 97 | $106.76 | 2026-08-27 16:00 ET | $112.77 | $578.31 | 5.58% | mid |
| 2026-08-20 16:00 ET | `WRBY` | BUY | 380 | $27.46 | 2026-08-27 16:00 ET | $25.27 | $-842.14 | -8.07% | mid |
| 2026-08-20 16:00 ET | `CELH` | BUY | 321 | $32.54 | 2026-08-27 16:00 ET | $32.99 | $136.03 | 1.3% | mid |
| 2026-08-20 16:00 ET | `IRTC` | BUY | 81 | $127.98 | 2026-08-27 16:00 ET | $116.33 | $-948.20 | -9.15% | mid |
| 2026-08-20 16:00 ET | `CALX` | BUY | 258 | $40.38 | 2026-08-27 16:00 ET | $37.69 | $-700.80 | -6.73% | mid |
| 2026-08-27 16:00 ET | `RRC` | BUY | 246 | $41.64 | 2026-09-03 16:00 ET | $42.37 | $173.11 | 1.69% | mid |
| 2026-08-27 16:00 ET | `CRK` | BUY | 701 | $14.62 | 2026-09-03 16:00 ET | $14.95 | $213.05 | 2.08% | mid |
| 2026-08-27 16:00 ET | `ACMR` | BUY | 127 | $80.49 | 2026-09-03 16:00 ET | $69.42 | $-1,410.72 | -13.8% | mid |
| 2026-08-27 16:00 ET | `MOS` | BUY | 431 | $23.76 | 2026-09-03 16:00 ET | $25.49 | $734.35 | 7.17% | mid |
| 2026-08-27 16:00 ET | `ELF` | BUY | 96 | $106.17 | 2026-09-03 16:00 ET | $107.41 | $114.38 | 1.12% | mid |
| 2026-08-27 16:00 ET | `EPAM` | BUY | 90 | $112.77 | 2026-09-03 16:00 ET | $119.88 | $635.28 | 6.26% | mid |
| 2026-08-27 16:00 ET | `CXT` | BUY | 206 | $49.71 | 2026-09-03 16:00 ET | $49.99 | $52.25 | 0.51% | mid |
| 2026-08-27 16:00 ET | `XP` | BUY | 579 | $17.68 | 2026-09-03 16:00 ET | $20.00 | $1,328.15 | 12.97% | mid |
| 2026-08-27 16:00 ET | `MNDY` | BUY | 103 | $99.50 | 2026-09-03 16:00 ET | $97.33 | $-228.72 | -2.23% | mid |
| 2026-08-27 16:00 ET | `VNT` | BUY | 309 | $33.12 | 2026-09-03 16:00 ET | $33.02 | $-39.01 | -0.38% | mid |

Full records: `data/book_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `BOOK_STRATEGY_SWEEP.md`. Dashboard: `dashboard/book-paper/index.html`.

