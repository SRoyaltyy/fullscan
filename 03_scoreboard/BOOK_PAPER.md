# Stock-book paper trading

_Generated 2026-09-03T11:35:01 — calls 2026-08-13 → 2026-09-03_

**Strategy:** LONG-only · top 10/day by book · entry close (16:00 ET) · hold 1w (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Selection: `1d` stock-book buy list (prints ~13:00-15:45 ET, hence close entry).

**Day gate:** trade only when the morning general predict score >= 1.0 (missing predict = allowed). News-judge hawkish items and high-uncertainty event binaries are advisory flags below.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $104,596.92 | **4.6%** | 4.06% | 29 | 216 | 55.2% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 29 | 55.2% | $4,554.34 |
| SELL (short) | 0 | 0% | $0.00 |

## Day gate (per session)

| Date | Predict | Score | SPY streak | Gate | Advisory |
|---|---|---:|---:|---|---|
| 2026-08-13 | UP | 8.525 | 0 | **OPEN** — predict score +8.53 >= +1.00 | — |
| 2026-08-14 | UP | 5.5 | 0 | **OPEN** — predict score +5.50 >= +1.00 | — |
| 2026-08-17 | UP | 2.25 | 1 | **OPEN** — predict score +2.25 >= +1.00 | — |
| 2026-08-18 | DOWN | -6.2 | 2 | **CLOSED** — predict DOWN score -6.20 < +1.00 | — |
| 2026-08-19 | DOWN | -7.2 | 3 | **CLOSED** — predict DOWN score -7.20 < +1.00 | — |
| 2026-08-20 | UP | 1.125 | 0 | **OPEN** — predict score +1.12 >= +1.00 | — |
| 2026-08-21 | UP | 3.25 | 1 | **OPEN** — predict score +3.25 >= +1.00 | events: high uncertainty |
| 2026-08-27 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-08-30 | — | — | 0 | **OPEN** — no predict on file — allowed | — |
| 2026-08-31 | DOWN | -5.85 | 0 | **CLOSED** — predict DOWN score -5.85 < +1.00 | — |
| 2026-09-01 | DOWN | -6.3 | 1 | **CLOSED** — predict DOWN score -6.30 < +1.00 | — |
| 2026-09-02 | DOWN | -3.825 | 2 | **CLOSED** — predict DOWN score -3.83 < +1.00 | news judge: hawkish/bearish top items |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-13 16:00 ET | `VOR` | BUY | 428 | $23.29 | 2026-08-20 16:00 ET | $23.07 | $-107.49 | -1.08% | small |
| 2026-08-13 16:00 ET | `SGRY` | BUY | 657 | $15.19 | 2026-08-20 16:00 ET | $13.94 | $-838.39 | -8.4% | small |
| 2026-08-13 16:00 ET | `WW` | BUY | 690 | $14.45 | 2026-08-20 16:00 ET | $15.00 | $361.50 | 3.63% | micro |
| 2026-08-13 16:00 ET | `TGTX` | BUY | 208 | $47.94 | 2026-08-20 16:00 ET | $50.56 | $539.48 | 5.41% | mid |
| 2026-08-13 16:00 ET | `MBRX` | BUY | 17478 | $0.57 | 2026-08-20 16:00 ET | $0.59 | $38.62 | 0.39% | micro |
| 2026-08-20 16:00 ET | `ELF` | BUY | 106 | $98.46 | 2026-08-27 16:00 ET | $106.97 | $897.33 | 8.6% | mid |
| 2026-08-20 16:00 ET | `MOS` | BUY | 447 | $23.35 | 2026-08-27 16:00 ET | $24.16 | $350.37 | 3.36% | mid |
| 2026-08-20 16:00 ET | `AUPH` | BUY | 605 | $17.27 | 2026-08-27 16:00 ET | $16.54 | $-457.44 | -4.38% | mid |
| 2026-08-20 16:00 ET | `CE` | BUY | 222 | $46.98 | 2026-08-27 16:00 ET | $44.70 | $-512.00 | -4.91% | mid |
| 2026-08-20 16:00 ET | `OCUL` | BUY | 942 | $11.09 | 2026-08-27 16:00 ET | $10.77 | $-325.98 | -3.12% | mid |
| 2026-08-20 16:00 ET | `EPAM` | BUY | 97 | $106.76 | 2026-08-27 16:00 ET | $109.36 | $247.54 | 2.39% | mid |
| 2026-08-20 16:00 ET | `WRBY` | BUY | 380 | $27.46 | 2026-08-27 16:00 ET | $25.48 | $-762.34 | -7.31% | mid |
| 2026-08-20 16:00 ET | `CELH` | BUY | 321 | $32.54 | 2026-08-27 16:00 ET | $35.23 | $855.07 | 8.19% | mid |
| 2026-08-20 16:00 ET | `IRTC` | BUY | 81 | $127.98 | 2026-08-27 16:00 ET | $119.15 | $-719.78 | -6.94% | mid |
| 2026-08-20 16:00 ET | `CALX` | BUY | 258 | $40.38 | 2026-08-27 16:00 ET | $38.36 | $-527.94 | -5.07% | mid |
| 2026-08-27 16:00 ET | `RRC` | BUY | 249 | $41.55 | 2026-09-03 16:00 ET | $42.40 | $205.10 | 1.98% | mid |
| 2026-08-27 16:00 ET | `CRK` | BUY | 714 | $14.50 | 2026-09-03 16:00 ET | $16.02 | $1,066.65 | 10.3% | mid |
| 2026-08-27 16:00 ET | `ACMR` | BUY | 130 | $79.11 | 2026-09-03 16:00 ET | $70.04 | $-1,183.95 | -11.51% | mid |
| 2026-08-27 16:00 ET | `MOS` | BUY | 428 | $24.16 | 2026-09-03 16:00 ET | $24.78 | $254.16 | 2.46% | mid |
| 2026-08-27 16:00 ET | `ELF` | BUY | 96 | $106.97 | 2026-09-03 16:00 ET | $105.54 | $-141.93 | -1.38% | mid |
| 2026-08-27 16:00 ET | `EPAM` | BUY | 94 | $109.36 | 2026-09-03 16:00 ET | $116.34 | $651.47 | 6.34% | mid |
| 2026-08-27 16:00 ET | `CXT` | BUY | 204 | $50.64 | 2026-09-03 16:00 ET | $49.16 | $-307.30 | -2.97% | mid |
| 2026-08-27 16:00 ET | `XP` | BUY | 581 | $17.81 | 2026-09-03 16:00 ET | $18.72 | $513.54 | 4.96% | mid |
| 2026-08-27 16:00 ET | `MNDY` | BUY | 111 | $92.55 | 2026-09-03 16:00 ET | $96.51 | $434.81 | 4.23% | mid |
| 2026-08-27 16:00 ET | `VNT` | BUY | 307 | $33.69 | 2026-09-03 16:00 ET | $31.84 | $-576.00 | -5.57% | mid |

Full records: `data/book_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `BOOK_STRATEGY_SWEEP.md`. Dashboard: `dashboard/book-paper/index.html`.

