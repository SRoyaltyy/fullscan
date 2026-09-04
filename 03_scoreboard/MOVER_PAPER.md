# Mover paper — .io fallback on soft-red 1d

_Generated 2026-09-04T10:37:07 — calls 2026-08-13 → 2026-09-03_

**Strategy:** LONG-only · top 10/day by cond / size · entry open+close (16:00 ET) · hold 1d (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). 1d .io size book at 16:00 when −3 < S < 0; mover 1d at 09:30 otherwise. S ≤ −3 takes no new 1d risk.

**Book:** .io fallback on soft-red mornings (−3 < S < 0), mover the rest (S ≥ 0 or missing). Hard-red S ≤ −3 = cash. Both books hold 1d. Open buys cannot spend the same day's close-sale cash. This window’s only soft-red morning is 2026-09-03; 1d .io cannot exit until the next session prints.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $101,546.51 | **1.55%** | 0.66% | 30 | 117 | 43.3% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 30 | 43.3% | $1,546.54 |
| SELL (short) | 0 | 0% | $0.00 |

## Day gate (per session)

| Date | Predict | Score | SPY streak | Book | Advisory |
|---|---|---:|---:|---|---|
| 2026-08-13 | UP | 8.525 | 0 | **MOVER** — predict +8.53 >= 0 — mover 1d (09:30) | mover source empty (no BUY calls) |
| 2026-08-14 | UP | 5.5 | 0 | **MOVER** — predict +5.50 >= 0 — mover 1d (09:30) | mover source empty (no BUY calls) |
| 2026-08-17 | UP | 2.25 | 1 | **MOVER** — predict +2.25 >= 0 — mover 1d (09:30) | — |
| 2026-08-18 | DOWN | -6.2 | 2 | **CASH** — predict -6.20 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-08-19 | DOWN | -7.2 | 3 | **CASH** — predict -7.20 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-08-20 | UP | 1.125 | 0 | **MOVER** — predict +1.12 >= 0 — mover 1d (09:30) | — |
| 2026-08-21 | UP | 3.25 | 1 | **MOVER** — predict +3.25 >= 0 — mover 1d (09:30) | — |
| 2026-08-24 | DOWN | -5.175 | 0 | **CASH** — predict -5.17 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-08-25 | UP | 1.8 | 1 | **MOVER** — predict +1.80 >= 0 — mover 1d (09:30) | — |
| 2026-08-26 | UP | 2.025 | 0 | **MOVER** — predict +2.02 >= 0 — mover 1d (09:30) | — |
| 2026-08-27 | — | — | 0 | **MOVER** — no predict on file — mover (the rest) | — |
| 2026-08-28 | FLAT | 0.75 | 1 | **MOVER** — predict +0.75 >= 0 — mover 1d (09:30) | — |
| 2026-08-30 | — | — | 0 | **MOVER** — no predict on file — mover (the rest) | mover source empty (no BUY calls) |
| 2026-08-31 | DOWN | -5.85 | 0 | **CASH** — predict -5.85 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-09-01 | DOWN | -6.3 | 1 | **CASH** — predict -6.30 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-09-02 | DOWN | -3.825 | 2 | **CASH** — predict -3.83 <= -3.0 — no new 1d risk | route cash — no new entries |
| 2026-09-03 | FLAT | -0.9 | 3 | **IO** — predict -0.90 in (-3.0, 0) — .io 1d size fallback (16:00) | 1d cannot settle (end of calendar) |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-20 09:30 ET | `IAG` | BUY | 515 | $19.63 | 2026-08-21 16:00 ET | $20.50 | $434.60 | 4.3% | mover |
| 2026-08-20 09:30 ET | `KGC` | BUY | 342 | $29.63 | 2026-08-21 16:00 ET | $31.43 | $606.64 | 5.99% | mover |
| 2026-08-20 09:30 ET | `NFGC` | BUY | 5842 | $1.75 | 2026-08-21 16:00 ET | $1.75 | $-151.77 | -1.48% | mover |
| 2026-08-20 09:30 ET | `WPM` | BUY | 70 | $144.54 | 2026-08-21 16:00 ET | $150.25 | $395.20 | 3.91% | mover |
| 2026-08-20 09:30 ET | `ABUS` | BUY | 2084 | $4.92 | 2026-08-21 16:00 ET | $4.77 | $-366.79 | -3.58% | mover |
| 2026-08-25 09:30 ET | `AU` | BUY | 85 | $119.46 | 2026-08-26 16:00 ET | $118.55 | $-81.94 | -0.81% | mover |
| 2026-08-25 09:30 ET | `ERO` | BUY | 268 | $38.00 | 2026-08-26 16:00 ET | $39.24 | $325.27 | 3.19% | mover |
| 2026-08-25 09:30 ET | `FCX` | BUY | 131 | $77.90 | 2026-08-26 16:00 ET | $77.49 | $-58.58 | -0.57% | mover |
| 2026-08-25 09:30 ET | `CNH` | BUY | 870 | $11.72 | 2026-08-26 16:00 ET | $11.62 | $-109.67 | -1.08% | mover |
| 2026-08-25 09:30 ET | `HMY` | BUY | 450 | $22.65 | 2026-08-26 16:00 ET | $22.50 | $-79.26 | -0.78% | mover |
| 2026-08-25 09:30 ET | `RHI` | BUY | 229 | $44.52 | 2026-08-26 16:00 ET | $44.48 | $-15.18 | -0.15% | mover |
| 2026-08-25 09:30 ET | `SUZ` | BUY | 1125 | $9.07 | 2026-08-26 16:00 ET | $8.94 | $-175.54 | -1.72% | mover |
| 2026-08-25 09:30 ET | `VALE` | BUY | 681 | $15.00 | 2026-08-26 16:00 ET | $15.01 | $-10.95 | -0.11% | mover |
| 2026-08-25 09:30 ET | `WPM` | BUY | 63 | $160.00 | 2026-08-26 16:00 ET | $158.25 | $-114.70 | -1.14% | mover |
| 2026-08-25 09:30 ET | `ABUS` | BUY | 1939 | $5.26 | 2026-08-26 16:00 ET | $5.20 | $-166.77 | -1.64% | mover |
| 2026-08-27 09:30 ET | `ACMR` | BUY | 125 | $80.97 | 2026-08-28 16:00 ET | $74.42 | $-823.58 | -8.14% | mover |
| 2026-08-27 09:30 ET | `GGB` | BUY | 2292 | $4.42 | 2026-08-28 16:00 ET | $4.46 | $32.08 | 0.32% | mover |
| 2026-08-27 09:30 ET | `MT` | BUY | 134 | $75.12 | 2026-08-28 16:00 ET | $74.53 | $-83.94 | -0.83% | mover |
| 2026-08-27 09:30 ET | `MU` | BUY | 10 | $925.74 | 2026-08-28 16:00 ET | $938.40 | $122.47 | 1.32% | mover |
| 2026-08-27 09:30 ET | `TX` | BUY | 183 | $55.20 | 2026-08-28 16:00 ET | $55.83 | $110.10 | 1.09% | mover |
| 2026-08-27 09:30 ET | `ANET` | BUY | 53 | $190.90 | 2026-08-28 16:00 ET | $202.25 | $597.16 | 5.9% | mover |
| 2026-08-27 09:30 ET | `ASML` | BUY | 5 | $1746.33 | 2026-08-28 16:00 ET | $1745.64 | $-7.53 | -0.09% | mover |
| 2026-08-27 09:30 ET | `DLO` | BUY | 654 | $15.60 | 2026-08-28 16:00 ET | $15.36 | $-174.03 | -1.71% | mover |
| 2026-08-27 09:30 ET | `GEN` | BUY | 352 | $28.89 | 2026-08-28 16:00 ET | $29.64 | $254.78 | 2.51% | mover |
| 2026-08-27 09:30 ET | `LRCX` | BUY | 32 | $314.61 | 2026-08-28 16:00 ET | $312.88 | $-59.63 | -0.59% | mover |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

