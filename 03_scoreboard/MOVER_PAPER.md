# Mover paper — empty list + skip days defer to live .io 2w_size

_Generated 2026-09-04T11:44:42 — calls 2026-08-13 → 2026-09-03_

**Strategy:** LONG-only · top 10/day by cond / live 2w_size · entry open+close (16:00 ET) · hold 1d / 2w_size mark (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Mover 1d at 09:30 when S ≥ +1 and the BUY list is non-empty. Empty BUY list and S ≥ 0 takes live .io 2w_size (source gap). Every mover-skip morning (S < +1, including hard-red) also takes that mark.

**Book:** skip days and empty non-negative mornings defer to live .io `2w_size` (same sleeve as the .io dashboard). 0 fills while calls exist is not a gap — cash is still in yesterday’s 1d holds. Hard-red S ≤ −3 blocks new 1d risk; it does not flatten 2w_size.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $117,501.56 | **17.5%** | 0.63% | 40 | 0 | 50.0% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 40 | 50.0% | $17,266.81 |
| SELL (short) | 0 | 0% | $0.00 |

## Day gate (per session)

| Date | Predict | Score | SPY streak | Book | Advisory |
|---|---|---:|---:|---|---|
| 2026-08-13 | UP | 8.525 | 0 | **IO-GAP** — mover BUY list empty and predict +8.53 >= 0 — live .io 2w_size mark (not a new 1d ticket) | no live 2w_size print (gap) |
| 2026-08-14 | UP | 5.5 | 0 | **IO-GAP** — mover BUY list empty and predict +5.50 >= 0 — live .io 2w_size mark (not a new 1d ticket) | live 2w_size +3.17% |
| 2026-08-17 | UP | 2.25 | 1 | **MOVER** — predict +2.25 >= +1.0 — mover 1d (09:30) | — |
| 2026-08-18 | DOWN | -6.2 | 2 | **IO** — predict -6.20 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.97% |
| 2026-08-19 | DOWN | -7.2 | 3 | **IO** — predict -7.20 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +4.08% |
| 2026-08-20 | UP | 1.125 | 0 | **MOVER** — predict +1.12 >= +1.0 — mover 1d (09:30) | — |
| 2026-08-21 | UP | 3.25 | 1 | **MOVER** — predict +3.25 >= +1.0 — mover 1d (09:30) | — |
| 2026-08-24 | DOWN | -5.175 | 0 | **IO** — predict -5.17 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | no live 2w_size print (gap) |
| 2026-08-25 | UP | 1.8 | 1 | **MOVER** — predict +1.80 >= +1.0 — mover 1d (09:30) | — |
| 2026-08-26 | UP | 2.025 | 0 | **MOVER** — predict +2.02 >= +1.0 — mover 1d (09:30) | — |
| 2026-08-27 | — | — | 0 | **MOVER** — no predict on file — mover (the rest) | — |
| 2026-08-28 | FLAT | 0.75 | 1 | **IO** — predict +0.75 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | no live 2w_size print (gap) |
| 2026-08-31 | DOWN | -5.85 | 0 | **IO** — predict -5.85 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.23% |
| 2026-09-01 | DOWN | -6.3 | 1 | **IO** — predict -6.30 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.35% |
| 2026-09-02 | DOWN | -3.825 | 2 | **IO** — predict -3.83 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.91% |
| 2026-09-03 | FLAT | -0.9 | 3 | **IO** — predict -0.90 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.99% |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
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
| 2026-08-13 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-13 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-14 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-14 16:00 ET | $0.00 | $3,166.97 | 3.17% | io |
| 2026-08-18 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-18 16:00 ET | $0.00 | $2,040.33 | 1.97% | io |
| 2026-08-19 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-19 16:00 ET | $0.00 | $4,306.12 | 4.08% | io |
| 2026-08-24 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-24 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-28 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-28 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-31 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-31 16:00 ET | $0.00 | $253.03 | 0.23% | io |
| 2026-09-01 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-01 16:00 ET | $0.00 | $1,503.68 | 1.35% | io |
| 2026-09-02 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-02 16:00 ET | $0.00 | $2,156.14 | 1.91% | io |
| 2026-09-03 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-03 16:00 ET | $0.00 | $2,294.00 | 1.99% | io |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

