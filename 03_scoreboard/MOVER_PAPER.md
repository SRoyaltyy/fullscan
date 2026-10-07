# Mover paper — empty list + skip days defer to live .io 2w_size

_Generated 2026-10-07T04:47:26 — calls 2026-08-13 → 2026-10-06_

**Strategy:** LONG-only · top 10/day by cond / live 2w_size · entry open+close (16:00 ET) · hold 1d / 2w_size mark (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Mover 1d at 09:30 when S ≥ +1 and the BUY list is non-empty. Empty BUY list and S ≥ 0 takes live .io 2w_size (source gap). Every mover-skip morning (S < +1, including hard-red) also takes that mark.

**Book:** skip days and empty non-negative mornings defer to live .io `2w_size` (same sleeve as the .io dashboard). 0 fills while calls exist is not a gap — cash is still in yesterday’s 1d holds. Hard-red S ≤ −3 blocks new 1d risk; it does not flatten 2w_size.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $114,705.15 | **14.71%** | 7.59% | 119 | 0 | 55.5% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 119 | 55.5% | $10,265.00 |
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
| 2026-08-28 | FLAT | 0.75 | 0 | **IO** — predict +0.75 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | no live 2w_size print (gap) |
| 2026-08-31 | DOWN | -5.85 | 1 | **IO** — predict -5.85 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.23% |
| 2026-09-01 | DOWN | -6.3 | 2 | **IO** — predict -6.30 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.34% |
| 2026-09-02 | DOWN | -3.825 | 3 | **IO** — predict -3.83 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.91% |
| 2026-09-03 | FLAT | -0.9 | 0 | **IO** — predict -0.90 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.99% |
| 2026-09-04 | UP | 2.25 | 0 | **MOVER** — predict +2.25 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-08 | DOWN | -11.475 | 1 | **IO** — predict -11.47 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -3.01% |
| 2026-09-09 | DOWN | -13.95 | 2 | **IO** — predict -13.95 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -0.26% |
| 2026-09-10 | DOWN | -13.275 | 2 | **IO** — predict -13.28 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.19% |
| 2026-09-11 | FLAT | 0.5 | 2 | **IO** — predict +0.50 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.22% |
| 2026-09-14 | DOWN | -11.002 | 3 | **IO** — predict -11.00 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -3.11% |
| 2026-09-15 | DOWN | -3.836 | 3 | **IO** — predict -3.84 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.23% |
| 2026-09-16 | UP | 5.297 | 3 | **MOVER** — predict +5.30 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-17 | UP | 7.383 | 3 | **MOVER** — predict +7.38 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-18 | UP | 4.861 | 3 | **MOVER** — predict +4.86 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-21 | UP | 12.871 | 4 | **MOVER** — predict +12.87 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-22 | DOWN | -0.497 | 4 | **IO** — predict -0.50 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.50% |
| 2026-09-23 | UP | 2.293 | 4 | **MOVER** — predict +2.29 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-24 | DOWN | -7.659 | 4 | **IO** — predict -7.66 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +0.54% |
| 2026-09-25 | UP | 2.706 | 4 | **MOVER** — predict +2.71 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-28 | DOWN | -6.14 | 0 | **IO** — predict -6.14 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -1.59% |
| 2026-09-29 | UP | 1.292 | 0 | **MOVER** — predict +1.29 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-30 | UP | 2.009 | 0 | **MOVER** — predict +2.01 >= +1.0 — mover 1d (09:30) | — |
| 2026-10-01 | DOWN | -1.343 | 0 | **IO** — predict -1.34 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -0.96% |
| 2026-10-02 | UP | 4.365 | 0 | **MOVER** — predict +4.37 >= +1.0 — mover 1d (09:30) | — |
| 2026-10-05 | FLAT | 0.086 | 0 | **IO** — predict +0.09 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size +1.25% |
| 2026-10-06 | UP | 2.623 | 0 | **MOVER** — predict +2.62 >= +1.0 — mover 1d (09:30) | — |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-10-02 09:30 ET | `CTKB` | BUY | 1687 | $5.86 | 2026-10-05 16:00 ET | $6.09 | $344.13 | 3.48% | mover |
| 2026-10-02 09:30 ET | `ETON` | BUY | 188 | $52.42 | 2026-10-05 16:00 ET | $55.62 | $596.38 | 6.05% | mover |
| 2026-10-02 09:30 ET | `LQDA` | BUY | 372 | $26.69 | 2026-10-05 16:00 ET | $27.64 | $343.66 | 3.46% | mover |
| 2026-10-02 09:30 ET | `MQ` | BUY | 581 | $17.23 | 2026-10-05 16:00 ET | $17.03 | $-131.36 | -1.31% | mover |
| 2026-08-13 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-13 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-14 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-14 16:00 ET | $0.00 | $3,166.97 | 3.17% | io |
| 2026-08-18 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-18 16:00 ET | $0.00 | $2,040.33 | 1.97% | io |
| 2026-08-19 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-19 16:00 ET | $0.00 | $4,306.12 | 4.08% | io |
| 2026-08-24 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-24 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-28 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-28 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-31 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-31 16:00 ET | $0.00 | $269.39 | 0.23% | io |
| 2026-09-01 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-01 16:00 ET | $0.00 | $1,543.54 | 1.34% | io |
| 2026-09-02 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-02 16:00 ET | $0.00 | $2,222.82 | 1.91% | io |
| 2026-09-03 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-03 16:00 ET | $0.00 | $2,361.82 | 1.99% | io |
| 2026-09-08 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-08 16:00 ET | $0.00 | $-3,617.70 | -3.01% | io |
| 2026-09-09 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-09 16:00 ET | $0.00 | $-301.36 | -0.26% | io |
| 2026-09-10 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-10 16:00 ET | $0.00 | $218.58 | 0.19% | io |
| 2026-09-11 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-11 16:00 ET | $0.00 | $254.15 | 0.22% | io |
| 2026-09-14 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-14 16:00 ET | $0.00 | $-3,632.62 | -3.11% | io |
| 2026-09-15 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-15 16:00 ET | $0.00 | $261.24 | 0.23% | io |
| 2026-09-22 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-22 16:00 ET | $0.00 | $586.55 | 0.5% | io |
| 2026-09-24 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-24 16:00 ET | $0.00 | $619.67 | 0.54% | io |
| 2026-09-28 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-28 16:00 ET | $0.00 | $-1,817.08 | -1.59% | io |
| 2026-10-01 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-10-01 16:00 ET | $0.00 | $-1,085.29 | -0.96% | io |
| 2026-10-05 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-10-05 16:00 ET | $0.00 | $1,418.83 | 1.25% | io |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

