# Mover paper — empty list + skip days defer to live .io 2w_size

_Generated 2026-09-10T06:11:41 — calls 2026-08-13 → 2026-09-08_

**Strategy:** LONG-only · top 10/day by cond / live 2w_size · entry open+close (16:00 ET) · hold 1d / 2w_size mark (exit 16:00 ET) · 10% of equity per trade · Futubull fees · cash-accounted (unfittable trades skipped and logged). Mover 1d at 09:30 when S ≥ +1 and the BUY list is non-empty. Empty BUY list and S ≥ 0 takes live .io 2w_size (source gap). Every mover-skip morning (S < +1, including hard-red) also takes that mark.

**Book:** skip days and empty non-negative mornings defer to live .io `2w_size` (same sleeve as the .io dashboard). 0 fills while calls exist is not a gap — cash is still in yesterday’s 1d holds. Hard-red S ≤ −3 blocks new 1d risk; it does not flatten 2w_size.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $113,702.44 | **13.7%** | 6.18% | 52 | 0 | 53.8% |

| Side | Trades | Win rate | P&L |
|---:|---:|---:|---:|
| BUY (long) | 52 | 53.8% | $14,429.51 |
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
| 2026-09-04 | UP | 2.25 | 0 | **MOVER** — predict +2.25 >= +1.0 — mover 1d (09:30) | — |
| 2026-09-08 | DOWN | -11.475 | 1 | **IO** — predict -11.47 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -3.00% |
| 2026-09-09 | DOWN | -13.95 | 0 | **IO** — predict -13.95 < +1.0 — mover skip; live .io 2w_size mark (already on; not a new 1d ticket) | live 2w_size -0.01% |

## Last 25 filled trades

| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | Exit px | P&L | Ret | Cond |
|---|---|---|---:|---:|---|---:|---:|---:|---|
| 2026-08-27 09:30 ET | `DLO` | BUY | 674 | $15.60 | 2026-08-28 16:00 ET | $15.03 | $-401.76 | -3.82% | mover |
| 2026-08-27 09:30 ET | `GEN` | BUY | 363 | $28.89 | 2026-08-28 16:00 ET | $31.02 | $763.68 | 7.28% | mover |
| 2026-08-27 09:30 ET | `LRCX` | BUY | 33 | $314.61 | 2026-08-28 16:00 ET | $301.90 | $-423.70 | -4.08% | mover |
| 2026-09-04 09:30 ET | `CABA` | BUY | 2870 | $3.63 | 2026-09-08 16:00 ET | $3.89 | $671.59 | 6.45% | mover |
| 2026-09-04 09:30 ET | `ALEC` | BUY | 3842 | $2.70 | 2026-09-08 16:00 ET | $2.46 | $-1,021.92 | -9.85% | mover |
| 2026-09-04 09:30 ET | `ATRC` | BUY | 194 | $52.88 | 2026-09-08 16:00 ET | $52.37 | $-104.20 | -1.02% | mover |
| 2026-09-04 09:30 ET | `BHC` | BUY | 1488 | $6.91 | 2026-09-08 16:00 ET | $6.99 | $80.31 | 0.78% | mover |
| 2026-09-04 09:30 ET | `BMEA` | BUY | 5318 | $1.93 | 2026-09-08 16:00 ET | $2.03 | $393.63 | 3.84% | mover |
| 2026-09-04 09:30 ET | `CRM` | BUY | 39 | $261.98 | 2026-09-08 16:00 ET | $256.31 | $-225.44 | -2.21% | mover |
| 2026-09-04 09:30 ET | `HRMY` | BUY | 238 | $42.93 | 2026-09-08 16:00 ET | $43.93 | $231.74 | 2.27% | mover |
| 2026-09-04 09:30 ET | `OABI` | BUY | 2013 | $5.08 | 2026-09-08 16:00 ET | $4.44 | $-1,340.66 | -13.11% | mover |
| 2026-09-04 09:30 ET | `OPK` | BUY | 5942 | $1.71 | 2026-09-08 16:00 ET | $1.62 | $-689.15 | -6.78% | mover |
| 2026-09-04 09:30 ET | `RVTY` | BUY | 76 | $132.45 | 2026-09-08 16:00 ET | $130.22 | $-174.01 | -1.73% | mover |
| 2026-08-13 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-13 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-14 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-14 16:00 ET | $0.00 | $3,166.97 | 3.17% | io |
| 2026-08-18 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-18 16:00 ET | $0.00 | $2,040.33 | 1.97% | io |
| 2026-08-19 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-19 16:00 ET | $0.00 | $4,306.12 | 4.08% | io |
| 2026-08-24 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-24 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-28 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-28 16:00 ET | $0.00 | $0.00 | 0% | io |
| 2026-08-31 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-08-31 16:00 ET | $0.00 | $260.98 | 0.23% | io |
| 2026-09-01 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-01 16:00 ET | $0.00 | $1,550.93 | 1.35% | io |
| 2026-09-02 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-02 16:00 ET | $0.00 | $2,223.89 | 1.91% | io |
| 2026-09-03 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-03 16:00 ET | $0.00 | $2,366.08 | 1.99% | io |
| 2026-09-08 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-08 16:00 ET | $0.00 | $-3,513.70 | -3.0% | io |
| 2026-09-09 16:00 ET | `2w_size` | BUY | 0 | $0.00 | 2026-09-09 16:00 ET | $0.00 | $-6.81 | -0.01% | io |

Full records: `data/mover_paper/trades.csv` (every fill with ET timestamps, prices, fees), `skipped.csv`, `equity_curve.csv`. Lever sweep: `MOVER_STRATEGY_SWEEP.md`. Dashboard: `dashboard/mover-paper/index.html`.

