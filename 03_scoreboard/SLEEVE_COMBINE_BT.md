# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-04T05:48:23-04:00 — 2026-08-13 → 2026-09-03 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 17 · days with mover BUY calls: 14 · days with a stock book: 13

## Finding (this window)

The 1d **combine** is **+0.59%**. That is worse than mover-only 1d (+1.55%) and worse than .io-only 1d size (+6.50%). The curve-stitch that mixed mover 1d with live .io 2w_size was not a valid combine — once holds, cash lock, and source gaps are enforced, switching books on S does not beat running .io 1d/3d size as its own account. Hard-red cash days skip .io fills that the solo .io book would have taken; 08-13/14 are mover gaps (no BUY calls), not days we can secretly fill from the afternoon book.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| **1d** | **combine** | +0.59% | 1.60% | 37.9% | 29 | $1,579 | $-990 | 9 |
| 1d | mover_only | +1.55% | 0.66% | 43.3% | 30 | $1,547 | $0 | 11 |
| 1d | io_only | +6.50% | 2.46% | 48.2% | 85 | $0 | $6,498 | 4 |
| 3d | combine | +3.42% | 1.32% | 42.1% | 19 | $4,672 | $-1,257 | 9 |
| 3d | mover_only | +4.22% | 0.90% | 50.0% | 20 | $4,223 | $0 | 11 |
| 3d | io_only | +11.50% | 2.07% | 47.6% | 42 | $0 | $11,502 | 4 |
| 1w | combine | +4.35% | 1.39% | 57.9% | 19 | $4,530 | $-181 | 9 |
| 1w | mover_only | +4.53% | 0.40% | 70.0% | 10 | $4,530 | $0 | 11 |
| 1w | io_only | +9.28% | 1.84% | 62.1% | 29 | $0 | $9,277 | 4 |
| 2w | io_only | +6.68% | 3.68% | 70.0% | 10 | $0 | $6,680 | 4 |

## What the 1d combine is allowed to do

| Clock | Action |
|---|---|
| ~05:55 | Read morning general score S (leak-free) |
| 09:30 | If S ≥ +1 **and** mover has BUY calls: fill from overnight cash |
| 16:00 | Exit anything whose hold elapsed (close) |
| 16:00 | If −3 ≤ S < +1 **and** a book exists: fill .io size picks |
| S < −3 | No new entries; existing holds ride to their exit |

If mover has no BUY calls on a green morning (2026-08-13, 2026-08-14), that day is a **source gap**, not a silent cash day. The combine does not invent .io fills at 09:30 to paper over it — that would leak the afternoon book.

## Primary book — combine hold=1d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $100,589.10 | **+0.59%** | 1.60% | 29 | 37.9% | 96 |

### Session blotter

| Date | S | Route | AM fills | PM fills | Exits | Open | Equity | Gap |
|---|---:|---|---:|---:|---:|---:|---:|---|
| 2026-08-13 | 8.525 | mover | 0 | 0 | 0 | 0 | $100,000 | mover source empty (no BUY calls) |
| 2026-08-14 | 5.5 | mover | 0 | 0 | 0 | 0 | $100,000 | mover source empty (no BUY calls) |
| 2026-08-17 | 2.25 | mover | 1 | 0 | 0 | 1 | $100,334 | — |
| 2026-08-18 | -6.2 | cash | 0 | 0 | 1 | 0 | $100,746 | route cash — no new entries |
| 2026-08-19 | -7.2 | cash | 0 | 0 | 0 | 0 | $100,746 | route cash — no new entries |
| 2026-08-20 | 1.125 | mover | 9 | 0 | 0 | 9 | $102,222 | — |
| 2026-08-21 | 3.25 | mover | 0 | 0 | 9 | 0 | $102,066 | — |
| 2026-08-24 | -5.175 | cash | 0 | 0 | 0 | 0 | $102,066 | route cash — no new entries |
| 2026-08-25 | 1.8 | mover | 10 | 0 | 0 | 10 | $101,901 | — |
| 2026-08-26 | 2.025 | mover | 0 | 0 | 10 | 0 | $101,579 | — |
| 2026-08-27 | — | io | 0 | 9 | 0 | 9 | $101,475 | — |
| 2026-08-28 | 0.75 | io | 0 | 0 | 9 | 0 | $100,589 | io source missing (no stock_book file) |
| 2026-08-30 | — | io | 0 | 0 | 0 | 0 | $100,589 | — |
| 2026-08-31 | -5.85 | cash | 0 | 0 | 0 | 0 | $100,589 | route cash — no new entries |
| 2026-09-01 | -6.3 | cash | 0 | 0 | 0 | 0 | $100,589 | route cash — no new entries |
| 2026-09-02 | -3.825 | cash | 0 | 0 | 0 | 0 | $100,589 | route cash — no new entries |
| 2026-09-03 | -0.9 | io | 0 | 0 | 0 | 0 | $100,589 | — |

### Last 20 fills

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-08-20 09:30 ET | mover | `ABUS` | 2084 | $4.92 | 2026-08-21 16:00 ET | $4.77 | $-366.79 |
| 2026-08-25 09:30 ET | mover | `AU` | 85 | $119.46 | 2026-08-26 16:00 ET | $118.55 | $-81.94 |
| 2026-08-25 09:30 ET | mover | `ERO` | 268 | $38.00 | 2026-08-26 16:00 ET | $39.24 | $325.27 |
| 2026-08-25 09:30 ET | mover | `FCX` | 131 | $77.90 | 2026-08-26 16:00 ET | $77.49 | $-58.58 |
| 2026-08-25 09:30 ET | mover | `CNH` | 870 | $11.72 | 2026-08-26 16:00 ET | $11.62 | $-109.67 |
| 2026-08-25 09:30 ET | mover | `HMY` | 450 | $22.65 | 2026-08-26 16:00 ET | $22.50 | $-79.26 |
| 2026-08-25 09:30 ET | mover | `RHI` | 229 | $44.52 | 2026-08-26 16:00 ET | $44.48 | $-15.18 |
| 2026-08-25 09:30 ET | mover | `SUZ` | 1125 | $9.07 | 2026-08-26 16:00 ET | $8.94 | $-175.54 |
| 2026-08-25 09:30 ET | mover | `VALE` | 681 | $15.00 | 2026-08-26 16:00 ET | $15.01 | $-10.95 |
| 2026-08-25 09:30 ET | mover | `WPM` | 63 | $160.00 | 2026-08-26 16:00 ET | $158.25 | $-114.70 |
| 2026-08-25 09:30 ET | mover | `ABUS` | 1939 | $5.26 | 2026-08-26 16:00 ET | $5.20 | $-166.77 |
| 2026-08-27 16:00 ET | io | `FUTU` | 79 | $127.34 | 2026-08-28 16:00 ET | $124.57 | $-223.38 |
| 2026-08-27 16:00 ET | io | `CNH` | 888 | $11.43 | 2026-08-28 16:00 ET | $11.68 | $198.85 |
| 2026-08-27 16:00 ET | io | `HOOD` | 93 | $108.54 | 2026-08-28 16:00 ET | $109.76 | $108.82 |
| 2026-08-27 16:00 ET | io | `RRC` | 243 | $41.64 | 2026-08-28 16:00 ET | $41.46 | $-50.13 |
| 2026-08-27 16:00 ET | io | `CRK` | 694 | $14.62 | 2026-08-28 16:00 ET | $14.29 | $-247.12 |
| 2026-08-27 16:00 ET | io | `ACMR` | 128 | $79.11 | 2026-08-28 16:00 ET | $74.42 | $-605.16 |
| 2026-08-27 16:00 ET | io | `SLI` | 3846 | $2.64 | 2026-08-28 16:00 ET | $2.55 | $-446.08 |
| 2026-08-27 16:00 ET | io | `VYX` | 1143 | $8.88 | 2026-08-28 16:00 ET | $9.18 | $313.14 |
| 2026-08-27 16:00 ET | io | `DEC` | 686 | $14.78 | 2026-08-28 16:00 ET | $14.75 | $-38.47 |

## Integrity checklist

- [x] Matched hold (combine refused for 2w/1m)
- [x] Mover entry = open; .io entry = close
- [x] Same-day close proceeds are not spendable at the open
- [x] Whole shares + Futubull fee file
- [x] Missing bars / books / BUY calls logged on the blotter
- [x] S < −3 does not flatten; scheduled exits still fire
- [x] No yfinance inside the sim — prices from the lookback bar store

Code: `src/sleeve_combine_bt.py`. Machine copy: `data/sleeve_combine/bt.json`.
