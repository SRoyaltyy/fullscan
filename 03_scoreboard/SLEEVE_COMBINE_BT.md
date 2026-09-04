# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-04T05:56:50-04:00 — 2026-08-13 → 2026-09-03 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 17 · days with mover BUY calls: 14 · days with a stock book: 13

## Finding (this window)

The 1d **switch** is **+0.59%**. That is worse than mover-only 1d (+1.55%) and worse than .io-only 1d size (+6.50%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days (weak-sector mover names bounced). The attribute that transfers is the **size book plus staying on**: two wallets, same hold — mover gated on green mornings, .io size always invested. That is `dual`. 1d dual (two wallets) is **+3.90%** / 1.57% DD.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +0.59% | 1.60% | 37.9% | 29 | $1,579 | $-990 | 9 |
| 1d | mover_only | +1.55% | 0.66% | 43.3% | 30 | $1,547 | $0 | 11 |
| 1d | io_only | +6.50% | 2.46% | 48.2% | 85 | $0 | $6,498 | 4 |
| **1d** | **dual** | +3.90% | 1.57% | 47.0% | 115 | $743 | $3,159 | 13 |
| 3d | combine | +3.42% | 1.32% | 42.1% | 19 | $4,672 | $-1,257 | 9 |
| 3d | mover_only | +4.22% | 0.90% | 50.0% | 20 | $4,223 | $0 | 11 |
| 3d | io_only | +11.50% | 2.07% | 47.6% | 42 | $0 | $11,502 | 4 |
| 3d | dual | +7.61% | 1.30% | 48.4% | 64 | $2,091 | $5,524 | 13 |
| 1w | combine | +4.35% | 1.39% | 57.9% | 19 | $4,530 | $-181 | 9 |
| 1w | mover_only | +4.53% | 0.40% | 70.0% | 10 | $4,530 | $0 | 11 |
| 1w | io_only | +9.28% | 1.84% | 62.1% | 29 | $0 | $9,277 | 4 |
| 1w | dual | +6.45% | 0.50% | 64.1% | 39 | $2,292 | $4,157 | 13 |
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

## .io attributes that do / do not transfer onto mover

Leak-free test: take every mover BUY with a 1d print and tag the 09:30 boxes (same boxes the lookback already shows before the open). Do **not** use today's afternoon book.

| Attribute | On S < +1 (down/messy) | Use it? |
|---|---|---|
| Green pile / join-good / sector-not-red | Hurts (weak-sector mover names bounced; join-good was −0.3% vs +1.5%) | No |
| AB-good + peer-good as a top-10 filter | Hurts vs raw cond top-10 | No |
| Yesterday's 1d book overlap | Rare (n=25) but 64% win / +1.0% | Size-up only, never a requirement |
| Size-bucket book, always on, own cash | This *is* the down-day engine (1d .io size +6.5%) | **Yes — dual wallets** |

`dual` is two accounts at half capital: mover still gated at S ≥ +1, .io size still buys on red mornings. Same hold. No shared cash clock.

## Primary book — dual hold=1d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $103,902.09 | **+3.90%** | 1.57% | 115 | 47.0% | 96 |

### Session blotter

| Date | S | Route | AM fills | PM fills | Exits | Open | Equity | Gap |
|---|---:|---|---:|---:|---:|---:|---:|---|
| 2026-08-13 | 8.525 | dual | 0 | 9 | 0 | 9 | $99,911 | mover source empty (no BUY calls) |
| 2026-08-14 | 5.5 | dual | 0 | 9 | 9 | 9 | $101,211 | mover source empty (no BUY calls) |
| 2026-08-17 | 2.25 | dual | 1 | 9 | 9 | 10 | $101,411 | — |
| 2026-08-18 | -6.2 | dual | 0 | 9 | 10 | 9 | $101,224 | route cash — no new entries |
| 2026-08-19 | -7.2 | dual | 0 | 9 | 9 | 9 | $103,771 | route cash — no new entries |
| 2026-08-20 | 1.125 | dual | 9 | 9 | 9 | 18 | $104,702 | — |
| 2026-08-21 | 3.25 | dual | 0 | 9 | 18 | 9 | $105,563 | — |
| 2026-08-24 | -5.175 | dual | 0 | 0 | 9 | 0 | $105,032 | route cash — no new entries; io source missing (no stock_book file) |
| 2026-08-25 | 1.8 | dual | 10 | 0 | 0 | 10 | $104,946 | io source missing (no stock_book file) |
| 2026-08-26 | 2.025 | dual | 0 | 0 | 10 | 0 | $104,781 | io source missing (no stock_book file) |
| 2026-08-27 | — | dual | 10 | 9 | 0 | 19 | $104,954 | — |
| 2026-08-28 | 0.75 | dual | 0 | 0 | 19 | 0 | $104,215 | route cash — no new entries; io source missing (no stock_book file) |
| 2026-08-30 | — | dual | 0 | 0 | 0 | 0 | $104,215 | mover source empty (no BUY calls) |
| 2026-08-31 | -5.85 | dual | 0 | 3 | 0 | 3 | $104,209 | route cash — no new entries |
| 2026-09-01 | -6.3 | dual | 0 | 6 | 3 | 6 | $104,346 | route cash — no new entries |
| 2026-09-02 | -3.825 | dual | 0 | 4 | 6 | 4 | $103,998 | route cash — no new entries |
| 2026-09-03 | -0.9 | dual | 0 | 0 | 4 | 0 | $103,902 | route cash — no new entries |

### Last 20 fills

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-08-27 16:00 ET | io | `HOOD` | 49 | $108.54 | 2026-08-28 16:00 ET | $109.76 | $55.45 |
| 2026-08-27 16:00 ET | io | `RRC` | 129 | $41.64 | 2026-08-28 16:00 ET | $41.46 | $-28.04 |
| 2026-08-27 16:00 ET | io | `CRK` | 369 | $14.62 | 2026-08-28 16:00 ET | $14.29 | $-131.39 |
| 2026-08-27 16:00 ET | io | `ACMR` | 68 | $79.11 | 2026-08-28 16:00 ET | $74.42 | $-323.36 |
| 2026-08-27 16:00 ET | io | `SLI` | 2044 | $2.64 | 2026-08-28 16:00 ET | $2.55 | $-237.08 |
| 2026-08-27 16:00 ET | io | `VYX` | 607 | $8.88 | 2026-08-28 16:00 ET | $9.18 | $166.29 |
| 2026-08-27 16:00 ET | io | `DEC` | 365 | $14.78 | 2026-08-28 16:00 ET | $14.75 | $-20.47 |
| 2026-08-31 16:00 ET | io | `CRM` | 21 | $253.92 | 2026-09-01 16:00 ET | $255.50 | $29.02 |
| 2026-08-31 16:00 ET | io | `AON` | 16 | $321.52 | 2026-09-01 16:00 ET | $326.30 | $72.35 |
| 2026-08-31 16:00 ET | io | `MPC` | 14 | $374.60 | 2026-09-01 16:00 ET | $377.99 | $43.35 |
| 2026-09-01 16:00 ET | io | `CRM` | 20 | $255.50 | 2026-09-02 16:00 ET | $258.11 | $48.05 |
| 2026-09-01 16:00 ET | io | `KMI` | 167 | $32.10 | 2026-09-02 16:00 ET | $31.97 | $-26.76 |
| 2026-09-01 16:00 ET | io | `FTI` | 67 | $79.53 | 2026-09-02 16:00 ET | $78.31 | $-86.17 |
| 2026-09-01 16:00 ET | io | `CNR` | 52 | $102.30 | 2026-09-02 16:00 ET | $101.24 | $-59.47 |
| 2026-09-01 16:00 ET | io | `DK` | 71 | $75.39 | 2026-09-02 16:00 ET | $73.34 | $-150.01 |
| 2026-09-01 16:00 ET | io | `INVX` | 174 | $30.67 | 2026-09-02 16:00 ET | $30.25 | $-78.17 |
| 2026-09-02 16:00 ET | io | `CVS` | 54 | $97.23 | 2026-09-03 16:00 ET | $97.20 | $-5.97 |
| 2026-09-02 16:00 ET | io | `CVE` | 160 | $33.13 | 2026-09-03 16:00 ET | $32.81 | $-56.21 |
| 2026-09-02 16:00 ET | io | `CNQ` | 102 | $51.81 | 2026-09-03 16:00 ET | $51.14 | $-72.99 |
| 2026-09-02 16:00 ET | io | `PBF` | 71 | $74.99 | 2026-09-03 16:00 ET | $75.48 | $30.33 |

## Integrity checklist

- [x] Matched hold (combine refused for 2w/1m)
- [x] Mover entry = open; .io entry = close
- [x] Same-day close proceeds are not spendable at the open
- [x] Whole shares + Futubull fee file
- [x] Missing bars / books / BUY calls logged on the blotter
- [x] S < −3 does not flatten; scheduled exits still fire
- [x] No yfinance inside the sim — prices from the lookback bar store

Code: `src/sleeve_combine_bt.py`. Machine copy: `data/sleeve_combine/bt.json`.
