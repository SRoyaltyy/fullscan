# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-09T00:55:45-04:00 — 2026-08-13 → 2026-09-08 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 21 · days with mover BUY calls: 14 · days with a stock book: 17

## Finding (this window)

The 1d **switch** is **+1.68%**. That is worse than mover-only 1d (+3.98%) and worse than .io-only 1d size (+4.61%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+4.15%** / 2.67% DD. Best book this window: **3d io_boost +12.49%**. Overlay / boost **beats** raw .io size (+9.61%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +1.68% | 3.12% | 31.2% | 64 | $4,540 | $-2,862 | 10 |
| 1d | mover_only | +3.98% | 1.01% | 56.7% | 30 | $3,982 | $0 | 15 |
| 1d | io_only | +4.61% | 4.18% | 38.3% | 120 | $0 | $4,613 | 5 |
| 1d | dual | +4.15% | 2.67% | 42.0% | 150 | $1,968 | $2,186 | 11 |
| 1d | overlay | +6.04% | 4.51% | 39.7% | 121 | $21 | $6,017 | 11 |
| 1d | overlay_boost | +6.46% | 5.59% | 39.7% | 121 | $-266 | $6,728 | 11 |
| 1d | io_boost | +4.31% | 5.81% | 39.0% | 118 | $0 | $4,309 | 5 |
| 3d | combine | +3.60% | 3.13% | 43.3% | 30 | $6,905 | $-3,305 | 12 |
| 3d | mover_only | +6.44% | 1.12% | 60.0% | 20 | $6,445 | $0 | 15 |
| 3d | io_only | +9.61% | 2.44% | 44.6% | 56 | $0 | $9,608 | 7 |
| 3d | dual | +7.75% | 1.54% | 48.7% | 78 | $3,192 | $4,557 | 11 |
| 3d | overlay | +9.11% | 3.16% | 43.1% | 58 | $-1,034 | $10,148 | 11 |
| 3d | overlay_boost | +11.44% | 3.03% | 46.8% | 47 | $-1,034 | $12,476 | 11 |
| **3d** | **io_boost** | +12.49% | 2.26% | 47.8% | 46 | $0 | $12,488 | 7 |
| 1w | combine | +4.49% | 3.68% | 63.2% | 19 | $4,672 | $-180 | 14 |
| 1w | mover_only | +4.67% | 2.35% | 80.0% | 10 | $4,672 | $0 | 15 |
| 1w | io_only | +8.90% | 1.84% | 61.5% | 39 | $0 | $8,896 | 9 |
| 1w | dual | +6.59% | 1.24% | 65.3% | 49 | $2,570 | $4,022 | 12 |
| 1w | overlay | +8.90% | 1.84% | 61.5% | 39 | $0 | $8,896 | 12 |
| 1w | overlay_boost | +9.91% | 2.67% | 64.5% | 31 | $0 | $9,906 | 12 |
| 1w | io_boost | +9.91% | 2.67% | 64.5% | 31 | $0 | $9,906 | 9 |
| 2w | io_only | +10.26% | 3.68% | 68.4% | 19 | $0 | $10,260 | 13 |

## What beats the raw size book

Do **not** split the account 50/50. That is dual, and it lost. Keep 100% of capital on the size book. Use mover as information at the close (today's BUY list is knowable at 09:30): size-up overlap names and add a mover name that already printed on the same-horizon BUY list. That is `io_boost`. On a 1d hold, also spend idle cash on **one** gated mover name at 09:30 (`overlay`).

| Clock | Overlay / boost |
|---|---|
| ~05:55 | Read morning S |
| 09:30 | 1d `overlay` only: if S ≥ +1, one mover name at 10% from idle cash |
| 16:00 | Exit anything whose hold elapsed |
| 16:00 | Always fill the size book. Size-up mover∩book names (20%). |
| S < −3 | No new *mover* satellite; .io size still buys |

Switching one account (the old `combine` route) is below for history. It is not the production book.

## What the 1d *switch* was allowed to do (loses)

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

## .io attributes on down days (inside the size book)

Different question from the mover-tag table above. Here the names are already .io size-sleeve picks, entered at the close. Unweighted close→next-close on the same 1d hold. Morning S is only used to split the tape — it does not pick the names.

Prints with a 1d exit: 120 · on S < +1: 37 · on S ≥ +1: 45

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.62% · 40.8% · n=120 |
| Down / messy (S < +1) | +1.33% · 54.1% · n=37 |
| Hard red (S < −3) | +1.81% · 54.8% · n=31 |
| Green mornings | +0.86% · 48.9% · n=45 |
| Down · large+ | -0.04% · 50.0% · n=18 |
| Down · mid | -0.43% · 41.7% · n=12 |
| Down · small/micro | +7.88% · 85.7% · n=7 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.19% · 50.0% · n=26 |
| Down · event-tagged | +0.29% · 61.5% · n=13 |
| Down · no event | +1.90% · 50.0% · n=24 |
| Down · join > 0 | +0.00% · 47.8% · n=23 |
| Down · join ≤ 0 / missing | +3.52% · 64.3% · n=14 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.61% · 42.1% · n=19 |
| Down · Energy | +0.15% · 52.9% · n=17 |
| Down · not Energy | +2.33% · 55.0% · n=20 |
| Down · Healthcare | +4.00% · 40.0% · n=10 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +4.61% | 4.18% | 38.3% | 120 |
| `large+` | +1.21% | 0.85% | 43.8% | 48 |
| `mid` | -2.59% | 2.93% | 28.2% | 39 |
| `small` | +6.18% | 0.54% | 42.4% | 33 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | +0.04% | 1.16% | 53.6% | 28 |
| `energy` | +0.73% | 1.79% | 53.6% | 28 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +0.45% | 2.90% | 34.7% | 101 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $112,487.89 | **+12.49%** | 2.26% | 46 | 47.8% | 121 |

### Session blotter

| Date | S | Route | AM fills | PM fills | Exits | Open | Equity | Gap |
|---|---:|---|---:|---:|---:|---:|---:|---|
| 2026-08-13 | 8.525 | io | 0 | 9 | 0 | 9 | $99,830 | — |
| 2026-08-14 | 5.5 | io | 0 | 0 | 0 | 9 | $101,742 | — |
| 2026-08-17 | 2.25 | io | 0 | 0 | 0 | 9 | $102,385 | — |
| 2026-08-18 | -6.2 | io | 0 | 9 | 9 | 9 | $102,442 | — |
| 2026-08-19 | -7.2 | io | 0 | 0 | 0 | 9 | $107,186 | — |
| 2026-08-20 | 1.125 | io | 0 | 0 | 0 | 9 | $109,844 | — |
| 2026-08-21 | 3.25 | io | 0 | 9 | 9 | 9 | $112,058 | — |
| 2026-08-24 | -5.175 | io | 0 | 0 | 0 | 9 | $111,058 | io source missing (no stock_book file) |
| 2026-08-25 | 1.8 | io | 0 | 0 | 0 | 9 | $112,298 | io source missing (no stock_book file) |
| 2026-08-26 | 2.025 | io | 0 | 0 | 9 | 0 | $111,357 | io source missing (no stock_book file) |
| 2026-08-27 | — | io | 0 | 8 | 0 | 8 | $111,276 | — |
| 2026-08-28 | 0.75 | io | 0 | 0 | 0 | 8 | $111,354 | io source missing (no stock_book file) |
| 2026-08-30 | — | io | 0 | 0 | 0 | 8 | $109,762 | — |
| 2026-08-31 | -5.85 | io | 0 | 6 | 8 | 6 | $110,344 | — |
| 2026-09-01 | -6.3 | io | 0 | 0 | 0 | 6 | $110,872 | — |
| 2026-09-02 | -3.825 | io | 0 | 0 | 0 | 6 | $111,865 | — |
| 2026-09-03 | -0.9 | io | 0 | 5 | 6 | 5 | $112,615 | — |
| 2026-09-04 | — | io | 0 | 0 | 0 | 5 | $112,508 | — |
| 2026-09-06 | — | io | 0 | 0 | 0 | 5 | $112,508 | 3d cannot settle (end of calendar) |
| 2026-09-07 | — | io | 0 | 0 | 5 | 0 | $112,488 | 3d cannot settle (end of calendar) |
| 2026-09-08 | — | io | 0 | 0 | 0 | 0 | $112,488 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-08-21 16:00 ET | io | `RZLT` | 2202 | $5.09 | 2026-08-26 16:00 ET | $5.04 | $-167.37 |
| 2026-08-27 16:00 ET | io | `VYX` | 2508 | $8.88 | 2026-08-31 16:00 ET | $8.90 | $-15.14 |
| 2026-08-27 16:00 ET | io | `PGY` | 993 | $22.41 | 2026-08-31 16:00 ET | $21.95 | $-482.74 |
| 2026-08-27 16:00 ET | io | `FUTU` | 87 | $127.34 | 2026-08-31 16:00 ET | $124.04 | $-291.70 |
| 2026-08-27 16:00 ET | io | `CNH` | 973 | $11.43 | 2026-08-31 16:00 ET | $11.79 | $324.92 |
| 2026-08-27 16:00 ET | io | `HOOD` | 102 | $108.54 | 2026-08-31 16:00 ET | $104.80 | $-386.18 |
| 2026-08-27 16:00 ET | io | `RRC` | 267 | $41.64 | 2026-08-31 16:00 ET | $41.78 | $30.36 |
| 2026-08-27 16:00 ET | io | `CRK` | 761 | $14.62 | 2026-08-31 16:00 ET | $14.51 | $-103.56 |
| 2026-08-27 16:00 ET | io | `MOS` | 468 | $23.76 | 2026-08-31 16:00 ET | $23.78 | $-2.88 |
| 2026-08-31 16:00 ET | io | `NOV` | 1043 | $21.16 | 2026-09-03 16:00 ET | $21.71 | $546.39 |
| 2026-08-31 16:00 ET | io | `PBF` | 306 | $72.02 | 2026-09-03 16:00 ET | $75.48 | $1,050.63 |
| 2026-08-31 16:00 ET | io | `WTTR` | 1111 | $19.87 | 2026-09-03 16:00 ET | $19.77 | $-140.12 |
| 2026-08-31 16:00 ET | io | `RES` | 3370 | $6.55 | 2026-09-03 16:00 ET | $6.57 | $-20.28 |
| 2026-08-31 16:00 ET | io | `BMO` | 64 | $170.31 | 2026-09-03 16:00 ET | $176.86 | $414.74 |
| 2026-08-31 16:00 ET | io | `VOD` | 687 | $16.04 | 2026-09-03 16:00 ET | $16.58 | $353.05 |
| 2026-09-03 16:00 ET | io | `CRM` | 87 | $256.93 | 2026-09-07 16:00 ET | $259.23 | $195.40 |
| 2026-09-03 16:00 ET | io | `RVTY` | 172 | $130.94 | 2026-09-07 16:00 ET | $130.22 | $-129.06 |
| 2026-09-03 16:00 ET | io | `NVDA` | 100 | $224.41 | 2026-09-07 16:00 ET | $230.36 | $590.22 |
| 2026-09-03 16:00 ET | io | `ATRC` | 428 | $52.59 | 2026-09-07 16:00 ET | $51.52 | $-469.25 |
| 2026-09-03 16:00 ET | io | `HRMY` | 525 | $42.86 | 2026-09-07 16:00 ET | $42.25 | $-334.06 |

## Integrity checklist

- [x] Matched hold (combine refused for 2w/1m)
- [x] Mover entry = open; .io entry = close
- [x] Same-day close proceeds are not spendable at the open
- [x] Whole shares + Futubull fee file
- [x] Missing bars / books / BUY calls logged on the blotter
- [x] S < −3 does not flatten; scheduled exits still fire
- [x] No yfinance inside the sim — prices from the lookback bar store

## How to backtest every session

The lookback payload ∪ stock books **is** all days we have (dashboard era starts 2026-08-13). Default CLI walks every session in that union.

```
python -m src.test_sleeve_combine_bt
python -m src.sleeve_combine_bt --mode dual --hold 1d
python -m src.sleeve_combine_bt --from 2026-08-13 --to 2026-09-03
```

Buy/sell blotter (every fill, day picker): `dashboard/sleeve-combine/index.html` — live [https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/). Round-trips: `data/sleeve_combine/bt_trades.csv`. Expanded BUY then SELL rows: `data/sleeve_combine/bt_fills.csv`.

Code: `src/sleeve_combine_bt.py`. Machine copy: `data/sleeve_combine/bt.json`.
