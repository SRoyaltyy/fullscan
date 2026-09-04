# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-04T06:21:02-04:00 — 2026-08-13 → 2026-09-03 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 17 · days with mover BUY calls: 14 · days with a stock book: 13

## Finding (this window)

The 1d **switch** is **+0.59%**. That is worse than mover-only 1d (+1.55%) and worse than .io-only 1d size (+6.50%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+3.90%** / 1.57% DD. Best book this window: **3d io_boost +12.63%**. Overlay / boost **beats** raw .io size (+11.50%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +0.59% | 1.60% | 37.9% | 29 | $1,579 | $-990 | 9 |
| 1d | mover_only | +1.55% | 0.66% | 43.3% | 30 | $1,547 | $0 | 11 |
| 1d | io_only | +6.50% | 2.46% | 48.2% | 85 | $0 | $6,498 | 4 |
| 1d | dual | +3.90% | 1.57% | 47.0% | 115 | $743 | $3,159 | 7 |
| 1d | overlay | +8.05% | 2.75% | 50.0% | 86 | $115 | $7,932 | 7 |
| 1d | overlay_boost | +9.34% | 3.34% | 50.6% | 87 | $-225 | $9,566 | 7 |
| 1d | io_boost | +7.09% | 3.61% | 50.0% | 84 | $0 | $7,086 | 4 |
| 3d | combine | +3.42% | 1.32% | 42.1% | 19 | $4,672 | $-1,257 | 9 |
| 3d | mover_only | +4.22% | 0.90% | 50.0% | 20 | $4,223 | $0 | 11 |
| 3d | io_only | +11.50% | 2.07% | 47.6% | 42 | $0 | $11,502 | 4 |
| 3d | dual | +7.61% | 1.30% | 48.4% | 64 | $2,091 | $5,524 | 7 |
| 3d | overlay | +10.50% | 2.88% | 45.5% | 44 | $-992 | $11,496 | 7 |
| 3d | overlay_boost | +11.63% | 2.54% | 47.6% | 42 | $-992 | $12,622 | 7 |
| **3d** | **io_boost** | +12.63% | 1.74% | 48.8% | 41 | $0 | $12,635 | 4 |
| 1w | combine | +4.35% | 1.39% | 57.9% | 19 | $4,530 | $-181 | 9 |
| 1w | mover_only | +4.53% | 0.40% | 70.0% | 10 | $4,530 | $0 | 11 |
| 1w | io_only | +9.28% | 1.84% | 62.1% | 29 | $0 | $9,277 | 4 |
| 1w | dual | +6.45% | 0.50% | 64.1% | 39 | $2,292 | $4,157 | 7 |
| 1w | overlay | +9.28% | 1.84% | 62.1% | 29 | $0 | $9,277 | 7 |
| 1w | overlay_boost | +9.82% | 2.48% | 64.0% | 25 | $0 | $9,817 | 7 |
| 1w | io_boost | +9.82% | 2.48% | 64.0% | 25 | $0 | $9,817 | 4 |
| 2w | io_only | +6.68% | 3.68% | 70.0% | 10 | $0 | $6,680 | 4 |

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

Prints with a 1d exit: 85 · on S < +1: 31 · on S ≥ +1: 45

| Cut | Mean · win · n |
|---|---|
| All size prints | +1.03% · 49.4% · n=85 |
| Down / messy (S < +1) | +1.81% · 54.8% · n=31 |
| Hard red (S < −3) | +1.81% · 54.8% · n=31 |
| Green mornings | +0.86% · 48.9% · n=45 |
| Down · large+ | -0.11% · 46.7% · n=15 |
| Down · mid | +0.37% · 50.0% · n=10 |
| Down · small/micro | +9.02% · 83.3% · n=6 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | +0.10% · 50.0% · n=20 |
| Down · event-tagged | +0.29% · 60.0% · n=10 |
| Down · no event | +2.53% · 52.4% · n=21 |
| Down · join > 0 | -0.08% · 45.0% · n=20 |
| Down · join ≤ 0 / missing | +5.24% · 72.7% · n=11 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.36% · 38.5% · n=13 |
| Down · Energy | +0.55% · 56.2% · n=16 |
| Down · not Energy | +3.15% · 53.3% · n=15 |
| Down · Healthcare | +4.72% · 44.4% · n=9 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +6.50% | 2.46% | 48.2% | 85 |
| `large+` | +1.82% | 0.53% | 54.5% | 33 |
| `mid` | -1.51% | 1.91% | 39.3% | 28 |
| `small` | +6.27% | 0.45% | 50.0% | 24 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | +0.41% | 0.79% | 57.1% | 21 |
| `energy` | +1.37% | 1.16% | 57.7% | 26 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +1.38% | 2.00% | 44.9% | 69 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $112,634.64 | **+12.63%** | 1.74% | 41 | 48.8% | 90 |

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
| 2026-08-30 | — | io | 0 | 0 | 0 | 8 | $111,354 | — |
| 2026-08-31 | -5.85 | io | 0 | 6 | 8 | 6 | $110,344 | — |
| 2026-09-01 | -6.3 | io | 0 | 0 | 0 | 6 | $110,872 | — |
| 2026-09-02 | -3.825 | io | 0 | 0 | 0 | 6 | $111,865 | — |
| 2026-09-03 | -0.9 | io | 0 | 0 | 6 | 0 | $112,635 | — |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-08-21 16:00 ET | io | `MOS` | 459 | $24.41 | 2026-08-26 16:00 ET | $24.16 | $-126.76 |
| 2026-08-21 16:00 ET | io | `GSHD` | 154 | $72.52 | 2026-08-26 16:00 ET | $71.31 | $-191.36 |
| 2026-08-21 16:00 ET | io | `OCUL` | 1011 | $11.08 | 2026-08-26 16:00 ET | $10.77 | $-339.75 |
| 2026-08-21 16:00 ET | io | `INSP` | 179 | $62.36 | 2026-08-26 16:00 ET | $61.80 | $-105.42 |
| 2026-08-21 16:00 ET | io | `CRMD` | 1367 | $8.20 | 2026-08-26 16:00 ET | $8.39 | $224.15 |
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
