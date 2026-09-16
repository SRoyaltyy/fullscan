# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-16T09:31:57-04:00 — 2026-08-13 → 2026-09-16 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 24 · days with mover BUY calls: 16 · days with a stock book: 20

## Finding (this window)

The 1d **switch** is **+1.58%**. That is worse than mover-only 1d (+2.03%) and worse than .io-only 1d size (+3.34%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+2.54%** / 4.27% DD. Best book this window: **1w overlay_boost +12.85%**. Overlay / boost **beats** raw .io size (+0.99%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +1.58% | 3.43% | 46.3% | 41 | $4,399 | $-2,820 | 15 |
| 1d | mover_only | +2.03% | 4.25% | 52.5% | 40 | $2,035 | $0 | 17 |
| 1d | io_only | +3.34% | 5.35% | 44.0% | 109 | $0 | $3,336 | 5 |
| 1d | dual | +2.54% | 4.27% | 46.3% | 149 | $988 | $1,556 | 7 |
| 1d | overlay | +4.26% | 6.12% | 45.5% | 110 | $714 | $3,541 | 7 |
| 1d | overlay_boost | +4.14% | 7.65% | 47.2% | 106 | $-266 | $4,410 | 7 |
| 1d | io_boost | +2.25% | 7.67% | 46.6% | 103 | $0 | $2,253 | 5 |
| 3d | combine | -4.54% | 10.93% | 39.5% | 38 | $4,838 | $-9,379 | 15 |
| 3d | mover_only | -8.27% | 14.80% | 43.3% | 30 | $-8,274 | $0 | 17 |
| 3d | io_only | +0.99% | 10.07% | 44.6% | 65 | $0 | $986 | 7 |
| 3d | dual | -4.39% | 12.69% | 42.9% | 98 | $-4,162 | $-228 | 9 |
| 3d | overlay | -1.07% | 11.83% | 44.1% | 68 | $-1,034 | $-34 | 9 |
| 3d | overlay_boost | +0.42% | 10.50% | 44.6% | 56 | $-1,034 | $1,457 | 9 |
| **3d** | **io_boost** | +1.08% | 9.99% | 45.5% | 55 | $0 | $1,085 | 7 |
| 1w | combine | -1.99% | 8.69% | 48.3% | 29 | $2,083 | $-4,073 | 16 |
| 1w | mover_only | -7.30% | 14.90% | 45.0% | 20 | $-7,302 | $0 | 17 |
| 1w | io_only | +5.99% | 5.54% | 55.0% | 40 | $0 | $5,989 | 9 |
| 1w | dual | +0.26% | 8.07% | 54.1% | 61 | $-3,392 | $3,648 | 11 |
| 1w | overlay | +5.99% | 5.54% | 55.0% | 40 | $0 | $5,989 | 11 |
| 1w | overlay_boost | +12.85% | 2.60% | 65.6% | 32 | $733 | $12,118 | 11 |
| 1w | io_boost | +12.11% | 2.69% | 64.5% | 31 | $0 | $12,111 | 9 |
| 2w | io_only | +5.05% | 5.39% | 57.9% | 19 | $0 | $5,051 | 14 |

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

Prints with a 1d exit: 108 · on S < +1: 45 · on S ≥ +1: 54

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.57% · 45.4% · n=108 |
| Down / messy (S < +1) | +0.85% · 51.1% · n=45 |
| Hard red (S < −3) | +1.42% · 56.8% · n=37 |
| Green mornings | +0.61% · 42.6% · n=54 |
| Down · large+ | -0.61% · 45.5% · n=22 |
| Down · mid | -0.15% · 50.0% · n=16 |
| Down · small/micro | +7.73% · 71.4% · n=7 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.46% · 47.1% · n=34 |
| Down · event-tagged | -0.01% · 50.0% · n=14 |
| Down · no event | +1.24% · 51.6% · n=31 |
| Down · join > 0 | -0.39% · 48.3% · n=29 |
| Down · join ≤ 0 / missing | +3.11% · 56.2% · n=16 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.83% · 40.7% · n=27 |
| Down · Energy | +0.33% · 55.6% · n=18 |
| Down · not Energy | +1.20% · 48.1% · n=27 |
| Down · Healthcare | +4.39% · 50.0% · n=10 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +3.34% | 5.35% | 44.0% | 109 |
| `large+` | -0.34% | 2.45% | 48.8% | 43 |
| `mid` | -2.79% | 3.21% | 36.8% | 38 |
| `small` | +6.72% | 0.79% | 46.4% | 28 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | -0.10% | 1.30% | 52.0% | 25 |
| `energy` | +0.88% | 1.65% | 57.1% | 28 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | -0.76% | 4.08% | 41.2% | 85 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $101,084.57 | **+1.08%** | 9.99% | 55 | 45.5% | 153 |

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
| 2026-08-31 | -5.85 | io | 0 | 0 | 0 | 8 | $110,514 | — |
| 2026-09-01 | -6.3 | io | 0 | 5 | 8 | 5 | $108,697 | — |
| 2026-09-02 | -3.825 | io | 0 | 0 | 0 | 5 | $108,168 | — |
| 2026-09-03 | -0.9 | io | 0 | 0 | 0 | 5 | $108,508 | — |
| 2026-09-04 | 2.25 | io | 0 | 5 | 5 | 5 | $107,936 | — |
| 2026-09-08 | -11.475 | io | 0 | 0 | 0 | 5 | $105,588 | — |
| 2026-09-09 | -13.95 | io | 0 | 0 | 0 | 5 | $104,326 | — |
| 2026-09-10 | -13.275 | io | 0 | 7 | 5 | 7 | $104,898 | — |
| 2026-09-11 | 0.5 | io | 0 | 3 | 0 | 10 | $105,402 | — |
| 2026-09-14 | -11.002 | io | 0 | 0 | 0 | 10 | $101,684 | 3d cannot settle (end of calendar) |
| 2026-09-15 | -3.836 | io | 0 | 0 | 7 | 3 | $101,092 | 3d cannot settle (end of calendar) |
| 2026-09-16 | 5.297 | io | 0 | 0 | 3 | 0 | $101,085 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-09-01 16:00 ET | io | `FTI` | 273 | $79.53 | 2026-09-04 16:00 ET | $80.08 | $142.89 |
| 2026-09-01 16:00 ET | io | `DK` | 288 | $75.39 | 2026-09-04 16:00 ET | $72.40 | $-868.77 |
| 2026-09-01 16:00 ET | io | `BTE` | 4440 | $4.90 | 2026-09-04 16:00 ET | $4.95 | $106.53 |
| 2026-09-01 16:00 ET | io | `MTDR` | 369 | $58.90 | 2026-09-04 16:00 ET | $59.72 | $292.82 |
| 2026-09-01 16:00 ET | io | `RES` | 3295 | $6.60 | 2026-09-04 16:00 ET | $6.47 | $-514.08 |
| 2026-09-04 16:00 ET | io | `HOOD` | 173 | $124.72 | 2026-09-10 16:00 ET | $113.33 | $-1,975.67 |
| 2026-09-04 16:00 ET | io | `XP` | 1079 | $20.00 | 2026-09-10 16:00 ET | $19.97 | $-60.56 |
| 2026-09-04 16:00 ET | io | `ASND` | 79 | $271.12 | 2026-09-10 16:00 ET | $261.23 | $-785.94 |
| 2026-09-04 16:00 ET | io | `OSCR` | 669 | $32.24 | 2026-09-10 16:00 ET | $31.70 | $-378.80 |
| 2026-09-04 16:00 ET | io | `ATRC` | 411 | $52.46 | 2026-09-10 16:00 ET | $52.96 | $194.66 |
| 2026-09-10 16:00 ET | io | `NU` | 698 | $15.02 | 2026-09-15 16:00 ET | $14.62 | $-297.40 |
| 2026-09-10 16:00 ET | io | `SANM` | 51 | $203.31 | 2026-09-15 16:00 ET | $191.61 | $-601.07 |
| 2026-09-10 16:00 ET | io | `UGP` | 1388 | $7.56 | 2026-09-15 16:00 ET | $7.45 | $-188.81 |
| 2026-09-10 16:00 ET | io | `LBRT` | 505 | $20.76 | 2026-09-15 16:00 ET | $20.96 | $87.81 |
| 2026-09-10 16:00 ET | io | `AEHR` | 111 | $93.81 | 2026-09-15 16:00 ET | $94.69 | $92.94 |
| 2026-09-10 16:00 ET | io | `CLB` | 837 | $12.53 | 2026-09-15 16:00 ET | $12.50 | $-46.93 |
| 2026-09-10 16:00 ET | io | `OIS` | 1221 | $8.59 | 2026-09-15 16:00 ET | $8.69 | $90.31 |
| 2026-09-11 16:00 ET | io | `ORCL` | 70 | $150.28 | 2026-09-16 16:00 ET | $140.35 | $-699.59 |
| 2026-09-11 16:00 ET | io | `NVT` | 64 | $162.38 | 2026-09-16 16:00 ET | $146.80 | $-1,001.57 |
| 2026-09-11 16:00 ET | io | `COHU` | 184 | $57.08 | 2026-09-16 16:00 ET | $49.97 | $-1,313.43 |

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
