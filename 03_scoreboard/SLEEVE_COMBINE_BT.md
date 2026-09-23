# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-23T05:51:04-04:00 — 2026-08-13 → 2026-09-23 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 29 · days with mover BUY calls: 16 · days with a stock book: 25

## Finding (this window)

The 1d **switch** is **+1.53%**. That is worse than mover-only 1d (+2.03%) and worse than .io-only 1d size (+4.86%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+3.24%** / 5.52% DD. Best book this window: **1w overlay_boost +9.86%**. Overlay / boost **beats** raw .io size (+2.33%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +1.53% | 3.47% | 42.2% | 45 | $4,399 | $-2,864 | 19 |
| 1d | mover_only | +2.03% | 4.25% | 52.5% | 40 | $2,035 | $0 | 22 |
| 1d | io_only | +4.86% | 7.74% | 44.6% | 157 | $0 | $4,863 | 5 |
| 1d | dual | +3.24% | 5.52% | 46.2% | 197 | $988 | $2,255 | 11 |
| 1d | overlay | +5.82% | 8.47% | 45.6% | 158 | $714 | $5,102 | 11 |
| 1d | overlay_boost | +5.70% | 9.96% | 46.8% | 154 | $-266 | $5,969 | 11 |
| 1d | io_boost | +3.75% | 10.00% | 46.4% | 151 | $0 | $3,751 | 5 |
| 3d | combine | -6.26% | 12.86% | 36.8% | 38 | $4,838 | $-11,103 | 20 |
| 3d | mover_only | -8.27% | 14.80% | 43.3% | 30 | $-8,274 | $0 | 22 |
| 3d | io_only | +2.33% | 12.61% | 45.2% | 84 | $0 | $2,330 | 7 |
| 3d | dual | -4.05% | 14.77% | 43.2% | 118 | $-4,162 | $108 | 12 |
| 3d | overlay | -1.74% | 16.17% | 41.9% | 86 | $-1,034 | $-701 | 12 |
| 3d | overlay_boost | +2.69% | 13.04% | 44.0% | 75 | $-1,034 | $3,719 | 12 |
| **3d** | **io_boost** | +3.40% | 12.54% | 44.6% | 74 | $0 | $3,396 | 7 |
| 1w | combine | -4.20% | 13.62% | 42.1% | 38 | $2,083 | $-6,287 | 20 |
| 1w | mover_only | -5.53% | 14.90% | 45.0% | 20 | $-5,532 | $0 | 22 |
| 1w | io_only | +3.38% | 10.64% | 48.0% | 50 | $0 | $3,383 | 9 |
| 1w | dual | -0.90% | 9.46% | 47.9% | 71 | $-2,510 | $1,613 | 12 |
| 1w | overlay | +3.38% | 10.64% | 48.0% | 50 | $0 | $3,383 | 12 |
| 1w | overlay_boost | +9.86% | 6.33% | 54.8% | 42 | $733 | $9,128 | 12 |
| 1w | io_boost | +9.15% | 6.88% | 53.7% | 41 | $0 | $9,153 | 9 |
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

Prints with a 1d exit: 153 · on S < +1: 54 · on S ≥ +1: 90

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.55% · 46.4% · n=153 |
| Down / messy (S < +1) | +0.63% · 50.0% · n=54 |
| Hard red (S < −3) | +1.05% · 54.3% · n=46 |
| Green mornings | +0.66% · 45.6% · n=90 |
| Down · large+ | -0.49% · 48.1% · n=27 |
| Down · mid | -0.35% · 45.0% · n=20 |
| Down · small/micro | +7.73% · 71.4% · n=7 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.47% · 46.5% · n=43 |
| Down · event-tagged | +0.12% · 53.3% · n=15 |
| Down · no event | +0.82% · 48.7% · n=39 |
| Down · join > 0 | -0.34% · 50.0% · n=36 |
| Down · join ≤ 0 / missing | +2.56% · 50.0% · n=18 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.75% · 41.7% · n=36 |
| Down · Energy | +0.43% · 60.0% · n=20 |
| Down · not Energy | +0.74% · 44.1% · n=34 |
| Down · Healthcare | +4.39% · 50.0% · n=10 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +4.86% | 7.74% | 44.6% | 157 |
| `large+` | +0.17% | 2.96% | 50.0% | 60 |
| `mid` | -2.27% | 4.12% | 38.9% | 54 |
| `small` | +7.21% | 1.28% | 44.2% | 43 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | +0.09% | 1.30% | 53.8% | 26 |
| `energy` | +1.14% | 1.65% | 60.0% | 30 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +1.23% | 6.05% | 44.4% | 126 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $103,396.40 | **+3.40%** | 12.54% | 74 | 44.6% | 179 |

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
| 2026-09-09 | -13.95 | io | 0 | 0 | 0 | 5 | $104,366 | — |
| 2026-09-10 | -13.275 | io | 0 | 9 | 5 | 9 | $105,473 | — |
| 2026-09-11 | 0.5 | io | 0 | 1 | 0 | 10 | $106,994 | — |
| 2026-09-14 | -11.002 | io | 0 | 0 | 0 | 10 | $102,364 | — |
| 2026-09-15 | -3.836 | io | 0 | 9 | 9 | 10 | $101,464 | — |
| 2026-09-16 | 5.297 | io | 0 | 1 | 1 | 10 | $100,994 | — |
| 2026-09-17 | 7.383 | io | 0 | 0 | 0 | 10 | $100,549 | — |
| 2026-09-18 | 4.861 | io | 0 | 9 | 9 | 10 | $98,219 | — |
| 2026-09-21 | 12.871 | io | 0 | 0 | 1 | 9 | $101,379 | 3d cannot settle (end of calendar) |
| 2026-09-22 | -0.497 | io | 0 | 0 | 0 | 9 | $103,481 | 3d cannot settle (end of calendar) |
| 2026-09-23 | 2.293 | io | 0 | 0 | 9 | 0 | $103,396 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-09-11 16:00 ET | io | `ORCL` | 71 | $150.28 | 2026-09-16 16:00 ET | $139.55 | $-766.02 |
| 2026-09-15 16:00 ET | io | `ICLR` | 58 | $172.45 | 2026-09-18 16:00 ET | $164.11 | $-488.13 |
| 2026-09-15 16:00 ET | io | `SMMT` | 583 | $17.40 | 2026-09-18 16:00 ET | $17.85 | $247.13 |
| 2026-09-15 16:00 ET | io | `ADSK` | 44 | $226.50 | 2026-09-18 16:00 ET | $216.95 | $-424.53 |
| 2026-09-15 16:00 ET | io | `WAY` | 380 | $26.67 | 2026-09-18 16:00 ET | $25.66 | $-393.74 |
| 2026-09-15 16:00 ET | io | `ATRC` | 180 | $56.22 | 2026-09-18 16:00 ET | $58.10 | $333.23 |
| 2026-09-15 16:00 ET | io | `DUOL` | 65 | $153.92 | 2026-09-18 16:00 ET | $141.90 | $-785.76 |
| 2026-09-15 16:00 ET | io | `RPD` | 814 | $12.46 | 2026-09-18 16:00 ET | $11.97 | $-420.07 |
| 2026-09-15 16:00 ET | io | `SPT` | 900 | $11.28 | 2026-09-18 16:00 ET | $10.17 | $-1,022.44 |
| 2026-09-15 16:00 ET | io | `NXDR` | 4126 | $2.46 | 2026-09-18 16:00 ET | $2.49 | $16.56 |
| 2026-09-16 16:00 ET | io | `FOXA` | 152 | $66.16 | 2026-09-21 16:00 ET | $64.79 | $-213.24 |
| 2026-09-18 16:00 ET | io | `RBRK` | 92 | $106.71 | 2026-09-23 16:00 ET | $111.87 | $470.09 |
| 2026-09-18 16:00 ET | io | `DELL` | 17 | $568.06 | 2026-09-23 16:00 ET | $548.92 | $-329.55 |
| 2026-09-18 16:00 ET | io | `GNRC` | 47 | $207.44 | 2026-09-23 16:00 ET | $206.17 | $-64.04 |
| 2026-09-18 16:00 ET | io | `VICR` | 44 | $222.72 | 2026-09-23 16:00 ET | $268.34 | $2,002.93 |
| 2026-09-18 16:00 ET | io | `ECO` | 115 | $84.95 | 2026-09-23 16:00 ET | $77.54 | $-856.91 |
| 2026-09-18 16:00 ET | io | `FIVN` | 302 | $32.47 | 2026-09-23 16:00 ET | $38.65 | $1,858.42 |
| 2026-09-18 16:00 ET | io | `RXT` | 2586 | $3.80 | 2026-09-23 16:00 ET | $4.13 | $786.15 |
| 2026-09-18 16:00 ET | io | `KOPN` | 2072 | $4.74 | 2026-09-23 16:00 ET | $4.94 | $360.52 |
| 2026-09-18 16:00 ET | io | `SONO` | 632 | $15.53 | 2026-09-23 16:00 ET | $16.85 | $817.75 |

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
