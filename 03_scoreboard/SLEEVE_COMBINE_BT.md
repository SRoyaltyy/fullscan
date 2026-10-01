# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-10-01T06:24:50-04:00 — 2026-08-13 → 2026-10-01 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 35 · days with mover BUY calls: 16 · days with a stock book: 31

## Finding (this window)

The 1d **switch** is **+1.06%**. That is worse than mover-only 1d (+2.03%) and worse than .io-only 1d size (+2.55%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+2.06%** / 5.52% DD. Best book this window: **1w overlay_boost +12.67%**. Overlay / boost **beats** raw .io size (+2.41%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +1.06% | 3.92% | 44.4% | 45 | $4,399 | $-3,336 | 25 |
| 1d | mover_only | +2.03% | 4.25% | 52.5% | 40 | $2,035 | $0 | 28 |
| 1d | io_only | +2.55% | 7.74% | 43.6% | 204 | $0 | $2,549 | 5 |
| 1d | dual | +2.06% | 5.52% | 45.1% | 244 | $988 | $1,072 | 15 |
| 1d | overlay | +3.48% | 8.47% | 44.4% | 205 | $714 | $2,769 | 15 |
| 1d | overlay_boost | +3.37% | 9.96% | 45.3% | 201 | $-266 | $3,638 | 15 |
| 1d | io_boost | +1.45% | 10.00% | 44.9% | 198 | $0 | $1,453 | 5 |
| 3d | combine | -6.80% | 13.22% | 34.0% | 47 | $4,838 | $-11,636 | 25 |
| 3d | mover_only | -8.27% | 14.80% | 43.3% | 30 | $-8,274 | $0 | 28 |
| 3d | io_only | +2.41% | 12.61% | 46.2% | 104 | $0 | $2,408 | 7 |
| 3d | dual | -3.47% | 14.77% | 44.9% | 138 | $-4,162 | $691 | 15 |
| 3d | overlay | -1.00% | 16.06% | 44.3% | 106 | $-1,034 | $38 | 15 |
| 3d | overlay_boost | +3.68% | 12.82% | 45.7% | 94 | $-1,034 | $4,715 | 15 |
| **3d** | **io_boost** | +4.41% | 12.31% | 46.2% | 93 | $0 | $4,411 | 7 |
| 1w | combine | -5.75% | 13.37% | 38.3% | 47 | $2,358 | $-8,111 | 25 |
| 1w | mover_only | -4.70% | 14.90% | 45.0% | 20 | $-4,697 | $0 | 28 |
| 1w | io_only | +7.15% | 10.64% | 50.8% | 59 | $0 | $7,148 | 9 |
| 1w | dual | +0.83% | 9.07% | 48.7% | 80 | $-2,090 | $2,920 | 16 |
| 1w | overlay | +7.15% | 10.64% | 50.8% | 59 | $0 | $7,148 | 16 |
| 1w | overlay_boost | +12.67% | 6.33% | 54.9% | 51 | $733 | $11,938 | 16 |
| 1w | io_boost | +11.96% | 6.88% | 54.0% | 50 | $0 | $11,959 | 9 |
| 2w | io_only | +2.58% | 10.14% | 48.3% | 29 | $0 | $2,583 | 14 |

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

Prints with a 1d exit: 199 · on S < +1: 73 · on S ≥ +1: 117

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.34% · 45.7% · n=199 |
| Down / messy (S < +1) | +0.19% · 45.2% · n=73 |
| Hard red (S < −3) | +0.54% · 49.2% · n=61 |
| Green mornings | +0.54% · 47.0% · n=117 |
| Down · large+ | -0.56% · 41.9% · n=31 |
| Down · mid | -0.72% · 34.6% · n=26 |
| Down · small/micro | +3.11% · 68.8% · n=16 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.65% · 41.9% · n=62 |
| Down · event-tagged | +0.03% · 47.1% · n=17 |
| Down · no event | +0.23% · 44.6% · n=56 |
| Down · join > 0 | -0.56% · 45.3% · n=53 |
| Down · join ≤ 0 / missing | +2.16% · 45.0% · n=20 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.86% · 38.2% · n=55 |
| Down · Energy | +0.13% · 48.0% · n=25 |
| Down · not Energy | +0.22% · 43.8% · n=48 |
| Down · Healthcare | +2.96% · 50.0% · n=16 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +2.55% | 7.74% | 43.6% | 204 |
| `large+` | -0.56% | 2.96% | 45.9% | 74 |
| `mid` | -3.69% | 4.15% | 35.2% | 71 |
| `small` | +7.20% | 1.28% | 50.8% | 59 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | -0.05% | 1.30% | 50.0% | 28 |
| `energy` | +0.55% | 1.96% | 51.4% | 35 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +0.85% | 6.05% | 42.6% | 162 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $104,410.82 | **+4.41%** | 12.31% | 93 | 46.2% | 214 |

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
| 2026-09-16 | 5.297 | io | 0 | 1 | 1 | 10 | $101,250 | — |
| 2026-09-17 | 7.383 | io | 0 | 0 | 0 | 10 | $100,804 | — |
| 2026-09-18 | 4.861 | io | 0 | 9 | 9 | 10 | $98,473 | — |
| 2026-09-21 | 12.871 | io | 0 | 1 | 1 | 10 | $101,637 | — |
| 2026-09-22 | -0.497 | io | 0 | 0 | 0 | 10 | $104,068 | — |
| 2026-09-23 | 2.293 | io | 0 | 8 | 9 | 9 | $104,060 | — |
| 2026-09-24 | -7.659 | io | 0 | 2 | 1 | 10 | $105,485 | — |
| 2026-09-25 | 2.706 | io | 0 | 0 | 0 | 10 | $104,802 | — |
| 2026-09-28 | -6.14 | io | 0 | 8 | 8 | 10 | $105,176 | — |
| 2026-09-29 | 1.292 | io | 0 | 0 | 2 | 8 | $104,884 | 3d cannot settle (end of calendar) |
| 2026-09-30 | 2.009 | io | 0 | 0 | 0 | 8 | $104,449 | 3d cannot settle (end of calendar) |
| 2026-10-01 | -1.343 | io | 0 | 0 | 8 | 0 | $104,411 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-09-18 16:00 ET | io | `SONO` | 634 | $15.53 | 2026-09-23 16:00 ET | $17.10 | $978.83 |
| 2026-09-21 16:00 ET | io | `A` | 62 | $161.94 | 2026-09-24 16:00 ET | $172.84 | $671.35 |
| 2026-09-23 16:00 ET | io | `DXCM` | 118 | $87.73 | 2026-09-28 16:00 ET | $86.68 | $-128.69 |
| 2026-09-23 16:00 ET | io | `HALO` | 92 | $112.44 | 2026-09-28 16:00 ET | $112.89 | $36.77 |
| 2026-09-23 16:00 ET | io | `ARQT` | 394 | $26.38 | 2026-09-28 16:00 ET | $27.33 | $363.99 |
| 2026-09-23 16:00 ET | io | `PGEN` | 1399 | $7.44 | 2026-09-28 16:00 ET | $7.81 | $481.21 |
| 2026-09-23 16:00 ET | io | `ADMA` | 1073 | $9.70 | 2026-09-28 16:00 ET | $10.12 | $422.71 |
| 2026-09-23 16:00 ET | io | `FTRE` | 533 | $19.52 | 2026-09-28 16:00 ET | $20.06 | $273.89 |
| 2026-09-23 16:00 ET | io | `OMER` | 501 | $20.74 | 2026-09-28 16:00 ET | $19.43 | $-669.39 |
| 2026-09-23 16:00 ET | io | `PUBM` | 596 | $17.45 | 2026-09-28 16:00 ET | $18.41 | $553.61 |
| 2026-09-24 16:00 ET | io | `CHKP` | 77 | $136.29 | 2026-09-29 16:00 ET | $126.93 | $-725.25 |
| 2026-09-24 16:00 ET | io | `EOG` | 73 | $142.86 | 2026-09-29 16:00 ET | $139.66 | $-238.11 |
| 2026-09-28 16:00 ET | io | `IT` | 56 | $185.73 | 2026-10-01 16:00 ET | $187.14 | $74.55 |
| 2026-09-28 16:00 ET | io | `TOST` | 346 | $30.38 | 2026-10-01 16:00 ET | $28.99 | $-490.00 |
| 2026-09-28 16:00 ET | io | `CNQ` | 221 | $47.55 | 2026-10-01 16:00 ET | $47.07 | $-111.90 |
| 2026-09-28 16:00 ET | io | `SONO` | 585 | $17.97 | 2026-10-01 16:00 ET | $17.89 | $-62.08 |
| 2026-09-28 16:00 ET | io | `CON` | 297 | $35.36 | 2026-10-01 16:00 ET | $34.42 | $-286.97 |
| 2026-09-28 16:00 ET | io | `FIVN` | 303 | $34.66 | 2026-10-01 16:00 ET | $34.01 | $-204.90 |
| 2026-09-28 16:00 ET | io | `MQ` | 622 | $16.89 | 2026-10-01 16:00 ET | $17.00 | $49.08 |
| 2026-09-28 16:00 ET | io | `AMN` | 307 | $34.26 | 2026-10-01 16:00 ET | $35.87 | $486.21 |

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
