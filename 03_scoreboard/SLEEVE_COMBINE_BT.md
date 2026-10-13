# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-10-13T08:32:34-04:00 — 2026-08-13 → 2026-10-13 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 43 · days with mover BUY calls: 36 · days with a stock book: 39

## Finding (this window)

The 1d **switch** is **+1.71%**. That is worse than mover-only 1d (-2.06%) and worse than .io-only 1d size (+4.82%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+1.27%** / 8.19% DD. Best book this window: **1w io_boost +20.79%**. Overlay / boost **beats** raw .io size (+7.00%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +1.71% | 8.32% | 51.2% | 125 | $4,843 | $-3,131 | 19 |
| 1d | mover_only | -2.06% | 9.08% | 49.1% | 108 | $-2,055 | $0 | 26 |
| 1d | io_only | +4.82% | 9.28% | 47.6% | 254 | $0 | $4,819 | 5 |
| 1d | dual | +1.27% | 8.19% | 48.2% | 363 | $-840 | $2,114 | 9 |
| 1d | overlay | +6.83% | 9.17% | 48.7% | 261 | $18 | $6,816 | 9 |
| 1d | overlay_boost | +9.30% | 9.08% | 49.1% | 216 | $-446 | $9,743 | 9 |
| 1d | io_boost | +8.35% | 8.74% | 49.1% | 212 | $0 | $8,348 | 5 |
| 3d | combine | +1.01% | 14.89% | 43.6% | 78 | $9,061 | $-8,053 | 21 |
| 3d | mover_only | -9.50% | 20.16% | 37.1% | 70 | $-9,504 | $0 | 26 |
| 3d | io_only | +7.00% | 14.55% | 47.6% | 126 | $0 | $6,999 | 7 |
| 3d | dual | -1.90% | 14.06% | 43.2% | 199 | $-4,820 | $2,916 | 9 |
| 3d | overlay | +3.79% | 15.49% | 46.9% | 128 | $-1,031 | $4,825 | 9 |
| 3d | overlay_boost | +14.75% | 11.65% | 50.0% | 82 | $-1,031 | $15,786 | 9 |
| **3d** | **io_boost** | +16.35% | 10.07% | 51.9% | 81 | $0 | $16,351 | 7 |
| 1w | combine | -1.62% | 11.70% | 45.8% | 59 | $3,306 | $-4,929 | 21 |
| 1w | mover_only | -11.31% | 19.52% | 34.0% | 50 | $-11,313 | $0 | 26 |
| 1w | io_only | +8.99% | 10.34% | 50.0% | 78 | $0 | $8,987 | 9 |
| 1w | dual | -2.13% | 11.22% | 43.1% | 130 | $-5,677 | $3,545 | 11 |
| 1w | overlay | +8.58% | 10.34% | 48.7% | 80 | $25 | $8,553 | 11 |
| 1w | overlay_boost | +13.75% | 11.15% | 54.7% | 53 | $-297 | $14,051 | 11 |
| 1w | io_boost | +20.79% | 7.49% | 58.8% | 51 | $0 | $20,790 | 9 |
| 2w | io_only | +2.86% | 10.41% | 48.7% | 39 | $0 | $2,857 | 14 |

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

Prints with a 1d exit: 246 · on S < +1: 92 · on S ≥ +1: 138

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.38% · 50.4% · n=246 |
| Down / messy (S < +1) | +0.44% · 54.3% · n=92 |
| Hard red (S < −3) | +0.66% · 53.5% · n=71 |
| Green mornings | +0.52% · 49.3% · n=138 |
| Down · large+ | -0.40% · 52.6% · n=38 |
| Down · mid | -0.25% · 41.2% · n=34 |
| Down · small/micro | +3.21% · 80.0% · n=20 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.17% · 53.1% · n=81 |
| Down · event-tagged | -0.17% · 56.5% · n=23 |
| Down · no event | +0.65% · 53.6% · n=69 |
| Down · join > 0 | -0.00% · 56.5% · n=69 |
| Down · join ≤ 0 / missing | +1.77% · 47.8% · n=23 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.27% · 51.4% · n=74 |
| Down · Energy | +0.73% · 62.1% · n=29 |
| Down · not Energy | +0.31% · 50.8% · n=63 |
| Down · Healthcare | +2.57% · 47.4% · n=19 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +4.82% | 9.28% | 47.6% | 254 |
| `large+` | +0.78% | 2.85% | 51.6% | 91 |
| `mid` | -2.35% | 3.57% | 42.0% | 88 |
| `small` | +6.63% | 3.49% | 49.3% | 75 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | -0.53% | 1.73% | 57.1% | 35 |
| `energy` | +2.36% | 0.82% | 61.5% | 39 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +0.61% | 7.75% | 46.0% | 200 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $116,350.71 | **+16.35%** | 10.07% | 81 | 51.9% | 472 |

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
| 2026-08-24 | -5.175 | io | 0 | 0 | 0 | 9 | $110,554 | io source missing (no stock_book file) |
| 2026-08-25 | 1.8 | io | 0 | 0 | 0 | 9 | $112,298 | io source missing (no stock_book file) |
| 2026-08-26 | 2.025 | io | 0 | 0 | 9 | 0 | $111,357 | io source missing (no stock_book file) |
| 2026-08-27 | — | io | 0 | 8 | 0 | 8 | $111,277 | — |
| 2026-08-28 | 0.75 | io | 0 | 0 | 0 | 8 | $108,368 | io source missing (no stock_book file) |
| 2026-08-31 | -5.85 | io | 0 | 0 | 0 | 8 | $107,493 | — |
| 2026-09-01 | -6.3 | io | 0 | 5 | 8 | 5 | $109,480 | — |
| 2026-09-02 | -3.825 | io | 0 | 0 | 0 | 5 | $109,807 | — |
| 2026-09-03 | -0.9 | io | 0 | 0 | 0 | 5 | $109,396 | — |
| 2026-09-04 | 2.25 | io | 0 | 5 | 5 | 5 | $108,516 | — |
| 2026-09-08 | -11.475 | io | 0 | 0 | 0 | 5 | $109,218 | — |
| 2026-09-09 | -13.95 | io | 0 | 0 | 0 | 5 | $105,937 | — |
| 2026-09-10 | -13.275 | io | 0 | 5 | 5 | 5 | $107,056 | — |
| 2026-09-11 | 0.5 | io | 0 | 0 | 0 | 5 | $108,118 | — |
| 2026-09-14 | -11.002 | io | 0 | 0 | 0 | 5 | $104,149 | — |
| 2026-09-15 | -3.836 | io | 0 | 5 | 5 | 5 | $102,454 | — |
| 2026-09-16 | 5.297 | io | 0 | 0 | 0 | 5 | $102,228 | — |
| 2026-09-17 | 7.383 | io | 0 | 0 | 0 | 5 | $102,729 | — |
| 2026-09-18 | 4.861 | io | 0 | 5 | 5 | 5 | $100,994 | — |
| 2026-09-21 | 12.871 | io | 0 | 0 | 0 | 5 | $104,315 | — |
| 2026-09-22 | -0.497 | io | 0 | 0 | 0 | 5 | $108,040 | — |
| 2026-09-23 | 2.293 | io | 0 | 5 | 5 | 5 | $108,855 | — |
| 2026-09-24 | -7.659 | io | 0 | 0 | 0 | 5 | $110,983 | — |
| 2026-09-25 | 2.706 | io | 0 | 0 | 0 | 5 | $111,193 | — |
| 2026-09-28 | -6.14 | io | 0 | 5 | 5 | 5 | $111,744 | — |
| 2026-09-29 | 1.292 | io | 0 | 0 | 0 | 5 | $111,071 | — |
| 2026-09-30 | 2.009 | io | 0 | 0 | 0 | 5 | $110,076 | — |
| 2026-10-01 | -1.343 | io | 0 | 5 | 5 | 5 | $112,183 | — |
| 2026-10-02 | 4.365 | io | 0 | 0 | 0 | 5 | $112,974 | — |
| 2026-10-05 | 0.086 | io | 0 | 0 | 0 | 5 | $116,433 | — |
| 2026-10-06 | 2.623 | io | 0 | 6 | 5 | 6 | $115,318 | — |
| 2026-10-07 | -5.211 | io | 0 | 0 | 0 | 6 | $114,138 | — |
| 2026-10-08 | -6.787 | io | 0 | 0 | 0 | 6 | $113,915 | — |
| 2026-10-09 | — | io | 0 | 0 | 6 | 0 | $116,351 | 3d cannot settle (end of calendar) |
| 2026-10-12 | — | io | 0 | 0 | 0 | 0 | $116,351 | 3d cannot settle (end of calendar) |
| 2026-10-13 | -6.767 | io | 0 | 0 | 0 | 0 | $116,351 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-09-23 16:00 ET | io | `A` | 131 | $165.32 | 2026-09-28 16:00 ET | $175.21 | $1,290.62 |
| 2026-09-23 16:00 ET | io | `HALO` | 193 | $112.44 | 2026-09-28 16:00 ET | $112.89 | $81.50 |
| 2026-09-23 16:00 ET | io | `ARQT` | 825 | $26.38 | 2026-09-28 16:00 ET | $27.33 | $762.15 |
| 2026-09-23 16:00 ET | io | `PGEN` | 2927 | $7.44 | 2026-09-28 16:00 ET | $7.81 | $1,006.80 |
| 2026-09-28 16:00 ET | io | `IT` | 120 | $185.73 | 2026-10-01 16:00 ET | $192.80 | $843.50 |
| 2026-09-28 16:00 ET | io | `TOST` | 735 | $30.38 | 2026-10-01 16:00 ET | $29.23 | $-864.51 |
| 2026-09-28 16:00 ET | io | `CNQ` | 470 | $47.55 | 2026-10-01 16:00 ET | $47.79 | $100.42 |
| 2026-09-28 16:00 ET | io | `SONO` | 1243 | $17.97 | 2026-10-01 16:00 ET | $17.75 | $-305.91 |
| 2026-09-28 16:00 ET | io | `FIVN` | 644 | $34.66 | 2026-10-01 16:00 ET | $35.75 | $685.05 |
| 2026-10-01 16:00 ET | io | `KSPI` | 243 | $92.02 | 2026-10-06 16:00 ET | $93.16 | $270.53 |
| 2026-10-01 16:00 ET | io | `IOT` | 560 | $40.04 | 2026-10-06 16:00 ET | $41.74 | $937.28 |
| 2026-10-01 16:00 ET | io | `AVPT` | 1594 | $14.08 | 2026-10-06 16:00 ET | $14.59 | $771.37 |
| 2026-10-01 16:00 ET | io | `RELY` | 1044 | $21.48 | 2026-10-06 16:00 ET | $23.20 | $1,768.38 |
| 2026-10-01 16:00 ET | io | `MQ` | 1320 | $17.00 | 2026-10-06 16:00 ET | $16.55 | $-628.45 |
| 2026-10-06 16:00 ET | io | `TOST` | 762 | $30.25 | 2026-10-09 16:00 ET | $31.31 | $787.74 |
| 2026-10-06 16:00 ET | io | `COP` | 178 | $129.35 | 2026-10-09 16:00 ET | $134.10 | $840.24 |
| 2026-10-06 16:00 ET | io | `PDFS` | 423 | $54.44 | 2026-10-09 16:00 ET | $54.40 | $-28.09 |
| 2026-10-06 16:00 ET | io | `AVPT` | 1581 | $14.59 | 2026-10-09 16:00 ET | $14.63 | $22.01 |
| 2026-10-06 16:00 ET | io | `ONON` | 348 | $33.13 | 2026-10-09 16:00 ET | $35.09 | $672.95 |
| 2026-10-06 16:00 ET | io | `BAND` | 191 | $60.09 | 2026-10-09 16:00 ET | $53.27 | $-1,307.86 |

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
