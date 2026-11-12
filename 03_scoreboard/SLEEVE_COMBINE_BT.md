# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-11-12T08:06:50-05:00 — 2026-08-13 → 2026-11-12 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 65 · days with mover BUY calls: 62 · days with a stock book: 61

## Finding (this window)

The 1d **switch** is **+6.14%**. That is worse than mover-only 1d (+3.09%) and worse than .io-only 1d size (+17.95%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+10.17%** / 8.19% DD. Best book this window: **1d overlay_boost +30.29%**. Overlay / boost **beats** raw .io size (+8.66%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +6.14% | 8.32% | 54.8% | 219 | $8,644 | $-2,505 | 26 |
| 1d | mover_only | +3.09% | 9.69% | 51.5% | 196 | $3,091 | $0 | 34 |
| 1d | io_only | +17.95% | 9.28% | 51.4% | 391 | $0 | $17,947 | 5 |
| 1d | dual | +10.17% | 8.19% | 51.4% | 589 | $1,719 | $8,447 | 7 |
| 1d | overlay | +23.76% | 9.17% | 52.6% | 409 | $3,206 | $20,550 | 7 |
| 1d | overlay_boost | +30.29% | 9.08% | 52.9% | 308 | $-1,013 | $31,298 | 7 |
| 1d | io_boost | +29.32% | 8.74% | 52.6% | 304 | $0 | $29,321 | 5 |
| 3d | combine | +12.50% | 14.89% | 53.7% | 136 | $12,017 | $485 | 28 |
| 3d | mover_only | -2.07% | 20.16% | 44.1% | 118 | $-2,075 | $0 | 36 |
| 3d | io_only | +8.66% | 14.55% | 52.3% | 195 | $0 | $8,656 | 7 |
| 3d | dual | +2.71% | 14.06% | 48.9% | 319 | $-536 | $3,244 | 9 |
| 3d | overlay | +9.14% | 15.49% | 53.8% | 199 | $-993 | $10,138 | 9 |
| 3d | overlay_boost | +12.35% | 14.57% | 52.9% | 121 | $-1,126 | $13,481 | 9 |
| **3d** | **io_boost** | +19.84% | 12.19% | 55.2% | 116 | $0 | $19,837 | 7 |
| 1w | combine | +9.87% | 11.70% | 52.0% | 98 | $3,841 | $6,034 | 30 |
| 1w | mover_only | -9.92% | 19.52% | 37.5% | 80 | $-9,920 | $0 | 38 |
| 1w | io_only | +18.45% | 10.34% | 55.9% | 118 | $0 | $18,454 | 9 |
| 1w | dual | +4.18% | 11.22% | 48.0% | 200 | $-4,888 | $9,065 | 11 |
| 1w | overlay | +22.65% | 10.34% | 56.9% | 123 | $129 | $22,525 | 11 |
| 1w | overlay_boost | +24.00% | 11.15% | 62.5% | 80 | $241 | $23,763 | 11 |
| 1w | io_boost | +24.56% | 7.49% | 61.4% | 70 | $0 | $24,565 | 9 |
| 2w | io_only | +14.76% | 10.41% | 57.6% | 59 | $0 | $14,761 | 14 |

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

Prints with a 1d exit: 382 · on S < +1: 136 · on S ≥ +1: 210

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.61% · 54.2% · n=382 |
| Down / messy (S < +1) | +0.84% · 55.1% · n=136 |
| Hard red (S < −3) | +1.25% · 55.8% · n=95 |
| Green mornings | +0.62% · 54.3% · n=210 |
| Down · large+ | -0.07% · 55.1% · n=49 |
| Down · mid | -0.14% · 43.8% · n=48 |
| Down · small/micro | +3.21% · 69.2% · n=39 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | +0.48% · 54.4% · n=125 |
| Down · event-tagged | -0.09% · 58.3% · n=24 |
| Down · no event | +1.04% · 54.5% · n=112 |
| Down · join > 0 | +0.69% · 57.3% · n=96 |
| Down · join ≤ 0 / missing | +1.20% · 50.0% · n=40 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | +0.46% · 53.4% · n=118 |
| Down · Energy | +0.73% · 62.1% · n=29 |
| Down · not Energy | +0.87% · 53.3% · n=107 |
| Down · Healthcare | +2.35% · 47.6% · n=42 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +17.95% | 9.28% | 51.4% | 391 |
| `large+` | +1.64% | 2.85% | 53.0% | 134 |
| `mid` | +1.35% | 3.57% | 49.3% | 136 |
| `small` | +14.70% | 3.49% | 52.1% | 121 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | -0.81% | 2.00% | 55.3% | 38 |
| `energy` | +2.52% | 0.82% | 62.5% | 40 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +7.30% | 7.75% | 51.0% | 304 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $119,837.26 | **+19.84%** | 12.19% | 116 | 55.2% | 822 |

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
| 2026-10-09 | — | io | 0 | 5 | 6 | 5 | $116,329 | — |
| 2026-10-12 | — | io | 0 | 0 | 0 | 5 | $117,622 | — |
| 2026-10-13 | -6.767 | io | 0 | 0 | 0 | 5 | $119,771 | — |
| 2026-10-14 | -1.645 | io | 0 | 5 | 5 | 5 | $118,441 | — |
| 2026-10-15 | — | io | 0 | 0 | 0 | 5 | $122,667 | — |
| 2026-10-16 | 5.526 | io | 0 | 0 | 0 | 5 | $122,643 | — |
| 2026-10-19 | 2.125 | io | 0 | 5 | 5 | 5 | $121,623 | — |
| 2026-10-20 | -8.071 | io | 0 | 0 | 0 | 5 | $125,202 | — |
| 2026-10-21 | 4.644 | io | 0 | 0 | 0 | 5 | $125,340 | — |
| 2026-10-22 | -14.314 | io | 0 | 1 | 5 | 1 | $125,072 | — |
| 2026-10-23 | -7.538 | io | 0 | 4 | 0 | 5 | $126,844 | — |
| 2026-10-26 | -2.128 | io | 0 | 0 | 0 | 5 | $127,751 | — |
| 2026-10-27 | 5.515 | io | 0 | 1 | 1 | 5 | $126,320 | — |
| 2026-10-28 | 6.621 | io | 0 | 4 | 4 | 5 | $127,266 | — |
| 2026-10-29 | -6.946 | io | 0 | 0 | 0 | 5 | $128,167 | — |
| 2026-10-30 | -12.921 | io | 0 | 1 | 1 | 5 | $132,472 | — |
| 2026-11-02 | 3.044 | io | 0 | 3 | 4 | 4 | $131,675 | — |
| 2026-11-03 | -5.632 | io | 0 | 0 | 0 | 4 | $131,464 | — |
| 2026-11-04 | 10.848 | io | 0 | 1 | 1 | 4 | $123,317 | — |
| 2026-11-05 | -2.495 | io | 0 | 4 | 3 | 5 | $117,775 | — |
| 2026-11-06 | — | io | 0 | 0 | 0 | 5 | $116,322 | — |
| 2026-11-09 | 9.449 | io | 0 | 1 | 1 | 5 | $119,834 | — |
| 2026-11-10 | 6.883 | io | 0 | 0 | 4 | 1 | $120,139 | 3d cannot settle (end of calendar) |
| 2026-11-11 | 12.369 | io | 0 | 0 | 0 | 1 | $119,840 | 3d cannot settle (end of calendar) |
| 2026-11-12 | -7.158 | io | 0 | 0 | 1 | 0 | $119,837 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-10-22 16:00 ET | io | `NET` | 82 | $303.61 | 2026-10-27 16:00 ET | $327.80 | $1,978.88 |
| 2026-10-23 16:00 ET | io | `BAX` | 1003 | $25.30 | 2026-10-28 16:00 ET | $25.46 | $134.23 |
| 2026-10-23 16:00 ET | io | `PACS` | 532 | $47.64 | 2026-10-28 16:00 ET | $49.06 | $741.42 |
| 2026-10-23 16:00 ET | io | `PRGO` | 1589 | $15.96 | 2026-10-28 16:00 ET | $15.92 | $-105.02 |
| 2026-10-23 16:00 ET | io | `IT` | 55 | $230.28 | 2026-10-28 16:00 ET | $219.16 | $-616.01 |
| 2026-10-27 16:00 ET | io | `SN` | 127 | $197.53 | 2026-10-30 16:00 ET | $208.00 | $1,324.72 |
| 2026-10-28 16:00 ET | io | `VEEV` | 80 | $317.82 | 2026-11-02 16:00 ET | $307.00 | $-870.27 |
| 2026-10-28 16:00 ET | io | `CORT` | 295 | $86.28 | 2026-11-02 16:00 ET | $80.46 | $-1,724.75 |
| 2026-10-28 16:00 ET | io | `CDNA` | 352 | $72.13 | 2026-11-02 16:00 ET | $85.88 | $4,830.62 |
| 2026-10-28 16:00 ET | io | `ARQT` | 809 | $31.45 | 2026-11-02 16:00 ET | $31.26 | $-174.92 |
| 2026-10-30 16:00 ET | io | `FIS` | 687 | $38.55 | 2026-11-04 16:00 ET | $29.76 | $-6,056.73 |
| 2026-11-02 16:00 ET | io | `VEEV` | 85 | $307.00 | 2026-11-05 16:00 ET | $310.10 | $258.78 |
| 2026-11-02 16:00 ET | io | `CORT` | 327 | $80.46 | 2026-11-05 16:00 ET | $71.63 | $-2,896.09 |
| 2026-11-02 16:00 ET | io | `VLO` | 69 | $381.46 | 2026-11-05 16:00 ET | $351.23 | $-2,090.47 |
| 2026-11-04 16:00 ET | io | `ILMN` | 108 | $226.54 | 2026-11-09 16:00 ET | $220.78 | $-626.91 |
| 2026-11-05 16:00 ET | io | `CSGP` | 735 | $32.05 | 2026-11-10 16:00 ET | $30.33 | $-1,283.46 |
| 2026-11-05 16:00 ET | io | `CMCSA` | 944 | $24.95 | 2026-11-10 16:00 ET | $24.75 | $-213.50 |
| 2026-11-05 16:00 ET | io | `CLX` | 236 | $99.70 | 2026-11-10 16:00 ET | $106.60 | $1,622.08 |
| 2026-11-05 16:00 ET | io | `COCO` | 370 | $63.63 | 2026-11-10 16:00 ET | $65.91 | $833.80 |
| 2026-11-09 16:00 ET | io | `ALGN` | 137 | $174.26 | 2026-11-12 16:00 ET | $172.08 | $-303.67 |

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
