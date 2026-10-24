# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-10-24T07:31:19-04:00 — 2026-08-13 → 2026-10-23 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 51 · days with mover BUY calls: 36 · days with a stock book: 47

## Finding (this window)

The 1d **switch** is **+2.98%**. That is worse than mover-only 1d (-2.06%) and worse than .io-only 1d size (+10.41%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+3.98%** / 8.19% DD. Best book this window: **1w io_boost +33.80%**. Overlay / boost **beats** raw .io size (+12.78%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +2.98% | 8.32% | 54.7% | 139 | $4,843 | $-1,860 | 25 |
| 1d | mover_only | -2.06% | 9.08% | 49.1% | 108 | $-2,055 | $0 | 34 |
| 1d | io_only | +10.41% | 9.28% | 52.0% | 302 | $0 | $10,411 | 5 |
| 1d | dual | +3.98% | 8.19% | 51.3% | 411 | $-840 | $4,819 | 13 |
| 1d | overlay | +12.52% | 9.17% | 52.8% | 309 | $18 | $12,500 | 13 |
| 1d | overlay_boost | +15.12% | 9.08% | 53.8% | 264 | $-446 | $15,568 | 13 |
| 1d | io_boost | +14.13% | 8.74% | 53.8% | 260 | $0 | $14,131 | 5 |
| 3d | combine | +3.93% | 14.89% | 50.5% | 97 | $9,061 | $-5,130 | 25 |
| 3d | mover_only | -9.50% | 20.16% | 37.1% | 70 | $-9,504 | $0 | 34 |
| 3d | io_only | +12.78% | 14.55% | 54.2% | 155 | $0 | $12,782 | 7 |
| 3d | dual | +0.58% | 14.06% | 48.0% | 227 | $-4,820 | $5,396 | 14 |
| 3d | overlay | +10.75% | 15.49% | 55.1% | 156 | $-1,031 | $11,786 | 14 |
| 3d | overlay_boost | +20.78% | 11.65% | 58.2% | 110 | $-1,031 | $21,815 | 14 |
| **3d** | **io_boost** | +22.53% | 10.07% | 60.0% | 110 | $0 | $22,535 | 7 |
| 1w | combine | +2.62% | 11.70% | 49.3% | 69 | $3,271 | $-655 | 25 |
| 1w | mover_only | -11.41% | 19.52% | 36.0% | 50 | $-11,412 | $0 | 34 |
| 1w | io_only | +23.07% | 10.34% | 58.3% | 96 | $0 | $23,070 | 9 |
| 1w | dual | +4.71% | 11.22% | 50.3% | 147 | $-5,722 | $10,435 | 15 |
| 1w | overlay | +21.04% | 10.34% | 57.7% | 97 | $25 | $21,012 | 15 |
| 1w | overlay_boost | +27.30% | 11.15% | 64.8% | 71 | $-297 | $27,596 | 15 |
| 1w | io_boost | +33.80% | 7.49% | 69.1% | 68 | $0 | $33,803 | 9 |
| 2w | io_only | +10.23% | 10.41% | 55.3% | 47 | $0 | $10,227 | 14 |

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

Prints with a 1d exit: 302 · on S < +1: 108 · on S ≥ +1: 164

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.51% · 53.3% · n=302 |
| Down / messy (S < +1) | +0.66% · 56.5% · n=108 |
| Hard red (S < −3) | +0.77% · 54.4% · n=79 |
| Green mornings | +0.62% · 53.0% · n=164 |
| Down · large+ | -0.23% · 52.4% · n=42 |
| Down · mid | -0.05% · 46.2% · n=39 |
| Down · small/micro | +3.09% · 77.8% · n=27 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | +0.18% · 55.7% · n=97 |
| Down · event-tagged | -0.17% · 56.5% · n=23 |
| Down · no event | +0.89% · 56.5% · n=85 |
| Down · join > 0 | +0.22% · 58.2% · n=79 |
| Down · join ≤ 0 / missing | +1.87% · 51.7% · n=29 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | +0.12% · 54.4% · n=90 |
| Down · Energy | +0.73% · 62.1% · n=29 |
| Down · not Energy | +0.64% · 54.4% · n=79 |
| Down · Healthcare | +2.14% · 50.0% · n=26 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +10.41% | 9.28% | 52.0% | 302 |
| `large+` | +1.15% | 2.85% | 52.8% | 106 |
| `mid` | +0.21% | 3.57% | 48.5% | 103 |
| `small` | +9.06% | 3.49% | 54.8% | 93 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | -0.53% | 1.73% | 57.1% | 35 |
| `energy` | +2.36% | 0.82% | 61.5% | 39 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +3.42% | 7.75% | 50.4% | 236 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $122,534.69 | **+22.53%** | 10.07% | 110 | 60.0% | 499 |

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
| 2026-10-09 | — | io | 0 | 9 | 6 | 9 | $116,320 | — |
| 2026-10-12 | — | io | 0 | 1 | 0 | 10 | $117,581 | — |
| 2026-10-13 | -6.767 | io | 0 | 0 | 0 | 10 | $118,651 | — |
| 2026-10-14 | -1.645 | io | 0 | 9 | 9 | 10 | $117,745 | — |
| 2026-10-15 | — | io | 0 | 1 | 1 | 10 | $121,954 | — |
| 2026-10-16 | 5.526 | io | 0 | 0 | 0 | 10 | $119,724 | — |
| 2026-10-19 | 2.125 | io | 0 | 8 | 9 | 9 | $119,712 | — |
| 2026-10-20 | -8.071 | io | 0 | 1 | 1 | 9 | $122,484 | — |
| 2026-10-21 | 4.644 | io | 0 | 0 | 0 | 9 | $122,865 | 3d cannot settle (end of calendar) |
| 2026-10-22 | -14.314 | io | 0 | 0 | 8 | 1 | $122,664 | 3d cannot settle (end of calendar) |
| 2026-10-23 | -7.538 | io | 0 | 0 | 1 | 0 | $122,535 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-10-12 16:00 ET | io | `FANG` | 60 | $195.11 | 2026-10-15 16:00 ET | $195.60 | $24.96 |
| 2026-10-14 16:00 ET | io | `TMUS` | 74 | $157.79 | 2026-10-19 16:00 ET | $156.09 | $-130.33 |
| 2026-10-14 16:00 ET | io | `TRU` | 175 | $66.94 | 2026-10-19 16:00 ET | $73.12 | $1,076.33 |
| 2026-10-14 16:00 ET | io | `WSM` | 48 | $242.48 | 2026-10-19 16:00 ET | $249.43 | $329.23 |
| 2026-10-14 16:00 ET | io | `CNK` | 308 | $38.20 | 2026-10-19 16:00 ET | $39.00 | $238.31 |
| 2026-10-14 16:00 ET | io | `AVPT` | 841 | $14.00 | 2026-10-19 16:00 ET | $14.74 | $600.40 |
| 2026-10-14 16:00 ET | io | `WLY` | 228 | $51.49 | 2026-10-19 16:00 ET | $51.78 | $60.11 |
| 2026-10-14 16:00 ET | io | `PRCH` | 716 | $16.44 | 2026-10-19 16:00 ET | $17.65 | $847.66 |
| 2026-10-14 16:00 ET | io | `EZPW` | 359 | $32.72 | 2026-10-19 16:00 ET | $32.75 | $1.35 |
| 2026-10-14 16:00 ET | io | `BAND` | 248 | $47.40 | 2026-10-19 16:00 ET | $43.50 | $-973.73 |
| 2026-10-15 16:00 ET | io | `IT` | 58 | $207.31 | 2026-10-20 16:00 ET | $219.64 | $710.70 |
| 2026-10-19 16:00 ET | io | `VEEV` | 37 | $316.80 | 2026-10-22 16:00 ET | $322.55 | $208.44 |
| 2026-10-19 16:00 ET | io | `SN` | 63 | $188.94 | 2026-10-22 16:00 ET | $199.16 | $639.39 |
| 2026-10-19 16:00 ET | io | `ILMN` | 45 | $262.66 | 2026-10-22 16:00 ET | $269.40 | $298.95 |
| 2026-10-19 16:00 ET | io | `CDNA` | 178 | $67.08 | 2026-10-22 16:00 ET | $69.16 | $365.07 |
| 2026-10-19 16:00 ET | io | `ARQT` | 375 | $31.89 | 2026-10-22 16:00 ET | $32.20 | $106.41 |
| 2026-10-19 16:00 ET | io | `PACS` | 261 | $45.85 | 2026-10-22 16:00 ET | $47.50 | $423.77 |
| 2026-10-19 16:00 ET | io | `PRCH` | 678 | $17.65 | 2026-10-22 16:00 ET | $17.94 | $178.91 |
| 2026-10-19 16:00 ET | io | `CBRL` | 173 | $69.02 | 2026-10-22 16:00 ET | $67.31 | $-300.97 |
| 2026-10-20 16:00 ET | io | `SGRY` | 790 | $15.49 | 2026-10-23 16:00 ET | $15.75 | $184.79 |

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
