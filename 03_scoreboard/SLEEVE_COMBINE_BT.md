# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-09-11T07:29:50-04:00 — 2026-08-13 → 2026-09-11 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 21 · days with mover BUY calls: 16 · days with a stock book: 17

## Finding (this window)

The 1d **switch** is **+2.95%**. That is worse than mover-only 1d (+2.03%) and worse than .io-only 1d size (+5.13%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+3.45%** / 3.60% DD. Best book this window: **1w overlay_boost +11.94%**. Overlay / boost **beats** raw .io size (+3.76%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +2.95% | 3.25% | 48.7% | 39 | $4,399 | $-1,446 | 13 |
| 1d | mover_only | +2.03% | 4.25% | 52.5% | 40 | $2,035 | $0 | 14 |
| 1d | io_only | +5.13% | 3.71% | 42.7% | 103 | $0 | $5,128 | 5 |
| 1d | dual | +3.45% | 3.60% | 45.5% | 143 | $988 | $2,459 | 7 |
| 1d | overlay | +6.07% | 4.48% | 44.2% | 104 | $714 | $5,358 | 7 |
| 1d | overlay_boost | +6.23% | 5.80% | 46.0% | 100 | $-266 | $6,498 | 7 |
| 1d | io_boost | +4.29% | 5.83% | 45.4% | 97 | $0 | $4,285 | 5 |
| 3d | combine | +0.32% | 6.40% | 51.7% | 29 | $5,349 | $-5,028 | 13 |
| 3d | mover_only | -5.53% | 12.24% | 43.3% | 30 | $-5,525 | $0 | 14 |
| 3d | io_only | +3.76% | 7.60% | 47.3% | 55 | $0 | $3,761 | 7 |
| 3d | dual | -1.23% | 9.81% | 44.3% | 88 | $-2,790 | $1,557 | 9 |
| 3d | overlay | +2.93% | 8.27% | 44.8% | 58 | $-1,034 | $3,960 | 9 |
| 3d | overlay_boost | +4.38% | 6.98% | 47.8% | 46 | $-1,034 | $5,412 | 9 |
| **3d** | **io_boost** | +5.05% | 6.45% | 48.9% | 45 | $0 | $5,054 | 7 |
| 1w | combine | +2.03% | 4.88% | 53.6% | 28 | $4,381 | $-2,350 | 14 |
| 1w | mover_only | +4.38% | 2.69% | 80.0% | 10 | $4,381 | $0 | 15 |
| 1w | io_only | +7.26% | 4.40% | 56.4% | 39 | $0 | $7,263 | 9 |
| 1w | dual | +5.94% | 2.13% | 63.3% | 49 | $2,476 | $3,460 | 11 |
| 1w | overlay | +7.26% | 4.40% | 56.4% | 39 | $0 | $7,263 | 11 |
| 1w | overlay_boost | +11.94% | 2.60% | 67.7% | 31 | $733 | $11,210 | 11 |
| 1w | io_boost | +11.21% | 2.60% | 66.7% | 30 | $0 | $11,210 | 9 |
| 2w | io_only | +9.65% | 3.68% | 73.7% | 19 | $0 | $9,648 | 13 |

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

Prints with a 1d exit: 100 · on S < +1: 37 · on S ≥ +1: 54

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.78% · 45.0% · n=100 |
| Down / messy (S < +1) | +1.49% · 51.4% · n=37 |
| Hard red (S < −3) | +1.81% · 54.8% · n=31 |
| Green mornings | +0.61% · 42.6% · n=54 |
| Down · large+ | -0.06% · 44.4% · n=18 |
| Down · mid | +0.17% · 50.0% · n=12 |
| Down · small/micro | +7.73% · 71.4% · n=7 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | +0.04% · 46.2% · n=26 |
| Down · event-tagged | +0.27% · 53.8% · n=13 |
| Down · no event | +2.15% · 50.0% · n=24 |
| Down · join > 0 | -0.08% · 43.5% · n=23 |
| Down · join ≤ 0 / missing | +4.07% · 64.3% · n=14 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.30% · 36.8% · n=19 |
| Down · Energy | +0.34% · 52.9% · n=17 |
| Down · not Energy | +2.46% · 50.0% · n=20 |
| Down · Healthcare | +4.39% · 50.0% · n=10 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +5.13% | 3.71% | 42.7% | 103 |
| `large+` | +0.92% | 1.22% | 47.5% | 40 |
| `mid` | -2.33% | 2.66% | 34.3% | 35 |
| `small` | +6.72% | 0.79% | 46.4% | 28 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | +0.27% | 0.99% | 54.2% | 24 |
| `energy` | +0.87% | 1.65% | 53.6% | 28 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +0.47% | 2.89% | 40.2% | 82 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $105,053.64 | **+5.05%** | 6.45% | 45 | 48.9% | 141 |

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
| 2026-09-09 | -13.95 | io | 0 | 0 | 0 | 5 | $105,087 | 3d cannot settle (end of calendar) |
| 2026-09-10 | -13.275 | io | 0 | 0 | 5 | 0 | $105,054 | 3d cannot settle (end of calendar) |
| 2026-09-11 | 0.5 | io | 0 | 0 | 0 | 0 | $105,054 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-08-21 16:00 ET | io | `CRMD` | 1367 | $8.20 | 2026-08-26 16:00 ET | $8.39 | $224.15 |
| 2026-08-21 16:00 ET | io | `RZLT` | 2202 | $5.09 | 2026-08-26 16:00 ET | $5.04 | $-167.37 |
| 2026-08-27 16:00 ET | io | `VYX` | 2508 | $8.88 | 2026-09-01 16:00 ET | $8.27 | $-1,595.17 |
| 2026-08-27 16:00 ET | io | `PGY` | 993 | $22.41 | 2026-09-01 16:00 ET | $21.25 | $-1,177.83 |
| 2026-08-27 16:00 ET | io | `FUTU` | 87 | $127.34 | 2026-09-01 16:00 ET | $120.88 | $-566.62 |
| 2026-08-27 16:00 ET | io | `CNH` | 973 | $11.43 | 2026-09-01 16:00 ET | $11.77 | $305.46 |
| 2026-08-27 16:00 ET | io | `HOOD` | 102 | $108.54 | 2026-09-01 16:00 ET | $107.41 | $-119.96 |
| 2026-08-27 16:00 ET | io | `RRC` | 267 | $41.64 | 2026-09-01 16:00 ET | $42.40 | $195.90 |
| 2026-08-27 16:00 ET | io | `CRK` | 761 | $14.62 | 2026-09-01 16:00 ET | $14.90 | $193.23 |
| 2026-08-27 16:00 ET | io | `MOS` | 468 | $23.76 | 2026-09-01 16:00 ET | $24.25 | $217.07 |
| 2026-09-01 16:00 ET | io | `FTI` | 273 | $79.53 | 2026-09-04 16:00 ET | $80.08 | $142.89 |
| 2026-09-01 16:00 ET | io | `DK` | 288 | $75.39 | 2026-09-04 16:00 ET | $72.40 | $-868.77 |
| 2026-09-01 16:00 ET | io | `BTE` | 4440 | $4.90 | 2026-09-04 16:00 ET | $4.95 | $106.53 |
| 2026-09-01 16:00 ET | io | `MTDR` | 369 | $58.90 | 2026-09-04 16:00 ET | $59.72 | $292.82 |
| 2026-09-01 16:00 ET | io | `RES` | 3295 | $6.60 | 2026-09-04 16:00 ET | $6.47 | $-514.08 |
| 2026-09-04 16:00 ET | io | `HOOD` | 173 | $124.72 | 2026-09-10 16:00 ET | $115.28 | $-1,638.33 |
| 2026-09-04 16:00 ET | io | `XP` | 1079 | $20.00 | 2026-09-10 16:00 ET | $19.00 | $-1,107.18 |
| 2026-09-04 16:00 ET | io | `ASND` | 79 | $271.12 | 2026-09-10 16:00 ET | $271.00 | $-14.12 |
| 2026-09-04 16:00 ET | io | `OSCR` | 669 | $32.24 | 2026-09-10 16:00 ET | $31.70 | $-378.80 |
| 2026-09-04 16:00 ET | io | `ATRC` | 411 | $52.46 | 2026-09-10 16:00 ET | $53.03 | $223.42 |

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
