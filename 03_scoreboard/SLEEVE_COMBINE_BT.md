# Sleeve combine backtest (matched hold, shared cash)

_Generated 2026-10-07T00:37:50-04:00 — 2026-08-13 → 2026-10-06 · $100,000 · 10 names · 10% equity / fill · Futubull fees_

This is the integrity backtest. Both sleeves use the **same hold** (1d / 3d / 1w). Mover still enters at 09:30, .io still enters at 16:00 — those clocks are data constraints, not a style choice. Open buys cannot spend the same day's close-sale cash. Missing mover calls and missing books are logged as gaps, not as a gate.

**2w / 1m are not combined with mover.** Live .io `2w_size` is a follow-the-book product with a 10-session min-hold; pairing it with mover 1d locks cash in ways a curve-stitch cannot see. The 2w row below is an .io-only reference.

Sessions in window: 38 · days with mover BUY calls: 16 · days with a stock book: 34

## Finding (this window)

The 1d **switch** is **+4.49%**. That is worse than mover-only 1d (+2.03%) and worse than .io-only 1d size (+6.52%). Copying .io green-pile / join-good / sector-not-red onto mover names also fails on down days. Fifty-fifty dual is a blend, not an upgrade — it cannot beat the stronger sleeve. 1d dual (two wallets) is **+3.99%** / 5.52% DD. Best book this window: **1w overlay_boost +12.42%**. Overlay / boost **beats** raw .io size (+4.45%) by keeping the size book at full capital and using mover only as idle-cash + close-print size-up.

## Sweep (size-sleeve .io picks)

| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1d | combine | +4.49% | 6.10% | 50.6% | 160 | $4,399 | $94 | 11 |
| 1d | mover_only | +2.03% | 4.25% | 52.5% | 40 | $2,035 | $0 | 31 |
| 1d | io_only | +6.52% | 7.74% | 47.7% | 222 | $0 | $6,522 | 5 |
| 1d | dual | +3.99% | 5.52% | 48.5% | 262 | $988 | $3,000 | 26 |
| 1d | overlay | +7.53% | 8.47% | 48.4% | 223 | $714 | $6,812 | 26 |
| 1d | overlay_boost | +7.41% | 9.96% | 49.3% | 219 | $-266 | $7,679 | 26 |
| 1d | io_boost | +5.41% | 10.00% | 49.1% | 216 | $0 | $5,408 | 5 |
| 3d | combine | +2.84% | 8.31% | 48.3% | 87 | $4,838 | $-1,994 | 13 |
| 3d | mover_only | -8.27% | 14.80% | 43.3% | 30 | $-8,274 | $0 | 31 |
| 3d | io_only | +4.45% | 12.61% | 49.1% | 114 | $0 | $4,448 | 7 |
| 3d | dual | -2.27% | 14.77% | 46.6% | 148 | $-4,162 | $1,892 | 26 |
| 3d | overlay | +0.82% | 16.06% | 47.4% | 116 | $-1,034 | $1,849 | 26 |
| 3d | overlay_boost | +5.45% | 12.82% | 49.0% | 104 | $-1,034 | $6,486 | 26 |
| **3d** | **io_boost** | +6.33% | 12.31% | 49.5% | 103 | $0 | $6,332 | 7 |
| 1w | combine | +1.01% | 13.33% | 47.5% | 59 | $2,358 | $-1,351 | 15 |
| 1w | mover_only | -4.70% | 14.90% | 45.0% | 20 | $-4,697 | $0 | 31 |
| 1w | io_only | +6.67% | 10.64% | 49.3% | 69 | $0 | $6,674 | 9 |
| 1w | dual | +0.72% | 9.07% | 47.8% | 90 | $-2,090 | $2,811 | 26 |
| 1w | overlay | +6.67% | 10.64% | 49.3% | 69 | $0 | $6,674 | 26 |
| 1w | overlay_boost | +12.42% | 6.33% | 52.5% | 61 | $733 | $11,688 | 26 |
| 1w | io_boost | +11.70% | 6.88% | 51.7% | 60 | $0 | $11,702 | 9 |
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

Prints with a 1d exit: 222 · on S < +1: 38 · on S ≥ +1: 54

| Cut | Mean · win · n |
|---|---|
| All size prints | +0.48% · 48.6% · n=222 |
| Down / messy (S < +1) | +1.38% · 50.0% · n=38 |
| Hard red (S < −3) | +1.68% · 53.1% · n=32 |
| Green mornings | +0.61% · 42.6% · n=54 |
| Down · large+ | -0.06% · 44.4% · n=18 |
| Down · mid | -0.04% · 46.2% · n=13 |
| Down · small/micro | +7.73% · 71.4% · n=7 |
| Down · rebound | +4.92% · 63.6% · n=11 |
| Down · not rebound | -0.06% · 44.4% · n=27 |
| Down · event-tagged | +0.27% · 53.8% · n=13 |
| Down · no event | +1.96% · 48.0% · n=25 |
| Down · join > 0 | -0.08% · 43.5% · n=23 |
| Down · join ≤ 0 / missing | +3.63% · 60.0% · n=15 |
| Down · sector > 0 | +3.38% · 66.7% · n=18 |
| Down · sector ≤ 0 / missing | -0.41% · 35.0% · n=20 |
| Down · Energy | +0.34% · 52.9% · n=17 |
| Down · not Energy | +2.23% · 47.6% · n=21 |
| Down · Healthcare | +4.39% · 50.0% · n=10 |

Cash-accounted .io-only 1d (same $100k / 10% / Futubull). Filtering the size book *reduces* names; leftover cash sits. `large+_on_down` keeps the full 3-bucket book on green mornings and large+ only when S < +1.

| Filter | Ret | Max DD | Win | Trades |
|---|---:|---:|---:|---:|
| `all` | +6.52% | 7.74% | 47.7% | 222 |
| `large+` | +2.02% | 2.96% | 51.2% | 82 |
| `mid` | -3.03% | 4.15% | 39.0% | 77 |
| `small` | +7.83% | 1.28% | 54.0% | 63 |
| `rebound` | +5.36% | 1.27% | 60.0% | 20 |
| `event` | +0.58% | 1.30% | 56.2% | 32 |
| `energy` | +1.18% | 1.96% | 56.4% | 39 |
| `sector_good` | +7.00% | 0.75% | 55.8% | 43 |
| `large+_on_down` | +1.97% | 6.82% | 47.0% | 202 |

The size book itself was *better* on S < +1 than on green mornings. Extra gates mostly do not improve the cash book: large+ / Energy / event / join>0 all lose to the raw 3-bucket sleeve. `sector_good` is the one filter that beat `all` this window — slightly, on half the names, with less DD. Treat that as a size-up tilt, not a new sleeve; thirteen book days is too thin to replace the 3-bucket rule. Rebound is already how the book stays long when gen is red. The down-day attribute that survives is still **stay in the size book**.

## Primary book — io_boost hold=3d

| Start | Final | Return | Max DD | Trades | Win | Skipped |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $106,331.84 | **+6.33%** | 12.31% | 103 | 49.5% | 231 |

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
| 2026-09-09 | — | io | 0 | 0 | 0 | 5 | $104,366 | — |
| 2026-09-10 | — | io | 0 | 9 | 5 | 9 | $105,473 | — |
| 2026-09-11 | — | io | 0 | 1 | 0 | 10 | $106,994 | — |
| 2026-09-14 | — | io | 0 | 0 | 0 | 10 | $102,364 | — |
| 2026-09-15 | — | io | 0 | 9 | 9 | 10 | $101,464 | — |
| 2026-09-16 | — | io | 0 | 1 | 1 | 10 | $101,250 | — |
| 2026-09-17 | — | io | 0 | 0 | 0 | 10 | $100,804 | — |
| 2026-09-18 | — | io | 0 | 9 | 9 | 10 | $98,473 | — |
| 2026-09-21 | — | io | 0 | 1 | 1 | 10 | $101,637 | — |
| 2026-09-22 | — | io | 0 | 0 | 0 | 10 | $104,068 | — |
| 2026-09-23 | — | io | 0 | 8 | 9 | 9 | $104,060 | — |
| 2026-09-24 | — | io | 0 | 2 | 1 | 10 | $105,485 | — |
| 2026-09-25 | — | io | 0 | 0 | 0 | 10 | $104,802 | — |
| 2026-09-28 | — | io | 0 | 8 | 8 | 10 | $105,176 | — |
| 2026-09-29 | — | io | 0 | 2 | 2 | 10 | $104,879 | — |
| 2026-09-30 | — | io | 0 | 0 | 0 | 10 | $104,316 | — |
| 2026-10-01 | — | io | 0 | 8 | 8 | 10 | $105,695 | — |
| 2026-10-02 | — | io | 0 | 0 | 2 | 8 | $105,990 | 3d cannot settle (end of calendar) |
| 2026-10-05 | — | io | 0 | 0 | 0 | 8 | $107,717 | 3d cannot settle (end of calendar) |
| 2026-10-06 | — | io | 0 | 0 | 8 | 0 | $106,332 | 3d cannot settle (end of calendar) |

### Last 20 round-trips

Every BUY and SELL is on the dashboard day picker ([sleeve-combine](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/)). This table is the tail of `bt_trades.csv`.

| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |
|---|---|---|---:|---:|---|---:|---:|
| 2026-09-24 16:00 ET | io | `CHKP` | 77 | $136.29 | 2026-09-29 16:00 ET | $126.93 | $-725.25 |
| 2026-09-24 16:00 ET | io | `EOG` | 73 | $142.86 | 2026-09-29 16:00 ET | $139.66 | $-238.11 |
| 2026-09-28 16:00 ET | io | `IT` | 56 | $185.73 | 2026-10-01 16:00 ET | $192.80 | $391.51 |
| 2026-09-28 16:00 ET | io | `TOST` | 346 | $30.38 | 2026-10-01 16:00 ET | $29.23 | $-406.96 |
| 2026-09-28 16:00 ET | io | `CNQ` | 221 | $47.55 | 2026-10-01 16:00 ET | $47.79 | $47.22 |
| 2026-09-28 16:00 ET | io | `SONO` | 585 | $17.97 | 2026-10-01 16:00 ET | $17.75 | $-143.98 |
| 2026-09-28 16:00 ET | io | `CON` | 297 | $35.36 | 2026-10-01 16:00 ET | $34.97 | $-123.62 |
| 2026-09-28 16:00 ET | io | `FIVN` | 303 | $34.66 | 2026-10-01 16:00 ET | $35.75 | $322.31 |
| 2026-09-28 16:00 ET | io | `MQ` | 622 | $16.89 | 2026-10-01 16:00 ET | $17.00 | $52.19 |
| 2026-09-28 16:00 ET | io | `AMN` | 307 | $34.26 | 2026-10-01 16:00 ET | $35.86 | $483.14 |
| 2026-09-29 16:00 ET | io | `SN` | 57 | $182.44 | 2026-10-02 16:00 ET | $182.57 | $3.00 |
| 2026-09-29 16:00 ET | io | `DT` | 182 | $57.53 | 2026-10-02 16:00 ET | $59.32 | $320.59 |
| 2026-10-01 16:00 ET | io | `IT` | 54 | $192.80 | 2026-10-06 16:00 ET | $184.92 | $-429.91 |
| 2026-10-01 16:00 ET | io | `KSPI` | 114 | $92.02 | 2026-10-06 16:00 ET | $93.16 | $125.19 |
| 2026-10-01 16:00 ET | io | `IOT` | 264 | $40.04 | 2026-10-06 16:00 ET | $41.74 | $441.85 |
| 2026-10-01 16:00 ET | io | `AVPT` | 750 | $14.08 | 2026-10-06 16:00 ET | $14.59 | $362.93 |
| 2026-10-01 16:00 ET | io | `SONO` | 595 | $17.75 | 2026-10-06 16:00 ET | $17.43 | $-205.94 |
| 2026-10-01 16:00 ET | io | `RELY` | 492 | $21.48 | 2026-10-06 16:00 ET | $23.20 | $833.37 |
| 2026-10-01 16:00 ET | io | `MQ` | 621 | $17.00 | 2026-10-06 16:00 ET | $16.55 | $-295.66 |
| 2026-10-01 16:00 ET | io | `BAND` | 169 | $62.44 | 2026-10-06 16:00 ET | $60.09 | $-402.26 |

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
