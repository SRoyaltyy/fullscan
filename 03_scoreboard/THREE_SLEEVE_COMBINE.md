# Three-sleeve combine — Excel · mover · .io

_Generated 2026-09-04T05:04:21-04:00 — window 2026-08-13 → 2026-09-03_

The three live books are complementary. They should not vote as equals. **Excel finds, mover times, .io stays long when the tape is only mildly ugly.**

The equity-curve stitch below is a *sketch*. The integrity path is `src/sleeve_combine_bt.py` / [`SLEEVE_COMBINE_BT.md`](SLEEVE_COMBINE_BT.md): matched hold, Futubull fees, 09:30-vs-16:00 cash clock, missing 8/13–8/14 mover calls logged as gaps. **Switching** one shared account by S lost to both solos. The combine that keeps .io's down-day book is **dual wallets** (mover gated, .io size always on, same hold). Do not pair mover 1d with live .io 2w. Copying green-pile / join-good onto mover names also fails on down days.

## Route (the combine)

| Morning general score S | Primary | Excel |
|---|---|---|
| S ≥ **+1.0** | **Mover** — 09:30 open, 1d hold, top 10 by cond | confirm (size-up if L3/L1 cluster) |
| **-3.0** ≤ S < **+1.0** | **.io dashboard** — close fill, prefer `2w_size, 1d_size, 3d_size` | confirm |
| S < **-3.0** | **No new 1d risk** — hold .io size sleeves; no mover / no fresh 1d fills | shorts only (S1/S2, unfunded) |

**Today (2026-09-03):** score -0.9 → **io** — predict -0.90 in [-3.0, +1.0) — .io dashboard keeps buying on flat/down days

## What each sleeve is actually good at

### Excel — vast swaths, not a book

Scans **3,603** grids (Yahoo OHLCV cluster colors, zero tokens). Live ledger: **1875** suggestions, win 41.0% vs entry open. That win rate is the tell: the engine is a *searchlight*, not a portfolio.

| strategy | n | mean vs open | win |
|---|---:|---:|---:|
| `L1_long_green_tp8_lowvol` | 526 | -0.7% | 41.4% |
| `L2_long_green_tp3_lowvol` | 526 | -0.7% | 41.4% |
| `L3_long_green_hold2_midcap` | 674 | -0.1% | 44.1% |
| `L5_long_green_hold2_midhibeta` | 101 | -5.3% | 34.7% |

Backtest (confirmation-close, 2022–2026) is a different story — tens of thousands of trades, holdout t ≥ 2 on every card, L3 midcap hold-2 and S1 1-day shorts are the durable ones. Live tracking is underwater because L1/L2 cap winners and leave losers open, and because the median trade is ~0: **you have to take every signal or the tail math breaks.** That is the opposite of how mover and .io size.

Use Excel to *see* names the other two never look at, and as a same-day confirm. Same-day Excel ∩ 1d book: **1** of 13 book days. All-three tickers in the window: `none`.

### Mover paper — highest hit-rate, losses almost deleted

| Start | Final | Return | Max DD | Trades | Win |
|---:|---:|---:|---:|---:|---:|
| $100,000 | $109,259.07 | **+9.3%** | -0.1% | 29 | **62.1%** |

Gross won $12,542 vs lost $3,283. The day gate (S ≥ +1.0) is the whole product: it closed **8** sessions and blocked **2** days whose ungated top-10 BUY basket was negative.

| Date | Score | Gate | Ungated top-10 1d |
|---|---:|---|---:|
| 2026-08-13 | 8.525 | OPEN | — |
| 2026-08-14 | 5.5 | OPEN | — |
| 2026-08-17 | 2.25 | OPEN | +4.1% |
| 2026-08-18 | -6.2 | **CLOSED** | +3.0% |
| 2026-08-19 | -7.2 | **CLOSED** | +0.1% |
| 2026-08-20 | 1.125 | OPEN | +3.2% |
| 2026-08-21 | 3.25 | OPEN | -0.1% |
| 2026-08-24 | -5.175 | **CLOSED** | +0.8% |
| 2026-08-25 | 1.8 | OPEN | — |
| 2026-08-26 | 2.025 | OPEN | — |
| 2026-08-27 | — | OPEN | +1.0% |
| 2026-08-28 | 0.75 | **CLOSED** | -3.5% |
| 2026-08-31 | -5.85 | **CLOSED** | -0.1% |
| 2026-09-01 | -6.3 | **CLOSED** | +0.4% |
| 2026-09-02 | -3.825 | **CLOSED** | +0.7% |
| 2026-09-03 | -0.9 | **CLOSED** | — |

Mover's weakness is the same as its strength: it is *off* most days. Over this window that is correct. A combine that only ran mover would sit in cash through the mild-down sessions .io is built to buy.

### .io dashboard — buys (and often wins) on down days

This is `src.paper_trade` following the stock book onto [the Pages dashboard](https://sroyaltyy.github.io/fullscan/dashboard/). No S ≥ +1 gate. Follow-the-book, close fill, size-bucket sleeves beat top-N. The user's rule of thumb — .io can keep *winning* on down days — is right for the longer size sleeves. The S < −3 caveat is a **new-buy** rule, not a flatten rule: `2w_size` stayed green through every hard-red session in this window because it was already on. `1d_top` / `1d_size` (new close fills) are the ones that wobble.

| Cut | n | 2w_size mean | vs SPY | win |
|---|---:|---:|---:|---:|
| all .io sessions | 12 | +1.1% | +1.1% | 75.0% |
| SPY down days | 7 | +0.2% | +0.7% | 71.4% |
| SPY up days | 5 | +2.2% | +1.7% | 80.0% |
| S ≥ +1 (mover's days) | 4 | +1.0% | +1.3% | 75.0% |
| -3.0 ≤ S < +1 | 1 | +2.0% | +0.9% | 100.0% |
| S < -3.0 (hard red) | 5 | +1.9% | +2.1% | 100.0% |
| 1d_size on SPY down | 7 | +0.0% | +0.5% | 42.9% |
| 1d_top on SPY down | 7 | -0.0% | +0.5% | 57.1% |

Per session (2w_size vs SPY):

| Date | Score | Route | SPY | 2w_size | 1d_size | 1d_top |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-14 | 5.5 | mover | -0.2% | +3.2% | +2.7% | +1.6% |
| 2026-08-17 | 2.25 | mover | -0.5% | +0.8% | -0.1% | +0.6% |
| 2026-08-18 | -6.2 | cash | -0.7% | +2.0% | -1.0% | +0.2% |
| 2026-08-19 | -7.2 | cash | +0.2% | +4.1% | +5.8% | +0.5% |
| 2026-08-20 | 1.125 | mover | -0.8% | -3.8% | +0.6% | -1.2% |
| 2026-08-21 | 3.25 | mover | +0.4% | +3.9% | +1.9% | +0.6% |
| 2026-08-27 | — | io | +0.7% | -0.6% | -1.4% | -0.6% |
| 2026-08-30 | — | io | -0.2% | -2.2% | -2.7% | -1.3% |
| 2026-08-31 | -5.85 | cash | -0.3% | +0.2% | -0.6% | -0.2% |
| 2026-09-01 | -6.3 | cash | -0.7% | +1.3% | +1.2% | +0.3% |
| 2026-09-02 | -3.825 | cash | +0.4% | +1.9% | +0.1% | +0.1% |
| 2026-09-03 | -0.9 | io | +1.0% | +2.0% | -0.6% | -1.0% |

## Stitched books (same window, existing curves)

This is not a new fill engine. It replays the *already realized* daily returns of mover paper and .io `2w_size`, then either routes by S or holds a fixed 40/40/20 (mover / .io / cash) split. First day of each series is a flat 0.

| Book | Return | Max DD |
|---|---:|---:|
| Mover alone | +8.9% | -0.1% |
| .io 2w_size alone | +13.2% | -3.8% |
| Router flatten (S < −3 → cash) | +6.1% | -2.8% |
| **Hold-through** (S ≥ +1 mover, else .io; no flatten) | **+16.6%** | -2.8% |
| Split 40/40/20 | +8.8% | -0.9% |

Flattening on S < −3 *hurt* this window: .io `2w_size` was the best single book (+13%) and it made that number on the hard-red days the flatten rule would skip. **Hold-through is the combine that matches the evidence** — mover on green-light mornings, .io the rest of the time, no forced cash-out. The 40/40/20 split is the defensive alternative (smaller DD, gives up some .io upside). Do not flatten a working size sleeve just because the morning stamp went hard-red.

## How to combine in production

1. **Do not average the three pick lists.** Excel dumps 30–50 names a day; mover wants 10; .io fills 10. Averaging re-imports Excel's median-zero / tail-or-nothing payoff.
2. **Excel = universe + confirm.** Overnight: scan. At 09:30: if the route is mover, size-up any top-N name that also confirmed L3 (midcap hold-2) or L1/L2 that session. At the close: if the route is .io, same confirm on the size-sleeve fills.
3. **Prefer .io size sleeves over 1d_top.** `2w_size` is the down-day engine (7 SPY-down sessions, +0.2% mean, 71% win, +0.7% vs SPY). 1d_top is the dashboard headline and the weakest long sleeve.
4. **Keep mover's +1 gate for mover fills.** Do not loosen it just because .io can buy below +1. That gate is why max DD is tiny. Below +1, do **not** switch the mover account onto the afternoon book — give .io its own cash and leave it on.
5. **Hard-red (S < −3) = no new *mover* risk, not flatten .io.** Dual keeps buying the size book on red mornings (1d size was *better* on S < +1 than on green in this window). A shared-account "cash" route is what made the switch lose. Do not stand up an Excel short book until S1/S2 have a fee-aware paper sleeve (borrow is ignored today).
6. **Intersection is a bonus, not a requirement.** Mover ∩ book tickers this window: `ACMR`, `AUPH`, `CRSP`, `MU`. Waiting for all three to agree starves the book.

## Caveats

- Window is ~16 sessions. Router vs split ranking can flip.
- Mover ungated day P&L uses lookback close→next-close, not the paper open→next-close fill.
- .io curve skips sessions with no stock-book file (2026-08-24..26).
- Excel live marks are vs entry open and many names are still open (tp strategies never sold). Do not compare that mean to paper P&L.
- Missing predict: solo mover allows the day; the *combine* parks in .io instead. A blank tape is not a +1 green light.

Backtest every session + buy/sell dashboard:

```
python -m src.sleeve_combine_bt --mode dual --hold 1d
```

Page: [`dashboard/sleeve-combine/index.html`](../dashboard/sleeve-combine/index.html) → [live](https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/). Linked from the .io paper dashboard.

Code: `src/sleeve_combine.py`. Machine copy: `data/sleeve_combine/analysis.json`.
