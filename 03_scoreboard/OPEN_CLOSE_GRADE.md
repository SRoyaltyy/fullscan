# Open vs close entry grading

_Generated 2026-09-12T03:16:13.135763-04:00 · research only · no live wire._

Same pick sets, three (plus one) clocks, Futubull fees. Official 09:30 / 16:00 from `data/prices/ohlc.parquet` — not yfinance, not a Finviz last-trade.

## Clocks

| id | What | Who already uses it |
|----|------|---------------------|
| `c2c` | close → next close | `STOCK_BOOK_BACKTEST` / paper book |
| `o2nc` | 09:30 open → next close | open fill, 1-session hop |
| `o2c` | 09:30 open → same-day close | hot-4 hold=1 **signal** clock |
| `o2o` | 09:30 open → next 09:30 open | hot-4 **cash-book** fill |

After-fee columns: **15 bp** round-trip (`excel_bot` `FEE_RT`) and the real `paper_trade.order_fees` helper on a $2,500 leftover slice.

## Plain board — does open-entry flip hot-4 vs book?

**NO — open-entry does not flip who wins on hit-rate. c2c winner=book (46.5% vs 43.8%); o2c winner=book (58.9% vs 53.6%). Book hit-rate does cross 50% (46.5% c2c → 58.9% o2c), but that is the afternoon-book leak, not a rescue of the 09:30 sleeve. Mean ranking does not flip (c2c hot4 wins, book -0.03% vs hot-4 +0.93%; o2c hot4 wins, book +0.51% vs hot-4 +1.74%).**

| Clock | book 1d top-10 | book 1d all BUY | hot-4 `union_hot_n4_h1` | winner (top-10 vs hot-4) |
|-------|----------------|-----------------|-------------------------|--------------------------|
| close → next close | 59/127 (46.5%) μ -0.03% | 109/235 (46.4%) μ -0.07% | 35/80 (43.8%) μ +0.93% | book (-2.7 pp hot-4) |
| 09:30 open → next close | 70/127 (55.1%) μ +0.55% | 121/235 (51.5%) μ +0.29% | 41/80 (51.2%) μ +2.65% | book (-3.9 pp hot-4) |
| 09:30 open → same-day close | 76/129 (58.9%) μ +0.51% | 129/237 (54.4%) μ +0.32% | 45/84 (53.6%) μ +1.74% | book (-5.3 pp hot-4) |
| 09:30 open → next 09:30 open | 78/127 (61.4%) μ +0.68% | 130/235 (55.3%) μ +0.35% | 45/80 (56.2%) μ +2.37% | book (-5.2 pp hot-4) |

### After 15 bp Futubull (name-day)

| Clock | book top-10 | hot-4 |
|-------|-------------|-------|
| close → next close | 58/127 (45.7%) μ -0.18% | 33/80 (41.2%) μ +0.78% |
| 09:30 open → next close | 68/127 (53.5%) μ +0.40% | 40/80 (50.0%) μ +2.50% |
| 09:30 open → same-day close | 70/129 (54.3%) μ +0.36% | 44/84 (52.4%) μ +1.59% |
| 09:30 open → next 09:30 open | 75/127 (59.1%) μ +0.53% | 45/80 (56.2%) μ +2.22% |

### After `order_fees` on a $2,500 slice

| Clock | book top-10 | hot-4 |
|-------|-------------|-------|
| close → next close | 57/127 (44.9%) μ -0.24% | 32/80 (40.0%) μ +0.34% |
| 09:30 open → next close | 68/127 (53.5%) μ +0.34% | 39/80 (48.8%) μ +2.04% |
| 09:30 open → same-day close | 69/129 (53.5%) μ +0.30% | 44/84 (52.4%) μ +1.11% |
| 09:30 open → next 09:30 open | 74/127 (58.3%) μ +0.47% | 42/80 (52.5%) μ +1.76% |

Books graded: **17** · hot-4 sessions: **21** · calendar: 2026-08-13 → 2026-09-11 (21 sessions).

Morning-landed books only (knowable before 09:30 — the only book slice that is allowed an open fill): `o2c` 12/23 (52.2%) μ +0.03% · `c2c` 7/21 (33.3%) μ -0.76% (4 days, small n).

Book land times (when the JSON was printed vs that date's 09:30 / 16:00): **4** preopen · **9** during the session · **4** after close / next morning.

## 9/2 CVS case

Book printed **2026-09-02T15:43:57.796492-04:00** · land=`session` · general bias **-0.44 (down)** · CVS was #1 1d BUY on a HARD_RED lattice.

Top 10 1d BUY names:

- close→next close (2026-09-03): **1/10** green (10.0%)
- open→same close (09:30→16:00 on 2026-09-02): **8/10** green (80.0%)
- already green on the printed sheet (`change_pct` > 0): **10** / 10

Same-day open→close is **not** a fill this book had. The file landed at 15:43 ET with Change% already on the row (CVS +3.93% / gap +1.15%). Every top-10 name was already green on the sheet. CVS itself was red open→close (-0.60%) — the 8/10 is the basket, not the #1 name.
close→next close is the 1d hop after that print (1/10 green).

| # | Ticker | o2c | c2c | change% at print | gap% |
|---|--------|-----|-----|------------------|------|
| 1 | `CVS` ← CVS | -0.60% | -0.03% | +3.93% | +1.15% |
| 2 | `CVE` | +0.06% | -0.03% | +3.05% | +1.43% |
| 3 | `CNQ` | +0.76% | -1.29% | +3.48% | +1.39% |
| 4 | `COR` | +1.74% | +0.26% | +2.28% | +0.73% |
| 5 | `BG` | +2.21% | -2.23% | +4.16% | +1.53% |
| 6 | `PBF` | -0.54% | -0.20% | +2.74% | +2.75% |
| 7 | `ADM` | +0.86% | -1.17% | +4.01% | +1.98% |
| 8 | `OXY` | +0.78% | -0.49% | +1.28% | +1.53% |
| 9 | `EOG` | +1.07% | -2.01% | +2.34% | +1.83% |
| 10 | `CVX` | +0.61% | -0.22% | +2.38% | +1.39% |

## Scoreboard default (research recommendation)

- **Stock-book 1d BUY → `c2c`.** Printed books land at mixed clocks (4 preopen / 9 session / 4 after close). Most 1d BUY lists are afternoon or overnight prints, so the honest 1d hop is close→next close (or next 09:30 if you refuse a same-day last). Same-day open→close is not a fill the afternoon book had — it grades a move already on the sheet.
- **Hot-4 / `union_hot_n4_h1` → `o2c`** (cash blotter `o2o`). union_hot_n4_h1 is a 09:30 list: hold=1 signal-only grades open→same close; the cash book buys the open and sells the next open. close→next close bills it for an overnight gap it never paid and is the wrong scoreboard default.
- Do not put both sleeves on one c2c board and call that a fair hot-4 vs book bake-off.

Live `flatten_robust` is not changed. This file is a clock audit, not a wire.

## Method

- Picks are frozen: book = printed `books.1d.buy`; hot-4 = panel rows that pass `union_hot_n4_h1` (union, no 🚨, top 4 by `ohlc_hot_score`).
- Hot-4 **list** still prints on hard-red sit mornings (cash book buys nobody). Those names are graded so the clock comparison is the shopping list, not leftover cash.
- Missing open/close or a missing next session drops that name-day for that clock.
- Hit = return > 0. After-fee hit = return − drag > 0.
- No network. Labor Day 2026-09-07 is not a session.
