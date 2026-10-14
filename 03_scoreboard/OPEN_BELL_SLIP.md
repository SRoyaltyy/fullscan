# Open-bell slippage / no-fill — research overlay

Open-bell fills at 09:30 ET: combo_sh_macd_5050_shared MARKET KILL · union_hot_n4_h1 MARKET KILL · flatten_would MARKET KILL. KILL combo_sh_macd_5050_shared / market: 264 filled fires but after-fee win 18.6% ≤ 55%; vs ideal-open $-164.50.

Research only. Live `flatten_robust`, hard-red sit, and Webull paper execution are **not** changed. Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30 pack / PR #239). This board does not wire MARKET or LIMIT into the live book.

## Plain board

The published cash books fill at the official 09:30 open. That is the **ideal / zero-slip** column. Two other realities use the same shopping lists and the same official OHLC store:

1. **MARKET-like** — a delayed market order walks into the post-open range. Buys slip toward the high; shorts slip toward the low. α comes from *prior* sessions only (default delay 35% of the adverse room). Same-day high/low are the realized path after the order was sent — not a 09:30 gate. Historically the adverse side of the open prints on a majority of sessions.
2. **LIMIT** — working order at the intended open, or at the prior close. Fill if the session trades through; miss if it does not. A miss is $0 after fees (no position). Daily OHLC has no natural partial, so a touch is a full fill.

Grade is **1 share, Futubull fees, open → same-day close**. After-fee P&L is always shown versus the ideal-open baseline. A fat ideal Book% that dies under slip is still a KILL for the realistic column. North star ~2%/day after fees is context — this sim is meant to stress fantasy paper fills that assumed the signal-day close.

Window `2026-08-13 → 2026-10-14` (44 sessions). KEEP bar: **≥30 filled fires** and **>55% after-fee hit rate**.

## KEEP / KILL vs ideal open

| Sleeve | Reality | Intended | Filled | Miss | After-fee win | After-fee $ | vs ideal $ | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_macd_5050_shared` | ideal | 264 | 264 | 0 | 31.4% | -164.69 | — | **KILL** |
| `combo_sh_macd_5050_shared` | market | 264 | 264 | 0 | 18.6% | -329.19 | $-164.50 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_open | 264 | 250 | 14 | 29.2% | -176.76 | $-12.07 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_prior | 264 | 234 | 30 | 31.6% | -122.78 | $+41.91 | **KILL** |
| `union_hot_n4_h1` | ideal | 176 | 176 | 0 | 34.7% | -44.39 | — | **KILL** |
| `union_hot_n4_h1` | market | 176 | 176 | 0 | 21.0% | -169.45 | $-125.06 | **KILL** |
| `union_hot_n4_h1` | limit_open | 176 | 166 | 10 | 31.9% | -56.65 | $-12.26 | **KILL** |
| `union_hot_n4_h1` | limit_prior | 176 | 157 | 19 | 34.4% | -35.50 | $+8.89 | **KILL** |
| `flatten_would` | ideal | 262 | 262 | 0 | 16.8% | -344.29 | — | **KILL** |
| `flatten_would` | market | 262 | 262 | 0 | 6.9% | -471.81 | $-127.52 | **KILL** |
| `flatten_would` | limit_open | 262 | 251 | 11 | 15.5% | -347.22 | $-2.93 | **KILL** |
| `flatten_would` | limit_prior | 262 | 212 | 50 | 14.1% | -257.90 | $+86.39 | **KILL** |

### Why, in plain language

- KILL combo_sh_macd_5050_shared / market: 264 filled fires but after-fee win 18.6% ≤ 55%; vs ideal-open $-164.50.
- KILL combo_sh_macd_5050_shared / limit_open: 250 filled fires but after-fee win 29.2% ≤ 55%; vs ideal-open $-12.07.
- KILL combo_sh_macd_5050_shared / limit_prior: 234 filled fires but after-fee win 31.6% ≤ 55%; vs ideal-open $+41.91.
- KILL union_hot_n4_h1 / market: 176 filled fires but after-fee win 21.0% ≤ 55%; vs ideal-open $-125.06.
- KILL union_hot_n4_h1 / limit_open: 166 filled fires but after-fee win 31.9% ≤ 55%; vs ideal-open $-12.26.
- KILL union_hot_n4_h1 / limit_prior: 157 filled fires but after-fee win 34.4% ≤ 55%; vs ideal-open $+8.89.
- KILL flatten_would / market: 262 filled fires but after-fee win 6.9% ≤ 55%; vs ideal-open $-127.52.
- KILL flatten_would / limit_open: 251 filled fires but after-fee win 15.5% ≤ 55%; vs ideal-open $-2.93.
- KILL flatten_would / limit_prior: 212 filled fires but after-fee win 14.1% ≤ 55%; vs ideal-open $+86.39.

## After-fee caveat

Every graded fill pays the Futubull US round-trip on 1 share (`paper_trade.order_fees` / `00_grounding/futubull_fees.json`). A MARKET fill that is only a few cents worse than the open can flip a tiny winner into a loser after fees. LIMIT misses are $0 — they do not get a free pass as 'no loss' in the hit rate (they are not fills). Ideal-open P&L is the zero-slip baseline, not a live number.

## Method (code names after the English)

- Clock: `09:30 ET` send. `ideal_fill` = official open. `market_fill` = open ± `DELAY_FRAC` (`0.35`) of the post-open adverse room, α from `calibrate_slip` on `ohlc_ripper.prior_bars` (date < session, n=20). `limit_at_open` requires a trade-through (`low < open` buy / `high > open` short). `limit_at_prior` uses prior close.
- Shopping lists: leak-free panel `pick_day` for `combo_sh_macd_5050_shared` (Webull paper combo), `union_hot_n4_h1`, and `flatten_h3` would-haves (`flatten_would`). Hard-red sit mornings still list names so the fill comparison is the card, not leftover cash. Live sit is unchanged.
- Prices: `data/prices/ohlc.parquet` via `load_bar_store` / `ticker_lookback.session_bar`. No yfinance. No new ripper.
- `align_sleeves` / `zip_sleeve_rows` zip unequal or empty sleeves without indexing — an empty flatten list is not an IndexError.

Live untouched: `flatten_robust`, `hard_red_sit`, `webull_paper`.

Regenerate: `PYTHONPATH=. python3 -m src.open_bell_slip --write`.

Dashboard overlay: [factor-mine open-bell slip](../dashboard/factor-mine/open-bell-slip.html) (GitHub Pages `.io` research section).

