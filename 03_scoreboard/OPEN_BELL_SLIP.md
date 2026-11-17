# Open-bell slippage / no-fill — research overlay

Open-bell fills at 09:30 ET: combo_sh_macd_5050_shared MARKET KILL · union_hot_n4_h1 MARKET KILL · flatten_would MARKET KILL. KILL combo_sh_macd_5050_shared / market: 436 filled fires but after-fee win 15.6% ≤ 55%; vs ideal-open $-252.98.

Research only. Live `flatten_robust`, hard-red sit, and Webull paper execution are **not** changed. Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30 pack / PR #239). This board does not wire MARKET or LIMIT into the live book.

## Plain board

The published cash books fill at the official 09:30 open. That is the **ideal / zero-slip** column. Two other realities use the same shopping lists and the same official OHLC store:

1. **MARKET-like** — a delayed market order walks into the post-open range. Buys slip toward the high; shorts slip toward the low. α comes from *prior* sessions only (default delay 35% of the adverse room). Same-day high/low are the realized path after the order was sent — not a 09:30 gate. Historically the adverse side of the open prints on a majority of sessions.
2. **LIMIT** — working order at the intended open, or at the prior close. Fill if the session trades through; miss if it does not. A miss is $0 after fees (no position). Daily OHLC has no natural partial, so a touch is a full fill.

Grade is **1 share, Futubull fees, open → same-day close**. After-fee P&L is always shown versus the ideal-open baseline. A fat ideal Book% that dies under slip is still a KILL for the realistic column. North star ~2%/day after fees is context — this sim is meant to stress fantasy paper fills that assumed the signal-day close.

Window `2026-08-13 → 2026-11-16` (67 sessions). KEEP bar: **≥30 filled fires** and **>55% after-fee hit rate**.

## KEEP / KILL vs ideal open

| Sleeve | Reality | Intended | Filled | Miss | After-fee win | After-fee $ | vs ideal $ | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_macd_5050_shared` | ideal | 436 | 436 | 0 | 29.6% | -306.82 | — | **KILL** |
| `combo_sh_macd_5050_shared` | market | 436 | 436 | 0 | 15.6% | -559.80 | $-252.98 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_open | 436 | 414 | 22 | 28.3% | -317.42 | $-10.60 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_prior | 436 | 381 | 55 | 31.0% | -217.34 | $+89.48 | **KILL** |
| `union_hot_n4_h1` | ideal | 268 | 268 | 0 | 32.8% | -57.80 | — | **KILL** |
| `union_hot_n4_h1` | market | 268 | 268 | 0 | 17.5% | -243.62 | $-185.82 | **KILL** |
| `union_hot_n4_h1` | limit_open | 268 | 253 | 15 | 31.2% | -68.89 | $-11.09 | **KILL** |
| `union_hot_n4_h1` | limit_prior | 268 | 239 | 29 | 32.2% | -39.27 | $+18.53 | **KILL** |
| `flatten_would` | ideal | 381 | 377 | 4 | 20.4% | -520.30 | — | **KILL** |
| `flatten_would` | market | 381 | 377 | 4 | 9.3% | -744.62 | $-224.32 | **KILL** |
| `flatten_would` | limit_open | 381 | 359 | 22 | 18.4% | -534.38 | $-14.08 | **KILL** |
| `flatten_would` | limit_prior | 381 | 315 | 66 | 17.8% | -402.74 | $+117.56 | **KILL** |

### Why, in plain language

- KILL combo_sh_macd_5050_shared / market: 436 filled fires but after-fee win 15.6% ≤ 55%; vs ideal-open $-252.98.
- KILL combo_sh_macd_5050_shared / limit_open: 414 filled fires but after-fee win 28.3% ≤ 55%; vs ideal-open $-10.60.
- KILL combo_sh_macd_5050_shared / limit_prior: 381 filled fires but after-fee win 31.0% ≤ 55%; vs ideal-open $+89.48.
- KILL union_hot_n4_h1 / market: 268 filled fires but after-fee win 17.5% ≤ 55%; vs ideal-open $-185.82.
- KILL union_hot_n4_h1 / limit_open: 253 filled fires but after-fee win 31.2% ≤ 55%; vs ideal-open $-11.09.
- KILL union_hot_n4_h1 / limit_prior: 239 filled fires but after-fee win 32.2% ≤ 55%; vs ideal-open $+18.53.
- KILL flatten_would / market: 377 filled fires but after-fee win 9.3% ≤ 55%; vs ideal-open $-224.32.
- KILL flatten_would / limit_open: 359 filled fires but after-fee win 18.4% ≤ 55%; vs ideal-open $-14.08.
- KILL flatten_would / limit_prior: 315 filled fires but after-fee win 17.8% ≤ 55%; vs ideal-open $+117.56.

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

