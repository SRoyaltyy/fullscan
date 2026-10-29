# Open-bell slippage / no-fill — research overlay

Open-bell fills at 09:30 ET: combo_sh_macd_5050_shared MARKET KILL · union_hot_n4_h1 MARKET KILL · flatten_would MARKET KILL. KILL combo_sh_macd_5050_shared / market: 335 filled fires but after-fee win 16.4% ≤ 55%; vs ideal-open $-208.21.

Research only. Live `flatten_robust`, hard-red sit, and Webull paper execution are **not** changed. Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30 pack / PR #239). This board does not wire MARKET or LIMIT into the live book.

## Plain board

The published cash books fill at the official 09:30 open. That is the **ideal / zero-slip** column. Two other realities use the same shopping lists and the same official OHLC store:

1. **MARKET-like** — a delayed market order walks into the post-open range. Buys slip toward the high; shorts slip toward the low. α comes from *prior* sessions only (default delay 35% of the adverse room). Same-day high/low are the realized path after the order was sent — not a 09:30 gate. Historically the adverse side of the open prints on a majority of sessions.
2. **LIMIT** — working order at the intended open, or at the prior close. Fill if the session trades through; miss if it does not. A miss is $0 after fees (no position). Daily OHLC has no natural partial, so a touch is a full fill.

Grade is **1 share, Futubull fees, open → same-day close**. After-fee P&L is always shown versus the ideal-open baseline. A fat ideal Book% that dies under slip is still a KILL for the realistic column. North star ~2%/day after fees is context — this sim is meant to stress fantasy paper fills that assumed the signal-day close.

Window `2026-08-13 → 2026-10-29` (55 sessions). KEEP bar: **≥30 filled fires** and **>55% after-fee hit rate**.

## KEEP / KILL vs ideal open

| Sleeve | Reality | Intended | Filled | Miss | After-fee win | After-fee $ | vs ideal $ | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_macd_5050_shared` | ideal | 335 | 335 | 0 | 29.0% | -257.35 | — | **KILL** |
| `combo_sh_macd_5050_shared` | market | 335 | 335 | 0 | 16.4% | -465.56 | $-208.21 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_open | 335 | 318 | 17 | 27.4% | -268.85 | $-11.50 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_prior | 335 | 293 | 42 | 29.3% | -187.59 | $+69.76 | **KILL** |
| `union_hot_n4_h1` | ideal | 220 | 220 | 0 | 32.7% | -67.50 | — | **KILL** |
| `union_hot_n4_h1` | market | 220 | 220 | 0 | 19.6% | -220.41 | $-152.91 | **KILL** |
| `union_hot_n4_h1` | limit_open | 220 | 208 | 12 | 30.8% | -79.30 | $-11.80 | **KILL** |
| `union_hot_n4_h1` | limit_prior | 220 | 194 | 26 | 32.5% | -54.72 | $+12.78 | **KILL** |
| `flatten_would` | ideal | 314 | 311 | 3 | 18.6% | -433.92 | — | **KILL** |
| `flatten_would` | market | 314 | 311 | 3 | 8.0% | -598.08 | $-164.16 | **KILL** |
| `flatten_would` | limit_open | 314 | 294 | 20 | 16.3% | -445.43 | $-11.51 | **KILL** |
| `flatten_would` | limit_prior | 314 | 254 | 60 | 16.1% | -332.10 | $+101.82 | **KILL** |

### Why, in plain language

- KILL combo_sh_macd_5050_shared / market: 335 filled fires but after-fee win 16.4% ≤ 55%; vs ideal-open $-208.21.
- KILL combo_sh_macd_5050_shared / limit_open: 318 filled fires but after-fee win 27.4% ≤ 55%; vs ideal-open $-11.50.
- KILL combo_sh_macd_5050_shared / limit_prior: 293 filled fires but after-fee win 29.3% ≤ 55%; vs ideal-open $+69.76.
- KILL union_hot_n4_h1 / market: 220 filled fires but after-fee win 19.6% ≤ 55%; vs ideal-open $-152.91.
- KILL union_hot_n4_h1 / limit_open: 208 filled fires but after-fee win 30.8% ≤ 55%; vs ideal-open $-11.80.
- KILL union_hot_n4_h1 / limit_prior: 194 filled fires but after-fee win 32.5% ≤ 55%; vs ideal-open $+12.78.
- KILL flatten_would / market: 311 filled fires but after-fee win 8.0% ≤ 55%; vs ideal-open $-164.16.
- KILL flatten_would / limit_open: 294 filled fires but after-fee win 16.3% ≤ 55%; vs ideal-open $-11.51.
- KILL flatten_would / limit_prior: 254 filled fires but after-fee win 16.1% ≤ 55%; vs ideal-open $+101.82.

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

