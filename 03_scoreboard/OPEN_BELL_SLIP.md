# Open-bell slippage / no-fill — research overlay

Open-bell fills at 09:30 ET: combo_sh_macd_5050_shared MARKET KILL · union_hot_n4_h1 MARKET KILL · flatten_would MARKET KILL. KILL combo_sh_macd_5050_shared / market: 131 filled fires but after-fee win 13.7% ≤ 55%; vs ideal-open $-54.30.

Research only. Live `flatten_robust`, hard-red sit, and Webull paper execution are **not** changed. Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30 pack / PR #239). This board does not wire MARKET or LIMIT into the live book.

## Plain board

The published cash books fill at the official 09:30 open. That is the **ideal / zero-slip** column. Two other realities use the same shopping lists and the same official OHLC store:

1. **MARKET-like** — a delayed market order walks into the post-open range. Buys slip toward the high; shorts slip toward the low. α comes from *prior* sessions only (default delay 35% of the adverse room). Same-day high/low are the realized path after the order was sent — not a 09:30 gate. Historically the adverse side of the open prints on a majority of sessions.
2. **LIMIT** — working order at the intended open, or at the prior close. Fill if the session trades through; miss if it does not. A miss is $0 after fees (no position). Daily OHLC has no natural partial, so a touch is a full fill.

Grade is **1 share, Futubull fees, open → same-day close**. After-fee P&L is always shown versus the ideal-open baseline. A fat ideal Book% that dies under slip is still a KILL for the realistic column. North star ~2%/day after fees is context — this sim is meant to stress fantasy paper fills that assumed the signal-day close.

Window `2026-08-13 → 2026-09-18` (26 sessions). KEEP bar: **≥30 filled fires** and **>55% after-fee hit rate**.

## KEEP / KILL vs ideal open

| Sleeve | Reality | Intended | Filled | Miss | After-fee win | After-fee $ | vs ideal $ | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_macd_5050_shared` | ideal | 152 | 131 | 21 | 29.8% | -77.36 | — | **KILL** |
| `combo_sh_macd_5050_shared` | market | 152 | 131 | 21 | 13.7% | -131.66 | $-54.30 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_open | 152 | 121 | 31 | 25.6% | -83.97 | $-6.61 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_prior | 152 | 113 | 39 | 30.1% | -63.06 | $+14.30 | **KILL** |
| `union_hot_n4_h1` | ideal | 104 | 84 | 20 | 32.1% | -27.15 | — | **KILL** |
| `union_hot_n4_h1` | market | 104 | 84 | 20 | 15.5% | -66.83 | $-39.68 | **KILL** |
| `union_hot_n4_h1` | limit_open | 104 | 78 | 26 | 26.9% | -33.94 | $-6.79 | **KILL** |
| `union_hot_n4_h1` | limit_prior | 104 | 74 | 30 | 32.4% | -25.20 | $+1.95 | **KILL** |
| `flatten_would` | ideal | 156 | 134 | 22 | 23.1% | -77.53 | — | **KILL** |
| `flatten_would` | market | 156 | 134 | 22 | 11.2% | -142.15 | $-64.62 | **KILL** |
| `flatten_would` | limit_open | 156 | 128 | 28 | 22.7% | -81.36 | $-3.83 | **KILL** |
| `flatten_would` | limit_prior | 156 | 102 | 54 | 22.6% | -44.33 | $+33.20 | **KILL** |

### Why, in plain language

- KILL combo_sh_macd_5050_shared / market: 131 filled fires but after-fee win 13.7% ≤ 55%; vs ideal-open $-54.30.
- KILL combo_sh_macd_5050_shared / limit_open: 121 filled fires but after-fee win 25.6% ≤ 55%; vs ideal-open $-6.61.
- KILL combo_sh_macd_5050_shared / limit_prior: 113 filled fires but after-fee win 30.1% ≤ 55%; vs ideal-open $+14.30.
- KILL union_hot_n4_h1 / market: 84 filled fires but after-fee win 15.5% ≤ 55%; vs ideal-open $-39.68.
- KILL union_hot_n4_h1 / limit_open: 78 filled fires but after-fee win 26.9% ≤ 55%; vs ideal-open $-6.79.
- KILL union_hot_n4_h1 / limit_prior: 74 filled fires but after-fee win 32.4% ≤ 55%; vs ideal-open $+1.95.
- KILL flatten_would / market: 134 filled fires but after-fee win 11.2% ≤ 55%; vs ideal-open $-64.62.
- KILL flatten_would / limit_open: 128 filled fires but after-fee win 22.7% ≤ 55%; vs ideal-open $-3.83.
- KILL flatten_would / limit_prior: 102 filled fires but after-fee win 22.6% ≤ 55%; vs ideal-open $+33.20.

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

