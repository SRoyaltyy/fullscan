# Open-bell slippage / no-fill — research overlay

Open-bell fills at 09:30 ET: combo_sh_macd_5050_shared MARKET KILL · union_hot_n4_h1 MARKET KILL · flatten_would MARKET KILL. KILL combo_sh_macd_5050_shared / market: 222 filled fires but after-fee win 18.5% ≤ 55%; vs ideal-open $-114.40.

Research only. Live `flatten_robust`, hard-red sit, and Webull paper execution are **not** changed. Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30 pack / PR #239). This board does not wire MARKET or LIMIT into the live book.

## Plain board

The published cash books fill at the official 09:30 open. That is the **ideal / zero-slip** column. Two other realities use the same shopping lists and the same official OHLC store:

1. **MARKET-like** — a delayed market order walks into the post-open range. Buys slip toward the high; shorts slip toward the low. α comes from *prior* sessions only (default delay 35% of the adverse room). Same-day high/low are the realized path after the order was sent — not a 09:30 gate. Historically the adverse side of the open prints on a majority of sessions.
2. **LIMIT** — working order at the intended open, or at the prior close. Fill if the session trades through; miss if it does not. A miss is $0 after fees (no position). Daily OHLC has no natural partial, so a touch is a full fill.

Grade is **1 share, Futubull fees, open → same-day close**. After-fee P&L is always shown versus the ideal-open baseline. A fat ideal Book% that dies under slip is still a KILL for the realistic column. North star ~2%/day after fees is context — this sim is meant to stress fantasy paper fills that assumed the signal-day close.

Window `2026-08-13 → 2026-10-05` (37 sessions). KEEP bar: **≥30 filled fires** and **>55% after-fee hit rate**.

## KEEP / KILL vs ideal open

| Sleeve | Reality | Intended | Filled | Miss | After-fee win | After-fee $ | vs ideal $ | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_macd_5050_shared` | ideal | 222 | 222 | 0 | 33.3% | -158.81 | — | **KILL** |
| `combo_sh_macd_5050_shared` | market | 222 | 222 | 0 | 18.5% | -273.21 | $-114.40 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_open | 222 | 208 | 14 | 30.8% | -170.88 | $-12.07 | **KILL** |
| `combo_sh_macd_5050_shared` | limit_prior | 222 | 194 | 28 | 33.0% | -135.59 | $+23.22 | **KILL** |
| `union_hot_n4_h1` | ideal | 148 | 148 | 0 | 37.2% | -53.42 | — | **KILL** |
| `union_hot_n4_h1` | market | 148 | 148 | 0 | 21.6% | -138.72 | $-85.30 | **KILL** |
| `union_hot_n4_h1` | limit_open | 148 | 138 | 10 | 34.1% | -65.68 | $-12.26 | **KILL** |
| `union_hot_n4_h1` | limit_prior | 148 | 130 | 18 | 36.9% | -48.58 | $+4.84 | **KILL** |
| `flatten_would` | ideal | 229 | 229 | 0 | 18.3% | -301.83 | — | **KILL** |
| `flatten_would` | market | 229 | 229 | 0 | 7.9% | -415.38 | $-113.55 | **KILL** |
| `flatten_would` | limit_open | 229 | 222 | 7 | 17.6% | -305.77 | $-3.94 | **KILL** |
| `flatten_would` | limit_prior | 229 | 183 | 46 | 15.3% | -225.41 | $+76.42 | **KILL** |

### Why, in plain language

- KILL combo_sh_macd_5050_shared / market: 222 filled fires but after-fee win 18.5% ≤ 55%; vs ideal-open $-114.40.
- KILL combo_sh_macd_5050_shared / limit_open: 208 filled fires but after-fee win 30.8% ≤ 55%; vs ideal-open $-12.07.
- KILL combo_sh_macd_5050_shared / limit_prior: 194 filled fires but after-fee win 33.0% ≤ 55%; vs ideal-open $+23.22.
- KILL union_hot_n4_h1 / market: 148 filled fires but after-fee win 21.6% ≤ 55%; vs ideal-open $-85.30.
- KILL union_hot_n4_h1 / limit_open: 138 filled fires but after-fee win 34.1% ≤ 55%; vs ideal-open $-12.26.
- KILL union_hot_n4_h1 / limit_prior: 130 filled fires but after-fee win 36.9% ≤ 55%; vs ideal-open $+4.84.
- KILL flatten_would / market: 229 filled fires but after-fee win 7.9% ≤ 55%; vs ideal-open $-113.55.
- KILL flatten_would / limit_open: 222 filled fires but after-fee win 17.6% ≤ 55%; vs ideal-open $-3.94.
- KILL flatten_would / limit_prior: 183 filled fires but after-fee win 15.3% ≤ 55%; vs ideal-open $+76.42.

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

