# Extreme RelVol / price names vs morning picks

_Research only · live `flatten_robust` untouched · no rubric-weight change._

Window: after-close **2026-08-13 → 2026-09-11**. An extreme is a Finviz print on session **T** with RelVol ≥ 3, |day move| ≥ 8%, and Perf Week ≥ +40%. Picks are the **T+1 09:30** book / flatten wish-list / factor-mine panel already sitting in this repo.

Finviz exports live in `data/exports/`. Theme-radar has no extra after-close tape for this cut. Weekend / Labor Day copies (08-29, 08-30, 09-05..07) are ignored; only the 21 closed sessions in `factor_mine` panel are T dates, and T=09-11 has no T+1 in-window.

## Plain English

The live dashboards **almost never select** these names, and most days they are **not allowed onto the BUY walk** even when they print in the morning book.

- **257 extreme name-days** across 20 T sessions (179 tickers). **97 are leveraged/theme ETFs** — the raw Finviz screen is 38% products, not single names. Common-stock extremes: **160**.
- Only **39/257 (15.2%)** clear the gainer-tape liquidity floor (mcap ≥ $100M and ADV ≥ 500k, not an ETF).
- Only **17/257 (6.6%)** clear the live BUY mcap floor ($400M, not micro).
- Morning book had the ticker on **45/257 (17.5%)** name-days (CSV universe). Missing T+1 book files: 08-24, 08-25, 08-26, 08-28 (those mornings cannot prove BUY-walk eligibility).
- **Green-pile eligible: 1/257 (0.4%)** (MRVI 08-21: cores passed, pile not yet the live ranker). On a used pile: **1** (RNXT 09-04: green, but $107M micro so BUY still skips).
- Factor-mine 09:30 panel (union of flatten / probable / yday_gainer / ohlc_hot / earn / mover): **32/257 (12.5%)**.
- Yesterday-gainer universe (liquid top-25, mcap ≥ $100M): **28/257 (10.9%)**.
- `ohlc_hot` / `probable` (these **drop exploded tape**): 1 / 1.
- **Chosen next morning: 1/257 (0.4%)** (1d BUY 0, 3d BUY 0, flatten would-buy 1).

So: the machines *can* see a few liquid rips on the yday-gainer list, but the live 3d size book, green pile, coil/probable recipes, and flatten gate are built to **keep exploded names off the ticket**.

## Do the gates allow them?

| Gate | What it does to extremes | This window |
|---|---|---|
| Liquidity BUY walk (`mcap < $400M` or micro) | Hard skip on BUY | 240 of 257 fail |
| Dead RelVol printed in (0, 0.7) | BUY veto + green-pile veto | 40 name-days |
| Green pile (all-green join/AB/peer/gen, sector/news not red) | Need 8 liquid greens or fallback weighted | eligible 1; pile used on 5/20 T+1 mornings |
| Decision lattice `bull_eligible=false` | BUY veto | 10 |
| HARD_RED (`S ≤ −3`) | Live flatten sits; wish-list still prints | 8 T+1 mornings |
| Flatten gate (`flatten_ok`) | Live 09:30 tickets only on GO | 2 GO mornings |
| `too_extended` (ret_5 > 18 or rvol > 2.8) | Dropped from ohlc_hot | 31 of those we could score |
| Probable / coil (`ret_5 ≤ 10`, rvol ≤ 2.2) | Exploded week names fail | probable list hits 1; coil recipe picks 1 |
| Rank / sector / industry / large-mega caps | 25 seats, 4/sector, 3/industry, 4 large-mega | see first-blocker table |

### First blocker (one per name-day)

| First blocker | n |
|---|---:|
| `not_in_morning_book` | 122 |
| `etf` | 97 |
| `illiquid_mcap_lt_400_or_micro` | 27 |
| `too_extended_ret5_or_rvol` | 8 |
| `sector_cap` | 2 |
| `chosen` | 1 |

Common stock only (ETFs stripped):

| First blocker | n |
|---|---:|
| `not_in_morning_book` | 122 |
| `illiquid_mcap_lt_400_or_micro` | 27 |
| `too_extended_ret5_or_rvol` | 8 |
| `sector_cap` | 2 |
| `chosen` | 1 |

A name can fail several gates at once. The first-blocker column is the earliest hard stop in the live BUY / select path. `flatten_robust` would-buy is **not** the $400M BUY walk — ARCT ($319M) still printed on the 08-21 wish-list as a mover / yday-gainer.

## Do the selectors choose them?

| Selector (T+1 morning) | Extreme name-days chosen |
|---|---|
| Live 1d BUY | 0/257 (0.0%) |
| Live 3d BUY (flatten_robust size book) | 0/257 (0.0%) |
| flatten_robust would-buy / wish-list | 1/257 (0.4%) |
| Any of the three above | 1/257 (0.4%) |
| Factor-mine panel (any 09:30 list) | 32/257 (12.5%) |
| `yday_gainer` universe | 28/257 (10.9%) |
| `ohlc_hot` universe | 1/257 (0.4%) |
| `probable` universe | 1/257 (0.4%) |
| `flatten` universe | 1/257 (0.4%) |
| Recipe `union_h1` top 8 | 2/257 (0.8%) |
| Recipe `yday_gainer_h1` top 8 | 6/257 (2.3%) |
| Recipe `flatten_h1` | 1/257 (0.4%) |
| Recipe `flatten_live_h1` (gate must fire) | 1/257 (0.4%) |
| Recipe `ohlc_hot_h1` | 1/257 (0.4%) |
| Recipe `probable_h1` | 1/257 (0.4%) |
| Recipe `coil_off` | 1/257 (0.4%) |
| Recipe `union_vol_green_h1` | 10/257 (3.9%) |

### Names that *were* chosen

| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | Where | T+1 H after fee |
|---|---|---|---:|---:|---:|---:|---|---:|
| 2026-08-20 | 2026-08-21 | **ARCT** | 42.7% | +9.1% | 4.53 | 319 | flatten | +19.24 |

ARCT is the only live hit: T=08-20 +9% / RelVol 4.5 / week +43%, T+1 flatten wish-list (also `yday_gainer` + `mover_buy`). It was **under** the $400M BUY floor, so the 3d size book never listed it; flatten still wanted it. Next-open H after fees **+19.24%**. One name. Not a pattern.

### Liquid names that were *not* chosen (mcap ≥ $100M, ADV ≥ 500k)

| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | First blocker | T+1 H after fee |
|---|---|---|---:|---:|---:|---:|---|---:|
| 2026-08-13 | 2026-08-14 | ARX | 58.9% | +43.4% | 52.53 | 4,260 | `too_extended_ret5_or_rvol` | +0.17 |
| 2026-08-13 | 2026-08-14 | EROC | 43.6% | +8.2% | 3.23 | 3,279 | `sector_cap` | -0.04 |
| 2026-08-13 | 2026-08-14 | QMCO | 104.2% | +8.6% | 3.28 | 938 | `too_extended_ret5_or_rvol` | +6.38 |
| 2026-08-14 | 2026-08-17 | HTFL | 41.7% | +31.7% | 9.43 | 3,522 | `too_extended_ret5_or_rvol` | +1.50 |
| 2026-08-14 | 2026-08-17 | CAPR | 58.0% | +53.4% | 38.12 | 374 | `too_extended_ret5_or_rvol` | +0.47 |
| 2026-08-14 | 2026-08-17 | INO | 54.2% | +15.0% | 4.50 | 107 | `illiquid_mcap_lt_400_or_micro` | +3.12 |
| 2026-08-17 | 2026-08-18 | WFF | 106.0% | +237.2% | 98.16 | 204 | `illiquid_mcap_lt_400_or_micro` | +1.47 |
| 2026-08-18 | 2026-08-19 | WEAV | 44.9% | +31.7% | 69.94 | 583 | `too_extended_ret5_or_rvol` | +0.06 |
| 2026-08-19 | 2026-08-20 | MRNA | 120.6% | +123.4% | 41.84 | 56,158 | `too_extended_ret5_or_rvol` | -7.09 |
| 2026-08-19 | 2026-08-20 | AMLX | 63.5% | +8.6% | 5.80 | 4,248 | `sector_cap` | +4.69 |
| 2026-08-20 | 2026-08-21 | MRNA | 119.5% | -19.9% | 13.48 | 55,789 | `too_extended_ret5_or_rvol` | +9.72 |
| 2026-08-20 | 2026-08-21 | MRVI | 42.9% | +14.4% | 5.39 | 2,145 | `too_extended_ret5_or_rvol` | +5.89 |
| 2026-08-21 | 2026-08-24 | MRNA | 130.3% | +9.5% | 11.40 | 58,280 | `not_in_morning_book` | -2.53 |
| 2026-08-21 | 2026-08-24 | ASST | 53.6% | +17.4% | 5.24 | 1,618 | `not_in_morning_book` | +5.61 |
| 2026-08-21 | 2026-08-24 | ARCT | 61.0% | +20.6% | 6.68 | 377 | `not_in_morning_book` | +3.62 |
| 2026-08-21 | 2026-08-24 | CAN | 52.2% | +25.5% | 15.82 | 246 | `not_in_morning_book` | -1.64 |
| 2026-08-21 | 2026-08-24 | USDE | 144.0% | +78.8% | 57.80 | 194 | `not_in_morning_book` | -13.35 |
| 2026-08-21 | 2026-08-24 | CYPH | 99.0% | +8.4% | 4.92 | 139 | `not_in_morning_book` | -10.79 |
| 2026-08-24 | 2026-08-25 | ASST | 50.3% | +8.8% | 4.22 | 1,693 | `not_in_morning_book` | -3.50 |
| 2026-08-24 | 2026-08-25 | USDE | 124.0% | -8.6% | 6.64 | 184 | `not_in_morning_book` | -1.06 |
| 2026-08-24 | 2026-08-25 | CYPH | 131.8% | +15.5% | 16.23 | 177 | `not_in_morning_book` | -3.68 |
| 2026-08-25 | 2026-08-26 | USDE | 94.3% | -13.6% | 3.77 | 147 | `not_in_morning_book` | — |
| 2026-08-27 | 2026-08-28 | ANF | 40.1% | +35.7% | 13.63 | 6,565 | `not_in_morning_book` | +0.58 |
| 2026-08-28 | 2026-08-31 | USDE | 101.8% | +35.0% | 5.24 | 219 | `illiquid_mcap_lt_400_or_micro` | +7.38 |
| 2026-08-28 | 2026-08-31 | CYPH | 58.8% | +15.9% | 3.04 | 204 | `illiquid_mcap_lt_400_or_micro` | +1.53 |
| 2026-08-28 | 2026-08-31 | DFDV | 40.0% | +17.3% | 3.55 | 164 | `illiquid_mcap_lt_400_or_micro` | -0.77 |
| 2026-09-01 | 2026-09-02 | GPRO | 179.5% | +95.1% | 87.26 | 320 | `illiquid_mcap_lt_400_or_micro` | -9.04 |
| 2026-09-02 | 2026-09-03 | GPRO | 102.2% | +40.4% | 52.57 | 230 | `illiquid_mcap_lt_400_or_micro` | +38.37 |
| 2026-09-03 | 2026-09-04 | GPRO | 183.1% | +37.4% | 16.35 | 316 | `illiquid_mcap_lt_400_or_micro` | -22.06 |
| 2026-09-03 | 2026-09-04 | RNXT | 69.1% | +19.4% | 9.24 | 114 | `illiquid_mcap_lt_400_or_micro` | -6.10 |
| 2026-09-04 | 2026-09-08 | GPRO | 129.7% | -17.8% | 8.35 | 260 | `illiquid_mcap_lt_400_or_micro` | -2.20 |
| 2026-09-04 | 2026-09-08 | DPRO | 47.1% | +13.0% | 5.38 | 239 | `illiquid_mcap_lt_400_or_micro` | -5.07 |
| 2026-09-04 | 2026-09-08 | CHPT | 55.8% | +75.0% | 72.32 | 235 | `illiquid_mcap_lt_400_or_micro` | -0.91 |
| 2026-09-09 | 2026-09-10 | IRD | 49.6% | +32.0% | 48.34 | 477 | `illiquid_mcap_lt_400_or_micro` | +3.26 |
| 2026-09-09 | 2026-09-10 | INDP | 84.1% | +12.4% | 9.11 | 277 | `illiquid_mcap_lt_400_or_micro` | +27.85 |
| 2026-09-09 | 2026-09-10 | BNC | 57.6% | -15.6% | 4.10 | 182 | `not_in_morning_book` | +9.04 |
| 2026-09-10 | 2026-09-11 | INDP | 118.8% | +23.1% | 4.62 | 341 | `illiquid_mcap_lt_400_or_micro` | +2.96 |
| 2026-09-10 | 2026-09-11 | PCLA | 50.5% | +31.0% | 14.05 | 93 | `illiquid_mcap_lt_400_or_micro` | +2.62 |

## Thin after-fee counterfactual

Equal-weight T+1 **Change from Open** minus **15 bp**. Not a cash book, not dollar-weighted, not a ship signal. Current picks = that morning's **3d BUY** (fallback 1d if 3d empty).
Force-include = those current picks **plus** that T's non-ETF extremes.

| Book | Day-mean after-fee H | Graded mornings | Name-day after-fee H | Name-day H+ |
|---|---:|---:|---:|---:|
| Current 3d/1d picks | +0.36 | 16 | — | — |
| Extremes only | -1.09 | 18 | -0.89 n=249 | +34.5% |
| Liquid extremes only | +2.62 | 16 | +1.73 n=38 | +57.9% |
| Force-include extremes into current | -0.36 | 19 | — | — |
| Extremes that *were* chosen | — | — | +19.24 n=1 | +100.0% |

Read this as a **miss / leak check**, not an edge. A +40% week name often fades the next open (MRNA 08-19 → 08-20 H −6.94% before fees). Force-include will usually **drag** the current book if the extremes are worse than the 3d size names — or inflate one-off if a rip continues.

## Session board

| T | T+1 | Ext | Liq | Book | Elig | Panel | yday | Chosen | Pile | HARD_RED | Flat GO | Ext Hff | Cur Hff | Force Hff |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---:|---:|---:|
| 2026-08-13 | 2026-08-14 | 20 | 3 | 20 | 3 | 2 | 2 | 0 | n | n | n | -2.76 | +1.15 | -0.73 |
| 2026-08-14 | 2026-08-17 | 18 | 3 | 3 | 2 | 3 | 3 | 0 | n | n | n | +7.02 | -1.12 | +1.55 |
| 2026-08-17 | 2026-08-18 | 21 | 1 | 1 | 0 | 1 | 1 | 0 | n | Y | n | +0.72 | -0.14 | +2.06 |
| 2026-08-18 | 2026-08-19 | 12 | 1 | 1 | 1 | 1 | 1 | 0 | n | Y | n | -7.46 | +0.65 | -1.49 |
| 2026-08-19 | 2026-08-20 | 13 | 2 | 2 | 2 | 1 | 1 | 0 | n | n | Y | -6.96 | +0.73 | -2.16 |
| 2026-08-20 | 2026-08-21 | 24 | 3 | 4 | 2 | 3 | 2 | 1 | n | n | Y | +2.00 | +1.34 | +0.39 |
| 2026-08-21 | 2026-08-24 | 49 | 6 | 0 | 0 | 4 | 4 | 0 | n | Y | n | -0.91 | — | -0.35 |
| 2026-08-24 | 2026-08-25 | 31 | 3 | 0 | 0 | 2 | 2 | 0 | n | n | n | -2.88 | — | -3.35 |
| 2026-08-25 | 2026-08-26 | 8 | 1 | 0 | 0 | 1 | 0 | 0 | n | n | n | — | — | — |
| 2026-08-26 | 2026-08-27 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | n | n | n | — | -0.50 | -0.50 |
| 2026-08-27 | 2026-08-28 | 8 | 1 | 0 | 0 | 1 | 1 | 0 | n | n | n | -2.20 | — | -2.20 |
| 2026-08-28 | 2026-08-31 | 11 | 3 | 3 | 0 | 3 | 3 | 0 | Y | Y | n | -1.36 | -0.30 | -1.28 |
| 2026-08-31 | 2026-09-01 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | Y | Y | n | -6.49 | +0.33 | -1.94 |
| 2026-09-01 | 2026-09-02 | 2 | 1 | 1 | 0 | 1 | 1 | 0 | Y | Y | n | +8.80 | +2.24 | +3.02 |
| 2026-09-02 | 2026-09-03 | 6 | 1 | 1 | 0 | 1 | 1 | 0 | Y | n | n | +7.57 | +2.63 | +3.77 |
| 2026-09-03 | 2026-09-04 | 5 | 2 | 2 | 0 | 1 | 1 | 0 | Y | n | n | -11.14 | +0.48 | -2.29 |
| 2026-09-04 | 2026-09-08 | 10 | 3 | 3 | 0 | 3 | 2 | 0 | n | n | n | -0.67 | +0.03 | -0.26 |
| 2026-09-08 | 2026-09-09 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | n | Y | n | -5.86 | -1.34 | -1.81 |
| 2026-09-09 | 2026-09-10 | 9 | 3 | 2 | 0 | 3 | 2 | 0 | n | Y | n | +1.52 | -0.35 | +0.52 |
| 2026-09-10 | 2026-09-11 | 5 | 2 | 2 | 0 | 1 | 1 | 0 | n | n | n | +1.42 | -0.01 | +0.23 |

## Big names (mcap ≥ $1B or chosen)

| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | First blocker | Chosen | T+1 Hff |
|---|---|---|---:|---:|---:|---:|---|---|---:|
| 2026-08-13 | 2026-08-14 | ARX | 58.9% | +43.4% | 52.53 | 4,260 | `too_extended_ret5_or_rvol` | n | +0.17 |
| 2026-08-13 | 2026-08-14 | EROC | 43.6% | +8.2% | 3.23 | 3,279 | `sector_cap` | n | -0.04 |
| 2026-08-14 | 2026-08-17 | HTFL | 41.7% | +31.7% | 9.43 | 3,522 | `too_extended_ret5_or_rvol` | n | +1.50 |
| 2026-08-19 | 2026-08-20 | MRNA | 120.6% | +123.4% | 41.84 | 56,158 | `too_extended_ret5_or_rvol` | n | -7.09 |
| 2026-08-19 | 2026-08-20 | AMLX | 63.5% | +8.6% | 5.80 | 4,248 | `sector_cap` | n | +4.69 |
| 2026-08-20 | 2026-08-21 | MRNA | 119.5% | -19.9% | 13.48 | 55,789 | `too_extended_ret5_or_rvol` | n | +9.72 |
| 2026-08-20 | 2026-08-21 | MRVI | 42.9% | +14.4% | 5.39 | 2,145 | `too_extended_ret5_or_rvol` | n | +5.89 |
| 2026-08-20 | 2026-08-21 | ARCT | 42.7% | +9.1% | 4.53 | 319 | `chosen` | Y | +19.24 |
| 2026-08-21 | 2026-08-24 | MRNA | 130.3% | +9.5% | 11.40 | 58,280 | `not_in_morning_book` | n | -2.53 |
| 2026-08-21 | 2026-08-24 | ASST | 53.6% | +17.4% | 5.24 | 1,618 | `not_in_morning_book` | n | +5.61 |
| 2026-08-24 | 2026-08-25 | ASST | 50.3% | +8.8% | 4.22 | 1,693 | `not_in_morning_book` | n | -3.50 |
| 2026-08-27 | 2026-08-28 | ANF | 40.1% | +35.7% | 13.63 | 6,565 | `not_in_morning_book` | n | +0.58 |

## Method notes

- T tape is the dated Finviz export (`data/exports/finviz_YYYY-MM-DD.csv`). That file is the after-close / overnight packet used everywhere else.
- T+1 picks: `data/stock_book/YYYY-MM-DD_stock_book.json` BUY lists, matching CSV universe, `*_green.json`, `03_scoreboard/flatten_lookback_action.json` (through 09-08) plus `01_daily/*_flatten_card.md` would-buy tables for 09-09..11, `data/factor_mine/panel.json` 09:30 candidate rows.
- T+1 H is Finviz **Change from Open** on the T+1 export (09:30 → 16:00 stand-in). `data/prices/ohlc.parquet` is not in this checkout; official bars were empty.
- Green-pile RelVol=0 (no print) is **not** a veto; only printed (0, 0.7) is. Morning books often show 0.01–0.2 pre-open — that **is** a veto.
- `flatten_robust` live tickets still sit on HARD_RED / no-flatten mornings; would-buy is the 3d size wish-list with holdings disregarded.
- Factor-mine recipes here are the leak-free 09:30 lists already mined; this cut does not rebuild the $10k cash blotter.

Reusable rows: `extreme_vol_price_names.csv`, `extreme_vol_price_sessions.csv`. Regenerate with `python3 excel_bot/research/extreme_vol_price_audit.py`.

Live frozen. Do not treat a force-include mean as a wire.
