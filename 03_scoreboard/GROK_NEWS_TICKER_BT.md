# Grok news overlay on existing strategies (research)

Window **2026-08-13 → 2026-09-18** · last closed session **2026-09-18**. Every pre-open Grok-pipeline article → exact named tickers → overlay on `union_hot_n4_h1` and `flatten_h5`. Not live. Does not touch factor-mine / flatten / Webull.

## Coverage

- Stage 1 automation days dumped: **0** (0 results).
- Grok-pipeline sessions with a pre-open file: **26** of 26.
- Sessions filled from repo files only: **26** of 26.
- `automation_get_results`: **False**. automation_get_results unavailable in this environment (cloud VM / GH Actions cannot see automation run logs)
- Articles kept: **699** (frozen=0, actions=142, parsed=975, events=173).
- Articles that name ≥1 listed ticker: **192**.
- Exact ticker-days: **277** (bull/bear used by overlay: 126).
- Raw Finviz CSV is **not** the article list. Sector baskets (Hormuz → COP/EOG, EPA → every generator) are **not** a map.
- GH Actions must not harvest. Replay reads frozen dumps + dated Grok pipeline.

## Leak rules

- `known_at` = automation createTime, or the pipeline file's `generated_at` / printed stamp. Earlier stamp only if that stamp sits on a file dated ≤ the fill session.
- Fill = next official 09:30 **strictly after** known_at. 09:30:00 ET is too late for that open. RTH → next open. Missing official open → no fill.
- A ticker is affected only if the article **names** it (ticker token or unique D-1 company name). Grok `ticker_actions` baskets and theme packs do not expand the set.
- No close digest, no post-close research_baseline, no OOS files while mapping an IS fill, no same-day Change%/Gap/RelVol.
- Long-only (no short locate) + hard-red S≤−3 sit. $10k leftover split, Futubull fees, whole shares.

## Overlay vs same-panel base

Base books are resimulated on the research panel with the live recipe (`pick_day`). Overlay books rewrite that day's list, then use the same cash book. IS / OOS are running-book splits of the full-window path. Published factor-mine numbers are a footnote only — overlay delta is vs the same-panel base.

| Window | `union_hot_n4_h1` | +veto_bear | +require_bull | +add_named | +full |
|---|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | +21.48 | +21.41 (-0.07) | +0.00 (-21.48) | +20.74 (-0.74) | +20.75 (-0.73) |
| 2026-08-13→2026-09-09 | +18.84 | +18.76 (-0.08) | +0.00 (-18.84) | +18.26 (-0.58) | +18.28 (-0.56) |
| 2026-09-10→2026-09-18 | +2.22 | +2.23 (+0.01) | +0.00 (-2.22) | +2.09 (-0.13) | +2.09 (-0.13) |

| Window | `flatten_h5` | +veto_bear | +require_bull | +add_named | +full |
|---|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | +7.99 | +9.66 (+1.67) | +0.00 (-7.99) | +5.73 (-2.26) | +7.03 (-0.96) |
| 2026-08-13→2026-09-09 | +10.54 | +10.54 (+0.00) | +0.00 (-10.54) | +7.02 (-3.52) | +7.02 (-3.52) |
| 2026-09-10→2026-09-18 | -2.31 | -0.79 (+1.51) | +0.00 (+2.31) | -1.21 (+1.10) | +0.01 (+2.31) |

### How often news actually touched the list (full window)

| Base | overlay | days changed | list ∩ named | vetoed names | added names | confirmed bulls |
|---|---|---:|---:|---:|---:|---:|
| `union_hot_n4_h1` | `veto_bear` | 0/26 | 0 | 0 | 0 | 0 |
| `union_hot_n4_h1` | `require_bull` | 26/26 | 0 | 104 | 0 | 0 |
| `union_hot_n4_h1` | `add_named` | 16/26 | 0 | 0 | 30 | 0 |
| `union_hot_n4_h1` | `full` | 16/26 | 0 | 0 | 30 | 0 |
| `flatten_h5` | `veto_bear` | 1/26 | 1 | 1 | 0 | 0 |
| `flatten_h5` | `require_bull` | 26/26 | 1 | 156 | 0 | 0 |
| `flatten_h5` | `add_named` | 16/26 | 1 | 0 | 33 | 0 |
| `flatten_h5` | `full` | 16/26 | 1 | 1 | 33 | 0 |

### Published factor-mine footnote (not the overlay delta)

- Published `union_hot_n4_h1`: +31.97. Published `flatten_h5`: +3.39.
- Same-panel resim can differ from the published phone book (start-on / lookback). Overlay minus base uses the resim.

## 3 hits (exact named, directional)

- **ORCL** `bearish` fill 2026-09-11 same-day -8.61 / next-close — — Nasdaq, Dow, S&P 500 Futures Rise After 4-Day Market Slide As CPI Looms Large: ORCL, ADBE, MU, TSLA, RKLB, IBRX, HOOD, GME In Focus
- **ABTC** `bearish` fill 2026-08-21 same-day -8.43 / next-close -0.23 — Stock Market Today: Futures Little Changed After Major Indexes Snap 3-Day Skids; Oil Prices, Treasury Yields, Bitcoin Rise; Walmart Stock Drops - Yahoo Finance
- **ARM** `bearish` fill 2026-08-28 same-day -4.80 / next-close -3.66 — Mortgage and refinance interest rates today, Thursday, August 27, 2026: Fixed rates rise, 5/1 ARM falls

## 3 misses

- **ABTC** `bullish` fill 2026-08-21 same-day -8.43 / next-close -0.23 — Bitcoin Rally Tops $79,000. Crypto Shorts, ETF Flows Soar. CFTC Explores Crypto Rules.
- **BBGI** `bullish` fill 2026-08-19 same-day -7.13 / next-close -6.86 — Disney sues FCC over Trump’s broadcast-license threat
- **SNDK** `bullish` fill 2026-08-18 same-day -3.08 / next-close -6.48 — Stock Market Today: Tech Futures Sink As Treasury Yields Jump; Nvidia, Micron, Sandisk All Tumble (Live Coverage)

Exact named same-day hit rate: 47/95 (49%). Next-close: 37/73 (51%).

## Method

1. Stage 1 harvest (Cursor / Grok Bot, not GH Action) dumps each automation result since 2026-08-13 into `data/grok_automations/{date}_{task}.json`. This environment has no `automation_get_results`, so Stage 2 reads the dated Grok pipeline the news-parsing automation already wrote (`*_parsed.json`, `*_actions.json` evidence, events).
2. Keep policy / regulator / exemption / ban / tariff / Fed / court / bill rows. Drop earnings PR, SEC filings, Yahoo/Cramer tabloid, single-name FDA approvals, carried Hormuz.
3. A stock is affected only when the article names it. Company names resolve through D-1 Finviz Company text. Theme packs set polarity of a named name; they do not add unnamed names.
4. Each session: `pick_day` the live recipe, then rewrite the list (`veto_bear` / `require_bull` / `add_named` / `full`). Cash book is the factor-mine family.
5. `full` also marks held names news🔴 when the article names them bearish, so flatten hold-5 can exit after the min-hold rule.

