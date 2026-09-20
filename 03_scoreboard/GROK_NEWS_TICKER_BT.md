# Grok news standalone catalyst book + overlay (research)

Window **2026-08-13 → 2026-09-18** · last closed session **2026-09-18**. Every pre-open Grok-pipeline article → exact named tickers. Standalone long-only book of policy/gov names (they are not on hot4). Overlay on `union_hot_n4_h1` / `flatten_h5` is secondary. Not live. Does not touch factor-mine / flatten / Webull.

## Coverage

- Stage 1 automation days dumped: **0** (0 results).
- Grok-pipeline sessions with a pre-open file: **26** of 26.
- Sessions filled from repo files only: **26** of 26.
- `automation_get_results`: **False**. automation_get_results unavailable in this environment (cloud VM / GH Actions cannot see automation run logs)
- Articles kept: **699** (frozen=0, actions=139, parsed=975, events=173).
- Articles that name ≥1 listed ticker: **171**.
- Exact ticker-days: **237** (bull/bear used by overlay: 106).
- Catalyst articles (policy/gov, not a market wrap): **465** → **12** standalone longs.
- Raw Finviz CSV is **not** the article list. Sector baskets (Hormuz → COP/EOG, EPA → every generator) are **not** a map. Stock-market-today / in-focus laundry lists are not a catalyst.
- GH Actions must not harvest. Replay reads frozen dumps + dated Grok pipeline.

## Leak rules

- `known_at` = automation createTime, or the pipeline file's `generated_at` / printed stamp. Earlier stamp only if that stamp sits on a file dated ≤ the fill session.
- Fill = next official 09:30 **strictly after** known_at. 09:30:00 ET is too late for that open. RTH → next open. Missing official open → no fill.
- A ticker is affected only if the article **names** it (ticker token or unique D-1 company name). Grok `ticker_actions` baskets and theme packs do not expand the set.
- No close digest, no post-close research_baseline, no OOS files while mapping an IS fill, no same-day Change%/Gap/RelVol.
- Long-only (no short locate) + hard-red S≤−3 sit. $10k leftover split, Futubull fees, whole shares. Standalone holds 1 and 2 — policy prints often land on the next session, not 09:30→close.

## Standalone policy-catalyst book

These names are **not** on `union_hot_n4_h1` (0/26 overlap). If we do not run a standalone book, the catalysts are unused. Long-only the exact bullish names on a policy/gov article. No shorts. No wraps. Cap 3 names per article. Does **not** inherit hard-red sit (that weather gate is why Disney/FCC never traded). Day-cap 35% so one name cannot dump $10k.

| Window | `grok_n4_h1` | `grok_n4_h2` | `grok_n8_h1` | `grok_n8_h2` | published hot4 | published flatten |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | +1.98 | +1.45 | +1.98 | +1.45 | +31.97 | +3.39 |
| 2026-08-13→2026-09-09 | +1.31 | +0.94 | +1.31 | +0.94 | +14.95 | +10.54 |
| 2026-09-10→2026-09-18 | +0.66 | +0.51 | +0.66 | +0.51 | +14.80 | -6.47 |

Catalyst same-day hit rate: 9/11 (82%). Next-close (the hold-2 print): 6/11 (55%).

## Overlay vs same-panel base

Base books are resimulated on the research panel with the live recipe (`pick_day`). Overlay books rewrite that day's list, then use the same cash book. IS / OOS are running-book splits of the full-window path. Published factor-mine numbers are a footnote only — overlay delta is vs the same-panel base.

| Window | `union_hot_n4_h1` | +veto_bear | +require_bull | +add_named | +full |
|---|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | +21.48 | +21.41 (-0.07) | +0.00 (-21.48) | +22.36 (+0.88) | +22.23 (+0.75) |
| 2026-08-13→2026-09-09 | +18.84 | +18.76 (-0.08) | +0.00 (-18.84) | +19.70 (+0.86) | +19.57 (+0.73) |
| 2026-09-10→2026-09-18 | +2.22 | +2.23 (+0.01) | +0.00 (-2.22) | +2.22 (+0.01) | +2.22 (+0.00) |

| Window | `flatten_h5` | +veto_bear | +require_bull | +add_named | +full |
|---|---:|---:|---:|---:|---:|
| 2026-08-13→2026-09-18 | +7.99 | +9.66 (+1.67) | +0.00 (-7.99) | +8.00 (+0.00) | +9.67 (+1.67) |
| 2026-08-13→2026-09-09 | +10.54 | +10.54 (+0.00) | +0.00 (-10.54) | +10.54 (-0.01) | +10.54 (-0.01) |
| 2026-09-10→2026-09-18 | -2.31 | -0.79 (+1.51) | +0.00 (+2.31) | -2.30 (+0.01) | -0.79 (+1.52) |

### How often news actually touched the list (full window)

| Base | overlay | days changed | list ∩ named | vetoed names | added names | confirmed bulls |
|---|---|---:|---:|---:|---:|---:|
| `union_hot_n4_h1` | `veto_bear` | 0/26 | 0 | 0 | 0 | 0 |
| `union_hot_n4_h1` | `require_bull` | 26/26 | 0 | 104 | 0 | 0 |
| `union_hot_n4_h1` | `add_named` | 11/26 | 0 | 0 | 20 | 0 |
| `union_hot_n4_h1` | `full` | 11/26 | 0 | 0 | 20 | 0 |
| `flatten_h5` | `veto_bear` | 1/26 | 1 | 1 | 0 | 0 |
| `flatten_h5` | `require_bull` | 26/26 | 1 | 156 | 0 | 0 |
| `flatten_h5` | `add_named` | 11/26 | 1 | 0 | 23 | 0 |
| `flatten_h5` | `full` | 12/26 | 1 | 1 | 23 | 0 |

### Published factor-mine footnote (not the overlay delta)

- Published `union_hot_n4_h1`: +31.97. Published `flatten_h5`: +3.39.
- Same-panel resim can differ from the published phone book (start-on / lookback). Overlay minus base uses the resim.

## 3 hits (exact named, directional)

- **ABTC** `bullish` fill 2026-08-24 same-day +8.00 / next-close +15.50 — Bitcoin Rally Tops $79K. Crypto Shorts, ETF Flows Soar. CFTC Explores Crypto Rules. - Investor's Business Daily
- **DIS** `bullish` fill 2026-08-19 same-day +2.76 / next-close +3.13 — Disney sues FCC over Trump’s broadcast-license threat
- **NOC** `bullish` fill 2026-08-14 same-day +1.12 / next-close -1.58 — Patriots and THAAD: Lockheed Martin Corporation (LMT) and Northrop Grumman Corporation (NOC) Bet Big on Missile Defense Surge

## 3 misses

- **ABTC** `bullish` fill 2026-08-21 same-day -8.43 / next-close -0.23 — Bitcoin Rally Tops $79,000. Crypto Shorts, ETF Flows Soar. CFTC Explores Crypto Rules.
- **STLD** `bullish` fill 2026-08-14 same-day -2.43 / next-close -1.52 — Widening Spreads and Tariff Protection Drive Steel Dynamics’ (STLD) Rally

Exact named same-day hit rate: 39/78 (50%). Next-close: 32/59 (54%).

## Method

1. Stage 1 harvest (Cursor / Grok Bot, not GH Action) dumps each automation result since 2026-08-13 into `data/grok_automations/{date}_{task}.json`. This environment has no `automation_get_results`, so Stage 2 reads the dated Grok pipeline the news-parsing automation already wrote (`*_parsed.json`, `*_actions.json` evidence, events).
2. Keep policy / regulator / exemption / ban / tariff / Fed / court / bill rows. Drop earnings PR, SEC filings, Yahoo/Cramer tabloid, single-name FDA approvals, carried Hormuz.
3. A stock is affected only when the article names it. Company names resolve through D-1 Finviz Company text. Theme packs set polarity of a named name; they do not add unnamed names. 3-letter tokens only in `$TICK` / `(TICK)`. Wraps are not maps.
4. Standalone book: long the bullish named names on a policy/gov article (`grok_n4/n8` × hold 1/2). These names are not on hot4.
5. Overlay: `pick_day` the live recipe, then rewrite (`veto_bear` / `require_bull` / `add_named` / `full`). `full` marks held names news🔴 when the article names them bearish.

