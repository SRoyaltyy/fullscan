# Excel CLEAR letter panel — Theme Radar join mine

_Generated 2026-09-12 · **research only** · live `flatten_robust` frozen._

## What this is

A clock-clean CSV Theme Radar can join on `date,ticker,FQ,ER,EP,AH,FR,DF_lag1`.

Values are **raw open-knowable letters** from `excel_open_features.open_features` / `OPEN_SAME_ROW_LABELS` / `CLOCK_MAP`. Not book P&L. Not a live wire. `flatten_robust` and the cash book are not imported or changed.

## Clock (hard)

- **Join clock Theme Radar uses:** Finviz after-close **T** × this panel on morning session **T+1** (values known 09:30 T+1).
- **FQ, ER, EP, AH, FR** — same-row **open** `value_mine_open` tallies (prior-print). Knowable at 09:30 on that `date`. In the locked 44.
- **DF_lag1** — yesterday’s DF pattern text (`open_features` `df` on the last *completed* prior bar). DF is **close same-row**; only **lag t−1+** is open-legal. Same-row DF/BB/BQ at 09:30 is a **leak** — abort.
- Never peek same-row H/I, M number, `core_score`, or H paint.
- Leak check: **PASS**.
- Fill OPEN same-row: `A, B, C, G, J, K, L, M, O, IR, IS, IT`
- Value OPEN same-row (44): `A, C, J, Q, Z, AC, AH, BT, BV, CG, CH, DC, DE, EB, EK, EN, EP, EQ, ER, ES, ET, EU, EV, FQ, FR, FS, FU, GD, GE, GF, HF, HG, HW, II, IR, IT, IY, IZ, JB, JC, JD, JE, JF, JL`
- Same-row leak abort: `DF, BB, BQ`
- Open-44 tally subs: `AH, JB, JC, FQ, FR, ER, EP, EN`

## How to join

1. Take Finviz after-close survivors dated **T** (Theme Radar’s tape).
2. Left-join this panel on `ticker` where `panel.date = T+1` (next Theme Radar morning in `PANEL_DATES`, skipping weekends / Labor Day as *sessions*).
3. Apply gates on the **raw columns** (do not treat this file as a book):
   - avoid `FQ == 1`
   - avoid `ER == 1` / `EP >= 0.03` / `AH >= 1`
   - elevate lag-Hammer: `"Hammer" in DF_lag1` and `"Inverted" not in DF_lag1` (matches `feature_flags.prior_hammer`)
   - elevate `FR >= 1`
4. **EP units:** fraction from `open_features` (0.03 = 3%). The board recipe is `avoid_EP_ge03`. A brief that says `EP≥0.3` is **not** this CSV’s unit — do not scale EP to percent.

Sunday Theme Radar mornings **2026-08-30** and **2026-09-06** are emitted. Prior bars for those rows are the last true weekday session (08-28 / 09-04). Labor Day **2026-09-07** is not a session and is not a panel date.

## Universe

liquid (prior-session volume ≥ 1,000,000) ∪ book-overlap (that morning’s join ranked ∪ PIT prior 1d-buy ∪ PIT prior green pile).

- Unique tickers in the CSV: **11582** (full list in `excel_clear_letter_panel.json`).
- Liquidity cut: prior-session volume ≥ 1,000,000 (same as the open-gates liquid tape).
- A name-day needs ≥ 6 completed true-session bars before `date` (AH is a 6-session count).
- Book-overlap uses **prior-day** `*_stock_book.json` 1d-buy and `*_green.json` (afternoon stamp → PIT). Same-day stock-book JSON is afternoon and is not an open feature.

## Row counts

Total rows: **138017**. Lag-Hammer prints: 2124.

| date | rows | universe | skipped (thin history) | weekday |
|---|---:|---:|---:|---|
| 2026-08-13 | 11475 | 11582 | 107 | Thu |
| 2026-08-14 | 11516 | 11590 | 74 | Fri |
| 2026-08-17 | 11500 | 11576 | 76 | Mon |
| 2026-08-18 | 11508 | 11587 | 79 | Tue |
| 2026-08-19 | 11518 | 11603 | 85 | Wed |
| 2026-08-20 | 6400 | 6424 | 24 | Thu |
| 2026-08-21 | 6375 | 6389 | 14 | Fri |
| 2026-08-27 | 6316 | 6330 | 14 | Thu |
| 2026-08-30 | 6347 | 6361 | 14 | Sun |
| 2026-08-31 | 6345 | 6361 | 16 | Mon |
| 2026-09-01 | 6342 | 6357 | 15 | Tue |
| 2026-09-02 | 6406 | 6421 | 15 | Wed |
| 2026-09-03 | 6387 | 6404 | 17 | Thu |
| 2026-09-04 | 6399 | 6419 | 20 | Fri |
| 2026-09-06 | 6358 | 6376 | 18 | Sun |
| 2026-09-08 | 6333 | 6351 | 18 | Tue |
| 2026-09-09 | 3488 | 6373 | 2885 | Wed |
| 2026-09-10 | 3485 | 6363 | 2878 | Thu |
| 2026-09-11 | 3519 | 6396 | 2877 | Fri |

## Gaps / sources

- Bar sources in the stitch: `{'parquet': 2903453, 'yahoo': 53426, 'finviz': 70849}`.
- `data/prices/ohlc.parquet` last date **2026-08-21** (Yahoo/cache).
- Finviz weekday dumps stitch 08-13…09-04 (no 08-26 export). Weekend / Labor Day Finviz dumps are **not** used as session bars.
- Yahoo fill for missing prior sessions ['2026-08-26', '2026-09-08', '2026-09-09', '2026-09-10']: tried 3844, ok 3828, fail 16. Bars landed: `{'2026-08-26': 3819, '2026-09-08': 3809, '2026-09-09': 3815, '2026-09-10': 3815}`.
- **2026-08-27:** if Yahoo 08-26 is missing, DF_lag1 may use **08-25** (documented fallback). Older priors are not emitted as lag-1.
- **2026-09-08:** prior true session is 09-04 (weekend + Labor Day). No Yahoo 09-08 bar is required for that morning’s letters.
- **2026-09-09…09-11:** need Yahoo 09-08 / 09-09 / 09-10. Yahoo fill is liquid ∪ book/green ∪ join top-80 (~3.8k names). Other join-only names are omitted here so DF_lag1 is not Friday 09-04 mislabeled as yesterday. ~3.5k rows remain — enough for an n≥30 fire bar.

## Rebuild

```
python3 excel_bot/engine/excel_clear_letter_panel.py
```

Reuse only: `excel_clock_gate`, `excel_open_features`, parquet / Finviz / `fastfetch` Yahoo. No new letter formulas.

## Explicitly not live

Research artifact for Theme Radar’s join mine. Do not wire `flatten_robust`, `LIVE_POLICY`, `join_rules.json`, or the cash book.

