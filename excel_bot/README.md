# Excel Bot — the STOCKHISTORY spreadsheet, running on GitHub

This is the exact Python replica of the `Simple View--Calculation.xlsx`
cluster-color engine (same formulas, same dependencies, same colors), moved
off the local PC. Zero tokens, zero LLM — pure math on Yahoo OHLCV.

## What it does daily (Tue–Sat 10:30 UTC = 18:30 HKT, after the US close)

Workflow: `.github/workflows/excel_bot.yml` → "Excel Bot (cluster signals daily)"

1. **Restore** the price cache from the `excel-state` branch
   (`state/rows.tar.gz`, ~3,603 tickers).
2. **Fetch** the last 14 days per ticker (Yahoo v8 HTTPS, incremental merge).
3. **Engine** rebuilds every grid through the Excel-replica model
   (`engine/model.json` = the extracted cell equations).
4. **Signals** — every validated strategy in `strategies/` is checked; a
   suggestion is a cluster whose *confirmation day* is the latest trading day.
5. **Store** — appended to `suggestions/suggestions.csv` (one file, deduped).
   All past suggestions get `current_price` / returns refreshed.
6. **Summary** — `daily/{date}_excel_bot.md`: today's signals, live strategy
   scoreboard, best/worst open suggestions.
7. **Save** — the price cache is re-packed and force-pushed to `excel-state`
   (history squashed, the branch never grows).

## Where to look

| Path | What |
|---|---|
| `excel_bot/daily/` | **Start here.** One human-readable MD per run. |
| `excel_bot/suggestions/suggestions.csv` | Every suggestion ever + live tracking. `ret_vs_open` = honest "how is it doing". |
| `excel_bot/strategies/README.md` | The strategy cards + backtest stats. |
| `excel_state` branch | Machine state only — never edit by hand. |

## Reading a suggestion row

- `signal_date` — trading day the cluster confirmed (colors in `signal_colors`,
  plain English, columns A→O like the spreadsheet).
- `ref_close` — close of signal day (model entry).
- `first_open` — next trading day's open = the price you could actually get.
- `exit_rule` — tp8/tp3 = limit sell at +8%/+3%; hold2 = sell after 2 sessions;
  hold1 = next day (shorts).

## Caveats (from live tracking + backtest)

- L3/L5 (midcap hold2) are the only strategies beating their backtest live;
  L1/L2 (take-profit) are underperforming — capped winners, uncapped losers.
- Wins are tail-driven: the median trade is ~0, profit comes from a few
  +20–40% runners. Take every signal or the math breaks.
- Strategies validated on a 6.5-month regime; cohort map
  (`data/finviz_with_descriptions.csv`) is a static snapshot.
- Shorts (S1/S2) ignore borrow fees in tracking.

## Manual run

Actions → "Excel Bot (cluster signals daily)" → Run workflow.
Inputs: `limit` (test on first N tickers), `signals_only` (skip fetch).
