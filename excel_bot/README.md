# Excel Bot — the STOCKHISTORY spreadsheet, running on GitHub

This is the exact Python replica of the `Simple View--Calculation.xlsx`
cluster-color engine (same formulas, same dependencies, same colors), moved
off the local PC. Zero tokens, zero LLM — pure math on Yahoo OHLCV.

## What it does daily

Two schedules share one job (`excel-bot`, no cancel-in-progress):

- **Draft** — Tue–Sat 10:30 UTC. This usually starts before the 16:00 ET close and writes only the draft markdown. It does not append `suggestions.csv` and does not write `freeze_manifest.json`.
- **Final** — Mon–Fri 21:17 UTC (17:17 EDT / 16:17 EST), after the cash close in both DST states. Same full path as a manual run left on limit 0 and signals_only false: every ticker, fetch included. It creates that session's dated file once, appends suggestion rows, and locks the closed session.

`run_date` and the dated file use the NYSE session in America/New_York, not the UTC date. A new manifest entry is keyed by the pick's `signal_date` (the confirmation session on the bar). A start after midnight UTC that is still the previous evening in New York stamps that evening. Before 16:00 ET the live session has not closed, so the run writes a draft and does not create its final. A weekend or full-day NYSE holiday stamps the previous completed session and will not overwrite a final that is already there.

Workflow: `.github/workflows/excel_bot.yml` → "Excel Bot (cluster signals daily)"

1. **Restore** the price cache from the `excel-state` branch
   (`state/rows.tar.gz`, ~3,603 tickers).
2. **Fetch** the last 14 days per ticker (Yahoo v8 HTTPS, incremental merge).
3. **Engine** rebuilds every grid through the Excel-replica model
   (`engine/model.json` = the extracted cell equations).
4. **Signals** — every validated strategy in `strategies/` is checked; a
   suggestion is a cluster whose *confirmation day* is the latest trading day.
5. **Store** — on a final run only, appended to `suggestions/suggestions.csv`
   (one file, deduped). All past suggestions get `current_price` / returns
   refreshed. From 2026-10-06 each closed signal day is fingerprinted before
   that write. A draft run leaves the csv and the manifest byte-identical.
6. **Summary** — before 16:00 ET, `daily/{date}_excel_bot_draft.md` only.
   At or after 16:00 ET, `daily/{date}_excel_bot.md` is created once
   (today's signals, live strategy scoreboard, best/worst open
   suggestions) and a later run refuses to overwrite it.
7. **Save** — the price cache is re-packed and force-pushed to `excel-state`
   (history squashed, the branch never grows).

## Where to look

| Path | What |
|---|---|
| `excel_bot/daily/` | **Start here.** Final `{date}_excel_bot.md` after the close. `{date}_excel_bot_draft.md` is the pre-close note. |
| `excel_bot/suggestions/suggestions.csv` | Every suggestion ever + live tracking. `ret_vs_open` = honest "how is it doing". |
| `excel_bot/freeze_manifest.json` | Append-only sha256 of each locked signal day from 2026-10-06. Days before that are labelled pre-lock and are not fingerprinted. |
| `excel_bot/strategies/README.md` | The strategy cards + backtest stats. |
| `excel_state` branch | Machine state only — never edit by hand. |

## Reading a suggestion row

- `signal_date` — trading day the cluster confirmed (colors in `signal_colors`,
  plain English, columns A→O like the spreadsheet).
- `ref_close` — close of signal day (model entry).
- `first_open` — next trading day's open = the price you could actually get.
- `exit_rule` — tp8/tp3 = limit sell at +8%/+3%; hold2 = sell after 2 sessions;
  hold1 = next day (shorts).

## Locked signals (from 2026-10-06)

Signal dates before 2026-10-06 are **pre-lock**. They are not fingerprinted,
not backfilled, and the guard does not rewrite them. Live marks on those
rows may still refresh.

From 2026-10-06 on, `freeze_manifest.json` stores a sha256 of each signal
day's locked pick fields: `run_date`, `signal_date`, `ticker`, `side`,
`strategy`, `exit_rule`, `ref_close`, `signal_colors`. `run_date` is in
the hash because a pick's run date is written once. `first_open` is
write-once: a blank may become a value once, and that fill appends a
manifest entry; a non-blank value must not change. `current_price`,
`ret_vs_close`, `ret_vs_open`, and `days_held` stay live. Manifest
entries are never removed or edited. If a locked day's picks are added,
removed, or changed, or a non-blank `first_open` changes, the run prints
`FAIL CLOSED` and commits nothing. A session that has not reached 16:00 ET
cannot be locked. The pre-close run does not append rows or manifest entries.

The dated file `daily/{date}_excel_bot.md` is write-once for that NYSE
session. A run before 16:00 ET on a session day writes only the `_draft`
file. At or after 16:00 ET, and on a weekend or holiday for the previous
completed session, the final file is created once. If that file is already
in the tree, the run refuses to overwrite it. A manual workflow dispatch
is the by-hand backup and uses the same clock. Strategy rules, names,
and which tickers qualify are unchanged.

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
