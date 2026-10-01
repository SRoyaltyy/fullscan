# factor_mine_score_search_v1 — preregistration

- status: protocol locked. This commit has no returns, no compounds, and no frozen ranking.
- written: 2026-09-26
- study: factor_mine_score_search_v1
- fingerprint_sha256: c03d665afb3873767263ef6e33bb95106ffb3f7fbe8b2adf7b06e80747fc0d31
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: on non-red mornings, does a small grid of morning-board Score formulas, buying the top 2, 4, or 8 longs, win on average through 2026-09-11, and does the frozen ranking still do so from 2026-09-14 through 2026-09-25.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.
- inputs_sha256: 6016c796f50c7d2f4ba6fa10d69ba665e8b17d6705a7c6ad7ebefe65973cf1d3

<!-- BEGIN COVERED -->

## Buy rule

The buy rule is not searched. The only lever is the Score formula.

Each morning is one session in `INPUTS.json`. The name list, `src_rank`, `cond_good`, and `cond_bad` are the rows in the earliest commit on `origin/main` whose `data/factor_mine/panel.json` contains that session. A later rewrite of the panel is not used. `e_pol` is stamped with `src/factor_mine_probe.py` `attach_erd_polarity` on those rows. Morning S is `src/factor_mine_book.py` `morning_s` on this tree, stored in `INPUTS.json`. The file sha256 is `6016c796f50c7d2f4ba6fa10d69ba665e8b17d6705a7c6ad7ebefe65973cf1d3`.

A morning is red when S is missing or S <= -3. That is the Factor Mine red-day guard (`HARD_RED = -3`). A red morning buys nothing. Lots already held still exit. 2026-08-27 has no S in the snapshot, so it sits.

On a non-red morning the book buys the top X longs by that formula's Score. X is 2, 4, and 8. Each X is reported. The ranking of formulas uses the equal-weight mean of the three X compounds. X is not a separate candidate.

The fill is the session open. The exit is that session's close, which is the board horizon when the hold is one session and the Sell column is the horizon close. A name with no positive open cannot take a slot. A name with no positive close is bought anyway, marked at its open, and sold at the next session's open. The close is not used to skip the buy.

One $10,000 cash book. Whole shares. Leftover cash is split evenly across the names that can buy one share; a name that cannot is dropped and the split is tried again. Futubull fees use `src/paper_trade.py` `order_fees`. Flat 15bp is one 7.5bp side, shown beside Futubull, and is not the rank key.

## Score grid

Luck N is the grid size: 34. These 34 tries are new. They are not added to any earlier denominator.

The current board Score is grid point `board_list`: `100 - src_rank`, or `100 - 99` when `src_rank` is missing. That is `rank_score` in `src/factor_mine_sim.py` for rank `list` or `score`. Ties take the ticker symbol ascending.

Every other point is a within-morning z-score blend. A feature with fewer than two finite values, or with zero variance, contributes 0. A missing value contributes 0 after the z-score. The population standard deviation is used. Higher score is bought. Ties take the ticker.

Features, higher meaning a higher long score:

- `cam`: `cond_good - cond_bad`.
- `yday`: prior close-to-close percent from that price version's bars dated before the session. Caps, when set, clip this percent before the z-score.
- `rsi`: `50 - RSI` so a more oversold name scores higher. `rsi_mom` uses `RSI - 50` instead. RSI is the prior Finviz export's `Relative Strength Index (14)` when that prior file has a number, else Wilder RSI(14) on that price version's prior bars.
- `macd`: MACD histogram (12, 26, 9) on those same prior bars. Caps clip it before the z-score.
- `flow`: 1 when prior relative volume is at least 1.5 and the absolute prior 1-day percent is at most 1.2, else 0. Missing bars leave it missing.
- `earn`: 1 when `e_pol` is `good`, -1 when `bad`, else 0.

The 34 ids, in lock order:

| id | weights | cap yday | cap macd | rsi |
| --- | --- | ---: | ---: | --- |
| `board_list` | list rank |  |  |  |
| `only_cam` | cam 1 |  |  | os |
| `only_yday` | yday 1 |  |  | os |
| `only_rsi` | rsi 1 |  |  | os |
| `only_macd` | macd 1 |  |  | os |
| `only_flow` | flow 1 |  |  | os |
| `only_earn` | earn 1 |  |  | os |
| `all_equal` | all six at 1 |  |  | os |
| `drop_cam` | all except cam |  |  | os |
| `drop_yday` | all except yday |  |  | os |
| `drop_rsi` | all except rsi |  |  | os |
| `drop_macd` | all except macd |  |  | os |
| `drop_flow` | all except flow |  |  | os |
| `drop_earn` | all except earn |  |  | os |
| `cam_yday` | cam 1, yday 1 |  |  | os |
| `cam_rsi` | cam 1, rsi 1 |  |  | os |
| `cam_macd` | cam 1, macd 1 |  |  | os |
| `cam_flow` | cam 1, flow 1 |  |  | os |
| `cam_earn` | cam 1, earn 1 |  |  | os |
| `yday_rsi` | yday 1, rsi 1 |  |  | os |
| `yday_macd` | yday 1, macd 1 |  |  | os |
| `rsi_macd` | rsi 1, macd 1 |  |  | os |
| `flow_earn` | flow 1, earn 1 |  |  | os |
| `w2_cam` | all six, cam 2 |  |  | os |
| `w2_yday` | all six, yday 2 |  |  | os |
| `w2_rsi` | all six, rsi 2 |  |  | os |
| `w2_macd` | all six, macd 2 |  |  | os |
| `cap_yday_10` | all six at 1 | 10 |  | os |
| `cap_yday_20` | all six at 1 | 20 |  | os |
| `cap_macd_0_5` | all six at 1 |  | 0.5 | os |
| `cap_macd_2` | all six at 1 |  | 2 | os |
| `cap_yday_10_macd_0_5` | all six at 1 | 10 | 0.5 | os |
| `yday_rev` | all six, yday -1 |  |  | os |
| `rsi_mom` | all six at 1 |  |  | mom |

Nothing else is a candidate. Caps and the RSI sign are only the rows above.

## Rank and freeze

Tune sessions, in order:

2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11.

The rank uses price version `clean` only, and only those sessions. For each formula, take the Futubull compound at X=2, X=4, and X=8, then the arithmetic mean of those three. Sort descending. Ties take the mean of the three ex-best compounds, descending, then the formula id ascending.

The freeze writes that order before any 2026-09-14 session is scored. Best is rank 1. Top 10 is ranks 1–10. Top 20 is ranks 1–20. The forward pass does not sort again.

Forward sessions:

2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25.

There is no session between 2026-09-11 and 2026-09-14. The forward book keeps walking from the 2026-08-13 cash state. It does not reset to $10,000.

For the best formula, the equal-weight mean of the top 10, and the equal-weight mean of the top 20, at each X, on each window, on each price version, the report states: Futubull compound, up/down/flat days, days entered, entries, closed trades, win rate, ex-best compound, and the share of formulas in that set with compound strictly above zero. A set of one uses 0 or 1. A formula with fewer than 30 closed trades in that window is flagged `too few` (rule 21). The flag does not remove it from the freeze.

Input recurrence is the count of frozen top-10 and top-20 formulas in which each input has a non-zero weight. `board_list` counts as list rank, not as a camera. The same counts are shown for the subset of that frozen list whose forward compound is positive. That subset is a reading. It does not change the freeze.

## Prices

Two versions. The candidate names do not change. A missing bar can.

`clean` is `research/breadth_rank_v1c/bars/ohlc.parquet` at commit `ad10f862ad51df88f4dd03ff3dbcdf628f7e2685`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. This study does not edit that file. The parquet has 2,067 names. All 77 names in `DROP_LIST.json` are absent. PR #362's text counts 76 unexplained names plus YAAS. The file omits all 77, so version `clean` cannot buy them. ALP, NFE, TNMG, and WCT are the matched splits inside the 77. YAAS is the explained split that is still absent.

`yahoo` is `data/prices/ohlc.parquet`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. Yahoo split-adjusted daily bars. The 77 names are present. Tape features (yesterday, RSI fallback, MACD, flow) are computed from the version being walked. Finviz RSI and `e_pol` do not change with the version.

The rank does not read version `yahoo`. After the freeze, the report also gives version `yahoo` on the tune window for the frozen formulas, and both versions on the forward window. On version `yahoo` it states the dropped names' attributed Futubull dollars divided by all tickers' attributed dollars.

The tune run refuses a bar dated after 2026-09-11. Prior bars used only to build RSI, MACD, yesterday's move, and flow may be older than 2026-08-13.

## Benchmarks

Same sessions, same fees, both price versions, both windows. They are not used to rank.

IWM is buy-and-hold. One buy at the 2026-08-13 open for the whole $10,000, Futubull fee on that buy, marked at each session close, still held on 2026-09-25. No exit fee. A session with no close is missing, not a flat zero. The last mark carries until the next real close, and that close includes the gap.

RANDOM4 draws 4 names from that morning's earliest board, among names with a positive open on that price version. Seed `20260813` plus the draw index, 1000 draws. It sits on a red morning. Hold matches the book: buy the open, sell the close. The report gives the mean and median compound.

## What is reported for a book

Up, down, and flat use the daily account return. A sit with return 0 is flat. Days entered are sessions with at least one buy. Entries are buy fills. A closed trade is a sell in the window. It wins when the round-trip Futubull dollars are positive. Ex-best removes the best ticker's attributed dollars inside the window and compounds again. The best ticker has the largest attributed dollars. Ties take the earlier first entry, then the ticker.

## Records

`freeze/FREEZE.json` is written from the tune rank only. `returns/` is written after that file is committed. A later commit that changes `PREREG.md`, `INPUTS.json`, `DROP_LIST.json`, `FREEZE.json`, or a result file fails CI. Files outside this study and `.github/workflows/factor_mine_score_search_v1.yml` stay as they are on the base. `factor_mine_avg_v1` is not scored again and is not edited here.
