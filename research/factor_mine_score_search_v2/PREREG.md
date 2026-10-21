# factor_mine_score_search_v2 — preregistration

- status: protocol locked. This commit has no returns, no compounds, and no frozen ranking.
- written: 2026-09-26
- study: factor_mine_score_search_v2
- fingerprint_sha256: eeca5d61cf6f2f6d6723b4e084fe1b560e5b7ff455e8e5f6c68059e91ce6cb55
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which of the same 34 morning Score formulas clear a joint bar on trade win rate and winning-day share from every required start through 2026-09-11, and what the frozen best, top 10, and top 20 do from 2026-09-14 through 2026-09-25.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.
- inputs_sha256: 6016c796f50c7d2f4ba6fa10d69ba665e8b17d6705a7c6ad7ebefe65973cf1d3
- v1: `factor_mine_score_search_v1` is not edited. Its preregistration and its compound pick rule stay as they were.

<!-- BEGIN COVERED -->

## What this study is

The buy rule and the 34 Score formulas are the v1 grid. The pick rule is new. v1 ranked by the mean Futubull compound across X=2, 4, and 8. This study does not. It does not change v1's files.

Luck N is 68. That is the 34 formulas tried in v1 plus the same 34 formulas tried again here. A start date is not a new formula. A book size is not a new formula. These 68 tries are not added to the 9,280 denominator.

## Buy rule

The buy rule is not searched. The only lever is the Score formula.

Each morning is one session in `INPUTS.json`. The name list, `src_rank`, `cond_good`, and `cond_bad` are the rows in the earliest commit on `origin/main` whose `data/factor_mine/panel.json` contains that session. A later rewrite of the panel is not used. `e_pol` is stamped with `src/factor_mine_probe.py` `attach_erd_polarity` on those rows. Morning S is `src/factor_mine_book.py` `morning_s` on this tree, stored in `INPUTS.json`. The file sha256 is `6016c796f50c7d2f4ba6fa10d69ba665e8b17d6705a7c6ad7ebefe65973cf1d3`.

A morning is red when S is missing or S <= -3. A red morning buys nothing. Lots already held still exit. 2026-08-27 has no S in the snapshot, so it sits.

On a non-red morning the book buys the top X longs by that formula's Score. X is 2, 4, and 8. Each X is walked. X is not a separate candidate.

The fill is the session open. The exit is that session's close. A name with no positive open cannot take a slot. A name with no positive close is bought anyway, marked at its open, and sold at the next session's open. The close is not used to skip the buy.

One $10,000 cash book. Whole shares. Leftover cash is split evenly across the names that can buy one share; a name that cannot is dropped and the split is tried again. Futubull fees use `src/paper_trade.py` `order_fees`. Flat 15bp is one 7.5bp side, shown beside Futubull, and is not the rank key.

A fresh start is a new $10,000 book whose first session is that start and whose last session is 2026-09-11. Cash from before the start is not carried in.

## Score grid

The 34 ids, in lock order, are the v1 grid. Nothing else is a candidate.

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

Features, higher meaning a higher long score, match v1. `board_list` is `100 - src_rank`, or `100 - 99` when `src_rank` is missing. Ties take the ticker ascending. A within-morning z-score uses the population standard deviation. Fewer than two finite values, or zero variance, contributes 0. A missing value contributes 0 after the z-score.

- `cam`: `cond_good - cond_bad`.
- `yday`: prior close-to-close percent from that price version's bars dated before the session. Caps clip this percent before the z-score.
- `rsi`: `50 - RSI` so a more oversold name scores higher. `rsi_mom` uses `RSI - 50`. RSI is the prior Finviz export's `Relative Strength Index (14)` when that prior file has a number, else Wilder RSI(14) on that price version's prior bars.
- `macd`: MACD histogram (12, 26, 9) on those same prior bars.
- `flow`: 1 when prior relative volume is at least 1.5 and the absolute prior 1-day percent is at most 1.2, else 0.
- `earn`: 1 when `e_pol` is `good`, -1 when `bad`, else 0.

## Non-red sessions and starts

Tune sessions end 2026-09-11. There is no 2026-09-12 or 2026-09-13. Forward sessions are 2026-09-14 through 2026-09-25. The tune rank does not read a bar dated after 2026-09-11. Prior bars used only to build RSI, MACD, yesterday's move, and flow may be older than 2026-08-13.

Stored morning S is present and above -3 on these tune sessions, and only these:

2026-08-13, 2026-08-14, 2026-08-17, 2026-08-20, 2026-08-21, 2026-08-25, 2026-08-26, 2026-08-28, 2026-09-03, 2026-09-04, 2026-09-11.

A same-day round trip closes at most one trade per filled name per session. The maximum closed trades from a start, at a book size X, is X times how many of those non-red sessions fall on or after the start. That maximum is known from S. It is not a score.

Rank starts, each a fresh book through 2026-09-11:

2026-08-20, 2026-08-21, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28.

At X=8 those starts can reach 30 closed trades (8, 7, 6, 5, 4, and 4 non-red sessions). At X=4 only 2026-08-20 can (8 times 4 is 32). At X=2 none can (11 times 2 is 22).

Observed starts, also fresh books through 2026-09-11, and not in the rank key, because X=8 cannot reach 30:

2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11.

Required cells: 2026-08-20 X=4; 2026-08-20 X=8; 2026-08-21 X=8; 2026-08-25 X=8; 2026-08-26 X=8; 2026-08-27 X=8; 2026-08-28 X=8.

## Pick rule

Using only mornings through 2026-09-11, on price version `clean`.

A closed trade is a sell in that fresh book's window. It wins when the round-trip Futubull dollars are positive. Trade win rate is wins divided by closed trades.

A day is traded when that session has a buy or a sell. An up day has a Futubull daily account return strictly above zero. Winning-day share, written W/T, is up days divided by days traded. A sit is not a traded day and is not an up day.

The joint metric of a cell is the minimum of trade win rate and winning-day share. A cell with fewer than 30 closed trades has no joint metric. It is flagged `too few` and kept in the freeze file.

A formula is a passer when every required cell has at least 30 closed trades. Its rank key is the minimum joint metric across those required cells. That is the worse of the two metrics at the worst required cell.

Sort passers first, by rank key descending, then by the mean of the required joint metrics descending, then by formula id ascending. Formulas that miss at least one required cell sort after every passer, by the same keys on the required cells they did clear, then by how many required cells cleared, then by id. X=2 and the observed starts are walked, flagged when under 30, and left out of the rank key.

Best is position 1 of that order. Top 10 is positions 1–10. Top 20 is positions 1–20. The freeze writes the full order, the passer list, and every cell, before any 2026-09-14 session is scored. The forward pass does not sort again.

## Forward report

The forward book is one continuous $10,000 walk from 2026-08-13 through 2026-09-25. It does not reset on 2026-09-14. It does not use the fresh-start cash paths. For best, for the top 10, and for the top 20, at each X, on the tune slice and on the forward slice, on each price version that is scored, the report states: Futubull compound, flat 15bp compound, win rate, W/T, up, down, flat, days entered, entries, closed trades, ex-best compound, and the share of formulas in that set with compound strictly above zero. A set of one uses 0 or 1. A formula with fewer than 30 closed trades in that window is flagged and kept.

IWM is buy-and-hold from the 2026-08-13 open, Futubull fee on that buy, marked at each session close, still held on 2026-09-25. No exit fee. A session with no close is missing, not a flat zero. RANDOM4 draws 4 names from that morning's earliest board, among names with a positive open, seed `20260813` plus the draw index, 1000 draws. It sits on a red morning. Hold matches the book. The report gives the mean and median compound. Benchmarks are not used to rank.

## Prices and the jump halt

Two versions. The candidate names do not change between them.

`clean` is `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` at `8e4683395`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. This study does not edit that file. The 77 names in `DROP_LIST.json` are absent from it.

`yahoo` is `data/prices/ohlc.parquet`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. The 77 names stay in that pin. They are not deleted.

`DROP_LIST.json` sha256 is `dfe06848fe878943ce98dfa4a65f6500cd6c39e0a93b3fc88dd71ce00d65c0db`. It is the 77 names in `research/breadth_rank_v1c/bars/DROPPED.json` (sha256 `4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad`), which are the 76 tickers in `research/breadth_rank_v1c/JUMPS.md` (sha256 `8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a`) plus YAAS. ALP, NFE, TNMG, and WCT are the matched splits inside that list. They are not rescaled.

Before a price version is scored, every bar dated on or before the walk's last session, for IWM and for every morning-board ticker that is not in the 77, is checked. A leg is that session's open divided by the previous close, or the close divided by the same bar's open. A ratio above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) has a split on that ticker and that later session's date, and the ratio is within 25% of the split or of one divided by the split. The known 77 are not a new halt. Any other unexplained jump halts that price version: no compound is written for it. If the cleaned tape halts, no freeze is written. A Yahoo halt does not change the freeze. The rank uses `clean` only.

## Records

`freeze/FREEZE.json` is written from the tune rank only. `returns/` is written after that file is committed. A later commit that changes `PREREG.md` fails the lock. Files outside this study and `.github/workflows/factor_mine_score_search_v2.yml` stay as they are on the base.
