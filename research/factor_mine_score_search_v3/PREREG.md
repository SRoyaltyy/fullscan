# factor_mine_score_search_v3 — preregistration

- status: protocol locked. This commit has no returns, no compounds, and no frozen ranking.
- written: 2026-09-26
- study: factor_mine_score_search_v3
- fingerprint_sha256: db4c3de7f8c038e8501764852c3006b65b495c05d3d833065cb5638a241edde7
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which of the 34 morning Score formulas clear a joint bar on the primary book from every required start through 2026-09-11, and which of the frozen best, top 10, and top 20 the 2026-09-14 through 2026-09-25 window can reject.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.
- inputs_sha256: d7e2b96f35b968eab2c902aacfa8e25dc3bba1adf1bd59c93484d3f96645792b
- v1 and v2: `factor_mine_score_search_v1` and `factor_mine_score_search_v2` are not edited. Their preregistrations were already committed.

<!-- BEGIN COVERED -->

## What this study is

v1 ranked 34 Score formulas by the mean Futubull compound across X=2, 4, and 8 on one window. v2 ranked the same 34 by the worse of trade win rate and winning-day share across required fresh starts. Both preregistrations are committed. This study does not change their files. It walks the same 34 ids under the v2 pick rule, with the input rules below locked before any score.

Luck N is 2754. v1 is 34 formulas × X in {2, 4, 8} × 1 start (the window that begins 2026-08-13) = 102. v2 is 34 × 3 × 13 fresh starts = 1326. The 13 starts are the six rank starts and the seven observed starts named below. This study is another 34 × 3 × 13 = 1326. The strict report is not another formula, book size, or start. These 2754 tries are not added to the 9,280 denominator.

## Buy rule

The buy rule is not searched. The only lever is the Score formula. The rank uses the primary book. The strict book is reported beside it and does not sort the freeze.

A morning is red when that variant's morning S is missing or S <= -3. A red morning buys nothing. Lots already held still exit. Primary S is the earliest commit of `01_daily/general/{D}_predict.md` when that file has a total score, else the earliest commit of `01_daily/weather/{D}_weather.json` `signals.general_score`, else missing. Strict S is the #354 `S` row only when `proven=yes`, read from that row's blob. Otherwise strict S is missing and the morning sits.

On a non-red morning the book buys the top X longs by that formula's Score. X is 2, 4, and 8. Each X is walked. X is not a separate candidate.

The fill is the session open. The exit is that session's close. A name with no positive open cannot take a slot. A name with no positive close is bought anyway, marked at its open, and sold at the next session's open. The close is not used to skip the buy.

One $10,000 cash book. Whole shares. Leftover cash is split evenly across the names that can buy one share; a name that cannot is dropped and the split is tried again. Futubull fees use `src/paper_trade.py` `order_fees`. Flat 15bp is one 7.5bp side, shown beside Futubull, and is not the rank key.

A fresh start is a new $10,000 book whose first session is that start and whose last session is 2026-09-11. Cash from before the start is not carried in.

## Input timing

Primary uses the owner rule. A file labelled for trading day D may be used on D. The bytes are the earliest committed version of that path. The label is `assumed pre-open, not server-proven`.

Strict uses the #354 audit `research/audit/FULLSCAN_FILE_PROOF.csv` at commit `1bf94d7dc3ce00c29667d0fae6fdb6bb8effb73c`, blob `832eaa4fdc19d368d87a4dad5c3edd3c200b0a54`, sha256 `1bfafd9d4f9a512b9bc3ac33d6d087349028f57ec4d0fedffe76ee0998e48f75`. This study does not vendor that CSV. An input counts on D only when that input's row has `proven=yes` and a blob. `PROVEN_BUT_CHANGED` counts. The strict read is that blob. On every other day that input sits out.

`yday`, RSI, and MACD histogram are computed from that price version's pinned bars dated before D. They are usable on every session. RSI is Wilder RSI(14) on those bars. It does not read a Finviz RSI column. `flow` is the same bars: 1 when prior relative volume is at least 1.5 and the absolute prior 1-day percent is at most 1.2, else 0. A feature with fewer than five prior bars is missing.

Excel colour is not a score formula. A row with `signal_date` D would trade at the next session's open. The proof list is `research/lever_search/excel_preopen_proof.csv`, sha256 `04e6b996c450427ea408646b305be91c33f0778c1f6ee167de9be1a08d129ded`. Session N uses `signal_date` equal to the previous trading day recorded in that file. Inside this study the usable sessions, each with at least one pre-open signal row and status `PROVEN` or `PROVEN_BUT_CHANGED`, are 2026-08-31, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, and 2026-09-25. Post-open rows stay out even on a proven day: CMII on 2026-09-02, AUBN on 2026-09-04, and SVCC on 2026-09-11. The current `suggestions.csv` is not the pre-open copy. The score grid does not read colour.

Nothing is taken from `data/factor_mine/panel.json` unless that session's rows are in the earliest commit whose blob is labelled for D. That read is primary only. Strict does not read panel.json.

## Earnings surprise

Earnings surprise stays in the grid. The labelled copy is `data/exports/finviz_{D}.csv`, column `EPS Surprise`. There is no separate earnings-surprise path in the #354 input list. Primary uses the earliest commit of that path when the path exists. Strict uses the `export` row only when `proven=yes`, and reads that blob. A surprise above 0.5 is +1. A surprise below -0.5 is -1. Anything else, including a missing ticker, is 0. A same-session earnings clock after 09:30 is not used. 2026-08-26 has no labelled copy on either variant, so earnings surprise sits out that day. The formula ids that mention earn stay in the grid.

## Candidate list

`n` below is the count of `books.1d.buy` names in the pinned blob. A price-only day has no stock_book names in that variant, so `n` is 0 and the walker builds the list from bars.

On a stock_book day the candidate list is that blob's `books.1d.buy` order. `src_rank` is 1 for the first name. Primary stock_book is the earliest commit of `data/stock_book/{D}_stock_book.json` when that path exists and the 1d buy list is not empty. Strict stock_book is the #354 `stock_book` blob when `proven=yes` and that list is not empty.

Otherwise the list is price-only, from that price version's bars dated strictly before D. The shared prior session is the latest bar date in the tape that is before D. Prior-day gainers are the 25 names on that prior session with a positive close-to-close percent, largest percent first, ticker ascending on a tie. Prior-day movers are the 20 names with the largest absolute percent, ticker ascending on a tie. The hot list is the 30 names on that prior session whose `from_bars` hot score is finite, excluding a name whose 5-day percent is above 18 or whose relative volume is above 2.8, highest hot score first, ticker ascending on a tie. The union keeps gainers, then movers not already kept, then hot names not already kept. Overnight is not a column of the bars, so it is not added. `flatten` is not a price-only source. `panel.json` is not the candidate source.

| session | S primary | S strict | primary | n | strict | n | earn primary | earn strict |
| --- | ---: | ---: | --- | ---: | --- | ---: | --- | --- |
| 2026-08-13 | 8.525 | 8.525 | stock_book | 25 | stock_book | 25 | on | on |
| 2026-08-14 | 5.5 | 5.5 | stock_book | 25 | stock_book | 25 | on | on |
| 2026-08-17 | 2.25 | 2.25 | stock_book | 25 | stock_book | 25 | on | on |
| 2026-08-18 | -6.2 | -6.2 | stock_book | 25 | price_only | 0 | on | out |
| 2026-08-19 | -7.2 | -7.2 | stock_book | 25 | price_only | 0 | on | out |
| 2026-08-20 | 1.125 | 1.125 | stock_book | 25 | price_only | 0 | on | out |
| 2026-08-21 | 3.25 | 3.25 | stock_book | 25 | price_only | 0 | on | out |
| 2026-08-24 | 0 | 0 | price_only | 0 | price_only | 0 | on | on |
| 2026-08-25 | 1.8 | missing | price_only | 0 | price_only | 0 | on | on |
| 2026-08-26 | 2.025 | 2.025 | price_only | 0 | price_only | 0 | out | out |
| 2026-08-27 | missing | missing | stock_book | 25 | stock_book | 25 | on | out |
| 2026-08-28 | 0.75 | 0.75 | price_only | 0 | price_only | 0 | on | on |
| 2026-08-31 | -5.85 | -5.85 | stock_book | 25 | stock_book | 25 | on | on |
| 2026-09-01 | -6.3 | -6.3 | stock_book | 10 | price_only | 0 | on | on |
| 2026-09-02 | -3.825 | -3.825 | stock_book | 10 | price_only | 0 | on | on |
| 2026-09-03 | -0.9 | -0.9 | stock_book | 8 | price_only | 0 | on | on |
| 2026-09-04 | 2.25 | 2.25 | stock_book | 8 | stock_book | 16 | on | on |
| 2026-09-08 | -11.475 | missing | stock_book | 2 | stock_book | 2 | on | on |
| 2026-09-09 | -13.95 | missing | stock_book | 3 | stock_book | 4 | on | on |
| 2026-09-10 | -13.275 | -13.275 | stock_book | 3 | stock_book | 1 | on | out |
| 2026-09-11 | 0.5 | 0.5 | stock_book | 3 | stock_book | 2 | on | on |
| 2026-09-14 | -11.002 | -11.002 | stock_book | 2 | stock_book | 1 | on | on |
| 2026-09-15 | -6.215 | -6.215 | stock_book | 10 | stock_book | 4 | on | on |
| 2026-09-16 | 5.297 | 5.297 | stock_book | 12 | stock_book | 12 | on | on |
| 2026-09-17 | 7.383 | 7.383 | stock_book | 17 | stock_book | 17 | on | on |
| 2026-09-18 | 4.861 | 4.861 | stock_book | 24 | stock_book | 24 | on | out |
| 2026-09-21 | 12.871 | 12.871 | stock_book | 11 | stock_book | 12 | on | on |
| 2026-09-22 | -0.497 | -0.497 | stock_book | 4 | stock_book | 4 | on | on |
| 2026-09-23 | 2.293 | 2.293 | stock_book | 15 | stock_book | 15 | on | out |
| 2026-09-24 | -7.659 | -7.659 | stock_book | 7 | stock_book | 7 | on | out |
| 2026-09-25 | 2.706 | 2.706 | stock_book | 19 | price_only | 0 | on | out |

`cam` is `cond_good - cond_bad` from the earliest `panel.json` commit that contains rows labelled for D, joined by ticker. A candidate that is not on that panel has a missing camera. Strict leaves `cam` missing on every name. `board_list` uses the candidate list's `src_rank`, not the panel rank.

## Score grid

The 34 ids, in lock order, are the v1 grid. Nothing else is a candidate. Excel colour is not one of them.

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

`board_list` is `100 - src_rank`. Ties take the ticker ascending. A within-morning z-score uses the population standard deviation. Fewer than two finite values, or zero variance, contributes 0. A missing value contributes 0 after the z-score. Caps clip yesterday's percent and the MACD histogram before the z-score. `rsi` is `50 - RSI`. `rsi_mom` uses `RSI - 50`.

## Non-red sessions and starts

Tune sessions end 2026-09-11. There is no 2026-09-12 or 2026-09-13. Forward sessions are 2026-09-14 through 2026-09-25. The tune rank does not read a bar dated after 2026-09-11. Prior bars used only to build RSI, MACD, yesterday's move, flow, and a price-only list may be older than 2026-08-13.

Primary morning S is present and above -3 on these tune sessions, and only these:

2026-08-13, 2026-08-14, 2026-08-17, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-28, 2026-09-03, 2026-09-04, 2026-09-11.

2026-08-24 scores 0, so it is not red. 2026-08-27 has no primary S, so it sits. The file sha256 is `d7e2b96f35b968eab2c902aacfa8e25dc3bba1adf1bd59c93484d3f96645792b`.

A same-day round trip closes at most one trade per filled name per session. The maximum closed trades from a start, at a book size X, is X times how many of those non-red sessions fall on or after the start. That maximum is known from S. It is not a score.

Rank starts, each a fresh book through 2026-09-11:

2026-08-20, 2026-08-21, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28.

At X=8 those starts can reach 30 closed trades (9, 8, 6, 5, 4, and 4 non-red sessions). At X=4, 2026-08-20 can (9 times 4 is 36) and 2026-08-21 can (8 times 4 is 32). At X=2 none can (12 times 2 is 24).

Observed starts, also fresh books through 2026-09-11, and not in the rank key, because X=8 cannot reach 30:

2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11.

Required cells: 2026-08-20 X=4; 2026-08-20 X=8; 2026-08-21 X=4; 2026-08-21 X=8; 2026-08-25 X=8; 2026-08-26 X=8; 2026-08-27 X=8; 2026-08-28 X=8.

## Pick rule

Using only mornings through 2026-09-11, on price version `clean`, on the primary book.

A closed trade is a sell in that fresh book's window. It wins when the round-trip Futubull dollars are positive. Trade win rate is wins divided by closed trades.

A day is traded when that session has a buy or a sell. An up day has a Futubull daily account return strictly above zero. Winning-day share, written W/T, is up days divided by days traded. A sit is not a traded day and is not an up day.

The joint metric of a cell is the minimum of trade win rate and winning-day share. A cell with fewer than 30 closed trades has no joint metric. It is flagged `too few` and kept in the freeze file.

A formula is a passer when every required cell has at least 30 closed trades. Its rank key is the minimum joint metric across those required cells. That is the worse of the two metrics at the worst required cell.

Sort passers first, by rank key descending, then by the mean of the required joint metrics descending, then by formula id ascending. Formulas that miss at least one required cell sort after every passer, by the same keys on the required cells they did clear, then by how many required cells cleared, then by id. X=2 and the observed starts are walked, flagged when under 30, and left out of the rank key.

Best is position 1 of that order. Top 10 is positions 1–10. Top 20 is positions 1–20. The freeze writes the full order, the passer list, and every primary cell, before any 2026-09-14 session is scored. The forward pass does not sort again. Strict cells are not in the freeze and are not in the rank key.

## Forward report

The 2026-09-14 through 2026-09-25 check can only reject a formula. With fewer than 30 closed trades it cannot prove one. It cannot add a formula and it cannot change best, top 10, or top 20.

A frozen formula is `rejected` on that window when the continuous primary clean book has at least 30 closed trades there and the joint metric is strictly below 0.5. It is `proven` when that window has at least 30 closed trades and the joint metric is at least 0.5. Fewer than 30 is `unproven`. Unproven is not a rejection. The frozen ids carry forward unchanged, append-only, from the next session on or after 2026-09-28. A formula counts as proven on that forward record only once the append-only record reaches 30 closed trades. This commit has no 2026-09-28 bar and does not score that session.

The forward book is one continuous $10,000 walk from 2026-08-13 through 2026-09-25. It does not reset on 2026-09-14. It does not use the fresh-start cash paths. For best, for the top 10, and for the top 20, at each X, on the tune slice and on the forward slice, on each price version that is scored, on primary and on strict, the report states: Futubull compound, flat 15bp compound, win rate, W/T, up, down, flat, days entered, entries, closed trades, ex-best compound, and the share of formulas in that set with compound strictly above zero. A set of one uses 0 or 1. A formula with fewer than 30 closed trades in that window is flagged and kept.

IWM is buy-and-hold from the 2026-08-13 open, Futubull fee on that buy, marked at each session close, still held on 2026-09-25. No exit fee. A session with no close is missing, not a flat zero. RANDOM4 draws 4 names from that variant's candidate list, among names with a positive open, seed `20260813` plus the draw index, 1000 draws. It sits on a red morning for that variant. Hold matches the book. The report gives the mean and median compound. Benchmarks are not used to rank.

## Prices and the jump halt

Two versions. The stock_book names do not change between them. A price-only list can.

`clean` is `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` at `8e4683395`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. This study does not edit that file. The 77 names in `DROP_LIST.json` are absent from it.

`yahoo` is `data/prices/ohlc.parquet`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. The 77 names stay in that pin. They are not deleted.

`DROP_LIST.json` sha256 is `dfe06848fe878943ce98dfa4a65f6500cd6c39e0a93b3fc88dd71ce00d65c0db`. It is the 77 names in `research/breadth_rank_v1c/bars/DROPPED.json` (sha256 `4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad`), which are the 76 tickers in `research/breadth_rank_v1c/JUMPS.md` (sha256 `8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a`) plus YAAS. ALP, NFE, TNMG, and WCT are the matched splits inside that list. They are not rescaled.

Before a price version is scored, every bar dated on or before the walk's last session, for IWM and for every primary or strict candidate ticker that is not in the 77, is checked. A leg is that session's open divided by the previous close, or the close divided by the same bar's open. A ratio above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) has a split on that ticker and that later session's date, and the ratio is within 25% of the split or of one divided by the split. The known 77 are not a new halt. Any other unexplained jump halts that price version: no compound is written for it. If the cleaned tape halts, no freeze is written. A Yahoo halt does not change the freeze. The rank uses `clean` only.

## Records

`INPUTS.json` holds the pinned blobs, morning S, candidate source, camera rows, and earnings polarity. It does not hold a compound. `freeze/FREEZE.json` is written from the primary tune rank only. `returns/` is written after that file is committed. A later commit that changes `PREREG.md` fails the lock. Files outside this study and `.github/workflows/factor_mine_score_search_v3.yml` stay as they are on the base.
