# Breadth rank v1b — preregistration

- study: `breadth_rank_v1b`
- status: protocol locked. This commit has no returns, no rank, and no luck-test output.
- written: 2026-09-26
- owner decision: resolve the 57 unexplained bar jumps from source, then rank long rules by after-fee compound on sessions through 2026-09-11. Freeze that rank 1 before any later session is scored.
- fingerprint_sha256: c6aa22dc78ec9385818e768f88fb9944b7ef490b68dc9fbdacf50426822a8093
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- study label: `assumed pre-open, not server-proven`
- creation date: 2026-09-26. Every session below is `designed_after`. None of them is a real record.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `breadth_rank_v1b`. It does not edit `breadth_rank_v1`, `breadth_mine_v1`, `breadth_mine_v1b`, `breadth_mine_v1c`, or `breadth_mine_v1d`, and it does not edit their return files. IRONCLAD 25 requires a new name because the bar file and the selection metric both change. The 40 tries here are added to the luck-test tally. Earlier studies stay frozen.

The input rule is the rule in `research/lever_search_labelled` (PR #358), the same labelled-input rule `breadth_mine_v1` used. Any file labelled for trading day D may be used on D. It is assumed received before 09:30 ET. The bytes are the earliest git commit of that path. A later rewrite is not read. The whole study is labelled `assumed pre-open, not server-proven`.

The lock is nothing fitted after 2026-09-13. No weight is estimated on any day. The score is an equal-weight sum of percentile ranks written in this file. The only choice made after seeing returns is which long rule ranks first on the metric below, using sessions through 2026-09-11 and no later session. That choice is written to `research/breadth_rank_v1b/FROZEN_RANK.json` before any session on or after 2026-09-14 is scored.

## Bars

The cleaned snapshot is `research/breadth_rank_v1b/bars/ohlc.parquet`, sha256 `8ac67b7110176e954c6cc938dea58f821aabef352da6205fa3b79015505a5339`, git blob `eaad5e25fe223314815c690dfd9376b7aa7cbcab`. The download uses `auto_adjust=false`. Open, high, low, and close are the split-adjusted columns Yahoo returned. Dividends are not applied. Adj Close is not stored. The download window is 2026-03-02 inclusive through 2026-09-26 exclusive.

Those bytes match `research/breadth_rank_v1/bars/ohlc.parquet`. Kept names were already that file. The old path is not modified. Jump names were not restored.

Split events are `research/breadth_rank_v1b/bars/splits.json`, sha256 `2bcbb61cc8c13cc0f590480c995f093cd893c268a7a129574c4c7c17c236c091`, git blob `7f67193aac2aab348dd4d4a27248b54e5ab15a4a`.

`breadth_mine_v1b` bars, sha256 `53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797`, have 60 open/previous-close jumps above 3 or below 1/3 and 57 that no Yahoo split on the later date explains. Each of those 57 was re-pulled on 2026-09-26 with the same Yahoo settings. Close and Adj Close were equal, and every jump was still present. Four of them match a Yahoo split dated on a session with no print, between the previous priced close and the next open: ALP 2026-09-10 (split 2026-09-09, 1:50), NFE 2026-09-15 (split 2026-09-14, 1:50), TNMG 2026-09-09 (split 2026-09-08, 1:8), WCT 2026-09-09 (split 2026-09-08, 1:5). The ratio is within 25% of the price factor. The series was not multiplied by that factor. The other 53 jumps match no Yahoo split in that gap. All 52 tickers are dropped. YAAS is also dropped: its 2026-07-30 jump is explained by a split on that date, and rescaling it would patch the price. The table is `research/breadth_rank_v1b/JUMPS.md`. The machine list is `research/breadth_rank_v1b/bars/DROPPED.json`, sha256 `924e0efd448cba93b4500e5d35f67337ba488f56be25941a878e49f04bd33aad`, git blob `93726f0bbf52207b5be4edea5eadf1984b1846c4`.

IRONCLAD 26: the scorer and the workflow exit when any remaining jump above 3 or below 1/3 is not explained by a Yahoo split on the later session's date. After the drops above, that count is zero.

A feature bar is dated strictly before D. D's high, low, and close are not features. The fill is D's open. The close is the mark only, except the `window_end` exit below, which fills at D's close on the last session only.

## Fees and book

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Entry is `open_equal`: leftover cash is split equally across the new names. A long uses cash. A short uses half of equity as the room. The flat 15 bp series reprices those same share counts at 7.5 bp per side. Shorts pay 1% annual borrow on the marked notional, `sessions / 252`, for a lot still held at the close. Sells happen before buys. A name already held is not bought again. A name sold at today's open may be bought again today as a new lot.

## N

The candidate list is 40 rules. That is at most 50. The atoms are the breadth_mine_v1 atom set, in byte order: `a09_sma50`, `ab_good`, `actions_good`, `book_buy`, `catalyst_on`, `cd_engulf_bull`, `fv_week_pos`, `hard_red`, `heat_up`, `hot_pos`, `judge_up`, `last_green`, `predict_up`, `s_gt_0`, `sector_up`.

Each atom is a continuous value, not a gate. A missing value is 0. No rule sits out because a file is absent.

| atom | continuous value |
| --- | --- |
| `last_green` | (close − open) / open of the last bar dated before D. 0 if that open is missing |
| `a09_sma50` | close / mean of the last 50 closes dated before D, minus 1. 0 until 50 closes exist |
| `cd_engulf_bull` | 1 when `candle_factor` says engulfing bull on the last completed bars, at most 8, dated before D. Otherwise 0. The flag itself is binary |
| `hot_pos` | `ohlc_ripper` hot score on bars dated before D |
| `fv_week_pos` | Finviz `Performance (Week)` with a trailing percent sign stripped. 0 when the cell is absent. Open, high, low, close, and price columns are not read |
| `ab_good` | `tanh(enriched score / 8)` when the name is in both earliest AB files. Otherwise 0. The 0.05 cut is not applied |
| `actions_good` | signed actions net, sell and short negative. 0 when the name is absent |
| `book_buy` | 1 when the name is on a stock_book buy list at any horizon. Otherwise 0 |
| `catalyst_on` | 1 when the name is a catalyst dossier target. Otherwise 0 |
| `judge_up` | the judge file's ticker score. 0 when the name is absent |
| `heat_up` | the first numeric heat `d1`. 0 when the name is absent |
| `sector_up` | 1 when the name's sector predict direction is up, −1 when it is down, otherwise 0 |
| `predict_up` | 1 when general predict direction is up, −1 when it is down, otherwise 0. The same number for every name |
| `s_gt_0` | general predict `total_score`, or 0 when the file has no score. The same number for every name |
| `hard_red` | the same `total_score` number as `s_gt_0`. It does not sit the book. The sit is the red-day guard below |

On each day, each atom is turned into a percentile rank: average rank from 1 through the number of names, divided by that count. Ties share the average rank. A rule's score is the sum of the percentile ranks of the atoms in its basket. A constant atom does not change the order.

Baskets, in this order:

| basket | atoms |
| --- | --- |
| `eq15` | all 15 |
| `price` | `a09_sma50`, `cd_engulf_bull`, `hot_pos`, `last_green` |
| `flow` | `ab_good`, `actions_good`, `fv_week_pos`, `heat_up` |
| `story` | `book_buy`, `catalyst_on`, `judge_up`, `sector_up` |
| `hot` | `hot_pos` |
| `heat` | `heat_up` |
| `ab` | `ab_good` |
| `judge` | `judge_up` |
| `week` | `fv_week_pos` |
| `sma` | `a09_sma50` |

The cross is side `long`, `short`, then top-N `4`, `8`. The exit is `time` with hold 1 and min_hold 1. A long buys the highest scores, ticker ascending on a tie. A short buys the lowest scores, ticker ascending on a tie. Top-N is applied to that order. There is no other gate.

**N = 10 × 2 × 2 = 40.** A red-day sit is not an extra try.

Rule id: `{basket}|{side}|time|h1m1|n{top_n}`. Enumeration is basket, then side, then top-N, in the lists above.

The luck-test denominator is 239,830 + 40 = **239,870**. The 239,830 are `breadth_mine_v1` through `breadth_mine_v1d` (239,790) plus the 40 tries in `breadth_rank_v1`. Luck is reported. It is not the rank metric.

## Red-day guard

The book sits out new buys only on a red day, and only by Factor Mine's guard. `S` is general predict `total_score` from the labelled file for D. The threshold is `src.factor_mine_book.HARD_RED`. The skip is `src.factor_mine_combo.hard_red_skip_new(side, HARD_RED_SIT)`. That mode sits both sides. A missing score does not sit. Lots already open still exit. No other sit rule is used.

## Positions that cannot fill

A held name with no positive open on D is closed that morning at its last marked price. The Futubull exit fee is charged. The reason is `missing_bar`. The lot is removed. Later sessions do not mark it, so it cannot carry a constant pnl. This includes a morning when the time exit is due and the open is missing.

On 2026-09-25 only, and only after the rank is frozen, any lot still open after that morning's open exits and new buys is closed at that session's close. A missing close uses the last marked price. The reason is `window_end`. A short still held pays that session's borrow. The round trip is a closed trade. The train phase does not do this on 2026-09-11. The book stays open from the rank window into the later sessions.

The time exit sells at the next session's open once one session has been held. The entry session counts as zero. There is no stop and no target, so one bar is never both.

## Universe and clock

Sessions written, every store session from 2026-08-13 through 2026-09-25:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-23`, `2026-09-24`, `2026-09-25`.

That is 31 sessions. 2026-09-07 is Labor Day and is not a session. 2026-09-12 and 2026-09-13 are not sessions.

The candidate list on D is the union of stock_book buy and sell names at every horizon, panel tickers whose row is labelled D in the earliest panel blob, catalyst targets, judge tickers, action tickers, and heat tickers. A name with no open on D in the pinned bars is dropped. The panel row's open and close are not read. `panel.json` contributes names only.

The sector label is the Finviz `Sector` cell when the export is in the manifest, else the stock_book row's sector. The slug is the lower-case sector with spaces written as underscores. `Financial Services` uses the `financial` file. A name with no sector gets `sector_up` of 0.

Commit and blob sha for every `(input, D)` are in `research/breadth_rank_v1b/INPUT_MANIFEST.json`, sha256 `014f6a9d635dfd3158fabc65081106043ecb12b72d10926b6f774e9e74ff0e01`. `panel.json` uses the earliest commit whose blob contains rows labelled D. A path with no manifest row is absent on D.

The rank window is every session from 2026-08-13 through 2026-09-11. The test sessions are 2026-09-14 through 2026-09-25. While the rank is chosen, the loader's maximum bar date is 2026-09-13. A bar dated on or after 2026-09-14 is not returned. The book is one continuous $10,000 account from 2026-08-13.

## Objective

Fixed before any score. The rank uses the rank window only.

A rule with no entry on that window is `untestable` and is left out of the ranking.

The pick is a long rule. Primary key, descending: Futubull compound on the rank-window sessions. Then the rule id, ascending. The ex-best-ticker compound is reported and is not the key. Short rules are scored with the same compound key and are written in their own section. A short rule is not rank 1.

Rank 1 is that first long rule. It is frozen. It is not replaced. If its closed trades on the rank window are fewer than 30, its label is `too few trades`. Otherwise its label is `ranked`. The keep bar of a win rate strictly above 55% after fees is reported beside it and does not change the pick.

The report, written only after the test sessions are appended, leads with the long rules in that frozen order. Each long rule has one table with a before-2026-09-14 row and an after-2026-09-14 row: Futubull compound, up days, down days, flat days, days entered, entries, closed trades, trade win rate, and the ex-best-ticker compound. Short rules follow in their own section, same columns, ordered by the same pre-2026-09-14 compound. A flat day is a recorded return of zero. A win is a closed trade with return strictly above zero. Top-ticker share is reported when the sum of ticker P&L on that block is strictly positive. Start-day win rate and asymmetric payoff are reported for rank 1. Start-day win rate is the fraction of sessions in the block whose remaining compound is strictly positive. Asymmetric payoff is the average winning closed-trade return divided by the absolute average losing closed-trade return.

The flat 15 bp compound is reported for rank 1 beside the Futubull compound.

## Luck test

Information only. It is not a rank input and it is not a success line.

RANDOM4 is `random.Random(20260813 + draw)` for 1,000 draws, 4 names sampled from that day's labelled universe, long, hold 1, Futubull fees, same missing-bar and window-end rules. IWM is buy-and-hold from the first session's open, Futubull fees, closed at the 2026-09-25 close. Neither baseline sits on a red day. Both are compounded on the same sessions as the block being described. Raw p is a one-sided Student t-test that the mean Futubull return on that block is above zero. Fewer than two sessions cannot reject, and raw p is 1. Adjusted p is `min(1, raw p × 239,870)`.

## Append-only

This commit writes none of the return files and does not write `FROZEN_RANK.json`.

Each session is written once to `research/breadth_rank_v1b/returns/{session}.json`. Its sha256 is one line of `research/breadth_rank_v1b/returns/manifest.jsonl`. A later rebuild may only add sessions after the last recorded one. `.github/workflows/breadth_rank_v1b_append_only.yml` fails the pull request when an existing per-day file, manifest line, or earlier return changes, or when a manifest line is removed or reordered. The same workflow fails when an unexplained 3x jump remains. `breadth_rank_v1` day files are not part of this manifest and are not rewritten.

The train run writes the rank-window files and `FROZEN_RANK.json`, and it refuses to load a bar dated on or after 2026-09-14. The test run requires that frozen file, checks that a fresh rank of the recorded rank-window files still picks the same long rule, and only then appends 2026-09-14 through 2026-09-25. `REPORT.md` is computed from those files. It is not a per-day file.

## Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when a pinned sha256 disagrees, when a per-day input is read from a blob other than the earliest commit of that path, when a panel row labelled D is read from a blob other than the earliest blob in the manifest, when a feature read includes a bar dated the session or later, when D's high or low is read as a feature or a fill, when the panel row's open or close is used as a fill or a feature, when the train phase loads a bar dated on or after 2026-09-14, when the test phase starts without the frozen rank, when an existing per-day file or the frozen rank would change, or when an unexplained consecutive-session jump remains.
