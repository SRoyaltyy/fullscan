# Breadth mine v1 — preregistration

- study: `breadth_mine_v1`
- status: protocol locked. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- owner: Cyrus. Find new long and short rules whose gain is spread across many trades. Big winners stay in the book. Rank on breadth.
- fingerprint_sha256: 2dc65df4207938f3f0325168b68f730576d92b31aab32e1f40fe43941c302531
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- study label: `assumed pre-open, not server-proven`
- live paths: not used. No existing file is rewritten. Nothing under `research/lever_search/` is edited.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `breadth_mine_v1`. It is not a rerun of `factor_mine_seq` or `factor_mine_seq_labelled`. The tries here are added to the luck-test tally. Earlier studies stay frozen.

The input rule is the rule in `research/lever_search_labelled` (PR #358). Any file labelled for trading day D may be used on D. It is assumed received before 09:30 ET. The bytes are the earliest git commit of that path. A later rewrite is not read. No Actions run start and no push-event time is required. `rebuild_match` does not strike. The whole study is labelled `assumed pre-open, not server-proven`.

Bars are `data/prices/ohlc.parquet` at git commit `ff996f535e1343dd739cc801780ae224018bd96c`, blob sha `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, content sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. Yahoo `auto_adjust=false`: split-adjusted, dividends not applied. A feature bar is dated strictly before D. The fill is D's open. D's high, low, and close are not features. The close is the mark only.

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Entry is `open_equal`: leftover cash is split equally across the new names. The flat 15 bp series reprices those same share counts at 7.5 bp per side. Shorts pay 1% annual borrow on the marked notional, `sessions / 252`. No price filter is added. The under-$3 share is the count of fills with fill price strictly under $3, divided by the number of fills, over every session this study writes.

## N

The grid is a bounded slice. It is not the deferred Group 1 figure of 24,595,200. One predicate stands for each Group 1 family in `research/lever_search/PREREG.md` (price, candle, AB-price, Finviz). One predicate stands for each fullscan input named below. The other Group 1 atoms are not in this study. A later study that adds them adds those tries to the tally.

Atoms, in byte order: `a09_sma50`, `ab_good`, `actions_good`, `book_buy`, `catalyst_on`, `cd_engulf_bull`, `fv_week_pos`, `hard_red`, `heat_up`, `hot_pos`, `judge_up`, `last_green`, `predict_up`, `s_gt_0`, `sector_up`. That is 15.

Singles are those 15. Pairs are every unordered pair, 15 × 14 / 2 = 105. Signals = 15 + 105 = 120.

The cross is both sides and the Layer A exit, hold, and top-N lists from PR #351. There is no entry lever, no stop lever, and no regime lever. `cut_loser` and `trail` are the exits, at the fractions that preregistration states.

| lever | values |
| --- | --- |
| side | `long`, `short` |
| exit | `time`, `list`, `cut_loser`, `trail` |
| hold, min_hold | (1,1), (2,1), (2,2), (3,1), (3,2), (3,3), (4,1), (4,2), (4,3), (4,4), (5,1), (5,2), (5,3), (5,4), (5,5) |
| top-N | 1, 4, 8, 12 |

Cross = 2 × 4 × 15 × 4 = 480. **N = 120 × 480 = 57,600.** A sat-out session is not an extra try.

The luck-test denominator is 9,390 + 57,600 = **66,990**. The 9,390 are the labelled study's denominator (the initial study's 9,280 plus that study's 110).

Combination id: `{signal}|{side}|{exit}|h{hold}m{min_hold}|n{top_n}`. A single signal is `f:{atom}`. A pair is `p:{a}+{b}` with `a` < `b` in byte order. Enumeration order is singles in atom order, then pairs in that order, then side, exit, hold pair, top-N in the lists above.

## Atoms

| id | true when |
| --- | --- |
| `last_green` | last bar dated before D has close above open |
| `a09_sma50` | that close is above the mean of the last 50 closes dated before D. False until 50 closes exist |
| `cd_engulf_bull` | `candle_factor` engulfing bull on the last completed bars, at most 8, dated before D |
| `hot_pos` | `ohlc_ripper` hot score on bars dated before D is strictly above 1 |
| `fv_week_pos` | Finviz `Performance (Week)` > 0. A trailing percent sign is stripped. Open, high, low, close, and price columns are not read |
| `s_gt_0` | general predict `total_score` > 0. Every candidate passes, or none does |
| `predict_up` | general predict `predicted_direction` is `up`. Every candidate passes, or none does |
| `hard_red` | general predict `total_score` ≤ −3. Every candidate passes, or none does |
| `sector_up` | the name's sector predict direction is `up` |
| `ab_good` | the name is in both earliest AB files, and `tanh(enriched score / 8)` ≥ 0.05. Part A flags are not read |
| `actions_good` | actions polarity is good, the same ≥ 0.05 cut as the labelled scorer |
| `judge_up` | the judge file's ticker score is > 0 |
| `heat_up` | the name is in the heat file with numeric `d1` > 0 |
| `catalyst_on` | the name is a catalyst dossier target |
| `book_buy` | the name is on a stock_book buy list at any horizon |

Rank, after the gate, is hot score descending, then ticker ascending. Top-N is applied after the gate.

A rule sits out new buys on D when any file its atoms read is absent. `ab_good` needs both AB files. Price, candle, AB-price, and hot score read the pinned bars, which are present every session. A file that exists and matches nobody is a search day with no new buy, not a sit-out. Lots already open still exit. A sit-out inside the run window is not a search day and not a check day.

## Universe and clock

Sessions written, every store session from 2026-08-13 through 2026-09-25:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-23`, `2026-09-24`, `2026-09-25`.

That is 31 sessions. 2026-09-07 is Labor Day and is not a session.

The candidate list on D is the union of stock_book buy and sell names at every horizon, panel tickers whose row is labelled D in the earliest panel blob, catalyst targets, judge tickers, action tickers, and heat tickers. A name with no open on D in the pinned bars is dropped. The panel row's open and close are not read. `panel.json` contributes names only. It is not a feature source.

The sector label is the Finviz `Sector` cell when the export is in the manifest, else the stock_book row's sector. The slug is the lower-case sector with spaces written as underscores. `Financial Services` uses the `financial` file. A name with no sector fails `sector_up` and does not sit the rule out.

Commit and blob sha for every `(input, D)` are in `research/breadth_mine_v1/INPUT_MANIFEST.json`, sha256 `ec5f49855b03b308b475509bdda14a6d0b8e82dc2d9f2458a4f311365a9094dc`. `panel.json` uses the earliest commit whose blob contains rows labelled D. A path with no manifest row is absent on D.

Search days are the 21 run-window sessions from 2026-08-13 through 2026-09-11 on which the rule does not sit out. Check days are the search days on or after 2026-08-20:

`2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`.

The book is one continuous $10,000 account from 2026-08-13. A name already held is not bought again. 2026-09-14 through 2026-09-25 are written and may open a position when the rule's files are present. They are not search days and not a check day.

## Exits

Hold is counted in store sessions. The entry session counts as zero. The same-day high and low are not read.

| exit | sell at the open when |
| --- | --- |
| `time` | sessions held ≥ hold |
| `list` | sessions held ≥ hold, or sessions held ≥ min_hold and the name is outside that morning's candidate list |
| `cut_loser` | sessions held ≥ hold, or sessions held ≥ min_hold and the open is 3% against the entry. A long sells when the open is at or below 97% of the entry. A short sells when the open is at or above 103% of the entry |
| `trail` | sessions held ≥ hold, or sessions held ≥ min_hold and the open is 5% off the favorable extreme. The extreme starts at the entry and then follows prior session closes. A long sells at or below 95% of that extreme. A short sells at or above 105% of it |

## Objective

Fixed before any score. The book walks every session. The ranked number uses check days only.

Primary objective: Futubull compound on the rule's check days after the single best ticker's attributed dollars are removed. Equity starts at $10,000 and follows every session return, including sessions that are not check days. On a check day the removed dollars come out of that day's end equity before the day's return enters the product.

The best ticker has the highest sum of attributed check-day dollars. Ties take the earlier first-entry session, then the ticker.

Also reported, on those check days: median closed-trade return, win rate, share of ticker P&L from the top 1 and the top 3 tickers, and entries per check day. A closed-trade return is the round-trip dollars, both fees included, divided by entry notional. A win is a closed trade with return strictly above zero. Top-1 share is the best ticker's dollars divided by the sum of ticker dollars, and it is defined only when that sum is strictly positive. Top-3 share uses the three largest ticker sums over that same positive sum.

A rule whose top-1 share is above 50% is rejected. It is not ranked and it is not something good. A rule with no positive P&L sum is not rejected by that screen. Its primary number still ranks it.

Rank among rules that are not rejected: primary objective descending, then the full check-day Futubull compound descending, then the combination id ascending. The report lists the top 20 of that ranking.

## Luck test

Raw p is a one-sided Student t-test that the mean check-day Futubull return is above zero. Fewer than two check days cannot reject, and raw p is 1. Adjusted p is `min(1, raw p × 66,990)`. Both are reported. The success line uses the adjusted p.

## Append-only forever

This commit writes none of the return files.

Each session is written once to `research/breadth_mine_v1/returns/{session}.json`. Its sha256 is one line of `research/breadth_mine_v1/returns/manifest.jsonl`. A line is one JSON object with `session`, `file`, `sha256`, and `input_blobs`. Each blob entry has `path` and `blob_sha`. Lines stay in the order they were written. A later line is a later session.

A later rebuild may only add sessions after the last recorded one. `.github/workflows/breadth_mine_v1_append_only.yml` fails the pull request when an existing per-day file, manifest line, or earlier return changes, or when a manifest line is removed or reordered. The check is `python3 -m src.breadth_mine_v1_append --check-against` the pull-request base sha.

`REPORT.md` is computed from those files. It is not a per-day file.

## Success

Fixed before any score. A rule is something good only when every line below holds.

(a) The primary objective, the ex-best-ticker Futubull compound on the rule's check days, is at least 10%.

(b) The median closed-trade return on those check days is strictly above zero.

(c) The win rate of those closed trades is strictly above 50%.

(d) The Futubull compound from 2026-09-14 through 2026-09-25 is greater than or equal to zero.

(e) The adjusted Futubull luck p is below 0.05 on the denominator 66,990. Raw p is reported beside it.

(f) The top-1 ticker share is not above 50%.

A rule that misses any line is not something good. A rule that meets all six lines and has fewer than 10 check days is labelled `too few days to judge` and is not proven. If no rule is proven, the report says `nothing proven yet`. The study label `assumed pre-open, not server-proven` is printed on the report either way.

## Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when the input manifest's sha256 disagrees with the pin above, when a per-day input is read from a blob other than the earliest commit of that path, when a panel row labelled D is read from a blob other than the earliest blob in the manifest, when a feature read includes a bar dated the session or later, when D's high or low is read, when the panel row's open or close is used as a fill or a feature, or when an existing per-day file, manifest line, or earlier return would change.
