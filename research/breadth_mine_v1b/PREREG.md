# Breadth mine v1b — preregistration

- study: `breadth_mine_v1b`
- status: protocol locked. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- owner: Cyrus. Find new long and short rules whose gain is spread across many trades. Big winners stay in the book. Rank on breadth.
- fingerprint_sha256: a396ff65a36c54908b28533d1721b730c0fbf9123bf436f17c97ef9c32cb1939
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- study label: `assumed pre-open, not server-proven`
- live paths: not used. `research/breadth_mine_v1/` is not edited. Nothing under `research/lever_search/` is edited. `data/prices/ohlc.parquet` is not rewritten.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `breadth_mine_v1b`. It replaces the bar tape used by `breadth_mine_v1`. That study's preregistration and its returns stay as written. Its 57,600 tries still count in the luck tally.

The input rule is the rule in `research/lever_search_labelled` (PR #358). Any file labelled for trading day D may be used on D. It is assumed received before 09:30 ET. The bytes are the earliest git commit of that path. A later rewrite is not read. No Actions run start and no push-event time is required. `rebuild_match` does not strike. The whole study is labelled `assumed pre-open, not server-proven`.

## Bars

The pinned store `data/prices/ohlc.parquet` at `ff996f535e1343dd739cc801780ae224018bd96c` keeps the first print it stored. REAX did a 1:10 reverse split on 2026-08-25. That file has the 2026-08-19 open at 2.64 and the 2026-08-20 open at 27.50. A fresh Yahoo download with `auto_adjust=false` has the 2026-08-19 open at 26.40 and the 2026-08-20 open at 27.50. This study does not read the pinned store.

Bars are one fresh Yahoo snapshot, `auto_adjust=false` (split-adjusted, dividends not applied), `actions=true`, `repair=false`, yfinance 1.7.0. The window is 2026-03-02 inclusive through 2026-09-26 exclusive. Tickers are the breadth candidate union for the 31 sessions plus every ticker that has a fill in PR #357 or PR #358. SKYT returned no bars.

The snapshot is `research/breadth_mine_v1b/bars/ohlc.parquet`, content sha256 `53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797`, git blob `5a7927a31aaf6eb213b31db175a167a79303af69`. Split events from that same download are `research/breadth_mine_v1b/bars/splits.json`, content sha256 `18e586e42670c4214fffc4f9a40dde340d6ec1bf75a409ac538973f209ca052a`, git blob `c171dbbc11737d4e71d543ee4a87810691e9cf7d`. Yahoo's split column is the share factor. The price factor is `1/split`.

A feature bar is dated strictly before D. The fill is D's open. D's high, low, and close are not features. The close is the mark only.

The jump audit is `research/breadth_mine_v1b/BAR_AUDIT.json`, sha256 `3970bf3e48b1c50bb25c8efd5ecd8651b45b74b0b4154cd9f2ac601d6671be06`. For each ticker the series is open, close, open, close, in date order. A leg is flagged when `close/open` on one bar, or `open` divided by the previous bar's close, is strictly above 3 or strictly below 1/3. A Yahoo split on the later print's date explains the leg when the ratio is within 25% of the price factor or of the share factor. Flagged rows: 96. Unexplained: 93. The report lists every flagged ticker and date. The CI test fails while any flagged leg is unexplained.

## Prior fills

Every fill in PR #357 (`research/lever_search/returns/factor_mine_seq/`) and PR #358 (`research/lever_search_labelled/returns/` at `802625b565817dd4ab54d7d776b52991672d3214`) is an open fill: the recorded price matches the pinned store's session open. A fill would change under the fresh bars when the fresh open is absent, or when the absolute gap is above $0.01 and the relative gap is above 0.1%.

| study | fills | would change | price differs | fresh bar absent |
| --- | ---: | ---: | ---: | ---: |
| #357 | 12812 | 293 | 248 | 45 |
| #358 | 25228 | 447 | 338 | 109 |
| both | 38040 | 740 | 586 | 154 |

The twelve REAX fills in #358 on 2026-08-19 and 2026-08-20 are in that count. The 2026-08-19 buys move from 2.64 to 26.40. The 2026-08-20 sells stay at 27.50.

## Fees

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Entry is `open_equal`: leftover cash is split equally across the new names. The flat 15 bp series reprices those same share counts at 7.5 bp per side. Shorts pay 1% annual borrow on the marked notional, `sessions / 252`. No price filter is added. The under-$3 share is the count of fills with fill price strictly under $3, divided by the number of fills, over every session this study writes.

## N

The grid is the same bounded slice as `breadth_mine_v1`. It is not the deferred Group 1 figure of 24,595,200. One predicate stands for each Group 1 family in `research/lever_search/PREREG.md` (price, candle, AB-price, Finviz). One predicate stands for each fullscan input named below.

Atoms, in byte order: `a09_sma50`, `ab_good`, `actions_good`, `book_buy`, `catalyst_on`, `cd_engulf_bull`, `fv_week_pos`, `hard_red`, `heat_up`, `hot_pos`, `judge_up`, `last_green`, `predict_up`, `s_gt_0`, `sector_up`. That is 15.

Singles are those 15. Pairs are every unordered pair, 15 × 14 / 2 = 105. Signals = 15 + 105 = 120.

The cross is both sides and the Layer A exit, hold, and top-N lists from PR #351. There is no entry lever, no stop lever, and no regime lever.

| lever | values |
| --- | --- |
| side | `long`, `short` |
| exit | `time`, `list`, `cut_loser`, `trail` |
| hold, min_hold | (1,1), (2,1), (2,2), (3,1), (3,2), (3,3), (4,1), (4,2), (4,3), (4,4), (5,1), (5,2), (5,3), (5,4), (5,5) |
| top-N | 1, 4, 8, 12 |

Cross = 2 × 4 × 15 × 4 = 480. **N = 120 × 480 = 57,600.** A sat-out session is not an extra try.

The luck-test denominator is 9,390 + 57,600 + 57,600 = **124,590**. The 9,390 are the labelled study's denominator. The first 57,600 are `breadth_mine_v1`, which still count. The second 57,600 are this study.

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

A rule sits out new buys on D when any file its atoms read is absent. `ab_good` needs both AB files. Price, candle, AB-price, and hot score read the fresh bars, which are present every session. A file that exists and matches nobody is a search day with no new buy, not a sit-out. Lots already open still exit. A sit-out inside the run window is not a search day and not a check day.

## Universe and clock

Sessions written, every store session from 2026-08-13 through 2026-09-25:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-23`, `2026-09-24`, `2026-09-25`.

That is 31 sessions. 2026-09-07 is Labor Day and is not a session.

The candidate list on D is the union of stock_book buy and sell names at every horizon, panel tickers whose row is labelled D in the earliest panel blob, catalyst targets, judge tickers, action tickers, and heat tickers. A name with no open on D in the fresh bars is dropped. The panel row's open and close are not read. `panel.json` contributes names only. It is not a feature source.

The sector label is the Finviz `Sector` cell when the export is in the manifest, else the stock_book row's sector. The slug is the lower-case sector with spaces written as underscores. `Financial Services` uses the `financial` file. A name with no sector fails `sector_up` and does not sit the rule out.

Commit and blob sha for every `(input, D)` are in `research/breadth_mine_v1b/INPUT_MANIFEST.json`, sha256 `88c68f70a217005939c8cbfb14a33e71c1362ea0333d452a98d52669340c1606`. `panel.json` uses the earliest commit whose blob contains rows labelled D. A path with no manifest row is absent on D.

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

Raw p is a one-sided Student t-test that the mean check-day Futubull return is above zero. Fewer than two check days cannot reject, and raw p is 1. Adjusted p is `min(1, raw p × 124,590)`. Both are reported. The success line uses the adjusted p.

## Append-only forever

This commit writes none of the return files.

Each session is written once to `research/breadth_mine_v1b/returns/{session}.json`. Its sha256 is one line of `research/breadth_mine_v1b/returns/manifest.jsonl`. A line is one JSON object with `session`, `file`, `sha256`, and `input_blobs`. Each blob entry has `path` and `blob_sha`. Lines stay in the order they were written. A later line is a later session.

A later rebuild may only add sessions after the last recorded one. `.github/workflows/breadth_mine_v1b_append_only.yml` fails the pull request when an existing per-day file, manifest line, or earlier return changes, or when a manifest line is removed or reordered. The check is `python3 -m src.breadth_mine_v1b_append --check-against` the pull-request base sha. The same workflow fails when a flagged open/close leg in the fresh snapshot is unexplained by a Yahoo split on that date.

`REPORT.md` is computed from those files. It is not a per-day file. It lists every flagged ticker and date from the bar audit.

## Success

Fixed before any score. A rule is something good only when every line below holds.

(a) The primary objective, the ex-best-ticker Futubull compound on the rule's check days, is at least 10%.

(b) The median closed-trade return on those check days is strictly above zero.

(c) The win rate of those closed trades is strictly above 50%.

(d) The Futubull compound from 2026-09-14 through 2026-09-25 is greater than or equal to zero.

(e) The adjusted Futubull luck p is below 0.05 on the denominator 124,590. Raw p is reported beside it.

(f) The top-1 ticker share is not above 50%.

A rule that misses any line is not something good. A rule that meets all six lines and has fewer than 10 check days is labelled `too few days to judge` and is not proven. If no rule is proven, the report says `nothing proven yet`. The study label `assumed pre-open, not server-proven` is printed on the report either way.

## Flagged ticker dates

| ticker | date | leg | ratio | split | explained |
| --- | --- | --- | ---: | ---: | --- |
| ADBT | 2026-09-03 | close_over_open | 0.223684 |  | false |
| AIXI | 2026-08-25 | open_over_prev_close | 3.187773 |  | false |
| AIXI | 2026-09-08 | open_over_prev_close | 6.90678 | 0.14285714 | true |
| AKAN | 2026-04-22 | close_over_open | 3.141538 |  | false |
| ALP | 2026-09-10 | open_over_prev_close | 52.121211 |  | false |
| AMIX | 2026-08-04 | close_over_open | 3.624535 |  | false |
| ASTC | 2026-05-27 | open_over_prev_close | 3.004049 |  | false |
| CAPR | 2026-07-27 | open_over_prev_close | 0.29797 |  | false |
| CAST | 2026-03-10 | close_over_open | 0.276667 |  | false |
| CAST | 2026-06-15 | open_over_prev_close | 3.122581 |  | false |
| CELZ | 2026-06-30 | open_over_prev_close | 3.221427 |  | false |
| CHAI | 2026-06-09 | open_over_prev_close | 4.353658 |  | false |
| CISS | 2026-07-27 | open_over_prev_close | 0.269231 |  | false |
| CLRO | 2026-08-06 | open_over_prev_close | 3.17663 |  | false |
| DSY | 2026-06-10 | open_over_prev_close | 5.206522 |  | false |
| EDHL | 2026-06-11 | open_over_prev_close | 4.722857 |  | false |
| EHGO | 2026-06-17 | open_over_prev_close | 3.643939 |  | false |
| EJH | 2026-03-09 | close_over_open | 0.278889 |  | false |
| ENGN | 2026-05-07 | open_over_prev_close | 0.242938 |  | false |
| EYPT | 2026-08-17 | open_over_prev_close | 0.275254 |  | false |
| FCUV | 2026-07-31 | open_over_prev_close | 5.43617 |  | false |
| FEED | 2026-09-15 | close_over_open | 0.308383 |  | false |
| FIRY | 2026-04-23 | close_over_open | 3.439227 |  | false |
| FTRK | 2026-08-28 | open_over_prev_close | 0.14 | 0.05 | false |
| GIPR | 2026-09-18 | open_over_prev_close | 3.386364 |  | false |
| HAO | 2026-05-11 | open_over_prev_close | 0.087671 |  | false |
| HAO | 2026-07-10 | close_over_open | 0.220245 |  | false |
| HKIT | 2026-03-23 | close_over_open | 0.097436 |  | false |
| HKIT | 2026-06-02 | open_over_prev_close | 0.253799 |  | false |
| HYFM | 2026-08-03 | open_over_prev_close | 5.759259 |  | false |
| ILLR | 2026-03-20 | close_over_open | 150.000003 |  | false |
| ILLR | 2026-03-20 | open_over_prev_close | 0.005 |  | false |
| ILLR | 2026-06-25 | open_over_prev_close | 3.459038 |  | false |
| INHD | 2026-06-08 | close_over_open | 35.576578 |  | false |
| IPST | 2026-08-17 | open_over_prev_close | 3.6 |  | false |
| JEM | 2026-06-30 | open_over_prev_close | 4.574074 |  | false |
| JLHL | 2026-07-09 | close_over_open | 3.428954 |  | false |
| JZ | 2026-06-01 | close_over_open | 3.202479 |  | false |
| JZ | 2026-06-02 | open_over_prev_close | 0.222581 |  | false |
| LBGJ | 2026-07-20 | close_over_open | 0.102778 |  | false |
| LGCL | 2026-08-18 | close_over_open | 0.210435 |  | false |
| LHSW | 2026-07-06 | open_over_prev_close | 3.777778 |  | false |
| LHSW | 2026-09-08 | close_over_open | 0.293333 |  | false |
| LZMH | 2026-04-17 | close_over_open | 0.141463 |  | false |
| MB | 2026-08-07 | open_over_prev_close | 3.813333 |  | false |
| MGN | 2026-03-26 | open_over_prev_close | 0.099764 |  | false |
| MGN | 2026-09-08 | open_over_prev_close | 0.025 | 0.025 | true |
| MGN | 2026-09-09 | open_over_prev_close | 3.46 |  | false |
| MPU | 2026-09-16 | open_over_prev_close | 19.4 |  | false |
| MSGY | 2026-09-25 | close_over_open | 3.788732 |  | false |
| MYSE | 2026-04-16 | open_over_prev_close | 3.833333 |  | false |
| NAMI | 2026-08-07 | open_over_prev_close | 3.130584 |  | false |
| NFE | 2026-09-15 | open_over_prev_close | 41.484846 |  | false |
| NXTT | 2026-08-04 | close_over_open | 0.302632 |  | false |
| OFAL | 2026-08-12 | open_over_prev_close | 3.562412 |  | false |
| OMH | 2026-07-21 | close_over_open | 3.224 |  | false |
| ONCO | 2026-03-27 | close_over_open | 0.332192 |  | false |
| PAAI | 2026-09-17 | close_over_open | 3.553191 |  | false |
| PFSA | 2026-08-18 | close_over_open | 3.115646 |  | false |
| PLAG | 2026-08-11 | close_over_open | 5.429906 |  | false |
| PLAG | 2026-08-12 | open_over_prev_close | 0.232358 |  | false |
| PPCB | 2026-08-27 | open_over_prev_close | 3.280374 |  | false |
| QH | 2026-04-20 | close_over_open | 5.111111 |  | false |
| QH | 2026-04-20 | open_over_prev_close | 0.209302 |  | false |
| QH | 2026-04-27 | open_over_prev_close | 30.0 |  | false |
| QNME | 2026-08-04 | open_over_prev_close | 3.566879 |  | false |
| RCON | 2026-08-05 | close_over_open | 0.153846 |  | false |
| RITR | 2026-09-17 | close_over_open | 0.195 |  | false |
| RITR | 2026-09-17 | open_over_prev_close | 0.262812 |  | false |
| SCKT | 2026-08-10 | open_over_prev_close | 6.692308 |  | false |
| SION | 2026-08-10 | open_over_prev_close | 0.095024 |  | false |
| SLBT | 2026-06-16 | open_over_prev_close | 3.366366 |  | false |
| SMJF | 2026-08-27 | close_over_open | 0.131603 |  | false |
| STAK | 2026-07-24 | close_over_open | 7.536586 |  | false |
| SXTC | 2026-07-23 | open_over_prev_close | 0.202475 |  | false |
| SXTC | 2026-07-24 | close_over_open | 0.25098 |  | false |
| TGHL | 2026-06-01 | open_over_prev_close | 6.091954 |  | false |
| TNMG | 2026-09-09 | open_over_prev_close | 7.91762 |  | false |
| UPC | 2026-06-29 | open_over_prev_close | 5.800676 |  | false |
| VSME | 2026-06-10 | open_over_prev_close | 4.495747 |  | false |
| VWAV | 2026-09-14 | open_over_prev_close | 0.043255 |  | false |
| VWAV | 2026-09-17 | open_over_prev_close | 19.787798 |  | false |
| WCT | 2026-09-09 | open_over_prev_close | 4.8125 |  | false |
| WETO | 2026-07-22 | open_over_prev_close | 0.2325 |  | false |
| WETO | 2026-07-31 | open_over_prev_close | 3.466667 |  | false |
| WOK | 2026-05-13 | open_over_prev_close | 0.186186 |  | false |
| XHG | 2026-08-13 | open_over_prev_close | 6.338798 |  | false |
| XHLD | 2026-08-06 | close_over_open | 3.469136 |  | false |
| YAAS | 2026-07-30 | open_over_prev_close | 5.202703 | 0.2 | true |
| YJ | 2026-08-19 | open_over_prev_close | 3.373563 |  | false |
| YXT | 2026-08-05 | close_over_open | 3.20082 |  | false |
| YYAI | 2026-07-27 | close_over_open | 0.277264 |  | false |
| YYAI | 2026-07-28 | close_over_open | 0.312195 |  | false |
| ZCMD | 2026-05-29 | close_over_open | 0.283531 |  | false |
| ZTG | 2026-08-17 | close_over_open | 0.332994 |  | false |
| ZTG | 2026-09-16 | open_over_prev_close | 3.432392 |  | false |

## Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when the input manifest's sha256 disagrees with the pin above, when the bar snapshot or the split file disagrees with its pin, when a per-day input is read from a blob other than the earliest commit of that path, when a panel row labelled D is read from a blob other than the earliest blob in the manifest, when a feature read includes a bar dated the session or later, when D's high or low is read, when the panel row's open or close is used as a fill or a feature, or when an existing per-day file, manifest line, or earlier return would change.
