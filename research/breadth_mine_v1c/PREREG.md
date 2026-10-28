# Breadth mine v1c — preregistration

- study: `breadth_mine_v1c`
- status: protocol locked. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- owner: Cyrus. Find new long and short rules whose gain is spread across many trades. Rank on breadth. The book must satisfy the IRONCLAD rules.
- fingerprint_sha256: 8af6c8a21e2c630addcb144314dd221487c5ab1d67b61606302d462553a92b7d
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- study label: `assumed pre-open, not server-proven`
- creation date: 2026-09-26
- live paths: not used. `research/breadth_mine_v1/` and `research/breadth_mine_v1b/` are not edited. Nothing under `research/lever_search/` is edited. `data/prices/ohlc.parquet` is not rewritten.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `breadth_mine_v1c`. `breadth_mine_v1` and `breadth_mine_v1b` stay as written. Their tries still count. A change of rules is a new name. Known errors are not repaired by editing those records.

The input rule is the rule in `research/lever_search_labelled` (PR #358). Any file labelled for trading day D may be used on D. It is assumed received before 09:30 ET. The bytes are the earliest git commit of that path. A later rewrite is not read. The whole study is labelled `assumed pre-open, not server-proven`.

Creation date is 2026-09-26. Every session in this file is before that date, so every session is `designed_after`. Those sessions are hindsight. They do not count as a real locked record. A rule that clears the hindsight bar is labelled `designed_after` and is not proven. The report says `nothing proven yet` unless a rule is proven on sessions on or after the creation date. There are no such sessions in this file.

## IRONCLAD

The attached rules are enforced here. A run that would break one stops.

1. A written session is not rewritten. Past days are not re-picked.
2. The record is append-only.
3. Each written session is sha256-fingerprinted in `manifest.jsonl`. A later run re-checks those fingerprints and fails on a mismatch.
4. This file is the new name. The previous studies stay.
5. Sessions before 2026-09-26 are `designed_after`.
6. This study writes no live ticket. There is no paper send.
7. Sessions are built in order. Day D uses inputs labelled D and the book's state at the prior close.
8. The scorer writes day D's file before it reads day D+1's inputs. A restart replays from the first session, refuses a changed file, and appends only a missing later session. An input that is not in the manifest for that day is not read.
9. Inputs are the earliest committed bytes in the manifest, hashed there.
10. There is no live lookup outside the manifest and the pinned bars.
11. Prices are the Yahoo split-adjusted snapshot below. Finviz open, high, low, close, and price are not read. A name with no session open is dropped. Stooq is not read.
12. Buys fill at the session open. Exits fill at a later session open. The close is the mark only.
13. Same-day high and low are not read, so one bar is never both a stop fill and a target fill.
14. Futubull is the main fee. Flat 15bp is reported beside it.
15. While the search sessions through 2026-09-11 are built, the scorer loads no bar dated 2026-09-14 or later. Those bars are loaded only after the 2026-09-11 file is on disk.
16. The candidate cap, the grid, and the rank metric are this file. Nothing is added after the fingerprint.
17. This fingerprint commit contains no scored test day.
18. Each day's candidate list is at most 50 names. The luck test is best-of-N on the denominator below.
19. Every ranked rule is compared with RANDOM4 and with IWM on that rule's check days, after Futubull fees.
20. The primary number removes the single best ticker.
21. The keep bar is at least 30 fires and a win rate strictly above 55% after fees. Median trade and after-fee P&L are reported beside them. This replaces the earlier win-rate line of 50%.
22. A rule with no entry in the whole walk is `untestable` and is not ranked.
23. No order is sent. Real money waits on about 20 locked sessions after the creation date that beat RANDOM4 and IWM after fees, including with the best ticker removed.
24. CI fails if an earlier session file or manifest line changes, disappears, or is reordered.
25. A later change of inputs, code, recipes, or prices is another new name. Its tries are added to the tally.
26. CI and the scorer fail on a consecutive-session open/previous-close jump above 3 or below 1/3 unless a Yahoo split on the later date explains it. The ratio is within 25% of the price factor `1/split` or of the share factor. This snapshot has 60 such jumps and 57 unexplained. The scorer stops before any return file.

## Bars

Bars are the fresh Yahoo snapshot already pinned by `breadth_mine_v1b`: `research/breadth_mine_v1b/bars/ohlc.parquet`, sha256 `53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797`, git blob `5a7927a31aaf6eb213b31db175a167a79303af69`. Splits: `research/breadth_mine_v1b/bars/splits.json`, sha256 `18e586e42670c4214fffc4f9a40dde340d6ec1bf75a409ac538973f209ca052a`, git blob `c171dbbc11737d4e71d543ee4a87810691e9cf7d`. `auto_adjust=false`. Dividends are not applied. This study does not rewrite those files.

A feature bar is dated strictly before D. The fill is D's open.

## Fees and book

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Entry is `open_equal`. Flat 15bp reprices the same share counts at 7.5bp per side. Shorts pay 1% annual borrow on the marked notional, `sessions / 252`. No price filter. The under-$3 share is fills with price strictly under $3, over all fills.

## N

The grid is the same 57,600 combinations as `breadth_mine_v1`. Atoms, in byte order: `a09_sma50`, `ab_good`, `actions_good`, `book_buy`, `catalyst_on`, `cd_engulf_bull`, `fv_week_pos`, `hard_red`, `heat_up`, `hot_pos`, `judge_up`, `last_green`, `predict_up`, `s_gt_0`, `sector_up`.

Singles 15. Pairs 105. Signals 120. Cross: side 2, exit 4 (`time`, `list`, `cut_loser`, `trail`), hold pairs 15 from (1,1) through (5,5) with min_hold ≤ hold, top-N 1, 4, 8, 12. Cross = 480. **N = 57,600.**

The luck denominator is 9,390 + 57,600 + 57,600 + 57,600 = **182,190**.

Combination id: `{signal}|{side}|{exit}|h{hold}m{min_hold}|n{top_n}`.

## Candidate list

On day D the tradable names are the labelled union that has a positive open. They are ranked by hot score descending, then ticker ascending. The candidate list is the first 50. A shorter list stays shorter. Atoms and top-N run inside that list. The day file records `candidate_n`, and the scorer stops if that number is above 50.

## Clock

Sessions are the 31 store sessions from 2026-08-13 through 2026-09-25. 2026-09-07 is not a session. Search days are run-window sessions through 2026-09-11 on which the rule does not sit out. Check days are search days on or after 2026-08-20. The book is continuous from 2026-08-13.

`cut_loser` is 3% against the entry at the open after min_hold. `trail` is 5% off the favorable extreme of the entry and prior closes. Hold is counted in store sessions. The entry session is zero.

## Objective

Primary: Futubull compound on the rule's check days after the best ticker's check-day dollars are removed. Rank rules that are not `untestable` and whose top-1 share is not above 50%: primary descending, then full check compound descending, then id ascending. The report lists 20.

RANDOM4: `random.Random(20260813 + draw)` for draws 0 through 999. Each draw samples 4 names, or fewer if the candidate list is shorter, from that day's candidate list sorted A to Z. Long, hold 1, exit `time`, same Futubull book. The reported figure is the fraction of draws whose check-day compound is strictly below the rule's, and the mean of those 1000 compounds.

IWM: buy at the 2026-08-13 open, hold, mark at each close, sell at the 2026-09-25 close, Futubull fees on the buy and the sell. The reported figure is the compound of that book's session returns on the rule's check days.

## Success

(a) Ex-best Futubull compound on check days ≥ 10%.

(b) Median closed-trade return on check days > 0.

(c) Win rate of those closed trades > 55%.

(d) Futubull compound from 2026-09-14 through 2026-09-25 ≥ 0.

(e) Adjusted luck p < 0.05 on 182,190. Raw p is reported.

(f) Top-1 share is not above 50%.

(g) Fires ≥ 30. A fire is an entry on a check day.

(h) The rule has at least one entry in the walk. Otherwise it is `untestable` and is not ranked.

A rule that clears (a) through (h) on sessions before the creation date is `designed_after`, not proven. Fewer than 10 check days is `too few days to judge`.

## Consecutive-session jumps

| ticker | date | ratio | split | explained |
| --- | --- | ---: | ---: | --- |
| AIXI | 2026-08-25 | 3.187773 |  | false |
| AIXI | 2026-09-08 | 6.90678 | 0.14285714 | true |
| ALP | 2026-09-10 | 52.121211 |  | false |
| ASTC | 2026-05-27 | 3.004049 |  | false |
| CAPR | 2026-07-27 | 0.29797 |  | false |
| CAST | 2026-06-15 | 3.122581 |  | false |
| CELZ | 2026-06-30 | 3.221427 |  | false |
| CHAI | 2026-06-09 | 4.353658 |  | false |
| CISS | 2026-07-27 | 0.269231 |  | false |
| CLRO | 2026-08-06 | 3.17663 |  | false |
| DSY | 2026-06-10 | 5.206522 |  | false |
| EDHL | 2026-06-11 | 4.722857 |  | false |
| EHGO | 2026-06-17 | 3.643939 |  | false |
| ENGN | 2026-05-07 | 0.242938 |  | false |
| EYPT | 2026-08-17 | 0.275254 |  | false |
| FCUV | 2026-07-31 | 5.43617 |  | false |
| FTRK | 2026-08-28 | 0.14 | 0.05 | false |
| GIPR | 2026-09-18 | 3.386364 |  | false |
| HAO | 2026-05-11 | 0.087671 |  | false |
| HKIT | 2026-06-02 | 0.253799 |  | false |
| HYFM | 2026-08-03 | 5.759259 |  | false |
| ILLR | 2026-03-20 | 0.005 |  | false |
| ILLR | 2026-06-25 | 3.459038 |  | false |
| IPST | 2026-08-17 | 3.6 |  | false |
| JEM | 2026-06-30 | 4.574074 |  | false |
| JZ | 2026-06-02 | 0.222581 |  | false |
| LHSW | 2026-07-06 | 3.777778 |  | false |
| MB | 2026-08-07 | 3.813333 |  | false |
| MGN | 2026-03-26 | 0.099764 |  | false |
| MGN | 2026-09-08 | 0.025 | 0.025 | true |
| MGN | 2026-09-09 | 3.46 |  | false |
| MPU | 2026-09-16 | 19.4 |  | false |
| MYSE | 2026-04-16 | 3.833333 |  | false |
| NAMI | 2026-08-07 | 3.130584 |  | false |
| NFE | 2026-09-15 | 41.484846 |  | false |
| OFAL | 2026-08-12 | 3.562412 |  | false |
| PLAG | 2026-08-12 | 0.232358 |  | false |
| PPCB | 2026-08-27 | 3.280374 |  | false |
| QH | 2026-04-20 | 0.209302 |  | false |
| QH | 2026-04-27 | 30.0 |  | false |
| QNME | 2026-08-04 | 3.566879 |  | false |
| RITR | 2026-09-17 | 0.262812 |  | false |
| SCKT | 2026-08-10 | 6.692308 |  | false |
| SION | 2026-08-10 | 0.095024 |  | false |
| SLBT | 2026-06-16 | 3.366366 |  | false |
| SXTC | 2026-07-23 | 0.202475 |  | false |
| TGHL | 2026-06-01 | 6.091954 |  | false |
| TNMG | 2026-09-09 | 7.91762 |  | false |
| UPC | 2026-06-29 | 5.800676 |  | false |
| VSME | 2026-06-10 | 4.495747 |  | false |
| VWAV | 2026-09-14 | 0.043255 |  | false |
| VWAV | 2026-09-17 | 19.787798 |  | false |
| WCT | 2026-09-09 | 4.8125 |  | false |
| WETO | 2026-07-22 | 0.2325 |  | false |
| WETO | 2026-07-31 | 3.466667 |  | false |
| WOK | 2026-05-13 | 0.186186 |  | false |
| XHG | 2026-08-13 | 6.338798 |  | false |
| YAAS | 2026-07-30 | 5.202703 | 0.2 | true |
| YJ | 2026-08-19 | 3.373563 |  | false |
| ZTG | 2026-09-16 | 3.432392 |  | false |

## Append-only

Each session is written once to `research/breadth_mine_v1c/returns/{session}.json`. `REPORT.md` is not a per-day file. `.github/workflows/breadth_mine_v1c_append_only.yml` runs the append check and the rule 26 test.

Inputs: `research/breadth_mine_v1c/INPUT_MANIFEST.json`, sha256 `37379a33553f4fddba18dd28edd1883bef5c6f4c1a9164cd09a277385a915fbb`.

## Refusal

The run refuses a fingerprint mismatch, a pin mismatch, a manifest blob other than the earliest commit, a feature bar dated on or after the session, a candidate list above 50, an unexplained consecutive-session jump, or a rewrite of an existing session file.
