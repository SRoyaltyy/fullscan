# Factor Mine sequential labelled study — preregistration

- study: `factor_mine_seq_labelled`
- status: protocol locked. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- owner: Cyrus, 2026-09-26. Any input file labelled for trading day D may be used on D. It is assumed received before 09:30 ET. This study does not require a GitHub server time. The study label is `assumed pre-open, not server-proven`.
- fingerprint_sha256: 111b1c2ae9433bb91aad0c67619ab380fc8172989a00ef0028510fa4f0975fa3
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- follows: [research/lever_search/PREREG.md](../lever_search/PREREG.md) at fullscan `8608a8fea760148b010e645ff573fe12154f0e04` (PR #351), except where this file says otherwise. Recipes are `build_recipes()` at `8e8c36a7117e60040d04cfa598e503b82fd7c6ea`.
- live paths: not used. No edits under `research/lever_search/`. No existing file is rewritten.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `factor_mine_seq_labelled`. It is not a rerun of `factor_mine_seq`. The 110 tries here are added to the luck-test tally. The earlier study stays frozen.

The walk is the original Factor Mine engine from commit `8e8c36a7117e60040d04cfa598e503b82fd7c6ea`: one session at a time, previous positions carried forward, a past buy or sell not re-picked, no locked file rewritten. The 110 recipes are that commit's `build_recipes()`, in the order below. `src/lever_search_proof.py` `build_group3_recipes()` is that list. There is no re-picking.

Owner decision, Cyrus, 2026-09-26: an input file labelled for trading day D may be used on D, and it is assumed received before 09:30 ET. No Actions run start and no push-event time is required. `rebuild_match` does not strike a recipe. The whole study is labelled `assumed pre-open, not server-proven`. That label is not a per-recipe strike.

Everything else follows `research/lever_search/PREREG.md`. Fees, fills, the cash book, the two fee columns, the share of fills under $3, and the four success lines are that protocol. The departures are this file.

## Recipes

N = 110. One try each. A sat-out session is not an extra try.

1. `union_h1` (long)
2. `union_h3` (long)
3. `union_h5` (long)
4. `flatten_h1` (long)
5. `flatten_h3` (long)
6. `flatten_h5` (long)
7. `probable_h1` (long)
8. `probable_h3` (long)
9. `probable_h5` (long)
10. `yday_gainer_h1` (long)
11. `yday_gainer_h3` (long)
12. `yday_gainer_h5` (long)
13. `ohlc_hot_h1` (long)
14. `ohlc_hot_h3` (long)
15. `ohlc_hot_h5` (long)
16. `union_vol_g_h1` (long)
17. `union_vol_g_h3` (long)
18. `union_vol_missing_h1` (long)
19. `union_vol_missing_h3` (long)
20. `union_ab_g_h1` (long)
21. `union_ab_g_h3` (long)
22. `union_join_g_h1` (long)
23. `union_join_g_h3` (long)
24. `union_join_present_h1` (long)
25. `union_join_present_h3` (long)
26. `union_news_g_h1` (long)
27. `union_news_g_h3` (long)
28. `union_news_present_h1` (long)
29. `union_news_present_h3` (long)
30. `union_news_missing_h1` (long)
31. `union_news_missing_h3` (long)
32. `union_catal_present_h1` (long)
33. `union_catal_present_h3` (long)
34. `union_blue_h1` (long)
35. `union_blue_h3` (long)
36. `union_white_h1` (long)
37. `union_white_h3` (long)
38. `union_last_green_h1` (long)
39. `union_last_green_h3` (long)
40. `union_last_red_h1` (long)
41. `union_last_red_h3` (long)
42. `union_candle_h1` (long)
43. `union_candle_h3` (long)
44. `union_coil_off_h1` (long)
45. `union_coil_off_h3` (long)
46. `union_earn_react_h1` (long)
47. `union_earn_react_h3` (long)
48. `union_e_fresh_h1` (long)
49. `union_e_fresh_h3` (long)
50. `union_r_up_h1` (long)
51. `union_r_up_h3` (long)
52. `union_break10_h1` (long)
53. `union_break10_h3` (long)
54. `union_vol_g_h5` (long)
55. `union_coil_off_h5` (long)
56. `union_last_green_h5` (long)
57. `union_news_g_h5` (long)
58. `union_white_h5` (long)
59. `union_vol_ab_h1` (long)
60. `union_vol_ab_h3` (long)
61. `union_blue_vol_h1` (long)
62. `union_blue_vol_h3` (long)
63. `union_news_vol_h1` (long)
64. `union_news_vol_h3` (long)
65. `union_e_green_h1` (long)
66. `union_e_green_h3` (long)
67. `probable_probable_ok_h1` (long)
68. `probable_probable_ok_h3` (long)
69. `union_vol_green_h1` (long)
70. `union_vol_green_h3` (long)
71. `union_coil_green_h1` (long)
72. `union_coil_green_h3` (long)
73. `union_blue_coil_h1` (long)
74. `union_blue_coil_h3` (long)
75. `union_join_vol_green_h1` (long)
76. `union_join_vol_green_h3` (long)
77. `union_white_coil_h1` (long)
78. `union_white_coil_h3` (long)
79. `flatten_vol_g_h3` (long)
80. `ohlc_hot_coil_h1` (long)
81. `union_hot_score_h1` (long)
82. `union_hot_score_h3` (long)
83. `union_candle_score_h1` (long)
84. `union_candle_score_h3` (long)
85. `union_ret_5_h1` (long)
86. `union_ret_5_h3` (long)
87. `union_cond_h1` (long)
88. `union_cond_h3` (long)
89. `union_w_hot_cond_h1` (long)
90. `union_w_hot_cond_h3` (long)
91. `union_w_hot_candle_h1` (long)
92. `union_w_hot_candle_h3` (long)
93. `union_hot_n4_h1` (long)
94. `union_hot_n12_h1` (long)
95. `union_cond_n4_h3` (long)
96. `union_h3_exit_alarm` (long)
97. `union_h5_exit_alarm` (long)
98. `union_h3_exit_red` (long)
99. `union_h3_exit_news_r` (long)
100. `coil_h3_exit_alarm` (long)
101. `short_alarm_h1` (short)
102. `short_alarm_h3` (short)
103. `short_news_r_h1` (short)
104. `short_news_r_h3` (short)
105. `short_r_down_h1` (short)
106. `short_r_down_h3` (short)
107. `short_extended_h1` (short)
108. `short_extended_h3` (short)
109. `short_last_red_h1` (short)
110. `short_last_red_h3` (short)

## Inputs

For each input and each session D, the bytes are the first git commit that contains the file, or the rows, labelled D. A later commit that rebuilds the same day is not read. Commit and blob sha for every `(input, D)` are in `research/lever_search_labelled/INPUT_MANIFEST.json`, sha256 `3f7245816a70327871e52a87f1e547f1201a3ab7f48af14227dae548ea45d571`.

The labelled inputs are:

| input | path |
| --- | --- |
| panel | `data/factor_mine/panel.json` rows whose `date` is D |
| stock_book | `data/stock_book/{D}_stock_book.json` |
| actions | `01_daily/news/{D}_actions.json` |
| ab_checklist | `data/ab_checklist/{D}_ab_checklist.csv` |
| ab_enriched | `data/ab_checklist/{D}_ab_checklist_enriched.csv` |
| export | `data/exports/finviz_{D}.csv` |
| catalyst | `01_daily/catalyst/{D}_dossiers.json` |
| join | `data/join/{D}_ranked.csv` |

A path that is not in the tree has no manifest row. That input is absent on D.

`data/factor_mine/panel.json` was added in commit `f5b46d20eec2a7acf5df3924fc5201023d0dcd40`, blob `d0e427d434d6d30da22ea6bf31133ed4648fb277`. That first blob already contains rows labelled 2026-08-13 through 2026-09-08. Those rows are the earliest version of those dates. Later commits are the earliest version only of the dates that first appear in them: 2026-09-09, 2026-09-10, 2026-09-11, and 2026-09-14 through 2026-09-25. A later blob's copy of an earlier date is not read. The manifest records the commit and blob for each date, and `n_rows` is the count of rows labelled that date in that blob. Several later dates are thin in the earliest blob (2026-09-16 has 4 rows, 2026-09-17 has 6, 2026-09-18 has 4). The fuller later rebuild is not used.

Every session from 2026-08-13 through 2026-09-25 has an earliest panel blob that contains rows labelled that day. The per-day source-file candidate list is therefore not used. Had a session lacked that earlier version, this file would say so, and the candidate list would be the initial study's stock_book buy list on a day that file exists, otherwise the price-only lists (prior-day gainers, prior-day movers, the hot list, continuation, and overnight) from the pinned bars.

## How a row is built

The candidate list on D is the rows labelled D in the pinned earliest panel blob, in that blob's order. `sources`, `src_rank`, `blue`, `alarm`, `zero_red`, and every camera box other than `vol`, `news`, and `ab` stay on that row. That includes `join` and `catal`.

These fields are replaced before any gate:

- Price features come from the pinned bar file, bars dated strictly before D: `ohlc_ret_1`, `ohlc_ret_5`, `ohlc_ret_10`, `ohlc_rvol`, `ohlc_hot_score`, `ohlc_break_10`, `last_green`, `last_red`, `candle_score`, `candle_capture`, and `rs_week` (the name's `ret_5` minus SPY `ret_5` when both exist). `boxes.vol` is that rvol: `good` at or above 1.5, `bad` below 0.7, `missing` when the feature is missing, otherwise `neutral`.
- When the actions file is in the manifest, `boxes.news` is the polarity of the earliest blob, same sign rule as the initial scorer. A ticker absent from that file is `missing`. When the file is absent, `boxes.news` is `missing`.
- When both AB files are in the manifest, `boxes.ab` is the polarity of `tanh(enriched score / 8)` for tickers present in both earliest blobs. Otherwise `boxes.ab` is `missing`. AB Part A is not recomputed.
- When the export file is in the manifest, `erd_earn_react`, `erd_days_since_E`, `erd_days_since_R`, `erd_flag_E`, and `erd_flag_R` are `asof_snapshot` of that earliest export as of D. Otherwise those fields are empty: `earn_react` false, day counts null, flags 0.

`cond_good` is the count of boxes other than `yday` whose tone is `good`, and `cond_bad` is the count whose tone is `bad`, both recounted after the replacements. The panel row's `open` and `close` are not read.

The earliest stock_book, join, and catalyst blobs are presence pins. Their bytes are not substituted over the earliest panel row. Actions, AB, and export are read, because those are the attachments the initial scorer parsed.

A role is present on D when its labelled file is in the manifest. `ab` is present only when both AB files are. The required roles are `required_roles()` in `src/lever_search_proof.py`, the same map as the initial study.

## Window

Sessions written, every store session from 2026-08-13 through 2026-09-25:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-23`, `2026-09-24`, `2026-09-25`.

That is 31 sessions. 2026-09-07 is Labor Day and is not a session. The four sessions before 2026-08-13 that the initial run wrote (2026-08-07, 2026-08-10, 2026-08-11, 2026-08-12) are outside this study.

The run window, which is the search calendar, is the 21 sessions from 2026-08-13 through 2026-09-11. The check calendar is the same 16 sessions as PR #351, every run-window session from 2026-08-20 through 2026-09-11:

`2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`.

Search days are the run-window sessions on which every role the recipe requires is present. Check days are those search days that also sit on the 16-session check calendar. A sit-out carries the locked book, opens no new position, and is not a search day and not a check day.

2026-09-14 through 2026-09-25 are the from-09-14 line. They are written, and the book may open a position on one of them when every required role is present that morning. They are not search days and not check days.

Files absent inside the 31 sessions:

- stock_book: 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-28.
- ab_checklist: 2026-08-13, 2026-08-14, 2026-08-17, 2026-08-26, 2026-09-18, 2026-09-25.
- ab_enriched: 2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-26.
- export: 2026-08-26.
- catalyst: 2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-09-08, 2026-09-09.
- actions and join: present on all 31 sessions.

Group 3 overall, 110 recipes: search days min 8, median 17, max 21. Check days min 8, median 12, max 16.

| group | recipes | search days | check days |
| --- | ---: | ---: | ---: |
| stock_book only | 48 | 17 | 12 |
| actions and stock_book | 22 | 17 | 12 |
| no fullscan role (price-only, or union with the price-only fallback) | 16 | 21 | 16 |
| export and stock_book | 6 | 17 | 12 |
| join and stock_book | 4 | 17 | 12 |
| ab, actions, and stock_book | 2 | 13 | 12 |
| ab and stock_book | 2 | 13 | 12 |
| actions only | 2 | 21 | 16 |
| actions, export, and stock_book | 2 | 17 | 12 |
| actions, join, and stock_book | 2 | 17 | 12 |
| catalyst and stock_book | 2 | 8 | 8 |
| export only | 2 | 20 | 15 |

## Bars, fees, and the book

Bars are `data/prices/ohlc.parquet` at git commit `ff996f535e1343dd739cc801780ae224018bd96c`, blob sha `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, content sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. Yahoo download uses `auto_adjust=false`: the bars are split-adjusted, and dividends are not applied. A feature bar is dated strictly before D. All 110 recipes trade at the 09:30 open, so the session open is the fill and the gap. The session close is the mark only. The session high and low are not read.

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. The flat 15 bp series is the same fills priced at 7.5 bp per side. Shorts pay 1% annual borrow on notional, `sessions / 252`. No price filter is added. The under-$3 share is the count of fills with fill price strictly under $3, divided by the number of fills.

The book is the initial study's cash book: one $10,000 account per recipe, Futubull share counts, the 15 bp series repricing those fills, a name already held not bought again, a finished hold or an exit rule selling at that session's open.

## Luck test

The luck-test denominator is 9,280 + 110 = 9,390. The 9,280 are the initial study's denominator (110 + 37 + 8,264 + 868 + 1). The added 110 are this study's recipes. p is a one-sided t-test that the mean check-day Futubull return is above zero, multiplied by 9,390, and capped at 1. Fewer than two check days cannot reject the null.

## Append-only forever

This commit writes none of the return files.

Each session is written once to `research/lever_search_labelled/returns/{session}.json`. Its sha256 is one line of `research/lever_search_labelled/returns/manifest.jsonl`. A line is one JSON object with `session`, `file`, `sha256`, and `input_blobs`. Each blob entry has `path`, `blob_sha`, and `commit`. Lines stay in the order they were written. A later line is a later session.

A later rebuild may only add sessions after the last recorded one. `.github/workflows/lever_search_labelled_append_only.yml` fails the pull request when an existing per-day file, manifest line, or earlier return changes, or when a manifest line is removed or reordered. The check is `python3 -m src.lever_search_labelled_append --check-against` the pull-request base sha.

The report computed from those files is `research/lever_search_labelled/returns/REPORT.md`. It is not a per-day file. It records, for every recipe, the side, the check-day count, the mean return per check day, both fee compounds and both fee means, the best stock removed and that ticker, the from-09-14 Futubull compound, the under-$3 share, and the luck p. It ranks all 110, and it lists the top 5 long recipes on their own.

## Success

Fixed before any score. A recipe is something good only when every line below holds.

(a) After Futubull fees, the cumulative return on the recipe's own check days is at least 20%.

(b) That same path, with the single best stock removed, is strictly positive. The best stock is the ticker with the highest attributed P&L on those check days. Ties break to the earlier first-entry session, then to the ticker.

(c) The Futubull compound from 2026-09-14 through 2026-09-25 is greater than or equal to zero.

(d) The Futubull luck p is below 0.05 on the denominator 9,390.

A recipe that misses any line is not something good. A recipe that meets all four lines and has fewer than 10 check days is labelled `too few days to judge` and is not proven. `rebuild_match` does not strike. A struck label is not used. If no recipe is proven, the report says `nothing proven yet`. The study label `assumed pre-open, not server-proven` is printed on the report either way.

## Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when the input manifest's sha256 disagrees with the pin above, when a row labelled D is read from a panel blob other than the earliest blob in the manifest, when a per-day input is read from a blob other than the earliest commit of that path, when a feature read includes a bar dated the session or later, when the panel row's open or close is used as a fill or a feature, when a recipe is struck for `rebuild_match`, or when an existing per-day file, manifest line, or earlier return would change.
