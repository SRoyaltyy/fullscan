# Errata: REAX split scale in the pinned bars

The score files under `research/lever_search_labelled/returns/` are unchanged. This note does not rescore them.

The pinned bar file is `data/prices/ohlc.parquet`, commit `ff996f535e1343dd739cc801780ae224018bd96c`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`. Fills in this study are that file's session opens.

REAX had a 1:10 reverse split on 2026-08-25 (Yahoo split event: numerator 1, denominator 10, `splitRatio` `1:10`, ex-date 2026-08-25 09:30 America/New_York). Yahoo's split-adjusted open on 2026-08-19 is 26.40. The pinned file has that session's open at 2.64, which is the pre-split scale (26.40 / 10). The pinned open on 2026-08-20 is 27.50, the same figure as Yahoo's adjusted open that day. The two pinned opens are therefore on different scales. Every REAX fill on 2026-08-19 and 2026-08-20 is a split artefact, not a one-session move of about ten times.

## Fills on a consecutive-day price break

A consecutive day is the next session on the SPY calendar in the pinned file. The ratio is the later session's open divided by the earlier session's open. The threshold is a ratio above 3 or below 1/3. Every fill in all 110 recipes was checked. A fill is listed when its session is either day of such a pair.

Twelve fills meet the test. All twelve are REAX, and all twelve sit on the 2026-08-19 / 2026-08-20 pair (ratio 27.50 / 2.64 = 10.42). No other fill does. The Yahoo split for that ticker is the 1:10 reverse split on 2026-08-25. The ex-date is after both sessions; the artefact is the mixed scale in the pinned file, not a split that fell between these two opens.

| recipe | session | side | shares | fill price | adjacent session | adjacent open | ratio | Yahoo split |
| --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| `ohlc_hot_h1` | 2026-08-19 | BUY | 443 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `ohlc_hot_h3` | 2026-08-19 | BUY | 459 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `ohlc_hot_h5` | 2026-08-19 | BUY | 2 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_hot_n12_h1` | 2026-08-19 | BUY | 288 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_hot_score_h1` | 2026-08-19 | BUY | 437 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_hot_score_h3` | 2026-08-19 | BUY | 5 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_w_hot_cond_h1` | 2026-08-19 | BUY | 434 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_w_hot_cond_h3` | 2026-08-19 | BUY | 4 | 2.64 | 2026-08-20 | 27.50 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `ohlc_hot_h1` | 2026-08-20 | SELL | 443 | 27.50 | 2026-08-19 | 2.64 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_hot_n12_h1` | 2026-08-20 | SELL | 288 | 27.50 | 2026-08-19 | 2.64 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_hot_score_h1` | 2026-08-20 | SELL | 437 | 27.50 | 2026-08-19 | 2.64 | 10.42 | 1:10 reverse split, 2026-08-25 |
| `union_w_hot_cond_h1` | 2026-08-20 | SELL | 434 | 27.50 | 2026-08-19 | 2.64 | 10.42 | 1:10 reverse split, 2026-08-25 |
