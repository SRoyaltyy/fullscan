# factor_mine_avg_v1 — preregistration

- status: protocol locked. This commit has no returns, no compounds, and no window scores.
- written: 2026-09-26
- study: factor_mine_avg_v1
- fingerprint_sha256: 3d6f2127f9e4107f5f3d864ae8cd8578d004dbf403b4d1d84098620f598775cd
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: do the frozen Factor Mine long recipes that buy the top-N ranked names win on average, on both windows, on cleaned bars and on bars that keep the jump names.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.

<!-- BEGIN COVERED -->

## Universe

The recipe set is the intersection of the #357 freeze and the #336 book.

#357 scored the 110 recipes from `build_group3_recipes()` in `src/lever_search_proof.py`, the grid at commit `8e8c36a7117e60040d04cfa598e503b82fd7c6ea`. #336 is the sequential book: one directory under `data/factor_mine/state/` per recipe, 339 names.

A recipe is included when all three hold:

1. It is in `build_group3_recipes()`.
2. `side` is `long`.
3. `rank` is set. The buy sorts by that rank and keeps `top_n` (8 unless the recipe sets 4 or 12). The tie break is the ticker symbol.

That rule is the whole selection. A recipe is not added because it won, and none is removed after a result. Nothing is refit after 2026-09-13. Sleeves whose `created_on` is on or after 2026-09-14 are outside the 110, so they are outside this universe. Shorts are outside it. Recipes with no rank key keep list order and are outside it. The #336-only rank sleeves `union_rsi_h1`, `union_rsi_h3`, `union_macd_hist_h1`, and `union_macd_hist_h3` are outside the #357 110, so they are outside this universe.

The count is part of the lock: 15. The names, in ticker-sort order, are:

| recipe | rank | top N | hold |
| --- | --- | ---: | ---: |
| `union_candle_score_h1` | candle_score | 8 | 1 |
| `union_candle_score_h3` | candle_score | 8 | 3 |
| `union_cond_h1` | cond | 8 | 1 |
| `union_cond_h3` | cond | 8 | 3 |
| `union_cond_n4_h3` | cond | 4 | 3 |
| `union_hot_n12_h1` | hot_score | 12 | 1 |
| `union_hot_n4_h1` | hot_score | 4 | 1 |
| `union_hot_score_h1` | hot_score | 8 | 1 |
| `union_hot_score_h3` | hot_score | 8 | 3 |
| `union_ret_5_h1` | ret_5 | 8 | 1 |
| `union_ret_5_h3` | ret_5 | 8 | 3 |
| `union_w_hot_candle_h1` | w_hot_candle | 8 | 1 |
| `union_w_hot_candle_h3` | w_hot_candle | 8 | 3 |
| `union_w_hot_cond_h1` | w_hot_cond | 8 | 1 |
| `union_w_hot_cond_h3` | w_hot_cond | 8 | 3 |

Each recipe is one $10,000 cash book. Buys are at the session open. The session close is a mark. Futubull fees use `src/paper_trade.py` `order_fees`. The walk is `src/lever_search_score.py` `walk_recipe` and `pick_day`. Gates, hold, and the alarm forbid stay as frozen. On a day the recipe's inputs are missing it sits: no new buy, open lots still exit on the hold, and the mark still prints. These 15 all forbid `alarm`, so a new buy requires a usable stock_book day. That sit rule is the frozen one. This study does not turn those days into entries.

## Windows

The book starts 2026-08-13, the Factor Mine dashboard start. Sessions before that stay out.

Before 2026-09-14, through 2026-09-11:

2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11.

From 2026-09-14 through 2026-09-25:

2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25.

One continuous walk covers both lists in that order. There is no session between 2026-09-11 and 2026-09-14. A lot still open on 2026-09-11 can exit on 2026-09-14.

Per window, across the 15 recipes, equal-weight mean and median of:

- Futubull after-fee compound of the daily returns inside the window.
- Up, down, and flat days. A day is up when that return is > 0, down when < 0, and flat when it is 0. Sit days with a zero return count as flat.
- Trade win rate. A closed trade is a lot sold in the window. It wins when sell proceeds minus the sell fee exceed the buy cost plus the buy fee. Lots still open at the end of the walk are not closed and are not in the rate. A recipe with no closed trade in the window is left out of the win-rate mean and median, and the report says how many were left out.
- Ex-best-ticker compound. The best ticker is the one with the largest attributed Futubull dollars inside the window. Ties take the earlier first entry, then the ticker symbol. Those dollars are removed from that window's daily equity changes, and the window is compounded again. Equity still walks every session from $10,000.

Also the share of the 15 recipes whose window compound is strictly positive.

The mean of a count is the arithmetic mean. The median of an even count is the average of the two middle values.

## Prices

Two versions. Kept-name prices are the same file in both, so the gap between them is the dropped names (including who replaces them in the top N).

The bar file is `data/prices/ohlc.parquet`, the #357 pin: commit `ff996f535e1343dd739cc801780ae224018bd96c`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. Yahoo split-adjusted daily bars, dividends not applied.

The drop list is the latest cleanup, PR #362 head `ad10f862ad51df88f4dd03ff3dbcdf628f7e2685`.

- Cleaned snapshot `research/breadth_rank_v1c/bars/ohlc.parquet`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`.
- Drop file `research/breadth_rank_v1c/bars/DROPPED.json`, blob `31e3b7dc38583288faba338001a91503bf69a758`. 77 names. Vendored here as `DROP_LIST.json`. This study does not edit the #362 files.

Version (a) deletes those 77 names from the candidate rows and from the bar tapes. A deleted name cannot fill and cannot hold a top-N slot.

Version (b) keeps those 77 names on the same Yahoo pin. The four matched splits ALP, NFE, TNMG, and WCT are flagged. Their jumps match a Yahoo split and the cleanup did not rescale them. YAAS is the explained split that the cleanup also left dropped; it is one of the 77 and it is not one of the four.

The 77 is the latest list. It is larger than 52 because #362 added close/open jumps. The earlier open/previous-close list is `research/breadth_rank_v1b/bars/DROPPED.json`, blob `93726f0bbf52207b5be4edea5eadf1984b1846c4`, 53 names. Removing YAAS leaves 52, and those 52 include ALP, NFE, TNMG, and WCT. Those 52 are inside the 77. The report gives their P&L share on version (b) as well as the share of all 77.

The breadth snapshot has 2,067 names and its kept-name prints are not the #357 pin. Version (a) does not swap in that snapshot. Doing so would mix a re-pull into the dropped-name gap. The snapshot is cited as the source of the drop list and its sha256.

For each window the report states:

- Mean compound on (a) and on (b), and (b) minus (a).
- On version (b), the dropped names' attributed Futubull dollars divided by all tickers' attributed dollars. The same share for the 52-name subset and for the four flagged splits.

## Benchmarks

Same sessions, same fees, both price versions.

IWM is buy-and-hold. One buy at the 2026-08-13 open for the whole $10,000, Futubull fee on that buy, marked at each session close, still held on 2026-09-25. No exit fee. The window compound uses those daily returns.

RANDOM4 draws 4 names from that day's stock_book candidate list, the list these recipes rank before the top-N cut. Seed `20260813` plus the draw index, 1000 draws, hold 1, Futubull, $10,000. Version (a) removes the 77 names before the draw. A day with no stock_book list is a sit. The report gives the mean and median compound across the 1000 draws on each window.

## Records

Results are written once under `research/factor_mine_avg_v1/returns/`. This commit writes none of them. A later commit that changes `PREREG.md`, `DROP_LIST.json`, or a result file fails CI. Files outside this study and `.github/workflows/factor_mine_avg_v1.yml` stay as they are on the base, including the #357 files under `research/lever_search/` and the #336 files under `data/factor_mine/`.

N for this study is 15. These 15 tries are new. They are not added into the #357 denominator of 9,280, and the #357 label stays `struck`.
