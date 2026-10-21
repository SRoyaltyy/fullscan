# factor_mine_diagnosis_v1 — preregistration

- status: measurement locked. This commit has no returns and no compounds. Nothing in this study is selected for trading.
- written: 2026-09-26
- study: factor_mine_diagnosis_v1
- fingerprint_sha256: 2ac97e101dbd2f6a040f6ead5a686f89dcc15425392c9e7e0a47af802cce0776
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: where the named Factor Mine books' dollars come from, which of their own parts move the result, and which 09:30 conditions separate winners from losers. The answer is a description. It is not a recipe to trade.
- creation date: 2026-09-26. Every session is `designed_after`. The numbers are research. They are not a live record.
- inputs_sha256: c44d31254f524c136b54e066a1405ede68cc7c2492afd19e6c9ac38ef81ce368
- recipes_sha256: 4c4f0eefdcfd8c3c1cb45a044e8f881a867c23fc4d40a8da473fc19b1620ca8f
- drop_sha256: 0b45c8a3d5b196845deba4178f2b1206f3eb9529171026d9e6bcf319a7c317d2

<!-- BEGIN COVERED -->

## What this study is

A diagnosis of books that already exist. It does not rank a recipe for trading, does not freeze a combo for trading, and does not add these looks to the 9,280 denominator. A table sorted for reading is not a selection. `factor_mine_avg_v1`, `factor_mine_score_search_v1` (#364), and any uncommitted v2 or v3 search stay untouched. No v2 preregistration is fingerprinted by this commit.

## Subjects

Nineteen books. The engine is `build_group3_recipes()` plus `src.factor_mine_book.simulate_book`'s cash rules: `matches`, `pick_day`, `rank_key`, `should_exit`, `lot_should_sell`, `split_budgets`, and Futubull `order_fees`. No new score and no new input is invented.

The #357 names are `probable_h3`, `probable_h5`, and `short_extended_h3`. The fifteen #363 names are the long group-3 recipes that have a rank key, in name order. `union_hot_n4_holdup` is the one named sleeve outside that 110. Its `created_on` is 2026-09-21, so the window through 2026-09-11 is before that sleeve existed. It is still described. It is not promoted.

`RECIPES.json` sha256 is `4c4f0eefdcfd8c3c1cb45a044e8f881a867c23fc4d40a8da473fc19b1620ca8f`.

| name | side | list | hold | N | sort | must-not |
| --- | --- | --- | --- | ---: | --- | --- |
| `probable_h3` | long | probable | 3 | 8 | list order |  |
| `probable_h5` | long | probable | 5 | 8 | list order |  |
| `short_extended_h3` | short | union | 3 | 8 | list order |  |
| `union_candle_score_h1` | long | union | 1 | 8 | candle_score | alarm |
| `union_candle_score_h3` | long | union | 3 | 8 | candle_score | alarm |
| `union_cond_h1` | long | union | 1 | 8 | cond | alarm |
| `union_cond_h3` | long | union | 3 | 8 | cond | alarm |
| `union_cond_n4_h3` | long | union | 3 | 4 | cond | alarm |
| `union_hot_n12_h1` | long | union | 1 | 12 | hot_score | alarm |
| `union_hot_n4_h1` | long | union | 1 | 4 | hot_score | alarm |
| `union_hot_score_h1` | long | union | 1 | 8 | hot_score | alarm |
| `union_hot_score_h3` | long | union | 3 | 8 | hot_score | alarm |
| `union_ret_5_h1` | long | union | 1 | 8 | ret_5 | alarm |
| `union_ret_5_h3` | long | union | 3 | 8 | ret_5 | alarm |
| `union_w_hot_candle_h1` | long | union | 1 | 8 | w_hot_candle | alarm |
| `union_w_hot_candle_h3` | long | union | 3 | 8 | w_hot_candle | alarm |
| `union_w_hot_cond_h1` | long | union | 1 | 8 | w_hot_cond | alarm |
| `union_w_hot_cond_h3` | long | union | 3 | 8 | w_hot_cond | alarm |
| `union_hot_n4_holdup` | long | union | 1 | 4 | hot_score | alarm |

`short_extended_h3` must-have is `ret_5_min` 15. `union_hot_n4_holdup` uses `s_boost=holdup`. Every other must-have is empty. Sell mode on all nineteen is list-drop.

## Inputs

Each morning's names and fields are the earliest commit on `origin/main` whose `data/factor_mine/panel.json` contains that session. A later rewrite is not used. The panel's own open and close are not fills. `INPUTS.json` sha256 is `c44d31254f524c136b54e066a1405ede68cc7c2492afd19e6c9ac38ef81ce368`.

Morning S is `morning_s` on this tree, stored in that file, and the walk reads the stored number only. A missing S does not sit. That is the engine: sit only when S is present and S <= -3. 2026-08-27 is missing, so the weather gate does not sit that day. Stored hard-red mornings are 2026-08-18, 2026-08-19, 2026-08-24, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-14, 2026-09-15, and 2026-09-24.

2026-09-16 has 4 names in that earliest board. The thin days stay thin.

## Prices and the leak check

Two tapes, side by side.

- Cleaned v1c: blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. The 76 jump tickers are absent.
- Yahoo pin: `data/prices/ohlc.parquet`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. Those 76 stay. ALP, NFE, TNMG, and WCT are the matched splits inside the 76. They are flagged, not rescaled.

`DROP_LIST.json` sha256 is `0b45c8a3d5b196845deba4178f2b1206f3eb9529171026d9e6bcf319a7c317d2`. It is the 76 tickers in `research/breadth_rank_v1c/JUMPS.md` (sha256 `8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a`). YAAS is in `DROPPED.json` (sha256 `4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad`) and is not one of the 76. YAAS dollars are reported apart from the 76. Futubull fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`.

The same-day leak check: a feature bar dated on or after the session raises. The 09:30 open is the fill. The close is a mark only. A missing open is not replaced by the close, and the next name is not pulled into that slot. Prior volume, when shown, is the last bar strictly before the session. Float is not on the earliest board, so it is omitted rather than read from a later file.

## Book rules

One continuous $10,000 cash book from 2026-08-13 through 2026-09-25. The forward window does not reset the cash. Whole shares. Leftover cash is split once across the new names; a name that cannot buy one share is skipped and the slice is not retried. Sell first, then buy.

Min-hold is the recipe hold. The entry morning counts as held 0. List-drop sells at a later 09:30 open once held is at least the min-hold and the name is off that morning's kept list. Early `exit_when` can sell inside the hold. None of these nineteen set `exit_when`.

Holdup, only when that sleeve's boost is on, S is present and above 0, and the morning is not hard-red: the new long lot's min-hold becomes 2. It is still held on the next session and can sell at the open of the session after that. This matches the engine's 2026-09-02 holdup example, which sells on 2026-09-04.

Weather on means no new buys when S <= -3. Lots already held still exit. Weather off buys on those mornings too.

Flat 15bp is the same share counts with 7.5bp per side. It is shown beside Futubull. It is not a second strategy.

A closed trade has fewer than 30 fills when `n_closed < 30`. Those rows stay in the tables and are flagged. They are not dropped.

## Windows

Tune is the 21 sessions through 2026-09-11. Forward is 2026-09-14 through 2026-09-25. There is no 2026-09-12 or 2026-09-13 session. Daily returns are sliced by session. A closed trade is counted in the window of its sell date. Every section below is reported on both windows and both tapes.

## Profit source

On closed trades, Futubull pnl:

- Each ticker's share of net pnl, and of gross winning pnl. The five largest net-pnl names are listed.
- Share of net pnl from entry price under $3, from $3 up to but not including $10, and from $10 up.
- Profit without the best 1, 3, and 5 names. Best is the highest window pnl, then the earlier first entry, then the ticker. Their dollars are removed from that window's daily equity changes and the book is recompounded. The same names' closed-trade pnl is also subtracted from realized pnl.
- Share of Futubull dollars from the 76 dropped names, and YAAS alone.

## Timing

Overnight gap dollars are shares times the move from the prior close to this open, for lots already held. The 09:30-to-close piece is the move from this open to this close for lots still held at the close. Fees sit in the session piece. The first day is the entry session's open-to-close after the buy fee. Later days are the gaps and sessions after the entry date, including the sell fee.

Up days are sessions whose book return is positive. For those days, and for down days and flat days, the table shows morning S, that day's IWM open-to-close (an outcome, not an input; Yahoo IWM stops early and those sessions are marked missing), the share of the morning list with `last_green` (knowable at 09:30), and the equal-weight open-to-close of that morning's list (an outcome).

## Drop-one, pairs, and build-up

Drop-one removes one part from the full recipe:

- `must_have`: clear require.
- `must_not`: clear forbid.
- `sort`: shuffle the names that still match, with `random.Random(20260813)` each morning, after sorting by ticker. This is not list order.
- `weather`: the hard-red sit is off.
- `hold_rule`: min-hold stays, `s_boost` is none, sell mode is `time`, and `exit_when` is cleared. List-drop and holdup are off.

N is not one of those five. The full recipe is also rerun at N of 4, 8, and 12, the engine's own book sizes. The recipe's own N is the baseline row.

Every pair of the five toggles is removed together. Interaction gain is `only_i + only_j - both - baseline`, for the Futubull compound and, separately, for the win rate. That is the combined effect minus the two single effects. A part the recipe does not use is still dropped; its single effect is then zero and the row is kept.

The bare book is the mixed union list, the recipe's side, the recipe's own N, no require, no forbid, board list order (`src_rank`, then ticker), hold 1, sell `time`, no holdup, weather off. Build-up adds only the parts this recipe actually has, in every order. Those parts are `list_source`, `must_have`, `must_not`, `sort`, `weather`, and `hold_rule`, and only when the full recipe differs from the bare book on that axis. `hold_rule` on means the recipe's hold, list-drop, holdup, and `exit_when`. Sort on means the recipe's sort key, not the shuffle. The book after a set of parts does not depend on order, so each subset is walked once. Every permutation is reported as the chain of those subsets. No order is chosen.

Contribution of a part is baseline minus the dropped book, for compound and for win rate.

## Winners, losers, and combos

A winner is a closed trade with pnl above zero. At the entry morning the description uses camera tones, the heat camera, candle capture, cond net, 5-day return, entry price, prior volume, earnings-react, news box, days on the list through that morning, and weather S. Float is omitted.

Bins are fixed, not fitted:

- price: under 3, 3 to under 10, 10 and up
- 5-day return: below 0, 0 through 10, above 10, missing
- cond net (`cond_good - cond_bad`): 0 or below, 1 through 3, 4 and up
- heat and vol: the camera tone
- candle capture, alarm, blue, zero red: yes or no
- news box: good, bad, neutral, missing, otherwise other
- earnings-react: yes or no
- days on the list: 1, 2 through 5, 6 and up
- weather: missing, S <= -3, S <= 0, S under 5, S at least 5

The tree is grown separately on tune trades and, as a labeled description only, on forward trades. The tune leaves are also scored on forward trades without growing a new tree. Depth 2 and depth 3. At each node the split is one bin value versus the rest. Both sides need at least 30 trades. The chosen split maximizes the absolute gap in win rate. Ties take the feature name, then the value. A leaf under 30 trades is flagged and is not a reported combo.

Joint buckets, the same minimum, are price band by weather by news, and price band by cond band by 5-day band.

The reading order for combo tables is the absolute distance of the win rate from 50%, then more trades, then the condition text. At most 20 leaves are in that table. The order selects nothing.

For each of those leaves: the leaf's win rate and mean trade return; the same stats for trades that match each single condition in the leaf; and RANDOM4 on the leaf's entry dates. The random column is the mean of the 1,000 hold-matched random books' daily returns on those dates. If the leaf mixes holds, the random column is the long hold-1 weather-on book and the mix is labeled.

## List versus sort

RANDOM4 draws 4 names from that morning's full earliest board, ticker-sorted, `random.Random(20260813 + draw)`, 1,000 draws. A drawn name with no open keeps the slot. The draw uses the comparison recipe's side, hold, sell mode, holdup, and weather, and it does not use that recipe's list filter or sort. It is not used to rank the recipes.

## Counts

Drop-one is 5, pairs are 10, N grid is 3, and the build-up subsets are the power set of each recipe's active parts. All of those walks are descriptions.
