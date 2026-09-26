# concentration_cap_v3 — preregistration

- status: measurement locked. This commit has no returns and no compounds.
- written: 2026-09-26
- study: concentration_cap_v3
- fingerprint_sha256: 6678e80fe036d4bc33212b4961c7fce9d43a2cabd9b1832f7782942de7ffed94
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which keep-held books, with a per-name weight cap and a book width on seven frozen Factor Mine recipes, clear the Monday joint rule through 2026-09-11 with dependence under the 20% line and R_-3 above zero. The 2026-09-14 window can only reject.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.

<!-- BEGIN COVERED -->

## What this study is

A new study, `concentration_cap_v3`. `concentration_cap_v1` and `concentration_cap_v2` are already fingerprinted and scored. Their files, freezes, and scores stay as committed. This study does not edit them. It does not edit Factor Mine's live core, `src/webull_exec.py`, `paper_open`, `flatten_robust`, Supabase, `00_grounding`, `research/factor_mine_recipe_search_v4`, `research/concentration_screen_v1`, or the `research/forward_shadow_v1` ledger.

The walker is the v1 keep-held walker, imported and not rewritten. Width replaces `top_n`. A weight cap adds the same next-open trim. The grid is the v2 grid. The new rule is the 20% line below.

## The 20% line

For a period with total return R (compounded, keep-held Futubull) and return R_-1 when the best stock is removed (the best stock is the one with the largest net dollar contribution, and the drop starts from prior equity):

- PASS requires R > 0 AND R_-1 >= 0.8 * R, i.e. dependence = 1 - R_-1/R < 20%. If R <= 0, the recipe fails the test.
- Also report, for information only, the gross share: the best stock's net dollar profit divided by the sum of net dollar profit over all stocks that made money (0-100%).

The net definition is the binding one because it matches what Cyrus measured (a drop from 33% to about 13% means 60% of gains). The top-3 rule stays R_-3 > 0. The same definitions apply in the 09-14 to 09-25 reject check.

R is the compounded Futubull return of that window. R_-1 is the compounded Futubull return after the best stock's daily pnl is removed. R_-3 removes the top 3 the same way. The drop is the fixed v4 `_drop_compound` path. On the tune book it starts at $10,000. On the P2 book it starts at the 2026-09-11 close.

The best stock is the largest summed daily pnl. Ties break by the earlier first session in the window, then by ticker. The scorer applies the pass sentence as written: R > 0 and R_-1 >= 0.8 * R. Dependence is reported as 1 - R_-1/R when R is not zero, and blank when R is zero. A dependence of exactly 20% meets R_-1 >= 0.8 * R.

Gross share uses the same best stock. Stocks that made money are those whose summed daily pnl is above zero. If that sum is zero, gross share is blank. Otherwise gross share is the best stock's summed daily pnl divided by that sum. It is reported on every row in both windows. It does not pass a recipe and it does not reject one.

## Bases

Seven frozen v4 recipes. Gates, hold, sell, boost, rank, and weather stay. `union_ret_5_h3` is the v4 part row with rank `ret_5`, hold 3, sell `list`, boost none, and forbid alarm. Weather on is `__w1`. Weather off is `__w0`.

| base id | rank | hold | boost | weather | forbid |
| --- | --- | ---: | --- | --- | --- |
| `union_hot_n4_h1__w0` | hot_score | 1 | none | off | alarm |
| `union_hot_n4_holdup__w0` | hot_score | 1 | holdup | off | alarm |
| `union_hot_n4_h1_nonews__w0` | hot_score | 1 | none | off | alarm and news bad |
| `union_hot_score_h3__w1` | hot_score | 3 | none | on | alarm |
| `union_hot_score_h3__w0` | hot_score | 3 | none | off | alarm |
| `union_ret_5_h3__w0` | ret_5 | 3 | none | off | alarm |
| `union_ret_5_h3__w1` | ret_5 | 3 | none | on | alarm |

Side is long. Universe is union. `exit_when` is empty. Sell is `list`.

## Grid

Book width N is 4, 6, 8, or 10 names. The slot is equal weight: new buys use the leftover split, not a score weight.

Per-name weight cap is 20%, 25%, or none.

Candidate id is `{base_id}__n{N}__c{20|25|none}`. Order is the base table above, then N 4, 6, 8, 10, then cap 20, 25, none.

7 × 4 × 3 = 84 candidates. That is this study's try count. A Monday start is the rank key inside a try. It is not another try. Keep-held is the only fill.

## Cash from a trim

Leftover cash sits in cash. A trim does not add shares to a name that is already held. Cash already in the account, including cash raised by a same-morning exit or trim, is split equally across newly selected names that are not already held. Whatever those buys do not spend stays cash.

## Fill model and the trim

Keep-held matches `src/webull_exec.py` `size_hot4_tickets` on the live path, as the v4 engine walks it and as v1 walks it. A name that is still selected and already held is not bought again. A full sell is a list-drop through `lot_should_sell` after the min-hold. Hard-red, and only when weather is on, sits new buys. It does not sell a lot by itself. Weather off buys on a hard-red morning. A missing S does not sit. Holdup, when that base's boost is holdup, S is present and above 0, and the morning is not a hard-red sit, sets the new lot's min-hold to 2.

When the cap is none, there is no trim. When the cap is 20% or 25%, the trim runs at the session open after full exits and before new buys. Book equity is cash plus open marks of the names still held. If a name's open value is above cap times that equity, shares are sold at the open. The shares kept are the largest integer whose remaining value is at or under cap times (equity minus the Futubull fee on the sold shares). Names are handled in ticker order. Each name sees equity after earlier trims that same morning. One pass. The Futubull fee is charged. The flat 15bp book sells the same shares at 7.5bp a side. A trim that leaves zero shares is a closed trade. A trim that leaves shares is not a closed trade. A new buy is not pre-cut to the cap.

Whole shares. Sell first, then trim, then buy. A fresh book starts at $10,000.

## Pick

Tune sessions are 2026-08-13 through 2026-09-11. The tune walker does not read a session on or after 2026-09-14.

Monday starts, each a fresh $10,000 book through 2026-09-11: 2026-08-17, 2026-08-24, 2026-08-31. 2026-09-07 is not a session.

Win rate is winning closed trades divided by closed trades. A win is Futubull pnl above zero on the sell date. Winning-day share is up days divided by days with a buy, a full sell, or a trim. The joint at a start is the lower of the win rate and the winning-day share. A start with fewer than 30 closed trades has no joint.

A passer needs all of these on the books named here. A joint at every Monday start. On the continuous tune book, R > 0 and R_-1 >= 0.8 * R. On that same book, R_-3 > 0.

Passers sort by the worst start's joint descending, then by the mean joint descending, then by id ascending. The freeze is that passer list. It is written before any 2026-09-14 score.

## Reject window

2026-09-14 through 2026-09-25 can only reject a frozen passer. It cannot add a recipe and it cannot change the rank. The book is the continuous walk from 2026-08-13. P2 starts at the 2026-09-11 close. R, R_-1, and R_-3 for P2 start at that same close.

The same 20% line and the same top-3 rule apply. A frozen passer is rejected if any one of these is true. P2 fails R > 0 and R_-1 >= 0.8 * R, which includes R <= 0. P2 fails R_-3 > 0. The P2 window has at least 30 closed trades and the joint is under 0.5. Fewer than 30 closed trades does not fire the joint rule. The 20% line and the top-3 rule still can. Gross share does not reject. A frozen passer that is not rejected is the carry list.

## What every row reports

For the continuous tune window and for the P2 window: Futubull total return R, flat 15bp total return, R_-1, R_-3, and the return without the top 5, dependence, gross share, distinct tickers, closed trades, and win rate. IWM buy-and-hold is once per window. RANDOM4 is the mean of 1000 draws, seed 20260813, 4 names, the recipe's hold, sell, holdup, weather, and weight cap, not its filter or sort. The keep bar, at least 30 closed trades and a win rate above 55% after fees, is reported and does not add or remove a frozen row.

## Luck

This study has 84 tries. v2's fingerprinted try count is 84. v1's fingerprinted try count is 45. The prior luck total is 21,796, the `concentration_screen_v1` total (21,536 + 260). Luck N is 84 + 84 + 45 + 21,796 = 22,009. v1's headline 21,841 and v2's headline 21,925 are not added again.

## Prices, inputs, fees

Prices are the cleaned tape `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` `9a0e12ca2f7a3e87f965190e8766053a37ffaabc`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. Open is the fill. Close is the mark. Before a compound is written, every cleaned bar on or before the window's last session, for IWM and for every board ticker in that window, is checked. A leg above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) names that ticker and date and the ratio is within 25% of the split or of one divided by the split. An unexplained jump halts the window. No freeze is written.

The board and the morning S are the v4 `INPUTS.json` rows, sha256 `0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969`. The tune phase drops every date after 2026-09-11 before the walk. The forward phase runs only after this study's freeze file exists.

Futubull fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Flat 15bp uses the same share counts.

## Carry

A frozen passer that P2 does not reject is appended to `research/forward_shadow_v1/hooks/v4_winners.json`. The ledger, the recipe spec, and the forward preregistration stay as they are. The winner name starts with `fwd_ccap3_`. `first_session` is 2026-09-28. The hooked recipe carries the base gates, the width, and the weight cap. The forward runner trims at the next open when `weight_cap` is a number. A recipe with no `weight_cap` is unchanged.

## IRONCLAD

| rule | how this study treats it |
| --- | --- |
| 1–3 append-only records | v1, v2, and every earlier study stay as committed. The forward ledger is not edited. |
| 4 new rules, new name | `concentration_cap_v3` is a new name. The binding 20% line is dependence, 1 - R_-1/R. |
| 5 designed_after | Creation date 2026-09-26. Every session scored here is `designed_after`. |
| 11 prices | Cleaned v1c only, sha-pinned. An unexplained jump halts the window. |
| 12 fills | Buy at the 09:30 open. Keep-held does not buy a name that is already held. A trim sells at that open. |
| 14 fees | Futubull passes or rejects. Flat 15bp is the same shares beside it. |
| 15–17 freeze before the later window | This preregistration is committed before any score. The passer freeze is committed before any 2026-09-14 score. |
| 18 candidate cap | 84 candidates. Luck N is 22,009. |
| 19 RANDOM4 and IWM | Seed 20260813, 1000 draws, and IWM on the same days. |
| 20 without the best name | Dependence is 1 - R_-1/R. R_-3 > 0 stays. Gross share is reported beside them. |
| 21 thirty fires | Under 30 closed trades has no joint. It cannot pass the Monday rule and it cannot fire the joint reject. |
| 22 untestable | A book that never trades is not a passer. |
| 23 no real money | This search selects nothing for a live account. |
