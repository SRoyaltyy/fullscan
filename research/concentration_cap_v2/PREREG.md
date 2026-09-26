# concentration_cap_v2 — preregistration

- status: measurement locked. This commit has no returns and no compounds.
- written: 2026-09-26
- study: concentration_cap_v2
- fingerprint_sha256: dea5dbf2b7f85660649ad750aa2b22dbb8b237319ab9f59e48892ffa76903b97
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which keep-held books, with a per-name weight cap and a book width on seven frozen Factor Mine recipes, clear the Monday joint rule through 2026-09-11 with the best stock under 20% of tune profit and a positive return without the top 3 stocks. The 2026-09-14 window can only reject.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.

<!-- BEGIN COVERED -->

## What this study is

A new study, `concentration_cap_v2`. `concentration_cap_v1` is already fingerprinted. Its files, freeze, and scores stay as committed. This study does not edit them. It does not edit Factor Mine's live core, `src/webull_exec.py`, `paper_open`, `flatten_robust`, Supabase, `00_grounding`, `research/factor_mine_recipe_search_v4`, `research/concentration_screen_v1`, or the `research/forward_shadow_v1` ledger.

The walker is the v1 keep-held walker, imported and not rewritten. Width replaces `top_n`. A weight cap adds the same next-open trim.

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

## Profit share

Total profit in a window is ending equity minus starting equity. The best stock is the largest summed daily pnl contributor. Ties break by the earlier first session in the window, then by ticker. Profit share is that stock's summed daily pnl divided by total profit. Every row reports this share for the tune window and for the P2 window. A zero profit is reported blank. A nonzero profit is reported as the ratio, including when profit is negative.

The share gate uses a positive profit only. Under 20% means strictly below 0.20. 20% or more means 0.20 or above.

## Pick

Tune sessions are 2026-08-13 through 2026-09-11. The tune walker does not read a session on or after 2026-09-14.

Monday starts, each a fresh $10,000 book through 2026-09-11: 2026-08-17, 2026-08-24, 2026-08-31. 2026-09-07 is not a session.

Win rate is winning closed trades divided by closed trades. A win is Futubull pnl above zero on the sell date. Winning-day share is up days divided by days with a buy, a full sell, or a trim. The joint at a start is the lower of the win rate and the winning-day share. A start with fewer than 30 closed trades has no joint.

A passer needs all three. A joint at every Monday start. On the continuous tune book, total profit is positive and the best stock's profit share is under 20%. On that same book, the Futubull return without the top 3 contributing stocks is above zero. The without-stock path is the fixed v4 `_drop_compound` arithmetic, starting at $10,000 on this tune book.

Passers sort by the worst start's joint descending, then by the mean joint descending, then by id ascending. The freeze is that passer list. It is written before any 2026-09-14 score.

## Reject window

2026-09-14 through 2026-09-25 can only reject a frozen passer. It cannot add a recipe and it cannot change the rank. The book is the continuous walk from 2026-08-13. P2 starts at the 2026-09-11 close. The without-stock path for P2 starts at that same close.

A frozen passer is rejected if any one of these is true. The P2 window has at least 30 closed trades and the joint is under 0.5. P2 total profit is positive and the best stock's profit share is 20% or more. The P2 return without the top 3 contributing stocks is negative. Fewer than 30 closed trades does not fire the joint rule. The profit-share rule and the top-3 rule still can. A frozen passer that is not rejected is the carry list.

## What every row reports

For the continuous tune window and for the P2 window: Futubull total return, flat 15bp total return, Futubull return without the top 1, top 3, and top 5, the best stock's profit share, distinct tickers, closed trades, and win rate. IWM buy-and-hold is once per window. RANDOM4 is the mean of 1000 draws, seed 20260813, 4 names, the recipe's hold, sell, holdup, weather, and weight cap, not its filter or sort. The keep bar, at least 30 closed trades and a win rate above 55% after fees, is reported and does not add or remove a frozen row.

## Luck

This study has 84 tries. v1's fingerprinted try count is 45. The prior luck total is 21,796, the `concentration_screen_v1` total (21,536 + 260). Luck N is 84 + 45 + 21,796 = 21,925. v1's own headline 21,841 is not added again.

## Prices, inputs, fees

Prices are the cleaned tape `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` `9a0e12ca2f7a3e87f965190e8766053a37ffaabc`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. Open is the fill. Close is the mark. Before a compound is written, every cleaned bar on or before the window's last session, for IWM and for every board ticker in that window, is checked. A leg above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) names that ticker and date and the ratio is within 25% of the split or of one divided by the split. An unexplained jump halts the window. No freeze is written.

The board and the morning S are the v4 `INPUTS.json` rows, sha256 `0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969`. The tune phase drops every date after 2026-09-11 before the walk. The forward phase runs only after this study's freeze file exists.

Futubull fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Flat 15bp uses the same share counts.

## Carry

A frozen passer that P2 does not reject is appended to `research/forward_shadow_v1/hooks/v4_winners.json`. The ledger, the recipe spec, and the forward preregistration stay as they are. The winner name starts with `fwd_ccap2_`. `first_session` is 2026-09-28. The hooked recipe carries the base gates, the width, and the weight cap. The forward runner trims at the next open when `weight_cap` is a number. A recipe with no `weight_cap` is unchanged.

## IRONCLAD

| rule | how this study treats it |
| --- | --- |
| 1–3 append-only records | v1 and every earlier study stay as committed. The forward ledger is not edited. |
| 4 new rules, new name | `concentration_cap_v2` is a new name. The share gate, the width 10, and `union_ret_5_h3` are new. |
| 5 designed_after | Creation date 2026-09-26. Every session scored here is `designed_after`. |
| 11 prices | Cleaned v1c only, sha-pinned. An unexplained jump halts the window. |
| 12 fills | Buy at the 09:30 open. Keep-held does not buy a name that is already held. A trim sells at that open. |
| 14 fees | Futubull passes or rejects. Flat 15bp is the same shares beside it. |
| 15–17 freeze before the later window | This preregistration is committed before any score. The passer freeze is committed before any 2026-09-14 score. |
| 18 candidate cap | 84 candidates. Luck N is 21,925. |
| 19 RANDOM4 and IWM | Seed 20260813, 1000 draws, and IWM on the same days. |
| 20 without the best name | Profit share, and the return without the top 1, top 3, and top 5, in each window. |
| 21 thirty fires | Under 30 closed trades has no joint. It cannot pass the Monday rule and it cannot fire the joint reject. |
| 22 untestable | A book that never trades is not a passer. |
| 23 no real money | This search selects nothing for a live account. |
