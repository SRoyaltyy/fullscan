# concentration_cap_v1 — preregistration

- status: measurement locked. This commit has no returns and no compounds.
- written: 2026-09-26
- study: concentration_cap_v1
- fingerprint_sha256: c3d52476b6ea9c4ab248a4b8deaa52803a98aa0359397ce19dfa71e4d6e93987
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which keep-held books, built by putting a per-name weight cap and a book width into five frozen Factor Mine recipes, still clear the Monday joint rule through 2026-09-11 after the top 3 contributing stocks are removed. The 2026-09-14 window can only reject a frozen recipe.
- creation date: 2026-09-26. Every session in this study is `designed_after`. The numbers are research. They are not a live record.

<!-- BEGIN COVERED -->

## What this study is

A new study, `concentration_cap_v1`. It does not edit Factor Mine's live core, `src/webull_exec.py`, `paper_open`, `flatten_robust`, Supabase, `00_grounding`, `research/factor_mine_recipe_search_v4`, or the `research/forward_shadow_v1` ledger. The limit is part of the rule. It is not a screen applied after the fact.

The five bases are frozen v4 recipes. Their gates, hold, sell, boost, and weather stay. Book width replaces `top_n`. A weight cap adds a next-open trim. Each pair is a new name.

| base id | hold | boost | weather | forbid |
| --- | ---: | --- | --- | --- |
| `union_hot_n4_h1__w0` | 1 | none | off | alarm |
| `union_hot_n4_holdup__w0` | 1 | holdup | off | alarm |
| `union_hot_n4_h1_nonews__w0` | 1 | none | off | alarm and news bad |
| `union_hot_score_h3__w1` | 3 | none | on | alarm |
| `union_hot_score_h3__w0` | 3 | none | off | alarm |

`union_hot_score_h3__w1` is the v4 base. `union_hot_score_h3__w0` is its weather-off twin. Sort is `hot_score`. Sell is `list`. Side is long. Universe is union. `exit_when` is empty.

## Grid

Book width N is 4, 6, or 8 names. The slot is equal weight: new buys use the leftover split, not a score weight.

Per-name weight cap is 20%, 25%, or none.

Candidate id is `{base_id}__n{N}__c{20|25|none}`. Order is the base table above, then N 4, 6, 8, then cap 20, 25, none.

5 × 3 × 3 = 45 candidates. That is the try count. A Monday start is the rank key inside a try. It is not another try. Keep-held is the only fill. It is not a second try.

## Cash from a trim

Leftover cash sits in cash. A trim does not add shares to a name that is already held. Cash already in the account, including cash raised by a same-morning exit or trim, is split equally across newly selected names that are not already held. Whatever those buys do not spend stays cash. That buy is the keep-held leftover split.

## Fill model and the trim

Keep-held matches `src/webull_exec.py` `size_hot4_tickets` on the live path, as the v4 engine walks it. A name that is still selected and already held is not bought again and pays no buy fee. A full sell is a recipe exit: list-drop through `lot_should_sell` after the min-hold. Hard-red, and only when weather is on, sits new buys. It does not sell a lot by itself. Weather off buys on a hard-red morning. A missing S does not sit. Holdup, when the base boost is holdup, S is present and above 0, and the morning is not a hard-red sit, sets the new lot's min-hold to 2.

The walker is the v4 keep-held walker (`research/factor_mine_recipe_search_v4/engine.py`) with one added step. When the cap is none, that step is skipped and the book is the v4 book at the new width.

When the cap is 20% or 25%, the step runs at the session open after full exits and before new buys. Book equity is cash plus open marks of the names still held. If a name's open value is above cap times that equity, shares are sold at the open. The shares kept are the largest integer whose remaining value is at or under cap times (equity minus the Futubull fee on the sold shares). Names are handled in ticker order. Each name sees equity after earlier trims that same morning. One pass. The Futubull fee is charged on the sold shares. The flat 15bp book sells the same shares and charges 7.5bp a side. A trim that leaves zero shares is a closed trade. A trim that leaves shares is not a closed trade. The next open trims again if the name is over the cap again. A new buy is not pre-cut to the cap. If it opens above the cap, the following session's open trims it.

Whole shares. One share that does not fit is skipped. Sell first, then trim, then buy. A fresh book starts at $10,000.

## Pick

Tune sessions are 2026-08-13 through 2026-09-11. The tune walker does not read a session on or after 2026-09-14.

Monday starts, each a fresh $10,000 book through 2026-09-11: 2026-08-17, 2026-08-24, 2026-08-31. 2026-09-07 is not a session.

Win rate is winning closed trades divided by closed trades. A win is Futubull pnl above zero on the sell date. Winning-day share is up days divided by days with a buy, a full sell, or a trim. Up is a Futubull day return above zero. Down is below zero. Any other active day is flat. The joint at a start is the lower of the win rate and the winning-day share. A start with fewer than 30 closed trades has no joint.

A passer has a joint at every Monday start, and the continuous book from 2026-08-13 through 2026-09-11 has a Futubull return above zero after the top 3 contributing stocks in that window are removed. Passers sort by the worst start's joint descending, then by the mean joint descending, then by id ascending. The freeze is that passer list. It is written before any 2026-09-14 score.

The top 3 are the three largest summed daily pnl contributors in the window. Ties break by the earlier first session in the window, then by ticker. Fewer than three names drops every name that has pnl. The without-stock return uses the fixed v4 `_drop_compound` arithmetic: each removed name's daily pnl is taken out of that day's equity change, and the path starts at the window's starting equity. On this tune book that start is $10,000.

## Reject window

2026-09-14 through 2026-09-25 can only reject a frozen passer. It cannot add a recipe and it cannot change the rank. The book is the continuous walk from 2026-08-13. P2 starts at the 2026-09-11 close. It does not restart at $10,000. The without-stock path for P2 starts at that same close.

A frozen passer is rejected when the P2 window has at least 30 closed trades and the joint is under 0.5. Fewer than 30 closed trades does not reject and does not prove. A frozen passer that is not rejected is the carry list.

## What every row reports

For the continuous tune window and for the P2 window, each of the 45 rows reports: Futubull total return, flat 15bp total return, Futubull return without the top 1, top 3, and top 5 contributing stocks, the best stock's profit share, distinct tickers, closed trades, and win rate. Profit share is the best stock's summed daily pnl divided by the window's ending equity minus its starting equity. A zero change is reported as blank. Distinct tickers are names with pnl, a buy, or a closed trade in the window.

IWM buy-and-hold is reported once per window: buy at the window's first open, whole shares, Futubull entry fee, mark at each close. RANDOM4 is reported as the mean compound: seed 20260813, 1000 draws, 4 names from that morning's board, the recipe's hold, sell, holdup, weather, and weight cap, not its filter or sort. Both are baselines. Neither is a pass rule.

The keep bar, at least 30 closed trades and a win rate above 55% after fees, is reported. It does not add or remove a frozen row.

## Luck

A try is one candidate. There are 45. Luck N is 45 + 21,536 + 260 = 21,841. 21,536 is the v4 luck total. 260 is the try count Cyrus assigned to `concentration_screen_v1`.

## Prices, inputs, fees

Prices are the cleaned tape `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` `9a0e12ca2f7a3e87f965190e8766053a37ffaabc`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. The loader is the v4 `CleanStore` read. Open is the fill. Close is the mark. A missing open keeps the slot.

Before a compound is written, every cleaned bar on or before the window's last session, for IWM and for every board ticker in that window, is checked. A leg above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) has that ticker and that date and the ratio is within 25% of the split or of one divided by the split. An unexplained jump halts the window. No freeze is written.

The board and the morning S are the v4 `INPUTS.json` rows, sha256 `0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969`. The tune phase drops every date after 2026-09-11 before the walk. The forward phase runs only after the freeze file exists.

Futubull fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Flat 15bp uses the same share counts.

## Carry

A frozen passer that P2 does not reject is appended to `research/forward_shadow_v1/hooks/v4_winners.json`. The ledger, the recipe spec, and the forward preregistration stay as they are. The winner name starts with `fwd_ccap_`. `first_session` is 2026-09-28, the first locked session after this fingerprint. The hooked recipe carries the base gates, the width, and the weight cap. The forward runner trims at the next open when `weight_cap` is a number, with the same share rule and the same sit-in-cash rule. A recipe with no `weight_cap` is unchanged.

## IRONCLAD

| rule | how this study treats it |
| --- | --- |
| 1–3 append-only records | No locked day and no earlier study file is rewritten. The forward ledger is not edited. |
| 4 new rules, new name | `concentration_cap_v1` is a new name. Each width and cap is a new id. |
| 5 designed_after | Creation date 2026-09-26. Every session scored here is `designed_after`. |
| 7–10 knowable at 09:30 | Names and S are the v4 stored morning rows. |
| 11 prices | Cleaned v1c only, sha-pinned. An unexplained jump halts the window. |
| 12 fills | Buy at the 09:30 open. Keep-held does not buy a name that is already held. A trim sells at that open. |
| 13 stop and target | These recipes have no stop and no target. |
| 14 fees | Futubull is the figure that passes or rejects. Flat 15bp is the same shares beside it. |
| 15–17 freeze before the later window | This preregistration is committed before any score. The passer freeze is committed before any 2026-09-14 score. |
| 18 candidate cap | 45 candidates. Luck N is 21,841. |
| 19 RANDOM4 and IWM | Seed 20260813, 1000 draws, and IWM on the same days. |
| 20 without the best name | Without the top 1, top 3, and top 5, in each window. The tune hard filter is the top 3. |
| 21 thirty fires | Under 30 closed trades has no joint. It cannot pass and it cannot reject. |
| 22 untestable | A book that never trades is not a passer. |
| 23 no real money | This search selects nothing for a live account. |
