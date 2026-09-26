# factor_mine_recipe_search_v4 — preregistration

- status: measurement locked. This commit has no returns and no compounds.
- written: 2026-09-26
- study: factor_mine_recipe_search_v4
- fingerprint_sha256: 0503a5a549a66093754c9102eb430d01bc035687b08bca4f04d699e3a7daeca7
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- question: which long Factor Mine books, built from existing parts around union_hot_score_h3, beat that base on the worse of win rate and winning-day share at the worst Monday start through 2026-09-11. The 2026-09-14 window can only reject a frozen recipe.
- creation date: 2026-09-26. Every session is `designed_after`. The numbers are research. They are not a live record.
- inputs_sha256: 0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969
- manifest_sha256: 1ca3bd4d1737474633391ef9780c2b9108be07092406e9526d4785605f669935

<!-- BEGIN COVERED -->

## What this study is

A new search, `factor_mine_recipe_search_v4`. It does not edit #364, #365, #366, #367, or #368. The engine is Factor Mine's group-3 recipe: `pick_day`, `matches`, `should_exit`, and `lot_should_sell`, on the union list, longs only. It is not a board-Score formula.

`union_hot_score_h3` with the weather gate on is the base (`union_hot_score_h3__w1`). If no other passer beats that base, the verdict is keep base. `union_hot_n4_h1` with the weather gate on was already picked from #368's 110. Its pre-09-11 lead is not new evidence. It is walked and shown. It cannot be the recipe that beats the base.

## Fill model

The live paper path is keep-held. `src/webull_exec.py` `size_hot4_tickets` skips a buy when the ticker is already held (`already held — skip`). A sell is a recipe exit: list-drop through `lot_should_sell` after the min-hold, or an early `exit_when` that function already fires. Hard-red sits new buys. It does not sell the lot by itself. A name that is still selected and already held stays, with no trade and no fee.

The old Group 3 walker renews that name: it sells at the open once the lot's min-hold is up and buys it again at that same open, paying Futubull fees on both sides. That sell plus a buy is reported beside the keep-held figure. Keep-held is the primary Futubull figure. The two fills are the same try. They are not an extra selection and they are not in the luck product.

`03_scoreboard/RESTATEMENTS.md` records the option on the keep-held branch. This study does not restate a locked day.

## Grid

Twenty-five part rows. Each is run with the weather gate on and off. That is 50 candidates. Weather on sits new buys when stored S is present and S <= -3. A missing S does not sit. Weather off buys on those mornings too. Lots already held still follow the exit rule.

The part rows use existing Factor Mine ranges only: sort `hot_score`, `candle_score`, `cond`, `ret_5`, `w_hot_cond`, `w_hot_candle`; N of 4, 8, or 12; hold 1, 3, or 5; sell `list` (list-drop at the 09:30 open after the min-hold) or `time`; `s_boost` `holdup` or `none`; must `last_green`, the coil bounds (`ret_5` 0 to 10 and `rvol` 0.7 to 2.2), or `vol` good; must-not `alarm`, and on two rows `alarm` plus `news` bad; one row exits early on `alarm`.

Two further base-shaped rows are in the 25. `union_hot_score_h3_earnnews` requires an earnings reaction or a morning news camera of good, from the earliest board labelled for that day. `union_hot_score_h3_skipfirst` skips a name on its first day on the list (`days_on_list` 1, meaning it was absent from the previous session's board). These patterns were found by #365 on the same pre-09-11 days this study ranks on, so their pre-09-11 numbers are optimistic. #365 saw about five earnings-react trades after 2026-09-14. The forward record from 2026-09-28 decides. A full on/off cross of those two parts through every other part would be 184 candidates, past the 50 cap in rule 18, so they are not crossed and they are not a second multiplier. The must and must-not rows stay.

| name | sort | N | hold | sell | boost | extra |
| --- | --- | ---: | ---: | --- | --- | --- |
| `union_hot_score_h3` | hot_score | 8 | 3 | list | none | base, weather on is the base id |
| `union_hot_n4_h1` | hot_score | 4 | 1 | list | none | already picked when weather is on |
| `union_hot_n4_holdup` | hot_score | 4 | 1 | list | holdup | |
| `union_candle_score_h3` | candle_score | 8 | 3 | list | none | |
| `union_cond_h3` | cond | 8 | 3 | list | none | |
| `union_ret_5_h3` | ret_5 | 8 | 3 | list | none | |
| `union_w_hot_cond_h3` | w_hot_cond | 8 | 3 | list | none | |
| `union_w_hot_candle_h3` | w_hot_candle | 8 | 3 | list | none | |
| `union_hot_n12_h1` | hot_score | 12 | 1 | list | none | |
| `union_hot_n4_h3` | hot_score | 4 | 3 | list | none | |
| `union_hot_score_h1` | hot_score | 8 | 1 | list | none | |
| `union_hot_n4_h5` | hot_score | 4 | 5 | list | none | |
| `union_hot_score_h5` | hot_score | 8 | 5 | list | none | |
| `union_hot_score_h3_time` | hot_score | 8 | 3 | time | none | |
| `union_hot_n4_h1_time` | hot_score | 4 | 1 | time | none | |
| `union_hot_score_h3_exitalarm` | hot_score | 8 | 3 | list | none | exit on alarm |
| `union_hot_score_h3_holdup` | hot_score | 8 | 3 | list | holdup | |
| `union_hot_score_h3_green` | hot_score | 8 | 3 | list | none | must last_green |
| `union_hot_n4_h1_green` | hot_score | 4 | 1 | list | none | must last_green |
| `union_hot_score_h3_coil` | hot_score | 8 | 3 | list | none | coil bounds |
| `union_hot_score_h3_nonews` | hot_score | 8 | 3 | list | none | must-not alarm and news bad |
| `union_hot_n4_h1_nonews` | hot_score | 4 | 1 | list | none | must-not alarm and news bad |
| `union_hot_score_h3_vol` | hot_score | 8 | 3 | list | none | must vol good |
| `union_hot_score_h3_earnnews` | hot_score | 8 | 3 | list | none | earnings or news reaction |
| `union_hot_score_h3_skipfirst` | hot_score | 8 | 3 | list | none | skip first day on the list |

Candidate id is the name plus `__w1` or `__w0`.

## Pick

Fresh $10,000 books. Rank starts are the Monday sessions from 2026-08-17 through 2026-09-08: 2026-08-17, 2026-08-24, and 2026-08-31. 2026-09-07 is not a session, so it is not a start. Each book runs from that start through 2026-09-11.

Win rate is winning closed trades divided by closed trades. A win is Futubull pnl above zero, counted on the sell date. Winning-day share is up days divided by days with a buy or a sell. Up is a Futubull day return above zero, down is below zero, and the rest of those active days are flat. The joint at a start is the lower of the win rate and the winning-day share. The rank key is the joint at the worst start. A start with fewer than 30 closed trades has no joint. A passer needs a joint at every start. Passers sort first, by rank key descending, then by the mean joint descending, then by id. The others follow, by how many starts cleared, then by id.

The freeze, written before any 2026-09-14 score, is the passers plus the top 10 plus the top 20 of that order. From 2026-09-14 through 2026-09-25 the continuous book can only reject a frozen recipe: at least 30 closed trades and a joint under 0.5. Fewer than 30 closed trades cannot prove. A recipe that is not rejected is not thereby proven. Frozen recipes carry forward from 2026-09-28.

Beside each result: Futubull return, the flat 15bp return, the renew return, win rate, up/down/flat days, trades, return without the best stock, the share of closed trades whose entry is under $3, and the return with CYPH, GLND, and INDP each removed. Benchmarks on the same days: the base, IWM buy-and-hold after the entry fee, and RANDOM4 (seed 20260813, 1000 draws of 4 names from that morning's board, the recipe's hold, sell, holdup, and weather, not its filter or sort).

## Luck

A v4 try is one part row, one start, and one weather setting. 25 × 3 × 2 = 150. Keep-held and renew are one try. The earnings and skip-first rows are already inside the 25.

Luck N is 21536. That is 150, plus #368's 9,500, plus Theme Radar's 9,132 Finviz tries in `theme-radar` `research/lever_panel/prior_tries_daily_returns.csv.gz`, plus the cells walked in #364, #366, and #367. #364 is 34 formulas × X in {2, 4, 8} × 1 start = 102. #366 is 34 × 3 × 13 starts = 1326. #367's own grid is 34 × 3 × 13 = 1326. #367's headline 2754 already contains #364 and #366, so that headline is not added again.

## Prices and inputs

Prices are the cleaned tape `research/breadth_rank_v1c/bars/ohlc.parquet` on `origin/main` `9a0e12ca2f7a3e87f965190e8766053a37ffaabc`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`, sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`. The loader is the #368 `CleanStore` read: duckdb, date as a string, open is the fill, close is the mark. A missing open keeps the slot and is not replaced by the close.

The board for day D is the earliest `origin/main` commit whose `data/factor_mine/panel.json` contains D. A later rebuild is not used. Morning S is the earliest `01_daily/general/D_predict.md` total score, else the earliest `01_daily/weather/D_weather.json` `general_score`. 2026-08-24's earliest predict score is 0, so the weather gate does not sit. 2026-08-27 has no score, so the weather gate does not sit. Stored hard-red mornings are 2026-08-18, 2026-08-19, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-14, 2026-09-15, and 2026-09-24.

`INPUTS.json` sha256 is `0ed63996e5a02fc5190785aa15f001ce6ed79ec1bb7eb5348d04f59d4f659969`. `MANIFEST.json` sha256 is `1ca3bd4d1737474633391ef9780c2b9108be07092406e9526d4785605f669935`. The manifest is the per-session commit, blob, row count, and S source. The walk reads the stored rows and the stored S.

Before a compound is written, every cleaned bar on or before the window's last session, for IWM and for every board ticker, is checked. A leg above 3 or below 1/3 is unexplained unless `research/breadth_rank_v1c/bars/splits.json` (sha256 `24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc`) has that ticker and date and the ratio is within 25% of the split or of one divided by the split. An unexplained jump halts that window. No freeze is written.

Futubull fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Flat 15bp is the same share counts with 7.5bp per side.

## Daily series

Every candidate's continuous book, from 2026-08-13, is committed as one JSON per session under both Futubull fees and flat 15bp, for keep-held and for renew. The tune commit stops at 2026-09-11. The forward commit appends 2026-09-14 through 2026-09-25 and does not rewrite the earlier files. Fresh-start Futubull daily returns are committed under `returns/starts/`.

## Book

One name that cannot buy one share is skipped. The leftover slice is not retried on the next name. Sell first, then buy. Whole shares. Holdup, when that row's boost is on, S is present and above 0, and the morning is not hard-red, sets the new lot's min-hold to 2. The renew fill uses that same min-hold, so holdup also delays the round trip.

## IRONCLAD

| rule | how this study treats it |
| --- | --- |
| 1–3 append-only records | No locked day, #357, #362, #364, #365, #366, #367, or #368 file is rewritten. |
| 4 new rules, new name | `factor_mine_recipe_search_v4` is a new name. Keep-held is an option. The old renewal stays the Group 3 default outside this study. |
| 5 designed_after | Creation date 2026-09-26. Every session is `designed_after`. |
| 7–10 knowable at 09:30 | Names are the earliest panel that contains the session. S is the earliest predict, else the earliest weather file. |
| 11 prices | Cleaned v1c only, sha-pinned. An unexplained jump halts the window. |
| 12 fills | Buy at the 09:30 open. A missing open keeps the slot. Keep-held does not sell and rebuy a name that is still selected. |
| 14 fees | Futubull is the primary figure. Flat 15bp is the same shares beside it. |
| 15–17 freeze before the later window | This preregistration is committed before any score. The freeze of passers, top 10, and top 20 is committed before any 2026-09-14 score. |
| 18 candidate cap | 50 candidates. Luck N counts the 150 cells plus the prior tries named above. |
| 19 RANDOM4 and IWM | Seed 20260813, 1000 draws, and IWM on the same days. |
| 20 without the best name | Ex-best, and CYPH, GLND, and INDP each removed, sit beside the result. |
| 21 thirty fires | Under 30 closed trades is flagged. It cannot be a passer and it cannot prove a forward recipe. |
| 22 untestable | A book that never trades stays in the table and is not a passer. |
| 23 no real money | This search selects nothing for a live account. |
