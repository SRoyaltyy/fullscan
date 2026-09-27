# start_day_sweep_v1

Report only. No recipe is picked, frozen, or added to any forward hook.

Luck N is 22,009, unchanged. New tries: 0, because nothing is selected.

Each book starts with $10,000 at the open of one pinned session and is carried, day by day, through the 2026-09-25 close. The start days are the 30 pinned sessions from 2026-08-13 through 2026-09-24. A start is positive when ending equity is above the $10,000 it started with. The 15bp column is that same keep-held book with a flat 15bp fee instead of the Futubull schedule. The median is the average of the 15th and 16th ending returns after they are sorted. The worst is the lowest ending return.

The walker, the pinned inputs, and the keep-held Futubull fee path are the ones in factor_mine_recipe_search_v4. Group 3 rows use that same walker on the Group 3 recipe (8 names, weather off), the path concentration_screen_v1 already used. The two cap rows use the concentration_cap_v3 book, which is the concentration_cap_v1 keep-held walker with a 20% weight cap and leftover cash left sitting.

P2 is 2026-09-14 through 2026-09-25, the same window as concentration_screen_v1. For a v4 row the P2 days are taken from the book that started on 2026-08-17 and was carried forward. For a Group 3 or cap row they are taken from the book that started on 2026-08-13. An up day finished above the prior close. A down day finished below it. A flat or sat-out day finished even with it, including a day that made no trade. Trades each P2 session are the buy, sell, and trim orders at that open, in this session order: 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25.

The 2026-08-17, 2026-08-24, and 2026-08-31 starts reproduce the concentration_screen_v1 P1 and P2 numbers for the v4 rows. The largest absolute difference on those returns, including the 15bp returns, is 0. The Group 3 books that start on 2026-08-13 reproduce that screen's P1 and P2 numbers. The two cap books reproduce the concentration_cap_v3 tune window and its P2 window. The largest absolute difference on every reproduction check in this file is 0.

| Recipe | Family | Positive start days | Positive start days at 15bp | Median ending return | Worst ending return | P2 up days | P2 down days | P2 flat or sat-out days | Trades each P2 session |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `union_hot_n4_holdup__w0` | v4 | positive from 30 of 30 start days | positive from 30 of 30 start days | 54.47% | 29.37% | 6 | 4 | 0 | 5, 6, 7, 2, 8, 6, 7, 8, 4, 7 |
| `union_hot_n4_holdup__w1` | v4 | positive from 29 of 30 start days | positive from 29 of 30 start days | 36.01% | -1.66% | 7 | 3 | 0 | 0, 2, 6, 0, 8, 4, 7, 8, 1, 6 |
| `union_hot_n4_h1__w0` | v4 | positive from 30 of 30 start days | positive from 30 of 30 start days | 50.74% | 29.37% | 5 | 5 | 0 | 6, 5, 7, 8, 8, 8, 6, 5, 5, 6 |
| `union_hot_n4_h1__w1` | v4 | positive from 29 of 30 start days | positive from 29 of 30 start days | 23.55% | -1.66% | 5 | 5 | 0 | 3, 0, 5, 8, 8, 8, 6, 5, 2, 5 |
| `union_hot_n4_h1_nonews__w0` | v4 | positive from 30 of 30 start days | positive from 30 of 30 start days | 50.05% | 28.78% | 5 | 5 | 0 | 6, 5, 7, 8, 8, 8, 6, 5, 5, 5 |
| `union_hot_n4_h1_nonews__w1` | v4 | positive from 29 of 30 start days | positive from 29 of 30 start days | 23.00% | -2.10% | 5 | 5 | 0 | 3, 0, 5, 8, 8, 8, 6, 5, 2, 4 |
| `union_hot_n4_h1_time__w0` | v4 | positive from 30 of 30 start days | positive from 30 of 30 start days | 43.78% | 29.08% | 5 | 5 | 0 | 8, 7, 7, 8, 8, 8, 8, 7, 7, 8 |
| `union_hot_score_h3__w0` | v4 | positive from 26 of 30 start days | positive from 29 of 30 start days | 4.44% | -8.91% | 5 | 5 | 0 | 17, 8, 7, 13, 9, 12, 12, 8, 10, 13 |
| `union_hot_score_h3__w1` | v4 | positive from 26 of 30 start days | positive from 29 of 30 start days | 3.63% | -2.24% | 6 | 4 | 0 | 1, 0, 11, 0, 0, 12, 1, 4, 6, 8 |
| `union_ret_5_h3` | Group 3 | positive from 27 of 30 start days | positive from 29 of 30 start days | 6.84% | -7.75% | 5 | 5 | 0 | 18, 8, 5, 14, 9, 12, 13, 9, 11, 14 |
| `union_vol_ab_h1` | Group 3 | positive from 15 of 30 start days | positive from 17 of 30 start days | 7.84% | -10.92% | 4 | 6 | 0 | 16, 16, 9, 2, 5, 12, 14, 16, 16, 16 |
| `union_hot_n4_holdup__w0__n4__c20` | cap v3 | positive from 30 of 30 start days | positive from 30 of 30 start days | 47.56% | 22.94% | 6 | 4 | 0 | 7, 6, 7, 8, 8, 9, 7, 9, 6, 7 |
| `union_hot_n4_h1_nonews__w0__n4__c20` | cap v3 | positive from 30 of 30 start days | positive from 30 of 30 start days | 44.78% | 26.33% | 5 | 5 | 0 | 7, 5, 7, 8, 8, 8, 7, 6, 6, 6 |
