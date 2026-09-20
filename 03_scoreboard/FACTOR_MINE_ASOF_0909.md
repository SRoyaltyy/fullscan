# Factor mine as-of 2026-09-09 — overfit remine

In-sample: **2026-08-13 → 2026-09-09** (19 sessions, 1668 rows). Out-of-sample: **2026-09-10 → 2026-09-18**.

Recipe grid: auto slice + auto-tweak neighbors + featured unions / combos / holdup / overnight / shorts / Clock-B catalogue. Mined **339** sleeves on 9/9 data only. KEEP selection never saw 2026-09-10–2026-09-18.

Same WORKABLE_BAR as live: min_trades 30, min_win 0.55, min_book_pct 0.0, min_start 0.5, min_dollar_days 0.4.

OOS **continued** walks the 9/9 cash book forward (lots + leftover). OOS **fresh $10k** wakes the same frozen recipe on 2026-09-10 with empty lots.

Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` are the full-window 8/13–9/18 pack and were not written.

## KEEP on 9/9 data + OOS holdout

| Strategy | Side | Selected 9/9 | ALWAYS | IS win% | IS $days | IS start | IS n | IS book% | OOS cont book% | OOS $days | Fresh $10k | Fresh $days |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `flatten_h5` | long | no | yes | 49% | 68% | 6/19 | 79 | +10.54 | -6.47 | 43% | -4.83 | 29% |
| `union_hot_n4_holdup` | long | no | yes | 53% | 63% | 17/19 | 62 | +37.49 | +13.85 | 57% | +14.09 | 57% |
| `overnight_mega_h1` | long | no | yes | 30% | 16% | 1/19 | 20 | -0.27 | +0.00 | 0% | +0.00 | 0% |
| `overnight_mega_h2` | long | no | yes | 57% | 11% | 7/19 | 6 | +0.50 | +0.00 | 0% | +0.00 | 0% |
| `overnight_h1` | long | no | yes | 31% | 32% | 1/19 | 94 | -10.88 | -9.15 | 0% | -9.13 | 0% |
| `combo_oh_5050_shared` | mix | no | yes | 56% | 42% | 9/19 | 100 | +5.75 | +12.16 | 57% | +12.17 | 57% |
| `combo_e1s_7030_shared` | mix | YES |  | 57% | 74% | 16/19 | 188 | +19.92 | -4.35 | 29% | -5.13 | 14% |
| `combo_jse_333_shared` | mix | YES |  | 60% | 63% | 16/19 | 220 | +30.34 | -0.21 | 57% | -0.88 | 43% |
| `combo_p2s_5050_shared` | mix | YES |  | 65% | 68% | 17/19 | 128 | +21.77 | -8.41 | 43% | -9.27 | 29% |
| `combo_ps_5050_shared` | mix | YES |  | 66% | 74% | 17/19 | 132 | +21.19 | -8.41 | 43% | -9.27 | 29% |
| `combo_ps_7030_shared` | mix | YES |  | 65% | 68% | 17/19 | 134 | +20.79 | -6.80 | 57% | -7.47 | 43% |
| `combo_se1_5050_shared` | mix | YES |  | 58% | 74% | 16/19 | 188 | +24.44 | -10.00 | 29% | -10.67 | 14% |
| `combo_se_5050_shared` | mix | YES |  | 60% | 58% | 14/19 | 150 | +24.21 | -5.07 | 43% | -5.38 | 29% |
| `combo_se_5050_skip` | mix | YES |  | 62% | 53% | 14/19 | 148 | +26.07 | -0.19 | 57% | -0.67 | 43% |
| `combo_se_5050_weather` | mix | YES |  | 60% | 58% | 14/19 | 150 | +24.21 | -5.07 | 43% | -5.38 | 29% |
| `combo_se_7030_shared` | mix | YES |  | 62% | 58% | 13/19 | 140 | +21.42 | -8.02 | 29% | -8.50 | 14% |
| `combo_ser_5050_shared` | mix | YES |  | 56% | 47% | 15/19 | 150 | +22.03 | -10.51 | 29% | -10.82 | 14% |
| `combo_sh_3070_shared` | mix | YES |  | 60% | 63% | 17/19 | 148 | +29.81 | +4.11 | 71% | +3.64 | 57% |
| `combo_sh_5050_shared` | mix | YES |  | 62% | 68% | 17/19 | 148 | +33.46 | -0.94 | 71% | -1.66 | 57% |
| `combo_sh_7030_shared` | mix | YES |  | 62% | 74% | 17/19 | 148 | +28.70 | -3.91 | 71% | -4.58 | 57% |
| `combo_sh_macd_5050_shared` | mix | YES |  | 62% | 58% | 17/19 | 138 | +35.66 | +2.81 | 43% | +2.12 | 29% |
| `combo_sj_3070_shared` | mix | YES |  | 57% | 74% | 17/19 | 186 | +20.50 | +3.68 | 43% | +2.89 | 29% |
| `combo_sj_5050_shared` | mix | YES |  | 57% | 79% | 17/19 | 186 | +21.25 | -0.13 | 43% | -0.97 | 29% |
| `combo_sn_3070_shared` | mix | YES |  | 58% | 74% | 17/19 | 204 | +12.88 | -13.23 | 29% | -13.29 | 14% |
| `combo_sn_5050_shared` | mix | YES |  | 60% | 74% | 17/19 | 202 | +14.13 | -17.11 | 29% | -17.88 | 14% |
| `combo_sn_7030_shared` | mix | YES |  | 60% | 68% | 17/19 | 202 | +11.42 | -16.79 | 29% | -17.17 | 14% |
| `short_news_head_h3` | short | YES |  | 58% | 68% | 17/19 | 48 | +13.60 | -10.70 | 43% | -11.29 | 29% |
| `short_news_or_h3` | short | YES |  | 57% | 63% | 17/19 | 72 | +11.88 | -10.54 | 29% | -11.16 | 14% |
| `short_news_r_h3` | short | YES |  | 57% | 63% | 17/19 | 72 | +11.88 | -10.54 | 29% | -11.16 | 14% |
| `short_news_r_macd_h3` | short | YES |  | 62% | 68% | 17/19 | 62 | +13.39 | -3.59 | 29% | -4.23 | 14% |
| `union_news_pack_h1` | long | YES |  | 58% | 47% | 17/19 | 62 | +15.08 | +5.65 | 43% | +5.73 | 43% |
| `union_news_pack_net2_h1` | long | YES |  | 60% | 47% | 17/19 | 56 | +19.16 | +5.78 | 43% | +5.73 | 43% |
| `union_news_pack_net3_h1` | long | YES |  | 60% | 47% | 17/19 | 56 | +19.16 | -1.97 | 14% | -1.96 | 14% |
| `union_hot_n4_h1` | long | no |  | 53% | 42% | 11/19 | 80 | +18.84 | +12.20 | 57% | +12.17 | 57% |

## Verdict

Cutoff **2026-09-09** inclusive. Holdout **2026-09-10 → 2026-09-18**.
Recipes that the 9/9 bar would KEEP: **27**. ALWAYS extras that fail the 9/9 bar: **6**.
Clock-B catalogue sleeves were in the grid; **none** passed the 9/9 bar. Excel fee-KEEP prove is a separate path and is not in this recipe set.

This was a **full-grid remine** (260 auto+tweak recipes + 79 combos → 339 scored) on the standing panel sliced to 8/13–9/9 (1,668 rows / 19 sessions), not a slice of the already-pruned live 25.

**Still working after the cut** (KEEP on 9/9 *and* continued OOS book% > 0): `combo_sh_3070_shared` (+4.11%), `combo_sh_macd_5050_shared` (+2.81%), `combo_sj_3070_shared` (+3.68%), `union_news_pack_h1` (+5.65%), `union_news_pack_net2_h1` (+5.78%).

**KEEP on 9/9, then faded OOS** (honestly selected, then gave it back): the se / ps / sn / news-short family, plus `combo_sh_5050_shared` (−0.94%) and `combo_sh_7030_shared` (−3.91%). Those names did *not* need 9/10–9/18 to be chosen — they just did not hold the 9/9 book through the holdout.

**Would not have been selected on 9/9 data alone** (the live-board shine is not a 9/9 KEEP):

- `union_hot_n4_h1` — win 53% < 55%. Fails the same bar on the live 8/13–9/18 pack too. OOS continued book is still +12.20% / $days 57%, so the *path* is fine; the *bar* never picks it.
- `union_hot_n4_holdup` — same win 53%. ALWAYS extra. Best IS book in this report (+37.49%) and still +13.85% OOS, but a clean remine would not KEEP it.
- `combo_oh_5050_shared` — start 47% < 50%. ALWAYS extra. OOS continued +12.16%.
- `flatten_h5` / overnight mega / `overnight_h1` — fail the 9/9 bar (starts / trades / win). Overnight is flat-to-red after the cut.

### Do hot4 / holdup / sh survive a clean remine?

- **hot4 (`union_hot_n4_h1`) — no.** Not KEEP on 9/9. Not KEEP on the live full window either (win 51%). It is a featured / money path, not a bar passer.
- **holdup (`union_hot_n4_holdup`) — no as KEEP, yes as cash.** Fails win% on 9/9. The ALWAYS pin is what keeps it on the live board. The frozen path is still green after the cut.
- **sh combos — yes for selection, mixed for OOS.** All four `combo_sh_*` names pass the 9/9 bar (win 60–62%, starts 17/19, book +29 to +36%). They are **not** a 9/10–9/18-only artifact. After the cut, `combo_sh_3070_shared` and `combo_sh_macd_5050_shared` stay green; the 50/50 and 70/30 give a little back. The live-board +29–36% full-window print is mostly the 8/13–9/9 book, not a holdout jackpot.

### Focus names

- `union_hot_n4_h1`: would NOT pass the 9/9 bar. IS book +18.84% · OOS continued +12.20% · fresh $10k +12.17% · OOS $days 57%.
- `union_hot_n4_holdup`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book +37.49% · OOS continued +13.85% · fresh $10k +14.09% · OOS $days 57%.
- `combo_sh_5050_shared`: would KEEP. IS book +33.46% · OOS continued -0.94% · fresh $10k -1.66% · OOS $days 71%.
- `combo_sh_macd_5050_shared`: would KEEP. IS book +35.66% · OOS continued +2.81% · fresh $10k +2.12% · OOS $days 43%.
- `combo_sh_3070_shared`: would KEEP. IS book +29.81% · OOS continued +4.11% · fresh $10k +3.64% · OOS $days 71%.
- `combo_sh_7030_shared`: would KEEP. IS book +28.70% · OOS continued -3.91% · fresh $10k -4.58% · OOS $days 71%.
- `combo_oh_5050_shared`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book +5.75% · OOS continued +12.16% · fresh $10k +12.17% · OOS $days 57%.
- `flatten_h5`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book +10.54% · OOS continued -6.47% · fresh $10k -4.83% · OOS $days 43%.
- `overnight_mega_h1`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book -0.27% · OOS continued +0.00% · fresh $10k +0.00% · OOS $days 0%.
- `overnight_mega_h2`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book +0.50% · OOS continued +0.00% · fresh $10k +0.00% · OOS $days 0%.
- `overnight_h1`: would NOT pass the 9/9 bar (ALWAYS extra — still reported). IS book -10.88% · OOS continued -9.15% · fresh $10k -9.13% · OOS $days 0%.

