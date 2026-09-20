# Factor mine blind formation — as-of 2026-09-09

This is a **blind formation remine**, not a KEEP-selection cut of the already-known live menu (that was [PR #285](https://github.com/SRoyaltyy/fullscan/pull/285)).

In-sample formation: **2026-08-13 → 2026-09-09** (19 sessions, 1668 rows). Out-of-sample (frozen discoveries only): **2026-09-10 → 2026-09-18**.

## What was frozen

- **Panel cutoff:** `2026-08-13` → `2026-09-09` only. No 9/10–09-18 row entered ranking, tweaking, or featuring.
- **Recipe-definition freeze:** yes. Recipe + combo-spec definitions frozen to commit cb7f09ae (2026-09-09, Factor-mine combo books #175). Current cash-book / mark / fee engine is used to score those frozen definitions. Clock-B, holdup, overnight_mega, fee-KEEP, and WORKABLE_ALWAYS were not in the 9/9 builders.
- **Excluded from the seed set:** Clock-B catalogue pins, `union_hot_n4_holdup`, overnight_mega / overnight_h1, `combo_oh_5050_shared`, `combo_sh_macd_5050_shared`, `short_news_r_macd_h3`, WORKABLE_ALWAYS extras, FOCUS / LONG_LED_PIN live featured pins.
- **Kept:** generic 9/9 auto-grid primitives (universe / hold / gate / rank / side / top_n / exit / size / sell / S-boost as of 9/9). `union_hot_n4_h1` is in that 9/9 menu (added 2026-09-05) — it is not injected as a live FOCUS seed.
- **Holdup primitive:** `s_boost=holdup` did not exist on 9/9 (landed 2026-09-19). It was not swept and no holdup twin was invented.

## Method (same as the current board, knowledge-capped)

1. Leak-free 09:30 panel / $10k cash books (current engine).
2. Default **auto** slice + **auto-tweak** neighbor sweep on the frozen 9/9 recipe menu (**161** singles).
3. Combo construction: 9/9 combo-engine specs (**74** mixes) plus extra shared 50/50 / 70/30 / 333 mixes **formed from IS singles** (**10** extras). Total scored: **245**.
4. Rank / KEEP from IS only. Cyrus would-have-featured: Starts YES ≥17/19 (or ≥85% when start_n < 19), Book% > 0, n ≥ 30. No ALWAYS / FOCUS pins. Formal WORKABLE_BAR is reported beside it and does **not** pin FOCUS / WORKABLE_ALWAYS / hot4-holdup.
5. Frozen discoveries replayed OOS continued + fresh $10k. OOS never added a name.

Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` were not written.

Formal WORKABLE_BAR (reported, not a live-board pin): min_trades 30, min_win 0.55, min_book_pct 0.0, min_start 0.5, min_dollar_days 0.4.

## Blindly formed keepers (IS) + OOS book

| Strategy | Side | Cyrus | Formal bar | IS start | IS book% | IS win% | IS n | OOS cont book% | Fresh $10k | Members / note |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_5050_shared` | mix | YES | YES | 17/19 | +33.46 | 62% | 148 | +1.11 | +0.43 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_sh_3070_shared` | mix | YES | YES | 17/19 | +29.81 | 60% | 148 | +1.62 | +1.18 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_sh_7030_shared` | mix | YES | YES | 17/19 | +28.70 | 62% | 148 | +0.64 | -0.03 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_seh_451540_shared` | mix | YES | no | 17/19 | +27.38 | 50% | 238 | +0.66 | +0.17 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_601525_shared` | mix | YES | no | 17/19 | +27.01 | 50% | 238 | +0.26 | -0.25 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_502525_shared` | mix | YES | no | 17/19 | +25.80 | 50% | 244 | +0.27 | -0.07 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_form_jovogrh1snerh3_5050_shared` | mix | YES | YES | 17/19 | +21.25 | 57% | 186 | -0.77 | -1.46 | formed · `union_join_vol_green_h1` + `short_news_r_h3` |
| `combo_sj_5050_shared` | mix | YES | YES | 17/19 | +21.25 | 57% | 186 | -0.77 | -1.46 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_sj_3070_shared` | mix | YES | YES | 17/19 | +20.50 | 57% | 186 | -0.86 | -1.43 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_sj_7030_shared` | mix | YES | no | 17/19 | +16.61 | 54% | 186 | -0.52 | -1.21 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_form_nevoh1snerh3_5050_shared` | mix | YES | YES | 17/19 | +16.23 | 60% | 148 | -2.92 | -3.58 | formed · `union_news_vol_h1` + `short_news_r_h3` |
| `combo_form_negh1jovogrh1snerh3_333_shared` | mix | YES | no | 17/19 | +15.05 | 54% | 308 | -1.09 | -1.91 | formed · `union_news_g_h1` + `union_join_vol_green_h1` + `short_news_r_h3` |
| `combo_snj_333_shared` | mix | YES | no | 17/19 | +15.05 | 54% | 308 | -1.09 | -1.91 | `short_news_r_h3` + `union_news_g_h1` + `union_join_vol_green_h1` |
| `combo_form_negh1snerh3_5050_shared` | mix | YES | YES | 17/19 | +14.13 | 60% | 202 | -1.06 | -1.77 | formed · `union_news_g_h1` + `short_news_r_h3` |
| `combo_sn_5050_shared` | mix | YES | YES | 17/19 | +14.13 | 60% | 202 | -1.06 | -1.77 | `short_news_r_h3` + `union_news_g_h1` |
| `combo_form_negh1snerh3_7030_shared` | mix | YES | YES | 17/19 | +12.88 | 58% | 204 | -1.41 | -1.95 | formed · `union_news_g_h1` + `short_news_r_h3` |
| `combo_sn_3070_shared` | mix | YES | YES | 17/19 | +12.88 | 58% | 204 | -1.41 | -1.95 | `short_news_r_h3` + `union_news_g_h1` |
| `short_news_r_h3` | short | YES | YES | 17/19 | +11.88 | 57% | 72 | -0.04 | -0.69 | 9/9 short grid (news🔴 hold 3) |
| `combo_form_negh1snerh3_3070_shared` | mix | YES | YES | 17/19 | +11.42 | 60% | 202 | -0.76 | -1.32 | formed · `union_news_g_h1` + `short_news_r_h3` |
| `combo_sn_7030_shared` | mix | YES | YES | 17/19 | +11.42 | 60% | 202 | -0.76 | -1.32 | `short_news_r_h3` + `union_news_g_h1` |
| `combo_form_jovogrh1snerh1_5050_shared` | mix | YES | no | 17/19 | +5.91 | 48% | 192 | -1.45 | -1.46 | formed · `union_join_vol_green_h1` + `short_news_r_h1` |
| `combo_form_negh1snerh1_5050_shared` | mix | YES | no | 17/19 | +1.79 | 53% | 204 | -1.75 | -1.77 | formed · `union_news_g_h1` + `short_news_r_h1` |
| `combo_jse_333_shared` | mix | no | YES | 16/19 | +30.34 | 60% | 220 | -0.78 | -1.08 | `union_join_vol_green_h1` + `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_se_5050_skip` | mix | no | YES | 14/19 | +26.07 | 62% | 148 | -0.17 | -0.56 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_se1_5050_shared` | mix | no | YES | 16/19 | +24.44 | 58% | 188 | +0.14 | -0.56 | `short_news_r_h3` + `union_e_fresh_h1` |
| `combo_se_5050_shared` | mix | no | YES | 14/19 | +24.21 | 60% | 150 | -0.23 | -0.56 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_se_5050_weather` | mix | no | YES | 14/19 | +24.21 | 60% | 150 | -0.23 | -0.56 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_ser_5050_shared` | mix | no | YES | 15/19 | +22.03 | 56% | 150 | -0.25 | -0.59 | `short_news_r_h3` + `union_earn_react_h3` |
| `combo_se_7030_shared` | mix | no | YES | 13/19 | +21.42 | 62% | 140 | -0.30 | -0.73 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_e1s_7030_shared` | mix | no | YES | 16/19 | +19.92 | 57% | 188 | +0.34 | -0.28 | `union_e_fresh_h1` + `short_news_r_h3` |
| `union_hot_n4_h1` | long | no | no | 11/19 | +18.84 | 53% | 80 | +2.22 | +2.23 | 9/9 grid point (union / h1 / n4 / hot_score); not featured |

Formed extras (IS singles mixed with combo-engine primitives; not live FOCUS pins):

- `combo_form_jovogrh1snerh3_5050_shared` = `union_join_vol_green_h1` + `short_news_r_h3`
- `combo_form_nevoh1snerh3_5050_shared` = `union_news_vol_h1` + `short_news_r_h3`
- `combo_form_negh1jovogrh1snerh3_333_shared` = `union_news_g_h1` + `union_join_vol_green_h1` + `short_news_r_h3`
- `combo_form_negh1snerh3_5050_shared` = `union_news_g_h1` + `short_news_r_h3`
- `combo_form_negh1snerh3_7030_shared` = `union_news_g_h1` + `short_news_r_h3`
- `combo_form_negh1snerh3_3070_shared` = `union_news_g_h1` + `short_news_r_h3`
- `combo_form_jovogrh1snerh1_5050_shared` = `union_join_vol_green_h1` + `short_news_r_h1`
- `combo_form_negh1snerh1_5050_shared` = `union_news_g_h1` + `short_news_r_h1`

## Verdict

### Formation vs #285 selection-cut

- **#285** scored today's live recipe menu (including Clock-B, holdup, overnight_mega) and applied WORKABLE_BAR + ALWAYS / FOCUS pins. That is a selection cut among already-known names.
- **This remine** freezes recipe *definitions* to 2026-09-09, strips post-9/9 catalogue pins from the seed set, forms extra combos from IS winners, and features with Cyrus Starts YES + Book%. Live FOCUS / ALWAYS never enter the keep set.
- Formation extras independently rebuilt `combo_sj_*` / `combo_sn_*` (join-green or news-green + news🔴 short) and added a new IS mix `union_news_vol_h1` + `short_news_r_h3`. Those formed names were not copied from today's live FOCUS list.

### Would a 9/9 researcher have found hot4 / holdup?

- **hot4 / `union_hot_n4_h1`:** No — `union_hot_n4_h1` was scored because it is a 9/9 grid point (union / h1 / n4 / hot_score), but it failed featuring (Starts 11/19, win 53%, book +18.84%). Twins: `union_hot_n4_h1`. Hot4 OOS continued +2.22% · fresh $10k +2.23%.
- **holdup / `union_hot_n4_holdup`:** No — `union_hot_n4_holdup` was **not** in the 9/9 recipe menu (added 2026-09-19 with the holdup s_boost primitive). The remine did not seed it and did not invent a holdup twin. A 9/9 researcher using this method would not have found holdup.

Cyrus featured that still print a positive continued OOS book: `combo_sh_5050_shared`, `combo_sh_3070_shared`, `combo_sh_7030_shared`, `combo_seh_451540_shared`, `combo_seh_601525_shared`, `combo_seh_502525_shared`.

Cyrus featured that fade after the cut: `combo_form_jovogrh1snerh3_5050_shared`, `combo_sj_5050_shared`, `combo_sj_3070_shared`, `combo_sj_7030_shared`, `combo_form_nevoh1snerh3_5050_shared`, `combo_form_negh1jovogrh1snerh3_333_shared`, `combo_snj_333_shared`, `combo_form_negh1snerh3_5050_shared`, `combo_sn_5050_shared`, `combo_form_negh1snerh3_7030_shared`, `combo_sn_3070_shared`, `short_news_r_h3`, `combo_form_negh1snerh3_3070_shared`, `combo_sn_7030_shared`, `combo_form_jovogrh1snerh1_5050_shared`, `combo_form_negh1snerh1_5050_shared`.

Formal-bar KEEP count: **22**. Cyrus featured count: **22**.

OOS **continued** walks the 9/9 cash book forward. OOS **fresh $10k** wakes the frozen recipe on 2026-09-10 with empty lots.

