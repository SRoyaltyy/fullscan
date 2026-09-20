# Factor mine blind formation — as-of 2026-09-09 WITH Clock-B / oppset

This is a **blind formation remine** of the 9/9 method **including** Theme Radar Clock-B / oppset. It is not a KEEP-selection cut of the live menu ([PR #285](https://github.com/SRoyaltyy/fullscan/pull/285)) and it is not the no-oppset remine ([PR #286](https://github.com/SRoyaltyy/fullscan/pull/286)).

In-sample formation: **2026-08-13 → 2026-09-09** (19 sessions, 2019 rows). Out-of-sample (frozen discoveries only): **2026-09-10 → 2026-09-18**.

## Freeze contract

- **Panel / IS window:** `2026-08-13` → `2026-09-09` only. Oppset filter: `join_morning <= 2026-09-09`. No 9/10–9/18 row entered ranking, tweaking, or featuring.
- **9/9 grid freeze:** `cb7f09ae` (2026-09-09 close, #175).
- **Clock-B / oppset builder freeze:** `e3aabf24` — Clock-B / oppset recipe builders first landed e3aabf24 (2026-09-19, #279). Named union_clk_* / oppset_* sleeves were not in the 9/9 close menu; the atoms they stamp (prior tape, morning packet, prior Finviz, Theme Radar T−1 gap+RelVol) were T−1-clean as of 2026-09-09.
- **Combined freeze id:** `cb7f09ae+e3aabf24`.
- **Still stripped:** `union_hot_n4_holdup` / `s_boost=holdup` (landed 9/19), overnight_mega, WORKABLE_ALWAYS extras, FOCUS / LONG_LED_PIN live featured pins as *seeds*.
- **Included toolbox:** Clock-B catalogue flags (T−1 atoms) + Theme Radar oppset stamp + `FULLSCAN_OPPSET_UNION` top-30 aisle + `CLOCK_B_CORE` / `CLOCK_B_OPPSET_RECIPES`.
- **Holdup primitive:** still absent. Twins: none.

## T−1 / aisle proof

- Theme Radar leaks (`finviz_asof >= join_morning`): **0** (clean).
- IS oppset rows: **5067**. asof min `2026-08-13` · max `2026-09-08` (cap `2026-09-08`; OK — IS uses asof≤09-08 only).
- Aisle adds (full panel, not persisted): tagged 1406, added 498, slim fallback 0, top_n=30.
- IS slice rows after union: **2019** (#286 no-oppset was 1668).
- Live `data/factor_mine/panel.json` was not rewritten.

## Method (same as the current board, knowledge-capped, + aisle)

1. Leak-free 09:30 panel ∪ T−1 oppset (top 30 by `opp_rvol`) / $10k cash books (current engine). Clock-B flags stamped from T−1 features only.
2. Default **auto** slice + **auto-tweak** on the frozen 9/9 menu **plus** Clock-B / oppset catalogue (**179** singles).
3. Combo construction: 9/9 combo-engine specs plus Clock-B/oppset × S/E/H 50/50 / 333 mixes (**32** Clock-B specs) plus extras **formed from IS singles** (**10**). Total scored: **295**.
4. Rank / KEEP from IS only. Cyrus would-have-featured: Starts YES ≥17/19 (or ≥85% when start_n < 19), Book% > 0, n ≥ 30. No ALWAYS / FOCUS pins. Formal WORKABLE_BAR is reported beside it and does **not** pin FOCUS / WORKABLE_ALWAYS / hot4-holdup.
5. Frozen discoveries replayed OOS continued + fresh $10k. OOS `join_morning` never added a name.

Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` were not written.

Formal WORKABLE_BAR (reported, not a live-board pin): min_trades 30, min_win 0.55, min_book_pct 0.0, min_start 0.5, min_dollar_days 0.4.

## Blindly formed keepers (IS) + OOS book

| Strategy | Side | Cyrus | Formal bar | IS start | IS book% | IS win% | IS n | OOS cont book% | Fresh $10k | Members / note |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_ecearnguid_5050_shared` | mix | YES | YES | 17/19 | +30.34 | 55% | 106 | +0.00 | +0.61 | `union_e_fresh_h3` + `union_clk_earn_guide_react_h1` |
| `combo_se_5050_skip` | mix | YES | YES | 17/19 | +29.36 | 68% | 156 | -0.48 | -0.64 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_ecnr7momop_5050_shared` | mix | YES | YES | 17/19 | +28.68 | 60% | 105 | -0.98 | -0.94 | `union_e_fresh_h3` + `union_clk_nr7_mom_opp_h1` |
| `combo_se_5050_shared` | mix | YES | YES | 17/19 | +26.83 | 65% | 160 | -0.49 | -0.64 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_se_5050_weather` | mix | YES | YES | 17/19 | +26.83 | 65% | 160 | -0.49 | -0.64 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_scnr7momop_5050_shared` | mix | YES | YES | 17/19 | +25.48 | 75% | 104 | -3.46 | -4.13 | `short_news_r_h3` + `union_clk_nr7_mom_opp_h1` |
| `combo_form_efrh1jovogrh1snerh3_333_shared` | mix | YES | no | 17/19 | +24.92 | 55% | 320 | -0.37 | -1.14 | formed · `union_e_fresh_h1` + `union_join_vol_green_h1` + `short_news_r_h3` |
| `combo_se1_5050_shared` | mix | YES | YES | 17/19 | +23.62 | 62% | 204 | +0.14 | -0.56 | `short_news_r_h3` + `union_e_fresh_h1` |
| `combo_e1s_7030_shared` | mix | YES | YES | 17/19 | +22.59 | 62% | 204 | +0.45 | -0.19 | `union_e_fresh_h1` + `short_news_r_h3` |
| `combo_form_efrh1snerh3_7030_shared` | mix | YES | YES | 17/19 | +22.59 | 62% | 204 | +0.45 | -0.19 | formed · `union_e_fresh_h1` + `short_news_r_h3` |
| `combo_form_efrh1snerh3_3070_shared` | mix | YES | YES | 17/19 | +22.58 | 60% | 204 | -0.07 | -0.81 | formed · `union_e_fresh_h1` + `short_news_r_h3` |
| `combo_scextvetooh_5050_shared` | mix | YES | no | 17/19 | +21.09 | 55% | 161 | +2.62 | +1.58 | `short_clk_ext_veto_opp_h3` + `union_hot_n4_h1` |
| `combo_sh_7030_shared` | mix | YES | YES | 17/19 | +19.77 | 67% | 164 | +0.47 | -0.21 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_sj_3070_shared` | mix | YES | YES | 17/19 | +15.12 | 58% | 212 | -0.97 | -1.58 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_form_jovogrh1snerh3_5050_shared` | mix | YES | YES | 17/19 | +14.90 | 58% | 212 | -0.97 | -1.64 | formed · `union_join_vol_green_h1` + `short_news_r_h3` |
| `combo_sj_5050_shared` | mix | YES | YES | 17/19 | +14.90 | 58% | 212 | -0.97 | -1.64 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_form_jovogrh1sclexve_5050_shared` | mix | YES | no | 17/19 | +14.77 | 48% | 203 | +0.72 | -0.32 | formed · `union_join_vol_green_h1` + `short_clk_ext_veto_opp_h3` |
| `combo_sj_7030_shared` | mix | YES | YES | 17/19 | +11.37 | 56% | 212 | -0.76 | -1.39 | `short_news_r_h3` + `union_join_vol_green_h1` |
| `combo_form_negh1snerh3_5050_shared` | mix | YES | YES | 17/19 | +9.59 | 62% | 226 | -1.02 | -1.82 | formed · `union_news_g_h1` + `short_news_r_h3` |
| `combo_sn_5050_shared` | mix | YES | YES | 17/19 | +9.59 | 62% | 226 | -1.02 | -1.82 | `short_news_r_h3` + `union_news_g_h1` |
| `combo_sn_3070_shared` | mix | YES | YES | 17/19 | +8.92 | 62% | 228 | -1.29 | -1.79 | `short_news_r_h3` + `union_news_g_h1` |
| `combo_scearnguid_5050_shared` | mix | YES | YES | 17/19 | +8.10 | 72% | 102 | -0.20 | -0.87 | `short_news_r_h3` + `union_clk_earn_guide_react_h1` |
| `combo_scrupcoilh_5050_shared` | mix | YES | YES | 17/19 | +8.02 | 75% | 90 | -0.20 | -0.87 | `short_news_r_h3` + `union_clk_r_up_coil_h1` |
| `short_news_r_h3` | short | YES | YES | 17/19 | +8.02 | 62% | 90 | -0.20 | -0.87 | 9/9 short grid (news🔴 hold 3) |
| `combo_sn_7030_shared` | mix | YES | YES | 17/19 | +7.50 | 62% | 226 | -0.76 | -1.45 | `short_news_r_h3` + `union_news_g_h1` |
| `combo_scholdvsse_5050_shared` | mix | YES | YES | 17/19 | +7.11 | 60% | 194 | -0.30 | -0.95 | `short_news_r_h3` + `union_clk_hold_vs_sector_h1` |
| `combo_jse_333_shared` | mix | no | YES | 16/19 | +31.18 | 59% | 240 | -0.98 | -1.19 | `union_join_vol_green_h1` + `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_se_3070_shared` | mix | no | YES | 16/19 | +28.39 | 62% | 166 | -0.49 | -0.30 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_es_8020_shared` | mix | no | YES | 16/19 | +28.27 | 60% | 164 | -0.32 | -0.07 | `union_e_fresh_h3` + `short_news_r_h3` |
| `combo_secmombreak_333_shared` | mix | no | YES | 11/19 | +28.18 | 56% | 272 | -0.03 | -0.16 | `short_news_r_h3` + `union_e_fresh_h3` + `union_clk_mom_break_peer_h1` |
| `combo_seopp_333_shared` | mix | no | YES | 12/19 | +25.35 | 56% | 232 | -1.46 | -1.61 | `short_news_r_h3` + `union_e_fresh_h3` + `oppset_h1` |
| `combo_nse_333_shared` | mix | no | YES | 16/19 | +24.41 | 58% | 252 | -0.96 | -1.15 | `union_news_g_h1` + `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_seuopp_333_shared` | mix | no | YES | 12/19 | +24.10 | 55% | 234 | -1.48 | -1.61 | `short_news_r_h3` + `union_e_fresh_h3` + `union_oppset_h1` |
| `combo_sh_5050_shared` | mix | no | YES | 12/19 | +22.71 | 67% | 164 | +0.94 | +0.26 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_se_7030_shared` | mix | no | YES | 12/19 | +22.63 | 65% | 148 | -0.53 | -0.84 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_seh_404020_shared` | mix | no | YES | 15/19 | +22.39 | 56% | 262 | -0.06 | -0.23 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_sh_3070_shared` | mix | no | YES | 10/19 | +22.15 | 67% | 164 | +1.50 | +1.04 | `short_news_r_h3` + `union_hot_n4_h1` |
| `combo_seh_333_skip` | mix | no | YES | 14/19 | +20.93 | 59% | 254 | +0.26 | +0.18 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_403525_shared` | mix | no | YES | 14/19 | +20.88 | 57% | 262 | +0.06 | -0.07 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_502525_shared` | mix | no | YES | 16/19 | +20.73 | 58% | 260 | +0.00 | -0.20 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_333_shared` | mix | no | YES | 14/19 | +19.31 | 57% | 262 | +0.22 | +0.18 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_333_weather` | mix | no | YES | 14/19 | +19.31 | 57% | 262 | +0.22 | +0.18 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_se_5050_split` | mix | no | YES | 16/19 | +15.89 | 57% | 175 | +0.03 | -0.26 | `short_news_r_h3` + `union_e_fresh_h3` |
| `combo_seh_333_split` | mix | no | YES | 13/19 | +15.79 | 57% | 254 | +0.62 | +0.40 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_seh_502525_split` | mix | no | YES | 13/19 | +12.84 | 55% | 253 | +0.35 | +0.15 | `short_news_r_h3` + `union_e_fresh_h3` + `union_hot_n4_h1` |
| `combo_sopp_5050_shared` | mix | no | YES | 14/19 | +12.23 | 57% | 212 | -1.04 | -1.69 | `short_news_r_h3` + `oppset_h1` |
| `combo_scmombreak_5050_shared` | mix | no | YES | 16/19 | +11.17 | 60% | 238 | +0.44 | -0.19 | `short_news_r_h3` + `union_clk_mom_break_peer_h1` |
| `combo_suopp_5050_shared` | mix | no | YES | 14/19 | +9.29 | 56% | 216 | -1.01 | -1.69 | `short_news_r_h3` + `union_oppset_h1` |
| `short_alarm_h3` | short | no | YES | 12/19 | +2.46 | 55% | 60 | +0.00 | +0.00 |  |
| `union_hot_n4_h1` | long | no | no | 8/19 | +19.52 | 55% | 78 | +2.22 | +2.23 | 9/9 grid point (union / h1 / n4 / hot_score); contamination check |
| `union_clk_mom_break_peer_h1` | long | no | no | 7/19 | +0.91 | 51% | 148 | +0.43 | +0.41 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_fresh_cat_coil_h1` | long | no | no | 0/19 | -5.56 | 48% | 142 | -0.58 | -0.59 | Clock-B / oppset catalogue (T−1 toolbox) |
| `short_clk_neg_weak_fail_h3` | short | no | no | 0/19 | -6.00 | 47% | 131 | +0.82 | -0.95 | Clock-B / oppset catalogue (T−1 toolbox) |
| `short_clk_ext_veto_h3` | short | no | no | 10/19 | -4.55 | 52% | 124 | -0.63 | -0.91 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_hold_vs_sector_h1` | long | no | no | 4/19 | -0.33 | 48% | 108 | +0.54 | +0.54 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_nr7_mom_h1` | long | no | no | 0/19 | -2.49 | 42% | 40 | -2.19 | -2.19 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_mom_break_peer_opp_h1` | long | no | no | 10/19 | +13.12 | 49% | 90 | +0.27 | +0.23 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_fresh_cat_coil_opp_h1` | long | no | no | 0/19 | -4.74 | 43% | 108 | -2.44 | -2.44 | Clock-B / oppset catalogue (T−1 toolbox) |
| `short_clk_neg_weak_fail_opp_h3` | short | no | no | 1/19 | -10.13 | 53% | 95 | -1.08 | -2.77 | Clock-B / oppset catalogue (T−1 toolbox) |
| `short_clk_ext_veto_opp_h3` | short | no | no | 16/19 | +4.27 | 53% | 91 | +1.48 | +0.45 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_hold_vs_sector_opp_h1` | long | no | no | 11/19 | +4.58 | 46% | 96 | +0.94 | +0.94 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_clk_nr7_mom_opp_h1` | long | no | no | 12/19 | +10.76 | 38% | 14 | -2.19 | -2.19 | Clock-B / oppset catalogue (T−1 toolbox) |
| `union_oppset_h1` | long | no | no | 1/19 | -3.21 | 52% | 136 | -0.23 | -0.23 | Clock-B / oppset catalogue (T−1 toolbox) |
| `oppset_h1` | long | no | no | 5/19 | -2.89 | 51% | 132 | -0.22 | -0.23 | Clock-B / oppset catalogue (T−1 toolbox) |

Formed extras (IS singles mixed with combo-engine primitives; not live FOCUS pins):

- `combo_form_efrh1jovogrh1snerh3_333_shared` = `union_e_fresh_h1` + `union_join_vol_green_h1` + `short_news_r_h3`
- `combo_form_efrh1snerh3_7030_shared` = `union_e_fresh_h1` + `short_news_r_h3`
- `combo_form_efrh1snerh3_3070_shared` = `union_e_fresh_h1` + `short_news_r_h3`
- `combo_form_jovogrh1snerh3_5050_shared` = `union_join_vol_green_h1` + `short_news_r_h3`
- `combo_form_jovogrh1sclexve_5050_shared` = `union_join_vol_green_h1` + `short_clk_ext_veto_opp_h3`
- `combo_form_negh1snerh3_5050_shared` = `union_news_g_h1` + `short_news_r_h3`

## Frozen recipe names (Excel fee-aware OOS handoff)

IS freeze only — do not re-select on 9/10–9/18. Same list as `03_scoreboard/factor_mine_blind_0909_oppset/FROZEN_RECIPES.txt`.

```
combo_ecearnguid_5050_shared
combo_se_5050_skip
combo_ecnr7momop_5050_shared
combo_se_5050_shared
combo_se_5050_weather
combo_scnr7momop_5050_shared
combo_form_efrh1jovogrh1snerh3_333_shared
combo_se1_5050_shared
combo_e1s_7030_shared
combo_form_efrh1snerh3_7030_shared
combo_form_efrh1snerh3_3070_shared
combo_scextvetooh_5050_shared
combo_sh_7030_shared
combo_sj_3070_shared
combo_form_jovogrh1snerh3_5050_shared
combo_sj_5050_shared
combo_form_jovogrh1sclexve_5050_shared
combo_sj_7030_shared
combo_form_negh1snerh3_5050_shared
combo_sn_5050_shared
combo_sn_3070_shared
combo_scearnguid_5050_shared
combo_scrupcoilh_5050_shared
short_news_r_h3
combo_sn_7030_shared
combo_scholdvsse_5050_shared
combo_jse_333_shared
combo_se_3070_shared
combo_es_8020_shared
combo_secmombreak_333_shared
combo_seopp_333_shared
combo_nse_333_shared
combo_seuopp_333_shared
combo_sh_5050_shared
combo_se_7030_shared
combo_seh_404020_shared
combo_sh_3070_shared
combo_seh_333_skip
combo_seh_403525_shared
combo_seh_502525_shared
combo_seh_333_shared
combo_seh_333_weather
combo_se_5050_split
combo_seh_333_split
combo_seh_502525_split
combo_sopp_5050_shared
combo_scmombreak_5050_shared
combo_suopp_5050_shared
short_alarm_h3
union_hot_n4_h1
union_clk_mom_break_peer_h1
union_clk_fresh_cat_coil_h1
short_clk_neg_weak_fail_h3
short_clk_ext_veto_h3
union_clk_hold_vs_sector_h1
union_clk_nr7_mom_h1
union_clk_mom_break_peer_opp_h1
union_clk_fresh_cat_coil_opp_h1
short_clk_neg_weak_fail_opp_h3
short_clk_ext_veto_opp_h3
union_clk_hold_vs_sector_opp_h1
union_clk_nr7_mom_opp_h1
union_oppset_h1
oppset_h1
```

## Verdict

### Contrast vs #286 (no-oppset) and the live contaminated board

- **#286** used this same 9/9 method **without** Clock-B / oppset seeds. It featured 22 Cyrus sleeves (mostly `combo_sh_*` / `combo_seh_*` / sj / sn / news-vol+short). hot4 failed featuring (11/19); holdup was not found. OOS Book% survivors were the six sh / seh mixes.
- **This remine** adds the T−1 Clock-B / oppset aisle to that toolbox and lets the ≤9/9 grid rediscover winners. Live FOCUS / ALWAYS still never enter the keep set.
- **Live Pages board** (8/13→9/18) ranked while seeing 9/10–9/18 and pins `union_hot_n4_holdup`, overnight_mega, Clock-B catalogue as live contamination. That pack is not this freeze.

### Would a 9/9 researcher with Clock-B / oppset have found them?

- **hot4 / `union_hot_n4_h1`:** No — 9/9 grid point, failed featuring (Starts 8/19, win 55%, book +19.52%). Twins: `union_hot_n4_h1`. Hot4 OOS continued +2.22% · fresh $10k +2.23%.
- **holdup / `union_hot_n4_holdup`:** No — `union_hot_n4_holdup` / `s_boost=holdup` stayed stripped (landed 2026-09-19). No holdup twin was invented.
- **Clock-B / oppset twins:** Yes — a 9/9 researcher with this toolbox would have featured `combo_ecearnguid_5050_shared`, `combo_ecnr7momop_5050_shared`, `combo_scnr7momop_5050_shared`, `combo_scextvetooh_5050_shared`, `combo_form_jovogrh1sclexve_5050_shared`, `combo_scearnguid_5050_shared`, `combo_scrupcoilh_5050_shared`, `combo_scholdvsse_5050_shared`.

Cyrus featured that still print a positive continued OOS book: `combo_ecearnguid_5050_shared`, `combo_se1_5050_shared`, `combo_e1s_7030_shared`, `combo_form_efrh1snerh3_7030_shared`, `combo_scextvetooh_5050_shared`, `combo_sh_7030_shared`, `combo_form_jovogrh1sclexve_5050_shared`.

Of those, Clock-B / oppset-touched: `combo_ecearnguid_5050_shared`, `combo_scextvetooh_5050_shared`, `combo_form_jovogrh1sclexve_5050_shared`.

Cyrus featured that fade after the cut: `combo_se_5050_skip`, `combo_ecnr7momop_5050_shared`, `combo_se_5050_shared`, `combo_se_5050_weather`, `combo_scnr7momop_5050_shared`, `combo_form_efrh1jovogrh1snerh3_333_shared`, `combo_form_efrh1snerh3_3070_shared`, `combo_sj_3070_shared`, `combo_form_jovogrh1snerh3_5050_shared`, `combo_sj_5050_shared`, `combo_sj_7030_shared`, `combo_form_negh1snerh3_5050_shared`, `combo_sn_5050_shared`, `combo_sn_3070_shared`, `combo_scearnguid_5050_shared`, `combo_scrupcoilh_5050_shared`, `short_news_r_h3`, `combo_sn_7030_shared`, `combo_scholdvsse_5050_shared`.

Formal-bar KEEP count: **46**. Cyrus featured count: **26**.

OOS **continued** walks the 9/9 cash book forward. OOS **fresh $10k** wakes the frozen recipe on 2026-09-10 with empty lots.

