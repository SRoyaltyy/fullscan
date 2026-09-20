# Factor mine blind 9/9 + Clock-B/oppset — fee-aware OOS (Excel lane)

status=DONE verdict=**FAIL** 0 KEEP cutoff=2026-09-09 OOS=2026-09-10→2026-09-18 FEE_RT=0.0015 KEEP=0 FAIL=64 freeze=cb7f09ae+e3aabf24

Research only. Live `flatten_robust` / `dashboard/factor-mine/` / `03_scoreboard/factor_mine.json` were not written. Recipe names are the #288 IS freeze (`FROZEN_RECIPES.txt`) — OOS dates did not re-select.

## KEEP bar

Cyrus OOS KEEP (plain): **IS Cyrus featured** (Starts YES + Book% on 8/13–9/9) **and** continued OOS Book% > 0 **and** OOS start-day YES ≥85% (7 sessions → ≥6/7). Same start rule as IS (≥17/19 when n≥19).

**Win% > 55% is not enough by itself.** After-fee H = same-session open→close minus 15 bp Futubull (`FEE_RT=0.0015`). Shorts pay the 15 bp (they do not collect it). Cash Book% already uses the Futubull order-fee schedule (`00_grounding/futubull_fees.json`). n≥30 is the IS trade-count bar; the 7-session window cannot meet it and does not KEEP.

OOS **continued** Book% is copied from #288 `holdout.json` (9/9 cash book walked forward). OOS **start-day YES** wakes $10k empty lots on each session in 2026-09-10–2026-09-18. Start 9/10 is the fresh $10k path.

## Headline

FAIL. No frozen ≤9/9+oppset Cyrus name keeps positive continued Book% **and** enough OOS start-day wins (2026-09-10–2026-09-18).

Taskforce continued-Book% survivors (not a KEEP bar): **7** of 26 Cyrus IS names. Win%>55% traps (WR clears, Cyrus FAIL): **11**. Frozen names verified: **64**. Start-day walks: **41/64**.

## Taskforce 7 — continued Book% > 0 on #288 Cyrus

| Strategy | Cyrus OOS | Book-only | OOS starts | Cont book% | Fresh $10k | Fresh WR | After-fee H WR | n H | Why |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_ecearnguid_5050_shared` | **FAIL** | yes | 2/7 | +0.00 | +0.61 | — | 62% | 8 | OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7); Win%>55% alone is not enough |
| `combo_se1_5050_shared` | **FAIL** | yes | 0/7 | +0.14 | -0.56 | 100% | 47% | 15 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7); Win%>55% alone is not enough |
| `combo_e1s_7030_shared` | **FAIL** | yes | 0/7 | +0.45 | -0.19 | 100% | 47% | 15 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7); Win%>55% alone is not enough |
| `combo_form_efrh1snerh3_7030_shared` | **FAIL** | yes | 0/7 | +0.45 | -0.19 | 100% | 47% | 15 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7); Win%>55% alone is not enough |
| `combo_scextvetooh_5050_shared` | **FAIL** | yes | 2/7 | +2.62 | +1.58 | — | 50% | 6 | OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_sh_7030_shared` | **FAIL** | yes | 0/7 | +0.47 | -0.21 | — | 36% | 11 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_form_jovogrh1sclexve_5050_shared` | **FAIL** | yes | 0/7 | +0.72 | -0.32 | — | 40% | 10 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7) |

### Start-day paths (Taskforce 7)

Each cell is the fee-aware $10k book that **starts** that morning (empty lots) through 9/18. YES = Book% > 0.

| Strategy | 09-10 | 09-11 | 09-14 | 09-15 | 09-16 | 09-17 | 09-18 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `combo_ecearnguid_5050_shared` | YES +0.61 | YES +0.61 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_se1_5050_shared` | no -0.56 | no -0.56 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_e1s_7030_shared` | no -0.19 | no -0.19 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_form_efrh1snerh3_7030_shared` | no -0.19 | no -0.19 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_scextvetooh_5050_shared` | YES +1.58 | YES +1.58 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_sh_7030_shared` | no -0.21 | no -0.21 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_form_jovogrh1sclexve_5050_shared` | no -0.32 | no -0.32 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |

## Contrast vs #286 (no-oppset)

- #286 Cyrus featured: **22**. This remine: **26**.
- New vs #286: `combo_e1s_7030_shared`, `combo_ecearnguid_5050_shared`, `combo_ecnr7momop_5050_shared`, `combo_form_efrh1jovogrh1snerh3_333_shared`, `combo_form_efrh1snerh3_3070_shared`, `combo_form_efrh1snerh3_7030_shared`, `combo_form_jovogrh1sclexve_5050_shared`, `combo_scearnguid_5050_shared`, `combo_scextvetooh_5050_shared`, `combo_scholdvsse_5050_shared`, `combo_scnr7momop_5050_shared`, `combo_scrupcoilh_5050_shared`, `combo_se1_5050_shared`, `combo_se_5050_shared`, `combo_se_5050_skip`, `combo_se_5050_weather`.
- Dropped vs #286: `combo_form_jovogrh1snerh1_5050_shared`, `combo_form_negh1jovogrh1snerh3_333_shared`, `combo_form_negh1snerh1_5050_shared`, `combo_form_negh1snerh3_3070_shared`, `combo_form_negh1snerh3_7030_shared`, `combo_form_nevoh1snerh3_5050_shared`, `combo_seh_451540_shared`, `combo_seh_502525_shared`, `combo_seh_601525_shared`, `combo_sh_3070_shared`, `combo_sh_5050_shared`, `combo_snj_333_shared`.
- Shared: `combo_form_jovogrh1snerh3_5050_shared`, `combo_form_negh1snerh3_5050_shared`, `combo_sh_7030_shared`, `combo_sj_3070_shared`, `combo_sj_5050_shared`, `combo_sj_7030_shared`, `combo_sn_3070_shared`, `combo_sn_5050_shared`, `combo_sn_7030_shared`, `short_news_r_h3`.
- Clock-B / oppset names in Cyrus: `combo_ecearnguid_5050_shared`, `combo_ecnr7momop_5050_shared`, `combo_form_jovogrh1sclexve_5050_shared`, `combo_scearnguid_5050_shared`, `combo_scextvetooh_5050_shared`, `combo_scholdvsse_5050_shared`, `combo_scnr7momop_5050_shared`, `combo_scrupcoilh_5050_shared`.

## Clock-B / oppset catalogue — IS + OOS

Pure Clock-B / oppset **singles** not featuring on 9/9 is **expected**. The aisle is a mixer (T−1 top-30 ∪ panel), not a solo KEEP. None of these clear Cyrus Starts YES ≥17/19.

| Strategy | Cyrus IS | OOS starts | Cont book% | Fresh $10k | After-fee H WR | Verdict |
|---|---|---:|---:|---:|---:|---|
| `union_clk_mom_break_peer_h1` | no | 2/7 | +0.43 | +0.41 | 62% | **FAIL** |
| `union_clk_fresh_cat_coil_h1` | no | 2/7 | -0.58 | -0.59 | 62% | **FAIL** |
| `short_clk_neg_weak_fail_h3` | no | 2/7 | +0.82 | -0.95 | 62% | **FAIL** |
| `short_clk_ext_veto_h3` | no | 0/7 | -0.63 | -0.91 | 17% | **FAIL** |
| `union_clk_hold_vs_sector_h1` | no | 2/7 | +0.54 | +0.54 | 50% | **FAIL** |
| `union_clk_nr7_mom_h1` | no | 0/7 | -2.19 | -2.19 | 0% | **FAIL** |
| `union_clk_mom_break_peer_opp_h1` | no | 2/7 | +0.27 | +0.23 | 50% | **FAIL** |
| `union_clk_fresh_cat_coil_opp_h1` | no | 0/7 | -2.44 | -2.44 | 25% | **FAIL** |
| `short_clk_neg_weak_fail_opp_h3` | no | 0/7 | -1.08 | -2.77 | 62% | **FAIL** |
| `short_clk_ext_veto_opp_h3` | no | 2/7 | +1.48 | +0.45 | 50% | **FAIL** |
| `union_clk_hold_vs_sector_opp_h1` | no | 2/7 | +0.94 | +0.94 | 50% | **FAIL** |
| `union_clk_nr7_mom_opp_h1` | no | 0/7 | -2.19 | -2.19 | 0% | **FAIL** |
| `union_oppset_h1` | no | 0/7 | -0.23 | -0.23 | 50% | **FAIL** |
| `oppset_h1` | no | 0/7 | -0.22 | -0.23 | 50% | **FAIL** |

Clock-B / oppset-touched mixes (formed or 50/50 overlay):

| Strategy | Cyrus IS | Members | OOS starts | Cont book% | Fresh $10k | Verdict |
|---|---|---|---:|---:|---:|---|
| `combo_ecearnguid_5050_shared` | YES | `union_e_fresh_h3` + `union_clk_earn_guide_react_h1` | 2/7 | +0.00 | +0.61 | **FAIL** |
| `combo_ecnr7momop_5050_shared` | YES | `union_e_fresh_h3` + `union_clk_nr7_mom_opp_h1` | 0/7 | -0.98 | -0.94 | **FAIL** |
| `combo_scnr7momop_5050_shared` | YES | `short_news_r_h3` + `union_clk_nr7_mom_opp_h1` | 0/7 | -3.46 | -4.13 | **FAIL** |
| `combo_scextvetooh_5050_shared` | YES | `short_clk_ext_veto_opp_h3` + `union_hot_n4_h1` | 2/7 | +2.62 | +1.58 | **FAIL** |
| `combo_form_jovogrh1sclexve_5050_shared` | YES | `union_join_vol_green_h1` + `short_clk_ext_veto_opp_h3` | 0/7 | +0.72 | -0.32 | **FAIL** |
| `combo_scearnguid_5050_shared` | YES | `short_news_r_h3` + `union_clk_earn_guide_react_h1` | 0/7 | -0.20 | -0.87 | **FAIL** |
| `combo_scrupcoilh_5050_shared` | YES | `short_news_r_h3` + `union_clk_r_up_coil_h1` | 0/7 | -0.20 | -0.87 | **FAIL** |
| `combo_scholdvsse_5050_shared` | YES | `short_news_r_h3` + `union_clk_hold_vs_sector_h1` | 0/7 | -0.30 | -0.95 | **FAIL** |
| `combo_secmombreak_333_shared` | no | `short_news_r_h3` + `union_e_fresh_h3` + `union_clk_mom_break_peer_h1` | — | -0.03 | -0.16 | **FAIL** |
| `combo_seopp_333_shared` | no | `short_news_r_h3` + `union_e_fresh_h3` + `oppset_h1` | — | -1.46 | -1.61 | **FAIL** |
| `combo_seuopp_333_shared` | no | `short_news_r_h3` + `union_e_fresh_h3` + `union_oppset_h1` | — | -1.48 | -1.61 | **FAIL** |
| `combo_sopp_5050_shared` | no | `short_news_r_h3` + `oppset_h1` | — | -1.04 | -1.69 | **FAIL** |
| `combo_scmombreak_5050_shared` | no | `short_news_r_h3` + `union_clk_mom_break_peer_h1` | — | +0.44 | -0.19 | **FAIL** |
| `combo_suopp_5050_shared` | no | `short_news_r_h3` + `union_oppset_h1` | — | -1.01 | -1.69 | **FAIL** |

## Frozen Cyrus featured — KEEP / FAIL

| Strategy | Side | IS start | IS book% | IS win% | OOS starts | Cont book% | Fresh $10k | After-fee H WR | WR-only | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `combo_ecearnguid_5050_shared` | mix | 17/19 | +30.34 | 55% | 2/7 | +0.00 | +0.61 | 62% | yes | **FAIL** |
| `combo_se_5050_skip` | mix | 17/19 | +29.36 | 68% | 0/7 | -0.48 | -0.64 | 47% |  | **FAIL** |
| `combo_ecnr7momop_5050_shared` | mix | 17/19 | +28.68 | 60% | 0/7 | -0.98 | -0.94 | 50% |  | **FAIL** |
| `combo_se_5050_shared` | mix | 17/19 | +26.83 | 65% | 0/7 | -0.49 | -0.64 | 47% |  | **FAIL** |
| `combo_se_5050_weather` | mix | 17/19 | +26.83 | 65% | 0/7 | -0.49 | -0.64 | 47% |  | **FAIL** |
| `combo_scnr7momop_5050_shared` | mix | 17/19 | +25.48 | 75% | 0/7 | -3.46 | -4.13 | 22% |  | **FAIL** |
| `combo_form_efrh1jovogrh1snerh3_333_shared` | mix | 17/19 | +24.92 | 55% | 0/7 | -0.37 | -1.14 | 43% | yes | **FAIL** |
| `combo_se1_5050_shared` | mix | 17/19 | +23.62 | 62% | 0/7 | +0.14 | -0.56 | 47% | yes | **FAIL** |
| `combo_e1s_7030_shared` | mix | 17/19 | +22.59 | 62% | 0/7 | +0.45 | -0.19 | 47% | yes | **FAIL** |
| `combo_form_efrh1snerh3_7030_shared` | mix | 17/19 | +22.59 | 62% | 0/7 | +0.45 | -0.19 | 47% | yes | **FAIL** |
| `combo_form_efrh1snerh3_3070_shared` | mix | 17/19 | +22.58 | 60% | 0/7 | -0.07 | -0.81 | 47% | yes | **FAIL** |
| `combo_scextvetooh_5050_shared` | mix | 17/19 | +21.09 | 55% | 2/7 | +2.62 | +1.58 | 50% |  | **FAIL** |
| `combo_sh_7030_shared` | mix | 17/19 | +19.77 | 67% | 0/7 | +0.47 | -0.21 | 36% |  | **FAIL** |
| `combo_sj_3070_shared` | mix | 17/19 | +15.12 | 58% | 0/7 | -0.97 | -1.58 | 33% |  | **FAIL** |
| `combo_form_jovogrh1snerh3_5050_shared` | mix | 17/19 | +14.90 | 58% | 0/7 | -0.97 | -1.64 | 33% |  | **FAIL** |
| `combo_sj_5050_shared` | mix | 17/19 | +14.90 | 58% | 0/7 | -0.97 | -1.64 | 33% |  | **FAIL** |
| `combo_form_jovogrh1sclexve_5050_shared` | mix | 17/19 | +14.77 | 48% | 0/7 | +0.72 | -0.32 | 40% |  | **FAIL** |
| `combo_sj_7030_shared` | mix | 17/19 | +11.37 | 56% | 0/7 | -0.76 | -1.39 | 33% |  | **FAIL** |
| `combo_form_negh1snerh3_5050_shared` | mix | 17/19 | +9.59 | 62% | 0/7 | -1.02 | -1.82 | 29% |  | **FAIL** |
| `combo_sn_5050_shared` | mix | 17/19 | +9.59 | 62% | 0/7 | -1.02 | -1.82 | 29% |  | **FAIL** |
| `combo_sn_3070_shared` | mix | 17/19 | +8.92 | 62% | 0/7 | -1.29 | -1.79 | 29% |  | **FAIL** |
| `combo_scearnguid_5050_shared` | mix | 17/19 | +8.10 | 72% | 0/7 | -0.20 | -0.87 | 29% |  | **FAIL** |
| `combo_scrupcoilh_5050_shared` | mix | 17/19 | +8.02 | 75% | 0/7 | -0.20 | -0.87 | 29% |  | **FAIL** |
| `short_news_r_h3` | short | 17/19 | +8.02 | 62% | 0/7 | -0.20 | -0.87 | 29% |  | **FAIL** |
| `combo_sn_7030_shared` | mix | 17/19 | +7.50 | 62% | 0/7 | -0.76 | -1.45 | 29% |  | **FAIL** |
| `combo_scholdvsse_5050_shared` | mix | 17/19 | +7.11 | 60% | 0/7 | -0.30 | -0.95 | 36% |  | **FAIL** |

## Formal-bar IS KEEP that missed Cyrus featuring

These cleared WORKABLE_BAR on 9/9 (Win% / $days / start≥50%) but not Starts YES ≥17/19. OOS cannot promote them. `combo_sh_5050_shared` / `combo_sh_3070_shared` were Cyrus on the no-oppset #286 freeze; the Clock-B aisle moved their IS starts below 17/19 here.

| Strategy | IS start | IS book% | IS win% | OOS starts | Cont book% | Fresh $10k | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `combo_jse_333_shared` | 16/19 | +31.18 | 59% | — | -0.98 | -1.19 | **FAIL** |
| `combo_se_3070_shared` | 16/19 | +28.39 | 62% | — | -0.49 | -0.30 | **FAIL** |
| `combo_es_8020_shared` | 16/19 | +28.27 | 60% | — | -0.32 | -0.07 | **FAIL** |
| `combo_secmombreak_333_shared` | 11/19 | +28.18 | 56% | — | -0.03 | -0.16 | **FAIL** |
| `combo_seopp_333_shared` | 12/19 | +25.35 | 56% | — | -1.46 | -1.61 | **FAIL** |
| `combo_nse_333_shared` | 16/19 | +24.41 | 58% | — | -0.96 | -1.15 | **FAIL** |
| `combo_seuopp_333_shared` | 12/19 | +24.10 | 55% | — | -1.48 | -1.61 | **FAIL** |
| `combo_sh_5050_shared` | 12/19 | +22.71 | 67% | — | +0.94 | +0.26 | **FAIL** |
| `combo_se_7030_shared` | 12/19 | +22.63 | 65% | — | -0.53 | -0.84 | **FAIL** |
| `combo_seh_404020_shared` | 15/19 | +22.39 | 56% | — | -0.06 | -0.23 | **FAIL** |
| `combo_sh_3070_shared` | 10/19 | +22.15 | 67% | — | +1.50 | +1.04 | **FAIL** |
| `combo_seh_333_skip` | 14/19 | +20.93 | 59% | — | +0.26 | +0.18 | **FAIL** |
| `combo_seh_403525_shared` | 14/19 | +20.88 | 57% | — | +0.06 | -0.07 | **FAIL** |
| `combo_seh_502525_shared` | 16/19 | +20.73 | 58% | — | +0.00 | -0.20 | **FAIL** |
| `combo_seh_333_shared` | 14/19 | +19.31 | 57% | — | +0.22 | +0.18 | **FAIL** |
| `combo_seh_333_weather` | 14/19 | +19.31 | 57% | — | +0.22 | +0.18 | **FAIL** |
| `combo_se_5050_split` | 16/19 | +15.89 | 57% | — | +0.03 | -0.26 | **FAIL** |
| `combo_seh_333_split` | 13/19 | +15.79 | 57% | — | +0.62 | +0.40 | **FAIL** |
| `combo_seh_502525_split` | 13/19 | +12.84 | 55% | — | +0.35 | +0.15 | **FAIL** |
| `combo_sopp_5050_shared` | 14/19 | +12.23 | 57% | — | -1.04 | -1.69 | **FAIL** |
| `combo_scmombreak_5050_shared` | 16/19 | +11.17 | 60% | — | +0.44 | -0.19 | **FAIL** |
| `combo_suopp_5050_shared` | 14/19 | +9.29 | 56% | — | -1.01 | -1.69 | **FAIL** |
| `short_alarm_h3` | 12/19 | +2.46 | 55% | — | +0.00 | +0.00 | **FAIL** |

## Other frozen names (contamination / thin singles)

In the freeze for the hot4 check or thin formal miss. Cannot KEEP from OOS.

| Strategy | IS start | IS book% | OOS starts | Cont book% | Fresh $10k | Verdict |
|---|---:|---:|---:|---:|---:|---|
| `union_hot_n4_h1` | 8/19 | +19.52 | 2/7 | +2.22 | +2.23 | **FAIL** |

## Win% > 55% alone

Win% is not the Cyrus bar. On 9/9, `combo_ecearnguid_5050_shared` IS WR 55%, `combo_se_5050_skip` IS WR 68%, `combo_ecnr7momop_5050_shared` IS WR 60%, `combo_se_5050_shared` IS WR 65%, `combo_se_5050_weather` IS WR 65%, `combo_scnr7momop_5050_shared` IS WR 75%, `combo_se1_5050_shared` IS WR 62%, `combo_e1s_7030_shared` IS WR 62%, `combo_form_efrh1snerh3_7030_shared` IS WR 62%, `combo_form_efrh1snerh3_3070_shared` IS WR 60%, `combo_sh_7030_shared` IS WR 67%, `combo_sj_3070_shared` IS WR 58%, `combo_form_jovogrh1snerh3_5050_shared` IS WR 58%, `combo_sj_5050_shared` IS WR 58%, `combo_sj_7030_shared` IS WR 56%, `combo_form_negh1snerh3_5050_shared` IS WR 62%, `combo_sn_5050_shared` IS WR 62%, `combo_sn_3070_shared` IS WR 62%, `combo_scearnguid_5050_shared` IS WR 72%, `combo_scrupcoilh_5050_shared` IS WR 75%, `short_news_r_h3` IS WR 62%, `combo_sn_7030_shared` IS WR 62%, `combo_scholdvsse_5050_shared` IS WR 60% would pass a Win%>55% screen; every one **FAIL**s OOS starts + Book%. Several Clock-B mixes were featured with IS WR at or under 55% — starts 17/19 + Book% kept them, not Win%.

OOS after-fee H / fresh cash WR > 55% and still **FAIL** Cyrus: `combo_ecearnguid_5050_shared` (H 62%, cash WR —), `combo_form_efrh1jovogrh1snerh3_333_shared` (H 43%, cash WR 100%), `combo_se1_5050_shared` (H 47%, cash WR 100%), `combo_e1s_7030_shared` (H 47%, cash WR 100%), `combo_form_efrh1snerh3_7030_shared` (H 47%, cash WR 100%), `combo_form_efrh1snerh3_3070_shared` (H 47%, cash WR 100%), `union_clk_mom_break_peer_h1` (H 62%, cash WR —), `union_clk_fresh_cat_coil_h1` (H 62%, cash WR —), `short_clk_neg_weak_fail_h3` (H 62%, cash WR —), `short_clk_neg_weak_fail_opp_h3` (H 62%, cash WR —), `union_clk_hold_vs_sector_opp_h1` (H 50%, cash WR 100%).

## Contamination vs today's full-sample board

Live board window **2026-08-13 → 2026-09-18** (63 featured pins). That pack saw 9/10–9/18 while ranking / pinning. This OOS board does not.

- **hot4 / `union_hot_n4_h1`:** In the 9/9 auto grid, **not** Cyrus featured (IS starts 8/19, book +19.52%). Live featured pin: no. Full-sample book +31.97% starts 26/26. A 9/9 researcher would not have featured it; OOS shine cannot promote it here.
- **holdup / `union_hot_n4_holdup`:** Still not in the 9/9 seed set (landed 2026-09-19). Live featured: yes. No holdup twin was invented. Not scored.
- **Post-9/9 live pins:** `union_hot_n4_holdup`, `overnight_mega_h1`, `overnight_mega_h2`, `overnight_h1`, `overnight_mega_green_h1`, `combo_oh_5050_shared`, `combo_sh_macd_5050_shared`.
- **Clock-B / oppset on the live featured strip:** none.
- **Taskforce 7 on the live featured pin list:** `combo_se1_5050_shared`, `combo_e1s_7030_shared`.

## Confirm vs #288 holdout books

MISMATCH vs holdout.json: `union_clk_fresh_cat_coil_h1` cont=True fresh=False, `short_clk_neg_weak_fail_h3` cont=True fresh=False, `union_clk_fresh_cat_coil_opp_h1` cont=True fresh=False, `short_clk_neg_weak_fail_opp_h3` cont=True fresh=False.

Recipe-definition freeze: `cb7f09ae+e3aabf24`. Side paths only.

