# Webull simulated books

Each book is `<strategy>_webull_sim`. These rows do not change any live record.

Fees from [https://www.webull.com/pricing](https://www.webull.com/pricing), retrieved 2026-10-06. Commission $0. SEC fee 0.0000206 per sale dollar. CAT fee 0.000003 per share on buys and sells. FINRA TAF $0 per share, the current rate printed on that page. Stock borrow is not published there, so borrow is not modeled. Algo-order and OTC charges are on the page and are not used.

The first fingerprinted day is 2026-10-06. Earlier days are BUILT AFTER THE FACT and are not part of the locked result. A missed locked day stays missing. Same-day reruns rewrite a row only while it is still unsealed.

Whole shares at the session open printed in the local Yahoo price store. The store keeps raw prints. A name with no later split matches a split-adjusted open. DCX and DHY have reverse splits on file; the fill is the morning print, not a back-adjusted price.

The stop fills before the target when the same daily bar touches both. Cash, positions, and fees carry inside a section. The locked section starts again at $10,000.

Excel sleeves buy the next 09:30 open. Their research cards buy the signal-day close. The two results are not comparable. Theme Radar shorts stay off the real Webull paper account.

Locked trade rows: 0. Built after the fact: 6846. Visible, not a locked trade: 372.

## Locked and not-yet-locked

| Book | Date | Section | Reason | Picks | Equity | Fees | Commit ET | Source | SHA | Sandbox |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| 1d_top_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| 1m_top_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| 1w_top_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| 2w_top_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| 3d_top_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| L1_long_green_tp8_lowvol_webull_sim | 2026-10-06 | not_a_locked_trade | open not observed | 12 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| L2_long_green_tp3_lowvol_webull_sim | 2026-10-06 | not_a_locked_trade | open not observed | 12 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| L3_long_green_hold2_midcap_webull_sim | 2026-10-06 | not_a_locked_trade | open not observed | 34 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| L4_long_green_hold8_bbailike_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| L5_long_green_hold2_midhibeta_webull_sim | 2026-10-06 | not_a_locked_trade | open not observed | 8 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| S1_short_red_1day_optionable_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| S2_short_red_1day_hivol_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-05T18:29:10-04:00 | excel_bot/daily/2026-10-05_excel_bot.md | caf155612ba7 | — |
| breadth_rank_v1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| breadth_rank_v1b_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| breadth_rank_v1c_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| coil_h3_exit_alarm_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_e1er_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_e1s_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ee1_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ee1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ee1_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_eer_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ef_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ef_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ef_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_eh_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_eh_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_eh_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ehs_601525_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ehs_702010_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ej_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_en_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_en_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_en_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ers_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_es_8020_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_es_9010_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fe1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fe_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fer_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fes_403030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fh_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_fse_333_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_he1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_her_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_hf_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_hj_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_hn_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_hn_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_hn_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_je1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_jer_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_jf_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_jse_333_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ne1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ner_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_nf_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_nj_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_nse_333_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_oh_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_p2s_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ps_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ps_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se1_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_5050_skip_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_5050_split_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_5050_weather_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_se_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_333_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_333_skip_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_333_split_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_333_weather_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_403525_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_404020_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_451540_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_502525_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_502525_split_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_seh_601525_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_ser_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sf_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sf_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sf_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sh_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sh_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sh_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sh_macd_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sj_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sj_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sj_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sn_3070_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sn_5050_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_sn_7030_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| combo_snj_333_shared_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| excel_all_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_s8_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_live_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_robust_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_vol_g_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| flatten_white_yday_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| forward_shadow_fwd_union_hot_n4_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| forward_shadow_fwd_union_hot_score_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| forward_shadow_union_hot_n4_h1__w0_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| forward_shadow_union_hot_n4_h1_nonews__w0_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| forward_shadow_union_hot_n4_holdup__w0_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | not observed |
| lever_search_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| ohlc_hot_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| ohlc_hot_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| ohlc_hot_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| ohlc_hot_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| oos0914_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| oppset_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_mega_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_mega_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| overnight_mega_h2_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| probable_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| probable_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| probable_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| probable_probable_ok_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| probable_probable_ok_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_alarm_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_alarm_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_clk_ext_veto_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_clk_ext_veto_opp_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_clk_neg_weak_fail_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_clk_neg_weak_fail_opp_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_extended_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_extended_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_last_red_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_last_red_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_macd_dn_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_macd_dn_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_head_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_or_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_pack_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_r_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_r_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_news_r_macd_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_r_down_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_r_down_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_rsi_ob_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| short_rsi_ob_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| stock_book_1d_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| stock_book_1m_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| stock_book_1w_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| stock_book_2w_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| stock_book_3d_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| theme_radar_fpe_delta_t3_earn_today_3d_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-06T01:20:22-04:00 | SRoyaltyy/theme-radar:research/shadow_log/log.csv | d410683fc1f7 | — |
| theme_radar_fresh_dcp_t1_avoid_ah_3d_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-06T01:20:22-04:00 | SRoyaltyy/theme-radar:research/shadow_log/log.csv | d410683fc1f7 | — |
| theme_radar_fresh_dcp_t1_ep_ge03_2d_webull_sim | 2026-10-06 | not_a_locked_trade | sat out, 0 picks | 0 | $10,000.00 | $0.00 | 2026-10-06T01:20:22-04:00 | SRoyaltyy/theme-radar:research/shadow_log/log.csv | d410683fc1f7 | — |
| union_ab_g_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_ab_g_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_coil_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_vol_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_blue_vol_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_break10_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_break10_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_candle_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_candle_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_candle_score_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_candle_score_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_catal_present_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_catal_present_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_earn_guide_react_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_flow_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_fresh_cat_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_fresh_cat_coil_opp_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_hold_vs_sector_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_hold_vs_sector_opp_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_insider_cash_stab_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_mom_break_peer_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_mom_break_peer_opp_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_nr7_mom_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_nr7_mom_opp_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_clk_r_up_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_coil_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_coil_green_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_coil_off_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_coil_off_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_coil_off_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_cond_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_cond_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_cond_n4_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_e_fresh_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_e_fresh_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_e_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_e_green_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_earn_react_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_earn_react_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_flow_in_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_flow_in_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_flow_in_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_flow_in_white_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_flow_in_white_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_exit_alarm_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_exit_news_r_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_exit_red_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_cut_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_exit_alarm_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_half_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_rankw_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_sboost_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_sizeup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_time_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_topheavy_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_trail_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_hot_n12_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_hot_n4_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_hot_n4_holdup_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_hot_score_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_hot_score_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_g_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_g_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_present_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_present_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_vol_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_join_vol_green_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_last_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_last_green_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_last_green_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_last_red_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_last_red_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_hist_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_hist_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_up_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_up_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_xup_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_macd_xup_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_both_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_both_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam61_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam61_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam71_conv_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam71_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam71_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam71_n2_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cam91_n1_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cond_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_cond_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_conv_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_conv_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_g_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_head_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_head_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_missing_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_missing_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net2_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net2_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net3_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net3_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net4_conv_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net4_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net4_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net4_rw_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net5_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_or_net5_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_net2_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_net2_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_net3_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_pack_net3_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_present_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_present_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_vol_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_news_vol_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_oppset_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_overnight_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_overnight_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_r_up_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_r_up_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_ret_5_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_ret_5_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_os_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_os_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_os_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_os_macd_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_rsi_os_macd_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_ab_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_ab_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_g_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_g_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_g_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_green_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_green_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_missing_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_vol_missing_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_w_hot_candle_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_w_hot_candle_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_w_hot_cond_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_w_hot_cond_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_any_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_any_h2_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_any_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_any_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_both_n4_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_both_n4_h2_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_both_n4_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_both_n4_h5_s12_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_both_n4_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_coil_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_coil_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_yday_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| union_white_yday_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| yday_gainer_h1_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| yday_gainer_h3_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |
| yday_gainer_h5_webull_sim | 2026-10-06 | not_a_locked_trade | no pre-09:30 plan | 0 | $10,000.00 | $0.00 | — | — | — | — |

## Built after the fact

These days were assembled from plans that already existed. They are not locked performance.

| Book | Days | Last equity | Last reason |
|---|---:|---:|---|
| 1d_top_webull_sim | 20 | $8,633.41 | traded |
| 1m_top_webull_sim | 20 | $9,620.56 | traded |
| 1w_top_webull_sim | 20 | $9,771.36 | traded |
| 2w_top_webull_sim | 20 | $9,999.17 | traded |
| 3d_top_webull_sim | 20 | $9,735.86 | traded |
| L1_long_green_tp8_lowvol_webull_sim | 21 | $10,436.79 | open not observed |
| L2_long_green_tp3_lowvol_webull_sim | 21 | $10,436.79 | open not observed |
| L3_long_green_hold2_midcap_webull_sim | 21 | $8,825.80 | traded |
| L4_long_green_hold8_bbailike_webull_sim | 21 | $10,000.00 | sat out, 0 picks |
| L5_long_green_hold2_midhibeta_webull_sim | 21 | $10,754.11 | traded |
| S1_short_red_1day_optionable_webull_sim | 21 | $10,000.00 | sat out, 0 picks |
| S2_short_red_1day_hivol_webull_sim | 21 | $10,000.00 | sat out, 0 picks |
| breadth_rank_v1_webull_sim | 25 | $10,000.00 | no pre-09:30 plan |
| breadth_rank_v1b_webull_sim | 25 | $10,000.00 | no pre-09:30 plan |
| breadth_rank_v1c_webull_sim | 25 | $10,000.00 | no pre-09:30 plan |
| coil_h3_exit_alarm_webull_sim | 20 | $11,060.48 | sat out, 0 picks |
| combo_e1er_5050_shared_webull_sim | 20 | $11,149.32 | sat out, 0 picks |
| combo_e1s_7030_shared_webull_sim | 20 | $9,212.04 | sat out, 0 picks |
| combo_ee1_3070_shared_webull_sim | 20 | $11,155.67 | sat out, 0 picks |
| combo_ee1_5050_shared_webull_sim | 20 | $11,155.67 | sat out, 0 picks |
| combo_ee1_7030_shared_webull_sim | 20 | $11,155.67 | sat out, 0 picks |
| combo_eer_5050_shared_webull_sim | 20 | $11,149.32 | sat out, 0 picks |
| combo_ef_3070_shared_webull_sim | 20 | $9,522.70 | sat out, 0 picks |
| combo_ef_5050_shared_webull_sim | 20 | $9,522.70 | sat out, 0 picks |
| combo_ef_7030_shared_webull_sim | 20 | $9,522.70 | sat out, 0 picks |
| combo_eh_3070_shared_webull_sim | 20 | $12,077.17 | sat out, 0 picks |
| combo_eh_5050_shared_webull_sim | 20 | $12,077.17 | sat out, 0 picks |
| combo_eh_7030_shared_webull_sim | 20 | $12,077.17 | sat out, 0 picks |
| combo_ehs_601525_shared_webull_sim | 20 | $11,931.46 | sat out, 0 picks |
| combo_ehs_702010_shared_webull_sim | 20 | $11,931.46 | sat out, 0 picks |
| combo_ej_5050_shared_webull_sim | 20 | $11,204.46 | sat out, 0 picks |
| combo_en_3070_shared_webull_sim | 20 | $9,890.48 | sat out, 0 picks |
| combo_en_5050_shared_webull_sim | 20 | $9,890.48 | sat out, 0 picks |
| combo_en_7030_shared_webull_sim | 20 | $9,890.48 | sat out, 0 picks |
| combo_ers_7030_shared_webull_sim | 20 | $8,381.12 | sat out, 0 picks |
| combo_es_8020_shared_webull_sim | 20 | $9,212.04 | sat out, 0 picks |
| combo_es_9010_shared_webull_sim | 20 | $9,212.04 | sat out, 0 picks |
| combo_fe1_5050_shared_webull_sim | 20 | $9,522.70 | sat out, 0 picks |
| combo_fe_5050_shared_webull_sim | 20 | $9,522.70 | sat out, 0 picks |
| combo_fer_5050_shared_webull_sim | 20 | $9,262.92 | sat out, 0 picks |
| combo_fes_403030_shared_webull_sim | 20 | $8,777.11 | sat out, 0 picks |
| combo_fh_7030_shared_webull_sim | 20 | $10,434.49 | sat out, 0 picks |
| combo_fse_333_shared_webull_sim | 20 | $8,850.28 | sat out, 0 picks |
| combo_he1_5050_shared_webull_sim | 20 | $12,077.17 | sat out, 0 picks |
| combo_her_5050_shared_webull_sim | 20 | $11,991.61 | sat out, 0 picks |
| combo_hf_5050_shared_webull_sim | 20 | $10,434.49 | sat out, 0 picks |
| combo_hj_5050_shared_webull_sim | 20 | $11,513.36 | sat out, 0 picks |
| combo_hn_3070_shared_webull_sim | 20 | $10,255.35 | sat out, 0 picks |
| combo_hn_5050_shared_webull_sim | 20 | $10,255.35 | sat out, 0 picks |
| combo_hn_7030_shared_webull_sim | 20 | $10,255.35 | sat out, 0 picks |
| combo_je1_5050_shared_webull_sim | 20 | $11,204.46 | sat out, 0 picks |
| combo_jer_5050_shared_webull_sim | 20 | $11,318.12 | sat out, 0 picks |
| combo_jf_5050_shared_webull_sim | 20 | $10,621.08 | sat out, 0 picks |
| combo_jse_333_shared_webull_sim | 20 | $10,627.67 | sat out, 0 picks |
| combo_ne1_5050_shared_webull_sim | 20 | $9,890.48 | sat out, 0 picks |
| combo_ner_5050_shared_webull_sim | 20 | $9,731.14 | sat out, 0 picks |
| combo_nf_5050_shared_webull_sim | 20 | $9,290.35 | sat out, 0 picks |
| combo_nj_5050_shared_webull_sim | 20 | $10,929.84 | sat out, 0 picks |
| combo_nse_333_shared_webull_sim | 20 | $9,545.62 | sat out, 0 picks |
| combo_oh_5050_shared_webull_sim | 11 | $11,963.65 | sat out, 0 picks |
| combo_p2s_5050_shared_webull_sim | 17 | $8,683.67 | sat out, 0 picks |
| combo_ps_5050_shared_webull_sim | 17 | $8,683.67 | sat out, 0 picks |
| combo_ps_7030_shared_webull_sim | 17 | $8,683.67 | sat out, 0 picks |
| combo_se1_5050_shared_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_se_3070_shared_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_se_5050_shared_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_se_5050_skip_webull_sim | 20 | $9,567.69 | sat out, 0 picks |
| combo_se_5050_split_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_se_5050_weather_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_se_7030_shared_webull_sim | 20 | $9,700.98 | sat out, 0 picks |
| combo_seh_333_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_333_skip_webull_sim | 20 | $12,078.12 | sat out, 0 picks |
| combo_seh_333_split_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_333_weather_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_403525_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_404020_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_451540_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_502525_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_502525_split_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_seh_601525_shared_webull_sim | 20 | $12,024.45 | sat out, 0 picks |
| combo_ser_5050_shared_webull_sim | 20 | $8,860.79 | sat out, 0 picks |
| combo_sf_3070_shared_webull_sim | 20 | $8,059.87 | sat out, 0 picks |
| combo_sf_5050_shared_webull_sim | 20 | $8,059.87 | sat out, 0 picks |
| combo_sf_7030_shared_webull_sim | 20 | $8,059.87 | sat out, 0 picks |
| combo_sh_3070_shared_webull_sim | 20 | $11,612.18 | sat out, 0 picks |
| combo_sh_5050_shared_webull_sim | 20 | $11,612.18 | sat out, 0 picks |
| combo_sh_7030_shared_webull_sim | 20 | $11,612.18 | sat out, 0 picks |
| combo_sh_macd_5050_shared_webull_sim | 16 | $12,714.65 | sat out, 0 picks |
| combo_sj_3070_shared_webull_sim | 20 | $10,677.81 | sat out, 0 picks |
| combo_sj_5050_shared_webull_sim | 20 | $10,677.81 | sat out, 0 picks |
| combo_sj_7030_shared_webull_sim | 20 | $10,677.81 | sat out, 0 picks |
| combo_sn_3070_shared_webull_sim | 20 | $8,677.82 | sat out, 0 picks |
| combo_sn_5050_shared_webull_sim | 20 | $8,677.82 | sat out, 0 picks |
| combo_sn_7030_shared_webull_sim | 20 | $8,677.82 | sat out, 0 picks |
| combo_snj_333_shared_webull_sim | 20 | $10,443.42 | sat out, 0 picks |
| flatten_h1_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_cut_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_half_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_rankw_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_sboost_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_sizeup_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_time_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_topheavy_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_trail_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h3_webull_sim | 20 | $9,555.54 | sat out, 0 picks |
| flatten_h5_cut_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_half_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_rankw_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_s8_webull_sim | 17 | $8,975.89 | sat out, 0 picks |
| flatten_h5_sboost_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_sizeup_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_time_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_topheavy_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_trail_webull_sim | 20 | $8,879.81 | sat out, 0 picks |
| flatten_h5_webull_sim | 20 | $9,754.19 | sat out, 0 picks |
| flatten_live_h1_cut_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_half_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_rankw_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_sboost_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_sizeup_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_time_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_topheavy_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_trail_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h1_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h3_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_live_h5_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| flatten_robust_webull_sim | 20 | $11,173.65 | traded |
| flatten_vol_g_h3_webull_sim | 20 | $9,613.31 | sat out, 0 picks |
| flatten_white_yday_h5_webull_sim | 17 | $9,206.73 | sat out, 0 picks |
| forward_shadow_fwd_union_hot_n4_h1_webull_sim | 6 | $10,000.00 | sat out, 0 picks |
| forward_shadow_fwd_union_hot_score_h3_webull_sim | 6 | $10,000.00 | sat out, 0 picks |
| forward_shadow_union_hot_n4_h1__w0_webull_sim | 6 | $10,000.00 | sat out, 0 picks |
| forward_shadow_union_hot_n4_h1_nonews__w0_webull_sim | 6 | $10,000.00 | sat out, 0 picks |
| forward_shadow_union_hot_n4_holdup__w0_webull_sim | 6 | $10,000.00 | sat out, 0 picks |
| h1_webull_sim | 6 | $12,883.39 | traded |
| lever_search_webull_sim | 25 | $10,000.00 | no pre-09:30 plan |
| ohlc_hot_coil_h1_webull_sim | 20 | $10,956.05 | sat out, 0 picks |
| ohlc_hot_h1_webull_sim | 20 | $11,100.49 | sat out, 0 picks |
| ohlc_hot_h3_webull_sim | 20 | $11,247.56 | sat out, 0 picks |
| ohlc_hot_h5_webull_sim | 20 | $10,908.09 | sat out, 0 picks |
| oos0914_webull_sim | 25 | $10,000.00 | no pre-09:30 plan |
| oppset_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| overnight_h1_webull_sim | 11 | $7,935.13 | sat out, 0 picks |
| overnight_h3_webull_sim | 11 | $8,927.99 | sat out, 0 picks |
| overnight_h5_webull_sim | 11 | $8,679.22 | sat out, 0 picks |
| overnight_mega_green_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| overnight_mega_h1_webull_sim | 11 | $10,161.81 | sat out, 0 picks |
| overnight_mega_h2_webull_sim | 11 | $10,161.81 | sat out, 0 picks |
| probable_h1_webull_sim | 20 | $11,924.14 | sat out, 0 picks |
| probable_h3_webull_sim | 20 | $11,688.62 | sat out, 0 picks |
| probable_h5_webull_sim | 20 | $11,443.42 | sat out, 0 picks |
| probable_probable_ok_h1_webull_sim | 20 | $12,391.52 | sat out, 0 picks |
| probable_probable_ok_h3_webull_sim | 20 | $9,443.24 | sat out, 0 picks |
| short_alarm_h1_webull_sim | 20 | $7,195.43 | sat out, 0 picks |
| short_alarm_h3_webull_sim | 20 | $9,058.01 | sat out, 0 picks |
| short_clk_ext_veto_h3_webull_sim | 11 | $-12,200.63 | sat out, 0 picks |
| short_clk_ext_veto_opp_h3_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| short_clk_neg_weak_fail_h3_webull_sim | 11 | $7,159.32 | sat out, 0 picks |
| short_clk_neg_weak_fail_opp_h3_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| short_extended_h1_webull_sim | 20 | $9,280.58 | sat out, 0 picks |
| short_extended_h3_webull_sim | 20 | $13,273.77 | sat out, 0 picks |
| short_last_red_h1_webull_sim | 20 | $10,447.65 | sat out, 0 picks |
| short_last_red_h3_webull_sim | 20 | $10,961.84 | sat out, 0 picks |
| short_macd_dn_h1_webull_sim | 16 | $5,192.49 | sat out, 0 picks |
| short_macd_dn_h3_webull_sim | 16 | $5,516.64 | sat out, 0 picks |
| short_news_head_h3_webull_sim | 17 | $3,383.41 | sat out, 0 picks |
| short_news_or_h3_webull_sim | 17 | $3,329.87 | sat out, 0 picks |
| short_news_pack_h3_webull_sim | 17 | $7,528.65 | sat out, 0 picks |
| short_news_r_h1_webull_sim | 20 | $6,777.95 | sat out, 0 picks |
| short_news_r_h3_webull_sim | 20 | $3,329.87 | sat out, 0 picks |
| short_news_r_macd_h3_webull_sim | 16 | $-700.13 | sat out, 0 picks |
| short_r_down_h1_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| short_r_down_h3_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| short_rsi_ob_h1_webull_sim | 16 | $2,471.04 | sat out, 0 picks |
| short_rsi_ob_h3_webull_sim | 16 | $-4,216.78 | sat out, 0 picks |
| stock_book_1d_webull_sim | 20 | $8,633.41 | traded |
| stock_book_1m_webull_sim | 20 | $9,620.56 | traded |
| stock_book_1w_webull_sim | 20 | $9,771.36 | traded |
| stock_book_2w_webull_sim | 20 | $9,999.17 | traded |
| stock_book_3d_webull_sim | 20 | $9,735.86 | traded |
| theme_radar_fpe_delta_t3_earn_today_3d_webull_sim | 16 | $10,000.00 | open not observed |
| theme_radar_fresh_dcp_t1_avoid_ah_3d_webull_sim | 13 | $10,426.94 | traded |
| theme_radar_fresh_dcp_t1_ep_ge03_2d_webull_sim | 13 | $10,296.57 | traded |
| union_ab_g_h1_webull_sim | 20 | $11,385.67 | sat out, 0 picks |
| union_ab_g_h3_webull_sim | 20 | $10,775.56 | sat out, 0 picks |
| union_blue_coil_h1_webull_sim | 20 | $10,905.43 | sat out, 0 picks |
| union_blue_coil_h3_webull_sim | 20 | $10,551.81 | sat out, 0 picks |
| union_blue_h1_webull_sim | 20 | $10,542.46 | sat out, 0 picks |
| union_blue_h3_webull_sim | 20 | $10,406.64 | sat out, 0 picks |
| union_blue_vol_h1_webull_sim | 20 | $11,665.32 | sat out, 0 picks |
| union_blue_vol_h3_webull_sim | 20 | $10,297.33 | sat out, 0 picks |
| union_break10_h1_webull_sim | 20 | $13,085.27 | sat out, 0 picks |
| union_break10_h3_webull_sim | 20 | $12,423.00 | sat out, 0 picks |
| union_candle_h1_webull_sim | 20 | $13,407.88 | sat out, 0 picks |
| union_candle_h3_webull_sim | 20 | $10,884.47 | sat out, 0 picks |
| union_candle_score_h1_webull_sim | 20 | $10,140.09 | sat out, 0 picks |
| union_candle_score_h3_webull_sim | 20 | $10,733.66 | sat out, 0 picks |
| union_catal_present_h1_webull_sim | 20 | $8,312.35 | sat out, 0 picks |
| union_catal_present_h3_webull_sim | 20 | $8,471.15 | sat out, 0 picks |
| union_clk_earn_guide_react_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_clk_flow_coil_h1_webull_sim | 11 | $9,353.55 | sat out, 0 picks |
| union_clk_fresh_cat_coil_h1_webull_sim | 11 | $11,040.88 | sat out, 0 picks |
| union_clk_fresh_cat_coil_opp_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_clk_hold_vs_sector_h1_webull_sim | 11 | $11,491.39 | sat out, 0 picks |
| union_clk_hold_vs_sector_opp_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_clk_insider_cash_stab_h3_webull_sim | 11 | $10,446.75 | sat out, 0 picks |
| union_clk_mom_break_peer_h1_webull_sim | 11 | $10,317.48 | sat out, 0 picks |
| union_clk_mom_break_peer_opp_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_clk_nr7_mom_h1_webull_sim | 11 | $17,464.05 | sat out, 0 picks |
| union_clk_nr7_mom_opp_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_clk_r_up_coil_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_coil_green_h1_webull_sim | 20 | $11,422.14 | sat out, 0 picks |
| union_coil_green_h3_webull_sim | 20 | $10,279.08 | sat out, 0 picks |
| union_coil_off_h1_webull_sim | 20 | $10,970.21 | sat out, 0 picks |
| union_coil_off_h3_webull_sim | 20 | $10,693.54 | sat out, 0 picks |
| union_coil_off_h5_webull_sim | 20 | $10,381.28 | sat out, 0 picks |
| union_cond_h1_webull_sim | 20 | $12,454.68 | sat out, 0 picks |
| union_cond_h3_webull_sim | 20 | $12,600.03 | sat out, 0 picks |
| union_cond_n4_h3_webull_sim | 20 | $11,543.94 | sat out, 0 picks |
| union_e_fresh_h1_webull_sim | 20 | $11,495.94 | sat out, 0 picks |
| union_e_fresh_h3_webull_sim | 20 | $11,102.43 | sat out, 0 picks |
| union_e_green_h1_webull_sim | 20 | $13,809.02 | sat out, 0 picks |
| union_e_green_h3_webull_sim | 20 | $10,310.26 | sat out, 0 picks |
| union_earn_react_h1_webull_sim | 20 | $11,184.60 | sat out, 0 picks |
| union_earn_react_h3_webull_sim | 20 | $11,242.12 | sat out, 0 picks |
| union_flow_in_h1_webull_sim | 16 | $8,942.04 | sat out, 0 picks |
| union_flow_in_h3_webull_sim | 16 | $8,926.39 | sat out, 0 picks |
| union_flow_in_h5_webull_sim | 16 | $8,635.60 | sat out, 0 picks |
| union_flow_in_white_h1_webull_sim | 16 | $10,000.00 | sat out, 0 picks |
| union_flow_in_white_h3_webull_sim | 16 | $10,000.00 | sat out, 0 picks |
| union_h1_cut_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_half_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_rankw_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_sboost_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_sizeup_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_time_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_topheavy_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_trail_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h1_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_cut_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_exit_alarm_webull_sim | 20 | $11,644.25 | sat out, 0 picks |
| union_h3_exit_news_r_webull_sim | 20 | $11,860.53 | sat out, 0 picks |
| union_h3_exit_red_webull_sim | 20 | $14,090.38 | sat out, 0 picks |
| union_h3_half_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_rankw_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_sboost_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_sizeup_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_time_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_topheavy_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_trail_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h3_webull_sim | 20 | $11,298.41 | sat out, 0 picks |
| union_h5_cut_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_exit_alarm_webull_sim | 20 | $11,644.25 | sat out, 0 picks |
| union_h5_half_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_rankw_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_sboost_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_sizeup_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_time_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_topheavy_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_trail_webull_sim | 20 | $12,257.77 | sat out, 0 picks |
| union_h5_webull_sim | 20 | $9,867.20 | sat out, 0 picks |
| union_hot_n12_h1_webull_sim | 20 | $11,749.23 | sat out, 0 picks |
| union_hot_n4_h1_webull_sim | 20 | $12,996.60 | sat out, 0 picks |
| union_hot_n4_holdup_webull_sim | 11 | $12,022.11 | sat out, 0 picks |
| union_hot_score_h1_webull_sim | 20 | $12,002.19 | sat out, 0 picks |
| union_hot_score_h3_webull_sim | 20 | $12,024.04 | sat out, 0 picks |
| union_join_g_h1_webull_sim | 20 | $11,666.05 | sat out, 0 picks |
| union_join_g_h3_webull_sim | 20 | $11,066.99 | sat out, 0 picks |
| union_join_present_h1_webull_sim | 20 | $11,644.25 | sat out, 0 picks |
| union_join_present_h3_webull_sim | 20 | $11,575.36 | sat out, 0 picks |
| union_join_vol_green_h1_webull_sim | 20 | $11,880.56 | sat out, 0 picks |
| union_join_vol_green_h3_webull_sim | 20 | $12,041.87 | sat out, 0 picks |
| union_last_green_h1_webull_sim | 20 | $14,090.38 | sat out, 0 picks |
| union_last_green_h3_webull_sim | 20 | $12,387.70 | sat out, 0 picks |
| union_last_green_h5_webull_sim | 20 | $12,250.88 | sat out, 0 picks |
| union_last_red_h1_webull_sim | 20 | $9,410.41 | sat out, 0 picks |
| union_last_red_h3_webull_sim | 20 | $9,722.58 | sat out, 0 picks |
| union_macd_hist_h1_webull_sim | 16 | $11,159.64 | sat out, 0 picks |
| union_macd_hist_h3_webull_sim | 16 | $11,489.29 | sat out, 0 picks |
| union_macd_up_h1_webull_sim | 16 | $12,848.72 | sat out, 0 picks |
| union_macd_up_h3_webull_sim | 16 | $11,151.17 | sat out, 0 picks |
| union_macd_xup_h1_webull_sim | 16 | $14,137.88 | sat out, 0 picks |
| union_macd_xup_h3_webull_sim | 16 | $9,491.82 | sat out, 0 picks |
| union_news_both_h1_webull_sim | 17 | $9,605.32 | sat out, 0 picks |
| union_news_both_h3_webull_sim | 17 | $9,605.32 | sat out, 0 picks |
| union_news_g_cam61_h1_webull_sim | 17 | $8,844.60 | sat out, 0 picks |
| union_news_g_cam61_h3_webull_sim | 17 | $9,896.49 | sat out, 0 picks |
| union_news_g_cam71_conv_h1_webull_sim | 17 | $9,388.22 | sat out, 0 picks |
| union_news_g_cam71_h1_webull_sim | 17 | $9,388.22 | sat out, 0 picks |
| union_news_g_cam71_h3_webull_sim | 17 | $10,524.30 | sat out, 0 picks |
| union_news_g_cam71_n2_h1_webull_sim | 17 | $10,527.47 | sat out, 0 picks |
| union_news_g_cam91_n1_h1_webull_sim | 17 | $10,074.20 | sat out, 0 picks |
| union_news_g_cond_h1_webull_sim | 17 | $9,912.22 | sat out, 0 picks |
| union_news_g_cond_h3_webull_sim | 17 | $10,058.28 | sat out, 0 picks |
| union_news_g_conv_h1_webull_sim | 17 | $10,474.95 | sat out, 0 picks |
| union_news_g_conv_h3_webull_sim | 17 | $10,913.97 | sat out, 0 picks |
| union_news_g_h1_webull_sim | 20 | $9,442.84 | sat out, 0 picks |
| union_news_g_h3_webull_sim | 20 | $9,526.16 | sat out, 0 picks |
| union_news_g_h5_webull_sim | 20 | $11,034.23 | sat out, 0 picks |
| union_news_head_h1_webull_sim | 17 | $10,072.54 | sat out, 0 picks |
| union_news_head_h3_webull_sim | 17 | $9,939.27 | sat out, 0 picks |
| union_news_missing_h1_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_news_missing_h3_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_news_or_h1_webull_sim | 17 | $9,912.22 | sat out, 0 picks |
| union_news_or_h3_webull_sim | 17 | $10,058.28 | sat out, 0 picks |
| union_news_or_net2_h1_webull_sim | 17 | $9,236.88 | sat out, 0 picks |
| union_news_or_net2_h3_webull_sim | 17 | $9,188.03 | sat out, 0 picks |
| union_news_or_net3_h1_webull_sim | 17 | $9,227.69 | sat out, 0 picks |
| union_news_or_net3_h3_webull_sim | 17 | $9,830.91 | sat out, 0 picks |
| union_news_or_net4_conv_h1_webull_sim | 17 | $8,822.52 | sat out, 0 picks |
| union_news_or_net4_h1_webull_sim | 17 | $9,055.03 | sat out, 0 picks |
| union_news_or_net4_h3_webull_sim | 17 | $9,834.01 | sat out, 0 picks |
| union_news_or_net4_rw_h1_webull_sim | 17 | $8,822.52 | sat out, 0 picks |
| union_news_or_net5_h1_webull_sim | 17 | $9,137.83 | sat out, 0 picks |
| union_news_or_net5_h3_webull_sim | 17 | $9,896.49 | sat out, 0 picks |
| union_news_pack_h1_webull_sim | 17 | $10,654.49 | sat out, 0 picks |
| union_news_pack_h3_webull_sim | 17 | $10,171.05 | sat out, 0 picks |
| union_news_pack_net2_h1_webull_sim | 17 | $10,654.49 | sat out, 0 picks |
| union_news_pack_net2_h3_webull_sim | 17 | $10,171.05 | sat out, 0 picks |
| union_news_pack_net3_h1_webull_sim | 17 | $10,727.33 | sat out, 0 picks |
| union_news_pack_net3_h3_webull_sim | 17 | $10,494.14 | sat out, 0 picks |
| union_news_present_h1_webull_sim | 20 | $11,644.25 | sat out, 0 picks |
| union_news_present_h3_webull_sim | 20 | $11,575.36 | sat out, 0 picks |
| union_news_vol_h1_webull_sim | 20 | $7,802.72 | sat out, 0 picks |
| union_news_vol_h3_webull_sim | 20 | $8,979.54 | sat out, 0 picks |
| union_oppset_h1_webull_sim | 11 | $10,000.00 | sat out, 0 picks |
| union_overnight_h1_webull_sim | 11 | $7,935.13 | sat out, 0 picks |
| union_overnight_h3_webull_sim | 11 | $8,927.99 | sat out, 0 picks |
| union_r_up_h1_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_r_up_h3_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_ret_5_h1_webull_sim | 20 | $12,659.14 | sat out, 0 picks |
| union_ret_5_h3_webull_sim | 20 | $11,828.76 | sat out, 0 picks |
| union_rsi_h1_webull_sim | 16 | $9,140.57 | sat out, 0 picks |
| union_rsi_h3_webull_sim | 16 | $9,865.26 | sat out, 0 picks |
| union_rsi_os_h1_webull_sim | 16 | $8,931.71 | sat out, 0 picks |
| union_rsi_os_h3_webull_sim | 16 | $8,481.00 | sat out, 0 picks |
| union_rsi_os_h5_webull_sim | 16 | $9,145.14 | sat out, 0 picks |
| union_rsi_os_macd_h1_webull_sim | 16 | $7,120.24 | sat out, 0 picks |
| union_rsi_os_macd_h3_webull_sim | 16 | $8,115.47 | sat out, 0 picks |
| union_vol_ab_h1_webull_sim | 20 | $12,199.04 | sat out, 0 picks |
| union_vol_ab_h3_webull_sim | 20 | $12,002.28 | sat out, 0 picks |
| union_vol_g_h1_webull_sim | 20 | $11,474.79 | sat out, 0 picks |
| union_vol_g_h3_webull_sim | 20 | $11,421.30 | sat out, 0 picks |
| union_vol_g_h5_webull_sim | 20 | $10,967.19 | sat out, 0 picks |
| union_vol_green_h1_webull_sim | 20 | $12,393.04 | sat out, 0 picks |
| union_vol_green_h3_webull_sim | 20 | $10,675.94 | sat out, 0 picks |
| union_vol_missing_h1_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_vol_missing_h3_webull_sim | 20 | $10,000.00 | sat out, 0 picks |
| union_w_hot_candle_h1_webull_sim | 20 | $12,061.53 | sat out, 0 picks |
| union_w_hot_candle_h3_webull_sim | 20 | $12,378.01 | sat out, 0 picks |
| union_w_hot_cond_h1_webull_sim | 20 | $12,792.33 | sat out, 0 picks |
| union_w_hot_cond_h3_webull_sim | 20 | $12,025.87 | sat out, 0 picks |
| union_white_any_h1_webull_sim | 17 | $10,044.81 | sat out, 0 picks |
| union_white_any_h2_webull_sim | 17 | $10,044.81 | sat out, 0 picks |
| union_white_any_h3_webull_sim | 17 | $8,919.26 | sat out, 0 picks |
| union_white_any_h5_webull_sim | 17 | $9,517.72 | sat out, 0 picks |
| union_white_both_n4_h1_webull_sim | 17 | $9,911.31 | sat out, 0 picks |
| union_white_both_n4_h2_webull_sim | 17 | $9,911.31 | sat out, 0 picks |
| union_white_both_n4_h3_webull_sim | 17 | $9,911.31 | sat out, 0 picks |
| union_white_both_n4_h5_s12_webull_sim | 17 | $9,911.31 | sat out, 0 picks |
| union_white_both_n4_h5_webull_sim | 17 | $9,911.31 | sat out, 0 picks |
| union_white_coil_h1_webull_sim | 20 | $11,403.46 | sat out, 0 picks |
| union_white_coil_h3_webull_sim | 20 | $9,215.89 | sat out, 0 picks |
| union_white_h1_webull_sim | 20 | $11,183.77 | sat out, 0 picks |
| union_white_h3_webull_sim | 20 | $9,678.73 | sat out, 0 picks |
| union_white_h5_webull_sim | 20 | $9,755.89 | sat out, 0 picks |
| union_white_yday_h1_webull_sim | 17 | $9,714.65 | sat out, 0 picks |
| union_white_yday_h3_webull_sim | 17 | $8,648.00 | sat out, 0 picks |
| yday_gainer_h1_webull_sim | 20 | $11,899.16 | sat out, 0 picks |
| yday_gainer_h3_webull_sim | 20 | $12,345.90 | sat out, 0 picks |
| yday_gainer_h5_webull_sim | 20 | $11,642.96 | sat out, 0 picks |

Per-day provenance (path, commit, commit time in ET) is on every row in `data/webull_sim/days.jsonl`.

Dispatch after 09:40 ET: `gh workflow run webull_sim.yml --ref main`.
