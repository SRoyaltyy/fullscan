# Ticket filter v2 — contradiction gates + cross-strat backtest

Window **2026-08-13 → 2026-09-15** · 23 sessions · fit `2026-09-04` / OOS `2026-09-05`→latest.

Per-strategy EW set = strategies with **≥8** marked hold-horizon trades on the full window (N chosen so tiny sleeves do not dominate). 301 strategies qualify. Proposed rows 45149 · unique 2764.

## Contract

- RWT 9/11 blocked=True
- OOS EW baseline=-1.301 F1=-1.3005 v2=-1.3001
- N=8 strats in EW (IS-or-full marked≥N): 301
- mute Layer 3: ship=False — mute-extra IS meanH 1.0158 > 0 (n=31539; mostly family-wide longs) — drop Layer 3

**Contract PASS** · shipped `oversold_crash_pause, short_vs_green_cameras`

## Equal-weight average across strats (meanH %)

| Split | baseline | F1-only | v2 | n_in_avg / n_strats |
|---|---:|---:|---:|---|
| Full | 1.0019 | 1.0063 | 1.0088 | 301/301 (empty 0) |
| IS | 1.1176 | 1.1219 | 1.121 | 300/301 (empty 1) |
| OOS | -1.301 | -1.3005 | -1.3001 | 154/301 (empty 147) |

EW uses defined means only (emptied sleeves excluded). `mean_empty0` treats empty as 0: OOS baseline=-0.6656 F1=-0.6654 v2=-0.6652.

## Unique (date, ticker, side) — same as #252

| Split | baseline | F1 | v2 | v2 blocked |
|---|---|---|---|---|
| Full | n=2764 marked=2279 meanH=-0.017% hit=48.6% d1=-0.0297 d5=-1.0823 | n=2759 marked=2274 meanH=-0.013% hit=48.6% d1=-0.0301 d5=-1.0793 | n=2628 marked=2148 meanH=+0.044% hit=48.8% d1=0.0235 d5=-1.1864 | n=136 marked=131 meanH=-1.015% hit=44.3% d1=-0.9424 d5=0.4288 |
| IS | n=2008 marked=1938 meanH=-0.032% hit=49.4% d1=-0.0221 d5=-1.0823 | n=2004 marked=1934 meanH=-0.028% hit=49.5% d1=-0.023 d5=-1.0793 | n=1884 marked=1814 meanH=+0.036% hit=49.8% d1=0.0401 d5=-1.1864 | n=124 marked=124 meanH=-1.036% hit=44.4% d1=-0.9593 d5=0.4288 |
| OOS | n=756 marked=341 meanH=+0.070% hit=43.7% d1=-0.0683 d5=None | n=755 marked=340 meanH=+0.073% hit=43.8% d1=-0.0663 d5=None | n=744 marked=334 meanH=+0.086% hit=43.7% d1=-0.0578 d5=None | n=12 marked=7 meanH=-0.643% hit=42.9% d1=-0.6428 d5=None |

## RWT 2026-09-11 short

- blocked=True rsi=18.36 1d=-14.788734286201677 ret_h=-0.8523
- audit: `RWT SHORT 2026-09-11 BLOCKED by oversold_crash_pause (rsi=18.36, 1d=-14.788734286201677, rule=rsi<=25 & crash)`

## Ablations (add one rule on top of F1)

| Tag | Ship | IS blocked meanH | OOS blocked meanH | OOS EW trial vs F1 | Why |
|---|---|---:|---:|---|---|
| C1 `short_vs_own_recipe` | False | -0.1404 n=70 | 0.6203 n=8 | -1.302 vs -1.3005 | OOS blocked meanH 0.6203 > 0 |
| C2 `short_vs_green_cameras` | False | -0.3488 n=228 | 0.5478 n=22 | -1.287 vs -1.3005 | OOS blocked meanH 0.5478 > 0 |
| C2t `short_vs_green_cameras` | True | -0.9778 n=121 | -0.6079 n=6 | -1.3001 vs -1.3005 | IS blocked -0.9778 OOS blocked -0.6079 OOS EW -1.3001 ≥ F1 -1.3005 |
| C3 `long_vs_hard_red_news` | False | 0.5985 n=5 | None n=0 | -1.3005 vs -1.3005 | IS blocked meanH 0.5985 > 0 |
| C4 `short_vs_sector_or_tape` | False | -0.506 n=22 | 0.0 n=1 | -1.3009 vs -1.3005 | OOS EW -1.3009 < F1 -1.3005 |
| F3 `hard_red_rsi30_short` | False | -1.2348 n=7 | 1.4374 n=1 | -1.3025 vs -1.3005 | OOS blocked meanH 1.4374 > 0 |
| F2t `overbought_meltup_pause` | False | 2.3678 n=3 | None n=0 | -1.3005 vs -1.3005 | IS blocked meanH 2.3678 > 0 |
| C2b `short_vs_green_cameras_and` | False | -0.3622 n=226 | 0.5478 n=22 | -1.287 vs -1.3005 | OOS blocked meanH 0.5478 > 0 |
| C2t_and `short_vs_green_cameras_net4_and` | False | -0.9778 n=121 | -0.6079 n=6 | -1.3001 vs -1.3005 | duplicate sleeve of shipped short_vs_green_cameras |

## Per-strategy meanH (full window, n_marked ≥ N)

| Strategy | n | n_marked | baseline | F1 | v2 | Δ v2−base | OOS v2 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **EW avg** | 301 | — | 1.0019 | 1.0063 | 1.0088 | 0.0069 | -1.3001 |
| `1d_top` | 50 | 13 | -0.225 | -0.1727 | -0.1727 | 0.0523 | -0.1727 |
| `coil_h3_exit_alarm` | 178 | 146 | -0.9551 | -0.9551 | -0.9551 | 0.0 | -3.6647 |
| `combo_e1er_5050_shared` | 129 | 109 | -2.3808 | -2.3808 | -2.3808 | 0.0 | None |
| `combo_e1s_7030_shared` | 167 | 135 | -0.2255 | -0.2255 | -0.2568 | -0.0313 | None |
| `combo_ee1_3070_shared` | 112 | 92 | -1.4079 | -1.4079 | -1.4079 | 0.0 | None |
| `combo_ee1_5050_shared` | 112 | 92 | -1.4079 | -1.4079 | -1.4079 | 0.0 | None |
| `combo_ee1_7030_shared` | 112 | 92 | -1.4079 | -1.4079 | -1.4079 | 0.0 | None |
| `combo_eer_5050_shared` | 129 | 109 | -2.3808 | -2.3808 | -2.3808 | 0.0 | None |
| `combo_ef_3070_shared` | 248 | 204 | 0.2826 | 0.2826 | 0.2826 | 0.0 | None |
| `combo_ef_5050_shared` | 248 | 204 | 0.2826 | 0.2826 | 0.2826 | 0.0 | None |
| `combo_ef_7030_shared` | 248 | 204 | 0.2826 | 0.2826 | 0.2826 | 0.0 | None |
| `combo_eh_3070_shared` | 197 | 157 | 0.3581 | 0.3581 | 0.3581 | 0.0 | None |
| `combo_eh_5050_shared` | 197 | 157 | 0.3581 | 0.3581 | 0.3581 | 0.0 | None |
| `combo_eh_7030_shared` | 197 | 157 | 0.3581 | 0.3581 | 0.3581 | 0.0 | None |
| `combo_ehs_601525_shared` | 250 | 198 | 0.8392 | 0.8392 | 0.84 | 0.0008 | None |
| `combo_ehs_702010_shared` | 250 | 198 | 0.8392 | 0.8392 | 0.84 | 0.0008 | None |
| `combo_ej_5050_shared` | 192 | 157 | -1.1241 | -1.1241 | -1.1241 | 0.0 | None |
| `combo_en_3070_shared` | 230 | 194 | 3.7287 | 3.7287 | 3.7287 | 0.0 | None |
| `combo_en_5050_shared` | 230 | 194 | 3.7287 | 3.7287 | 3.7287 | 0.0 | None |
| `combo_en_7030_shared` | 230 | 194 | 3.7287 | 3.7287 | 3.7287 | 0.0 | None |
| `combo_ers_7030_shared` | 166 | 136 | -1.0302 | -1.0243 | -1.0801 | -0.0499 | None |
| `combo_es_8020_shared` | 167 | 135 | -0.2255 | -0.2255 | -0.2568 | -0.0313 | None |
| `combo_es_9010_shared` | 167 | 135 | -0.2255 | -0.2255 | -0.2568 | -0.0313 | None |
| `combo_fe1_5050_shared` | 248 | 204 | 0.2826 | 0.2826 | 0.2826 | 0.0 | None |
| `combo_fe_5050_shared` | 248 | 204 | 0.2826 | 0.2826 | 0.2826 | 0.0 | None |
| `combo_fer_5050_shared` | 243 | 201 | -0.2739 | -0.2739 | -0.2739 | 0.0 | None |
| `combo_fes_403030_shared` | 301 | 246 | 0.632 | 0.632 | 0.6292 | -0.0028 | None |
| `combo_fh_7030_shared` | 218 | 174 | 2.1202 | 2.1202 | 2.1202 | 0.0 | None |
| `combo_fse_333_shared` | 301 | 246 | 0.7786 | 0.7892 | 0.789 | 0.0104 | None |
| `combo_he1_5050_shared` | 197 | 157 | 0.3581 | 0.3581 | 0.3581 | 0.0 | None |
| `combo_her_5050_shared` | 193 | 155 | -0.5723 | -0.5723 | -0.5723 | 0.0 | None |
| `combo_hf_5050_shared` | 218 | 174 | 2.1202 | 2.1202 | 2.1202 | 0.0 | None |
| `combo_hj_5050_shared` | 161 | 127 | 0.6953 | 0.6953 | 0.6953 | 0.0 | None |
| `combo_hn_3070_shared` | 206 | 172 | 6.4786 | 6.4786 | 6.4786 | 0.0 | None |
| `combo_hn_5050_shared` | 206 | 172 | 6.4786 | 6.4786 | 6.4786 | 0.0 | None |
| `combo_hn_7030_shared` | 206 | 172 | 6.4786 | 6.4786 | 6.4786 | 0.0 | None |
| `combo_je1_5050_shared` | 192 | 157 | -1.1241 | -1.1241 | -1.1241 | 0.0 | None |
| `combo_jer_5050_shared` | 188 | 155 | -2.0328 | -2.0328 | -2.0328 | 0.0 | None |
| `combo_jf_5050_shared` | 207 | 167 | 0.4337 | 0.4337 | 0.4337 | 0.0 | None |
| `combo_jse_333_shared` | 247 | 200 | -0.2066 | -0.1985 | -0.219 | -0.0124 | None |
| `combo_ne1_5050_shared` | 230 | 194 | 3.7287 | 3.7287 | 3.7287 | 0.0 | None |
| `combo_ner_5050_shared` | 230 | 196 | 3.0906 | 3.0906 | 3.0906 | 0.0 | None |
| `combo_nf_5050_shared` | 249 | 210 | 5.3195 | 5.3195 | 5.3195 | 0.0 | None |
| `combo_nj_5050_shared` | 202 | 173 | 4.633 | 4.633 | 4.633 | 0.0 | None |
| `combo_nse_333_shared` | 285 | 237 | 3.6224 | 3.6455 | 3.6945 | 0.0721 | None |
| `combo_p2s_5050_shared` | 106 | 90 | 0.2452 | 0.2684 | 0.2434 | -0.0018 | None |
| `combo_ps_5050_shared` | 112 | 95 | 0.1671 | 0.1883 | 0.161 | -0.0061 | None |
| `combo_ps_7030_shared` | 112 | 95 | 0.1671 | 0.1883 | 0.161 | -0.0061 | None |
| `combo_se1_5050_shared` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_se_3070_shared` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_se_5050_shared` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_se_5050_skip` | 160 | 128 | -0.0969 | -0.0969 | -0.1259 | -0.029 | None |
| `combo_se_5050_split` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_se_5050_weather` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_se_7030_shared` | 167 | 135 | 0.0416 | 0.0555 | 0.0326 | -0.009 | None |
| `combo_seh_333_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_333_skip` | 241 | 189 | 0.9171 | 0.9171 | 0.9197 | 0.0026 | None |
| `combo_seh_333_split` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_333_weather` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_403525_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_404020_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_451540_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_502525_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_502525_split` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_seh_601525_shared` | 250 | 198 | 0.9117 | 0.9256 | 0.9282 | 0.0165 | None |
| `combo_ser_5050_shared` | 166 | 136 | -0.9028 | -0.896 | -0.9478 | -0.045 | None |
| `combo_sf_3070_shared` | 198 | 163 | 2.2845 | 2.3098 | 2.348 | 0.0635 | None |
| `combo_sf_5050_shared` | 198 | 163 | 2.2845 | 2.3098 | 2.348 | 0.0635 | None |
| `combo_sf_7030_shared` | 198 | 163 | 2.2845 | 2.3098 | 2.348 | 0.0635 | None |
| `combo_sh_3070_shared` | 148 | 116 | 2.8156 | 2.8559 | 2.9299 | 0.1143 | None |
| `combo_sh_5050_shared` | 148 | 116 | 2.8156 | 2.8559 | 2.9299 | 0.1143 | None |
| `combo_sh_7030_shared` | 148 | 116 | 2.8156 | 2.8559 | 2.9299 | 0.1143 | None |
| `combo_sh_macd_5050_shared` | 133 | 107 | 2.8471 | 2.8471 | 2.9266 | 0.0795 | None |
| `combo_sj_3070_shared` | 146 | 118 | 0.3475 | 0.366 | 0.3506 | 0.0031 | None |
| `combo_sj_5050_shared` | 146 | 118 | 0.3475 | 0.366 | 0.3506 | 0.0031 | None |
| `combo_sj_7030_shared` | 146 | 118 | 0.3475 | 0.366 | 0.3506 | 0.0031 | None |
| `combo_sn_3070_shared` | 190 | 162 | 6.0808 | 6.1299 | 6.2657 | 0.1849 | None |
| `combo_sn_5050_shared` | 190 | 162 | 6.0808 | 6.1299 | 6.2657 | 0.1849 | None |
| `combo_sn_7030_shared` | 190 | 162 | 6.0808 | 6.1299 | 6.2657 | 0.1849 | None |
| `combo_snj_333_shared` | 264 | 223 | 4.1194 | 4.1462 | 4.2076 | 0.0882 | None |
| `flatten_h1` | 144 | 136 | 0.3794 | 0.3794 | 0.3794 | 0.0 | -0.7882 |
| `flatten_h3` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_cut` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_half` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_rankw` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_sboost` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_sizeup` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_time` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_topheavy` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h3_trail` | 144 | 124 | 1.3156 | 1.3156 | 1.3156 | 0.0 | -0.1445 |
| `flatten_h5` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_cut` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_half` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_rankw` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_s8` | 132 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_sboost` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_sizeup` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_time` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_topheavy` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_h5_trail` | 144 | 114 | 2.228 | 2.228 | 2.228 | 0.0 | None |
| `flatten_live_h1` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_cut` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_half` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_rankw` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_sboost` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_sizeup` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_time` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_topheavy` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h1_trail` | 24 | 24 | 1.4719 | 1.4719 | 1.4719 | 0.0 | None |
| `flatten_live_h3` | 24 | 24 | 2.489 | 2.489 | 2.489 | 0.0 | None |
| `flatten_live_h5` | 24 | 24 | 2.0353 | 2.0353 | 2.0353 | 0.0 | None |
| `flatten_vol_g_h3` | 32 | 30 | 1.2601 | 1.2601 | 1.2601 | 0.0 | -0.7238 |
| `flatten_white_yday_h5` | 25 | 25 | 4.0078 | 4.0078 | 4.0078 | 0.0 | None |
| `ohlc_hot_coil_h1` | 100 | 84 | -0.3992 | -0.3992 | -0.3992 | 0.0 | -2.1589 |
| `ohlc_hot_h1` | 168 | 152 | -0.0799 | -0.0799 | -0.0799 | 0.0 | -0.702 |
| `ohlc_hot_h3` | 168 | 136 | 5.9748 | 5.9748 | 5.9748 | 0.0 | -5.1488 |
| `ohlc_hot_h5` | 168 | 120 | 7.1145 | 7.1145 | 7.1145 | 0.0 | None |
| `probable_h1` | 168 | 152 | 0.233 | 0.233 | 0.233 | 0.0 | -1.2291 |
| `probable_h3` | 168 | 136 | 0.5403 | 0.5403 | 0.5403 | 0.0 | -4.9164 |
| `probable_h5` | 168 | 120 | 0.1951 | 0.1951 | 0.1951 | 0.0 | None |
| `probable_probable_ok_h1` | 74 | 68 | -0.0228 | -0.0228 | -0.0228 | 0.0 | -1.9164 |
| `probable_probable_ok_h3` | 74 | 64 | 1.2128 | 1.2128 | 1.2128 | 0.0 | -7.7946 |
| `short_alarm_h1` | 117 | 105 | 0.3051 | 0.308 | 0.4047 | 0.0996 | 0.3458 |
| `short_alarm_h3` | 117 | 89 | 0.9726 | 1.1337 | 0.9318 | -0.0408 | 4.9995 |
| `short_extended_h1` | 162 | 146 | 0.389 | 0.389 | 1.2486 | 0.8596 | 2.0874 |
| `short_extended_h3` | 162 | 130 | -1.1558 | -1.1558 | -0.2599 | 0.8959 | 1.4445 |
| `short_last_red_h1` | 180 | 164 | -0.836 | -0.8411 | -0.7405 | 0.0955 | -0.1438 |
| `short_last_red_h3` | 180 | 148 | -1.2992 | -1.2182 | -1.6623 | -0.3631 | 1.7556 |
| `short_macd_dn_h1` | 160 | 144 | -0.3812 | -0.3999 | -0.1254 | 0.2558 | -0.0363 |
| `short_macd_dn_h3` | 160 | 144 | -0.6064 | -0.5333 | -0.432 | 0.1744 | 1.1264 |
| `short_news_head_h3` | 48 | 42 | 1.053 | 1.1172 | 1.1172 | 0.0642 | 4.0042 |
| `short_news_or_h3` | 62 | 54 | 0.9881 | 1.0365 | 1.2831 | 0.295 | 3.1805 |
| `short_news_pack_h3` | 14 | 12 | 0.7609 | 0.7609 | 2.1334 | 1.3725 | 0.7094 |
| `short_news_r_h1` | 62 | 54 | 0.2035 | 0.2302 | 0.3455 | 0.142 | 1.0299 |
| `short_news_r_h3` | 62 | 54 | 0.9881 | 1.0365 | 1.2831 | 0.295 | 3.1805 |
| `short_news_r_macd_h3` | 50 | 44 | 1.7108 | 1.7108 | 2.0802 | 0.3694 | 4.0042 |
| `short_rsi_ob_h1` | 151 | 135 | -0.6106 | -0.6106 | -0.1909 | 0.4197 | 2.4642 |
| `short_rsi_ob_h3` | 151 | 135 | -9.4588 | -9.4588 | -13.6031 | -4.1443 | 4.5837 |
| `stock_book_1d` | 426 | 370 | 0.4616 | 0.4652 | 0.4652 | 0.0036 | 0.0898 |
| `stock_book_1m` | 539 | 10 | 4.5845 | 4.5845 | 4.5845 | 0.0 | None |
| `stock_book_1w` | 539 | 316 | 0.5089 | 0.5105 | 0.5105 | 0.0016 | None |
| `stock_book_2w` | 539 | 211 | 1.785 | 1.785 | 1.785 | 0.0 | None |
| `stock_book_3d` | 539 | 365 | 0.4584 | 0.4842 | 0.4842 | 0.0258 | 2.7463 |
| `union_ab_g_h1` | 144 | 128 | -0.155 | -0.155 | -0.155 | 0.0 | -0.1635 |
| `union_ab_g_h3` | 144 | 112 | -0.8958 | -0.8958 | -0.8958 | 0.0 | -2.133 |
| `union_blue_coil_h1` | 153 | 139 | -0.0868 | -0.0868 | -0.0868 | 0.0 | -0.4217 |
| `union_blue_coil_h3` | 153 | 123 | -0.4444 | -0.4444 | -0.4444 | 0.0 | -0.7899 |
| `union_blue_h1` | 160 | 145 | 0.1968 | 0.1968 | 0.1968 | 0.0 | -0.6093 |
| `union_blue_h3` | 160 | 129 | 0.0857 | 0.0857 | 0.0857 | 0.0 | -0.9589 |
| `union_blue_vol_h1` | 100 | 90 | 0.4525 | 0.4525 | 0.4525 | 0.0 | -2.9524 |
| `union_blue_vol_h3` | 100 | 90 | 0.7601 | 0.7601 | 0.7601 | 0.0 | -4.4179 |
| `union_break10_h1` | 171 | 155 | 0.5232 | 0.5232 | 0.5232 | 0.0 | 0.9293 |
| `union_break10_h3` | 171 | 139 | 1.2264 | 1.2264 | 1.2264 | 0.0 | 0.2898 |
| `union_candle_h1` | 179 | 163 | 0.2453 | 0.2453 | 0.2453 | 0.0 | 0.3289 |
| `union_candle_h3` | 179 | 147 | 0.5433 | 0.5433 | 0.5433 | 0.0 | -1.814 |
| `union_candle_score_h1` | 184 | 168 | 0.2155 | 0.2155 | 0.2155 | 0.0 | -0.3439 |
| `union_candle_score_h3` | 184 | 152 | 0.5548 | 0.5548 | 0.5548 | 0.0 | -2.6489 |
| `union_coil_green_h1` | 164 | 148 | -0.3844 | -0.3844 | -0.3844 | 0.0 | -1.3799 |
| `union_coil_green_h3` | 164 | 132 | -0.8522 | -0.8522 | -0.8522 | 0.0 | -3.6474 |
| `union_coil_off_h1` | 175 | 159 | -0.6819 | -0.6819 | -0.6819 | 0.0 | -0.5817 |
| `union_coil_off_h3` | 175 | 143 | -0.9349 | -0.9349 | -0.9349 | 0.0 | -2.2437 |
| `union_coil_off_h5` | 175 | 127 | -1.6313 | -1.6313 | -1.6313 | 0.0 | None |
| `union_cond_h1` | 184 | 168 | 0.2093 | 0.2093 | 0.2093 | 0.0 | 1.4801 |
| `union_cond_h3` | 184 | 152 | -0.6292 | -0.6292 | -0.6292 | 0.0 | -0.6095 |
| `union_cond_n4_h3` | 92 | 76 | -0.0435 | -0.0435 | -0.0435 | 0.0 | 1.1516 |
| `union_e_fresh_h1` | 118 | 114 | 0.0144 | 0.0144 | 0.0144 | 0.0 | 1.3506 |
| `union_e_fresh_h3` | 118 | 102 | -0.4564 | -0.4564 | -0.4564 | 0.0 | 1.3027 |
| `union_e_green_h1` | 63 | 62 | -1.4455 | -1.4455 | -1.4455 | 0.0 | 2.1023 |
| `union_e_green_h3` | 63 | 60 | -3.0653 | -3.0653 | -3.0653 | 0.0 | 1.5867 |
| `union_earn_react_h1` | 111 | 107 | -0.4988 | -0.4988 | -0.4988 | 0.0 | 1.7295 |
| `union_earn_react_h3` | 111 | 97 | -1.6357 | -1.6357 | -1.6357 | 0.0 | 1.7044 |
| `union_flow_in_h1` | 49 | 47 | 0.5008 | 0.5008 | 0.5008 | 0.0 | 0.6345 |
| `union_flow_in_h3` | 49 | 47 | -0.6458 | -0.6458 | -0.6458 | 0.0 | 2.7327 |
| `union_flow_in_h5` | 49 | 40 | -1.9175 | -1.9175 | -1.9175 | 0.0 | None |
| `union_flow_in_white_h1` | 10 | 10 | 1.1849 | 1.1849 | 1.1849 | 0.0 | None |
| `union_flow_in_white_h3` | 10 | 10 | -0.8885 | -0.8885 | -0.8885 | 0.0 | None |
| `union_h1` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_cut` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_half` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_rankw` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_sboost` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_sizeup` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_time` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_topheavy` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h1_trail` | 184 | 168 | 0.3084 | 0.3084 | 0.3084 | 0.0 | -1.326 |
| `union_h3` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_cut` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_exit_alarm` | 184 | 152 | 0.7727 | 0.7727 | 0.7727 | 0.0 | -3.2332 |
| `union_h3_exit_news_r` | 184 | 152 | 0.609 | 0.609 | 0.609 | 0.0 | -3.933 |
| `union_h3_exit_red` | 181 | 149 | 1.2587 | 1.2587 | 1.2587 | 0.0 | -1.2418 |
| `union_h3_half` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_rankw` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_sboost` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_sizeup` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_time` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_topheavy` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h3_trail` | 184 | 152 | 0.9265 | 0.9265 | 0.9265 | 0.0 | -3.3031 |
| `union_h5` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_cut` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_exit_alarm` | 184 | 136 | 1.6366 | 1.6366 | 1.6366 | 0.0 | None |
| `union_h5_half` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_rankw` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_sboost` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_sizeup` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_time` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_topheavy` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_h5_trail` | 184 | 136 | 1.8951 | 1.8951 | 1.8951 | 0.0 | None |
| `union_hot_n12_h1` | 273 | 249 | 0.6312 | 0.6312 | 0.6312 | 0.0 | 0.2951 |
| `union_hot_n4_h1` | 92 | 84 | 1.6228 | 1.6228 | 1.6228 | 0.0 | 1.3972 |
| `union_hot_score_h1` | 184 | 168 | 0.8834 | 0.8834 | 0.8834 | 0.0 | 0.5261 |
| `union_hot_score_h3` | 184 | 152 | 7.9628 | 7.9628 | 7.9628 | 0.0 | -1.7937 |
| `union_join_g_h1` | 179 | 163 | 0.23 | 0.23 | 0.23 | 0.0 | -0.5255 |
| `union_join_g_h3` | 179 | 147 | -0.0261 | -0.0261 | -0.0261 | 0.0 | -2.6858 |
| `union_join_present_h1` | 184 | 168 | 0.2976 | 0.2976 | 0.2976 | 0.0 | -0.6626 |
| `union_join_present_h3` | 184 | 152 | 0.7727 | 0.7727 | 0.7727 | 0.0 | -3.2332 |
| `union_join_vol_green_h1` | 84 | 74 | 0.3108 | 0.3108 | 0.3108 | 0.0 | -1.2644 |
| `union_join_vol_green_h3` | 84 | 74 | -0.3811 | -0.3811 | -0.3811 | 0.0 | -6.6995 |
| `union_last_green_h1` | 181 | 165 | 0.5862 | 0.5862 | 0.5862 | 0.0 | 1.0268 |
| `union_last_green_h3` | 181 | 149 | 1.2587 | 1.2587 | 1.2587 | 0.0 | -1.2418 |
| `union_last_green_h5` | 181 | 133 | 1.2378 | 1.2378 | 1.2378 | 0.0 | None |
| `union_last_red_h1` | 178 | 162 | 0.7874 | 0.7874 | 0.7874 | 0.0 | 0.3295 |
| `union_last_red_h3` | 178 | 146 | 0.7225 | 0.7225 | 0.7225 | 0.0 | -2.119 |
| `union_macd_hist_h1` | 168 | 152 | -0.2469 | -0.2469 | -0.2469 | 0.0 | -1.2179 |
| `union_macd_hist_h3` | 168 | 152 | -1.6819 | -1.6819 | -1.6819 | 0.0 | -4.381 |
| `union_macd_up_h1` | 165 | 149 | 0.5377 | 0.5377 | 0.5377 | 0.0 | -0.4844 |
| `union_macd_up_h3` | 165 | 149 | 0.7291 | 0.7291 | 0.7291 | 0.0 | -0.2427 |
| `union_macd_xup_h1` | 76 | 73 | -1.0816 | -1.0816 | -1.0816 | 0.0 | -1.8315 |
| `union_macd_xup_h3` | 76 | 73 | -1.9116 | -1.9116 | -1.9116 | 0.0 | -4.2924 |
| `union_news_g_cam61_h1` | 40 | 40 | -0.3555 | -0.3555 | -0.3555 | 0.0 | None |
| `union_news_g_cam61_h3` | 40 | 40 | -0.8309 | -0.8309 | -0.8309 | 0.0 | None |
| `union_news_g_cam71_conv_h1` | 23 | 23 | 0.0132 | 0.0132 | 0.0132 | 0.0 | None |
| `union_news_g_cam71_h1` | 26 | 26 | 0.0803 | 0.0803 | 0.0803 | 0.0 | None |
| `union_news_g_cam71_h3` | 26 | 26 | -0.8668 | -0.8668 | -0.8668 | 0.0 | None |
| `union_news_g_cam71_n2_h1` | 16 | 16 | -0.0052 | -0.0052 | -0.0052 | 0.0 | None |
| `union_news_g_cond_h1` | 125 | 121 | 0.6823 | 0.6823 | 0.6823 | 0.0 | -1.6627 |
| `union_news_g_cond_h3` | 125 | 121 | 7.6435 | 7.6435 | 7.6435 | 0.0 | -5.1479 |
| `union_news_g_conv_h1` | 75 | 71 | 0.6537 | 0.6537 | 0.6537 | 0.0 | -2.1053 |
| `union_news_g_conv_h3` | 75 | 71 | 12.4343 | 12.4343 | 12.4343 | 0.0 | -5.5126 |
| `union_news_g_h1` | 131 | 127 | 0.6595 | 0.6595 | 0.6595 | 0.0 | -1.6796 |
| `union_news_g_h3` | 131 | 121 | 7.585 | 7.585 | 7.585 | 0.0 | -5.1479 |
| `union_news_g_h5` | 131 | 112 | 7.7498 | 7.7498 | 7.7498 | 0.0 | None |
| `union_news_head_h1` | 96 | 94 | 0.3873 | 0.3873 | 0.3873 | 0.0 | -2.1536 |
| `union_news_head_h3` | 96 | 94 | 9.7751 | 9.7751 | 9.7751 | 0.0 | -4.9054 |
| `union_news_missing_h1` | 16 | 16 | 1.0886 | 1.0886 | 1.0886 | 0.0 | None |
| `union_news_missing_h3` | 16 | 16 | 2.4962 | 2.4962 | 2.4962 | 0.0 | None |
| `union_news_or_h1` | 125 | 121 | 0.7175 | 0.7175 | 0.7175 | 0.0 | -1.6627 |
| `union_news_or_h3` | 125 | 121 | 7.6839 | 7.6839 | 7.6839 | 0.0 | -5.1479 |
| `union_news_or_net2_h1` | 94 | 91 | 0.1584 | 0.1584 | 0.1584 | 0.0 | -1.155 |
| `union_news_or_net2_h3` | 94 | 91 | -0.5245 | -0.5245 | -0.5245 | 0.0 | -4.4667 |
| `union_news_or_net3_h1` | 79 | 76 | 0.123 | 0.123 | 0.123 | 0.0 | -0.3977 |
| `union_news_or_net3_h3` | 79 | 76 | -0.4708 | -0.4708 | -0.4708 | 0.0 | -4.428 |
| `union_news_or_net4_conv_h1` | 53 | 51 | 0.4793 | 0.4793 | 0.4793 | 0.0 | -0.3977 |
| `union_news_or_net4_h1` | 71 | 69 | 0.1356 | 0.1356 | 0.1356 | 0.0 | -0.3977 |
| `union_news_or_net4_h3` | 71 | 69 | 0.0807 | 0.0807 | 0.0807 | 0.0 | -4.428 |
| `union_news_or_net4_rw_h1` | 53 | 51 | 0.4793 | 0.4793 | 0.4793 | 0.0 | -0.3977 |
| `union_news_or_net5_h1` | 51 | 50 | -0.0079 | -0.0079 | -0.0079 | 0.0 | None |
| `union_news_or_net5_h3` | 51 | 50 | -0.5769 | -0.5769 | -0.5769 | 0.0 | None |
| `union_news_pack_h1` | 50 | 48 | 1.041 | 1.041 | 1.041 | 0.0 | 0.2364 |
| `union_news_pack_h3` | 50 | 48 | -0.568 | -0.568 | -0.568 | 0.0 | -4.6142 |
| `union_news_pack_net2_h1` | 44 | 42 | 1.1294 | 1.1294 | 1.1294 | 0.0 | 0.7543 |
| `union_news_pack_net2_h3` | 44 | 42 | -0.5003 | -0.5003 | -0.5003 | 0.0 | -3.6296 |
| `union_news_pack_net3_h1` | 41 | 39 | 1.1879 | 1.1879 | 1.1879 | 0.0 | 0.7543 |
| `union_news_pack_net3_h3` | 41 | 39 | -0.8667 | -0.8667 | -0.8667 | 0.0 | -3.6296 |
| `union_news_present_h1` | 174 | 158 | 0.2 | 0.2 | 0.2 | 0.0 | -0.6626 |
| `union_news_present_h3` | 174 | 142 | 0.4171 | 0.4171 | 0.4171 | 0.0 | -3.2332 |
| `union_news_vol_h1` | 67 | 64 | 0.355 | 0.355 | 0.355 | 0.0 | -1.8476 |
| `union_news_vol_h3` | 67 | 64 | 13.9453 | 13.9453 | 13.9453 | 0.0 | -5.2249 |
| `union_ret_5_h1` | 184 | 168 | 0.6104 | 0.6104 | 0.6104 | 0.0 | 0.8626 |
| `union_ret_5_h3` | 184 | 152 | 1.7686 | 1.7686 | 1.7686 | 0.0 | -1.5177 |
| `union_rsi_h1` | 168 | 152 | -0.6104 | -0.6104 | -0.6104 | 0.0 | -1.7299 |
| `union_rsi_h3` | 168 | 152 | -0.7085 | -0.7085 | -0.7085 | 0.0 | -5.4987 |
| `union_rsi_os_h1` | 31 | 31 | 0.0735 | 0.0735 | 0.0735 | 0.0 | -5.5306 |
| `union_rsi_os_h3` | 31 | 31 | -0.935 | -0.935 | -0.935 | 0.0 | -8.0393 |
| `union_rsi_os_h5` | 31 | 29 | -3.1888 | -3.1888 | -3.1888 | 0.0 | None |
| `union_vol_ab_h1` | 94 | 78 | 0.5923 | 0.5923 | 0.5923 | 0.0 | -0.7023 |
| `union_vol_ab_h3` | 94 | 76 | -0.0695 | -0.0695 | -0.0695 | 0.0 | -6.5234 |
| `union_vol_g_h1` | 135 | 119 | 0.4851 | 0.4851 | 0.4851 | 0.0 | -0.8162 |
| `union_vol_g_h3` | 135 | 113 | 0.5015 | 0.5015 | 0.5015 | 0.0 | -4.219 |
| `union_vol_g_h5` | 135 | 102 | 0.1003 | 0.1003 | 0.1003 | 0.0 | None |
| `union_vol_green_h1` | 120 | 105 | -0.424 | -0.424 | -0.424 | 0.0 | -1.2514 |
| `union_vol_green_h3` | 120 | 103 | -0.3501 | -0.3501 | -0.3501 | 0.0 | -7.4636 |
| `union_vol_missing_h1` | 8 | 8 | 1.8816 | 1.8816 | 1.8816 | 0.0 | None |
| `union_vol_missing_h3` | 8 | 8 | 5.59 | 5.59 | 5.59 | 0.0 | None |
| `union_w_hot_candle_h1` | 184 | 168 | 0.9412 | 0.9412 | 0.9412 | 0.0 | 0.5705 |
| `union_w_hot_candle_h3` | 184 | 152 | 7.28 | 7.28 | 7.28 | 0.0 | -1.1986 |
| `union_w_hot_cond_h1` | 184 | 168 | 0.5319 | 0.5319 | 0.5319 | 0.0 | 1.2416 |
| `union_w_hot_cond_h3` | 184 | 152 | 7.2288 | 7.2288 | 7.2288 | 0.0 | 0.5489 |
| `union_white_any_h1` | 68 | 68 | 0.2679 | 0.2679 | 0.2679 | 0.0 | None |
| `union_white_any_h2` | 68 | 68 | 0.778 | 0.778 | 0.778 | 0.0 | None |
| `union_white_any_h3` | 68 | 68 | 0.3245 | 0.3245 | 0.3245 | 0.0 | None |
| `union_white_any_h5` | 68 | 68 | -0.4084 | -0.4084 | -0.4084 | 0.0 | None |
| `union_white_coil_h1` | 64 | 64 | -0.4088 | -0.4088 | -0.4088 | 0.0 | None |
| `union_white_coil_h3` | 64 | 64 | -0.988 | -0.988 | -0.988 | 0.0 | None |
| `union_white_h1` | 72 | 72 | -0.3031 | -0.3031 | -0.3031 | 0.0 | None |
| `union_white_h3` | 72 | 72 | -0.3321 | -0.3321 | -0.3321 | 0.0 | None |
| `union_white_h5` | 72 | 72 | -0.3361 | -0.3361 | -0.3361 | 0.0 | None |
| `union_white_yday_h1` | 66 | 66 | 0.7019 | 0.7019 | 0.7019 | 0.0 | None |
| `union_white_yday_h3` | 66 | 66 | -0.1019 | -0.1019 | -0.1019 | 0.0 | None |
| `yday_gainer_h1` | 168 | 152 | 0.4994 | 0.4994 | 0.4994 | 0.0 | -1.2291 |
| `yday_gainer_h3` | 168 | 136 | 1.4388 | 1.4388 | 1.4388 | 0.0 | -4.9164 |
| `yday_gainer_h5` | 168 | 120 | 1.4898 | 1.4898 | 1.4898 | 0.0 | None |

## Blocked blotter (v2 unique)

| Date | Ticker | Side | RSI | 1d | H% | Lesson |
|---|---|---|---:|---:|---:|---|
| 2026-08-14 | AIRO | short | 77.46 | 26.14841374414345 | 13.9389 | short_vs_green_cameras |
| 2026-08-14 | AMPY | short | 72.15 | 10.02278065153337 | 3.2389 | short_vs_green_cameras |
| 2026-08-14 | ANGX | short | 56.24 | 17.391306882995238 | -1.3921 | short_vs_green_cameras |
| 2026-08-14 | DAVE | short | 38.69 | 5.559943842792259 | -1.106 | short_vs_green_cameras |
| 2026-08-14 | HLIT | short | 56.8 | 8.999999364217114 | -5.6146 | short_vs_green_cameras |
| 2026-08-14 | MH | short | 74.51 | 16.982752142848568 | 3.321 | short_vs_green_cameras |
| 2026-08-14 | QMLS | short | 22.22 | 14.944357121164998 | -0.4115 | short_vs_green_cameras |
| 2026-08-14 | TBBB | short | 76.37 | 16.31206063907813 | 2.1098 | short_vs_green_cameras |
| 2026-08-17 | AEHR | short | 68.23 | 8.726678216154781 | -9.6543 | short_vs_green_cameras |
| 2026-08-17 | EOG | short | 53.22 | 0.8485941002515762 | -2.3674 | short_vs_green_cameras |
| 2026-08-17 | KLC | short | 19.93 | -46.16977338091569 | 2.2901 | oversold_crash_pause |
| 2026-08-17 | LPTH | short | 64.57 | 15.625001590062947 | 0.9371 | short_vs_green_cameras |
| 2026-08-17 | UMAC | short | 72.49 | 25.036716811590853 | 7.3733 | short_vs_green_cameras |
| 2026-08-18 | INV | short | 24.02 | -14.241487165034815 | -0.0 | oversold_crash_pause |
| 2026-08-19 | BIDU | short | 23.05 | -12.72570077840779 | -1.2097 | oversold_crash_pause |
| 2026-08-20 | AEG | short | 54.8 | -1.381510271508346 | -0.0 | short_vs_green_cameras |
| 2026-08-20 | AEM | short | 78.79 | 11.115865255115786 | -3.7124 | short_vs_green_cameras |
| 2026-08-20 | ASST | short | 63.92 | 15.078119708923698 | -0.8125 | short_vs_green_cameras |
| 2026-08-20 | BHP | short | 64.95 | 3.591878672326021 | -2.8788 | short_vs_green_cameras |
| 2026-08-20 | CRCL | short | 63.15 | -5.014591293482007 | -0.8073 | short_vs_green_cameras |
| 2026-08-20 | CYPH | short | 73.5 | 32.919259993162875 | -3.4783 | short_vs_green_cameras |
| 2026-08-20 | EL | short | 72.51 | 16.304741949096258 | 1.3138 | short_vs_green_cameras |
| 2026-08-20 | HDSN | short | 49.85 | 3.900714046752185 | 3.4662 | short_vs_green_cameras |
| 2026-08-20 | KGC | short | 71.39 | 10.250731178964024 | -6.0749 | short_vs_green_cameras |
| 2026-08-20 | MRNA | short | 89.65 | 176.96951623021624 | 11.2029 | short_vs_green_cameras |
| 2026-08-20 | MRVI | short | 68.11 | -1.3840899245549543 | -11.4247 | short_vs_green_cameras |
| 2026-08-20 | PPC | short | 69.43 | 9.792913085087896 | -1.925 | short_vs_green_cameras |
| 2026-08-20 | SCZM | short | 70.0 | 13.978495726297059 | -3.1712 | short_vs_green_cameras |
| 2026-08-20 | WPM | short | 73.56 | 11.100221482157302 | -3.9505 | short_vs_green_cameras |
| 2026-08-21 | AEM | short | 80.54 | 2.0797188715783177 | 0.111 | short_vs_green_cameras |
| 2026-08-21 | ARCT | short | 85.11 | 6.906615336474964 | -20.8446 | short_vs_green_cameras |
| 2026-08-21 | AU | short | 77.13 | 6.748227933849837 | -1.4988 | short_vs_green_cameras |
| 2026-08-21 | AUGO | short | 74.25 | 1.98823816636029 | 2.0651 | short_vs_green_cameras |
| 2026-08-21 | AUTL | short | 67.92 | -3.149603342816265 | 2.4291 | short_vs_green_cameras |
| 2026-08-21 | BEKE | short | 52.65 | -2.243955169622891 | 1.0315 | short_vs_green_cameras |
| 2026-08-21 | CRDL | short | 84.55 | -3.826532970612717 | 3.6269 | short_vs_green_cameras |
| 2026-08-21 | CRSP | short | 67.62 | -2.8256184628026815 | 0.3684 | short_vs_green_cameras |
| 2026-08-21 | CYPH | short | 79.78 | 11.214953166906039 | -7.5758 | short_vs_green_cameras |
| 2026-08-21 | DE | short | 58.94 | 6.942458574239452 | -3.8844 | short_vs_green_cameras |
| 2026-08-21 | DFDV | short | 73.98 | 10.233915111833646 | 2.4752 | short_vs_green_cameras |
| 2026-08-21 | EYPT | short | 30.3 | -14.285715366977648 | 2.9197 | short_vs_green_cameras |
| 2026-08-21 | FUTU | short | 59.58 | 3.0250459179112843 | -7.345 | short_vs_green_cameras |
| 2026-08-21 | GMAB | short | 75.22 | -2.0521863102137217 | -0.2698 | short_vs_green_cameras |
| 2026-08-21 | INDP | short | 39.82 | 46.80851050338468 | 7.1942 | short_vs_green_cameras |
| 2026-08-21 | MRVI | short | 77.17 | 45.438600688686726 | -4.3478 | short_vs_green_cameras |
| 2026-08-21 | PRQR | short | 75.03 | 2.2522500753112684 | -2.6316 | short_vs_green_cameras |
| 2026-08-24 | AEM | short | 81.99 | 1.8958707789790585 | -0.3963 | short_vs_green_cameras |
| 2026-08-24 | BJ | short | 52.79 | 5.607880554270062 | -1.5152 | short_vs_green_cameras |
| 2026-08-24 | ERO | short | 71.0 | 9.439193969940153 | 2.5038 | short_vs_green_cameras |
| 2026-08-25 | ALVO | short | 78.52 | 18.510162605782774 | 3.6259 | short_vs_green_cameras |
| 2026-08-25 | BMEA | short | 80.44 | 16.0583961930296 | -6.135 | short_vs_green_cameras |
| 2026-08-25 | CYPH | short | 88.11 | 18.309859036689847 | -5.1282 | short_vs_green_cameras |
| 2026-08-25 | DEFT | short | 69.6 | 7.826090021259691 | 2.5807 | short_vs_green_cameras |
| 2026-08-25 | GORO | short | 63.99 | 10.658304328231383 | -9.0141 | short_vs_green_cameras |
| 2026-08-25 | INSP | short | 64.24 | -1.4271959283796098 | 0.1961 | short_vs_green_cameras |
| 2026-08-25 | MOS | short | 59.77 | -1.6796388774069415 | -2.1035 | short_vs_green_cameras |
| 2026-08-25 | OCUL | short | 59.29 | -2.7075829675143104 | 0.9107 | short_vs_green_cameras |
| 2026-08-25 | SSRM | short | 73.04 | 2.2239876685392623 | -3.8675 | short_vs_green_cameras |
| 2026-08-26 | B | short | 72.04 | 1.4742505953533769 | 2.4491 | short_vs_green_cameras |
| 2026-08-26 | BRR | short | 67.57 | 4.651158148692014 | 1.3636 | short_vs_green_cameras |
| 2026-08-26 | BTG | short | 75.39 | 2.6223793858287214 | 0.1739 | short_vs_green_cameras |
| 2026-08-26 | BZ | short | 56.23 | 5.5051901440721585 | -12.3435 | short_vs_green_cameras |
| 2026-08-26 | FIGR | short | 69.97 | 6.469979398265058 | 8.4444 | short_vs_green_cameras |
| 2026-08-26 | FUTU | short | 62.39 | 8.479405838736698 | -2.1417 | short_vs_green_cameras |
| 2026-08-26 | NEM | short | 73.88 | 2.503036364852651 | 0.7841 | short_vs_green_cameras |
| 2026-08-26 | USDE | short | 62.39 | -3.3492829152439785 | -2.926 | short_vs_green_cameras |
| 2026-08-26 | XHG | short | 75.24 | -1.9512176967531936 | -6.5617 | short_vs_green_cameras |
| 2026-08-27 | ACMR | short | 46.08 | -0.8273737752152788 | 1.4207 | short_vs_green_cameras |
| 2026-08-27 | ANET | short | 59.81 | 5.923325345124808 | 2.3361 | short_vs_green_cameras |
| 2026-08-27 | ASML | short | 48.91 | 0.08485347902413842 | 0.6596 | short_vs_green_cameras |
| 2026-08-27 | DLO | short | 59.79 | -1.0946560155633778 | 1.2394 | short_vs_green_cameras |
| 2026-08-27 | GGB | short | 42.11 | 2.293575724089769 | -2.8446 | short_vs_green_cameras |
| 2026-08-27 | LRCX | short | 48.23 | -0.5656895565310793 | 0.0941 | short_vs_green_cameras |
| 2026-08-27 | MOS | short | 60.06 | -0.4532369521088153 | 1.0 | short_vs_green_cameras |
| 2026-08-27 | MT | short | 59.65 | 1.2360717179640268 | -0.1207 | short_vs_green_cameras |
| 2026-08-27 | NUE | short | 48.32 | 2.1207836539199443 | -0.1468 | short_vs_green_cameras |
| 2026-08-27 | NVDA | short | 45.88 | -1.5911754710582593 | -2.2974 | short_vs_green_cameras |
| 2026-08-27 | PLTR | short | 64.19 | 2.7615378859761286 | -4.0168 | short_vs_green_cameras |
| 2026-08-27 | TX | short | 66.98 | -0.1268110395955202 | -1.0498 | short_vs_green_cameras |
| 2026-08-28 | ANF | short | 79.06 | -1.3536379018612488 | -1.6088 | short_vs_green_cameras |
| 2026-08-28 | CXM | short | 72.4 | 6.291837754077778 | -3.5533 | short_vs_green_cameras |
| 2026-08-28 | GRRR | short | 54.9 | 1.359223564656875 | 7.9821 | short_vs_green_cameras |
| 2026-08-28 | PATH | short | 71.57 | 9.367539817324722 | -0.1379 | short_vs_green_cameras |
| 2026-08-28 | SEDG | short | 42.46 | 1.2998683998706628 | 4.5289 | short_vs_green_cameras |
| 2026-08-28 | URBN | short | 65.7 | -5.015064720629869 | -2.1027 | short_vs_green_cameras |
| 2026-08-28 | VYX | short | 61.0 | 3.3783804827546504 | 3.8335 | short_vs_green_cameras |
| 2026-08-31 | CRM | short | 79.97 | 1.567148145374575 | -1.2383 | short_vs_green_cameras |
| 2026-08-31 | NOV | short | 55.46 | 1.1078975900797206 | -0.0 | short_vs_green_cameras |
| 2026-08-31 | SNPS | short | 71.74 | -4.792537717490831 | -0.3745 | short_vs_green_cameras |
| 2026-09-01 | OKE | short | 60.37 | 1.3191219626641049 | 2.0573 | short_vs_green_cameras |
| 2026-09-01 | TRGP | short | 72.7 | 2.025566820898028 | 1.142 | short_vs_green_cameras |
| 2026-09-02 | GPRO | short | 87.6 | 40.410963145073154 | -38.5246 | short_vs_green_cameras |
| 2026-09-02 | PBH | short | 51.97 | 1.6608741418036033 | 0.0946 | short_vs_green_cameras |
| 2026-09-02 | PRQR | short | 63.68 | -0.4694942884761688 | -0.0 | short_vs_green_cameras |
| 2026-09-02 | VSTM | short | 61.92 | 5.401666961993112 | -4.1559 | short_vs_green_cameras |
| 2026-09-03 | AIIO | short | 24.86 | -17.0731664767653 | -8.9386 | oversold_crash_pause |
| 2026-09-03 | ARCT | short | 81.15 | 3.1423305192007644 | 7.2153 | short_vs_green_cameras |
| 2026-09-03 | ATRC | short | 73.82 | 7.502048489389179 | 0.7943 | short_vs_green_cameras |
| 2026-09-03 | CLYM | short | 49.77 | 1.551960193298596 | -4.5129 | short_vs_green_cameras |
| 2026-09-03 | CNH | short | 68.14 | 9.199996948242184 | -0.9482 | short_vs_green_cameras |
| 2026-09-03 | CRDL | short | 73.15 | 0.9302316297383983 | 0.9174 | short_vs_green_cameras |
| 2026-09-03 | CRK | short | 70.4 | -2.996257691603399 | 3.2362 | short_vs_green_cameras |
| 2026-09-03 | KLRA | short | 42.15 | -3.412550098412681 | 1.3166 | short_vs_green_cameras |
| 2026-09-03 | MMED | short | 71.82 | 5.9768070830863 | 0.1675 | short_vs_green_cameras |
| 2026-09-03 | MRNA | short | 67.47 | -2.2428253179771485 | -2.0042 | short_vs_green_cameras |
| 2026-09-03 | NVAX | short | 71.68 | 1.976282722596956 | 0.7678 | short_vs_green_cameras |
| 2026-09-04 | ALEC | short | 69.5 | -7.720589556547585 | 2.381 | short_vs_green_cameras |
| 2026-09-04 | ATRC | short | 80.84 | -0.24719731458079375 | 0.9802 | short_vs_green_cameras |
| 2026-09-04 | BAK | short | 48.26 | -1.5228411651818319 | 2.5773 | short_vs_green_cameras |
| 2026-09-04 | BHC | short | 62.19 | -1.024885733618519 | 2.2355 | short_vs_green_cameras |
| 2026-09-04 | BMEA | short | 80.51 | -1.2919884701511064 | -6.8421 | short_vs_green_cameras |
| 2026-09-04 | BRR | short | 64.36 | 13.85282162718724 | -5.9761 | short_vs_green_cameras |
| 2026-09-04 | CABA | short | 68.96 | -2.521006046281926 | -0.289 | short_vs_green_cameras |
| 2026-09-04 | CRDO | short | 31.78 | -0.6355181237138519 | -5.2252 | short_vs_green_cameras |
| 2026-09-04 | CRM | short | 80.05 | 2.9190831019343833 | 1.5682 | short_vs_green_cameras |
| 2026-09-04 | FMC | short | 72.95 | -2.6256592908698084 | -0.1544 | short_vs_green_cameras |
| 2026-09-04 | GPRO | short | 87.82 | -17.751482921188977 | -14.8649 | short_vs_green_cameras |
| 2026-09-04 | HRMY | short | 70.68 | -2.3331777549216337 | -1.8072 | short_vs_green_cameras |
| 2026-09-04 | IRD | short | 65.36 | 3.370788804420255 | -3.0905 | short_vs_green_cameras |
| 2026-09-04 | LENZ | short | 64.82 | 0.5172367583558657 | -3.6522 | short_vs_green_cameras |
| 2026-09-04 | OABI | short | 79.62 | -5.753967540634752 | 9.4142 | short_vs_green_cameras |
| 2026-09-04 | SLBT | short | 59.48 | -4.833832331663157 | 8.5714 | short_vs_green_cameras |
| 2026-09-04 | TARS | short | 78.47 | 0.5376989890499395 | -9.7703 | short_vs_green_cameras |
| 2026-09-04 | VIR | short | 69.31 | -0.4347842672596802 | -0.6631 | short_vs_green_cameras |
| 2026-09-08 | CYPH | short | 79.66 | 6.607933772149566 | -8.2305 | short_vs_green_cameras |
| 2026-09-08 | HOOD | short | 68.93 | -2.092688089164574 | 6.1805 | short_vs_green_cameras |
| 2026-09-08 | JCI | short | 45.84 | 1.9053693280414397 | -0.5979 | short_vs_green_cameras |
| 2026-09-08 | VNT | short | 53.8 | 0.8479672180956577 | 2.0451 | short_vs_green_cameras |
| 2026-09-09 | INTC | short | 54.55 | 9.050102184507413 | -3.4469 | short_vs_green_cameras |
| 2026-09-09 | UGP | short | 72.58 | 2.335159265898734 | 0.4021 | short_vs_green_cameras |
| 2026-09-11 | RWT | short | 18.36 | -14.788734286201677 | -0.8523 | oversold_crash_pause |
| 2026-09-14 | BG | short | 59.43 | -1.7497333171670237 | None | short_vs_green_cameras |
| 2026-09-14 | CVE | short | 61.77 | 2.533208902940398 | None | short_vs_green_cameras |
| 2026-09-14 | HPQ | short | 71.11 | 8.402077722155997 | None | short_vs_green_cameras |
| 2026-09-15 | NTSK | short | 66.05 | -2.649010345029257 | None | short_vs_green_cameras |
| 2026-09-15 | SAIL | short | 57.95 | -2.2675716120772793 | None | short_vs_green_cameras |

## Shipped vs dropped

- **shipped:** ['oversold_crash_pause', 'short_vs_green_cameras']
- dropped `C1 short_vs_own_recipe`: OOS blocked meanH 0.6203 > 0
- dropped `C2 short_vs_green_cameras@net2`: OOS blocked meanH 0.5478 > 0
- dropped `C3 long_vs_hard_red_news`: IS blocked meanH 0.5985 > 0
- dropped `C4 short_vs_sector_or_tape`: OOS EW -1.3009 < F1 -1.3005
- dropped `F3 hard_red_rsi30_short`: OOS blocked meanH 1.4374 > 0
- dropped `F2t overbought_meltup_pause`: IS blocked meanH 2.3678 > 0
- dropped `C2b short_vs_green_cameras_and`: OOS blocked meanH 0.5478 > 0
- dropped ` short_vs_green_cameras_net4_and`: duplicate sleeve of shipped short_vs_green_cameras
- Layer 3 mute **off**: mute-extra IS meanH 1.0158 > 0 (n=31539; mostly family-wide longs) — drop Layer 3
  extra IS meanH=1.0158 n=31539; OOS n_in_avg mute=48 vs v2=154.

## Coverage holes

- dated strategy_tickets missing 2026-08-13→2026-09-09 — reconstructed from panel pick_day + stock-book files
- rsi missing 29/2764; ret_1 missing 18/2764; news missing 961/2764; camera_net missing 961/2764; sector missing 961/2764
- hold-horizon unmarked 485/2764 (exit not in archive)
- macro / tape_anchor / channel1 not attached on ticket rows — unused in v1
- data/prices/ohlc.parquet asof 2026-09-11 — 09-14/09-15 official opens/closes are missing, so RWT hold>1 and OOS 5d marks are unmarked. Do not invent the 3.83/3.94 bounce.
- 09-12/09-13 are weekend — not sessions

These are archive marks, not a 60% claim. North star ~2%/day after fees.

