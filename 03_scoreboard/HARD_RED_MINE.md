# Hard-red strategy mine

Research only. Same leak-free 09:30 catalog that produced the factor-mine combo sleeves. Live `flatten_robust` and Webull `combo_sh_macd_5050_shared` stay on full hard-red sit.

Window `2026-08-13 → 2026-09-15` (23 sessions). Hard-red mornings (S≤-3): **11** — `2026-08-18` S=-6.2, `2026-08-19` S=-7.2, `2026-08-24` S=-5.175, `2026-08-31` S=-5.85, `2026-09-01` S=-6.3, `2026-09-02` S=-3.825, `2026-09-08` S=-11.475, `2026-09-09` S=-13.95, `2026-09-10` S=-13.275, `2026-09-14` S=-11.002, `2026-09-15` S=-3.836.

Calendar time-split: cutoff `2026-09-04` (hold_frac=0.3). Discovery fires need the entry **and** the horizon close strictly before cutoff. Holdout = entry on/after cutoff. Missing open/exit fail closed. Yahoo regular-session overlay filled `1212` holes on ['2026-09-14', '2026-09-15'] (research only — parquet was not written).

## What this is grading

Each recipe's 09:30 list on a hard-red morning, filled at the official open, exited at the hold-th session close (1 / 2 / 3 / 5). 1 share, Futubull round-trip. Close grades — it does not trigger. A fat all-day Book% that sits every red morning is **not** a hard-red edge.

**Published KEEP** (same as HARD_RED_SIT): ≥30 fires and >55% after fees. Thin n is KILL. This board does not wire a live policy change.

**Research survivor** (reporting only): discovery and holdout each ≥8 graded fires and >55% after fees.

Recipes scored: **233**. Combo specs: **78**. Name-day rows: **2644** (includes polarity flips).

## Why the lists lose

Morning S is already known at 09:30. The gap has printed. These books grade the **residual** (official open → horizon close), not the overnight. That residual is not one-way: some hard-red mornings bounce after the open, some keep dumping. The 09:30 lists are yesterday-hot / camera-green names — buying yesterday's winners into a red weather print. Fees eat the small moves: when the open→close is inside the Futubull round-trip, **long and short both lose**.

| Window | Days | Median OC | Days up/down | Long win | Short win | Both lose (fees) |
|---|---:|---:|---:|---:|---:|---:|
| Hard-red | 11 | -0.22% | 5/6 | 24.2% | 27.3% | 48.5% |
| Discovery red | 6 | -0.09% | 3/3 | 23.3% | 23.2% | 53.5% |
| Holdout red | 5 | -0.22% | 2/3 | 25.2% | 32.2% | 42.6% |
| Other mornings | 12 | +0.06% | 7/5 | 26.8% | 27.6% | 45.6% |

Per hard-red morning (panel names, 1-share after fees, same-day close):

| Date | S | Median OC | % up | Long win | Short win | Both lose | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| `2026-08-18` | -6.2 | -0.80% | 43.4% | 22.2% | 35.4% | 42.4% | 99 |
| `2026-08-19` | -7.2 | +0.11% | 52.0% | 31.0% | 22.0% | 47.0% | 100 |
| `2026-08-24` | -5.175 | -1.03% | 31.9% | 5.8% | 40.6% | 53.6% | 69 |
| `2026-08-31` | -5.85 | +0.51% | 64.7% | 35.3% | 13.2% | 51.5% | 68 |
| `2026-09-01` | -6.3 | -0.28% | 44.7% | 17.6% | 18.8% | 63.5% | 85 |
| `2026-09-02` | -3.825 | +0.86% | 65.2% | 28.1% | 9.0% | 62.9% | 89 |
| `2026-09-08` | -11.475 | -1.09% | 35.1% | 14.3% | 32.5% | 53.2% | 77 |
| `2026-09-09` | -13.95 | -1.06% | 33.3% | 19.1% | 40.5% | 40.5% | 84 |
| `2026-09-10` | -13.275 | +0.10% | 51.6% | 29.7% | 31.2% | 39.1% | 64 |
| `2026-09-14` | -11.002 | -0.22% | 46.9% | 28.1% | 29.7% | 42.2% | 64 |
| `2026-09-15` | -3.836 | +0.57% | 54.5% | 34.8% | 27.3% | 37.9% | 66 |


## Holdout survivors

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_macd_hist_h1_fade_x1.5_h5` | short | 5 | 8/75.0% | 21/61.9% | 50/58.0% | +411.22 | YES | KEEP |
| `union_macd_hist_h3_fade_x1.5_h5` | short | 5 | 8/75.0% | 21/61.9% | 50/58.0% | +411.22 | YES | KEEP |

## Holdout leaders (hidden window, not a KEEP)

Best after-fee win% on hard-red entries on/after cutoff. A holdout tease that missed discovery is still KILL — we would not have picked it with the earlier tape only.

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `short_alarm_h1` | short | 5 | 24/50.0% | 32/65.6% | 76/48.7% | +415.74 | no | KILL |
| `short_alarm_h3` | short | 5 | 24/50.0% | 32/65.6% | 76/48.7% | +415.74 | no | KILL |
| `union_macd_xup_h1_flip` | short | 5 | 12/33.3% | 25/64.0% | 49/53.1% | +50.51 | no | KILL |
| `union_macd_xup_h3_flip` | short | 5 | 12/33.3% | 25/64.0% | 49/53.1% | +50.51 | no | KILL |
| `combo_sh_macd_5050_shared` | mix | baked | 33/27.3% | 25/56.0% | 59/39.0% | -11.52 | no | KILL |
| `union_macd_hist_h1_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_macd_hist_h3_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `ohlc_hot_h1_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `ohlc_hot_h3_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `ohlc_hot_h5_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `union_ret_5_h1_flip` | short | 3 | 40/32.5% | 40/52.5% | 88/38.6% | +11.61 | no | KILL |
| `union_ret_5_h3_flip` | short | 3 | 40/32.5% | 40/52.5% | 88/38.6% | +11.61 | no | KILL |

## Discovery leaders (not confirmed)

Best discovery win% at each recipe's best hold. Holdout is shown so a full-sample tease that dies hidden is visible.

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_news_pack_net3_h1_flip` | short | 5 | 2/100.0% | 6/50.0% | 16/56.2% | +144.62 | no | KILL |
| `union_news_pack_net3_h3_flip` | short | 5 | 2/100.0% | 6/50.0% | 16/56.2% | +144.62 | no | KILL |
| `flatten_vol_g_h3_flip` | short | 5 | 2/100.0% | 3/33.3% | 5/60.0% | -8.15 | no | KILL |
| `union_macd_hist_h1_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_macd_hist_h3_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_earn_react_h1_flip` | short | 5 | 16/75.0% | 21/28.6% | 45/55.6% | +125.25 | no | KEEP |
| `union_earn_react_h3_flip` | short | 5 | 16/75.0% | 21/28.6% | 45/55.6% | +125.25 | no | KEEP |
| `union_ab_g_h1_flip` | short | 5 | 8/75.0% | 40/32.5% | 72/37.5% | -25.69 | no | KILL |
| `union_ab_g_h3_flip` | short | 5 | 8/75.0% | 40/32.5% | 72/37.5% | -25.69 | no | KILL |
| `combo_jer_5050_shared_h5_flip` | mix | 5 | 24/70.8% | 48/35.4% | 82/51.2% | +89.66 | no | KILL |
| `union_e_fresh_h1_flip` | short | 3 | 19/68.4% | 22/31.8% | 47/51.1% | +75.87 | no | KILL |
| `union_e_fresh_h3_flip` | short | 3 | 19/68.4% | 22/31.8% | 47/51.1% | +75.87 | no | KILL |

## Polarity flips (fade the same 09:30 list)

Same names, opposite side, same fees and time-split. A long that lost 60% of the time is **not** automatically a 40% short — the fee dead-zone makes both sides lose on small moves. Flip survivors still need discovery **and** holdout >55% on ≥8 fires.

**Flip survivors: 0.** No polarity flip cleared both windows.

Flip discovery leaders (not confirmed):

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_news_pack_net3_h1_flip` | short | 5 | 2/100.0% | 6/50.0% | 16/56.2% | +144.62 | no | KILL |
| `union_news_pack_net3_h3_flip` | short | 5 | 2/100.0% | 6/50.0% | 16/56.2% | +144.62 | no | KILL |
| `flatten_vol_g_h3_flip` | short | 5 | 2/100.0% | 3/33.3% | 5/60.0% | -8.15 | no | KILL |
| `union_macd_hist_h1_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_macd_hist_h3_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_earn_react_h1_flip` | short | 5 | 16/75.0% | 21/28.6% | 45/55.6% | +125.25 | no | KEEP |
| `union_earn_react_h3_flip` | short | 5 | 16/75.0% | 21/28.6% | 45/55.6% | +125.25 | no | KEEP |
| `union_ab_g_h1_flip` | short | 5 | 8/75.0% | 40/32.5% | 72/37.5% | -25.69 | no | KILL |
| `union_ab_g_h3_flip` | short | 5 | 8/75.0% | 40/32.5% | 72/37.5% | -25.69 | no | KILL |
| `combo_jer_5050_shared_h5_flip` | mix | 5 | 24/70.8% | 48/35.4% | 82/51.2% | +89.66 | no | KILL |
| `union_e_fresh_h1_flip` | short | 3 | 19/68.4% | 22/31.8% | 47/51.1% | +75.87 | no | KILL |
| `union_e_fresh_h3_flip` | short | 3 | 19/68.4% | 22/31.8% | 47/51.1% | +75.87 | no | KILL |

Flip holdout leaders (hidden window, not a KEEP):

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_macd_xup_h1_flip` | short | 5 | 12/33.3% | 25/64.0% | 49/53.1% | +50.51 | no | KILL |
| `union_macd_xup_h3_flip` | short | 5 | 12/33.3% | 25/64.0% | 49/53.1% | +50.51 | no | KILL |
| `union_macd_hist_h1_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `union_macd_hist_h3_flip` | short | 5 | 24/87.5% | 38/52.6% | 86/58.1% | +595.70 | no | KEEP |
| `ohlc_hot_h1_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `ohlc_hot_h3_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `ohlc_hot_h5_flip` | short | 5 | 24/58.3% | 40/52.5% | 88/48.9% | +295.15 | no | KILL |
| `union_ret_5_h1_flip` | short | 3 | 40/32.5% | 40/52.5% | 88/38.6% | +11.61 | no | KILL |
| `union_ret_5_h3_flip` | short | 3 | 40/32.5% | 40/52.5% | 88/38.6% | +11.61 | no | KILL |
| `union_join_vol_green_h1_flip` | short | 3 | 10/20.0% | 29/51.7% | 39/43.6% | -52.77 | no | KILL |
| `union_join_vol_green_h3_flip` | short | 3 | 10/20.0% | 29/51.7% | 39/43.6% | -52.77 | no | KILL |
| `union_w_hot_candle_h1_flip` | short | 5 | 24/33.3% | 40/50.0% | 88/44.3% | -24.30 | no | KILL |

## After the open: dip-scoop and rally-fade

Assume we watch the official print in real time. **Scoop** = long the first touch of open−X% (session low as the daily first-hit proxy). **Fade** = short the first touch of open+X% (session high). Close / last never trigger; they only grade. Daily OHLC cannot prove the print was after 09:30, so these fills are slightly optimistic vs a live monitor. A name that never tags X% is a skip, not a fire.

Whole-panel same-day (every 09:30 name, not a recipe filter):

| X | Scoop n/win disc | Scoop n/win holdout | Fade n/win disc | Fade n/win holdout |
|---:|---:|---:|---:|---:|
| 0.5% | 418/18.9% | 308/20.1% | 457/23.6% | 298/28.5% |
| 1% | 360/19.7% | 279/21.1% | 393/25.7% | 258/27.1% |
| 1.5% | 310/20.6% | 249/20.1% | 339/25.7% | 213/25.8% |
| 2% | 265/21.5% | 207/21.3% | 284/25.4% | 183/24.6% |
| 3% | 200/25.5% | 149/14.1% | 207/22.7% | 139/23.7% |

**Intraday recipe survivors: 2.** Same KEEP / research bars. Recipe lists are the 09:30 names; the trigger is the limit after the open.

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_macd_hist_h1_fade_x1.5_h5` | short | 5 | 8/75.0% | 21/61.9% | 50/58.0% | +411.22 | YES | KEEP |
| `union_macd_hist_h3_fade_x1.5_h5` | short | 5 | 8/75.0% | 21/61.9% | 50/58.0% | +411.22 | YES | KEEP |

Scoop discovery leaders:

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_news_pack_h1_scoop_x1.5_h3` | long | 3 | 7/100.0% | 5/60.0% | 12/83.3% | +35.39 | no | KILL |
| `union_news_pack_h3_scoop_x1.5_h3` | long | 3 | 7/100.0% | 5/60.0% | 12/83.3% | +35.39 | no | KILL |
| `union_news_pack_net2_h1_scoop_x1.5_h3` | long | 3 | 6/100.0% | 4/75.0% | 10/90.0% | +37.23 | no | KILL |
| `union_news_pack_net2_h3_scoop_x1.5_h3` | long | 3 | 6/100.0% | 4/75.0% | 10/90.0% | +37.23 | no | KILL |
| `union_news_or_net3_h1_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_or_net4_h1_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_or_net3_h3_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_or_net4_h3_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_or_net4_rw_h1_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_or_net4_conv_h1_scoop_x2_h3` | long | 3 | 5/100.0% | 4/25.0% | 9/66.7% | +15.76 | no | KILL |
| `union_news_pack_net3_h1_scoop_x1.5_h3` | long | 3 | 4/100.0% | 4/75.0% | 8/87.5% | +33.89 | no | KILL |
| `union_news_pack_net3_h3_scoop_x1.5_h3` | long | 3 | 4/100.0% | 4/75.0% | 8/87.5% | +33.89 | no | KILL |

Scoop holdout leaders:

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_earn_react_h1_scoop_x2_h5` | long | 5 | 14/7.1% | 15/60.0% | 36/30.6% | -171.22 | no | KILL |
| `union_earn_react_h3_scoop_x2_h5` | long | 5 | 14/7.1% | 15/60.0% | 36/30.6% | -171.22 | no | KILL |
| `union_w_hot_cond_h1_scoop_x1_h2` | long | 2 | 38/50.0% | 30/50.0% | 68/50.0% | +0.41 | no | KILL |
| `union_w_hot_cond_h3_scoop_x1_h2` | long | 2 | 38/50.0% | 30/50.0% | 68/50.0% | +0.41 | no | KILL |
| `union_hot_n4_h1_scoop_x1_h1` | long | 1 | 22/40.9% | 18/50.0% | 40/45.0% | -5.89 | no | KILL |
| `union_flow_in_h1_scoop_x1.5_h5` | long | 5 | 3/33.3% | 10/50.0% | 14/50.0% | -3.30 | no | KILL |
| `union_flow_in_h3_scoop_x1.5_h5` | long | 5 | 3/33.3% | 10/50.0% | 14/50.0% | -3.30 | no | KILL |
| `union_flow_in_h5_scoop_x1.5_h5` | long | 5 | 3/33.3% | 10/50.0% | 14/50.0% | -3.30 | no | KILL |
| `union_news_or_net2_h1_scoop_x1.5_h5` | long | 5 | 6/33.3% | 8/50.0% | 23/34.8% | +9.63 | no | KILL |
| `union_news_or_net2_h3_scoop_x1.5_h5` | long | 5 | 6/33.3% | 8/50.0% | 23/34.8% | +9.63 | no | KILL |
| `union_cond_n4_h3_scoop_x2_h5` | long | 5 | 8/25.0% | 8/50.0% | 18/44.4% | -1.95 | no | KILL |
| `union_e_fresh_h1_scoop_x2_h5` | long | 5 | 15/20.0% | 15/46.7% | 37/29.7% | -158.56 | no | KILL |

Fade discovery leaders:

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_news_or_net2_h1_fade_x3_h3` | short | 3 | 3/100.0% | 4/75.0% | 7/85.7% | +104.45 | no | KILL |
| `union_news_or_net3_h1_fade_x3_h3` | short | 3 | 3/100.0% | 3/66.7% | 6/83.3% | +103.39 | no | KILL |
| `union_news_pack_net3_h1_fade_x3_h3` | short | 3 | 3/100.0% | 1/100.0% | 4/100.0% | +107.34 | no | KILL |
| `union_news_pack_net2_h1_fade_x3_h3` | short | 3 | 3/100.0% | 1/100.0% | 4/100.0% | +107.34 | no | KILL |
| `union_news_or_net2_h3_fade_x3_h3` | short | 3 | 3/100.0% | 4/75.0% | 7/85.7% | +104.45 | no | KILL |
| `union_news_or_net3_h3_fade_x3_h3` | short | 3 | 3/100.0% | 3/66.7% | 6/83.3% | +103.39 | no | KILL |
| `union_news_pack_net3_h3_fade_x3_h3` | short | 3 | 3/100.0% | 1/100.0% | 4/100.0% | +107.34 | no | KILL |
| `union_news_pack_net2_h3_fade_x3_h3` | short | 3 | 3/100.0% | 1/100.0% | 4/100.0% | +107.34 | no | KILL |
| `union_cond_n4_h3_fade_x3_h1` | short | 1 | 3/100.0% | 8/50.0% | 11/63.6% | +9.59 | no | KILL |
| `union_news_or_net4_h1_fade_x0.5_h5` | short | 5 | 3/100.0% | 7/28.6% | 19/47.4% | +140.02 | no | KILL |
| `union_news_or_net4_h3_fade_x0.5_h5` | short | 5 | 3/100.0% | 7/28.6% | 19/47.4% | +140.02 | no | KILL |
| `union_news_or_net4_rw_h1_fade_x0.5_h5` | short | 5 | 3/100.0% | 7/28.6% | 18/50.0% | +141.35 | no | KILL |

Fade holdout leaders:

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `short_alarm_h1_fade_x1.5_h5` | short | 5 | 16/50.0% | 21/76.2% | 49/53.1% | +375.42 | no | KILL |
| `short_alarm_h3_fade_x1.5_h5` | short | 5 | 16/50.0% | 21/76.2% | 49/53.1% | +375.42 | no | KILL |
| `union_macd_hist_h1_fade_x3_h5` | short | 5 | 1/100.0% | 10/70.0% | 26/57.7% | +59.35 | no | KILL |
| `union_macd_hist_h3_fade_x3_h5` | short | 5 | 1/100.0% | 10/70.0% | 26/57.7% | +59.35 | no | KILL |
| `ohlc_hot_h1_fade_x3_h5` | short | 5 | 11/27.3% | 21/66.7% | 46/47.8% | +29.44 | no | KILL |
| `ohlc_hot_h3_fade_x3_h5` | short | 5 | 11/27.3% | 21/66.7% | 46/47.8% | +29.44 | no | KILL |
| `ohlc_hot_h5_fade_x3_h5` | short | 5 | 11/27.3% | 21/66.7% | 46/47.8% | +29.44 | no | KILL |
| `union_news_or_net2_h1_fade_x1_h3` | short | 3 | 12/25.0% | 8/62.5% | 23/39.1% | +64.47 | no | KILL |
| `union_news_or_net2_h3_fade_x1_h3` | short | 3 | 12/25.0% | 8/62.5% | 23/39.1% | +64.47 | no | KILL |
| `short_rsi_ob_h1_fade_x3_h3` | short | 3 | 23/21.7% | 17/58.8% | 47/34.0% | -44.13 | no | KILL |
| `short_rsi_ob_h3_fade_x3_h3` | short | 3 | 23/21.7% | 17/58.8% | 47/34.0% | -44.13 | no | KILL |
| `union_macd_xup_h1_fade_x0.5_h5` | short | 5 | 11/36.4% | 21/57.1% | 43/53.5% | +60.31 | no | KILL |

## Live combo vs all-day sit book

`combo_sh_macd_5050_shared` is the Webull paper sleeve. Sit is the published all-day cash book (red mornings take no new lots). Allow is the research counterfactual: same leftover book, 09:30 fills on hard-red too.

| Book | Book% | Book win | Hard-red fires | Hard-red win | Hard-red $ | Audit |
|---|---:|---:|---:|---:|---:|---|
| sit (live / all-day) | +42.16 | 61.4% | 0 | — | +0.00 | PASS |
| allow (research) | +126.30 | 60.0% | 43 | 48.8% | +5776.88 | PASS |

## Cash books for research survivors / leaders

Sit = published all-day path. Allow = trade through red at the 09:30 open. Hard-red $ is only the red-morning fills.

| Sleeve | Sit Book% | Sit hard-red n/win | Allow Book% | Allow hard-red n/win | Allow hard-red $ |
|---|---:|---:|---:|---:|---:|
| `short_alarm_h1` | +1.71 | 0/— | -0.76 | 73/42.5% | -264.64 |
| `short_alarm_h3` | +2.59 | 0/— | -3.43 | 73/49.3% | -630.61 |

## Gate (do not change live)

Hard-red S≤−3 still blocks new lots in `combo_broker.size_combo_tickets`, `simulate_shared` (default sit), and `simulate_book` / `flatten_robust`. This mine adds opt-in `hard_red_mode=allow` for research books only.

**Survivors: 2.** 2 research survivor(s) on the hidden hard-red window. Published KEEP still hit (≥30 fires, >55%). Do not change live sit from this board.
