# A+B1 Feature Checklist — 2026-10-22

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-22.csv` · prior export for Δ: `2026-10-21`
- score = sum of flags over **30** features

## Framing (per asof trading day)

- **A05/A06/A12/A13** use **exactly two connected sessions**: `pair_day_a` (prev) + `pair_day_b` (asof).
  No multi-day green/red sums.
- **RSI crosses**: cross **up** through 30 or 50 → GOOD; cross **down** through 50 or 70 → BAD.
- **A11 downside structure**: last ~63 sessions split into 3 equal sections; lowest **low** in each;
  GOOD if rising lows or span(highest low − lowest low)/lowest ≤ 12%.
- **B17/B18**: current export EPS/Rev surprise vs **prior export** snapshot (proxy for last 2 prints).
- Analyst last-2 rating actions (upgrade/downgrade) come from quote scrape → merge step (B19).

## Ranked (top 15)

| Rank | Ticker | score | good | bad | pair | Industry |
|-----:|--------|------:|-----:|----:|------|----------|
| 1 | KMI | +17 | 18 | 1 | 2026-10-20→2026-10-21 | Oil & Gas Midstream |
| 2 | CRSR | +16 | 17 | 1 | 2026-10-20→2026-10-21 | Computer Hardware |
| 3 | ISRG | +16 | 17 | 1 | 2026-10-20→2026-10-21 | Medical Instruments & Supplies |
| 4 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 5 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 6 | INOD | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Information Technology Services |
| 7 | CORT | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Biotechnology |
| 8 | ADSK | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Software - Application |
| 9 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 10 | WMG | +14 | 16 | 2 | 2026-10-20→2026-10-21 | Entertainment |
| 11 | SEZL | +14 | 17 | 3 | 2026-10-14→2026-10-15 | Credit Services |
| 12 | AMRX | +14 | 15 | 1 | 2026-10-20→2026-10-21 | Drug Manufacturers - Specialty & Gen |
| 13 | PODD | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Devices |
| 14 | CXT | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Business Equipment & Supplies |
| 15 | DDOG | +14 | 15 | 1 | 2026-10-20→2026-10-21 | Software - Application |

## Full checklist — top 15

### KMI  ·  score **+17**  ·  Oil & Gas Midstream
price=33.970001220703125  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.17 on 2026-10-21; prev RSI=61.66 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=18.500 (G=0.3700 R=0.0200); 2026-10-20:RED:O=33.5900,C=33.5700,body=-0.0200,vol=11940232.0; 2026-10-21:GREEN:O=33.6000,C=33.9700,body=+0.3700,vol=18340062.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.536 (Gvol=18340062 Rvol=11940232); 2026-10-20:RED:O=33.5900,C=33.5700,body=-0.0200,vol=11940232.0; 2026-10-21:GREEN:O=33.6000,C=33.9700,body=+0.3700,vol=18340062.0 | **GOOD** |
| `A07_rvol` | RVOL=1.578 on 2026-10-21: today_vol=18340062 / avg20=11621521 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.706 on 2026-10-21 (price=33.9700, mid=32.1385, upper=34.7341, lower=29.5429; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=33.9700 vs SMA50=31.8162 dist=+6.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=32.1385 SMA50=31.8162 SMA80=31.7966 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-08-04@30.6800; S2[2026-08-24→2026-09-22] low=2026-09-16@30.4100; S3[2026-09-23→2026-10-21] low=2026-10-01@29.4300 | lows=[30.68000030517578, 30.40999984741211, 29.43000030517578] span=4.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5967796516314011 wick_frac=0.4032203483685988 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.06375708344480385 wick_frac=0.9362429165551962 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.49971390425329 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:RED:body=-0.9300:wick=0.4400; 2026-10-16:GREEN:body=+1.1800:wick=0.0435; 2026-10-19:RED:body=-0.6800:wick=0.2950; 2026-10-20:RED:body=-0.0200:wick=0.2937; 2026-10-21:GREEN:body=+0.3700:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.37 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.98 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17896.0 | **NEUTRAL** |
| `B04_income` | 3449.0 | **GOOD** |
| `B05_profit_margin` | 19.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.3 vs prior_export=36.3 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.19 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 2.4 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.37 (this export) | prior_export=15.37 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.98 (this export) | prior_export=5.98 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=13.8100004196167  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.28 on 2026-10-21; prev RSI=49.53 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6200 R=0.0000); 2026-10-20:GREEN:O=12.8700,C=13.1800,body=+0.3100,vol=1113572.0; 2026-10-21:GREEN:O=13.5000,C=13.8100,body=+0.3100,vol=1003664.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2117236 Rvol=0); 2026-10-20:GREEN:O=12.8700,C=13.1800,body=+0.3100,vol=1113572.0; 2026-10-21:GREEN:O=13.5000,C=13.8100,body=+0.3100,vol=1003664.0 | **GOOD** |
| `A07_rvol` | RVOL=0.898 on 2026-10-21: today_vol=1003664 / avg20=1118268 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.463 on 2026-10-21 (price=13.8100, mid=13.4970, upper=14.1735, lower=12.8205; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=13.8100 vs SMA50=12.8870 dist=+7.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=13.4970 SMA50=12.8870 SMA80=11.9204 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-24@9.7200; S2[2026-08-24→2026-09-22] low=2026-08-24@10.7260; S3[2026-09-23→2026-10-21] low=2026-10-15@12.4240 | lows=[9.720000267028809, 10.72599983215332, 12.423999786376953] span=27.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5596946233661975 wick_frac=0.44030537663380254 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+0.5100:wick=0.3610; 2026-10-16:RED:body=-0.3500:wick=0.4664; 2026-10-19:GREEN:body=+0.0300:wick=0.3100; 2026-10-20:GREEN:body=+0.3100:wick=0.2200; 2026-10-21:GREEN:body=+0.3100:wick=0.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 19.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ISRG  ·  score **+16**  ·  Medical Instruments & Supplies
price=428.1199951171875  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.19 on 2026-10-21; prev RSI=69.72 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=30.1230 R=0.0000); 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0; 2026-10-21:GREEN:O=402.4950,C=428.1200,body=+25.6250,vol=4701233.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7841239 Rvol=0); 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0; 2026-10-21:GREEN:O=402.4950,C=428.1200,body=+25.6250,vol=4701233.0 | **GOOD** |
| `A07_rvol` | RVOL=2.348 on 2026-10-21: today_vol=4701233 / avg20=2002457 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.486 on 2026-10-21 (price=428.1200, mid=416.0210, upper=440.9124, lower=391.1296; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=428.1200 vs SMA50=394.1864 dist=+8.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=416.0210 SMA50=394.1864 SMA80=388.8721 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-24@332.1500; S2[2026-08-24→2026-09-22] low=2026-09-10@345.2500; S3[2026-09-23→2026-10-21] low=2026-10-05@389.5600 | lows=[332.1499938964844, 345.25, 389.55999755859375] span=17.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6472860820051523 wick_frac=0.35271391799484786 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+2.0200:wick=8.3300; 2026-10-16:RED:body=-1.7300:wick=7.6500; 2026-10-19:GREEN:body=+5.8400:wick=2.0820; 2026-10-20:GREEN:body=+4.4980:wick=3.9020; 2026-10-21:GREEN:body=+25.6250:wick=8.1322 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.5 (current export asof; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.37 (current export; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11034.4 | **NEUTRAL** |
| `B04_income` | 3138.8 | **GOOD** |
| `B05_profit_margin` | 28.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 479.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=479.86 vs prior_export=479.86 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.8 vs prior=-0.8 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.5 (this export) | prior_export=11.5 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.37 (this export) | prior_export=2.37 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.549999237060547  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.72 on 2026-10-12; prev RSI=57.75 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=8.166 (G=0.4900 R=0.0600); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.297 (Gvol=849222 Rvol=654764); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A07_rvol` | RVOL=0.785 on 2026-10-12: today_vol=654764 / avg20=834281 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.229 on 2026-10-12 (price=16.5500, mid=16.3710, upper=17.1537, lower=15.5883; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=16.5500 vs SMA50=16.1181 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.37_50=16.12_80=16.12 on 2026-10-12: SMA20=16.3710 SMA50=16.1181 SMA80=16.1191 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-31@14.3630; S2[2026-08-13→2026-09-11] low=2026-08-14@14.9010; S3[2026-09-14→2026-10-12] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5764713538160505 wick_frac=0.42352864618394953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.2754882213854103 wick_frac=0.7245117786145897 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.16647593616886 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=422.0899963378906  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.14 on 2026-10-12; prev RSI=59.13 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=17.8700 R=0.0000); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2928672 Rvol=0); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A07_rvol` | RVOL=0.737 on 2026-10-12: today_vol=869447 / avg20=1180302 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.925 on 2026-10-12 (price=422.0900, mid=401.9415, upper=423.7173, lower=380.1657; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=422.0900 vs SMA50=400.5118 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=401.9415 SMA50=400.5118 SMA80=398.6746 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-28@362.9800; S2[2026-08-13→2026-09-11] low=2026-08-17@388.1200; S3[2026-09-14→2026-10-12] low=2026-10-01@375.7400 | lows=[362.9800109863281, 388.1199951171875, 375.739990234375] span=6.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6505804361528325 wick_frac=0.34941956384716766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+3.4100:wick=3.1374; 2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INOD  ·  score **+15**  ·  Information Technology Services
price=60.560001373291016  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.38 on 2026-10-21; prev RSI=41.03 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.1100 R=0.0000); 2026-10-20:GREEN:O=53.3800,C=55.0450,body=+1.6650,vol=538422.0; 2026-10-21:GREEN:O=56.1150,C=60.5600,body=+4.4450,vol=1108698.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1647120 Rvol=0); 2026-10-20:GREEN:O=53.3800,C=55.0450,body=+1.6650,vol=538422.0; 2026-10-21:GREEN:O=56.1150,C=60.5600,body=+4.4450,vol=1108698.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-10-21: today_vol=1108698 / avg20=1382606 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.167 on 2026-10-21 (price=60.5600, mid=62.6390, upper=75.0729, lower=50.2051; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=60.5600 vs SMA50=60.3436 dist=+0.36% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=62.64_50=60.34_80=61.99 on 2026-10-21: SMA20=62.6390 SMA50=60.3436 SMA80=61.9860 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-29@54.3600; S2[2026-08-24→2026-09-22] low=2026-09-14@51.9700; S3[2026-09-23→2026-10-21] low=2026-10-15@50.6900 | lows=[54.36000061035156, 51.970001220703125, 50.689998626708984] span=7.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7067738789184911 wick_frac=0.2932261210815089 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+5.4200:wick=0.5590; 2026-10-16:RED:body=-2.6300:wick=1.2384; 2026-10-19:RED:body=-0.5100:wick=1.9200; 2026-10-20:GREEN:body=+1.6650:wick=1.3150; 2026-10-21:GREEN:body=+4.4450:wick=0.7549 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=92.49 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 317.16 | **NEUTRAL** |
| `B04_income` | 46.48 | **GOOD** |
| `B05_profit_margin` | 14.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 123.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=123.67 vs prior_export=123.67 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | -49.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-49.78 vs prior=-49.78 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.2 | **GOOD** |
| `B13_short_float` | 14.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=92.49 (this export) | prior_export=92.49 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.76 (this export) | prior_export=6.76 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CORT  ·  score **+15**  ·  Biotechnology
price=116.27999877929688  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.31 on 2026-10-21; prev RSI=51.59 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=22.647 (G=3.8500 R=0.1700); 2026-10-20:GREEN:O=112.6000,C=116.4500,body=+3.8500,vol=549300.0; 2026-10-21:RED:O=116.4500,C=116.2800,body=-0.1700,vol=516745.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.063 (Gvol=549300 Rvol=516745); 2026-10-20:GREEN:O=112.6000,C=116.4500,body=+3.8500,vol=549300.0; 2026-10-21:RED:O=116.4500,C=116.2800,body=-0.1700,vol=516745.0 | **GOOD** |
| `A07_rvol` | RVOL=0.688 on 2026-10-21: today_vol=516745 / avg20=751001 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.075 on 2026-10-21 (price=116.2800, mid=116.7490, upper=122.9651, lower=110.5329; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=116.2800 vs SMA50=115.7982 dist=+0.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=116.7490 SMA50=115.7982 SMA80=108.5460 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-28@90.0000; S2[2026-08-24→2026-09-22] low=2026-09-08@108.2500; S3[2026-09-23→2026-10-21] low=2026-10-16@107.7900 | lows=[90.0, 108.25, 107.79000091552734] span=20.28% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7432429048844323 wick_frac=0.25675709511556766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.07456039083805986 wick_frac=0.9254396091619401 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=22.647293779732518 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:RED:body=-2.9400:wick=1.5200; 2026-10-16:RED:body=-3.0100:wick=2.8400; 2026-10-19:GREEN:body=+0.4000:wick=2.1900; 2026-10-20:GREEN:body=+3.8500:wick=1.3300; 2026-10-21:RED:body=-0.1700:wick=2.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1381.48 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.83 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 830.81 | **NEUTRAL** |
| `B04_income` | 54.2 | **GOOD** |
| `B05_profit_margin` | 6.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=141.0 vs prior_export=141.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -6.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.34 vs prior=-6.34 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.39 | **GOOD** |
| `B13_short_float` | 9.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1381.48 (this export) | prior_export=1381.48 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.83 (this export) | prior_export=15.83 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+15**  ·  Software - Application
price=244.83999633789062  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.68 on 2026-10-21; prev RSI=63.49 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.2200 R=0.0000); 2026-10-20:GREEN:O=241.1000,C=245.5800,body=+4.4800,vol=1472196.0; 2026-10-21:GREEN:O=243.1000,C=244.8400,body=+1.7400,vol=872468.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2344664 Rvol=0); 2026-10-20:GREEN:O=241.1000,C=245.5800,body=+4.4800,vol=1472196.0; 2026-10-21:GREEN:O=243.1000,C=244.8400,body=+1.7400,vol=872468.0 | **GOOD** |
| `A07_rvol` | RVOL=0.458 on 2026-10-21: today_vol=872468 / avg20=1906042 (avg window 2026-09-23→2026-10-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.633 on 2026-10-21 (price=244.8400, mid=226.5780, upper=255.4194, lower=197.7366; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=244.8400 vs SMA50=232.8012 dist=+5.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=226.58_50=232.80_80=227.70 on 2026-10-21: SMA20=226.5780 SMA50=232.8012 SMA80=227.6994 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-23@202.3700; S2[2026-08-24→2026-09-22] low=2026-09-09@206.3300; S3[2026-09-23→2026-10-21] low=2026-09-29@199.1801 | lows=[202.3699951171875, 206.3300018310547, 199.1800994873047] span=3.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5115603248467959 wick_frac=0.48843967515320413 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+10.9800:wick=1.7350; 2026-10-16:RED:body=-8.1200:wick=5.8450; 2026-10-19:GREEN:body=+0.3500:wick=4.8275; 2026-10-20:GREEN:body=+4.4800:wick=2.1000; 2026-10-21:GREEN:body=+1.7400:wick=3.3437 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
price=51.970001220703125  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.48 on 2026-10-12; prev RSI=42.64 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6400 R=0.0000); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1914338 Rvol=0); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A07_rvol` | RVOL=0.622 on 2026-10-12: today_vol=717503 / avg20=1153282 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.033 on 2026-10-12 (price=51.9700, mid=52.1205, upper=56.6668, lower=47.5742; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=51.9700 vs SMA50=51.7088 dist=+0.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=52.1205 SMA50=51.7088 SMA80=50.5664 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-08-12@47.8600; S2[2026-08-13→2026-09-11] low=2026-08-13@48.1100; S3[2026-09-14→2026-10-12] low=2026-10-02@48.3600 | lows=[47.86000061035156, 48.11000061035156, 48.36000061035156] span=1.04% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.37251635024746754 wick_frac=0.6274836497525325 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.1800:wick=1.0800; 2026-10-07:RED:body=-1.8300:wick=0.3050; 2026-10-08:RED:body=-0.2900:wick=0.9400; 2026-10-09:GREEN:body=+0.3600:wick=1.6550; 2026-10-12:GREEN:body=+1.2800:wick=0.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMG  ·  score **+14**  ·  Entertainment
price=31.440000534057617  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.88 on 2026-10-21; prev RSI=72.35 on 2026-10-20 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.35@2026-10-20 → 72.88@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.35@2026-10-20 → 72.88@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.35@2026-10-20 → 72.88@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6300 R=0.0000); 2026-10-20:GREEN:O=31.1300,C=31.3300,body=+0.2000,vol=3403925.0; 2026-10-21:GREEN:O=31.0100,C=31.4400,body=+0.4300,vol=1864888.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5268813 Rvol=0); 2026-10-20:GREEN:O=31.1300,C=31.3300,body=+0.2000,vol=3403925.0; 2026-10-21:GREEN:O=31.0100,C=31.4400,body=+0.4300,vol=1864888.0 | **GOOD** |
| `A07_rvol` | RVOL=0.795 on 2026-10-21: today_vol=1864888 / avg20=2344564 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.849 on 2026-10-21 (price=31.4400, mid=28.7033, upper=31.9280, lower=25.4785; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-21: price=31.4400 vs SMA50=28.0673 dist=+12.02% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=28.7033 SMA50=28.0673 SMA80=27.8274 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-08-12@24.5150; S2[2026-08-24→2026-09-22] low=2026-08-24@27.0540; S3[2026-09-23→2026-10-21] low=2026-09-28@26.4500 | lows=[24.514999389648438, 27.054000854492188, 26.450000762939453] span=10.36% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6124870425559064 wick_frac=0.38751295744409353 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+1.2200:wick=0.3150; 2026-10-16:GREEN:body=+0.4000:wick=0.3250; 2026-10-19:RED:body=-0.2200:wick=0.4500; 2026-10-20:GREEN:body=+0.2000:wick=0.4450; 2026-10-21:GREEN:body=+0.4300:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SEZL  ·  score **+14**  ·  Credit Services
price=132.3699951171875  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.93 on 2026-10-15; prev RSI=54.94 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=3.118 (G=11.1300 R=3.5700); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.009 (Gvol=462162 Rvol=457963); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A07_rvol` | RVOL=0.843 on 2026-10-15: today_vol=462162 / avg20=548541 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.975 on 2026-10-15 (price=132.3700, mid=115.5997, upper=132.7973, lower=98.4022; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=132.3700 vs SMA50=119.8667 dist=+10.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-15: SMA20=115.5997 SMA50=119.8667 SMA80=138.2212 | **BAD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@114.1600; S2[2026-08-18→2026-09-16] low=2026-09-01@112.4100; S3[2026-09-17→2026-10-15] low=2026-09-24@104.8700 | lows=[114.16000366210938, 112.41000366210938, 104.87000274658203] span=8.86% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.9823466866705229 wick_frac=0.017653313329477113 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.38867693830950656 wick_frac=0.6113230616904934 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.117646555979886 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+8.6400:wick=1.6700; 2026-10-12:GREEN:body=+2.7300:wick=3.1975; 2026-10-13:RED:body=-1.1400:wick=5.0900; 2026-10-14:RED:body=-3.5700:wick=5.6150; 2026-10-15:GREEN:body=+11.1300:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.7 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.81 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 531.89 | **NEUTRAL** |
| `B04_income` | 161.43 | **GOOD** |
| `B05_profit_margin` | 30.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 168.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=168.0 vs prior_export=168.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -1.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.0 vs prior=-1.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.93 | **GOOD** |
| `B13_short_float` | 25.11 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.7 (this export) | prior_export=9.7 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.81 (this export) | prior_export=10.81 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMRX  ·  score **+14**  ·  Drug Manufacturers - Specialty & Generic
price=20.200000762939453  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.78 on 2026-10-21; prev RSI=56.31 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7300 R=0.0000); 2026-10-20:GREEN:O=19.4100,C=19.8400,body=+0.4300,vol=5203935.0; 2026-10-21:GREEN:O=19.9000,C=20.2000,body=+0.3000,vol=4657958.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9861893 Rvol=0); 2026-10-20:GREEN:O=19.4100,C=19.8400,body=+0.4300,vol=5203935.0; 2026-10-21:GREEN:O=19.9000,C=20.2000,body=+0.3000,vol=4657958.0 | **GOOD** |
| `A07_rvol` | RVOL=1.505 on 2026-10-21: today_vol=4657958 / avg20=3095990 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.403 on 2026-10-21 (price=20.2000, mid=19.8485, upper=20.7197, lower=18.9773; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=20.2000 vs SMA50=18.6890 dist=+8.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=19.8485 SMA50=18.6890 SMA80=18.3155 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-08-12@16.7700; S2[2026-08-24→2026-09-22] low=2026-09-16@16.5600; S3[2026-09-23→2026-10-21] low=2026-09-23@18.7350 | lows=[16.770000457763672, 16.559999465942383, 18.735000610351562] span=13.13% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6215848937719632 wick_frac=0.3784151062280368 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-15:GREEN:body=+0.2900:wick=0.6950; 2026-10-16:RED:body=-0.1000:wick=0.3450; 2026-10-19:GREEN:body=+0.2600:wick=0.2350; 2026-10-20:GREEN:body=+0.4300:wick=0.1700; 2026-10-21:GREEN:body=+0.3000:wick=0.2698 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=34.83 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.64 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3117.55 | **NEUTRAL** |
| `B04_income` | 157.36 | **GOOD** |
| `B05_profit_margin` | 5.05 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.71 vs prior_export=22.71 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -0.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.25 vs prior=-0.25 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.94 | **GOOD** |
| `B13_short_float` | 7.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=34.83 (this export) | prior_export=34.83 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.64 (this export) | prior_export=3.64 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+14**  ·  Medical Devices
price=139.80999755859375  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.20 on 2026-10-12; prev RSI=44.78 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.9300 R=0.0000); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2183896 Rvol=0); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A07_rvol` | RVOL=0.819 on 2026-10-12: today_vol=1319431 / avg20=1610443 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-10-12 (price=139.8100, mid=136.1735, upper=143.1581, lower=129.1889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=139.8100 vs SMA50=141.5730 dist=-1.25% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=136.1735 SMA50=141.5730 SMA80=147.5865 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-05@126.4000; S2[2026-08-13→2026-09-11] low=2026-09-11@131.2400; S3[2026-09-14→2026-10-12] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5660465898505131 wick_frac=0.43395341014948685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.0400:wick=1.3950; 2026-10-07:RED:body=-0.5800:wick=3.1000; 2026-10-08:GREEN:body=+0.5100:wick=1.2300; 2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+14**  ·  Business Equipment & Supplies
price=51.11000061035156  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.07 on 2026-10-12; prev RSI=57.77 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.5300 R=0.0000); 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0; 2026-10-12:GREEN:O=50.4100,C=51.1100,body=+0.7000,vol=467391.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=886404 Rvol=0); 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0; 2026-10-12:GREEN:O=50.4100,C=51.1100,body=+0.7000,vol=467391.0 | **GOOD** |
| `A07_rvol` | RVOL=0.835 on 2026-10-12: today_vol=467391 / avg20=559837 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.062 on 2026-10-12 (price=51.1100, mid=47.9275, upper=50.9230, lower=44.9320; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=51.1100 vs SMA50=49.6434 dist=+2.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=47.9275 SMA50=49.6434 SMA80=49.6754 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-10@49.7400; S2[2026-08-13→2026-09-11] low=2026-08-20@47.5800; S3[2026-09-14→2026-10-12] low=2026-09-18@44.8400 | lows=[49.7400016784668, 47.58000183105469, 44.84000015258789] span=10.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6434306991572664 wick_frac=0.35656930084273364 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.3400:wick=0.6200; 2026-10-07:RED:body=-0.2500:wick=1.7600; 2026-10-08:GREEN:body=+0.4100:wick=0.7600; 2026-10-09:GREEN:body=+1.8300:wick=0.2700; 2026-10-12:GREEN:body=+0.7000:wick=0.9850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.7 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.9 | **NEUTRAL** |
| `B04_income` | 140.3 | **GOOD** |
| `B05_profit_margin` | 7.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.71 vs prior_export=68.71 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.45 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DDOG  ·  score **+14**  ·  Software - Application
price=273.0899963378906  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.34 on 2026-10-21; prev RSI=47.30 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.30@2026-10-20 → 55.34@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.30@2026-10-20 → 55.34@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.30@2026-10-20 → 55.34@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=19.304 (G=13.3200 R=0.6900); 2026-10-20:RED:O=257.1200,C=256.4300,body=-0.6900,vol=2185400.0; 2026-10-21:GREEN:O=259.7700,C=273.0900,body=+13.3200,vol=2877680.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.317 (Gvol=2877680 Rvol=2185400); 2026-10-20:RED:O=257.1200,C=256.4300,body=-0.6900,vol=2185400.0; 2026-10-21:GREEN:O=259.7700,C=273.0900,body=+13.3200,vol=2877680.0 | **GOOD** |
| `A07_rvol` | RVOL=1.002 on 2026-10-21: today_vol=2877680 / avg20=2871470 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.083 on 2026-10-21 (price=273.0900, mid=271.3047, upper=292.9297, lower=249.6798; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=273.0900 vs SMA50=248.0666 dist=+10.09% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=271.30_50=248.07_80=252.24 on 2026-10-21: SMA20=271.3047 SMA50=248.0666 SMA80=252.2370 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-08-06@225.2600; S2[2026-08-24→2026-09-22] low=2026-09-08@203.2400; S3[2026-09-23→2026-10-21] low=2026-10-16@243.6300 | lows=[225.25999450683594, 203.24000549316406, 243.6300048828125] span=19.87% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7615775597482521 wick_frac=0.2384224402517479 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.11022386022386023 wick_frac=0.8897761397761398 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.304290137107476 need>1.4; red_wick_gt_green=False 5d trail=2026-10-15:GREEN:body=+5.5260:wick=5.1640; 2026-10-16:RED:body=-18.3000:wick=7.1700; 2026-10-19:GREEN:body=+2.5350:wick=7.3250; 2026-10-20:RED:body=-0.6900:wick=5.5700; 2026-10-21:GREEN:body=+13.3200:wick=4.1700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.47 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.86 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3966.72 | **NEUTRAL** |
| `B04_income` | 177.58 | **GOOD** |
| `B05_profit_margin` | 4.48 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 289.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=289.09 vs prior_export=289.09 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.39 | **GOOD** |
| `B10_insider_transactions` | -8.55 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.55 vs prior=-8.55 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.24 | **GOOD** |
| `B13_short_float` | 3.12 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.47 (this export) | prior_export=11.47 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.86 (this export) | prior_export=3.86 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-22_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.