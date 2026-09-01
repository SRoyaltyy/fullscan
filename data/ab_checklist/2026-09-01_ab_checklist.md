# A+B1 Feature Checklist — 2026-09-01

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,682** names
- Export: `finviz_2026-09-01.csv` · prior export for Δ: `2026-08-31`
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
| 1 | RCI | +17 | 17 | 0 | 2026-08-27→2026-08-31 | Telecom Services |
| 2 | FAF | +16 | 17 | 1 | 2026-08-27→2026-08-31 | Insurance - Specialty |
| 3 | BCE | +16 | 16 | 0 | 2026-08-27→2026-08-31 | Telecom Services |
| 4 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 5 | PANW | +16 | 17 | 1 | 2026-08-27→2026-08-31 | Software - Infrastructure |
| 6 | SON | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 7 | HLNE | +16 | 17 | 1 | 2026-08-27→2026-08-31 | Asset Management |
| 8 | OMDA | +15 | 16 | 1 | 2026-08-27→2026-08-31 | Health Information Services |
| 9 | NWBI | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Banks - Regional |
| 10 | CRSR | +15 | 17 | 2 | 2026-08-27→2026-08-31 | Computer Hardware |
| 11 | GPK | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 12 | OTF | +15 | 15 | 0 | 2026-08-27→2026-08-31 | Asset Management |
| 13 | XYZ | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 14 | ACCO | +15 | 16 | 1 | 2026-08-27→2026-08-31 | Business Equipment & Supplies |
| 15 | MITK | +15 | 16 | 1 | 2026-08-27→2026-08-31 | Software - Application |

## Full checklist — top 15

### RCI  ·  score **+17**  ·  Telecom Services
price=36.88999938964844  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.65 on 2026-08-31; prev RSI=59.20 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.20@2026-08-27 → 62.65@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.20@2026-08-27 → 62.65@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.20@2026-08-27 → 62.65@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=1.950 (G=0.3900 R=0.2000); 2026-08-27:RED:O=36.6400,C=36.4400,body=-0.2000,vol=931500.0; 2026-08-31:GREEN:O=36.5000,C=36.8900,body=+0.3900,vol=1019200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.094 (Gvol=1019200 Rvol=931500); 2026-08-27:RED:O=36.6400,C=36.4400,body=-0.2000,vol=931500.0; 2026-08-31:GREEN:O=36.5000,C=36.8900,body=+0.3900,vol=1019200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.962 on 2026-08-31: today_vol=1019200 / avg20=1060010 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.571 on 2026-08-31 (price=36.8900, mid=35.6920, upper=37.7890, lower=33.5950; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=36.8900 vs SMA50=34.8516 dist=+5.85% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=35.69_50=34.85_80=35.58 on 2026-08-31: SMA20=35.6920 SMA50=34.8516 SMA80=35.5779 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-25@34.5600; S2[2026-06-26→2026-07-29] low=2026-07-02@31.3800; S3[2026-07-30→2026-08-31] low=2026-08-05@33.3200 | lows=[34.560001373291016, 31.3799991607666, 33.31999969482422] span=10.13% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.5064924127202739 wick_frac=0.49350758727972616 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.2941186370317181 wick_frac=0.705881362968282 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9499895096225373 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.2300:wick=0.4400; 2026-08-25:GREEN:body=+0.3400:wick=0.1700; 2026-08-26:GREEN:body=+0.4300:wick=0.2700; 2026-08-27:RED:body=-0.2000:wick=0.4800; 2026-08-31:GREEN:body=+0.3900:wick=0.3800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.94 (current export asof; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.28 (current export; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16362.29 | **NEUTRAL** |
| `B04_income` | 4477.33 | **GOOD** |
| `B05_profit_margin` | 27.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 42.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.010000000000005116 (now=42.56 vs prior_export=42.55 on finviz_2026-08-31) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.03 | **GOOD** |
| `B13_short_float` | 4.43 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.28 (this export) | prior_export=1.28 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FAF  ·  score **+16**  ·  Insurance - Specialty
price=73.91999816894531  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.18 on 2026-08-31; prev RSI=57.03 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.03@2026-08-27 → 54.18@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.03@2026-08-27 → 54.18@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.03@2026-08-27 → 54.18@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=8.450 (G=1.6900 R=0.2000); 2026-08-27:GREEN:O=72.8600,C=74.5500,body=+1.6900,vol=836000.0; 2026-08-31:RED:O=74.1200,C=73.9200,body=-0.2000,vol=811900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.030 (Gvol=836000 Rvol=811900); 2026-08-27:GREEN:O=72.8600,C=74.5500,body=+1.6900,vol=836000.0; 2026-08-31:RED:O=74.1200,C=73.9200,body=-0.2000,vol=811900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.376 on 2026-08-31: today_vol=811900 / avg20=589875 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.166 on 2026-08-31 (price=73.9200, mid=73.4865, upper=76.0944, lower=70.8786; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=73.9200 vs SMA50=71.7030 dist=+3.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=73.4865 SMA50=71.7030 SMA80=69.8634 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-03@63.4450; S2[2026-06-26→2026-07-29] low=2026-06-26@65.0800; S3[2026-07-30→2026-08-31] low=2026-08-20@71.2200 | lows=[63.44503130060075, 65.08000183105469, 71.22000122070312] span=12.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.7681839935080699 wick_frac=0.23181600649193018 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.1418476172955073 wick_frac=0.8581523827044927 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.449818806027084 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.4800:wick=0.9400; 2026-08-25:GREEN:body=+0.5400:wick=1.2000; 2026-08-26:GREEN:body=+0.3800:wick=0.5700; 2026-08-27:GREEN:body=+1.6900:wick=0.5100; 2026-08-31:RED:body=-0.2000:wick=1.2100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.87 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.36 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7983.9 | **NEUTRAL** |
| `B04_income` | 745.1 | **GOOD** |
| `B05_profit_margin` | 9.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 87.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=87.67 vs prior_export=87.67 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -0.4 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.4 vs prior=-0.4 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.86 | **GOOD** |
| `B13_short_float` | 5.67 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.87 (this export) | prior_export=16.87 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.36 (this export) | prior_export=4.36 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BCE  ·  score **+16**  ·  Telecom Services
price=23.59000015258789  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.59 on 2026-08-31; prev RSI=56.92 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.92@2026-08-27 → 59.59@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.92@2026-08-27 → 59.59@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.92@2026-08-27 → 59.59@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1700 R=0.0000); 2026-08-27:DOJI:O=23.4000,C=23.4000,body=+0.0000,vol=1709400.0; 2026-08-31:GREEN:O=23.4200,C=23.5900,body=+0.1700,vol=3257500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=4.811 (Gvol=4112200 Rvol=854700); 2026-08-27:DOJI:O=23.4000,C=23.4000,body=+0.0000,vol=1709400.0; 2026-08-31:GREEN:O=23.4200,C=23.5900,body=+0.1700,vol=3257500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.982 on 2026-08-31: today_vol=3257500 / avg20=3315635 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.352 on 2026-08-31 (price=23.5900, mid=23.1510, upper=24.3966, lower=21.9054; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=23.5900 vs SMA50=22.4920 dist=+4.88% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=23.15_50=22.49_80=23.09 on 2026-08-31: SMA20=23.1510 SMA50=22.4920 SMA80=23.0941 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-22@22.6500; S2[2026-06-26→2026-07-29] low=2026-07-06@20.8700; S3[2026-07-30→2026-08-31] low=2026-08-04@21.3600 | lows=[22.649999618530273, 20.8700008392334, 21.360000610351562] span=8.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.5666647593253098 wick_frac=0.4333352406746902 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.2300:wick=0.1200; 2026-08-25:RED:body=-0.1600:wick=0.1800; 2026-08-26:RED:body=-0.0400:wick=0.1900; 2026-08-27:DOJI:body=+0.0000:wick=0.2800; 2026-08-31:GREEN:body=+0.1700:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.25 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.82 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17943.05 | **NEUTRAL** |
| `B04_income` | 4547.27 | **GOOD** |
| `B05_profit_margin` | 25.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.010000000000001563 (now=26.5 vs prior_export=26.49 on finviz_2026-08-31) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.36 | **GOOD** |
| `B13_short_float` | 3.3 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.25 (this export) | prior_export=2.25 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.82 (this export) | prior_export=0.82 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+16**  ·  Engineering & Construction
price=38.58000183105469  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.00 on 2026-08-21; prev RSI=55.61 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6400 R=0.0000); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1163800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=3.022 (Gvol=1739450 Rvol=575650); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1163800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-08-21: today_vol=1163800 / avg20=1561180 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.586 on 2026-08-21 (price=38.5800, mid=37.4205, upper=39.4004, lower=35.4406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.5800 vs SMA50=35.8930 dist=+7.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=37.4205 SMA50=35.8930 SMA80=35.2023 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@30.9469; S2[2026-06-18→2026-07-20] low=2026-06-22@31.6100; S3[2026-07-23→2026-08-21] low=2026-07-30@32.1400 | lows=[30.946867052586228, 31.610000610351562, 32.13999938964844] span=3.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.633665198703761 wick_frac=0.36633480129623897 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.8200:wick=0.0900; 2026-08-18:RED:body=-0.2600:wick=0.5300; 2026-08-19:GREEN:body=+0.2700:wick=0.6400; 2026-08-20:DOJI:body=+0.0000:wick=0.9600; 2026-08-21:GREEN:body=+0.6400:wick=0.3700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.91 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+16**  ·  Software - Infrastructure
price=382.1300048828125  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.12 on 2026-08-31; prev RSI=60.44 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.44@2026-08-27 → 60.12@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.44@2026-08-27 → 60.12@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.44@2026-08-27 → 60.12@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=36.4200 R=0.0000); 2026-08-27:GREEN:O=358.5600,C=382.8500,body=+24.2900,vol=7308800.0; 2026-08-31:GREEN:O=370.0000,C=382.1300,body=+12.1300,vol=8797000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=99.000 (Gvol=16105800 Rvol=0); 2026-08-27:GREEN:O=358.5600,C=382.8500,body=+24.2900,vol=7308800.0; 2026-08-31:GREEN:O=370.0000,C=382.1300,body=+12.1300,vol=8797000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.596 on 2026-08-31: today_vol=8797000 / avg20=5510360 (avg window 2026-07-31→2026-08-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.436 on 2026-08-31 (price=382.1300, mid=367.3890, upper=401.2083, lower=333.5697; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=382.1300 vs SMA50=343.6260 dist=+11.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=367.3890 SMA50=343.6260 SMA80=310.3147 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-31 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-09@251.1500; S2[2026-07-01→2026-07-30] low=2026-07-28@308.5400; S3[2026-07-31→2026-08-31] low=2026-07-31@319.4800 | lows=[251.14999389648438, 308.5400085449219, 319.4800109863281] span=27.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.8377231811062529 wick_frac=0.16227681889374707 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:RED:body=-5.1600:wick=5.6600; 2026-08-25:RED:body=-13.8700:wick=4.3900; 2026-08-26:GREEN:body=+8.8500:wick=5.4000; 2026-08-27:GREEN:body=+24.2900:wick=6.3800; 2026-08-31:GREEN:body=+12.1300:wick=1.6000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.2 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10606.3 | **NEUTRAL** |
| `B04_income` | 842.8 | **GOOD** |
| `B05_profit_margin` | 7.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 372.52 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.3999999999999773 (now=372.52 vs prior_export=370.12 on finviz_2026-08-31) | **GOOD** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -0.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.83 vs prior=-0.83 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.98 | **GOOD** |
| `B13_short_float` | 2.78 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.2 (this export) | prior_export=7.2 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SON  ·  score **+16**  ·  Packaging & Containers
price=59.47999954223633  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.69 on 2026-08-21; prev RSI=57.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0400 R=0.0000); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=833100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1410300 Rvol=0); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=833100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.823 on 2026-08-21: today_vol=833100 / avg20=1012315 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.847 on 2026-08-21 (price=59.4800, mid=57.8227, upper=59.7788, lower=55.8666; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=59.4800 vs SMA50=54.9048 dist=+8.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=57.8227 SMA50=54.9048 SMA80=52.5635 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@45.4827; S2[2026-06-18→2026-07-20] low=2026-06-22@49.4158; S3[2026-07-23→2026-08-21] low=2026-07-23@54.7457 | lows=[45.482707673306166, 49.41576037269584, 54.7456863407041] span=20.37% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6465540631459084 wick_frac=0.3534459368540917 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-1.0000:wick=0.1300; 2026-08-18:RED:body=-0.0700:wick=1.1000; 2026-08-19:GREEN:body=+0.2800:wick=0.5600; 2026-08-20:GREEN:body=+1.5500:wick=0.3800; 2026-08-21:GREEN:body=+0.4900:wick=0.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.23 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.29 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7458.91 | **NEUTRAL** |
| `B04_income` | 646.79 | **GOOD** |
| `B05_profit_margin` | 8.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.49 vs prior=1.49 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.95 | **GOOD** |
| `B13_short_float` | 10.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HLNE  ·  score **+16**  ·  Asset Management
price=108.81999969482422  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.48 on 2026-08-31; prev RSI=65.07 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.07@2026-08-27 → 67.48@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.07@2026-08-27 → 67.48@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.07@2026-08-27 → 67.48@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.8600 R=0.0000); 2026-08-27:GREEN:O=105.6200,C=106.8800,body=+1.2600,vol=585700.0; 2026-08-31:GREEN:O=105.2200,C=108.8200,body=+3.6000,vol=684700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1270400 Rvol=0); 2026-08-27:GREEN:O=105.6200,C=106.8800,body=+1.2600,vol=585700.0; 2026-08-31:GREEN:O=105.2200,C=108.8200,body=+3.6000,vol=684700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.004 on 2026-08-31: today_vol=684700 / avg20=682080 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.818 on 2026-08-31 (price=108.8200, mid=103.2965, upper=110.0492, lower=96.5438; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-31: price=108.8200 vs SMA50=90.4123 dist=+20.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=103.2965 SMA50=90.4123 SMA80=88.5363 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-24@71.8800; S2[2026-06-26→2026-07-29] low=2026-06-29@72.7800; S3[2026-07-30→2026-08-31] low=2026-07-30@85.7800 | lows=[71.87999725341797, 72.77999877929688, 85.77999877929688] span=19.34% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.5447455763788279 wick_frac=0.4552544236211721 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.0100:wick=2.9600; 2026-08-25:GREEN:body=+1.8100:wick=1.7100; 2026-08-26:RED:body=-1.0400:wick=2.0900; 2026-08-27:GREEN:body=+1.2600:wick=2.3930; 2026-08-31:GREEN:body=+3.6000:wick=1.2350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.41 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=24.05 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 858.37 | **NEUTRAL** |
| `B04_income` | 275.89 | **GOOD** |
| `B05_profit_margin` | 32.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 135.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=135.0 vs prior_export=135.0 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 1.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.13 vs prior=1.13 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.84 | **GOOD** |
| `B13_short_float` | 11.3 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.41 (this export) | prior_export=26.41 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=24.05 (this export) | prior_export=24.05 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OMDA  ·  score **+15**  ·  Health Information Services
price=24.549999237060547  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.77 on 2026-08-31; prev RSI=60.15 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.15@2026-08-27 → 61.77@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.15@2026-08-27 → 61.77@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.15@2026-08-27 → 61.77@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=18.800 (G=0.9400 R=0.0500); 2026-08-27:RED:O=24.3200,C=24.2700,body=-0.0500,vol=1107300.0; 2026-08-31:GREEN:O=23.6100,C=24.5500,body=+0.9400,vol=1404700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.269 (Gvol=1404700 Rvol=1107300); 2026-08-27:RED:O=24.3200,C=24.2700,body=-0.0500,vol=1107300.0; 2026-08-31:GREEN:O=23.6100,C=24.5500,body=+0.9400,vol=1404700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.989 on 2026-08-31: today_vol=1404700 / avg20=1419920 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.368 on 2026-08-31 (price=24.5500, mid=23.2315, upper=26.8106, lower=19.6524; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=24.5500 vs SMA50=21.9378 dist=+11.91% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=23.2315 SMA50=21.9378 SMA80=19.9764 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-05-27@16.0240; S2[2026-06-26→2026-07-29] low=2026-06-26@18.9000; S3[2026-07-30→2026-08-31] low=2026-07-30@18.2400 | lows=[16.02400016784668, 18.899999618530273, 18.239999771118164] span=17.95% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.949493781849358 wick_frac=0.050506218150642046 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.03854988661798053 wick_frac=0.9614501133820195 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.800259403372245 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.1400:wick=1.1930; 2026-08-25:RED:body=-0.1800:wick=0.6500; 2026-08-26:GREEN:body=+0.0300:wick=0.5950; 2026-08-27:RED:body=-0.0500:wick=1.2470; 2026-08-31:GREEN:body=+0.9400:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=165.59 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.74 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 309.76 | **NEUTRAL** |
| `B04_income` | 4.3 | **GOOD** |
| `B05_profit_margin` | 1.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.25 vs prior_export=28.25 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | -0.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.89 vs prior=-0.89 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.09 | **GOOD** |
| `B13_short_float` | 19.57 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=165.59 (this export) | prior_export=165.59 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.74 (this export) | prior_export=9.74 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+15**  ·  Banks - Regional
price=15.420000076293945  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.05 on 2026-08-21; prev RSI=47.68 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.500 (G=0.0900 R=0.0600); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=864700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.771 (Gvol=667000 Rvol=864700); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=864700.0 | **BAD** |
| `A07_rvol` | RVOL=0.891 on 2026-08-21: today_vol=864700 / avg20=970710 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.394 on 2026-08-21 (price=15.4200, mid=15.5735, upper=15.9625, lower=15.1844; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=15.4200 vs SMA50=15.0801 dist=+2.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=15.5735 SMA50=15.0801 SMA80=14.5336 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@13.3506; S2[2026-06-18→2026-07-20] low=2026-06-18@14.1504; S3[2026-07-23→2026-08-21] low=2026-07-23@14.8812 | lows=[13.350576971196979, 14.150425914132185, 14.881151984844093] span=11.46% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5624981373641766 wick_frac=0.4375018626358234 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.22221986747481598 wick_frac=0.7777801325251841 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5000158947134183 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.0300:wick=0.1400; 2026-08-18:RED:body=-0.0700:wick=0.1200; 2026-08-19:RED:body=-0.3800:wick=0.0400; 2026-08-20:GREEN:body=+0.0900:wick=0.0700; 2026-08-21:RED:body=-0.0600:wick=0.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.57 | **GOOD** |
| `B13_short_float` | 5.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+15**  ·  Computer Hardware
price=12.149999618530273  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.27 on 2026-08-31; prev RSI=55.00 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.00@2026-08-27 → 56.27@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.00@2026-08-27 → 56.27@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.00@2026-08-27 → 56.27@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=1.611 (G=0.1450 R=0.0900); 2026-08-27:RED:O=12.0700,C=11.9800,body=-0.0900,vol=1142300.0; 2026-08-31:GREEN:O=12.0050,C=12.1500,body=+0.1450,vol=1265100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.108 (Gvol=1265100 Rvol=1142300); 2026-08-27:RED:O=12.0700,C=11.9800,body=-0.0900,vol=1142300.0; 2026-08-31:GREEN:O=12.0050,C=12.1500,body=+0.1450,vol=1265100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.555 on 2026-08-31: today_vol=1265100 / avg20=2279320 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.063 on 2026-08-31 (price=12.1500, mid=12.0155, upper=14.1454, lower=9.8856; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=12.1500 vs SMA50=10.5387 dist=+15.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=12.0155 SMA50=10.5387 SMA80=9.8011 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-31 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-09@8.0500; S2[2026-07-01→2026-07-30] low=2026-07-07@8.3000; S3[2026-07-31→2026-08-31] low=2026-07-31@10.2250 | lows=[8.050000190734863, 8.300000190734863, 10.225000381469727] span=27.02% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.34198013477404204 wick_frac=0.6580198652259579 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.18000030517578125 wick_frac=0.8199996948242188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6111028694951892 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.1600:wick=0.1040; 2026-08-25:GREEN:body=+0.7400:wick=0.1000; 2026-08-26:GREEN:body=+0.3200:wick=0.0700; 2026-08-27:RED:body=-0.0900:wick=0.4100; 2026-08-31:GREEN:body=+0.1450:wick=0.2790 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 23.38 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPK  ·  score **+15**  ·  Packaging & Containers
price=11.960000038146973  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.53 on 2026-08-21; prev RSI=55.27 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6200 R=0.0000); 2026-08-20:GREEN:O=11.2600,C=11.6300,body=+0.3700,vol=4254600.0; 2026-08-21:GREEN:O=11.7100,C=11.9600,body=+0.2500,vol=4464100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8718700 Rvol=0); 2026-08-20:GREEN:O=11.2600,C=11.6300,body=+0.3700,vol=4254600.0; 2026-08-21:GREEN:O=11.7100,C=11.9600,body=+0.2500,vol=4464100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.904 on 2026-08-21: today_vol=4464100 / avg20=4935460 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.536 on 2026-08-21 (price=11.9600, mid=11.5490, upper=12.3154, lower=10.7826; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=11.9600 vs SMA50=10.9839 dist=+8.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=11.5490 SMA50=10.9839 SMA80=10.6719 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@9.2143; S2[2026-06-18→2026-07-20] low=2026-07-08@9.8600; S3[2026-07-23→2026-08-21] low=2026-07-24@10.5000 | lows=[9.214289930855712, 9.859999656677246, 10.5] span=13.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.666736810290141 wick_frac=0.3332631897098591 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.2300:wick=0.1500; 2026-08-18:RED:body=-0.1400:wick=0.1300; 2026-08-19:GREEN:body=+0.2700:wick=0.1900; 2026-08-20:GREEN:body=+0.3700:wick=0.0400; 2026-08-21:GREEN:body=+0.2500:wick=0.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.85 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.59 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8637.0 | **NEUTRAL** |
| `B04_income` | 194.0 | **GOOD** |
| `B05_profit_margin` | 2.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.58 vs prior_export=12.58 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 3.14 | **NEUTRAL** |
| `B10_insider_transactions` | 2.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.06 vs prior=2.06 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 6.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.85 (this export) | prior_export=14.85 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OTF  ·  score **+15**  ·  Asset Management
price=11.680000305175781  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.39 on 2026-08-31; prev RSI=58.24 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.24@2026-08-27 → 64.39@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.24@2026-08-27 → 64.39@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.24@2026-08-27 → 64.39@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5200 R=0.0000); 2026-08-27:GREEN:O=11.1100,C=11.3000,body=+0.1900,vol=3626700.0; 2026-08-31:GREEN:O=11.3500,C=11.6800,body=+0.3300,vol=2929100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6555800 Rvol=0); 2026-08-27:GREEN:O=11.1100,C=11.3000,body=+0.1900,vol=3626700.0; 2026-08-31:GREEN:O=11.3500,C=11.6800,body=+0.3300,vol=2929100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.938 on 2026-08-31: today_vol=2929100 / avg20=3121755 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.631 on 2026-08-31 (price=11.6800, mid=11.1930, upper=11.9653, lower=10.4207; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=11.6800 vs SMA50=10.5819 dist=+10.38% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=11.19_50=10.58_80=10.60 on 2026-08-31: SMA20=11.1930 SMA50=10.5819 SMA80=10.6021 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-31 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-25@9.7990; S2[2026-06-30→2026-07-29] low=2026-07-23@9.7800; S3[2026-07-30→2026-08-31] low=2026-07-30@9.7900 | lows=[9.798954069538013, 9.779999732971191, 9.789999961853027] span=0.19% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.7382538085174262 wick_frac=0.2617461914825738 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:GREEN:body=+0.1300:wick=0.1250; 2026-08-25:GREEN:body=+0.1000:wick=0.1700; 2026-08-26:RED:body=-0.0400:wick=0.1200; 2026-08-27:GREEN:body=+0.1900:wick=0.1350; 2026-08-31:GREEN:body=+0.3300:wick=0.0400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.33 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1456.25 | **NEUTRAL** |
| `B04_income` | 375.08 | **GOOD** |
| `B05_profit_margin` | 25.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.61 vs prior_export=13.61 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 0.03 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.009999999999999998 (now=0.03 vs prior=0.02 on finviz_2026-08-31) | **GOOD** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.33 (this export) | prior_export=0.33 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### XYZ  ·  score **+15**  ·  Software - Infrastructure
price=82.16000366210938  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.42 on 2026-08-21; prev RSI=50.53 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.53@2026-08-20 → 55.42@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.53@2026-08-20 → 55.42@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.53@2026-08-20 → 55.42@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.437 (G=2.1200 R=0.8700); 2026-08-20:RED:O=80.9500,C=80.0800,body=-0.8700,vol=2869700.0; 2026-08-21:GREEN:O=80.0400,C=82.1600,body=+2.1200,vol=6718200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=2.341 (Gvol=6718200 Rvol=2869700); 2026-08-20:RED:O=80.9500,C=80.0800,body=-0.8700,vol=2869700.0; 2026-08-21:GREEN:O=80.0400,C=82.1600,body=+2.1200,vol=6718200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.406 on 2026-08-21: today_vol=6718200 / avg20=4778680 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.248 on 2026-08-21 (price=82.1600, mid=81.2200, upper=85.0171, lower=77.4229; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=82.1600 vs SMA50=78.0048 dist=+5.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=81.2200 SMA50=78.0048 SMA80=75.4482 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-11@65.4600; S2[2026-06-22→2026-07-23] low=2026-06-23@71.5000; S3[2026-07-24→2026-08-21] low=2026-07-24@75.9800 | lows=[65.45999908447266, 71.5, 75.9800033569336] span=16.07% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7036184543705054 wick_frac=0.2963815456294946 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.34661235903826865 wick_frac=0.6533876409617314 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.436798442542444 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-1.4300:wick=0.4450; 2026-08-18:GREEN:body=+0.2400:wick=2.2100; 2026-08-19:GREEN:body=+1.1700:wick=1.2800; 2026-08-20:RED:body=-0.8700:wick=1.6400; 2026-08-21:GREEN:body=+2.1200:wick=0.8930 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=17.09 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.22 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 25041.96 | **NEUTRAL** |
| `B04_income` | 357.14 | **GOOD** |
| `B05_profit_margin` | 1.43 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.12 vs prior_export=99.12 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.51 | **GOOD** |
| `B10_insider_transactions` | -1.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.25 vs prior=-1.25 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 2.57 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.22 (this export) | prior_export=2.22 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ACCO  ·  score **+15**  ·  Business Equipment & Supplies
price=4.28000020980835  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.26 on 2026-08-31; prev RSI=54.91 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.91@2026-08-27 → 54.26@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.91@2026-08-27 → 54.26@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.91@2026-08-27 → 54.26@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=7.000 (G=0.1400 R=0.0200); 2026-08-27:GREEN:O=4.1500,C=4.2900,body=+0.1400,vol=691800.0; 2026-08-31:RED:O=4.3000,C=4.2800,body=-0.0200,vol=550100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.258 (Gvol=691800 Rvol=550100); 2026-08-27:GREEN:O=4.1500,C=4.2900,body=+0.1400,vol=691800.0; 2026-08-31:RED:O=4.3000,C=4.2800,body=-0.0200,vol=550100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.807 on 2026-08-31: today_vol=550100 / avg20=681835 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.009 on 2026-08-31 (price=4.2800, mid=4.2814, upper=4.4434, lower=4.1194; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=4.2800 vs SMA50=4.1384 dist=+3.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-31: SMA20=4.2814 SMA50=4.1384 SMA80=4.0444 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-31 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-01@3.8000; S2[2026-06-26→2026-07-29] low=2026-07-09@3.8000; S3[2026-07-30→2026-08-31] low=2026-08-25@4.0900 | lows=[3.799999952316284, 3.799999952316284, 4.090000152587891] span=7.63% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=1.0 wick_frac=0.0 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.25 wick_frac=0.75 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:RED:body=-0.0500:wick=0.0200; 2026-08-25:GREEN:body=+0.0300:wick=0.1000; 2026-08-26:RED:body=-0.0200:wick=0.0500; 2026-08-27:GREEN:body=+0.1400:wick=0.0000; 2026-08-31:RED:body=-0.0200:wick=0.0600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.74 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.12 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1571.3 | **NEUTRAL** |
| `B04_income` | 58.8 | **GOOD** |
| `B05_profit_margin` | 3.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.0 vs prior_export=8.0 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -2.19 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.19 vs prior=-2.19 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 6.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.74 (this export) | prior_export=8.74 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.12 (this export) | prior_export=3.12 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MITK  ·  score **+15**  ·  Software - Application
price=18.459999084472656  pair=`2026-08-27→2026-08-31`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.80 on 2026-08-31; prev RSI=57.70 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.70@2026-08-27 → 51.80@2026-08-31 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.70@2026-08-27 → 51.80@2026-08-31 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.70@2026-08-27 → 51.80@2026-08-31 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_body_sum/RED_body_sum=5.524 (G=1.1600 R=0.2100); 2026-08-27:GREEN:O=18.0500,C=19.2100,body=+1.1600,vol=678600.0; 2026-08-31:RED:O=18.6700,C=18.4600,body=-0.2100,vol=449400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-31; ratio=GREEN_vol/RED_vol=1.510 (Gvol=678600 Rvol=449400); 2026-08-27:GREEN:O=18.0500,C=19.2100,body=+1.1600,vol=678600.0; 2026-08-31:RED:O=18.6700,C=18.4600,body=-0.2100,vol=449400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.593 on 2026-08-31: today_vol=449400 / avg20=758460 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.146 on 2026-08-31 (price=18.4600, mid=18.2545, upper=19.6625, lower=16.8465; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-31: price=18.4600 vs SMA50=18.2602 dist=+1.09% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=18.25_50=18.26_80=17.28 on 2026-08-31: SMA20=18.2545 SMA50=18.2602 SMA80=17.2787 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-31 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-09@14.5000; S2[2026-06-29→2026-07-30] low=2026-07-23@15.8300; S3[2026-07-31→2026-08-31] low=2026-08-06@16.4700 | lows=[14.5, 15.829999923706055, 16.469999313354492] span=13.59% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: GREEN body_frac=0.805555150832809 wick_frac=0.19444484916719096 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-31: RED body_frac=0.3559337923900042 wick_frac=0.6440662076099958 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.523782708603918 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.5700:wick=0.2700; 2026-08-25:RED:body=-0.0300:wick=0.6700; 2026-08-26:RED:body=-0.3700:wick=0.2700; 2026-08-27:GREEN:body=+1.1600:wick=0.2800; 2026-08-31:RED:body=-0.2100:wick=0.3800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.55 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.34 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 197.9 | **NEUTRAL** |
| `B04_income` | 22.54 | **GOOD** |
| `B05_profit_margin` | 11.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.62 vs prior_export=23.62 on finviz_2026-08-31) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -9.72 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.72 vs prior=-9.72 on finviz_2026-08-31) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.74 | **GOOD** |
| `B13_short_float` | 8.5 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.55 (this export) | prior_export=23.55 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.34 (this export) | prior_export=6.34 (finviz_2026-08-31) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-01_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.