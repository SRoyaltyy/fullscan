# A+B1 Feature Checklist — 2026-11-16

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,610** names
- Export: `finviz_2026-11-16.csv` · prior export for Δ: `2026-11-13`
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
| 1 | FOXA | +17 | 17 | 0 | 2026-11-13→2026-11-16 | Entertainment |
| 2 | ABNB | +17 | 18 | 1 | 2026-11-13→2026-11-16 | Travel Services |
| 3 | PBH | +16 | 17 | 1 | 2026-11-13→2026-11-16 | Drug Manufacturers - Specialty & Gen |
| 4 | CBRL | +16 | 17 | 1 | 2026-11-13→2026-11-16 | Restaurants |
| 5 | DRH | +15 | 16 | 1 | 2026-11-13→2026-11-16 | REIT - Hotel & Motel |
| 6 | LFST | +15 | 17 | 2 | 2026-11-13→2026-11-16 | Medical Care Facilities |
| 7 | RPD | +15 | 16 | 1 | 2026-11-13→2026-11-16 | Software - Infrastructure |
| 8 | MSCI | +15 | 16 | 1 | 2026-11-13→2026-11-16 | Financial Data & Stock Exchanges |
| 9 | SBH | +14 | 17 | 3 | 2026-11-13→2026-11-16 | Specialty Retail |
| 10 | KMB | +14 | 14 | 0 | 2026-11-13→2026-11-16 | Household & Personal Products |
| 11 | OMCL | +14 | 15 | 1 | 2026-11-13→2026-11-16 | Health Information Services |
| 12 | PPL | +14 | 15 | 1 | 2026-11-13→2026-11-16 | Utilities - Regulated Electric |
| 13 | ALEC | +14 | 16 | 2 | 2026-11-13→2026-11-16 | Biotechnology |
| 14 | WWW | +14 | 17 | 3 | 2026-11-13→2026-11-16 | Footwear & Accessories |
| 15 | VNT | +14 | 16 | 2 | 2026-11-13→2026-11-16 | Scientific & Technical Instruments |

## Full checklist — top 15

### FOXA  ·  score **+17**  ·  Entertainment
price=67.94999694824219  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.36 on 2026-11-16; prev RSI=56.76 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=2.646 (G=1.6800 R=0.6350); 2026-11-13:GREEN:O=66.7400,C=68.4200,body=+1.6800,vol=6667302.0; 2026-11-16:RED:O=68.5850,C=67.9500,body=-0.6350,vol=5675600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.175 (Gvol=6667302 Rvol=5675600); 2026-11-13:GREEN:O=66.7400,C=68.4200,body=+1.6800,vol=6667302.0; 2026-11-16:RED:O=68.5850,C=67.9500,body=-0.6350,vol=5675600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.593 on 2026-11-16: today_vol=5675600 / avg20=9576184 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.410 on 2026-11-16 (price=67.9500, mid=66.3825, upper=70.2075, lower=62.5575; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=67.9500 vs SMA50=65.1592 dist=+4.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=66.3825 SMA50=65.1592 SMA80=64.5067 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@62.9300; S2[2026-09-15→2026-10-16] low=2026-10-01@60.2600; S3[2026-10-19→2026-11-16] low=2026-11-03@62.1000 | lows=[62.93000030517578, 60.2599983215332, 62.099998474121094] span=4.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.7691241036531482 wick_frac=0.23087589634685174 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.31909689341456027 wick_frac=0.6809031065854397 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.645660871550264 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.2200:wick=1.6200; 2026-11-11:GREEN:body=+0.7300:wick=0.8199; 2026-11-12:RED:body=-1.8700:wick=0.6100; 2026-11-13:GREEN:body=+1.6800:wick=0.5043; 2026-11-16:RED:body=-0.6350:wick=1.3550 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=23.12 (current export asof; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.74 (current export; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17959.0 | **NEUTRAL** |
| `B04_income` | 2029.0 | **GOOD** |
| `B05_profit_margin` | 11.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 81.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.480000000000004 (now=81.72 vs prior_export=81.24 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.31 | **GOOD** |
| `B13_short_float` | 14.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.12 (this export) | prior_export=23.12 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.74 (this export) | prior_export=5.74 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ABNB  ·  score **+17**  ·  Travel Services
price=186.19000244140625  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.23 on 2026-11-16; prev RSI=60.88 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.0250 R=0.0000); 2026-11-13:GREEN:O=182.6650,C=186.7800,body=+4.1150,vol=4949070.0; 2026-11-16:GREEN:O=184.2800,C=186.1900,body=+1.9100,vol=7216300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12165370 Rvol=0); 2026-11-13:GREEN:O=182.6650,C=186.7800,body=+4.1150,vol=4949070.0; 2026-11-16:GREEN:O=184.2800,C=186.1900,body=+1.9100,vol=7216300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.585 on 2026-11-16: today_vol=7216300 / avg20=4553408 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.597 on 2026-11-16 (price=186.1900, mid=176.1068, upper=192.9950, lower=159.2185; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=186.1900 vs SMA50=169.2045 dist=+10.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=176.11_50=169.20_80=171.23 on 2026-11-16: SMA20=176.1068 SMA50=169.2045 SMA80=171.2323 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@165.0000; S2[2026-09-18→2026-10-16] low=2026-09-23@149.0000; S3[2026-10-19→2026-11-16] low=2026-10-30@164.1000 | lows=[165.0, 149.0, 164.10000610351562] span=10.74% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.507175531102692 wick_frac=0.49282446889730813 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.1000:wick=5.0900; 2026-11-11:RED:body=-1.5700:wick=3.5769; 2026-11-12:GREEN:body=+1.5250:wick=3.5850; 2026-11-13:GREEN:body=+4.1150:wick=1.1050; 2026-11-16:GREEN:body=+1.9100:wick=6.5400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.8 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 13958.0 | **NEUTRAL** |
| `B04_income` | 3207.0 | **GOOD** |
| `B05_profit_margin` | 22.98 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 206.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.039999999999992 (now=206.57 vs prior_export=205.53 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 1.98 | **GOOD** |
| `B10_insider_transactions` | -3.29 | **BAD** |
| `B11_insider_tx_delta` | delta=0.029999999999999805 (now=-3.29 vs prior=-3.32 on finviz_2026-11-13) | **GOOD** |
| `B12_institutional_transactions` | 1.16 | **GOOD** |
| `B13_short_float` | 3.21 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.8 (this export) | prior_export=10.8 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBH  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=48.0  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.44 on 2026-11-16; prev RSI=47.48 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8000 R=0.0000); 2026-11-13:GREEN:O=45.6900,C=46.1900,body=+0.5000,vol=500400.0; 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1031500 Rvol=0); 2026-11-13:GREEN:O=45.6900,C=46.1900,body=+0.5000,vol=500400.0; 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.838 on 2026-11-16: today_vol=531100 / avg20=633628 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.119 on 2026-11-16 (price=48.0000, mid=47.3925, upper=52.4980, lower=42.2870; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=48.0000 vs SMA50=47.5500 dist=+0.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-16: SMA20=47.3925 SMA50=47.5500 SMA80=49.0785 | **BAD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-11@46.4800; S2[2026-09-15→2026-10-16] low=2026-09-21@44.4600; S3[2026-10-19→2026-11-16] low=2026-11-06@41.7700 | lows=[46.47999954223633, 44.459999084472656, 41.77000045776367] span=11.28% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.6130989489588102 wick_frac=0.38690105104118977 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+1.4800:wick=0.6000; 2026-11-11:RED:body=-0.1000:wick=0.9927; 2026-11-12:GREEN:body=+0.6000:wick=0.8700; 2026-11-13:GREEN:body=+0.5000:wick=0.6800; 2026-11-16:GREEN:body=+1.3000:wick=0.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.32 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1167.08 | **NEUTRAL** |
| `B04_income` | 155.1 | **GOOD** |
| `B05_profit_margin` | 13.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.25 vs prior_export=65.25 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.05 (now=0.0 vs prior=-0.05 on finviz_2026-11-13) | **GOOD** |
| `B12_institutional_transactions` | 4.06 | **GOOD** |
| `B13_short_float` | 9.09 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=67.9800033569336  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.57 on 2026-11-16; prev RSI=54.50 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=4.429 (G=1.2400 R=0.2800); 2026-11-13:GREEN:O=65.7100,C=66.9500,body=+1.2400,vol=549900.0; 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.209 (Gvol=549900 Rvol=454700); 2026-11-13:GREEN:O=65.7100,C=66.9500,body=+1.2400,vol=549900.0; 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.639 on 2026-11-16: today_vol=454700 / avg20=711850 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.033 on 2026-11-16 (price=67.9800, mid=67.8435, upper=72.0056, lower=63.6814; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=67.9800 vs SMA50=59.3040 dist=+14.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=67.8435 SMA50=59.3040 SMA80=58.2534 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@42.8700; S2[2026-09-18→2026-10-16] low=2026-09-18@43.7000; S3[2026-10-19→2026-11-16] low=2026-11-11@63.4800 | lows=[42.869998931884766, 43.70000076293945, 63.47999954223633] span=48.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.590475602268475 wick_frac=0.40952439773152505 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.10937462747061488 wick_frac=0.8906253725293851 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.42858310626703 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.1700:wick=0.8800; 2026-11-11:GREEN:body=+1.7900:wick=1.2400; 2026-11-12:RED:body=-1.1600:wick=2.3400; 2026-11-13:GREEN:body=+1.2400:wick=0.8600; 2026-11-16:RED:body=-0.2800:wick=2.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=469.95 (current export asof; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3318.71 | **NEUTRAL** |
| `B04_income` | 31.68 | **GOOD** |
| `B05_profit_margin` | 0.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.71 vs prior_export=50.71 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | -0.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.71 vs prior=-0.71 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.18 | **GOOD** |
| `B13_short_float` | 23.32 | **GOOD** |
| `B14_earnings_date` | 9/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=469.95 (this export) | prior_export=469.95 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRH  ·  score **+15**  ·  REIT - Hotel & Motel
price=14.5600004196167  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.25 on 2026-11-16; prev RSI=60.75 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.75@2026-11-13 → 63.25@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.75@2026-11-13 → 63.25@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.75@2026-11-13 → 63.25@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=16.000 (G=0.1600 R=0.0100); 2026-11-13:RED:O=14.4100,C=14.4000,body=-0.0100,vol=2490500.0; 2026-11-16:GREEN:O=14.4000,C=14.5600,body=+0.1600,vol=2597200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.043 (Gvol=2597200 Rvol=2490500); 2026-11-13:RED:O=14.4100,C=14.4000,body=-0.0100,vol=2490500.0; 2026-11-16:GREEN:O=14.4000,C=14.5600,body=+0.1600,vol=2597200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.780 on 2026-11-16: today_vol=2597200 / avg20=3331506 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.498 on 2026-11-16 (price=14.5600, mid=14.2575, upper=14.8646, lower=13.6504; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=14.5600 vs SMA50=13.1555 dist=+10.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=14.2575 SMA50=13.1555 SMA80=12.9836 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-09@11.8500; S2[2026-09-15→2026-10-16] low=2026-09-16@12.0500; S3[2026-10-19→2026-11-16] low=2026-10-19@13.3300 | lows=[11.850000381469727, 12.050000190734863, 13.329999923706055] span=12.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5333356645357358 wick_frac=0.4666643354642643 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.03448355240441058 wick_frac=0.9655164475955894 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.99971390425329 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.3700:wick=0.1300; 2026-11-11:RED:body=-0.1500:wick=0.0600; 2026-11-12:RED:body=-0.3000:wick=0.0950; 2026-11-13:RED:body=-0.0100:wick=0.2800; 2026-11-16:GREEN:body=+0.1600:wick=0.1400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.12 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.89 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1148.59 | **NEUTRAL** |
| `B04_income` | 162.03 | **GOOD** |
| `B05_profit_margin` | 14.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.69 vs prior_export=14.69 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -1.46 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.46 vs prior=-1.46 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.07 | **GOOD** |
| `B13_short_float` | 5.88 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.12 (this export) | prior_export=5.12 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.89 (this export) | prior_export=1.89 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LFST  ·  score **+15**  ·  Medical Care Facilities
price=14.479999542236328  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.19 on 2026-11-16; prev RSI=60.16 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=5.286 (G=0.3700 R=0.0700); 2026-11-13:RED:O=14.1500,C=14.0800,body=-0.0700,vol=3728704.0; 2026-11-16:GREEN:O=14.1100,C=14.4800,body=+0.3700,vol=3768600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.011 (Gvol=3768600 Rvol=3728704); 2026-11-13:RED:O=14.1500,C=14.0800,body=-0.0700,vol=3728704.0; 2026-11-16:GREEN:O=14.1100,C=14.4800,body=+0.3700,vol=3768600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.568 on 2026-11-16: today_vol=3768600 / avg20=6638183 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.643 on 2026-11-16 (price=14.4800, mid=13.5453, upper=14.9987, lower=12.0918; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=14.4800 vs SMA50=12.8255 dist=+12.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=13.5453 SMA50=12.8255 SMA80=12.4538 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-08-25@11.9150; S2[2026-09-18→2026-10-16] low=2026-10-02@11.4700; S3[2026-10-19→2026-11-16] low=2026-10-19@11.6900 | lows=[11.914999961853027, 11.470000267028809, 11.6899995803833] span=3.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.7872333517980808 wick_frac=0.21276664820191912 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.11232309877087313 wick_frac=0.8876769012291269 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.285735694822888 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+0.1700:wick=0.4200; 2026-11-11:RED:body=-0.3100:wick=0.3510; 2026-11-12:RED:body=-0.1900:wick=0.3192; 2026-11-13:RED:body=-0.0700:wick=0.5532; 2026-11-16:GREEN:body=+0.3700:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=62.6 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.62 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1676.89 | **NEUTRAL** |
| `B04_income` | 71.91 | **GOOD** |
| `B05_profit_margin` | 4.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.45 | **GOOD** |
| `B10_insider_transactions` | -36.02 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.05000000000000426 (now=-36.02 vs prior=-35.97 on finviz_2026-11-13) | **BAD** |
| `B12_institutional_transactions` | 4.24 | **GOOD** |
| `B13_short_float` | 5.3 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=62.6 (this export) | prior_export=62.6 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.62 (this export) | prior_export=5.62 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RPD  ·  score **+15**  ·  Software - Infrastructure
price=14.6899995803833  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.30 on 2026-11-16; prev RSI=49.14 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=1.708 (G=0.4100 R=0.2400); 2026-11-13:RED:O=14.6500,C=14.4100,body=-0.2400,vol=2061580.0; 2026-11-16:GREEN:O=14.2800,C=14.6900,body=+0.4100,vol=1067400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=0.518 (Gvol=1067400 Rvol=2061580); 2026-11-13:RED:O=14.6500,C=14.4100,body=-0.2400,vol=2061580.0; 2026-11-16:GREEN:O=14.2800,C=14.6900,body=+0.4100,vol=1067400.0 | **BAD** |
| `A07_rvol` | RVOL=0.507 on 2026-11-16: today_vol=1067400 / avg20=2106540 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.223 on 2026-11-16 (price=14.6900, mid=15.2907, upper=17.9867, lower=12.5948; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=14.6900 vs SMA50=13.4710 dist=+9.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=15.2907 SMA50=13.4710 SMA80=12.7715 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-11@10.1200; S2[2026-09-18→2026-10-16] low=2026-09-28@11.1300; S3[2026-10-19→2026-11-16] low=2026-10-19@12.5900 | lows=[10.119999885559082, 11.130000114440918, 12.59000015258789] span=24.41% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5222929007741164 wick_frac=0.47770709922588356 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.23837864104339557 wick_frac=0.7616213589566044 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.708334326745027 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-0.0300:wick=1.2100; 2026-11-11:RED:body=-0.1700:wick=0.5898; 2026-11-12:GREEN:body=+0.0060:wick=0.8653; 2026-11-13:RED:body=-0.2400:wick=0.7668; 2026-11-16:GREEN:body=+0.4100:wick=0.3750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.43 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.6 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 850.37 | **NEUTRAL** |
| `B04_income` | 13.84 | **GOOD** |
| `B05_profit_margin` | 1.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.97 vs prior_export=15.97 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 3.32 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.26 | **GOOD** |
| `B13_short_float` | 10.46 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.43 (this export) | prior_export=31.43 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.6 (this export) | prior_export=1.6 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSCI  ·  score **+15**  ·  Financial Data & Stock Exchanges
price=664.489990234375  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.40 on 2026-11-16; prev RSI=64.74 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.74@2026-11-13 → 66.40@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.74@2026-11-13 → 66.40@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.74@2026-11-13 → 66.40@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.4199 R=0.0000); 2026-11-13:GREEN:O=656.0900,C=658.1700,body=+2.0800,vol=408900.0; 2026-11-16:GREEN:O=660.1500,C=664.4900,body=+4.3400,vol=633700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1042600 Rvol=0); 2026-11-13:GREEN:O=656.0900,C=658.1700,body=+2.0800,vol=408900.0; 2026-11-16:GREEN:O=660.1500,C=664.4900,body=+4.3400,vol=633700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.968 on 2026-11-16: today_vol=633700 / avg20=654379 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.624 on 2026-11-16 (price=664.4900, mid=634.2760, upper=682.7126, lower=585.8394; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=664.4900 vs SMA50=585.8956 dist=+13.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=634.2760 SMA50=585.8956 SMA80=578.7380 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-16@537.3600; S2[2026-09-18→2026-10-16] low=2026-09-29@533.4600; S3[2026-10-19→2026-11-16] low=2026-10-19@558.5750 | lows=[537.3599853515625, 533.4600219726562, 558.5750122070312] span=4.71% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.39928065592303663 wick_frac=0.6007193440769634 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+7.7200:wick=6.3600; 2026-11-11:GREEN:body=+9.2100:wick=6.8893; 2026-11-12:RED:body=-10.0000:wick=-1.5700; 2026-11-13:GREEN:body=+2.0800:wick=6.0001; 2026-11-16:GREEN:body=+4.3400:wick=3.6801 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.65 (current export asof; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.47 (current export; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3432.33 | **NEUTRAL** |
| `B04_income` | 1383.67 | **GOOD** |
| `B05_profit_margin` | 40.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 696.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.5599999999999454 (now=696.78 vs prior_export=693.22 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | 0.1 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.1 vs prior=0.1 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 1.99 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.65 (this export) | prior_export=1.65 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.47 (this export) | prior_export=0.47 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBH  ·  score **+14**  ·  Specialty Retail
price=18.75  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.84 on 2026-11-16; prev RSI=56.34 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.34@2026-11-13 → 54.84@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.34@2026-11-13 → 54.84@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.34@2026-11-13 → 54.84@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=1.238 (G=0.5200 R=0.4200); 2026-11-13:GREEN:O=18.4400,C=18.9600,body=+0.5200,vol=1339600.0; 2026-11-16:RED:O=19.1700,C=18.7500,body=-0.4200,vol=1068500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.254 (Gvol=1339600 Rvol=1068500); 2026-11-13:GREEN:O=18.4400,C=18.9600,body=+0.5200,vol=1339600.0; 2026-11-16:RED:O=19.1700,C=18.7500,body=-0.4200,vol=1068500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.711 on 2026-11-16: today_vol=1068500 / avg20=1503181 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.258 on 2026-11-16 (price=18.7500, mid=18.2520, upper=20.1818, lower=16.3222; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=18.7500 vs SMA50=17.0602 dist=+9.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=18.2520 SMA50=17.0602 SMA80=16.7561 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@15.5700; S2[2026-09-18→2026-10-16] low=2026-09-18@15.4600; S3[2026-10-19→2026-11-16] low=2026-10-20@16.3600 | lows=[15.569999694824219, 15.460000038146973, 16.360000610351562] span=5.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5531896329573424 wick_frac=0.44681036704265764 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.5600001017252604 wick_frac=0.43999989827473956 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2380915618003552 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+2.1900:wick=0.5600; 2026-11-11:RED:body=-1.0450:wick=0.2850; 2026-11-12:RED:body=-0.8500:wick=0.0700; 2026-11-13:GREEN:body=+0.5200:wick=0.4200; 2026-11-16:RED:body=-0.4200:wick=0.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.33 (current export asof; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.25 (current export; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3734.57 | **NEUTRAL** |
| `B04_income` | 200.08 | **GOOD** |
| `B05_profit_margin` | 5.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.6000000000000014 (now=22.0 vs prior_export=21.4 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -8.01 | **BAD** |
| `B11_insider_tx_delta` | delta=-4.779999999999999 (now=-8.01 vs prior=-3.23 on finviz_2026-11-13) | **BAD** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 20.45 | **GOOD** |
| `B14_earnings_date` | 11/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.33 (this export) | prior_export=3.33 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.25 (this export) | prior_export=0.25 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KMB  ·  score **+14**  ·  Household & Personal Products
price=111.33999633789062  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.40 on 2026-11-16; prev RSI=55.16 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.16@2026-11-13 → 58.40@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.16@2026-11-13 → 58.40@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.16@2026-11-13 → 58.40@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.9100 R=0.0000); 2026-11-13:GREEN:O=107.6600,C=109.4300,body=+1.7700,vol=4760600.0; 2026-11-16:GREEN:O=110.2000,C=111.3400,body=+1.1400,vol=6816200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=11576800 Rvol=0); 2026-11-13:GREEN:O=107.6600,C=109.4300,body=+1.7700,vol=4760600.0; 2026-11-16:GREEN:O=110.2000,C=111.3400,body=+1.1400,vol=6816200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.953 on 2026-11-16: today_vol=6816200 / avg20=7149906 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.598 on 2026-11-16 (price=111.3400, mid=109.1213, upper=112.8284, lower=105.4141; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=111.3400 vs SMA50=102.8401 dist=+8.27% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=109.12_50=102.84_80=105.36 on 2026-11-16: SMA20=109.1213 SMA50=102.8401 SMA80=105.3587 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-15@97.1900; S2[2026-09-18→2026-10-16] low=2026-10-05@93.9450; S3[2026-10-19→2026-11-16] low=2026-10-19@103.8850 | lows=[97.19000244140625, 93.94499969482422, 103.88500213623047] span=10.58% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.7581461142170558 wick_frac=0.2418538857829442 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.0400:wick=1.7200; 2026-11-11:RED:body=-0.3200:wick=1.6650; 2026-11-12:RED:body=-2.2700:wick=0.5500; 2026-11-13:GREEN:body=+1.7700:wick=0.4700; 2026-11-16:GREEN:body=+1.1400:wick=0.4300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.86 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16566.0 | **NEUTRAL** |
| `B04_income` | 3302.0 | **GOOD** |
| `B05_profit_margin` | 19.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 117.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=117.79 vs prior_export=117.79 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.67 | **GOOD** |
| `B13_short_float` | 8.63 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.86 (this export) | prior_export=2.86 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.18 (this export) | prior_export=0.18 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OMCL  ·  score **+14**  ·  Health Information Services
price=35.439998626708984  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.95 on 2026-11-16; prev RSI=44.72 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.72@2026-11-13 → 46.95@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 44.72@2026-11-13 → 46.95@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 44.72@2026-11-13 → 46.95@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=18.665 (G=0.5600 R=0.0300); 2026-11-13:RED:O=35.0600,C=35.0300,body=-0.0300,vol=313200.0; 2026-11-16:GREEN:O=34.8800,C=35.4400,body=+0.5600,vol=445800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.423 (Gvol=445800 Rvol=313200); 2026-11-13:RED:O=35.0600,C=35.0300,body=-0.0300,vol=313200.0; 2026-11-16:GREEN:O=34.8800,C=35.4400,body=+0.5600,vol=445800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.718 on 2026-11-16: today_vol=445800 / avg20=621308 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.525 on 2026-11-16 (price=35.4400, mid=36.5890, upper=38.7792, lower=34.3988; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=35.4400 vs SMA50=35.0048 dist=+1.24% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.59_50=35.00_80=35.92 on 2026-11-16: SMA20=36.5890 SMA50=35.0048 SMA80=35.9178 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@32.1200; S2[2026-09-15→2026-10-16] low=2026-09-21@31.9800; S3[2026-10-19→2026-11-16] low=2026-11-16@34.4500 | lows=[32.119998931884766, 31.979999542236328, 34.45000076293945] span=7.72% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.504502027630765 wick_frac=0.495497972369235 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.05000445049146141 wick_frac=0.9499955495085386 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.66497139224412 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.1000:wick=0.9960; 2026-11-11:RED:body=-0.2200:wick=0.6700; 2026-11-12:RED:body=-0.6500:wick=0.3750; 2026-11-13:RED:body=-0.0300:wick=0.5700; 2026-11-16:GREEN:body=+0.5600:wick=0.5500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=38.92 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1243.92 | **NEUTRAL** |
| `B04_income` | 44.66 | **GOOD** |
| `B05_profit_margin` | 3.59 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 57.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=57.43 vs prior_export=57.43 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | -1.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.34 vs prior=-1.34 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.63 | **GOOD** |
| `B13_short_float` | 8.49 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=38.92 (this export) | prior_export=38.92 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PPL  ·  score **+14**  ·  Utilities - Regulated Electric
price=35.79999923706055  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.67 on 2026-11-16; prev RSI=58.19 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=3.875 (G=0.3100 R=0.0800); 2026-11-13:GREEN:O=35.3600,C=35.6700,body=+0.3100,vol=5891200.0; 2026-11-16:RED:O=35.8800,C=35.8000,body=-0.0800,vol=5731400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.028 (Gvol=5891200 Rvol=5731400); 2026-11-13:GREEN:O=35.3600,C=35.6700,body=+0.3100,vol=5891200.0; 2026-11-16:RED:O=35.8800,C=35.8000,body=-0.0800,vol=5731400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.652 on 2026-11-16: today_vol=5731400 / avg20=8787981 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.408 on 2026-11-16 (price=35.8000, mid=35.4420, upper=36.3196, lower=34.5644; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=35.8000 vs SMA50=34.3396 dist=+4.25% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=35.44_50=34.34_80=34.69 on 2026-11-16: SMA20=35.4420 SMA50=34.3396 SMA80=34.6931 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-14@33.6700; S2[2026-09-15→2026-10-16] low=2026-09-25@31.5550; S3[2026-10-19→2026-11-16] low=2026-10-20@33.9100 | lows=[33.66999816894531, 31.55500030517578, 33.90999984741211] span=7.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.6739090773389945 wick_frac=0.3260909226610054 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.2285776566757493 wick_frac=0.7714223433242506 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.8748807934388707 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.2200:wick=0.3600; 2026-11-11:RED:body=-0.1900:wick=0.1850; 2026-11-12:RED:body=-0.3300:wick=0.2000; 2026-11-13:GREEN:body=+0.3100:wick=0.1500; 2026-11-16:RED:body=-0.0800:wick=0.2700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.73 (current export asof; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.5 (current export; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9657.0 | **NEUTRAL** |
| `B04_income` | 1314.0 | **GOOD** |
| `B05_profit_margin` | 13.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.12 vs prior_export=40.12 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.78 vs prior=-0.78 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.58 | **GOOD** |
| `B13_short_float` | 4.58 | **NEUTRAL** |
| `B14_earnings_date` | 11/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.73 (this export) | prior_export=1.73 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.5 (this export) | prior_export=3.5 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ALEC  ·  score **+14**  ·  Biotechnology
price=2.569999933242798  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.32 on 2026-11-16; prev RSI=61.53 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.53@2026-11-13 → 62.32@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.53@2026-11-13 → 62.32@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.53@2026-11-13 → 62.32@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=1.500 (G=0.0300 R=0.0200); 2026-11-13:RED:O=2.5600,C=2.5400,body=-0.0200,vol=1793164.0; 2026-11-16:GREEN:O=2.5400,C=2.5700,body=+0.0300,vol=2769700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.545 (Gvol=2769700 Rvol=1793164); 2026-11-13:RED:O=2.5600,C=2.5400,body=-0.0200,vol=1793164.0; 2026-11-16:GREEN:O=2.5400,C=2.5700,body=+0.0300,vol=2769700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.398 on 2026-11-16: today_vol=2769700 / avg20=1980936 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.688 on 2026-11-16 (price=2.5700, mid=2.2085, upper=2.7343, lower=1.6827; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=2.5700 vs SMA50=2.1128 dist=+21.64% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=2.2085 SMA50=2.1128 SMA80=2.0451 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-17@1.9000; S2[2026-09-15→2026-10-16] low=2026-10-07@1.7300; S3[2026-10-19→2026-11-16] low=2026-10-23@1.7200 | lows=[1.899999976158142, 1.7300000190734863, 1.7200000286102295] span=10.47% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.21428571428571427 wick_frac=0.7857142857142857 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.0689654605425421 wick_frac=0.9310345394574578 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.1300:wick=0.1250; 2026-11-11:GREEN:body=+0.0900:wick=0.2100; 2026-11-12:RED:body=-0.2100:wick=0.0600; 2026-11-13:RED:body=-0.0200:wick=0.2700; 2026-11-16:GREEN:body=+0.0300:wick=0.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=591.37 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6743.11 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 173.47 | **NEUTRAL** |
| `B04_income` | 41.41 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.52 (now=4.42 vs prior_export=3.9 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | -0.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.57 vs prior=-0.57 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.03 | **GOOD** |
| `B13_short_float` | 7.67 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=591.37 (this export) | prior_export=591.37 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6743.11 (this export) | prior_export=6743.11 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WWW  ·  score **+14**  ·  Footwear & Accessories
price=25.290000915527344  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.92 on 2026-11-16; prev RSI=71.66 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 71.66@2026-11-13 → 72.92@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.66@2026-11-13 → 72.92@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 71.66@2026-11-13 → 72.92@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7800 R=0.0000); 2026-11-13:GREEN:O=23.4000,C=24.8900,body=+1.4900,vol=2571900.0; 2026-11-16:GREEN:O=25.0000,C=25.2900,body=+0.2900,vol=2403100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4975000 Rvol=0); 2026-11-13:GREEN:O=23.4000,C=24.8900,body=+1.4900,vol=2571900.0; 2026-11-16:GREEN:O=25.0000,C=25.2900,body=+0.2900,vol=2403100.0 | **GOOD** |
| `A07_rvol` | RVOL=2.312 on 2026-11-16: today_vol=2403100 / avg20=1039460 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.188 on 2026-11-16 (price=25.2900, mid=21.5195, upper=24.6925, lower=18.3465; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-16: price=25.2900 vs SMA50=20.2532 dist=+24.87% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=21.5195 SMA50=20.2532 SMA80=20.0579 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-16 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-15@18.9500; S2[2026-09-16→2026-10-16] low=2026-10-08@18.5100; S3[2026-10-19→2026-11-16] low=2026-10-28@19.3200 | lows=[18.950000762939453, 18.510000228881836, 19.31999969482422] span=4.38% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5389164082649271 wick_frac=0.4610835917350728 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:RED:body=-0.2200:wick=0.7800; 2026-11-11:GREEN:body=+0.4400:wick=0.3650; 2026-11-12:GREEN:body=+0.9400:wick=1.8398; 2026-11-13:GREEN:body=+1.4900:wick=0.5800; 2026-11-16:GREEN:body=+0.2900:wick=0.5200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=72.53 (current export asof; earnings_date=11/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.06 (current export; earnings_date=11/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1986.9 | **NEUTRAL** |
| `B04_income` | 145.3 | **GOOD** |
| `B05_profit_margin` | 7.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.1999999999999993 (now=25.8 vs prior_export=24.6 on finviz_2026-11-13) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -4.08 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.08 vs prior=-4.08 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.13 | **GOOD** |
| `B13_short_float` | 6.42 | **NEUTRAL** |
| `B14_earnings_date` | 11/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.53 (this export) | prior_export=72.53 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.06 (this export) | prior_export=1.06 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+14**  ·  Scientific & Technical Instruments
price=43.5  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.00 on 2026-11-16; prev RSI=80.13 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=3.600 (G=0.1800 R=0.0500); 2026-11-13:GREEN:O=43.4600,C=43.6400,body=+0.1800,vol=1515428.0; 2026-11-16:RED:O=43.5500,C=43.5000,body=-0.0500,vol=1513300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.001 (Gvol=1515428 Rvol=1513300); 2026-11-13:GREEN:O=43.4600,C=43.6400,body=+0.1800,vol=1515428.0; 2026-11-16:RED:O=43.5500,C=43.5000,body=-0.0500,vol=1513300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.876 on 2026-11-16: today_vol=1513300 / avg20=1728385 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.644 on 2026-11-16 (price=43.5000, mid=38.3535, upper=46.3460, lower=30.3610; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=43.5000 vs SMA50=34.5268 dist=+25.99% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=38.3535 SMA50=34.5268 SMA80=33.8500 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@31.1100; S2[2026-09-15→2026-10-16] low=2026-09-24@30.4250; S3[2026-10-19→2026-11-16] low=2026-10-19@32.9300 | lows=[31.110000610351562, 30.424999237060547, 32.93000030517578] span=8.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.3050851841075874 wick_frac=0.6949148158924127 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.0709211031810877 wick_frac=0.9290788968189123 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.600061036087587 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-0.1200:wick=2.0268; 2026-11-11:GREEN:body=+0.7900:wick=0.5350; 2026-11-12:RED:body=-0.5800:wick=0.1500; 2026-11-13:GREEN:body=+0.1800:wick=0.4100; 2026-11-16:RED:body=-0.0500:wick=0.6550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=46.56 vs prior_export=46.56 on finviz_2026-11-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.96 | **GOOD** |
| `B13_short_float` | 9.18 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-16_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.