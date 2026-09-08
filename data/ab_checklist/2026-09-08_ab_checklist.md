# A+B1 Feature Checklist — 2026-09-08

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,666** names
- Export: `finviz_2026-09-08.csv` · prior export for Δ: `2026-09-07`
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
| 1 | LEU | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Uranium |
| 2 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 3 | AHR | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 4 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 5 | SON | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 6 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 7 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 8 | GPK | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 9 | CP | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Railroads |
| 10 | CXW | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Security & Protection Services |
| 11 | FCX | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Copper |
| 12 | WPM | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Gold |
| 13 | AVNT | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Specialty Chemicals |
| 14 | RYAN | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Insurance Brokers |
| 15 | RUSHA | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Auto & Truck Dealerships |

## Full checklist — top 15

### LEU  ·  score **+17**  ·  Uranium
price=186.25999450683594  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.85 on 2026-08-21; prev RSI=48.28 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.724 (G=7.8600 R=4.5600); 2026-08-20:RED:O=180.6900,C=176.1300,body=-4.5600,vol=425200.0; 2026-08-21:GREEN:O=178.4000,C=186.2600,body=+7.8600,vol=458200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.078 (Gvol=458200 Rvol=425200); 2026-08-20:RED:O=180.6900,C=176.1300,body=-4.5600,vol=425200.0; 2026-08-21:GREEN:O=178.4000,C=186.2600,body=+7.8600,vol=458200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.759 on 2026-08-21: today_vol=458200 / avg20=603300 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.252 on 2026-08-21 (price=186.2600, mid=182.0740, upper=198.6958, lower=165.4522; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=186.2600 vs SMA50=173.0796 dist=+7.62% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=182.07_50=173.08_80=178.60 on 2026-08-21: SMA20=182.0740 SMA50=173.0796 SMA80=178.6011 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-10@144.6500; S2[2026-06-24→2026-07-23] low=2026-07-17@142.1300; S3[2026-07-24→2026-08-21] low=2026-07-28@157.8900 | lows=[144.64999389648438, 142.1300048828125, 157.88999938964844] span=11.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7987808471112187 wick_frac=0.20121915288878137 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.41988925435669316 wick_frac=0.5801107456433069 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7236852672297251 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-4.8800:wick=2.6600; 2026-08-18:RED:body=-2.1500:wick=7.0700; 2026-08-19:GREEN:body=+6.8200:wick=4.8000; 2026-08-20:RED:body=-4.5600:wick=6.3000; 2026-08-21:GREEN:body=+7.8600:wick=1.9800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=17.52 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 473.9 | **NEUTRAL** |
| `B04_income` | 48.5 | **GOOD** |
| `B05_profit_margin` | 10.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 243.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=243.93 vs prior_export=243.93 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.05 | **GOOD** |
| `B13_short_float` | 29.02 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BLMN  ·  score **+16**  ·  Restaurants
price=11.630000114440918  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.89 on 2026-08-21; prev RSI=58.18 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0500 R=0.0000); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3319300 Rvol=0); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.839 on 2026-08-21: today_vol=1849400 / avg20=2203390 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.614 on 2026-08-21 (price=11.6300, mid=10.1665, upper=12.5510, lower=7.7820; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=11.6300 vs SMA50=9.0324 dist=+28.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=10.1665 SMA50=9.0324 SMA80=8.4386 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-08@7.0300; S2[2026-06-22→2026-07-22] low=2026-07-08@7.6600; S3[2026-07-23→2026-08-21] low=2026-07-24@7.9100 | lows=[7.03000020980835, 7.659999847412109, 7.909999847412109] span=12.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5011365148646267 wick_frac=0.4988634851353733 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.2700:wick=0.3000; 2026-08-18:RED:body=-0.0100:wick=0.4200; 2026-08-19:GREEN:body=+0.0100:wick=0.5000; 2026-08-20:GREEN:body=+0.0300:wick=0.3700; 2026-08-21:GREEN:body=+1.0200:wick=0.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=35.18 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3979.52 | **NEUTRAL** |
| `B04_income` | 29.38 | **GOOD** |
| `B05_profit_margin` | 0.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.78 | **GOOD** |
| `B13_short_float` | 9.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AHR  ·  score **+16**  ·  REIT - Healthcare Facilities
price=55.93000030517578  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.58 on 2026-08-21; prev RSI=55.93 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=4.750 (G=0.7600 R=0.1600); 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0; 2026-08-21:RED:O=56.0900,C=55.9300,body=-0.1600,vol=1992700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.148 (Gvol=2287500 Rvol=1992700); 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0; 2026-08-21:RED:O=56.0900,C=55.9300,body=-0.1600,vol=1992700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.673 on 2026-08-21: today_vol=1992700 / avg20=2960060 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.191 on 2026-08-21 (price=55.9300, mid=55.3530, upper=58.3667, lower=52.3393; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=55.9300 vs SMA50=52.8823 dist=+5.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=55.3530 SMA50=52.8823 SMA80=51.6333 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-08@44.5946; S2[2026-06-18→2026-07-20] low=2026-06-18@45.5400; S3[2026-07-23→2026-08-21] low=2026-08-11@52.3300 | lows=[44.59456890815638, 45.539996507535484, 52.33000183105469] span=17.35% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6608705459322113 wick_frac=0.3391294540677887 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.131147284688696 wick_frac=0.868852715311304 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.750017881410486 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.2500:wick=0.4100; 2026-08-18:RED:body=-0.6800:wick=0.3300; 2026-08-19:RED:body=-0.0400:wick=0.9100; 2026-08-20:GREEN:body=+0.7600:wick=0.3900; 2026-08-21:RED:body=-0.1600:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.62 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.98 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2502.39 | **NEUTRAL** |
| `B04_income` | 121.02 | **GOOD** |
| `B05_profit_margin` | 4.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 64.4 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=64.4 vs prior_export=64.4 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.89 vs prior=-1.89 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.01 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.84 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 62.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=62.89 vs prior_export=62.89 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 1.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.49 vs prior=1.49 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.79 | **GOOD** |
| `B13_short_float` | 10.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+16**  ·  Computer Hardware
price=188.64999389648438  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.73 on 2026-08-21; prev RSI=48.51 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.128 (G=4.8800 R=1.5600); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8123600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.931 (Gvol=8123600 Rvol=4207300); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8123600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.017 on 2026-08-21: today_vol=8123600 / avg20=7987355 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.026 on 2026-08-21 (price=188.6500, mid=187.9825, upper=214.0069, lower=161.9581; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=188.6500 vs SMA50=177.3556 dist=+6.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=187.9825 SMA50=177.3556 SMA80=168.9896 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-09@145.3200; S2[2026-06-24→2026-07-23] low=2026-06-26@154.7400; S3[2026-07-24→2026-08-21] low=2026-07-29@156.8400 | lows=[145.32000732421875, 154.74000549316406, 156.83999633789062] span=7.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7124066096114693 wick_frac=0.28759339038853077 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.3505614895331493 wick_frac=0.6494385104668506 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.128203372588912 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.6400:wick=3.2850; 2026-08-18:RED:body=-2.8200:wick=5.5000; 2026-08-19:RED:body=-8.5900:wick=2.4000; 2026-08-20:RED:body=-1.5600:wick=2.8900; 2026-08-21:GREEN:body=+4.8800:wick=1.9700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 248.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=248.86 vs prior_export=248.86 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -3.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.1 vs prior=-3.1 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.66 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+16**  ·  Marine Shipping
price=30.5  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.24 on 2026-08-21; prev RSI=59.81 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.426 (G=1.0000 R=0.1843); 2026-08-20:RED:O=29.3443,C=29.1600,body=-0.1843,vol=1605200.0; 2026-08-21:GREEN:O=29.5000,C=30.5000,body=+1.0000,vol=2808200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.749 (Gvol=2808200 Rvol=1605200); 2026-08-20:RED:O=29.3443,C=29.1600,body=-0.1843,vol=1605200.0; 2026-08-21:GREEN:O=29.5000,C=30.5000,body=+1.0000,vol=2808200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.868 on 2026-08-21: today_vol=2808200 / avg20=1503535 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.220 on 2026-08-21 (price=30.5000, mid=28.5647, upper=30.1513, lower=26.9781; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=30.5000 vs SMA50=27.0602 dist=+12.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=28.5647 SMA50=27.0602 SMA80=26.6572 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@25.4230; S2[2026-06-18→2026-07-20] low=2026-06-26@23.8600; S3[2026-07-23→2026-08-21] low=2026-07-23@26.4700 | lows=[25.423019363615396, 23.860000610351562, 26.469999313354492] span=10.94% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6451616078833834 wick_frac=0.3548383921166166 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2923086854838944 wick_frac=0.7076913145161056 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.425585721237033 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.0089:wick=0.1358; 2026-08-18:GREEN:body=+0.0970:wick=0.5238; 2026-08-19:RED:body=-0.1261:wick=0.7178; 2026-08-20:RED:body=-0.1843:wick=0.4462; 2026-08-21:GREEN:body=+1.0000:wick=0.5500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.67 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.67 vs prior=-0.67 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=12.58 vs prior_export=12.58 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 3.15 | **NEUTRAL** |
| `B10_insider_transactions` | 0.69 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.69 vs prior=0.69 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 6.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.85 (this export) | prior_export=14.85 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=93.5  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.49 on 2026-08-14; prev RSI=61.30 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=2.469 (G=0.7900 R=0.3200); 2026-08-13:GREEN:O=93.1300,C=93.9200,body=+0.7900,vol=2037600.0; 2026-08-14:RED:O=93.8200,C=93.5000,body=-0.3200,vol=2714700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=0.751 (Gvol=2037600 Rvol=2714700); 2026-08-13:GREEN:O=93.1300,C=93.9200,body=+0.7900,vol=2037600.0; 2026-08-14:RED:O=93.8200,C=93.5000,body=-0.3200,vol=2714700.0 | **BAD** |
| `A07_rvol` | RVOL=0.915 on 2026-08-14: today_vol=2714700 / avg20=2966730 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.673 on 2026-08-14 (price=93.5000, mid=91.6560, upper=94.3945, lower=88.9175; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=93.5000 vs SMA50=89.6680 dist=+4.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=91.6560 SMA50=89.6680 SMA80=88.1804 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-12@83.9473; S2[2026-06-11→2026-07-13] low=2026-06-24@84.2666; S3[2026-07-14→2026-08-14] low=2026-07-30@87.2000 | lows=[83.94731893206304, 84.26662384723396, 87.19999694824219] span=3.87% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.9186050637852415 wick_frac=0.08139493621475843 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.2499985098913413 wick_frac=0.7500014901086587 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4687552154113916 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.4200:wick=1.1600; 2026-08-11:GREEN:body=+1.3200:wick=0.3100; 2026-08-12:GREEN:body=+1.5000:wick=0.4000; 2026-08-13:GREEN:body=+0.7900:wick=0.0700; 2026-08-14:RED:body=-0.3200:wick=0.9600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.49 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.49 vs prior_export=102.49 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.17 | **GOOD** |
| `B13_short_float` | 2.13 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXW  ·  score **+15**  ·  Security & Protection Services
price=34.0099983215332  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.95 on 2026-08-21; prev RSI=60.01 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.01@2026-08-20 → 61.95@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.01@2026-08-20 → 61.95@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.01@2026-08-20 → 61.95@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8200 R=0.0000); 2026-08-20:GREEN:O=32.0000,C=33.5100,body=+1.5100,vol=1727400.0; 2026-08-21:GREEN:O=33.7000,C=34.0100,body=+0.3100,vol=2437400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4164800 Rvol=0); 2026-08-20:GREEN:O=32.0000,C=33.5100,body=+1.5100,vol=1727400.0; 2026-08-21:GREEN:O=33.7000,C=34.0100,body=+0.3100,vol=2437400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.655 on 2026-08-21: today_vol=2437400 / avg20=1472675 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.733 on 2026-08-21 (price=34.0100, mid=31.9680, upper=34.7551, lower=29.1809; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=34.0100 vs SMA50=30.5322 dist=+11.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=31.9680 SMA50=30.5322 SMA80=26.9536 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@20.7400; S2[2026-06-18→2026-07-20] low=2026-06-18@27.9100; S3[2026-07-23→2026-08-21] low=2026-08-06@28.5000 | lows=[20.739999771118164, 27.90999984741211, 28.5] span=37.42% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5864273413833205 wick_frac=0.4135726586166795 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.1200:wick=0.8600; 2026-08-18:RED:body=-0.9800:wick=0.1500; 2026-08-19:RED:body=-0.5400:wick=0.9800; 2026-08-20:GREEN:body=+1.5100:wick=0.2400; 2026-08-21:GREEN:body=+0.3100:wick=0.6900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.56 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2484.04 | **NEUTRAL** |
| `B04_income` | 127.9 | **GOOD** |
| `B05_profit_margin` | 5.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.8 vs prior_export=41.8 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -14.61 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-14.61 vs prior=-14.61 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.04 | **GOOD** |
| `B13_short_float` | 16.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.56 (this export) | prior_export=7.56 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.71 (this export) | prior_export=10.71 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FCX  ·  score **+15**  ·  Copper
price=76.66000366210938  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.30 on 2026-08-21; prev RSI=61.63 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.6000 R=0.0000); 2026-08-20:GREEN:O=67.8200,C=71.2200,body=+3.4000,vol=17647400.0; 2026-08-21:GREEN:O=74.4600,C=76.6600,body=+2.2000,vol=28496500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=46143900 Rvol=0); 2026-08-20:GREEN:O=67.8200,C=71.2200,body=+3.4000,vol=17647400.0; 2026-08-21:GREEN:O=74.4600,C=76.6600,body=+2.2000,vol=28496500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.245 on 2026-08-21: today_vol=28496500 / avg20=12691485 (avg window 2026-07-24→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.218 on 2026-08-21 (price=76.6600, mid=67.1070, upper=74.9471, lower=59.2669; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=76.6600 vs SMA50=64.6077 dist=+18.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=67.1070 SMA50=64.6077 SMA80=63.9602 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-22@61.3611; S2[2026-06-24→2026-07-23] low=2026-07-08@55.8644; S3[2026-07-24→2026-08-21] low=2026-07-29@58.1900 | lows=[61.36106366423981, 55.86440695057745, 58.189998626708984] span=9.84% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.8227965239697523 wick_frac=0.17720347603024772 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.7600:wick=0.5900; 2026-08-18:RED:body=-0.7600:wick=1.5600; 2026-08-19:GREEN:body=+0.1900:wick=2.4100; 2026-08-20:GREEN:body=+3.4000:wick=0.4000; 2026-08-21:GREEN:body=+2.2000:wick=0.7300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.25 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.71 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 25156.0 | **NEUTRAL** |
| `B04_income` | 2923.0 | **GOOD** |
| `B05_profit_margin` | 11.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 74.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=74.0 vs prior_export=74.0 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -1.22 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.22 vs prior=-1.22 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.99 | **GOOD** |
| `B13_short_float` | 1.82 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.25 (this export) | prior_export=20.25 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WPM  ·  score **+15**  ·  Gold
price=157.77999877929688  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.50 on 2026-08-21; prev RSI=76.11 on 2026-08-20 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.7900 R=0.0000); 2026-08-20:GREEN:O=144.5400,C=150.2500,body=+5.7100,vol=2584200.0; 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5883500 Rvol=0); 2026-08-20:GREEN:O=144.5400,C=150.2500,body=+5.7100,vol=2584200.0; 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.514 on 2026-08-21: today_vol=3299300 / avg20=2179180 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.038 on 2026-08-21 (price=157.7800, mid=127.9234, upper=156.6917, lower=99.1551; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=157.7800 vs SMA50=118.9574 dist=+32.64% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=127.92_50=118.96_80=122.97 on 2026-08-21: SMA20=127.9234 SMA50=118.9574 SMA80=122.9732 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-10@106.8200; S2[2026-06-22→2026-07-21] low=2026-07-17@101.5900; S3[2026-07-23→2026-08-21] low=2026-07-29@106.3000 | lows=[106.81999969482422, 101.58999633789062, 106.30000305175781] span=5.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7911342774102055 wick_frac=0.20886572258979444 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.2483:wick=1.9774; 2026-08-18:RED:body=-1.3382:wick=2.1871; 2026-08-19:GREEN:body=+7.8496:wick=1.3283; 2026-08-20:GREEN:body=+5.7100:wick=0.4500; 2026-08-21:GREEN:body=+3.0800:wick=1.6200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.52 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3171.64 | **NEUTRAL** |
| `B04_income` | 2050.75 | **GOOD** |
| `B05_profit_margin` | 64.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 172.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.6199999999999761 (now=172.45 vs prior_export=170.83 on finviz_2026-09-07) | **GOOD** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.1 | **GOOD** |
| `B13_short_float` | 1.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVNT  ·  score **+15**  ·  Specialty Chemicals
price=44.65999984741211  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.72 on 2026-08-21; prev RSI=59.90 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=800300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1444200 Rvol=0); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=800300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.885 on 2026-08-21: today_vol=800300 / avg20=904395 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.356 on 2026-08-21 (price=44.6600, mid=41.8005, upper=49.8272, lower=33.7738; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=44.6600 vs SMA50=38.7045 dist=+15.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=41.8005 SMA50=38.7045 SMA80=37.3873 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@31.9895; S2[2026-06-18→2026-07-20] low=2026-07-08@34.9500; S3[2026-07-23→2026-08-21] low=2026-07-29@35.5700 | lows=[31.989543385452947, 34.95000076293945, 35.56999969482422] span=11.19% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6157398137259762 wick_frac=0.3842601862740238 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.6700:wick=0.2300; 2026-08-18:RED:body=-1.7400:wick=0.0300; 2026-08-19:GREEN:body=+0.0800:wick=0.7700; 2026-08-20:GREEN:body=+0.6100:wick=0.4700; 2026-08-21:GREEN:body=+0.5800:wick=0.2900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.51 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.06 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3331.5 | **NEUTRAL** |
| `B04_income` | 170.0 | **GOOD** |
| `B05_profit_margin` | 5.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.75 vs prior_export=50.75 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.38 | **GOOD** |
| `B13_short_float` | 4.32 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.51 (this export) | prior_export=7.51 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.06 (this export) | prior_export=2.06 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYAN  ·  score **+15**  ·  Insurance Brokers
price=43.279998779296875  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.11 on 2026-08-21; prev RSI=55.09 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.09@2026-08-20 → 56.11@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.09@2026-08-20 → 56.11@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.09@2026-08-20 → 56.11@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2400 R=0.0000); 2026-08-20:GREEN:O=42.0900,C=43.0400,body=+0.9500,vol=960900.0; 2026-08-21:GREEN:O=42.9900,C=43.2800,body=+0.2900,vol=819300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1780200 Rvol=0); 2026-08-20:GREEN:O=42.0900,C=43.0400,body=+0.9500,vol=960900.0; 2026-08-21:GREEN:O=42.9900,C=43.2800,body=+0.2900,vol=819300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.378 on 2026-08-21: today_vol=819300 / avg20=2167400 (avg window 2026-07-24→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.018 on 2026-08-21 (price=43.2800, mid=43.3284, upper=46.0458, lower=40.6111; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=43.2800 vs SMA50=40.3150 dist=+7.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=43.3284 SMA50=40.3150 SMA80=37.2223 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-03@30.5761; S2[2026-06-22→2026-07-23] low=2026-06-22@32.9787; S3[2026-07-24→2026-08-21] low=2026-07-24@39.5186 | lows=[30.57607472607481, 32.97869682501655, 39.51860394876054] span=29.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5039308840977527 wick_frac=0.49606911590224734 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.2800:wick=0.7300; 2026-08-18:RED:body=-1.0700:wick=0.3550; 2026-08-19:GREEN:body=+0.8500:wick=0.7300; 2026-08-20:GREEN:body=+0.9500:wick=0.3000; 2026-08-21:GREEN:body=+0.2900:wick=0.8800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=23.79 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.89 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3224.94 | **NEUTRAL** |
| `B04_income` | 99.03 | **GOOD** |
| `B05_profit_margin` | 3.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.91 vs prior_export=49.91 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 2.32 | **GOOD** |
| `B10_insider_transactions` | 0.9 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.9 vs prior=0.9 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.95 | **GOOD** |
| `B13_short_float` | 12.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.79 (this export) | prior_export=23.79 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.89 (this export) | prior_export=4.89 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RUSHA  ·  score **+15**  ·  Auto & Truck Dealerships
price=78.77999877929688  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.42 on 2026-08-21; prev RSI=46.44 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.44@2026-08-20 → 51.42@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.44@2026-08-20 → 51.42@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.44@2026-08-20 → 51.42@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.595 (G=1.3300 R=0.3700); 2026-08-20:RED:O=77.4800,C=77.1100,body=-0.3700,vol=385200.0; 2026-08-21:GREEN:O=77.4500,C=78.7800,body=+1.3300,vol=390400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.013 (Gvol=390400 Rvol=385200); 2026-08-20:RED:O=77.4800,C=77.1100,body=-0.3700,vol=385200.0; 2026-08-21:GREEN:O=77.4500,C=78.7800,body=+1.3300,vol=390400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-08-21: today_vol=390400 / avg20=486495 (avg window 2026-07-17→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.174 on 2026-08-21 (price=78.7800, mid=79.5805, upper=84.1921, lower=74.9689; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=78.7800 vs SMA50=74.9480 dist=+5.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=79.5805 SMA50=74.9480 SMA80=73.4861 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-15→2026-08-21 (63 bars); S1[2026-05-15→2026-06-15] low=2026-06-05@65.6800; S2[2026-06-16→2026-07-16] low=2026-06-17@67.3300; S3[2026-07-17→2026-08-21] low=2026-07-20@74.9200 | lows=[65.68000030517578, 67.33000183105469, 74.91999816894531] span=14.07% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6425130565865274 wick_frac=0.3574869434134727 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.18048954770615228 wick_frac=0.8195104522938477 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.594572860176918 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.4300:wick=0.9000; 2026-08-18:RED:body=-2.7400:wick=0.1000; 2026-08-19:RED:body=-2.0900:wick=0.1500; 2026-08-20:RED:body=-0.3700:wick=1.6800; 2026-08-21:GREEN:body=+1.3300:wick=0.7400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.48 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.36 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7236.52 | **NEUTRAL** |
| `B04_income` | 265.23 | **GOOD** |
| `B05_profit_margin` | 3.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 59.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=59.67 vs prior_export=59.67 on finviz_2026-09-07) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -0.49 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.49 vs prior=-0.49 on finviz_2026-09-07) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 7.49 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.48 (this export) | prior_export=6.48 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.36 (this export) | prior_export=0.36 (finviz_2026-09-07) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-08_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.