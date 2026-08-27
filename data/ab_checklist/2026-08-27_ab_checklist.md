# A+B1 Feature Checklist — 2026-08-27

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,698** names
- Export: `finviz_2026-08-27.csv` · prior export for Δ: `2026-08-25`
- score = sum of flags over **30** features

## Framing (per asof trading day)

- **A05/A06/A12/A13** use **exactly two connected sessions**: `pair_day_a` (prev) + `pair_day_b` (asof).
  No multi-day green/red sums.
- **RSI crosses**: cross **up** through 30 or 50 → GOOD; cross **down** through 50 or 70 → BAD.
- **A11 downside structure**: last ~63 sessions split into 3 equal sections; lowest **low** in each;
  GOOD if rising lows or span(highest low − lowest low)/lowest ≤ 12%.
- **B17/B18**: current export EPS/Rev surprise vs **prior export** snapshot (proxy for last 2 prints).
- Analyst last-2 rating actions (upgrade/downgrade) come from quote scrape → merge step (B19).

## Ranked (top 20)

| Rank | Ticker | score | good | bad | pair | Industry |
|-----:|--------|------:|-----:|----:|------|----------|
| 1 | LEU | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Uranium |
| 2 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 3 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 4 | MSI | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Communication Equipment |
| 5 | SON | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 6 | AHR | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 7 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 8 | CAKE | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Restaurants |
| 9 | FCX | +15 | 18 | 3 | 2026-08-20→2026-08-21 | Copper |
| 10 | GPK | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 11 | CBRL | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Restaurants |
| 12 | TECK | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Copper |
| 13 | PLTR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 14 | NWBI | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Banks - Regional |
| 15 | RUSHA | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Auto & Truck Dealerships |
| 16 | XYZ | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 17 | ABR | +15 | 15 | 0 | 2026-08-20→2026-08-21 | REIT - Mortgage |
| 18 | RYAN | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Insurance - Specialty |
| 19 | SOLV | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Medical Instruments & Supplies |
| 20 | ARDT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Care Facilities |

## Full checklist — top 20

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
| `B07_target_price` | 250.13 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=250.13 vs prior_export=250.13 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.06 | **GOOD** |
| `B13_short_float` | 29.02 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.1 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.64 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.62 | **GOOD** |
| `B13_short_float` | 9.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSI  ·  score **+16**  ·  Communication Equipment
price=480.4700012207031  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.83 on 2026-08-21; prev RSI=63.38 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.38@2026-08-20 → 65.83@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.38@2026-08-20 → 65.83@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.38@2026-08-20 → 65.83@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.079 (G=8.9200 R=4.2900); 2026-08-20:RED:O=477.9200,C=473.6300,body=-4.2900,vol=1564800.0; 2026-08-21:GREEN:O=471.5500,C=480.4700,body=+8.9200,vol=1298100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.830 (Gvol=1298100 Rvol=1564800); 2026-08-20:RED:O=477.9200,C=473.6300,body=-4.2900,vol=1564800.0; 2026-08-21:GREEN:O=471.5500,C=480.4700,body=+8.9200,vol=1298100.0 | **BAD** |
| `A07_rvol` | RVOL=1.286 on 2026-08-21: today_vol=1298100 / avg20=1009265 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.648 on 2026-08-21 (price=480.4700, mid=454.9205, upper=494.3274, lower=415.5136; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=480.4700 vs SMA50=428.0138 dist=+12.26% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=454.9205 SMA50=428.0138 SMA80=422.1571 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-21@389.6322; S2[2026-06-18→2026-07-20] low=2026-06-24@389.0200; S3[2026-07-23→2026-08-21] low=2026-07-23@402.4100 | lows=[389.6322122089833, 389.0199890136719, 402.4100036621094] span=3.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7335535149162147 wick_frac=0.26644648508378527 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.31132140239135586 wick_frac=0.6886785976086441 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.079253067757425 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-6.0000:wick=4.1200; 2026-08-18:GREEN:body=+10.6100:wick=3.2000; 2026-08-19:GREEN:body=+11.1000:wick=4.1400; 2026-08-20:RED:body=-4.2900:wick=9.4900; 2026-08-21:GREEN:body=+8.9200:wick=3.2400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.51 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.37 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 12236.0 | **NEUTRAL** |
| `B04_income` | 2134.0 | **GOOD** |
| `B05_profit_margin` | 17.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 528.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.5 (now=528.5 vs prior_export=526.0 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 1.59 | **GOOD** |
| `B10_insider_transactions` | -11.94 | **BAD** |
| `B11_insider_tx_delta` | delta=10.110000000000001 (now=-11.94 vs prior=-22.05 on finviz_2026-08-25) | **GOOD** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 1.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.51 (this export) | prior_export=14.51 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.37 (this export) | prior_export=4.37 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.37 vs prior=1.37 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.88 | **GOOD** |
| `B13_short_float` | 10.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 63.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.67 vs prior_export=63.67 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -0.06 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.06 vs prior=-0.06 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.27 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 249.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=249.97 vs prior_export=249.97 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.97 vs prior=-2.97 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CAKE  ·  score **+15**  ·  Restaurants
price=113.33000183105469  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.73 on 2026-08-21; prev RSI=61.25 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.25@2026-08-20 → 68.73@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.25@2026-08-20 → 68.73@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.25@2026-08-20 → 68.73@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=7.7300 R=0.0000); 2026-08-20:GREEN:O=105.2500,C=106.5300,body=+1.2800,vol=1751900.0; 2026-08-21:GREEN:O=106.8800,C=113.3300,body=+6.4500,vol=1865300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3617200 Rvol=0); 2026-08-20:GREEN:O=105.2500,C=106.5300,body=+1.2800,vol=1751900.0; 2026-08-21:GREEN:O=106.8800,C=113.3300,body=+6.4500,vol=1865300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.915 on 2026-08-21: today_vol=1865300 / avg20=2038370 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.498 on 2026-08-21 (price=113.3300, mid=105.7592, upper=120.9607, lower=90.5577; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=113.3300 vs SMA50=89.3225 dist=+26.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=105.7592 SMA50=89.3225 SMA80=79.0076 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@57.6540; S2[2026-06-22→2026-07-23] low=2026-06-23@75.3661; S3[2026-07-24→2026-08-21] low=2026-07-24@82.6963 | lows=[57.65402651356531, 75.366107094546, 82.69627796157368] span=43.44% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6100485991864628 wick_frac=0.38995140081353713 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-1.6600:wick=1.6500; 2026-08-18:RED:body=-2.3900:wick=1.6900; 2026-08-19:RED:body=-1.1200:wick=3.7100; 2026-08-20:GREEN:body=+1.2800:wick=2.9500; 2026-08-21:GREEN:body=+6.4500:wick=0.5800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.17 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3877.25 | **NEUTRAL** |
| `B04_income` | 178.62 | **GOOD** |
| `B05_profit_margin` | 4.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 92.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.8700000000000045 (now=92.67 vs prior_export=91.8 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 2.71 | **NEUTRAL** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.009999999999999787 (now=-8.05 vs prior=-8.06 on finviz_2026-08-25) | **GOOD** |
| `B12_institutional_transactions` | -6.63 | **BAD** |
| `B13_short_float` | 20.15 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.17 (this export) | prior_export=22.17 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.3199999999999932 (now=74.0 vs prior_export=73.68 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 1.54 | **GOOD** |
| `B10_insider_transactions` | -1.18 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.8799999999999999 (now=-1.18 vs prior=-0.3 on finviz_2026-08-25) | **BAD** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 1.82 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.25 (this export) | prior_export=20.25 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=12.58 vs prior_export=12.58 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 3.14 | **NEUTRAL** |
| `B10_insider_transactions` | 2.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.06 vs prior=2.06 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 6.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.85 (this export) | prior_export=14.85 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+15**  ·  Restaurants
price=57.650001525878906  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.52 on 2026-08-21; prev RSI=50.63 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.349 (G=2.3000 R=0.4300); 2026-08-20:RED:O=55.5600,C=55.1300,body=-0.4300,vol=546800.0; 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.000 (Gvol=546700 Rvol=546800); 2026-08-20:RED:O=55.5600,C=55.1300,body=-0.4300,vol=546800.0; 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0 | **BAD** |
| `A07_rvol` | RVOL=0.678 on 2026-08-21: today_vol=546700 / avg20=805905 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.223 on 2026-08-21 (price=57.6500, mid=56.8255, upper=60.5311, lower=53.1199; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=57.6500 vs SMA50=51.7908 dist=+11.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=56.8255 SMA50=51.7908 SMA80=44.1081 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@28.0918; S2[2026-06-18→2026-07-20] low=2026-06-18@44.3219; S3[2026-07-23→2026-08-21] low=2026-07-27@50.2000 | lows=[28.091751487486153, 44.32188230619164, 50.20000076293945] span=78.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.8745267884856941 wick_frac=0.12547321151430585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.23497309905924876 wick_frac=0.7650269009407512 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.348840510281932 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.7900:wick=1.7700; 2026-08-18:RED:body=-0.7800:wick=1.3000; 2026-08-19:RED:body=-1.8300:wick=1.7800; 2026-08-20:RED:body=-0.4300:wick=1.4000; 2026-08-21:GREEN:body=+2.3000:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=160.4 (current export asof; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.66 (current export; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3337.38 | **NEUTRAL** |
| `B04_income` | 26.23 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 23.31 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TECK  ·  score **+15**  ·  Copper
price=69.19999694824219  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.85 on 2026-08-21; prev RSI=58.71 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.71@2026-08-20 → 64.85@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.71@2026-08-20 → 64.85@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.71@2026-08-20 → 64.85@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.5300 R=0.0000); 2026-08-20:GREEN:O=64.5100,C=66.1600,body=+1.6500,vol=2144800.0; 2026-08-21:GREEN:O=68.3200,C=69.2000,body=+0.8800,vol=4872700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7017500 Rvol=0); 2026-08-20:GREEN:O=64.5100,C=66.1600,body=+1.6500,vol=2144800.0; 2026-08-21:GREEN:O=68.3200,C=69.2000,body=+0.8800,vol=4872700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.696 on 2026-08-21: today_vol=4872700 / avg20=2873210 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.837 on 2026-08-21 (price=69.2000, mid=63.9215, upper=70.2292, lower=57.6138; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=69.2000 vs SMA50=61.7080 dist=+12.14% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=63.92_50=61.71_80=62.06 on 2026-08-21: SMA20=63.9215 SMA50=61.7080 SMA80=62.0561 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@58.4197; S2[2026-06-18→2026-07-20] low=2026-07-17@53.6400; S3[2026-07-23→2026-08-21] low=2026-07-29@56.7200 | lows=[58.41969930703951, 53.63999938964844, 56.720001220703125] span=8.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7288305789946603 wick_frac=0.27116942100533975 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.4900:wick=0.6000; 2026-08-18:RED:body=-0.1500:wick=1.5600; 2026-08-19:GREEN:body=+0.3700:wick=1.1800; 2026-08-20:GREEN:body=+1.6500:wick=0.2800; 2026-08-21:GREEN:body=+0.8800:wick=0.5800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=40.05 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.84 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 10129.71 | **NEUTRAL** |
| `B04_income` | 1808.14 | **GOOD** |
| `B05_profit_margin` | 17.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 64.98 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.6200000000000045 (now=64.98 vs prior_export=64.36 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 2.65 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.32 | **GOOD** |
| `B13_short_float` | 2.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=40.05 (this export) | prior_export=40.05 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.84 (this export) | prior_export=4.84 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLTR  ·  score **+15**  ·  Software - Infrastructure
price=179.94000244140625  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.41 on 2026-08-21; prev RSI=66.30 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.104 (G=5.9600 R=1.9200); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.517 (Gvol=40986600 Rvol=27018400); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.879 on 2026-08-21: today_vol=40986600 / avg20=46642400 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.500 on 2026-08-21 (price=179.9400, mid=157.2745, upper=202.5798, lower=111.9692; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=179.9400 vs SMA50=138.9318 dist=+29.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=157.2745 SMA50=138.9318 SMA80=138.8852 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-23@116.1800; S2[2026-06-24→2026-07-23] low=2026-06-25@106.3700; S3[2026-07-24→2026-08-21] low=2026-07-28@117.8900 | lows=[116.18000030517578, 106.37000274658203, 117.88999938964844] span=10.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6026296341438955 wick_frac=0.3973703658561045 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.4334073882717644 wick_frac=0.5665926117282356 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.1041731238426755 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-1.5100:wick=2.4640; 2026-08-18:RED:body=-0.4300:wick=3.9400; 2026-08-19:GREEN:body=+2.5800:wick=4.4690; 2026-08-20:RED:body=-1.9200:wick=2.5100; 2026-08-21:GREEN:body=+5.9600:wick=3.9300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.98 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.8 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6155.94 | **NEUTRAL** |
| `B04_income` | 3016.69 | **GOOD** |
| `B05_profit_margin` | 49.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 200.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=200.88 vs prior_export=200.88 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -1.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.8 vs prior=-1.8 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.91 | **GOOD** |
| `B13_short_float` | 3.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.56 | **GOOD** |
| `B13_short_float` | 5.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B01_eps_surprise` | EPS surprise=6.46 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.36 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7236.52 | **NEUTRAL** |
| `B04_income` | 265.23 | **GOOD** |
| `B05_profit_margin` | 3.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.5 vs prior_export=89.5 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -0.74 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.74 vs prior=-0.74 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 7.49 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.46 (this export) | prior_export=6.46 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.36 (this export) | prior_export=0.36 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=99.12 vs prior_export=99.12 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.49 | **GOOD** |
| `B10_insider_transactions` | -1.22 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.22 vs prior=-1.22 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.19 | **GOOD** |
| `B13_short_float` | 2.57 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.22 (this export) | prior_export=2.22 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ABR  ·  score **+15**  ·  REIT - Mortgage
price=5.190000057220459  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.65 on 2026-08-21; prev RSI=56.27 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.27@2026-08-20 → 56.65@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.27@2026-08-20 → 56.65@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.27@2026-08-20 → 56.65@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.0800 R=0.0000); 2026-08-20:GREEN:O=5.1000,C=5.1800,body=+0.0800,vol=3601600.0; 2026-08-21:DOJI:O=5.1900,C=5.1900,body=+0.0000,vol=3945500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=2.826 (Gvol=5574350 Rvol=1972750); 2026-08-20:GREEN:O=5.1000,C=5.1800,body=+0.0800,vol=3601600.0; 2026-08-21:DOJI:O=5.1900,C=5.1900,body=+0.0000,vol=3945500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.926 on 2026-08-21: today_vol=3945500 / avg20=4263020 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.580 on 2026-08-21 (price=5.1900, mid=5.0232, upper=5.3108, lower=4.7356; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=5.1900 vs SMA50=4.9814 dist=+4.19% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=5.02_50=4.98_80=5.37 on 2026-08-21: SMA20=5.0232 SMA50=4.9814 SMA80=5.3716 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-21 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-17@4.8287; S2[2026-06-23→2026-07-23] low=2026-07-08@4.7029; S3[2026-07-24→2026-08-21] low=2026-07-31@4.5771 | lows=[4.828726128677582, 4.702927930627254, 4.577129379898831] span=5.50% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5333341810459926 wick_frac=0.4666658189540073 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.0700:wick=0.0400; 2026-08-18:RED:body=-0.0700:wick=0.0600; 2026-08-19:GREEN:body=+0.2000:wick=0.1000; 2026-08-20:GREEN:body=+0.0800:wick=0.0700; 2026-08-21:DOJI:body=+0.0000:wick=0.1500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=100.0 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.54 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1198.87 | **NEUTRAL** |
| `B04_income` | 16.32 | **GOOD** |
| `B05_profit_margin` | 1.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.88 vs prior_export=5.88 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 3.4 | **NEUTRAL** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.2 | **GOOD** |
| `B13_short_float` | 24.92 | **GOOD** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=100.0 (this export) | prior_export=100.0 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.54 (this export) | prior_export=7.54 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYAN  ·  score **+15**  ·  Insurance - Specialty
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
| `B08_target_price_delta` | delta=0.0 (now=49.91 vs prior_export=49.91 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.27 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.43 vs prior=0.43 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 12.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.79 (this export) | prior_export=23.79 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.89 (this export) | prior_export=4.89 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLV  ·  score **+15**  ·  Medical Instruments & Supplies
price=89.56999969482422  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.96 on 2026-08-21; prev RSI=62.09 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.8000 R=0.0000); 2026-08-20:GREEN:O=87.0900,C=88.6100,body=+1.5200,vol=805100.0; 2026-08-21:GREEN:O=88.2900,C=89.5700,body=+1.2800,vol=925200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1730300 Rvol=0); 2026-08-20:GREEN:O=87.0900,C=88.6100,body=+1.5200,vol=805100.0; 2026-08-21:GREEN:O=88.2900,C=89.5700,body=+1.2800,vol=925200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.713 on 2026-08-21: today_vol=925200 / avg20=1298110 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.607 on 2026-08-21 (price=89.5700, mid=85.9675, upper=91.9047, lower=80.0303; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=89.5700 vs SMA50=80.9856 dist=+10.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=85.9675 SMA50=80.9856 SMA80=78.2257 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@72.7420; S2[2026-06-18→2026-07-20] low=2026-06-23@73.0400; S3[2026-07-23→2026-08-21] low=2026-07-23@76.7700 | lows=[72.74199676513672, 73.04000091552734, 76.7699966430664] span=5.54% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6842287034422322 wick_frac=0.3157712965577678 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.9300:wick=0.5550; 2026-08-18:RED:body=-0.0500:wick=1.3900; 2026-08-19:RED:body=-0.0700:wick=2.0050; 2026-08-20:GREEN:body=+1.5200:wick=0.8900; 2026-08-21:GREEN:body=+1.2800:wick=0.4550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=33.89 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.55 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8310.0 | **NEUTRAL** |
| `B04_income` | 1434.0 | **GOOD** |
| `B05_profit_margin` | 17.26 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 95.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=95.83 vs prior_export=95.83 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.38 | **BAD** |
| `B13_short_float` | 2.4 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.89 (this export) | prior_export=33.89 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.55 (this export) | prior_export=2.55 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARDT  ·  score **+15**  ·  Medical Care Facilities
price=10.989999771118164  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.96 on 2026-08-21; prev RSI=45.66 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.66@2026-08-20 → 54.96@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.66@2026-08-20 → 54.96@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.66@2026-08-20 → 54.96@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=4.800 (G=0.4800 R=0.1000); 2026-08-20:RED:O=10.5800,C=10.4800,body=-0.1000,vol=237600.0; 2026-08-21:GREEN:O=10.5100,C=10.9900,body=+0.4800,vol=239100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.006 (Gvol=239100 Rvol=237600); 2026-08-20:RED:O=10.5800,C=10.4800,body=-0.1000,vol=237600.0; 2026-08-21:GREEN:O=10.5100,C=10.9900,body=+0.4800,vol=239100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.505 on 2026-08-21: today_vol=239100 / avg20=473340 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.026 on 2026-08-21 (price=10.9900, mid=10.9745, upper=11.5681, lower=10.3809; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=10.9900 vs SMA50=10.2604 dist=+7.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=10.9745 SMA50=10.2604 SMA80=10.0081 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-03@7.7100; S2[2026-06-18→2026-07-20] low=2026-06-18@8.8400; S3[2026-07-23→2026-08-21] low=2026-07-23@10.0300 | lows=[7.710000038146973, 8.84000015258789, 10.029999732971191] span=30.09% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.716417145638862 wick_frac=0.2835828543611379 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2941186370317181 wick_frac=0.705881362968282 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.799977111903718 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.0200:wick=0.1400; 2026-08-18:RED:body=-0.1000:wick=0.1300; 2026-08-19:RED:body=-0.4000:wick=0.2700; 2026-08-20:RED:body=-0.1000:wick=0.2400; 2026-08-21:GREEN:body=+0.4800:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=-33.96 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **BAD** |
| `B02_revenue_surprise` | Revenue surprise=2.05 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6405.94 | **NEUTRAL** |
| `B04_income` | 78.23 | **GOOD** |
| `B05_profit_margin` | 1.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.68 vs prior_export=12.68 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.77 | **GOOD** |
| `B13_short_float` | 18.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-33.96 (this export) | prior_export=-33.96 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-27_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.