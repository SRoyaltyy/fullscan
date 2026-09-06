# A+B1 Feature Checklist — 2026-09-06

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,678** names
- Export: `finviz_2026-09-06.csv` · prior export for Δ: `2026-09-05`
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
| 2 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 3 | XYZ | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 4 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 5 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 6 | TILE | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Furnishings, Fixtures & Appliances |
| 7 | SON | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 8 | AHR | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 9 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 10 | WTW | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Insurance Brokers |
| 11 | PLTR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 12 | NWBI | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Banks - Regional |
| 13 | RMD | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Instruments & Supplies |
| 14 | CBRL | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Restaurants |
| 15 | ARDT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Care Facilities |

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
| `B08_target_price_delta` | delta=0.0 (now=243.93 vs prior_export=243.93 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.05 | **GOOD** |
| `B13_short_float` | 29.02 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.67 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.67 vs prior=-0.67 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.55 | **GOOD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### XYZ  ·  score **+16**  ·  Software - Infrastructure
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
| `B07_target_price` | 98.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=98.8 vs prior_export=98.8 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.54 | **GOOD** |
| `B10_insider_transactions` | -1.29 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-1.29 vs prior=-1.3 on finviz_2026-09-05) | **GOOD** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 2.57 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.22 (this export) | prior_export=2.22 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.91 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=248.86 vs prior_export=248.86 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -3.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.1 vs prior=-3.1 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TILE  ·  score **+16**  ·  Furnishings, Fixtures & Appliances
price=38.939998626708984  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.28 on 2026-08-21; prev RSI=67.39 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.098 (G=0.5600 R=0.5100); 2026-08-20:GREEN:O=38.5200,C=39.0800,body=+0.5600,vol=482300.0; 2026-08-21:RED:O=39.4500,C=38.9400,body=-0.5100,vol=314900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.532 (Gvol=482300 Rvol=314900); 2026-08-20:GREEN:O=38.5200,C=39.0800,body=+0.5600,vol=482300.0; 2026-08-21:RED:O=39.4500,C=38.9400,body=-0.5100,vol=314900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.691 on 2026-08-21: today_vol=314900 / avg20=455630 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.522 on 2026-08-21 (price=38.9400, mid=36.5890, upper=41.0959, lower=32.0821; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.9400 vs SMA50=34.4898 dist=+12.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=36.5890 SMA50=34.4898 SMA80=32.2381 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@27.0127; S2[2026-06-18→2026-07-20] low=2026-07-08@31.1100; S3[2026-07-23→2026-08-21] low=2026-07-23@31.9600 | lows=[27.012733591234557, 31.110000610351562, 31.959999084472656] span=18.31% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5045054642930785 wick_frac=0.4954945357069214 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.47222339959592535 wick_frac=0.5277766004040747 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0980373090789415 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.0900:wick=0.6800; 2026-08-18:GREEN:body=+0.5900:wick=0.8000; 2026-08-19:GREEN:body=+0.0300:wick=0.6500; 2026-08-20:GREEN:body=+0.5600:wick=0.5500; 2026-08-21:RED:body=-0.5100:wick=0.5700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=37.59 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1440.65 | **NEUTRAL** |
| `B04_income` | 145.55 | **GOOD** |
| `B05_profit_margin` | 10.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.25 vs prior_export=45.25 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -7.98 | **BAD** |
| `B11_insider_tx_delta` | delta=0.879999999999999 (now=-7.98 vs prior=-8.86 on finviz_2026-09-05) | **GOOD** |
| `B12_institutional_transactions` | 2.67 | **GOOD** |
| `B13_short_float` | 7.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.59 (this export) | prior_export=37.59 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=62.89 vs prior_export=62.89 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 1.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.49 vs prior=1.49 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.95 | **GOOD** |
| `B13_short_float` | 10.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=64.4 vs prior_export=64.4 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.87 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.87 vs prior=-1.87 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.06 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.19 | **GOOD** |
| `B13_short_float` | 9.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WTW  ·  score **+15**  ·  Insurance Brokers
price=341.989990234375  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.02 on 2026-08-21; prev RSI=67.57 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.57@2026-08-20 → 68.02@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.57@2026-08-20 → 68.02@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.57@2026-08-20 → 68.02@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.368 (G=5.6900 R=1.0600); 2026-08-20:GREEN:O=335.4600,C=341.1500,body=+5.6900,vol=458800.0; 2026-08-21:RED:O=343.0500,C=341.9900,body=-1.0600,vol=499400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.919 (Gvol=458800 Rvol=499400); 2026-08-20:GREEN:O=335.4600,C=341.1500,body=+5.6900,vol=458800.0; 2026-08-21:RED:O=343.0500,C=341.9900,body=-1.0600,vol=499400.0 | **BAD** |
| `A07_rvol` | RVOL=0.862 on 2026-08-21: today_vol=499400 / avg20=579685 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.368 on 2026-08-21 (price=341.9900, mid=331.6940, upper=359.6507, lower=303.7373; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=341.9900 vs SMA50=296.6662 dist=+15.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=331.6940 SMA50=296.6662 SMA80=282.2589 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-01@247.6386; S2[2026-06-18→2026-07-20] low=2026-06-22@249.3324; S3[2026-07-23→2026-08-21] low=2026-07-23@284.8000 | lows=[247.63860027759392, 249.33244136274328, 284.79998779296875] span=15.01% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6480641494874924 wick_frac=0.3519358505125077 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.21500996620157725 wick_frac=0.7849900337984227 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.3679391950250475 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-3.5400:wick=3.7100; 2026-08-18:RED:body=-1.9500:wick=3.9200; 2026-08-19:GREEN:body=+6.3100:wick=2.5200; 2026-08-20:GREEN:body=+5.6900:wick=3.0900; 2026-08-21:RED:body=-1.0600:wick=3.8700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.64 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.05 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 10104.0 | **NEUTRAL** |
| `B04_income` | 1565.0 | **GOOD** |
| `B05_profit_margin` | 15.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 382.35 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=382.35 vs prior_export=382.35 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | 0.78 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.74 (now=0.78 vs prior=0.04 on finviz_2026-09-05) | **GOOD** |
| `B12_institutional_transactions` | -0.76 | **BAD** |
| `B13_short_float` | 3.12 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.64 (this export) | prior_export=7.64 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=200.88 vs prior_export=200.88 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -0.85 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.85 vs prior=-0.85 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.95 | **GOOD** |
| `B13_short_float` | 3.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.57 | **GOOD** |
| `B13_short_float` | 5.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RMD  ·  score **+15**  ·  Medical Instruments & Supplies
price=231.52000427246094  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.95 on 2026-08-21; prev RSI=60.97 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.97@2026-08-20 → 62.95@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.97@2026-08-20 → 62.95@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.97@2026-08-20 → 62.95@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.525 (G=2.0800 R=0.5900); 2026-08-20:RED:O=229.0300,C=228.4400,body=-0.5900,vol=1285700.0; 2026-08-21:GREEN:O=229.4400,C=231.5200,body=+2.0800,vol=1725300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.342 (Gvol=1725300 Rvol=1285700); 2026-08-20:RED:O=229.0300,C=228.4400,body=-0.5900,vol=1285700.0; 2026-08-21:GREEN:O=229.4400,C=231.5200,body=+2.0800,vol=1725300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.180 on 2026-08-21: today_vol=1725300 / avg20=1461500 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.662 on 2026-08-21 (price=231.5200, mid=218.9415, upper=237.9401, lower=199.9429; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=231.5200 vs SMA50=206.5806 dist=+12.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=218.9415 SMA50=206.5806 SMA80=205.6616 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-02@180.2700; S2[2026-06-18→2026-07-20] low=2026-06-18@187.0100; S3[2026-07-23→2026-08-21] low=2026-07-23@190.2100 | lows=[180.27000427246094, 187.00999450683594, 190.2100067138672] span=5.51% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.3714284935776917 wick_frac=0.6285715064223083 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.12798183509256225 wick_frac=0.8720181649074378 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.525448714633011 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-2.3034:wick=2.9615; 2026-08-18:RED:body=-1.0171:wick=2.4729; 2026-08-19:GREEN:body=+9.6224:wick=0.9473; 2026-08-20:RED:body=-0.5900:wick=4.0200; 2026-08-21:GREEN:body=+2.0800:wick=3.5200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.98 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.16 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5653.44 | **NEUTRAL** |
| `B04_income` | 1523.29 | **GOOD** |
| `B05_profit_margin` | 26.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 261.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=261.69 vs prior_export=261.69 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.13999999999999968 (now=-2.97 vs prior=-3.11 on finviz_2026-09-05) | **GOOD** |
| `B12_institutional_transactions` | 0.82 | **GOOD** |
| `B13_short_float` | 8.96 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.16 (this export) | prior_export=0.16 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.16 | **GOOD** |
| `B13_short_float` | 23.31 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 12.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.77 vs prior_export=12.77 on finviz_2026-09-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-09-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 18.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-33.96 (this export) | prior_export=-33.96 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-09-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-06_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.