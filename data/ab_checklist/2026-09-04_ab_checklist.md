# A+B1 Feature Checklist — 2026-09-04

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,676** names
- Export: `finviz_2026-09-04.csv` · prior export for Δ: `2026-09-03`
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
| 1 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 2 | RMD | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Medical Instruments & Supplies |
| 3 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 4 | AHR | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 5 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 6 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 7 | LEU | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Uranium |
| 8 | CSL | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Building Products & Equipment |
| 9 | RYAN | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Insurance - Specialty |
| 10 | DRH | +15 | 16 | 1 | 2026-08-20→2026-08-21 | REIT - Hotel & Motel |
| 11 | PATH | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 12 | PAA | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Oil & Gas Midstream |
| 13 | PAGP | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Oil & Gas Midstream |
| 14 | ARDT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Care Facilities |
| 15 | VNT | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Scientific & Technical Instruments |

## Full checklist — top 15

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
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.67 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.67 vs prior=-0.67 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.55 | **GOOD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RMD  ·  score **+16**  ·  Medical Instruments & Supplies
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
| `B07_target_price` | 261.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.18000000000000682 (now=261.25 vs prior_export=261.07 on finviz_2026-09-03) | **GOOD** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -3.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.06999999999999984 (now=-3.04 vs prior=-3.11 on finviz_2026-09-03) | **GOOD** |
| `B12_institutional_transactions` | 0.82 | **GOOD** |
| `B13_short_float` | 8.96 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.16 (this export) | prior_export=0.16 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.91 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=64.4 vs prior_export=64.4 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.87 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.87 vs prior=-1.87 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.06 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=248.86 vs prior_export=248.86 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -3.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.11 vs prior=-3.11 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.19 | **GOOD** |
| `B13_short_float` | 9.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LEU  ·  score **+16**  ·  Uranium
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
| `B07_target_price` | 245.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-5.069999999999993 (now=245.06 vs prior_export=250.13 on finviz_2026-09-03) | **BAD** |
| `B09_analyst_recom` | 1.82 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.05 | **GOOD** |
| `B13_short_float` | 29.02 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CSL  ·  score **+15**  ·  Building Products & Equipment
price=367.67999267578125  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.90 on 2026-08-21; prev RSI=47.00 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.00@2026-08-20 → 51.90@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.00@2026-08-20 → 51.90@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.00@2026-08-20 → 51.90@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.264 (G=6.5600 R=5.1900); 2026-08-20:RED:O=362.7400,C=357.5500,body=-5.1900,vol=380900.0; 2026-08-21:GREEN:O=361.1200,C=367.6800,body=+6.5600,vol=511600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.343 (Gvol=511600 Rvol=380900); 2026-08-20:RED:O=362.7400,C=357.5500,body=-5.1900,vol=380900.0; 2026-08-21:GREEN:O=361.1200,C=367.6800,body=+6.5600,vol=511600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.112 on 2026-08-21: today_vol=511600 / avg20=460215 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.002 on 2026-08-21 (price=367.6800, mid=367.5917, upper=403.0348, lower=332.1487; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=367.6800 vs SMA50=357.7259 dist=+2.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=367.5917 SMA50=357.7259 SMA80=353.4580 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@321.2500; S2[2026-06-18→2026-07-20] low=2026-07-20@325.6200; S3[2026-07-23→2026-08-21] low=2026-07-29@320.2300 | lows=[321.25, 325.6199951171875, 320.2300109863281] span=1.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6023864770039569 wick_frac=0.3976135229960431 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.4020140177526682 wick_frac=0.5979859822473318 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2639681064998294 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-5.4807:wick=3.8664; 2026-08-18:RED:body=-6.6267:wick=4.2849; 2026-08-19:GREEN:body=+6.7100:wick=5.6000; 2026-08-20:RED:body=-5.1900:wick=7.7200; 2026-08-21:GREEN:body=+6.5600:wick=4.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.65 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.39 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5097.0 | **NEUTRAL** |
| `B04_income` | 728.5 | **GOOD** |
| `B05_profit_margin` | 14.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 411.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=411.88 vs prior_export=411.88 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.86 | **BAD** |
| `B13_short_float` | 5.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.65 (this export) | prior_export=10.65 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.39 (this export) | prior_export=6.39 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=49.91 vs prior_export=49.91 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.32 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.43 vs prior=0.43 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.32 | **GOOD** |
| `B13_short_float` | 12.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.79 (this export) | prior_export=23.79 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.89 (this export) | prior_export=4.89 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRH  ·  score **+15**  ·  REIT - Hotel & Motel
price=12.819999694824219  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.65 on 2026-08-21; prev RSI=56.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.1200 R=0.0600); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1831800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.020 (Gvol=1869100 Rvol=1831800); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1831800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.723 on 2026-08-21: today_vol=1831800 / avg20=2534385 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.110 on 2026-08-21 (price=12.8200, mid=12.7280, upper=13.5676, lower=11.8884; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=12.8200 vs SMA50=12.3671 dist=+3.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=12.7280 SMA50=12.3671 SMA80=11.7526 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-21 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@10.4726; S2[2026-06-23→2026-07-23] low=2026-07-08@11.5700; S3[2026-07-24→2026-08-21] low=2026-08-11@11.9900 | lows=[10.472552720822522, 11.569999694824219, 11.989999771118164] span=14.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5454576980930872 wick_frac=0.4545423019069128 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.18750149011754047 wick_frac=0.8124985098824595 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.1100:wick=0.1700; 2026-08-18:GREEN:body=+0.1200:wick=0.1100; 2026-08-19:RED:body=-0.1800:wick=0.2300; 2026-08-20:GREEN:body=+0.1200:wick=0.1000; 2026-08-21:RED:body=-0.0600:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=103.61 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.05 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1136.37 | **NEUTRAL** |
| `B04_income` | 148.78 | **GOOD** |
| `B05_profit_margin` | 13.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.71 vs prior_export=13.71 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -1.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.6 vs prior=-1.6 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 7.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=103.61 (this export) | prior_export=103.61 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.05 (this export) | prior_export=1.05 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PATH  ·  score **+15**  ·  Software - Infrastructure
price=16.389999389648438  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.55 on 2026-08-21; prev RSI=66.94 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.94@2026-08-20 → 69.55@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.94@2026-08-20 → 69.55@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.94@2026-08-20 → 69.55@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9100 R=0.0000); 2026-08-20:GREEN:O=15.5500,C=15.9200,body=+0.3700,vol=66428000.0; 2026-08-21:GREEN:O=15.8500,C=16.3900,body=+0.5400,vol=52219100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=118647100 Rvol=0); 2026-08-20:GREEN:O=15.5500,C=15.9200,body=+0.3700,vol=66428000.0; 2026-08-21:GREEN:O=15.8500,C=16.3900,body=+0.5400,vol=52219100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.792 on 2026-08-21: today_vol=52219100 / avg20=65971040 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.581 on 2026-08-21 (price=16.3900, mid=14.5235, upper=17.7356, lower=11.3114; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=16.3900 vs SMA50=12.4724 dist=+31.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=14.5235 SMA50=12.4724 SMA80=11.8793 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-18@9.8800; S2[2026-06-24→2026-07-23] low=2026-06-26@9.8700; S3[2026-07-24→2026-08-21] low=2026-07-24@10.2700 | lows=[9.880000114440918, 9.869999885559082, 10.270000457763672] span=4.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6711738752352502 wick_frac=0.3288261247647498 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.0400:wick=0.6000; 2026-08-18:GREEN:body=+0.1900:wick=0.7600; 2026-08-19:GREEN:body=+0.1800:wick=0.4700; 2026-08-20:GREEN:body=+0.3700:wick=0.2200; 2026-08-21:GREEN:body=+0.5400:wick=0.2150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.01 (current export asof; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.14 (current export; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1720.86 | **NEUTRAL** |
| `B04_income` | 361.92 | **GOOD** |
| `B05_profit_margin` | 21.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.6600000000000001 (now=15.73 vs prior_export=14.07 on finviz_2026-09-03) | **GOOD** |
| `B09_analyst_recom` | 2.82 | **NEUTRAL** |
| `B10_insider_transactions` | -1.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.24 vs prior=-1.24 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.96 | **GOOD** |
| `B13_short_float` | 29.27 | **GOOD** |
| `B14_earnings_date` | 9/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.01 (this export) | prior_export=-5.9 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.14 (this export) | prior_export=5.24 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAA  ·  score **+15**  ·  Oil & Gas Midstream
price=24.610000610351562  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.23 on 2026-08-21; prev RSI=63.06 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.06@2026-08-20 → 63.23@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.06@2026-08-20 → 63.23@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.06@2026-08-20 → 63.23@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=6.600 (G=0.3300 R=0.0500); 2026-08-20:GREEN:O=24.2600,C=24.5900,body=+0.3300,vol=2603400.0; 2026-08-21:RED:O=24.6600,C=24.6100,body=-0.0500,vol=2882100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.903 (Gvol=2603400 Rvol=2882100); 2026-08-20:GREEN:O=24.2600,C=24.5900,body=+0.3300,vol=2603400.0; 2026-08-21:RED:O=24.6600,C=24.6100,body=-0.0500,vol=2882100.0 | **BAD** |
| `A07_rvol` | RVOL=1.082 on 2026-08-21: today_vol=2882100 / avg20=2664125 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.749 on 2026-08-21 (price=24.6100, mid=23.8169, upper=24.8751, lower=22.7587; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=24.6100 vs SMA50=22.8873 dist=+7.53% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=23.8169 SMA50=22.8873 SMA80=22.6586 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-18@20.6070; S2[2026-06-22→2026-07-21] low=2026-06-22@20.7545; S3[2026-07-23→2026-08-21] low=2026-08-07@22.7300 | lows=[20.607006609339088, 20.7544821990416, 22.729999542236328] span=10.30% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6734695466346958 wick_frac=0.3265304533653041 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.12820337258891193 wick_frac=0.8717966274110881 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.600099183642328 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.0600:wick=0.1900; 2026-08-18:GREEN:body=+0.2100:wick=0.1500; 2026-08-19:RED:body=-0.3400:wick=0.2000; 2026-08-20:GREEN:body=+0.3300:wick=0.1600; 2026-08-21:RED:body=-0.0500:wick=0.3400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.44 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=39.05 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 2548.0 | **GOOD** |
| `B05_profit_margin` | 4.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.19 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.19000000000000128 (now=25.19 vs prior_export=25.0 on finviz_2026-09-03) | **GOOD** |
| `B09_analyst_recom` | 2.37 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.97 | **GOOD** |
| `B13_short_float` | 3.0 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.44 (this export) | prior_export=7.44 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=39.05 (this export) | prior_export=39.05 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+15**  ·  Oil & Gas Midstream
price=26.84000015258789  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.77 on 2026-08-21; prev RSI=66.44 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.44@2026-08-20 → 66.77@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.44@2026-08-20 → 66.77@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.44@2026-08-20 → 66.77@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=13.000 (G=0.3900 R=0.0300); 2026-08-20:GREEN:O=26.4100,C=26.8000,body=+0.3900,vol=1429400.0; 2026-08-21:RED:O=26.8700,C=26.8400,body=-0.0300,vol=2372600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.602 (Gvol=1429400 Rvol=2372600); 2026-08-20:GREEN:O=26.4100,C=26.8000,body=+0.3900,vol=1429400.0; 2026-08-21:RED:O=26.8700,C=26.8400,body=-0.0300,vol=2372600.0 | **BAD** |
| `A07_rvol` | RVOL=1.421 on 2026-08-21: today_vol=2372600 / avg20=1669755 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.915 on 2026-08-21 (price=26.8400, mid=25.7750, upper=26.9394, lower=24.6106; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=26.8400 vs SMA50=24.7929 dist=+8.26% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=25.7750 SMA50=24.7929 SMA80=24.4849 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-18@22.4514; S2[2026-06-22→2026-07-23] low=2026-06-22@22.6876; S3[2026-07-24→2026-08-21] low=2026-08-07@24.6700 | lows=[22.451422571353564, 22.6876479865078, 24.670000076293945] span=9.88% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6724128292232158 wick_frac=0.3275871707767843 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.0810827529679824 wick_frac=0.9189172470320176 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.99968211583699 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.0600:wick=0.2300; 2026-08-18:GREEN:body=+0.3200:wick=0.0600; 2026-08-19:RED:body=-0.1700:wick=0.3600; 2026-08-20:GREEN:body=+0.3900:wick=0.1900; 2026-08-21:RED:body=-0.0300:wick=0.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.25 (now=25.25 vs prior_export=25.0 on finviz_2026-09-03) | **GOOD** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 7.59 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=12.77 vs prior_export=12.77 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-09-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 18.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-33.96 (this export) | prior_export=-33.96 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+15**  ·  Scientific & Technical Instruments
price=32.97999954223633  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.33 on 2026-08-21; prev RSI=54.20 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.20@2026-08-20 → 55.33@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.20@2026-08-20 → 55.33@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.20@2026-08-20 → 55.33@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=4.250 (G=0.1700 R=0.0400); 2026-08-20:GREEN:O=32.6400,C=32.8100,body=+0.1700,vol=1547600.0; 2026-08-21:RED:O=33.0200,C=32.9800,body=-0.0400,vol=1254800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.233 (Gvol=1547600 Rvol=1254800); 2026-08-20:GREEN:O=32.6400,C=32.8100,body=+0.1700,vol=1547600.0; 2026-08-21:RED:O=33.0200,C=32.9800,body=-0.0400,vol=1254800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.737 on 2026-08-21: today_vol=1254800 / avg20=1702645 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.032 on 2026-08-21 (price=32.9800, mid=33.0500, upper=35.2518, lower=30.8482; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=32.9800 vs SMA50=30.8448 dist=+6.92% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=33.0500 SMA50=30.8448 SMA80=30.8246 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@27.2291; S2[2026-06-18→2026-07-20] low=2026-07-08@28.1500; S3[2026-07-23→2026-08-21] low=2026-07-23@29.7700 | lows=[27.229069258481378, 28.149999618530273, 29.770000457763672] span=9.33% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.18182077811867614 wick_frac=0.8181792218813239 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.08510810986299591 wick_frac=0.9148918901370041 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.2499523173755485 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.3300:wick=0.1810; 2026-08-18:RED:body=-0.1900:wick=0.7100; 2026-08-19:GREEN:body=+0.7300:wick=0.0100; 2026-08-20:GREEN:body=+0.1700:wick=0.7650; 2026-08-21:RED:body=-0.0400:wick=0.4300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.57 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.28 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3068.3 | **NEUTRAL** |
| `B04_income` | 348.0 | **GOOD** |
| `B05_profit_margin` | 11.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.22 vs prior_export=40.22 on finviz_2026-09-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.58 (now=0.0 vs prior=-0.58 on finviz_2026-09-03) | **GOOD** |
| `B12_institutional_transactions` | 3.21 | **GOOD** |
| `B13_short_float` | 8.41 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.57 (this export) | prior_export=10.57 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.28 (this export) | prior_export=1.28 (finviz_2026-09-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-04_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.