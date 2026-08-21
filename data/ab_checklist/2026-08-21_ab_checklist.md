# A+B1 Feature Checklist — 2026-08-21

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,707** names
- Export: `finviz_2026-08-21.csv` · prior export for Δ: `2026-08-20`
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
| 1 | SON | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 2 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 3 | NWBI | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Banks - Regional |
| 4 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 5 | WEN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 6 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 7 | PANW | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 8 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 9 | DRH | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Hotel & Motel |
| 10 | QGEN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Diagnostics & Research |
| 11 | HLN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Drug Manufacturers - Specialty & Gen |
| 12 | AVNT | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Specialty Chemicals |
| 13 | PLTR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 14 | TILE | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Furnishings, Fixtures & Appliances |
| 15 | WAT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Diagnostics & Research |

## Full checklist — top 15

### SON  ·  score **+17**  ·  Packaging & Containers
price=59.47999954223633  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.69 on 2026-08-21; prev RSI=57.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0400 R=0.0000); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=569091.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1146291 Rvol=0); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=569091.0 | **GOOD** |
| `A07_rvol` | RVOL=0.562 on 2026-08-21: today_vol=569091 / avg20=1012315 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
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
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=1.37 vs prior=1.36 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 4.61 | **GOOD** |
| `B13_short_float` | 11.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+16**  ·  Engineering & Construction
price=38.58000183105469  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.00 on 2026-08-21; prev RSI=55.61 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6400 R=0.0000); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1051755.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=2.827 (Gvol=1627405 Rvol=575650); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1051755.0 | **GOOD** |
| `A07_rvol` | RVOL=0.674 on 2026-08-21: today_vol=1051755 / avg20=1561180 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.586 on 2026-08-21 (price=38.5800, mid=37.4205, upper=39.4004, lower=35.4406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.5800 vs SMA50=35.8930 dist=+7.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=37.4205 SMA50=35.8930 SMA80=35.2023 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@30.9469; S2[2026-06-18→2026-07-20] low=2026-06-22@31.6100; S3[2026-07-23→2026-08-21] low=2026-07-30@32.1400 | lows=[30.946867052586228, 31.610000610351562, 32.13999938964844] span=3.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7111164799579535 wick_frac=0.28888352004204654 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.8200:wick=0.0900; 2026-08-18:RED:body=-0.2600:wick=0.5300; 2026-08-19:GREEN:body=+0.2700:wick=0.6400; 2026-08-20:DOJI:body=+0.0000:wick=0.9600; 2026-08-21:GREEN:body=+0.6400:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.1 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+16**  ·  Banks - Regional
price=15.420000076293945  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.05 on 2026-08-21; prev RSI=47.68 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.500 (G=0.0900 R=0.0600); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=848488.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.786 (Gvol=667000 Rvol=848488); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=848488.0 | **BAD** |
| `A07_rvol` | RVOL=0.874 on 2026-08-21: today_vol=848488 / avg20=970710 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
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
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.01999999999999999 (now=0.19 vs prior=0.17 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 2.25 | **GOOD** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BLMN  ·  score **+16**  ·  Restaurants
price=11.630000114440918  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.89 on 2026-08-21; prev RSI=58.18 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0600 R=0.0000); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6000,C=11.6300,body=+1.0300,vol=1816384.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3286284 Rvol=0); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6000,C=11.6300,body=+1.0300,vol=1816384.0 | **GOOD** |
| `A07_rvol` | RVOL=0.824 on 2026-08-21: today_vol=1816384 / avg20=2203390 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.614 on 2026-08-21 (price=11.6300, mid=10.1665, upper=12.5510, lower=7.7820; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=11.6300 vs SMA50=9.0324 dist=+28.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=10.1665 SMA50=9.0324 SMA80=8.4386 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-08@7.0300; S2[2026-06-22→2026-07-22] low=2026-07-08@7.6600; S3[2026-07-23→2026-08-21] low=2026-07-24@7.9100 | lows=[7.03000020980835, 7.659999847412109, 7.909999847412109] span=12.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5078195096738243 wick_frac=0.4921804903261758 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.2700:wick=0.3000; 2026-08-18:RED:body=-0.0100:wick=0.4200; 2026-08-19:GREEN:body=+0.0100:wick=0.5000; 2026-08-20:GREEN:body=+0.0300:wick=0.3700; 2026-08-21:GREEN:body=+1.0300:wick=0.0650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=35.18 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3979.52 | **NEUTRAL** |
| `B04_income` | 29.38 | **GOOD** |
| `B05_profit_margin` | 0.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.34 | **GOOD** |
| `B13_short_float` | 10.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WEN  ·  score **+16**  ·  Restaurants
price=9.010000228881836  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.25 on 2026-08-21; prev RSI=62.13 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.13@2026-08-20 → 64.25@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.13@2026-08-20 → 64.25@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.13@2026-08-20 → 64.25@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.286 (G=0.1600 R=0.0700); 2026-08-20:RED:O=8.8800,C=8.8100,body=-0.0700,vol=5053800.0; 2026-08-21:GREEN:O=8.8500,C=9.0100,body=+0.1600,vol=5539520.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.096 (Gvol=5539520 Rvol=5053800); 2026-08-20:RED:O=8.8800,C=8.8100,body=-0.0700,vol=5053800.0; 2026-08-21:GREEN:O=8.8500,C=9.0100,body=+0.1600,vol=5539520.0 | **GOOD** |
| `A07_rvol` | RVOL=0.550 on 2026-08-21: today_vol=5539520 / avg20=10075855 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.800 on 2026-08-21 (price=9.0100, mid=8.0125, upper=9.2597, lower=6.7653; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=9.0100 vs SMA50=7.6808 dist=+17.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=8.0125 SMA50=7.6808 SMA80=7.4897 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-23@6.0700; S2[2026-06-24→2026-07-23] low=2026-07-23@7.0800; S3[2026-07-24→2026-08-21] low=2026-07-24@6.9700 | lows=[6.070000171661377, 7.079999923706055, 6.96999979019165] span=16.64% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5079367001613679 wick_frac=0.49206329983863206 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.30434584302554596 wick_frac=0.695654156974454 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.285722070844687 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.0900:wick=0.1200; 2026-08-18:RED:body=-0.0100:wick=0.2200; 2026-08-19:GREEN:body=+0.4100:wick=0.0600; 2026-08-20:RED:body=-0.0700:wick=0.1600; 2026-08-21:GREEN:body=+0.1600:wick=0.1550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.5 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.41 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2203.7 | **NEUTRAL** |
| `B04_income` | 126.06 | **GOOD** |
| `B05_profit_margin` | 5.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 7.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=7.79 vs prior_export=7.79 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 3.12 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.2 | **GOOD** |
| `B13_short_float` | 33.96 | **GOOD** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.5 (this export) | prior_export=10.5 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.41 (this export) | prior_export=2.41 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+16**  ·  Marine Shipping
price=30.5  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.14 on 2026-08-21; prev RSI=63.88 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.88@2026-08-20 → 66.14@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.88@2026-08-20 → 66.14@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.88@2026-08-20 → 66.14@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.395 (G=1.0250 R=0.1900); 2026-08-20:RED:O=30.2500,C=30.0600,body=-0.1900,vol=1605200.0; 2026-08-21:GREEN:O=29.4750,C=30.5000,body=+1.0250,vol=2647916.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.650 (Gvol=2647916 Rvol=1605200); 2026-08-20:RED:O=30.2500,C=30.0600,body=-0.1900,vol=1605200.0; 2026-08-21:GREEN:O=29.4750,C=30.5000,body=+1.0250,vol=2647916.0 | **GOOD** |
| `A07_rvol` | RVOL=1.761 on 2026-08-21: today_vol=2647916 / avg20=1503535 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.872 on 2026-08-21 (price=30.5000, mid=28.7450, upper=30.7565, lower=26.7335; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=30.5000 vs SMA50=27.1323 dist=+12.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=28.7450 SMA50=27.1323 SMA80=26.7022 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@25.4230; S2[2026-06-18→2026-07-20] low=2026-06-26@23.8600; S3[2026-07-23→2026-08-21] low=2026-07-23@26.4700 | lows=[25.423019363615396, 23.860000610351562, 26.469999313354492] span=10.94% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6621443128248221 wick_frac=0.33785568717517783 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.29230868548389466 wick_frac=0.7076913145161053 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.39471967073232 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.0400:wick=0.1400; 2026-08-18:GREEN:body=+0.1000:wick=0.5400; 2026-08-19:RED:body=-0.1300:wick=0.7400; 2026-08-20:RED:body=-0.1900:wick=0.4600; 2026-08-21:GREEN:body=+1.0250:wick=0.5230 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.48 vs prior=-0.48 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.19 | **GOOD** |
| `B13_short_float` | 2.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+16**  ·  Software - Infrastructure
price=357.8699951171875  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.58 on 2026-08-21; prev RSI=47.92 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.178 (G=5.3700 R=4.5600); 2026-08-20:RED:O=354.1200,C=349.5600,body=-4.5600,vol=4736200.0; 2026-08-21:GREEN:O=352.5000,C=357.8700,body=+5.3700,vol=5107946.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.078 (Gvol=5107946 Rvol=4736200); 2026-08-20:RED:O=354.1200,C=349.5600,body=-4.5600,vol=4736200.0; 2026-08-21:GREEN:O=352.5000,C=357.8700,body=+5.3700,vol=5107946.0 | **GOOD** |
| `A07_rvol` | RVOL=0.904 on 2026-08-21: today_vol=5107946 / avg20=5652170 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.003 on 2026-08-21 (price=357.8700, mid=358.0335, upper=408.4984, lower=307.5686; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=357.8700 vs SMA50=335.8386 dist=+6.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=358.0335 SMA50=335.8386 SMA80=299.2571 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-27@243.0400; S2[2026-06-24→2026-07-23] low=2026-06-24@284.2800; S3[2026-07-24→2026-08-21] low=2026-07-28@308.5400 | lows=[243.0399932861328, 284.2799987792969, 308.5400085449219] span=26.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.3072083019078745 wick_frac=0.6927916980921255 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.35022970185636604 wick_frac=0.649770298143634 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1776311386542813 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-7.4900:wick=6.2100; 2026-08-18:GREEN:body=+3.8000:wick=6.2400; 2026-08-19:RED:body=-15.5000:wick=9.4700; 2026-08-20:RED:body=-4.5600:wick=8.4600; 2026-08-21:GREEN:body=+5.3700:wick=12.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.2 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10606.3 | **NEUTRAL** |
| `B04_income` | 842.8 | **GOOD** |
| `B05_profit_margin` | 7.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 362.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.2799999999999727 (now=362.7 vs prior_export=361.42 on finviz_2026-08-20) | **GOOD** |
| `B09_analyst_recom` | 1.65 | **GOOD** |
| `B10_insider_transactions` | -0.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-0.82 vs prior=-0.83 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 3.86 | **GOOD** |
| `B13_short_float` | 2.6 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.2 (this export) | prior_export=7.2 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+16**  ·  Computer Hardware
price=188.64999389648438  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.73 on 2026-08-21; prev RSI=48.51 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.128 (G=4.8800 R=1.5600); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8120203.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.930 (Gvol=8120203 Rvol=4207300); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8120203.0 | **GOOD** |
| `A07_rvol` | RVOL=1.017 on 2026-08-21: today_vol=8120203 / avg20=7987355 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
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
| `B08_target_price_delta` | delta=0.0 (now=249.97 vs prior_export=249.97 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -2.94 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.94 vs prior=-2.94 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.44 | **GOOD** |
| `B13_short_float` | 1.22 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRH  ·  score **+16**  ·  REIT - Hotel & Motel
price=12.819999694824219  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.65 on 2026-08-21; prev RSI=56.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.1200 R=0.0600); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1504211.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.243 (Gvol=1869100 Rvol=1504211); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1504211.0 | **GOOD** |
| `A07_rvol` | RVOL=0.594 on 2026-08-21: today_vol=1504211 / avg20=2534385 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.110 on 2026-08-21 (price=12.8200, mid=12.7280, upper=13.5676, lower=11.8884; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=12.8200 vs SMA50=12.3671 dist=+3.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=12.7280 SMA50=12.3671 SMA80=11.7526 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-21 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@10.4726; S2[2026-06-23→2026-07-23] low=2026-07-08@11.5700; S3[2026-07-24→2026-08-21] low=2026-08-11@11.9900 | lows=[10.472552720822522, 11.569999694824219, 11.989999771118164] span=14.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5454576980930872 wick_frac=0.4545423019069128 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2142874172771891 wick_frac=0.7857125827228109 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.1100:wick=0.1700; 2026-08-18:GREEN:body=+0.1200:wick=0.1100; 2026-08-19:RED:body=-0.1800:wick=0.2300; 2026-08-20:GREEN:body=+0.1200:wick=0.1000; 2026-08-21:RED:body=-0.0600:wick=0.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=103.61 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.05 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1136.37 | **NEUTRAL** |
| `B04_income` | 148.78 | **GOOD** |
| `B05_profit_margin` | 13.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.08000000000000007 (now=13.56 vs prior_export=13.48 on finviz_2026-08-20) | **GOOD** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -1.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.6 vs prior=-1.6 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.54 | **GOOD** |
| `B13_short_float` | 6.11 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=103.61 (this export) | prior_export=103.61 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.05 (this export) | prior_export=1.05 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### QGEN  ·  score **+16**  ·  Diagnostics & Research
price=44.06999969482422  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.12 on 2026-08-21; prev RSI=67.19 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.19@2026-08-20 → 61.12@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.19@2026-08-20 → 61.12@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.19@2026-08-20 → 61.12@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.156 (G=0.6900 R=0.3200); 2026-08-20:GREEN:O=44.2100,C=44.9000,body=+0.6900,vol=4797500.0; 2026-08-21:RED:O=44.3900,C=44.0700,body=-0.3200,vol=2902784.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.653 (Gvol=4797500 Rvol=2902784); 2026-08-20:GREEN:O=44.2100,C=44.9000,body=+0.6900,vol=4797500.0; 2026-08-21:RED:O=44.3900,C=44.0700,body=-0.3200,vol=2902784.0 | **GOOD** |
| `A07_rvol` | RVOL=0.955 on 2026-08-21: today_vol=2902784 / avg20=3040135 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.688 on 2026-08-21 (price=44.0700, mid=42.6350, upper=44.7199, lower=40.5501; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=44.0700 vs SMA50=40.3275 dist=+9.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=42.6350 SMA50=40.3275 SMA80=38.1856 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@33.4879; S2[2026-06-18→2026-07-20] low=2026-06-18@35.8665; S3[2026-07-23→2026-08-21] low=2026-07-23@40.5100 | lows=[33.48791749560078, 35.86645935054753, 40.5099983215332] span=20.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5187996018941577 wick_frac=0.48120039810584225 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.463766032728881 wick_frac=0.5362339672711189 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.156259685764013 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.1700:wick=0.4600; 2026-08-18:RED:body=-0.7400:wick=0.4000; 2026-08-19:GREEN:body=+1.0600:wick=0.3700; 2026-08-20:GREEN:body=+0.6900:wick=0.6400; 2026-08-21:RED:body=-0.3200:wick=0.3700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.61 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.43 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2100.36 | **NEUTRAL** |
| `B04_income` | 409.32 | **GOOD** |
| `B05_profit_margin` | 19.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.04 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.04 vs prior_export=45.04 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.28 | **GOOD** |
| `B13_short_float` | 4.02 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.61 (this export) | prior_export=3.61 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.43 (this export) | prior_export=1.43 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HLN  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=10.0  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.46 on 2026-08-21; prev RSI=54.40 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.40@2026-08-20 → 58.46@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.40@2026-08-20 → 58.46@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.40@2026-08-20 → 58.46@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1400 R=0.0000); 2026-08-20:GREEN:O=9.8200,C=9.8700,body=+0.0500,vol=8078500.0; 2026-08-21:GREEN:O=9.9100,C=10.0000,body=+0.0900,vol=7171279.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=15249779 Rvol=0); 2026-08-20:GREEN:O=9.8200,C=9.8700,body=+0.0500,vol=8078500.0; 2026-08-21:GREEN:O=9.9100,C=10.0000,body=+0.0900,vol=7171279.0 | **GOOD** |
| `A07_rvol` | RVOL=0.692 on 2026-08-21: today_vol=7171279 / avg20=10360385 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.410 on 2026-08-21 (price=10.0000, mid=9.8630, upper=10.1968, lower=9.5291; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=10.0000 vs SMA50=9.5609 dist=+4.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=9.8630 SMA50=9.5609 SMA80=9.3895 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-03@8.5935; S2[2026-06-18→2026-07-20] low=2026-06-18@8.7078; S3[2026-07-23→2026-08-21] low=2026-07-23@9.5621 | lows=[8.593510009673297, 8.70776015533131, 9.562143384982017] span=11.27% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5313397059488486 wick_frac=0.4686602940511514 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.0600:wick=0.0400; 2026-08-18:GREEN:body=+0.0700:wick=0.1050; 2026-08-19:GREEN:body=+0.0600:wick=0.1100; 2026-08-20:GREEN:body=+0.0500:wick=0.0850; 2026-08-21:GREEN:body=+0.0900:wick=0.0400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.63 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.18 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 14967.67 | **NEUTRAL** |
| `B04_income` | 2177.99 | **GOOD** |
| `B05_profit_margin` | 14.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.019999999999999574 (now=11.36 vs prior_export=11.34 on finviz_2026-08-20) | **GOOD** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 21.34 | **GOOD** |
| `B13_short_float` | 0.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.63 (this export) | prior_export=3.63 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.18 (this export) | prior_export=1.18 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVNT  ·  score **+15**  ·  Specialty Chemicals
price=44.65999984741211  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.72 on 2026-08-21; prev RSI=59.90 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=637084.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1280984 Rvol=0); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=637084.0 | **GOOD** |
| `A07_rvol` | RVOL=0.704 on 2026-08-21: today_vol=637084 / avg20=904395 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.356 on 2026-08-21 (price=44.6600, mid=41.8005, upper=49.8272, lower=33.7738; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=44.6600 vs SMA50=38.7045 dist=+15.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=41.8005 SMA50=38.7045 SMA80=37.3873 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@31.9895; S2[2026-06-18→2026-07-20] low=2026-07-08@34.9500; S3[2026-07-23→2026-08-21] low=2026-07-29@35.5700 | lows=[31.989543385452947, 34.95000076293945, 35.56999969482422] span=11.19% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6176655296278847 wick_frac=0.3823344703721153 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.6700:wick=0.2300; 2026-08-18:RED:body=-1.7400:wick=0.0300; 2026-08-19:GREEN:body=+0.0800:wick=0.7700; 2026-08-20:GREEN:body=+0.6100:wick=0.4700; 2026-08-21:GREEN:body=+0.5800:wick=0.2850 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.51 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.06 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3331.5 | **NEUTRAL** |
| `B04_income` | 170.0 | **GOOD** |
| `B05_profit_margin` | 5.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.75 vs prior_export=50.75 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.64 | **GOOD** |
| `B13_short_float` | 4.48 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.51 (this export) | prior_export=7.51 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.06 (this export) | prior_export=2.06 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLTR  ·  score **+15**  ·  Software - Infrastructure
price=179.94000244140625  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.41 on 2026-08-21; prev RSI=66.30 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.104 (G=5.9600 R=1.9200); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40929832.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.515 (Gvol=40929832 Rvol=27018400); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40929832.0 | **GOOD** |
| `A07_rvol` | RVOL=0.878 on 2026-08-21: today_vol=40929832 / avg20=46642400 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
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
| `B07_target_price` | 199.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=199.08 vs prior_export=199.08 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.91 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.57 | **GOOD** |
| `B13_short_float` | 3.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TILE  ·  score **+15**  ·  Furnishings, Fixtures & Appliances
price=38.939998626708984  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.28 on 2026-08-21; prev RSI=67.39 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.39@2026-08-20 → 66.28@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.098 (G=0.5600 R=0.5100); 2026-08-20:GREEN:O=38.5200,C=39.0800,body=+0.5600,vol=482300.0; 2026-08-21:RED:O=39.4500,C=38.9400,body=-0.5100,vol=314891.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.532 (Gvol=482300 Rvol=314891); 2026-08-20:GREEN:O=38.5200,C=39.0800,body=+0.5600,vol=482300.0; 2026-08-21:RED:O=39.4500,C=38.9400,body=-0.5100,vol=314891.0 | **GOOD** |
| `A07_rvol` | RVOL=0.691 on 2026-08-21: today_vol=314891 / avg20=455630 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
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
| `B08_target_price_delta` | delta=0.0 (now=45.25 vs prior_export=45.25 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -8.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.99 vs prior=-8.99 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.56 | **GOOD** |
| `B13_short_float` | 7.64 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.59 (this export) | prior_export=37.59 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+15**  ·  Diagnostics & Research
price=410.70001220703125  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.04 on 2026-08-21; prev RSI=56.08 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.895 (G=10.0600 R=5.3100); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=683844.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.554 (Gvol=683844 Rvol=1233800); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=683844.0 | **BAD** |
| `A07_rvol` | RVOL=0.738 on 2026-08-21: today_vol=683844 / avg20=926920 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.382 on 2026-08-21 (price=410.7000, mid=398.7880, upper=429.9547, lower=367.6213; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=410.7000 vs SMA50=380.8772 dist=+7.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=398.7880 SMA50=380.8772 SMA80=365.6629 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@323.8500; S2[2026-06-18→2026-07-20] low=2026-06-22@353.5300; S3[2026-07-23→2026-08-21] low=2026-07-27@367.6700 | lows=[323.8500061035156, 353.5299987792969, 367.6700134277344] span=13.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5938604550613414 wick_frac=0.4061395449386586 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2900053668276715 wick_frac=0.7099946331723285 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8945390176898584 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.7400:wick=9.1600; 2026-08-18:RED:body=-5.9900:wick=2.7200; 2026-08-19:GREEN:body=+10.1100:wick=5.0700; 2026-08-20:RED:body=-5.3100:wick=13.0000; 2026-08-21:GREEN:body=+10.0600:wick=6.8800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3770.58 | **NEUTRAL** |
| `B04_income` | 449.25 | **GOOD** |
| `B05_profit_margin` | 11.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 439.95 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=439.95 vs prior_export=439.95 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -1.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.6000000000000001 (now=-1.52 vs prior=-2.12 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 26.0 | **GOOD** |
| `B13_short_float` | 4.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-21_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.