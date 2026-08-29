# A+B1 Feature Checklist — 2026-08-28

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,696** names
- Export: `finviz_2026-08-28.csv` · prior export for Δ: `2026-08-27`
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
| 2 | SON | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 3 | ANET | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Computer Hardware |
| 4 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 5 | DRH | +16 | 17 | 1 | 2026-08-20→2026-08-21 | REIT - Hotel & Motel |
| 6 | MSI | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Communication Equipment |
| 7 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 8 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 9 | CBRL | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Restaurants |
| 10 | SOLV | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Medical Instruments & Supplies |
| 11 | AMCR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 12 | ARDT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Care Facilities |
| 13 | FCX | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Copper |
| 14 | PLTR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 15 | AVNT | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Specialty Chemicals |
| 16 | ABR | +15 | 15 | 0 | 2026-08-20→2026-08-21 | REIT - Mortgage |
| 17 | CP | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Railroads |
| 18 | AHR | +15 | 17 | 2 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 19 | WAT | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Diagnostics & Research |
| 20 | RUSHA | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Auto & Truck Dealerships |

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
| `B08_target_price_delta` | delta=0.0 (now=250.13 vs prior_export=250.13 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.06 | **GOOD** |
| `B13_short_float` | 29.02 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SON  ·  score **+17**  ·  Packaging & Containers
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
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.11999999999999988 (now=1.49 vs prior=1.37 on finviz_2026-08-27) | **GOOD** |
| `B12_institutional_transactions` | 11.88 | **GOOD** |
| `B13_short_float` | 10.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=249.97 vs prior_export=249.97 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.97 vs prior=-2.97 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.1 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.64 | **GOOD** |
| `B13_short_float` | 6.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRH  ·  score **+16**  ·  REIT - Hotel & Motel
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
| `B08_target_price_delta` | delta=0.15000000000000036 (now=13.71 vs prior_export=13.56 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -1.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.6 vs prior=-1.6 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.35 | **GOOD** |
| `B13_short_float` | 7.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=103.61 (this export) | prior_export=103.61 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.05 (this export) | prior_export=1.05 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 530.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.0 (now=530.5 vs prior_export=528.5 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.65 | **GOOD** |
| `B10_insider_transactions` | -5.45 | **BAD** |
| `B11_insider_tx_delta` | delta=6.489999999999999 (now=-5.45 vs prior=-11.94 on finviz_2026-08-27) | **GOOD** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 1.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.51 (this export) | prior_export=14.51 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.37 (this export) | prior_export=4.37 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.62 | **GOOD** |
| `B13_short_float` | 9.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.66 vs prior=-0.66 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.37 | **GOOD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 23.31 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=95.83 vs prior_export=95.83 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.38 | **BAD** |
| `B13_short_float` | 2.4 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.89 (this export) | prior_export=33.89 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.55 (this export) | prior_export=2.55 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMCR  ·  score **+15**  ·  Packaging & Containers
price=48.59000015258789  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.84 on 2026-08-21; prev RSI=61.84 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.84@2026-08-20 → 64.84@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.84@2026-08-20 → 64.84@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.84@2026-08-20 → 64.84@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5300 R=0.0000); 2026-08-20:GREEN:O=46.6400,C=47.8100,body=+1.1700,vol=4900100.0; 2026-08-21:GREEN:O=48.2300,C=48.5900,body=+0.3600,vol=3057900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7958000 Rvol=0); 2026-08-20:GREEN:O=46.6400,C=47.8100,body=+1.1700,vol=4900100.0; 2026-08-21:GREEN:O=48.2300,C=48.5900,body=+0.3600,vol=3057900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.749 on 2026-08-21: today_vol=3057900 / avg20=4084595 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.953 on 2026-08-21 (price=48.5900, mid=46.6175, upper=48.6875, lower=44.5475; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=48.5900 vs SMA50=44.1438 dist=+10.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=46.6175 SMA50=44.1438 SMA80=41.8290 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-21 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@36.5511; S2[2026-06-23→2026-07-23] low=2026-06-23@40.0800; S3[2026-07-24→2026-08-21] low=2026-07-24@42.9200 | lows=[36.551127113761716, 40.08000183105469, 42.91999816894531] span=17.42% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5625008514947707 wick_frac=0.4374991485052293 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.4700:wick=0.3400; 2026-08-18:DOJI:body=+0.0000:wick=0.6500; 2026-08-19:GREEN:body=+0.9800:wick=0.0700; 2026-08-20:GREEN:body=+1.1700:wick=0.5100; 2026-08-21:GREEN:body=+0.3600:wick=0.4800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.19 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23506.0 | **NEUTRAL** |
| `B04_income` | 1106.0 | **GOOD** |
| `B05_profit_margin` | 4.71 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.030000000000001137 (now=49.06 vs prior_export=49.03 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.54 | **GOOD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.19 (this export) | prior_export=3.19 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=12.68 vs prior_export=12.68 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.77 | **GOOD** |
| `B13_short_float` | 18.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-33.96 (this export) | prior_export=-33.96 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=74.0 vs prior_export=74.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.54 | **GOOD** |
| `B10_insider_transactions` | -1.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.18 vs prior=-1.18 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 1.82 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.25 (this export) | prior_export=20.25 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=200.88 vs prior_export=200.88 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -1.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.8 vs prior=-1.8 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.91 | **GOOD** |
| `B13_short_float` | 3.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=50.75 vs prior_export=50.75 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.36 | **GOOD** |
| `B13_short_float` | 4.32 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.51 (this export) | prior_export=7.51 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.06 (this export) | prior_export=2.06 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=5.88 vs prior_export=5.88 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 3.4 | **NEUTRAL** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.2 | **GOOD** |
| `B13_short_float` | 24.92 | **GOOD** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=100.0 (this export) | prior_export=100.0 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.54 (this export) | prior_export=7.54 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B07_target_price` | 102.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.78 vs prior_export=102.78 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.14 | **GOOD** |
| `B13_short_float` | 2.13 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AHR  ·  score **+15**  ·  REIT - Healthcare Facilities
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
| `B08_target_price_delta` | delta=0.0 (now=63.67 vs prior_export=63.67 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.76 | **BAD** |
| `B11_insider_tx_delta` | delta=-1.7 (now=-1.76 vs prior=-0.06 on finviz_2026-08-27) | **BAD** |
| `B12_institutional_transactions` | 17.27 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+15**  ·  Diagnostics & Research
price=410.70001220703125  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.04 on 2026-08-21; prev RSI=56.08 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.08@2026-08-20 → 59.04@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.895 (G=10.0600 R=5.3100); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=816900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.662 (Gvol=816900 Rvol=1233800); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=816900.0 | **BAD** |
| `A07_rvol` | RVOL=0.881 on 2026-08-21: today_vol=816900 / avg20=926920 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.382 on 2026-08-21 (price=410.7000, mid=398.7880, upper=429.9547, lower=367.6213; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=410.7000 vs SMA50=380.8772 dist=+7.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=398.7880 SMA50=380.8772 SMA80=365.6629 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@323.8500; S2[2026-06-18→2026-07-20] low=2026-06-22@353.5300; S3[2026-07-23→2026-08-21] low=2026-07-27@367.6700 | lows=[323.8500061035156, 353.5299987792969, 367.6700134277344] span=13.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5935097530148464 wick_frac=0.4064902469851535 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2900053668276715 wick_frac=0.7099946331723285 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8945390176898584 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.7400:wick=9.1600; 2026-08-18:RED:body=-5.9900:wick=2.7200; 2026-08-19:GREEN:body=+10.1100:wick=5.0700; 2026-08-20:RED:body=-5.3100:wick=13.0000; 2026-08-21:GREEN:body=+10.0600:wick=6.8900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4644.25 | **NEUTRAL** |
| `B04_income` | 166.14 | **GOOD** |
| `B05_profit_margin` | 3.58 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 443.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.6399999999999864 (now=443.59 vs prior_export=439.95 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -1.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.52 vs prior=-1.52 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.12 | **GOOD** |
| `B13_short_float` | 3.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=89.5 vs prior_export=89.5 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -0.74 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.74 vs prior=-0.74 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 7.49 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.46 (this export) | prior_export=6.46 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.36 (this export) | prior_export=0.36 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-28_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.