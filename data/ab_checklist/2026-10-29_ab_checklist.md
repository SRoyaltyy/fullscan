# A+B1 Feature Checklist — 2026-10-29

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-29.csv` · prior export for Δ: `2026-10-28`
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
| 1 | EPD | +17 | 17 | 0 | 2026-10-27→2026-10-28 | Oil & Gas Midstream |
| 2 | SHAK | +16 | 17 | 1 | 2026-10-27→2026-10-28 | Restaurants |
| 3 | ORCL | +16 | 17 | 1 | 2026-10-27→2026-10-28 | Software - Infrastructure |
| 4 | VEEV | +16 | 17 | 1 | 2026-10-27→2026-10-28 | Health Information Services |
| 5 | EC | +16 | 16 | 0 | 2026-10-27→2026-10-28 | Oil & Gas Integrated |
| 6 | GCT | +16 | 17 | 1 | 2026-10-27→2026-10-28 | Software - Infrastructure |
| 7 | MCHP | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Semiconductors |
| 8 | BLMN | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Restaurants |
| 9 | HSIC | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Medical Distribution |
| 10 | FIGS | +15 | 17 | 2 | 2026-10-27→2026-10-28 | Apparel Manufacturing |
| 11 | ESI | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Specialty Chemicals |
| 12 | AMD | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Semiconductors |
| 13 | AUPH | +15 | 16 | 1 | 2026-10-27→2026-10-28 | Biotechnology |
| 14 | SMA | +15 | 16 | 1 | 2026-10-27→2026-10-28 | REIT - Industrial |
| 15 | V | +14 | 16 | 2 | 2026-10-27→2026-10-28 | Credit Services |

## Full checklist — top 15

### EPD  ·  score **+17**  ·  Oil & Gas Midstream
price=38.060001373291016  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.07 on 2026-10-28; prev RSI=49.68 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.68@2026-10-27 → 57.07@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.68@2026-10-27 → 57.07@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.68@2026-10-27 → 57.07@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0900 R=0.0000); 2026-10-27:GREEN:O=36.9300,C=37.1700,body=+0.2400,vol=2560300.0; 2026-10-28:GREEN:O=37.2100,C=38.0600,body=+0.8500,vol=6082200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8642500 Rvol=0); 2026-10-27:GREEN:O=36.9300,C=37.1700,body=+0.2400,vol=2560300.0; 2026-10-28:GREEN:O=37.2100,C=38.0600,body=+0.8500,vol=6082200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.838 on 2026-10-28: today_vol=6082200 / avg20=3308907 (avg window 2026-09-17→2026-10-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.604 on 2026-10-28 (price=38.0600, mid=36.9515, upper=38.7861, lower=35.1169; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=38.0600 vs SMA50=37.9434 dist=+0.31% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.95_50=37.94_80=37.68 on 2026-10-28: SMA20=36.9515 SMA50=37.9434 SMA80=37.6782 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-28 (63 bars); S1[2026-07-20→2026-08-17] low=2026-07-31@37.2200; S2[2026-08-18→2026-09-16] low=2026-08-21@37.9600; S3[2026-09-17→2026-10-28] low=2026-10-01@35.3700 | lows=[37.220001220703125, 37.959999084472656, 35.369998931884766] span=7.32% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6600603309827222 wick_frac=0.33993966901727773 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.5900:wick=0.3074; 2026-10-12:RED:body=-0.2400:wick=0.2600; 2026-10-26:GREEN:body=+0.0800:wick=0.2400; 2026-10-27:GREEN:body=+0.2400:wick=0.2800; 2026-10-28:GREEN:body=+0.8500:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.24 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=33.47 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 58217.0 | **NEUTRAL** |
| `B04_income` | 6244.0 | **GOOD** |
| `B05_profit_margin` | 10.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.65 vs prior_export=41.65 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.5 | **GOOD** |
| `B13_short_float` | 1.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.24 (this export) | prior_export=12.24 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=33.47 (this export) | prior_export=33.47 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHAK  ·  score **+16**  ·  Restaurants
price=67.22000122070312  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.50 on 2026-10-28; prev RSI=46.67 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.67@2026-10-27 → 53.50@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.67@2026-10-27 → 53.50@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.67@2026-10-27 → 53.50@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=9.387 (G=2.9100 R=0.3100); 2026-10-27:RED:O=64.8500,C=64.5400,body=-0.3100,vol=1029618.0; 2026-10-28:GREEN:O=64.3100,C=67.2200,body=+2.9100,vol=1077169.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.046 (Gvol=1077169 Rvol=1029618); 2026-10-27:RED:O=64.8500,C=64.5400,body=-0.3100,vol=1029618.0; 2026-10-28:GREEN:O=64.3100,C=67.2200,body=+2.9100,vol=1077169.0 | **GOOD** |
| `A07_rvol` | RVOL=0.805 on 2026-10-28: today_vol=1077169 / avg20=1338285 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.067 on 2026-10-28 (price=67.2200, mid=66.7735, upper=73.4271, lower=60.1199; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=67.2200 vs SMA50=65.3990 dist=+2.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=66.7735 SMA50=65.3990 SMA80=64.7695 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-07-30@61.5300; S2[2026-08-31→2026-09-29] low=2026-09-18@54.7300; S3[2026-09-30→2026-10-28] low=2026-09-30@57.3000 | lows=[61.529998779296875, 54.72999954223633, 57.29999923706055] span=12.42% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.9432753729996018 wick_frac=0.05672462700039816 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.2526260814419433 wick_frac=0.7473739185580567 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.387182516243355 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:GREEN:body=+0.4800:wick=1.6300; 2026-10-23:RED:body=-1.6100:wick=1.6200; 2026-10-26:RED:body=-0.4700:wick=3.1150; 2026-10-27:RED:body=-0.3100:wick=0.9171; 2026-10-28:GREEN:body=+2.9100:wick=0.1750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=40.75 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1552.3 | **NEUTRAL** |
| `B04_income` | 39.72 | **GOOD** |
| `B05_profit_margin` | 2.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 82.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=82.77 vs prior_export=82.77 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | 2.61 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.61 vs prior=2.61 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | -13.43 | **BAD** |
| `B13_short_float` | 9.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=40.75 (this export) | prior_export=40.75 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ORCL  ·  score **+16**  ·  Software - Infrastructure
price=144.5399932861328  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.60 on 2026-10-28; prev RSI=44.59 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.59@2026-10-27 → 57.60@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.59@2026-10-27 → 57.60@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.59@2026-10-27 → 57.60@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.8700 R=0.0000); 2026-10-27:GREEN:O=126.8800,C=130.5300,body=+3.6500,vol=25904273.0; 2026-10-28:GREEN:O=131.3200,C=144.5400,body=+13.2200,vol=80484071.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=106388344 Rvol=0); 2026-10-27:GREEN:O=126.8800,C=130.5300,body=+3.6500,vol=25904273.0; 2026-10-28:GREEN:O=131.3200,C=144.5400,body=+13.2200,vol=80484071.0 | **GOOD** |
| `A07_rvol` | RVOL=2.978 on 2026-10-28: today_vol=80484071 / avg20=27027479 (avg window 2026-09-30→2026-10-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.674 on 2026-10-28 (price=144.5400, mid=135.2400, upper=149.0334, lower=121.4466; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=144.5400 vs SMA50=142.1110 dist=+1.71% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=135.24_50=142.11_80=139.57 on 2026-10-28: SMA20=135.2400 SMA50=142.1110 SMA80=139.5662 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-31→2026-10-28 (63 bars); S1[2026-07-31→2026-08-28] low=2026-07-31@125.7000; S2[2026-08-31→2026-09-29] low=2026-09-28@131.5800; S3[2026-09-30→2026-10-28] low=2026-10-22@122.7700 | lows=[125.69999694824219, 131.5800018310547, 122.7699966430664] span=7.18% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6840449103807897 wick_frac=0.3159550896192103 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-0.8100:wick=4.3599; 2026-10-23:GREEN:body=+1.2900:wick=2.6400; 2026-10-26:DOJI:body=+0.0000:wick=4.2400; 2026-10-27:GREEN:body=+3.6500:wick=1.5000; 2026-10-28:GREEN:body=+13.2200:wick=6.8300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.41 (current export asof; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 71777.0 | **NEUTRAL** |
| `B04_income` | 18736.0 | **GOOD** |
| `B05_profit_margin` | 26.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 244.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=244.47 vs prior_export=244.47 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.58 | **GOOD** |
| `B13_short_float` | 2.82 | **NEUTRAL** |
| `B14_earnings_date` | 9/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.41 (this export) | prior_export=10.41 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+16**  ·  Health Information Services
price=317.82000732421875  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.08 on 2026-10-28; prev RSI=64.52 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.52@2026-10-27 → 67.08@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.52@2026-10-27 → 67.08@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.52@2026-10-27 → 67.08@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=3.833 (G=7.8200 R=2.0400); 2026-10-27:RED:O=315.5000,C=313.4600,body=-2.0400,vol=1356323.0; 2026-10-28:GREEN:O=310.0000,C=317.8200,body=+7.8200,vol=1395807.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.029 (Gvol=1395807 Rvol=1356323); 2026-10-27:RED:O=315.5000,C=313.4600,body=-2.0400,vol=1356323.0; 2026-10-28:GREEN:O=310.0000,C=317.8200,body=+7.8200,vol=1395807.0 | **GOOD** |
| `A07_rvol` | RVOL=0.953 on 2026-10-28: today_vol=1395807 / avg20=1465022 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.455 on 2026-10-28 (price=317.8200, mid=302.7485, upper=335.8660, lower=269.6310; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=317.8200 vs SMA50=281.4224 dist=+12.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=302.7485 SMA50=281.4224 SMA80=254.2407 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-31→2026-10-28 (63 bars); S1[2026-07-31→2026-08-28] low=2026-07-31@199.0100; S2[2026-08-31→2026-09-29] low=2026-09-21@257.8700; S3[2026-09-30→2026-10-28] low=2026-10-02@272.8350 | lows=[199.00999450683594, 257.8699951171875, 272.8349914550781] span=37.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.7447626023065477 wick_frac=0.2552373976934524 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.257251270920643 wick_frac=0.742748729079357 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.833320867054617 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-1.4400:wick=6.4900; 2026-10-23:GREEN:body=+2.3700:wick=3.3900; 2026-10-26:RED:body=-4.8600:wick=4.1000; 2026-10-27:RED:body=-2.0400:wick=5.8900; 2026-10-28:GREEN:body=+7.8200:wick=2.6800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.38 vs prior=-0.38 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 3.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EC  ·  score **+16**  ·  Oil & Gas Integrated
price=17.68000030517578  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.91 on 2026-10-28; prev RSI=50.23 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.23@2026-10-27 → 54.91@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.23@2026-10-27 → 54.91@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.23@2026-10-27 → 54.91@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=1.867 (G=0.2800 R=0.1500); 2026-10-27:RED:O=17.4400,C=17.2900,body=-0.1500,vol=1060325.0; 2026-10-28:GREEN:O=17.4000,C=17.6800,body=+0.2800,vol=1763652.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.663 (Gvol=1763652 Rvol=1060325); 2026-10-27:RED:O=17.4400,C=17.2900,body=-0.1500,vol=1060325.0; 2026-10-28:GREEN:O=17.4000,C=17.6800,body=+0.2800,vol=1763652.0 | **GOOD** |
| `A07_rvol` | RVOL=1.186 on 2026-10-28: today_vol=1763652 / avg20=1487177 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.421 on 2026-10-28 (price=17.6800, mid=17.2855, upper=18.2218, lower=16.3492; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=17.6800 vs SMA50=17.2300 dist=+2.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=17.2855 SMA50=17.2300 SMA80=16.9136 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-08-04@16.0500; S2[2026-08-31→2026-09-29] low=2026-09-25@16.0700; S3[2026-09-30→2026-10-28] low=2026-10-01@16.0700 | lows=[16.049999237060547, 16.06999969482422, 16.06999969482422] span=0.12% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6292154576782623 wick_frac=0.3707845423217378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.3061218134612166 wick_frac=0.6938781865387834 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8666759915059192 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-0.3200:wick=0.5050; 2026-10-23:GREEN:body=+0.5600:wick=0.1300; 2026-10-26:RED:body=-0.1000:wick=0.4300; 2026-10-27:RED:body=-0.1500:wick=0.3400; 2026-10-28:GREEN:body=+0.2800:wick=0.1650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=27.29 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.82 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 32293.12 | **NEUTRAL** |
| `B04_income` | 3501.13 | **GOOD** |
| `B05_profit_margin` | 10.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.3 vs prior_export=14.3 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 3.31 | **NEUTRAL** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 216.14 | **GOOD** |
| `B13_short_float` | 0.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.29 (this export) | prior_export=27.29 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.82 (this export) | prior_export=7.82 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+16**  ·  Software - Infrastructure
price=60.630001068115234  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.81 on 2026-10-28; prev RSI=62.99 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.99@2026-10-27 → 65.81@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.99@2026-10-27 → 65.81@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.99@2026-10-27 → 65.81@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=2.639 (G=0.9500 R=0.3600); 2026-10-27:RED:O=59.9600,C=59.6000,body=-0.3600,vol=311361.0; 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.298 (Gvol=404220 Rvol=311361); 2026-10-27:RED:O=59.9600,C=59.6000,body=-0.3600,vol=311361.0; 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0 | **GOOD** |
| `A07_rvol` | RVOL=0.625 on 2026-10-28: today_vol=404220 / avg20=646820 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.737 on 2026-10-28 (price=60.6300, mid=57.4740, upper=61.7571, lower=53.1908; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=60.6300 vs SMA50=54.0305 dist=+12.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=57.4740 SMA50=54.0305 SMA80=49.6433 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-07-30@40.7500; S2[2026-08-31→2026-09-29] low=2026-09-01@46.0100; S3[2026-09-30→2026-10-28] low=2026-09-30@50.5800 | lows=[40.75, 46.0099983215332, 50.58000183105469] span=24.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6506858620959945 wick_frac=0.34931413790400545 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.24657591513599666 wick_frac=0.7534240848640034 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6388865341414824 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-0.7800:wick=1.8199; 2026-10-23:RED:body=-0.1950:wick=1.8050; 2026-10-26:GREEN:body=+0.6500:wick=1.3300; 2026-10-27:RED:body=-0.3600:wick=1.1000; 2026-10-28:GREEN:body=+0.9500:wick=0.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MCHP  ·  score **+15**  ·  Semiconductors
price=79.62999725341797  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.36 on 2026-10-28; prev RSI=59.81 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.81@2026-10-27 → 58.36@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.81@2026-10-27 → 58.36@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.81@2026-10-27 → 58.36@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=80.998 (G=3.2400 R=0.0400); 2026-10-27:GREEN:O=76.9300,C=80.1700,body=+3.2400,vol=11855001.0; 2026-10-28:RED:O=79.6700,C=79.6300,body=-0.0400,vol=9308922.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.274 (Gvol=11855001 Rvol=9308922); 2026-10-27:GREEN:O=76.9300,C=80.1700,body=+3.2400,vol=11855001.0; 2026-10-28:RED:O=79.6700,C=79.6300,body=-0.0400,vol=9308922.0 | **GOOD** |
| `A07_rvol` | RVOL=0.964 on 2026-10-28: today_vol=9308922 / avg20=9656304 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.518 on 2026-10-28 (price=79.6300, mid=76.5355, upper=82.5148, lower=70.5562; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=79.6300 vs SMA50=75.1522 dist=+5.96% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=76.54_50=75.15_80=77.17 on 2026-10-28: SMA20=76.5355 SMA50=75.1522 SMA80=77.1685 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-31→2026-10-28 (63 bars); S1[2026-07-31→2026-08-28] low=2026-08-03@72.5800; S2[2026-08-31→2026-09-29] low=2026-09-16@68.7500; S3[2026-09-30→2026-10-28] low=2026-10-15@69.7600 | lows=[72.58000183105469, 68.75, 69.76000213623047] span=5.57% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.8019794875087578 wick_frac=0.19802051249124225 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.01975359807098184 wick_frac=0.9802464019290181 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=80.99809269502194 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-0.6400:wick=1.6900; 2026-10-23:RED:body=-0.1600:wick=1.6700; 2026-10-26:GREEN:body=+1.4500:wick=1.0100; 2026-10-27:GREEN:body=+3.2400:wick=0.8000; 2026-10-28:RED:body=-0.0400:wick=1.9850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.59 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.8 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5122.3 | **NEUTRAL** |
| `B04_income` | 367.2 | **GOOD** |
| `B05_profit_margin` | 7.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.26 vs prior_export=110.26 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | -5.62 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.62 vs prior=-5.62 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.19 | **GOOD** |
| `B13_short_float` | 6.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.59 (this export) | prior_export=8.59 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.8 (this export) | prior_export=1.8 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BLMN  ·  score **+15**  ·  Restaurants
price=9.989999771118164  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.03 on 2026-10-28; prev RSI=48.23 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.23@2026-10-27 → 59.03@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.23@2026-10-27 → 59.03@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.23@2026-10-27 → 59.03@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=34.499 (G=0.6900 R=0.0200); 2026-10-27:RED:O=9.1000,C=9.0800,body=-0.0200,vol=3057581.0; 2026-10-28:GREEN:O=9.3000,C=9.9900,body=+0.6900,vol=4480603.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.465 (Gvol=4480603 Rvol=3057581); 2026-10-27:RED:O=9.1000,C=9.0800,body=-0.0200,vol=3057581.0; 2026-10-28:GREEN:O=9.3000,C=9.9900,body=+0.6900,vol=4480603.0 | **GOOD** |
| `A07_rvol` | RVOL=1.591 on 2026-10-28: today_vol=4480603 / avg20=2816623 (avg window 2026-09-30→2026-10-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.536 on 2026-10-28 (price=9.9900, mid=9.1975, upper=10.6764, lower=7.7186; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=9.9900 vs SMA50=9.2623 dist=+7.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-28: SMA20=9.1975 SMA50=9.2623 SMA80=9.2643 | **BAD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-07-30@8.5700; S2[2026-08-31→2026-09-29] low=2026-09-28@7.7150; S3[2026-09-30→2026-10-28] low=2026-09-30@7.6600 | lows=[8.569999694824219, 7.715000152587891, 7.659999847412109] span=11.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.5476186151137673 wick_frac=0.45238138488623275 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.053334554036458336 wick_frac=0.9466654459635416 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=34.49918939538432 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:GREEN:body=+0.4550:wick=0.0350; 2026-10-23:RED:body=-0.9400:wick=0.1899; 2026-10-26:RED:body=-0.1800:wick=0.6400; 2026-10-27:RED:body=-0.0200:wick=0.3550; 2026-10-28:GREEN:body=+0.6900:wick=0.5700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=35.18 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3979.52 | **NEUTRAL** |
| `B04_income` | 29.38 | **GOOD** |
| `B05_profit_margin` | 0.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.73 vs prior_export=11.73 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.7 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 10.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSIC  ·  score **+15**  ·  Medical Distribution
price=84.95999908447266  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.13 on 2026-10-28; prev RSI=43.20 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.20@2026-10-27 → 47.13@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.20@2026-10-27 → 47.13@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.20@2026-10-27 → 47.13@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=2.324 (G=0.8600 R=0.3700); 2026-10-27:RED:O=84.6200,C=84.2500,body=-0.3700,vol=709800.0; 2026-10-28:GREEN:O=84.1000,C=84.9600,body=+0.8600,vol=934500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.317 (Gvol=934500 Rvol=709800); 2026-10-27:RED:O=84.6200,C=84.2500,body=-0.3700,vol=709800.0; 2026-10-28:GREEN:O=84.1000,C=84.9600,body=+0.8600,vol=934500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.716 on 2026-10-28: today_vol=934500 / avg20=1305877 (avg window 2026-09-17→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.020 on 2026-10-28 (price=84.9600, mid=84.9240, upper=86.7013, lower=83.1467; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-28: price=84.9600 vs SMA50=87.3440 dist=-2.73% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=84.92_50=87.34_80=86.50 on 2026-10-28: SMA20=84.9240 SMA50=87.3440 SMA80=86.4965 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-28 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@83.1900; S2[2026-08-18→2026-09-16] low=2026-09-16@86.4400; S3[2026-09-17→2026-10-28] low=2026-10-08@79.3700 | lows=[83.19000244140625, 86.44000244140625, 79.37000274658203] span=8.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.5375008940705243 wick_frac=0.46249910592947574 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.20000164960017816 wick_frac=0.7999983503998218 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3243087201270183 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1000:wick=1.5150; 2026-10-12:GREEN:body=+0.0700:wick=0.9600; 2026-10-26:RED:body=-0.5000:wick=0.8100; 2026-10-27:RED:body=-0.3700:wick=1.4800; 2026-10-28:GREEN:body=+0.8600:wick=0.7400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.45 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13602.0 | **NEUTRAL** |
| `B04_income` | 403.0 | **GOOD** |
| `B05_profit_margin` | 2.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.64 vs prior_export=99.64 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.49 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FIGS  ·  score **+15**  ·  Apparel Manufacturing
price=20.65999984741211  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=76.27 on 2026-10-28; prev RSI=75.58 on 2026-10-27 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 75.58@2026-10-27 → 76.27@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.58@2026-10-27 → 76.27@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 75.58@2026-10-27 → 76.27@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=4.063 (G=0.6500 R=0.1600); 2026-10-27:RED:O=20.6400,C=20.4800,body=-0.1600,vol=2750826.0; 2026-10-28:GREEN:O=20.0100,C=20.6600,body=+0.6500,vol=3220200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.171 (Gvol=3220200 Rvol=2750826); 2026-10-27:RED:O=20.6400,C=20.4800,body=-0.1600,vol=2750826.0; 2026-10-28:GREEN:O=20.0100,C=20.6600,body=+0.6500,vol=3220200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.759 on 2026-10-28: today_vol=3220200 / avg20=4241594 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.528 on 2026-10-28 (price=20.6600, mid=17.7550, upper=23.2574, lower=12.2526; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=20.6600 vs SMA50=15.4910 dist=+33.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=17.7550 SMA50=15.4910 SMA80=13.9351 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-07-30@10.1050; S2[2026-08-31→2026-09-29] low=2026-09-24@11.8100; S3[2026-09-30→2026-10-28] low=2026-10-05@13.2050 | lows=[10.104999542236328, 11.8100004196167, 13.204999923706055] span=30.68% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.8387095980291541 wick_frac=0.16129040197084585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.16161604485160247 wick_frac=0.8383839551483976 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.0625014901175405 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:GREEN:body=+0.2600:wick=0.5750; 2026-10-23:RED:body=-0.0300:wick=0.6050; 2026-10-26:GREEN:body=+0.6000:wick=0.0650; 2026-10-27:RED:body=-0.1600:wick=0.8300; 2026-10-28:GREEN:body=+0.6500:wick=0.1250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=122.88 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.61 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 710.08 | **NEUTRAL** |
| `B04_income` | 61.92 | **GOOD** |
| `B05_profit_margin` | 8.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.62 vs prior_export=18.62 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.83 vs prior=-0.83 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.96 | **GOOD** |
| `B13_short_float` | 11.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=122.88 (this export) | prior_export=122.88 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.61 (this export) | prior_export=5.61 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESI  ·  score **+15**  ·  Specialty Chemicals
price=35.790000915527344  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.16 on 2026-10-28; prev RSI=46.72 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.72@2026-10-27 → 50.16@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.72@2026-10-27 → 50.16@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.72@2026-10-27 → 50.16@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2500 R=0.0000); 2026-10-27:GREEN:O=34.0900,C=35.0300,body=+0.9400,vol=3733000.0; 2026-10-28:GREEN:O=35.4800,C=35.7900,body=+0.3100,vol=3106800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6839800 Rvol=0); 2026-10-27:GREEN:O=34.0900,C=35.0300,body=+0.9400,vol=3733000.0; 2026-10-28:GREEN:O=35.4800,C=35.7900,body=+0.3100,vol=3106800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.788 on 2026-10-28: today_vol=3106800 / avg20=3942293 (avg window 2026-09-17→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.015 on 2026-10-28 (price=35.7900, mid=35.7470, upper=38.5992, lower=32.8948; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=35.7900 vs SMA50=35.7788 dist=+0.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-28: SMA20=35.7470 SMA50=35.7788 SMA80=37.7454 | **BAD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-28 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-29@34.1000; S2[2026-08-18→2026-09-16] low=2026-09-15@31.4400; S3[2026-09-17→2026-10-28] low=2026-09-17@32.5200 | lows=[34.099998474121094, 31.440000534057617, 32.52000045776367] span=8.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6063894765290765 wick_frac=0.3936105234709235 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.6300:wick=0.8300; 2026-10-12:GREEN:body=+0.5400:wick=0.4300; 2026-10-26:GREEN:body=+0.2100:wick=1.0700; 2026-10-27:GREEN:body=+0.9400:wick=0.1800; 2026-10-28:GREEN:body=+0.3100:wick=0.5200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.7 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.99 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3150.2 | **NEUTRAL** |
| `B04_income` | 178.6 | **GOOD** |
| `B05_profit_margin` | 5.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 47.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=47.0 vs prior_export=47.0 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.15 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.62 | **GOOD** |
| `B13_short_float` | 3.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.7 (this export) | prior_export=8.7 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.99 (this export) | prior_export=11.99 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMD  ·  score **+15**  ·  Semiconductors
price=578.4400024414062  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.62 on 2026-10-28; prev RSI=49.82 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.82@2026-10-27 → 53.62@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.82@2026-10-27 → 53.62@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.82@2026-10-27 → 53.62@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=26.1300 R=0.0000); 2026-10-27:GREEN:O=539.4900,C=560.2600,body=+20.7700,vol=20488014.0; 2026-10-28:GREEN:O=573.0800,C=578.4400,body=+5.3600,vol=18557439.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=39045453 Rvol=0); 2026-10-27:GREEN:O=539.4900,C=560.2600,body=+20.7700,vol=20488014.0; 2026-10-28:GREEN:O=573.0800,C=578.4400,body=+5.3600,vol=18557439.0 | **GOOD** |
| `A07_rvol` | RVOL=0.965 on 2026-10-28: today_vol=18557439 / avg20=19225518 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.010 on 2026-10-28 (price=578.4400, mid=579.3615, upper=669.7908, lower=488.9322; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=578.4400 vs SMA50=545.5834 dist=+6.02% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=579.3615 SMA50=545.5834 SMA80=529.8088 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-31→2026-10-28 (63 bars); S1[2026-07-31→2026-08-28] low=2026-08-24@451.0000; S2[2026-08-31→2026-09-29] low=2026-09-03@440.5000; S3[2026-09-30→2026-10-28] low=2026-10-22@502.6000 | lows=[451.0, 440.5, 502.6000061035156] span=14.10% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.5527545947125906 wick_frac=0.4472454052874094 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:RED:body=-15.6900:wick=11.6900; 2026-10-23:GREEN:body=+6.5800:wick=8.9829; 2026-10-26:GREEN:body=+8.4901:wick=10.9999; 2026-10-27:GREEN:body=+20.7700:wick=4.6450; 2026-10-28:GREEN:body=+5.3600:wick=13.2334 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.71 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 41305.0 | **NEUTRAL** |
| `B04_income` | 6434.0 | **GOOD** |
| `B05_profit_margin` | 15.58 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 635.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=635.54 vs prior_export=635.54 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.49 | **GOOD** |
| `B10_insider_transactions` | -6.76 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.76 vs prior=-6.76 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.6 | **GOOD** |
| `B13_short_float` | 2.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.71 (this export) | prior_export=2.71 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+15**  ·  Biotechnology
price=17.360000610351562  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.43 on 2026-10-28; prev RSI=65.16 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.16@2026-10-27 → 66.43@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.16@2026-10-27 → 66.43@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.16@2026-10-27 → 66.43@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7900 R=0.0000); 2026-10-27:GREEN:O=16.9800,C=17.2500,body=+0.2700,vol=494500.0; 2026-10-28:GREEN:O=16.8400,C=17.3600,body=+0.5200,vol=625900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1120400 Rvol=0); 2026-10-27:GREEN:O=16.9800,C=17.2500,body=+0.2700,vol=494500.0; 2026-10-28:GREEN:O=16.8400,C=17.3600,body=+0.5200,vol=625900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.794 on 2026-10-28: today_vol=625900 / avg20=788134 (avg window 2026-09-17→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.963 on 2026-10-28 (price=17.3600, mid=16.4510, upper=17.3945, lower=15.5075; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-28: price=17.3600 vs SMA50=16.2573 dist=+6.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=16.4510 SMA50=16.2573 SMA80=16.1381 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-28 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-31@14.3630; S2[2026-08-18→2026-09-16] low=2026-08-18@15.1500; S3[2026-09-17→2026-10-28] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.149999618530273, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.9169654697996987 wick_frac=0.08303453020030122 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-26:GREEN:body=+0.0600:wick=0.1850; 2026-10-27:GREEN:body=+0.2700:wick=0.0400; 2026-10-28:GREEN:body=+0.5200:wick=0.0200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SMA  ·  score **+15**  ·  REIT - Industrial
price=35.220001220703125  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.62 on 2026-10-28; prev RSI=65.74 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.74@2026-10-27 → 64.62@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.74@2026-10-27 → 64.62@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.74@2026-10-27 → 64.62@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=4.167 (G=0.2500 R=0.0600); 2026-10-27:GREEN:O=35.0600,C=35.3100,body=+0.2500,vol=827770.0; 2026-10-28:RED:O=35.2800,C=35.2200,body=-0.0600,vol=703612.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=1.176 (Gvol=827770 Rvol=703612); 2026-10-27:GREEN:O=35.0600,C=35.3100,body=+0.2500,vol=827770.0; 2026-10-28:RED:O=35.2800,C=35.2200,body=-0.0600,vol=703612.0 | **GOOD** |
| `A07_rvol` | RVOL=0.990 on 2026-10-28: today_vol=703612 / avg20=710411 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.637 on 2026-10-28 (price=35.2200, mid=33.7320, upper=36.0663, lower=31.3977; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=35.2200 vs SMA50=33.1034 dist=+6.39% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=33.73_50=33.10_80=33.35 on 2026-10-28: SMA20=33.7320 SMA50=33.1034 SMA80=33.3499 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-28 (63 bars); S1[2026-07-30→2026-08-28] low=2026-08-28@32.9300; S2[2026-08-31→2026-09-29] low=2026-09-25@31.0600; S3[2026-09-30→2026-10-28] low=2026-10-01@31.9300 | lows=[32.93000030517578, 31.059999465942383, 31.93000030517578] span=6.02% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.6410266442349075 wick_frac=0.35897335576509254 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.07999674479166667 wick_frac=0.9200032552083334 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.1668362156663274 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:GREEN:body=+1.1900:wick=0.0200; 2026-10-23:GREEN:body=+0.1400:wick=0.4200; 2026-10-26:RED:body=-0.5100:wick=0.2000; 2026-10-27:GREEN:body=+0.2500:wick=0.1400; 2026-10-28:RED:body=-0.0600:wick=0.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=92.68 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 306.46 | **NEUTRAL** |
| `B04_income` | 28.11 | **GOOD** |
| `B05_profit_margin` | 9.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.34 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.34 vs prior_export=37.34 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.82 | **GOOD** |
| `B10_insider_transactions` | -0.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.02 vs prior=-0.02 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.29 | **GOOD** |
| `B13_short_float` | 9.52 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=92.68 (this export) | prior_export=92.68 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+14**  ·  Credit Services
price=400.6000061035156  pair=`2026-10-27→2026-10-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.95 on 2026-10-28; prev RSI=62.49 on 2026-10-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.49@2026-10-27 → 60.95@2026-10-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.49@2026-10-27 → 60.95@2026-10-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.49@2026-10-27 → 60.95@2026-10-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_body_sum/RED_body_sum=25.474 (G=5.3500 R=0.2100); 2026-10-27:RED:O=402.0700,C=401.8600,body=-0.2100,vol=8146153.0; 2026-10-28:GREEN:O=395.2500,C=400.6000,body=+5.3500,vol=6972892.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-27 + 2026-10-28; ratio=GREEN_vol/RED_vol=0.856 (Gvol=6972892 Rvol=8146153); 2026-10-27:RED:O=402.0700,C=401.8600,body=-0.2100,vol=8146153.0; 2026-10-28:GREEN:O=395.2500,C=400.6000,body=+5.3500,vol=6972892.0 | **BAD** |
| `A07_rvol` | RVOL=1.162 on 2026-10-28: today_vol=6972892 / avg20=6002873 (avg window 2026-09-30→2026-10-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.283 on 2026-10-28 (price=400.6000, mid=390.9425, upper=425.0392, lower=356.8458; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-28: price=400.6000 vs SMA50=379.4538 dist=+5.57% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-28: SMA20=390.9425 SMA50=379.4538 SMA80=372.1799 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-31→2026-10-28 (63 bars); S1[2026-07-31→2026-08-28] low=2026-08-18@358.0500; S2[2026-08-31→2026-09-29] low=2026-09-22@359.8000; S3[2026-09-30→2026-10-28] low=2026-10-02@356.7500 | lows=[358.04998779296875, 359.79998779296875, 356.75] span=0.85% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: GREEN body_frac=0.5037672846814332 wick_frac=0.4962327153185669 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-27+2026-10-28: RED body_frac=0.034168768494429326 wick_frac=0.9658312315055707 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=25.473554199360652 need>1.4; red_wick_gt_green=True 5d trail=2026-10-22:GREEN:body=+6.3900:wick=0.9300; 2026-10-23:RED:body=-2.6700:wick=3.1101; 2026-10-26:RED:body=-1.5900:wick=3.3000; 2026-10-27:RED:body=-0.2100:wick=5.9366; 2026-10-28:GREEN:body=+5.3500:wick=5.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.68 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.04 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 44488.0 | **NEUTRAL** |
| `B04_income` | 22399.0 | **GOOD** |
| `B05_profit_margin` | 50.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 423.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=423.54 vs prior_export=423.54 on finviz_2026-10-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -0.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.1 vs prior=-0.1 on finviz_2026-10-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.87 | **GOOD** |
| `B13_short_float` | 1.09 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.04 (this export) | prior_export=2.04 (finviz_2026-10-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-29_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.