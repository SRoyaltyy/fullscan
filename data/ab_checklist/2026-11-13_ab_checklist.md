# A+B1 Feature Checklist — 2026-11-13

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,609** names
- Export: `finviz_2026-11-13.csv` · prior export for Δ: `2026-11-12`
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
| 1 | WCN | +17 | 18 | 1 | 2026-11-11→2026-11-12 | Waste Management |
| 2 | WMT | +16 | 17 | 1 | 2026-11-11→2026-11-12 | Discount Stores |
| 3 | PM | +16 | 16 | 0 | 2026-11-11→2026-11-12 | Tobacco |
| 4 | ECO | +16 | 17 | 1 | 2026-11-11→2026-11-12 | Marine Shipping |
| 5 | HTHT | +15 | 16 | 1 | 2026-11-11→2026-11-12 | Lodging |
| 6 | RSG | +15 | 18 | 3 | 2026-11-11→2026-11-12 | Waste Management |
| 7 | SONO | +15 | 17 | 2 | 2026-11-11→2026-11-12 | Consumer Electronics |
| 8 | UNP | +15 | 16 | 1 | 2026-11-11→2026-11-12 | Railroads |
| 9 | EW | +15 | 18 | 3 | 2026-11-11→2026-11-12 | Medical Devices |
| 10 | HONA | +14 | 15 | 1 | 2026-11-11→2026-11-12 | Aerospace & Defense |
| 11 | VNT | +14 | 16 | 2 | 2026-11-11→2026-11-12 | Scientific & Technical Instruments |
| 12 | TD | +14 | 15 | 1 | 2026-11-11→2026-11-12 | Banks - Diversified |
| 13 | AMT | +14 | 16 | 2 | 2026-11-11→2026-11-12 | REIT - Specialty |
| 14 | LNG | +14 | 15 | 1 | 2026-11-11→2026-11-12 | Oil & Gas Midstream |
| 15 | CHRD | +14 | 16 | 2 | 2026-11-11→2026-11-12 | Oil & Gas E&P |

## Full checklist — top 15

### WCN  ·  score **+17**  ·  Waste Management
price=193.9199981689453  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.89 on 2026-11-12; prev RSI=71.44 on 2026-11-11 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 71.44@2026-11-11 → 72.89@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.44@2026-11-11 → 72.89@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 71.44@2026-11-11 → 72.89@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.925 (G=2.8300 R=1.4700); 2026-11-11:GREEN:O=189.4000,C=192.2300,body=+2.8300,vol=2499259.0; 2026-11-12:RED:O=195.3900,C=193.9200,body=-1.4700,vol=2308538.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.083 (Gvol=2499259 Rvol=2308538); 2026-11-11:GREEN:O=189.4000,C=192.2300,body=+2.8300,vol=2499259.0; 2026-11-12:RED:O=195.3900,C=193.9200,body=-1.4700,vol=2308538.0 | **GOOD** |
| `A07_rvol` | RVOL=1.016 on 2026-11-12: today_vol=2308538 / avg20=2273242 (avg window 2026-10-12→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.578 on 2026-11-12 (price=193.9200, mid=183.6005, upper=201.4512, lower=165.7498; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=193.9200 vs SMA50=168.0290 dist=+15.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=183.6005 SMA50=168.0290 SMA80=167.9045 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-12 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-10@158.8500; S2[2026-09-11→2026-10-09] low=2026-10-01@149.3900; S3[2026-10-12→2026-11-12] low=2026-10-12@152.3750 | lows=[158.85000610351562, 149.38999938964844, 152.375] span=6.33% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.7764088781721213 wick_frac=0.22359112182787866 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.36029425514329416 wick_frac=0.6397057448567058 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.925169714961905 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+4.4700:wick=0.2800; 2026-11-09:GREEN:body=+0.9900:wick=2.3000; 2026-11-10:GREEN:body=+2.1600:wick=1.3700; 2026-11-11:GREEN:body=+2.8300:wick=0.8150; 2026-11-12:RED:body=-1.4700:wick=2.6100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.93 (current export asof; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9943.62 | **NEUTRAL** |
| `B04_income` | 1133.47 | **GOOD** |
| `B05_profit_margin` | 11.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 208.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=208.08 vs prior_export=208.08 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 0.68 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.68 vs prior=0.68 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.26 | **GOOD** |
| `B13_short_float` | 1.83 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.93 (this export) | prior_export=3.93 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMT  ·  score **+16**  ·  Discount Stores
price=126.19000244140625  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.63 on 2026-11-12; prev RSI=63.69 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.69@2026-11-11 → 64.63@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.69@2026-11-11 → 64.63@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.69@2026-11-11 → 64.63@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=2.078 (G=1.3300 R=0.6400); 2026-11-11:GREEN:O=124.3400,C=125.6700,body=+1.3300,vol=19918341.0; 2026-11-12:RED:O=126.8300,C=126.1900,body=-0.6400,vol=16688228.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.194 (Gvol=19918341 Rvol=16688228); 2026-11-11:GREEN:O=124.3400,C=125.6700,body=+1.3300,vol=19918341.0; 2026-11-12:RED:O=126.8300,C=126.1900,body=-0.6400,vol=16688228.0 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-11-12: today_vol=16688228 / avg20=22391884 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.457 on 2026-11-12 (price=126.1900, mid=122.7670, upper=130.2625, lower=115.2715; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=126.1900 vs SMA50=114.1442 dist=+10.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=122.7670 SMA50=114.1442 SMA80=112.5905 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-21@102.1500; S2[2026-09-16→2026-10-14] low=2026-10-05@103.3900; S3[2026-10-15→2026-11-12] low=2026-10-21@116.6300 | lows=[102.1500015258789, 103.38999938964844, 116.62999725341797] span=14.18% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.5964124793442152 wick_frac=0.4035875206557848 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.2935776605759842 wick_frac=0.7064223394240158 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0781298428820065 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:GREEN:body=+1.5200:wick=2.0900; 2026-11-09:GREEN:body=+0.9400:wick=1.4700; 2026-11-10:RED:body=-0.3000:wick=1.4100; 2026-11-11:GREEN:body=+1.3300:wick=0.9000; 2026-11-12:RED:body=-0.6400:wick=1.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.19 (current export asof; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 735840.0 | **NEUTRAL** |
| `B04_income` | 22076.0 | **GOOD** |
| `B05_profit_margin` | 3.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 132.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7699999999999818 (now=132.57 vs prior_export=131.8 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 1.48 | **GOOD** |
| `B10_insider_transactions` | -0.26 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.26 vs prior=-0.26 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.29 | **GOOD** |
| `B13_short_float` | 1.65 | **NEUTRAL** |
| `B14_earnings_date` | 11/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.19 (this export) | prior_export=9.19 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+16**  ·  Tobacco
price=196.66000366210938  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=45.90 on 2026-11-12; prev RSI=45.45 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.45@2026-11-11 → 45.90@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 45.45@2026-11-11 → 45.90@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 45.45@2026-11-11 → 45.90@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=3.554 (G=2.9500 R=0.8300); 2026-11-11:GREEN:O=193.4000,C=196.3500,body=+2.9500,vol=4367890.0; 2026-11-12:RED:O=197.4900,C=196.6600,body=-0.8300,vol=3338752.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.308 (Gvol=4367890 Rvol=3338752); 2026-11-11:GREEN:O=193.4000,C=196.3500,body=+2.9500,vol=4367890.0; 2026-11-12:RED:O=197.4900,C=196.6600,body=-0.8300,vol=3338752.0 | **GOOD** |
| `A07_rvol` | RVOL=0.651 on 2026-11-12: today_vol=3338752 / avg20=5132138 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.499 on 2026-11-12 (price=196.6600, mid=201.8300, upper=212.1903, lower=191.4697; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=196.6600 vs SMA50=196.0416 dist=+0.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=201.8300 SMA50=196.0416 SMA80=193.8491 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-08@181.0100; S2[2026-09-16→2026-10-14] low=2026-10-05@184.1100; S3[2026-10-15→2026-11-12] low=2026-10-21@184.0000 | lows=[181.00999450683594, 184.11000061035156, 184.0] span=1.71% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.7468391104312253 wick_frac=0.2531608895687747 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.23640030074273025 wick_frac=0.7635996992572698 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5542237337990623 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:GREEN:body=+0.2000:wick=6.8400; 2026-11-09:RED:body=-5.0800:wick=1.7700; 2026-11-10:RED:body=-1.5800:wick=0.9400; 2026-11-11:GREEN:body=+2.9500:wick=1.0000; 2026-11-12:RED:body=-0.8300:wick=2.6810 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.2 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.07 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 43014.0 | **NEUTRAL** |
| `B04_income` | 10681.0 | **GOOD** |
| `B05_profit_margin` | 24.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 215.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=215.25 vs prior_export=215.25 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.33 | **GOOD** |
| `B13_short_float` | 0.96 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.2 (this export) | prior_export=3.2 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.07 (this export) | prior_export=1.07 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ECO  ·  score **+16**  ·  Marine Shipping
price=87.66999816894531  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.62 on 2026-11-12; prev RSI=47.17 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.17@2026-11-11 → 49.62@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.17@2026-11-11 → 49.62@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.17@2026-11-11 → 49.62@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.3000 R=0.0000); 2026-11-11:GREEN:O=84.6500,C=85.7500,body=+1.1000,vol=752279.0; 2026-11-12:GREEN:O=83.4700,C=87.6700,body=+4.2000,vol=1276539.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2028818 Rvol=0); 2026-11-11:GREEN:O=84.6500,C=85.7500,body=+1.1000,vol=752279.0; 2026-11-12:GREEN:O=83.4700,C=87.6700,body=+4.2000,vol=1276539.0 | **GOOD** |
| `A07_rvol` | RVOL=1.403 on 2026-11-12: today_vol=1276539 / avg20=910140 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.210 on 2026-11-12 (price=87.6700, mid=90.2895, upper=102.7908, lower=77.7882; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=87.6700 vs SMA50=85.9464 dist=+2.01% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=90.2895 SMA50=85.9464 SMA80=76.1412 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-18@61.0000; S2[2026-09-16→2026-10-14] low=2026-09-22@75.5600; S3[2026-10-15→2026-11-12] low=2026-11-10@78.6000 | lows=[61.0, 75.55999755859375, 78.5999984741211] span=28.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.5958884580111631 wick_frac=0.404111541988837 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+0.0500:wick=2.9700; 2026-11-09:RED:body=-1.9000:wick=2.5830; 2026-11-10:GREEN:body=+1.8500:wick=2.1700; 2026-11-11:GREEN:body=+1.1000:wick=1.3500; 2026-11-12:GREEN:body=+4.2000:wick=1.4543 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=62.07 (current export asof; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=53.2 (current export; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 706.47 | **NEUTRAL** |
| `B04_income` | 402.14 | **GOOD** |
| `B05_profit_margin` | 56.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 84.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.6200000000000045 (now=84.58 vs prior_export=80.96 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 2.25 | **GOOD** |
| `B10_insider_transactions` | -0.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.82 vs prior=-0.82 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.98 | **GOOD** |
| `B13_short_float` | 6.51 | **NEUTRAL** |
| `B14_earnings_date` | 11/11/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=62.07 (this export) | prior_export=28.23 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=53.2 (this export) | prior_export=21.73 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+15**  ·  Lodging
price=49.95000076293945  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.86 on 2026-11-12; prev RSI=63.38 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.38@2026-11-11 → 63.86@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.38@2026-11-11 → 63.86@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.38@2026-11-11 → 63.86@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=0.978 (G=0.4500 R=0.4600); 2026-11-11:RED:O=50.2900,C=49.8300,body=-0.4600,vol=1257944.0; 2026-11-12:GREEN:O=49.5000,C=49.9500,body=+0.4500,vol=1577101.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.254 (Gvol=1577101 Rvol=1257944); 2026-11-11:RED:O=50.2900,C=49.8300,body=-0.4600,vol=1257944.0; 2026-11-12:GREEN:O=49.5000,C=49.9500,body=+0.4500,vol=1577101.0 | **GOOD** |
| `A07_rvol` | RVOL=0.992 on 2026-11-12: today_vol=1577101 / avg20=1589943 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.500 on 2026-11-12 (price=49.9500, mid=48.0450, upper=51.8572, lower=44.2328; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=49.9500 vs SMA50=45.2182 dist=+10.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=48.0450 SMA50=45.2182 SMA80=44.9744 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-11@42.6100; S2[2026-09-16→2026-10-14] low=2026-10-02@41.2900; S3[2026-10-15→2026-11-12] low=2026-10-15@44.1700 | lows=[42.61000061035156, 41.290000915527344, 44.16999816894531] span=6.98% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.6521763167642457 wick_frac=0.3478236832357543 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.4554453949729005 wick_frac=0.5445546050270995 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=0.9782644751463686 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:GREEN:body=+1.0300:wick=0.1400; 2026-11-09:RED:body=-0.9000:wick=0.1200; 2026-11-10:GREEN:body=+0.3200:wick=0.8750; 2026-11-11:RED:body=-0.4600:wick=0.5500; 2026-11-12:GREEN:body=+0.4500:wick=0.2400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.56 (current export asof; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.03 (current export; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.86 | **NEUTRAL** |
| `B04_income` | 720.34 | **GOOD** |
| `B05_profit_margin` | 18.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.32 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.29999999999999716 (now=61.32 vs prior_export=61.02 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.28 | **GOOD** |
| `B13_short_float` | 4.04 | **NEUTRAL** |
| `B14_earnings_date` | 11/16/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.56 (this export) | prior_export=3.56 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.03 (this export) | prior_export=5.03 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+15**  ·  Waste Management
price=253.0  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.39 on 2026-11-12; prev RSI=68.35 on 2026-11-11 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 68.35@2026-11-11 → 71.39@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.35@2026-11-11 → 71.39@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 68.35@2026-11-11 → 71.39@2026-11-12 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.953 (G=2.4800 R=1.2700); 2026-11-11:GREEN:O=246.8600,C=249.3400,body=+2.4800,vol=1500158.0; 2026-11-12:RED:O=254.2700,C=253.0000,body=-1.2700,vol=1410726.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.063 (Gvol=1500158 Rvol=1410726); 2026-11-11:GREEN:O=246.8600,C=249.3400,body=+2.4800,vol=1500158.0; 2026-11-12:RED:O=254.2700,C=253.0000,body=-1.2700,vol=1410726.0 | **GOOD** |
| `A07_rvol` | RVOL=0.969 on 2026-11-12: today_vol=1410726 / avg20=1455423 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.824 on 2026-11-12 (price=253.0000, mid=241.2085, upper=255.5197, lower=226.8973; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-12: price=253.0000 vs SMA50=227.0764 dist=+11.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=241.2085 SMA50=227.0764 SMA80=223.2820 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-17@213.0000; S2[2026-09-16→2026-10-14] low=2026-10-02@205.9700; S3[2026-10-15→2026-11-12] low=2026-10-15@228.4200 | lows=[213.0, 205.97000122070312, 228.4199981689453] span=10.90% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.8000009844409114 wick_frac=0.1999990155590886 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.308628745179472 wick_frac=0.6913712548205281 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.952745972053682 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+3.6600:wick=1.1700; 2026-11-09:GREEN:body=+0.0900:wick=3.4900; 2026-11-10:GREEN:body=+2.3800:wick=2.3100; 2026-11-11:GREEN:body=+2.4800:wick=0.6200; 2026-11-12:RED:body=-1.2700:wick=2.8450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.16 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17238.0 | **NEUTRAL** |
| `B04_income` | 2227.56 | **GOOD** |
| `B05_profit_margin` | 12.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 260.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=260.78 vs prior_export=260.78 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 6.6 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=6.6 vs prior=6.6 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.12 | **GOOD** |
| `B13_short_float` | 2.35 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.16 (this export) | prior_export=3.16 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+15**  ·  Consumer Electronics
price=18.110000610351562  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.24 on 2026-11-12; prev RSI=65.95 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.95@2026-11-11 → 57.24@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.95@2026-11-11 → 57.24@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.95@2026-11-11 → 57.24@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.753 (G=1.5600 R=0.8900); 2026-11-11:GREEN:O=17.5000,C=19.0600,body=+1.5600,vol=6090368.0; 2026-11-12:RED:O=19.0000,C=18.1100,body=-0.8900,vol=3691637.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.650 (Gvol=6090368 Rvol=3691637); 2026-11-11:GREEN:O=17.5000,C=19.0600,body=+1.5600,vol=6090368.0; 2026-11-12:RED:O=19.0000,C=18.1100,body=-0.8900,vol=3691637.0 | **GOOD** |
| `A07_rvol` | RVOL=2.329 on 2026-11-12: today_vol=3691637 / avg20=1585119 (avg window 2026-10-15→2026-11-11, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.582 on 2026-11-12 (price=18.1100, mid=17.1723, upper=18.7825, lower=15.5620; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=18.1100 vs SMA50=16.8918 dist=+7.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=17.1723 SMA50=16.8918 SMA80=16.4224 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-09@14.2000; S2[2026-09-16→2026-10-14] low=2026-09-18@15.2800; S3[2026-10-15→2026-11-12] low=2026-11-06@15.4000 | lows=[14.199999809265137, 15.279999732971191, 15.399999618530273] span=8.45% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.8188978821908591 wick_frac=0.18110211780914087 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.5913613010009454 wick_frac=0.4086386989990546 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7528095907555676 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+0.4300:wick=0.6200; 2026-11-09:GREEN:body=+0.5200:wick=0.1600; 2026-11-10:GREEN:body=+0.0300:wick=0.5800; 2026-11-11:GREEN:body=+1.5600:wick=0.3450; 2026-11-12:RED:body=-0.8900:wick=0.6150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=77.78 (current export asof; earnings_date=11/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.21 (current export; earnings_date=11/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1546.89 | **NEUTRAL** |
| `B04_income` | 89.6 | **GOOD** |
| `B05_profit_margin` | 5.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | -4.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.45 vs prior=-4.45 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 14.2 | **NEUTRAL** |
| `B14_earnings_date` | 11/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=77.78 (this export) | prior_export=77.78 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.21 (this export) | prior_export=1.21 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UNP  ·  score **+15**  ·  Railroads
price=304.20001220703125  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.33 on 2026-11-12; prev RSI=57.63 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.63@2026-11-11 → 59.33@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.63@2026-11-11 → 59.33@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.63@2026-11-11 → 59.33@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=2.986 (G=2.1500 R=0.7200); 2026-11-11:GREEN:O=299.9600,C=302.1100,body=+2.1500,vol=2415026.0; 2026-11-12:RED:O=304.9200,C=304.2000,body=-0.7200,vol=1609201.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.501 (Gvol=2415026 Rvol=1609201); 2026-11-11:GREEN:O=299.9600,C=302.1100,body=+2.1500,vol=2415026.0; 2026-11-12:RED:O=304.9200,C=304.2000,body=-0.7200,vol=1609201.0 | **GOOD** |
| `A07_rvol` | RVOL=0.565 on 2026-11-12: today_vol=1609201 / avg20=2850292 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.489 on 2026-11-12 (price=304.2000, mid=297.7040, upper=310.9887, lower=284.4193; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=304.2000 vs SMA50=287.0106 dist=+5.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=297.70_50=287.01_80=291.38 on 2026-11-12: SMA20=297.7040 SMA50=287.0106 SMA80=291.3785 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-15@282.6900; S2[2026-09-16→2026-10-14] low=2026-09-21@268.2600; S3[2026-10-15→2026-11-12] low=2026-10-22@278.5854 | lows=[282.69000244140625, 268.260009765625, 278.58538818359375] span=5.38% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.7517419464985008 wick_frac=0.24825805350149918 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.17888256211568643 wick_frac=0.8211174378843136 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.9860975713135254 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:RED:body=-2.1500:wick=2.8300; 2026-11-09:GREEN:body=+4.4100:wick=2.5200; 2026-11-10:GREEN:body=+3.0000:wick=3.3600; 2026-11-11:GREEN:body=+2.1500:wick=0.7100; 2026-11-12:RED:body=-0.7200:wick=3.3050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.59 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.52 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 26363.0 | **NEUTRAL** |
| `B04_income` | 7618.0 | **GOOD** |
| `B05_profit_margin` | 28.9 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 333.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=333.17 vs prior_export=333.17 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.25 vs prior=-0.25 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.93 | **GOOD** |
| `B13_short_float` | 5.14 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.59 (this export) | prior_export=1.59 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.52 (this export) | prior_export=0.52 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EW  ·  score **+15**  ·  Medical Devices
price=105.80000305175781  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.53 on 2026-11-12; prev RSI=79.57 on 2026-11-11 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.57@2026-11-11 → 79.53@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.57@2026-11-11 → 79.53@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.57@2026-11-11 → 79.53@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=7.593 (G=2.0500 R=0.2700); 2026-11-11:GREEN:O=103.7600,C=105.8100,body=+2.0500,vol=4203314.0; 2026-11-12:RED:O=106.0700,C=105.8000,body=-0.2700,vol=4157395.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.011 (Gvol=4203314 Rvol=4157395); 2026-11-11:GREEN:O=103.7600,C=105.8100,body=+2.0500,vol=4203314.0; 2026-11-12:RED:O=106.0700,C=105.8000,body=-0.2700,vol=4157395.0 | **GOOD** |
| `A07_rvol` | RVOL=0.868 on 2026-11-12: today_vol=4157395 / avg20=4787293 (avg window 2026-10-12→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.805 on 2026-11-12 (price=105.8000, mid=95.0195, upper=108.4101, lower=81.6289; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-12: price=105.8000 vs SMA50=90.3266 dist=+17.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=95.0195 SMA50=90.3266 SMA80=89.7769 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-12 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-10@85.0000; S2[2026-09-11→2026-10-09] low=2026-10-08@82.4300; S3[2026-10-12→2026-11-12] low=2026-10-12@84.5500 | lows=[85.0, 82.43000030517578, 84.55000305175781] span=3.12% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.6193344212719663 wick_frac=0.3806655787280338 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.14673759806279335 wick_frac=0.8532624019372066 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.592670038712594 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+3.1400:wick=0.5500; 2026-11-09:GREEN:body=+1.8600:wick=1.3400; 2026-11-10:RED:body=-0.2400:wick=1.9150; 2026-11-11:GREEN:body=+2.0500:wick=1.2600; 2026-11-12:RED:body=-0.2700:wick=1.5700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.56 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.1 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6674.7 | **NEUTRAL** |
| `B04_income` | 994.3 | **GOOD** |
| `B05_profit_margin` | 14.9 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 115.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=115.29 vs prior_export=115.29 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.53 | **GOOD** |
| `B10_insider_transactions` | -2.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000231 (now=-2.84 vs prior=-2.85 on finviz_2026-11-12) | **GOOD** |
| `B12_institutional_transactions` | 2.05 | **GOOD** |
| `B13_short_float` | 1.65 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.56 (this export) | prior_export=7.56 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.1 (this export) | prior_export=2.1 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HONA  ·  score **+14**  ·  Aerospace & Defense
price=166.22999572753906  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.55 on 2026-11-12; prev RSI=57.76 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.76@2026-11-11 → 54.55@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.76@2026-11-11 → 54.55@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.76@2026-11-11 → 54.55@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.585 (G=3.0600 R=1.9300); 2026-11-11:GREEN:O=166.4700,C=169.5300,body=+3.0600,vol=2154159.0; 2026-11-12:RED:O=168.1600,C=166.2300,body=-1.9300,vol=2642834.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=0.815 (Gvol=2154159 Rvol=2642834); 2026-11-11:GREEN:O=166.4700,C=169.5300,body=+3.0600,vol=2154159.0; 2026-11-12:RED:O=168.1600,C=166.2300,body=-1.9300,vol=2642834.0 | **BAD** |
| `A07_rvol` | RVOL=0.742 on 2026-11-12: today_vol=2642834 / avg20=3560188 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.290 on 2026-11-12 (price=166.2300, mid=162.4190, upper=175.5442, lower=149.2938; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=166.2300 vs SMA50=160.0528 dist=+3.86% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=162.42_50=160.05_80=166.61 on 2026-11-12: SMA20=162.4190 SMA50=160.0528 SMA80=166.6102 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-10@149.1900; S2[2026-09-16→2026-10-14] low=2026-10-08@149.8800; S3[2026-10-15→2026-11-12] low=2026-11-06@147.4000 | lows=[149.19000244140625, 149.8800048828125, 147.39999389648438] span=1.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.7149707473073619 wick_frac=0.2850292526926382 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.4269925022702491 wick_frac=0.5730074977297509 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5854844447958256 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:GREEN:body=+3.6300:wick=3.2000; 2026-11-09:GREEN:body=+8.9000:wick=3.9500; 2026-11-10:GREEN:body=+3.0300:wick=1.8299; 2026-11-11:GREEN:body=+3.0600:wick=1.2199; 2026-11-12:RED:body=-1.9300:wick=2.5900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.34 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18338.0 | **NEUTRAL** |
| `B04_income` | 1898.0 | **GOOD** |
| `B05_profit_margin` | 10.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 188.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=188.43 vs prior_export=188.43 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.39 | **GOOD** |
| `B10_insider_transactions` | 7.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.21 vs prior=7.21 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 213.36 | **GOOD** |
| `B13_short_float` | 2.75 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.34 (this export) | prior_export=18.34 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+14**  ·  Scientific & Technical Instruments
price=43.18000030517578  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.22 on 2026-11-12; prev RSI=83.84 on 2026-11-11 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 83.84@2026-11-11 → 79.22@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 83.84@2026-11-11 → 79.22@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 83.84@2026-11-11 → 79.22@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.362 (G=0.7900 R=0.5800); 2026-11-11:GREEN:O=42.9900,C=43.7800,body=+0.7900,vol=1682247.0; 2026-11-12:RED:O=43.7600,C=43.1800,body=-0.5800,vol=1557539.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.080 (Gvol=1682247 Rvol=1557539); 2026-11-11:GREEN:O=42.9900,C=43.7800,body=+0.7900,vol=1682247.0; 2026-11-12:RED:O=43.7600,C=43.1800,body=-0.5800,vol=1557539.0 | **GOOD** |
| `A07_rvol` | RVOL=0.922 on 2026-11-12: today_vol=1557539 / avg20=1690159 (avg window 2026-10-12→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.763 on 2026-11-12 (price=43.1800, mid=37.3220, upper=44.9959, lower=29.6481; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=43.1800 vs SMA50=34.0650 dist=+26.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=37.3220 SMA50=34.0650 SMA80=33.5143 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-12 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-10@31.1100; S2[2026-09-11→2026-10-09] low=2026-09-24@30.4250; S3[2026-10-12→2026-11-12] low=2026-10-12@31.8300 | lows=[31.110000610351562, 30.424999237060547, 31.829999923706055] span=4.62% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.5962238837338524 wick_frac=0.4037761162661477 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.794518328848013 wick_frac=0.20548167115198704 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.3620686253230994 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:GREEN:body=+2.5800:wick=0.4900; 2026-11-09:GREEN:body=+2.2000:wick=0.8000; 2026-11-10:RED:body=-0.1200:wick=2.0268; 2026-11-11:GREEN:body=+0.7900:wick=0.5350; 2026-11-12:RED:body=-0.5800:wick=0.1500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.45000000000000284 (now=46.56 vs prior_export=46.11 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.96 | **GOOD** |
| `B13_short_float` | 9.18 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TD  ·  score **+14**  ·  Banks - Diversified
price=123.80000305175781  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.22 on 2026-11-12; prev RSI=53.68 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.68@2026-11-11 → 52.22@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.68@2026-11-11 → 52.22@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.68@2026-11-11 → 52.22@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=4.750 (G=0.7600 R=0.1600); 2026-11-11:GREEN:O=123.5200,C=124.2800,body=+0.7600,vol=779554.0; 2026-11-12:RED:O=123.9600,C=123.8000,body=-0.1600,vol=645016.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=1.209 (Gvol=779554 Rvol=645016); 2026-11-11:GREEN:O=123.5200,C=124.2800,body=+0.7600,vol=779554.0; 2026-11-12:RED:O=123.9600,C=123.8000,body=-0.1600,vol=645016.0 | **GOOD** |
| `A07_rvol` | RVOL=0.518 on 2026-11-12: today_vol=645016 / avg20=1246171 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.124 on 2026-11-12 (price=123.8000, mid=124.2975, upper=128.3193, lower=120.2757; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=123.8000 vs SMA50=121.3302 dist=+2.04% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=124.2975 SMA50=121.3302 SMA80=120.9774 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-24@115.1200; S2[2026-09-16→2026-10-14] low=2026-10-08@112.6700; S3[2026-10-15→2026-11-12] low=2026-10-15@116.9675 | lows=[115.12000274658203, 112.66999816894531, 116.96749877929688] span=3.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.5352134621377376 wick_frac=0.46478653786226237 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.19511718568278455 wick_frac=0.8048828143172154 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.75013113347003 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:RED:body=-0.5000:wick=1.1200; 2026-11-09:GREEN:body=+0.2400:wick=1.7800; 2026-11-10:RED:body=-0.5000:wick=0.9650; 2026-11-11:GREEN:body=+0.7600:wick=0.6600; 2026-11-12:RED:body=-0.1600:wick=0.6600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.12 (current export asof; earnings_date=12/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.99 (current export; earnings_date=12/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 81571.94 | **NEUTRAL** |
| `B04_income` | 11259.13 | **GOOD** |
| `B05_profit_margin` | 13.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 133.66 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=133.66 vs prior_export=133.66 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.28 | **BAD** |
| `B13_short_float` | 0.5 | **NEUTRAL** |
| `B14_earnings_date` | 12/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.12 (this export) | prior_export=12.12 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.99 (this export) | prior_export=12.99 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMT  ·  score **+14**  ·  REIT - Specialty
price=189.7899932861328  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.65 on 2026-11-12; prev RSI=59.51 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.51@2026-11-11 → 58.65@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.51@2026-11-11 → 58.65@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.51@2026-11-11 → 58.65@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=1.991 (G=2.1700 R=1.0900); 2026-11-11:GREEN:O=188.0700,C=190.2400,body=+2.1700,vol=1806866.0; 2026-11-12:RED:O=190.8800,C=189.7900,body=-1.0900,vol=2013763.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=0.897 (Gvol=1806866 Rvol=2013763); 2026-11-11:GREEN:O=188.0700,C=190.2400,body=+2.1700,vol=1806866.0; 2026-11-12:RED:O=190.8800,C=189.7900,body=-1.0900,vol=2013763.0 | **BAD** |
| `A07_rvol` | RVOL=0.613 on 2026-11-12: today_vol=2013763 / avg20=3286361 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.324 on 2026-11-12 (price=189.7900, mid=187.1095, upper=195.3773, lower=178.8417; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=189.7900 vs SMA50=178.4508 dist=+6.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=187.1095 SMA50=178.4508 SMA80=176.4549 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-17@170.1500; S2[2026-09-16→2026-10-14] low=2026-10-05@160.3250; S3[2026-10-15→2026-11-12] low=2026-10-28@175.7750 | lows=[170.14999389648438, 160.3249969482422, 175.77499389648438] span=9.64% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.6676917442908654 wick_frac=0.33230825570913464 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=0.41762886658209053 wick_frac=0.5823711334179095 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.990802827745503 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:RED:body=-1.2700:wick=4.7400; 2026-11-09:RED:body=-0.4300:wick=2.6200; 2026-11-10:GREEN:body=+3.0700:wick=1.8900; 2026-11-11:GREEN:body=+2.1700:wick=1.0800; 2026-11-12:RED:body=-1.0900:wick=1.5200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.21 (current export asof; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.86 (current export; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11009.5 | **NEUTRAL** |
| `B04_income` | 3313.5 | **GOOD** |
| `B05_profit_margin` | 30.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 215.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=215.67 vs prior_export=215.67 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | -0.65 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.65 vs prior=-0.65 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.16 | **GOOD** |
| `B13_short_float` | 1.36 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.21 (this export) | prior_export=0.21 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.86 (this export) | prior_export=0.86 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNG  ·  score **+14**  ·  Oil & Gas Midstream
price=309.4800109863281  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.05 on 2026-11-12; prev RSI=56.88 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.88@2026-11-11 → 59.05@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.88@2026-11-11 → 59.05@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.88@2026-11-11 → 59.05@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=12.5100 R=0.0000); 2026-11-11:GREEN:O=296.2200,C=304.5100,body=+8.2900,vol=2066497.0; 2026-11-12:GREEN:O=305.2600,C=309.4800,body=+4.2200,vol=1700319.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3766816 Rvol=0); 2026-11-11:GREEN:O=296.2200,C=304.5100,body=+8.2900,vol=2066497.0; 2026-11-12:GREEN:O=305.2600,C=309.4800,body=+4.2200,vol=1700319.0 | **GOOD** |
| `A07_rvol` | RVOL=0.696 on 2026-11-12: today_vol=1700319 / avg20=2444031 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.444 on 2026-11-12 (price=309.4800, mid=297.0725, upper=325.0099, lower=269.1352; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=309.4800 vs SMA50=284.1004 dist=+8.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=297.0725 SMA50=284.1004 SMA80=278.9727 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-17@264.7400; S2[2026-09-16→2026-10-14] low=2026-10-02@259.5000; S3[2026-10-15→2026-11-12] low=2026-11-09@273.5600 | lows=[264.739990234375, 259.5, 273.55999755859375] span=5.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.5609205406050272 wick_frac=0.43907945939497284 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-06:RED:body=-0.3500:wick=5.7200; 2026-11-09:GREEN:body=+9.3100:wick=3.3100; 2026-11-10:GREEN:body=+12.0300:wick=1.0500; 2026-11-11:GREEN:body=+8.2900:wick=3.9950; 2026-11-12:GREEN:body=+4.2200:wick=5.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.62 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.07 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23923.0 | **NEUTRAL** |
| `B04_income` | 3031.0 | **GOOD** |
| `B05_profit_margin` | 12.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 339.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=339.45 vs prior_export=339.45 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.81 | **BAD** |
| `B13_short_float` | 2.27 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.62 (this export) | prior_export=30.62 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.07 (this export) | prior_export=15.07 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CHRD  ·  score **+14**  ·  Oil & Gas E&P
price=147.0500030517578  pair=`2026-11-11→2026-11-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.01 on 2026-11-12; prev RSI=48.88 on 2026-11-11 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.88@2026-11-11 → 53.01@2026-11-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.88@2026-11-11 → 53.01@2026-11-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.88@2026-11-11 → 53.01@2026-11-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.7400 R=0.0000); 2026-11-11:GREEN:O=140.9900,C=143.4600,body=+2.4700,vol=633336.0; 2026-11-12:GREEN:O=143.7800,C=147.0500,body=+3.2700,vol=515072.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-11 + 2026-11-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1148408 Rvol=0); 2026-11-11:GREEN:O=140.9900,C=143.4600,body=+2.4700,vol=633336.0; 2026-11-12:GREEN:O=143.7800,C=147.0500,body=+3.2700,vol=515072.0 | **GOOD** |
| `A07_rvol` | RVOL=0.593 on 2026-11-12: today_vol=515072 / avg20=868389 (avg window 2026-10-15→2026-11-11, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.039 on 2026-11-12 (price=147.0500, mid=147.6985, upper=164.3643, lower=131.0327; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-12: price=147.0500 vs SMA50=144.8856 dist=+1.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-12: SMA20=147.6985 SMA50=144.8856 SMA80=142.9366 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-12 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-17@135.3789; S2[2026-09-16→2026-10-14] low=2026-09-29@132.6900; S3[2026-10-15→2026-11-12] low=2026-11-06@131.0100 | lows=[135.37893358014497, 132.69000244140625, 131.00999450683594] span=3.33% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: GREEN body_frac=0.6067521644197207 wick_frac=0.39324783558027926 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-11+2026-11-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-06:GREEN:body=+4.8000:wick=1.8000; 2026-11-09:GREEN:body=+4.2300:wick=1.0500; 2026-11-10:GREEN:body=+3.1800:wick=3.8700; 2026-11-11:GREEN:body=+2.4700:wick=1.5450; 2026-11-12:GREEN:body=+3.2700:wick=2.1954 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.99 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.02 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6713.66 | **NEUTRAL** |
| `B04_income` | 892.8 | **GOOD** |
| `B05_profit_margin` | 13.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 182.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=182.83 vs prior_export=182.83 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.52 | **GOOD** |
| `B10_insider_transactions` | -2.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.3400000000000003 (now=-2.11 vs prior=-2.45 on finviz_2026-11-12) | **GOOD** |
| `B12_institutional_transactions` | -5.96 | **BAD** |
| `B13_short_float` | 8.49 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.99 (this export) | prior_export=6.99 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.02 (this export) | prior_export=4.02 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-13_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.