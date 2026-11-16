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
| 1 | ECO | +18 | 19 | 1 | 2026-11-12→2026-11-13 | Marine Shipping |
| 2 | BEKE | +17 | 18 | 1 | 2026-11-12→2026-11-13 | Real Estate Services |
| 3 | YMM | +17 | 17 | 0 | 2026-11-12→2026-11-13 | Software - Application |
| 4 | HAL | +16 | 17 | 1 | 2026-11-12→2026-11-13 | Oil & Gas Equipment & Services |
| 5 | SB | +16 | 16 | 0 | 2026-11-12→2026-11-13 | Marine Shipping |
| 6 | HTHT | +16 | 16 | 0 | 2026-11-12→2026-11-13 | Lodging |
| 7 | IR | +16 | 17 | 1 | 2026-11-12→2026-11-13 | Specialty Industrial Machinery |
| 8 | WMT | +15 | 17 | 2 | 2026-11-12→2026-11-13 | Discount Stores |
| 9 | HAE | +15 | 16 | 1 | 2026-11-12→2026-11-13 | Medical Devices |
| 10 | ASC | +15 | 16 | 1 | 2026-11-12→2026-11-13 | Marine Shipping |
| 11 | CSGP | +15 | 16 | 1 | 2026-11-12→2026-11-13 | Real Estate Services |
| 12 | TTE | +14 | 15 | 1 | 2026-11-12→2026-11-13 | Oil & Gas Integrated |
| 13 | BTE | +14 | 16 | 2 | 2026-11-12→2026-11-13 | Oil & Gas E&P |
| 14 | SVV | +14 | 15 | 1 | 2026-11-12→2026-11-13 | Specialty Retail |
| 15 | FLYW | +14 | 16 | 2 | 2026-11-12→2026-11-13 | Software - Infrastructure |

## Full checklist — top 15

### ECO  ·  score **+18**  ·  Marine Shipping
price=88.81999969482422  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.09 on 2026-11-13; prev RSI=49.62 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.62@2026-11-12 → 51.09@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.62@2026-11-12 → 51.09@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.62@2026-11-12 → 51.09@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=17.500 (G=4.2000 R=0.2400); 2026-11-12:GREEN:O=83.4700,C=87.6700,body=+4.2000,vol=1276539.0; 2026-11-13:RED:O=89.0600,C=88.8200,body=-0.2400,vol=888394.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.437 (Gvol=1276539 Rvol=888394); 2026-11-12:GREEN:O=83.4700,C=87.6700,body=+4.2000,vol=1276539.0; 2026-11-13:RED:O=89.0600,C=88.8200,body=-0.2400,vol=888394.0 | **GOOD** |
| `A07_rvol` | RVOL=0.981 on 2026-11-13: today_vol=888394 / avg20=905356 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.114 on 2026-11-13 (price=88.8200, mid=90.2455, upper=102.7617, lower=77.7293; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=88.8200 vs SMA50=86.3316 dist=+2.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=90.2455 SMA50=86.3316 SMA80=76.5971 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-08-18@61.0000; S2[2026-09-17→2026-10-15] low=2026-09-22@75.5600; S3[2026-10-16→2026-11-13] low=2026-11-10@78.6000 | lows=[61.0, 75.55999755859375, 78.5999984741211] span=28.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.7427973877374784 wick_frac=0.2572026122625216 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.06629784184791772 wick_frac=0.9337021581520822 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=17.500143052420764 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:RED:body=-1.9000:wick=2.5830; 2026-11-10:GREEN:body=+1.8500:wick=2.1700; 2026-11-11:GREEN:body=+1.1000:wick=1.3500; 2026-11-12:GREEN:body=+4.2000:wick=1.4543; 2026-11-13:RED:body=-0.2400:wick=3.3800 | **GOOD** |
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

### BEKE  ·  score **+17**  ·  Real Estate Services
price=17.860000610351562  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.36 on 2026-11-13; prev RSI=48.61 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.61@2026-11-12 → 51.36@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.61@2026-11-12 → 51.36@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.61@2026-11-12 → 51.36@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=3.600 (G=0.3600 R=0.1000); 2026-11-12:RED:O=17.7500,C=17.6500,body=-0.1000,vol=4416966.0; 2026-11-13:GREEN:O=17.5000,C=17.8600,body=+0.3600,vol=6369792.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.442 (Gvol=6369792 Rvol=4416966); 2026-11-12:RED:O=17.7500,C=17.6500,body=-0.1000,vol=4416966.0; 2026-11-13:GREEN:O=17.5000,C=17.8600,body=+0.3600,vol=6369792.0 | **GOOD** |
| `A07_rvol` | RVOL=1.258 on 2026-11-13: today_vol=6369792 / avg20=5062860 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.005 on 2026-11-13 (price=17.8600, mid=17.8550, upper=18.8557, lower=16.8543; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=17.8600 vs SMA50=17.2754 dist=+3.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=17.8550 SMA50=17.2754 SMA80=17.2498 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-16@15.8600; S2[2026-09-17→2026-10-15] low=2026-09-17@15.9200; S3[2026-10-16→2026-11-13] low=2026-10-22@17.0500 | lows=[15.859999656677246, 15.920000076293945, 17.049999237060547] span=7.50% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.7578954131313821 wick_frac=0.2421045868686179 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.2247201124693538 wick_frac=0.7752798875306461 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5999923706345727 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:RED:body=-0.1800:wick=0.1730; 2026-11-10:RED:body=-0.2150:wick=0.2150; 2026-11-11:GREEN:body=+0.0600:wick=0.1100; 2026-11-12:RED:body=-0.1000:wick=0.3450; 2026-11-13:GREEN:body=+0.3600:wick=0.1150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.18 (current export asof; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 12683.5 | **NEUTRAL** |
| `B04_income` | 683.71 | **GOOD** |
| `B05_profit_margin` | 5.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.27 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.27 vs prior_export=24.27 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | -68.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-68.97 vs prior=-68.97 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 27.18 | **GOOD** |
| `B13_short_float` | 2.19 | **NEUTRAL** |
| `B14_earnings_date` | 11/16/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.18 (this export) | prior_export=31.18 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YMM  ·  score **+17**  ·  Software - Application
price=8.6899995803833  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.91 on 2026-11-13; prev RSI=47.04 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.04@2026-11-12 → 51.91@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.04@2026-11-12 → 51.91@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.04@2026-11-12 → 51.91@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=3.333 (G=0.2000 R=0.0600); 2026-11-12:RED:O=8.5500,C=8.4900,body=-0.0600,vol=7604956.0; 2026-11-13:GREEN:O=8.4900,C=8.6900,body=+0.2000,vol=10010584.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.316 (Gvol=10010584 Rvol=7604956); 2026-11-12:RED:O=8.5500,C=8.4900,body=-0.0600,vol=7604956.0; 2026-11-13:GREEN:O=8.4900,C=8.6900,body=+0.2000,vol=10010584.0 | **GOOD** |
| `A07_rvol` | RVOL=1.341 on 2026-11-13: today_vol=10010584 / avg20=7463576 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.114 on 2026-11-13 (price=8.6900, mid=8.6195, upper=9.2360, lower=8.0030; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=8.6900 vs SMA50=8.4430 dist=+2.93% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.62_50=8.44_80=8.70 on 2026-11-13: SMA20=8.6195 SMA50=8.4430 SMA80=8.6979 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-11@8.1050; S2[2026-09-14→2026-10-12] low=2026-09-21@7.9850; S3[2026-10-16→2026-11-13] low=2026-10-28@7.9050 | lows=[8.104999542236328, 7.985000133514404, 7.90500020980835] span=2.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.7017540924361875 wick_frac=0.2982459075638126 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.2500019868233873 wick_frac=0.7499980131766127 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.333306842565366 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:RED:body=-0.1400:wick=0.1100; 2026-11-10:RED:body=-0.1700:wick=0.2150; 2026-11-11:RED:body=-0.0500:wick=0.1050; 2026-11-12:RED:body=-0.0600:wick=0.1800; 2026-11-13:GREEN:body=+0.2000:wick=0.0850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.72 (current export asof; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.61 (current export; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1827.6 | **NEUTRAL** |
| `B04_income` | 604.57 | **GOOD** |
| `B05_profit_margin` | 33.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.62 vs prior_export=12.62 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.69 | **GOOD** |
| `B13_short_float` | 2.22 | **NEUTRAL** |
| `B14_earnings_date` | 11/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.72 (this export) | prior_export=3.72 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.61 (this export) | prior_export=7.61 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAL  ·  score **+16**  ·  Oil & Gas Equipment & Services
price=33.779998779296875  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.40 on 2026-11-13; prev RSI=44.97 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.97@2026-11-12 → 52.40@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.97@2026-11-12 → 52.40@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.97@2026-11-12 → 52.40@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=7.250 (G=0.5800 R=0.0800); 2026-11-12:RED:O=32.8400,C=32.7600,body=-0.0800,vol=8442357.0; 2026-11-13:GREEN:O=33.2000,C=33.7800,body=+0.5800,vol=8857100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.049 (Gvol=8857100 Rvol=8442357); 2026-11-12:RED:O=32.8400,C=32.7600,body=-0.0800,vol=8442357.0; 2026-11-13:GREEN:O=33.2000,C=33.7800,body=+0.5800,vol=8857100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.831 on 2026-11-13: today_vol=8857100 / avg20=10661694 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.010 on 2026-11-13 (price=33.7800, mid=33.8105, upper=36.7226, lower=30.8984; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=33.7800 vs SMA50=33.6646 dist=+0.34% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=33.81_50=33.66_80=33.78 on 2026-11-13: SMA20=33.8105 SMA50=33.6646 SMA80=33.7755 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-08-26@33.0300; S2[2026-09-17→2026-10-15] low=2026-09-30@31.2600; S3[2026-10-16→2026-11-13] low=2026-11-10@30.8000 | lows=[33.029998779296875, 31.260000228881836, 30.799999237060547] span=7.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6744158194496194 wick_frac=0.3255841805503806 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.11111346585851736 wick_frac=0.8888865341414827 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.249809269502194 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:GREEN:body=+0.0800:wick=0.8600; 2026-11-10:GREEN:body=+0.6200:wick=0.6200; 2026-11-11:GREEN:body=+0.4900:wick=0.3600; 2026-11-12:RED:body=-0.0800:wick=0.6400; 2026-11-13:GREEN:body=+0.5800:wick=0.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.6 (current export asof; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.19 (current export; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 22476.0 | **NEUTRAL** |
| `B04_income` | 2001.0 | **GOOD** |
| `B05_profit_margin` | 8.9 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 43.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=43.83 vs prior_export=43.83 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.74 | **GOOD** |
| `B10_insider_transactions` | -11.51 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-11.51 vs prior=-11.51 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.4 | **GOOD** |
| `B13_short_float` | 3.97 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.6 (this export) | prior_export=2.6 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.19 (this export) | prior_export=2.19 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SB  ·  score **+16**  ·  Marine Shipping
price=8.289999961853027  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.71 on 2026-11-13; prev RSI=54.61 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.61@2026-11-12 → 57.71@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.61@2026-11-12 → 57.71@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.61@2026-11-12 → 57.71@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3800 R=0.0000); 2026-11-12:GREEN:O=7.9000,C=8.1100,body=+0.2100,vol=1255261.0; 2026-11-13:GREEN:O=8.1200,C=8.2900,body=+0.1700,vol=1474200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2729461 Rvol=0); 2026-11-12:GREEN:O=7.9000,C=8.1100,body=+0.2100,vol=1255261.0; 2026-11-13:GREEN:O=8.1200,C=8.2900,body=+0.1700,vol=1474200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.619 on 2026-11-13: today_vol=1474200 / avg20=910307 (avg window 2026-10-16→2026-11-12, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.677 on 2026-11-13 (price=8.2900, mid=7.7785, upper=8.5343, lower=7.0227; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=8.2900 vs SMA50=8.2844 dist=+0.07% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=7.78_50=8.28_80=8.11 on 2026-11-13: SMA20=7.7785 SMA50=8.2844 SMA80=8.1084 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-13@7.4300; S2[2026-09-14→2026-10-12] low=2026-09-25@7.9917; S3[2026-10-16→2026-11-13] low=2026-11-05@7.0200 | lows=[7.429999828338623, 7.991700172424316, 7.019999980926514] span=13.84% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6560112141363863 wick_frac=0.3439887858636137 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:GREEN:body=+0.0300:wick=0.1400; 2026-11-10:GREEN:body=+0.2500:wick=0.1900; 2026-11-11:RED:body=-0.1200:wick=0.1750; 2026-11-12:GREEN:body=+0.2100:wick=0.0650; 2026-11-13:GREEN:body=+0.1700:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.71 (current export asof; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 307.5 | **NEUTRAL** |
| `B04_income` | 78.98 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.030000000000001137 (now=9.71 vs prior_export=9.68 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.61 | **GOOD** |
| `B13_short_float` | 5.54 | **NEUTRAL** |
| `B14_earnings_date` | 11/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.71 (this export) | prior_export=10.71 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+16**  ·  Lodging
price=50.38999938964844  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.62 on 2026-11-13; prev RSI=63.86 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.86@2026-11-12 → 65.62@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.86@2026-11-12 → 65.62@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.86@2026-11-12 → 65.62@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-11-12:GREEN:O=49.5000,C=49.9500,body=+0.4500,vol=1577101.0; 2026-11-13:GREEN:O=49.6500,C=50.3900,body=+0.7400,vol=1841800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3418901 Rvol=0); 2026-11-12:GREEN:O=49.5000,C=49.9500,body=+0.4500,vol=1577101.0; 2026-11-13:GREEN:O=49.6500,C=50.3900,body=+0.7400,vol=1841800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.171 on 2026-11-13: today_vol=1841800 / avg20=1572332 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.555 on 2026-11-13 (price=50.3900, mid=48.2395, upper=52.1162, lower=44.3628; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=50.3900 vs SMA50=45.2992 dist=+11.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=48.2395 SMA50=45.2992 SMA80=45.0849 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-16@42.5100; S2[2026-09-17→2026-10-15] low=2026-10-02@41.2900; S3[2026-10-16→2026-11-13] low=2026-10-21@44.2000 | lows=[42.5099983215332, 41.290000915527344, 44.20000076293945] span=7.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6129088322523523 wick_frac=0.38709116774764774 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-09:RED:body=-0.9000:wick=0.1200; 2026-11-10:GREEN:body=+0.3200:wick=0.8750; 2026-11-11:RED:body=-0.4600:wick=0.5500; 2026-11-12:GREEN:body=+0.4500:wick=0.2400; 2026-11-13:GREEN:body=+0.7400:wick=0.5500 | **NEUTRAL** |
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

### IR  ·  score **+16**  ·  Specialty Industrial Machinery
price=85.05999755859375  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.40 on 2026-11-13; prev RSI=63.29 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.29@2026-11-12 → 62.40@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.29@2026-11-12 → 62.40@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.29@2026-11-12 → 62.40@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=1.413 (G=0.8900 R=0.6300); 2026-11-12:GREEN:O=84.4200,C=85.3100,body=+0.8900,vol=4421904.0; 2026-11-13:RED:O=85.6900,C=85.0600,body=-0.6300,vol=3279071.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.349 (Gvol=4421904 Rvol=3279071); 2026-11-12:GREEN:O=84.4200,C=85.3100,body=+0.8900,vol=4421904.0; 2026-11-13:RED:O=85.6900,C=85.0600,body=-0.6300,vol=3279071.0 | **GOOD** |
| `A07_rvol` | RVOL=0.692 on 2026-11-13: today_vol=3279071 / avg20=4736964 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.502 on 2026-11-13 (price=85.0600, mid=80.6525, upper=89.4388, lower=71.8662; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=85.0600 vs SMA50=77.4054 dist=+9.89% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=80.65_50=77.41_80=79.60 on 2026-11-13: SMA20=80.6525 SMA50=77.4054 SMA80=79.6025 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@71.6500; S2[2026-09-14→2026-10-12] low=2026-09-16@70.9450; S3[2026-10-16→2026-11-13] low=2026-10-23@72.3600 | lows=[71.6500015258789, 70.94499969482422, 72.36000061035156] span=1.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.7606844294899383 wick_frac=0.2393155705100617 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.47368722967314114 wick_frac=0.5263127703268589 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4126864948653362 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:GREEN:body=+2.2500:wick=0.8700; 2026-11-10:RED:body=-0.3500:wick=1.1900; 2026-11-11:RED:body=-1.2100:wick=0.9200; 2026-11-12:GREEN:body=+0.8900:wick=0.2800; 2026-11-13:RED:body=-0.6300:wick=0.7000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.55 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.63 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8052.1 | **NEUTRAL** |
| `B04_income` | 1384.9 | **GOOD** |
| `B05_profit_margin` | 17.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 96.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.1700000000000017 (now=96.5 vs prior_export=96.33 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.22 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.22 vs prior=0.22 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.14 | **BAD** |
| `B13_short_float` | 3.78 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.55 (this export) | prior_export=18.55 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.63 (this export) | prior_export=0.63 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMT  ·  score **+15**  ·  Discount Stores
price=128.08999633789062  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.90 on 2026-11-13; prev RSI=64.63 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.63@2026-11-12 → 67.90@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.63@2026-11-12 → 67.90@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.63@2026-11-12 → 67.90@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=2.063 (G=1.3200 R=0.6400); 2026-11-12:RED:O=126.8300,C=126.1900,body=-0.6400,vol=16688228.0; 2026-11-13:GREEN:O=126.7700,C=128.0900,body=+1.3200,vol=16619500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=0.996 (Gvol=16619500 Rvol=16688228); 2026-11-12:RED:O=126.8300,C=126.1900,body=-0.6400,vol=16688228.0; 2026-11-13:GREEN:O=126.7700,C=128.0900,body=+1.3200,vol=16619500.0 | **BAD** |
| `A07_rvol` | RVOL=0.768 on 2026-11-13: today_vol=16619500 / avg20=21628865 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.638 on 2026-11-13 (price=128.0900, mid=123.2310, upper=130.8432, lower=115.6188; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=128.0900 vs SMA50=114.5376 dist=+11.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=123.2310 SMA50=114.5376 SMA80=112.8366 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-08-21@102.1500; S2[2026-09-17→2026-10-15] low=2026-10-05@103.3900; S3[2026-10-16→2026-11-13] low=2026-10-21@116.6300 | lows=[102.1500015258789, 103.38999938964844, 116.62999725341797] span=14.18% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6439013316064875 wick_frac=0.3560986683935124 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.2935776605759842 wick_frac=0.7064223394240158 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0625014901175405 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:GREEN:body=+0.9400:wick=1.4700; 2026-11-10:RED:body=-0.3000:wick=1.4100; 2026-11-11:GREEN:body=+1.3300:wick=0.9000; 2026-11-12:RED:body=-0.6400:wick=1.5400; 2026-11-13:GREEN:body=+1.3200:wick=0.7300 | **GOOD** |
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

### HAE  ·  score **+15**  ·  Medical Devices
price=122.94000244140625  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.75 on 2026-11-13; prev RSI=58.41 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.41@2026-11-12 → 58.75@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.41@2026-11-12 → 58.75@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.41@2026-11-12 → 58.75@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=1.462 (G=1.9300 R=1.3200); 2026-11-12:GREEN:O=120.7800,C=122.7100,body=+1.9300,vol=588457.0; 2026-11-13:RED:O=124.2600,C=122.9400,body=-1.3200,vol=439700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.338 (Gvol=588457 Rvol=439700); 2026-11-12:GREEN:O=120.7800,C=122.7100,body=+1.9300,vol=588457.0; 2026-11-13:RED:O=124.2600,C=122.9400,body=-1.3200,vol=439700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.641 on 2026-11-13: today_vol=439700 / avg20=685469 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-11-13 (price=122.9400, mid=120.2140, upper=125.4449, lower=114.9831; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=122.9400 vs SMA50=113.3108 dist=+8.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=120.2140 SMA50=113.3108 SMA80=106.1601 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-11@100.5300; S2[2026-09-17→2026-10-15] low=2026-10-07@101.3300; S3[2026-10-16→2026-11-13] low=2026-10-23@115.4900 | lows=[100.52999877929688, 101.33000183105469, 115.48999786376953] span=14.88% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.42418556452099726 wick_frac=0.5758144354790028 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.31205698063250087 wick_frac=0.6879430193674991 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.462121781348438 need>1.4; red_wick_gt_green=False 5d trail=2026-11-09:RED:body=-2.9500:wick=2.5400; 2026-11-10:GREEN:body=+4.4600:wick=1.9200; 2026-11-11:RED:body=-2.5500:wick=0.4900; 2026-11-12:GREEN:body=+1.9300:wick=2.6199; 2026-11-13:RED:body=-1.3200:wick=2.9100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.56 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.65 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1382.46 | **NEUTRAL** |
| `B04_income` | 103.91 | **GOOD** |
| `B05_profit_margin` | 7.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 144.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.599999999999994 (now=144.0 vs prior_export=139.4 on finviz_2026-11-12) | **GOOD** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.35 | **GOOD** |
| `B13_short_float` | 8.06 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.56 (this export) | prior_export=4.56 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.65 (this export) | prior_export=3.65 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASC  ·  score **+15**  ·  Marine Shipping
price=19.520000457763672  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.45 on 2026-11-13; prev RSI=51.14 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.14@2026-11-12 → 54.45@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.14@2026-11-12 → 54.45@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.14@2026-11-12 → 54.45@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0200 R=0.0000); 2026-11-12:GREEN:O=18.3200,C=19.0200,body=+0.7000,vol=885439.0; 2026-11-13:GREEN:O=19.2000,C=19.5200,body=+0.3200,vol=674343.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1559782 Rvol=0); 2026-11-12:GREEN:O=18.3200,C=19.0200,body=+0.7000,vol=885439.0; 2026-11-13:GREEN:O=19.2000,C=19.5200,body=+0.3200,vol=674343.0 | **GOOD** |
| `A07_rvol` | RVOL=0.823 on 2026-11-13: today_vol=674343 / avg20=818894 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.120 on 2026-11-13 (price=19.5200, mid=19.2195, upper=21.7237, lower=16.7153; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=19.5200 vs SMA50=18.8376 dist=+3.62% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=19.2195 SMA50=18.8376 SMA80=18.1779 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-28@16.6100; S2[2026-09-14→2026-10-12] low=2026-09-23@17.2000; S3[2026-10-16→2026-11-13] low=2026-11-10@16.5400 | lows=[16.610000610351562, 17.200000762939453, 16.540000915527344] span=3.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.5555413938305082 wick_frac=0.44445860616949184 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:RED:body=-0.2600:wick=0.6600; 2026-11-10:GREEN:body=+0.9000:wick=0.4650; 2026-11-11:GREEN:body=+0.1200:wick=0.4700; 2026-11-12:GREEN:body=+0.7000:wick=0.3600; 2026-11-13:GREEN:body=+0.3200:wick=0.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.39 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=54.79 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 374.75 | **NEUTRAL** |
| `B04_income` | 109.79 | **GOOD** |
| `B05_profit_margin` | 29.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.33 vs prior_export=24.33 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.8 vs prior=-0.8 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.53 | **GOOD** |
| `B13_short_float` | 7.33 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.39 (this export) | prior_export=3.39 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=54.79 (this export) | prior_export=54.79 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CSGP  ·  score **+15**  ·  Real Estate Services
price=30.959999084472656  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.62 on 2026-11-13; prev RSI=47.12 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.12@2026-11-12 → 50.62@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.12@2026-11-12 → 50.62@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.12@2026-11-12 → 50.62@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=1.404 (G=0.7300 R=0.5200); 2026-11-12:RED:O=30.6200,C=30.1000,body=-0.5200,vol=2750292.0; 2026-11-13:GREEN:O=30.2300,C=30.9600,body=+0.7300,vol=3185500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.158 (Gvol=3185500 Rvol=2750292); 2026-11-12:RED:O=30.6200,C=30.1000,body=-0.5200,vol=2750292.0; 2026-11-13:GREEN:O=30.2300,C=30.9600,body=+0.7300,vol=3185500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.506 on 2026-11-13: today_vol=3185500 / avg20=6296274 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.142 on 2026-11-13 (price=30.9600, mid=31.4952, upper=35.2643, lower=27.7262; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=30.9600 vs SMA50=30.1401 dist=+2.72% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=31.50_50=30.14_80=30.50 on 2026-11-13: SMA20=31.4952 SMA50=30.1401 SMA80=30.4993 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-10@28.7900; S2[2026-09-17→2026-10-15] low=2026-09-29@26.5200; S3[2026-10-16→2026-11-13] low=2026-10-28@26.8550 | lows=[28.790000915527344, 26.520000457763672, 26.854999542236328] span=8.56% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.8202247672604455 wick_frac=0.1797752327395546 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.7123298408799937 wick_frac=0.28767015912000626 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.403844037706782 need>1.4; red_wick_gt_green=False 5d trail=2026-11-09:GREEN:body=+0.6100:wick=0.9000; 2026-11-10:RED:body=-0.5100:wick=0.5595; 2026-11-11:GREEN:body=+0.4450:wick=0.2500; 2026-11-12:RED:body=-0.5200:wick=0.2100; 2026-11-13:GREEN:body=+0.7300:wick=0.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.79 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.65 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3674.0 | **NEUTRAL** |
| `B04_income` | 179.0 | **GOOD** |
| `B05_profit_margin` | 4.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 34.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=34.72 vs prior_export=34.72 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | 2.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.09 vs prior=2.09 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.01 | **GOOD** |
| `B13_short_float` | 3.14 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.79 (this export) | prior_export=12.79 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.65 (this export) | prior_export=0.65 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTE  ·  score **+14**  ·  Oil & Gas Integrated
price=90.73999786376953  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.90 on 2026-11-13; prev RSI=48.64 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.64@2026-11-12 → 52.90@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.64@2026-11-12 → 52.90@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.64@2026-11-12 → 52.90@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1700 R=0.0000); 2026-11-12:GREEN:O=88.6900,C=89.5500,body=+0.8600,vol=714724.0; 2026-11-13:GREEN:O=90.4300,C=90.7400,body=+0.3100,vol=870700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1585424 Rvol=0); 2026-11-12:GREEN:O=88.6900,C=89.5500,body=+0.8600,vol=714724.0; 2026-11-13:GREEN:O=90.4300,C=90.7400,body=+0.3100,vol=870700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.526 on 2026-11-13: today_vol=870700 / avg20=1654247 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.047 on 2026-11-13 (price=90.7400, mid=90.9550, upper=95.4993, lower=86.4107; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=90.7400 vs SMA50=89.7686 dist=+1.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=90.9550 SMA50=89.7686 SMA80=88.9787 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-13 (63 bars); S1[2026-08-18→2026-09-16] low=2026-08-27@86.0000; S2[2026-09-17→2026-10-15] low=2026-10-02@83.1500; S3[2026-10-16→2026-11-13] low=2026-11-10@86.3900 | lows=[86.0, 83.1500015258789, 86.38999938964844] span=3.90% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.5389448069523974 wick_frac=0.4610551930476025 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-09:GREEN:body=+0.2800:wick=0.9000; 2026-11-10:GREEN:body=+0.9600:wick=0.5800; 2026-11-11:RED:body=-0.0500:wick=0.5256; 2026-11-12:GREEN:body=+0.8600:wick=0.3771; 2026-11-13:GREEN:body=+0.3100:wick=0.5000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.56 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.68 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 214397.0 | **NEUTRAL** |
| `B04_income` | 22417.0 | **GOOD** |
| `B05_profit_margin` | 10.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.25 vs prior_export=103.25 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.82 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.04 | **BAD** |
| `B13_short_float` | 0.2 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.56 (this export) | prior_export=5.56 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.68 (this export) | prior_export=11.68 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BTE  ·  score **+14**  ·  Oil & Gas E&P
price=4.349999904632568  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=37.75 on 2026-11-13; prev RSI=36.92 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 36.92@2026-11-12 → 37.75@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 36.92@2026-11-12 → 37.75@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 36.92@2026-11-12 → 37.75@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=9.000 (G=0.0900 R=0.0100); 2026-11-12:GREEN:O=4.2400,C=4.3300,body=+0.0900,vol=13314328.0; 2026-11-13:RED:O=4.3600,C=4.3500,body=-0.0100,vol=12177300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.093 (Gvol=13314328 Rvol=12177300); 2026-11-12:GREEN:O=4.2400,C=4.3300,body=+0.0900,vol=13314328.0; 2026-11-13:RED:O=4.3600,C=4.3500,body=-0.0100,vol=12177300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.613 on 2026-11-13: today_vol=12177300 / avg20=19851128 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.474 on 2026-11-13 (price=4.3500, mid=4.8880, upper=6.0242, lower=3.7518; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-11-13: price=4.3500 vs SMA50=4.8344 dist=-10.02% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=4.8880 SMA50=4.8344 SMA80=4.6851 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-13@4.2500; S2[2026-09-14→2026-10-12] low=2026-09-29@4.4000; S3[2026-10-16→2026-11-13] low=2026-11-10@3.9550 | lows=[4.25, 4.400000095367432, 3.9549999237060547] span=11.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6428588458486176 wick_frac=0.3571411541513823 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.12500298023508094 wick_frac=0.8749970197649191 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.999809269502194 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:RED:body=-0.0300:wick=0.1000; 2026-11-10:GREEN:body=+0.1500:wick=0.0750; 2026-11-11:GREEN:body=+0.0400:wick=0.0600; 2026-11-12:GREEN:body=+0.0900:wick=0.0500; 2026-11-13:RED:body=-0.0100:wick=0.0700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.06 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=36.65 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2185.03 | **NEUTRAL** |
| `B04_income` | 153.64 | **GOOD** |
| `B05_profit_margin` | 7.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 6.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.010000000000000675 (now=6.18 vs prior_export=6.19 on finviz_2026-11-12) | **BAD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 3.07 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.06 (this export) | prior_export=14.06 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=36.65 (this export) | prior_export=36.65 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SVV  ·  score **+14**  ·  Specialty Retail
price=10.460000038146973  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.76 on 2026-11-13; prev RSI=55.17 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.17@2026-11-12 → 58.76@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.17@2026-11-12 → 58.76@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.17@2026-11-12 → 58.76@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4000 R=0.0000); 2026-11-12:GREEN:O=9.9500,C=10.0900,body=+0.1400,vol=1413167.0; 2026-11-13:GREEN:O=10.2000,C=10.4600,body=+0.2600,vol=1169920.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2583087 Rvol=0); 2026-11-12:GREEN:O=9.9500,C=10.0900,body=+0.1400,vol=1413167.0; 2026-11-13:GREEN:O=10.2000,C=10.4600,body=+0.2600,vol=1169920.0 | **GOOD** |
| `A07_rvol` | RVOL=0.586 on 2026-11-13: today_vol=1169920 / avg20=1997703 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.760 on 2026-11-13 (price=10.4600, mid=9.6635, upper=10.7113, lower=8.6157; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=10.4600 vs SMA50=9.5686 dist=+9.32% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=9.66_50=9.57_80=9.95 on 2026-11-13: SMA20=9.6635 SMA50=9.5686 SMA80=9.9454 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@9.5450; S2[2026-09-14→2026-10-12] low=2026-09-24@8.8300; S3[2026-10-16→2026-11-13] low=2026-10-28@8.6500 | lows=[9.545000076293945, 8.829999923706055, 8.649999618530273] span=10.35% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.6416996097456121 wick_frac=0.35830039025438787 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-09:GREEN:body=+0.5900:wick=0.1700; 2026-11-10:RED:body=-0.1100:wick=0.4450; 2026-11-11:RED:body=-0.3900:wick=0.1448; 2026-11-12:GREEN:body=+0.1400:wick=0.1190; 2026-11-13:GREEN:body=+0.2600:wick=0.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.51 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.84 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1779.16 | **NEUTRAL** |
| `B04_income` | 62.22 | **GOOD** |
| `B05_profit_margin` | 3.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.23 vs prior_export=14.23 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -33.33 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-33.33 vs prior=-33.33 on finviz_2026-11-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.29 | **GOOD** |
| `B13_short_float` | 15.07 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.51 (this export) | prior_export=4.51 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.84 (this export) | prior_export=0.84 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLYW  ·  score **+14**  ·  Software - Infrastructure
price=22.260000228881836  pair=`2026-11-12→2026-11-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.17 on 2026-11-13; prev RSI=63.67 on 2026-11-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.67@2026-11-12 → 68.17@2026-11-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.67@2026-11-12 → 68.17@2026-11-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.67@2026-11-12 → 68.17@2026-11-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_body_sum/RED_body_sum=1.060 (G=0.7100 R=0.6700); 2026-11-12:RED:O=22.1400,C=21.4700,body=-0.6700,vol=1784280.0; 2026-11-13:GREEN:O=21.5500,C=22.2600,body=+0.7100,vol=2403247.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-12 + 2026-11-13; ratio=GREEN_vol/RED_vol=1.347 (Gvol=2403247 Rvol=1784280); 2026-11-12:RED:O=22.1400,C=21.4700,body=-0.6700,vol=1784280.0; 2026-11-13:GREEN:O=21.5500,C=22.2600,body=+0.7100,vol=2403247.0 | **GOOD** |
| `A07_rvol` | RVOL=1.215 on 2026-11-13: today_vol=2403247 / avg20=1978453 (avg window 2026-10-16→2026-11-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.699 on 2026-11-13 (price=22.2600, mid=20.1815, upper=23.1556, lower=17.2074; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-13: price=22.2600 vs SMA50=18.7913 dist=+18.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-13: SMA20=20.1815 SMA50=18.7913 SMA80=18.4670 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-13 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-02@16.9000; S2[2026-09-14→2026-10-12] low=2026-09-24@16.6700; S3[2026-10-16→2026-11-13] low=2026-10-19@18.1200 | lows=[16.899999618530273, 16.670000076293945, 18.1200008392334] span=8.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: GREEN body_frac=0.5546888038528479 wick_frac=0.4453111961471521 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-12+2026-11-13: RED body_frac=0.795725450220863 wick_frac=0.20427454977913692 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0597028521975784 need>1.4; red_wick_gt_green=False 5d trail=2026-11-09:GREEN:body=+0.2750:wick=0.5450; 2026-11-10:GREEN:body=+0.3900:wick=0.4100; 2026-11-11:GREEN:body=+0.0400:wick=0.9450; 2026-11-12:RED:body=-0.6700:wick=0.1720; 2026-11-13:GREEN:body=+0.7100:wick=0.5700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=82.75 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.82 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 763.7 | **NEUTRAL** |
| `B04_income` | 78.95 | **GOOD** |
| `B05_profit_margin` | 10.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.5 vs prior_export=23.5 on finviz_2026-11-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | -14.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.009999999999999787 (now=-14.05 vs prior=-14.06 on finviz_2026-11-12) | **GOOD** |
| `B12_institutional_transactions` | 8.93 | **GOOD** |
| `B13_short_float` | 9.42 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=82.75 (this export) | prior_export=82.75 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.82 (this export) | prior_export=4.82 (finviz_2026-11-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-13_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.