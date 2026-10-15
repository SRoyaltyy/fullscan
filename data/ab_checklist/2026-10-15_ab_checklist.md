# A+B1 Feature Checklist — 2026-10-15

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-15.csv` · prior export for Δ: `2026-10-14`
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
| 1 | WAY | +17 | 18 | 1 | 2026-10-13→2026-10-14 | Health Information Services |
| 2 | AUPH | +17 | 17 | 0 | 2026-10-13→2026-10-14 | Biotechnology |
| 3 | RSG | +16 | 17 | 1 | 2026-10-13→2026-10-14 | Waste Management |
| 4 | BIIB | +16 | 18 | 2 | 2026-10-13→2026-10-14 | Drug Manufacturers - General |
| 5 | ATRC | +16 | 17 | 1 | 2026-10-13→2026-10-14 | Medical Instruments & Supplies |
| 6 | PAA | +15 | 15 | 0 | 2026-10-13→2026-10-14 | Oil & Gas Midstream |
| 7 | CVLT | +15 | 16 | 1 | 2026-10-13→2026-10-14 | Software - Application |
| 8 | ICLR | +15 | 17 | 2 | 2026-10-13→2026-10-14 | Diagnostics & Research |
| 9 | GCT | +15 | 17 | 2 | 2026-10-13→2026-10-14 | Software - Infrastructure |
| 10 | SONO | +15 | 16 | 1 | 2026-10-13→2026-10-14 | Consumer Electronics |
| 11 | PFE | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Drug Manufacturers - General |
| 12 | PBR | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Oil & Gas Integrated |
| 13 | SKM | +14 | 15 | 1 | 2026-10-13→2026-10-14 | Telecom Services |
| 14 | TASK | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Information Technology Services |
| 15 | MRK | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Drug Manufacturers - General |

## Full checklist — top 15

### WAY  ·  score **+17**  ·  Health Information Services
price=26.8700008392334  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.22 on 2026-10-14; prev RSI=55.58 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.58@2026-10-13 → 59.22@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.58@2026-10-13 → 59.22@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.58@2026-10-13 → 59.22@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.579 (G=0.9000 R=0.5700); 2026-10-13:RED:O=26.8700,C=26.3000,body=-0.5700,vol=1343100.0; 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=2.773 (Gvol=3725000 Rvol=1343100); 2026-10-13:RED:O=26.8700,C=26.3000,body=-0.5700,vol=1343100.0; 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.918 on 2026-10-14: today_vol=3725000 / avg20=1942485 (avg window 2026-09-16→2026-10-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.742 on 2026-10-14 (price=26.8700, mid=25.7775, upper=27.2498, lower=24.3052; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=26.8700 vs SMA50=25.2088 dist=+6.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=25.7775 SMA50=25.2088 SMA80=23.8803 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-30@20.1410; S2[2026-08-17→2026-09-15] low=2026-09-10@22.9550; S3[2026-09-16→2026-10-14] low=2026-09-25@24.3250 | lows=[20.141000747680664, 22.954999923706055, 24.325000762939453] span=20.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.8181829216062644 wick_frac=0.18181707839373557 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.3475616776746182 wick_frac=0.6524383223253818 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5789456072545969 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.4900:wick=0.4650; 2026-10-09:GREEN:body=+0.3000:wick=0.4850; 2026-10-12:RED:body=-0.3500:wick=0.4125; 2026-10-13:RED:body=-0.5700:wick=1.0700; 2026-10-14:GREEN:body=+0.9000:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.68 vs prior_export=33.68 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+17**  ·  Biotechnology
price=16.610000610351562  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.15 on 2026-10-14; prev RSI=51.98 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.98@2026-10-13 → 56.15@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.98@2026-10-13 → 56.15@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.98@2026-10-13 → 56.15@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=3.700 (G=0.3700 R=0.1000); 2026-10-13:RED:O=16.4600,C=16.3600,body=-0.1000,vol=1137600.0; 2026-10-14:GREEN:O=16.2400,C=16.6100,body=+0.3700,vol=1225700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.077 (Gvol=1225700 Rvol=1137600); 2026-10-13:RED:O=16.4600,C=16.3600,body=-0.1000,vol=1137600.0; 2026-10-14:GREEN:O=16.2400,C=16.6100,body=+0.3700,vol=1225700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.461 on 2026-10-14: today_vol=1225700 / avg20=839124 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.334 on 2026-10-14 (price=16.6100, mid=16.3650, upper=17.0994, lower=15.6306; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=16.6100 vs SMA50=16.1875 dist=+2.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=16.3650 SMA50=16.1875 SMA80=16.1237 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-14 (63 bars); S1[2026-07-16→2026-08-14] low=2026-07-31@14.3630; S2[2026-08-17→2026-09-15] low=2026-08-17@15.0700; S3[2026-09-16→2026-10-14] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.069999694824219, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5203946680115568 wick_frac=0.47960533198844324 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.27026553325738323 wick_frac=0.7297344667426168 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.700064850843061 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-13:RED:body=-0.1000:wick=0.2700; 2026-10-14:GREEN:body=+0.3700:wick=0.3410 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+16**  ·  Waste Management
price=224.44000244140625  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.32 on 2026-10-14; prev RSI=59.00 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.327 (G=3.7000 R=1.5900); 2026-10-13:RED:O=221.5000,C=219.9100,body=-1.5900,vol=955255.0; 2026-10-14:GREEN:O=220.7400,C=224.4400,body=+3.7000,vol=1226703.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.284 (Gvol=1226703 Rvol=955255); 2026-10-13:RED:O=221.5000,C=219.9100,body=-1.5900,vol=955255.0; 2026-10-14:GREEN:O=220.7400,C=224.4400,body=+3.7000,vol=1226703.0 | **GOOD** |
| `A07_rvol` | RVOL=0.858 on 2026-10-14: today_vol=1226703 / avg20=1429824 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.166 on 2026-10-14 (price=224.4400, mid=214.7780, upper=223.0672, lower=206.4888; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=224.4400 vs SMA50=217.4566 dist=+3.21% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=214.78_50=217.46_80=216.68 on 2026-10-14: SMA20=214.7780 SMA50=217.4566 SMA80=216.6833 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-08-05@204.8000; S2[2026-08-17→2026-09-15] low=2026-08-17@213.0000; S3[2026-09-16→2026-10-14] low=2026-10-02@205.9700 | lows=[204.8000030517578, 213.0, 205.97000122070312] span=4.00% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=1.0164828485313413 wick_frac=-0.016482848531341306 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4291503644825172 wick_frac=0.5708496355174828 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3270474654997026 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.2300:wick=3.1200; 2026-10-09:GREEN:body=+0.9100:wick=2.2800; 2026-10-12:GREEN:body=+1.8200:wick=2.8700; 2026-10-13:RED:body=-1.5900:wick=2.1150; 2026-10-14:GREEN:body=+3.7000:wick=-0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 6.57 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=6.57 vs prior=6.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.56 | **GOOD** |
| `B13_short_float` | 2.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BIIB  ·  score **+16**  ·  Drug Manufacturers - General
price=232.8300018310547  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.66 on 2026-10-14; prev RSI=59.62 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.62@2026-10-13 → 63.66@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.62@2026-10-13 → 63.66@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.62@2026-10-13 → 63.66@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=3.8400 R=1.4400); 2026-10-13:RED:O=230.3400,C=228.9000,body=-1.4400,vol=1049100.0; 2026-10-14:GREEN:O=228.9900,C=232.8300,body=+3.8400,vol=1947100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.856 (Gvol=1947100 Rvol=1049100); 2026-10-13:RED:O=230.3400,C=228.9000,body=-1.4400,vol=1049100.0; 2026-10-14:GREEN:O=228.9900,C=232.8300,body=+3.8400,vol=1947100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.622 on 2026-10-14: today_vol=1947100 / avg20=1200244 (avg window 2026-09-16→2026-10-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.947 on 2026-10-14 (price=232.8300, mid=224.4360, upper=233.3027, lower=215.5693; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=232.8300 vs SMA50=218.6068 dist=+6.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=224.4360 SMA50=218.6068 SMA80=213.2534 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-15@186.0300; S2[2026-08-17→2026-09-15] low=2026-08-17@207.6200; S3[2026-09-16→2026-10-14] low=2026-09-21@214.5900 | lows=[186.02999877929688, 207.6199951171875, 214.58999633789062] span=15.35% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.6193545529049549 wick_frac=0.38064544709504505 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.3325651055432216 wick_frac=0.6674348944567784 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6666596024244478 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-1.7200:wick=5.6400; 2026-10-09:GREEN:body=+5.0900:wick=2.8850; 2026-10-12:RED:body=-4.8300:wick=4.4100; 2026-10-13:RED:body=-1.4400:wick=2.8900; 2026-10-14:GREEN:body=+3.8400:wick=2.3600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.49 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.2 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9661.4 | **NEUTRAL** |
| `B04_income` | 834.6 | **GOOD** |
| `B05_profit_margin` | 8.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 239.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=239.28 vs prior_export=239.28 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -0.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.11 vs prior=-0.11 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.12 | **GOOD** |
| `B13_short_float` | 3.97 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.49 (this export) | prior_export=22.49 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.2 (this export) | prior_export=11.2 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATRC  ·  score **+16**  ·  Medical Instruments & Supplies
price=57.29999923706055  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.12 on 2026-10-14; prev RSI=56.98 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.98@2026-10-13 → 58.12@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.98@2026-10-13 → 58.12@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.98@2026-10-13 → 58.12@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.603 (G=1.1700 R=0.7300); 2026-10-13:RED:O=57.6000,C=56.8700,body=-0.7300,vol=596100.0; 2026-10-14:GREEN:O=56.1300,C=57.3000,body=+1.1700,vol=799300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.341 (Gvol=799300 Rvol=596100); 2026-10-13:RED:O=57.6000,C=56.8700,body=-0.7300,vol=596100.0; 2026-10-14:GREEN:O=56.1300,C=57.3000,body=+1.1700,vol=799300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.543 on 2026-10-14: today_vol=799300 / avg20=1471690 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.084 on 2026-10-14 (price=57.3000, mid=56.9165, upper=61.4656, lower=52.3674; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=57.3000 vs SMA50=51.8058 dist=+10.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=56.9165 SMA50=51.8058 SMA80=44.8561 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@31.9000; S2[2026-08-17→2026-09-15] low=2026-08-17@43.4100; S3[2026-09-16→2026-10-14] low=2026-10-08@51.6300 | lows=[31.899999618530273, 43.40999984741211, 51.630001068115234] span=61.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5707310265261121 wick_frac=0.4292689734738879 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4591191648932725 wick_frac=0.5408808351067276 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6027382227680087 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.2300:wick=1.6000; 2026-10-09:GREEN:body=+2.6700:wick=0.7850; 2026-10-12:GREEN:body=+0.9900:wick=0.6900; 2026-10-13:RED:body=-0.7300:wick=0.8600; 2026-10-14:GREEN:body=+1.1700:wick=0.8800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10688.24 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 569.62 | **NEUTRAL** |
| `B04_income` | 10.55 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.33 vs prior_export=53.33 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -3.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.89 vs prior=-3.89 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.44 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10688.24 (this export) | prior_export=10688.24 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAA  ·  score **+15**  ·  Oil & Gas Midstream
price=24.979999542236328  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.14 on 2026-10-14; prev RSI=57.22 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.22@2026-10-13 → 58.14@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.22@2026-10-13 → 58.14@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.22@2026-10-13 → 58.14@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=10.000 (G=0.2000 R=0.0200); 2026-10-13:GREEN:O=24.7200,C=24.9200,body=+0.2000,vol=2296600.0; 2026-10-14:RED:O=25.0000,C=24.9800,body=-0.0200,vol=2008700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.143 (Gvol=2296600 Rvol=2008700); 2026-10-13:GREEN:O=24.7200,C=24.9200,body=+0.2000,vol=2296600.0; 2026-10-14:RED:O=25.0000,C=24.9800,body=-0.0200,vol=2008700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.847 on 2026-10-14: today_vol=2008700 / avg20=2372588 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.530 on 2026-10-14 (price=24.9800, mid=24.4335, upper=25.4650, lower=23.4020; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=24.9800 vs SMA50=24.6344 dist=+1.40% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=24.43_50=24.63_80=23.95 on 2026-10-14: SMA20=24.4335 SMA50=24.6344 SMA80=23.9453 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-14 (63 bars); S1[2026-07-15→2026-08-14] low=2026-08-07@22.7300; S2[2026-08-17→2026-09-15] low=2026-08-17@23.6900; S3[2026-09-16→2026-10-14] low=2026-10-02@23.4400 | lows=[22.729999542236328, 23.690000534057617, 23.440000534057617] span=4.22% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5000023841880648 wick_frac=0.49999761581193525 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.07692476983457433 wick_frac=0.9230752301654257 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.999809269502194 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.0300:wick=0.3500; 2026-10-09:RED:body=-0.2200:wick=0.2000; 2026-10-12:GREEN:body=+0.0300:wick=0.2200; 2026-10-13:GREEN:body=+0.2000:wick=0.2000; 2026-10-14:RED:body=-0.0200:wick=0.2400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.44 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=39.05 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 2548.0 | **GOOD** |
| `B05_profit_margin` | 4.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.54 | **GOOD** |
| `B13_short_float` | 4.0 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.44 (this export) | prior_export=7.44 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=39.05 (this export) | prior_export=39.05 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVLT  ·  score **+15**  ·  Software - Application
price=154.3300018310547  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.37 on 2026-10-14; prev RSI=64.01 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.642 (G=4.5000 R=2.7400); 2026-10-13:GREEN:O=154.3500,C=158.8500,body=+4.5000,vol=597026.0; 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.094 (Gvol=597026 Rvol=545597); 2026-10-13:GREEN:O=154.3500,C=158.8500,body=+4.5000,vol=597026.0; 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0 | **GOOD** |
| `A07_rvol` | RVOL=0.901 on 2026-10-14: today_vol=545597 / avg20=605488 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.476 on 2026-10-14 (price=154.3300, mid=150.4310, upper=158.6300, lower=142.2320; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=154.3300 vs SMA50=142.3926 dist=+8.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=150.4310 SMA50=142.3926 SMA80=141.2843 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-30@115.3100; S2[2026-08-17→2026-09-15] low=2026-09-02@125.9000; S3[2026-09-16→2026-10-14] low=2026-09-18@141.0000 | lows=[115.30999755859375, 125.9000015258789, 141.0] span=22.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7894719934895973 wick_frac=0.21052800651040274 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4528932414948057 wick_frac=0.5471067585051943 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6423324738679839 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.6200:wick=3.3100; 2026-10-09:GREEN:body=+4.4700:wick=2.8850; 2026-10-12:RED:body=-3.2300:wick=1.2300; 2026-10-13:GREEN:body=+4.5000:wick=1.2000; 2026-10-14:RED:body=-2.7400:wick=3.3100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=22.16 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1215.84 | **NEUTRAL** |
| `B04_income` | 68.3 | **GOOD** |
| `B05_profit_margin` | 5.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 160.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=160.75 vs prior_export=160.75 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -15.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.45 vs prior=-15.45 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 8.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.16 (this export) | prior_export=22.16 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICLR  ·  score **+15**  ·  Diagnostics & Research
price=168.97000122070312  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.14 on 2026-10-14; prev RSI=47.41 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.41@2026-10-13 → 53.14@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.41@2026-10-13 → 53.14@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.41@2026-10-13 → 53.14@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=27.150 (G=5.4300 R=0.2000); 2026-10-13:RED:O=164.5400,C=164.3400,body=-0.2000,vol=815100.0; 2026-10-14:GREEN:O=163.5400,C=168.9700,body=+5.4300,vol=629700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=0.773 (Gvol=629700 Rvol=815100); 2026-10-13:RED:O=164.5400,C=164.3400,body=-0.2000,vol=815100.0; 2026-10-14:GREEN:O=163.5400,C=168.9700,body=+5.4300,vol=629700.0 | **BAD** |
| `A07_rvol` | RVOL=0.633 on 2026-10-14: today_vol=629700 / avg20=995479 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.234 on 2026-10-14 (price=168.9700, mid=167.0435, upper=175.2864, lower=158.8006; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=168.9700 vs SMA50=166.9774 dist=+1.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=167.0435 SMA50=166.9774 SMA80=166.2920 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-08-04@150.8600; S2[2026-08-17→2026-09-15] low=2026-09-10@156.7000; S3[2026-09-16→2026-10-14] low=2026-10-07@157.3750 | lows=[150.86000061035156, 156.6999969482422, 157.375] span=4.32% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7562676788177214 wick_frac=0.24373232118227858 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.031007293468052338 wick_frac=0.9689927065319477 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=27.150453955901426 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+2.8800:wick=2.9100; 2026-10-09:RED:body=-2.8700:wick=3.4900; 2026-10-12:RED:body=-0.7400:wick=5.1900; 2026-10-13:RED:body=-0.2000:wick=6.2500; 2026-10-14:GREEN:body=+5.4300:wick=1.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.57 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.95 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8294.42 | **NEUTRAL** |
| `B04_income` | 42.34 | **GOOD** |
| `B05_profit_margin` | 0.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 187.27 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=187.27 vs prior_export=187.27 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -2.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.84 vs prior=-2.84 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.74 | **GOOD** |
| `B13_short_float` | 3.44 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.57 (this export) | prior_export=0.57 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.95 (this export) | prior_export=2.95 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+15**  ·  Software - Infrastructure
price=57.75  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.41 on 2026-10-14; prev RSI=65.32 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=3.553 (G=1.6700 R=0.4700); 2026-10-13:RED:O=57.5000,C=57.0300,body=-0.4700,vol=633969.0; 2026-10-14:GREEN:O=56.0800,C=57.7500,body=+1.6700,vol=819507.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.293 (Gvol=819507 Rvol=633969); 2026-10-13:RED:O=57.5000,C=57.0300,body=-0.4700,vol=633969.0; 2026-10-14:GREEN:O=56.0800,C=57.7500,body=+1.6700,vol=819507.0 | **GOOD** |
| `A07_rvol` | RVOL=1.114 on 2026-10-14: today_vol=819507 / avg20=735712 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.824 on 2026-10-14 (price=57.7500, mid=54.2905, upper=58.4886, lower=50.0924; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=57.7500 vs SMA50=52.4347 dist=+10.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=54.2905 SMA50=52.4347 SMA80=46.3573 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-14 (63 bars); S1[2026-07-15→2026-08-14] low=2026-07-23@35.4000; S2[2026-08-17→2026-09-15] low=2026-09-01@46.0100; S3[2026-09-16→2026-10-14] low=2026-09-22@49.3400 | lows=[35.400001525878906, 46.0099983215332, 49.34000015258789] span=39.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5502465413200236 wick_frac=0.4497534586799764 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.21159761281181572 wick_frac=0.7884023871881842 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.553178365041231 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+1.0500:wick=1.3900; 2026-10-09:GREEN:body=+0.0700:wick=2.0300; 2026-10-12:GREEN:body=+0.3400:wick=1.1400; 2026-10-13:RED:body=-0.4700:wick=1.7512; 2026-10-14:GREEN:body=+1.6700:wick=1.3650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+15**  ·  Consumer Electronics
price=17.600000381469727  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.34 on 2026-10-14; prev RSI=60.06 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=5.400 (G=0.5400 R=0.1000); 2026-10-13:GREEN:O=17.1800,C=17.7200,body=+0.5400,vol=2387607.0; 2026-10-14:RED:O=17.7000,C=17.6000,body=-0.1000,vol=1507052.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.584 (Gvol=2387607 Rvol=1507052); 2026-10-13:GREEN:O=17.1800,C=17.7200,body=+0.5400,vol=2387607.0; 2026-10-14:RED:O=17.7000,C=17.6000,body=-0.1000,vol=1507052.0 | **GOOD** |
| `A07_rvol` | RVOL=0.698 on 2026-10-14: today_vol=1507052 / avg20=2157867 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.205 on 2026-10-14 (price=17.6000, mid=17.3153, upper=18.7075, lower=15.9230; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=17.6000 vs SMA50=16.2435 dist=+8.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=17.3153 SMA50=16.2435 SMA80=15.6261 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-14 (63 bars); S1[2026-07-15→2026-08-14] low=2026-07-15@13.7600; S2[2026-08-17→2026-09-15] low=2026-09-09@14.2000; S3[2026-09-16→2026-10-14] low=2026-09-18@15.2800 | lows=[13.760000228881836, 14.199999809265137, 15.279999732971191] span=11.05% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7346919180385725 wick_frac=0.26530808196142747 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.17857167185509634 wick_frac=0.8214283281449036 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.39996948253829 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:RED:body=-0.0300:wick=0.3600; 2026-10-09:GREEN:body=+0.2800:wick=0.1600; 2026-10-12:GREEN:body=+0.0900:wick=0.7750; 2026-10-13:GREEN:body=+0.5400:wick=0.1950; 2026-10-14:RED:body=-0.1000:wick=0.4600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.43 vs prior=-4.43 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PFE  ·  score **+14**  ·  Drug Manufacturers - General
price=29.209999084472656  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.53 on 2026-10-14; prev RSI=56.97 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.542 (G=0.6100 R=0.2400); 2026-10-13:RED:O=28.6100,C=28.3700,body=-0.2400,vol=26826046.0; 2026-10-14:GREEN:O=28.6000,C=29.2100,body=+0.6100,vol=39302193.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.465 (Gvol=39302193 Rvol=26826046); 2026-10-13:RED:O=28.6100,C=28.3700,body=-0.2400,vol=26826046.0; 2026-10-14:GREEN:O=28.6000,C=29.2100,body=+0.6100,vol=39302193.0 | **GOOD** |
| `A07_rvol` | RVOL=1.236 on 2026-10-14: today_vol=39302193 / avg20=31794297 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.107 on 2026-10-14 (price=29.2100, mid=28.1550, upper=29.1085, lower=27.2015; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=29.2100 vs SMA50=27.8288 dist=+4.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=28.1550 SMA50=27.8288 SMA80=26.4884 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-21@24.2262; S2[2026-08-17→2026-09-15] low=2026-08-17@26.5500; S3[2026-09-16→2026-10-14] low=2026-10-05@27.1200 | lows=[24.226188874908235, 26.549999237060547, 27.1200008392334] span=11.94% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.6489344217388923 wick_frac=0.35106557826110774 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4999980131845076 wick_frac=0.5000019868154923 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5416636864315856 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.0800:wick=0.4400; 2026-10-09:GREEN:body=+0.6700:wick=0.0250; 2026-10-12:RED:body=-0.0200:wick=0.1950; 2026-10-13:RED:body=-0.2400:wick=0.2400; 2026-10-14:GREEN:body=+0.6100:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.41 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 63696.0 | **NEUTRAL** |
| `B04_income` | 4332.0 | **GOOD** |
| `B05_profit_margin` | 6.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.57 vs prior_export=28.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | 2.18 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.18 vs prior=2.18 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.47 | **BAD** |
| `B13_short_float` | 2.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.87 (this export) | prior_export=12.87 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.41 (this export) | prior_export=4.41 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+14**  ·  Oil & Gas Integrated
price=25.940000534057617  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=81.22 on 2026-10-14; prev RSI=79.73 on 2026-10-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5400 R=0.0000); 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0; 2026-10-14:GREEN:O=25.6400,C=25.9400,body=+0.3000,vol=19676375.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=37240359 Rvol=0); 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0; 2026-10-14:GREEN:O=25.6400,C=25.9400,body=+0.3000,vol=19676375.0 | **GOOD** |
| `A07_rvol` | RVOL=0.815 on 2026-10-14: today_vol=19676375 / avg20=24132281 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.839 on 2026-10-14 (price=25.9400, mid=22.4530, upper=26.6098, lower=18.2962; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=25.9400 vs SMA50=20.5430 dist=+26.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=22.4530 SMA50=20.5430 SMA80=19.4493 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-08-13@17.6000; S2[2026-08-17→2026-09-15] low=2026-08-27@17.5400; S3[2026-09-16→2026-10-14] low=2026-09-25@20.2000 | lows=[17.600000381469727, 17.540000915527344, 20.200000762939453] span=15.17% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.6239204341911745 wick_frac=0.3760795658088255 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.2900:wick=0.3000; 2026-10-09:GREEN:body=+0.6760:wick=0.3140; 2026-10-12:RED:body=-0.0200:wick=0.5000; 2026-10-13:GREEN:body=+0.2400:wick=0.2250; 2026-10-14:GREEN:body=+0.3000:wick=0.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+14**  ·  Telecom Services
price=33.88999938964844  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=40.62 on 2026-10-14; prev RSI=42.97 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=132.971 (G=1.3300 R=0.0100); 2026-10-13:GREEN:O=33.0900,C=34.4200,body=+1.3300,vol=1991125.0; 2026-10-14:RED:O=33.9000,C=33.8900,body=-0.0100,vol=1393508.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.429 (Gvol=1991125 Rvol=1393508); 2026-10-13:GREEN:O=33.0900,C=34.4200,body=+1.3300,vol=1991125.0; 2026-10-14:RED:O=33.9000,C=33.8900,body=-0.0100,vol=1393508.0 | **GOOD** |
| `A07_rvol` | RVOL=1.257 on 2026-10-14: today_vol=1393508 / avg20=1108773 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.803 on 2026-10-14 (price=33.8900, mid=35.4260, upper=37.3391, lower=33.5129; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-14: price=33.8900 vs SMA50=36.6332 dist=-7.49% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=35.43_50=36.63_80=35.06 on 2026-10-14: SMA20=35.4260 SMA50=36.6332 SMA80=35.0587 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-29@29.5300; S2[2026-08-17→2026-09-15] low=2026-09-15@35.5700; S3[2026-09-16→2026-10-14] low=2026-10-12@32.8500 | lows=[29.530000686645508, 35.56999969482422, 32.849998474121094] span=20.45% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.9109581166880046 wick_frac=0.0890418833119954 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.009825266147798684 wick_frac=0.9901747338522013 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=132.97139588100686 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-1.0100:wick=0.3000; 2026-10-09:RED:body=-0.3600:wick=0.4800; 2026-10-12:GREEN:body=+0.1900:wick=0.5400; 2026-10-13:GREEN:body=+1.3300:wick=0.1300; 2026-10-14:RED:body=-0.0100:wick=1.0080 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=44.43 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.38 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11760.23 | **NEUTRAL** |
| `B04_income` | 483.22 | **GOOD** |
| `B05_profit_margin` | 4.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.14 vs prior_export=45.14 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.85 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TASK  ·  score **+14**  ·  Information Technology Services
price=8.140000343322754  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.78 on 2026-10-14; prev RSI=53.64 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.64@2026-10-13 → 52.78@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.64@2026-10-13 → 52.78@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.64@2026-10-13 → 52.78@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=4.333 (G=0.1300 R=0.0300); 2026-10-13:GREEN:O=8.0500,C=8.1800,body=+0.1300,vol=195400.0; 2026-10-14:RED:O=8.1700,C=8.1400,body=-0.0300,vol=173200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.128 (Gvol=195400 Rvol=173200); 2026-10-13:GREEN:O=8.0500,C=8.1800,body=+0.1300,vol=195400.0; 2026-10-14:RED:O=8.1700,C=8.1400,body=-0.0300,vol=173200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.380 on 2026-10-14: today_vol=173200 / avg20=456001 (avg window 2026-09-16→2026-10-13, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.025 on 2026-10-14 (price=8.1400, mid=8.1510, upper=8.5830, lower=7.7190; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=8.1400 vs SMA50=7.8887 dist=+3.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=8.1510 SMA50=7.8887 SMA80=6.9658 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@5.3200; S2[2026-08-17→2026-09-15] low=2026-08-17@7.1050; S3[2026-09-16→2026-10-14] low=2026-09-24@7.5300 | lows=[5.320000171661377, 7.105000019073486, 7.53000020980835] span=41.54% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5777797557739829 wick_frac=0.42222024422601717 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.1249987582378501 wick_frac=0.8750012417621499 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.333375719235782 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.1600:wick=0.0400; 2026-10-09:GREEN:body=+0.1000:wick=0.0600; 2026-10-12:RED:body=-0.1400:wick=0.1300; 2026-10-13:GREEN:body=+0.1300:wick=0.0950; 2026-10-14:RED:body=-0.0300:wick=0.2100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.65 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.8 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1226.79 | **NEUTRAL** |
| `B04_income` | 107.38 | **GOOD** |
| `B05_profit_margin` | 8.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.5 vs prior_export=8.5 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.88 | **NEUTRAL** |
| `B10_insider_transactions` | -0.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.05 vs prior=-0.05 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.32 | **GOOD** |
| `B13_short_float` | 11.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.65 (this export) | prior_export=16.65 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.8 (this export) | prior_export=3.8 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MRK  ·  score **+14**  ·  Drug Manufacturers - General
price=147.3699951171875  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.63 on 2026-10-14; prev RSI=44.26 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.103 (G=4.2700 R=3.8700); 2026-10-13:RED:O=145.8500,C=141.9800,body=-3.8700,vol=8378020.0; 2026-10-14:GREEN:O=143.1000,C=147.3700,body=+4.2700,vol=9784632.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.168 (Gvol=9784632 Rvol=8378020); 2026-10-13:RED:O=145.8500,C=141.9800,body=-3.8700,vol=8378020.0; 2026-10-14:GREEN:O=143.1000,C=147.3700,body=+4.2700,vol=9784632.0 | **GOOD** |
| `A07_rvol` | RVOL=1.010 on 2026-10-14: today_vol=9784632 / avg20=9684545 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.241 on 2026-10-14 (price=147.3700, mid=145.8855, upper=152.0487, lower=139.7223; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=147.3700 vs SMA50=144.4346 dist=+2.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=145.8855 SMA50=144.4346 SMA80=137.8877 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-21@124.1000; S2[2026-08-17→2026-09-15] low=2026-08-17@134.5500; S3[2026-09-16→2026-10-14] low=2026-10-05@138.8000 | lows=[124.0999984741211, 134.5500030517578, 138.8000030517578] span=11.85% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7806193895369921 wick_frac=0.2193806104630079 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.8889422419745471 wick_frac=0.11105775802545292 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.103353376047314 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.4800:wick=3.4000; 2026-10-09:GREEN:body=+4.2300:wick=1.0900; 2026-10-12:RED:body=-0.3000:wick=1.4199; 2026-10-13:RED:body=-3.8700:wick=0.4835; 2026-10-14:GREEN:body=+4.2700:wick=1.2000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=51.94 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.45 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 66287.0 | **NEUTRAL** |
| `B04_income` | 3173.0 | **GOOD** |
| `B05_profit_margin` | 4.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=153.76 vs prior_export=153.76 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -8.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.47 vs prior=-8.47 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.51 | **GOOD** |
| `B13_short_float` | 1.4 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=51.94 (this export) | prior_export=51.94 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.45 (this export) | prior_export=1.45 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-15_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.