# A+B1 Feature Checklist — 2026-08-25

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,705** names
- Export: `finviz_2026-08-25.csv` · prior export for Δ: `2026-08-24`
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
| 1 | BNL | +19 | 19 | 0 | 2026-08-24→2026-08-25 | REIT - Diversified |
| 2 | GOOD | +18 | 18 | 0 | 2026-08-24→2026-08-25 | REIT - Diversified |
| 3 | CRSR | +17 | 18 | 1 | 2026-08-24→2026-08-25 | Computer Hardware |
| 4 | BZ | +17 | 18 | 1 | 2026-08-24→2026-08-25 | Internet Content & Information |
| 5 | VNO | +16 | 16 | 0 | 2026-08-24→2026-08-25 | REIT - Office |
| 6 | RSG | +16 | 17 | 1 | 2026-08-24→2026-08-25 | Waste Management |
| 7 | RHI | +16 | 17 | 1 | 2026-08-24→2026-08-25 | Staffing & Employment Services |
| 8 | FDX | +16 | 17 | 1 | 2026-08-24→2026-08-25 | Integrated Freight & Logistics |
| 9 | AMH | +16 | 17 | 1 | 2026-08-24→2026-08-25 | REIT - Residential |
| 10 | RYAN | +16 | 18 | 2 | 2026-08-24→2026-08-25 | Insurance - Specialty |
| 11 | ASH | +16 | 16 | 0 | 2026-08-24→2026-08-25 | Specialty Chemicals |
| 12 | DLR | +16 | 16 | 0 | 2026-08-24→2026-08-25 | REIT - Specialty |
| 13 | CM | +16 | 17 | 1 | 2026-08-24→2026-08-25 | Banks - Diversified |
| 14 | GDS | +16 | 17 | 1 | 2026-08-24→2026-08-25 | Information Technology Services |
| 15 | TSLX | +16 | 16 | 0 | 2026-08-24→2026-08-25 | Asset Management |

## Full checklist — top 15

### BNL  ·  score **+19**  ·  REIT - Diversified
price=21.549999237060547  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.72 on 2026-08-25; prev RSI=48.85 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.85@2026-08-24 → 54.72@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.85@2026-08-24 → 54.72@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.85@2026-08-24 → 54.72@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=15.002 (G=0.1500 R=0.0100); 2026-08-24:RED:O=21.2100,C=21.2000,body=-0.0100,vol=2169000.0; 2026-08-25:GREEN:O=21.4000,C=21.5500,body=+0.1500,vol=2187343.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.008 (Gvol=2187343 Rvol=2169000); 2026-08-24:RED:O=21.2100,C=21.2000,body=-0.0100,vol=2169000.0; 2026-08-25:GREEN:O=21.4000,C=21.5500,body=+0.1500,vol=2187343.0 | **GOOD** |
| `A07_rvol` | RVOL=0.861 on 2026-08-25: today_vol=2187343 / avg20=2539515 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.352 on 2026-08-25 (price=21.5500, mid=21.2400, upper=22.1218, lower=20.3582; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=21.5500 vs SMA50=21.2917 dist=+1.21% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=21.24_50=21.29_80=20.75 on 2026-08-25: SMA20=21.2400 SMA50=21.2917 SMA80=20.7482 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-01@19.5209; S2[2026-06-23→2026-07-24] low=2026-06-29@20.4725; S3[2026-07-27→2026-08-25] low=2026-08-11@20.3600 | lows=[19.52090051423501, 20.472539694579044, 20.360000610351562] span=4.87% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5555532008081493 wick_frac=0.4444467991918507 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.03225250722943457 wick_frac=0.9677474927705655 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.0024799694773 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.5800:wick=0.0000; 2026-08-20:GREEN:body=+0.1700:wick=0.2950; 2026-08-21:RED:body=-0.1600:wick=0.1000; 2026-08-24:RED:body=-0.0100:wick=0.3000; 2026-08-25:GREEN:body=+0.1500:wick=0.1200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.66 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.56 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 477.57 | **NEUTRAL** |
| `B04_income` | 140.69 | **GOOD** |
| `B05_profit_margin` | 29.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.7 vs prior_export=23.7 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | 0.5 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.46 (now=0.5 vs prior=0.04 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 6.0 | **GOOD** |
| `B13_short_float` | 6.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.66 (this export) | prior_export=9.66 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.56 (this export) | prior_export=0.56 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GOOD  ·  score **+18**  ·  REIT - Diversified
price=13.119999885559082  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.06 on 2026-08-25; prev RSI=58.64 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.64@2026-08-24 → 57.06@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.64@2026-08-24 → 57.06@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.64@2026-08-24 → 57.06@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.857 (G=0.2000 R=0.0700); 2026-08-24:GREEN:O=12.9800,C=13.1800,body=+0.2000,vol=369000.0; 2026-08-25:RED:O=13.1900,C=13.1200,body=-0.0700,vol=318868.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.157 (Gvol=369000 Rvol=318868); 2026-08-24:GREEN:O=12.9800,C=13.1800,body=+0.2000,vol=369000.0; 2026-08-25:RED:O=13.1900,C=13.1200,body=-0.0700,vol=318868.0 | **GOOD** |
| `A07_rvol` | RVOL=0.744 on 2026-08-25: today_vol=318868 / avg20=428355 (avg window 2026-07-23→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.508 on 2026-08-25 (price=13.1200, mid=12.8665, upper=13.3657, lower=12.3673; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=13.1200 vs SMA50=12.5807 dist=+4.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=12.8665 SMA50=12.5807 SMA80=12.5035 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-25 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-17@11.6884; S2[2026-06-22→2026-07-22] low=2026-06-22@11.7179; S3[2026-07-23→2026-08-25] low=2026-08-05@12.1100 | lows=[11.688406380704969, 11.717921671402111, 12.109999656677246] span=3.61% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8695666596177848 wick_frac=0.13043334038221527 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.3783778210798717 wick_frac=0.6216221789201283 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.85716621253406 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.1600:wick=0.0500; 2026-08-20:RED:body=-0.0300:wick=0.0600; 2026-08-21:RED:body=-0.1300:wick=0.0800; 2026-08-24:GREEN:body=+0.2000:wick=0.0300; 2026-08-25:RED:body=-0.0700:wick=0.1150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=144.44 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.29 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 170.2 | **NEUTRAL** |
| `B04_income` | 12.18 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.36 vs prior_export=14.36 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.17 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.17 vs prior=0.17 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.1 | **GOOD** |
| `B13_short_float` | 7.44 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=144.44 (this export) | prior_export=144.44 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.29 (this export) | prior_export=4.29 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+17**  ·  Computer Hardware
price=11.84000015258789  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.98 on 2026-08-25; prev RSI=47.11 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.11@2026-08-24 → 53.98@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.11@2026-08-24 → 53.98@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.11@2026-08-24 → 53.98@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9700 R=0.0000); 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0; 2026-08-25:GREEN:O=11.0300,C=11.8400,body=+0.8100,vol=1665910.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3481110 Rvol=0); 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0; 2026-08-25:GREEN:O=11.0300,C=11.8400,body=+0.8100,vol=1665910.0 | **GOOD** |
| `A07_rvol` | RVOL=0.722 on 2026-08-25: today_vol=1665910 / avg20=2308740 (avg window 2026-07-28→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.020 on 2026-08-25 (price=11.8400, mid=11.7927, upper=14.1959, lower=9.3896; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=11.8400 vs SMA50=10.3271 dist=+14.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=11.7927 SMA50=10.3271 SMA80=9.6136 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-25 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-09@8.0500; S2[2026-06-26→2026-07-27] low=2026-06-26@8.1950; S3[2026-07-28→2026-08-25] low=2026-07-28@9.9200 | lows=[8.050000190734863, 8.194999694824219, 9.920000076293945] span=23.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.7852305861979265 wick_frac=0.21476941380207348 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.0100:wick=0.4680; 2026-08-20:RED:body=-0.3300:wick=0.2800; 2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1040; 2026-08-25:GREEN:body=+0.8100:wick=0.0299 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 25.27 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BZ  ·  score **+17**  ·  Internet Content & Information
price=16.290000915527344  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.56 on 2026-08-25; prev RSI=48.18 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.18@2026-08-24 → 56.56@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.18@2026-08-24 → 56.56@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.18@2026-08-24 → 56.56@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=1.0200 R=0.5100); 2026-08-24:RED:O=15.9500,C=15.4400,body=-0.5100,vol=5020200.0; 2026-08-25:GREEN:O=15.2700,C=16.2900,body=+1.0200,vol=14347755.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=2.858 (Gvol=14347755 Rvol=5020200); 2026-08-24:RED:O=15.9500,C=15.4400,body=-0.5100,vol=5020200.0; 2026-08-25:GREEN:O=15.2700,C=16.2900,body=+1.0200,vol=14347755.0 | **GOOD** |
| `A07_rvol` | RVOL=3.550 on 2026-08-25: today_vol=14347755 / avg20=4041765 (avg window 2026-07-27→2026-08-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.187 on 2026-08-25 (price=16.2900, mid=16.1050, upper=17.0931, lower=15.1169; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=16.2900 vs SMA50=14.8184 dist=+9.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=16.1050 SMA50=14.8184 SMA80=14.4877 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-28@13.0200; S2[2026-06-23→2026-07-24] low=2026-06-26@12.5700; S3[2026-07-27→2026-08-25] low=2026-08-14@14.8000 | lows=[13.020000457763672, 12.569999694824219, 14.800000190734863] span=17.74% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8160009887702856 wick_frac=0.18399901122971438 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.47663586386874585 wick_frac=0.5233641361312541 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.2800:wick=0.3000; 2026-08-20:RED:body=-0.1000:wick=0.3450; 2026-08-21:GREEN:body=+0.1000:wick=0.3050; 2026-08-24:RED:body=-0.5100:wick=0.5600; 2026-08-25:GREEN:body=+1.0200:wick=0.2300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.91 (current export asof; earnings_date=8/25/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.56 (current export; earnings_date=8/25/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1184.81 | **NEUTRAL** |
| `B04_income` | 476.69 | **GOOD** |
| `B05_profit_margin` | 40.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 20.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=20.89 vs prior_export=20.89 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | -4.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.99 vs prior=-4.99 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.68 | **GOOD** |
| `B13_short_float` | 5.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/25/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.91 (this export) | prior_export=5.91 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.56 (this export) | prior_export=0.56 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNO  ·  score **+16**  ·  REIT - Office
price=39.939998626708984  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.13 on 2026-08-25; prev RSI=50.18 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.18@2026-08-24 → 56.13@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.18@2026-08-24 → 56.13@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.18@2026-08-24 → 56.13@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8500 R=0.0000); 2026-08-24:GREEN:O=37.9200,C=38.8700,body=+0.9500,vol=862800.0; 2026-08-25:GREEN:O=39.0400,C=39.9400,body=+0.9000,vol=1275762.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2138562 Rvol=0); 2026-08-24:GREEN:O=37.9200,C=38.8700,body=+0.9500,vol=862800.0; 2026-08-25:GREEN:O=39.0400,C=39.9400,body=+0.9000,vol=1275762.0 | **GOOD** |
| `A07_rvol` | RVOL=0.849 on 2026-08-25: today_vol=1275762 / avg20=1503420 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.382 on 2026-08-25 (price=39.9400, mid=39.2510, upper=41.0565, lower=37.4455; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=39.9400 vs SMA50=39.0360 dist=+2.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=39.2510 SMA50=39.0360 SMA80=36.4876 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@31.0100; S2[2026-06-23→2026-07-24] low=2026-06-24@37.3100; S3[2026-07-27→2026-08-25] low=2026-08-18@37.5700 | lows=[31.010000228881836, 37.310001373291016, 37.56999969482422] span=21.15% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6808829783054418 wick_frac=0.31911702169455836 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.2800:wick=0.6900; 2026-08-20:GREEN:body=+0.4600:wick=0.7000; 2026-08-21:RED:body=-0.3900:wick=0.7800; 2026-08-24:GREEN:body=+0.9500:wick=0.3800; 2026-08-25:GREEN:body=+0.9000:wick=0.4900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=322.84 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.71 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1828.76 | **NEUTRAL** |
| `B04_income` | 5.78 | **GOOD** |
| `B05_profit_margin` | 0.32 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.38 vs prior_export=40.38 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.86 | **NEUTRAL** |
| `B10_insider_transactions` | 0.54 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.54 vs prior=0.54 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.05 | **GOOD** |
| `B13_short_float` | 5.83 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=322.84 (this export) | prior_export=322.84 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.71 (this export) | prior_export=1.71 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+16**  ·  Waste Management
price=220.9600067138672  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.45 on 2026-08-25; prev RSI=63.25 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.25@2026-08-24 → 58.45@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.25@2026-08-24 → 58.45@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.25@2026-08-24 → 58.45@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.429 (G=1.5300 R=0.6300); 2026-08-24:GREEN:O=221.5500,C=223.0800,body=+1.5300,vol=1362300.0; 2026-08-25:RED:O=221.5900,C=220.9600,body=-0.6300,vol=1343925.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.014 (Gvol=1362300 Rvol=1343925); 2026-08-24:GREEN:O=221.5500,C=223.0800,body=+1.5300,vol=1362300.0; 2026-08-25:RED:O=221.5900,C=220.9600,body=-0.6300,vol=1343925.0 | **GOOD** |
| `A07_rvol` | RVOL=0.936 on 2026-08-25: today_vol=1343925 / avg20=1436150 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.613 on 2026-08-25 (price=220.9600, mid=215.3345, upper=224.5066, lower=206.1624; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=220.9600 vs SMA50=214.9799 dist=+2.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=215.3345 SMA50=214.9799 SMA80=211.3870 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-02@197.0447; S2[2026-06-23→2026-07-24] low=2026-06-23@205.2907; S3[2026-07-27→2026-08-25] low=2026-08-05@204.8000 | lows=[197.04474803654423, 205.29067036217452, 204.8000030517578] span=4.18% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5839700880585194 wick_frac=0.4160299119414807 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.17646731975859534 wick_frac=0.8235326802414047 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.428609489669872 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+3.2700:wick=5.2300; 2026-08-20:RED:body=-1.4000:wick=1.3400; 2026-08-21:GREEN:body=+1.0100:wick=1.3500; 2026-08-24:GREEN:body=+1.5300:wick=1.0900; 2026-08-25:RED:body=-0.6300:wick=2.9400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.93 | **GOOD** |
| `B10_insider_transactions` | 2.75 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.3799999999999999 (now=2.75 vs prior=2.37 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | -0.24 | **BAD** |
| `B13_short_float` | 1.99 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RHI  ·  score **+16**  ·  Staffing & Employment Services
price=44.900001525878906  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.05 on 2026-08-25; prev RSI=66.11 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.11@2026-08-24 → 65.05@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.11@2026-08-24 → 65.05@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.11@2026-08-24 → 65.05@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=3.455 (G=1.1400 R=0.3300); 2026-08-24:RED:O=45.4500,C=45.1200,body=-0.3300,vol=3348900.0; 2026-08-25:GREEN:O=43.7600,C=44.9000,body=+1.1400,vol=3939741.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.176 (Gvol=3939741 Rvol=3348900); 2026-08-24:RED:O=45.4500,C=45.1200,body=-0.3300,vol=3348900.0; 2026-08-25:GREEN:O=43.7600,C=44.9000,body=+1.1400,vol=3939741.0 | **GOOD** |
| `A07_rvol` | RVOL=1.862 on 2026-08-25: today_vol=3939741 / avg20=2116395 (avg window 2026-07-27→2026-08-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.645 on 2026-08-25 (price=44.9000, mid=42.1170, upper=46.4349, lower=37.7991; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=44.9000 vs SMA50=37.1730 dist=+20.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=42.1170 SMA50=37.1730 SMA80=33.4676 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@24.9563; S2[2026-06-23→2026-07-24] low=2026-06-24@28.6000; S3[2026-07-27→2026-08-25] low=2026-07-27@36.4000 | lows=[24.956251006712353, 28.600000381469727, 36.400001525878906] span=45.86% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8976399948336402 wick_frac=0.10236000516635979 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.1964296256601923 wick_frac=0.8035703743398077 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.454535996670828 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-0.2200:wick=1.8200; 2026-08-20:GREEN:body=+0.6800:wick=0.7500; 2026-08-21:GREEN:body=+0.8700:wick=1.1700; 2026-08-24:RED:body=-0.3300:wick=1.3500; 2026-08-25:GREEN:body=+1.1400:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.93 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5293.4 | **NEUTRAL** |
| `B04_income` | 114.78 | **GOOD** |
| `B05_profit_margin` | 2.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 34.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=34.78 vs prior_export=34.78 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.73 | **BAD** |
| `B13_short_float` | 23.76 | **GOOD** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.93 (this export) | prior_export=0.93 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FDX  ·  score **+16**  ·  Integrated Freight & Logistics
price=333.7099914550781  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.88 on 2026-08-25; prev RSI=61.29 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.29@2026-08-24 → 60.88@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.29@2026-08-24 → 60.88@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.29@2026-08-24 → 60.88@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.776 (G=9.0500 R=3.2600); 2026-08-24:GREEN:O=325.0000,C=334.0500,body=+9.0500,vol=1489800.0; 2026-08-25:RED:O=336.9700,C=333.7100,body=-3.2600,vol=1096805.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.358 (Gvol=1489800 Rvol=1096805); 2026-08-24:GREEN:O=325.0000,C=334.0500,body=+9.0500,vol=1489800.0; 2026-08-25:RED:O=336.9700,C=333.7100,body=-3.2600,vol=1096805.0 | **GOOD** |
| `A07_rvol` | RVOL=0.716 on 2026-08-25: today_vol=1096805 / avg20=1531300 (avg window 2026-07-28→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.558 on 2026-08-25 (price=333.7100, mid=322.3850, upper=342.6859, lower=302.0841; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=333.7100 vs SMA50=319.7633 dist=+4.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=322.3850 SMA50=319.7633 SMA80=317.9178 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-25 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-24@306.0500; S2[2026-06-25→2026-07-27] low=2026-07-09@304.0600; S3[2026-07-28→2026-08-25] low=2026-07-28@303.0000 | lows=[306.04998779296875, 304.05999755859375, 303.0] span=1.01% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8987071708680078 wick_frac=0.10129282913199222 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.4190293882290179 wick_frac=0.5809706117709821 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.77606155920018 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-2.5700:wick=8.4100; 2026-08-20:RED:body=-0.1300:wick=7.7200; 2026-08-21:RED:body=-3.2200:wick=2.7500; 2026-08-24:GREEN:body=+9.0500:wick=1.0200; 2026-08-25:RED:body=-3.2600:wick=4.5199 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.97 (current export asof; earnings_date=06/24/2026) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.93 (current export; earnings_date=06/24/2026) | **GOOD** |
| `B03_sales` | 94720.0 | **NEUTRAL** |
| `B04_income` | 4428.0 | **GOOD** |
| `B05_profit_margin` | 4.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 368.13 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=368.13 vs prior_export=368.13 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 2.3 | **NEUTRAL** |
| `B14_earnings_date` | 06/24/2026 | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.93 (this export) | prior_export=3.93 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMH  ·  score **+16**  ·  REIT - Residential
price=34.560001373291016  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.26 on 2026-08-25; prev RSI=60.98 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.98@2026-08-24 → 57.26@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.98@2026-08-24 → 57.26@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.98@2026-08-24 → 57.26@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.600 (G=0.3200 R=0.2000); 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0; 2026-08-25:RED:O=34.7600,C=34.5600,body=-0.2000,vol=2041679.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=0.629 (Gvol=1283900 Rvol=2041679); 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0; 2026-08-25:RED:O=34.7600,C=34.5600,body=-0.2000,vol=2041679.0 | **BAD** |
| `A07_rvol` | RVOL=0.961 on 2026-08-25: today_vol=2041679 / avg20=2124830 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.457 on 2026-08-25 (price=34.5600, mid=34.1690, upper=35.0246, lower=33.3134; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=34.5600 vs SMA50=33.6103 dist=+2.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=34.1690 SMA50=33.6103 SMA80=32.9142 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-22@31.4700; S2[2026-06-23→2026-07-24] low=2026-06-23@31.8300; S3[2026-07-27→2026-08-25] low=2026-07-30@32.8600 | lows=[31.469999313354492, 31.829999923706055, 32.86000061035156] span=4.42% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.7111092273131862 wick_frac=0.2888907726868139 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.47618096111751934 wick_frac=0.5238190388824806 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6000228885328451 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.2900:wick=0.2400; 2026-08-20:GREEN:body=+0.1000:wick=0.2300; 2026-08-21:RED:body=-0.1200:wick=0.2800; 2026-08-24:GREEN:body=+0.3200:wick=0.1300; 2026-08-25:RED:body=-0.2000:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.77 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1891.62 | **NEUTRAL** |
| `B04_income` | 463.49 | **GOOD** |
| `B05_profit_margin` | 24.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.16 vs prior_export=37.16 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.41 (this export) | prior_export=72.41 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.77 (this export) | prior_export=0.77 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYAN  ·  score **+16**  ·  Insurance - Specialty
price=43.93000030517578  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.04 on 2026-08-25; prev RSI=60.40 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.40@2026-08-24 → 58.04@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.40@2026-08-24 → 58.04@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.40@2026-08-24 → 58.04@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=19.500 (G=0.7800 R=0.0400); 2026-08-24:GREEN:O=43.5600,C=44.3400,body=+0.7800,vol=1453600.0; 2026-08-25:RED:O=43.9700,C=43.9300,body=-0.0400,vol=898098.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.619 (Gvol=1453600 Rvol=898098); 2026-08-24:GREEN:O=43.5600,C=44.3400,body=+0.7800,vol=1453600.0; 2026-08-25:RED:O=43.9700,C=43.9300,body=-0.0400,vol=898098.0 | **GOOD** |
| `A07_rvol` | RVOL=0.442 on 2026-08-25: today_vol=898098 / avg20=2031680 (avg window 2026-07-28→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.253 on 2026-08-25 (price=43.9300, mid=43.2956, upper=45.8049, lower=40.7863; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=43.9300 vs SMA50=40.7243 dist=+7.87% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=43.2956 SMA50=40.7243 SMA80=37.4634 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@30.5761; S2[2026-06-24→2026-07-27] low=2026-06-24@33.8261; S3[2026-07-28→2026-08-25] low=2026-08-11@41.0300 | lows=[30.57607472607481, 33.826090954123764, 41.029998779296875] span=34.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5397916567669312 wick_frac=0.4602083432330688 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.027397904527996238 wick_frac=0.9726020954720037 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.499523173755485 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.8500:wick=0.7300; 2026-08-20:GREEN:body=+0.9500:wick=0.3000; 2026-08-21:GREEN:body=+0.2900:wick=0.8800; 2026-08-24:GREEN:body=+0.7800:wick=0.6650; 2026-08-25:RED:body=-0.0400:wick=1.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.79 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.89 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3224.94 | **NEUTRAL** |
| `B04_income` | 99.03 | **GOOD** |
| `B05_profit_margin` | 3.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.91 vs prior_export=49.91 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.27 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=-0.08000000000000002 (now=0.43 vs prior=0.51 on finviz_2026-08-24) | **BAD** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 12.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.79 (this export) | prior_export=23.79 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.89 (this export) | prior_export=4.89 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASH  ·  score **+16**  ·  Specialty Chemicals
price=73.76000213623047  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.88 on 2026-08-25; prev RSI=59.71 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.71@2026-08-24 → 59.88@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.71@2026-08-24 → 59.88@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.71@2026-08-24 → 59.88@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=4.529 (G=0.7700 R=0.1700); 2026-08-24:GREEN:O=72.9400,C=73.7100,body=+0.7700,vol=492900.0; 2026-08-25:RED:O=73.9300,C=73.7600,body=-0.1700,vol=460708.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.070 (Gvol=492900 Rvol=460708); 2026-08-24:GREEN:O=72.9400,C=73.7100,body=+0.7700,vol=492900.0; 2026-08-25:RED:O=73.9300,C=73.7600,body=-0.1700,vol=460708.0 | **GOOD** |
| `A07_rvol` | RVOL=0.572 on 2026-08-25: today_vol=460708 / avg20=804790 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.175 on 2026-08-25 (price=73.7600, mid=73.1605, upper=76.5796, lower=69.7414; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=73.7600 vs SMA50=68.8286 dist=+7.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=73.1605 SMA50=68.8286 SMA80=63.8410 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@54.1841; S2[2026-06-23→2026-07-24] low=2026-06-29@62.1100; S3[2026-07-27→2026-08-25] low=2026-07-27@65.6300 | lows=[54.18408440102093, 62.11000061035156, 65.62999725341797] span=21.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5746226592346715 wick_frac=0.4253773407653285 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.10862485862485863 wick_frac=0.8913751413751414 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.529440804236604 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:RED:body=-2.0500:wick=0.4600; 2026-08-20:GREEN:body=+1.2400:wick=0.8900; 2026-08-21:RED:body=-0.7300:wick=0.2400; 2026-08-24:GREEN:body=+0.7700:wick=0.5700; 2026-08-25:RED:body=-0.1700:wick=1.3950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.55 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1843.0 | **NEUTRAL** |
| `B04_income` | 52.0 | **GOOD** |
| `B05_profit_margin` | 2.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=80.18 vs prior_export=80.18 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.72 | **GOOD** |
| `B13_short_float` | 9.91 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.55 (this export) | prior_export=3.55 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DLR  ·  score **+16**  ·  REIT - Specialty
price=192.32000732421875  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.61 on 2026-08-25; prev RSI=46.69 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.69@2026-08-24 → 51.61@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.69@2026-08-24 → 51.61@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.69@2026-08-24 → 51.61@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.531 (G=3.0000 R=1.9600); 2026-08-24:RED:O=190.5100,C=188.5500,body=-1.9600,vol=1739800.0; 2026-08-25:GREEN:O=189.3200,C=192.3200,body=+3.0000,vol=1907170.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.096 (Gvol=1907170 Rvol=1739800); 2026-08-24:RED:O=190.5100,C=188.5500,body=-1.9600,vol=1739800.0; 2026-08-25:GREEN:O=189.3200,C=192.3200,body=+3.0000,vol=1907170.0 | **GOOD** |
| `A07_rvol` | RVOL=0.919 on 2026-08-25: today_vol=1907170 / avg20=2074970 (avg window 2026-07-28→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.138 on 2026-08-25 (price=192.3200, mid=193.2245, upper=199.7832, lower=186.6658; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=192.3200 vs SMA50=187.3614 dist=+2.65% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=193.22_50=187.36_80=188.29 on 2026-08-25: SMA20=193.2245 SMA50=187.3614 SMA80=188.2921 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-25 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-11@177.2284; S2[2026-06-25→2026-07-27] low=2026-07-15@171.0300; S3[2026-07-28→2026-08-25] low=2026-08-24@184.2200 | lows=[177.22836105054336, 171.02999877929688, 184.22000122070312] span=7.71% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8620700238966961 wick_frac=0.13792997610330388 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.3116046974802351 wick_frac=0.6883953025197649 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5306189178668743 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-4.8300:wick=3.9000; 2026-08-20:GREEN:body=+1.9500:wick=1.9200; 2026-08-21:RED:body=-4.9100:wick=1.4200; 2026-08-24:RED:body=-1.9600:wick=4.3300; 2026-08-25:GREEN:body=+3.0000:wick=0.4800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=150.57 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.08 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6848.84 | **NEUTRAL** |
| `B04_income` | 758.3 | **GOOD** |
| `B05_profit_margin` | 11.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 223.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=223.55 vs prior_export=223.55 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.05 | **GOOD** |
| `B13_short_float` | 2.29 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=150.57 (this export) | prior_export=150.57 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.08 (this export) | prior_export=16.08 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CM  ·  score **+16**  ·  Banks - Diversified
price=118.33000183105469  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.31 on 2026-08-25; prev RSI=41.07 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.07@2026-08-24 → 50.31@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.07@2026-08-24 → 50.31@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.07@2026-08-24 → 50.31@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=4.163 (G=2.0400 R=0.4900); 2026-08-24:RED:O=115.4200,C=114.9300,body=-0.4900,vol=792100.0; 2026-08-25:GREEN:O=116.2900,C=118.3300,body=+2.0400,vol=802409.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.013 (Gvol=802409 Rvol=792100); 2026-08-24:RED:O=115.4200,C=114.9300,body=-0.4900,vol=792100.0; 2026-08-25:GREEN:O=116.2900,C=118.3300,body=+2.0400,vol=802409.0 | **GOOD** |
| `A07_rvol` | RVOL=0.826 on 2026-08-25: today_vol=802409 / avg20=971470 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.149 on 2026-08-25 (price=118.3300, mid=119.0510, upper=123.8768, lower=114.2252; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=118.3300 vs SMA50=116.7249 dist=+1.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=119.0510 SMA50=116.7249 SMA80=114.2788 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-01@105.1584; S2[2026-06-23→2026-07-24] low=2026-06-23@111.1087; S3[2026-07-27→2026-08-25] low=2026-08-24@114.1800 | lows=[105.1583652580943, 111.10866186662082, 114.18000030517578] span=8.58% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6315785744148639 wick_frac=0.3684214255851362 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.2648639286053043 wick_frac=0.7351360713946957 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.163285325029194 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:RED:body=-4.4100:wick=0.7600; 2026-08-20:RED:body=-2.0100:wick=0.8700; 2026-08-21:RED:body=-0.2500:wick=1.5900; 2026-08-24:RED:body=-0.4900:wick=1.3600; 2026-08-25:GREEN:body=+2.0400:wick=1.1900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.66 (current export asof; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.24 (current export; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 44520.77 | **NEUTRAL** |
| `B04_income` | 6809.7 | **GOOD** |
| `B05_profit_margin` | 15.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 128.87 | **NEUTRAL** |
| `B08_target_price_delta` | delta=7.410000000000011 (now=128.87 vs prior_export=121.46 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.68 | **BAD** |
| `B13_short_float` | 1.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.24 (this export) | prior_export=0.24 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GDS  ·  score **+16**  ·  Information Technology Services
price=32.9900016784668  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.82 on 2026-08-25; prev RSI=44.99 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.99@2026-08-24 → 50.82@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.99@2026-08-24 → 50.82@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.99@2026-08-24 → 50.82@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.523 (G=0.9900 R=0.6500); 2026-08-24:RED:O=32.4000,C=31.7500,body=-0.6500,vol=1733400.0; 2026-08-25:GREEN:O=32.0000,C=32.9900,body=+0.9900,vol=979881.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=0.565 (Gvol=979881 Rvol=1733400); 2026-08-24:RED:O=32.4000,C=31.7500,body=-0.6500,vol=1733400.0; 2026-08-25:GREEN:O=32.0000,C=32.9900,body=+0.9900,vol=979881.0 | **BAD** |
| `A07_rvol` | RVOL=0.504 on 2026-08-25: today_vol=979881 / avg20=1945810 (avg window 2026-07-27→2026-08-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.117 on 2026-08-25 (price=32.9900, mid=32.6750, upper=35.3776, lower=29.9724; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=32.9900 vs SMA50=32.1512 dist=+2.61% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=32.68_50=32.15_80=34.82 on 2026-08-25: SMA20=32.6750 SMA50=32.1512 SMA80=34.8244 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-22@29.9900; S2[2026-06-24→2026-07-24] low=2026-06-29@28.4500; S3[2026-07-27→2026-08-25] low=2026-07-28@29.1200 | lows=[29.989999771118164, 28.450000762939453, 29.1200008392334] span=5.41% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8959279454688878 wick_frac=0.10407205453111218 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.3963420803807251 wick_frac=0.603657919619275 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5230759299036352 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-0.2500:wick=0.9550; 2026-08-20:RED:body=-0.8100:wick=0.4300; 2026-08-21:RED:body=-0.9100:wick=0.4750; 2026-08-24:RED:body=-0.6500:wick=0.9900; 2026-08-25:GREEN:body=+0.9900:wick=0.1150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=487.39 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.07 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1755.35 | **NEUTRAL** |
| `B04_income` | 528.66 | **GOOD** |
| `B05_profit_margin` | 30.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 52.15 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.14999999999999858 (now=52.15 vs prior_export=52.0 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.31 | **GOOD** |
| `B13_short_float` | 6.74 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=487.39 (this export) | prior_export=487.39 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.07 (this export) | prior_export=0.07 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TSLX  ·  score **+16**  ·  Asset Management
price=18.979999542236328  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.75 on 2026-08-25; prev RSI=61.87 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.87@2026-08-24 → 62.75@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.87@2026-08-24 → 62.75@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.87@2026-08-24 → 62.75@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.2700 R=0.0000); 2026-08-24:GREEN:O=18.7700,C=18.9100,body=+0.1400,vol=301200.0; 2026-08-25:GREEN:O=18.8500,C=18.9800,body=+0.1300,vol=854656.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1155856 Rvol=0); 2026-08-24:GREEN:O=18.7700,C=18.9100,body=+0.1400,vol=301200.0; 2026-08-25:GREEN:O=18.8500,C=18.9800,body=+0.1300,vol=854656.0 | **GOOD** |
| `A07_rvol` | RVOL=1.743 on 2026-08-25: today_vol=854656 / avg20=490400 (avg window 2026-07-27→2026-08-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.453 on 2026-08-25 (price=18.9800, mid=18.4785, upper=19.5851, lower=17.3719; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=18.9800 vs SMA50=17.5228 dist=+8.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=18.4785 SMA50=17.5228 SMA80=17.5010 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-22@16.2300; S2[2026-06-23→2026-07-24] low=2026-06-25@16.0400; S3[2026-07-27→2026-08-25] low=2026-07-27@16.9300 | lows=[16.229999542236328, 16.040000915527344, 16.93000030517578] span=5.55% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6136349843510226 wick_frac=0.3863650156489774 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.2000:wick=0.1700; 2026-08-20:GREEN:body=+0.0300:wick=0.3300; 2026-08-21:RED:body=-0.0400:wick=0.1500; 2026-08-24:GREEN:body=+0.1400:wick=0.0800; 2026-08-25:GREEN:body=+0.1300:wick=0.0900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.75 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.64 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 346.59 | **NEUTRAL** |
| `B04_income` | 89.04 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.77 vs prior_export=19.77 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 18.12 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=18.12 vs prior=18.12 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 6.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.75 (this export) | prior_export=1.75 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.64 (this export) | prior_export=2.64 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-25_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.