# A+B1 Feature Checklist — 2026-11-20

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,600** names
- Export: `finviz_2026-11-20.csv` · prior export for Δ: `2026-11-19`
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
| 1 | ARDT | +17 | 17 | 0 | 2026-11-19→2026-11-20 | Medical Care Facilities |
| 2 | LFST | +17 | 18 | 1 | 2026-11-19→2026-11-20 | Medical Care Facilities |
| 3 | GPN | +15 | 16 | 1 | 2026-11-19→2026-11-20 | Software - Infrastructure |
| 4 | RKT | +14 | 15 | 1 | 2026-11-19→2026-11-20 | Mortgage Finance |
| 5 | WMT | +14 | 16 | 2 | 2026-11-19→2026-11-20 | Discount Stores |
| 6 | AVPT | +14 | 16 | 2 | 2026-11-19→2026-11-20 | Software - Infrastructure |
| 7 | GME | +14 | 15 | 1 | 2026-11-19→2026-11-20 | Specialty Retail |
| 8 | LIN | +14 | 17 | 3 | 2026-11-19→2026-11-20 | Specialty Chemicals |
| 9 | RCI | +14 | 16 | 2 | 2026-11-19→2026-11-20 | Telecom Services |
| 10 | LNG | +13 | 15 | 2 | 2026-11-19→2026-11-20 | Oil & Gas Midstream |
| 11 | SJM | +13 | 15 | 2 | 2026-11-19→2026-11-20 | Packaged Foods |
| 12 | COST | +13 | 15 | 2 | 2026-11-19→2026-11-20 | Discount Stores |
| 13 | CCEP | +13 | 15 | 2 | 2026-11-19→2026-11-20 | Beverages - Non-Alcoholic |
| 14 | WTRG | +13 | 15 | 2 | 2026-11-19→2026-11-20 | Utilities - Regulated Water |
| 15 | EMBC | +12 | 14 | 2 | 2026-11-19→2026-11-20 | Medical Instruments & Supplies |

## Full checklist — top 15

### ARDT  ·  score **+17**  ·  Medical Care Facilities
price=15.079999923706055  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.11 on 2026-11-20; prev RSI=49.75 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.75@2026-11-19 → 53.11@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.75@2026-11-19 → 53.11@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.75@2026-11-19 → 53.11@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7600 R=0.0000); 2026-11-19:GREEN:O=14.3300,C=14.5000,body=+0.1700,vol=2193200.0; 2026-11-20:GREEN:O=14.4900,C=15.0800,body=+0.5900,vol=1540900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3734100 Rvol=0); 2026-11-19:GREEN:O=14.3300,C=14.5000,body=+0.1700,vol=2193200.0; 2026-11-20:GREEN:O=14.4900,C=15.0800,body=+0.5900,vol=1540900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.229 on 2026-11-20: today_vol=1540900 / avg20=1253779 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.022 on 2026-11-20 (price=15.0800, mid=14.9650, upper=20.3072, lower=9.6228; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=15.0800 vs SMA50=12.6622 dist=+19.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=14.9650 SMA50=12.6622 SMA80=11.7190 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-11-20 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-14@9.0100; S2[2026-08-17→2026-10-22] low=2026-08-21@10.3500; S3[2026-10-23→2026-11-20] low=2026-11-03@11.5500 | lows=[9.010000228881836, 10.350000381469727, 11.550000190734863] span=28.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.5133208189628808 wick_frac=0.48667918103711916 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:GREEN:body=+0.0500:wick=0.4500; 2026-11-17:RED:body=-0.3400:wick=0.3500; 2026-11-18:GREEN:body=+0.6500:wick=0.4200; 2026-11-19:GREEN:body=+0.1700:wick=0.4000; 2026-11-20:GREEN:body=+0.5900:wick=0.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=33.62 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.31 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6454.05 | **NEUTRAL** |
| `B04_income` | 146.22 | **GOOD** |
| `B05_profit_margin` | 2.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.77 vs prior_export=17.77 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.78 | **GOOD** |
| `B13_short_float` | 15.69 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.62 (this export) | prior_export=33.62 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.31 (this export) | prior_export=1.31 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LFST  ·  score **+17**  ·  Medical Care Facilities
price=13.819999694824219  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.66 on 2026-11-20; prev RSI=48.01 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.01@2026-11-19 → 53.66@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.01@2026-11-19 → 53.66@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.01@2026-11-19 → 53.66@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=1.525 (G=0.6100 R=0.4000); 2026-11-19:RED:O=13.7400,C=13.3400,body=-0.4000,vol=4922800.0; 2026-11-20:GREEN:O=13.2100,C=13.8200,body=+0.6100,vol=5622400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.142 (Gvol=5622400 Rvol=4922800); 2026-11-19:RED:O=13.7400,C=13.3400,body=-0.4000,vol=4922800.0; 2026-11-20:GREEN:O=13.2100,C=13.8200,body=+0.6100,vol=5622400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.797 on 2026-11-20: today_vol=5622400 / avg20=7052370 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.022 on 2026-11-20 (price=13.8200, mid=13.7983, upper=14.7962, lower=12.8003; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=13.8200 vs SMA50=12.8997 dist=+7.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=13.7983 SMA50=12.8997 SMA80=12.5941 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-23@11.7500; S2[2026-09-24→2026-10-22] low=2026-10-02@11.4700; S3[2026-10-23→2026-11-20] low=2026-10-23@12.4150 | lows=[11.75, 11.470000267028809, 12.414999961853027] span=8.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.9838705956004842 wick_frac=0.016129404399515778 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.40609112482294085 wick_frac=0.5939088751770591 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.525000596047016 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:GREEN:body=+0.3700:wick=0.1000; 2026-11-17:RED:body=-0.3100:wick=0.0200; 2026-11-18:RED:body=-0.3100:wick=0.3570; 2026-11-19:RED:body=-0.4000:wick=0.5850; 2026-11-20:GREEN:body=+0.6100:wick=0.0100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=62.6 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.62 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1676.89 | **NEUTRAL** |
| `B04_income` | 71.91 | **GOOD** |
| `B05_profit_margin` | 4.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.45 | **GOOD** |
| `B10_insider_transactions` | -37.7 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-37.7 vs prior=-37.7 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.6 | **GOOD** |
| `B13_short_float` | 5.3 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=62.6 (this export) | prior_export=62.6 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.62 (this export) | prior_export=5.62 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPN  ·  score **+15**  ·  Software - Infrastructure
price=91.83000183105469  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.65 on 2026-11-20; prev RSI=41.26 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.26@2026-11-19 → 52.65@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.26@2026-11-19 → 52.65@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.26@2026-11-19 → 52.65@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=3.990 (G=7.8200 R=1.9600); 2026-11-19:RED:O=85.4800,C=83.5200,body=-1.9600,vol=2581100.0; 2026-11-20:GREEN:O=84.0100,C=91.8300,body=+7.8200,vol=9535900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=3.695 (Gvol=9535900 Rvol=2581100); 2026-11-19:RED:O=85.4800,C=83.5200,body=-1.9600,vol=2581100.0; 2026-11-20:GREEN:O=84.0100,C=91.8300,body=+7.8200,vol=9535900.0 | **GOOD** |
| `A07_rvol` | RVOL=2.372 on 2026-11-20: today_vol=9535900 / avg20=4020673 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.020 on 2026-11-20 (price=91.8300, mid=91.5645, upper=105.0478, lower=78.0812; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=91.8300 vs SMA50=87.2580 dist=+5.24% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=91.56_50=87.26_80=88.32 on 2026-11-20: SMA20=91.5645 SMA50=87.2580 SMA80=88.3151 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-20 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-18@84.1400; S2[2026-09-21→2026-10-22] low=2026-10-02@77.9300; S3[2026-10-23→2026-11-20] low=2026-10-28@80.8500 | lows=[84.13999938964844, 77.93000030517578, 80.8499984741211] span=7.97% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.6425637527850115 wick_frac=0.35743624721498846 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.7000016348684748 wick_frac=0.29999836513152517 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.98978209589649 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:RED:body=-2.0800:wick=1.7900; 2026-11-17:RED:body=-3.9900:wick=0.4400; 2026-11-18:RED:body=-0.1700:wick=2.2300; 2026-11-19:RED:body=-1.9600:wick=0.8400; 2026-11-20:GREEN:body=+7.8200:wick=4.3500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.09 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.01 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11493.52 | **NEUTRAL** |
| `B04_income` | 197.89 | **GOOD** |
| `B05_profit_margin` | 1.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=109.31 vs prior_export=109.31 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.81 | **GOOD** |
| `B13_short_float` | 6.52 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.09 (this export) | prior_export=0.09 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.01 (this export) | prior_export=0.01 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RKT  ·  score **+14**  ·  Mortgage Finance
price=13.75  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.41 on 2026-11-20; prev RSI=49.80 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.80@2026-11-19 → 55.41@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.80@2026-11-19 → 55.41@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.80@2026-11-19 → 55.41@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=3.389 (G=0.6100 R=0.1800); 2026-11-19:RED:O=13.3700,C=13.1900,body=-0.1800,vol=29590400.0; 2026-11-20:GREEN:O=13.1400,C=13.7500,body=+0.6100,vol=51192300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.730 (Gvol=51192300 Rvol=29590400); 2026-11-19:RED:O=13.3700,C=13.1900,body=-0.1800,vol=29590400.0; 2026-11-20:GREEN:O=13.1400,C=13.7500,body=+0.6100,vol=51192300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.469 on 2026-11-20: today_vol=51192300 / avg20=34848403 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.086 on 2026-11-20 (price=13.7500, mid=13.5515, upper=15.8497, lower=11.2533; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=13.7500 vs SMA50=12.4780 dist=+10.19% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=13.55_50=12.48_80=13.01 on 2026-11-20: SMA20=13.5515 SMA50=12.4780 SMA80=13.0077 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-23@12.1100; S2[2026-09-24→2026-10-22] low=2026-10-14@10.1800; S3[2026-10-23→2026-11-20] low=2026-10-28@11.3550 | lows=[12.109999656677246, 10.180000305175781, 11.354999542236328] span=18.96% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.8133328755696615 wick_frac=0.18666712443033853 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.4615399663523612 wick_frac=0.5384600336476388 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3888812359598184 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:GREEN:body=+0.1000:wick=0.2600; 2026-11-17:RED:body=-0.3700:wick=0.1350; 2026-11-18:GREEN:body=+0.3000:wick=0.3210; 2026-11-19:RED:body=-0.1800:wick=0.2100; 2026-11-20:GREEN:body=+0.6100:wick=0.1400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.78 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=27.26 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10278.0 | **NEUTRAL** |
| `B04_income` | 1304.0 | **GOOD** |
| `B05_profit_margin` | 12.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.67 vs prior_export=14.67 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | -0.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.07 vs prior=-0.07 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 8.54 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.78 (this export) | prior_export=10.78 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=27.26 (this export) | prior_export=27.26 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMT  ·  score **+14**  ·  Discount Stores
price=127.94999694824219  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.99 on 2026-11-20; prev RSI=59.82 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.82@2026-11-19 → 63.99@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.82@2026-11-19 → 63.99@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.82@2026-11-19 → 63.99@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.0900 R=0.0000); 2026-11-19:GREEN:O=123.6400,C=126.0400,body=+2.4000,vol=32289300.0; 2026-11-20:GREEN:O=127.2600,C=127.9500,body=+0.6900,vol=42631700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=74921000 Rvol=0); 2026-11-19:GREEN:O=123.6400,C=126.0400,body=+2.4000,vol=32289300.0; 2026-11-20:GREEN:O=127.2600,C=127.9500,body=+0.6900,vol=42631700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.921 on 2026-11-20: today_vol=42631700 / avg20=22192664 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.420 on 2026-11-20 (price=127.9500, mid=125.3270, upper=131.5712, lower=119.0828; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=127.9500 vs SMA50=116.6452 dist=+9.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=125.3270 SMA50=116.6452 SMA80=113.8072 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-08-27@102.2700; S2[2026-09-24→2026-10-22] low=2026-10-05@103.3900; S3[2026-10-23→2026-11-20] low=2026-10-28@118.1118 | lows=[102.2699966430664, 103.38999938964844, 118.11180114746094] span=15.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.42937965063725003 wick_frac=0.57062034936275 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:RED:body=-1.4200:wick=0.5500; 2026-11-17:GREEN:body=+0.0200:wick=1.8400; 2026-11-18:GREEN:body=+0.9200:wick=1.7800; 2026-11-19:GREEN:body=+2.4000:wick=1.2800; 2026-11-20:GREEN:body=+0.6900:wick=2.6500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.85 (current export asof; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.78 (current export; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 745179.0 | **NEUTRAL** |
| `B04_income` | 20878.0 | **GOOD** |
| `B05_profit_margin` | 2.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 135.02 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.460000000000008 (now=135.02 vs prior_export=133.56 on finviz_2026-11-19) | **GOOD** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | -0.26 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.26 vs prior=-0.26 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.24 | **GOOD** |
| `B13_short_float` | 1.65 | **NEUTRAL** |
| `B14_earnings_date` | 11/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.85 (this export) | prior_export=9.19 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.78 (this export) | prior_export=0.69 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+14**  ·  Software - Infrastructure
price=16.489999771118164  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.00 on 2026-11-20; prev RSI=59.36 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.36@2026-11-19 → 54.00@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.36@2026-11-19 → 54.00@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.36@2026-11-19 → 54.00@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=1.538 (G=0.4000 R=0.2600); 2026-11-19:GREEN:O=16.5400,C=16.9400,body=+0.4000,vol=2945000.0; 2026-11-20:RED:O=16.7500,C=16.4900,body=-0.2600,vol=3163700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=0.931 (Gvol=2945000 Rvol=3163700); 2026-11-19:GREEN:O=16.5400,C=16.9400,body=+0.4000,vol=2945000.0; 2026-11-20:RED:O=16.7500,C=16.4900,body=-0.2600,vol=3163700.0 | **BAD** |
| `A07_rvol` | RVOL=0.766 on 2026-11-20: today_vol=3163700 / avg20=4129296 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.239 on 2026-11-20 (price=16.4900, mid=16.6590, upper=17.3668, lower=15.9512; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=16.4900 vs SMA50=15.1775 dist=+8.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=16.6590 SMA50=15.1775 SMA80=14.4677 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-11@12.6100; S2[2026-09-24→2026-10-22] low=2026-09-28@12.6400; S3[2026-10-23→2026-11-20] low=2026-11-17@15.6200 | lows=[12.609999656677246, 12.640000343322754, 15.619999885559082] span=23.87% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.6956505874625994 wick_frac=0.30434941253740055 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.31137786102608617 wick_frac=0.6886221389739139 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.538458716942376 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:RED:body=-0.1400:wick=0.1580; 2026-11-17:RED:body=-0.4200:wick=0.3100; 2026-11-18:GREEN:body=+0.5500:wick=0.2500; 2026-11-19:GREEN:body=+0.4000:wick=0.1750; 2026-11-20:RED:body=-0.2600:wick=0.5750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=35.93 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.32 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 487.53 | **NEUTRAL** |
| `B04_income` | 76.4 | **GOOD** |
| `B05_profit_margin` | 15.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.22 vs prior_export=19.22 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | -0.4 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.4 vs prior=-0.4 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.44 | **GOOD** |
| `B13_short_float` | 8.22 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.93 (this export) | prior_export=35.93 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GME  ·  score **+14**  ·  Specialty Retail
price=26.65999984741211  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.53 on 2026-11-20; prev RSI=64.30 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.30@2026-11-19 → 61.53@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.30@2026-11-19 → 61.53@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.30@2026-11-19 → 61.53@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=2.263 (G=0.4300 R=0.1900); 2026-11-19:GREEN:O=26.5000,C=26.9300,body=+0.4300,vol=18265200.0; 2026-11-20:RED:O=26.8500,C=26.6600,body=-0.1900,vol=9642000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.894 (Gvol=18265200 Rvol=9642000); 2026-11-19:GREEN:O=26.5000,C=26.9300,body=+0.4300,vol=18265200.0; 2026-11-20:RED:O=26.8500,C=26.6600,body=-0.1900,vol=9642000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.419 on 2026-11-20: today_vol=9642000 / avg20=6793006 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.762 on 2026-11-20 (price=26.6600, mid=25.0415, upper=27.1647, lower=22.9183; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=26.6600 vs SMA50=24.8824 dist=+7.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=25.0415 SMA50=24.8824 SMA80=22.6326 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-08-27@17.8000; S2[2026-09-24→2026-10-22] low=2026-09-25@23.0550; S3[2026-10-23→2026-11-20] low=2026-10-30@23.0703 | lows=[17.799999237060547, 23.05500030517578, 23.07029914855957] span=29.61% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.3185186545707831 wick_frac=0.6814813454292169 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.15447179685985657 wick_frac=0.8455282031401434 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.2631531395874114 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:GREEN:body=+0.0200:wick=0.8100; 2026-11-17:RED:body=-0.2900:wick=0.5200; 2026-11-18:GREEN:body=+1.2800:wick=0.1200; 2026-11-19:GREEN:body=+0.4300:wick=0.9200; 2026-11-20:RED:body=-0.1900:wick=1.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.0 (current export asof; earnings_date=9/8/2026 8:30:00 AM) | **NEUTRAL** |
| `B02_revenue_surprise` | Revenue surprise=4.41 (current export; earnings_date=9/8/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3550.8 | **NEUTRAL** |
| `B04_income` | 893.3 | **GOOD** |
| `B05_profit_margin` | 25.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.0 vs prior_export=33.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 5.31 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.31 vs prior=5.31 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.22 | **GOOD** |
| `B13_short_float` | 10.06 | **NEUTRAL** |
| `B14_earnings_date` | 9/8/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.0 (this export) | prior_export=0.0 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **NEUTRAL** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.41 (this export) | prior_export=4.41 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LIN  ·  score **+14**  ·  Specialty Chemicals
price=502.8299865722656  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=44.05 on 2026-11-20; prev RSI=49.78 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.78@2026-11-19 → 44.05@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.78@2026-11-19 → 44.05@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.78@2026-11-19 → 44.05@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=0.000 (G=0.0000 R=11.3500); 2026-11-19:RED:O=512.6400,C=510.2100,body=-2.4300,vol=2472800.0; 2026-11-20:RED:O=511.7500,C=502.8300,body=-8.9200,vol=3511200.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=0.000 (Gvol=0 Rvol=5984000); 2026-11-19:RED:O=512.6400,C=510.2100,body=-2.4300,vol=2472800.0; 2026-11-20:RED:O=511.7500,C=502.8300,body=-8.9200,vol=3511200.0 | **BAD** |
| `A07_rvol` | RVOL=1.667 on 2026-11-20: today_vol=3511200 / avg20=2106636 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.859 on 2026-11-20 (price=502.8300, mid=515.7615, upper=530.8171, lower=500.7058; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=True on 2026-11-20: price=502.8300 vs SMA50=494.6672 dist=+1.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=515.7615 SMA50=494.6672 SMA80=490.0401 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-18@454.1900; S2[2026-09-24→2026-10-22] low=2026-10-01@462.3700; S3[2026-10-23→2026-11-20] low=2026-10-29@498.3950 | lows=[454.19000244140625, 462.3699951171875, 498.3949890136719] span=9.73% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.46019224555453797 wick_frac=0.539807754445462 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=0.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:RED:body=-5.2599:wick=2.7300; 2026-11-17:RED:body=-4.3600:wick=6.6000; 2026-11-18:GREEN:body=+0.8700:wick=4.4001; 2026-11-19:RED:body=-2.4300:wick=4.1600; 2026-11-20:RED:body=-8.9200:wick=7.2500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.59 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.64 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 36238.0 | **NEUTRAL** |
| `B04_income` | 7007.0 | **GOOD** |
| `B05_profit_margin` | 19.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 554.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.1099999999999 (now=554.3 vs prior_export=550.19 on finviz_2026-11-19) | **GOOD** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | -3.03 | **BAD** |
| `B11_insider_tx_delta` | delta=0.16000000000000014 (now=-3.03 vs prior=-3.19 on finviz_2026-11-19) | **GOOD** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.59 (this export) | prior_export=1.59 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.64 (this export) | prior_export=2.64 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RCI  ·  score **+14**  ·  Telecom Services
price=35.45000076293945  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.85 on 2026-11-20; prev RSI=57.38 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.38@2026-11-19 → 63.85@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.38@2026-11-19 → 63.85@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.38@2026-11-19 → 63.85@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=9.100 (G=0.9100 R=0.1000); 2026-11-19:RED:O=34.3700,C=34.2700,body=-0.1000,vol=1026600.0; 2026-11-20:GREEN:O=34.5400,C=35.4500,body=+0.9100,vol=2247600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=2.189 (Gvol=2247600 Rvol=1026600); 2026-11-19:RED:O=34.3700,C=34.2700,body=-0.1000,vol=1026600.0; 2026-11-20:GREEN:O=34.5400,C=35.4500,body=+0.9100,vol=2247600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.637 on 2026-11-20: today_vol=2247600 / avg20=1372686 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.912 on 2026-11-20 (price=35.4500, mid=33.4000, upper=35.6469, lower=31.1531; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-20: price=35.4500 vs SMA50=33.0134 dist=+7.38% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=33.40_50=33.01_80=34.04 on 2026-11-20: SMA20=33.4000 SMA50=33.0134 SMA80=34.0376 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-20 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-18@34.3600; S2[2026-09-21→2026-10-22] low=2026-10-09@29.2550; S3[2026-10-23→2026-11-20] low=2026-10-29@30.9600 | lows=[34.36000061035156, 29.2549991607666, 30.959999084472656] span=17.45% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.8348621284607873 wick_frac=0.16513787153921264 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.15151287460624802 wick_frac=0.848487125393752 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.10013733119707 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:GREEN:body=+0.9500:wick=0.4200; 2026-11-17:GREEN:body=+0.0400:wick=0.5100; 2026-11-18:GREEN:body=+0.1400:wick=0.4700; 2026-11-19:RED:body=-0.1000:wick=0.5600; 2026-11-20:GREEN:body=+0.9100:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.3 (current export asof; earnings_date=10/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.55 (current export; earnings_date=10/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16366.86 | **NEUTRAL** |
| `B04_income` | 643.36 | **GOOD** |
| `B05_profit_margin` | 3.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.49 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.01999999999999602 (now=41.49 vs prior_export=41.51 on finviz_2026-11-19) | **BAD** |
| `B09_analyst_recom` | 1.85 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.53 | **GOOD** |
| `B13_short_float` | 5.49 | **NEUTRAL** |
| `B14_earnings_date` | 10/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.3 (this export) | prior_export=7.3 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.55 (this export) | prior_export=2.55 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNG  ·  score **+13**  ·  Oil & Gas Midstream
price=294.8399963378906  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.63 on 2026-11-20; prev RSI=49.99 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.99@2026-11-19 → 49.63@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.99@2026-11-19 → 49.63@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.99@2026-11-19 → 49.63@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=0.293 (G=1.0000 R=3.4100); 2026-11-19:RED:O=298.7800,C=295.3700,body=-3.4100,vol=1363100.0; 2026-11-20:GREEN:O=293.8400,C=294.8400,body=+1.0000,vol=1947200.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.429 (Gvol=1947200 Rvol=1363100); 2026-11-19:RED:O=298.7800,C=295.3700,body=-3.4100,vol=1363100.0; 2026-11-20:GREEN:O=293.8400,C=294.8400,body=+1.0000,vol=1947200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.820 on 2026-11-20: today_vol=1947200 / avg20=2374922 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.233 on 2026-11-20 (price=294.8400, mid=300.7395, upper=326.0696, lower=275.4094; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=294.8400 vs SMA50=286.3332 dist=+2.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=300.7395 SMA50=286.3332 SMA80=281.9674 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-17@264.3600; S2[2026-09-24→2026-10-22] low=2026-10-02@259.5000; S3[2026-10-23→2026-11-20] low=2026-11-09@273.5600 | lows=[264.3599853515625, 259.5, 273.55999755859375] span=5.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.16366651349569455 wick_frac=0.8363334865043054 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.487838846709656 wick_frac=0.512161153290344 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=0.2932548170289693 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:RED:body=-3.9000:wick=1.1000; 2026-11-17:RED:body=-3.2300:wick=3.1500; 2026-11-18:RED:body=-5.1400:wick=2.3700; 2026-11-19:RED:body=-3.4100:wick=3.5800; 2026-11-20:GREEN:body=+1.0000:wick=5.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.62 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.07 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23923.0 | **NEUTRAL** |
| `B04_income` | 3031.0 | **GOOD** |
| `B05_profit_margin` | 12.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 346.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=5.0 (now=346.05 vs prior_export=341.05 on finviz_2026-11-19) | **GOOD** |
| `B09_analyst_recom` | 1.48 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.58 | **GOOD** |
| `B13_short_float` | 2.27 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.62 (this export) | prior_export=30.62 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.07 (this export) | prior_export=15.07 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SJM  ·  score **+13**  ·  Packaged Foods
price=133.67999267578125  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.19 on 2026-11-20; prev RSI=52.85 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.85@2026-11-19 → 57.19@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.85@2026-11-19 → 57.19@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.85@2026-11-19 → 57.19@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=2.524 (G=0.5300 R=0.2100); 2026-11-19:RED:O=131.5000,C=131.2900,body=-0.2100,vol=1256700.0; 2026-11-20:GREEN:O=133.1500,C=133.6800,body=+0.5300,vol=1315300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.047 (Gvol=1315300 Rvol=1256700); 2026-11-19:RED:O=131.5000,C=131.2900,body=-0.2100,vol=1256700.0; 2026-11-20:GREEN:O=133.1500,C=133.6800,body=+0.5300,vol=1315300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.109 on 2026-11-20: today_vol=1315300 / avg20=1185871 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.432 on 2026-11-20 (price=133.6800, mid=131.2615, upper=136.8576, lower=125.6654; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=133.6800 vs SMA50=125.8120 dist=+6.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=131.2615 SMA50=125.8120 SMA80=124.8067 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-21@119.3700; S2[2026-09-24→2026-10-22] low=2026-10-01@115.1100; S3[2026-10-23→2026-11-20] low=2026-10-28@124.6800 | lows=[119.37000274658203, 115.11000061035156, 124.68000030517578] span=8.31% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.17376991770267905 wick_frac=0.826230082297321 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.0864227764800442 wick_frac=0.9135772235199557 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.523723025503161 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:GREEN:body=+0.9200:wick=1.4900; 2026-11-17:RED:body=-1.0300:wick=1.2200; 2026-11-18:GREEN:body=+1.8700:wick=1.9400; 2026-11-19:RED:body=-0.2100:wick=2.2200; 2026-11-20:GREEN:body=+0.5300:wick=2.5200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=46.21 (current export asof; earnings_date=11/24/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.14 (current export; earnings_date=11/24/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9156.9 | **NEUTRAL** |
| `B04_income` | 229.5 | **GOOD** |
| `B05_profit_margin` | 2.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 145.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=145.21 vs prior_export=145.21 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -5.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.45 vs prior=-5.45 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 3.01 | **NEUTRAL** |
| `B14_earnings_date` | 11/24/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=46.21 (this export) | prior_export=46.21 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.14 (this export) | prior_export=4.14 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### COST  ·  score **+13**  ·  Discount Stores
price=1102.1500244140625  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.60 on 2026-11-20; prev RSI=67.82 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.82@2026-11-19 → 69.60@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.82@2026-11-19 → 69.60@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.82@2026-11-19 → 69.60@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.4399 R=0.0000); 2026-11-19:GREEN:O=1089.5000,C=1093.8300,body=+4.3300,vol=2130100.0; 2026-11-20:GREEN:O=1102.0400,C=1102.1500,body=+0.1100,vol=4129900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6260000 Rvol=0); 2026-11-19:GREEN:O=1089.5000,C=1093.8300,body=+4.3300,vol=2130100.0; 2026-11-20:GREEN:O=1102.0400,C=1102.1500,body=+0.1100,vol=4129900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.705 on 2026-11-20: today_vol=4129900 / avg20=2422548 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.515 on 2026-11-20 (price=1102.1500, mid=1057.4290, upper=1144.2175, lower=970.6405; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=1102.1500 vs SMA50=986.7311 dist=+11.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=1057.4290 SMA50=986.7311 SMA80=969.9137 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-21@885.5000; S2[2026-09-24→2026-10-22] low=2026-09-25@883.1000; S3[2026-10-23→2026-11-20] low=2026-10-28@980.3150 | lows=[885.5, 883.0999755859375, 980.3150024414062] span=11.01% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.18825751035269794 wick_frac=0.811742489647302 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:GREEN:body=+2.5599:wick=9.5500; 2026-11-17:RED:body=-0.4100:wick=14.5800; 2026-11-18:RED:body=-7.3800:wick=10.4200; 2026-11-19:GREEN:body=+4.3300:wick=7.3101; 2026-11-20:GREEN:body=+0.1100:wick=24.1901 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.21 (current export asof; earnings_date=12/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.8 (current export; earnings_date=12/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 303154.0 | **NEUTRAL** |
| `B04_income` | 9226.0 | **GOOD** |
| `B05_profit_margin` | 3.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 1101.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=1101.97 vs prior_export=1101.97 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.02 | **GOOD** |
| `B10_insider_transactions` | -1.23 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.23 vs prior=-1.23 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 1.78 | **NEUTRAL** |
| `B14_earnings_date` | 12/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.21 (this export) | prior_export=3.21 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.8 (this export) | prior_export=0.8 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCEP  ·  score **+13**  ·  Beverages - Non-Alcoholic
price=115.52999877929688  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.55 on 2026-11-20; prev RSI=55.62 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.62@2026-11-19 → 53.55@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.62@2026-11-19 → 53.55@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.62@2026-11-19 → 53.55@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=1.012 (G=0.8400 R=0.8300); 2026-11-19:RED:O=117.0900,C=116.2600,body=-0.8300,vol=1467900.0; 2026-11-20:GREEN:O=114.6900,C=115.5300,body=+0.8400,vol=3025700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=2.061 (Gvol=3025700 Rvol=1467900); 2026-11-19:RED:O=117.0900,C=116.2600,body=-0.8300,vol=1467900.0; 2026-11-20:GREEN:O=114.6900,C=115.5300,body=+0.8400,vol=3025700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.768 on 2026-11-20: today_vol=3025700 / avg20=1711324 (avg window 2026-10-23→2026-11-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.008 on 2026-11-20 (price=115.5300, mid=115.5765, upper=121.2912, lower=109.8618; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=115.5300 vs SMA50=108.9580 dist=+6.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=115.5765 SMA50=108.9580 SMA80=108.3513 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-20 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-23@99.0000; S2[2026-09-24→2026-10-22] low=2026-09-25@99.8400; S3[2026-10-23→2026-11-20] low=2026-10-29@109.6700 | lows=[99.0, 99.83999633789062, 109.66999816894531] span=10.78% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.3589721853613686 wick_frac=0.6410278146386315 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.37899236364649813 wick_frac=0.6210076363535019 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0120508507293935 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:RED:body=-0.3800:wick=1.0900; 2026-11-17:GREEN:body=+0.3300:wick=1.1800; 2026-11-18:GREEN:body=+1.0400:wick=0.9400; 2026-11-19:RED:body=-0.8300:wick=1.3600; 2026-11-20:GREEN:body=+0.8400:wick=1.5000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.67 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.0 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 24903.24 | **NEUTRAL** |
| `B04_income` | 2328.07 | **GOOD** |
| `B05_profit_margin` | 9.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=111.06 vs prior_export=111.06 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.48 | **BAD** |
| `B13_short_float` | 6.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.67 (this export) | prior_export=2.67 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.0 (this export) | prior_export=1.0 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WTRG  ·  score **+13**  ·  Utilities - Regulated Water
price=45.189998626708984  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.26 on 2026-11-20; prev RSI=58.49 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.49@2026-11-19 → 60.26@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.49@2026-11-19 → 60.26@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.49@2026-11-19 → 60.26@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=15.500 (G=0.3100 R=0.0200); 2026-11-19:RED:O=44.9000,C=44.8800,body=-0.0200,vol=1857900.0; 2026-11-20:GREEN:O=44.8800,C=45.1900,body=+0.3100,vol=2092100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=1.126 (Gvol=2092100 Rvol=1857900); 2026-11-19:RED:O=44.9000,C=44.8800,body=-0.0200,vol=1857900.0; 2026-11-20:GREEN:O=44.8800,C=45.1900,body=+0.3100,vol=2092100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.926 on 2026-11-20: today_vol=2092100 / avg20=2258090 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.304 on 2026-11-20 (price=45.1900, mid=44.5095, upper=46.7514, lower=42.2676; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-20: price=45.1900 vs SMA50=42.0024 dist=+7.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=44.5095 SMA50=42.0024 SMA80=41.3981 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-20 (63 bars); S1[2026-08-20→2026-09-18] low=2026-08-21@40.0100; S2[2026-09-21→2026-10-22] low=2026-10-01@38.1900; S3[2026-10-23→2026-11-20] low=2026-10-27@42.1200 | lows=[40.0099983215332, 38.189998626708984, 42.119998931884766] span=10.29% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.26050245070539924 wick_frac=0.7394975492946008 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=0.01724177620220529 wick_frac=0.9827582237977948 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.499523173755483 need>1.4; red_wick_gt_green=True 5d trail=2026-11-16:RED:body=-0.0300:wick=0.5700; 2026-11-17:RED:body=-0.2800:wick=0.6700; 2026-11-18:GREEN:body=+0.8700:wick=0.2600; 2026-11-19:RED:body=-0.0200:wick=1.1400; 2026-11-20:GREEN:body=+0.3100:wick=0.8800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.7 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.84 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2599.98 | **NEUTRAL** |
| `B04_income` | 566.63 | **GOOD** |
| `B05_profit_margin` | 21.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.33 vs prior_export=40.33 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 3.12 | **NEUTRAL** |
| `B10_insider_transactions` | -0.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.43 vs prior=-0.43 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.82 | **GOOD** |
| `B13_short_float` | 2.49 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.7 (this export) | prior_export=13.7 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.84 (this export) | prior_export=12.84 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBC  ·  score **+12**  ·  Medical Instruments & Supplies
price=6.019999980926514  pair=`2026-11-19→2026-11-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=44.84 on 2026-11-20; prev RSI=49.09 on 2026-11-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.09@2026-11-19 → 44.84@2026-11-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.09@2026-11-19 → 44.84@2026-11-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.09@2026-11-19 → 44.84@2026-11-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1700 R=0.0000); 2026-11-19:GREEN:O=6.0700,C=6.1900,body=+0.1200,vol=835900.0; 2026-11-20:GREEN:O=5.9700,C=6.0200,body=+0.0500,vol=862800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-19 + 2026-11-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1698700 Rvol=0); 2026-11-19:GREEN:O=6.0700,C=6.1900,body=+0.1200,vol=835900.0; 2026-11-20:GREEN:O=5.9700,C=6.0200,body=+0.0500,vol=862800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.047 on 2026-11-20: today_vol=862800 / avg20=824167 (avg window 2026-10-23→2026-11-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.804 on 2026-11-20 (price=6.0200, mid=6.3445, upper=6.7483, lower=5.9407; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=True on 2026-11-20: price=6.0200 vs SMA50=5.9775 dist=+0.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-20: SMA20=6.3445 SMA50=5.9775 SMA80=5.5196 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-20 (63 bars); S1[2026-08-20→2026-09-18] low=2026-08-27@4.8000; S2[2026-09-21→2026-10-22] low=2026-09-21@5.1300; S3[2026-10-23→2026-11-20] low=2026-11-06@5.7800 | lows=[4.800000190734863, 5.130000114440918, 5.78000020980835] span=20.42% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: GREEN body_frac=0.18953499708899818 wick_frac=0.8104650029110019 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-19+2026-11-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-16:RED:body=-0.1700:wick=0.2310; 2026-11-17:GREEN:body=+0.0300:wick=0.2100; 2026-11-18:GREEN:body=+0.0200:wick=0.2940; 2026-11-19:GREEN:body=+0.1200:wick=0.3100; 2026-11-20:GREEN:body=+0.0500:wick=0.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=110.68 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.75 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1018.7 | **NEUTRAL** |
| `B04_income` | 87.5 | **GOOD** |
| `B05_profit_margin` | 8.59 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=4.0 vs prior_export=4.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | -21.44 | **BAD** |
| `B13_short_float` | 11.19 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=110.68 (this export) | prior_export=110.68 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.75 (this export) | prior_export=6.75 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-20_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.