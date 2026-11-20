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
| 1 | SLDE | +16 | 17 | 1 | 2026-11-16→2026-11-17 | Insurance - Property & Casualty |
| 2 | CBRL | +16 | 17 | 1 | 2026-11-16→2026-11-17 | Restaurants |
| 3 | REYN | +15 | 15 | 0 | 2026-11-16→2026-11-17 | Packaging & Containers |
| 4 | POWW | +15 | 17 | 2 | 2026-11-16→2026-11-17 | Aerospace & Defense |
| 5 | SO | +15 | 16 | 1 | 2026-11-16→2026-11-17 | Utilities - Regulated Electric |
| 6 | CL | +15 | 17 | 2 | 2026-11-16→2026-11-17 | Household & Personal Products |
| 7 | AES | +15 | 15 | 0 | 2026-11-16→2026-11-17 | Utilities - Diversified |
| 8 | SEZL | +14 | 15 | 1 | 2026-11-16→2026-11-17 | Credit Services |
| 9 | ELV | +14 | 16 | 2 | 2026-11-16→2026-11-17 | Healthcare Plans |
| 10 | CNC | +14 | 16 | 2 | 2026-11-16→2026-11-17 | Healthcare Plans |
| 11 | SWBI | +14 | 14 | 0 | 2026-11-16→2026-11-17 | Aerospace & Defense |
| 12 | PBH | +14 | 15 | 1 | 2026-11-16→2026-11-17 | Drug Manufacturers - Specialty & Gen |
| 13 | TAK | +14 | 16 | 2 | 2026-11-16→2026-11-17 | Drug Manufacturers - Specialty & Gen |
| 14 | ZIP | +14 | 15 | 1 | 2026-11-16→2026-11-17 | Internet Content & Information |
| 15 | Q | +13 | 15 | 2 | 2026-11-16→2026-11-17 | Semiconductor Equipment & Materials |

## Full checklist — top 15

### SLDE  ·  score **+16**  ·  Insurance - Property & Casualty
price=26.579999923706055  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.53 on 2026-11-17; prev RSI=47.20 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.20@2026-11-16 → 52.53@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.20@2026-11-16 → 52.53@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.20@2026-11-16 → 52.53@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=4.500 (G=0.5400 R=0.1200); 2026-11-16:RED:O=26.1600,C=26.0400,body=-0.1200,vol=857600.0; 2026-11-17:GREEN:O=26.0400,C=26.5800,body=+0.5400,vol=1380500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.610 (Gvol=1380500 Rvol=857600); 2026-11-16:RED:O=26.1600,C=26.0400,body=-0.1200,vol=857600.0; 2026-11-17:GREEN:O=26.0400,C=26.5800,body=+0.5400,vol=1380500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.088 on 2026-11-17: today_vol=1380500 / avg20=1269283 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.243 on 2026-11-17 (price=26.5800, mid=27.0520, upper=28.9929, lower=25.1111; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=26.5800 vs SMA50=25.6769 dist=+3.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=27.0520 SMA50=25.6769 SMA80=24.3876 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-08-20@22.1900; S2[2026-09-21→2026-10-19] low=2026-10-01@22.2550; S3[2026-10-20→2026-11-17] low=2026-11-06@24.0760 | lows=[22.190000534057617, 22.2549991607666, 24.076000213623047] span=8.50% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5624995032941532 wick_frac=0.43750049670584684 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.21052455461712466 wick_frac=0.7894754453828754 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.500031789426837 need>1.4; red_wick_gt_green=False 5d trail=2026-11-11:RED:body=-0.1100:wick=0.2400; 2026-11-12:RED:body=-0.2400:wick=0.4300; 2026-11-13:RED:body=-0.2900:wick=0.5650; 2026-11-16:RED:body=-0.1200:wick=0.4500; 2026-11-17:GREEN:body=+0.5400:wick=0.4200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=32.65 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.86 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1484.16 | **NEUTRAL** |
| `B04_income` | 582.99 | **GOOD** |
| `B05_profit_margin` | 39.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 31.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=31.33 vs prior_export=31.33 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -5.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.25 vs prior=-5.25 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.63 | **GOOD** |
| `B13_short_float` | 9.57 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=32.65 (this export) | prior_export=32.65 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.86 (this export) | prior_export=16.86 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=69.56999969482422  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.63 on 2026-11-17; prev RSI=56.57 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.57@2026-11-16 → 59.63@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.57@2026-11-16 → 59.63@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.57@2026-11-16 → 59.63@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=5.643 (G=1.5800 R=0.2800); 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0; 2026-11-17:GREEN:O=67.9900,C=69.5700,body=+1.5800,vol=712700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.567 (Gvol=712700 Rvol=454700); 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0; 2026-11-17:GREEN:O=67.9900,C=69.5700,body=+1.5800,vol=712700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.054 on 2026-11-17: today_vol=712700 / avg20=675960 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.377 on 2026-11-17 (price=69.5700, mid=67.9875, upper=72.1807, lower=63.7943; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=69.5700 vs SMA50=59.6402 dist=+16.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=67.9875 SMA50=59.6402 SMA80=58.4516 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-17@42.8700; S2[2026-09-21→2026-10-19] low=2026-09-21@44.1700; S3[2026-10-20→2026-11-17] low=2026-11-11@63.4800 | lows=[42.869998931884766, 44.16999816894531, 63.47999954223633] span=48.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5984856804323324 wick_frac=0.40151431956766753 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.10937462747061488 wick_frac=0.8906253725293851 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.642888283378746 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+1.7900:wick=1.2400; 2026-11-12:RED:body=-1.1600:wick=2.3400; 2026-11-13:GREEN:body=+1.2400:wick=0.8600; 2026-11-16:RED:body=-0.2800:wick=2.2800; 2026-11-17:GREEN:body=+1.5800:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=469.95 (current export asof; earnings_date=12/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=12/2/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3318.71 | **NEUTRAL** |
| `B04_income` | 31.68 | **GOOD** |
| `B05_profit_margin` | 0.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.71 vs prior_export=50.71 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | -0.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.71 vs prior=-0.71 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.15 | **GOOD** |
| `B13_short_float` | 23.32 | **GOOD** |
| `B14_earnings_date` | 12/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=469.95 (this export) | prior_export=469.95 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### REYN  ·  score **+15**  ·  Packaging & Containers
price=24.90999984741211  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.70 on 2026-11-17; prev RSI=63.27 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.27@2026-11-16 → 59.70@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.27@2026-11-16 → 59.70@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.27@2026-11-16 → 59.70@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=3.273 (G=0.3600 R=0.1100); 2026-11-16:GREEN:O=24.8500,C=25.2100,body=+0.3600,vol=1172300.0; 2026-11-17:RED:O=25.0200,C=24.9100,body=-0.1100,vol=1073300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.092 (Gvol=1172300 Rvol=1073300); 2026-11-16:GREEN:O=24.8500,C=25.2100,body=+0.3600,vol=1172300.0; 2026-11-17:RED:O=25.0200,C=24.9100,body=-0.1100,vol=1073300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.870 on 2026-11-17: today_vol=1073300 / avg20=1233827 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.482 on 2026-11-17 (price=24.9100, mid=24.1015, upper=25.7794, lower=22.4236; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=24.9100 vs SMA50=22.8762 dist=+8.89% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=24.10_50=22.88_80=23.81 on 2026-11-17: SMA20=24.1015 SMA50=22.8762 SMA80=23.8114 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-17 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-11@21.2650; S2[2026-09-16→2026-10-19] low=2026-10-01@21.4000; S3[2026-10-20→2026-11-17] low=2026-10-21@22.5300 | lows=[21.264999389648438, 21.399999618530273, 22.530000686645508] span=5.95% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.6545440043279535 wick_frac=0.34545599567204655 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.194691161726132 wick_frac=0.805308838273868 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2726973227909557 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:RED:body=-0.1800:wick=0.4800; 2026-11-12:RED:body=-0.3300:wick=0.3300; 2026-11-13:GREEN:body=+0.1600:wick=0.2750; 2026-11-16:GREEN:body=+0.3600:wick=0.1900; 2026-11-17:RED:body=-0.1100:wick=0.4550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.64 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3799.0 | **NEUTRAL** |
| `B04_income` | 324.0 | **GOOD** |
| `B05_profit_margin` | 8.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.5 vs prior_export=26.5 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.71 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 15.28 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.64 (this export) | prior_export=1.64 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### POWW  ·  score **+15**  ·  Aerospace & Defense
price=2.5  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.18 on 2026-11-17; prev RSI=54.57 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.57@2026-11-16 → 63.18@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.57@2026-11-16 → 63.18@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.57@2026-11-16 → 63.18@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=6.000 (G=0.1200 R=0.0200); 2026-11-16:RED:O=2.3900,C=2.3700,body=-0.0200,vol=1050500.0; 2026-11-17:GREEN:O=2.3800,C=2.5000,body=+0.1200,vol=1631700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.553 (Gvol=1631700 Rvol=1050500); 2026-11-16:RED:O=2.3900,C=2.3700,body=-0.0200,vol=1050500.0; 2026-11-17:GREEN:O=2.3800,C=2.5000,body=+0.1200,vol=1631700.0 | **GOOD** |
| `A07_rvol` | RVOL=2.954 on 2026-11-17: today_vol=1631700 / avg20=552397 (avg window 2026-10-20→2026-11-16, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.998 on 2026-11-17 (price=2.5000, mid=2.3680, upper=2.5003, lower=2.2357; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-17: price=2.5000 vs SMA50=2.2788 dist=+9.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=2.3680 SMA50=2.2788 SMA80=2.2520 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-17 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-08@2.1000; S2[2026-09-16→2026-10-19] low=2026-09-18@2.1000; S3[2026-10-20→2026-11-17] low=2026-11-10@2.1900 | lows=[2.0999999046325684, 2.0999999046325684, 2.190000057220459] span=4.29% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.4137927632552526 wick_frac=0.5862072367447474 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.09091007610970288 wick_frac=0.9090899238902971 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.9999284752107 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+0.0300:wick=0.0200; 2026-11-12:RED:body=-0.0400:wick=0.0350; 2026-11-13:DOJI:body=+0.0000:wick=0.0650; 2026-11-16:RED:body=-0.0200:wick=0.2000; 2026-11-17:GREEN:body=+0.1200:wick=0.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=100.0 (current export asof; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.52 (current export; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 55.5 | **NEUTRAL** |
| `B04_income` | 4.33 | **GOOD** |
| `B05_profit_margin` | 7.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 3.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=3.62 vs prior_export=3.62 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.11 | **GOOD** |
| `B13_short_float` | 2.72 | **NEUTRAL** |
| `B14_earnings_date` | 11/16/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=100.0 (this export) | prior_export=100.0 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.52 (this export) | prior_export=6.52 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SO  ·  score **+15**  ·  Utilities - Regulated Electric
price=93.5999984741211  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.16 on 2026-11-17; prev RSI=62.46 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.46@2026-11-16 → 57.16@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.46@2026-11-16 → 57.16@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.46@2026-11-16 → 57.16@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=11.500 (G=1.1500 R=0.1000); 2026-11-16:GREEN:O=93.4900,C=94.6400,body=+1.1500,vol=13104000.0; 2026-11-17:RED:O=93.7000,C=93.6000,body=-0.1000,vol=8982800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.459 (Gvol=13104000 Rvol=8982800); 2026-11-16:GREEN:O=93.4900,C=94.6400,body=+1.1500,vol=13104000.0; 2026-11-17:RED:O=93.7000,C=93.6000,body=-0.1000,vol=8982800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.077 on 2026-11-17: today_vol=8982800 / avg20=8339312 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.224 on 2026-11-17 (price=93.6000, mid=93.1530, upper=95.1468, lower=91.1592; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=93.6000 vs SMA50=88.9074 dist=+5.28% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=93.15_50=88.91_80=89.85 on 2026-11-17: SMA20=93.1530 SMA50=88.9074 SMA80=89.8456 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-18@85.4300; S2[2026-09-21→2026-10-19] low=2026-09-29@81.6900; S3[2026-10-20→2026-11-17] low=2026-10-20@89.7000 | lows=[85.43000030517578, 81.69000244140625, 89.69999694824219] span=9.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.7098796247456861 wick_frac=0.29012037525431394 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.12194940406963221 wick_frac=0.8780505959303678 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.500190737773709 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+0.7500:wick=0.2900; 2026-11-12:RED:body=-1.0700:wick=0.9300; 2026-11-13:GREEN:body=+1.1200:wick=1.2500; 2026-11-16:GREEN:body=+1.1500:wick=0.4700; 2026-11-17:RED:body=-0.1000:wick=0.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.39 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.59 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31198.0 | **NEUTRAL** |
| `B04_income` | 4992.0 | **GOOD** |
| `B05_profit_margin` | 16.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 97.34 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.37000000000000455 (now=97.34 vs prior_export=96.97 on finviz_2026-11-19) | **GOOD** |
| `B09_analyst_recom` | 2.58 | **NEUTRAL** |
| `B10_insider_transactions` | -1.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.0 vs prior=-1.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.75 | **GOOD** |
| `B13_short_float` | 2.61 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.39 (this export) | prior_export=13.39 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.59 (this export) | prior_export=5.59 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CL  ·  score **+15**  ·  Household & Personal Products
price=99.61000061035156  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.49 on 2026-11-17; prev RSI=67.92 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.92@2026-11-16 → 62.49@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.92@2026-11-16 → 62.49@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.92@2026-11-16 → 62.49@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=2.538 (G=0.9900 R=0.3900); 2026-11-16:GREEN:O=100.0000,C=100.9900,body=+0.9900,vol=8810200.0; 2026-11-17:RED:O=100.0000,C=99.6100,body=-0.3900,vol=5050900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.744 (Gvol=8810200 Rvol=5050900); 2026-11-16:GREEN:O=100.0000,C=100.9900,body=+0.9900,vol=8810200.0; 2026-11-17:RED:O=100.0000,C=99.6100,body=-0.3900,vol=5050900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.973 on 2026-11-17: today_vol=5050900 / avg20=5191430 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.572 on 2026-11-17 (price=99.6100, mid=96.4750, upper=101.9569, lower=90.9931; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=99.6100 vs SMA50=91.4298 dist=+8.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=96.4750 SMA50=91.4298 SMA80=91.4127 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-11@86.3800; S2[2026-09-21→2026-10-19] low=2026-10-02@83.9900; S3[2026-10-20→2026-11-17] low=2026-10-28@90.4700 | lows=[86.37999725341797, 83.98999786376953, 90.47000122070312] span=7.72% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5469608834935087 wick_frac=0.4530391165064913 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.24840730283842688 wick_frac=0.7515926971615731 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5384600336476386 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+1.1500:wick=0.1200; 2026-11-12:RED:body=-1.0000:wick=0.4300; 2026-11-13:GREEN:body=+1.1700:wick=0.5300; 2026-11-16:GREEN:body=+0.9900:wick=0.8200; 2026-11-17:RED:body=-0.3900:wick=1.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.71 (current export asof; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.61 (current export; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21284.0 | **NEUTRAL** |
| `B04_income` | 2053.0 | **GOOD** |
| `B05_profit_margin` | 9.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.42 vs prior_export=102.42 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -21.26 | **BAD** |
| `B11_insider_tx_delta` | delta=0.08999999999999986 (now=-21.26 vs prior=-21.35 on finviz_2026-11-19) | **GOOD** |
| `B12_institutional_transactions` | -1.66 | **BAD** |
| `B13_short_float` | 2.34 | **NEUTRAL** |
| `B14_earnings_date` | 10/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.61 (this export) | prior_export=0.61 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AES  ·  score **+15**  ·  Utilities - Diversified
price=14.970000267028809  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.60 on 2026-11-17; prev RSI=52.79 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.79@2026-11-16 → 56.60@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.79@2026-11-16 → 56.60@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.79@2026-11-16 → 56.60@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.0700 R=0.0000); 2026-11-16:GREEN:O=14.9100,C=14.9400,body=+0.0300,vol=8840500.0; 2026-11-17:GREEN:O=14.9300,C=14.9700,body=+0.0400,vol=17827300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=99.000 (Gvol=26667800 Rvol=0); 2026-11-16:GREEN:O=14.9100,C=14.9400,body=+0.0300,vol=8840500.0; 2026-11-17:GREEN:O=14.9300,C=14.9700,body=+0.0400,vol=17827300.0 | **GOOD** |
| `A07_rvol` | RVOL=2.126 on 2026-11-17: today_vol=17827300 / avg20=8384669 (avg window 2026-10-20→2026-11-16, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.255 on 2026-11-17 (price=14.9700, mid=14.9405, upper=15.0561, lower=14.8249; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=14.9700 vs SMA50=14.9072 dist=+0.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=14.9405 SMA50=14.9072 SMA80=14.8409 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-08-20@14.7000; S2[2026-09-21→2026-10-19] low=2026-09-23@14.8000; S3[2026-10-20→2026-11-17] low=2026-10-30@14.8300 | lows=[14.699999809265137, 14.800000190734863, 14.829999923706055] span=0.88% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.75 wick_frac=0.25 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-11:RED:body=-0.0200:wick=0.0000; 2026-11-12:RED:body=-0.0200:wick=0.0200; 2026-11-13:RED:body=-0.0400:wick=0.0000; 2026-11-16:GREEN:body=+0.0300:wick=0.0000; 2026-11-17:GREEN:body=+0.0400:wick=0.0400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=63.19 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.25 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13542.0 | **NEUTRAL** |
| `B04_income` | 1793.0 | **GOOD** |
| `B05_profit_margin` | 13.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.0 vs prior_export=15.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.68 | **GOOD** |
| `B13_short_float` | 3.7 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=63.19 (this export) | prior_export=63.19 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.25 (this export) | prior_export=12.25 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SEZL  ·  score **+14**  ·  Credit Services
price=154.25  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.36 on 2026-11-17; prev RSI=69.34 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.34@2026-11-16 → 61.36@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.34@2026-11-16 → 61.36@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.34@2026-11-16 → 61.36@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=1.313 (G=7.9200 R=6.0300); 2026-11-16:GREEN:O=154.0000,C=161.9200,body=+7.9200,vol=788700.0; 2026-11-17:RED:O=160.2800,C=154.2500,body=-6.0300,vol=447500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.762 (Gvol=788700 Rvol=447500); 2026-11-16:GREEN:O=154.0000,C=161.9200,body=+7.9200,vol=788700.0; 2026-11-17:RED:O=160.2800,C=154.2500,body=-6.0300,vol=447500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.693 on 2026-11-17: today_vol=447500 / avg20=645774 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.527 on 2026-11-17 (price=154.2500, mid=141.5625, upper=165.6455, lower=117.4795; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=154.2500 vs SMA50=127.0919 dist=+21.37% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=141.56_50=127.09_80=129.33 on 2026-11-17: SMA20=141.5625 SMA50=127.0919 SMA80=129.3347 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-01@112.4100; S2[2026-09-21→2026-10-19] low=2026-09-24@104.8700; S3[2026-10-20→2026-11-17] low=2026-11-03@121.4100 | lows=[112.41000366210938, 104.87000274658203, 121.41000366210938] span=15.77% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5424659681759988 wick_frac=0.45753403182400126 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.41962427475598674 wick_frac=0.5803757252440133 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.3134327980525429 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+1.2100:wick=4.3800; 2026-11-12:GREEN:body=+4.0200:wick=5.6700; 2026-11-13:RED:body=-2.5400:wick=4.5300; 2026-11-16:GREEN:body=+7.9200:wick=6.6800; 2026-11-17:RED:body=-6.0300:wick=8.3400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.8 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.41 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 581.59 | **NEUTRAL** |
| `B04_income` | 182.58 | **GOOD** |
| `B05_profit_margin` | 31.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 192.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=192.67 vs prior_export=192.67 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -0.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.77 vs prior=-0.77 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.75 | **GOOD** |
| `B13_short_float` | 23.6 | **GOOD** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.41 (this export) | prior_export=7.41 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+14**  ·  Healthcare Plans
price=433.92999267578125  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.47 on 2026-11-17; prev RSI=63.73 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.73@2026-11-16 → 55.47@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.73@2026-11-16 → 55.47@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.73@2026-11-16 → 55.47@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=1.968 (G=8.0100 R=4.0700); 2026-11-16:GREEN:O=438.2700,C=446.2800,body=+8.0100,vol=2280400.0; 2026-11-17:RED:O=438.0000,C=433.9300,body=-4.0700,vol=1175000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.941 (Gvol=2280400 Rvol=1175000); 2026-11-16:GREEN:O=438.2700,C=446.2800,body=+8.0100,vol=2280400.0; 2026-11-17:RED:O=438.0000,C=433.9300,body=-4.0700,vol=1175000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.825 on 2026-11-17: today_vol=1175000 / avg20=1424736 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.295 on 2026-11-17 (price=433.9300, mid=428.2500, upper=447.5018, lower=408.9982; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=433.9300 vs SMA50=415.6624 dist=+4.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=428.2500 SMA50=415.6624 SMA80=406.8119 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-17 (63 bars); S1[2026-08-17→2026-09-15] low=2026-08-17@388.1200; S2[2026-09-16→2026-10-19] low=2026-10-01@375.7400; S3[2026-10-20→2026-11-17] low=2026-11-02@406.9000 | lows=[388.1199951171875, 375.739990234375, 406.8999938964844] span=8.29% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.8754085675787452 wick_frac=0.12459143242125485 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.47769929508854375 wick_frac=0.5223007049114562 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.968057825832671 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+0.5400:wick=8.0000; 2026-11-12:RED:body=-3.8800:wick=3.9700; 2026-11-13:RED:body=-2.4900:wick=10.3700; 2026-11-16:GREEN:body=+8.0100:wick=1.1400; 2026-11-17:RED:body=-4.0700:wick=4.4500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=48.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.35 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201831.0 | **NEUTRAL** |
| `B04_income` | 5139.0 | **GOOD** |
| `B05_profit_margin` | 2.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 479.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=479.62 vs prior_export=479.62 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -2.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.01 vs prior=-2.01 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.12 | **BAD** |
| `B13_short_float` | 2.11 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=48.28 (this export) | prior_export=48.28 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.35 (this export) | prior_export=2.35 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNC  ·  score **+14**  ·  Healthcare Plans
price=72.55000305175781  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.31 on 2026-11-17; prev RSI=61.99 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.99@2026-11-16 → 56.31@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.99@2026-11-16 → 56.31@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.99@2026-11-16 → 56.31@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=1.217 (G=0.7300 R=0.6000); 2026-11-16:GREEN:O=74.0400,C=74.7700,body=+0.7300,vol=7626100.0; 2026-11-17:RED:O=73.1500,C=72.5500,body=-0.6000,vol=6021500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.266 (Gvol=7626100 Rvol=6021500); 2026-11-16:GREEN:O=74.0400,C=74.7700,body=+0.7300,vol=7626100.0; 2026-11-17:RED:O=73.1500,C=72.5500,body=-0.6000,vol=6021500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.002 on 2026-11-17: today_vol=6021500 / avg20=6007020 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.454 on 2026-11-17 (price=72.5500, mid=70.2255, upper=75.3445, lower=65.1065; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=72.5500 vs SMA50=67.3916 dist=+7.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=70.2255 SMA50=67.3916 SMA80=66.5179 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-09@61.8100; S2[2026-09-21→2026-10-19] low=2026-10-01@59.4600; S3[2026-10-20→2026-11-17] low=2026-11-02@64.8500 | lows=[61.810001373291016, 59.459999084472656, 64.8499984741211] span=9.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.3578408823166422 wick_frac=0.6421591176833579 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.2926817467937982 wick_frac=0.7073182532062018 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.216662640031535 need>1.4; red_wick_gt_green=False 5d trail=2026-11-11:GREEN:body=+0.1500:wick=0.9600; 2026-11-12:RED:body=-0.4800:wick=1.8350; 2026-11-13:GREEN:body=+0.4500:wick=2.3600; 2026-11-16:GREEN:body=+0.7300:wick=1.3100; 2026-11-17:RED:body=-0.6000:wick=1.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=506.06 (current export asof; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.49 (current export; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 203895.0 | **NEUTRAL** |
| `B04_income` | 1460.0 | **GOOD** |
| `B05_profit_margin` | 0.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.10999999999999943 (now=76.33 vs prior_export=76.22 on finviz_2026-11-19) | **GOOD** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.16 | **GOOD** |
| `B13_short_float` | 2.16 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=506.06 (this export) | prior_export=506.06 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.49 (this export) | prior_export=7.49 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWBI  ·  score **+14**  ·  Aerospace & Defense
price=16.110000610351562  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.53 on 2026-11-17; prev RSI=67.87 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.87@2026-11-16 → 69.53@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.87@2026-11-16 → 69.53@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.87@2026-11-16 → 69.53@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6000 R=0.0000); 2026-11-16:GREEN:O=15.5900,C=15.9100,body=+0.3200,vol=707300.0; 2026-11-17:GREEN:O=15.8300,C=16.1100,body=+0.2800,vol=912900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1620200 Rvol=0); 2026-11-16:GREEN:O=15.5900,C=15.9100,body=+0.3200,vol=707300.0; 2026-11-17:GREEN:O=15.8300,C=16.1100,body=+0.2800,vol=912900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.287 on 2026-11-17: today_vol=912900 / avg20=709401 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.793 on 2026-11-17 (price=16.1100, mid=14.5530, upper=16.5165, lower=12.5895; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=16.1100 vs SMA50=14.2226 dist=+13.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=14.5530 SMA50=14.2226 SMA80=14.1021 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-03@12.0900; S2[2026-09-21→2026-10-19] low=2026-09-21@13.5700; S3[2026-10-20→2026-11-17] low=2026-11-02@13.1200 | lows=[12.09000015258789, 13.569999694824219, 13.119999885559082] span=12.24% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.718146717536931 wick_frac=0.2818532824630689 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-11:GREEN:body=+0.2300:wick=0.3960; 2026-11-12:RED:body=-0.1500:wick=0.3100; 2026-11-13:GREEN:body=+0.0500:wick=0.6300; 2026-11-16:GREEN:body=+0.3200:wick=0.0500; 2026-11-17:GREEN:body=+0.2800:wick=0.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=209.09 (current export asof; earnings_date=12/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.07 (current export; earnings_date=12/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 551.35 | **NEUTRAL** |
| `B04_income` | 24.48 | **GOOD** |
| `B05_profit_margin` | 4.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.25 vs prior_export=17.25 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.43 | **GOOD** |
| `B13_short_float` | 3.31 | **NEUTRAL** |
| `B14_earnings_date` | 12/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=209.09 (this export) | prior_export=209.09 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.07 (this export) | prior_export=14.07 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBH  ·  score **+14**  ·  Drug Manufacturers - Specialty & Generic
price=48.220001220703125  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.02 on 2026-11-17; prev RSI=52.44 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.44@2026-11-16 → 53.02@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.44@2026-11-16 → 53.02@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.44@2026-11-16 → 53.02@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.9300 R=0.0000); 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0; 2026-11-17:GREEN:O=47.5900,C=48.2200,body=+0.6300,vol=597800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1128900 Rvol=0); 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0; 2026-11-17:GREEN:O=47.5900,C=48.2200,body=+0.6300,vol=597800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.955 on 2026-11-17: today_vol=597800 / avg20=626078 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.176 on 2026-11-17 (price=48.2200, mid=47.3355, upper=52.3735, lower=42.2975; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=48.2200 vs SMA50=47.4578 dist=+1.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-17: SMA20=47.3355 SMA50=47.4578 SMA80=49.0572 | **BAD** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-17 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-15@46.3800; S2[2026-09-16→2026-10-19] low=2026-09-21@44.4600; S3[2026-10-20→2026-11-17] low=2026-11-06@41.7700 | lows=[46.380001068115234, 44.459999084472656, 41.77000045776367] span=11.04% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.6328525327731764 wick_frac=0.36714746722682357 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:RED:body=-0.1000:wick=0.9927; 2026-11-12:GREEN:body=+0.6000:wick=0.8700; 2026-11-13:GREEN:body=+0.5000:wick=0.6800; 2026-11-16:GREEN:body=+1.3000:wick=0.3200; 2026-11-17:GREEN:body=+0.6300:wick=0.7300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.32 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1167.08 | **NEUTRAL** |
| `B04_income` | 155.1 | **GOOD** |
| `B05_profit_margin` | 13.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.25 vs prior_export=65.25 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 9.09 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TAK  ·  score **+14**  ·  Drug Manufacturers - Specialty & Generic
price=21.639999389648438  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.50 on 2026-11-17; prev RSI=72.00 on 2026-11-16 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.00@2026-11-16 → 72.50@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.00@2026-11-16 → 72.50@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.00@2026-11-16 → 72.50@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=1.071 (G=0.1500 R=0.1400); 2026-11-16:RED:O=21.7300,C=21.5900,body=-0.1400,vol=3257800.0; 2026-11-17:GREEN:O=21.4900,C=21.6400,body=+0.1500,vol=3374600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.036 (Gvol=3374600 Rvol=3257800); 2026-11-16:RED:O=21.7300,C=21.5900,body=-0.1400,vol=3257800.0; 2026-11-17:GREEN:O=21.4900,C=21.6400,body=+0.1500,vol=3374600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.131 on 2026-11-17: today_vol=3374600 / avg20=2983820 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.831 on 2026-11-17 (price=21.6400, mid=20.7915, upper=21.8119, lower=19.7711; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-17: price=21.6400 vs SMA50=19.5256 dist=+10.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-17: SMA20=20.7915 SMA50=19.5256 SMA80=18.8670 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-08-28@17.9600; S2[2026-09-21→2026-10-19] low=2026-10-05@17.7500; S3[2026-10-20→2026-11-17] low=2026-10-29@19.5000 | lows=[17.959999084472656, 17.75, 19.5] span=9.86% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5999984741210938 wick_frac=0.40000152587890625 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.4666628519839529 wick_frac=0.5333371480160471 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0714305177111716 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:GREEN:body=+0.0500:wick=0.1300; 2026-11-12:RED:body=-0.0800:wick=0.1801; 2026-11-13:RED:body=-0.0200:wick=0.2600; 2026-11-16:RED:body=-0.1400:wick=0.1600; 2026-11-17:GREEN:body=+0.1500:wick=0.1000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=164.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.72 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 29820.42 | **NEUTRAL** |
| `B04_income` | 242.88 | **GOOD** |
| `B05_profit_margin` | 0.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.06 vs prior_export=22.06 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.12 | **GOOD** |
| `B13_short_float` | 0.1 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=164.69 (this export) | prior_export=164.69 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.72 (this export) | prior_export=2.72 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZIP  ·  score **+14**  ·  Internet Content & Information
price=5.989999771118164  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.67 on 2026-11-17; prev RSI=64.24 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.24@2026-11-16 → 62.67@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.24@2026-11-16 → 62.67@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.24@2026-11-16 → 62.67@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=7.857 (G=0.5500 R=0.0700); 2026-11-16:GREEN:O=5.5600,C=6.1100,body=+0.5500,vol=1335100.0; 2026-11-17:RED:O=6.0600,C=5.9900,body=-0.0700,vol=1274400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.048 (Gvol=1335100 Rvol=1274400); 2026-11-16:GREEN:O=5.5600,C=6.1100,body=+0.5500,vol=1335100.0; 2026-11-17:RED:O=6.0600,C=5.9900,body=-0.0700,vol=1274400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.123 on 2026-11-17: today_vol=1274400 / avg20=1134316 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.709 on 2026-11-17 (price=5.9900, mid=4.8145, upper=6.4728, lower=3.1562; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-17: price=5.9900 vs SMA50=4.2586 dist=+40.66% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=4.81_50=4.26_80=4.35 on 2026-11-17: SMA20=4.8145 SMA50=4.2586 SMA80=4.3485 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-10@3.6100; S2[2026-09-21→2026-10-19] low=2026-09-29@3.3500; S3[2026-10-20→2026-11-17] low=2026-11-06@3.6150 | lows=[3.609999895095825, 3.3499999046325684, 3.615000009536743] span=7.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.6790126291213303 wick_frac=0.3209873708786697 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.17948768109119942 wick_frac=0.8205123189088006 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.857126313853448 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:RED:body=-0.5300:wick=0.4149; 2026-11-12:RED:body=-0.3500:wick=0.2850; 2026-11-13:RED:body=-0.0300:wick=0.4500; 2026-11-16:GREEN:body=+0.5500:wick=0.2600; 2026-11-17:RED:body=-0.0700:wick=0.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=534.78 (current export asof; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.78 (current export; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 461.73 | **NEUTRAL** |
| `B04_income` | 42.96 | **GOOD** |
| `B05_profit_margin` | 9.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.25 vs prior_export=5.25 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.6 | **NEUTRAL** |
| `B10_insider_transactions` | -2.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.25 vs prior=-2.25 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.8 | **GOOD** |
| `B13_short_float` | 4.84 | **NEUTRAL** |
| `B14_earnings_date` | 11/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=534.78 (this export) | prior_export=534.78 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.78 (this export) | prior_export=2.78 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### Q  ·  score **+13**  ·  Semiconductor Equipment & Materials
price=114.48999786376953  pair=`2026-11-16→2026-11-17`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=44.16 on 2026-11-17; prev RSI=44.60 on 2026-11-16 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.60@2026-11-16 → 44.16@2026-11-17 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 44.60@2026-11-16 → 44.16@2026-11-17 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 44.60@2026-11-16 → 44.16@2026-11-17 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_body_sum/RED_body_sum=1.838 (G=2.5000 R=1.3600); 2026-11-16:GREEN:O=112.4600,C=114.9600,body=+2.5000,vol=1917700.0; 2026-11-17:RED:O=115.8500,C=114.4900,body=-1.3600,vol=1261600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-16 + 2026-11-17; ratio=GREEN_vol/RED_vol=1.520 (Gvol=1917700 Rvol=1261600); 2026-11-16:GREEN:O=112.4600,C=114.9600,body=+2.5000,vol=1917700.0; 2026-11-17:RED:O=115.8500,C=114.4900,body=-1.3600,vol=1261600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.549 on 2026-11-17: today_vol=1261600 / avg20=2299229 (avg window 2026-10-20→2026-11-16, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.343 on 2026-11-17 (price=114.4900, mid=117.9800, upper=128.1448, lower=107.8152; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-11-17: price=114.4900 vs SMA50=121.2262 dist=-5.56% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-17: SMA20=117.9800 SMA50=121.2262 SMA80=124.4891 | **BAD** |
| `A11_three_section_lows` | window=2026-08-20→2026-11-17 (63 bars); S1[2026-08-20→2026-09-18] low=2026-09-15@111.7500; S2[2026-09-21→2026-10-19] low=2026-10-16@115.1000; S3[2026-10-20→2026-11-17] low=2026-10-23@105.0050 | lows=[111.75, 115.0999984741211, 105.00499725341797] span=9.61% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: GREEN body_frac=0.5091653808562643 wick_frac=0.4908346191437357 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-16+2026-11-17: RED body_frac=0.2682447124681168 wick_frac=0.7317552875318832 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.838234469140235 need>1.4; red_wick_gt_green=True 5d trail=2026-11-11:RED:body=-2.8000:wick=2.9299; 2026-11-12:RED:body=-0.2300:wick=4.9000; 2026-11-13:RED:body=-2.8800:wick=3.7700; 2026-11-16:GREEN:body=+2.5000:wick=2.4100; 2026-11-17:RED:body=-1.3600:wick=3.7100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=21.45 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.73 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5490.0 | **NEUTRAL** |
| `B04_income` | 558.0 | **GOOD** |
| `B05_profit_margin` | 10.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 175.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=175.8 vs prior_export=175.8 on finviz_2026-11-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.19 | **GOOD** |
| `B13_short_float` | 2.07 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=21.45 (this export) | prior_export=21.45 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.73 (this export) | prior_export=4.73 (finviz_2026-11-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-20_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.