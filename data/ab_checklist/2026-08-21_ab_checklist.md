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

## Ranked (top 20)

| Rank | Ticker | score | good | bad | pair | Industry |
|-----:|--------|------:|-----:|----:|------|----------|
| 1 | BNL | +17 | 17 | 0 | 2026-08-13→2026-08-14 | REIT - Diversified |
| 2 | COR | +17 | 18 | 1 | 2026-08-13→2026-08-14 | Medical Distribution |
| 3 | EVTC | +16 | 17 | 1 | 2026-08-13→2026-08-14 | Software - Infrastructure |
| 4 | HOPE | +16 | 17 | 1 | 2026-08-13→2026-08-14 | Banks - Regional |
| 5 | HON | +16 | 17 | 1 | 2026-08-13→2026-08-14 | Conglomerates |
| 6 | FAF | +16 | 17 | 1 | 2026-08-13→2026-08-14 | Insurance - Specialty |
| 7 | DRS | +15 | 17 | 2 | 2026-08-13→2026-08-14 | Aerospace & Defense |
| 8 | RYN | +15 | 16 | 1 | 2026-08-13→2026-08-14 | REIT - Specialty |
| 9 | PRG | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Rental & Leasing Services |
| 10 | CCEP | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Beverages - Non-Alcoholic |
| 11 | ELS | +15 | 16 | 1 | 2026-08-13→2026-08-14 | REIT - Residential |
| 12 | WMS | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Building Products & Equipment |
| 13 | DD | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Specialty Chemicals |
| 14 | BBDC | +15 | 15 | 0 | 2026-08-13→2026-08-14 | Asset Management |
| 15 | UMH | +15 | 17 | 2 | 2026-08-13→2026-08-14 | REIT - Residential |
| 16 | YPF | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Oil & Gas Integrated |
| 17 | KDP | +15 | 15 | 0 | 2026-08-13→2026-08-14 | Beverages - Non-Alcoholic |
| 18 | AFRM | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Credit Services |
| 19 | PAGP | +15 | 15 | 0 | 2026-08-13→2026-08-14 | Oil & Gas Midstream |
| 20 | SBLK | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Marine Shipping |

## Full checklist — top 20

### BNL  ·  score **+17**  ·  REIT - Diversified
price=21.389999389648438  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.85 on 2026-08-14; prev RSI=48.30 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3300 R=0.0000); 2026-08-13:GREEN:O=21.0600,C=21.2400,body=+0.1800,vol=1317600.0; 2026-08-14:GREEN:O=21.2400,C=21.3900,body=+0.1500,vol=1740000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3057600 Rvol=0); 2026-08-13:GREEN:O=21.0600,C=21.2400,body=+0.1800,vol=1317600.0; 2026-08-14:GREEN:O=21.2400,C=21.3900,body=+0.1500,vol=1740000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.762 on 2026-08-14: today_vol=1740000 / avg20=2284320 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.202 on 2026-08-14 (price=21.3900, mid=21.6905, upper=23.1770, lower=20.2040; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=21.3900 vs SMA50=21.1398 dist=+1.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=21.6905 SMA50=21.1398 SMA80=20.6293 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-15@19.2694; S2[2026-06-11→2026-07-13] low=2026-06-17@19.9893; S3[2026-07-14→2026-08-14] low=2026-08-11@20.3600 | lows=[19.269433772998582, 19.98932373945091, 20.360000610351562] span=5.66% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6309168010093771 wick_frac=0.3690831989906229 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-10:RED:body=-0.3700:wick=0.0650; 2026-08-11:GREEN:body=+0.1100:wick=0.3100; 2026-08-12:GREEN:body=+0.3900:wick=0.0000; 2026-08-13:GREEN:body=+0.1800:wick=0.1050; 2026-08-14:GREEN:body=+0.1500:wick=0.0880 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.66 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.56 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 477.57 | **NEUTRAL** |
| `B04_income` | 140.69 | **GOOD** |
| `B05_profit_margin` | 29.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.7 vs prior_export=23.7 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | 0.04 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.04 vs prior=0.04 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.57 | **GOOD** |
| `B13_short_float` | 6.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.66 (this export) | prior_export=9.66 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.56 (this export) | prior_export=0.56 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### COR  ·  score **+17**  ·  Medical Distribution
price=313.82000732421875  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.18 on 2026-08-14; prev RSI=50.46 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.46@2026-08-13 → 53.18@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.46@2026-08-13 → 53.18@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.46@2026-08-13 → 53.18@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=4.334 (G=9.0400 R=2.0860); 2026-08-13:RED:O=311.4960,C=309.4100,body=-2.0860,vol=1542800.0; 2026-08-14:GREEN:O=304.7800,C=313.8200,body=+9.0400,vol=1524200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=0.988 (Gvol=1524200 Rvol=1542800); 2026-08-13:RED:O=311.4960,C=309.4100,body=-2.0860,vol=1542800.0; 2026-08-14:GREEN:O=304.7800,C=313.8200,body=+9.0400,vol=1524200.0 | **BAD** |
| `A07_rvol` | RVOL=0.957 on 2026-08-14: today_vol=1524200 / avg20=1592200 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.060 on 2026-08-14 (price=313.8200, mid=312.7829, upper=330.0814, lower=295.4844; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=313.8200 vs SMA50=295.4784 dist=+6.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=312.7829 SMA50=295.4784 SMA80=290.7826 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-14@251.1135; S2[2026-06-11→2026-07-13] low=2026-06-22@268.7788; S3[2026-07-14→2026-08-14] low=2026-07-15@295.7265 | lows=[251.11353951783084, 268.7787651178076, 295.72653577303686] span=17.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.734960773708212 wick_frac=0.26503922629178805 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.3738815218400091 wick_frac=0.6261184781599909 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.333758252780748 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+3.9224:wick=1.8364; 2026-08-11:GREEN:body=+8.2540:wick=1.3774; 2026-08-12:RED:body=-5.4095:wick=5.4894; 2026-08-13:RED:body=-2.0860:wick=3.4932; 2026-08-14:GREEN:body=+9.0400:wick=3.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.07 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.52 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 332771.32 | **NEUTRAL** |
| `B04_income` | 2624.79 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 368.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=368.92 vs prior_export=368.92 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.68 | **GOOD** |
| `B10_insider_transactions` | 0.5 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.14 (now=0.5 vs prior=0.36 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 0.98 | **GOOD** |
| `B13_short_float` | 2.72 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.07 (this export) | prior_export=3.07 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.52 (this export) | prior_export=0.52 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVTC  ·  score **+16**  ·  Software - Infrastructure
price=31.56999969482422  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.17 on 2026-08-14; prev RSI=60.64 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=9.813 (G=1.5700 R=0.1600); 2026-08-13:GREEN:O=30.4100,C=31.9800,body=+1.5700,vol=461800.0; 2026-08-14:RED:O=31.7300,C=31.5700,body=-0.1600,vol=475000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=0.972 (Gvol=461800 Rvol=475000); 2026-08-13:GREEN:O=30.4100,C=31.9800,body=+1.5700,vol=461800.0; 2026-08-14:RED:O=31.7300,C=31.5700,body=-0.1600,vol=475000.0 | **BAD** |
| `A07_rvol` | RVOL=1.164 on 2026-08-14: today_vol=475000 / avg20=407905 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.435 on 2026-08-14 (price=31.5700, mid=30.7550, upper=32.6286, lower=28.8814; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=31.5700 vs SMA50=28.1554 dist=+12.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=30.7550 SMA50=28.1554 SMA80=27.5883 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-05@21.8100; S2[2026-06-11→2026-07-13] low=2026-06-11@23.7200; S3[2026-07-14→2026-08-14] low=2026-07-23@28.7000 | lows=[21.809999465942383, 23.719999313354492, 28.700000762939453] span=31.59% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.9691362530700092 wick_frac=0.03086374692999084 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.3137250502081253 wick_frac=0.6862749497918746 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.812507450587702 need>1.4; red_wick_gt_green=False 5d trail=2026-08-10:GREEN:body=+0.1500:wick=0.7800; 2026-08-11:GREEN:body=+0.0100:wick=0.6400; 2026-08-12:GREEN:body=+0.9600:wick=0.5300; 2026-08-13:GREEN:body=+1.5700:wick=0.0500; 2026-08-14:RED:body=-0.1600:wick=0.3500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.8 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 996.16 | **NEUTRAL** |
| `B04_income` | 97.58 | **GOOD** |
| `B05_profit_margin` | 9.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.6 vs prior_export=35.6 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 3.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.10999999999999988 (now=3.55 vs prior=3.44 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 4.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.8 (this export) | prior_export=4.8 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HOPE  ·  score **+16**  ·  Banks - Regional
price=14.449999809265137  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.91 on 2026-08-14; prev RSI=69.80 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.80@2026-08-13 → 67.91@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.80@2026-08-13 → 67.91@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.80@2026-08-13 → 67.91@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.2000 R=0.0500); 2026-08-13:GREEN:O=14.3000,C=14.5000,body=+0.2000,vol=1189300.0; 2026-08-14:RED:O=14.5000,C=14.4500,body=-0.0500,vol=859000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.385 (Gvol=1189300 Rvol=859000); 2026-08-13:GREEN:O=14.3000,C=14.5000,body=+0.2000,vol=1189300.0; 2026-08-14:RED:O=14.5000,C=14.4500,body=-0.0500,vol=859000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.778 on 2026-08-14: today_vol=859000 / avg20=1103665 (avg window 2026-07-15→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.760 on 2026-08-14 (price=14.4500, mid=13.9387, upper=14.6118, lower=13.2655; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=14.4500 vs SMA50=13.3936 dist=+7.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=13.9387 SMA50=13.3936 SMA80=12.9482 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-13→2026-08-14 (63 bars); S1[2026-05-13→2026-06-11] low=2026-05-13@11.5852; S2[2026-06-12→2026-07-14] low=2026-06-17@12.4566; S3[2026-07-15→2026-08-14] low=2026-07-23@13.0408 | lows=[11.585213278419676, 12.45658002292461, 13.04079206448225] span=12.56% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=1.0 wick_frac=0.0 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.2173916649044462 wick_frac=0.7826083350955538 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.999980926586431 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+0.0600:wick=0.1300; 2026-08-11:GREEN:body=+0.2200:wick=0.0300; 2026-08-12:GREEN:body=+0.0200:wick=0.1300; 2026-08-13:GREEN:body=+0.2000:wick=0.0000; 2026-08-14:RED:body=-0.0500:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.96 (current export asof; earnings_date=7/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.9 (current export; earnings_date=7/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1018.23 | **NEUTRAL** |
| `B04_income` | 127.88 | **GOOD** |
| `B05_profit_margin` | 12.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.5 vs prior_export=15.5 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.08 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.08 vs prior=-1.08 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.5 | **GOOD** |
| `B13_short_float` | 4.49 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.96 (this export) | prior_export=1.96 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.9 (this export) | prior_export=0.9 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HON  ·  score **+16**  ·  Conglomerates
price=233.9600067138672  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.40 on 2026-08-14; prev RSI=46.75 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.75@2026-08-13 → 47.40@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 46.75@2026-08-13 → 47.40@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 46.75@2026-08-13 → 47.40@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=1.043 (G=1.5700 R=1.5055); 2026-08-13:RED:O=234.7955,C=233.2900,body=-1.5055,vol=2477500.0; 2026-08-14:GREEN:O=232.3900,C=233.9600,body=+1.5700,vol=2686400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.084 (Gvol=2686400 Rvol=2477500); 2026-08-13:RED:O=234.7955,C=233.2900,body=-1.5055,vol=2477500.0; 2026-08-14:GREEN:O=232.3900,C=233.9600,body=+1.5700,vol=2686400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.758 on 2026-08-14: today_vol=2686400 / avg20=3543720 (avg window 2026-07-16→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.363 on 2026-08-14 (price=233.9600, mid=239.2901, upper=253.9817, lower=224.5986; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=233.9600 vs SMA50=233.1418 dist=+0.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=239.2901 SMA50=233.1418 SMA80=231.6032 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-14→2026-08-14 (63 bars); S1[2026-05-14→2026-06-12] low=2026-06-10@214.8979; S2[2026-06-15→2026-07-15] low=2026-07-09@217.4475; S3[2026-07-16→2026-08-14] low=2026-07-16@220.5084 | lows=[214.8979342617044, 217.44754059424912, 220.50835634747548] span=2.61% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6514543313368198 wick_frac=0.34854566866318015 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.39220574363690147 wick_frac=0.6077942563630986 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0428635291273258 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-2.8215:wick=1.5354; 2026-08-11:RED:body=-12.1635:wick=2.9013; 2026-08-12:GREEN:body=+4.3270:wick=2.7617; 2026-08-13:RED:body=-1.5055:wick=2.3330; 2026-08-14:GREEN:body=+1.5700:wick=0.8400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.79 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.21 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 39028.0 | **NEUTRAL** |
| `B04_income` | 8623.0 | **GOOD** |
| `B05_profit_margin` | 22.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 267.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=267.89 vs prior_export=267.89 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.96 | **GOOD** |
| `B10_insider_transactions` | -7.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.5800000000000001 (now=-7.02 vs prior=-7.6 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 1.36 | **GOOD** |
| `B13_short_float` | 2.66 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.79 (this export) | prior_export=6.79 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.21 (this export) | prior_export=3.21 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FAF  ·  score **+16**  ·  Insurance - Specialty
price=73.86000061035156  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.49 on 2026-08-14; prev RSI=54.14 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.14@2026-08-13 → 53.49@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.14@2026-08-13 → 53.49@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.14@2026-08-13 → 53.49@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=7.737 (G=1.4700 R=0.1900); 2026-08-13:GREEN:O=72.5900,C=74.0600,body=+1.4700,vol=820100.0; 2026-08-14:RED:O=74.0500,C=73.8600,body=-0.1900,vol=442800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.852 (Gvol=820100 Rvol=442800); 2026-08-13:GREEN:O=72.5900,C=74.0600,body=+1.4700,vol=820100.0; 2026-08-14:RED:O=74.0500,C=73.8600,body=-0.1900,vol=442800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.578 on 2026-08-14: today_vol=442800 / avg20=766005 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.022 on 2026-08-14 (price=73.8600, mid=73.9750, upper=79.1634, lower=68.7866; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=73.8600 vs SMA50=70.1692 dist=+5.26% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=73.9750 SMA50=70.1692 SMA80=69.2533 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-03@63.4450; S2[2026-06-11→2026-07-13] low=2026-06-12@64.3800; S3[2026-07-14→2026-08-14] low=2026-07-15@68.6100 | lows=[63.44503130060075, 64.37999725341797, 68.61000061035156] span=8.14% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.8497100394699124 wick_frac=0.15028996053008753 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.24050914077665214 wick_frac=0.7594908592233478 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.736749116607774 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-1.2500:wick=1.2200; 2026-08-11:RED:body=-0.1700:wick=0.9000; 2026-08-12:GREEN:body=+0.9500:wick=0.7300; 2026-08-13:GREEN:body=+1.4700:wick=0.2600; 2026-08-14:RED:body=-0.1900:wick=0.6000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.87 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.36 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7983.9 | **NEUTRAL** |
| `B04_income` | 745.1 | **GOOD** |
| `B05_profit_margin` | 9.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 87.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=87.67 vs prior_export=87.67 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -0.4 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.4 vs prior=-0.4 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.7 | **GOOD** |
| `B13_short_float` | 6.08 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.87 (this export) | prior_export=16.87 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.36 (this export) | prior_export=4.36 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRS  ·  score **+15**  ·  Aerospace & Defense
price=45.529998779296875  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.55 on 2026-08-14; prev RSI=47.53 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.53@2026-08-13 → 51.55@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.53@2026-08-13 → 51.55@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.53@2026-08-13 → 51.55@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=3.125 (G=0.7500 R=0.2400); 2026-08-13:RED:O=44.9600,C=44.7200,body=-0.2400,vol=626300.0; 2026-08-14:GREEN:O=44.7800,C=45.5300,body=+0.7500,vol=786300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.255 (Gvol=786300 Rvol=626300); 2026-08-13:RED:O=44.9600,C=44.7200,body=-0.2400,vol=626300.0; 2026-08-14:GREEN:O=44.7800,C=45.5300,body=+0.7500,vol=786300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.888 on 2026-08-14: today_vol=786300 / avg20=885395 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.004 on 2026-08-14 (price=45.5300, mid=45.5421, upper=48.8323, lower=42.2519; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=45.5300 vs SMA50=45.3898 dist=+0.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=45.5421 SMA50=45.3898 SMA80=44.3129 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-13@40.6830; S2[2026-06-11→2026-07-13] low=2026-06-29@40.3195; S3[2026-07-14→2026-08-14] low=2026-07-17@42.4253 | lows=[40.68295564265675, 40.31950483529062, 42.42529821245888] span=5.22% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.4934209040327863 wick_frac=0.5065790959672137 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.23762204218835573 wick_frac=0.7623779578116443 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.125027815748482 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+0.0798:wick=0.5988; 2026-08-11:RED:body=-0.3593:wick=1.0080; 2026-08-12:GREEN:body=+0.5988:wick=0.6387; 2026-08-13:RED:body=-0.2400:wick=0.7700; 2026-08-14:GREEN:body=+0.7500:wick=0.7700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.3 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.08 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3779.0 | **NEUTRAL** |
| `B04_income` | 322.0 | **GOOD** |
| `B05_profit_margin` | 8.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 54.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=54.89 vs prior_export=54.89 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.36 | **GOOD** |
| `B10_insider_transactions` | -0.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.07 vs prior=-0.07 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.19 | **GOOD** |
| `B13_short_float` | 3.92 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.3 (this export) | prior_export=28.3 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.08 (this export) | prior_export=1.08 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYN  ·  score **+15**  ·  REIT - Specialty
price=21.56999969482422  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.01 on 2026-08-14; prev RSI=52.81 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.81@2026-08-13 → 50.01@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.81@2026-08-13 → 50.01@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.81@2026-08-13 → 50.01@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=1.056 (G=0.1900 R=0.1800); 2026-08-13:GREEN:O=21.5700,C=21.7600,body=+0.1900,vol=2640500.0; 2026-08-14:RED:O=21.7500,C=21.5700,body=-0.1800,vol=2266500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.165 (Gvol=2640500 Rvol=2266500); 2026-08-13:GREEN:O=21.5700,C=21.7600,body=+0.1900,vol=2640500.0; 2026-08-14:RED:O=21.7500,C=21.5700,body=-0.1800,vol=2266500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.794 on 2026-08-14: today_vol=2266500 / avg20=2853315 (avg window 2026-07-15→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.170 on 2026-08-14 (price=21.5700, mid=21.6690, upper=22.2523, lower=21.0857; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=21.5700 vs SMA50=21.3858 dist=+0.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=21.6690 SMA50=21.3858 SMA80=21.0356 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-13→2026-08-14 (63 bars); S1[2026-05-13→2026-06-11] low=2026-05-19@19.3239; S2[2026-06-12→2026-07-14] low=2026-06-17@20.7000; S3[2026-07-15→2026-08-14] low=2026-07-21@20.9600 | lows=[19.323899575454348, 20.700000762939453, 20.959999084472656] span=8.47% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.9047601747486399 wick_frac=0.09523982525136011 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.40909110613081795 wick_frac=0.5909088938691821 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0555567329292588 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.1300:wick=0.4200; 2026-08-11:RED:body=-0.1500:wick=0.1700; 2026-08-12:GREEN:body=+0.1400:wick=0.2600; 2026-08-13:GREEN:body=+0.1900:wick=0.0200; 2026-08-14:RED:body=-0.1800:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.94 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 968.3 | **NEUTRAL** |
| `B04_income` | 75.79 | **GOOD** |
| `B05_profit_margin` | 7.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | -2.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.39 vs prior=-2.39 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 38.16 | **GOOD** |
| `B13_short_float` | 4.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.7 (this export) | prior_export=0.7 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.94 (this export) | prior_export=5.94 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PRG  ·  score **+15**  ·  Rental & Leasing Services
price=42.619998931884766  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.93 on 2026-08-14; prev RSI=48.53 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.53@2026-08-13 → 47.93@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 48.53@2026-08-13 → 47.93@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 48.53@2026-08-13 → 47.93@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-13:GREEN:O=41.5900,C=42.7800,body=+1.1900,vol=452400.0; 2026-08-14:DOJI:O=42.6200,C=42.6200,body=+0.0000,vol=477000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=2.897 (Gvol=690900 Rvol=238500); 2026-08-13:GREEN:O=41.5900,C=42.7800,body=+1.1900,vol=452400.0; 2026-08-14:DOJI:O=42.6200,C=42.6200,body=+0.0000,vol=477000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.900 on 2026-08-14: today_vol=477000 / avg20=530040 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.398 on 2026-08-14 (price=42.6200, mid=44.0565, upper=47.6621, lower=40.4509; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=42.6200 vs SMA50=41.9146 dist=+1.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=44.0565 SMA50=41.9146 SMA80=38.9645 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-20@32.5100; S2[2026-06-11→2026-07-13] low=2026-06-11@35.6200; S3[2026-07-14→2026-08-14] low=2026-07-29@41.0000 | lows=[32.5099983215332, 35.619998931884766, 41.0] span=26.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.8095222030766676 wick_frac=0.19047779692333244 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.3400:wick=0.7400; 2026-08-11:RED:body=-0.8500:wick=0.4400; 2026-08-12:RED:body=-0.5200:wick=0.6500; 2026-08-13:GREEN:body=+1.1900:wick=0.2800; 2026-08-14:DOJI:body=+0.0000:wick=0.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.69 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.87 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2632.08 | **NEUTRAL** |
| `B04_income` | 146.67 | **GOOD** |
| `B05_profit_margin` | 5.57 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.43 vs prior_export=53.43 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.5 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.69 (this export) | prior_export=25.69 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.87 (this export) | prior_export=0.87 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCEP  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=107.63999938964844  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.94 on 2026-08-14; prev RSI=54.49 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.49@2026-08-13 → 54.94@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.49@2026-08-13 → 54.94@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.49@2026-08-13 → 54.94@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-13:GREEN:O=106.8200,C=107.4800,body=+0.6600,vol=2237000.0; 2026-08-14:GREEN:O=107.1100,C=107.6400,body=+0.5300,vol=1279600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3516600 Rvol=0); 2026-08-13:GREEN:O=106.8200,C=107.4800,body=+0.6600,vol=2237000.0; 2026-08-14:GREEN:O=107.1100,C=107.6400,body=+0.5300,vol=1279600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.626 on 2026-08-14: today_vol=1279600 / avg20=2044150 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.057 on 2026-08-14 (price=107.6400, mid=107.3865, upper=111.8509, lower=102.9221; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=107.6400 vs SMA50=102.7436 dist=+4.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=107.3865 SMA50=102.7436 SMA80=99.2559 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-01@89.2400; S2[2026-06-11→2026-07-13] low=2026-06-22@95.9600; S3[2026-07-14→2026-08-14] low=2026-08-04@102.5900 | lows=[89.23999786376953, 95.95999908447266, 102.58999633789062] span=14.96% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.622666302788667 wick_frac=0.3773336972113331 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.6200:wick=1.9600; 2026-08-11:GREEN:body=+1.0700:wick=0.9000; 2026-08-12:GREEN:body=+0.8500:wick=0.9800; 2026-08-13:GREEN:body=+0.6600:wick=0.2200; 2026-08-14:GREEN:body=+0.5300:wick=0.5400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.67 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.0 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 24903.24 | **NEUTRAL** |
| `B04_income` | 2328.07 | **GOOD** |
| `B05_profit_margin` | 9.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.84 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.3299999999999983 (now=111.84 vs prior_export=110.51 on finviz_2026-08-20) | **GOOD** |
| `B09_analyst_recom` | 2.19 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -4.1 | **BAD** |
| `B13_short_float` | 4.33 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.67 (this export) | prior_export=2.67 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.0 (this export) | prior_export=1.0 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELS  ·  score **+15**  ·  REIT - Residential
price=65.61000061035156  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.48 on 2026-08-14; prev RSI=51.46 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.46@2026-08-13 → 54.48@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.46@2026-08-13 → 54.48@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.46@2026-08-13 → 54.48@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2300 R=0.0000); 2026-08-13:GREEN:O=64.2400,C=65.0100,body=+0.7700,vol=1433700.0; 2026-08-14:GREEN:O=65.1500,C=65.6100,body=+0.4600,vol=1442800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2876500 Rvol=0); 2026-08-13:GREEN:O=64.2400,C=65.0100,body=+0.7700,vol=1433700.0; 2026-08-14:GREEN:O=65.1500,C=65.6100,body=+0.4600,vol=1442800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.870 on 2026-08-14: today_vol=1442800 / avg20=1657720 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.183 on 2026-08-14 (price=65.6100, mid=65.2850, upper=67.0632, lower=63.5068; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=65.6100 vs SMA50=64.0179 dist=+2.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=65.2850 SMA50=64.0179 SMA80=63.4462 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-02@60.0870; S2[2026-06-11→2026-07-13] low=2026-06-22@60.8900; S3[2026-07-14→2026-08-14] low=2026-07-15@63.3900 | lows=[60.0870209770581, 60.890028175119156, 63.38999938964844] span=5.50% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6770168152799244 wick_frac=0.3229831847200756 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.1800:wick=0.6400; 2026-08-11:RED:body=-0.2500:wick=0.5600; 2026-08-12:RED:body=-0.0300:wick=0.6600; 2026-08-13:GREEN:body=+0.7700:wick=0.1700; 2026-08-14:GREEN:body=+0.4600:wick=0.4000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.05 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.69 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1564.18 | **NEUTRAL** |
| `B04_income` | 401.81 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.22 vs prior_export=70.22 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.84 | **GOOD** |
| `B10_insider_transactions` | -0.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.13 vs prior=-0.13 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 3.91 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.05 (this export) | prior_export=15.05 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.69 (this export) | prior_export=3.69 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMS  ·  score **+15**  ·  Building Products & Equipment
price=150.91000366210938  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.55 on 2026-08-14; prev RSI=54.84 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.84@2026-08-13 → 56.55@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.84@2026-08-13 → 56.55@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.84@2026-08-13 → 56.55@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.2600 R=0.0000); 2026-08-13:GREEN:O=141.7900,C=149.2300,body=+7.4400,vol=1130500.0; 2026-08-14:GREEN:O=148.0900,C=150.9100,body=+2.8200,vol=1099700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2230200 Rvol=0); 2026-08-13:GREEN:O=141.7900,C=149.2300,body=+7.4400,vol=1130500.0; 2026-08-14:GREEN:O=148.0900,C=150.9100,body=+2.8200,vol=1099700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.578 on 2026-08-14: today_vol=1099700 / avg20=696860 (avg window 2026-07-14→2026-08-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.660 on 2026-08-14 (price=150.9100, mid=145.1220, upper=153.8865, lower=136.3575; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=150.9100 vs SMA50=143.9404 dist=+4.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=145.1220 SMA50=143.9404 SMA80=143.7879 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-22@127.8460; S2[2026-06-11→2026-07-13] low=2026-06-11@130.5500; S3[2026-07-14→2026-08-14] low=2026-07-30@136.4900 | lows=[127.8459947864609, 130.5500030517578, 136.49000549316406] span=6.76% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.8634855812747215 wick_frac=0.13651441872527845 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.4400:wick=1.9600; 2026-08-11:GREEN:body=+0.8800:wick=1.1400; 2026-08-12:RED:body=-3.8700:wick=1.7770; 2026-08-13:GREEN:body=+7.4400:wick=1.8150; 2026-08-14:GREEN:body=+2.8200:wick=0.2350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.38 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.98 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3221.61 | **NEUTRAL** |
| `B04_income` | 451.06 | **GOOD** |
| `B05_profit_margin` | 14.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 183.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=183.36 vs prior_export=183.36 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.07 | **BAD** |
| `B13_short_float` | 3.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.38 (this export) | prior_export=18.38 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.98 (this export) | prior_export=0.98 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DD  ·  score **+15**  ·  Specialty Chemicals
price=146.25999450683594  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.53 on 2026-08-14; prev RSI=57.61 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.61@2026-08-13 → 61.53@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.61@2026-08-13 → 61.53@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.61@2026-08-13 → 61.53@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=12.286 (G=2.5800 R=0.2100); 2026-08-13:RED:O=144.3100,C=144.1000,body=-0.2100,vol=908200.0; 2026-08-14:GREEN:O=143.6800,C=146.2600,body=+2.5800,vol=1108700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.221 (Gvol=1108700 Rvol=908200); 2026-08-13:RED:O=144.3100,C=144.1000,body=-0.2100,vol=908200.0; 2026-08-14:GREEN:O=143.6800,C=146.2600,body=+2.5800,vol=1108700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.786 on 2026-08-14: today_vol=1108700 / avg20=1411045 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.720 on 2026-08-14 (price=146.2600, mid=140.5915, upper=148.4665, lower=132.7165; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=146.2600 vs SMA50=140.1690 dist=+4.35% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=140.59_50=140.17_80=141.25 on 2026-08-14: SMA20=140.5915 SMA50=140.1690 SMA80=141.2497 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-10@134.7900; S2[2026-06-11→2026-07-13] low=2026-06-26@131.1400; S3[2026-07-14→2026-08-14] low=2026-08-04@130.1900 | lows=[134.7899932861328, 131.13999938964844, 130.19000244140625] span=3.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6954170248294186 wick_frac=0.3045829751705814 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.11601753498566852 wick_frac=0.8839824650143315 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.286222932713269 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-1.3000:wick=1.3600; 2026-08-11:GREEN:body=+3.1500:wick=1.8600; 2026-08-12:RED:body=-0.0800:wick=2.7200; 2026-08-13:RED:body=-0.2100:wick=1.6000; 2026-08-14:GREEN:body=+2.5800:wick=1.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.76 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8265.0 | **NEUTRAL** |
| `B04_income` | 55.0 | **GOOD** |
| `B05_profit_margin` | 0.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 168.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=168.93 vs prior_export=168.93 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -0.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.05 vs prior=-0.05 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 3.87 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.76 (this export) | prior_export=6.76 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BBDC  ·  score **+15**  ·  Asset Management
price=9.3100004196167  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.01 on 2026-08-14; prev RSI=69.28 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.28@2026-08-13 → 65.01@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.28@2026-08-13 → 65.01@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.28@2026-08-13 → 65.01@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=14.001 (G=0.1400 R=0.0100); 2026-08-13:GREEN:O=9.2900,C=9.4300,body=+0.1400,vol=1041900.0; 2026-08-14:RED:O=9.3200,C=9.3100,body=-0.0100,vol=593600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.755 (Gvol=1041900 Rvol=593600); 2026-08-13:GREEN:O=9.2900,C=9.4300,body=+0.1400,vol=1041900.0; 2026-08-14:RED:O=9.3200,C=9.3100,body=-0.0100,vol=593600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.839 on 2026-08-14: today_vol=593600 / avg20=707435 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.656 on 2026-08-14 (price=9.3100, mid=8.7150, upper=9.6224, lower=7.8076; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=9.3100 vs SMA50=8.5164 dist=+9.32% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.72_50=8.52_80=8.52 on 2026-08-14: SMA20=8.7150 SMA50=8.5164 SMA80=8.5228 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-20@8.1410; S2[2026-06-11→2026-07-13] low=2026-06-24@8.0000; S3[2026-07-14→2026-08-14] low=2026-07-23@8.1500 | lows=[8.140982072545915, 8.0, 8.149999618530273] span=1.87% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.8235310617195302 wick_frac=0.1764689382804699 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.06249552964737858 wick_frac=0.9375044703526214 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.001049117787316 need>1.4; red_wick_gt_green=False 5d trail=2026-08-10:GREEN:body=+0.1200:wick=0.1500; 2026-08-11:RED:body=-0.0900:wick=0.0300; 2026-08-12:RED:body=-0.0700:wick=0.0700; 2026-08-13:GREEN:body=+0.1400:wick=0.0300; 2026-08-14:RED:body=-0.0100:wick=0.1500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.68 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.49 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 264.4 | **NEUTRAL** |
| `B04_income` | 87.11 | **GOOD** |
| `B05_profit_margin` | 32.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=9.96 vs prior_export=9.96 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 1.56 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.56 vs prior=1.56 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 2.87 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.68 (this export) | prior_export=12.68 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.49 (this export) | prior_export=8.49 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UMH  ·  score **+15**  ·  REIT - Residential
price=16.450000762939453  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.41 on 2026-08-14; prev RSI=67.79 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.79@2026-08-13 → 64.41@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.79@2026-08-13 → 64.41@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.79@2026-08-13 → 64.41@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=1.571 (G=0.2200 R=0.1400); 2026-08-13:GREEN:O=16.3700,C=16.5900,body=+0.2200,vol=748000.0; 2026-08-14:RED:O=16.5900,C=16.4500,body=-0.1400,vol=685900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.091 (Gvol=748000 Rvol=685900); 2026-08-13:GREEN:O=16.3700,C=16.5900,body=+0.2200,vol=748000.0; 2026-08-14:RED:O=16.5900,C=16.4500,body=-0.1400,vol=685900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.144 on 2026-08-14: today_vol=685900 / avg20=599815 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.926 on 2026-08-14 (price=16.4500, mid=15.7105, upper=16.5089, lower=14.9121; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-14: price=16.4500 vs SMA50=15.4074 dist=+6.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=15.7105 SMA50=15.4074 SMA80=15.3830 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-06-01@14.7400; S2[2026-06-11→2026-07-13] low=2026-06-17@14.8600; S3[2026-07-14→2026-08-14] low=2026-08-05@15.0900 | lows=[14.739999771118164, 14.859999656677246, 15.09000015258789] span=2.37% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.7586209164512671 wick_frac=0.24137908354873291 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.45161171237133063 wick_frac=0.5483882876286693 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5714305177111716 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.2200:wick=0.2300; 2026-08-11:GREEN:body=+0.3400:wick=0.2300; 2026-08-12:GREEN:body=+0.2000:wick=0.0400; 2026-08-13:GREEN:body=+0.2200:wick=0.0700; 2026-08-14:RED:body=-0.1400:wick=0.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=78.57 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=17.91 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 271.36 | **NEUTRAL** |
| `B04_income` | 10.7 | **GOOD** |
| `B05_profit_margin` | 3.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.88 vs prior_export=18.88 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.05 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.05 vs prior=0.05 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.47 | **BAD** |
| `B13_short_float` | 4.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=78.57 (this export) | prior_export=78.57 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.91 (this export) | prior_export=17.91 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
price=50.04999923706055  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.38 on 2026-08-14; prev RSI=49.04 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.04@2026-08-13 → 51.38@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.04@2026-08-13 → 51.38@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.04@2026-08-13 → 51.38@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4600 R=0.0000); 2026-08-13:GREEN:O=48.4000,C=49.4500,body=+1.0500,vol=874600.0; 2026-08-14:GREEN:O=49.6400,C=50.0500,body=+0.4100,vol=915400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1790000 Rvol=0); 2026-08-13:GREEN:O=48.4000,C=49.4500,body=+1.0500,vol=874600.0; 2026-08-14:GREEN:O=49.6400,C=50.0500,body=+0.4100,vol=915400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.855 on 2026-08-14: today_vol=915400 / avg20=1070545 (avg window 2026-07-16→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.136 on 2026-08-14 (price=50.0500, mid=50.3560, upper=52.6056, lower=48.1064; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=50.0500 vs SMA50=49.9502 dist=+0.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=50.3560 SMA50=49.9502 SMA80=48.5191 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-14→2026-08-14 (63 bars); S1[2026-05-14→2026-06-12] low=2026-05-14@43.2100; S2[2026-06-15→2026-07-15] low=2026-07-01@44.0200; S3[2026-07-16→2026-08-14] low=2026-08-12@47.8600 | lows=[43.209999084472656, 44.02000045776367, 47.86000061035156] span=10.76% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6193275102906265 wick_frac=0.38067248970937356 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+1.0200:wick=0.5700; 2026-08-11:RED:body=-2.5900:wick=1.9600; 2026-08-12:GREEN:body=+0.5600:wick=1.1300; 2026-08-13:GREEN:body=+1.0500:wick=0.3600; 2026-08-14:GREEN:body=+0.4100:wick=0.4200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.98 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.98 vs prior_export=61.98 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | -0.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.15 vs prior=-0.15 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 443.02 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KDP  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=31.440000534057617  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.43 on 2026-08-14; prev RSI=54.27 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.27@2026-08-13 → 56.43@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.27@2026-08-13 → 56.43@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.27@2026-08-13 → 56.43@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2300 R=0.0000); 2026-08-13:GREEN:O=30.0600,C=31.1200,body=+1.0600,vol=12212300.0; 2026-08-14:GREEN:O=31.2700,C=31.4400,body=+0.1700,vol=13701800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=25914100 Rvol=0); 2026-08-13:GREEN:O=30.0600,C=31.1200,body=+1.0600,vol=12212300.0; 2026-08-14:GREEN:O=31.2700,C=31.4400,body=+0.1700,vol=13701800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.055 on 2026-08-14: today_vol=13701800 / avg20=12993235 (avg window 2026-07-16→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.626 on 2026-08-14 (price=31.4400, mid=30.4995, upper=32.0012, lower=28.9978; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=31.4400 vs SMA50=31.0324 dist=+1.31% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=30.50_50=31.03_80=30.18 on 2026-08-14: SMA20=30.4995 SMA50=31.0324 SMA80=30.1829 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-14→2026-08-14 (63 bars); S1[2026-05-14→2026-06-12] low=2026-05-21@28.0800; S2[2026-06-15→2026-07-15] low=2026-06-23@29.6190; S3[2026-07-16→2026-08-14] low=2026-08-11@28.6600 | lows=[28.079988537850486, 29.619025024446326, 28.65999984741211] span=5.48% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.5525419269641582 wick_frac=0.44745807303584184 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.4500:wick=0.4200; 2026-08-11:RED:body=-0.0200:wick=0.6900; 2026-08-12:GREEN:body=+0.4400:wick=0.2800; 2026-08-13:GREEN:body=+1.0600:wick=0.0800; 2026-08-14:GREEN:body=+0.1700:wick=0.8000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.2 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.07 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20090.0 | **NEUTRAL** |
| `B04_income` | 1345.0 | **GOOD** |
| `B05_profit_margin` | 6.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.38 vs prior_export=36.38 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.2 | **GOOD** |
| `B13_short_float` | 4.92 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.2 (this export) | prior_export=6.2 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.07 (this export) | prior_export=1.07 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AFRM  ·  score **+15**  ·  Credit Services
price=78.3499984741211  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.77 on 2026-08-14; prev RSI=54.71 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.71@2026-08-13 → 54.77@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.71@2026-08-13 → 54.77@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.71@2026-08-13 → 54.77@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=9.059 (G=4.6200 R=0.5100); 2026-08-13:GREEN:O=73.7000,C=78.3200,body=+4.6200,vol=2909900.0; 2026-08-14:RED:O=78.8600,C=78.3500,body=-0.5100,vol=1973900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.474 (Gvol=2909900 Rvol=1973900); 2026-08-13:GREEN:O=73.7000,C=78.3200,body=+4.6200,vol=2909900.0; 2026-08-14:RED:O=78.8600,C=78.3500,body=-0.5100,vol=1973900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.599 on 2026-08-14: today_vol=1973900 / avg20=3294765 (avg window 2026-07-16→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.674 on 2026-08-14 (price=78.3500, mid=74.8320, upper=80.0499, lower=69.6141; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=78.3500 vs SMA50=75.3622 dist=+3.96% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=74.83_50=75.36_80=72.11 on 2026-08-14: SMA20=74.8320 SMA50=75.3622 SMA80=72.1091 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-14→2026-08-14 (63 bars); S1[2026-05-14→2026-06-12] low=2026-06-11@61.1800; S2[2026-06-15→2026-07-15] low=2026-06-15@68.4700; S3[2026-07-16→2026-08-14] low=2026-07-29@69.6200 | lows=[61.18000030517578, 68.47000122070312, 69.62000274658203] span=13.80% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.8369575282958106 wick_frac=0.1630424717041894 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.21702232654479106 wick_frac=0.782977673455209 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.058790970425001 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+0.5400:wick=0.9850; 2026-08-11:GREEN:body=+1.0450:wick=0.9000; 2026-08-12:RED:body=-3.9800:wick=1.3600; 2026-08-13:GREEN:body=+4.6200:wick=0.9000; 2026-08-14:RED:body=-0.5100:wick=1.8400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=76.06 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.37 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3675.5 | **NEUTRAL** |
| `B04_income` | 382.42 | **GOOD** |
| `B05_profit_margin` | 10.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 94.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=94.0 vs prior_export=94.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -0.2 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.2 vs prior=-0.2 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.45 | **GOOD** |
| `B13_short_float` | 5.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=76.06 (this export) | prior_export=76.06 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.37 (this export) | prior_export=4.37 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+15**  ·  Oil & Gas Midstream
price=25.84000015258789  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.70 on 2026-08-14; prev RSI=54.77 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.77@2026-08-13 → 58.70@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.77@2026-08-13 → 58.70@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.77@2026-08-13 → 58.70@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6100 R=0.0000); 2026-08-13:GREEN:O=25.1400,C=25.4900,body=+0.3500,vol=2312000.0; 2026-08-14:GREEN:O=25.5800,C=25.8400,body=+0.2600,vol=1934800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4246800 Rvol=0); 2026-08-13:GREEN:O=25.1400,C=25.4900,body=+0.3500,vol=2312000.0; 2026-08-14:GREEN:O=25.5800,C=25.8400,body=+0.2600,vol=1934800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.255 on 2026-08-14: today_vol=1934800 / avg20=1541890 (avg window 2026-07-15→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.278 on 2026-08-14 (price=25.8400, mid=25.5803, upper=26.5155, lower=24.6451; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=25.8400 vs SMA50=24.5896 dist=+5.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=25.5803 SMA50=24.5896 SMA80=24.2247 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-13→2026-08-14 (63 bars); S1[2026-05-13→2026-06-11] low=2026-05-13@22.8451; S2[2026-06-12→2026-07-14] low=2026-06-18@22.4514; S3[2026-07-15→2026-08-14] low=2026-08-07@24.6700 | lows=[22.84513393121253, 22.451422571353564, 24.670000076293945] span=9.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6267251025204925 wick_frac=0.3732748974795075 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+0.3100:wick=0.0900; 2026-08-11:RED:body=-0.0700:wick=0.2500; 2026-08-12:GREEN:body=+0.2300:wick=0.1500; 2026-08-13:GREEN:body=+0.3500:wick=0.2300; 2026-08-14:GREEN:body=+0.2600:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.0 vs prior_export=25.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 8.04 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+15**  ·  Marine Shipping
price=29.049999237060547  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.66 on 2026-08-14; prev RSI=58.07 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.07@2026-08-13 → 59.66@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.07@2026-08-13 → 59.66@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.07@2026-08-13 → 59.66@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9500 R=0.0000); 2026-08-13:GREEN:O=27.9000,C=28.7400,body=+0.8400,vol=1612100.0; 2026-08-14:GREEN:O=28.9400,C=29.0500,body=+0.1100,vol=1196100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2808200 Rvol=0); 2026-08-13:GREEN:O=27.9000,C=28.7400,body=+0.8400,vol=1612100.0; 2026-08-14:GREEN:O=28.9400,C=29.0500,body=+0.1100,vol=1196100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.877 on 2026-08-14: today_vol=1196100 / avg20=1363095 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.575 on 2026-08-14 (price=29.0500, mid=27.7230, upper=30.0310, lower=25.4150; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=29.0500 vs SMA50=26.8078 dist=+8.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=27.7230 SMA50=26.8078 SMA80=26.3203 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-19@25.4230; S2[2026-06-11→2026-07-13] low=2026-06-26@23.8600; S3[2026-07-14→2026-08-14] low=2026-07-17@24.8600 | lows=[25.423019363615396, 23.860000610351562, 24.860000610351562] span=6.55% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.5400275536738276 wick_frac=0.4599724463261724 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.4500:wick=0.4600; 2026-08-11:RED:body=-0.5400:wick=0.5300; 2026-08-12:GREEN:body=+0.4100:wick=0.4100; 2026-08-13:GREEN:body=+0.8400:wick=0.1000; 2026-08-14:GREEN:body=+0.1100:wick=0.4800 | **GOOD** |
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

CSV: `data/ab_checklist/2026-08-21_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.