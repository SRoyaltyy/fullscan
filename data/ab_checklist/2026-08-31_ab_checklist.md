# A+B1 Feature Checklist — 2026-08-31

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,685** names
- Export: `finviz_2026-08-31.csv` · prior export for Δ: `2026-08-30`
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
| 1 | ADSK | +19 | 19 | 0 | 2026-08-27→2026-08-28 | Software - Application |
| 2 | WDAY | +18 | 19 | 1 | 2026-08-27→2026-08-28 | Software - Application |
| 3 | EVTC | +17 | 18 | 1 | 2026-08-27→2026-08-28 | Software - Infrastructure |
| 4 | NMRK | +16 | 16 | 0 | 2026-08-27→2026-08-28 | Real Estate Services |
| 5 | CMBT | +16 | 16 | 0 | 2026-08-27→2026-08-28 | Oil & Gas Midstream |
| 6 | MTDR | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Oil & Gas E&P |
| 7 | UMBF | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Banks - Regional |
| 8 | TWLO | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Software - Infrastructure |
| 9 | FIVN | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Software - Infrastructure |
| 10 | SCHW | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Capital Markets |
| 11 | DINO | +16 | 16 | 0 | 2026-08-27→2026-08-28 | Oil & Gas Refining & Marketing |
| 12 | PANW | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Software - Infrastructure |
| 13 | BG | +16 | 17 | 1 | 2026-08-27→2026-08-28 | Farm Products |
| 14 | TILE | +15 | 16 | 1 | 2026-08-27→2026-08-28 | Furnishings, Fixtures & Appliances |
| 15 | INSW | +15 | 16 | 1 | 2026-08-27→2026-08-28 | Oil & Gas Midstream |

## Full checklist — top 15

### ADSK  ·  score **+19**  ·  Software - Application
price=260.6600036621094  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.84 on 2026-08-28; prev RSI=69.52 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.52@2026-08-27 → 60.84@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.52@2026-08-27 → 60.84@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.52@2026-08-27 → 60.84@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=18.220 (G=9.1100 R=0.5000); 2026-08-27:GREEN:O=261.4700,C=270.5800,body=+9.1100,vol=3963200.0; 2026-08-28:RED:O=261.1600,C=260.6600,body=-0.5000,vol=3437900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.153 (Gvol=3963200 Rvol=3437900); 2026-08-27:GREEN:O=261.4700,C=270.5800,body=+9.1100,vol=3963200.0; 2026-08-28:RED:O=261.1600,C=260.6600,body=-0.5000,vol=3437900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.858 on 2026-08-28: today_vol=3437900 / avg20=1849975 (avg window 2026-07-30→2026-08-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.616 on 2026-08-28 (price=260.6600, mid=250.3250, upper=267.0986, lower=233.5514; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=260.6600 vs SMA50=224.5442 dist=+16.08% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=250.32_50=224.54_80=228.65 on 2026-08-28: SMA20=250.3250 SMA50=224.5442 SMA80=228.6496 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-22@185.5000; S2[2026-06-26→2026-07-29] low=2026-06-26@191.5500; S3[2026-07-30→2026-08-28] low=2026-08-04@229.7000 | lows=[185.5, 191.5500030517578, 229.6999969482422] span=23.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.797024600975068 wick_frac=0.20297539902493206 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.03612717718942733 wick_frac=0.9638728228105726 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.219970703125 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.8400:wick=3.9200; 2026-08-25:RED:body=-0.2100:wick=4.7700; 2026-08-26:GREEN:body=+9.5000:wick=2.0300; 2026-08-27:GREEN:body=+9.1100:wick=2.3200; 2026-08-28:RED:body=-0.5000:wick=13.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 316.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.2400000000000091 (now=316.17 vs prior_export=315.93 on finviz_2026-08-30) | **GOOD** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | 1.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.35 vs prior=1.35 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.44 | **GOOD** |
| `B13_short_float` | 3.53 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WDAY  ·  score **+18**  ·  Software - Application
price=204.72000122070312  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.23 on 2026-08-28; prev RSI=60.73 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.73@2026-08-27 → 66.23@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.73@2026-08-27 → 66.23@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.73@2026-08-27 → 66.23@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=7.672 (G=14.0400 R=1.8300); 2026-08-27:RED:O=195.4000,C=193.5700,body=-1.8300,vol=8166600.0; 2026-08-28:GREEN:O=190.6800,C=204.7200,body=+14.0400,vol=8668700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.061 (Gvol=8668700 Rvol=8166600); 2026-08-27:RED:O=195.4000,C=193.5700,body=-1.8300,vol=8166600.0; 2026-08-28:GREEN:O=190.6800,C=204.7200,body=+14.0400,vol=8668700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.829 on 2026-08-28: today_vol=8668700 / avg20=4739795 (avg window 2026-07-31→2026-08-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.659 on 2026-08-28 (price=204.7200, mid=188.1455, upper=213.3135, lower=162.9775; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=204.7200 vs SMA50=157.3396 dist=+30.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=188.1455 SMA50=157.3396 SMA80=147.6794 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-28 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-22@111.5000; S2[2026-07-01→2026-07-30] low=2026-07-01@126.0800; S3[2026-07-31→2026-08-28] low=2026-07-31@152.1000 | lows=[111.5, 126.08000183105469, 152.10000610351562] span=36.41% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7552453636747177 wick_frac=0.24475463632528233 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.17923461007916333 wick_frac=0.8207653899208367 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.672192112065371 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.5500:wick=3.7300; 2026-08-25:RED:body=-1.7600:wick=2.9200; 2026-08-26:GREEN:body=+2.5700:wick=2.9800; 2026-08-27:RED:body=-1.8300:wick=8.3800; 2026-08-28:GREEN:body=+14.0400:wick=4.5500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.32 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.49 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10164.0 | **NEUTRAL** |
| `B04_income` | 1251.0 | **GOOD** |
| `B05_profit_margin` | 12.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 211.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.44999999999998863 (now=211.14 vs prior_export=210.69 on finviz_2026-08-30) | **GOOD** |
| `B09_analyst_recom` | 2.3 | **GOOD** |
| `B10_insider_transactions` | -4.53 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.53 vs prior=-4.53 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.38 | **GOOD** |
| `B13_short_float` | 10.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.49 (this export) | prior_export=0.49 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVTC  ·  score **+17**  ·  Software - Infrastructure
price=29.979999542236328  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.50 on 2026-08-28; prev RSI=46.89 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.89@2026-08-27 → 50.50@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.89@2026-08-27 → 50.50@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.89@2026-08-27 → 50.50@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=1.966 (G=0.5700 R=0.2900); 2026-08-27:RED:O=29.7600,C=29.4700,body=-0.2900,vol=242000.0; 2026-08-28:GREEN:O=29.4100,C=29.9800,body=+0.5700,vol=380100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.571 (Gvol=380100 Rvol=242000); 2026-08-27:RED:O=29.7600,C=29.4700,body=-0.2900,vol=242000.0; 2026-08-28:GREEN:O=29.4100,C=29.9800,body=+0.5700,vol=380100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.951 on 2026-08-28: today_vol=380100 / avg20=399630 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.199 on 2026-08-28 (price=29.9800, mid=30.3450, upper=32.1752, lower=28.5148; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=29.9800 vs SMA50=29.3432 dist=+2.17% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=30.3450 SMA50=29.3432 SMA80=27.5596 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-05@21.8100; S2[2026-06-26→2026-07-29] low=2026-06-26@25.8000; S3[2026-07-30→2026-08-28] low=2026-08-25@29.0000 | lows=[21.809999465942383, 25.799999237060547, 29.0] span=32.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7402607375260403 wick_frac=0.2597392624739597 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.3866678873697917 wick_frac=0.6133321126302084 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9655099839520138 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.3700:wick=0.5200; 2026-08-25:GREEN:body=+0.5300:wick=0.4400; 2026-08-26:RED:body=-0.0900:wick=0.6900; 2026-08-27:RED:body=-0.2900:wick=0.4600; 2026-08-28:GREEN:body=+0.5700:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.8 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 996.16 | **NEUTRAL** |
| `B04_income` | 97.58 | **GOOD** |
| `B05_profit_margin` | 9.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.6 vs prior_export=35.6 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 3.47 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.47 vs prior=3.47 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.01 | **BAD** |
| `B13_short_float` | 4.44 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.8 (this export) | prior_export=4.8 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NMRK  ·  score **+16**  ·  Real Estate Services
price=15.829999923706055  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.15 on 2026-08-28; prev RSI=50.91 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.91@2026-08-27 → 54.15@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.91@2026-08-27 → 54.15@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.91@2026-08-27 → 54.15@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=2.889 (G=0.2600 R=0.0900); 2026-08-27:RED:O=15.6300,C=15.5400,body=-0.0900,vol=982200.0; 2026-08-28:GREEN:O=15.5700,C=15.8300,body=+0.2600,vol=1195800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.217 (Gvol=1195800 Rvol=982200); 2026-08-27:RED:O=15.6300,C=15.5400,body=-0.0900,vol=982200.0; 2026-08-28:GREEN:O=15.5700,C=15.8300,body=+0.2600,vol=1195800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.878 on 2026-08-28: today_vol=1195800 / avg20=1362000 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.347 on 2026-08-28 (price=15.8300, mid=15.4209, upper=16.5992, lower=14.2427; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=15.8300 vs SMA50=15.3055 dist=+3.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=15.4209 SMA50=15.3055 SMA80=15.1781 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-01@13.5666; S2[2026-06-26→2026-07-29] low=2026-07-15@14.6075; S3[2026-07-30→2026-08-28] low=2026-08-10@14.0547 | lows=[13.566587782447842, 14.607490242948554, 14.054666484599304] span=7.67% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.6666666666666666 wick_frac=0.3333333333333333 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.3000003178912367 wick_frac=0.6999996821087633 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8888865341414824 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.1400:wick=0.3300; 2026-08-25:RED:body=-0.1200:wick=0.3350; 2026-08-26:RED:body=-0.2700:wick=0.1300; 2026-08-27:RED:body=-0.0900:wick=0.2100; 2026-08-28:GREEN:body=+0.2600:wick=0.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.88 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3640.53 | **NEUTRAL** |
| `B04_income` | 148.25 | **GOOD** |
| `B05_profit_margin` | 4.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.75 vs prior_export=19.75 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.01 | **GOOD** |
| `B13_short_float` | 3.82 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.88 (this export) | prior_export=0.88 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CMBT  ·  score **+16**  ·  Oil & Gas Midstream
price=18.350000381469727  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.10 on 2026-08-28; prev RSI=68.53 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.53@2026-08-27 → 69.10@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.53@2026-08-27 → 69.10@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.53@2026-08-27 → 69.10@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=2.174 (G=0.5000 R=0.2300); 2026-08-27:GREEN:O=17.7800,C=18.2800,body=+0.5000,vol=1656200.0; 2026-08-28:RED:O=18.5800,C=18.3500,body=-0.2300,vol=874100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.895 (Gvol=1656200 Rvol=874100); 2026-08-27:GREEN:O=17.7800,C=18.2800,body=+0.5000,vol=1656200.0; 2026-08-28:RED:O=18.5800,C=18.3500,body=-0.2300,vol=874100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.860 on 2026-08-28: today_vol=874100 / avg20=1016875 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.697 on 2026-08-28 (price=18.3500, mid=17.1570, upper=18.8690, lower=15.4450; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=18.3500 vs SMA50=15.9462 dist=+15.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=17.1570 SMA50=15.9462 SMA80=15.5230 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-18@13.9880; S2[2026-06-26→2026-07-29] low=2026-06-30@13.8950; S3[2026-07-30→2026-08-28] low=2026-07-30@15.5800 | lows=[13.98799991607666, 13.895000457763672, 15.579999923706055] span=12.13% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7874012909892077 wick_frac=0.21259870901079228 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.3965509870530473 wick_frac=0.6034490129469526 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1739173701756425 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:GREEN:body=+0.2200:wick=0.0350; 2026-08-25:GREEN:body=+0.0700:wick=0.2400; 2026-08-26:RED:body=-0.2600:wick=0.2700; 2026-08-27:GREEN:body=+0.5000:wick=0.1350; 2026-08-28:RED:body=-0.2300:wick=0.3500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=42.02 (current export asof; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.0 (current export; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2266.8 | **NEUTRAL** |
| `B04_income` | 842.15 | **GOOD** |
| `B05_profit_margin` | 37.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 20.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=20.59 vs prior_export=20.59 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.03 | **GOOD** |
| `B13_short_float` | 1.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=42.02 (this export) | prior_export=42.02 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.0 (this export) | prior_export=21.0 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MTDR  ·  score **+16**  ·  Oil & Gas E&P
price=56.38999938964844  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.09 on 2026-08-28; prev RSI=60.54 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.54@2026-08-27 → 59.09@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.54@2026-08-27 → 59.09@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.54@2026-08-27 → 59.09@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=3.394 (G=2.2400 R=0.6600); 2026-08-27:GREEN:O=54.5000,C=56.7400,body=+2.2400,vol=1588700.0; 2026-08-28:RED:O=57.0500,C=56.3900,body=-0.6600,vol=901100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.763 (Gvol=1588700 Rvol=901100); 2026-08-27:GREEN:O=54.5000,C=56.7400,body=+2.2400,vol=1588700.0; 2026-08-28:RED:O=57.0500,C=56.3900,body=-0.6600,vol=901100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.421 on 2026-08-28: today_vol=901100 / avg20=2139565 (avg window 2026-07-31→2026-08-27, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.356 on 2026-08-28 (price=56.3900, mid=53.6294, upper=61.3776, lower=45.8811; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=56.3900 vs SMA50=51.6172 dist=+9.25% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=53.63_50=51.62_80=53.25 on 2026-08-28: SMA20=53.6294 SMA50=51.6172 SMA80=53.2451 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-28 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-24@48.0102; S2[2026-06-29→2026-07-30] low=2026-07-28@45.0927; S3[2026-07-31→2026-08-28] low=2026-08-05@46.5316 | lows=[48.01019827347788, 45.09266872932533, 46.531587687321455] span=6.47% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.9372394353608162 wick_frac=0.06276056463918381 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.48175208903566047 wick_frac=0.5182479109643395 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3939427217293296 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.3100:wick=1.6800; 2026-08-25:RED:body=-0.3500:wick=0.9400; 2026-08-26:GREEN:body=+0.4600:wick=1.3400; 2026-08-27:GREEN:body=+2.2400:wick=0.1500; 2026-08-28:RED:body=-0.6600:wick=0.7100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=69.1 vs prior_export=69.1 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.38 vs prior=0.38 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.66 | **GOOD** |
| `B13_short_float` | 12.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UMBF  ·  score **+16**  ·  Banks - Regional
price=144.58999633789062  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.75 on 2026-08-28; prev RSI=47.60 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.60@2026-08-27 → 47.75@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.60@2026-08-27 → 47.75@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.60@2026-08-27 → 47.75@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=13.706 (G=2.3300 R=0.1700); 2026-08-27:GREEN:O=142.2100,C=144.5400,body=+2.3300,vol=564200.0; 2026-08-28:RED:O=144.7600,C=144.5900,body=-0.1700,vol=467700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.206 (Gvol=564200 Rvol=467700); 2026-08-27:GREEN:O=142.2100,C=144.5400,body=+2.3300,vol=564200.0; 2026-08-28:RED:O=144.7600,C=144.5900,body=-0.1700,vol=467700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.969 on 2026-08-28: today_vol=467700 / avg20=482465 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.425 on 2026-08-28 (price=144.5900, mid=146.8980, upper=152.3233, lower=141.4727; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=144.5900 vs SMA50=144.0832 dist=+0.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=146.8980 SMA50=144.0832 SMA80=138.6661 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-16@125.1500; S2[2026-06-26→2026-07-29] low=2026-07-23@137.2300; S3[2026-07-30→2026-08-28] low=2026-08-27@141.9600 | lows=[125.1500015258789, 137.22999572753906, 141.9600067138672] span=13.43% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.749191676847368 wick_frac=0.250808323152632 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.09769978865766928 wick_frac=0.9023002113423307 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.705950991831973 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-1.3400:wick=1.8100; 2026-08-25:RED:body=-0.5600:wick=1.5400; 2026-08-26:RED:body=-0.9800:wick=0.5500; 2026-08-27:GREEN:body=+2.3300:wick=0.7800; 2026-08-28:RED:body=-0.1700:wick=1.5700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.51 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.92 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4355.38 | **NEUTRAL** |
| `B04_income` | 917.3 | **GOOD** |
| `B05_profit_margin` | 21.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 166.81 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=166.81 vs prior_export=166.81 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | -1.53 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.53 vs prior=-1.53 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.14 | **GOOD** |
| `B13_short_float` | 4.66 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.51 (this export) | prior_export=14.51 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.92 (this export) | prior_export=6.92 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TWLO  ·  score **+16**  ·  Software - Infrastructure
price=237.77999877929688  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.41 on 2026-08-28; prev RSI=61.94 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.94@2026-08-27 → 59.41@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.94@2026-08-27 → 59.41@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.94@2026-08-27 → 59.41@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=2.650 (G=8.0300 R=3.0300); 2026-08-27:GREEN:O=233.3100,C=241.3400,body=+8.0300,vol=2042900.0; 2026-08-28:RED:O=240.8100,C=237.7800,body=-3.0300,vol=1756900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.163 (Gvol=2042900 Rvol=1756900); 2026-08-27:GREEN:O=233.3100,C=241.3400,body=+8.0300,vol=2042900.0; 2026-08-28:RED:O=240.8100,C=237.7800,body=-3.0300,vol=1756900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.657 on 2026-08-28: today_vol=1756900 / avg20=2673590 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.274 on 2026-08-28 (price=237.7800, mid=227.0250, upper=266.3396, lower=187.7104; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=237.7800 vs SMA50=211.0212 dist=+12.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=227.0250 SMA50=211.0212 SMA80=207.4269 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-28 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-22@179.2000; S2[2026-06-30→2026-07-30] low=2026-07-23@179.6450; S3[2026-07-31→2026-08-28] low=2026-08-06@187.1000 | lows=[179.1999969482422, 179.64500427246094, 187.10000610351562] span=4.41% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7930862991898148 wick_frac=0.20691370081018517 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.45291445045571077 wick_frac=0.5470855495442892 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.650165681307724 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:DOJI:body=+0.0000:wick=7.2600; 2026-08-25:GREEN:body=+2.1200:wick=6.1100; 2026-08-26:GREEN:body=+8.3900:wick=0.7000; 2026-08-27:GREEN:body=+8.0300:wick=2.0950; 2026-08-28:RED:body=-3.0300:wick=3.6600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.06 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5572.33 | **NEUTRAL** |
| `B04_income` | 1148.74 | **GOOD** |
| `B05_profit_margin` | 20.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 260.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=260.18 vs prior_export=260.18 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | -81.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-81.0 vs prior=-81.0 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.14 | **GOOD** |
| `B13_short_float` | 2.75 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.06 (this export) | prior_export=11.06 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.76 (this export) | prior_export=4.76 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FIVN  ·  score **+16**  ·  Software - Infrastructure
price=34.04999923706055  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.09 on 2026-08-28; prev RSI=65.60 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.60@2026-08-27 → 63.09@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.60@2026-08-27 → 63.09@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.60@2026-08-27 → 63.09@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=8.286 (G=1.7400 R=0.2100); 2026-08-27:GREEN:O=32.8300,C=34.5700,body=+1.7400,vol=1601600.0; 2026-08-28:RED:O=34.2600,C=34.0500,body=-0.2100,vol=1595600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.004 (Gvol=1601600 Rvol=1595600); 2026-08-27:GREEN:O=32.8300,C=34.5700,body=+1.7400,vol=1601600.0; 2026-08-28:RED:O=34.2600,C=34.0500,body=-0.2100,vol=1595600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.506 on 2026-08-28: today_vol=1595600 / avg20=3153625 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.523 on 2026-08-28 (price=34.0500, mid=32.1788, upper=35.7558, lower=28.6017; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=34.0500 vs SMA50=27.0987 dist=+25.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=32.1788 SMA50=27.0987 SMA80=25.2750 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-28 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-22@18.1500; S2[2026-07-01→2026-07-30] low=2026-07-23@20.7600; S3[2026-07-31→2026-08-28] low=2026-07-31@26.9400 | lows=[18.149999618530273, 20.760000228881836, 26.940000534057617] span=48.43% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.961326083291182 wick_frac=0.038673916708818076 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.16091927143471005 wick_frac=0.8390807285652899 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.285740236148955 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.0400:wick=1.0570; 2026-08-25:RED:body=-0.3450:wick=1.2250; 2026-08-26:GREEN:body=+0.5400:wick=1.0000; 2026-08-27:GREEN:body=+1.7400:wick=0.0700; 2026-08-28:RED:body=-0.2100:wick=1.0950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.74 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.88 | **NEUTRAL** |
| `B04_income` | 59.47 | **GOOD** |
| `B05_profit_margin` | 4.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.0 vs prior_export=35.0 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.65 | **GOOD** |
| `B10_insider_transactions` | -3.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.78 vs prior=-3.78 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.76 | **GOOD** |
| `B13_short_float` | 15.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.9 (this export) | prior_export=1.9 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SCHW  ·  score **+16**  ·  Capital Markets
price=110.16000366210938  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.27 on 2026-08-28; prev RSI=51.77 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.77@2026-08-27 → 57.27@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.77@2026-08-27 → 57.27@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.77@2026-08-27 → 57.27@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=1.956 (G=1.7600 R=0.9000); 2026-08-27:RED:O=108.9500,C=108.0500,body=-0.9000,vol=10104700.0; 2026-08-28:GREEN:O=108.4000,C=110.1600,body=+1.7600,vol=12819300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.269 (Gvol=12819300 Rvol=10104700); 2026-08-27:RED:O=108.9500,C=108.0500,body=-0.9000,vol=10104700.0; 2026-08-28:GREEN:O=108.4000,C=110.1600,body=+1.7600,vol=12819300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.764 on 2026-08-28: today_vol=12819300 / avg20=7266355 (avg window 2026-07-31→2026-08-27, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.175 on 2026-08-28 (price=110.1600, mid=109.3804, upper=113.8227, lower=104.9381; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=110.1600 vs SMA50=102.9533 dist=+7.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=109.3804 SMA50=102.9533 SMA80=97.7993 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-28 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-03@85.3312; S2[2026-06-30→2026-07-30] low=2026-06-30@91.0246; S3[2026-07-31→2026-08-28] low=2026-07-31@103.7475 | lows=[85.33117602299234, 91.02457027706238, 103.74747845559459] span=21.58% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.5448917359334661 wick_frac=0.45510826406653393 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.39130247292388837 wick_frac=0.6086975270761117 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.955571191210878 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:GREEN:body=+0.7600:wick=1.2600; 2026-08-25:RED:body=-0.7700:wick=0.5300; 2026-08-26:GREEN:body=+1.1400:wick=2.8400; 2026-08-27:RED:body=-0.9000:wick=1.4000; 2026-08-28:GREEN:body=+1.7600:wick=1.4700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.17 (current export asof; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.56 (current export; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 29370.0 | **NEUTRAL** |
| `B04_income` | 9722.0 | **GOOD** |
| `B05_profit_margin` | 33.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 124.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=124.11 vs prior_export=124.11 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -1.88 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.88 vs prior=-1.88 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 1.03 | **NEUTRAL** |
| `B14_earnings_date` | 7/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.17 (this export) | prior_export=4.17 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.56 (this export) | prior_export=2.56 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+16**  ·  Oil & Gas Refining & Marketing
price=99.70999908447266  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.14 on 2026-08-28; prev RSI=63.63 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.63@2026-08-27 → 67.14@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.63@2026-08-27 → 67.14@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.63@2026-08-27 → 67.14@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7100 R=0.0000); 2026-08-27:DOJI:O=97.0000,C=97.0000,body=+0.0000,vol=1840400.0; 2026-08-28:GREEN:O=97.0000,C=99.7100,body=+2.7100,vol=1805500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=2.962 (Gvol=2725700 Rvol=920200); 2026-08-27:DOJI:O=97.0000,C=97.0000,body=+0.0000,vol=1840400.0; 2026-08-28:GREEN:O=97.0000,C=99.7100,body=+2.7100,vol=1805500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.700 on 2026-08-28: today_vol=1805500 / avg20=2578365 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.757 on 2026-08-28 (price=99.7100, mid=91.1156, upper=102.4757, lower=79.7555; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=99.7100 vs SMA50=84.0402 dist=+18.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=91.1156 SMA50=84.0402 SMA80=78.6922 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-28 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-22@63.4268; S2[2026-07-01→2026-07-30] low=2026-07-01@69.2507; S3[2026-07-31→2026-08-28] low=2026-08-07@80.0041 | lows=[63.42683480953448, 69.25073631698324, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.8658151637396253 wick_frac=0.13418483626037464 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.8600:wick=3.6300; 2026-08-25:RED:body=-0.8900:wick=1.8000; 2026-08-26:GREEN:body=+4.5200:wick=2.1000; 2026-08-27:DOJI:body=+0.0000:wick=3.7500; 2026-08-28:GREEN:body=+2.7100:wick=0.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 93.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=93.29 vs prior_export=93.29 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.64 | **GOOD** |
| `B13_short_float` | 5.02 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+16**  ·  Software - Infrastructure
price=371.5899963378906  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.83 on 2026-08-28; prev RSI=60.44 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.44@2026-08-27 → 55.83@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.44@2026-08-27 → 55.83@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.44@2026-08-27 → 55.83@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=4.284 (G=24.2900 R=5.6700); 2026-08-27:GREEN:O=358.5600,C=382.8500,body=+24.2900,vol=7308800.0; 2026-08-28:RED:O=377.2600,C=371.5900,body=-5.6700,vol=5840700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.251 (Gvol=7308800 Rvol=5840700); 2026-08-27:GREEN:O=358.5600,C=382.8500,body=+24.2900,vol=7308800.0; 2026-08-28:RED:O=377.2600,C=371.5900,body=-5.6700,vol=5840700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.060 on 2026-08-28: today_vol=5840700 / avg20=5510360 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.143 on 2026-08-28 (price=371.5900, mid=366.8620, upper=400.0365, lower=333.6875; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=371.5900 vs SMA50=343.4152 dist=+8.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=366.8620 SMA50=343.4152 SMA80=310.1830 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-08-28 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-09@251.1500; S2[2026-07-01→2026-07-30] low=2026-07-28@308.5400; S3[2026-07-31→2026-08-28] low=2026-07-31@319.4800 | lows=[251.14999389648438, 308.5400085449219, 319.4800109863281] span=27.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7919798526160355 wick_frac=0.2080201473839645 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.24630825950294238 wick_frac=0.7536917404970576 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.283941979062946 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-5.1600:wick=5.6600; 2026-08-25:RED:body=-13.8700:wick=4.3900; 2026-08-26:GREEN:body=+8.8500:wick=5.4000; 2026-08-27:GREEN:body=+24.2900:wick=6.3800; 2026-08-28:RED:body=-5.6700:wick=17.3500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.2 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10606.3 | **NEUTRAL** |
| `B04_income` | 842.8 | **GOOD** |
| `B05_profit_margin` | 7.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 370.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=370.12 vs prior_export=370.12 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -0.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.83 vs prior=-0.83 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.98 | **GOOD** |
| `B13_short_float` | 2.78 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.2 (this export) | prior_export=7.2 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BG  ·  score **+16**  ·  Farm Products
price=115.4800033569336  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.80 on 2026-08-28; prev RSI=47.05 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.05@2026-08-27 → 54.80@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.05@2026-08-27 → 54.80@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.05@2026-08-27 → 54.80@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=2.102 (G=2.4800 R=1.1800); 2026-08-27:RED:O=112.7500,C=111.5700,body=-1.1800,vol=1478600.0; 2026-08-28:GREEN:O=113.0000,C=115.4800,body=+2.4800,vol=1576400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.066 (Gvol=1576400 Rvol=1478600); 2026-08-27:RED:O=112.7500,C=111.5700,body=-1.1800,vol=1478600.0; 2026-08-28:GREEN:O=113.0000,C=115.4800,body=+2.4800,vol=1576400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.148 on 2026-08-28: today_vol=1576400 / avg20=1373085 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.627 on 2026-08-28 (price=115.4800, mid=111.6475, upper=117.7646, lower=105.5304; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=115.4800 vs SMA50=112.8852 dist=+2.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-08-28: SMA20=111.6475 SMA50=112.8852 SMA80=117.3744 | **BAD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-24@106.0000; S2[2026-06-26→2026-07-29] low=2026-07-01@104.1200; S3[2026-07-30→2026-08-28] low=2026-07-30@102.8100 | lows=[106.0, 104.12000274658203, 102.80999755859375] span=3.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.6078448238785429 wick_frac=0.3921551761214571 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.3868849588513395 wick_frac=0.6131150411486604 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1016972165648338 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:RED:body=-3.1500:wick=1.0900; 2026-08-25:GREEN:body=+2.7200:wick=3.0000; 2026-08-26:GREEN:body=+2.4500:wick=1.3600; 2026-08-27:RED:body=-1.1800:wick=1.8700; 2026-08-28:GREEN:body=+2.4800:wick=1.6000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.71 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.38 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 91812.0 | **NEUTRAL** |
| `B04_income` | 1010.0 | **GOOD** |
| `B05_profit_margin` | 1.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 144.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=144.2 vs prior_export=144.2 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.18 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 3.94 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.71 (this export) | prior_export=1.71 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.38 (this export) | prior_export=5.38 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TILE  ·  score **+15**  ·  Furnishings, Fixtures & Appliances
price=38.58000183105469  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.82 on 2026-08-28; prev RSI=56.31 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.31@2026-08-27 → 59.82@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.31@2026-08-27 → 59.82@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.31@2026-08-27 → 59.82@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=1.211 (G=0.6900 R=0.5700); 2026-08-27:RED:O=38.4600,C=37.8900,body=-0.5700,vol=358100.0; 2026-08-28:GREEN:O=37.8900,C=38.5800,body=+0.6900,vol=488400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.364 (Gvol=488400 Rvol=358100); 2026-08-27:RED:O=38.4600,C=37.8900,body=-0.5700,vol=358100.0; 2026-08-28:GREEN:O=37.8900,C=38.5800,body=+0.6900,vol=488400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.019 on 2026-08-28: today_vol=488400 / avg20=479120 (avg window 2026-07-30→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.263 on 2026-08-28 (price=38.5800, mid=37.7845, upper=40.8086, lower=34.7604; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=38.5800 vs SMA50=35.2288 dist=+9.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=37.7845 SMA50=35.2288 SMA80=32.9042 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-28 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-03@28.1416; S2[2026-06-26→2026-07-29] low=2026-07-08@31.1100; S3[2026-07-30→2026-08-28] low=2026-07-30@33.4100 | lows=[28.141592803455403, 31.110000610351562, 33.40999984741211] span=18.72% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.5609778034158611 wick_frac=0.43902219658413894 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.331395671199214 wick_frac=0.668604328800786 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.210531247072051 need>1.4; red_wick_gt_green=True 5d trail=2026-08-24:RED:body=-0.8200:wick=0.6200; 2026-08-25:GREEN:body=+0.4000:wick=0.2700; 2026-08-26:RED:body=-0.4400:wick=0.7300; 2026-08-27:RED:body=-0.5700:wick=1.1500; 2026-08-28:GREEN:body=+0.6900:wick=0.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=37.59 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1440.65 | **NEUTRAL** |
| `B04_income` | 145.55 | **GOOD** |
| `B05_profit_margin` | 10.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.25 vs prior_export=45.25 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -9.42 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.42 vs prior=-9.42 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.67 | **GOOD** |
| `B13_short_float` | 7.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.59 (this export) | prior_export=37.59 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INSW  ·  score **+15**  ·  Oil & Gas Midstream
price=98.80999755859375  pair=`2026-08-27→2026-08-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.36 on 2026-08-28; prev RSI=59.38 on 2026-08-27 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.38@2026-08-27 → 58.36@2026-08-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.38@2026-08-27 → 58.36@2026-08-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.38@2026-08-27 → 58.36@2026-08-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_body_sum/RED_body_sum=8.894 (G=4.1800 R=0.4700); 2026-08-27:GREEN:O=95.0800,C=99.2600,body=+4.1800,vol=551500.0; 2026-08-28:RED:O=99.2800,C=98.8100,body=-0.4700,vol=463500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-27 + 2026-08-28; ratio=GREEN_vol/RED_vol=1.190 (Gvol=551500 Rvol=463500); 2026-08-27:GREEN:O=95.0800,C=99.2600,body=+4.1800,vol=551500.0; 2026-08-28:RED:O=99.2800,C=98.8100,body=-0.4700,vol=463500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.914 on 2026-08-28: today_vol=463500 / avg20=506865 (avg window 2026-07-31→2026-08-27, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.421 on 2026-08-28 (price=98.8100, mid=95.7315, upper=103.0379, lower=88.4251; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-28: price=98.8100 vs SMA50=90.2914 dist=+9.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-28: SMA20=95.7315 SMA50=90.2914 SMA80=86.0894 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-28 (63 bars); S1[2026-05-28→2026-06-26] low=2026-05-29@70.9451; S2[2026-06-29→2026-07-30] low=2026-06-30@75.0500; S3[2026-07-31→2026-08-28] low=2026-08-11@86.1300 | lows=[70.94506887640667, 75.05000305175781, 86.12999725341797] span=21.40% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: GREEN body_frac=0.7219352594392454 wick_frac=0.2780647405607546 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-27+2026-08-28: RED body_frac=0.16845962427192432 wick_frac=0.8315403757280757 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.893594571781053 need>1.4; red_wick_gt_green=False 5d trail=2026-08-24:GREEN:body=+0.9400:wick=1.1900; 2026-08-25:RED:body=-0.4000:wick=1.3450; 2026-08-26:RED:body=-3.6700:wick=0.5300; 2026-08-27:GREEN:body=+4.1800:wick=1.6100; 2026-08-28:RED:body=-0.4700:wick=2.3200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.4 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.5 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1259.47 | **NEUTRAL** |
| `B04_income` | 779.12 | **GOOD** |
| `B05_profit_margin` | 61.86 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 101.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=101.67 vs prior_export=101.67 on finviz_2026-08-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -1.29 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.29 vs prior=-1.29 on finviz_2026-08-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.09 | **GOOD** |
| `B13_short_float` | 6.51 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.4 (this export) | prior_export=6.4 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.5 (this export) | prior_export=6.5 (finviz_2026-08-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-31_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.