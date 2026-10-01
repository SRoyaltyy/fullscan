# A+B1 Feature Checklist — 2026-10-01

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-01.csv` · prior export for Δ: `2026-09-30`
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
| 1 | FPI | +17 | 18 | 1 | 2026-09-28→2026-09-29 | REIT - Specialty |
| 2 | GMAB | +17 | 17 | 0 | 2026-09-28→2026-09-29 | Biotechnology |
| 3 | ATRC | +16 | 17 | 1 | 2026-09-28→2026-09-29 | Medical Instruments & Supplies |
| 4 | SFL | +16 | 16 | 0 | 2026-09-28→2026-09-29 | Marine Shipping |
| 5 | PBR-A | +16 | 16 | 0 | 2026-09-28→2026-09-29 | Oil & Gas Integrated |
| 6 | AVPT | +16 | 17 | 1 | 2026-09-28→2026-09-29 | Software - Infrastructure |
| 7 | KBR | +16 | 17 | 1 | 2026-09-28→2026-09-29 | Engineering & Construction |
| 8 | CRMD | +15 | 15 | 0 | 2026-09-28→2026-09-29 | Biotechnology |
| 9 | TWLO | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Software - Infrastructure |
| 10 | FLS | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Specialty Industrial Machinery |
| 11 | FUTU | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Capital Markets |
| 12 | PBR | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Oil & Gas Integrated |
| 13 | PCRX | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Drug Manufacturers - Specialty & Gen |
| 14 | PM | +15 | 15 | 0 | 2026-09-28→2026-09-29 | Tobacco |
| 15 | ASC | +15 | 16 | 1 | 2026-09-28→2026-09-29 | Marine Shipping |

## Full checklist — top 15

### FPI  ·  score **+17**  ·  REIT - Specialty
price=11.09000015258789  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.71 on 2026-09-29; prev RSI=56.22 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.22@2026-09-28 → 65.71@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.22@2026-09-28 → 65.71@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.22@2026-09-28 → 65.71@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=3.556 (G=0.3200 R=0.0900); 2026-09-28:RED:O=10.8300,C=10.7400,body=-0.0900,vol=478889.0; 2026-09-29:GREEN:O=10.7700,C=11.0900,body=+0.3200,vol=1150391.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=2.402 (Gvol=1150391 Rvol=478889); 2026-09-28:RED:O=10.8300,C=10.7400,body=-0.0900,vol=478889.0; 2026-09-29:GREEN:O=10.7700,C=11.0900,body=+0.3200,vol=1150391.0 | **GOOD** |
| `A07_rvol` | RVOL=1.852 on 2026-09-29: today_vol=1150391 / avg20=621067 (avg window 2026-08-31→2026-09-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.056 on 2026-09-29 (price=11.0900, mid=10.7610, upper=11.0727, lower=10.4493; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-29: price=11.0900 vs SMA50=10.2174 dist=+8.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=10.7610 SMA50=10.2174 SMA80=10.0406 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-29→2026-09-29 (63 bars); S1[2026-06-29→2026-07-30] low=2026-07-30@9.2100; S2[2026-07-31→2026-08-28] low=2026-07-31@9.2850; S3[2026-08-31→2026-09-29] low=2026-08-31@10.1300 | lows=[9.210000038146973, 9.28499984741211, 10.130000114440918] span=9.99% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.9014087533983086 wick_frac=0.09859124660169136 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.4864862078386696 wick_frac=0.5135137921613304 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5555461365659307 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.0700:wick=0.1000; 2026-09-24:RED:body=-0.1400:wick=0.1790; 2026-09-25:GREEN:body=+0.1400:wick=0.1000; 2026-09-28:RED:body=-0.0900:wick=0.0950; 2026-09-29:GREEN:body=+0.3200:wick=0.0350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=366.67 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 51.47 | **NEUTRAL** |
| `B04_income` | 24.17 | **GOOD** |
| `B05_profit_margin` | 46.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.5 vs prior_export=10.5 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 9.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=366.67 (this export) | prior_export=366.67 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+17**  ·  Biotechnology
price=35.56999969482422  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.29 on 2026-09-29; prev RSI=65.97 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.97@2026-09-28 → 67.29@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.97@2026-09-28 → 67.29@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.97@2026-09-28 → 67.29@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-09-28:GREEN:O=34.7600,C=35.3600,body=+0.6000,vol=2794568.0; 2026-09-29:GREEN:O=34.9800,C=35.5700,body=+0.5900,vol=3827540.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6622108 Rvol=0); 2026-09-28:GREEN:O=34.7600,C=35.3600,body=+0.6000,vol=2794568.0; 2026-09-29:GREEN:O=34.9800,C=35.5700,body=+0.5900,vol=3827540.0 | **GOOD** |
| `A07_rvol` | RVOL=1.848 on 2026-09-29: today_vol=3827540 / avg20=2071437 (avg window 2026-08-31→2026-09-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.707 on 2026-09-29 (price=35.5700, mid=34.1500, upper=36.1582, lower=32.1418; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=35.5700 vs SMA50=32.2892 dist=+10.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=34.1500 SMA50=32.2892 SMA80=30.1025 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-06-26@25.3350; S2[2026-07-30→2026-08-28] low=2026-07-30@28.0300; S3[2026-08-31→2026-09-29] low=2026-09-10@32.2100 | lows=[25.334999084472656, 28.030000686645508, 32.209999084472656] span=27.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.6737084163416158 wick_frac=0.3262915836583842 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-23:RED:body=-0.6400:wick=0.4150; 2026-09-24:GREEN:body=+0.1400:wick=0.5300; 2026-09-25:GREEN:body=+0.3200:wick=0.4500; 2026-09-28:GREEN:body=+0.6000:wick=0.4700; 2026-09-29:GREEN:body=+0.5900:wick=0.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.39 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.060000000000002274 (now=39.39 vs prior_export=39.33 on finviz_2026-09-30) | **GOOD** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATRC  ·  score **+16**  ·  Medical Instruments & Supplies
price=58.04999923706055  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.73 on 2026-09-29; prev RSI=68.64 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.64@2026-09-28 → 66.73@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.64@2026-09-28 → 66.73@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.64@2026-09-28 → 66.73@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=1.604 (G=0.8500 R=0.5300); 2026-09-28:GREEN:O=57.5600,C=58.4100,body=+0.8500,vol=1161755.0; 2026-09-29:RED:O=58.5800,C=58.0500,body=-0.5300,vol=876275.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=1.326 (Gvol=1161755 Rvol=876275); 2026-09-28:GREEN:O=57.5600,C=58.4100,body=+0.8500,vol=1161755.0; 2026-09-29:RED:O=58.5800,C=58.0500,body=-0.5300,vol=876275.0 | **GOOD** |
| `A07_rvol` | RVOL=0.562 on 2026-09-29: today_vol=876275 / avg20=1558998 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.323 on 2026-09-29 (price=58.0500, mid=55.9420, upper=62.4741, lower=49.4099; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=58.0500 vs SMA50=47.8232 dist=+21.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=55.9420 SMA50=47.8232 SMA80=41.0178 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-06-29@27.2400; S2[2026-07-30→2026-08-28] low=2026-08-05@37.9800; S3[2026-08-31→2026-09-29] low=2026-08-31@46.6100 | lows=[27.239999771118164, 37.97999954223633, 46.61000061035156] span=71.11% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.534590183993148 wick_frac=0.46540981600685205 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.2150111810085347 wick_frac=0.7849888189914653 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6037628565464923 need>1.4; red_wick_gt_green=False 5d trail=2026-09-23:GREEN:body=+1.0400:wick=1.7200; 2026-09-24:GREEN:body=+1.1400:wick=1.4300; 2026-09-25:RED:body=-2.4700:wick=0.9500; 2026-09-28:GREEN:body=+0.8500:wick=0.7400; 2026-09-29:RED:body=-0.5300:wick=1.9350 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10688.24 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 569.62 | **NEUTRAL** |
| `B04_income` | 10.55 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.33 vs prior_export=53.33 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -3.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.009999999999999787 (now=-3.89 vs prior=-3.9 on finviz_2026-09-30) | **GOOD** |
| `B12_institutional_transactions` | 4.44 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10688.24 (this export) | prior_export=10688.24 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SFL  ·  score **+16**  ·  Marine Shipping
price=13.0600004196167  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.07 on 2026-09-29; prev RSI=51.03 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.03@2026-09-28 → 55.07@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.03@2026-09-28 → 55.07@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.03@2026-09-28 → 55.07@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=3.000 (G=0.3600 R=0.1200); 2026-09-28:RED:O=12.9500,C=12.8300,body=-0.1200,vol=1020380.0; 2026-09-29:GREEN:O=12.7000,C=13.0600,body=+0.3600,vol=1198965.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=1.175 (Gvol=1198965 Rvol=1020380); 2026-09-28:RED:O=12.9500,C=12.8300,body=-0.1200,vol=1020380.0; 2026-09-29:GREEN:O=12.7000,C=13.0600,body=+0.3600,vol=1198965.0 | **GOOD** |
| `A07_rvol` | RVOL=0.764 on 2026-09-29: today_vol=1198965 / avg20=1568844 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.125 on 2026-09-29 (price=13.0600, mid=12.9605, upper=13.7553, lower=12.1657; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=13.0600 vs SMA50=12.4304 dist=+5.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=12.9605 SMA50=12.4304 SMA80=11.8898 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-06-30@10.0700; S2[2026-07-30→2026-08-28] low=2026-08-12@11.4300; S3[2026-08-31→2026-09-29] low=2026-09-09@12.2800 | lows=[10.069999694824219, 11.430000305175781, 12.279999732971191] span=21.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.6793751349794831 wick_frac=0.3206248650205169 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.358208572819431 wick_frac=0.6417914271805689 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.000007947293549 need>1.4; red_wick_gt_green=False 5d trail=2026-09-23:RED:body=-0.0900:wick=0.2700; 2026-09-24:GREEN:body=+0.2000:wick=0.2700; 2026-09-25:RED:body=-0.0800:wick=0.2100; 2026-09-28:RED:body=-0.1200:wick=0.2150; 2026-09-29:GREEN:body=+0.3600:wick=0.1699 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=84.93 (current export asof; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.75 (current export; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 728.96 | **NEUTRAL** |
| `B04_income` | 63.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.32 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.32 vs prior_export=13.32 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.75 | **GOOD** |
| `B13_short_float` | 3.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=84.93 (this export) | prior_export=84.93 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.75 (this export) | prior_export=11.75 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR-A  ·  score **+16**  ·  Oil & Gas Integrated
price=18.790000915527344  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.54 on 2026-09-29; prev RSI=54.90 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.90@2026-09-28 → 56.54@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.90@2026-09-28 → 56.54@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.90@2026-09-28 → 56.54@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3600 R=0.0000); 2026-09-28:DOJI:O=18.6500,C=18.6500,body=+0.0000,vol=6905117.0; 2026-09-29:GREEN:O=18.4300,C=18.7900,body=+0.3600,vol=6896269.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=2.997 (Gvol=10348828 Rvol=3452558); 2026-09-28:DOJI:O=18.6500,C=18.6500,body=+0.0000,vol=6905117.0; 2026-09-29:GREEN:O=18.4300,C=18.7900,body=+0.3600,vol=6896269.0 | **GOOD** |
| `A07_rvol` | RVOL=0.927 on 2026-09-29: today_vol=6896269 / avg20=7435336 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.105 on 2026-09-29 (price=18.7900, mid=18.8580, upper=19.5047, lower=18.2113; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=18.7900 vs SMA50=17.4500 dist=+7.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=18.8580 SMA50=17.4500 SMA80=16.6820 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-09-29 (63 bars); S1[2026-07-01→2026-07-30] low=2026-07-01@14.4100; S2[2026-07-31→2026-08-28] low=2026-08-13@15.8400; S3[2026-08-31→2026-09-29] low=2026-08-31@17.1100 | lows=[14.40999984741211, 15.84000015258789, 17.110000610351562] span=18.74% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.7826115801170949 wick_frac=0.21738841988290514 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.0300:wick=0.3800; 2026-09-24:RED:body=-0.2300:wick=0.2100; 2026-09-25:RED:body=-0.1350:wick=0.1700; 2026-09-28:DOJI:body=+0.0000:wick=0.2900; 2026-09-29:GREEN:body=+0.3600:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.94 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.74 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.51 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.51 vs prior_export=22.51 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.42 | **GOOD** |
| `B13_short_float` | 0.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.94 (this export) | prior_export=12.94 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.74 (this export) | prior_export=6.74 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+16**  ·  Software - Infrastructure
price=13.569999694824219  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.86 on 2026-09-29; prev RSI=56.56 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.56@2026-09-28 → 54.86@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.56@2026-09-28 → 54.86@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.56@2026-09-28 → 54.86@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=21.750 (G=0.8700 R=0.0400); 2026-09-28:GREEN:O=12.8000,C=13.6700,body=+0.8700,vol=2682870.0; 2026-09-29:RED:O=13.6100,C=13.5700,body=-0.0400,vol=1982316.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=1.353 (Gvol=2682870 Rvol=1982316); 2026-09-28:GREEN:O=12.8000,C=13.6700,body=+0.8700,vol=2682870.0; 2026-09-29:RED:O=13.6100,C=13.5700,body=-0.0400,vol=1982316.0 | **GOOD** |
| `A07_rvol` | RVOL=1.125 on 2026-09-29: today_vol=1982316 / avg20=1762573 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.489 on 2026-09-29 (price=13.5700, mid=13.2870, upper=13.8656, lower=12.7084; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=13.5700 vs SMA50=13.1979 dist=+2.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=13.2870 SMA50=13.1979 SMA80=12.5319 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-29 (63 bars); S1[2026-06-30→2026-07-29] low=2026-06-30@10.8500; S2[2026-07-30→2026-08-28] low=2026-08-07@12.4100; S3[2026-08-31→2026-09-29] low=2026-09-11@12.6100 | lows=[10.850000381469727, 12.40999984741211, 12.609999656677246] span=16.22% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.828571921627896 wick_frac=0.171428078372104 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.06611568945188118 wick_frac=0.9338843105481188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=21.750017881410486 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.0200:wick=0.2950; 2026-09-24:GREEN:body=+0.0800:wick=0.2000; 2026-09-25:RED:body=-0.1500:wick=0.1000; 2026-09-28:GREEN:body=+0.8700:wick=0.1800; 2026-09-29:RED:body=-0.0400:wick=0.5650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=101.09 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466.15 | **NEUTRAL** |
| `B04_income` | 71.48 | **GOOD** |
| `B05_profit_margin` | 15.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.18 vs prior=-0.18 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.33 | **GOOD** |
| `B13_short_float` | 7.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=101.09 (this export) | prior_export=101.09 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+16**  ·  Engineering & Construction
price=35.130001068115234  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=41.12 on 2026-09-29; prev RSI=38.22 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 38.22@2026-09-28 → 41.12@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 38.22@2026-09-28 → 41.12@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 38.22@2026-09-28 → 41.12@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=6.625 (G=0.5300 R=0.0800); 2026-09-28:RED:O=34.9200,C=34.8400,body=-0.0800,vol=1938309.0; 2026-09-29:GREEN:O=34.6000,C=35.1300,body=+0.5300,vol=2185261.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=1.127 (Gvol=2185261 Rvol=1938309); 2026-09-28:RED:O=34.9200,C=34.8400,body=-0.0800,vol=1938309.0; 2026-09-29:GREEN:O=34.6000,C=35.1300,body=+0.5300,vol=2185261.0 | **GOOD** |
| `A07_rvol` | RVOL=1.631 on 2026-09-29: today_vol=2185261 / avg20=1339813 (avg window 2026-08-31→2026-09-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.544 on 2026-09-29 (price=35.1300, mid=36.1295, upper=37.9658, lower=34.2932; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-09-29: price=35.1300 vs SMA50=36.8040 dist=-4.55% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=36.13_50=36.80_80=36.08 on 2026-09-29: SMA20=36.1295 SMA50=36.8040 SMA80=36.0844 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-06-26@32.1800; S2[2026-07-30→2026-08-28] low=2026-07-30@32.1400; S3[2026-08-31→2026-09-29] low=2026-09-25@33.8800 | lows=[32.18000030517578, 32.13999938964844, 33.880001068115234] span=5.41% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.5120798764553901 wick_frac=0.48792012354460984 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.08080625149312967 wick_frac=0.9191937485068703 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.625196700205045 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:GREEN:body=+0.3400:wick=0.7800; 2026-09-24:RED:body=-0.5000:wick=0.8800; 2026-09-25:GREEN:body=+0.0300:wick=0.7375; 2026-09-28:RED:body=-0.0800:wick=0.9100; 2026-09-29:GREEN:body=+0.5300:wick=0.5050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.17 vs prior_export=44.17 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.86 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRMD  ·  score **+15**  ·  Biotechnology
price=7.929999828338623  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.61 on 2026-09-29; prev RSI=48.22 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.22@2026-09-28 → 49.61@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 48.22@2026-09-28 → 49.61@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 48.22@2026-09-28 → 49.61@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1380 R=0.0000); 2026-09-28:GREEN:O=7.7720,C=7.8900,body=+0.1180,vol=483750.0; 2026-09-29:GREEN:O=7.9100,C=7.9300,body=+0.0200,vol=817285.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1301035 Rvol=0); 2026-09-28:GREEN:O=7.7720,C=7.8900,body=+0.1180,vol=483750.0; 2026-09-29:GREEN:O=7.9100,C=7.9300,body=+0.0200,vol=817285.0 | **GOOD** |
| `A07_rvol` | RVOL=0.956 on 2026-09-29: today_vol=817285 / avg20=855336 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.093 on 2026-09-29 (price=7.9300, mid=7.9825, upper=8.5464, lower=7.4186; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=7.9300 vs SMA50=7.9124 dist=+0.22% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=7.98_50=7.91_80=8.12 on 2026-09-29: SMA20=7.9825 SMA50=7.9124 SMA80=8.1229 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-07-29@7.1300; S2[2026-07-30→2026-08-28] low=2026-08-12@7.1000; S3[2026-08-31→2026-09-29] low=2026-09-18@7.6200 | lows=[7.130000114440918, 7.099999904632568, 7.619999885559082] span=7.32% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.5323652565229701 wick_frac=0.4676347434770299 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.3400:wick=0.2200; 2026-09-24:GREEN:body=+0.0700:wick=0.1000; 2026-09-25:GREEN:body=+0.0600:wick=0.1500; 2026-09-28:GREEN:body=+0.1180:wick=0.0100; 2026-09-29:GREEN:body=+0.0200:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=21.85 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.13 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 462.25 | **NEUTRAL** |
| `B04_income` | 187.17 | **GOOD** |
| `B05_profit_margin` | 40.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.33 vs prior_export=15.33 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 22.67 | **GOOD** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=21.85 (this export) | prior_export=21.85 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.13 (this export) | prior_export=6.13 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TWLO  ·  score **+15**  ·  Software - Infrastructure
price=290.7799987792969  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.85 on 2026-09-29; prev RSI=65.77 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.77@2026-09-28 → 66.85@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.77@2026-09-28 → 66.85@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.77@2026-09-28 → 66.85@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=24.1800 R=0.0000); 2026-09-28:GREEN:O=267.8500,C=286.9800,body=+19.1300,vol=2726244.0; 2026-09-29:GREEN:O=285.7300,C=290.7800,body=+5.0500,vol=2772902.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5499146 Rvol=0); 2026-09-28:GREEN:O=267.8500,C=286.9800,body=+19.1300,vol=2726244.0; 2026-09-29:GREEN:O=285.7300,C=290.7800,body=+5.0500,vol=2772902.0 | **GOOD** |
| `A07_rvol` | RVOL=1.170 on 2026-09-29: today_vol=2772902 / avg20=2370311 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.740 on 2026-09-29 (price=290.7800, mid=251.8965, upper=304.4322, lower=199.3608; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=290.7800 vs SMA50=231.1476 dist=+25.80% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=251.8965 SMA50=231.1476 SMA80=221.4619 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-29 (63 bars); S1[2026-06-30→2026-07-30] low=2026-07-23@179.6450; S2[2026-07-31→2026-08-28] low=2026-08-06@187.1000; S3[2026-08-31→2026-09-29] low=2026-09-08@221.8600 | lows=[179.64500427246094, 187.10000610351562, 221.86000061035156] span=23.50% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.6559408287800425 wick_frac=0.34405917121995755 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:GREEN:body=+5.7000:wick=6.5550; 2026-09-24:GREEN:body=+13.1600:wick=6.5900; 2026-09-25:RED:body=-10.8800:wick=13.9200; 2026-09-28:GREEN:body=+19.1300:wick=3.6400; 2026-09-29:GREEN:body=+5.0500:wick=5.6550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.06 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5572.33 | **NEUTRAL** |
| `B04_income` | 1148.74 | **GOOD** |
| `B05_profit_margin` | 20.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 267.41 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=267.41 vs prior_export=267.41 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -81.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-81.43 vs prior=-81.43 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 3.53 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.06 (this export) | prior_export=11.06 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.76 (this export) | prior_export=4.76 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+15**  ·  Specialty Industrial Machinery
price=73.80999755859375  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=44.49 on 2026-09-29; prev RSI=45.28 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.28@2026-09-28 → 44.49@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 45.28@2026-09-28 → 44.49@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 45.28@2026-09-28 → 44.49@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=1.686 (G=0.8600 R=0.5100); 2026-09-28:GREEN:O=73.2300,C=74.0900,body=+0.8600,vol=1339822.0; 2026-09-29:RED:O=74.3200,C=73.8100,body=-0.5100,vol=874812.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=1.532 (Gvol=1339822 Rvol=874812); 2026-09-28:GREEN:O=73.2300,C=74.0900,body=+0.8600,vol=1339822.0; 2026-09-29:RED:O=74.3200,C=73.8100,body=-0.5100,vol=874812.0 | **GOOD** |
| `A07_rvol` | RVOL=0.517 on 2026-09-29: today_vol=874812 / avg20=1691463 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.244 on 2026-09-29 (price=73.8100, mid=74.7810, upper=78.7575, lower=70.8045; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-09-29: price=73.8100 vs SMA50=76.2524 dist=-3.20% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=74.78_50=76.25_80=75.58 on 2026-09-29: SMA20=74.7810 SMA50=76.2524 SMA80=75.5839 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-07-20@66.3300; S2[2026-07-30→2026-08-28] low=2026-07-30@73.4000; S3[2026-08-31→2026-09-29] low=2026-09-14@68.3600 | lows=[66.33000183105469, 73.4000015258789, 68.36000061035156] span=10.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.5103823305683342 wick_frac=0.4896176694316659 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=0.4636389488066917 wick_frac=0.5363610511933083 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6862536837853606 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:GREEN:body=+1.4800:wick=0.6800; 2026-09-24:RED:body=-0.2700:wick=1.3100; 2026-09-25:RED:body=-2.4900:wick=1.5600; 2026-09-28:GREEN:body=+0.8600:wick=0.8250; 2026-09-29:RED:body=-0.5100:wick=0.5900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.09999999999999432 (now=89.1 vs prior_export=89.0 on finviz_2026-09-30) | **GOOD** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.32 | **GOOD** |
| `B13_short_float` | 6.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FUTU  ·  score **+15**  ·  Capital Markets
price=112.9800033569336  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.09 on 2026-09-29; prev RSI=48.78 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.78@2026-09-28 → 50.09@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.78@2026-09-28 → 50.09@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.78@2026-09-28 → 50.09@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1400 R=0.0000); 2026-09-28:GREEN:O=111.3600,C=112.2700,body=+0.9100,vol=695417.0; 2026-09-29:GREEN:O=112.7500,C=112.9800,body=+0.2300,vol=809021.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1504438 Rvol=0); 2026-09-28:GREEN:O=111.3600,C=112.2700,body=+0.9100,vol=695417.0; 2026-09-29:GREEN:O=112.7500,C=112.9800,body=+0.2300,vol=809021.0 | **GOOD** |
| `A07_rvol` | RVOL=0.966 on 2026-09-29: today_vol=809021 / avg20=837132 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.124 on 2026-09-29 (price=112.9800, mid=113.9685, upper=121.9434, lower=105.9936; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=112.9800 vs SMA50=111.5248 dist=+1.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=113.9685 SMA50=111.5248 SMA80=105.8607 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-09-29 (63 bars); S1[2026-07-01→2026-07-30] low=2026-07-07@91.8600; S2[2026-07-31→2026-08-28] low=2026-07-31@102.3900; S3[2026-08-31→2026-09-29] low=2026-09-24@108.1000 | lows=[91.86000061035156, 102.38999938964844, 108.0999984741211] span=17.68% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.22671005117407325 wick_frac=0.7732899488259268 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-23:RED:body=-2.8600:wick=0.5870; 2026-09-24:GREEN:body=+0.2600:wick=2.1400; 2026-09-25:GREEN:body=+2.2200:wick=1.1000; 2026-09-28:GREEN:body=+0.9100:wick=1.6150; 2026-09-29:GREEN:body=+0.2300:wick=2.2425 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.72 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.86 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3314.94 | **NEUTRAL** |
| `B04_income` | 1422.92 | **GOOD** |
| `B05_profit_margin` | 42.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.05000000000001137 (now=153.21 vs prior_export=153.16 on finviz_2026-09-30) | **GOOD** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.31 | **GOOD** |
| `B13_short_float` | 5.06 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.72 (this export) | prior_export=11.72 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.86 (this export) | prior_export=16.86 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+15**  ·  Oil & Gas Integrated
price=20.649999618530273  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.61 on 2026-09-29; prev RSI=54.61 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.61@2026-09-28 → 54.61@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.61@2026-09-28 → 54.61@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.61@2026-09-28 → 54.61@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4150 R=0.0000); 2026-09-28:GREEN:O=20.5950,C=20.6500,body=+0.0550,vol=18480341.0; 2026-09-29:GREEN:O=20.2900,C=20.6500,body=+0.3600,vol=16727861.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=35208202 Rvol=0); 2026-09-28:GREEN:O=20.5950,C=20.6500,body=+0.0550,vol=18480341.0; 2026-09-29:GREEN:O=20.2900,C=20.6500,body=+0.3600,vol=16727861.0 | **GOOD** |
| `A07_rvol` | RVOL=0.756 on 2026-09-29: today_vol=16727861 / avg20=22112561 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.249 on 2026-09-29 (price=20.6500, mid=20.8385, upper=21.5964, lower=20.0806; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=20.6500 vs SMA50=19.4206 dist=+6.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=20.8385 SMA50=19.4206 SMA80=18.5831 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-09-29 (63 bars); S1[2026-07-01→2026-07-30] low=2026-07-01@15.8600; S2[2026-07-31→2026-08-28] low=2026-08-27@17.5400; S3[2026-08-31→2026-09-29] low=2026-08-31@18.9500 | lows=[15.859999656677246, 17.540000915527344, 18.950000762939453] span=19.48% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.49819036545669815 wick_frac=0.5018096345433019 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.0100:wick=0.4400; 2026-09-24:RED:body=-0.3000:wick=0.2900; 2026-09-25:RED:body=-0.1350:wick=0.2450; 2026-09-28:GREEN:body=+0.0550:wick=0.3400; 2026-09-29:GREEN:body=+0.3600:wick=0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PCRX  ·  score **+15**  ·  Drug Manufacturers - Specialty & Generic
price=25.649999618530273  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.63 on 2026-09-29; prev RSI=52.55 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.55@2026-09-28 → 54.63@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.55@2026-09-28 → 54.63@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.55@2026-09-28 → 54.63@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9500 R=0.0000); 2026-09-28:GREEN:O=24.7900,C=25.3900,body=+0.6000,vol=478754.0; 2026-09-29:GREEN:O=25.3000,C=25.6500,body=+0.3500,vol=521037.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=999791 Rvol=0); 2026-09-28:GREEN:O=24.7900,C=25.3900,body=+0.6000,vol=478754.0; 2026-09-29:GREEN:O=25.3000,C=25.6500,body=+0.3500,vol=521037.0 | **GOOD** |
| `A07_rvol` | RVOL=0.953 on 2026-09-29: today_vol=521037 / avg20=546471 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.212 on 2026-09-29 (price=25.6500, mid=25.3135, upper=26.9033, lower=23.7237; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=25.6500 vs SMA50=25.2774 dist=+1.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=25.3135 SMA50=25.2774 SMA80=24.8086 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-06-26@24.2900; S2[2026-07-30→2026-08-28] low=2026-08-14@22.7900; S3[2026-08-31→2026-09-29] low=2026-09-15@23.6500 | lows=[24.290000915527344, 22.790000915527344, 23.649999618530273] span=6.58% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.633351660757234 wick_frac=0.36664833924276596 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.3700:wick=0.3300; 2026-09-24:GREEN:body=+0.9800:wick=0.1200; 2026-09-25:RED:body=-0.3800:wick=0.2400; 2026-09-28:GREEN:body=+0.6000:wick=0.3700; 2026-09-29:GREEN:body=+0.3500:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.95 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.7 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 746.16 | **NEUTRAL** |
| `B04_income` | 14.64 | **GOOD** |
| `B05_profit_margin` | 1.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 30.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=30.0 vs prior_export=30.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.11 | **GOOD** |
| `B10_insider_transactions` | -2.58 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.58 vs prior=-2.58 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.97 | **GOOD** |
| `B13_short_float` | 19.8 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.95 (this export) | prior_export=12.95 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.7 (this export) | prior_export=0.7 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+15**  ·  Tobacco
price=193.89999389648438  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.63 on 2026-09-29; prev RSI=57.40 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.40@2026-09-28 → 57.63@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.40@2026-09-28 → 57.63@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.40@2026-09-28 → 57.63@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.1500 R=0.0000); 2026-09-28:GREEN:O=190.5600,C=193.7600,body=+3.2000,vol=3898095.0; 2026-09-29:GREEN:O=191.9500,C=193.9000,body=+1.9500,vol=3598203.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7496298 Rvol=0); 2026-09-28:GREEN:O=190.5600,C=193.7600,body=+3.2000,vol=3898095.0; 2026-09-29:GREEN:O=191.9500,C=193.9000,body=+1.9500,vol=3598203.0 | **GOOD** |
| `A07_rvol` | RVOL=0.818 on 2026-09-29: today_vol=3598203 / avg20=4397164 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.636 on 2026-09-29 (price=193.9000, mid=189.6515, upper=196.3364, lower=182.9666; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=193.9000 vs SMA50=190.1226 dist=+1.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=189.65_50=190.12_80=186.76 on 2026-09-29: SMA20=189.6515 SMA50=190.1226 SMA80=186.7569 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-01→2026-09-29 (63 bars); S1[2026-07-01→2026-07-30] low=2026-07-15@175.7600; S2[2026-07-31→2026-08-28] low=2026-08-12@184.0200; S3[2026-08-31→2026-09-29] low=2026-09-08@181.0100 | lows=[175.75999450683594, 184.02000427246094, 181.00999450683594] span=4.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=0.6945255659073721 wick_frac=0.30547443409262787 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:GREEN:body=+0.7900:wick=3.6600; 2026-09-24:RED:body=-0.9100:wick=2.6600; 2026-09-25:RED:body=-0.0700:wick=3.6080; 2026-09-28:GREEN:body=+3.2000:wick=1.1300; 2026-09-29:GREEN:body=+1.9500:wick=1.0499 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASC  ·  score **+15**  ·  Marine Shipping
price=18.440000534057617  pair=`2026-09-28→2026-09-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.49 on 2026-09-29; prev RSI=51.75 on 2026-09-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.75@2026-09-28 → 55.49@2026-09-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.75@2026-09-28 → 55.49@2026-09-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.75@2026-09-28 → 55.49@2026-09-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4400 R=0.0000); 2026-09-28:DOJI:O=18.1500,C=18.1500,body=+0.0000,vol=934751.0; 2026-09-29:GREEN:O=18.0000,C=18.4400,body=+0.4400,vol=713147.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-28 + 2026-09-29; ratio=GREEN_vol/RED_vol=2.526 (Gvol=1180522 Rvol=467376); 2026-09-28:DOJI:O=18.1500,C=18.1500,body=+0.0000,vol=934751.0; 2026-09-29:GREEN:O=18.0000,C=18.4400,body=+0.4400,vol=713147.0 | **GOOD** |
| `A07_rvol` | RVOL=1.210 on 2026-09-29: today_vol=713147 / avg20=589584 (avg window 2026-08-31→2026-09-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.143 on 2026-09-29 (price=18.4400, mid=18.3070, upper=19.2399, lower=17.3741; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-29: price=18.4400 vs SMA50=17.5698 dist=+4.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-29: SMA20=18.3070 SMA50=17.5698 SMA80=17.0066 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-29 (63 bars); S1[2026-06-26→2026-07-29] low=2026-07-01@13.8100; S2[2026-07-30→2026-08-28] low=2026-08-11@15.9200; S3[2026-08-31→2026-09-29] low=2026-08-31@17.1000 | lows=[13.8100004196167, 15.920000076293945, 17.100000381469727] span=23.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: GREEN body_frac=1.0864192298092183 wick_frac=-0.08641922980921836 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-28+2026-09-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-23:RED:body=-0.2200:wick=0.4500; 2026-09-24:GREEN:body=+0.2500:wick=0.3600; 2026-09-25:GREEN:body=+0.0600:wick=0.4300; 2026-09-28:DOJI:body=+0.0000:wick=0.5449; 2026-09-29:GREEN:body=+0.4400:wick=-0.0350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.72 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=34.7 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 368.29 | **NEUTRAL** |
| `B04_income` | 105.58 | **GOOD** |
| `B05_profit_margin` | 28.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.0 vs prior_export=21.0 on finviz_2026-09-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.93 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.93 vs prior=-0.93 on finviz_2026-09-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 7.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.72 (this export) | prior_export=6.72 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=34.7 (this export) | prior_export=34.7 (finviz_2026-09-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-01_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.