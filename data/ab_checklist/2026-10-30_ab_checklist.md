# A+B1 Feature Checklist — 2026-10-30

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-30.csv` · prior export for Δ: `2026-10-29`
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
| 1 | FLS | +17 | 18 | 1 | 2026-10-28→2026-10-29 | Specialty Industrial Machinery |
| 2 | WMG | +16 | 16 | 0 | 2026-10-28→2026-10-29 | Entertainment |
| 3 | AUPH | +16 | 17 | 1 | 2026-10-28→2026-10-29 | Biotechnology |
| 4 | AME | +16 | 17 | 1 | 2026-10-28→2026-10-29 | Specialty Industrial Machinery |
| 5 | SWKS | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Semiconductors |
| 6 | TJX | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Apparel Retail |
| 7 | ZM | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Software - Application |
| 8 | ASH | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Specialty Chemicals |
| 9 | HSIC | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Medical Distribution |
| 10 | FRSH | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Software - Application |
| 11 | PODD | +15 | 17 | 2 | 2026-10-28→2026-10-29 | Medical Devices |
| 12 | TLYS | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Apparel Retail |
| 13 | EPD | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Oil & Gas Midstream |
| 14 | EPAM | +15 | 15 | 0 | 2026-10-28→2026-10-29 | Information Technology Services |
| 15 | GCT | +14 | 16 | 2 | 2026-10-28→2026-10-29 | Software - Infrastructure |

## Full checklist — top 15

### FLS  ·  score **+17**  ·  Specialty Industrial Machinery
price=80.3499984741211  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.56 on 2026-10-29; prev RSI=44.34 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.34@2026-10-28 → 64.56@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.34@2026-10-28 → 64.56@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.34@2026-10-28 → 64.56@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=20.558 (G=8.8400 R=0.4300); 2026-10-28:RED:O=72.2400,C=71.8100,body=-0.4300,vol=1645300.0; 2026-10-29:GREEN:O=71.5100,C=80.3500,body=+8.8400,vol=3722000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=2.262 (Gvol=3722000 Rvol=1645300); 2026-10-28:RED:O=72.2400,C=71.8100,body=-0.4300,vol=1645300.0; 2026-10-29:GREEN:O=71.5100,C=80.3500,body=+8.8400,vol=3722000.0 | **GOOD** |
| `A07_rvol` | RVOL=2.370 on 2026-10-29: today_vol=3722000 / avg20=1570754 (avg window 2026-09-17→2026-10-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.328 on 2026-10-29 (price=80.3500, mid=73.6885, upper=78.7037, lower=68.6733; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=80.3500 vs SMA50=76.2440 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=73.69_50=76.24_80=75.05 on 2026-10-29: SMA20=73.6885 SMA50=76.2440 SMA80=75.0472 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-29 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-20@66.3300; S2[2026-08-18→2026-09-16] low=2026-09-14@68.3600; S3[2026-09-17→2026-10-29] low=2026-10-08@68.8000 | lows=[66.33000183105469, 68.36000061035156, 68.80000305175781] span=3.72% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=1.0 wick_frac=0.0 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.31159504420081935 wick_frac=0.6884049557991807 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.558116428026473 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+1.4800:wick=2.1500; 2026-10-12:RED:body=-1.0100:wick=0.9600; 2026-10-27:GREEN:body=+0.3800:wick=1.1100; 2026-10-28:RED:body=-0.4300:wick=0.9500; 2026-10-29:GREEN:body=+8.8400:wick=0.0000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.32 | **GOOD** |
| `B13_short_float` | 6.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMG  ·  score **+16**  ·  Entertainment
price=30.469999313354492  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.78 on 2026-10-29; prev RSI=51.11 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.093 (G=0.9000 R=0.4300); 2026-10-28:RED:O=30.0100,C=29.5800,body=-0.4300,vol=1561033.0; 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=2.076 (Gvol=3240998 Rvol=1561033); 2026-10-28:RED:O=30.0100,C=29.5800,body=-0.4300,vol=1561033.0; 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0 | **GOOD** |
| `A07_rvol` | RVOL=1.432 on 2026-10-29: today_vol=3240998 / avg20=2263832 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.271 on 2026-10-29 (price=30.4700, mid=29.6708, upper=32.6176, lower=26.7239; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=30.4700 vs SMA50=28.6298 dist=+6.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=29.6708 SMA50=28.6298 SMA80=28.0813 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-12@24.5150; S2[2026-09-01→2026-09-30] low=2026-09-28@26.4500; S3[2026-10-01→2026-10-29] low=2026-10-05@26.7000 | lows=[24.514999389648438, 26.450000762939453, 26.700000762939453] span=8.91% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6498200066653997 wick_frac=0.3501799933346003 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.477778319370829 wick_frac=0.522221680629171 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.093020883234861 need>1.4; red_wick_gt_green=False 5d trail=2026-10-23:RED:body=-0.5000:wick=0.5200; 2026-10-26:RED:body=-0.4600:wick=0.3050; 2026-10-27:RED:body=-0.2700:wick=0.1900; 2026-10-28:RED:body=-0.4300:wick=0.4700; 2026-10-29:GREEN:body=+0.9000:wick=0.4850 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=17.209999084472656  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.02 on 2026-10-29; prev RSI=66.19 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.19@2026-10-28 → 63.02@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.19@2026-10-28 → 63.02@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.19@2026-10-28 → 63.02@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.261 (G=0.5200 R=0.2300); 2026-10-28:GREEN:O=16.8400,C=17.3600,body=+0.5200,vol=625900.0; 2026-10-29:RED:O=17.4400,C=17.2100,body=-0.2300,vol=744700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=0.840 (Gvol=625900 Rvol=744700); 2026-10-28:GREEN:O=16.8400,C=17.3600,body=+0.5200,vol=625900.0; 2026-10-29:RED:O=17.4400,C=17.2100,body=-0.2300,vol=744700.0 | **BAD** |
| `A07_rvol` | RVOL=0.955 on 2026-10-29: today_vol=744700 / avg20=779694 (avg window 2026-09-17→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.775 on 2026-10-29 (price=17.2100, mid=16.4595, upper=17.4281, lower=15.4909; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=17.2100 vs SMA50=16.2607 dist=+5.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=16.4595 SMA50=16.2607 SMA80=16.1402 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-29 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-31@14.3630; S2[2026-08-18→2026-09-16] low=2026-08-18@15.1500; S3[2026-09-17→2026-10-29] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.149999618530273, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.9629655793582114 wick_frac=0.03703442064178867 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.4791701436075944 wick_frac=0.5208298563924055 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.260857306343138 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-27:GREEN:body=+0.2700:wick=0.0400; 2026-10-28:GREEN:body=+0.5200:wick=0.0200; 2026-10-29:RED:body=-0.2300:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+16**  ·  Specialty Industrial Machinery
price=251.77999877929688  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.95 on 2026-10-29; prev RSI=53.32 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=4.099 (G=3.7300 R=0.9100); 2026-10-28:RED:O=249.0400,C=248.1300,body=-0.9100,vol=1138431.0; 2026-10-29:GREEN:O=248.0500,C=251.7800,body=+3.7300,vol=1840954.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.617 (Gvol=1840954 Rvol=1138431); 2026-10-28:RED:O=249.0400,C=248.1300,body=-0.9100,vol=1138431.0; 2026-10-29:GREEN:O=248.0500,C=251.7800,body=+3.7300,vol=1840954.0 | **GOOD** |
| `A07_rvol` | RVOL=1.477 on 2026-10-29: today_vol=1840954 / avg20=1246038 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.505 on 2026-10-29 (price=251.7800, mid=247.9415, upper=255.5396, lower=240.3434; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=251.7800 vs SMA50=243.4302 dist=+3.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=247.9415 SMA50=243.4302 SMA80=243.4298 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-31@234.3900; S2[2026-09-01→2026-09-30] low=2026-09-01@229.6000; S3[2026-10-01→2026-10-29] low=2026-10-22@234.4300 | lows=[234.38999938964844, 229.60000610351562, 234.42999267578125] span=2.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7159291594790346 wick_frac=0.28407084052096543 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.28173052848389796 wick_frac=0.7182694715161021 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.098948639267569 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+3.8700:wick=2.3900; 2026-10-26:GREEN:body=+0.8200:wick=2.1900; 2026-10-27:GREEN:body=+3.2100:wick=2.0000; 2026-10-28:RED:body=-0.9100:wick=2.3200; 2026-10-29:GREEN:body=+3.7300:wick=1.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+15**  ·  Semiconductors
price=89.43000030517578  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.62 on 2026-10-29; prev RSI=63.06 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.1800 R=0.0000); 2026-10-28:GREEN:O=85.4800,C=87.2300,body=+1.7500,vol=3198881.0; 2026-10-29:GREEN:O=88.0000,C=89.4300,body=+1.4300,vol=4330962.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7529843 Rvol=0); 2026-10-28:GREEN:O=85.4800,C=87.2300,body=+1.7500,vol=3198881.0; 2026-10-29:GREEN:O=88.0000,C=89.4300,body=+1.4300,vol=4330962.0 | **GOOD** |
| `A07_rvol` | RVOL=0.643 on 2026-10-29: today_vol=4330962 / avg20=6734475 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.019 on 2026-10-29 (price=89.4300, mid=81.7130, upper=89.2853, lower=74.1407; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=89.4300 vs SMA50=80.0816 dist=+11.67% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=81.7130 SMA50=80.0816 SMA80=73.8719 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-03@59.9200; S2[2026-09-01→2026-09-30] low=2026-09-01@64.6500; S3[2026-10-01→2026-10-29] low=2026-10-13@73.7600 | lows=[59.91999816894531, 64.6500015258789, 73.76000213623047] span=23.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6065945528108387 wick_frac=0.3934054471891612 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+1.4800:wick=1.2300; 2026-10-26:RED:body=-0.0900:wick=2.1100; 2026-10-27:GREEN:body=+1.7300:wick=0.5750; 2026-10-28:GREEN:body=+1.7500:wick=0.3150; 2026-10-29:GREEN:body=+1.4300:wick=2.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TJX  ·  score **+15**  ·  Apparel Retail
price=141.38999938964844  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.22 on 2026-10-29; prev RSI=45.38 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.475 (G=1.4600 R=0.5900); 2026-10-28:RED:O=138.9800,C=138.3900,body=-0.5900,vol=4065949.0; 2026-10-29:GREEN:O=139.9300,C=141.3900,body=+1.4600,vol=4238070.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.042 (Gvol=4238070 Rvol=4065949); 2026-10-28:RED:O=138.9800,C=138.3900,body=-0.5900,vol=4065949.0; 2026-10-29:GREEN:O=139.9300,C=141.3900,body=+1.4600,vol=4238070.0 | **GOOD** |
| `A07_rvol` | RVOL=0.650 on 2026-10-29: today_vol=4238070 / avg20=6525016 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.039 on 2026-10-29 (price=141.3900, mid=141.7285, upper=150.4207, lower=133.0363; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=141.3900 vs SMA50=135.6918 dist=+4.20% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=141.73_50=135.69_80=142.74 on 2026-10-29: SMA20=141.7285 SMA50=135.6918 SMA80=142.7383 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-31@133.0800; S2[2026-09-01→2026-09-30] low=2026-09-16@122.7800; S3[2026-10-01→2026-10-29] low=2026-10-01@131.4400 | lows=[133.0800018310547, 122.77999877929688, 131.44000244140625] span=8.39% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6854475901197776 wick_frac=0.3145524098802224 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.32113817762015895 wick_frac=0.678861822379841 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.474603010396731 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-1.6400:wick=1.0100; 2026-10-26:RED:body=-1.1300:wick=1.3900; 2026-10-27:RED:body=-2.0700:wick=0.8600; 2026-10-28:RED:body=-0.5900:wick=1.2472; 2026-10-29:GREEN:body=+1.4600:wick=0.6700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.6 (current export asof; earnings_date=8/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.1 (current export; earnings_date=8/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 62363.0 | **NEUTRAL** |
| `B04_income` | 6067.0 | **GOOD** |
| `B05_profit_margin` | 9.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 171.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=171.58 vs prior_export=171.58 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.68 | **GOOD** |
| `B10_insider_transactions` | -8.46 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.46 vs prior=-8.46 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 1.68 | **NEUTRAL** |
| `B14_earnings_date` | 8/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.6 (this export) | prior_export=2.6 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.1 (this export) | prior_export=0.1 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZM  ·  score **+15**  ·  Software - Application
price=97.66000366210938  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.76 on 2026-10-29; prev RSI=58.36 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=4.500 (G=3.6000 R=0.8000); 2026-10-28:GREEN:O=96.6600,C=100.2600,body=+3.6000,vol=2347456.0; 2026-10-29:RED:O=98.4600,C=97.6600,body=-0.8000,vol=1863368.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.260 (Gvol=2347456 Rvol=1863368); 2026-10-28:GREEN:O=96.6600,C=100.2600,body=+3.6000,vol=2347456.0; 2026-10-29:RED:O=98.4600,C=97.6600,body=-0.8000,vol=1863368.0 | **GOOD** |
| `A07_rvol` | RVOL=0.700 on 2026-10-29: today_vol=1863368 / avg20=2663389 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.266 on 2026-10-29 (price=97.6600, mid=96.5485, upper=100.7258, lower=92.3712; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=97.6600 vs SMA50=95.7775 dist=+1.97% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=96.55_50=95.78_80=96.11 on 2026-10-29: SMA20=96.5485 SMA50=95.7775 SMA80=96.1128 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-26@92.7200; S2[2026-09-01→2026-09-30] low=2026-09-28@85.4900; S3[2026-10-01→2026-10-29] low=2026-10-01@90.5800 | lows=[92.72000122070312, 85.48999786376953, 90.58000183105469] span=8.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.8181811876535632 wick_frac=0.18181881234643682 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.2816881355021558 wick_frac=0.7183118644978441 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.500023841994335 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.2300:wick=2.2313; 2026-10-26:GREEN:body=+2.2700:wick=0.5400; 2026-10-27:RED:body=-0.2200:wick=2.0100; 2026-10-28:GREEN:body=+3.6000:wick=0.8000; 2026-10-29:RED:body=-0.8000:wick=2.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.83 (current export asof; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.83 (current export; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4993.05 | **NEUTRAL** |
| `B04_income` | 3255.07 | **GOOD** |
| `B05_profit_margin` | 65.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 118.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=118.17 vs prior_export=118.17 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.01 vs prior=-1.01 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.1 | **GOOD** |
| `B13_short_float` | 2.69 | **NEUTRAL** |
| `B14_earnings_date` | 8/25/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.83 (this export) | prior_export=4.83 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.83 (this export) | prior_export=0.83 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASH  ·  score **+15**  ·  Specialty Chemicals
price=74.37999725341797  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.34 on 2026-10-29; prev RSI=62.08 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.08@2026-10-28 → 68.34@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.08@2026-10-28 → 68.34@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.08@2026-10-28 → 68.34@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.280 (G=1.1400 R=0.5000); 2026-10-28:RED:O=73.1700,C=72.6700,body=-0.5000,vol=374900.0; 2026-10-29:GREEN:O=73.2400,C=74.3800,body=+1.1400,vol=545400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.455 (Gvol=545400 Rvol=374900); 2026-10-28:RED:O=73.1700,C=72.6700,body=-0.5000,vol=374900.0; 2026-10-29:GREEN:O=73.2400,C=74.3800,body=+1.1400,vol=545400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.871 on 2026-10-29: today_vol=545400 / avg20=626516 (avg window 2026-09-17→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.284 on 2026-10-29 (price=74.3800, mid=69.9900, upper=73.4092, lower=66.5708; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=74.3800 vs SMA50=71.8378 dist=+3.54% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=69.99_50=71.84_80=70.12 on 2026-10-29: SMA20=69.9900 SMA50=71.8378 SMA80=70.1211 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-29 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-27@65.6300; S2[2026-08-18→2026-09-16] low=2026-09-11@67.7500; S3[2026-09-17→2026-10-29] low=2026-10-01@67.0000 | lows=[65.62999725341797, 67.75, 67.0] span=3.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7402589038449153 wick_frac=0.25974109615508467 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.2994015258805793 wick_frac=0.7005984741194207 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.279998779296875 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.0100:wick=1.1050; 2026-10-12:GREEN:body=+0.6200:wick=0.7050; 2026-10-27:GREEN:body=+0.5200:wick=0.6800; 2026-10-28:RED:body=-0.5000:wick=1.1700; 2026-10-29:GREEN:body=+1.1400:wick=0.4000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.55 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1843.0 | **NEUTRAL** |
| `B04_income` | 52.0 | **GOOD** |
| `B05_profit_margin` | 2.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=80.18 vs prior_export=80.18 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.08 | **GOOD** |
| `B13_short_float` | 9.74 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.55 (this export) | prior_export=3.55 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSIC  ·  score **+15**  ·  Medical Distribution
price=86.44000244140625  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.82 on 2026-10-29; prev RSI=46.81 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.81@2026-10-28 → 53.82@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.81@2026-10-28 → 53.82@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.81@2026-10-28 → 53.82@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6500 R=0.0000); 2026-10-28:GREEN:O=84.1000,C=84.9600,body=+0.8600,vol=934500.0; 2026-10-29:GREEN:O=85.6500,C=86.4400,body=+0.7900,vol=924700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1859200 Rvol=0); 2026-10-28:GREEN:O=84.1000,C=84.9600,body=+0.8600,vol=934500.0; 2026-10-29:GREEN:O=85.6500,C=86.4400,body=+0.7900,vol=924700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.708 on 2026-10-29: today_vol=924700 / avg20=1305707 (avg window 2026-09-17→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.752 on 2026-10-29 (price=86.4400, mid=85.0205, upper=86.9093, lower=83.1317; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-29: price=86.4400 vs SMA50=87.3826 dist=-1.08% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=85.02_50=87.38_80=86.52 on 2026-10-29: SMA20=85.0205 SMA50=87.3826 SMA80=86.5206 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-29 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@83.1900; S2[2026-08-18→2026-09-16] low=2026-09-16@86.4400; S3[2026-09-17→2026-10-29] low=2026-10-08@79.3700 | lows=[83.19000244140625, 86.44000244140625, 79.37000274658203] span=8.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.5951973533449653 wick_frac=0.4048026466550346 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1000:wick=1.5150; 2026-10-12:GREEN:body=+0.0700:wick=0.9600; 2026-10-27:RED:body=-0.3700:wick=1.4800; 2026-10-28:GREEN:body=+0.8600:wick=0.7400; 2026-10-29:GREEN:body=+0.7900:wick=0.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.45 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13602.0 | **NEUTRAL** |
| `B04_income` | 403.0 | **GOOD** |
| `B05_profit_margin` | 2.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.64 vs prior_export=99.64 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.49 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+15**  ·  Software - Application
price=15.010000228881836  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.04 on 2026-10-29; prev RSI=66.75 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6900 R=0.0000); 2026-10-28:GREEN:O=14.6900,C=15.3800,body=+0.6900,vol=5075048.0; 2026-10-29:DOJI:O=15.0100,C=15.0100,body=+0.0000,vol=3723214.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=3.726 (Gvol=6936655 Rvol=1861607); 2026-10-28:GREEN:O=14.6900,C=15.3800,body=+0.6900,vol=5075048.0; 2026-10-29:DOJI:O=15.0100,C=15.0100,body=+0.0000,vol=3723214.0 | **GOOD** |
| `A07_rvol` | RVOL=0.514 on 2026-10-29: today_vol=3723214 / avg20=7240706 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.456 on 2026-10-29 (price=15.0100, mid=14.2842, upper=15.8766, lower=12.6919; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=15.0100 vs SMA50=13.3955 dist=+12.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=14.2842 SMA50=13.3955 SMA80=12.6164 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-06@10.8720; S2[2026-09-01→2026-09-30] low=2026-09-10@11.7040; S3[2026-10-01→2026-10-29] low=2026-10-02@12.6914 | lows=[10.871999740600586, 11.704000473022461, 12.691399574279785] span=16.73% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.9078951661275062 wick_frac=0.09210483387249378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+0.2000:wick=0.1850; 2026-10-26:RED:body=-0.1000:wick=0.2450; 2026-10-27:RED:body=-0.3100:wick=0.2300; 2026-10-28:GREEN:body=+0.6900:wick=0.0700; 2026-10-29:DOJI:body=+0.0000:wick=0.5700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.64 vs prior_export=14.64 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.42 | **GOOD** |
| `B13_short_float` | 11.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+15**  ·  Medical Devices
price=152.5  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.17 on 2026-10-29; prev RSI=65.60 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.60@2026-10-28 → 69.17@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.60@2026-10-28 → 69.17@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.60@2026-10-28 → 69.17@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=5.600 (G=1.9600 R=0.3500); 2026-10-28:RED:O=149.2800,C=148.9300,body=-0.3500,vol=1035200.0; 2026-10-29:GREEN:O=150.5400,C=152.5000,body=+1.9600,vol=1296800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.253 (Gvol=1296800 Rvol=1035200); 2026-10-28:RED:O=149.2800,C=148.9300,body=-0.3500,vol=1035200.0; 2026-10-29:GREEN:O=150.5400,C=152.5000,body=+1.9600,vol=1296800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.855 on 2026-10-29: today_vol=1296800 / avg20=1516039 (avg window 2026-09-17→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.184 on 2026-10-29 (price=152.5000, mid=137.6670, upper=150.1906, lower=125.1434; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=152.5000 vs SMA50=141.2026 dist=+8.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-29: SMA20=137.6670 SMA50=141.2026 SMA80=147.7354 | **BAD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-29 (63 bars); S1[2026-07-15→2026-08-17] low=2026-08-05@126.4000; S2[2026-08-18→2026-09-16] low=2026-09-11@131.2400; S3[2026-09-17→2026-10-29] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.9245269438666446 wick_frac=0.0754730561333554 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.08883398137964153 wick_frac=0.9111660186203585 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.599921527596129 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650; 2026-10-27:GREEN:body=+1.7600:wick=2.0500; 2026-10-28:RED:body=-0.3500:wick=3.5900; 2026-10-29:GREEN:body=+1.9600:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TLYS  ·  score **+15**  ·  Apparel Retail
price=4.340000152587891  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.56 on 2026-10-29; prev RSI=48.20 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.1000 R=0.0200); 2026-10-28:GREEN:O=4.2600,C=4.3600,body=+0.1000,vol=103707.0; 2026-10-29:RED:O=4.3600,C=4.3400,body=-0.0200,vol=83313.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.245 (Gvol=103707 Rvol=83313); 2026-10-28:GREEN:O=4.2600,C=4.3600,body=+0.1000,vol=103707.0; 2026-10-29:RED:O=4.3600,C=4.3400,body=-0.0200,vol=83313.0 | **GOOD** |
| `A07_rvol` | RVOL=0.596 on 2026-10-29: today_vol=83313 / avg20=139831 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.421 on 2026-10-29 (price=4.3400, mid=4.4780, upper=4.8055, lower=4.1505; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=4.3400 vs SMA50=4.3194 dist=+0.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=4.4780 SMA50=4.3194 SMA80=4.2071 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-18@3.6000; S2[2026-09-01→2026-09-30] low=2026-09-02@3.8000; S3[2026-10-01→2026-10-29] low=2026-10-26@4.1100 | lows=[3.5999999046325684, 3.799999952316284, 4.110000133514404] span=14.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.769229358471188 wick_frac=0.23077064152881194 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.1333331214058422 wick_frac=0.8666668785941578 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.3200:wick=0.0700; 2026-10-26:RED:body=-0.0800:wick=0.1500; 2026-10-27:GREEN:body=+0.1000:wick=0.0150; 2026-10-28:GREEN:body=+0.1000:wick=0.0300; 2026-10-29:RED:body=-0.0200:wick=0.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=63.64 (current export asof; earnings_date=9/2/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.14 (current export; earnings_date=9/2/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 582.94 | **NEUTRAL** |
| `B04_income` | 1.96 | **GOOD** |
| `B05_profit_margin` | 0.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 6.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=6.65 vs prior_export=6.65 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -21.5 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.5 vs prior=-21.5 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.73 | **GOOD** |
| `B13_short_float` | 3.32 | **NEUTRAL** |
| `B14_earnings_date` | 9/2/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=63.64 (this export) | prior_export=63.64 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.14 (this export) | prior_export=4.14 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPD  ·  score **+15**  ·  Oil & Gas Midstream
price=39.04999923706055  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.70 on 2026-10-29; prev RSI=56.53 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.53@2026-10-28 → 62.70@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.53@2026-10-28 → 62.70@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.53@2026-10-28 → 62.70@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.3700 R=0.0000); 2026-10-28:GREEN:O=37.2100,C=38.0600,body=+0.8500,vol=6082200.0; 2026-10-29:GREEN:O=38.5300,C=39.0500,body=+0.5200,vol=6866300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12948500 Rvol=0); 2026-10-28:GREEN:O=37.2100,C=38.0600,body=+0.8500,vol=6082200.0; 2026-10-29:GREEN:O=38.5300,C=39.0500,body=+0.5200,vol=6866300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.962 on 2026-10-29: today_vol=6866300 / avg20=3500487 (avg window 2026-09-17→2026-10-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.968 on 2026-10-29 (price=39.0500, mid=37.0540, upper=39.1151, lower=34.9929; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=39.0500 vs SMA50=37.9844 dist=+2.81% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=37.05_50=37.98_80=37.70 on 2026-10-29: SMA20=37.0540 SMA50=37.9844 SMA80=37.7038 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-29 (63 bars); S1[2026-07-20→2026-08-17] low=2026-07-31@37.2200; S2[2026-08-18→2026-09-16] low=2026-08-21@37.9600; S3[2026-09-17→2026-10-29] low=2026-10-01@35.3700 | lows=[37.220001220703125, 37.959999084472656, 35.369998931884766] span=7.32% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7713996772918935 wick_frac=0.22860032270810654 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.5900:wick=0.3074; 2026-10-12:RED:body=-0.2400:wick=0.2600; 2026-10-27:GREEN:body=+0.2400:wick=0.2800; 2026-10-28:GREEN:body=+0.8500:wick=0.1400; 2026-10-29:GREEN:body=+0.5200:wick=0.2400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.24 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=33.47 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 58217.0 | **NEUTRAL** |
| `B04_income` | 6244.0 | **GOOD** |
| `B05_profit_margin` | 10.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.65 vs prior_export=41.65 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.5 | **GOOD** |
| `B13_short_float` | 1.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.24 (this export) | prior_export=12.24 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=33.47 (this export) | prior_export=33.47 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=134.6999969482422  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.92 on 2026-10-29; prev RSI=68.72 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.2800 R=0.0000); 2026-10-28:GREEN:O=132.3600,C=136.2300,body=+3.8700,vol=440777.0; 2026-10-29:GREEN:O=132.2900,C=134.7000,body=+2.4100,vol=702187.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1142964 Rvol=0); 2026-10-28:GREEN:O=132.3600,C=136.2300,body=+3.8700,vol=440777.0; 2026-10-29:GREEN:O=132.2900,C=134.7000,body=+2.4100,vol=702187.0 | **GOOD** |
| `A07_rvol` | RVOL=0.750 on 2026-10-29: today_vol=702187 / avg20=936768 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.517 on 2026-10-29 (price=134.7000, mid=122.9725, upper=145.6707, lower=100.2743; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=134.7000 vs SMA50=117.4008 dist=+14.74% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=122.9725 SMA50=117.4008 SMA80=109.1614 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-06@87.0000; S2[2026-09-01→2026-09-30] low=2026-09-28@104.3300; S3[2026-10-01→2026-10-29] low=2026-10-08@106.1200 | lows=[87.0, 104.33000183105469, 106.12000274658203] span=21.98% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6943269757186539 wick_frac=0.3056730242813462 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-23:GREEN:body=+0.5100:wick=6.1700; 2026-10-26:GREEN:body=+1.3100:wick=2.5800; 2026-10-27:RED:body=-0.8400:wick=3.0650; 2026-10-28:GREEN:body=+3.8700:wick=0.4900; 2026-10-29:GREEN:body=+2.4100:wick=2.4000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+14**  ·  Software - Infrastructure
price=60.619998931884766  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.76 on 2026-10-29; prev RSI=65.81 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=1.508 (G=0.9500 R=0.6300); 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0; 2026-10-29:RED:O=61.2500,C=60.6200,body=-0.6300,vol=480148.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=0.842 (Gvol=404220 Rvol=480148); 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0; 2026-10-29:RED:O=61.2500,C=60.6200,body=-0.6300,vol=480148.0 | **BAD** |
| `A07_rvol` | RVOL=0.754 on 2026-10-29: today_vol=480148 / avg20=636471 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.694 on 2026-10-29 (price=60.6200, mid=57.8455, upper=61.8435, lower=53.8475; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=60.6200 vs SMA50=54.2015 dist=+11.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=57.8455 SMA50=54.2015 SMA80=49.9751 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-03@43.1820; S2[2026-09-01→2026-09-30] low=2026-09-01@46.0100; S3[2026-10-01→2026-10-29] low=2026-10-01@51.9500 | lows=[43.18199920654297, 46.0099983215332, 51.95000076293945] span=20.30% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6506858620959945 wick_frac=0.34931413790400545 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.2636430248763208 wick_frac=0.7363569751236791 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5079351623665616 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.1950:wick=1.8050; 2026-10-26:GREEN:body=+0.6500:wick=1.3300; 2026-10-27:RED:body=-0.3600:wick=1.1000; 2026-10-28:GREEN:body=+0.9500:wick=0.5100; 2026-10-29:RED:body=-0.6300:wick=1.7596 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-30_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.