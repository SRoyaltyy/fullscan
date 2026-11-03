# A+B1 Feature Checklist — 2026-11-03

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-11-03.csv` · prior export for Δ: `2026-11-02`
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
| 1 | TRI | +17 | 17 | 0 | 2026-10-30→2026-11-02 | Specialty Business Services |
| 2 | CDNA | +15 | 18 | 3 | 2026-10-30→2026-11-02 | Diagnostics & Research |
| 3 | DEO | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Beverages - Wineries & Distilleries |
| 4 | HOMB | +15 | 16 | 1 | 2026-10-12→2026-11-02 | Banks - Regional |
| 5 | HHH | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Real Estate - Diversified |
| 6 | CP | +15 | 16 | 1 | 2026-10-12→2026-11-02 | Railroads |
| 7 | EPAM | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Information Technology Services |
| 8 | PAYS | +15 | 17 | 2 | 2026-10-23→2026-11-02 | Software - Infrastructure |
| 9 | NWBI | +15 | 16 | 1 | 2026-10-12→2026-11-02 | Banks - Regional |
| 10 | UBSI | +14 | 16 | 2 | 2026-10-12→2026-11-02 | Banks - Regional |
| 11 | UHS | +14 | 15 | 1 | 2026-10-12→2026-11-02 | Medical Care Facilities |
| 12 | TME | +14 | 16 | 2 | 2026-10-30→2026-11-02 | Internet Content & Information |
| 13 | BR | +14 | 16 | 2 | 2026-10-30→2026-11-02 | Information Technology Services |
| 14 | HSIC | +14 | 16 | 2 | 2026-10-12→2026-11-02 | Medical Distribution |
| 15 | TOL | +14 | 17 | 3 | 2026-10-12→2026-11-02 | Residential Construction |

## Full checklist — top 15

### TRI  ·  score **+17**  ·  Specialty Business Services
price=122.23999786376953  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.70 on 2026-11-02; prev RSI=65.23 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=2.962 (G=4.6800 R=1.5800); 2026-10-30:RED:O=119.8300,C=118.2500,body=-1.5800,vol=2377280.0; 2026-11-02:GREEN:O=117.5600,C=122.2400,body=+4.6800,vol=2500561.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.052 (Gvol=2500561 Rvol=2377280); 2026-10-30:RED:O=119.8300,C=118.2500,body=-1.5800,vol=2377280.0; 2026-11-02:GREEN:O=117.5600,C=122.2400,body=+4.6800,vol=2500561.0 | **GOOD** |
| `A07_rvol` | RVOL=1.603 on 2026-11-02: today_vol=2500561 / avg20=1560185 (avg window 2026-10-05→2026-10-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.658 on 2026-11-02 (price=122.2400, mid=111.3990, upper=127.8706, lower=94.9274; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=122.2400 vs SMA50=105.1538 dist=+16.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=111.3990 SMA50=105.1538 SMA80=102.8594 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@96.3300; S2[2026-09-03→2026-10-02] low=2026-09-29@92.5700; S3[2026-10-05→2026-11-02] low=2026-10-05@95.1425 | lows=[96.33000183105469, 92.56999969482422, 95.14250183105469] span=4.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.7279514206170445 wick_frac=0.27204857938295546 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.48840966282012277 wick_frac=0.5115903371798772 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.9620220769312486 need>1.4; red_wick_gt_green=False 5d trail=2026-10-27:RED:body=-4.1900:wick=1.4950; 2026-10-28:GREEN:body=+0.9900:wick=1.8500; 2026-10-29:RED:body=-1.1100:wick=1.6800; 2026-10-30:RED:body=-1.5800:wick=1.6550; 2026-11-02:GREEN:body=+4.6800:wick=1.7490 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.32 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7832.0 | **NEUTRAL** |
| `B04_income` | 1658.0 | **GOOD** |
| `B05_profit_margin` | 21.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.64 vs prior_export=122.64 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 9.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.32 (this export) | prior_export=3.32 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.86 (this export) | prior_export=1.86 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CDNA  ·  score **+15**  ·  Diagnostics & Research
price=85.87999725341797  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.19 on 2026-11-02; prev RSI=81.90 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=5.310 (G=5.3100 R=1.0000); 2026-10-30:GREEN:O=82.1000,C=87.4100,body=+5.3100,vol=3552531.0; 2026-11-02:RED:O=86.8800,C=85.8800,body=-1.0000,vol=2190573.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.622 (Gvol=3552531 Rvol=2190573); 2026-10-30:GREEN:O=82.1000,C=87.4100,body=+5.3100,vol=3552531.0; 2026-11-02:RED:O=86.8800,C=85.8800,body=-1.0000,vol=2190573.0 | **GOOD** |
| `A07_rvol` | RVOL=1.538 on 2026-11-02: today_vol=2190573 / avg20=1424559 (avg window 2026-10-05→2026-10-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.244 on 2026-11-02 (price=85.8800, mid=68.9290, upper=82.5605, lower=55.2975; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=85.8800 vs SMA50=60.6746 dist=+41.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=68.9290 SMA50=60.6746 SMA80=53.2779 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-07@44.5700; S2[2026-09-03→2026-10-02] low=2026-09-03@48.3860; S3[2026-10-05→2026-11-02] low=2026-10-08@61.1900 | lows=[44.56999969482422, 48.38600158691406, 61.189998626708984] span=37.29% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.5502595179646518 wick_frac=0.4497404820353482 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.16454135753649932 wick_frac=0.8354586424635007 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.310005187988281 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:GREEN:body=+1.4300:wick=2.4500; 2026-10-28:GREEN:body=+2.2200:wick=2.2100; 2026-10-29:RED:body=-0.5000:wick=2.6300; 2026-10-30:GREEN:body=+5.3100:wick=4.3400; 2026-11-02:RED:body=-1.0000:wick=5.0775 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=59.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.69 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 458.09 | **NEUTRAL** |
| `B04_income` | 111.01 | **GOOD** |
| `B05_profit_margin` | 24.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 56.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=56.14 vs prior_export=56.14 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.99 vs prior=-6.99 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.52 | **GOOD** |
| `B13_short_float` | 13.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=59.41 (this export) | prior_export=59.41 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.69 (this export) | prior_export=15.69 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DEO  ·  score **+15**  ·  Beverages - Wineries & Distilleries
price=97.62999725341797  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.74 on 2026-11-02; prev RSI=57.02 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=52.624 (G=2.1050 R=0.0400); 2026-10-30:RED:O=93.4800,C=93.4400,body=-0.0400,vol=928094.0; 2026-11-02:GREEN:O=95.5250,C=97.6300,body=+2.1050,vol=1312683.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.414 (Gvol=1312683 Rvol=928094); 2026-10-30:RED:O=93.4800,C=93.4400,body=-0.0400,vol=928094.0; 2026-11-02:GREEN:O=95.5250,C=97.6300,body=+2.1050,vol=1312683.0 | **GOOD** |
| `A07_rvol` | RVOL=1.213 on 2026-11-02: today_vol=1312683 / avg20=1082088 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.765 on 2026-11-02 (price=97.6300, mid=91.8850, upper=99.3995, lower=84.3705; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=97.6300 vs SMA50=89.7156 dist=+8.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=91.8850 SMA50=89.7156 SMA80=89.2676 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-05@87.2600; S2[2026-09-03→2026-10-02] low=2026-10-01@84.0000; S3[2026-10-05→2026-11-02] low=2026-10-05@83.2000 | lows=[87.26000213623047, 84.0, 83.19999694824219] span=4.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.9152143193593971 wick_frac=0.08478568064060292 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.043479342543910574 wick_frac=0.9565206574560894 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=52.62368872782758 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:GREEN:body=+0.0200:wick=0.6650; 2026-10-28:RED:body=-0.5800:wick=1.1050; 2026-10-29:RED:body=-0.6000:wick=1.1330; 2026-10-30:RED:body=-0.0400:wick=0.8800; 2026-11-02:GREEN:body=+2.1050:wick=0.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.73 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.38 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 19602.05 | **NEUTRAL** |
| `B04_income` | 1729.57 | **GOOD** |
| `B05_profit_margin` | 8.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 107.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=107.71 vs prior_export=107.71 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -43.51 | **BAD** |
| `B13_short_float` | 0.56 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.73 (this export) | prior_export=9.73 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.38 (this export) | prior_export=0.38 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HOMB  ·  score **+15**  ·  Banks - Regional
price=30.420000076293945  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.31 on 2026-11-02; prev RSI=41.65 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.65@2026-10-12 → 64.31@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.65@2026-10-12 → 64.31@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.65@2026-10-12 → 64.31@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6600 R=0.0000); 2026-10-12:GREEN:O=28.3300,C=28.5000,body=+0.1700,vol=1171549.0; 2026-11-02:GREEN:O=29.9300,C=30.4200,body=+0.4900,vol=1538500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2710049 Rvol=0); 2026-10-12:GREEN:O=28.3300,C=28.5000,body=+0.1700,vol=1171549.0; 2026-11-02:GREEN:O=29.9300,C=30.4200,body=+0.4900,vol=1538500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.989 on 2026-11-02: today_vol=1538500 / avg20=1555479 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.174 on 2026-11-02 (price=30.4200, mid=28.9330, upper=30.1993, lower=27.6667; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=30.4200 vs SMA50=29.7408 dist=+2.28% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=28.93_50=29.74_80=29.52 on 2026-11-02: SMA20=28.9330 SMA50=29.7408 SMA80=29.5192 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-13@28.4673; S2[2026-08-14→2026-09-14] low=2026-09-01@29.0900; S3[2026-09-15→2026-11-02] low=2026-10-01@27.7300 | lows=[28.46728176680695, 29.09000015258789, 27.729999542236328] span=4.90% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.5249099239020713 wick_frac=0.4750900760979287 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-07:RED:body=-0.1600:wick=0.1500; 2026-10-08:GREEN:body=+0.4300:wick=0.1300; 2026-10-09:RED:body=-0.1700:wick=0.0700; 2026-10-12:GREEN:body=+0.1700:wick=0.2000; 2026-11-02:GREEN:body=+0.4900:wick=0.3400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.14 (current export asof; earnings_date=10/14/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.21 (current export; earnings_date=10/14/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1491.58 | **NEUTRAL** |
| `B04_income` | 479.37 | **GOOD** |
| `B05_profit_margin` | 32.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.25 vs prior_export=33.25 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | 0.54 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.54 vs prior=0.54 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.53 | **GOOD** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 10/14/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.14 (this export) | prior_export=5.14 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.21 (this export) | prior_export=2.21 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HHH  ·  score **+15**  ·  Real Estate - Diversified
price=77.91000366210938  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.60 on 2026-11-02; prev RSI=65.58 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7500 R=0.0000); 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0; 2026-11-02:GREEN:O=77.0400,C=77.9100,body=+0.8700,vol=720915.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1613971 Rvol=0); 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0; 2026-11-02:GREEN:O=77.0400,C=77.9100,body=+0.8700,vol=720915.0 | **GOOD** |
| `A07_rvol` | RVOL=0.768 on 2026-11-02: today_vol=720915 / avg20=938184 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.040 on 2026-11-02 (price=77.9100, mid=71.8310, upper=77.6755, lower=65.9865; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=77.9100 vs SMA50=68.1356 dist=+14.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=71.8310 SMA50=68.1356 SMA80=68.0869 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-09-01@63.0200; S2[2026-09-03→2026-10-02] low=2026-09-18@60.0900; S3[2026-10-05→2026-11-02] low=2026-10-19@66.1400 | lows=[63.02000045776367, 60.09000015258789, 66.13999938964844] span=10.07% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.5853732169820749 wick_frac=0.414626783017925 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-27:GREEN:body=+0.2900:wick=1.6200; 2026-10-28:GREEN:body=+0.4400:wick=1.2550; 2026-10-29:GREEN:body=+1.5600:wick=0.5550; 2026-10-30:GREEN:body=+1.8800:wick=0.9600; 2026-11-02:GREEN:body=+0.8700:wick=0.8400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=170.05 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=139.3 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2334.65 | **NEUTRAL** |
| `B04_income` | 292.1 | **GOOD** |
| `B05_profit_margin` | 12.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.67 vs prior_export=89.67 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 1.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.67 vs prior=1.67 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=170.05 (this export) | prior_export=170.05 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=139.3 (this export) | prior_export=139.3 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=91.48999786376953  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.08 on 2026-11-02; prev RSI=42.80 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.80@2026-10-12 → 63.08@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.80@2026-10-12 → 63.08@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.80@2026-10-12 → 63.08@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.0300 R=0.0000); 2026-10-12:GREEN:O=84.4000,C=85.5300,body=+1.1300,vol=1729371.0; 2026-11-02:GREEN:O=89.5900,C=91.4900,body=+1.9000,vol=2767500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4496871 Rvol=0); 2026-10-12:GREEN:O=84.4000,C=85.5300,body=+1.1300,vol=1729371.0; 2026-11-02:GREEN:O=89.5900,C=91.4900,body=+1.9000,vol=2767500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.239 on 2026-11-02: today_vol=2767500 / avg20=2233588 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.344 on 2026-11-02 (price=91.4900, mid=86.5260, upper=90.2183, lower=82.8337; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=91.4900 vs SMA50=89.6500 dist=+2.05% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.53_50=89.65_80=89.42 on 2026-11-02: SMA20=86.5260 SMA50=89.6500 SMA80=89.4205 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-08→2026-11-02 (63 bars); S1[2026-07-08→2026-08-10] low=2026-07-30@87.2000; S2[2026-08-11→2026-09-14] low=2026-09-14@88.0400; S3[2026-09-15→2026-11-02] low=2026-10-08@83.2500 | lows=[87.19999694824219, 88.04000091552734, 83.25] span=5.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.7939089014770991 wick_frac=0.2060910985229008 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-1.2200:wick=0.6450; 2026-10-08:GREEN:body=+1.0800:wick=0.4100; 2026-10-09:RED:body=-0.0100:wick=0.7600; 2026-10-12:GREEN:body=+1.1300:wick=0.3200; 2026-11-02:GREEN:body=+1.9000:wick=0.4500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.75 vs prior_export=100.75 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.2 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=143.8000030517578  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=73.13 on 2026-11-02; prev RSI=70.79 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.1000 R=0.0000); 2026-10-30:GREEN:O=137.2100,C=140.5100,body=+3.3000,vol=1291536.0; 2026-11-02:GREEN:O=141.0000,C=143.8000,body=+2.8000,vol=1330402.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2621938 Rvol=0); 2026-10-30:GREEN:O=137.2100,C=140.5100,body=+3.3000,vol=1291536.0; 2026-11-02:GREEN:O=141.0000,C=143.8000,body=+2.8000,vol=1330402.0 | **GOOD** |
| `A07_rvol` | RVOL=1.462 on 2026-11-02: today_vol=1330402 / avg20=909862 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.751 on 2026-11-02 (price=143.8000, mid=126.2760, upper=149.6051, lower=102.9469; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=143.8000 vs SMA50=118.7450 dist=+21.10% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=126.2760 SMA50=118.7450 SMA80=110.5102 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@87.0000; S2[2026-09-03→2026-10-02] low=2026-09-28@104.3300; S3[2026-10-05→2026-11-02] low=2026-10-08@106.1200 | lows=[87.0, 104.33000183105469, 106.12000274658203] span=21.98% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.6239463412015004 wick_frac=0.37605365879849956 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:RED:body=-0.8400:wick=3.0650; 2026-10-28:GREEN:body=+3.8700:wick=0.4900; 2026-10-29:GREEN:body=+2.4100:wick=2.4000; 2026-10-30:GREEN:body=+3.3000:wick=0.8500; 2026-11-02:GREEN:body=+2.8000:wick=3.3849 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAYS  ·  score **+15**  ·  Software - Infrastructure
price=14.399999618530273  pair=`2026-10-23→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.94 on 2026-11-02; prev RSI=55.32 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.32@2026-10-23 → 49.94@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_down | RSI 55.32@2026-10-23 → 49.94@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **BAD** |
| `A04_rsi_cross_70` | below | RSI 55.32@2026-10-23 → 49.94@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=3.750 (G=0.4500 R=0.1200); 2026-10-23:RED:O=15.0400,C=14.9200,body=-0.1200,vol=653158.0; 2026-11-02:GREEN:O=13.9500,C=14.4000,body=+0.4500,vol=1541200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-11-02; ratio=GREEN_vol/RED_vol=2.360 (Gvol=1541200 Rvol=653158); 2026-10-23:RED:O=15.0400,C=14.9200,body=-0.1200,vol=653158.0; 2026-11-02:GREEN:O=13.9500,C=14.4000,body=+0.4500,vol=1541200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.726 on 2026-11-02: today_vol=1541200 / avg20=892815 (avg window 2026-09-28→2026-10-23, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.293 on 2026-11-02 (price=14.4000, mid=14.8140, upper=16.2260, lower=13.4020; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=14.4000 vs SMA50=13.7156 dist=+4.99% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=14.8140 SMA50=13.7156 SMA80=12.2141 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-11-02 (63 bars); S1[2026-07-28→2026-08-26] low=2026-07-28@8.8000; S2[2026-08-27→2026-09-25] low=2026-09-16@11.3600; S3[2026-09-28→2026-11-02] low=2026-09-28@12.3400 | lows=[8.800000190734863, 11.359999656677246, 12.34000015258789] span=40.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-11-02: GREEN body_frac=0.6164376761193582 wick_frac=0.3835623238806419 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-11-02: RED body_frac=0.16925239427526095 wick_frac=0.830747605724739 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.750001986823387 need>1.4; red_wick_gt_green=False 5d trail=2026-10-20:GREEN:body=+0.0500:wick=0.9500; 2026-10-21:GREEN:body=+0.3200:wick=0.5500; 2026-10-22:RED:body=-0.2700:wick=0.6600; 2026-10-23:RED:body=-0.1200:wick=0.5890; 2026-11-02:GREEN:body=+0.4500:wick=0.2800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=83.33 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.18 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 100.64 | **NEUTRAL** |
| `B04_income` | 15.77 | **GOOD** |
| `B05_profit_margin` | 15.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.5 vs prior_export=13.5 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -4.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.39 vs prior=-4.39 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 14.38 | **GOOD** |
| `B13_short_float` | 5.02 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=83.33 (this export) | prior_export=83.33 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.18 (this export) | prior_export=7.18 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+15**  ·  Banks - Regional
price=15.670000076293945  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.78 on 2026-11-02; prev RSI=43.30 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.30@2026-10-12 → 60.78@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.30@2026-10-12 → 60.78@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.30@2026-10-12 → 60.78@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4900 R=0.0000); 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0; 2026-11-02:GREEN:O=15.3500,C=15.6700,body=+0.3200,vol=1483300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2474754 Rvol=0); 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0; 2026-11-02:GREEN:O=15.3500,C=15.6700,body=+0.3200,vol=1483300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.265 on 2026-11-02: today_vol=1483300 / avg20=1172912 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.052 on 2026-11-02 (price=15.6700, mid=15.1065, upper=15.6422, lower=14.5708; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=15.6700 vs SMA50=15.3450 dist=+2.12% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.11_50=15.34_80=15.21 on 2026-11-02: SMA20=15.1065 SMA50=15.3450 SMA80=15.2088 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-13@14.7824; S2[2026-08-14→2026-09-14] low=2026-09-01@15.0700; S3[2026-09-15→2026-11-02] low=2026-10-01@14.5400 | lows=[14.782406048106942, 15.069999694824219, 14.539999961853027] span=3.65% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.8918118087972026 wick_frac=0.10818819120279743 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-0.0800:wick=0.1000; 2026-10-08:GREEN:body=+0.1500:wick=0.1500; 2026-10-09:RED:body=-0.1000:wick=0.1050; 2026-10-12:GREEN:body=+0.1700:wick=0.0200; 2026-11-02:GREEN:body=+0.3200:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UBSI  ·  score **+14**  ·  Banks - Regional
price=49.310001373291016  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.32 on 2026-11-02; prev RSI=43.31 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.31@2026-10-12 → 66.32@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.31@2026-10-12 → 66.32@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.31@2026-10-12 → 66.32@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0050 R=0.0000); 2026-10-12:GREEN:O=45.5700,C=45.8750,body=+0.3050,vol=598850.0; 2026-11-02:GREEN:O=48.6100,C=49.3100,body=+0.7000,vol=920400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1519250 Rvol=0); 2026-10-12:GREEN:O=45.5700,C=45.8750,body=+0.3050,vol=598850.0; 2026-11-02:GREEN:O=48.6100,C=49.3100,body=+0.7000,vol=920400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.789 on 2026-11-02: today_vol=920400 / avg20=1167197 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.620 on 2026-11-02 (price=49.3100, mid=46.5388, upper=48.2494, lower=44.8281; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=49.3100 vs SMA50=47.3845 dist=+4.06% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=46.54_50=47.38_80=47.08 on 2026-11-02: SMA20=46.5388 SMA50=47.3845 SMA80=47.0787 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-13@45.8800; S2[2026-08-14→2026-09-14] low=2026-09-01@46.3200; S3[2026-09-15→2026-11-02] low=2026-10-07@45.0200 | lows=[45.880001068115234, 46.31999969482422, 45.02000045776367] span=2.89% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.5658630695782332 wick_frac=0.4341369304217668 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-0.3700:wick=0.4900; 2026-10-08:GREEN:body=+0.5000:wick=0.3500; 2026-10-09:RED:body=-0.1200:wick=0.3800; 2026-10-12:GREEN:body=+0.3050:wick=0.3600; 2026-11-02:GREEN:body=+0.7000:wick=0.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.14 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.17 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1841.33 | **NEUTRAL** |
| `B04_income` | 514.23 | **GOOD** |
| `B05_profit_margin` | 27.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.33 vs prior_export=50.33 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | -0.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.1 vs prior=-0.1 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.84 | **GOOD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.14 (this export) | prior_export=7.14 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.17 (this export) | prior_export=1.17 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UHS  ·  score **+14**  ·  Medical Care Facilities
price=180.64999389648438  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.77 on 2026-11-02; prev RSI=56.80 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.80@2026-10-12 → 59.77@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.80@2026-10-12 → 59.77@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.80@2026-10-12 → 59.77@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=9.1200 R=0.0000); 2026-10-12:GREEN:O=177.5600,C=178.8300,body=+1.2700,vol=558297.0; 2026-11-02:GREEN:O=172.8000,C=180.6500,body=+7.8500,vol=910700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1468997 Rvol=0); 2026-10-12:GREEN:O=177.5600,C=178.8300,body=+1.2700,vol=558297.0; 2026-11-02:GREEN:O=172.8000,C=180.6500,body=+7.8500,vol=910700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.233 on 2026-11-02: today_vol=910700 / avg20=738789 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.699 on 2026-11-02 (price=180.6500, mid=177.4240, upper=182.0373, lower=172.8107; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=180.6500 vs SMA50=174.4066 dist=+3.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=177.4240 SMA50=174.4066 SMA80=166.2280 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-14@142.3800; S2[2026-08-14→2026-09-14] low=2026-09-03@166.4300; S3[2026-09-15→2026-11-02] low=2026-09-15@169.0400 | lows=[142.3800048828125, 166.42999267578125, 169.0399932861328] span=18.72% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.6423382167541188 wick_frac=0.35766178324588116 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-1.2100:wick=2.7300; 2026-10-08:GREEN:body=+2.3300:wick=1.4800; 2026-10-09:GREEN:body=+2.0400:wick=1.8500; 2026-10-12:GREEN:body=+1.2700:wick=2.6950; 2026-11-02:GREEN:body=+7.8500:wick=0.2900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.67 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.29 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18114.49 | **NEUTRAL** |
| `B04_income` | 1526.03 | **GOOD** |
| `B05_profit_margin` | 8.42 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 193.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=193.25 vs prior_export=193.25 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.58 | **BAD** |
| `B13_short_float` | 5.58 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.67 (this export) | prior_export=0.67 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.29 (this export) | prior_export=1.29 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TME  ·  score **+14**  ·  Internet Content & Information
price=8.90999984741211  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.48 on 2026-11-02; prev RSI=51.42 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.42@2026-10-30 → 65.48@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.42@2026-10-30 → 65.48@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.42@2026-10-30 → 65.48@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=3.444 (G=0.3100 R=0.0900); 2026-10-30:RED:O=8.5000,C=8.4100,body=-0.0900,vol=6268475.0; 2026-11-02:GREEN:O=8.6000,C=8.9100,body=+0.3100,vol=9726853.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.552 (Gvol=9726853 Rvol=6268475); 2026-10-30:RED:O=8.5000,C=8.4100,body=-0.0900,vol=6268475.0; 2026-11-02:GREEN:O=8.6000,C=8.9100,body=+0.3100,vol=9726853.0 | **GOOD** |
| `A07_rvol` | RVOL=2.033 on 2026-11-02: today_vol=9726853 / avg20=4783973 (avg window 2026-10-05→2026-10-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.971 on 2026-11-02 (price=8.9100, mid=8.4375, upper=8.9242, lower=7.9508; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=8.9100 vs SMA50=8.2760 dist=+7.66% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.44_50=8.28_80=8.59 on 2026-11-02: SMA20=8.4375 SMA50=8.2760 SMA80=8.5855 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-09-01@8.1300; S2[2026-09-03→2026-10-02] low=2026-10-02@7.6600; S3[2026-10-05→2026-11-02] low=2026-10-05@7.7300 | lows=[8.130000114440918, 7.659999847412109, 7.730000019073486] span=6.14% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.7560965397891681 wick_frac=0.2439034602108319 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.529412424687812 wick_frac=0.470587575312188 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.444432670707413 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:GREEN:body=+0.0900:wick=0.0000; 2026-10-28:RED:body=-0.0900:wick=0.1900; 2026-10-29:GREEN:body=+0.0900:wick=0.0700; 2026-10-30:RED:body=-0.0900:wick=0.0800; 2026-11-02:GREEN:body=+0.3100:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.44 (current export asof; earnings_date=8/11/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.46 (current export; earnings_date=8/11/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4853.58 | **NEUTRAL** |
| `B04_income` | 1276.49 | **GOOD** |
| `B05_profit_margin` | 26.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.98 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.98 vs prior_export=11.98 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 4.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/11/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.44 (this export) | prior_export=1.44 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.46 (this export) | prior_export=1.46 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BR  ·  score **+14**  ·  Information Technology Services
price=184.22000122070312  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.19 on 2026-11-02; prev RSI=59.77 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.77@2026-10-30 → 66.19@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.77@2026-10-30 → 66.19@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.77@2026-10-30 → 66.19@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=14.889 (G=5.3600 R=0.3600); 2026-10-30:RED:O=179.1800,C=178.8200,body=-0.3600,vol=640450.0; 2026-11-02:GREEN:O=178.8600,C=184.2200,body=+5.3600,vol=1051632.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.642 (Gvol=1051632 Rvol=640450); 2026-10-30:RED:O=179.1800,C=178.8200,body=-0.3600,vol=640450.0; 2026-11-02:GREEN:O=178.8600,C=184.2200,body=+5.3600,vol=1051632.0 | **GOOD** |
| `A07_rvol` | RVOL=1.373 on 2026-11-02: today_vol=1051632 / avg20=765733 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.639 on 2026-11-02 (price=184.2200, mid=174.0460, upper=189.9591, lower=158.1329; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=184.2200 vs SMA50=171.6730 dist=+7.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=174.0460 SMA50=171.6730 SMA80=167.4084 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@160.0000; S2[2026-09-03→2026-10-02] low=2026-10-02@156.8700; S3[2026-10-05→2026-11-02] low=2026-10-05@155.6800 | lows=[160.0, 156.8699951171875, 155.67999267578125] span=2.77% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.9605732708762316 wick_frac=0.03942672912376843 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.05546778016025279 wick_frac=0.9445322198397472 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.889496439471007 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:RED:body=-2.7700:wick=1.2500; 2026-10-28:GREEN:body=+1.8000:wick=1.0800; 2026-10-29:GREEN:body=+0.9500:wick=1.7500; 2026-10-30:RED:body=-0.3600:wick=6.1300; 2026-11-02:GREEN:body=+5.3600:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.66 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7476.8 | **NEUTRAL** |
| `B04_income` | 1124.3 | **GOOD** |
| `B05_profit_margin` | 15.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 208.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=208.14 vs prior_export=208.14 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -0.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.34 vs prior=-0.34 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.7 | **BAD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.66 (this export) | prior_export=1.66 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSIC  ·  score **+14**  ·  Medical Distribution
price=86.62000274658203  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.43 on 2026-11-02; prev RSI=42.84 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.84@2026-10-12 → 53.43@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.84@2026-10-12 → 53.43@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.84@2026-10-12 → 53.43@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7600 R=0.0000); 2026-10-12:GREEN:O=84.1600,C=84.2300,body=+0.0700,vol=955238.0; 2026-11-02:GREEN:O=85.9300,C=86.6200,body=+0.6900,vol=2501800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3457038 Rvol=0); 2026-10-12:GREEN:O=84.1600,C=84.2300,body=+0.0700,vol=955238.0; 2026-11-02:GREEN:O=85.9300,C=86.6200,body=+0.6900,vol=2501800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.850 on 2026-11-02: today_vol=2501800 / avg20=1351972 (avg window 2026-09-15→2026-10-12, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.720 on 2026-11-02 (price=86.6200, mid=85.1595, upper=87.1891, lower=83.1299; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-11-02: price=86.6200 vs SMA50=87.5760 dist=-1.09% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=85.16_50=87.58_80=86.42 on 2026-11-02: SMA20=85.1595 SMA50=87.5760 SMA80=86.4225 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-23@83.1900; S2[2026-08-14→2026-09-14] low=2026-09-10@87.1100; S3[2026-09-15→2026-11-02] low=2026-10-08@79.3700 | lows=[83.19000244140625, 87.11000061035156, 79.37000274658203] span=9.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.19444604481636862 wick_frac=0.8055539551836314 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-1.1300:wick=0.9350; 2026-10-08:GREEN:body=+4.6800:wick=0.6100; 2026-10-09:RED:body=-0.1000:wick=1.5150; 2026-10-12:GREEN:body=+0.0700:wick=0.9600; 2026-11-02:GREEN:body=+0.6900:wick=1.4600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.45 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13602.0 | **NEUTRAL** |
| `B04_income` | 403.0 | **GOOD** |
| `B05_profit_margin` | 2.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.64 vs prior_export=99.64 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.49 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TOL  ·  score **+14**  ·  Residential Construction
price=153.35000610351562  pair=`2026-10-12→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.05 on 2026-11-02; prev RSI=39.45 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.45@2026-10-12 → 69.05@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 39.45@2026-10-12 → 69.05@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 39.45@2026-10-12 → 69.05@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=36.562 (G=5.8500 R=0.1600); 2026-10-12:RED:O=132.3100,C=132.1500,body=-0.1600,vol=794655.0; 2026-11-02:GREEN:O=147.5000,C=153.3500,body=+5.8500,vol=1725600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-11-02; ratio=GREEN_vol/RED_vol=2.172 (Gvol=1725600 Rvol=794655); 2026-10-12:RED:O=132.3100,C=132.1500,body=-0.1600,vol=794655.0; 2026-11-02:GREEN:O=147.5000,C=153.3500,body=+5.8500,vol=1725600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.461 on 2026-11-02: today_vol=1725600 / avg20=1181150 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.967 on 2026-11-02 (price=153.3500, mid=136.1215, upper=144.8786, lower=127.3644; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=153.3500 vs SMA50=141.3632 dist=+8.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-02: SMA20=136.1215 SMA50=141.3632 SMA80=145.8403 | **BAD** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-02 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-23@145.2800; S2[2026-08-14→2026-09-14] low=2026-09-10@130.2600; S3[2026-09-15→2026-11-02] low=2026-10-08@131.2000 | lows=[145.27999877929688, 130.25999450683594, 131.1999969482422] span=11.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: GREEN body_frac=0.874440055104964 wick_frac=0.125559944895036 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-11-02: RED body_frac=0.08864429847919995 wick_frac=0.9113557015208 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=36.561701316040434 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-1.5700:wick=1.5600; 2026-10-08:GREEN:body=+2.8300:wick=0.8900; 2026-10-09:RED:body=-0.9200:wick=1.5600; 2026-10-12:RED:body=-0.1600:wick=1.6450; 2026-11-02:GREEN:body=+5.8500:wick=0.8400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.23 (current export asof; earnings_date=8/18/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.54 (current export; earnings_date=8/18/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10759.04 | **NEUTRAL** |
| `B04_income` | 1198.38 | **GOOD** |
| `B05_profit_margin` | 11.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 172.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=172.79 vs prior_export=172.79 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | -10.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-10.13 vs prior=-10.13 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.76 | **GOOD** |
| `B13_short_float` | 4.12 | **NEUTRAL** |
| `B14_earnings_date` | 8/18/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.54 (this export) | prior_export=1.54 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-03_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.