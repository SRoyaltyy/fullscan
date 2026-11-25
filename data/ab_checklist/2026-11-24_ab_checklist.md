# A+B1 Feature Checklist — 2026-11-24

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,602** names
- Export: `finviz_2026-11-24.csv` · prior export for Δ: `2026-11-23`
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
| 1 | PLX | +18 | 18 | 0 | 2026-11-23→2026-11-24 | Biotechnology |
| 2 | VFC | +17 | 17 | 0 | 2026-11-23→2026-11-24 | Apparel Manufacturing |
| 3 | GEHC | +17 | 17 | 0 | 2026-11-23→2026-11-24 | Medical Devices |
| 4 | EVRG | +16 | 17 | 1 | 2026-11-23→2026-11-24 | Utilities - Regulated Electric |
| 5 | GLOB | +16 | 18 | 2 | 2026-11-23→2026-11-24 | Information Technology Services |
| 6 | YUM | +16 | 17 | 1 | 2026-11-23→2026-11-24 | Restaurants |
| 7 | NYT | +16 | 17 | 1 | 2026-11-23→2026-11-24 | Publishing |
| 8 | V | +16 | 16 | 0 | 2026-11-23→2026-11-24 | Credit Services |
| 9 | OSPN | +15 | 16 | 1 | 2026-11-23→2026-11-24 | Software - Infrastructure |
| 10 | LIND | +15 | 16 | 1 | 2026-11-23→2026-11-24 | Travel Services |
| 11 | EL | +15 | 16 | 1 | 2026-11-23→2026-11-24 | Household & Personal Products |
| 12 | BRX | +15 | 16 | 1 | 2026-11-23→2026-11-24 | REIT - Retail |
| 13 | ATAT | +15 | 16 | 1 | 2026-11-23→2026-11-24 | Lodging |
| 14 | SEZL | +15 | 17 | 2 | 2026-11-23→2026-11-24 | Credit Services |
| 15 | CL | +15 | 16 | 1 | 2026-11-23→2026-11-24 | Household & Personal Products |

## Full checklist — top 15

### PLX  ·  score **+18**  ·  Biotechnology
price=3.1700000762939453  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.05 on 2026-11-24; prev RSI=55.84 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.84@2026-11-23 → 63.05@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.84@2026-11-23 → 63.05@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.84@2026-11-23 → 63.05@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=25.000 (G=0.2500 R=0.0100); 2026-11-23:RED:O=2.9500,C=2.9400,body=-0.0100,vol=871500.0; 2026-11-24:GREEN:O=2.9200,C=3.1700,body=+0.2500,vol=2112900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=2.424 (Gvol=2112900 Rvol=871500); 2026-11-23:RED:O=2.9500,C=2.9400,body=-0.0100,vol=871500.0; 2026-11-24:GREEN:O=2.9200,C=3.1700,body=+0.2500,vol=2112900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.878 on 2026-11-24: today_vol=2112900 / avg20=1124905 (avg window 2026-10-27→2026-11-23, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.787 on 2026-11-24 (price=3.1700, mid=2.8135, upper=3.2662, lower=2.3608; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=3.1700 vs SMA50=2.7346 dist=+15.92% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=2.8135 SMA50=2.7346 SMA80=2.6335 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-24 (63 bars); S1[2026-08-24→2026-09-22] low=2026-08-24@2.3400; S2[2026-09-23→2026-10-26] low=2026-10-26@2.4600; S3[2026-10-27→2026-11-24] low=2026-11-10@2.4500 | lows=[2.3399999141693115, 2.4600000381469727, 2.450000047683716] span=5.13% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.8333334657881205 wick_frac=0.16666653421187944 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=0.08333333333333333 wick_frac=0.9166666666666666 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=25.000023841880648 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.1300:wick=0.0100; 2026-11-19:GREEN:body=+0.0900:wick=0.1300; 2026-11-20:RED:body=-0.1200:wick=0.0500; 2026-11-23:RED:body=-0.0100:wick=0.1100; 2026-11-24:GREEN:body=+0.2500:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=350.0 (current export asof; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=76.53 (current export; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 84.69 | **NEUTRAL** |
| `B04_income` | 20.55 | **GOOD** |
| `B05_profit_margin` | 24.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=9.08 vs prior_export=9.08 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.42 | **GOOD** |
| `B13_short_float` | 4.54 | **NEUTRAL** |
| `B14_earnings_date` | 11/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=350.0 (this export) | prior_export=350.0 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=76.53 (this export) | prior_export=76.53 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VFC  ·  score **+17**  ·  Apparel Manufacturing
price=20.90999984741211  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.97 on 2026-11-24; prev RSI=57.87 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.87@2026-11-23 → 62.97@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.87@2026-11-23 → 62.97@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.87@2026-11-23 → 62.97@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0100 R=0.0000); 2026-11-23:GREEN:O=19.8900,C=20.0300,body=+0.1400,vol=8818800.0; 2026-11-24:GREEN:O=20.0400,C=20.9100,body=+0.8700,vol=7730400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=16549200 Rvol=0); 2026-11-23:GREEN:O=19.8900,C=20.0300,body=+0.1400,vol=8818800.0; 2026-11-24:GREEN:O=20.0400,C=20.9100,body=+0.8700,vol=7730400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.717 on 2026-11-24: today_vol=7730400 / avg20=10782573 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.463 on 2026-11-24 (price=20.9100, mid=19.9740, upper=21.9939, lower=17.9541; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=20.9100 vs SMA50=17.0580 dist=+22.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=19.9740 SMA50=17.0580 SMA80=15.8976 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-10@12.4600; S2[2026-09-28→2026-10-26] low=2026-09-28@13.8500; S3[2026-10-27→2026-11-24] low=2026-10-28@15.9510 | lows=[12.460000038146973, 13.850000381469727, 15.951000213623047] span=28.02% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.5138377679702423 wick_frac=0.4861622320297577 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-18:GREEN:body=+0.7100:wick=0.5100; 2026-11-19:RED:body=-0.7900:wick=0.3500; 2026-11-20:RED:body=-0.5400:wick=0.2400; 2026-11-23:GREEN:body=+0.1400:wick=0.4700; 2026-11-24:GREEN:body=+0.8700:wick=0.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.82 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.13 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9403.78 | **NEUTRAL** |
| `B04_income` | 369.66 | **GOOD** |
| `B05_profit_margin` | 3.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.14000000000000057 (now=19.62 vs prior_export=19.48 on finviz_2026-11-23) | **GOOD** |
| `B09_analyst_recom` | 2.26 | **GOOD** |
| `B10_insider_transactions` | 0.33 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.33 vs prior=0.33 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.85 | **GOOD** |
| `B13_short_float` | 9.37 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.82 (this export) | prior_export=30.82 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.13 (this export) | prior_export=1.13 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEHC  ·  score **+17**  ·  Medical Devices
price=82.5199966430664  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.07 on 2026-11-24; prev RSI=58.20 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.20@2026-11-23 → 61.07@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.20@2026-11-23 → 61.07@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.20@2026-11-23 → 61.07@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.4700 R=0.0000); 2026-11-23:GREEN:O=80.6400,C=81.3100,body=+0.6700,vol=3497900.0; 2026-11-24:GREEN:O=80.7200,C=82.5200,body=+1.8000,vol=2354500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5852400 Rvol=0); 2026-11-23:GREEN:O=80.6400,C=81.3100,body=+0.6700,vol=3497900.0; 2026-11-24:GREEN:O=80.7200,C=82.5200,body=+1.8000,vol=2354500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.527 on 2026-11-24: today_vol=2354500 / avg20=4468615 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.183 on 2026-11-24 (price=82.5200, mid=81.5460, upper=86.8730, lower=76.2190; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=82.5200 vs SMA50=72.9013 dist=+13.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=81.5460 SMA50=72.9013 SMA80=72.0804 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-17@62.9950; S2[2026-09-28→2026-10-26] low=2026-10-08@63.3150; S3[2026-10-27→2026-11-24] low=2026-10-27@70.1700 | lows=[62.994998931884766, 63.314998626708984, 70.16999816894531] span=11.39% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.6711793339153207 wick_frac=0.3288206660846792 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+2.3500:wick=0.5100; 2026-11-19:RED:body=-1.7700:wick=0.7500; 2026-11-20:RED:body=-1.8600:wick=0.9200; 2026-11-23:GREEN:body=+0.6700:wick=0.6100; 2026-11-24:GREEN:body=+1.8000:wick=0.3980 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.28 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.02 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21637.0 | **NEUTRAL** |
| `B04_income` | 2154.0 | **GOOD** |
| `B05_profit_margin` | 9.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 88.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=88.26 vs prior_export=88.26 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.76 | **GOOD** |
| `B10_insider_transactions` | 0.7 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.7 vs prior=0.7 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.64 | **GOOD** |
| `B13_short_float` | 4.05 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.28 (this export) | prior_export=11.28 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.02 (this export) | prior_export=3.02 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVRG  ·  score **+16**  ·  Utilities - Regulated Electric
price=87.20999908447266  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.89 on 2026-11-24; prev RSI=56.84 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.84@2026-11-23 → 57.89@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.84@2026-11-23 → 57.89@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.84@2026-11-23 → 57.89@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4500 R=0.0000); 2026-11-23:GREEN:O=85.8300,C=87.0000,body=+1.1700,vol=1909700.0; 2026-11-24:GREEN:O=86.9300,C=87.2100,body=+0.2800,vol=1405800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3315500 Rvol=0); 2026-11-23:GREEN:O=85.8300,C=87.0000,body=+1.1700,vol=1909700.0; 2026-11-24:GREEN:O=86.9300,C=87.2100,body=+0.2800,vol=1405800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.537 on 2026-11-24: today_vol=1405800 / avg20=2618111 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.091 on 2026-11-24 (price=87.2100, mid=86.9895, upper=89.4159, lower=84.5631; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=87.2100 vs SMA50=83.2436 dist=+4.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=86.9895 SMA50=83.2436 SMA80=82.9242 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-24 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-21@78.8900; S2[2026-09-23→2026-10-26] low=2026-09-25@77.1100; S3[2026-10-27→2026-11-24] low=2026-11-04@83.2400 | lows=[78.88999938964844, 77.11000061035156, 83.23999786376953] span=7.95% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.5443959810687735 wick_frac=0.45560401893122643 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:RED:body=-0.0100:wick=0.9800; 2026-11-19:RED:body=-0.2800:wick=0.7300; 2026-11-20:RED:body=-0.3700:wick=0.8500; 2026-11-23:GREEN:body=+1.1700:wick=0.1200; 2026-11-24:GREEN:body=+0.2800:wick=1.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.69 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.68 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6330.8 | **NEUTRAL** |
| `B04_income` | 1063.7 | **GOOD** |
| `B05_profit_margin` | 16.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 93.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.37999999999999545 (now=93.5 vs prior_export=93.12 on finviz_2026-11-23) | **GOOD** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -0.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.34 vs prior=-0.34 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.69 | **GOOD** |
| `B13_short_float` | 7.54 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.69 (this export) | prior_export=10.69 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.68 (this export) | prior_export=8.68 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GLOB  ·  score **+16**  ·  Information Technology Services
price=50.04999923706055  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.80 on 2026-11-24; prev RSI=55.25 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.25@2026-11-23 → 63.80@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.25@2026-11-23 → 63.80@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.25@2026-11-23 → 63.80@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=7.6900 R=0.0000); 2026-11-23:GREEN:O=41.7200,C=45.5700,body=+3.8500,vol=1503500.0; 2026-11-24:GREEN:O=46.2100,C=50.0500,body=+3.8400,vol=2559300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4062800 Rvol=0); 2026-11-23:GREEN:O=41.7200,C=45.5700,body=+3.8500,vol=1503500.0; 2026-11-24:GREEN:O=46.2100,C=50.0500,body=+3.8400,vol=2559300.0 | **GOOD** |
| `A07_rvol` | RVOL=2.043 on 2026-11-24: today_vol=2559300 / avg20=1252700 (avg window 2026-10-27→2026-11-23, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.820 on 2026-11-24 (price=50.0500, mid=45.6465, upper=51.0186, lower=40.2744; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-24: price=50.0500 vs SMA50=40.9392 dist=+22.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=45.6465 SMA50=40.9392 SMA80=40.1411 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-24@33.2600; S2[2026-09-28→2026-10-26] low=2026-09-29@33.0000; S3[2026-10-27→2026-11-24] low=2026-11-20@39.1200 | lows=[33.2599983215332, 33.0, 39.119998931884766] span=18.55% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.7979398611033912 wick_frac=0.20206013889660884 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.4400:wick=0.5900; 2026-11-19:RED:body=-2.4300:wick=0.4400; 2026-11-20:RED:body=-1.8150:wick=3.9750; 2026-11-23:GREEN:body=+3.8500:wick=0.3800; 2026-11-24:GREEN:body=+3.8400:wick=1.7600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.07 (current export asof; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.06 (current export; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2445.11 | **NEUTRAL** |
| `B04_income` | 94.23 | **GOOD** |
| `B05_profit_margin` | 3.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.1600000000000037 (now=53.21 vs prior_export=52.05 on finviz_2026-11-23) | **GOOD** |
| `B09_analyst_recom` | 2.25 | **GOOD** |
| `B10_insider_transactions` | 0.94 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.94 vs prior=0.94 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | -11.67 | **BAD** |
| `B13_short_float` | 26.32 | **GOOD** |
| `B14_earnings_date` | 11/19/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.07 (this export) | prior_export=0.07 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.06 (this export) | prior_export=0.06 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YUM  ·  score **+16**  ·  Restaurants
price=166.3300018310547  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.67 on 2026-11-24; prev RSI=66.04 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.04@2026-11-23 → 68.67@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.04@2026-11-23 → 68.67@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.04@2026-11-23 → 68.67@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.2300 R=0.0000); 2026-11-23:GREEN:O=160.6400,C=164.4400,body=+3.8000,vol=1734500.0; 2026-11-24:GREEN:O=165.9000,C=166.3300,body=+0.4300,vol=1553700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3288200 Rvol=0); 2026-11-23:GREEN:O=160.6400,C=164.4400,body=+3.8000,vol=1734500.0; 2026-11-24:GREEN:O=165.9000,C=166.3300,body=+0.4300,vol=1553700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.658 on 2026-11-24: today_vol=1553700 / avg20=2362004 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.750 on 2026-11-24 (price=166.3300, mid=160.2700, upper=168.3526, lower=152.1874; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=166.3300 vs SMA50=150.0110 dist=+10.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=160.2700 SMA50=150.0110 SMA80=149.7913 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-18@134.2600; S2[2026-09-28→2026-10-26] low=2026-10-05@135.3500; S3[2026-10-27→2026-11-24] low=2026-10-27@146.1400 | lows=[134.25999450683594, 135.35000610351562, 146.13999938964844] span=8.85% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.5162645281660808 wick_frac=0.4837354718339192 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.8000:wick=2.1100; 2026-11-19:GREEN:body=+0.2500:wick=2.1600; 2026-11-20:RED:body=-0.6800:wick=2.8700; 2026-11-23:GREEN:body=+3.8000:wick=0.7800; 2026-11-24:GREEN:body=+0.4300:wick=1.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.26 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.08 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8796.0 | **NEUTRAL** |
| `B04_income` | 3443.0 | **GOOD** |
| `B05_profit_margin` | 39.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 176.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.5700000000000216 (now=176.96 vs prior_export=176.39 on finviz_2026-11-23) | **GOOD** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | -8.65 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.65 vs prior=-8.65 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.03 | **GOOD** |
| `B13_short_float` | 4.69 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.26 (this export) | prior_export=4.26 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.08 (this export) | prior_export=0.08 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NYT  ·  score **+16**  ·  Publishing
price=70.44000244140625  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.33 on 2026-11-24; prev RSI=45.34 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.34@2026-11-23 → 52.33@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.34@2026-11-23 → 52.33@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.34@2026-11-23 → 52.33@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=3.966 (G=2.3000 R=0.5800); 2026-11-23:RED:O=68.9000,C=68.3200,body=-0.5800,vol=1880900.0; 2026-11-24:GREEN:O=68.1400,C=70.4400,body=+2.3000,vol=2153200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=1.145 (Gvol=2153200 Rvol=1880900); 2026-11-23:RED:O=68.9000,C=68.3200,body=-0.5800,vol=1880900.0; 2026-11-24:GREEN:O=68.1400,C=70.4400,body=+2.3000,vol=2153200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.831 on 2026-11-24: today_vol=2153200 / avg20=2591475 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.066 on 2026-11-24 (price=70.4400, mid=70.1270, upper=74.8417, lower=65.4123; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=70.4400 vs SMA50=68.1026 dist=+3.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=70.1270 SMA50=68.1026 SMA80=67.6387 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-23@61.8000; S2[2026-09-28→2026-10-26] low=2026-10-02@58.9500; S3[2026-10-27→2026-11-24] low=2026-11-04@64.5400 | lows=[61.79999923706055, 58.95000076293945, 64.54000091552734] span=9.48% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.6336088038470592 wick_frac=0.3663911961529408 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=0.42028969482529854 wick_frac=0.5797103051747015 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.965509983952014 need>1.4; red_wick_gt_green=False 5d trail=2026-11-18:RED:body=-1.1900:wick=1.2400; 2026-11-19:RED:body=-0.6400:wick=1.7100; 2026-11-20:RED:body=-0.8400:wick=1.6700; 2026-11-23:RED:body=-0.5800:wick=0.8000; 2026-11-24:GREEN:body=+2.3000:wick=1.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.27 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.17 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3045.54 | **NEUTRAL** |
| `B04_income` | 408.53 | **GOOD** |
| `B05_profit_margin` | 13.41 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=76.0 vs prior_export=76.0 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.76 | **GOOD** |
| `B13_short_float` | 11.89 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.27 (this export) | prior_export=6.27 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.17 (this export) | prior_export=0.17 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+16**  ·  Credit Services
price=416.7799987792969  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.55 on 2026-11-24; prev RSI=49.50 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.50@2026-11-23 → 54.55@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.50@2026-11-23 → 54.55@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.50@2026-11-23 → 54.55@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=13.1700 R=0.0000); 2026-11-23:GREEN:O=404.0100,C=409.4000,body=+5.3900,vol=8676200.0; 2026-11-24:GREEN:O=409.0000,C=416.7800,body=+7.7800,vol=6253600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=14929800 Rvol=0); 2026-11-23:GREEN:O=404.0100,C=409.4000,body=+5.3900,vol=8676200.0; 2026-11-24:GREEN:O=409.0000,C=416.7800,body=+7.7800,vol=6253600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.837 on 2026-11-24: today_vol=6253600 / avg20=7467938 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.060 on 2026-11-24 (price=416.7800, mid=417.9000, upper=436.5763, lower=399.2237; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=416.7800 vs SMA50=396.1444 dist=+5.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=417.9000 SMA50=396.1444 SMA80=386.6755 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-22@359.8000; S2[2026-09-28→2026-10-26] low=2026-10-02@356.7500; S3[2026-10-27→2026-11-24] low=2026-10-28@394.0000 | lows=[359.79998779296875, 356.75, 394.0] span=10.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.7749591552646948 wick_frac=0.2250408447353052 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:RED:body=-0.7600:wick=5.3100; 2026-11-19:RED:body=-8.7900:wick=3.9800; 2026-11-20:RED:body=-1.6900:wick=5.8600; 2026-11-23:GREEN:body=+5.3900:wick=2.0100; 2026-11-24:GREEN:body=+7.7800:wick=1.6900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.2 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 45903.0 | **NEUTRAL** |
| `B04_income` | 23313.0 | **GOOD** |
| `B05_profit_margin` | 50.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 457.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=457.86 vs prior_export=457.86 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.08 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.2 (this export) | prior_export=1.2 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OSPN  ·  score **+15**  ·  Software - Infrastructure
price=23.360000610351562  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.24 on 2026-11-24; prev RSI=59.23 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.23@2026-11-23 → 61.24@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.23@2026-11-23 → 61.24@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.23@2026-11-23 → 61.24@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4100 R=0.0000); 2026-11-23:GREEN:O=22.2500,C=22.9100,body=+0.6600,vol=465000.0; 2026-11-24:GREEN:O=22.6100,C=23.3600,body=+0.7500,vol=517700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=982700 Rvol=0); 2026-11-23:GREEN:O=22.2500,C=22.9100,body=+0.6600,vol=465000.0; 2026-11-24:GREEN:O=22.6100,C=23.3600,body=+0.7500,vol=517700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.546 on 2026-11-24: today_vol=517700 / avg20=947789 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.251 on 2026-11-24 (price=23.3600, mid=22.6415, upper=25.4988, lower=19.7842; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=23.3600 vs SMA50=19.2067 dist=+21.62% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=22.6415 SMA50=19.2067 SMA80=17.5827 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-11-24 (63 bars); S1[2026-07-16→2026-08-18] low=2026-07-23@14.5474; S2[2026-08-19→2026-10-26] low=2026-08-26@15.0600; S3[2026-10-27→2026-11-24] low=2026-10-29@19.0400 | lows=[14.54736430915783, 15.0600004196167, 19.040000915527344] span=30.88% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.702439081122824 wick_frac=0.297560918877176 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.8700:wick=0.3400; 2026-11-19:RED:body=-0.3000:wick=1.0700; 2026-11-20:RED:body=-0.3300:wick=0.8200; 2026-11-23:GREEN:body=+0.6600:wick=0.1600; 2026-11-24:GREEN:body=+0.7500:wick=0.5000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=48.31 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.23 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 253.21 | **NEUTRAL** |
| `B04_income` | 69.69 | **GOOD** |
| `B05_profit_margin` | 27.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.5 vs prior_export=23.5 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | -6.64 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.64 vs prior=-6.64 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.96 | **GOOD** |
| `B13_short_float` | 9.69 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=48.31 (this export) | prior_export=48.31 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.23 (this export) | prior_export=11.23 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LIND  ·  score **+15**  ·  Travel Services
price=32.84000015258789  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.03 on 2026-11-24; prev RSI=47.08 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.08@2026-11-23 → 48.03@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.08@2026-11-23 → 48.03@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.08@2026-11-23 → 48.03@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.5600 R=0.0000); 2026-11-23:GREEN:O=30.7600,C=32.5700,body=+1.8100,vol=910500.0; 2026-11-24:GREEN:O=32.0900,C=32.8400,body=+0.7500,vol=1035100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1945600 Rvol=0); 2026-11-23:GREEN:O=30.7600,C=32.5700,body=+1.8100,vol=910500.0; 2026-11-24:GREEN:O=32.0900,C=32.8400,body=+0.7500,vol=1035100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.154 on 2026-11-24: today_vol=1035100 / avg20=896622 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.296 on 2026-11-24 (price=32.8400, mid=34.0075, upper=37.9499, lower=30.0651; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=32.8400 vs SMA50=32.6886 dist=+0.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=34.0075 SMA50=32.6886 SMA80=31.8992 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-24 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-10@24.3100; S2[2026-09-23→2026-10-26] low=2026-09-23@26.3200; S3[2026-10-27→2026-11-24] low=2026-11-20@30.0100 | lows=[24.309999465942383, 26.31999969482422, 30.010000228881836] span=23.45% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.6345573336583408 wick_frac=0.3654426663416592 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-18:GREEN:body=+0.8100:wick=0.8600; 2026-11-19:RED:body=-2.1800:wick=0.8400; 2026-11-20:RED:body=-0.5100:wick=0.7800; 2026-11-23:GREEN:body=+1.8100:wick=0.1400; 2026-11-24:GREEN:body=+0.7500:wick=1.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.36 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.13 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 859.59 | **NEUTRAL** |
| `B04_income` | 4.02 | **GOOD** |
| `B05_profit_margin` | 0.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.57 vs prior_export=36.57 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -6.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0600000000000005 (now=-6.8 vs prior=-6.86 on finviz_2026-11-23) | **GOOD** |
| `B12_institutional_transactions` | 4.96 | **GOOD** |
| `B13_short_float` | 10.64 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.13 (this export) | prior_export=2.13 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EL  ·  score **+15**  ·  Household & Personal Products
price=108.12999725341797  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.64 on 2026-11-24; prev RSI=46.72 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.72@2026-11-23 → 53.64@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.72@2026-11-23 → 53.64@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.72@2026-11-23 → 53.64@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.3500 R=0.0000); 2026-11-23:GREEN:O=102.0000,C=103.7500,body=+1.7500,vol=4244400.0; 2026-11-24:GREEN:O=103.5300,C=108.1300,body=+4.6000,vol=3776000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8020400 Rvol=0); 2026-11-23:GREEN:O=102.0000,C=103.7500,body=+1.7500,vol=4244400.0; 2026-11-24:GREEN:O=103.5300,C=108.1300,body=+4.6000,vol=3776000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.958 on 2026-11-24: today_vol=3776000 / avg20=3942047 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.025 on 2026-11-24 (price=108.1300, mid=107.9360, upper=115.8160, lower=100.0560; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=108.1300 vs SMA50=102.5494 dist=+5.44% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=107.9360 SMA50=102.5494 SMA80=99.9544 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-18@93.0000; S2[2026-09-28→2026-10-26] low=2026-10-01@88.6800; S3[2026-10-27→2026-11-24] low=2026-11-20@99.2200 | lows=[93.0, 88.68000030517578, 99.22000122070312] span=11.89% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.5703825897111778 wick_frac=0.42961741028882217 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-18:RED:body=-0.0800:wick=2.6000; 2026-11-19:RED:body=-1.5700:wick=1.1700; 2026-11-20:RED:body=-1.7800:wick=2.4900; 2026-11-23:GREEN:body=+1.7500:wick=2.8500; 2026-11-24:GREEN:body=+4.6000:wick=1.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=41.87 (current export asof; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 15320.0 | **NEUTRAL** |
| `B04_income` | 368.0 | **GOOD** |
| `B05_profit_margin` | 2.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 116.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=116.73 vs prior_export=116.73 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.18 | **GOOD** |
| `B10_insider_transactions` | -0.21 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.21 vs prior=-0.21 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.1 | **GOOD** |
| `B13_short_float` | 2.78 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=41.87 (this export) | prior_export=41.87 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BRX  ·  score **+15**  ·  REIT - Retail
price=30.850000381469727  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.08 on 2026-11-24; prev RSI=62.49 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.49@2026-11-23 → 68.08@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.49@2026-11-23 → 68.08@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.49@2026-11-23 → 68.08@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1800 R=0.0000); 2026-11-23:GREEN:O=29.8000,C=30.3400,body=+0.5400,vol=4769000.0; 2026-11-24:GREEN:O=30.2100,C=30.8500,body=+0.6400,vol=4241200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9010200 Rvol=0); 2026-11-23:GREEN:O=29.8000,C=30.3400,body=+0.5400,vol=4769000.0; 2026-11-24:GREEN:O=30.2100,C=30.8500,body=+0.6400,vol=4241200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.185 on 2026-11-24: today_vol=4241200 / avg20=3578259 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.698 on 2026-11-24 (price=30.8500, mid=30.0195, upper=31.2087, lower=28.8303; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=30.8500 vs SMA50=28.6916 dist=+7.52% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=30.02_50=28.69_80=29.11 on 2026-11-24: SMA20=30.0195 SMA50=28.6916 SMA80=29.1062 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-24 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-21@27.8000; S2[2026-09-23→2026-10-26] low=2026-10-08@26.7400; S3[2026-10-27→2026-11-24] low=2026-10-27@27.8300 | lows=[27.799999237060547, 26.739999771118164, 27.829999923706055] span=4.08% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.8123591231521397 wick_frac=0.18764087684786035 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.6900:wick=0.0900; 2026-11-19:RED:body=-0.1600:wick=0.3300; 2026-11-20:RED:body=-0.1800:wick=0.3700; 2026-11-23:GREEN:body=+0.5400:wick=0.1500; 2026-11-24:GREEN:body=+0.6400:wick=0.1200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.73 (current export asof; earnings_date=10/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=10/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1422.37 | **NEUTRAL** |
| `B04_income` | 425.04 | **GOOD** |
| `B05_profit_margin` | 29.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.18999999999999773 (now=33.78 vs prior_export=33.59 on finviz_2026-11-23) | **GOOD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | -0.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.57 vs prior=-0.57 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.59 | **GOOD** |
| `B13_short_float` | 8.37 | **NEUTRAL** |
| `B14_earnings_date` | 10/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.73 (this export) | prior_export=23.73 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATAT  ·  score **+15**  ·  Lodging
price=39.20000076293945  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.04 on 2026-11-24; prev RSI=62.27 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.27@2026-11-23 → 67.04@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.27@2026-11-23 → 67.04@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.27@2026-11-23 → 67.04@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=8.933 (G=1.3400 R=0.1500); 2026-11-23:RED:O=38.1100,C=37.9600,body=-0.1500,vol=726700.0; 2026-11-24:GREEN:O=37.8600,C=39.2000,body=+1.3400,vol=795500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=1.095 (Gvol=795500 Rvol=726700); 2026-11-23:RED:O=38.1100,C=37.9600,body=-0.1500,vol=726700.0; 2026-11-24:GREEN:O=37.8600,C=39.2000,body=+1.3400,vol=795500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.772 on 2026-11-24: today_vol=795500 / avg20=1030378 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.593 on 2026-11-24 (price=39.2000, mid=36.1630, upper=41.2840, lower=31.0420; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=39.2000 vs SMA50=34.0108 dist=+15.26% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.16_50=34.01_80=34.16 on 2026-11-24: SMA20=36.1630 SMA50=34.0108 SMA80=34.1582 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-15@31.0800; S2[2026-09-28→2026-10-26] low=2026-10-08@31.3000; S3[2026-10-27→2026-11-24] low=2026-10-28@30.0700 | lows=[31.079999923706055, 31.299999237060547, 30.06999969482422] span=4.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.9781030637333386 wick_frac=0.02189693626666147 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=0.2336464702282275 wick_frac=0.7663535297717725 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.933243476934031 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+1.0200:wick=0.6150; 2026-11-19:RED:body=-0.5500:wick=0.3800; 2026-11-20:RED:body=-0.2300:wick=0.3800; 2026-11-23:RED:body=-0.1500:wick=0.4920; 2026-11-24:GREEN:body=+1.3400:wick=0.0300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.36 (current export asof; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.35 (current export; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1820.59 | **NEUTRAL** |
| `B04_income` | 281.67 | **GOOD** |
| `B05_profit_margin` | 15.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.4 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.14999999999999858 (now=49.4 vs prior_export=49.55 on finviz_2026-11-23) | **BAD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.73 | **GOOD** |
| `B13_short_float` | 2.05 | **NEUTRAL** |
| `B14_earnings_date` | 11/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.36 (this export) | prior_export=12.36 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.35 (this export) | prior_export=6.35 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SEZL  ·  score **+15**  ·  Credit Services
price=145.80999755859375  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.78 on 2026-11-24; prev RSI=51.19 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.19@2026-11-23 → 52.78@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.19@2026-11-23 → 52.78@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.19@2026-11-23 → 52.78@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=1.566 (G=4.6500 R=2.9700); 2026-11-23:RED:O=146.3400,C=143.3700,body=-2.9700,vol=434400.0; 2026-11-24:GREEN:O=141.1600,C=145.8100,body=+4.6500,vol=1330300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=3.062 (Gvol=1330300 Rvol=434400); 2026-11-23:RED:O=146.3400,C=143.3700,body=-2.9700,vol=434400.0; 2026-11-24:GREEN:O=141.1600,C=145.8100,body=+4.6500,vol=1330300.0 | **GOOD** |
| `A07_rvol` | RVOL=2.031 on 2026-11-24: today_vol=1330300 / avg20=654909 (avg window 2026-10-27→2026-11-23, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.036 on 2026-11-24 (price=145.8100, mid=145.0160, upper=167.2267, lower=122.8053; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=145.8100 vs SMA50=129.8209 dist=+12.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=145.0160 SMA50=129.8209 SMA80=128.6702 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-24@104.8700; S2[2026-09-28→2026-10-26] low=2026-09-30@106.0000; S3[2026-10-27→2026-11-24] low=2026-11-03@121.4100 | lows=[104.87000274658203, 106.0, 121.41000366210938] span=15.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.4899885358163287 wick_frac=0.5100114641836714 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=0.424285888671875 wick_frac=0.575714111328125 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5656538670995983 need>1.4; red_wick_gt_green=False 5d trail=2026-11-18:GREEN:body=+2.3100:wick=3.4800; 2026-11-19:RED:body=-13.6000:wick=1.4000; 2026-11-20:RED:body=-5.3800:wick=8.7600; 2026-11-23:RED:body=-2.9700:wick=4.0300; 2026-11-24:GREEN:body=+4.6500:wick=4.8400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.8 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.41 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 581.59 | **NEUTRAL** |
| `B04_income` | 182.58 | **GOOD** |
| `B05_profit_margin` | 31.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 192.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=192.67 vs prior_export=192.67 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -0.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.77 vs prior=-0.77 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.1 | **GOOD** |
| `B13_short_float` | 23.6 | **GOOD** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.41 (this export) | prior_export=7.41 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CL  ·  score **+15**  ·  Household & Personal Products
price=99.23999786376953  pair=`2026-11-23→2026-11-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.49 on 2026-11-24; prev RSI=55.16 on 2026-11-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.16@2026-11-23 → 58.49@2026-11-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.16@2026-11-23 → 58.49@2026-11-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.16@2026-11-23 → 58.49@2026-11-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.7200 R=0.0000); 2026-11-23:GREEN:O=95.2000,C=97.9300,body=+2.7300,vol=9951600.0; 2026-11-24:GREEN:O=98.2500,C=99.2400,body=+0.9900,vol=4570800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-23 + 2026-11-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=14522400 Rvol=0); 2026-11-23:GREEN:O=95.2000,C=97.9300,body=+2.7300,vol=9951600.0; 2026-11-24:GREEN:O=98.2500,C=99.2400,body=+0.9900,vol=4570800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.795 on 2026-11-24: today_vol=4570800 / avg20=5748179 (avg window 2026-10-27→2026-11-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.380 on 2026-11-24 (price=99.2400, mid=97.5505, upper=101.9997, lower=93.1013; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-24: price=99.2400 vs SMA50=92.5096 dist=+7.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-24: SMA20=97.5505 SMA50=92.5096 SMA80=91.8204 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-27→2026-11-24 (63 bars); S1[2026-08-27→2026-09-25] low=2026-09-25@85.0600; S2[2026-09-28→2026-10-26] low=2026-10-02@83.9900; S3[2026-10-27→2026-11-24] low=2026-10-28@90.4700 | lows=[85.05999755859375, 83.98999786376953, 90.47000122070312] span=7.72% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: GREEN body_frac=0.6491061052200586 wick_frac=0.3508938947799413 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-23+2026-11-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-18:GREEN:body=+0.1700:wick=1.5900; 2026-11-19:RED:body=-0.4700:wick=1.3100; 2026-11-20:GREEN:body=+0.5300:wick=0.9200; 2026-11-23:GREEN:body=+2.7300:wick=1.1800; 2026-11-24:GREEN:body=+0.9900:wick=0.6600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.71 (current export asof; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.61 (current export; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21284.0 | **NEUTRAL** |
| `B04_income` | 2053.0 | **GOOD** |
| `B05_profit_margin` | 9.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.42 vs prior_export=102.42 on finviz_2026-11-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.96 | **GOOD** |
| `B10_insider_transactions` | -21.26 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.26 vs prior=-21.26 on finviz_2026-11-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.14 | **GOOD** |
| `B13_short_float` | 2.34 | **NEUTRAL** |
| `B14_earnings_date` | 10/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.61 (this export) | prior_export=0.61 (finviz_2026-11-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-24_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.