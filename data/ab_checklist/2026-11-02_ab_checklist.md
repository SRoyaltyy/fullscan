# A+B1 Feature Checklist — 2026-11-02

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-11-02.csv` · prior export for Δ: `2026-10-30`
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
| 1 | MTDR | +17 | 17 | 0 | 2026-10-29→2026-10-30 | Oil & Gas E&P |
| 2 | EPD | +16 | 16 | 0 | 2026-10-29→2026-10-30 | Oil & Gas Midstream |
| 3 | WMG | +16 | 16 | 0 | 2026-10-29→2026-10-30 | Entertainment |
| 4 | FLS | +16 | 17 | 1 | 2026-10-29→2026-10-30 | Specialty Industrial Machinery |
| 5 | BP | +16 | 16 | 0 | 2026-10-29→2026-10-30 | Oil & Gas Integrated |
| 6 | AMZN | +15 | 17 | 2 | 2026-10-29→2026-10-30 | Internet Retail |
| 7 | TWLO | +15 | 17 | 2 | 2026-10-29→2026-10-30 | Software - Infrastructure |
| 8 | TASK | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Information Technology Services |
| 9 | BEKE | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Real Estate Services |
| 10 | SM | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Oil & Gas E&P |
| 11 | V | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Credit Services |
| 12 | CVLT | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Software - Application |
| 13 | SHAK | +15 | 16 | 1 | 2026-10-29→2026-10-30 | Restaurants |
| 14 | HAE | +14 | 15 | 1 | 2026-10-29→2026-10-30 | Medical Devices |
| 15 | HHH | +14 | 15 | 1 | 2026-10-29→2026-10-30 | Real Estate - Diversified |

## Full checklist — top 15

### MTDR  ·  score **+17**  ·  Oil & Gas E&P
price=60.880001068115234  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.96 on 2026-10-30; prev RSI=62.93 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.93@2026-10-29 → 65.96@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.93@2026-10-29 → 65.96@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.93@2026-10-29 → 65.96@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=1.241 (G=0.3600 R=0.2900); 2026-10-29:RED:O=60.1200,C=59.8300,body=-0.2900,vol=1231202.0; 2026-10-30:GREEN:O=60.5200,C=60.8800,body=+0.3600,vol=1424235.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.157 (Gvol=1424235 Rvol=1231202); 2026-10-29:RED:O=60.1200,C=59.8300,body=-0.2900,vol=1231202.0; 2026-10-30:GREEN:O=60.5200,C=60.8800,body=+0.3600,vol=1424235.0 | **GOOD** |
| `A07_rvol` | RVOL=1.065 on 2026-10-30: today_vol=1424235 / avg20=1337936 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.778 on 2026-10-30 (price=60.8800, mid=56.7115, upper=62.0682, lower=51.3548; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=60.8800 vs SMA50=56.6432 dist=+7.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=56.7115 SMA50=56.6432 SMA80=54.6933 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-05@46.5316; S2[2026-09-02→2026-10-01] low=2026-09-29@50.3900; S3[2026-10-02→2026-10-30] low=2026-10-07@51.9800 | lows=[46.531587687321455, 50.38999938964844, 51.97999954223633] span=11.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.5179867171634008 wick_frac=0.48201328283659917 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.2959155472341486 wick_frac=0.7040844527658514 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2413938253903527 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:GREEN:body=+1.5500:wick=0.8600; 2026-10-27:RED:body=-0.5400:wick=1.2650; 2026-10-28:RED:body=-0.0600:wick=1.0400; 2026-10-29:RED:body=-0.2900:wick=0.6900; 2026-10-30:GREEN:body=+0.3600:wick=0.3350 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.59 vs prior_export=70.59 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.4 vs prior=0.4 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 11.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPD  ·  score **+16**  ·  Oil & Gas Midstream
price=38.150001525878906  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.79 on 2026-10-30; prev RSI=61.99 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.99@2026-10-29 → 54.79@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.99@2026-10-29 → 54.79@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.99@2026-10-29 → 54.79@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=3.250 (G=0.5200 R=0.1600); 2026-10-29:GREEN:O=38.5300,C=39.0500,body=+0.5200,vol=6866300.0; 2026-10-30:RED:O=38.3100,C=38.1500,body=-0.1600,vol=4653400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.476 (Gvol=6866300 Rvol=4653400); 2026-10-29:GREEN:O=38.5300,C=39.0500,body=+0.5200,vol=6866300.0; 2026-10-30:RED:O=38.3100,C=38.1500,body=-0.1600,vol=4653400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.252 on 2026-10-30: today_vol=4653400 / avg20=3715787 (avg window 2026-09-17→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.494 on 2026-10-30 (price=38.1500, mid=37.1030, upper=39.2215, lower=34.9845; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=38.1500 vs SMA50=38.0040 dist=+0.38% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=37.10_50=38.00_80=37.72 on 2026-10-30: SMA20=37.1030 SMA50=38.0040 SMA80=37.7161 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-30 (63 bars); S1[2026-07-20→2026-08-17] low=2026-07-31@37.2200; S2[2026-08-18→2026-09-16] low=2026-08-21@37.9600; S3[2026-09-17→2026-10-30] low=2026-10-01@35.3700 | lows=[37.220001220703125, 37.959999084472656, 35.369998931884766] span=7.32% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.6842126397261443 wick_frac=0.31578736027385573 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.2580647146047782 wick_frac=0.7419352853952218 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.250005960470162 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.5900:wick=0.3074; 2026-10-12:RED:body=-0.2400:wick=0.2600; 2026-10-28:GREEN:body=+0.8500:wick=0.1400; 2026-10-29:GREEN:body=+0.5200:wick=0.2400; 2026-10-30:RED:body=-0.1600:wick=0.4600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.24 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=33.47 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 58217.0 | **NEUTRAL** |
| `B04_income` | 6244.0 | **GOOD** |
| `B05_profit_margin` | 10.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.65 vs prior_export=41.65 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.5 | **GOOD** |
| `B13_short_float` | 1.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.24 (this export) | prior_export=12.24 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=33.47 (this export) | prior_export=33.47 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMG  ·  score **+16**  ·  Entertainment
price=30.6200008392334  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.81 on 2026-10-30; prev RSI=57.78 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.78@2026-10-29 → 58.81@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.78@2026-10-29 → 58.81@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.78@2026-10-29 → 58.81@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.9000 R=0.1800); 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0; 2026-10-30:RED:O=30.8000,C=30.6200,body=-0.1800,vol=2105715.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.539 (Gvol=3240998 Rvol=2105715); 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0; 2026-10-30:RED:O=30.8000,C=30.6200,body=-0.1800,vol=2105715.0 | **GOOD** |
| `A07_rvol` | RVOL=0.924 on 2026-10-30: today_vol=2105715 / avg20=2277847 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.287 on 2026-10-30 (price=30.6200, mid=29.8610, upper=32.5076, lower=27.2144; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=30.6200 vs SMA50=28.7002 dist=+6.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=29.8610 SMA50=28.7002 SMA80=28.1215 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-12@24.5150; S2[2026-09-02→2026-10-01] low=2026-09-28@26.4500; S3[2026-10-02→2026-10-30] low=2026-10-05@26.7000 | lows=[24.514999389648438, 26.450000762939453, 26.700000762939453] span=8.91% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.6498200066653997 wick_frac=0.3501799933346003 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.24170546924223565 wick_frac=0.7582945307577643 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.000042385902449 need>1.4; red_wick_gt_green=False 5d trail=2026-10-26:RED:body=-0.4600:wick=0.3050; 2026-10-27:RED:body=-0.2700:wick=0.1900; 2026-10-28:RED:body=-0.4300:wick=0.4700; 2026-10-29:GREEN:body=+0.9000:wick=0.4850; 2026-10-30:RED:body=-0.1800:wick=0.5647 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+16**  ·  Specialty Industrial Machinery
price=79.01000213623047  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.99 on 2026-10-30; prev RSI=63.51 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.51@2026-10-29 → 59.99@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.51@2026-10-29 → 59.99@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.51@2026-10-29 → 59.99@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=176.816 (G=8.8400 R=0.0500); 2026-10-29:GREEN:O=71.5100,C=80.3500,body=+8.8400,vol=3722000.0; 2026-10-30:RED:O=79.0600,C=79.0100,body=-0.0500,vol=2528900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.472 (Gvol=3722000 Rvol=2528900); 2026-10-29:GREEN:O=71.5100,C=80.3500,body=+8.8400,vol=3722000.0; 2026-10-30:RED:O=79.0600,C=79.0100,body=-0.0500,vol=2528900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.498 on 2026-10-30: today_vol=2528900 / avg20=1688259 (avg window 2026-09-17→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.917 on 2026-10-30 (price=79.0100, mid=74.0970, upper=79.4545, lower=68.7395; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-30: price=79.0100 vs SMA50=76.4074 dist=+3.41% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=74.10_50=76.41_80=75.15 on 2026-10-30: SMA20=74.0970 SMA50=76.4074 SMA80=75.1493 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-30 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-20@66.3300; S2[2026-08-18→2026-09-16] low=2026-09-14@68.3600; S3[2026-09-17→2026-10-30] low=2026-10-08@68.8000 | lows=[66.33000183105469, 68.36000061035156, 68.80000305175781] span=3.72% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=1.0 wick_frac=0.0 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.015478479410055698 wick_frac=0.9845215205899444 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=176.81611475660003 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+1.4800:wick=2.1500; 2026-10-12:RED:body=-1.0100:wick=0.9600; 2026-10-28:RED:body=-0.4300:wick=0.9500; 2026-10-29:GREEN:body=+8.8400:wick=0.0000; 2026-10-30:RED:body=-0.0500:wick=3.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.32 | **GOOD** |
| `B13_short_float` | 6.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BP  ·  score **+16**  ·  Oil & Gas Integrated
price=50.38999938964844  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.21 on 2026-10-30; prev RSI=63.05 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.05@2026-10-29 → 63.21@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.05@2026-10-29 → 63.21@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.05@2026-10-29 → 63.21@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=5.250 (G=0.6300 R=0.1200); 2026-10-29:RED:O=50.4700,C=50.3500,body=-0.1200,vol=7294437.0; 2026-10-30:GREEN:O=49.7600,C=50.3900,body=+0.6300,vol=9865969.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.353 (Gvol=9865969 Rvol=7294437); 2026-10-29:RED:O=50.4700,C=50.3500,body=-0.1200,vol=7294437.0; 2026-10-30:GREEN:O=49.7600,C=50.3900,body=+0.6300,vol=9865969.0 | **GOOD** |
| `A07_rvol` | RVOL=1.083 on 2026-10-30: today_vol=9865969 / avg20=9109046 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.451 on 2026-10-30 (price=50.3900, mid=48.4755, upper=52.7246, lower=44.2264; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=50.3900 vs SMA50=45.9614 dist=+9.64% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=48.4755 SMA50=45.9614 SMA80=44.5590 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-05@40.6603; S2[2026-09-02→2026-10-01] low=2026-09-22@42.4700; S3[2026-10-02→2026-10-30] low=2026-10-02@44.0150 | lows=[40.66027501103988, 42.470001220703125, 44.01499938964844] span=8.25% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.5806466356802824 wick_frac=0.4193533643197176 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.14454675782973092 wick_frac=0.8554532421702691 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.249888740542946 need>1.4; red_wick_gt_green=False 5d trail=2026-10-26:GREEN:body=+0.1100:wick=0.7500; 2026-10-27:RED:body=-1.1600:wick=0.4800; 2026-10-28:GREEN:body=+0.5300:wick=0.8550; 2026-10-29:RED:body=-0.1200:wick=0.7102; 2026-10-30:GREEN:body=+0.6300:wick=0.4550 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.35 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.04 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 216829.8 | **NEUTRAL** |
| `B04_income` | 5490.34 | **GOOD** |
| `B05_profit_margin` | 2.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.54 vs prior_export=50.54 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 60.28 | **GOOD** |
| `B13_short_float` | 0.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.35 (this export) | prior_export=18.35 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.04 (this export) | prior_export=11.04 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMZN  ·  score **+15**  ·  Internet Retail
price=240.35000610351562  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=38.26 on 2026-10-30; prev RSI=40.72 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 40.72@2026-10-29 → 38.26@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 40.72@2026-10-29 → 38.26@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 40.72@2026-10-29 → 38.26@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=2.506 (G=7.8200 R=3.1200); 2026-10-29:RED:O=246.9100,C=243.7900,body=-3.1200,vol=79750364.0; 2026-10-30:GREEN:O=232.5300,C=240.3500,body=+7.8200,vol=104652185.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.312 (Gvol=104652185 Rvol=79750364); 2026-10-29:RED:O=246.9100,C=243.7900,body=-3.1200,vol=79750364.0; 2026-10-30:GREEN:O=232.5300,C=240.3500,body=+7.8200,vol=104652185.0 | **GOOD** |
| `A07_rvol` | RVOL=2.824 on 2026-10-30: today_vol=104652185 / avg20=37054823 (avg window 2026-10-02→2026-10-29, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.107 on 2026-10-30 (price=240.3500, mid=254.3905, upper=267.0737, lower=241.7073; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-30: price=240.3500 vs SMA50=254.2790 dist=-5.48% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=254.39_50=254.28_80=254.79 on 2026-10-30: SMA20=254.3905 SMA50=254.2790 SMA80=254.7909 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-09-01@251.9300; S2[2026-09-02→2026-10-01] low=2026-09-16@244.3000; S3[2026-10-02→2026-10-30] low=2026-10-30@229.3632 | lows=[251.92999267578125, 244.3000030517578, 229.3632049560547] span=9.84% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.6416785822500795 wick_frac=0.3583214177499205 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.3949383274036474 wick_frac=0.6050616725963526 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.506404268534232 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:RED:body=-0.3800:wick=3.1800; 2026-10-27:GREEN:body=+2.7500:wick=3.2099; 2026-10-28:GREEN:body=+1.6000:wick=1.7800; 2026-10-29:RED:body=-3.1200:wick=4.7800; 2026-10-30:GREEN:body=+7.8200:wick=4.3668 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=215.36 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.81 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 775680.0 | **NEUTRAL** |
| `B04_income` | 135281.0 | **GOOD** |
| `B05_profit_margin` | 17.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 330.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=330.94 vs prior_export=330.94 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -0.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.15 vs prior=-0.15 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.02 | **GOOD** |
| `B13_short_float` | 0.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=215.36 (this export) | prior_export=215.36 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.81 (this export) | prior_export=1.81 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TWLO  ·  score **+15**  ·  Software - Infrastructure
price=205.02999877929688  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=30.22 on 2026-10-30; prev RSI=40.48 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 40.48@2026-10-29 → 30.22@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 40.48@2026-10-29 → 30.22@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 40.48@2026-10-29 → 30.22@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=15.897 (G=18.4400 R=1.1600); 2026-10-29:RED:O=243.0600,C=241.9000,body=-1.1600,vol=4847236.0; 2026-10-30:GREEN:O=186.5900,C=205.0300,body=+18.4400,vol=7339001.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.514 (Gvol=7339001 Rvol=4847236); 2026-10-29:RED:O=243.0600,C=241.9000,body=-1.1600,vol=4847236.0; 2026-10-30:GREEN:O=186.5900,C=205.0300,body=+18.4400,vol=7339001.0 | **GOOD** |
| `A07_rvol` | RVOL=1.741 on 2026-10-30: today_vol=7339001 / avg20=4214583 (avg window 2026-10-02→2026-10-29, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.446 on 2026-10-30 (price=205.0300, mid=264.4845, upper=305.5999, lower=223.3691; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-30: price=205.0300 vs SMA50=256.6200 dist=-20.10% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=264.4845 SMA50=256.6200 SMA80=240.3704 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-06@187.1000; S2[2026-09-02→2026-10-01] low=2026-09-08@221.8600; S3[2026-10-02→2026-10-30] low=2026-10-30@180.0000 | lows=[187.10000610351562, 221.86000061035156, 180.0] span=23.26% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.6856292809846414 wick_frac=0.3143707190153586 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.08033273488675834 wick_frac=0.9196672651132417 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.896503643682092 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:GREEN:body=+0.7500:wick=6.2700; 2026-10-27:RED:body=-7.4400:wick=3.9015; 2026-10-28:RED:body=-0.6800:wick=9.3500; 2026-10-29:RED:body=-1.1600:wick=13.2800; 2026-10-30:GREEN:body=+18.4400:wick=8.4550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.06 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5572.33 | **NEUTRAL** |
| `B04_income` | 1148.74 | **GOOD** |
| `B05_profit_margin` | 20.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 267.41 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=267.41 vs prior_export=267.41 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -81.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-81.43 vs prior=-81.43 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 3.53 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.06 (this export) | prior_export=11.06 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.76 (this export) | prior_export=4.76 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TASK  ·  score **+15**  ·  Information Technology Services
price=8.300000190734863  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.81 on 2026-10-30; prev RSI=52.18 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.18@2026-10-29 → 55.81@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.18@2026-10-29 → 55.81@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.18@2026-10-29 → 55.81@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=3.333 (G=0.2000 R=0.0600); 2026-10-29:RED:O=8.1700,C=8.1100,body=-0.0600,vol=259700.0; 2026-10-30:GREEN:O=8.1000,C=8.3000,body=+0.2000,vol=516500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.989 (Gvol=516500 Rvol=259700); 2026-10-29:RED:O=8.1700,C=8.1100,body=-0.0600,vol=259700.0; 2026-10-30:GREEN:O=8.1000,C=8.3000,body=+0.2000,vol=516500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.122 on 2026-10-30: today_vol=516500 / avg20=460446 (avg window 2026-09-17→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.327 on 2026-10-30 (price=8.3000, mid=8.1570, upper=8.5944, lower=7.7196; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=8.3000 vs SMA50=7.9275 dist=+4.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=8.1570 SMA50=7.9275 SMA80=7.0051 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-30 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@5.3200; S2[2026-08-18→2026-09-16] low=2026-08-18@7.1650; S3[2026-09-17→2026-10-30] low=2026-09-24@7.5300 | lows=[5.320000171661377, 7.164999961853027, 7.53000020980835] span=41.54% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.5405401922298717 wick_frac=0.45945980777012835 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.31579079455905235 wick_frac=0.6842092054409477 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.333306842565366 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.1000:wick=0.0600; 2026-10-12:RED:body=-0.1400:wick=0.1300; 2026-10-28:RED:body=-0.1300:wick=0.1950; 2026-10-29:RED:body=-0.0600:wick=0.1300; 2026-10-30:GREEN:body=+0.2000:wick=0.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.65 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.8 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1226.79 | **NEUTRAL** |
| `B04_income` | 107.38 | **GOOD** |
| `B05_profit_margin` | 8.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.5 vs prior_export=8.5 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.88 | **NEUTRAL** |
| `B10_insider_transactions` | -0.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.05 vs prior=-0.05 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.32 | **GOOD** |
| `B13_short_float` | 11.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.65 (this export) | prior_export=16.65 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.8 (this export) | prior_export=3.8 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BEKE  ·  score **+15**  ·  Real Estate Services
price=17.709999084472656  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.64 on 2026-10-30; prev RSI=51.68 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.68@2026-10-29 → 55.64@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.68@2026-10-29 → 55.64@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.68@2026-10-29 → 55.64@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3600 R=0.0000); 2026-10-29:GREEN:O=17.2600,C=17.4200,body=+0.1600,vol=2904906.0; 2026-10-30:GREEN:O=17.5100,C=17.7100,body=+0.2000,vol=4036290.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6941196 Rvol=0); 2026-10-29:GREEN:O=17.2600,C=17.4200,body=+0.1600,vol=2904906.0; 2026-10-30:GREEN:O=17.5100,C=17.7100,body=+0.2000,vol=4036290.0 | **GOOD** |
| `A07_rvol` | RVOL=0.983 on 2026-10-30: today_vol=4036290 / avg20=4104499 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.294 on 2026-10-30 (price=17.7100, mid=17.4590, upper=18.3130, lower=16.6050; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=17.7100 vs SMA50=17.2044 dist=+2.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=17.4590 SMA50=17.2044 SMA80=17.0240 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-06@16.6700; S2[2026-09-02→2026-10-01] low=2026-09-16@15.8600; S3[2026-10-02→2026-10-30] low=2026-10-02@16.3700 | lows=[16.670000076293945, 15.859999656677246, 16.3700008392334] span=5.11% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.7555523248382336 wick_frac=0.24444767516176633 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:GREEN:body=+0.1000:wick=0.3850; 2026-10-27:GREEN:body=+0.0800:wick=0.2100; 2026-10-28:RED:body=-0.1500:wick=0.2550; 2026-10-29:GREEN:body=+0.1600:wick=0.0650; 2026-10-30:GREEN:body=+0.2000:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.18 (current export asof; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 12683.5 | **NEUTRAL** |
| `B04_income` | 683.71 | **GOOD** |
| `B05_profit_margin` | 5.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.08 vs prior_export=24.08 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | -27.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-27.52 vs prior=-27.52 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.7 | **GOOD** |
| `B13_short_float` | 1.98 | **NEUTRAL** |
| `B14_earnings_date` | 8/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.18 (this export) | prior_export=31.18 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SM  ·  score **+15**  ·  Oil & Gas E&P
price=40.380001068115234  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.82 on 2026-10-30; prev RSI=60.40 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.40@2026-10-29 → 63.82@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.40@2026-10-29 → 63.82@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.40@2026-10-29 → 63.82@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=19.003 (G=0.3800 R=0.0200); 2026-10-29:RED:O=39.6000,C=39.5800,body=-0.0200,vol=1897135.0; 2026-10-30:GREEN:O=40.0000,C=40.3800,body=+0.3800,vol=2611415.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.377 (Gvol=2611415 Rvol=1897135); 2026-10-29:RED:O=39.6000,C=39.5800,body=-0.0200,vol=1897135.0; 2026-10-30:GREEN:O=40.0000,C=40.3800,body=+0.3800,vol=2611415.0 | **GOOD** |
| `A07_rvol` | RVOL=1.088 on 2026-10-30: today_vol=2611415 / avg20=2399528 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.655 on 2026-10-30 (price=40.3800, mid=38.2285, upper=41.5139, lower=34.9431; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=40.3800 vs SMA50=37.1484 dist=+8.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=38.2285 SMA50=37.1484 SMA80=35.1102 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-06@28.2000; S2[2026-09-02→2026-10-01] low=2026-09-29@32.5510; S3[2026-10-02→2026-10-30] low=2026-10-02@34.1300 | lows=[28.200000762939453, 32.55099868774414, 34.130001068115234] span=21.03% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.5066680908203125 wick_frac=0.4933319091796875 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.028566601816883832 wick_frac=0.9714333981831161 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.003243037008776 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:GREEN:body=+0.6400:wick=0.4100; 2026-10-27:RED:body=-0.8300:wick=0.5688; 2026-10-28:GREEN:body=+0.1700:wick=0.5401; 2026-10-29:RED:body=-0.0200:wick=0.6800; 2026-10-30:GREEN:body=+0.3800:wick=0.3700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.53 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.46 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5495.59 | **NEUTRAL** |
| `B04_income` | 1000.09 | **GOOD** |
| `B05_profit_margin` | 18.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.11 vs prior_export=44.11 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.96 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.96 vs prior=-1.96 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.44 | **GOOD** |
| `B13_short_float` | 5.72 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.53 (this export) | prior_export=11.53 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.46 (this export) | prior_export=23.46 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+15**  ·  Credit Services
price=413.5899963378906  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.44 on 2026-10-30; prev RSI=67.70 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.70@2026-10-29 → 69.44@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.70@2026-10-29 → 69.44@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.70@2026-10-29 → 69.44@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=3.423 (G=5.6650 R=1.6550); 2026-10-29:GREEN:O=404.8750,C=410.5400,body=+5.6650,vol=7689807.0; 2026-10-30:RED:O=415.2450,C=413.5900,body=-1.6550,vol=7073982.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.087 (Gvol=7689807 Rvol=7073982); 2026-10-29:GREEN:O=404.8750,C=410.5400,body=+5.6650,vol=7689807.0; 2026-10-30:RED:O=415.2450,C=413.5900,body=-1.6550,vol=7073982.0 | **GOOD** |
| `A07_rvol` | RVOL=1.159 on 2026-10-30: today_vol=7073982 / avg20=6101773 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.602 on 2026-10-30 (price=413.5900, mid=396.1235, upper=425.1376, lower=367.1094; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=413.5900 vs SMA50=381.3110 dist=+8.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=396.1235 SMA50=381.3110 SMA80=373.8010 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-18@358.0500; S2[2026-09-02→2026-10-01] low=2026-10-01@357.3600; S3[2026-10-02→2026-10-30] low=2026-10-02@356.7500 | lows=[358.04998779296875, 357.3599853515625, 356.75] span=0.36% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.7024187018019177 wick_frac=0.29758129819808227 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.3201168762174606 wick_frac=0.6798831237825395 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.422968412900371 need>1.4; red_wick_gt_green=False 5d trail=2026-10-26:RED:body=-1.5900:wick=3.3000; 2026-10-27:RED:body=-0.2100:wick=5.9366; 2026-10-28:GREEN:body=+5.3500:wick=5.2700; 2026-10-29:GREEN:body=+5.6650:wick=2.4000; 2026-10-30:RED:body=-1.6550:wick=3.5150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.68 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.04 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 44488.0 | **NEUTRAL** |
| `B04_income` | 22399.0 | **GOOD** |
| `B05_profit_margin` | 50.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 423.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=423.54 vs prior_export=423.54 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -0.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.1 vs prior=-0.1 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.87 | **GOOD** |
| `B13_short_float` | 1.09 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.04 (this export) | prior_export=2.04 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVLT  ·  score **+15**  ·  Software - Application
price=165.1300048828125  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.58 on 2026-10-30; prev RSI=63.68 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.68@2026-10-29 → 56.58@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.68@2026-10-29 → 56.58@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.68@2026-10-29 → 56.58@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=1.747 (G=10.1500 R=5.8100); 2026-10-29:GREEN:O=161.4600,C=171.6100,body=+10.1500,vol=709958.0; 2026-10-30:RED:O=170.9400,C=165.1300,body=-5.8100,vol=651170.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.090 (Gvol=709958 Rvol=651170); 2026-10-29:GREEN:O=161.4600,C=171.6100,body=+10.1500,vol=709958.0; 2026-10-30:RED:O=170.9400,C=165.1300,body=-5.8100,vol=651170.0 | **GOOD** |
| `A07_rvol` | RVOL=0.935 on 2026-10-30: today_vol=651170 / avg20=696266 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.353 on 2026-10-30 (price=165.1300, mid=159.8327, upper=174.8580, lower=144.8075; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=165.1300 vs SMA50=148.0765 dist=+11.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=159.8327 SMA50=148.0765 SMA80=145.2192 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-04@125.9400; S2[2026-09-02→2026-10-01] low=2026-09-02@125.9000; S3[2026-10-02→2026-10-30] low=2026-10-16@147.0400 | lows=[125.94000244140625, 125.9000015258789, 147.0399932861328] span=16.79% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.9504628789513316 wick_frac=0.04953712104866837 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.49237254711796463 wick_frac=0.5076274528820354 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7469876353856981 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:RED:body=-2.2700:wick=5.2900; 2026-10-27:GREEN:body=+18.8200:wick=1.1300; 2026-10-28:RED:body=-5.0900:wick=2.1200; 2026-10-29:GREEN:body=+10.1500:wick=0.5290; 2026-10-30:RED:body=-5.8100:wick=5.9900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.16 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1215.84 | **NEUTRAL** |
| `B04_income` | 68.3 | **GOOD** |
| `B05_profit_margin` | 5.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 160.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=160.75 vs prior_export=160.75 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -15.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.45 vs prior=-15.45 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 8.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.16 (this export) | prior_export=22.16 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHAK  ·  score **+15**  ·  Restaurants
price=70.4800033569336  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.14 on 2026-10-30; prev RSI=60.22 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.22@2026-10-29 → 60.14@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.22@2026-10-29 → 60.14@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.22@2026-10-29 → 60.14@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=3.641 (G=2.3300 R=0.6400); 2026-10-29:GREEN:O=68.1800,C=70.5100,body=+2.3300,vol=1480110.0; 2026-10-30:RED:O=71.1200,C=70.4800,body=-0.6400,vol=954911.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.550 (Gvol=1480110 Rvol=954911); 2026-10-29:GREEN:O=68.1800,C=70.5100,body=+2.3300,vol=1480110.0; 2026-10-30:RED:O=71.1200,C=70.4800,body=-0.6400,vol=954911.0 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-10-30: today_vol=954911 / avg20=1280949 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.498 on 2026-10-30 (price=70.4800, mid=67.7645, upper=73.2212, lower=62.3078; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=70.4800 vs SMA50=65.2764 dist=+7.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=67.7645 SMA50=65.2764 SMA80=65.1681 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-08-05@64.0100; S2[2026-09-02→2026-10-01] low=2026-09-18@54.7300; S3[2026-10-02→2026-10-30] low=2026-10-02@59.6200 | lows=[64.01000213623047, 54.72999954223633, 59.619998931884766] span=16.96% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.8566179358009177 wick_frac=0.14338206419908223 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.2575449627587393 wick_frac=0.7424550372412607 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.640631332999547 need>1.4; red_wick_gt_green=True 5d trail=2026-10-26:RED:body=-0.4700:wick=3.1150; 2026-10-27:RED:body=-0.3100:wick=0.9171; 2026-10-28:GREEN:body=+2.9100:wick=0.1750; 2026-10-29:GREEN:body=+2.3300:wick=0.3900; 2026-10-30:RED:body=-0.6400:wick=1.8450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=40.75 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1552.3 | **NEUTRAL** |
| `B04_income` | 39.72 | **GOOD** |
| `B05_profit_margin` | 2.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 82.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=82.77 vs prior_export=82.77 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | 2.61 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.61 vs prior=2.61 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | -13.43 | **BAD** |
| `B13_short_float` | 9.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=40.75 (this export) | prior_export=40.75 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAE  ·  score **+14**  ·  Medical Devices
price=119.6500015258789  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.95 on 2026-10-30; prev RSI=68.37 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.37@2026-10-29 → 67.95@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.37@2026-10-29 → 67.95@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.37@2026-10-29 → 67.95@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=1.106 (G=0.9400 R=0.8500); 2026-10-29:GREEN:O=118.8500,C=119.7900,body=+0.9400,vol=558500.0; 2026-10-30:RED:O=120.5000,C=119.6500,body=-0.8500,vol=453300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=1.232 (Gvol=558500 Rvol=453300); 2026-10-29:GREEN:O=118.8500,C=119.7900,body=+0.9400,vol=558500.0; 2026-10-30:RED:O=120.5000,C=119.6500,body=-0.8500,vol=453300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.644 on 2026-10-30: today_vol=453300 / avg20=703931 (avg window 2026-09-23→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.456 on 2026-10-30 (price=119.6500, mid=112.3765, upper=128.3188, lower=96.4342; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-30: price=119.6500 vs SMA50=107.0992 dist=+11.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-30: SMA20=112.3765 SMA50=107.0992 SMA80=96.7332 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-30 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-23@74.9500; S2[2026-08-24→2026-09-22] low=2026-09-11@100.5300; S3[2026-09-23→2026-10-30] low=2026-10-07@101.3300 | lows=[74.94999694824219, 100.52999877929688, 101.33000183105469] span=35.20% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.3657597482596369 wick_frac=0.634240251740363 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=0.4047614722562316 wick_frac=0.5952385277437684 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.105887210419079 need>1.4; red_wick_gt_green=False 5d trail=2026-10-15:RED:body=-0.9100:wick=2.9000; 2026-10-16:RED:body=-3.5500:wick=-0.0614; 2026-10-28:RED:body=-1.0800:wick=2.0000; 2026-10-29:GREEN:body=+0.9400:wick=1.6300; 2026-10-30:RED:body=-0.8500:wick=1.2500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1352.01 | **NEUTRAL** |
| `B04_income` | 96.29 | **GOOD** |
| `B05_profit_margin` | 7.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=111.5 vs prior_export=111.5 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.4 | **GOOD** |
| `B13_short_float` | 7.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.51 (this export) | prior_export=3.51 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HHH  ·  score **+14**  ·  Real Estate - Diversified
price=76.2300033569336  pair=`2026-10-29→2026-10-30`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.58 on 2026-10-30; prev RSI=64.34 on 2026-10-29 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.34@2026-10-29 → 65.58@2026-10-30 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.34@2026-10-29 → 65.58@2026-10-30 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.34@2026-10-29 → 65.58@2026-10-30 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.4400 R=0.0000); 2026-10-29:GREEN:O=74.0200,C=75.5800,body=+1.5600,vol=762083.0; 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-29 + 2026-10-30; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1655139 Rvol=0); 2026-10-29:GREEN:O=74.0200,C=75.5800,body=+1.5600,vol=762083.0; 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0 | **GOOD** |
| `A07_rvol` | RVOL=0.964 on 2026-10-30: today_vol=893056 / avg20=926494 (avg window 2026-10-02→2026-10-29, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.924 on 2026-10-30 (price=76.2300, mid=71.5185, upper=76.6149, lower=66.4221; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-30: price=76.2300 vs SMA50=67.9074 dist=+12.26% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=71.52_50=67.91_80=68.04 on 2026-10-30: SMA20=71.5185 SMA50=67.9074 SMA80=68.0365 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-04→2026-10-30 (63 bars); S1[2026-08-04→2026-09-01] low=2026-09-01@63.0200; S2[2026-09-02→2026-10-01] low=2026-09-18@60.0900; S3[2026-10-02→2026-10-30] low=2026-10-19@66.1400 | lows=[63.02000045776367, 60.09000015258789, 66.13999938964844] span=10.07% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: GREEN body_frac=0.6997822379916965 wick_frac=0.3002177620083035 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-29+2026-10-30: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-26:GREEN:body=+0.1400:wick=2.0800; 2026-10-27:GREEN:body=+0.2900:wick=1.6200; 2026-10-28:GREEN:body=+0.4400:wick=1.2550; 2026-10-29:GREEN:body=+1.5600:wick=0.5550; 2026-10-30:GREEN:body=+1.8800:wick=0.9600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=170.05 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=139.3 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2334.65 | **NEUTRAL** |
| `B04_income` | 292.1 | **GOOD** |
| `B05_profit_margin` | 12.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.67 vs prior_export=89.67 on finviz_2026-10-30) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 1.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.67 vs prior=1.67 on finviz_2026-10-30) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=170.05 (this export) | prior_export=170.05 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=139.3 (this export) | prior_export=139.3 (finviz_2026-10-30) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-02_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.