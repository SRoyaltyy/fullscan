# A+B1 Feature Checklist — 2026-11-09

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,618** names
- Export: `finviz_2026-11-09.csv` · prior export for Δ: `2026-11-06`
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
| 1 | EPAM | +17 | 19 | 2 | 2026-11-05→2026-11-06 | Information Technology Services |
| 2 | HUM | +17 | 18 | 1 | 2026-11-05→2026-11-06 | Healthcare Plans |
| 3 | PCTY | +17 | 18 | 1 | 2026-11-05→2026-11-06 | Software - Application |
| 4 | CLOV | +16 | 18 | 2 | 2026-11-05→2026-11-06 | Healthcare Plans |
| 5 | APLE | +16 | 17 | 1 | 2026-11-05→2026-11-06 | REIT - Hotel & Motel |
| 6 | G | +16 | 19 | 3 | 2026-11-05→2026-11-06 | Information Technology Services |
| 7 | KRMN | +15 | 17 | 2 | 2026-11-05→2026-11-06 | Aerospace & Defense |
| 8 | TSCO | +15 | 17 | 2 | 2026-11-05→2026-11-06 | Specialty Retail |
| 9 | GPN | +15 | 16 | 1 | 2026-11-05→2026-11-06 | Software - Infrastructure |
| 10 | FLOC | +15 | 17 | 2 | 2026-11-05→2026-11-06 | Oil & Gas Equipment & Services |
| 11 | CAE | +15 | 15 | 0 | 2026-11-05→2026-11-06 | Aerospace & Defense |
| 12 | EVRG | +14 | 16 | 2 | 2026-11-05→2026-11-06 | Utilities - Regulated Electric |
| 13 | WCN | +14 | 16 | 2 | 2026-11-05→2026-11-06 | Waste Management |
| 14 | IAG | +14 | 16 | 2 | 2026-11-05→2026-11-06 | Gold |
| 15 | VNT | +14 | 17 | 3 | 2026-11-05→2026-11-06 | Scientific & Technical Instruments |

## Full checklist — top 15

### EPAM  ·  score **+17**  ·  Information Technology Services
price=159.30999755859375  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.07 on 2026-11-06; prev RSI=78.39 on 2026-11-05 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 78.39@2026-11-05 → 71.07@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 78.39@2026-11-05 → 71.07@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 78.39@2026-11-05 → 71.07@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=12.180 (G=9.5000 R=0.7800); 2026-11-05:GREEN:O=156.2000,C=165.7000,body=+9.5000,vol=3671830.0; 2026-11-06:RED:O=160.0900,C=159.3100,body=-0.7800,vol=3339800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.099 (Gvol=3671830 Rvol=3339800); 2026-11-05:GREEN:O=156.2000,C=165.7000,body=+9.5000,vol=3671830.0; 2026-11-06:RED:O=160.0900,C=159.3100,body=-0.7800,vol=3339800.0 | **GOOD** |
| `A07_rvol` | RVOL=2.647 on 2026-11-06: today_vol=3339800 / avg20=1261579 (avg window 2026-10-09→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.951 on 2026-11-06 (price=159.3100, mid=134.0665, upper=160.6106, lower=107.5224; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=159.3100 vs SMA50=121.8510 dist=+30.74% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=134.0665 SMA50=121.8510 SMA80=113.7226 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-12@96.2400; S2[2026-09-10→2026-10-08] low=2026-09-28@104.3300; S3[2026-10-09→2026-11-06] low=2026-10-09@111.8843 | lows=[96.23999786376953, 104.33000183105469, 111.8843002319336] span=16.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.6569851738511053 wick_frac=0.3430148261488946 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.08724816177725588 wick_frac=0.9127518382227441 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.179506240463242 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+2.8000:wick=3.3849; 2026-11-03:RED:body=-0.4500:wick=7.1600; 2026-11-04:RED:body=-1.2600:wick=7.1700; 2026-11-05:GREEN:body=+9.5000:wick=4.9600; 2026-11-06:RED:body=-0.7800:wick=8.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.06 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.5 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5648.81 | **NEUTRAL** |
| `B04_income` | 425.17 | **GOOD** |
| `B05_profit_margin` | 7.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 167.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=26.25 (now=167.44 vs prior_export=141.19 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.18 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.22 | **GOOD** |
| `B13_short_float` | 12.95 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.06 (this export) | prior_export=3.06 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.5 (this export) | prior_export=0.5 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+17**  ·  Healthcare Plans
price=433.739990234375  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.06 on 2026-11-06; prev RSI=50.04 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.04@2026-11-05 → 61.06@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.04@2026-11-05 → 61.06@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.04@2026-11-05 → 61.06@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=3.299 (G=18.7400 R=5.6800); 2026-11-05:RED:O=417.4700,C=411.7900,body=-5.6800,vol=1359908.0; 2026-11-06:GREEN:O=415.0000,C=433.7400,body=+18.7400,vol=2576200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.894 (Gvol=2576200 Rvol=1359908); 2026-11-05:RED:O=417.4700,C=411.7900,body=-5.6800,vol=1359908.0; 2026-11-06:GREEN:O=415.0000,C=433.7400,body=+18.7400,vol=2576200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.724 on 2026-11-06: today_vol=2576200 / avg20=1494163 (avg window 2026-10-09→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.912 on 2026-11-06 (price=433.7400, mid=420.2345, upper=435.0445, lower=405.4245; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=433.7400 vs SMA50=404.0734 dist=+7.34% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=420.2345 SMA50=404.0734 SMA80=396.5836 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-12@370.3800; S2[2026-09-10→2026-10-08] low=2026-09-23@369.1300; S3[2026-10-09→2026-11-06] low=2026-11-02@399.3500 | lows=[370.3800048828125, 369.1300048828125, 399.3500061035156] span=8.19% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.5776815297170068 wick_frac=0.42231847028299313 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.30677815523018825 wick_frac=0.6932218447698117 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.299298309710835 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:RED:body=-10.2300:wick=11.0600; 2026-11-03:GREEN:body=+10.0000:wick=7.2000; 2026-11-04:GREEN:body=+3.0400:wick=19.0450; 2026-11-05:RED:body=-5.6800:wick=12.8350; 2026-11-06:GREEN:body=+18.7400:wick=13.7000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.06 (current export asof; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 468.52 | **NEUTRAL** |
| `B08_target_price_delta` | delta=8.060000000000002 (now=468.52 vs prior_export=460.46 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 3.06 | **NEUTRAL** |
| `B14_earnings_date` | 11/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.06 (this export) | prior_export=4.7 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.71 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PCTY  ·  score **+17**  ·  Software - Application
price=173.14999389648438  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.38 on 2026-11-06; prev RSI=60.21 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.21@2026-11-05 → 64.38@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.21@2026-11-05 → 64.38@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.21@2026-11-05 → 64.38@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=9.170 (G=10.8200 R=1.1800); 2026-11-05:RED:O=170.7500,C=169.5700,body=-1.1800,vol=948100.0; 2026-11-06:GREEN:O=162.3300,C=173.1500,body=+10.8200,vol=1225900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.293 (Gvol=1225900 Rvol=948100); 2026-11-05:RED:O=170.7500,C=169.5700,body=-1.1800,vol=948100.0; 2026-11-06:GREEN:O=162.3300,C=173.1500,body=+10.8200,vol=1225900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.946 on 2026-11-06: today_vol=1225900 / avg20=630060 (avg window 2026-10-06→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.336 on 2026-11-06 (price=173.1500, mid=167.5500, upper=184.2164, lower=150.8836; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=173.1500 vs SMA50=155.6860 dist=+11.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=167.5500 SMA50=155.6860 SMA80=149.2110 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-12@140.0200; S2[2026-09-04→2026-10-05] low=2026-09-29@139.5050; S3[2026-10-06→2026-11-06] low=2026-10-06@145.3150 | lows=[140.02000427246094, 139.5050048828125, 145.31500244140625] span=4.16% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.5181987325269367 wick_frac=0.4818012674730633 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.1581758195455503 wick_frac=0.8418241804544497 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.169541716236488 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:GREEN:body=+5.3500:wick=2.0600; 2026-11-03:GREEN:body=+0.0900:wick=6.3600; 2026-11-04:RED:body=-1.5100:wick=3.2250; 2026-11-05:RED:body=-1.1800:wick=6.2800; 2026-11-06:GREEN:body=+10.8200:wick=10.0600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.27 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.93 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1814.82 | **NEUTRAL** |
| `B04_income` | 279.39 | **GOOD** |
| `B05_profit_margin` | 15.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 196.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=13.210000000000008 (now=196.05 vs prior_export=182.84 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 1.52 | **GOOD** |
| `B10_insider_transactions` | -2.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.47 vs prior=-2.47 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.02 | **GOOD** |
| `B13_short_float` | 7.27 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.27 (this export) | prior_export=13.78 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.93 (this export) | prior_export=3.08 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CLOV  ·  score **+16**  ·  Healthcare Plans
price=4.820000171661377  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.24 on 2026-11-06; prev RSI=47.73 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.73@2026-11-05 → 56.24@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.73@2026-11-05 → 56.24@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.73@2026-11-05 → 56.24@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=0.4000 R=0.1500); 2026-11-05:RED:O=4.6300,C=4.4800,body=-0.1500,vol=8230514.0; 2026-11-06:GREEN:O=4.4200,C=4.8200,body=+0.4000,vol=7552000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.918 (Gvol=7552000 Rvol=8230514); 2026-11-05:RED:O=4.6300,C=4.4800,body=-0.1500,vol=8230514.0; 2026-11-06:GREEN:O=4.4200,C=4.8200,body=+0.4000,vol=7552000.0 | **BAD** |
| `A07_rvol` | RVOL=1.717 on 2026-11-06: today_vol=7552000 / avg20=4398599 (avg window 2026-10-09→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.575 on 2026-11-06 (price=4.8200, mid=4.6250, upper=4.9639, lower=4.2861; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=4.8200 vs SMA50=4.5407 dist=+6.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=4.6250 SMA50=4.5407 SMA80=4.4784 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-25@4.0350; S2[2026-09-10→2026-10-08] low=2026-10-01@4.1100; S3[2026-10-09→2026-11-06] low=2026-11-03@4.1900 | lows=[4.034999847412109, 4.110000133514404, 4.190000057220459] span=3.84% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.888889477576988 wick_frac=0.11111052242301196 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.20689671048088212 wick_frac=0.7931032895191179 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.666665607029211 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:RED:body=-0.2200:wick=0.0398; 2026-11-03:RED:body=-0.0200:wick=0.1600; 2026-11-04:GREEN:body=+0.2300:wick=0.1450; 2026-11-05:RED:body=-0.1500:wick=0.5750; 2026-11-06:GREEN:body=+0.4000:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=296.08 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.6 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2728.85 | **NEUTRAL** |
| `B04_income` | 12.45 | **GOOD** |
| `B05_profit_margin` | 0.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.6 vs prior_export=5.6 on finviz_2026-11-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -1.5 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.5 vs prior=-1.5 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.28 | **GOOD** |
| `B13_short_float` | 6.32 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=296.08 (this export) | prior_export=613.73 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.6 (this export) | prior_export=2.6 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### APLE  ·  score **+16**  ·  REIT - Hotel & Motel
price=18.8799991607666  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.43 on 2026-11-06; prev RSI=59.26 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.26@2026-11-05 → 67.43@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.26@2026-11-05 → 67.43@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.26@2026-11-05 → 67.43@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.633 (G=0.8000 R=0.4900); 2026-11-05:RED:O=18.6400,C=18.1500,body=-0.4900,vol=3740100.0; 2026-11-06:GREEN:O=18.0800,C=18.8800,body=+0.8000,vol=5795500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.550 (Gvol=5795500 Rvol=3740100); 2026-11-05:RED:O=18.6400,C=18.1500,body=-0.4900,vol=3740100.0; 2026-11-06:GREEN:O=18.0800,C=18.8800,body=+0.8000,vol=5795500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.289 on 2026-11-06: today_vol=5795500 / avg20=2532145 (avg window 2026-10-06→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.518 on 2026-11-06 (price=18.8800, mid=17.8615, upper=19.8277, lower=15.8953; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=18.8800 vs SMA50=16.7070 dist=+13.01% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=17.8615 SMA50=16.7070 SMA80=16.5864 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-01@15.5000; S2[2026-09-04→2026-10-05] low=2026-09-09@15.1500; S3[2026-10-06→2026-11-06] low=2026-10-08@15.9500 | lows=[15.5, 15.149999618530273, 15.949999809265137] span=5.28% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.7441848086442753 wick_frac=0.2558151913557247 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.6049383878890346 wick_frac=0.39506161211096535 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6326522668265206 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:RED:body=-0.0900:wick=0.3650; 2026-11-03:RED:body=-0.0600:wick=0.2100; 2026-11-04:RED:body=-0.0500:wick=0.3240; 2026-11-05:RED:body=-0.4900:wick=0.3200; 2026-11-06:GREEN:body=+0.8000:wick=0.2750 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=20.59 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.56 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1465.06 | **NEUTRAL** |
| `B04_income` | 184.86 | **GOOD** |
| `B05_profit_margin` | 12.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.16000000000000014 (now=17.94 vs prior_export=17.78 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.36 | **GOOD** |
| `B10_insider_transactions` | 0.03 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.03 vs prior=0.03 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.0 | **NEUTRAL** |
| `B13_short_float` | 6.76 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.59 (this export) | prior_export=4.95 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.56 (this export) | prior_export=2.72 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### G  ·  score **+16**  ·  Information Technology Services
price=42.27000045776367  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.93 on 2026-11-06; prev RSI=67.12 on 2026-11-05 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 67.12@2026-11-05 → 70.93@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.12@2026-11-05 → 70.93@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 67.12@2026-11-05 → 70.93@2026-11-06 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=13.286 (G=2.7900 R=0.2100); 2026-11-05:RED:O=41.3100,C=41.1000,body=-0.2100,vol=1950000.0; 2026-11-06:GREEN:O=39.4800,C=42.2700,body=+2.7900,vol=3931800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=2.016 (Gvol=3931800 Rvol=1950000); 2026-11-05:RED:O=41.3100,C=41.1000,body=-0.2100,vol=1950000.0; 2026-11-06:GREEN:O=39.4800,C=42.2700,body=+2.7900,vol=3931800.0 | **GOOD** |
| `A07_rvol` | RVOL=2.108 on 2026-11-06: today_vol=3931800 / avg20=1864990 (avg window 2026-10-06→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.678 on 2026-11-06 (price=42.2700, mid=38.7325, upper=43.9535, lower=33.5115; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=42.2700 vs SMA50=36.5986 dist=+15.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=38.7325 SMA50=36.5986 SMA80=35.4493 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-12@33.0000; S2[2026-09-04→2026-10-05] low=2026-09-29@32.0350; S3[2026-10-06→2026-11-06] low=2026-10-07@33.0700 | lows=[33.0, 32.03499984741211, 33.06999969482422] span=3.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.7859159197559453 wick_frac=0.2140840802440547 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.1521763167642457 wick_frac=0.8478236832357543 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.285535230967648 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+0.9400:wick=0.5100; 2026-11-03:GREEN:body=+0.3800:wick=1.2200; 2026-11-04:RED:body=-0.0400:wick=1.1100; 2026-11-05:RED:body=-0.2100:wick=1.1700; 2026-11-06:GREEN:body=+2.7900:wick=0.7600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.53 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5359.94 | **NEUTRAL** |
| `B04_income` | 588.48 | **GOOD** |
| `B05_profit_margin` | 10.98 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.82 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.9100000000000037 (now=45.82 vs prior_export=42.91 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | -0.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.43 vs prior=-0.43 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.7 | **GOOD** |
| `B13_short_float` | 7.29 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.53 (this export) | prior_export=3.67 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.84 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KRMN  ·  score **+15**  ·  Aerospace & Defense
price=45.61000061035156  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.19 on 2026-11-06; prev RSI=39.50 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.50@2026-11-05 → 64.19@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 39.50@2026-11-05 → 64.19@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 39.50@2026-11-05 → 64.19@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=10.554 (G=9.7100 R=0.9200); 2026-11-05:RED:O=33.0100,C=32.0900,body=-0.9200,vol=5158795.0; 2026-11-06:GREEN:O=35.9000,C=45.6100,body=+9.7100,vol=10165200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.970 (Gvol=10165200 Rvol=5158795); 2026-11-05:RED:O=33.0100,C=32.0900,body=-0.9200,vol=5158795.0; 2026-11-06:GREEN:O=35.9000,C=45.6100,body=+9.7100,vol=10165200.0 | **GOOD** |
| `A07_rvol` | RVOL=3.081 on 2026-11-06: today_vol=10165200 / avg20=3298800 (avg window 2026-10-09→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.226 on 2026-11-06 (price=45.6100, mid=36.0510, upper=43.8492, lower=28.2528; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=45.6100 vs SMA50=35.8824 dist=+27.11% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.05_50=35.88_80=42.28 on 2026-11-06: SMA20=36.0510 SMA50=35.8824 SMA80=42.2786 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-09-09@35.1800; S2[2026-09-10→2026-10-08] low=2026-10-08@30.8700; S3[2026-10-09→2026-11-06] low=2026-10-15@29.3400 | lows=[35.18000030517578, 30.8700008392334, 29.34000015258789] span=19.90% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.828498129106007 wick_frac=0.1715018708939931 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.2958764486532808 wick_frac=0.7041235513467192 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.554367837062346 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:RED:body=-1.2000:wick=1.0200; 2026-11-03:GREEN:body=+0.2100:wick=1.7450; 2026-11-04:RED:body=-4.8200:wick=2.1500; 2026-11-05:RED:body=-0.9200:wick=2.1894; 2026-11-06:GREEN:body=+9.7100:wick=2.0100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=26.16 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.51 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 669.39 | **NEUTRAL** |
| `B04_income` | 50.45 | **GOOD** |
| `B05_profit_margin` | 7.54 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 67.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-5.240000000000009 (now=67.38 vs prior_export=72.62 on finviz_2026-11-06) | **BAD** |
| `B09_analyst_recom` | 1.14 | **GOOD** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 23.65 | **GOOD** |
| `B13_short_float` | 12.8 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.16 (this export) | prior_export=6.79 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.51 (this export) | prior_export=1.11 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TSCO  ·  score **+15**  ·  Specialty Retail
price=36.349998474121094  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.03 on 2026-11-06; prev RSI=65.88 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.88@2026-11-05 → 64.03@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.88@2026-11-05 → 64.03@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.88@2026-11-05 → 64.03@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=2.156 (G=0.9700 R=0.4500); 2026-11-05:RED:O=37.0600,C=36.6100,body=-0.4500,vol=10722975.0; 2026-11-06:GREEN:O=35.3800,C=36.3500,body=+0.9700,vol=8330800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.777 (Gvol=8330800 Rvol=10722975); 2026-11-05:RED:O=37.0600,C=36.6100,body=-0.4500,vol=10722975.0; 2026-11-06:GREEN:O=35.3800,C=36.3500,body=+0.9700,vol=8330800.0 | **BAD** |
| `A07_rvol` | RVOL=0.554 on 2026-11-06: today_vol=8330800 / avg20=15029033 (avg window 2026-10-09→2026-11-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.855 on 2026-11-06 (price=36.3500, mid=33.8448, upper=36.7758, lower=30.9137; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=36.3500 vs SMA50=33.3687 dist=+8.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=33.8448 SMA50=33.3687 SMA80=33.3463 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-09-09@33.3200; S2[2026-09-10→2026-10-08] low=2026-10-05@30.5250; S3[2026-10-09→2026-11-06] low=2026-10-22@29.9000 | lows=[33.31999969482422, 30.524999618530273, 29.899999618530273] span=11.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.642383102135475 wick_frac=0.35761689786452505 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.22842664167428314 wick_frac=0.7715733583257168 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1555461365659307 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+1.0300:wick=0.6750; 2026-11-03:RED:body=-0.2300:wick=0.7300; 2026-11-04:GREEN:body=+0.3300:wick=0.9351; 2026-11-05:RED:body=-0.4500:wick=1.5200; 2026-11-06:GREEN:body=+0.9700:wick=0.5400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.31 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.31 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 15942.14 | **NEUTRAL** |
| `B04_income` | 977.32 | **GOOD** |
| `B05_profit_margin` | 6.13 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.15 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.15 vs prior_export=37.15 on finviz_2026-11-06) | **NEUTRAL** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | 1.16 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.16 vs prior=1.16 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.31 | **GOOD** |
| `B13_short_float` | 8.94 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.31 (this export) | prior_export=5.31 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.31 (this export) | prior_export=2.31 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPN  ·  score **+15**  ·  Software - Infrastructure
price=97.4000015258789  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.17 on 2026-11-06; prev RSI=63.09 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.09@2026-11-05 → 66.17@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.09@2026-11-05 → 66.17@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.09@2026-11-05 → 66.17@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.719 (G=3.3700 R=1.9600); 2026-11-05:RED:O=96.9400,C=94.9800,body=-1.9600,vol=3710200.0; 2026-11-06:GREEN:O=94.0300,C=97.4000,body=+3.3700,vol=3648100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.983 (Gvol=3648100 Rvol=3710200); 2026-11-05:RED:O=96.9400,C=94.9800,body=-1.9600,vol=3710200.0; 2026-11-06:GREEN:O=94.0300,C=97.4000,body=+3.3700,vol=3648100.0 | **BAD** |
| `A07_rvol` | RVOL=1.144 on 2026-11-06: today_vol=3648100 / avg20=3187524 (avg window 2026-10-06→2026-11-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.743 on 2026-11-06 (price=97.4000, mid=87.4460, upper=100.8358, lower=74.0562; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=97.4000 vs SMA50=87.3220 dist=+11.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=87.4460 SMA50=87.3220 SMA80=86.6282 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-12@84.2000; S2[2026-09-04→2026-10-05] low=2026-10-02@77.9300; S3[2026-10-06→2026-11-06] low=2026-10-08@80.3200 | lows=[84.19999694824219, 77.93000030517578, 80.31999969482422] span=8.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.6419052850632441 wick_frac=0.35809471493675593 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.41176432874982166 wick_frac=0.5882356712501783 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.719389959556405 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:GREEN:body=+2.2000:wick=2.1500; 2026-11-03:RED:body=-1.3400:wick=1.3900; 2026-11-04:RED:body=-2.0300:wick=1.7600; 2026-11-05:RED:body=-1.9600:wick=2.8000; 2026-11-06:GREEN:body=+3.3700:wick=1.8800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.09 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.01 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11493.52 | **NEUTRAL** |
| `B04_income` | 197.89 | **GOOD** |
| `B05_profit_margin` | 1.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 107.34 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.5 (now=107.34 vs prior_export=104.84 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.09 | **GOOD** |
| `B13_short_float` | 6.62 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.09 (this export) | prior_export=0.09 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.01 (this export) | prior_export=0.01 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLOC  ·  score **+15**  ·  Oil & Gas Equipment & Services
price=20.059999465942383  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.25 on 2026-11-06; prev RSI=44.62 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.62@2026-11-05 → 55.25@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.62@2026-11-05 → 55.25@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.62@2026-11-05 → 55.25@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.914 (G=0.6700 R=0.3500); 2026-11-05:RED:O=19.4000,C=19.0500,body=-0.3500,vol=1251400.0; 2026-11-06:GREEN:O=19.3900,C=20.0600,body=+0.6700,vol=725200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.580 (Gvol=725200 Rvol=1251400); 2026-11-05:RED:O=19.4000,C=19.0500,body=-0.3500,vol=1251400.0; 2026-11-06:GREEN:O=19.3900,C=20.0600,body=+0.6700,vol=725200.0 | **BAD** |
| `A07_rvol` | RVOL=1.163 on 2026-11-06: today_vol=725200 / avg20=623377 (avg window 2026-10-06→2026-11-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.691 on 2026-11-06 (price=20.0600, mid=19.4360, upper=20.3394, lower=18.5326; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=20.0600 vs SMA50=19.8210 dist=+1.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-06: SMA20=19.4360 SMA50=19.8210 SMA80=20.3273 | **BAD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-26@19.9000; S2[2026-09-04→2026-10-05] low=2026-09-30@17.8300; S3[2026-10-06→2026-11-06] low=2026-10-06@18.1600 | lows=[19.899999618530273, 17.829999923706055, 18.15999984741211] span=11.61% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.5403227419064568 wick_frac=0.45967725809354315 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.33333393884127577 wick_frac=0.6666660611587242 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9142838458646 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:RED:body=-1.9200:wick=0.0750; 2026-11-03:DOJI:body=+0.0000:wick=0.9000; 2026-11-04:RED:body=-2.0400:wick=0.3250; 2026-11-05:RED:body=-0.3500:wick=0.7000; 2026-11-06:GREEN:body=+0.6700:wick=0.5700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.45 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.03 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 901.22 | **NEUTRAL** |
| `B04_income` | 57.32 | **GOOD** |
| `B05_profit_margin` | 6.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 30.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=30.11 vs prior_export=30.11 on finviz_2026-11-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | 0.12 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.12 (now=0.12 vs prior=0.0 on finviz_2026-11-06) | **GOOD** |
| `B12_institutional_transactions` | 8.71 | **GOOD** |
| `B13_short_float` | 9.85 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.45 (this export) | prior_export=30.45 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.03 (this export) | prior_export=6.03 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CAE  ·  score **+15**  ·  Aerospace & Defense
price=26.850000381469727  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.88 on 2026-11-06; prev RSI=65.45 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.45@2026-11-05 → 65.88@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.45@2026-11-05 → 65.88@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.45@2026-11-05 → 65.88@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.094 (G=0.3500 R=0.3200); 2026-11-05:RED:O=27.1100,C=26.7900,body=-0.3200,vol=625300.0; 2026-11-06:GREEN:O=26.5000,C=26.8500,body=+0.3500,vol=632000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.011 (Gvol=632000 Rvol=625300); 2026-11-05:RED:O=27.1100,C=26.7900,body=-0.3200,vol=625300.0; 2026-11-06:GREEN:O=26.5000,C=26.8500,body=+0.3500,vol=632000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.249 on 2026-11-06: today_vol=632000 / avg20=506140 (avg window 2026-10-06→2026-11-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.627 on 2026-11-06 (price=26.8500, mid=25.2405, upper=27.8055, lower=22.6755; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=26.8500 vs SMA50=24.6305 dist=+9.01% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=25.24_50=24.63_80=25.09 on 2026-11-06: SMA20=25.2405 SMA50=24.6305 SMA80=25.0941 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-01@23.2600; S2[2026-09-04→2026-10-05] low=2026-09-18@23.5300; S3[2026-10-06→2026-11-06] low=2026-10-08@22.9700 | lows=[23.260000228881836, 23.530000686645508, 22.969999313354492] span=2.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.5468761175881554 wick_frac=0.45312388241184465 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.20915003347208352 wick_frac=0.7908499665279165 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0937522351763107 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+0.4900:wick=0.5500; 2026-11-03:GREEN:body=+0.4600:wick=0.3100; 2026-11-04:GREEN:body=+0.3400:wick=0.3700; 2026-11-05:RED:body=-0.3200:wick=1.2100; 2026-11-06:GREEN:body=+0.3500:wick=0.2900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.89 (current export asof; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.52 (current export; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3610.72 | **NEUTRAL** |
| `B04_income` | 207.45 | **GOOD** |
| `B05_profit_margin` | 5.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 32.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=32.16 vs prior_export=32.16 on finviz_2026-11-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.44 | **GOOD** |
| `B13_short_float` | 1.93 | **NEUTRAL** |
| `B14_earnings_date` | 11/11/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.89 (this export) | prior_export=12.89 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVRG  ·  score **+14**  ·  Utilities - Regulated Electric
price=87.54000091552734  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.27 on 2026-11-06; prev RSI=68.88 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.88@2026-11-05 → 69.27@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.88@2026-11-05 → 69.27@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.88@2026-11-05 → 69.27@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.099 (G=1.2200 R=1.1100); 2026-11-05:RED:O=88.5200,C=87.4100,body=-1.1100,vol=6527600.0; 2026-11-06:GREEN:O=86.3200,C=87.5400,body=+1.2200,vol=3813700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.584 (Gvol=3813700 Rvol=6527600); 2026-11-05:RED:O=88.5200,C=87.4100,body=-1.1100,vol=6527600.0; 2026-11-06:GREEN:O=86.3200,C=87.5400,body=+1.2200,vol=3813700.0 | **BAD** |
| `A07_rvol` | RVOL=1.607 on 2026-11-06: today_vol=3813700 / avg20=2373681 (avg window 2026-10-06→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.764 on 2026-11-06 (price=87.5400, mid=84.1160, upper=88.5986, lower=79.6334; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=87.5400 vs SMA50=81.7690 dist=+7.06% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=84.12_50=81.77_80=82.69 on 2026-11-06: SMA20=84.1160 SMA50=81.7690 SMA80=82.6856 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-31@79.8500; S2[2026-09-04→2026-10-05] low=2026-09-25@77.1100; S3[2026-10-06→2026-11-06] low=2026-10-06@79.2300 | lows=[79.8499984741211, 77.11000061035156, 79.2300033569336] span=3.55% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.6387428699250643 wick_frac=0.3612571300749357 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.3749967781346173 wick_frac=0.6250032218653827 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.099107148994082 need>1.4; red_wick_gt_green=False 5d trail=2026-11-02:GREEN:body=+0.8200:wick=0.4400; 2026-11-03:RED:body=-0.6700:wick=0.2600; 2026-11-04:GREEN:body=+1.1600:wick=2.0400; 2026-11-05:RED:body=-1.1100:wick=1.8500; 2026-11-06:GREEN:body=+1.2200:wick=0.6900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.69 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.68 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6330.8 | **NEUTRAL** |
| `B04_income` | 1063.7 | **GOOD** |
| `B05_profit_margin` | 16.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 91.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.46000000000000796 (now=91.92 vs prior_export=91.46 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -0.35 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.35 vs prior=-0.35 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.54 | **GOOD** |
| `B13_short_float` | 8.13 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.69 (this export) | prior_export=10.69 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.68 (this export) | prior_export=8.68 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WCN  ·  score **+14**  ·  Waste Management
price=189.07000732421875  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.72 on 2026-11-06; prev RSI=73.65 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 73.65@2026-11-05 → 68.72@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 73.65@2026-11-05 → 68.72@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 73.65@2026-11-05 → 68.72@2026-11-06 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=2.569 (G=4.4700 R=1.7400); 2026-11-05:RED:O=193.4000,C=191.6600,body=-1.7400,vol=2575500.0; 2026-11-06:GREEN:O=184.6000,C=189.0700,body=+4.4700,vol=1761800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=0.684 (Gvol=1761800 Rvol=2575500); 2026-11-05:RED:O=193.4000,C=191.6600,body=-1.7400,vol=2575500.0; 2026-11-06:GREEN:O=184.6000,C=189.0700,body=+4.4700,vol=1761800.0 | **BAD** |
| `A07_rvol` | RVOL=0.797 on 2026-11-06: today_vol=1761800 / avg20=2209905 (avg window 2026-10-06→2026-11-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.474 on 2026-11-06 (price=189.0700, mid=176.0545, upper=203.5104, lower=148.5986; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-06: price=189.0700 vs SMA50=165.9810 dist=+13.91% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=176.05_50=165.98_80=166.82 on 2026-11-06: SMA20=176.0545 SMA50=165.9810 SMA80=166.8193 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@162.2800; S2[2026-09-04→2026-10-05] low=2026-10-01@149.3900; S3[2026-10-06→2026-11-06] low=2026-10-09@152.3300 | lows=[162.27999877929688, 149.38999938964844, 152.3300018310547] span=8.63% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.941052888569079 wick_frac=0.05894711143092105 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.31868002884083324 wick_frac=0.6813199711591668 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5689806370141715 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+4.9000:wick=0.0100; 2026-11-03:GREEN:body=+0.0700:wick=3.0400; 2026-11-04:GREEN:body=+3.0000:wick=3.4000; 2026-11-05:RED:body=-1.7400:wick=3.7200; 2026-11-06:GREEN:body=+4.4700:wick=0.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.93 (current export asof; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9943.62 | **NEUTRAL** |
| `B04_income` | 1133.47 | **GOOD** |
| `B05_profit_margin` | 11.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 208.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=208.0 vs prior_export=208.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 0.68 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.68 vs prior=0.68 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.12 | **GOOD** |
| `B13_short_float` | 2.11 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.93 (this export) | prior_export=3.93 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IAG  ·  score **+14**  ·  Gold
price=18.760000228881836  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.19 on 2026-11-06; prev RSI=50.29 on 2026-11-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.29@2026-11-05 → 58.19@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.29@2026-11-05 → 58.19@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.29@2026-11-05 → 58.19@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=3.629 (G=1.2700 R=0.3500); 2026-11-05:RED:O=17.8500,C=17.5000,body=-0.3500,vol=8936614.0; 2026-11-06:GREEN:O=17.4900,C=18.7600,body=+1.2700,vol=9513300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.065 (Gvol=9513300 Rvol=8936614); 2026-11-05:RED:O=17.8500,C=17.5000,body=-0.3500,vol=8936614.0; 2026-11-06:GREEN:O=17.4900,C=18.7600,body=+1.2700,vol=9513300.0 | **GOOD** |
| `A07_rvol` | RVOL=2.093 on 2026-11-06: today_vol=9513300 / avg20=4546171 (avg window 2026-10-09→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.829 on 2026-11-06 (price=18.7600, mid=17.2125, upper=19.0800, lower=15.3450; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=18.7600 vs SMA50=18.6280 dist=+0.71% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=17.21_50=18.63_80=18.09 on 2026-11-06: SMA20=17.2125 SMA50=18.6280 SMA80=18.0851 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-06 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-13@17.4700; S2[2026-09-10→2026-10-08] low=2026-10-07@17.6400; S3[2026-10-09→2026-11-06] low=2026-10-30@15.1250 | lows=[17.469999313354492, 17.639999389648438, 15.125] span=16.63% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.9007084235715532 wick_frac=0.09929157642844678 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.2083335225543935 wick_frac=0.7916664774456065 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.628568781641517 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+0.2500:wick=0.2250; 2026-11-03:RED:body=-0.2700:wick=0.1800; 2026-11-04:GREEN:body=+0.6300:wick=0.3600; 2026-11-05:RED:body=-0.3500:wick=1.3300; 2026-11-06:GREEN:body=+1.2700:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.14 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.26 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3681.8 | **NEUTRAL** |
| `B04_income` | 1156.2 | **GOOD** |
| `B05_profit_margin` | 31.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.03999999999999915 (now=23.61 vs prior_export=23.65 on finviz_2026-11-06) | **BAD** |
| `B09_analyst_recom` | 1.31 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.92 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.14 (this export) | prior_export=-3.96 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.26 (this export) | prior_export=-5.11 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+14**  ·  Scientific & Technical Instruments
price=42.22999954223633  pair=`2026-11-05→2026-11-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=86.83 on 2026-11-06; prev RSI=83.02 on 2026-11-05 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 83.02@2026-11-05 → 86.83@2026-11-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 83.02@2026-11-05 → 86.83@2026-11-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 83.02@2026-11-05 → 86.83@2026-11-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_body_sum/RED_body_sum=1.223 (G=2.5800 R=2.1100); 2026-11-05:RED:O=42.0100,C=39.9000,body=-2.1100,vol=3341800.0; 2026-11-06:GREEN:O=39.6500,C=42.2300,body=+2.5800,vol=4229800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-05 + 2026-11-06; ratio=GREEN_vol/RED_vol=1.266 (Gvol=4229800 Rvol=3341800); 2026-11-05:RED:O=42.0100,C=39.9000,body=-2.1100,vol=3341800.0; 2026-11-06:GREEN:O=39.6500,C=42.2300,body=+2.5800,vol=4229800.0 | **GOOD** |
| `A07_rvol` | RVOL=3.113 on 2026-11-06: today_vol=4229800 / avg20=1358584 (avg window 2026-10-06→2026-11-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.365 on 2026-11-06 (price=42.2300, mid=35.0510, upper=40.3115, lower=29.7905; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-06: price=42.2300 vs SMA50=33.2448 dist=+27.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-06: SMA20=35.0510 SMA50=33.2448 SMA80=32.8026 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-06 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-01@31.6000; S2[2026-09-04→2026-10-05] low=2026-09-24@30.4250; S3[2026-10-06→2026-11-06] low=2026-10-07@31.6950 | lows=[31.600000381469727, 30.424999237060547, 31.69499969482422] span=4.17% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: GREEN body_frac=0.8403892726362262 wick_frac=0.15961072736377382 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-05+2026-11-06: RED body_frac=0.594365422287078 wick_frac=0.40563457771292194 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.222749731976432 need>1.4; red_wick_gt_green=True 5d trail=2026-11-02:GREEN:body=+0.7400:wick=0.5200; 2026-11-03:RED:body=-0.2400:wick=0.4200; 2026-11-04:GREEN:body=+0.4500:wick=0.4700; 2026-11-05:RED:body=-2.1100:wick=1.4400; 2026-11-06:GREEN:body=+2.5800:wick=0.4900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 43.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.4399999999999977 (now=43.44 vs prior_export=40.0 on finviz_2026-11-06) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.0 | **GOOD** |
| `B13_short_float` | 9.17 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-09_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.