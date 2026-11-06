# A+B1 Feature Checklist — 2026-11-06

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,610** names
- Export: `finviz_2026-11-06.csv` · prior export for Δ: `2026-11-05`
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
| 1 | IFF | +18 | 18 | 0 | 2026-10-09→2026-10-12 | Specialty Chemicals |
| 2 | SWBI | +17 | 17 | 0 | 2026-11-04→2026-11-05 | Aerospace & Defense |
| 3 | MNTK | +17 | 19 | 2 | 2026-09-04→2026-09-08 | Specialty Chemicals |
| 4 | EPAM | +16 | 19 | 3 | 2026-11-04→2026-11-05 | Information Technology Services |
| 5 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 6 | TEVA | +16 | 18 | 2 | 2026-11-04→2026-11-05 | Drug Manufacturers - Specialty & Gen |
| 7 | ADSK | +16 | 17 | 1 | 2026-11-04→2026-11-05 | Software - Application |
| 8 | PLX | +15 | 15 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 9 | CTSH | +15 | 16 | 1 | 2026-11-04→2026-11-05 | Information Technology Services |
| 10 | PODD | +15 | 17 | 2 | 2026-10-09→2026-10-12 | Medical Devices |
| 11 | SEZL | +15 | 18 | 3 | 2026-10-14→2026-10-15 | Credit Services |
| 12 | UPBD | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Software - Application |
| 13 | MICC | +15 | 16 | 1 | 2026-11-04→2026-11-05 | Packaged Foods |
| 14 | CNK | +15 | 16 | 1 | 2026-11-03→2026-11-04 | Entertainment |
| 15 | CNC | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Healthcare Plans |

## Full checklist — top 15

### IFF  ·  score **+18**  ·  Specialty Chemicals
price=84.95999908447266  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.76 on 2026-10-12; prev RSI=51.57 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=3.571 (G=0.7500 R=0.2100); 2026-10-09:RED:O=84.7600,C=84.5500,body=-0.2100,vol=1111114.0; 2026-10-12:GREEN:O=84.2100,C=84.9600,body=+0.7500,vol=1289930.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.161 (Gvol=1289930 Rvol=1111114); 2026-10-09:RED:O=84.7600,C=84.5500,body=-0.2100,vol=1111114.0; 2026-10-12:GREEN:O=84.2100,C=84.9600,body=+0.7500,vol=1289930.0 | **GOOD** |
| `A07_rvol` | RVOL=0.711 on 2026-10-12: today_vol=1289930 / avg20=1813615 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.251 on 2026-10-12 (price=84.9600, mid=84.4295, upper=86.5461, lower=82.3129; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=84.9600 vs SMA50=84.6470 dist=+0.37% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=84.43_50=84.65_80=82.01 on 2026-10-12: SMA20=84.4295 SMA50=84.6470 SMA80=82.0119 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-23@72.1100; S2[2026-08-13→2026-09-11] low=2026-08-18@80.8700; S3[2026-09-14→2026-10-12] low=2026-10-01@81.5400 | lows=[72.11000061035156, 80.87000274658203, 81.54000091552734] span=13.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.7614975250400874 wick_frac=0.2385024749599126 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.24561419163707102 wick_frac=0.754385808362929 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.571444141689373 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.0900:wick=0.9750; 2026-10-07:GREEN:body=+0.6700:wick=0.4730; 2026-10-08:GREEN:body=+0.9500:wick=1.1000; 2026-10-09:RED:body=-0.2100:wick=0.6450; 2026-10-12:GREEN:body=+0.7500:wick=0.2349 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.13 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.76 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9226.0 | **NEUTRAL** |
| `B04_income` | 182.0 | **GOOD** |
| `B05_profit_margin` | 1.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 98.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.6299999999999955 (now=98.88 vs prior_export=98.25 on finviz_2026-11-05) | **GOOD** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | 5.85 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.85 vs prior=5.85 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.8 | **GOOD** |
| `B13_short_float` | 6.58 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.13 (this export) | prior_export=2.13 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.76 (this export) | prior_export=1.76 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWBI  ·  score **+17**  ·  Aerospace & Defense
price=14.34000015258789  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.87 on 2026-11-05; prev RSI=57.07 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.07@2026-11-04 → 54.87@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.07@2026-11-04 → 54.87@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.07@2026-11-04 → 54.87@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=6.600 (G=0.9900 R=0.1500); 2026-11-04:GREEN:O=13.5000,C=14.4900,body=+0.9900,vol=1220063.0; 2026-11-05:RED:O=14.4900,C=14.3400,body=-0.1500,vol=1182456.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.032 (Gvol=1220063 Rvol=1182456); 2026-11-04:GREEN:O=13.5000,C=14.4900,body=+0.9900,vol=1220063.0; 2026-11-05:RED:O=14.4900,C=14.3400,body=-0.1500,vol=1182456.0 | **GOOD** |
| `A07_rvol` | RVOL=2.194 on 2026-11-05: today_vol=1182456 / avg20=538947 (avg window 2026-10-08→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.262 on 2026-11-05 (price=14.3400, mid=14.0665, upper=15.1099, lower=13.0231; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=14.3400 vs SMA50=13.7888 dist=+4.00% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=14.07_50=13.79_80=14.05 on 2026-11-05: SMA20=14.0665 SMA50=13.7888 SMA80=14.0493 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-09-03@12.0900; S2[2026-09-09→2026-10-07] low=2026-09-17@12.9000; S3[2026-10-08→2026-11-05] low=2026-11-02@13.1200 | lows=[12.09000015258789, 12.899999618530273, 13.119999885559082] span=8.52% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.8319328741269669 wick_frac=0.16806712587303305 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.2002130874384064 wick_frac=0.7997869125615936 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.6000152588278675 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:RED:body=-0.3800:wick=0.1600; 2026-11-02:RED:body=-0.0800:wick=0.2250; 2026-11-03:RED:body=-0.1300:wick=0.0700; 2026-11-04:GREEN:body=+0.9900:wick=0.2000; 2026-11-05:RED:body=-0.1500:wick=0.5992 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=209.09 (current export asof; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.07 (current export; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 551.35 | **NEUTRAL** |
| `B04_income` | 24.48 | **GOOD** |
| `B05_profit_margin` | 4.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.25 vs prior_export=17.25 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.61 | **GOOD** |
| `B13_short_float` | 3.68 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=209.09 (this export) | prior_export=209.09 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.07 (this export) | prior_export=14.07 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MNTK  ·  score **+17**  ·  Specialty Chemicals
price=2.5  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.23 on 2026-09-08; prev RSI=71.31 on 2026-09-04 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=14.500 (G=0.2900 R=0.0200); 2026-09-04:RED:O=2.2500,C=2.2300,body=-0.0200,vol=305800.0; 2026-09-08:GREEN:O=2.2100,C=2.5000,body=+0.2900,vol=703900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=2.302 (Gvol=703900 Rvol=305800); 2026-09-04:RED:O=2.2500,C=2.2300,body=-0.0200,vol=305800.0; 2026-09-08:GREEN:O=2.2100,C=2.5000,body=+0.2900,vol=703900.0 | **GOOD** |
| `A07_rvol` | RVOL=3.481 on 2026-09-08: today_vol=703900 / avg20=202225 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.451 on 2026-09-08 (price=2.5000, mid=1.9230, upper=2.3207, lower=1.5253; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-08: price=2.5000 vs SMA50=1.7735 dist=+40.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=1.9230 SMA50=1.7735 SMA80=1.7129 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-26@1.4550; S2[2026-07-07→2026-08-07] low=2026-07-09@1.5250; S3[2026-08-10→2026-09-08] low=2026-08-11@1.7000 | lows=[1.4550000429153442, 1.524999976158142, 1.7000000476837158] span=16.84% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.8787879663612982 wick_frac=0.12121203363870185 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2222222222222222 wick_frac=0.7777777777777778 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.500011920940324 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.0400:wick=0.0600; 2026-09-02:GREEN:body=+0.2900:wick=0.0800; 2026-09-03:GREEN:body=+0.1200:wick=0.0500; 2026-09-04:RED:body=-0.0200:wick=0.0700; 2026-09-08:GREEN:body=+0.2900:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=542.86 (current export asof; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.47 (current export; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 189.1 | **NEUTRAL** |
| `B04_income` | 7.93 | **GOOD** |
| `B05_profit_margin` | 4.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 2.03 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=2.03 vs prior_export=2.03 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.3 | **GOOD** |
| `B13_short_float` | 3.52 | **NEUTRAL** |
| `B14_earnings_date` | 11/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=542.86 (this export) | prior_export=542.86 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.47 (this export) | prior_export=9.47 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+16**  ·  Information Technology Services
price=165.6999969482422  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.39 on 2026-11-05; prev RSI=62.47 on 2026-11-04 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 62.47@2026-11-04 → 78.39@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.47@2026-11-04 → 78.39@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 62.47@2026-11-04 → 78.39@2026-11-05 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=7.540 (G=9.5000 R=1.2600); 2026-11-04:RED:O=138.6200,C=137.3600,body=-1.2600,vol=3251889.0; 2026-11-05:GREEN:O=156.2000,C=165.7000,body=+9.5000,vol=3671830.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.129 (Gvol=3671830 Rvol=3251889); 2026-11-04:RED:O=138.6200,C=137.3600,body=-1.2600,vol=3251889.0; 2026-11-05:GREEN:O=156.2000,C=165.7000,body=+9.5000,vol=3671830.0 | **GOOD** |
| `A07_rvol` | RVOL=3.277 on 2026-11-05: today_vol=3671830 / avg20=1120358 (avg window 2026-10-08→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.342 on 2026-11-05 (price=165.7000, mid=131.7630, upper=157.0495, lower=106.4765; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=165.7000 vs SMA50=120.9202 dist=+37.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=131.7630 SMA50=120.9202 SMA80=112.8110 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-10@95.6400; S2[2026-09-09→2026-10-07] low=2026-09-28@104.3300; S3[2026-10-08→2026-11-05] low=2026-10-08@106.1200 | lows=[95.63999938964844, 104.33000183105469, 106.12000274658203] span=10.96% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.6569851738511053 wick_frac=0.3430148261488946 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.14946567040986988 wick_frac=0.8505343295901301 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.539715410233121 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:GREEN:body=+3.3000:wick=0.8500; 2026-11-02:GREEN:body=+2.8000:wick=3.3849; 2026-11-03:RED:body=-0.4500:wick=7.1600; 2026-11-04:RED:body=-1.2600:wick=7.1700; 2026-11-05:GREEN:body=+9.5000:wick=4.9600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.06 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.5 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.19 | **NEUTRAL** |
| `B08_target_price_delta` | delta=10.25 (now=141.19 vs prior_export=130.94 on finviz_2026-11-05) | **GOOD** |
| `B09_analyst_recom` | 2.18 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.22 | **GOOD** |
| `B13_short_float` | 12.95 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.06 (this export) | prior_export=7.09 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.5 (this export) | prior_export=0.51 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.549999237060547  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.72 on 2026-10-12; prev RSI=57.75 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=8.166 (G=0.4900 R=0.0600); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.297 (Gvol=849222 Rvol=654764); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A07_rvol` | RVOL=0.785 on 2026-10-12: today_vol=654764 / avg20=834281 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.229 on 2026-10-12 (price=16.5500, mid=16.3710, upper=17.1537, lower=15.5883; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=16.5500 vs SMA50=16.1181 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.37_50=16.12_80=16.12 on 2026-10-12: SMA20=16.3710 SMA50=16.1181 SMA80=16.1191 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-31@14.3630; S2[2026-08-13→2026-09-11] low=2026-08-14@14.9010; S3[2026-09-14→2026-10-12] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5764713538160505 wick_frac=0.42352864618394953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.2754882213854103 wick_frac=0.7245117786145897 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.16647593616886 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.55 (current export asof; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 321.74 | **NEUTRAL** |
| `B04_income` | 314.27 | **GOOD** |
| `B05_profit_margin` | 97.68 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.5 vs prior_export=18.5 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.49 vs prior=5.49 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.71 | **GOOD** |
| `B13_short_float` | 6.54 | **NEUTRAL** |
| `B14_earnings_date` | 10/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.55 (this export) | prior_export=4.55 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TEVA  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=41.779998779296875  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.59 on 2026-11-05; prev RSI=59.10 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.10@2026-11-04 → 58.59@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.10@2026-11-04 → 58.59@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.10@2026-11-04 → 58.59@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=7.250 (G=0.2900 R=0.0400); 2026-11-04:RED:O=41.8900,C=41.8500,body=-0.0400,vol=10024800.0; 2026-11-05:GREEN:O=41.4900,C=41.7800,body=+0.2900,vol=13632519.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.360 (Gvol=13632519 Rvol=10024800); 2026-11-04:RED:O=41.8900,C=41.8500,body=-0.0400,vol=10024800.0; 2026-11-05:GREEN:O=41.4900,C=41.7800,body=+0.2900,vol=13632519.0 | **GOOD** |
| `A07_rvol` | RVOL=1.916 on 2026-11-05: today_vol=13632519 / avg20=7113682 (avg window 2026-10-08→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.559 on 2026-11-05 (price=41.7800, mid=40.9680, upper=42.4211, lower=39.5149; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=41.7800 vs SMA50=39.4040 dist=+6.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=40.9680 SMA50=39.4040 SMA80=37.6135 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-10@35.0000; S2[2026-09-09→2026-10-07] low=2026-09-10@36.0400; S3[2026-10-08→2026-11-05] low=2026-10-08@38.2500 | lows=[35.0, 36.040000915527344, 38.25] span=9.29% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.19926972094218057 wick_frac=0.8007302790578195 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.030769953079196097 wick_frac=0.969230046920804 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.249761586877741 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:GREEN:body=+0.9200:wick=0.4500; 2026-11-02:RED:body=-0.4200:wick=0.9500; 2026-11-03:DOJI:body=+0.0000:wick=1.8000; 2026-11-04:RED:body=-0.0400:wick=1.2600; 2026-11-05:GREEN:body=+0.2900:wick=1.1653 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.31 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.85 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17239.0 | **NEUTRAL** |
| `B04_income` | 506.0 | **GOOD** |
| `B05_profit_margin` | 2.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.42 vs prior_export=50.42 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.31 | **GOOD** |
| `B10_insider_transactions` | -1.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.28 (now=-1.41 vs prior=-1.69 on finviz_2026-11-05) | **GOOD** |
| `B12_institutional_transactions` | 5.54 | **GOOD** |
| `B13_short_float` | 2.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.31 (this export) | prior_export=19.31 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.85 (this export) | prior_export=3.85 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+16**  ·  Software - Application
price=269.1600036621094  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.44 on 2026-11-05; prev RSI=69.31 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.31@2026-11-04 → 68.44@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.31@2026-11-04 → 68.44@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.31@2026-11-04 → 68.44@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=2.075 (G=11.6000 R=5.5900); 2026-11-04:GREEN:O=258.4900,C=270.0900,body=+11.6000,vol=3823430.0; 2026-11-05:RED:O=274.7500,C=269.1600,body=-5.5900,vol=3807246.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.004 (Gvol=3823430 Rvol=3807246); 2026-11-04:GREEN:O=258.4900,C=270.0900,body=+11.6000,vol=3823430.0; 2026-11-05:RED:O=274.7500,C=269.1600,body=-5.5900,vol=3807246.0 | **GOOD** |
| `A07_rvol` | RVOL=2.107 on 2026-11-05: today_vol=3807246 / avg20=1807372 (avg window 2026-10-08→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.037 on 2026-11-05 (price=269.1600, mid=246.4270, upper=268.3567, lower=224.4973; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=269.1600 vs SMA50=233.1572 dist=+15.44% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=246.43_50=233.16_80=234.65 on 2026-11-05: SMA20=246.4270 SMA50=233.1572 SMA80=234.6496 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-09-08@208.0900; S2[2026-09-09→2026-10-07] low=2026-09-29@199.1801; S3[2026-10-08→2026-11-05] low=2026-10-14@228.5900 | lows=[208.08999633789062, 199.1800994873047, 228.58999633789062] span=14.77% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.7211695533635758 wick_frac=0.2788304466364242 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.42030086229446506 wick_frac=0.5796991377055349 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.075136619479945 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:GREEN:body=+4.2500:wick=3.5800; 2026-11-02:GREEN:body=+7.7900:wick=5.4553; 2026-11-03:RED:body=-4.5000:wick=5.3900; 2026-11-04:GREEN:body=+11.6000:wick=4.4850; 2026-11-05:RED:body=-5.5900:wick=7.7100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 4.34 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLX  ·  score **+15**  ·  Biotechnology
price=2.7799999713897705  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.19 on 2026-10-12; prev RSI=59.19 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.19@2026-10-09 → 59.19@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.19@2026-10-09 → 59.19@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.19@2026-10-09 → 59.19@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1100 R=0.0000); 2026-10-09:GREEN:O=2.7000,C=2.7800,body=+0.0800,vol=1196839.0; 2026-10-12:GREEN:O=2.7500,C=2.7800,body=+0.0300,vol=716039.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1912878 Rvol=0); 2026-10-09:GREEN:O=2.7000,C=2.7800,body=+0.0800,vol=1196839.0; 2026-10-12:GREEN:O=2.7500,C=2.7800,body=+0.0300,vol=716039.0 | **GOOD** |
| `A07_rvol` | RVOL=0.844 on 2026-10-12: today_vol=716039 / avg20=848649 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.574 on 2026-10-12 (price=2.7800, mid=2.7265, upper=2.8197, lower=2.6333; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=2.7800 vs SMA50=2.5870 dist=+7.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=2.7265 SMA50=2.5870 SMA80=2.4800 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-12 (63 bars); S1[2026-07-13→2026-08-12] low=2026-07-30@2.2300; S2[2026-08-13→2026-09-11] low=2026-08-14@2.1700; S3[2026-09-14→2026-10-12] low=2026-10-01@2.6000 | lows=[2.2300000190734863, 2.1700000762939453, 2.5999999046325684] span=19.82% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5877187698943513 wick_frac=0.41228123010564866 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.0100:wick=0.0950; 2026-10-07:GREEN:body=+0.0100:wick=0.0500; 2026-10-08:GREEN:body=+0.0200:wick=0.0800; 2026-10-09:GREEN:body=+0.0800:wick=0.0150; 2026-10-12:GREEN:body=+0.0300:wick=0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=600.0 (current export asof; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=53.64 (current export; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 80.62 | **NEUTRAL** |
| `B04_income` | 18.95 | **GOOD** |
| `B05_profit_margin` | 23.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.0 vs prior_export=11.0 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 4.79 | **NEUTRAL** |
| `B14_earnings_date` | 11/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=600.0 (this export) | prior_export=600.0 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=53.64 (this export) | prior_export=53.64 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CTSH  ·  score **+15**  ·  Information Technology Services
price=66.54000091552734  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.25 on 2026-11-05; prev RSI=53.66 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.66@2026-11-04 → 55.25@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.66@2026-11-04 → 55.25@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.66@2026-11-04 → 55.25@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=2.793 (G=1.6200 R=0.5800); 2026-11-04:GREEN:O=64.1600,C=65.7800,body=+1.6200,vol=10844604.0; 2026-11-05:RED:O=67.1200,C=66.5400,body=-0.5800,vol=7924940.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.368 (Gvol=10844604 Rvol=7924940); 2026-11-04:GREEN:O=64.1600,C=65.7800,body=+1.6200,vol=10844604.0; 2026-11-05:RED:O=67.1200,C=66.5400,body=-0.5800,vol=7924940.0 | **GOOD** |
| `A07_rvol` | RVOL=1.152 on 2026-11-05: today_vol=7924940 / avg20=6879676 (avg window 2026-10-08→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.116 on 2026-11-05 (price=66.5400, mid=65.7025, upper=72.9142, lower=58.4908; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=66.5400 vs SMA50=62.4436 dist=+6.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=65.7025 SMA50=62.4436 SMA80=59.4170 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-17@56.2204; S2[2026-09-09→2026-10-07] low=2026-09-28@55.6300; S3[2026-10-08→2026-11-05] low=2026-10-08@54.7000 | lows=[56.220350988128516, 55.630001068115234, 54.70000076293945] span=2.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.5624988410196907 wick_frac=0.4375011589803093 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.3062797378037234 wick_frac=0.6937202621962766 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.793086211886033 need>1.4; red_wick_gt_green=False 5d trail=2026-10-30:GREEN:body=+1.4100:wick=1.0350; 2026-11-02:GREEN:body=+0.1900:wick=1.8550; 2026-11-03:GREEN:body=+0.3900:wick=1.4500; 2026-11-04:GREEN:body=+1.6200:wick=1.2600; 2026-11-05:RED:body=-0.5800:wick=1.3137 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.78 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21891.0 | **NEUTRAL** |
| `B04_income` | 2573.0 | **GOOD** |
| `B05_profit_margin` | 11.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.35 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=69.35 vs prior_export=69.35 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | -0.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.66 vs prior=-0.66 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 6.59 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+15**  ·  Medical Devices
price=139.80999755859375  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.20 on 2026-10-12; prev RSI=44.78 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.9300 R=0.0000); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2183896 Rvol=0); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A07_rvol` | RVOL=0.819 on 2026-10-12: today_vol=1319431 / avg20=1610443 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-10-12 (price=139.8100, mid=136.1735, upper=143.1581, lower=129.1889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=139.8100 vs SMA50=141.5730 dist=-1.25% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=136.1735 SMA50=141.5730 SMA80=147.5865 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-05@126.4000; S2[2026-08-13→2026-09-11] low=2026-09-11@131.2400; S3[2026-09-14→2026-10-12] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5660465898505131 wick_frac=0.43395341014948685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.0400:wick=1.3950; 2026-10-07:RED:body=-0.5800:wick=3.1000; 2026-10-08:GREEN:body=+0.5100:wick=1.2300; 2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.4 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.13 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3189.2 | **NEUTRAL** |
| `B04_income` | 428.8 | **GOOD** |
| `B05_profit_margin` | 13.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 177.27 | **NEUTRAL** |
| `B08_target_price_delta` | delta=8.409999999999997 (now=177.27 vs prior_export=168.86 on finviz_2026-11-05) | **GOOD** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | 2.32 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.32 vs prior=2.32 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.12 | **GOOD** |
| `B13_short_float` | 8.15 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.4 (this export) | prior_export=25.4 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.13 (this export) | prior_export=1.13 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SEZL  ·  score **+15**  ·  Credit Services
price=132.3699951171875  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.93 on 2026-10-15; prev RSI=54.94 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=3.118 (G=11.1300 R=3.5700); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.009 (Gvol=462162 Rvol=457963); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A07_rvol` | RVOL=0.843 on 2026-10-15: today_vol=462162 / avg20=548541 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.975 on 2026-10-15 (price=132.3700, mid=115.5997, upper=132.7973, lower=98.4022; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=132.3700 vs SMA50=119.8667 dist=+10.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-15: SMA20=115.5997 SMA50=119.8667 SMA80=138.2212 | **BAD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@114.1600; S2[2026-08-18→2026-09-16] low=2026-09-01@112.4100; S3[2026-09-17→2026-10-15] low=2026-09-24@104.8700 | lows=[114.16000366210938, 112.41000366210938, 104.87000274658203] span=8.86% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.9823466866705229 wick_frac=0.017653313329477113 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.38867693830950656 wick_frac=0.6113230616904934 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.117646555979886 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+8.6400:wick=1.6700; 2026-10-12:GREEN:body=+2.7300:wick=3.1975; 2026-10-13:RED:body=-1.1400:wick=5.0900; 2026-10-14:RED:body=-3.5700:wick=5.6150; 2026-10-15:GREEN:body=+11.1300:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.8 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.41 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 581.59 | **NEUTRAL** |
| `B04_income` | 182.58 | **GOOD** |
| `B05_profit_margin` | 31.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 186.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=19.330000000000013 (now=186.5 vs prior_export=167.17 on finviz_2026-11-05) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -1.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.02 vs prior=-1.02 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.47 | **GOOD** |
| `B13_short_float` | 23.69 | **GOOD** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.8 (this export) | prior_export=9.7 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.41 (this export) | prior_export=10.81 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UPBD  ·  score **+15**  ·  Software - Application
price=24.8799991607666  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=84.91 on 2026-11-05; prev RSI=83.62 on 2026-11-04 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 83.62@2026-11-04 → 84.91@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 83.62@2026-11-04 → 84.91@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 83.62@2026-11-04 → 84.91@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=101.998 (G=1.0200 R=0.0100); 2026-11-04:GREEN:O=23.1200,C=24.1400,body=+1.0200,vol=1825669.0; 2026-11-05:RED:O=24.8900,C=24.8800,body=-0.0100,vol=1570976.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.162 (Gvol=1825669 Rvol=1570976); 2026-11-04:GREEN:O=23.1200,C=24.1400,body=+1.0200,vol=1825669.0; 2026-11-05:RED:O=24.8900,C=24.8800,body=-0.0100,vol=1570976.0 | **GOOD** |
| `A07_rvol` | RVOL=1.149 on 2026-11-05: today_vol=1570976 / avg20=1366921 (avg window 2026-10-08→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.074 on 2026-11-05 (price=24.8800, mid=18.1617, upper=24.4145, lower=11.9090; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=24.8800 vs SMA50=17.5853 dist=+41.48% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=18.16_50=17.59_80=18.57 on 2026-11-05: SMA20=18.1617 SMA50=17.5853 SMA80=18.5653 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-09-01@17.9000; S2[2026-09-09→2026-10-07] low=2026-10-07@14.9800; S3[2026-10-08→2026-11-05] low=2026-10-14@15.1500 | lows=[17.899999618530273, 14.979999542236328, 15.149999618530273] span=19.49% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.8225796279746569 wick_frac=0.17742037202534314 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.010363995050268242 wick_frac=0.9896360049497318 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=101.99752050352852 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:GREEN:body=+0.6500:wick=0.5500; 2026-11-02:GREEN:body=+2.1400:wick=0.3200; 2026-11-03:RED:body=-0.5400:wick=0.5300; 2026-11-04:GREEN:body=+1.0200:wick=0.2200; 2026-11-05:RED:body=-0.0100:wick=0.9549 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.29 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.99 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4725.76 | **NEUTRAL** |
| `B04_income` | 111.39 | **GOOD** |
| `B05_profit_margin` | 2.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 27.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=27.33 vs prior_export=27.33 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 2.24 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.1900000000000004 (now=2.24 vs prior=2.05 on finviz_2026-11-05) | **GOOD** |
| `B12_institutional_transactions` | 4.66 | **GOOD** |
| `B13_short_float` | 16.45 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.29 (this export) | prior_export=11.29 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.99 (this export) | prior_export=1.99 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MICC  ·  score **+15**  ·  Packaged Foods
price=20.110000610351562  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.54 on 2026-11-05; prev RSI=60.89 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.89@2026-11-04 → 60.54@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.89@2026-11-04 → 60.54@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.89@2026-11-04 → 60.54@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=12.501 (G=0.2500 R=0.0200); 2026-11-04:GREEN:O=19.8900,C=20.1400,body=+0.2500,vol=822677.0; 2026-11-05:RED:O=20.1300,C=20.1100,body=-0.0200,vol=1384946.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=0.594 (Gvol=822677 Rvol=1384946); 2026-11-04:GREEN:O=19.8900,C=20.1400,body=+0.2500,vol=822677.0; 2026-11-05:RED:O=20.1300,C=20.1100,body=-0.0200,vol=1384946.0 | **BAD** |
| `A07_rvol` | RVOL=1.618 on 2026-11-05: today_vol=1384946 / avg20=856028 (avg window 2026-10-08→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.681 on 2026-11-05 (price=20.1100, mid=19.0005, upper=20.6296, lower=17.3714; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=20.1100 vs SMA50=18.9422 dist=+6.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=19.00_50=18.94_80=18.96 on 2026-11-05: SMA20=19.0005 SMA50=18.9422 SMA80=18.9553 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-18@18.8770; S2[2026-09-09→2026-10-07] low=2026-10-07@16.8600; S3[2026-10-08→2026-11-05] low=2026-10-08@16.9900 | lows=[18.87700080871582, 16.860000610351562, 16.989999771118164] span=11.96% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.609756324491296 wick_frac=0.39024367550870404 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.05262761632284294 wick_frac=0.947372383677157 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.500906056270862 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:RED:body=-0.0700:wick=0.3074; 2026-11-02:GREEN:body=+0.1200:wick=0.0850; 2026-11-03:RED:body=-0.2000:wick=0.0550; 2026-11-04:GREEN:body=+0.2500:wick=0.1600; 2026-11-05:RED:body=-0.0200:wick=0.3600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.59 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9445.44 | **NEUTRAL** |
| `B04_income` | 211.17 | **GOOD** |
| `B05_profit_margin` | 2.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.6 vs prior_export=19.6 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.62 | **GOOD** |
| `B13_short_float` | 1.64 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.0 (this export) | prior_export=6.0 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.59 (this export) | prior_export=2.59 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNK  ·  score **+15**  ·  Entertainment
price=40.380001068115234  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.08 on 2026-11-04; prev RSI=51.89 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.89@2026-11-03 → 62.08@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.89@2026-11-03 → 62.08@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.89@2026-11-03 → 62.08@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.9000 R=0.0000); 2026-11-03:GREEN:O=37.5300,C=38.5100,body=+0.9800,vol=3344500.0; 2026-11-04:GREEN:O=39.4600,C=40.3800,body=+0.9200,vol=2344135.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5688635 Rvol=0); 2026-11-03:GREEN:O=37.5300,C=38.5100,body=+0.9800,vol=3344500.0; 2026-11-04:GREEN:O=39.4600,C=40.3800,body=+0.9200,vol=2344135.0 | **GOOD** |
| `A07_rvol` | RVOL=1.291 on 2026-11-04: today_vol=2344135 / avg20=1815355 (avg window 2026-10-07→2026-11-03, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.699 on 2026-11-04 (price=40.3800, mid=38.7970, upper=41.0604, lower=36.5336; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-04: price=40.3800 vs SMA50=37.0354 dist=+9.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=38.7970 SMA50=37.0354 SMA80=36.3821 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-01@34.7100; S2[2026-09-08→2026-10-06] low=2026-09-15@33.7000; S3[2026-10-07→2026-11-04] low=2026-10-07@35.5300 | lows=[34.709999084472656, 33.70000076293945, 35.529998779296875] span=5.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.35004618130651965 wick_frac=0.6499538186934803 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-29:GREEN:body=+0.5800:wick=0.3700; 2026-10-30:RED:body=-0.9800:wick=0.2800; 2026-11-02:RED:body=-0.5000:wick=0.5150; 2026-11-03:GREEN:body=+0.9800:wick=2.5200; 2026-11-04:GREEN:body=+0.9200:wick=1.2700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.68 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.87 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3564.9 | **NEUTRAL** |
| `B04_income` | 287.0 | **GOOD** |
| `B05_profit_margin` | 8.05 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.38000000000000256 (now=44.38 vs prior_export=44.0 on finviz_2026-11-05) | **GOOD** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=1.2 (now=0.0 vs prior=-1.2 on finviz_2026-11-05) | **GOOD** |
| `B12_institutional_transactions` | 2.01 | **GOOD** |
| `B13_short_float` | 7.33 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.87 (this export) | prior_export=1.87 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNC  ·  score **+15**  ·  Healthcare Plans
price=74.16999816894531  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.04 on 2026-11-05; prev RSI=68.40 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.40@2026-11-04 → 63.04@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.40@2026-11-04 → 63.04@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.40@2026-11-04 → 63.04@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=2.344 (G=5.3800 R=2.2950); 2026-11-04:GREEN:O=70.6300,C=76.0100,body=+5.3800,vol=14968384.0; 2026-11-05:RED:O=76.4650,C=74.1700,body=-2.2950,vol=7687156.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.947 (Gvol=14968384 Rvol=7687156); 2026-11-04:GREEN:O=70.6300,C=76.0100,body=+5.3800,vol=14968384.0; 2026-11-05:RED:O=76.4650,C=74.1700,body=-2.2950,vol=7687156.0 | **GOOD** |
| `A07_rvol` | RVOL=1.439 on 2026-11-05: today_vol=7687156 / avg20=5340649 (avg window 2026-10-08→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.960 on 2026-11-05 (price=74.1700, mid=69.3605, upper=74.3686, lower=64.3524; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=74.1700 vs SMA50=66.5680 dist=+11.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=69.3605 SMA50=66.5680 SMA80=65.9190 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-31@63.4200; S2[2026-09-09→2026-10-07] low=2026-10-01@59.4600; S3[2026-10-08→2026-11-05] low=2026-10-08@63.3500 | lows=[63.41999816894531, 59.459999084472656, 63.349998474121094] span=6.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.7865499652550436 wick_frac=0.2134500347449564 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.4226515727370697 wick_frac=0.5773484272629302 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3442305774409093 need>1.4; red_wick_gt_green=True 5d trail=2026-10-30:RED:body=-0.8600:wick=2.4900; 2026-11-02:RED:body=-2.5900:wick=2.2200; 2026-11-03:GREEN:body=+1.3600:wick=0.0500; 2026-11-04:GREEN:body=+5.3800:wick=1.4600; 2026-11-05:RED:body=-2.2950:wick=3.1350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=506.06 (current export asof; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.49 (current export; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 203895.0 | **NEUTRAL** |
| `B04_income` | 1460.0 | **GOOD** |
| `B05_profit_margin` | 0.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 75.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=75.61 vs prior_export=75.61 on finviz_2026-11-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-11-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.8 | **GOOD** |
| `B13_short_float` | 2.25 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=506.06 (this export) | prior_export=506.06 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.49 (this export) | prior_export=7.49 (finviz_2026-11-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-06_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.