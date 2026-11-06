# A+B1 Feature Checklist — 2026-11-05

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,615** names
- Export: `finviz_2026-11-05.csv` · prior export for Δ: `2026-11-04`
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
| 1 | SWBI | +17 | 17 | 0 | 2026-11-04→2026-11-05 | Aerospace & Defense |
| 2 | MGNI | +17 | 20 | 3 | 2026-11-04→2026-11-05 | Advertising Agencies |
| 3 | CTSH | +16 | 17 | 1 | 2026-11-04→2026-11-05 | Information Technology Services |
| 4 | MICC | +16 | 17 | 1 | 2026-11-04→2026-11-05 | Packaged Foods |
| 5 | CNC | +16 | 18 | 2 | 2026-11-04→2026-11-05 | Healthcare Plans |
| 6 | HAE | +16 | 18 | 2 | 2026-11-04→2026-11-05 | Medical Devices |
| 7 | RERE | +16 | 17 | 1 | 2026-11-04→2026-11-05 | Internet Retail |
| 8 | EPAM | +16 | 19 | 3 | 2026-11-04→2026-11-05 | Information Technology Services |
| 9 | ADSK | +16 | 17 | 1 | 2026-11-04→2026-11-05 | Software - Application |
| 10 | TEVA | +16 | 18 | 2 | 2026-11-04→2026-11-05 | Drug Manufacturers - Specialty & Gen |
| 11 | MMSI | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Medical Instruments & Supplies |
| 12 | KDP | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Beverages - Non-Alcoholic |
| 13 | NCNO | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Software - Application |
| 14 | WAY | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Health Information Services |
| 15 | OLLI | +15 | 17 | 2 | 2026-11-04→2026-11-05 | Discount Stores |

## Full checklist — top 15

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
| `B08_target_price_delta` | delta=0.0 (now=17.25 vs prior_export=17.25 on finviz_2026-11-04) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.61 | **GOOD** |
| `B13_short_float` | 3.68 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=209.09 (this export) | prior_export=209.09 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.07 (this export) | prior_export=14.07 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MGNI  ·  score **+17**  ·  Advertising Agencies
price=27.520000457763672  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.28 on 2026-11-05; prev RSI=40.20 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 40.20@2026-11-04 → 63.28@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 40.20@2026-11-04 → 63.28@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 40.20@2026-11-04 → 63.28@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=25.286 (G=1.7700 R=0.0700); 2026-11-04:RED:O=22.8600,C=22.7900,body=-0.0700,vol=3204400.0; 2026-11-05:GREEN:O=25.7500,C=27.5200,body=+1.7700,vol=9699600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=3.027 (Gvol=9699600 Rvol=3204400); 2026-11-04:RED:O=22.8600,C=22.7900,body=-0.0700,vol=3204400.0; 2026-11-05:GREEN:O=25.7500,C=27.5200,body=+1.7700,vol=9699600.0 | **GOOD** |
| `A07_rvol` | RVOL=3.914 on 2026-11-05: today_vol=9699600 / avg20=2477989 (avg window 2026-09-17→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.220 on 2026-11-05 (price=27.5200, mid=24.7740, upper=27.0243, lower=22.5237; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=27.5200 vs SMA50=24.3116 dist=+13.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=24.7740 SMA50=24.3116 SMA80=22.4956 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-11-05 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@17.7300; S2[2026-08-18→2026-09-16] low=2026-09-02@22.4400; S3[2026-09-17→2026-11-05] low=2026-11-03@21.4300 | lows=[17.729999542236328, 22.440000534057617, 21.43000030517578] span=26.57% rising_lows=False flatish(≤12%)=False | **BAD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.7314051247776402 wick_frac=0.2685948752223598 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.0874997019764919 wick_frac=0.9125002980235081 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=25.2858310626703 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1600:wick=0.3750; 2026-10-12:RED:body=-0.2700:wick=0.4500; 2026-11-03:GREEN:body=+0.7700:wick=0.2300; 2026-11-04:RED:body=-0.0700:wick=0.7300; 2026-11-05:GREEN:body=+1.7700:wick=0.6500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.04 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.83 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 742.04 | **NEUTRAL** |
| `B04_income` | 166.89 | **GOOD** |
| `B05_profit_margin` | 22.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 30.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0600000000000023 (now=30.28 vs prior_export=29.22 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.39 | **GOOD** |
| `B10_insider_transactions` | -26.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.6699999999999982 (now=-26.8 vs prior=-27.47 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 7.74 | **GOOD** |
| `B13_short_float` | 7.5 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.04 (this export) | prior_export=7.04 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.83 (this export) | prior_export=5.83 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CTSH  ·  score **+16**  ·  Information Technology Services
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
| `B08_target_price_delta` | delta=5.089999999999989 (now=69.35 vs prior_export=64.26 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | -0.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.66 vs prior=-0.66 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 6.59 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=-0.54 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.0 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MICC  ·  score **+16**  ·  Packaged Foods
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
| `B08_target_price_delta` | delta=0.740000000000002 (now=19.6 vs prior_export=18.86 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.62 | **GOOD** |
| `B13_short_float` | 1.64 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.0 (this export) | prior_export=6.0 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.59 (this export) | prior_export=3.07 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNC  ·  score **+16**  ·  Healthcare Plans
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
| `B08_target_price_delta` | delta=3.8299999999999983 (now=75.61 vs prior_export=71.78 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.8 | **GOOD** |
| `B13_short_float` | 2.25 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=506.06 (this export) | prior_export=131.29 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.49 (this export) | prior_export=12.47 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAE  ·  score **+16**  ·  Medical Devices
price=124.11000061035156  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.18 on 2026-11-05; prev RSI=75.87 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 75.87@2026-11-04 → 68.18@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.87@2026-11-04 → 68.18@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 75.87@2026-11-04 → 68.18@2026-11-05 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=2.697 (G=5.2600 R=1.9500); 2026-11-04:GREEN:O=122.1600,C=127.4200,body=+5.2600,vol=981900.0; 2026-11-05:RED:O=126.0600,C=124.1100,body=-1.9500,vol=1302100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=0.754 (Gvol=981900 Rvol=1302100); 2026-11-04:GREEN:O=122.1600,C=127.4200,body=+5.2600,vol=981900.0; 2026-11-05:RED:O=126.0600,C=124.1100,body=-1.9500,vol=1302100.0 | **BAD** |
| `A07_rvol` | RVOL=1.756 on 2026-11-05: today_vol=1302100 / avg20=741611 (avg window 2026-09-23→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.625 on 2026-11-05 (price=124.1100, mid=113.0790, upper=130.7344, lower=95.4236; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=124.1100 vs SMA50=107.3802 dist=+15.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=113.0790 SMA50=107.3802 SMA80=96.9089 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-11-05 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-23@74.9500; S2[2026-08-24→2026-09-22] low=2026-09-11@100.5300; S3[2026-09-23→2026-11-05] low=2026-10-07@101.3300 | lows=[74.94999694824219, 100.52999877929688, 101.33000183105469] span=35.20% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.6948473869070716 wick_frac=0.30515261309292846 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.18089027542258923 wick_frac=0.8191097245774107 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6974373019288707 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:RED:body=-0.9100:wick=2.9000; 2026-10-16:RED:body=-3.5500:wick=-0.0614; 2026-11-03:GREEN:body=+1.2100:wick=1.2300; 2026-11-04:GREEN:body=+5.2600:wick=2.3100; 2026-11-05:RED:body=-1.9500:wick=8.8300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.51 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1352.01 | **NEUTRAL** |
| `B04_income` | 96.29 | **GOOD** |
| `B05_profit_margin` | 7.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 124.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=13.0 (now=124.5 vs prior_export=111.5 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.41 | **GOOD** |
| `B13_short_float` | 7.68 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.51 (this export) | prior_export=3.51 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RERE  ·  score **+16**  ·  Internet Retail
price=4.300000190734863  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.60 on 2026-11-05; prev RSI=67.60 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.60@2026-11-04 → 67.60@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.60@2026-11-04 → 67.60@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.60@2026-11-04 → 67.60@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1500 R=0.0000); 2026-11-04:GREEN:O=4.1500,C=4.3000,body=+0.1500,vol=688000.0; 2026-11-05:DOJI:O=4.3000,C=4.3000,body=+0.0000,vol=1087800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=2.265 (Gvol=1231900 Rvol=543900); 2026-11-04:GREEN:O=4.1500,C=4.3000,body=+0.1500,vol=688000.0; 2026-11-05:DOJI:O=4.3000,C=4.3000,body=+0.0000,vol=1087800.0 | **GOOD** |
| `A07_rvol` | RVOL=2.098 on 2026-11-05: today_vol=1087800 / avg20=518513 (avg window 2026-09-17→2026-11-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.110 on 2026-11-05 (price=4.3000, mid=3.8795, upper=4.2582, lower=3.5008; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=4.3000 vs SMA50=4.0740 dist=+5.55% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=3.88_50=4.07_80=4.04 on 2026-11-05: SMA20=3.8795 SMA50=4.0740 SMA80=4.0408 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-11-05 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@3.7550; S2[2026-08-18→2026-09-16] low=2026-08-25@3.8600; S3[2026-09-17→2026-11-05] low=2026-10-02@3.6300 | lows=[3.755000114440918, 3.859999895095825, 3.630000114440918] span=6.34% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.5769229358471188 wick_frac=0.4230770641528812 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.0400:wick=0.0500; 2026-10-12:RED:body=-0.0100:wick=0.1250; 2026-11-03:DOJI:body=+0.0000:wick=0.1100; 2026-11-04:GREEN:body=+0.1500:wick=0.1100; 2026-11-05:DOJI:body=+0.0000:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=17.72 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.29 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3462.05 | **NEUTRAL** |
| `B04_income` | 69.55 | **GOOD** |
| `B05_profit_margin` | 2.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 6.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.04999999999999982 (now=6.79 vs prior_export=6.74 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 1.54 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.72 (this export) | prior_export=17.72 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.29 (this export) | prior_export=5.29 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 130.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=10.25 (now=130.94 vs prior_export=120.69 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.18 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.22 | **GOOD** |
| `B13_short_float` | 12.95 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-11-04) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 4.34 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=6.219999999999999 (now=50.42 vs prior_export=44.2 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | -1.69 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.69 vs prior=-1.69 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.54 | **GOOD** |
| `B13_short_float` | 2.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.31 (this export) | prior_export=-61.39 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.85 (this export) | prior_export=3.02 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MMSI  ·  score **+15**  ·  Medical Instruments & Supplies
price=89.62000274658203  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.08 on 2026-11-05; prev RSI=58.67 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.67@2026-11-04 → 56.08@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.67@2026-11-04 → 56.08@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.67@2026-11-04 → 56.08@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=2.308 (G=3.6000 R=1.5600); 2026-11-04:GREEN:O=87.0300,C=90.6300,body=+3.6000,vol=1387200.0; 2026-11-05:RED:O=91.1800,C=89.6200,body=-1.5600,vol=940200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.475 (Gvol=1387200 Rvol=940200); 2026-11-04:GREEN:O=87.0300,C=90.6300,body=+3.6000,vol=1387200.0; 2026-11-05:RED:O=91.1800,C=89.6200,body=-1.5600,vol=940200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.468 on 2026-11-05: today_vol=940200 / avg20=640429 (avg window 2026-09-17→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.815 on 2026-11-05 (price=89.6200, mid=86.8210, upper=90.2559, lower=83.3861; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=89.6200 vs SMA50=88.2476 dist=+1.56% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.82_50=88.25_80=82.89 on 2026-11-05: SMA20=86.8210 SMA50=88.2476 SMA80=82.8902 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-11-05 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-15@71.0200; S2[2026-08-18→2026-09-16] low=2026-09-10@85.1900; S3[2026-09-17→2026-11-05] low=2026-11-03@83.5600 | lows=[71.0199966430664, 85.19000244140625, 83.55999755859375] span=19.95% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.6326883883078573 wick_frac=0.36731161169214266 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.37864063287013716 wick_frac=0.6213593671298628 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3076949411166323 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+1.8000:wick=0.4100; 2026-10-12:GREEN:body=+2.1000:wick=0.1800; 2026-11-03:GREEN:body=+0.1200:wick=1.3800; 2026-11-04:GREEN:body=+3.6000:wick=2.0900; 2026-11-05:RED:body=-1.5600:wick=2.5600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.76 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1612.95 | **NEUTRAL** |
| `B04_income` | 160.12 | **GOOD** |
| `B05_profit_margin` | 9.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 107.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=5.549999999999997 (now=107.0 vs prior_export=101.45 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -1.55 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-1.55 vs prior=-1.56 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 3.6 | **GOOD** |
| `B13_short_float` | 6.5 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.76 (this export) | prior_export=24.09 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=3.36 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KDP  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=36.88999938964844  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.85 on 2026-11-05; prev RSI=65.46 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.46@2026-11-04 → 65.85@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.46@2026-11-04 → 65.85@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.46@2026-11-04 → 65.85@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=3.063 (G=0.9800 R=0.3200); 2026-11-04:GREEN:O=35.8100,C=36.7900,body=+0.9800,vol=28541657.0; 2026-11-05:RED:O=37.2100,C=36.8900,body=-0.3200,vol=22793135.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.252 (Gvol=28541657 Rvol=22793135); 2026-11-04:GREEN:O=35.8100,C=36.7900,body=+0.9800,vol=28541657.0; 2026-11-05:RED:O=37.2100,C=36.8900,body=-0.3200,vol=22793135.0 | **GOOD** |
| `A07_rvol` | RVOL=1.315 on 2026-11-05: today_vol=22793135 / avg20=17327907 (avg window 2026-10-08→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.810 on 2026-11-05 (price=36.8900, mid=34.6840, upper=37.4068, lower=31.9612; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=36.8900 vs SMA50=32.7744 dist=+12.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=34.6840 SMA50=32.7744 SMA80=32.0345 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-05 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-11@28.6600; S2[2026-09-09→2026-10-07] low=2026-09-30@30.0600; S3[2026-10-08→2026-11-05] low=2026-10-08@30.6000 | lows=[28.65999984741211, 30.059999465942383, 30.600000381469727] span=6.77% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.506460350755449 wick_frac=0.493539649244551 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.28318625895443283 wick_frac=0.7168137410455672 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0625014901175405 need>1.4; red_wick_gt_green=False 5d trail=2026-10-30:RED:body=-0.2900:wick=0.3150; 2026-11-02:GREEN:body=+0.7600:wick=0.7600; 2026-11-03:RED:body=-0.5700:wick=0.8600; 2026-11-04:GREEN:body=+0.9800:wick=0.9550; 2026-11-05:RED:body=-0.3200:wick=0.8100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.74 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.23 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23128.0 | **NEUTRAL** |
| `B04_income` | 1493.0 | **GOOD** |
| `B05_profit_margin` | 6.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.3699999999999974 (now=38.75 vs prior_export=36.38 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.25 | **GOOD** |
| `B13_short_float` | 6.48 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.74 (this export) | prior_export=6.2 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.23 (this export) | prior_export=1.07 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NCNO  ·  score **+15**  ·  Software - Application
price=23.209999084472656  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.58 on 2026-11-05; prev RSI=68.36 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.36@2026-11-04 → 66.58@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.36@2026-11-04 → 66.58@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.36@2026-11-04 → 66.58@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=1.615 (G=0.6300 R=0.3900); 2026-11-04:GREEN:O=22.8100,C=23.4400,body=+0.6300,vol=2265700.0; 2026-11-05:RED:O=23.6000,C=23.2100,body=-0.3900,vol=1608800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.408 (Gvol=2265700 Rvol=1608800); 2026-11-04:GREEN:O=22.8100,C=23.4400,body=+0.6300,vol=2265700.0; 2026-11-05:RED:O=23.6000,C=23.2100,body=-0.3900,vol=1608800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.794 on 2026-11-05: today_vol=1608800 / avg20=2026750 (avg window 2026-09-17→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.066 on 2026-11-05 (price=23.2100, mid=19.9090, upper=23.0042, lower=16.8138; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-05: price=23.2100 vs SMA50=20.6996 dist=+12.13% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=19.91_50=20.70_80=19.41 on 2026-11-05: SMA20=19.9090 SMA50=20.6996 SMA80=19.4075 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-11-05 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-23@16.1300; S2[2026-08-18→2026-09-16] low=2026-08-26@19.1370; S3[2026-09-17→2026-11-05] low=2026-09-29@17.6650 | lows=[16.1299991607666, 19.136999130249023, 17.665000915527344] span=18.64% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.6631584864899593 wick_frac=0.33684151351004066 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.4239144181032249 wick_frac=0.5760855818967752 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.61538198197317 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1300:wick=0.4000; 2026-10-12:RED:body=-0.2500:wick=0.3650; 2026-11-03:RED:body=-0.2800:wick=0.5110; 2026-11-04:GREEN:body=+0.6300:wick=0.3200; 2026-11-05:RED:body=-0.3900:wick=0.5300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.13 (current export asof; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.17 (current export; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 622.24 | **NEUTRAL** |
| `B04_income` | 33.6 | **GOOD** |
| `B05_profit_margin` | 5.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.15000000000000213 (now=24.23 vs prior_export=24.08 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | -3.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.11000000000000032 (now=-3.11 vs prior=-3.22 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 1.57 | **GOOD** |
| `B13_short_float` | 19.27 | **NEUTRAL** |
| `B14_earnings_date` | 8/25/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.13 (this export) | prior_export=18.13 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.17 (this export) | prior_export=1.17 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+15**  ·  Health Information Services
price=28.8799991607666  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.60 on 2026-11-05; prev RSI=65.24 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.24@2026-11-04 → 59.60@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.24@2026-11-04 → 59.60@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.24@2026-11-04 → 59.60@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=1.860 (G=1.7300 R=0.9300); 2026-11-04:GREEN:O=27.9600,C=29.6900,body=+1.7300,vol=3258400.0; 2026-11-05:RED:O=29.8100,C=28.8800,body=-0.9300,vol=2381000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.369 (Gvol=3258400 Rvol=2381000); 2026-11-04:GREEN:O=27.9600,C=29.6900,body=+1.7300,vol=3258400.0; 2026-11-05:RED:O=29.8100,C=28.8800,body=-0.9300,vol=2381000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.025 on 2026-11-05: today_vol=2381000 / avg20=2323207 (avg window 2026-09-30→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.554 on 2026-11-05 (price=28.8800, mid=27.3542, upper=30.1075, lower=24.6010; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=28.8800 vs SMA50=26.0471 dist=+10.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=27.3542 SMA50=26.0471 SMA80=25.0435 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-11-05 (63 bars); S1[2026-07-30→2026-08-28] low=2026-07-30@20.1410; S2[2026-08-31→2026-09-29] low=2026-09-10@22.9550; S3[2026-09-30→2026-11-05] low=2026-11-03@24.1000 | lows=[20.141000747680664, 22.954999923706055, 24.100000381469727] span=19.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.9637890674393819 wick_frac=0.03621093256061809 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.42272763134052865 wick_frac=0.5772723686594713 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8602160020345044 need>1.4; red_wick_gt_green=False 5d trail=2026-10-22:GREEN:body=+0.0800:wick=0.6000; 2026-10-23:RED:body=-0.1800:wick=0.5200; 2026-11-03:GREEN:body=+2.2250:wick=1.7950; 2026-11-04:GREEN:body=+1.7300:wick=0.0650; 2026-11-05:RED:body=-0.9300:wick=1.2700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.06 (current export asof; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.68 (current export; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1262.71 | **NEUTRAL** |
| `B04_income` | 135.16 | **GOOD** |
| `B05_profit_margin` | 10.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 34.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.9100000000000037 (now=34.59 vs prior_export=33.68 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | -0.54 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.13000000000000006 (now=-0.54 vs prior=-0.41 on finviz_2026-11-04) | **BAD** |
| `B12_institutional_transactions` | 4.06 | **GOOD** |
| `B13_short_float` | 11.66 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.06 (this export) | prior_export=8.04 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.68 (this export) | prior_export=1.1 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OLLI  ·  score **+15**  ·  Discount Stores
price=93.0  pair=`2026-11-04→2026-11-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.14 on 2026-11-05; prev RSI=64.44 on 2026-11-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.44@2026-11-04 → 62.14@2026-11-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.44@2026-11-04 → 62.14@2026-11-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.44@2026-11-04 → 62.14@2026-11-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_body_sum/RED_body_sum=4.380 (G=3.1100 R=0.7100); 2026-11-04:GREEN:O=91.0000,C=94.1100,body=+3.1100,vol=1711600.0; 2026-11-05:RED:O=93.7100,C=93.0000,body=-0.7100,vol=1275400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-04 + 2026-11-05; ratio=GREEN_vol/RED_vol=1.342 (Gvol=1711600 Rvol=1275400); 2026-11-04:GREEN:O=91.0000,C=94.1100,body=+3.1100,vol=1711600.0; 2026-11-05:RED:O=93.7100,C=93.0000,body=-0.7100,vol=1275400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.712 on 2026-11-05: today_vol=1275400 / avg20=1791880 (avg window 2026-09-22→2026-11-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.734 on 2026-11-05 (price=93.0000, mid=87.8765, upper=94.8563, lower=80.8967; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-05: price=93.0000 vs SMA50=80.4737 dist=+15.57% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-05: SMA20=87.8765 SMA50=80.4737 SMA80=76.8239 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-11-05 (63 bars); S1[2026-07-20→2026-08-20] low=2026-07-23@64.0000; S2[2026-08-21→2026-09-21] low=2026-09-03@70.0200; S3[2026-09-22→2026-11-05] low=2026-09-22@80.8050 | lows=[64.0, 70.0199966430664, 80.80500030517578] span=26.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: GREEN body_frac=0.6086106142632335 wick_frac=0.3913893857367665 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-04+2026-11-05: RED body_frac=0.13867182843382686 wick_frac=0.8613281715661731 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.380288198063636 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:GREEN:body=+4.6900:wick=0.4450; 2026-10-15:GREEN:body=+1.5050:wick=0.9850; 2026-11-03:RED:body=-0.1850:wick=2.5150; 2026-11-04:GREEN:body=+3.1100:wick=2.0000; 2026-11-05:RED:body=-0.7100:wick=4.4100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.54 (current export asof; earnings_date=9/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=-0.86 (current export; earnings_date=9/2/2026 8:30:00 AM) | **BAD** |
| `B03_sales` | 2793.11 | **NEUTRAL** |
| `B04_income` | 273.58 | **GOOD** |
| `B05_profit_margin` | 9.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 101.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.7399999999999949 (now=101.67 vs prior_export=99.93 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=4.07 (now=0.0 vs prior=-4.07 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 1.63 | **GOOD** |
| `B13_short_float` | 13.2 | **NEUTRAL** |
| `B14_earnings_date` | 9/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.54 (this export) | prior_export=26.54 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=-0.86 (this export) | prior_export=-0.86 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **BAD** |

CSV: `data/ab_checklist/2026-11-05_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.