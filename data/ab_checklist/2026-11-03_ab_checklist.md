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
| 1 | ZBRA | +16 | 18 | 2 | 2026-11-02→2026-11-03 | Communication Equipment |
| 2 | PAYS | +16 | 17 | 1 | 2026-11-02→2026-11-03 | Software - Infrastructure |
| 3 | WMG | +16 | 16 | 0 | 2026-11-02→2026-11-03 | Entertainment |
| 4 | HUM | +15 | 16 | 1 | 2026-11-02→2026-11-03 | Healthcare Plans |
| 5 | ICE | +15 | 17 | 2 | 2026-11-02→2026-11-03 | Financial Data & Stock Exchanges |
| 6 | ADSK | +15 | 16 | 1 | 2026-11-02→2026-11-03 | Software - Application |
| 7 | TME | +15 | 16 | 1 | 2026-11-02→2026-11-03 | Internet Content & Information |
| 8 | VFF | +14 | 16 | 2 | 2026-11-02→2026-11-03 | Farm Products |
| 9 | TPR | +14 | 16 | 2 | 2026-11-02→2026-11-03 | Luxury Goods |
| 10 | JKHY | +14 | 16 | 2 | 2026-11-02→2026-11-03 | Information Technology Services |
| 11 | CP | +14 | 15 | 1 | 2026-11-02→2026-11-03 | Railroads |
| 12 | FOX | +14 | 15 | 1 | 2026-11-02→2026-11-03 | Entertainment |
| 13 | BNL | +14 | 16 | 2 | 2026-11-02→2026-11-03 | REIT - Diversified |
| 14 | AS | +14 | 15 | 1 | 2026-11-02→2026-11-03 | Leisure |
| 15 | BR | +14 | 16 | 2 | 2026-11-02→2026-11-03 | Information Technology Services |

## Full checklist — top 15

### ZBRA  ·  score **+16**  ·  Communication Equipment
price=329.5  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=27.19 on 2026-11-03; prev RSI=40.05 on 2026-11-02 | **GOOD** |
| `A02_rsi_cross_30` | cross_down | RSI 40.05@2026-11-02 → 27.19@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 40.05@2026-11-02 → 27.19@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 40.05@2026-11-02 → 27.19@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=3.506 (G=17.8800 R=5.1000); 2026-11-02:RED:O=366.2300,C=361.1300,body=-5.1000,vol=1010118.0; 2026-11-03:GREEN:O=311.6200,C=329.5000,body=+17.8800,vol=2063100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=2.042 (Gvol=2063100 Rvol=1010118); 2026-11-02:RED:O=366.2300,C=361.1300,body=-5.1000,vol=1010118.0; 2026-11-03:GREEN:O=311.6200,C=329.5000,body=+17.8800,vol=2063100.0 | **GOOD** |
| `A07_rvol` | RVOL=3.916 on 2026-11-03: today_vol=2063100 / avg20=526854 (avg window 2026-10-06→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.715 on 2026-11-03 (price=329.5000, mid=377.7355, upper=405.8590, lower=349.6120; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-11-03: price=329.5000 vs SMA50=366.7130 dist=-10.15% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=377.7355 SMA50=366.7130 SMA80=349.3421 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-02@341.9700; S2[2026-09-04→2026-10-05] low=2026-09-10@336.4000; S3[2026-10-06→2026-11-03] low=2026-11-03@311.0700 | lows=[341.9700012207031, 336.3999938964844, 311.07000732421875] span=9.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.6180438786554119 wick_frac=0.3819561213445882 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.3539212218887856 wick_frac=0.6460787781112144 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5058791146322634 need>1.4; red_wick_gt_green=False 5d trail=2026-10-28:RED:body=-1.7600:wick=6.8000; 2026-10-29:RED:body=-7.5500:wick=7.1300; 2026-10-30:RED:body=-8.2450:wick=21.7650; 2026-11-02:RED:body=-5.1000:wick=9.3100; 2026-11-03:GREEN:body=+17.8800:wick=11.0500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=45.54 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.99 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5887.0 | **NEUTRAL** |
| `B04_income` | 539.0 | **GOOD** |
| `B05_profit_margin` | 9.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 419.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=419.23 vs prior_export=419.23 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.59 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.59 vs prior=-1.59 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.31 | **GOOD** |
| `B13_short_float` | 3.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=45.54 (this export) | prior_export=45.54 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.99 (this export) | prior_export=3.99 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAYS  ·  score **+16**  ·  Software - Infrastructure
price=15.130000114440918  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.35 on 2026-11-03; prev RSI=49.94 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.94@2026-11-02 → 56.35@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.94@2026-11-02 → 56.35@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.94@2026-11-02 → 56.35@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1200 R=0.0000); 2026-11-02:GREEN:O=13.9500,C=14.4000,body=+0.4500,vol=1541200.0; 2026-11-03:GREEN:O=14.4600,C=15.1300,body=+0.6700,vol=932800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2474000 Rvol=0); 2026-11-02:GREEN:O=13.9500,C=14.4000,body=+0.4500,vol=1541200.0; 2026-11-03:GREEN:O=14.4600,C=15.1300,body=+0.6700,vol=932800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.990 on 2026-11-03: today_vol=932800 / avg20=942384 (avg window 2026-09-29→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.189 on 2026-11-03 (price=15.1300, mid=14.9280, upper=15.9994, lower=13.8566; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=15.1300 vs SMA50=13.7582 dist=+9.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=14.9280 SMA50=13.7582 SMA80=12.3009 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-11-03 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-30@8.9200; S2[2026-08-28→2026-09-28] low=2026-09-16@11.3600; S3[2026-09-29→2026-11-03] low=2026-09-29@12.5600 | lows=[8.920000076293945, 11.359999656677246, 12.5600004196167] span=40.81% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.5850783649899434 wick_frac=0.41492163501005663 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.3200:wick=0.5500; 2026-10-22:RED:body=-0.2700:wick=0.6600; 2026-10-23:RED:body=-0.1200:wick=0.5890; 2026-11-02:GREEN:body=+0.4500:wick=0.2800; 2026-11-03:GREEN:body=+0.6700:wick=0.5400 | **GOOD** |
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

### WMG  ·  score **+16**  ·  Entertainment
price=31.1299991607666  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.23 on 2026-11-03; prev RSI=61.17 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.17@2026-11-02 → 62.23@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.17@2026-11-02 → 62.23@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.17@2026-11-02 → 62.23@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7700 R=0.0000); 2026-11-02:GREEN:O=30.6600,C=30.9700,body=+0.3100,vol=2053046.0; 2026-11-03:GREEN:O=30.6700,C=31.1300,body=+0.4600,vol=1451600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3504646 Rvol=0); 2026-11-02:GREEN:O=30.6600,C=30.9700,body=+0.3100,vol=2053046.0; 2026-11-03:GREEN:O=30.6700,C=31.1300,body=+0.4600,vol=1451600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.649 on 2026-11-03: today_vol=1451600 / avg20=2235101 (avg window 2026-10-06→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.414 on 2026-11-03 (price=31.1300, mid=30.2040, upper=32.4384, lower=27.9696; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=31.1300 vs SMA50=28.8444 dist=+7.92% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=30.2040 SMA50=28.8444 SMA80=28.1812 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-12@24.5150; S2[2026-09-04→2026-10-05] low=2026-09-28@26.4500; S3[2026-10-06→2026-11-03] low=2026-10-07@27.2300 | lows=[24.514999389648438, 26.450000762939453, 27.229999542236328] span=11.07% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.5688701835958784 wick_frac=0.4311298164041217 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:RED:body=-0.4300:wick=0.4700; 2026-10-29:GREEN:body=+0.9000:wick=0.4850; 2026-10-30:RED:body=-0.1800:wick=0.5647; 2026-11-02:GREEN:body=+0.3100:wick=0.4400; 2026-11-03:GREEN:body=+0.4600:wick=0.1750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+15**  ·  Healthcare Plans
price=417.8699951171875  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.95 on 2026-11-03; prev RSI=49.78 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.78@2026-11-02 → 53.95@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.78@2026-11-02 → 53.95@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.78@2026-11-02 → 53.95@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=0.978 (G=10.0000 R=10.2300); 2026-11-02:RED:O=420.6400,C=410.4100,body=-10.2300,vol=916903.0; 2026-11-03:GREEN:O=407.8700,C=417.8700,body=+10.0000,vol=1368400.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.492 (Gvol=1368400 Rvol=916903); 2026-11-02:RED:O=420.6400,C=410.4100,body=-10.2300,vol=916903.0; 2026-11-03:GREEN:O=407.8700,C=417.8700,body=+10.0000,vol=1368400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.861 on 2026-11-03: today_vol=1368400 / avg20=1589494 (avg window 2026-10-06→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.000 on 2026-11-03 (price=417.8700, mid=417.8620, upper=440.6307, lower=395.0934; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=417.8700 vs SMA50=402.2824 dist=+3.87% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=417.8620 SMA50=402.2824 SMA80=395.7465 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@355.7100; S2[2026-09-04→2026-10-05] low=2026-09-23@369.1300; S3[2026-10-06→2026-11-03] low=2026-10-08@375.8600 | lows=[355.7099914550781, 369.1300048828125, 375.8599853515625] span=5.66% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.5813959677719838 wick_frac=0.41860403222801623 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.4805076035898634 wick_frac=0.5194923964101366 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=0.977516056763231 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:RED:body=-4.1300:wick=6.8700; 2026-10-29:GREEN:body=+1.4500:wick=8.3200; 2026-10-30:RED:body=-1.1600:wick=9.6800; 2026-11-02:RED:body=-10.2300:wick=11.0600; 2026-11-03:GREEN:body=+10.0000:wick=7.2000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 425.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=425.46 vs prior_export=425.46 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 2.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICE  ·  score **+15**  ·  Financial Data & Stock Exchanges
price=170.92999267578125  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.10 on 2026-11-03; prev RSI=68.66 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.66@2026-11-02 → 66.10@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.66@2026-11-02 → 66.10@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.66@2026-11-02 → 66.10@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=15.833 (G=3.8000 R=0.2400); 2026-11-02:GREEN:O=168.4800,C=172.2800,body=+3.8000,vol=3415287.0; 2026-11-03:RED:O=171.1700,C=170.9300,body=-0.2400,vol=2437300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.401 (Gvol=3415287 Rvol=2437300); 2026-11-02:GREEN:O=168.4800,C=172.2800,body=+3.8000,vol=3415287.0; 2026-11-03:RED:O=171.1700,C=170.9300,body=-0.2400,vol=2437300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.739 on 2026-11-03: today_vol=2437300 / avg20=3297977 (avg window 2026-10-06→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.997 on 2026-11-03 (price=170.9300, mid=160.0420, upper=170.9627, lower=149.1213; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=170.9300 vs SMA50=157.9476 dist=+8.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=160.0420 SMA50=157.9476 SMA80=154.8597 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@147.4700; S2[2026-09-04→2026-10-05] low=2026-10-05@147.5100; S3[2026-10-06→2026-11-03] low=2026-10-07@151.2300 | lows=[147.47000122070312, 147.50999450683594, 151.22999572753906] span=2.55% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.7900193192885172 wick_frac=0.20998068071148276 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.07547323720640099 wick_frac=0.924526762793599 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.83298366075402 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:GREEN:body=+1.1300:wick=1.1000; 2026-10-29:GREEN:body=+2.1600:wick=3.6600; 2026-10-30:GREEN:body=+2.1700:wick=3.1900; 2026-11-02:GREEN:body=+3.8000:wick=1.0100; 2026-11-03:RED:body=-0.2400:wick=2.9400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.49 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.33 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13138.0 | **NEUTRAL** |
| `B04_income` | 4038.0 | **GOOD** |
| `B05_profit_margin` | 30.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 186.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=186.86 vs prior_export=186.86 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | -2.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.02 vs prior=-2.02 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.15 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.49 (this export) | prior_export=3.49 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.33 (this export) | prior_export=1.33 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+15**  ·  Software - Application
price=252.32000732421875  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.27 on 2026-11-03; prev RSI=65.95 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.95@2026-11-02 → 60.27@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.95@2026-11-02 → 60.27@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.95@2026-11-02 → 60.27@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=1.731 (G=7.7900 R=4.5000); 2026-11-02:GREEN:O=250.1200,C=257.9100,body=+7.7900,vol=2022139.0; 2026-11-03:RED:O=256.8200,C=252.3200,body=-4.5000,vol=2764900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=0.731 (Gvol=2022139 Rvol=2764900); 2026-11-02:GREEN:O=250.1200,C=257.9100,body=+7.7900,vol=2022139.0; 2026-11-03:RED:O=256.8200,C=252.3200,body=-4.5000,vol=2764900.0 | **BAD** |
| `A07_rvol` | RVOL=1.628 on 2026-11-03: today_vol=2764900 / avg20=1698155 (avg window 2026-10-06→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.580 on 2026-11-03 (price=252.3200, mid=242.8970, upper=259.1344, lower=226.6596; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=252.3200 vs SMA50=232.4918 dist=+8.53% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=242.90_50=232.49_80=233.12 on 2026-11-03: SMA20=242.8970 SMA50=232.4918 SMA80=233.1219 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@235.2600; S2[2026-09-04→2026-10-05] low=2026-09-29@199.1801; S3[2026-10-06→2026-11-03] low=2026-10-06@221.7050 | lows=[235.25999450683594, 199.1800994873047, 221.7050018310547] span=18.11% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.588133103698541 wick_frac=0.411866896301459 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.45500508369191744 wick_frac=0.5449949163080825 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7311130099826388 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:GREEN:body=+2.7000:wick=3.3900; 2026-10-29:GREEN:body=+3.0900:wick=4.6300; 2026-10-30:GREEN:body=+4.2500:wick=3.5800; 2026-11-02:GREEN:body=+7.7900:wick=5.4553; 2026-11-03:RED:body=-4.5000:wick=5.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TME  ·  score **+15**  ·  Internet Content & Information
price=9.039999961853027  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.07 on 2026-11-03; prev RSI=65.48 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.48@2026-11-02 → 68.07@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.48@2026-11-02 → 68.07@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.48@2026-11-02 → 68.07@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4300 R=0.0000); 2026-11-02:GREEN:O=8.6000,C=8.9100,body=+0.3100,vol=9726853.0; 2026-11-03:GREEN:O=8.9200,C=9.0400,body=+0.1200,vol=7640600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=17367453 Rvol=0); 2026-11-02:GREEN:O=8.6000,C=8.9100,body=+0.3100,vol=9726853.0; 2026-11-03:GREEN:O=8.9200,C=9.0400,body=+0.1200,vol=7640600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.538 on 2026-11-03: today_vol=7640600 / avg20=4968908 (avg window 2026-10-06→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.103 on 2026-11-03 (price=9.0400, mid=8.4930, upper=8.9891, lower=7.9969; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=9.0400 vs SMA50=8.2836 dist=+9.13% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.49_50=8.28_80=8.59 on 2026-11-03: SMA20=8.4930 SMA50=8.2836 SMA80=8.5879 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-01@8.1300; S2[2026-09-04→2026-10-05] low=2026-10-02@7.6600; S3[2026-10-06→2026-11-03] low=2026-10-06@7.7700 | lows=[8.130000114440918, 7.659999847412109, 7.769999980926514] span=6.14% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.6938381398432361 wick_frac=0.3061618601567639 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:RED:body=-0.0900:wick=0.1900; 2026-10-29:GREEN:body=+0.0900:wick=0.0700; 2026-10-30:RED:body=-0.0900:wick=0.0800; 2026-11-02:GREEN:body=+0.3100:wick=0.1000; 2026-11-03:GREEN:body=+0.1200:wick=0.0700 | **GOOD** |
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

### VFF  ·  score **+14**  ·  Farm Products
price=2.8499999046325684  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.08 on 2026-11-03; prev RSI=46.58 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.58@2026-11-02 → 59.08@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.58@2026-11-02 → 59.08@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.58@2026-11-02 → 59.08@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.2300 R=0.0000); 2026-11-02:GREEN:O=2.5500,C=2.6100,body=+0.0600,vol=650779.0; 2026-11-03:GREEN:O=2.6800,C=2.8500,body=+0.1700,vol=1365200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2015979 Rvol=0); 2026-11-02:GREEN:O=2.5500,C=2.6100,body=+0.0600,vol=650779.0; 2026-11-03:GREEN:O=2.6800,C=2.8500,body=+0.1700,vol=1365200.0 | **GOOD** |
| `A07_rvol` | RVOL=2.146 on 2026-11-03: today_vol=1365200 / avg20=636036 (avg window 2026-10-06→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.315 on 2026-11-03 (price=2.8500, mid=2.6255, upper=2.7962, lower=2.4548; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=2.8500 vs SMA50=2.7982 dist=+1.85% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=2.63_50=2.80_80=2.57 on 2026-11-03: SMA20=2.6255 SMA50=2.7982 SMA80=2.5721 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@1.9300; S2[2026-09-04→2026-10-05] low=2026-10-05@2.6900; S3[2026-10-06→2026-11-03] low=2026-10-30@2.4500 | lows=[1.9299999475479126, 2.690000057220459, 2.450000047683716] span=39.38% rising_lows=False flatish(≤12%)=False | **BAD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.536129180672096 wick_frac=0.4638708193279039 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-28:DOJI:body=+0.0000:wick=0.0501; 2026-10-29:GREEN:body=+0.0100:wick=0.0350; 2026-10-30:RED:body=-0.0600:wick=0.0550; 2026-11-02:GREEN:body=+0.0600:wick=0.0690; 2026-11-03:GREEN:body=+0.1700:wick=0.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=294.74 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=13.63 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 230.57 | **NEUTRAL** |
| `B04_income` | 22.71 | **GOOD** |
| `B05_profit_margin` | 9.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.83 vs prior_export=5.83 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.67 vs prior=0.67 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.12 | **GOOD** |
| `B13_short_float` | 4.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=294.74 (this export) | prior_export=294.74 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=13.63 (this export) | prior_export=13.63 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TPR  ·  score **+14**  ·  Luxury Goods
price=121.0199966430664  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.54 on 2026-11-03; prev RSI=58.14 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.14@2026-11-02 → 58.54@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.14@2026-11-02 → 58.54@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.14@2026-11-02 → 58.54@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=8.743 (G=3.0600 R=0.3500); 2026-11-02:GREEN:O=117.7000,C=120.7600,body=+3.0600,vol=2955231.0; 2026-11-03:RED:O=121.3700,C=121.0200,body=-0.3500,vol=2952400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.001 (Gvol=2955231 Rvol=2952400); 2026-11-02:GREEN:O=117.7000,C=120.7600,body=+3.0600,vol=2955231.0; 2026-11-03:RED:O=121.3700,C=121.0200,body=-0.3500,vol=2952400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.259 on 2026-11-03: today_vol=2952400 / avg20=2345407 (avg window 2026-10-06→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.788 on 2026-11-03 (price=121.0200, mid=115.8520, upper=122.4081, lower=109.2959; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=121.0200 vs SMA50=117.3008 dist=+3.17% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-03: SMA20=115.8520 SMA50=117.3008 SMA80=127.2959 | **BAD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-01@120.3100; S2[2026-09-04→2026-10-05] low=2026-09-22@109.7900; S3[2026-10-06→2026-11-03] low=2026-10-27@109.1250 | lows=[120.30999755859375, 109.79000091552734, 109.125] span=10.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.7183107167866296 wick_frac=0.28168928321337044 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.12727494673295456 wick_frac=0.8727250532670454 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.74271950475194 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:GREEN:body=+0.2300:wick=1.1950; 2026-10-29:GREEN:body=+4.5900:wick=0.5000; 2026-10-30:RED:body=-3.3900:wick=0.3500; 2026-11-02:GREEN:body=+3.0600:wick=1.2000; 2026-11-03:RED:body=-0.3500:wick=2.4000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.34 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.18 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8004.2 | **NEUTRAL** |
| `B04_income` | 1527.7 | **GOOD** |
| `B05_profit_margin` | 19.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 172.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=172.61 vs prior_export=172.61 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.76 | **GOOD** |
| `B10_insider_transactions` | -6.33 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.33 vs prior=-6.33 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.2 | **GOOD** |
| `B13_short_float` | 8.06 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.34 (this export) | prior_export=3.34 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.18 (this export) | prior_export=0.18 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### JKHY  ·  score **+14**  ·  Information Technology Services
price=161.66000366210938  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.39 on 2026-11-03; prev RSI=66.58 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.58@2026-11-02 → 63.39@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.58@2026-11-02 → 63.39@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.58@2026-11-02 → 63.39@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=16.501 (G=1.9800 R=0.1200); 2026-11-02:GREEN:O=161.4400,C=163.4200,body=+1.9800,vol=1329500.0; 2026-11-03:RED:O=161.7800,C=161.6600,body=-0.1200,vol=1010800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.315 (Gvol=1329500 Rvol=1010800); 2026-11-02:GREEN:O=161.4400,C=163.4200,body=+1.9800,vol=1329500.0; 2026-11-03:RED:O=161.7800,C=161.6600,body=-0.1200,vol=1010800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.000 on 2026-11-03: today_vol=1010800 / avg20=1010827 (avg window 2026-09-16→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.075 on 2026-11-03 (price=161.6600, mid=149.3780, upper=160.7996, lower=137.9564; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=161.6600 vs SMA50=156.6742 dist=+3.18% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=149.38_50=156.67_80=152.13 on 2026-11-03: SMA20=149.3780 SMA50=156.6742 SMA80=152.1296 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-11-03 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@144.3800; S2[2026-08-17→2026-09-15] low=2026-08-17@149.1900; S3[2026-09-16→2026-11-03] low=2026-10-02@141.7400 | lows=[144.3800048828125, 149.19000244140625, 141.74000549316406] span=5.26% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.5210510887940346 wick_frac=0.4789489112059654 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.030225461030525255 wick_frac=0.9697745389694747 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=16.50063580874873 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+3.3100:wick=0.4200; 2026-10-09:RED:body=-2.2900:wick=1.8750; 2026-10-12:RED:body=-0.5000:wick=2.1050; 2026-11-02:GREEN:body=+1.9800:wick=1.8200; 2026-11-03:RED:body=-0.1200:wick=3.8500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.0 (current export asof; earnings_date=8/18/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.97 (current export; earnings_date=8/18/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2533.41 | **NEUTRAL** |
| `B04_income` | 502.78 | **GOOD** |
| `B05_profit_margin` | 19.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 188.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=188.61 vs prior_export=188.61 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | 0.56 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.56 vs prior=0.56 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -5.25 | **BAD** |
| `B13_short_float` | 6.13 | **NEUTRAL** |
| `B14_earnings_date` | 8/18/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.0 (this export) | prior_export=7.0 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.97 (this export) | prior_export=1.97 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+14**  ·  Railroads
price=91.2300033569336  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.04 on 2026-11-03; prev RSI=63.08 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.08@2026-11-02 → 62.04@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.08@2026-11-02 → 62.04@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.08@2026-11-02 → 62.04@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2600 R=0.0000); 2026-11-02:GREEN:O=89.5900,C=91.4900,body=+1.9000,vol=2767500.0; 2026-11-03:GREEN:O=90.8700,C=91.2300,body=+0.3600,vol=1694500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4462000 Rvol=0); 2026-11-02:GREEN:O=89.5900,C=91.4900,body=+1.9000,vol=2767500.0; 2026-11-03:GREEN:O=90.8700,C=91.2300,body=+0.3600,vol=1694500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.739 on 2026-11-03: today_vol=1694500 / avg20=2292908 (avg window 2026-09-16→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.071 on 2026-11-03 (price=91.2300, mid=86.7035, upper=90.9318, lower=82.4752; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=91.2300 vs SMA50=89.6488 dist=+1.76% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.70_50=89.65_80=89.45 on 2026-11-03: SMA20=86.7035 SMA50=89.6488 SMA80=89.4469 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-11-03 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-30@87.2000; S2[2026-08-12→2026-09-15] low=2026-09-14@88.0400; S3[2026-09-16→2026-11-03] low=2026-10-08@83.2500 | lows=[87.19999694824219, 88.04000091552734, 83.25] span=5.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.54825615043212 wick_frac=0.45174384956788 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+1.0800:wick=0.4100; 2026-10-09:RED:body=-0.0100:wick=0.7600; 2026-10-12:GREEN:body=+1.1300:wick=0.3200; 2026-11-02:GREEN:body=+1.9000:wick=0.4500; 2026-11-03:GREEN:body=+0.3600:wick=0.8900 | **GOOD** |
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

### FOX  ·  score **+14**  ·  Entertainment
price=59.630001068115234  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.50 on 2026-11-03; prev RSI=45.04 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.04@2026-11-02 → 60.50@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.04@2026-11-02 → 60.50@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.04@2026-11-02 → 60.50@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=1.327 (G=2.6400 R=1.9900); 2026-11-02:RED:O=57.9500,C=55.9600,body=-1.9900,vol=1193300.0; 2026-11-03:GREEN:O=56.9900,C=59.6300,body=+2.6400,vol=1356600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.137 (Gvol=1356600 Rvol=1193300); 2026-11-02:RED:O=57.9500,C=55.9600,body=-1.9900,vol=1193300.0; 2026-11-03:GREEN:O=56.9900,C=59.6300,body=+2.6400,vol=1356600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.172 on 2026-11-03: today_vol=1356600 / avg20=1157187 (avg window 2026-09-16→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.321 on 2026-11-03 (price=59.6300, mid=56.7135, upper=58.9214, lower=54.5056; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-03: price=59.6300 vs SMA50=58.1950 dist=+2.47% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=56.71_50=58.20_80=54.75 on 2026-11-03: SMA20=56.7135 SMA50=58.1950 SMA80=54.7533 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-11-03 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@48.5900; S2[2026-08-17→2026-09-15] low=2026-09-10@56.2100; S3[2026-09-16→2026-11-03] low=2026-10-01@53.9000 | lows=[48.59000015258789, 56.209999084472656, 53.900001525878906] span=15.68% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.5593217599327579 wick_frac=0.4406782400672421 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.47268458698870813 wick_frac=0.5273154130112919 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.326631740171412 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.6200:wick=0.7900; 2026-10-09:RED:body=-0.8000:wick=0.5350; 2026-10-12:GREEN:body=+0.0900:wick=0.7500; 2026-11-02:RED:body=-1.9900:wick=2.2200; 2026-11-03:GREEN:body=+2.6400:wick=2.0800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=24.57 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.71 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17126.0 | **NEUTRAL** |
| `B04_income` | 1685.0 | **GOOD** |
| `B05_profit_margin` | 9.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 77.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=77.94 vs prior_export=77.94 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.69 | **GOOD** |
| `B13_short_float` | 7.93 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.57 (this export) | prior_export=24.57 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.71 (this export) | prior_export=15.71 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BNL  ·  score **+14**  ·  REIT - Diversified
price=17.979999542236328  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=33.32 on 2026-11-03; prev RSI=24.17 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | cross_up | RSI 24.17@2026-11-02 → 33.32@2026-11-03 vs 30 | rule: cross_up=GOOD | **GOOD** |
| `A03_rsi_cross_50` | below | RSI 24.17@2026-11-02 → 33.32@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 24.17@2026-11-02 → 33.32@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8100 R=0.0000); 2026-11-02:GREEN:O=17.2400,C=17.5900,body=+0.3500,vol=3804900.0; 2026-11-03:GREEN:O=17.5200,C=17.9800,body=+0.4600,vol=5346100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9151000 Rvol=0); 2026-11-02:GREEN:O=17.2400,C=17.5900,body=+0.3500,vol=3804900.0; 2026-11-03:GREEN:O=17.5200,C=17.9800,body=+0.4600,vol=5346100.0 | **GOOD** |
| `A07_rvol` | RVOL=2.464 on 2026-11-03: today_vol=5346100 / avg20=2169415 (avg window 2026-09-16→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.709 on 2026-11-03 (price=17.9800, mid=18.6185, upper=19.5186, lower=17.7184; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-11-03: price=17.9800 vs SMA50=19.9788 dist=-10.00% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-03: SMA20=18.6185 SMA50=19.9788 SMA80=20.5483 | **BAD** |
| `A11_three_section_lows` | window=2026-07-14→2026-11-03 (63 bars); S1[2026-07-14→2026-08-14] low=2026-08-11@20.3600; S2[2026-08-17→2026-09-15] low=2026-09-15@19.9200; S3[2026-09-16→2026-11-03] low=2026-11-02@17.0040 | lows=[20.360000610351562, 19.920000076293945, 17.003999710083008] span=19.74% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.7132045890889782 wick_frac=0.28679541091102184 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.0900:wick=0.3550; 2026-10-09:RED:body=-0.0600:wick=0.1550; 2026-10-12:GREEN:body=+0.2200:wick=0.0500; 2026-11-02:GREEN:body=+0.3500:wick=0.2960; 2026-11-03:GREEN:body=+0.4600:wick=0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.66 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.56 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 477.57 | **NEUTRAL** |
| `B04_income` | 140.69 | **GOOD** |
| `B05_profit_margin` | 29.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.7 vs prior_export=23.7 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | 0.46 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.46 vs prior=0.46 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.08 | **GOOD** |
| `B13_short_float` | 10.62 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.66 (this export) | prior_export=9.66 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.56 (this export) | prior_export=0.56 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AS  ·  score **+14**  ·  Leisure
price=30.489999771118164  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.26 on 2026-11-03; prev RSI=62.90 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.90@2026-11-02 → 61.26@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.90@2026-11-02 → 61.26@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.90@2026-11-02 → 61.26@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=3.250 (G=0.9100 R=0.2800); 2026-11-02:GREEN:O=29.7600,C=30.6700,body=+0.9100,vol=5193304.0; 2026-11-03:RED:O=30.7700,C=30.4900,body=-0.2800,vol=4799900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=1.082 (Gvol=5193304 Rvol=4799900); 2026-11-02:GREEN:O=29.7600,C=30.6700,body=+0.9100,vol=5193304.0; 2026-11-03:RED:O=30.7700,C=30.4900,body=-0.2800,vol=4799900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.883 on 2026-11-03: today_vol=4799900 / avg20=5437999 (avg window 2026-10-06→2026-11-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.696 on 2026-11-03 (price=30.4900, mid=29.0600, upper=31.1134, lower=27.0066; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=30.4900 vs SMA50=28.5292 dist=+6.87% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=29.06_50=28.53_80=30.83 on 2026-11-03: SMA20=29.0600 SMA50=28.5292 SMA80=30.8289 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-09-03@28.0950; S2[2026-09-04→2026-10-05] low=2026-09-18@26.1000; S3[2026-10-06→2026-11-03] low=2026-10-08@26.7400 | lows=[28.094999313354492, 26.100000381469727, 26.739999771118164] span=7.64% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.6232871433031859 wick_frac=0.3767128566968141 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=0.4666675143766312 wick_frac=0.5333324856233688 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2499914850716274 need>1.4; red_wick_gt_green=False 5d trail=2026-10-28:DOJI:body=+0.0000:wick=0.6650; 2026-10-29:GREEN:body=+0.7300:wick=0.3300; 2026-10-30:RED:body=-0.0700:wick=0.4794; 2026-11-02:GREEN:body=+0.9100:wick=0.5500; 2026-11-03:RED:body=-0.2800:wick=0.3200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=108.33 (current export asof; earnings_date=8/18/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.95 (current export; earnings_date=8/18/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7435.5 | **NEUTRAL** |
| `B04_income` | 546.4 | **GOOD** |
| `B05_profit_margin` | 7.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 47.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=47.64 vs prior_export=47.64 on finviz_2026-11-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | -0.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.24 vs prior=-0.24 on finviz_2026-11-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.41 | **GOOD** |
| `B13_short_float` | 7.31 | **NEUTRAL** |
| `B14_earnings_date` | 8/18/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=108.33 (this export) | prior_export=108.33 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.95 (this export) | prior_export=5.95 (finviz_2026-11-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BR  ·  score **+14**  ·  Information Technology Services
price=183.75999450683594  pair=`2026-11-02→2026-11-03`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.24 on 2026-11-03; prev RSI=66.19 on 2026-11-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.19@2026-11-02 → 65.24@2026-11-03 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.19@2026-11-02 → 65.24@2026-11-03 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.19@2026-11-02 → 65.24@2026-11-03 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.7000 R=0.0000); 2026-11-02:GREEN:O=178.8600,C=184.2200,body=+5.3600,vol=1051632.0; 2026-11-03:GREEN:O=180.4200,C=183.7600,body=+3.3400,vol=1793200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-02 + 2026-11-03; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2844832 Rvol=0); 2026-11-02:GREEN:O=178.8600,C=184.2200,body=+5.3600,vol=1051632.0; 2026-11-03:GREEN:O=180.4200,C=183.7600,body=+3.3400,vol=1793200.0 | **GOOD** |
| `A07_rvol` | RVOL=2.290 on 2026-11-03: today_vol=1793200 / avg20=783216 (avg window 2026-10-06→2026-11-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.575 on 2026-11-03 (price=183.7600, mid=175.3010, upper=190.0191, lower=160.5829; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-03: price=183.7600 vs SMA50=171.6392 dist=+7.06% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-03: SMA20=175.3010 SMA50=171.6392 SMA80=167.8670 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-06→2026-11-03 (63 bars); S1[2026-08-06→2026-09-03] low=2026-08-06@160.0000; S2[2026-09-04→2026-10-05] low=2026-10-05@155.6800; S3[2026-10-06→2026-11-03] low=2026-10-06@157.5200 | lows=[160.0, 155.67999267578125, 157.52000427246094] span=2.77% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: GREEN body_frac=0.7178397590408402 wick_frac=0.28216024095915976 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-02+2026-11-03: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-28:GREEN:body=+1.8000:wick=1.0800; 2026-10-29:GREEN:body=+0.9500:wick=1.7500; 2026-10-30:RED:body=-0.3600:wick=6.1300; 2026-11-02:GREEN:body=+5.3600:wick=0.2200; 2026-11-03:GREEN:body=+3.3400:wick=3.6900 | **GOOD** |
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

CSV: `data/ab_checklist/2026-11-03_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.