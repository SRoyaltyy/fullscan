# A+B1 Feature Checklist — 2026-09-12

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,653** names
- Export: `finviz_2026-09-12.csv` · prior export for Δ: `2026-09-11`
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
| 1 | CFFN | +17 | 17 | 0 | 2026-09-04→2026-09-08 | Banks - Regional |
| 2 | ANET | +17 | 18 | 1 | 2026-09-10→2026-09-11 | Computer Hardware |
| 3 | WWW | +17 | 18 | 1 | 2026-09-10→2026-09-11 | Footwear & Accessories |
| 4 | HSBC | +16 | 17 | 1 | 2026-09-04→2026-09-08 | Banks - Diversified |
| 5 | SKM | +16 | 16 | 0 | 2026-09-04→2026-09-08 | Telecom Services |
| 6 | WDS | +16 | 16 | 0 | 2026-09-10→2026-09-11 | Oil & Gas E&P |
| 7 | BMRN | +16 | 17 | 1 | 2026-09-10→2026-09-11 | Biotechnology |
| 8 | DSGX | +16 | 17 | 1 | 2026-09-10→2026-09-11 | Software - Application |
| 9 | CXW | +16 | 17 | 1 | 2026-09-10→2026-09-11 | Security & Protection Services |
| 10 | ETN | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Specialty Industrial Machinery |
| 11 | CDNA | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Diagnostics & Research |
| 12 | ALB | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Specialty Chemicals |
| 13 | HAS | +15 | 16 | 1 | 2026-09-10→2026-09-11 | Leisure |
| 14 | SLDE | +15 | 16 | 1 | 2026-09-10→2026-09-11 | Insurance - Property & Casualty |
| 15 | SM | +15 | 16 | 1 | 2026-09-10→2026-09-11 | Oil & Gas E&P |

## Full checklist — top 15

### CFFN  ·  score **+17**  ·  Banks - Regional
price=8.800000190734863  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.51 on 2026-09-08; prev RSI=61.88 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.88@2026-09-04 → 57.51@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.88@2026-09-04 → 57.51@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.88@2026-09-04 → 57.51@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=2.500 (G=0.1000 R=0.0400); 2026-09-04:GREEN:O=8.7700,C=8.8700,body=+0.1000,vol=1091800.0; 2026-09-08:RED:O=8.8400,C=8.8000,body=-0.0400,vol=695300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.570 (Gvol=1091800 Rvol=695300); 2026-09-04:GREEN:O=8.7700,C=8.8700,body=+0.1000,vol=1091800.0; 2026-09-08:RED:O=8.8400,C=8.8000,body=-0.0400,vol=695300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.905 on 2026-09-08: today_vol=695300 / avg20=768625 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.490 on 2026-09-08 (price=8.8000, mid=8.6740, upper=8.9309, lower=8.4171; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=8.8000 vs SMA50=8.6087 dist=+2.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=8.6740 SMA50=8.6087 SMA80=8.3024 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-04@7.6371; S2[2026-07-07→2026-08-07] low=2026-07-08@8.1126; S3[2026-08-10→2026-09-08] low=2026-08-21@8.3900 | lows=[7.637102193559722, 8.112563409381613, 8.390000343322754] span=9.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.769225690496277 wick_frac=0.230774309503723 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.44444797660298185 wick_frac=0.5555520233970181 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4999880790596762 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.0300:wick=0.1400; 2026-09-02:GREEN:body=+0.0700:wick=0.1100; 2026-09-03:GREEN:body=+0.0600:wick=0.1000; 2026-09-04:GREEN:body=+0.1000:wick=0.0300; 2026-09-08:RED:body=-0.0400:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.76 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 443.11 | **NEUTRAL** |
| `B04_income` | 82.73 | **GOOD** |
| `B05_profit_margin` | 18.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=9.5 vs prior_export=9.5 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 5.67 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.76 (this export) | prior_export=11.76 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+17**  ·  Computer Hardware
price=199.58999633789062  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.06 on 2026-09-11; prev RSI=48.84 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.84@2026-09-10 → 57.06@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.84@2026-09-10 → 57.06@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.84@2026-09-10 → 57.06@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=20.444 (G=7.3600 R=0.3600); 2026-09-10:RED:O=189.3500,C=188.9900,body=-0.3600,vol=2810900.0; 2026-09-11:GREEN:O=192.2300,C=199.5900,body=+7.3600,vol=4638000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.650 (Gvol=4638000 Rvol=2810900); 2026-09-10:RED:O=189.3500,C=188.9900,body=-0.3600,vol=2810900.0; 2026-09-11:GREEN:O=192.2300,C=199.5900,body=+7.3600,vol=4638000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.895 on 2026-09-11: today_vol=4638000 / avg20=5183020 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.583 on 2026-09-11 (price=199.5900, mid=193.1600, upper=204.1900, lower=182.1300; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=199.5900 vs SMA50=185.2968 dist=+7.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=193.1600 SMA50=185.2968 SMA80=176.1383 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-12→2026-09-11 (63 bars); S1[2026-06-12→2026-07-14] low=2026-06-26@154.7400; S2[2026-07-15→2026-08-12] low=2026-07-29@156.8400; S3[2026-08-13→2026-09-11] low=2026-09-03@181.2700 | lows=[154.74000549316406, 156.83999633789062, 181.27000427246094] span=17.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.9036214548390662 wick_frac=0.09637854516093378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.06994393930859469 wick_frac=0.9300560606914053 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.444411477980758 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+0.9600:wick=5.5800; 2026-09-08:RED:body=-1.8600:wick=4.0950; 2026-09-09:RED:body=-1.9250:wick=2.9850; 2026-09-10:RED:body=-0.3600:wick=4.7870; 2026-09-11:GREEN:body=+7.3600:wick=0.7850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 248.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=248.86 vs prior_export=248.86 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -3.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.11 vs prior=-3.11 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.66 | **GOOD** |
| `B13_short_float` | 1.38 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WWW  ·  score **+17**  ·  Footwear & Accessories
price=19.93000030517578  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.82 on 2026-09-11; prev RSI=47.20 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.20@2026-09-10 → 51.82@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.20@2026-09-10 → 51.82@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.20@2026-09-10 → 51.82@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=1.556 (G=0.2800 R=0.1800); 2026-09-10:RED:O=19.4700,C=19.2900,body=-0.1800,vol=481700.0; 2026-09-11:GREEN:O=19.6500,C=19.9300,body=+0.2800,vol=646300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.342 (Gvol=646300 Rvol=481700); 2026-09-10:RED:O=19.4700,C=19.2900,body=-0.1800,vol=481700.0; 2026-09-11:GREEN:O=19.6500,C=19.9300,body=+0.2800,vol=646300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.806 on 2026-09-11: today_vol=646300 / avg20=802335 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.154 on 2026-09-11 (price=19.9300, mid=20.1080, upper=21.2655, lower=18.9505; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=19.9300 vs SMA50=19.1154 dist=+4.26% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=20.1080 SMA50=19.1154 SMA80=18.2217 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-10→2026-09-11 (63 bars); S1[2026-06-10→2026-07-10] low=2026-06-30@15.9628; S2[2026-07-13→2026-08-12] low=2026-07-14@17.4500; S3[2026-08-13→2026-09-11] low=2026-08-13@18.9800 | lows=[15.962842417770247, 17.450000762939453, 18.979999542236328] span=18.90% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.5000017029914748 wick_frac=0.49999829700852516 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.4285675360239054 wick_frac=0.5714324639760946 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5555732163482425 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+1.2600:wick=0.2000; 2026-09-08:RED:body=-0.7200:wick=0.2400; 2026-09-09:RED:body=-0.3100:wick=0.3500; 2026-09-10:RED:body=-0.1800:wick=0.2400; 2026-09-11:GREEN:body=+0.2800:wick=0.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.99 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.13 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1951.8 | **NEUTRAL** |
| `B04_income` | 105.5 | **GOOD** |
| `B05_profit_margin` | 5.41 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.3 vs prior_export=24.3 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -3.42 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.42 vs prior=-3.42 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.99 (this export) | prior_export=4.99 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.13 (this export) | prior_export=1.13 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSBC  ·  score **+16**  ·  Banks - Diversified
price=106.26000213623047  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.53 on 2026-09-08; prev RSI=67.33 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.33@2026-09-04 → 62.53@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.33@2026-09-04 → 62.53@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.33@2026-09-04 → 62.53@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.455 (G=0.6400 R=0.4400); 2026-09-04:GREEN:O=106.4700,C=107.1100,body=+0.6400,vol=1208700.0; 2026-09-08:RED:O=106.7000,C=106.2600,body=-0.4400,vol=827900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.460 (Gvol=1208700 Rvol=827900); 2026-09-04:GREEN:O=106.4700,C=107.1100,body=+0.6400,vol=1208700.0; 2026-09-08:RED:O=106.7000,C=106.2600,body=-0.4400,vol=827900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.806 on 2026-09-08: today_vol=827900 / avg20=1026770 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.871 on 2026-09-08 (price=106.2600, mid=103.9622, upper=106.6005, lower=101.3239; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-08: price=106.2600 vs SMA50=101.3244 dist=+4.87% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=103.9622 SMA50=101.3244 SMA80=97.7063 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-10@85.7062; S2[2026-07-07→2026-08-07] low=2026-07-08@94.7226; S3[2026-08-10→2026-09-08] low=2026-08-19@102.0400 | lows=[85.70616298395647, 94.72262694022997, 102.04000091552734] span=19.06% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.6037743997236138 wick_frac=0.39622560027638626 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.4835088366477749 wick_frac=0.5164911633522251 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4545612179431604 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.3100:wick=0.9000; 2026-09-02:GREEN:body=+0.9300:wick=0.1500; 2026-09-03:GREEN:body=+0.4800:wick=0.3900; 2026-09-04:GREEN:body=+0.6400:wick=0.4200; 2026-09-08:RED:body=-0.4400:wick=0.4700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.31 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.09 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 135929.32 | **NEUTRAL** |
| `B04_income` | 24184.21 | **GOOD** |
| `B05_profit_margin` | 17.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 108.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=108.62 vs prior_export=108.62 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.21 | **GOOD** |
| `B13_short_float` | 0.15 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.31 (this export) | prior_export=3.31 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+16**  ·  Telecom Services
price=38.41999816894531  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.36 on 2026-09-08; prev RSI=56.77 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.77@2026-09-04 → 55.36@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.77@2026-09-04 → 55.36@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.77@2026-09-04 → 55.36@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.4400 R=0.2200); 2026-09-04:GREEN:O=38.2600,C=38.7000,body=+0.4400,vol=2695800.0; 2026-09-08:RED:O=38.6400,C=38.4200,body=-0.2200,vol=1404400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.920 (Gvol=2695800 Rvol=1404400); 2026-09-04:GREEN:O=38.2600,C=38.7000,body=+0.4400,vol=2695800.0; 2026-09-08:RED:O=38.6400,C=38.4200,body=-0.2200,vol=1404400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.729 on 2026-09-08: today_vol=1404400 / avg20=1926150 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.042 on 2026-09-08 (price=38.4200, mid=38.2770, upper=41.6471, lower=34.9069; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=38.4200 vs SMA50=34.8930 dist=+10.11% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=38.28_50=34.89_80=36.04 on 2026-09-08: SMA20=38.2770 SMA50=34.8930 SMA80=36.0371 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-07-08@30.7500; S2[2026-07-10→2026-08-07] low=2026-07-29@29.5300; S3[2026-08-10→2026-09-08] low=2026-08-10@32.8400 | lows=[30.75, 29.530000686645508, 32.84000015258789] span=11.21% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.8979610902211739 wick_frac=0.10203890977882617 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2444464224406495 wick_frac=0.7555535775593505 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.0300:wick=0.5700; 2026-09-02:RED:body=-0.4300:wick=0.4500; 2026-09-03:GREEN:body=+0.5800:wick=0.8000; 2026-09-04:GREEN:body=+0.4400:wick=0.0500; 2026-09-08:RED:body=-0.2200:wick=0.6800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=44.43 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.38 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11760.23 | **NEUTRAL** |
| `B04_income` | 483.22 | **GOOD** |
| `B05_profit_margin` | 4.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.69 vs prior_export=44.69 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 0.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WDS  ·  score **+16**  ·  Oil & Gas E&P
price=23.690000534057617  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.05 on 2026-09-11; prev RSI=56.03 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.03@2026-09-10 → 54.05@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.03@2026-09-10 → 54.05@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.03@2026-09-10 → 54.05@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=2.273 (G=0.2500 R=0.1100); 2026-09-10:RED:O=24.0000,C=23.8900,body=-0.1100,vol=642100.0; 2026-09-11:GREEN:O=23.4400,C=23.6900,body=+0.2500,vol=701100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.092 (Gvol=701100 Rvol=642100); 2026-09-10:RED:O=24.0000,C=23.8900,body=-0.1100,vol=642100.0; 2026-09-11:GREEN:O=23.4400,C=23.6900,body=+0.2500,vol=701100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.392 on 2026-09-11: today_vol=701100 / avg20=503500 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.139 on 2026-09-11 (price=23.6900, mid=23.5630, upper=24.4739, lower=22.6521; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=23.6900 vs SMA50=22.4970 dist=+5.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=23.5630 SMA50=22.4970 SMA80=22.0047 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-12→2026-09-11 (63 bars); S1[2026-06-12→2026-07-14] low=2026-06-25@18.8000; S2[2026-07-15→2026-08-12] low=2026-07-16@20.6000; S3[2026-08-13→2026-09-11] low=2026-08-13@22.5600 | lows=[18.799999237060547, 20.600000381469727, 22.559999465942383] span=20.00% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.609756324491296 wick_frac=0.39024367550870404 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.21153944907016836 wick_frac=0.7884605509298317 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.272714662227771 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+0.0100:wick=0.2500; 2026-09-08:GREEN:body=+0.1200:wick=0.2900; 2026-09-09:GREEN:body=+0.0200:wick=0.1600; 2026-09-10:RED:body=-0.1100:wick=0.4100; 2026-09-11:GREEN:body=+0.2500:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.2 (current export asof; earnings_date=8/24/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.03 (current export; earnings_date=8/24/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 13861.99 | **NEUTRAL** |
| `B04_income` | 3082.03 | **GOOD** |
| `B05_profit_margin` | 22.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.77 vs prior_export=23.77 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.69 | **NEUTRAL** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.64 | **GOOD** |
| `B13_short_float` | 0.19 | **NEUTRAL** |
| `B14_earnings_date` | 8/24/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.2 (this export) | prior_export=13.2 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.03 (this export) | prior_export=1.03 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BMRN  ·  score **+16**  ·  Biotechnology
price=65.68000030517578  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.90 on 2026-09-11; prev RSI=52.28 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.28@2026-09-10 → 52.90@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.28@2026-09-10 → 52.90@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.28@2026-09-10 → 52.90@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=3.929 (G=1.1000 R=0.2800); 2026-09-10:GREEN:O=64.4100,C=65.5100,body=+1.1000,vol=2202700.0; 2026-09-11:RED:O=65.9600,C=65.6800,body=-0.2800,vol=1604800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.373 (Gvol=2202700 Rvol=1604800); 2026-09-10:GREEN:O=64.4100,C=65.5100,body=+1.1000,vol=2202700.0; 2026-09-11:RED:O=65.9600,C=65.6800,body=-0.2800,vol=1604800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.877 on 2026-09-11: today_vol=1604800 / avg20=1829890 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.164 on 2026-09-11 (price=65.6800, mid=66.0845, upper=68.5509, lower=63.6181; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=65.6800 vs SMA50=63.0636 dist=+4.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=66.0845 SMA50=63.0636 SMA80=60.0094 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-10→2026-09-11 (63 bars); S1[2026-06-10→2026-07-10] low=2026-06-18@54.2600; S2[2026-07-13→2026-08-12] low=2026-07-23@57.4200; S3[2026-08-13→2026-09-11] low=2026-09-10@63.5700 | lows=[54.2599983215332, 57.41999816894531, 63.56999969482422] span=17.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.5445546050270995 wick_frac=0.4554453949729005 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.3684184108818953 wick_frac=0.6315815891181047 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.92858310626703 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+0.7400:wick=0.3800; 2026-09-08:RED:body=-0.6100:wick=1.6400; 2026-09-09:GREEN:body=+0.5800:wick=1.1100; 2026-09-10:GREEN:body=+1.1000:wick=0.9200; 2026-09-11:RED:body=-0.2800:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.66 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.19 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3456.79 | **NEUTRAL** |
| `B04_income` | 72.97 | **GOOD** |
| `B05_profit_margin` | 2.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 91.48 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=91.48 vs prior_export=91.48 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | -1.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.47 vs prior=-1.47 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.11 | **GOOD** |
| `B13_short_float` | 5.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.66 (this export) | prior_export=26.66 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.19 (this export) | prior_export=6.19 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DSGX  ·  score **+16**  ·  Software - Application
price=76.04000091552734  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.34 on 2026-09-11; prev RSI=36.69 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 36.69@2026-09-10 → 48.34@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 36.69@2026-09-10 → 48.34@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 36.69@2026-09-10 → 48.34@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=2.562 (G=4.3300 R=1.6900); 2026-09-10:RED:O=73.0100,C=71.3200,body=-1.6900,vol=1014300.0; 2026-09-11:GREEN:O=71.7100,C=76.0400,body=+4.3300,vol=1814500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.789 (Gvol=1814500 Rvol=1014300); 2026-09-10:RED:O=73.0100,C=71.3200,body=-1.6900,vol=1014300.0; 2026-09-11:GREEN:O=71.7100,C=76.0400,body=+4.3300,vol=1814500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.832 on 2026-09-11: today_vol=1814500 / avg20=640815 (avg window 2026-08-13→2026-09-10, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.299 on 2026-09-11 (price=76.0400, mid=77.6320, upper=82.9505, lower=72.3135; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=76.0400 vs SMA50=75.4098 dist=+0.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=77.6320 SMA50=75.4098 SMA80=74.0383 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-11 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-22@65.8100; S2[2026-07-10→2026-08-12] low=2026-07-23@65.6700; S3[2026-08-13→2026-09-11] low=2026-09-10@70.5500 | lows=[65.80999755859375, 65.66999816894531, 70.55000305175781] span=7.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.7930407613745768 wick_frac=0.2069592386254232 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.38940181277379704 wick_frac=0.610598187226203 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.56212755968074 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:RED:body=-0.9800:wick=2.1900; 2026-09-08:RED:body=-2.9200:wick=1.7900; 2026-09-09:RED:body=-1.8000:wick=0.4600; 2026-09-10:RED:body=-1.6900:wick=2.6500; 2026-09-11:GREEN:body=+4.3300:wick=1.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.07 (current export asof; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.83 (current export; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 775.17 | **NEUTRAL** |
| `B04_income` | 188.01 | **GOOD** |
| `B05_profit_margin` | 24.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 101.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=101.75 vs prior_export=101.75 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.67 | **BAD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 9/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.07 (this export) | prior_export=3.07 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.83 (this export) | prior_export=0.83 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXW  ·  score **+16**  ·  Security & Protection Services
price=34.93000030517578  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.51 on 2026-09-11; prev RSI=58.87 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.87@2026-09-10 → 61.51@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.87@2026-09-10 → 61.51@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.87@2026-09-10 → 61.51@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9100 R=0.0000); 2026-09-10:DOJI:O=34.4100,C=34.4100,body=+0.0000,vol=2259900.0; 2026-09-11:GREEN:O=34.0200,C=34.9300,body=+0.9100,vol=2768700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=3.450 (Gvol=3898650 Rvol=1129950); 2026-09-10:DOJI:O=34.4100,C=34.4100,body=+0.0000,vol=2259900.0; 2026-09-11:GREEN:O=34.0200,C=34.9300,body=+0.9100,vol=2768700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.662 on 2026-09-11: today_vol=2768700 / avg20=1665595 (avg window 2026-08-13→2026-09-10, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.771 on 2026-09-11 (price=34.9300, mid=33.6280, upper=35.3175, lower=31.9385; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=34.9300 vs SMA50=32.1702 dist=+8.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=33.6280 SMA50=32.1702 SMA80=29.3082 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-11 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-09@24.5600; S2[2026-07-10→2026-08-12] low=2026-08-06@28.5000; S3[2026-08-13→2026-09-11] low=2026-08-19@31.0100 | lows=[24.559999465942383, 28.5, 31.010000228881836] span=26.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.8666671510730206 wick_frac=0.13333284892697939 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+1.2500:wick=0.4600; 2026-09-08:GREEN:body=+0.5600:wick=0.5300; 2026-09-09:RED:body=-0.7200:wick=0.8500; 2026-09-10:DOJI:body=+0.0000:wick=0.5500; 2026-09-11:GREEN:body=+0.9100:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.56 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2484.04 | **NEUTRAL** |
| `B04_income` | 127.9 | **GOOD** |
| `B05_profit_margin` | 5.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.8 vs prior_export=41.8 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -15.17 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.17 vs prior=-15.17 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.04 | **GOOD** |
| `B13_short_float` | 14.98 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.56 (this export) | prior_export=7.56 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.71 (this export) | prior_export=10.71 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ETN  ·  score **+15**  ·  Specialty Industrial Machinery
price=422.1300048828125  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.16 on 2026-09-08; prev RSI=48.14 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.14@2026-09-04 → 53.16@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.14@2026-09-04 → 53.16@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.14@2026-09-04 → 53.16@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=6.419 (G=9.9500 R=1.5500); 2026-09-04:GREEN:O=400.9000,C=410.8500,body=+9.9500,vol=1889700.0; 2026-09-08:RED:O=423.6800,C=422.1300,body=-1.5500,vol=2035200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.929 (Gvol=1889700 Rvol=2035200); 2026-09-04:GREEN:O=400.9000,C=410.8500,body=+9.9500,vol=1889700.0; 2026-09-08:RED:O=423.6800,C=422.1300,body=-1.5500,vol=2035200.0 | **BAD** |
| `A07_rvol` | RVOL=1.187 on 2026-09-08: today_vol=2035200 / avg20=1714525 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.003 on 2026-09-08 (price=422.1300, mid=421.9825, upper=467.3977, lower=376.5673; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=422.1300 vs SMA50=415.4024 dist=+1.62% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=421.9825 SMA50=415.4024 SMA80=409.8194 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-05→2026-09-08 (63 bars); S1[2026-06-05→2026-07-07] low=2026-06-10@374.1195; S2[2026-07-08→2026-08-07] low=2026-07-29@357.2510; S3[2026-08-10→2026-09-08] low=2026-09-03@384.7700 | lows=[374.11953053930307, 357.2510251368342, 384.7699890136719] span=7.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.8891876707920388 wick_frac=0.1108123292079613 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.1441849109738372 wick_frac=0.8558150890261628 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.419413270328805 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-7.2800:wick=6.7200; 2026-09-02:RED:body=-0.4300:wick=8.4600; 2026-09-03:GREEN:body=+7.0900:wick=6.0100; 2026-09-04:GREEN:body=+9.9500:wick=1.2400; 2026-09-08:RED:body=-1.5500:wick=9.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.53 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.61 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 30025.0 | **NEUTRAL** |
| `B04_income` | 3829.0 | **GOOD** |
| `B05_profit_margin` | 12.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 485.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=485.22 vs prior_export=485.22 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -4.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.71 vs prior=-4.71 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 1.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.53 (this export) | prior_export=2.53 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.61 (this export) | prior_export=4.61 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CDNA  ·  score **+15**  ·  Diagnostics & Research
price=50.310001373291016  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.18 on 2026-09-08; prev RSI=65.67 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.67@2026-09-04 → 63.18@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.67@2026-09-04 → 63.18@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.67@2026-09-04 → 63.18@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=6.474 (G=1.2300 R=0.1900); 2026-09-04:GREEN:O=49.6500,C=50.8800,body=+1.2300,vol=1307200.0; 2026-09-08:RED:O=50.5000,C=50.3100,body=-0.1900,vol=935100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.398 (Gvol=1307200 Rvol=935100); 2026-09-04:GREEN:O=49.6500,C=50.8800,body=+1.2300,vol=1307200.0; 2026-09-08:RED:O=50.5000,C=50.3100,body=-0.1900,vol=935100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.859 on 2026-09-08: today_vol=935100 / avg20=1089070 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.421 on 2026-09-08 (price=50.3100, mid=48.6525, upper=52.5936, lower=44.7114; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=50.3100 vs SMA50=40.7490 dist=+23.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=48.6525 SMA50=40.7490 SMA80=33.9530 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-05→2026-09-08 (63 bars); S1[2026-06-05→2026-07-07] low=2026-06-09@21.6200; S2[2026-07-08→2026-08-07] low=2026-07-08@27.3200; S3[2026-08-10→2026-09-08] low=2026-08-18@44.8000 | lows=[21.6200008392334, 27.31999969482422, 44.79999923706055] span=107.22% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5324670177507171 wick_frac=0.4675329822492829 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.13475081366689842 wick_frac=0.8652491863331015 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.473728592366535 need>1.4; red_wick_gt_green=False 5d trail=2026-09-01:RED:body=-0.9600:wick=1.4100; 2026-09-02:GREEN:body=+0.0500:wick=2.9200; 2026-09-03:GREEN:body=+0.1100:wick=2.0040; 2026-09-04:GREEN:body=+1.2300:wick=1.0800; 2026-09-08:RED:body=-0.1900:wick=1.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=59.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.69 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 458.09 | **NEUTRAL** |
| `B04_income` | 111.01 | **GOOD** |
| `B05_profit_margin` | 24.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 54.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=54.14 vs prior_export=54.14 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.98 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.98 vs prior=-6.98 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.91 | **GOOD** |
| `B13_short_float` | 12.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=59.41 (this export) | prior_export=59.41 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.69 (this export) | prior_export=15.69 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ALB  ·  score **+15**  ·  Specialty Chemicals
price=129.57000732421875  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.59 on 2026-09-08; prev RSI=42.09 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.09@2026-09-04 → 46.59@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.09@2026-09-04 → 46.59@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.09@2026-09-04 → 46.59@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=5.661 (G=3.5100 R=0.6200); 2026-09-04:RED:O=126.9000,C=126.2800,body=-0.6200,vol=3239400.0; 2026-09-08:GREEN:O=126.0600,C=129.5700,body=+3.5100,vol=4961400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.532 (Gvol=4961400 Rvol=3239400); 2026-09-04:RED:O=126.9000,C=126.2800,body=-0.6200,vol=3239400.0; 2026-09-08:GREEN:O=126.0600,C=129.5700,body=+3.5100,vol=4961400.0 | **GOOD** |
| `A07_rvol` | RVOL=2.446 on 2026-09-08: today_vol=4961400 / avg20=2028365 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.540 on 2026-09-08 (price=129.5700, mid=134.0675, upper=142.3971, lower=125.7379; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=129.5700 vs SMA50=127.8938 dist=+1.31% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=134.07_50=127.89_80=141.52 on 2026-09-08: SMA20=134.0675 SMA50=127.8938 SMA80=141.5219 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-07-08@124.5800; S2[2026-07-10→2026-08-07] low=2026-07-28@111.5500; S3[2026-08-10→2026-09-08] low=2026-09-04@122.4500 | lows=[124.58000183105469, 111.55000305175781, 122.44999694824219] span=11.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5969407151652453 wick_frac=0.4030592848347548 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.09117683357829663 wick_frac=0.9088231664217034 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.66128099427798 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+3.5400:wick=3.1300; 2026-09-02:GREEN:body=+3.8100:wick=1.3100; 2026-09-03:RED:body=-3.8300:wick=2.2900; 2026-09-04:RED:body=-0.6200:wick=6.1800; 2026-09-08:GREEN:body=+3.5100:wick=2.3700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=17.09 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.49 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5907.9 | **NEUTRAL** |
| `B04_income` | 57.43 | **GOOD** |
| `B05_profit_margin` | 0.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 176.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=176.0 vs prior_export=176.0 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | -7.86 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-7.86 vs prior=-7.86 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.45 | **GOOD** |
| `B13_short_float` | 8.19 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.49 (this export) | prior_export=8.49 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAS  ·  score **+15**  ·  Leisure
price=91.54000091552734  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.02 on 2026-09-11; prev RSI=42.94 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.94@2026-09-10 → 48.02@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.94@2026-09-10 → 48.02@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.94@2026-09-10 → 48.02@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=12.166 (G=1.4600 R=0.1200); 2026-09-10:GREEN:O=88.7000,C=90.1600,body=+1.4600,vol=1490900.0; 2026-09-11:RED:O=91.6600,C=91.5400,body=-0.1200,vol=1071400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.392 (Gvol=1490900 Rvol=1071400); 2026-09-10:GREEN:O=88.7000,C=90.1600,body=+1.4600,vol=1490900.0; 2026-09-11:RED:O=91.6600,C=91.5400,body=-0.1200,vol=1071400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.787 on 2026-09-11: today_vol=1071400 / avg20=1361570 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.461 on 2026-09-11 (price=91.5400, mid=93.4461, upper=97.5814, lower=89.3108; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=91.5400 vs SMA50=89.2686 dist=+2.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=93.4461 SMA50=89.2686 SMA80=88.1355 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-11 (63 bars); S1[2026-06-09→2026-07-09] low=2026-07-08@75.1900; S2[2026-07-10→2026-08-12] low=2026-07-10@77.1800; S3[2026-08-13→2026-09-11] low=2026-09-10@87.7200 | lows=[75.19000244140625, 77.18000030517578, 87.72000122070312] span=16.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.5887133988395917 wick_frac=0.41128660116040827 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.0731724336847198 wick_frac=0.9268275663152802 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.166444147752559 need>1.4; red_wick_gt_green=False 5d trail=2026-09-04:GREEN:body=+0.3800:wick=1.9700; 2026-09-08:RED:body=-1.6100:wick=1.2600; 2026-09-09:RED:body=-0.7100:wick=0.8900; 2026-09-10:GREEN:body=+1.4600:wick=1.0200; 2026-09-11:RED:body=-0.1200:wick=1.5200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.67 (current export asof; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.62 (current export; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4971.2 | **NEUTRAL** |
| `B04_income` | 794.1 | **GOOD** |
| `B05_profit_margin` | 15.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.38 vs prior_export=110.38 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | -7.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-7.1 vs prior=-7.1 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 5.14 | **NEUTRAL** |
| `B14_earnings_date` | 7/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.67 (this export) | prior_export=10.67 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.62 (this export) | prior_export=6.62 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SLDE  ·  score **+15**  ·  Insurance - Property & Casualty
price=24.84000015258789  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.12 on 2026-09-11; prev RSI=66.52 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.52@2026-09-10 → 69.12@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.52@2026-09-10 → 69.12@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.52@2026-09-10 → 69.12@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=1.258 (G=0.3900 R=0.3100); 2026-09-10:RED:O=24.7600,C=24.4500,body=-0.3100,vol=1087400.0; 2026-09-11:GREEN:O=24.4500,C=24.8400,body=+0.3900,vol=1337200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.230 (Gvol=1337200 Rvol=1087400); 2026-09-10:RED:O=24.7600,C=24.4500,body=-0.3100,vol=1087400.0; 2026-09-11:GREEN:O=24.4500,C=24.8400,body=+0.3900,vol=1337200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.296 on 2026-09-11: today_vol=1337200 / avg20=1031815 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.694 on 2026-09-11 (price=24.8400, mid=23.4550, upper=25.4509, lower=21.4591; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=24.8400 vs SMA50=21.7751 dist=+14.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=23.4550 SMA50=21.7751 SMA80=20.2052 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-11 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-18@16.1973; S2[2026-07-10→2026-08-12] low=2026-07-23@19.1477; S3[2026-08-13→2026-09-11] low=2026-08-13@21.0964 | lows=[16.197313232547558, 19.14771451817286, 21.09637916496827] span=30.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.6425027416156837 wick_frac=0.3574972583843164 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.37170216074793716 wick_frac=0.6282978392520628 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2580647146047783 need>1.4; red_wick_gt_green=True 5d trail=2026-09-04:GREEN:body=+0.3050:wick=0.2050; 2026-09-08:RED:body=-0.2100:wick=0.3650; 2026-09-09:GREEN:body=+0.2500:wick=0.3950; 2026-09-10:RED:body=-0.3100:wick=0.5240; 2026-09-11:GREEN:body=+0.3900:wick=0.2170 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=24.75 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.66 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1388.8 | **NEUTRAL** |
| `B04_income` | 555.76 | **GOOD** |
| `B05_profit_margin` | 40.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -12.55 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-12.55 vs prior=-12.55 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.91 | **GOOD** |
| `B13_short_float` | 8.79 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.75 (this export) | prior_export=24.75 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.66 (this export) | prior_export=7.66 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SM  ·  score **+15**  ·  Oil & Gas E&P
price=38.099998474121094  pair=`2026-09-10→2026-09-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.28 on 2026-09-11; prev RSI=63.80 on 2026-09-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.80@2026-09-10 → 63.28@2026-09-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.80@2026-09-10 → 63.28@2026-09-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.80@2026-09-10 → 63.28@2026-09-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_body_sum/RED_body_sum=1.452 (G=0.6100 R=0.4200); 2026-09-10:RED:O=38.6000,C=38.1800,body=-0.4200,vol=2795300.0; 2026-09-11:GREEN:O=37.4900,C=38.1000,body=+0.6100,vol=3515200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-10 + 2026-09-11; ratio=GREEN_vol/RED_vol=1.258 (Gvol=3515200 Rvol=2795300); 2026-09-10:RED:O=38.6000,C=38.1800,body=-0.4200,vol=2795300.0; 2026-09-11:GREEN:O=37.4900,C=38.1000,body=+0.6100,vol=3515200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.988 on 2026-09-11: today_vol=3515200 / avg20=3556835 (avg window 2026-08-13→2026-09-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.530 on 2026-09-11 (price=38.1000, mid=36.8100, upper=39.2429, lower=34.3771; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-11: price=38.1000 vs SMA50=32.9012 dist=+15.80% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-11: SMA20=36.8100 SMA50=32.9012 SMA80=31.9829 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-11→2026-09-11 (63 bars); S1[2026-06-11→2026-07-13] low=2026-07-01@25.4700; S2[2026-07-14→2026-08-12] low=2026-08-06@28.2000; S3[2026-08-13→2026-09-11] low=2026-08-13@31.5900 | lows=[25.469999313354492, 28.200000762939453, 31.59000015258789] span=24.03% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: GREEN body_frac=0.7349342770475228 wick_frac=0.26506572295247727 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-10+2026-09-11: RED body_frac=0.3157876621961153 wick_frac=0.6842123378038847 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.452379654859219 need>1.4; red_wick_gt_green=False 5d trail=2026-09-04:RED:body=-0.4800:wick=0.3700; 2026-09-08:GREEN:body=+0.4600:wick=0.7200; 2026-09-09:RED:body=-0.5200:wick=0.2000; 2026-09-10:RED:body=-0.4200:wick=0.9100; 2026-09-11:GREEN:body=+0.6100:wick=0.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.53 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.46 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5495.59 | **NEUTRAL** |
| `B04_income` | 1000.09 | **GOOD** |
| `B05_profit_margin` | 18.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 42.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=42.28 vs prior_export=42.28 on finviz_2026-09-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.96 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.96 vs prior=-1.96 on finviz_2026-09-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 5.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.53 (this export) | prior_export=11.53 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.46 (this export) | prior_export=23.46 (finviz_2026-09-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-12_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.