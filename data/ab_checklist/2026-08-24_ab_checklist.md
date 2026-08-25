# A+B1 Feature Checklist — 2026-08-24

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,702** names
- Export: `finviz_2026-08-24.csv` · prior export for Δ: `2026-08-21`
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
| 1 | HUM | +18 | 18 | 0 | 2026-08-21→2026-08-24 | Healthcare Plans |
| 2 | DINO | +17 | 17 | 0 | 2026-08-21→2026-08-24 | Oil & Gas Refining & Marketing |
| 3 | SKWD | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Insurance - Property & Casualty |
| 4 | SYY | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Food Distribution |
| 5 | CRSR | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Computer Hardware |
| 6 | AMH | +16 | 17 | 1 | 2026-08-21→2026-08-24 | REIT - Residential |
| 7 | CBRL | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Restaurants |
| 8 | SGHC | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Gambling |
| 9 | EMBJ | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Aerospace & Defense |
| 10 | DE | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Farm & Heavy Construction Machinery |
| 11 | AME | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Specialty Industrial Machinery |
| 12 | MIAX | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Capital Markets |
| 13 | ZBRA | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Communication Equipment |
| 14 | FRSH | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Software - Application |
| 15 | SKY | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Residential Construction |

## Full checklist — top 15

### HUM  ·  score **+18**  ·  Healthcare Plans
price=386.6099853515625  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.88 on 2026-08-24; prev RSI=49.50 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.178 (G=6.1200 R=2.8100); 2026-08-21:RED:O=381.6900,C=378.8800,body=-2.8100,vol=1143600.0; 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1455000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.272 (Gvol=1455000 Rvol=1143600); 2026-08-21:RED:O=381.6900,C=378.8800,body=-2.8100,vol=1143600.0; 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1455000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.046 on 2026-08-24: today_vol=1455000 / avg20=1390370 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.458 on 2026-08-24 (price=386.6100, mid=378.3285, upper=396.3976, lower=360.2594; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=386.6100 vs SMA50=382.0114 dist=+1.20% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=378.33_50=382.01_80=347.01 on 2026-08-24: SMA20=378.3285 SMA50=382.0114 SMA80=347.0056 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@299.2939; S2[2026-06-22→2026-07-23] low=2026-06-24@352.2290; S3[2026-07-24→2026-08-24] low=2026-08-05@353.6900 | lows=[299.2938790186219, 352.22899615508356, 353.69000244140625] span=18.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5784485096023491 wick_frac=0.4215514903976509 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.3005343020618119 wick_frac=0.699465697938188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.177936097656335 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-2.0000:wick=8.8400; 2026-08-19:RED:body=-6.9800:wick=9.1300; 2026-08-20:GREEN:body=+6.4400:wick=7.0600; 2026-08-21:RED:body=-2.8100:wick=6.5400; 2026-08-24:GREEN:body=+6.1200:wick=4.4600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 420.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.080000000000041 (now=420.54 vs prior_export=418.46 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.23 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.23 vs prior=0.23 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.22 | **GOOD** |
| `B13_short_float` | 3.06 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+17**  ·  Oil & Gas Refining & Marketing
price=95.18000030517578  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.65 on 2026-08-24; prev RSI=67.90 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=3.860 (G=3.3200 R=0.8600); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=95.1800,body=-0.8600,vol=1954700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.484 (Gvol=2901400 Rvol=1954700); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=95.1800,body=-0.8600,vol=1954700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.663 on 2026-08-24: today_vol=1954700 / avg20=2947205 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.550 on 2026-08-24 (price=95.1800, mid=89.8407, upper=99.5445, lower=80.1369; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=95.1800 vs SMA50=81.6974 dist=+16.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=89.8407 SMA50=81.6974 SMA80=77.3487 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-22@63.4268; S2[2026-06-25→2026-07-24] low=2026-06-25@64.8977; S3[2026-07-27→2026-08-24] low=2026-08-07@80.0041 | lows=[63.42683480953448, 64.8977242324992, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7562642725310477 wick_frac=0.24373572746895236 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.19153697539391654 wick_frac=0.8084630246060834 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.8604620216106884 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.2900:wick=1.7800; 2026-08-19:RED:body=-1.6300:wick=1.0200; 2026-08-20:RED:body=-2.4600:wick=1.7500; 2026-08-21:GREEN:body=+3.3200:wick=1.0700; 2026-08-24:RED:body=-0.8600:wick=3.6300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 92.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=92.79 vs prior_export=92.79 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.63 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.37 | **GOOD** |
| `B13_short_float` | 5.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKWD  ·  score **+16**  ·  Insurance - Property & Casualty
price=57.91999816894531  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.15 on 2026-08-24; prev RSI=43.96 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8700 R=0.0000); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=57.9200,body=+1.0200,vol=370000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=893200 Rvol=0); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=57.9200,body=+1.0200,vol=370000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.726 on 2026-08-24: today_vol=370000 / avg20=509590 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.462 on 2026-08-24 (price=57.9200, mid=60.3520, upper=65.6152, lower=55.0888; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=57.9200 vs SMA50=57.8120 dist=+0.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=60.3520 SMA50=57.8120 SMA80=53.1741 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-03@42.5000; S2[2026-06-22→2026-07-23] low=2026-06-22@50.7550; S3[2026-07-24→2026-08-24] low=2026-08-20@54.0200 | lows=[42.5, 50.755001068115234, 54.02000045776367] span=27.11% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5130396187920134 wick_frac=0.48696038120798657 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:GREEN:body=+0.0400:wick=1.5800; 2026-08-19:RED:body=-2.0400:wick=0.6900; 2026-08-20:GREEN:body=+0.9500:wick=1.4900; 2026-08-21:GREEN:body=+0.8500:wick=0.7850; 2026-08-24:GREEN:body=+1.0200:wick=0.9950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.67 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.81 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1735.66 | **NEUTRAL** |
| `B04_income` | 187.9 | **GOOD** |
| `B05_profit_margin` | 10.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.92 vs prior_export=70.92 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.98 | **GOOD** |
| `B13_short_float` | 4.74 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.67 (this export) | prior_export=10.67 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.81 (this export) | prior_export=4.81 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SYY  ·  score **+16**  ·  Food Distribution
price=83.86000061035156  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.11 on 2026-08-24; prev RSI=55.45 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.130 (G=0.4900 R=0.2300); 2026-08-21:GREEN:O=83.6100,C=84.1000,body=+0.4900,vol=2171400.0; 2026-08-24:RED:O=84.0900,C=83.8600,body=-0.2300,vol=1746800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.243 (Gvol=2171400 Rvol=1746800); 2026-08-21:GREEN:O=83.6100,C=84.1000,body=+0.4900,vol=2171400.0; 2026-08-24:RED:O=84.0900,C=83.8600,body=-0.2300,vol=1746800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.482 on 2026-08-24: today_vol=1746800 / avg20=3624650 (avg window 2026-07-27→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.013 on 2026-08-24 (price=83.8600, mid=83.8870, upper=86.0143, lower=81.7597; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=83.8600 vs SMA50=82.3886 dist=+1.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=83.8870 SMA50=82.3886 SMA80=79.2079 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-02@72.5491; S2[2026-06-23→2026-07-24] low=2026-06-23@77.7348; S3[2026-07-27→2026-08-24] low=2026-08-04@80.4300 | lows=[72.5491367244224, 77.73476358499943, 80.43000030517578] span=10.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5505597750612924 wick_frac=0.4494402249387076 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.15231483586719818 wick_frac=0.8476851641328018 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1304650699927024 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.9800:wick=0.1300; 2026-08-19:RED:body=-0.0400:wick=1.1800; 2026-08-20:GREEN:body=+0.6600:wick=1.6800; 2026-08-21:GREEN:body=+0.4900:wick=0.4000; 2026-08-24:RED:body=-0.2300:wick=1.2800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.27 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.81 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 84553.0 | **NEUTRAL** |
| `B04_income` | 1756.0 | **GOOD** |
| `B05_profit_margin` | 2.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 90.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=90.67 vs prior_export=90.67 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | 0.64 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.64 vs prior=0.64 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.08 | **GOOD** |
| `B13_short_float` | 3.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.27 (this export) | prior_export=1.27 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.81 (this export) | prior_export=0.81 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=10.90999984741211  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.11 on 2026-08-24; prev RSI=47.25 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=1.143 (G=0.1600 R=0.1400); 2026-08-21:RED:O=11.0700,C=10.9300,body=-0.1400,vol=1193100.0; 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.521 (Gvol=1815200 Rvol=1193100); 2026-08-21:RED:O=11.0700,C=10.9300,body=-0.1400,vol=1193100.0; 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.786 on 2026-08-24: today_vol=1815200 / avg20=2308520 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.338 on 2026-08-24 (price=10.9100, mid=11.7377, upper=14.1862, lower=9.2893; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=10.9100 vs SMA50=10.2589 dist=+6.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=11.7377 SMA50=10.2589 SMA80=9.5505 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-26@7.5500; S2[2026-06-25→2026-07-24] low=2026-06-26@8.1950; S3[2026-07-27→2026-08-24] low=2026-07-27@9.9100 | lows=[7.550000190734863, 8.194999694824219, 9.90999984741211] span=31.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6060601681935093 wick_frac=0.3939398318064908 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.42424067277403693 wick_frac=0.575759327225963 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1428610354223434 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0400:wick=0.3400; 2026-08-19:GREEN:body=+0.0100:wick=0.4680; 2026-08-20:RED:body=-0.3300:wick=0.2800; 2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1040 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 25.27 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMH  ·  score **+16**  ·  REIT - Residential
price=34.810001373291016  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.98 on 2026-08-24; prev RSI=56.11 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=0.3200 R=0.1200); 2026-08-21:RED:O=34.4700,C=34.3500,body=-0.1200,vol=1411400.0; 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=0.910 (Gvol=1283900 Rvol=1411400); 2026-08-21:RED:O=34.4700,C=34.3500,body=-0.1200,vol=1411400.0; 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0 | **BAD** |
| `A07_rvol` | RVOL=0.597 on 2026-08-24: today_vol=1283900 / avg20=2152050 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.781 on 2026-08-24 (price=34.8100, mid=34.1135, upper=35.0056, lower=33.2214; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=34.8100 vs SMA50=33.5818 dist=+3.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=34.1135 SMA50=33.5818 SMA80=32.8572 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@31.1085; S2[2026-06-22→2026-07-23] low=2026-06-22@31.4700; S3[2026-07-24→2026-08-24] low=2026-07-30@32.8600 | lows=[31.108537337013935, 31.469999313354492, 32.86000061035156] span=5.63% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7111092273131862 wick_frac=0.2888907726868139 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.30000572202407066 wick_frac=0.6999942779759294 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6666030898340645 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.2500:wick=0.2200; 2026-08-19:GREEN:body=+0.2900:wick=0.2400; 2026-08-20:GREEN:body=+0.1000:wick=0.2300; 2026-08-21:RED:body=-0.1200:wick=0.2800; 2026-08-24:GREEN:body=+0.3200:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.77 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1891.62 | **NEUTRAL** |
| `B04_income` | 463.49 | **GOOD** |
| `B05_profit_margin` | 24.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.16 vs prior_export=37.16 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.41 (this export) | prior_export=72.41 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.77 (this export) | prior_export=0.77 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=58.02000045776367  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.43 on 2026-08-24; prev RSI=57.52 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.8200 R=0.0000); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:GREEN:O=57.5000,C=58.0200,body=+0.5200,vol=870100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1416800 Rvol=0); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:GREEN:O=57.5000,C=58.0200,body=+0.5200,vol=870100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.085 on 2026-08-24: today_vol=870100 / avg20=802260 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.285 on 2026-08-24 (price=58.0200, mid=57.0410, upper=60.4751, lower=53.6069; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=58.0200 vs SMA50=52.2704 dist=+11.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=57.0410 SMA50=52.2704 SMA80=44.4682 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@28.0918; S2[2026-06-22→2026-07-23] low=2026-06-23@45.2175; S3[2026-07-24→2026-08-24] low=2026-07-27@50.2000 | lows=[28.091751487486153, 45.21746996696082, 50.20000076293945] span=78.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.601820707726853 wick_frac=0.39817929227314686 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.7800:wick=1.3000; 2026-08-19:RED:body=-1.8300:wick=1.7800; 2026-08-20:RED:body=-0.4300:wick=1.4000; 2026-08-21:GREEN:body=+2.3000:wick=0.3300; 2026-08-24:GREEN:body=+0.5200:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=160.4 (current export asof; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.66 (current export; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3337.38 | **NEUTRAL** |
| `B04_income` | 26.23 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 23.97 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SGHC  ·  score **+16**  ·  Gambling
price=13.989999771118164  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.58 on 2026-08-24; prev RSI=46.31 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7500 R=0.0000); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4299300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6353300 Rvol=0); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4299300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.965 on 2026-08-24: today_vol=4299300 / avg20=2188330 (avg window 2026-07-24→2026-08-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.353 on 2026-08-24 (price=13.9900, mid=13.4995, upper=14.8889, lower=12.1101; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=13.9900 vs SMA50=13.9203 dist=+0.50% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=13.50_50=13.92_80=13.58 on 2026-08-24: SMA20=13.4995 SMA50=13.9203 SMA80=13.5774 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-03@12.1657; S2[2026-06-23→2026-07-23] low=2026-06-30@13.2350; S3[2026-07-24→2026-08-24] low=2026-08-12@12.7410 | lows=[12.165696572387432, 13.234999656677246, 12.741000175476074] span=8.79% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6080805572015702 wick_frac=0.3919194427984298 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.2100:wick=0.0650; 2026-08-19:GREEN:body=+0.2800:wick=0.1500; 2026-08-20:RED:body=-0.0200:wick=0.4170; 2026-08-21:GREEN:body=+0.0900:wick=0.1200; 2026-08-24:GREEN:body=+0.6600:wick=0.1780 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.51 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.65 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2431.0 | **NEUTRAL** |
| `B04_income` | 370.0 | **GOOD** |
| `B05_profit_margin` | 15.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.5 vs prior_export=19.5 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.13 vs prior=-0.13 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.32 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.65 (this export) | prior_export=3.65 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBJ  ·  score **+16**  ·  Aerospace & Defense
price=76.26000213623047  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.50 on 2026-08-24; prev RSI=63.83 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=4.571 (G=1.2800 R=0.2800); 2026-08-21:RED:O=76.2300,C=75.9500,body=-0.2800,vol=871800.0; 2026-08-24:GREEN:O=74.9800,C=76.2600,body=+1.2800,vol=1094000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.255 (Gvol=1094000 Rvol=871800); 2026-08-21:RED:O=76.2300,C=75.9500,body=-0.2800,vol=871800.0; 2026-08-24:GREEN:O=74.9800,C=76.2600,body=+1.2800,vol=1094000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.801 on 2026-08-24: today_vol=1094000 / avg20=1366410 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.507 on 2026-08-24 (price=76.2600, mid=73.0025, upper=79.4228, lower=66.5822; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=76.2600 vs SMA50=66.6958 dist=+14.34% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=73.0025 SMA50=66.6958 SMA80=63.8784 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-11@53.3748; S2[2026-06-22→2026-07-23] low=2026-06-23@59.4139; S3[2026-07-24→2026-08-24] low=2026-07-24@64.6100 | lows=[53.3748138669873, 59.41386146883149, 64.61000061035156] span=21.05% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7852729723657605 wick_frac=0.2147270276342395 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.12785666508737215 wick_frac=0.8721433349126279 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.571319582572682 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.1900:wick=1.1200; 2026-08-19:RED:body=-1.2200:wick=0.8100; 2026-08-20:RED:body=-2.4400:wick=0.5000; 2026-08-21:RED:body=-0.2800:wick=1.9100; 2026-08-24:GREEN:body=+1.2800:wick=0.3500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=91.21 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.12 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8338.75 | **NEUTRAL** |
| `B04_income` | 445.04 | **GOOD** |
| `B05_profit_margin` | 5.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.54 vs prior_export=89.54 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.07 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.45 | **GOOD** |
| `B13_short_float` | 1.33 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=91.21 (this export) | prior_export=91.21 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.12 (this export) | prior_export=11.12 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DE  ·  score **+16**  ·  Farm & Heavy Construction Machinery
price=648.6400146484375  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.42 on 2026-08-24; prev RSI=62.14 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=5.502 (G=24.2100 R=4.4000); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=648.6400,body=-4.4000,vol=1228900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.947 (Gvol=2392500 Rvol=1228900); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=648.6400,body=-4.4000,vol=1228900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.023 on 2026-08-24: today_vol=1228900 / avg20=1201820 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.984 on 2026-08-24 (price=648.6400, mid=614.9965, upper=649.1845, lower=580.8085; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-24: price=648.6400 vs SMA50=604.8177 dist=+7.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=614.9965 SMA50=604.8177 SMA80=589.9240 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@513.8182; S2[2026-06-22→2026-07-23] low=2026-07-15@576.4500; S3[2026-07-24→2026-08-24] low=2026-08-19@579.3100 | lows=[513.8182476448627, 576.4500122070312, 579.3099975585938] span=12.75% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5219913408519654 wick_frac=0.4780086591480346 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.29470633204285956 wick_frac=0.7052936679571404 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.502309645022126 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-15.2100:wick=0.8300; 2026-08-19:RED:body=-8.9500:wick=7.8000; 2026-08-20:GREEN:body=+9.8200:wick=42.7000; 2026-08-21:GREEN:body=+24.2100:wick=22.1700; 2026-08-24:RED:body=-4.4000:wick=10.5300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.65 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 47976.0 | **NEUTRAL** |
| `B04_income` | 4873.0 | **GOOD** |
| `B05_profit_margin` | 10.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 671.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=6.040000000000077 (now=671.33 vs prior_export=665.29 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 2.52 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.65 (this export) | prior_export=8.65 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+15**  ·  Specialty Industrial Machinery
price=242.3300018310547  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.41 on 2026-08-24; prev RSI=42.51 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.51@2026-08-21 → 46.41@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.51@2026-08-21 → 46.41@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.51@2026-08-21 → 46.41@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=8.786 (G=3.6900 R=0.4200); 2026-08-21:RED:O=239.9700,C=239.5500,body=-0.4200,vol=1288100.0; 2026-08-24:GREEN:O=238.6400,C=242.3300,body=+3.6900,vol=1825300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.417 (Gvol=1825300 Rvol=1288100); 2026-08-21:RED:O=239.9700,C=239.5500,body=-0.4200,vol=1288100.0; 2026-08-24:GREEN:O=238.6400,C=242.3300,body=+3.6900,vol=1825300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.348 on 2026-08-24: today_vol=1825300 / avg20=1353725 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.406 on 2026-08-24 (price=242.3300, mid=248.2240, upper=262.7413, lower=233.7067; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=242.3300 vs SMA50=239.9863 dist=+0.98% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=248.2240 SMA50=239.9863 SMA80=235.7517 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-01@219.7705; S2[2026-06-22→2026-07-23] low=2026-07-08@226.5100; S3[2026-07-24→2026-08-24] low=2026-07-29@233.9900 | lows=[219.77050537498957, 226.50999450683594, 233.99000549316406] span=6.47% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7834388920385519 wick_frac=0.21656110796144812 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.12574751817114455 wick_frac=0.8742524818288555 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.785758401453224 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-2.5500:wick=2.0500; 2026-08-19:RED:body=-9.1500:wick=0.6200; 2026-08-20:RED:body=-2.2500:wick=2.2600; 2026-08-21:RED:body=-0.4200:wick=2.9200; 2026-08-24:GREEN:body=+3.6900:wick=1.0200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.06 vs prior_export=281.06 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | -0.32 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.32 vs prior=-0.32 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.04 | **GOOD** |
| `B13_short_float` | 1.38 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MIAX  ·  score **+15**  ·  Capital Markets
price=44.029998779296875  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.91 on 2026-08-24; prev RSI=48.37 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.37@2026-08-21 → 53.91@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.37@2026-08-21 → 53.91@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.37@2026-08-21 → 53.91@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.4000 R=0.0000); 2026-08-21:GREEN:O=41.6200,C=42.5900,body=+0.9700,vol=1120800.0; 2026-08-24:GREEN:O=42.6000,C=44.0300,body=+1.4300,vol=826800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1947600 Rvol=0); 2026-08-21:GREEN:O=41.6200,C=42.5900,body=+0.9700,vol=1120800.0; 2026-08-24:GREEN:O=42.6000,C=44.0300,body=+1.4300,vol=826800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.638 on 2026-08-24: today_vol=826800 / avg20=1295250 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.015 on 2026-08-24 (price=44.0300, mid=43.9755, upper=47.5850, lower=40.3660; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=44.0300 vs SMA50=41.8592 dist=+5.19% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=43.98_50=41.86_80=44.37 on 2026-08-24: SMA20=43.9755 SMA50=41.8592 SMA80=44.3653 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-18@36.6800; S2[2026-06-22→2026-07-23] low=2026-06-23@35.4300; S3[2026-07-24→2026-08-24] low=2026-08-19@38.7100 | lows=[36.68000030517578, 35.43000030517578, 38.709999084472656] span=9.26% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.659309792740154 wick_frac=0.340690207259846 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.9500:wick=0.2400; 2026-08-19:RED:body=-1.1800:wick=2.7700; 2026-08-20:GREEN:body=+0.3200:wick=1.1800; 2026-08-21:GREEN:body=+0.9700:wick=1.2750; 2026-08-24:GREEN:body=+1.4300:wick=0.1830 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=17.53 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.87 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1466.53 | **NEUTRAL** |
| `B04_income` | 142.3 | **GOOD** |
| `B05_profit_margin` | 9.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 52.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=52.0 vs prior_export=52.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -11.17 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-11.17 vs prior=-11.17 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 29.52 | **GOOD** |
| `B13_short_float` | 7.21 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.53 (this export) | prior_export=17.53 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.87 (this export) | prior_export=3.87 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZBRA  ·  score **+15**  ·  Communication Equipment
price=363.29998779296875  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.67 on 2026-08-24; prev RSI=68.09 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.09@2026-08-21 → 64.67@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.09@2026-08-21 → 64.67@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.09@2026-08-21 → 64.67@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=3.751 (G=7.2400 R=1.9300); 2026-08-21:GREEN:O=361.2700,C=368.5100,body=+7.2400,vol=718600.0; 2026-08-24:RED:O=365.2300,C=363.3000,body=-1.9300,vol=637900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.127 (Gvol=718600 Rvol=637900); 2026-08-21:GREEN:O=361.2700,C=368.5100,body=+7.2400,vol=718600.0; 2026-08-24:RED:O=365.2300,C=363.3000,body=-1.9300,vol=637900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.734 on 2026-08-24: today_vol=637900 / avg20=869000 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.184 on 2026-08-24 (price=363.3000, mid=348.5280, upper=428.6082, lower=268.4478; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=363.3000 vs SMA50=290.7888 dist=+24.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=348.5280 SMA50=290.7888 SMA80=271.4669 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-11@211.8600; S2[2026-06-22→2026-07-23] low=2026-06-22@235.0500; S3[2026-07-24→2026-08-24] low=2026-07-24@256.6700 | lows=[211.86000061035156, 235.0500030517578, 256.6700134277344] span=21.15% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6073836070610222 wick_frac=0.3926163929389777 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.20209628197549012 wick_frac=0.7979037180245099 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.751261009123539 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-1.7300:wick=10.1400; 2026-08-19:RED:body=-15.5400:wick=2.0500; 2026-08-20:GREEN:body=+6.6500:wick=6.9900; 2026-08-21:GREEN:body=+7.2400:wick=4.6800; 2026-08-24:RED:body=-1.9300:wick=7.6200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=45.54 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.99 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5887.0 | **NEUTRAL** |
| `B04_income` | 539.0 | **GOOD** |
| `B05_profit_margin` | 9.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 417.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=417.36 vs prior_export=417.36 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -1.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.6 vs prior=-1.6 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.55 | **GOOD** |
| `B13_short_float` | 5.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=45.54 (this export) | prior_export=45.54 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.99 (this export) | prior_export=3.99 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+15**  ·  Software - Application
price=13.1899995803833  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.09 on 2026-08-24; prev RSI=66.31 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.31@2026-08-21 → 68.09@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.31@2026-08-21 → 68.09@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.31@2026-08-21 → 68.09@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=4.800 (G=0.2400 R=0.0500); 2026-08-21:RED:O=13.0300,C=12.9800,body=-0.0500,vol=4271800.0; 2026-08-24:GREEN:O=12.9500,C=13.1900,body=+0.2400,vol=4275800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.001 (Gvol=4275800 Rvol=4271800); 2026-08-21:RED:O=13.0300,C=12.9800,body=-0.0500,vol=4271800.0; 2026-08-24:GREEN:O=12.9500,C=13.1900,body=+0.2400,vol=4275800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.487 on 2026-08-24: today_vol=4275800 / avg20=8777420 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.784 on 2026-08-24 (price=13.1900, mid=12.0938, upper=13.4912, lower=10.6963; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=13.1900 vs SMA50=10.8276 dist=+21.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=12.0938 SMA50=10.8276 SMA80=10.1614 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-18@8.6200; S2[2026-06-23→2026-07-23] low=2026-06-23@8.9950; S3[2026-07-24→2026-08-24] low=2026-07-24@9.9700 | lows=[8.619999885559082, 8.994999885559082, 9.970000267028809] span=15.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.578312754648301 wick_frac=0.42168724535169905 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.12658261465807796 wick_frac=0.873417385341922 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.799977111903718 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.2500:wick=0.3170; 2026-08-19:GREEN:body=+0.3500:wick=0.2250; 2026-08-20:RED:body=-0.0200:wick=0.2500; 2026-08-21:RED:body=-0.0500:wick=0.3450; 2026-08-24:GREEN:body=+0.2400:wick=0.1750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.38 vs prior_export=14.38 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | 0.1 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.1 vs prior=0.1 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.7 | **BAD** |
| `B13_short_float` | 12.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKY  ·  score **+15**  ·  Residential Construction
price=91.52999877929688  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.28 on 2026-08-24; prev RSI=53.92 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.92@2026-08-21 → 56.28@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.92@2026-08-21 → 56.28@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.92@2026-08-21 → 56.28@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=1.218 (G=0.6700 R=0.5500); 2026-08-21:RED:O=90.8000,C=90.2500,body=-0.5500,vol=611800.0; 2026-08-24:GREEN:O=90.8600,C=91.5300,body=+0.6700,vol=1177700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.925 (Gvol=1177700 Rvol=611800); 2026-08-21:RED:O=90.8000,C=90.2500,body=-0.5500,vol=611800.0; 2026-08-24:GREEN:O=90.8600,C=91.5300,body=+0.6700,vol=1177700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.646 on 2026-08-24: today_vol=1177700 / avg20=715655 (avg window 2026-07-24→2026-08-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.198 on 2026-08-24 (price=91.5300, mid=89.3595, upper=100.3235, lower=78.3955; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=91.5300 vs SMA50=85.2988 dist=+7.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=89.3595 SMA50=85.2988 SMA80=80.4719 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@64.9800; S2[2026-06-22→2026-07-23] low=2026-07-08@78.2200; S3[2026-07-24→2026-08-24] low=2026-08-03@78.8600 | lows=[64.9800033569336, 78.22000122070312, 78.86000061035156] span=21.36% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.40119694823884144 wick_frac=0.5988030517611586 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.33333487462373246 wick_frac=0.6666651253762675 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2181717297822168 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-2.4900:wick=0.0000; 2026-08-19:RED:body=-0.4800:wick=4.1000; 2026-08-20:GREEN:body=+0.6400:wick=0.9200; 2026-08-21:RED:body=-0.5500:wick=1.1000; 2026-08-24:GREEN:body=+0.6700:wick=1.0000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.84 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.18 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2672.55 | **NEUTRAL** |
| `B04_income` | 191.37 | **GOOD** |
| `B05_profit_margin` | 7.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.5 vs prior_export=103.5 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -2.37 | **BAD** |
| `B11_insider_tx_delta` | delta=0.06999999999999984 (now=-2.37 vs prior=-2.44 on finviz_2026-08-21) | **GOOD** |
| `B12_institutional_transactions` | 2.07 | **GOOD** |
| `B13_short_float` | 5.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.84 (this export) | prior_export=0.84 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.18 (this export) | prior_export=1.18 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-24_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.