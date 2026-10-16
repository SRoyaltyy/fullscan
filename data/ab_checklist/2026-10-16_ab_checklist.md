# A+B1 Feature Checklist — 2026-10-16

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-16.csv` · prior export for Δ: `2026-10-15`
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
| 1 | FIVN | +17 | 18 | 1 | 2026-10-14→2026-10-15 | Software - Infrastructure |
| 2 | ESTC | +17 | 18 | 1 | 2026-10-14→2026-10-15 | Software - Application |
| 3 | AUPH | +17 | 17 | 0 | 2026-10-14→2026-10-15 | Biotechnology |
| 4 | VFF | +16 | 17 | 1 | 2026-10-14→2026-10-15 | Farm Products |
| 5 | TASK | +16 | 17 | 1 | 2026-10-14→2026-10-15 | Information Technology Services |
| 6 | HTHT | +16 | 17 | 1 | 2026-10-14→2026-10-15 | Lodging |
| 7 | SWKS | +16 | 16 | 0 | 2026-10-14→2026-10-15 | Semiconductors |
| 8 | FRSH | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Software - Application |
| 9 | EPAM | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Information Technology Services |
| 10 | ADSK | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Software - Application |
| 11 | TTEK | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Engineering & Construction |
| 12 | WAY | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Health Information Services |
| 13 | TENB | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Software - Infrastructure |
| 14 | AME | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Specialty Industrial Machinery |
| 15 | MH | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Education & Training Services |

## Full checklist — top 15

### FIVN  ·  score **+17**  ·  Software - Infrastructure
price=35.099998474121094  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.73 on 2026-10-15; prev RSI=45.85 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=2.221 (G=2.5100 R=1.1300); 2026-10-14:RED:O=34.2100,C=33.0800,body=-1.1300,vol=1631365.0; 2026-10-15:GREEN:O=32.5900,C=35.1000,body=+2.5100,vol=1774710.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.088 (Gvol=1774710 Rvol=1631365); 2026-10-14:RED:O=34.2100,C=33.0800,body=-1.1300,vol=1631365.0; 2026-10-15:GREEN:O=32.5900,C=35.1000,body=+2.5100,vol=1774710.0 | **GOOD** |
| `A07_rvol` | RVOL=0.738 on 2026-10-15: today_vol=1774710 / avg20=2404949 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.020 on 2026-10-15 (price=35.1000, mid=35.1640, upper=38.3399, lower=31.9881; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=35.1000 vs SMA50=33.6785 dist=+4.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=35.1640 SMA50=33.6785 SMA80=30.3294 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-15 (63 bars); S1[2026-07-20→2026-08-17] low=2026-07-23@20.7600; S2[2026-08-18→2026-09-16] low=2026-09-10@30.1000; S3[2026-09-17→2026-10-15] low=2026-09-18@32.1400 | lows=[20.760000228881836, 30.100000381469727, 32.13999938964844] span=54.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7699376309107291 wick_frac=0.23006236908927089 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.39718691338160367 wick_frac=0.6028130866183964 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.22124285164505 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+1.2900:wick=0.4450; 2026-10-12:RED:body=-1.0900:wick=0.7400; 2026-10-13:GREEN:body=+0.0580:wick=1.0019; 2026-10-14:RED:body=-1.1300:wick=1.7150; 2026-10-15:GREEN:body=+2.5100:wick=0.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.74 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.88 | **NEUTRAL** |
| `B04_income` | 59.47 | **GOOD** |
| `B05_profit_margin` | 4.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.8 vs prior_export=35.8 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -4.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.84 vs prior=-4.84 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.75 | **GOOD** |
| `B13_short_float` | 12.24 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.9 (this export) | prior_export=1.9 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESTC  ·  score **+17**  ·  Software - Application
price=97.23999786376953  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.63 on 2026-10-15; prev RSI=52.55 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.55@2026-10-14 → 60.63@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.55@2026-10-14 → 60.63@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.55@2026-10-14 → 60.63@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=11.143 (G=6.2400 R=0.5600); 2026-10-14:RED:O=92.6200,C=92.0600,body=-0.5600,vol=1592400.0; 2026-10-15:GREEN:O=91.0000,C=97.2400,body=+6.2400,vol=4181500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=2.626 (Gvol=4181500 Rvol=1592400); 2026-10-14:RED:O=92.6200,C=92.0600,body=-0.5600,vol=1592400.0; 2026-10-15:GREEN:O=91.0000,C=97.2400,body=+6.2400,vol=4181500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.435 on 2026-10-15: today_vol=4181500 / avg20=1717034 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.769 on 2026-10-15 (price=97.2400, mid=92.3250, upper=98.7181, lower=85.9319; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=97.2400 vs SMA50=88.0374 dist=+10.45% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=92.3250 SMA50=88.0374 SMA80=77.8616 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@56.0600; S2[2026-08-18→2026-09-16] low=2026-08-26@78.7900; S3[2026-09-17→2026-10-15] low=2026-09-17@83.8100 | lows=[56.060001373291016, 78.79000091552734, 83.80999755859375] span=49.50% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8606893605199354 wick_frac=0.13931063948006467 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.14621567514267786 wick_frac=0.8537843248573221 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.142750098772495 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.4300:wick=2.7324; 2026-10-12:RED:body=-3.9300:wick=2.4600; 2026-10-13:RED:body=-0.5800:wick=2.8900; 2026-10-14:RED:body=-0.5600:wick=3.2700; 2026-10-15:GREEN:body=+6.2400:wick=1.0100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.84 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.78 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.16 | **NEUTRAL** |
| `B04_income` | 375.64 | **GOOD** |
| `B05_profit_margin` | 20.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.42 vs prior_export=110.42 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.05 vs prior=-8.05 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 4.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.84 (this export) | prior_export=19.84 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+17**  ·  Biotechnology
price=16.729999542236328  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.03 on 2026-10-15; prev RSI=56.15 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.15@2026-10-14 → 58.03@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.15@2026-10-14 → 58.03@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.15@2026-10-14 → 58.03@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=3.364 (G=0.3700 R=0.1100); 2026-10-14:GREEN:O=16.2400,C=16.6100,body=+0.3700,vol=1225700.0; 2026-10-15:RED:O=16.8400,C=16.7300,body=-0.1100,vol=1031800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.188 (Gvol=1225700 Rvol=1031800); 2026-10-14:GREEN:O=16.2400,C=16.6100,body=+0.3700,vol=1225700.0; 2026-10-15:RED:O=16.8400,C=16.7300,body=-0.1100,vol=1031800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.226 on 2026-10-15: today_vol=1031800 / avg20=841839 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.537 on 2026-10-15 (price=16.7300, mid=16.3535, upper=17.0551, lower=15.6519; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=16.7300 vs SMA50=16.2183 dist=+3.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=16.3535 SMA50=16.2183 SMA80=16.1137 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-15 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-31@14.3630; S2[2026-08-18→2026-09-16] low=2026-08-18@15.1500; S3[2026-09-17→2026-10-15] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.149999618530273, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.5203946680115568 wick_frac=0.47960533198844324 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.18803491257185148 wick_frac=0.8119650874281485 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3636253294492997 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-13:RED:body=-0.1000:wick=0.2700; 2026-10-14:GREEN:body=+0.3700:wick=0.3410; 2026-10-15:RED:body=-0.1100:wick=0.4750 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VFF  ·  score **+16**  ·  Farm Products
price=2.5999999046325684  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=38.54 on 2026-10-15; prev RSI=35.18 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.667 (G=0.0500 R=0.0300); 2026-10-14:RED:O=2.5900,C=2.5600,body=-0.0300,vol=737285.0; 2026-10-15:GREEN:O=2.5500,C=2.6000,body=+0.0500,vol=835664.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.133 (Gvol=835664 Rvol=737285); 2026-10-14:RED:O=2.5900,C=2.5600,body=-0.0300,vol=737285.0; 2026-10-15:GREEN:O=2.5500,C=2.6000,body=+0.0500,vol=835664.0 | **GOOD** |
| `A07_rvol` | RVOL=1.077 on 2026-10-15: today_vol=835664 / avg20=775784 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.624 on 2026-10-15 (price=2.6000, mid=2.8430, upper=3.2322, lower=2.4538; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-15: price=2.6000 vs SMA50=2.7609 dist=-5.83% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=2.8430 SMA50=2.7609 SMA80=2.4623 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-15 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-17@1.9000; S2[2026-08-18→2026-09-16] low=2026-08-19@2.4700; S3[2026-09-17→2026-10-15] low=2026-10-15@2.5300 | lows=[1.899999976158142, 2.4700000286102295, 2.5299999713897705] span=33.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7142857142857143 wick_frac=0.2857142857142857 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.28571396133532545 wick_frac=0.7142860386646745 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6666666666666667 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.0100:wick=0.0650; 2026-10-12:GREEN:body=+0.0193:wick=0.0507; 2026-10-13:GREEN:body=+0.0400:wick=0.0199; 2026-10-14:RED:body=-0.0300:wick=0.0750; 2026-10-15:GREEN:body=+0.0500:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=294.74 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=13.63 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 230.57 | **NEUTRAL** |
| `B04_income` | 22.71 | **GOOD** |
| `B05_profit_margin` | 9.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.83 vs prior_export=5.83 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.67 vs prior=0.67 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.12 | **GOOD** |
| `B13_short_float` | 4.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=294.74 (this export) | prior_export=294.74 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=13.63 (this export) | prior_export=13.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TASK  ·  score **+16**  ·  Information Technology Services
price=8.520000457763672  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.47 on 2026-10-15; prev RSI=52.78 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.78@2026-10-14 → 59.47@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.78@2026-10-14 → 59.47@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.78@2026-10-14 → 59.47@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=12.333 (G=0.3700 R=0.0300); 2026-10-14:RED:O=8.1700,C=8.1400,body=-0.0300,vol=173200.0; 2026-10-15:GREEN:O=8.1500,C=8.5200,body=+0.3700,vol=709500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=4.096 (Gvol=709500 Rvol=173200); 2026-10-14:RED:O=8.1700,C=8.1400,body=-0.0300,vol=173200.0; 2026-10-15:GREEN:O=8.1500,C=8.5200,body=+0.3700,vol=709500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.576 on 2026-10-15: today_vol=709500 / avg20=450171 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.757 on 2026-10-15 (price=8.5200, mid=8.1700, upper=8.6323, lower=7.7077; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=8.5200 vs SMA50=7.9327 dist=+7.40% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=8.1700 SMA50=7.9327 SMA80=7.0084 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@5.3200; S2[2026-08-18→2026-09-16] low=2026-08-18@7.1650; S3[2026-09-17→2026-10-15] low=2026-09-24@7.5300 | lows=[5.320000171661377, 7.164999961853027, 7.53000020980835] span=41.54% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7551026366136437 wick_frac=0.24489736338635626 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.1249987582378501 wick_frac=0.8750012417621499 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.333471087516292 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.1000:wick=0.0600; 2026-10-12:RED:body=-0.1400:wick=0.1300; 2026-10-13:GREEN:body=+0.1300:wick=0.0950; 2026-10-14:RED:body=-0.0300:wick=0.2100; 2026-10-15:GREEN:body=+0.3700:wick=0.1200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.65 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.8 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1226.79 | **NEUTRAL** |
| `B04_income` | 107.38 | **GOOD** |
| `B05_profit_margin` | 8.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.5 vs prior_export=8.5 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.88 | **NEUTRAL** |
| `B10_insider_transactions` | -0.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.05 vs prior=-0.05 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.32 | **GOOD** |
| `B13_short_float` | 11.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.65 (this export) | prior_export=16.65 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.8 (this export) | prior_export=3.8 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+16**  ·  Lodging
price=45.939998626708984  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.11 on 2026-10-15; prev RSI=55.77 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=7.737 (G=1.4700 R=0.1900); 2026-10-14:RED:O=44.3900,C=44.2000,body=-0.1900,vol=1243111.0; 2026-10-15:GREEN:O=44.4700,C=45.9400,body=+1.4700,vol=1929322.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.552 (Gvol=1929322 Rvol=1243111); 2026-10-14:RED:O=44.3900,C=44.2000,body=-0.1900,vol=1243111.0; 2026-10-15:GREEN:O=44.4700,C=45.9400,body=+1.4700,vol=1929322.0 | **GOOD** |
| `A07_rvol` | RVOL=1.563 on 2026-10-15: today_vol=1929322 / avg20=1234228 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.269 on 2026-10-15 (price=45.9400, mid=43.0840, upper=45.3354, lower=40.8326; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=45.9400 vs SMA50=44.3390 dist=+3.61% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=43.08_50=44.34_80=43.46 on 2026-10-15: SMA20=43.0840 SMA50=44.3390 SMA80=43.4629 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@40.0800; S2[2026-08-18→2026-09-16] low=2026-09-16@42.5100; S3[2026-09-17→2026-10-15] low=2026-10-02@41.2900 | lows=[40.08000183105469, 42.5099983215332, 41.290000915527344] span=6.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7538445286503215 wick_frac=0.2461554713496785 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.23312645098479742 wick_frac=0.7668735490152025 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.736884373682414 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.9600:wick=0.3300; 2026-10-12:GREEN:body=+0.2950:wick=0.6150; 2026-10-13:RED:body=-0.4100:wick=0.9100; 2026-10-14:RED:body=-0.1900:wick=0.6250; 2026-10-15:GREEN:body=+1.4700:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.56 (current export asof; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.03 (current export; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.86 | **NEUTRAL** |
| `B04_income` | 720.34 | **GOOD** |
| `B05_profit_margin` | 18.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.99 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=60.99 vs prior_export=60.99 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.21 | **GOOD** |
| `B13_short_float` | 3.59 | **NEUTRAL** |
| `B14_earnings_date` | 8/17/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.56 (this export) | prior_export=3.56 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.03 (this export) | prior_export=5.03 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=79.0999984741211  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.48 on 2026-10-15; prev RSI=43.33 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.6800 R=0.0000); 2026-10-14:GREEN:O=75.5000,C=76.8900,body=+1.3900,vol=5128746.0; 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10897948 Rvol=0); 2026-10-14:GREEN:O=75.5000,C=76.8900,body=+1.3900,vol=5128746.0; 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0 | **GOOD** |
| `A07_rvol` | RVOL=0.731 on 2026-10-15: today_vol=5769202 / avg20=7892153 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.477 on 2026-10-15 (price=79.1000, mid=83.9920, upper=94.2480, lower=73.7360; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=79.1000 vs SMA50=77.1322 dist=+2.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=83.9920 SMA50=77.1322 SMA80=71.9984 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-16@55.5500; S2[2026-08-18→2026-09-16] low=2026-08-25@63.0600; S3[2026-09-17→2026-10-15] low=2026-10-13@73.7600 | lows=[55.54999923706055, 63.060001373291016, 73.76000213623047] span=32.78% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8611556980962559 wick_frac=0.13884430190374406 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-3.0000:wick=2.6489; 2026-10-12:RED:body=-1.4000:wick=1.9000; 2026-10-13:GREEN:body=+2.8000:wick=0.6850; 2026-10-14:GREEN:body=+1.3900:wick=0.3700; 2026-10-15:GREEN:body=+3.2900:wick=0.2380 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+15**  ·  Software - Application
price=14.25  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.85 on 2026-10-15; prev RSI=60.32 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=4.462 (G=0.5800 R=0.1300); 2026-10-14:RED:O=13.8200,C=13.6900,body=-0.1300,vol=4573770.0; 2026-10-15:GREEN:O=13.6700,C=14.2500,body=+0.5800,vol=5580073.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.220 (Gvol=5580073 Rvol=4573770); 2026-10-14:RED:O=13.8200,C=13.6900,body=-0.1300,vol=4573770.0; 2026-10-15:GREEN:O=13.6700,C=14.2500,body=+0.5800,vol=5580073.0 | **GOOD** |
| `A07_rvol` | RVOL=0.732 on 2026-10-15: today_vol=5580073 / avg20=7621307 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.876 on 2026-10-15 (price=14.2500, mid=13.1535, upper=14.4054, lower=11.9016; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=14.2500 vs SMA50=12.8653 dist=+10.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=13.1535 SMA50=12.8653 SMA80=11.9758 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-23@9.7650; S2[2026-08-18→2026-09-16] low=2026-09-10@11.7040; S3[2026-09-17→2026-10-15] low=2026-09-28@11.9600 | lows=[9.765000343322754, 11.704000473022461, 11.960000038146973] span=22.48% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.659090736680802 wick_frac=0.340909263319198 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.24074096967492065 wick_frac=0.7592590303250794 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.461533947107802 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.2000:wick=0.0750; 2026-10-12:RED:body=-0.0800:wick=0.3450; 2026-10-13:DOJI:body=+0.0000:wick=0.3000; 2026-10-14:RED:body=-0.1300:wick=0.4100; 2026-10-15:GREEN:body=+0.5800:wick=0.3000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.64 vs prior_export=14.64 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.42 | **GOOD** |
| `B13_short_float` | 11.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=122.26000213623047  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.09 on 2026-10-15; prev RSI=55.38 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=4.006 (G=6.2900 R=1.5700); 2026-10-14:RED:O=116.2000,C=114.6300,body=-1.5700,vol=559019.0; 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=2.617 (Gvol=1462982 Rvol=559019); 2026-10-14:RED:O=116.2000,C=114.6300,body=-1.5700,vol=559019.0; 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0 | **GOOD** |
| `A07_rvol` | RVOL=1.633 on 2026-10-15: today_vol=1462982 / avg20=895921 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.416 on 2026-10-15 (price=122.2600, mid=111.4875, upper=119.0931, lower=103.8819; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=122.2600 vs SMA50=110.7090 dist=+10.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=111.4875 SMA50=110.7090 SMA80=102.5095 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@84.0300; S2[2026-08-18→2026-09-16] low=2026-08-18@99.4700; S3[2026-09-17→2026-10-15] low=2026-09-28@104.3300 | lows=[84.02999877929688, 99.47000122070312, 104.33000183105469] span=24.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=1.0413907222029242 wick_frac=-0.04139072220292418 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.5547698512403824 wick_frac=0.44523014875961764 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.006370788646293 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-1.4600:wick=2.4457; 2026-10-12:RED:body=-1.9000:wick=2.3450; 2026-10-13:GREEN:body=+1.3200:wick=1.9100; 2026-10-14:RED:body=-1.5700:wick=1.2600; 2026-10-15:GREEN:body=+6.2900:wick=-0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+15**  ·  Software - Application
price=245.16000366210938  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.07 on 2026-10-15; prev RSI=55.48 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=11.7700 R=0.0000); 2026-10-14:GREEN:O=230.3500,C=231.1400,body=+0.7900,vol=1371131.0; 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3793779 Rvol=0); 2026-10-14:GREEN:O=230.3500,C=231.1400,body=+0.7900,vol=1371131.0; 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0 | **GOOD** |
| `A07_rvol` | RVOL=1.187 on 2026-10-15: today_vol=2422648 / avg20=2040941 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.989 on 2026-10-15 (price=245.1600, mid=221.6555, upper=245.4298, lower=197.8812; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=245.1600 vs SMA50=233.3650 dist=+5.05% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=221.66_50=233.36_80=225.10 on 2026-10-15: SMA20=221.6555 SMA50=233.3650 SMA80=225.0982 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@202.3700; S2[2026-08-18→2026-09-16] low=2026-09-09@206.3300; S3[2026-09-17→2026-10-15] low=2026-09-29@199.1801 | lows=[202.3699951171875, 206.3300018310547, 199.1800994873047] span=3.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.5379553925498008 wick_frac=0.4620446074501992 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1500:wick=4.9200; 2026-10-12:RED:body=-1.1630:wick=6.7920; 2026-10-13:RED:body=-2.3100:wick=3.4000; 2026-10-14:GREEN:body=+0.7900:wick=2.9300; 2026-10-15:GREEN:body=+10.9800:wick=1.7350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTEK  ·  score **+15**  ·  Engineering & Construction
price=34.779998779296875  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.50 on 2026-10-15; prev RSI=43.96 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.96@2026-10-14 → 55.50@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.96@2026-10-14 → 55.50@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.96@2026-10-14 → 55.50@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=60.998 (G=1.2200 R=0.0200); 2026-10-14:RED:O=33.6300,C=33.6100,body=-0.0200,vol=2260300.0; 2026-10-15:GREEN:O=33.5600,C=34.7800,body=+1.2200,vol=2312800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.023 (Gvol=2312800 Rvol=2260300); 2026-10-14:RED:O=33.6300,C=33.6100,body=-0.0200,vol=2260300.0; 2026-10-15:GREEN:O=33.5600,C=34.7800,body=+1.2200,vol=2312800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.081 on 2026-10-15: today_vol=2312800 / avg20=2139440 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.462 on 2026-10-15 (price=34.7800, mid=33.9270, upper=35.7746, lower=32.0794; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-15: price=34.7800 vs SMA50=35.2275 dist=-1.27% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=33.93_50=35.23_80=33.57 on 2026-10-15: SMA20=33.9270 SMA50=35.2275 SMA80=33.5692 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@30.3483; S2[2026-08-18→2026-09-16] low=2026-09-09@35.3000; S3[2026-09-17→2026-10-15] low=2026-10-07@32.0200 | lows=[30.348322603720742, 35.29999923706055, 32.02000045776367] span=16.32% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8652464808737551 wick_frac=0.13475351912624484 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.02702772365016032 wick_frac=0.9729722763498396 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=60.99847415601755 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.0700:wick=0.4200; 2026-10-12:GREEN:body=+0.0400:wick=0.4200; 2026-10-13:RED:body=-0.0900:wick=0.6300; 2026-10-14:RED:body=-0.0200:wick=0.7200; 2026-10-15:GREEN:body=+1.2200:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5069.48 | **NEUTRAL** |
| `B04_income` | 435.98 | **GOOD** |
| `B05_profit_margin` | 8.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.33 vs prior_export=40.33 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.71 | **GOOD** |
| `B13_short_float` | 4.35 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+15**  ·  Health Information Services
price=28.239999771118164  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.34 on 2026-10-15; prev RSI=59.22 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.1600 R=0.0000); 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0; 2026-10-15:GREEN:O=26.9800,C=28.2400,body=+1.2600,vol=3547027.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7272027 Rvol=0); 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0; 2026-10-15:GREEN:O=26.9800,C=28.2400,body=+1.2600,vol=3547027.0 | **GOOD** |
| `A07_rvol` | RVOL=1.833 on 2026-10-15: today_vol=3547027 / avg20=1934585 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.308 on 2026-10-15 (price=28.2400, mid=25.8640, upper=27.6805, lower=24.0475; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=28.2400 vs SMA50=25.3060 dist=+11.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=25.8640 SMA50=25.3060 SMA80=24.0052 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@20.1410; S2[2026-08-18→2026-09-16] low=2026-09-10@22.9550; S3[2026-09-17→2026-10-15] low=2026-09-25@24.3250 | lows=[20.141000747680664, 22.954999923706055, 24.325000762939453] span=20.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8391256238221849 wick_frac=0.16087437617781514 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.3000:wick=0.4850; 2026-10-12:RED:body=-0.3500:wick=0.4125; 2026-10-13:RED:body=-0.5700:wick=1.0700; 2026-10-14:GREEN:body=+0.9000:wick=0.2000; 2026-10-15:GREEN:body=+1.2600:wick=0.2050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.68 vs prior_export=33.68 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TENB  ·  score **+15**  ·  Software - Infrastructure
price=39.31999969482422  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.84 on 2026-10-15; prev RSI=52.46 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.005 (G=1.9700 R=1.9600); 2026-10-14:RED:O=40.0800,C=38.1200,body=-1.9600,vol=2410560.0; 2026-10-15:GREEN:O=37.3500,C=39.3200,body=+1.9700,vol=4873720.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=2.022 (Gvol=4873720 Rvol=2410560); 2026-10-14:RED:O=40.0800,C=38.1200,body=-1.9600,vol=2410560.0; 2026-10-15:GREEN:O=37.3500,C=39.3200,body=+1.9700,vol=4873720.0 | **GOOD** |
| `A07_rvol` | RVOL=1.526 on 2026-10-15: today_vol=4873720 / avg20=3193561 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.403 on 2026-10-15 (price=39.3200, mid=37.4483, upper=42.0947, lower=32.8018; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=39.3200 vs SMA50=36.2841 dist=+8.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=37.4483 SMA50=36.2841 SMA80=35.9376 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-30@27.5000; S2[2026-08-18→2026-09-16] low=2026-09-11@29.9700; S3[2026-09-17→2026-10-15] low=2026-09-28@32.5000 | lows=[27.5, 29.969999313354492, 32.5] span=18.18% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.6852178158967391 wick_frac=0.31478218410326086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.7025097691767093 wick_frac=0.2974902308232907 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0051011769102165 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+2.4700:wick=0.1200; 2026-10-12:RED:body=-1.7100:wick=0.2660; 2026-10-13:GREEN:body=+0.9400:wick=0.9000; 2026-10-14:RED:body=-1.9600:wick=0.8300; 2026-10-15:GREEN:body=+1.9700:wick=0.9050 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.28 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1043.54 | **NEUTRAL** |
| `B04_income` | 6.74 | **GOOD** |
| `B05_profit_margin` | 0.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.0 vs prior_export=36.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 16.77 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.28 (this export) | prior_export=8.28 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+15**  ·  Specialty Industrial Machinery
price=248.52000427246094  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.47 on 2026-10-15; prev RSI=50.77 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.914 (G=4.0000 R=2.0900); 2026-10-14:RED:O=248.9200,C=246.8300,body=-2.0900,vol=1378770.0; 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.292 (Gvol=1780759 Rvol=1378770); 2026-10-14:RED:O=248.9200,C=246.8300,body=-2.0900,vol=1378770.0; 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0 | **GOOD** |
| `A07_rvol` | RVOL=1.308 on 2026-10-15: today_vol=1780759 / avg20=1361144 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.026 on 2026-10-15 (price=248.5200, mid=248.3245, upper=255.8084, lower=240.8406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=248.5200 vs SMA50=244.8364 dist=+1.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=248.3245 SMA50=244.8364 SMA80=242.3984 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-17@229.8900; S2[2026-08-18→2026-09-16] low=2026-09-01@229.6000; S3[2026-09-17→2026-10-15] low=2026-09-17@231.8700 | lows=[229.88999938964844, 229.60000610351562, 231.8699951171875] span=0.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.5730662533528914 wick_frac=0.4269337466471086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.42609518002575797 wick_frac=0.573904819974242 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.91387895159524 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.2400:wick=3.1900; 2026-10-12:GREEN:body=+1.3700:wick=2.8450; 2026-10-13:GREEN:body=+0.8400:wick=3.1900; 2026-10-14:RED:body=-2.0900:wick=2.8150; 2026-10-15:GREEN:body=+4.0000:wick=2.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MH  ·  score **+15**  ·  Education & Training Services
price=14.199999809265137  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.07 on 2026-10-15; prev RSI=53.44 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.44@2026-10-14 → 64.07@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.44@2026-10-14 → 64.07@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.44@2026-10-14 → 64.07@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=11.714 (G=0.8200 R=0.0700); 2026-10-14:RED:O=13.3300,C=13.2600,body=-0.0700,vol=240700.0; 2026-10-15:GREEN:O=13.3800,C=14.2000,body=+0.8200,vol=1204300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=5.003 (Gvol=1204300 Rvol=240700); 2026-10-14:RED:O=13.3300,C=13.2600,body=-0.0700,vol=240700.0; 2026-10-15:GREEN:O=13.3800,C=14.2000,body=+0.8200,vol=1204300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.989 on 2026-10-15: today_vol=1204300 / avg20=605482 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.144 on 2026-10-15 (price=14.2000, mid=13.1900, upper=14.0732, lower=12.3068; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=14.2000 vs SMA50=12.9536 dist=+9.62% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=13.1900 SMA50=12.9536 SMA80=11.8916 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@8.9700; S2[2026-08-18→2026-09-16] low=2026-09-10@11.9950; S3[2026-09-17→2026-10-15] low=2026-09-28@12.1700 | lows=[8.970000267028809, 11.994999885559082, 12.170000076293945] span=35.67% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.9111111582061591 wick_frac=0.08888884179384096 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.26414944956868786 wick_frac=0.7358505504313122 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.71433242506812 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:RED:body=-0.0900:wick=0.2550; 2026-10-12:GREEN:body=+0.1200:wick=0.3050; 2026-10-13:RED:body=-0.2500:wick=0.1700; 2026-10-14:RED:body=-0.0700:wick=0.1950; 2026-10-15:GREEN:body=+0.8200:wick=0.0800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=23.59 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2116.97 | **NEUTRAL** |
| `B04_income` | 92.68 | **GOOD** |
| `B05_profit_margin` | 4.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.92 vs prior_export=17.92 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.23 | **BAD** |
| `B13_short_float` | 9.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.59 (this export) | prior_export=23.59 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-16_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.