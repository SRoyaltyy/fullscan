# A+B1 Feature Checklist — 2026-10-09

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-09.csv` · prior export for Δ: `2026-10-08`
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
| 1 | SHEL | +16 | 17 | 1 | 2026-10-06→2026-10-07 | Oil & Gas Integrated |
| 2 | SOLS | +16 | 17 | 1 | 2026-10-06→2026-10-07 | Specialty Chemicals |
| 3 | HASI | +15 | 16 | 1 | 2026-10-06→2026-10-07 | Asset Management |
| 4 | RMD | +15 | 17 | 2 | 2026-10-06→2026-10-07 | Medical Instruments & Supplies |
| 5 | OSCR | +15 | 16 | 1 | 2026-10-06→2026-10-07 | Healthcare Plans |
| 6 | NBIS | +15 | 17 | 2 | 2026-10-06→2026-10-07 | Software - Infrastructure |
| 7 | GFS | +15 | 17 | 2 | 2026-10-06→2026-10-07 | Semiconductors |
| 8 | HHH | +15 | 16 | 1 | 2026-10-06→2026-10-07 | Real Estate - Diversified |
| 9 | STM | +15 | 15 | 0 | 2026-10-06→2026-10-07 | Semiconductors |
| 10 | ADSK | +14 | 15 | 1 | 2026-10-06→2026-10-07 | Software - Application |
| 11 | COST | +14 | 17 | 3 | 2026-10-06→2026-10-07 | Discount Stores |
| 12 | CNP | +14 | 16 | 2 | 2026-10-06→2026-10-07 | Utilities - Regulated Electric |
| 13 | VNT | +14 | 15 | 1 | 2026-10-06→2026-10-07 | Scientific & Technical Instruments |
| 14 | ANET | +14 | 17 | 3 | 2026-10-06→2026-10-07 | Computer Hardware |
| 15 | SLDE | +14 | 17 | 3 | 2026-10-06→2026-10-07 | Insurance - Property & Casualty |

## Full checklist — top 15

### SHEL  ·  score **+16**  ·  Oil & Gas Integrated
price=96.8499984741211  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.06 on 2026-10-07; prev RSI=63.25 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.25@2026-10-06 → 59.06@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.25@2026-10-06 → 59.06@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.25@2026-10-06 → 59.06@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=2.398 (G=1.3550 R=0.5650); 2026-10-06:GREEN:O=96.2650,C=97.6200,body=+1.3550,vol=8158923.0; 2026-10-07:RED:O=97.4150,C=96.8500,body=-0.5650,vol=4925432.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.656 (Gvol=8158923 Rvol=4925432); 2026-10-06:GREEN:O=96.2650,C=97.6200,body=+1.3550,vol=8158923.0; 2026-10-07:RED:O=97.4150,C=96.8500,body=-0.5650,vol=4925432.0 | **GOOD** |
| `A07_rvol` | RVOL=0.767 on 2026-10-07: today_vol=4925432 / avg20=6424299 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.388 on 2026-10-07 (price=96.8500, mid=95.8835, upper=98.3740, lower=93.3930; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=96.8500 vs SMA50=93.0421 dist=+4.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-07: SMA20=95.8835 SMA50=93.0421 SMA80=88.6998 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-07-10@80.7667; S2[2026-08-10→2026-09-08] low=2026-08-10@87.9096; S3[2026-09-09→2026-10-07] low=2026-09-22@92.4700 | lows=[80.76667254889078, 87.9095701030474, 92.47000122070312] span=14.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.6302336722201523 wick_frac=0.36976632777984775 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.3717129534354938 wick_frac=0.6282870465645062 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3982256670627633 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+1.2400:wick=0.1250; 2026-10-02:GREEN:body=+1.2000:wick=0.2100; 2026-10-05:GREEN:body=+0.4300:wick=1.7600; 2026-10-06:GREEN:body=+1.3550:wick=0.7950; 2026-10-07:RED:body=-0.5650:wick=0.9550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-1.3499999999999943 (now=103.64 vs prior_export=104.99 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.45 | **GOOD** |
| `B13_short_float` | 0.7 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLS  ·  score **+16**  ·  Specialty Chemicals
price=60.36000061035156  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.83 on 2026-10-07; prev RSI=58.13 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.13@2026-10-06 → 54.83@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.13@2026-10-06 → 54.83@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.13@2026-10-06 → 54.83@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=6.313 (G=3.0300 R=0.4800); 2026-10-06:GREEN:O=58.2400,C=61.2700,body=+3.0300,vol=2599801.0; 2026-10-07:RED:O=60.8400,C=60.3600,body=-0.4800,vol=1701577.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.528 (Gvol=2599801 Rvol=1701577); 2026-10-06:GREEN:O=58.2400,C=61.2700,body=+3.0300,vol=2599801.0; 2026-10-07:RED:O=60.8400,C=60.3600,body=-0.4800,vol=1701577.0 | **GOOD** |
| `A07_rvol` | RVOL=0.795 on 2026-10-07: today_vol=1701577 / avg20=2139916 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.622 on 2026-10-07 (price=60.3600, mid=58.1735, upper=61.6867, lower=54.6603; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=60.3600 vs SMA50=59.4596 dist=+1.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-07: SMA20=58.1735 SMA50=59.4596 SMA80=64.4524 | **BAD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-07 (63 bars); S1[2026-07-08→2026-08-07] low=2026-07-29@53.8700; S2[2026-08-10→2026-09-08] low=2026-08-24@54.5600; S3[2026-09-09→2026-10-07] low=2026-10-01@54.7850 | lows=[53.869998931884766, 54.560001373291016, 54.78499984741211] span=1.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.9498432864288798 wick_frac=0.05015671357112022 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.3076917433884752 wick_frac=0.6923082566115247 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.312503476940928 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+0.9800:wick=1.2550; 2026-10-02:RED:body=-0.0200:wick=1.3100; 2026-10-05:GREEN:body=+0.7300:wick=1.2950; 2026-10-06:GREEN:body=+3.0300:wick=0.1600; 2026-10-07:RED:body=-0.4800:wick=1.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.74 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.43 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4095.0 | **NEUTRAL** |
| `B04_income` | 210.0 | **GOOD** |
| `B05_profit_margin` | 5.13 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0300000000000011 (now=80.25 vs prior_export=79.22 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.91 | **GOOD** |
| `B13_short_float` | 3.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.74 (this export) | prior_export=13.74 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.43 (this export) | prior_export=6.43 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HASI  ·  score **+15**  ·  Asset Management
price=36.349998474121094  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=41.74 on 2026-10-07; prev RSI=43.59 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.59@2026-10-06 → 41.74@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.59@2026-10-06 → 41.74@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.59@2026-10-06 → 41.74@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=11.999 (G=0.6000 R=0.0500); 2026-10-06:GREEN:O=35.9700,C=36.5700,body=+0.6000,vol=1237495.0; 2026-10-07:RED:O=36.4000,C=36.3500,body=-0.0500,vol=963827.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.284 (Gvol=1237495 Rvol=963827); 2026-10-06:GREEN:O=35.9700,C=36.5700,body=+0.6000,vol=1237495.0; 2026-10-07:RED:O=36.4000,C=36.3500,body=-0.0500,vol=963827.0 | **GOOD** |
| `A07_rvol` | RVOL=0.881 on 2026-10-07: today_vol=963827 / avg20=1094516 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.178 on 2026-10-07 (price=36.3500, mid=36.5385, upper=37.5987, lower=35.4783; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-07: price=36.3500 vs SMA50=38.5484 dist=-5.70% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=36.54_50=38.55_80=38.40 on 2026-10-07: SMA20=36.5385 SMA50=38.5484 SMA80=38.3994 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-07-08@37.1200; S2[2026-08-10→2026-09-08] low=2026-09-04@37.6400; S3[2026-09-09→2026-10-07] low=2026-09-29@35.3700 | lows=[37.119998931884766, 37.63999938964844, 35.369998931884766] span=6.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=1.1111064016162986 wick_frac=-0.11110640161629862 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.07194686865360338 wick_frac=0.9280531313463967 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.999237107110162 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+0.4200:wick=0.3000; 2026-10-02:GREEN:body=+0.2000:wick=0.5350; 2026-10-05:GREEN:body=+0.1500:wick=0.9800; 2026-10-06:GREEN:body=+0.6000:wick=-0.0600; 2026-10-07:RED:body=-0.0500:wick=0.6450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.66 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 462.89 | **NEUTRAL** |
| `B04_income` | 83.73 | **GOOD** |
| `B05_profit_margin` | 18.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.11999999999999744 (now=49.33 vs prior_export=49.21 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.27 | **GOOD** |
| `B13_short_float` | 11.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.9 (this export) | prior_export=7.9 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RMD  ·  score **+15**  ·  Medical Instruments & Supplies
price=225.97999572753906  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.64 on 2026-10-07; prev RSI=45.72 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.72@2026-10-06 → 54.64@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.72@2026-10-06 → 54.64@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.72@2026-10-06 → 54.64@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=1.156 (G=3.7100 R=3.2100); 2026-10-06:RED:O=223.9000,C=220.6900,body=-3.2100,vol=942516.0; 2026-10-07:GREEN:O=222.2700,C=225.9800,body=+3.7100,vol=1101663.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.169 (Gvol=1101663 Rvol=942516); 2026-10-06:RED:O=223.9000,C=220.6900,body=-3.2100,vol=942516.0; 2026-10-07:GREEN:O=222.2700,C=225.9800,body=+3.7100,vol=1101663.0 | **GOOD** |
| `A07_rvol` | RVOL=0.942 on 2026-10-07: today_vol=1101663 / avg20=1169757 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.490 on 2026-10-07 (price=225.9800, mid=222.9655, upper=229.1141, lower=216.8169; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=225.9800 vs SMA50=224.4296 dist=+0.69% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=222.97_50=224.43_80=214.75 on 2026-10-07: SMA20=222.9655 SMA50=224.4296 SMA80=214.7494 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-07-23@190.2100; S2[2026-08-10→2026-09-08] low=2026-08-10@210.5800; S3[2026-09-09→2026-10-07] low=2026-09-10@215.8000 | lows=[190.2100067138672, 210.5800018310547, 215.8000030517578] span=13.45% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.536126074675475 wick_frac=0.463873925324525 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.6091078077678111 wick_frac=0.39089219223218885 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1557636545134762 need>1.4; red_wick_gt_green=False 5d trail=2026-10-01:GREEN:body=+0.4000:wick=4.7400; 2026-10-02:RED:body=-2.7700:wick=2.9000; 2026-10-05:GREEN:body=+4.4200:wick=0.4200; 2026-10-06:RED:body=-3.2100:wick=2.0600; 2026-10-07:GREEN:body=+3.7100:wick=3.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.98 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.16 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5653.44 | **NEUTRAL** |
| `B04_income` | 1523.29 | **GOOD** |
| `B05_profit_margin` | 26.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 262.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.3500000000000227 (now=262.31 vs prior_export=260.96 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.1499999999999999 (now=-2.97 vs prior=-3.12 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.75 | **GOOD** |
| `B13_short_float` | 9.94 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.16 (this export) | prior_export=0.16 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OSCR  ·  score **+15**  ·  Healthcare Plans
price=32.90999984741211  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.34 on 2026-10-07; prev RSI=56.87 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.87@2026-10-06 → 59.34@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.87@2026-10-06 → 59.34@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.87@2026-10-06 → 59.34@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=4.632 (G=0.8800 R=0.1900); 2026-10-06:RED:O=32.5300,C=32.3400,body=-0.1900,vol=4859454.0; 2026-10-07:GREEN:O=32.0300,C=32.9100,body=+0.8800,vol=5418230.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.115 (Gvol=5418230 Rvol=4859454); 2026-10-06:RED:O=32.5300,C=32.3400,body=-0.1900,vol=4859454.0; 2026-10-07:GREEN:O=32.0300,C=32.9100,body=+0.8800,vol=5418230.0 | **GOOD** |
| `A07_rvol` | RVOL=0.984 on 2026-10-07: today_vol=5418230 / avg20=5508656 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.556 on 2026-10-07 (price=32.9100, mid=31.3890, upper=34.1248, lower=28.6532; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=32.9100 vs SMA50=30.9576 dist=+6.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-07: SMA20=31.3890 SMA50=30.9576 SMA80=30.5433 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-08-07@25.3700; S2[2026-08-10→2026-09-08] low=2026-08-11@27.0850; S3[2026-09-09→2026-10-07] low=2026-09-24@28.3000 | lows=[25.3700008392334, 27.084999084472656, 28.299999237060547] span=11.55% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.5146207831956918 wick_frac=0.4853792168043082 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.23173375765358345 wick_frac=0.7682662423464165 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.631618045656233 need>1.4; red_wick_gt_green=False 5d trail=2026-10-01:RED:body=-0.7950:wick=0.5330; 2026-10-02:GREEN:body=+1.1200:wick=0.3600; 2026-10-05:GREEN:body=+1.4500:wick=0.3750; 2026-10-06:RED:body=-0.1900:wick=0.6299; 2026-10-07:GREEN:body=+0.8800:wick=0.8300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=172.95 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.25 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 15318.63 | **NEUTRAL** |
| `B04_income` | 550.74 | **GOOD** |
| `B05_profit_margin` | 3.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.36 vs prior_export=35.36 on finviz_2026-10-08) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | -6.06 | **BAD** |
| `B11_insider_tx_delta` | delta=1.5200000000000005 (now=-6.06 vs prior=-7.58 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 8.97 | **GOOD** |
| `B13_short_float` | 7.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=172.95 (this export) | prior_export=172.95 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.25 (this export) | prior_export=1.25 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NBIS  ·  score **+15**  ·  Software - Infrastructure
price=237.14999389648438  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.92 on 2026-10-07; prev RSI=58.74 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.74@2026-10-06 → 52.92@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.74@2026-10-06 → 52.92@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.74@2026-10-06 → 52.92@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=2.442 (G=12.6500 R=5.1800); 2026-10-06:GREEN:O=237.2200,C=249.8700,body=+12.6500,vol=21263376.0; 2026-10-07:RED:O=242.3300,C=237.1500,body=-5.1800,vol=17614180.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.207 (Gvol=21263376 Rvol=17614180); 2026-10-06:GREEN:O=237.2200,C=249.8700,body=+12.6500,vol=21263376.0; 2026-10-07:RED:O=242.3300,C=237.1500,body=-5.1800,vol=17614180.0 | **GOOD** |
| `A07_rvol` | RVOL=1.150 on 2026-10-07: today_vol=17614180 / avg20=15314614 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.315 on 2026-10-07 (price=237.1500, mid=229.9627, upper=252.8142, lower=207.1113; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=237.1500 vs SMA50=222.3365 dist=+6.66% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=229.96_50=222.34_80=223.82 on 2026-10-07: SMA20=229.9627 SMA50=222.3365 SMA80=223.8163 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-07-29@145.8000; S2[2026-08-10→2026-09-08] low=2026-08-10@183.1100; S3[2026-09-09→2026-10-07] low=2026-09-14@203.8700 | lows=[145.8000030517578, 183.11000061035156, 203.8699951171875] span=39.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.6995903873514585 wick_frac=0.30040961264854155 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.5401470190456491 wick_frac=0.4598529809543509 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.442080023094348 need>1.4; red_wick_gt_green=False 5d trail=2026-10-01:RED:body=-3.9000:wick=7.2600; 2026-10-02:GREEN:body=+7.0500:wick=5.9483; 2026-10-05:RED:body=-11.4600:wick=2.2200; 2026-10-06:GREEN:body=+12.6500:wick=5.4320; 2026-10-07:RED:body=-5.1800:wick=4.4100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=83.36 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1355.1 | **NEUTRAL** |
| `B04_income` | 61.6 | **GOOD** |
| `B05_profit_margin` | 4.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 289.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.2599999999999909 (now=289.45 vs prior_export=289.19 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -1.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.9200000000000002 (now=-1.82 vs prior=-2.74 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 26.28 | **GOOD** |
| `B13_short_float` | 20.98 | **GOOD** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=83.36 (this export) | prior_export=83.36 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GFS  ·  score **+15**  ·  Semiconductors
price=48.06999969482422  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.47 on 2026-10-07; prev RSI=52.54 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.54@2026-10-06 → 50.47@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.54@2026-10-06 → 50.47@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.54@2026-10-06 → 50.47@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=4.100 (G=0.8200 R=0.2000); 2026-10-06:RED:O=48.8300,C=48.6300,body=-0.2000,vol=2404749.0; 2026-10-07:GREEN:O=47.2500,C=48.0700,body=+0.8200,vol=2966217.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.233 (Gvol=2966217 Rvol=2404749); 2026-10-06:RED:O=48.8300,C=48.6300,body=-0.2000,vol=2404749.0; 2026-10-07:GREEN:O=47.2500,C=48.0700,body=+0.8200,vol=2966217.0 | **GOOD** |
| `A07_rvol` | RVOL=0.786 on 2026-10-07: today_vol=2966217 / avg20=3774702 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.233 on 2026-10-07 (price=48.0700, mid=47.1275, upper=51.1688, lower=43.0862; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=48.0700 vs SMA50=47.9182 dist=+0.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-07: SMA20=47.1275 SMA50=47.9182 SMA80=56.1486 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-07-29@46.3900; S2[2026-08-10→2026-09-08] low=2026-09-03@42.7000; S3[2026-09-09→2026-10-07] low=2026-09-15@42.1500 | lows=[46.38999938964844, 42.70000076293945, 42.150001525878906] span=10.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.7961938203287626 wick_frac=0.20380617967123735 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.21276707992614086 wick_frac=0.7872329200738591 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.099982833927788 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+0.7600:wick=0.6200; 2026-10-02:GREEN:body=+0.1700:wick=0.9931; 2026-10-05:RED:body=-0.3650:wick=1.3320; 2026-10-06:RED:body=-0.2000:wick=0.7400; 2026-10-07:GREEN:body=+0.8200:wick=0.2099 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.28 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.17 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6938.0 | **NEUTRAL** |
| `B04_income` | 716.0 | **GOOD** |
| `B05_profit_margin` | 10.32 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 75.85 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.4499999999999886 (now=75.85 vs prior_export=74.4 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 5.22 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.28 (this export) | prior_export=6.28 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.17 (this export) | prior_export=1.17 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HHH  ·  score **+15**  ·  Real Estate - Diversified
price=71.8499984741211  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.77 on 2026-10-07; prev RSI=64.26 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.26@2026-10-06 → 59.77@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.26@2026-10-06 → 59.77@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.26@2026-10-06 → 59.77@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=3.000 (G=1.3500 R=0.4500); 2026-10-06:GREEN:O=71.9300,C=73.2800,body=+1.3500,vol=688197.0; 2026-10-07:RED:O=72.3000,C=71.8500,body=-0.4500,vol=840702.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=0.819 (Gvol=688197 Rvol=840702); 2026-10-06:GREEN:O=71.9300,C=73.2800,body=+1.3500,vol=688197.0; 2026-10-07:RED:O=72.3000,C=71.8500,body=-0.4500,vol=840702.0 | **BAD** |
| `A07_rvol` | RVOL=0.910 on 2026-10-07: today_vol=840702 / avg20=923997 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.459 on 2026-10-07 (price=71.8500, mid=67.0655, upper=77.4977, lower=56.6333; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=71.8500 vs SMA50=66.2818 dist=+8.40% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=67.07_50=66.28_80=67.61 on 2026-10-07: SMA20=67.0655 SMA50=66.2818 SMA80=67.6098 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-08-03@63.8300; S2[2026-08-10→2026-09-08] low=2026-09-04@63.0000; S3[2026-09-09→2026-10-07] low=2026-09-18@60.0900 | lows=[63.83000183105469, 63.0, 60.09000015258789] span=6.22% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.5882605212153047 wick_frac=0.4117394787846953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.3474943736817919 wick_frac=0.6525056263182081 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.999966091924792 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:RED:body=-1.4700:wick=2.1100; 2026-10-02:RED:body=-1.5800:wick=1.5850; 2026-10-05:GREEN:body=+0.1400:wick=1.3247; 2026-10-06:GREEN:body=+1.3500:wick=0.9449; 2026-10-07:RED:body=-0.4500:wick=0.8450 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=170.05 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=139.3 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2334.65 | **NEUTRAL** |
| `B04_income` | 292.1 | **GOOD** |
| `B05_profit_margin` | 12.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.67 vs prior_export=89.67 on finviz_2026-10-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 1.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.06999999999999984 (now=1.67 vs prior=1.6 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=170.05 (this export) | prior_export=170.05 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=139.3 (this export) | prior_export=139.3 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### STM  ·  score **+15**  ·  Semiconductors
price=56.18000030517578  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.88 on 2026-10-07; prev RSI=69.49 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.49@2026-10-06 → 58.88@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.49@2026-10-06 → 58.88@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.49@2026-10-06 → 58.88@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7000 R=0.0000); 2026-10-06:GREEN:O=57.9600,C=58.7200,body=+0.7600,vol=8925464.0; 2026-10-07:GREEN:O=55.2400,C=56.1800,body=+0.9400,vol=9534067.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=99.000 (Gvol=18459531 Rvol=0); 2026-10-06:GREEN:O=57.9600,C=58.7200,body=+0.7600,vol=8925464.0; 2026-10-07:GREEN:O=55.2400,C=56.1800,body=+0.9400,vol=9534067.0 | **GOOD** |
| `A07_rvol` | RVOL=1.068 on 2026-10-07: today_vol=9534067 / avg20=8930478 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.587 on 2026-10-07 (price=56.1800, mid=52.4245, upper=58.8197, lower=46.0293; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=56.1800 vs SMA50=52.1168 dist=+7.80% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=52.42_50=52.12_80=58.50 on 2026-10-07: SMA20=52.4245 SMA50=52.1168 SMA80=58.4975 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-07 (63 bars); S1[2026-07-09→2026-08-07] low=2026-07-29@47.8800; S2[2026-08-10→2026-09-08] low=2026-09-01@48.4800; S3[2026-09-09→2026-10-07] low=2026-09-16@47.5300 | lows=[47.880001068115234, 48.47999954223633, 47.529998779296875] span=2.00% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.610640546264243 wick_frac=0.389359453735757 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-01:GREEN:body=+0.7800:wick=0.3800; 2026-10-02:GREEN:body=+1.9100:wick=0.2600; 2026-10-05:GREEN:body=+1.0800:wick=0.5150; 2026-10-06:GREEN:body=+0.7600:wick=0.8595; 2026-10-07:GREEN:body=+0.9400:wick=0.3100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.85 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13085.46 | **NEUTRAL** |
| `B04_income` | 464.75 | **GOOD** |
| `B05_profit_margin` | 3.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.4 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.1700000000000017 (now=78.4 vs prior_export=78.23 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.68 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 118.35 | **GOOD** |
| `B13_short_float` | 1.63 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.85 (this export) | prior_export=16.85 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+14**  ·  Software - Application
price=235.0500030517578  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.97 on 2026-10-07; prev RSI=57.64 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.64@2026-10-06 → 59.97@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.64@2026-10-06 → 59.97@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.64@2026-10-06 → 59.97@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=12.9900 R=0.0000); 2026-10-06:GREEN:O=221.9200,C=231.2800,body=+9.3600,vol=2160760.0; 2026-10-07:GREEN:O=231.4200,C=235.0500,body=+3.6300,vol=2243226.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4403986 Rvol=0); 2026-10-06:GREEN:O=221.9200,C=231.2800,body=+9.3600,vol=2160760.0; 2026-10-07:GREEN:O=231.4200,C=235.0500,body=+3.6300,vol=2243226.0 | **GOOD** |
| `A07_rvol` | RVOL=1.008 on 2026-10-07: today_vol=2243226 / avg20=2225902 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.062 on 2026-10-07 (price=235.0500, mid=217.1140, upper=234.0055, lower=200.2225; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-07: price=235.0500 vs SMA50=233.7716 dist=+0.55% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=217.11_50=233.77_80=222.71 on 2026-10-07: SMA20=217.1140 SMA50=233.7716 SMA80=222.7121 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-07-09@198.0100; S2[2026-08-10→2026-09-08] low=2026-09-08@208.0900; S3[2026-09-09→2026-10-07] low=2026-09-29@199.1801 | lows=[198.00999450683594, 208.08999633789062, 199.1800994873047] span=5.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.7223070818816024 wick_frac=0.2776929181183976 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-01:RED:body=-2.6100:wick=5.0100; 2026-10-02:RED:body=-1.0000:wick=2.6200; 2026-10-05:GREEN:body=+2.0150:wick=6.7050; 2026-10-06:GREEN:body=+9.3600:wick=0.4850; 2026-10-07:GREEN:body=+3.6300:wick=3.7200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### COST  ·  score **+14**  ·  Discount Stores
price=942.25  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.92 on 2026-10-07; prev RSI=58.28 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.28@2026-10-06 → 60.92@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.28@2026-10-06 → 60.92@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.28@2026-10-06 → 60.92@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=15.7700 R=0.0000); 2026-10-06:GREEN:O=921.3200,C=935.6800,body=+14.3600,vol=1786429.0; 2026-10-07:GREEN:O=940.8400,C=942.2500,body=+1.4100,vol=1751820.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3538249 Rvol=0); 2026-10-06:GREEN:O=921.3200,C=935.6800,body=+14.3600,vol=1786429.0; 2026-10-07:GREEN:O=940.8400,C=942.2500,body=+1.4100,vol=1751820.0 | **GOOD** |
| `A07_rvol` | RVOL=0.739 on 2026-10-07: today_vol=1751820 / avg20=2369174 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.070 on 2026-10-07 (price=942.2500, mid=911.3557, upper=940.2295, lower=882.4820; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-07: price=942.2500 vs SMA50=932.0941 dist=+1.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-07: SMA20=911.3557 SMA50=932.0941 SMA80=935.9212 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-07-10@905.7699; S2[2026-08-10→2026-09-08] low=2026-09-08@906.1000; S3[2026-09-09→2026-10-07] low=2026-09-25@883.1000 | lows=[905.7699043838991, 906.0999755859375, 883.0999755859375] span=2.60% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.5079568394081238 wick_frac=0.49204316059187625 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+4.0700:wick=15.6500; 2026-10-02:RED:body=-0.5300:wick=10.8700; 2026-10-05:GREEN:body=+4.7300:wick=5.8505; 2026-10-06:GREEN:body=+14.3600:wick=1.8750; 2026-10-07:GREEN:body=+1.4100:wick=9.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.21 (current export asof; earnings_date=9/24/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.8 (current export; earnings_date=9/24/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 303154.0 | **NEUTRAL** |
| `B04_income` | 9226.0 | **GOOD** |
| `B05_profit_margin` | 3.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 1074.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.0 (now=1074.1 vs prior_export=1070.1 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -0.3 | **BAD** |
| `B11_insider_tx_delta` | delta=0.43 (now=-0.3 vs prior=-0.73 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.61 | **GOOD** |
| `B13_short_float` | 1.66 | **NEUTRAL** |
| `B14_earnings_date` | 9/24/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.21 (this export) | prior_export=3.21 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.8 (this export) | prior_export=0.8 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNP  ·  score **+14**  ·  Utilities - Regulated Electric
price=38.529998779296875  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.43 on 2026-10-07; prev RSI=52.77 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.77@2026-10-06 → 52.43@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.77@2026-10-06 → 52.43@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.77@2026-10-06 → 52.43@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8600 R=0.0000); 2026-10-06:GREEN:O=38.0600,C=38.5600,body=+0.5000,vol=6478319.0; 2026-10-07:GREEN:O=38.1700,C=38.5300,body=+0.3600,vol=9916836.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=99.000 (Gvol=16395155 Rvol=0); 2026-10-06:GREEN:O=38.0600,C=38.5600,body=+0.5000,vol=6478319.0; 2026-10-07:GREEN:O=38.1700,C=38.5300,body=+0.3600,vol=9916836.0 | **GOOD** |
| `A07_rvol` | RVOL=1.422 on 2026-10-07: today_vol=9916836 / avg20=6975954 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.431 on 2026-10-07 (price=38.5300, mid=37.8415, upper=39.4400, lower=36.2430; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-07: price=38.5300 vs SMA50=39.3118 dist=-1.99% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-07: SMA20=37.8415 SMA50=39.3118 SMA80=40.9522 | **BAD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-07 (63 bars); S1[2026-07-08→2026-08-07] low=2026-08-05@40.2700; S2[2026-08-10→2026-09-08] low=2026-08-24@38.4500; S3[2026-09-09→2026-10-07] low=2026-09-29@36.3700 | lows=[40.27000045776367, 38.45000076293945, 36.369998931884766] span=10.72% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.6704103591787856 wick_frac=0.3295896408212144 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+0.5600:wick=0.3600; 2026-10-02:GREEN:body=+0.3600:wick=0.2300; 2026-10-05:RED:body=-0.0900:wick=0.6100; 2026-10-06:GREEN:body=+0.5000:wick=0.2350; 2026-10-07:GREEN:body=+0.3600:wick=0.1850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.21 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9620.0 | **NEUTRAL** |
| `B04_income` | 1117.0 | **GOOD** |
| `B05_profit_margin` | 11.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7999999999999972 (now=45.47 vs prior_export=44.67 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 7.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.21 (this export) | prior_export=7.21 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+14**  ·  Scientific & Technical Instruments
price=32.27000045776367  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.31 on 2026-10-07; prev RSI=58.96 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.96@2026-10-06 → 53.31@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.96@2026-10-06 → 53.31@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.96@2026-10-06 → 53.31@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=6.500 (G=0.7800 R=0.1200); 2026-10-06:GREEN:O=32.0600,C=32.8400,body=+0.7800,vol=1608185.0; 2026-10-07:RED:O=32.3900,C=32.2700,body=-0.1200,vol=1430181.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.124 (Gvol=1608185 Rvol=1430181); 2026-10-06:GREEN:O=32.0600,C=32.8400,body=+0.7800,vol=1608185.0; 2026-10-07:RED:O=32.3900,C=32.2700,body=-0.1200,vol=1430181.0 | **GOOD** |
| `A07_rvol` | RVOL=1.053 on 2026-10-07: today_vol=1430181 / avg20=1357861 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.620 on 2026-10-07 (price=32.2700, mid=31.6625, upper=32.6422, lower=30.6827; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-07: price=32.2700 vs SMA50=32.5022 dist=-0.71% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=31.66_50=32.50_80=31.40 on 2026-10-07: SMA20=31.6625 SMA50=32.5022 SMA80=31.3968 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-07-08@28.1500; S2[2026-08-10→2026-09-08] low=2026-09-01@31.6000; S3[2026-09-09→2026-10-07] low=2026-09-24@30.4250 | lows=[28.149999618530273, 31.600000381469727, 30.424999237060547] span=12.26% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.8018902887396441 wick_frac=0.1981097112603559 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.12847824932711982 wick_frac=0.8715217506728802 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.500047684140255 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+0.3700:wick=0.8550; 2026-10-02:RED:body=-0.4300:wick=0.3600; 2026-10-05:GREEN:body=+0.2100:wick=0.4750; 2026-10-06:GREEN:body=+0.7800:wick=0.1927; 2026-10-07:RED:body=-0.1200:wick=0.8140 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.57 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.28 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3068.3 | **NEUTRAL** |
| `B04_income` | 348.0 | **GOOD** |
| `B05_profit_margin` | 11.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.21999999999999886 (now=40.22 vs prior_export=40.0 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.96 | **GOOD** |
| `B13_short_float` | 8.75 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.57 (this export) | prior_export=10.57 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.28 (this export) | prior_export=1.28 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+14**  ·  Computer Hardware
price=215.8300018310547  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.17 on 2026-10-07; prev RSI=66.82 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.82@2026-10-06 → 67.17@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.82@2026-10-06 → 67.17@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.82@2026-10-06 → 67.17@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=11.8900 R=0.0000); 2026-10-06:GREEN:O=208.0000,C=215.3600,body=+7.3600,vol=6854425.0; 2026-10-07:GREEN:O=211.3000,C=215.8300,body=+4.5300,vol=4987941.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=99.000 (Gvol=11842366 Rvol=0); 2026-10-06:GREEN:O=208.0000,C=215.3600,body=+7.3600,vol=6854425.0; 2026-10-07:GREEN:O=211.3000,C=215.8300,body=+4.5300,vol=4987941.0 | **GOOD** |
| `A07_rvol` | RVOL=1.029 on 2026-10-07: today_vol=4987941 / avg20=4847731 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.915 on 2026-10-07 (price=215.8300, mid=202.6660, upper=217.0522, lower=188.2798; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-07: price=215.8300 vs SMA50=195.8894 dist=+10.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-07: SMA20=202.6660 SMA50=195.8894 SMA80=186.5429 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-07 (63 bars); S1[2026-07-10→2026-08-07] low=2026-07-29@156.8400; S2[2026-08-10→2026-09-08] low=2026-09-03@181.2700; S3[2026-09-09→2026-10-07] low=2026-09-14@186.1100 | lows=[156.83999633789062, 181.27000427246094, 186.11000061035156] span=18.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.6933753299563538 wick_frac=0.3066246700436463 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:RED:body=-0.2050:wick=5.3740; 2026-10-02:RED:body=-0.9100:wick=3.0623; 2026-10-05:RED:body=-0.8600:wick=3.5379; 2026-10-06:GREEN:body=+7.3600:wick=1.4700; 2026-10-07:GREEN:body=+4.5300:wick=3.6583 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 248.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-1.5699999999999932 (now=248.86 vs prior_export=250.43 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -3.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.23999999999999977 (now=-3.24 vs prior=-3.48 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.66 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SLDE  ·  score **+14**  ·  Insurance - Property & Casualty
price=24.6299991607666  pair=`2026-10-06→2026-10-07`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.81 on 2026-10-07; prev RSI=52.56 on 2026-10-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.56@2026-10-06 → 57.81@2026-10-07 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.56@2026-10-06 → 57.81@2026-10-07 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.56@2026-10-06 → 57.81@2026-10-07 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_body_sum/RED_body_sum=4.500 (G=0.9000 R=0.2000); 2026-10-06:RED:O=24.0900,C=23.8900,body=-0.2000,vol=1353636.0; 2026-10-07:GREEN:O=23.7300,C=24.6300,body=+0.9000,vol=1363109.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-06 + 2026-10-07; ratio=GREEN_vol/RED_vol=1.007 (Gvol=1363109 Rvol=1353636); 2026-10-06:RED:O=24.0900,C=23.8900,body=-0.2000,vol=1353636.0; 2026-10-07:GREEN:O=23.7300,C=24.6300,body=+0.9000,vol=1363109.0 | **GOOD** |
| `A07_rvol` | RVOL=0.938 on 2026-10-07: today_vol=1363109 / avg20=1453686 (avg window 2026-09-09→2026-10-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.209 on 2026-10-07 (price=24.6300, mid=24.1445, upper=26.4655, lower=21.8235; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-07: price=24.6300 vs SMA50=23.0797 dist=+6.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-07: SMA20=24.1445 SMA50=23.0797 SMA80=21.6156 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-07 (63 bars); S1[2026-07-07→2026-08-07] low=2026-07-23@19.1477; S2[2026-08-10→2026-09-08] low=2026-08-11@20.3139; S3[2026-09-09→2026-10-07] low=2026-10-01@22.2550 | lows=[19.14771451817286, 20.313922820890404, 22.2549991607666] span=16.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: GREEN body_frac=0.7725325190939677 wick_frac=0.2274674809060323 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-06+2026-10-07: RED body_frac=0.24844805853307175 wick_frac=0.7515519414669283 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.4999809265864315 need>1.4; red_wick_gt_green=True 5d trail=2026-10-01:GREEN:body=+1.2900:wick=0.1250; 2026-10-02:RED:body=-0.0400:wick=0.4200; 2026-10-05:GREEN:body=+0.3300:wick=0.4400; 2026-10-06:RED:body=-0.2000:wick=0.6050; 2026-10-07:GREEN:body=+0.9000:wick=0.2650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.75 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.66 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1388.8 | **NEUTRAL** |
| `B04_income` | 555.76 | **GOOD** |
| `B05_profit_margin` | 40.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.5 (now=26.0 vs prior_export=26.5 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -11.11 | **BAD** |
| `B11_insider_tx_delta` | delta=-1.6999999999999993 (now=-11.11 vs prior=-9.41 on finviz_2026-10-08) | **BAD** |
| `B12_institutional_transactions` | 9.23 | **GOOD** |
| `B13_short_float` | 9.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.75 (this export) | prior_export=24.75 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.66 (this export) | prior_export=7.66 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-09_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.