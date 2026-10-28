# A+B1 Feature Checklist — 2026-10-28

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-28.csv` · prior export for Δ: `2026-10-27`
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
| 1 | PBR | +17 | 17 | 0 | 2026-10-26→2026-10-27 | Oil & Gas Integrated |
| 2 | MTDR | +17 | 17 | 0 | 2026-10-26→2026-10-27 | Oil & Gas E&P |
| 3 | SONO | +16 | 17 | 1 | 2026-10-26→2026-10-27 | Consumer Electronics |
| 4 | SWKS | +16 | 17 | 1 | 2026-10-26→2026-10-27 | Semiconductors |
| 5 | PANW | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Software - Infrastructure |
| 6 | ATRC | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Medical Instruments & Supplies |
| 7 | CP | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Railroads |
| 8 | ASC | +15 | 17 | 2 | 2026-10-26→2026-10-27 | Marine Shipping |
| 9 | AUPH | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Biotechnology |
| 10 | CON | +15 | 17 | 2 | 2026-10-26→2026-10-27 | Medical Care Facilities |
| 11 | SNPS | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Software - Infrastructure |
| 12 | PBR-A | +14 | 15 | 1 | 2026-10-26→2026-10-27 | Oil & Gas Integrated |
| 13 | BKR | +14 | 17 | 3 | 2026-10-26→2026-10-27 | Oil & Gas Equipment & Services |
| 14 | MCHP | +14 | 15 | 1 | 2026-10-26→2026-10-27 | Semiconductors |
| 15 | JCI | +14 | 16 | 2 | 2026-10-26→2026-10-27 | Building Products & Equipment |

## Full checklist — top 15

### PBR  ·  score **+17**  ·  Oil & Gas Integrated
price=22.299999237060547  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=41.62 on 2026-10-27; prev RSI=42.64 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=2.633 (G=0.7900 R=0.3000); 2026-10-26:GREEN:O=21.7100,C=22.5000,body=+0.7900,vol=102623400.0; 2026-10-27:RED:O=22.6000,C=22.3000,body=-0.3000,vol=37103285.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=2.766 (Gvol=102623400 Rvol=37103285); 2026-10-26:GREEN:O=21.7100,C=22.5000,body=+0.7900,vol=102623400.0; 2026-10-27:RED:O=22.6000,C=22.3000,body=-0.3000,vol=37103285.0 | **GOOD** |
| `A07_rvol` | RVOL=1.313 on 2026-10-27: today_vol=37103285 / avg20=28267864 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.586 on 2026-10-27 (price=22.3000, mid=24.3540, upper=27.8600, lower=20.8480; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=22.3000 vs SMA50=21.7814 dist=+2.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=24.3540 SMA50=21.7814 SMA80=20.4206 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-27@17.5400; S2[2026-08-28→2026-09-28] low=2026-08-28@18.1800; S3[2026-09-29→2026-10-27] low=2026-09-29@20.2400 | lows=[17.540000915527344, 18.18000030517578, 20.239999771118164] span=15.39% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5642865219896921 wick_frac=0.43571347801030785 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.3773598938602573 wick_frac=0.6226401061397426 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.633326339748358 need>1.4; red_wick_gt_green=False 5d trail=2026-10-21:RED:body=-0.2300:wick=0.1500; 2026-10-22:RED:body=-0.3200:wick=0.5845; 2026-10-23:GREEN:body=+0.2000:wick=0.4000; 2026-10-26:GREEN:body=+0.7900:wick=0.6100; 2026-10-27:RED:body=-0.3000:wick=0.4950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MTDR  ·  score **+17**  ·  Oil & Gas E&P
price=59.27000045776367  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.37 on 2026-10-27; prev RSI=65.15 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=2.870 (G=1.5500 R=0.5400); 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0; 2026-10-27:RED:O=59.8100,C=59.2700,body=-0.5400,vol=956924.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=2.115 (Gvol=2024300 Rvol=956924); 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0; 2026-10-27:RED:O=59.8100,C=59.2700,body=-0.5400,vol=956924.0 | **GOOD** |
| `A07_rvol` | RVOL=0.685 on 2026-10-27: today_vol=956924 / avg20=1397966 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.719 on 2026-10-27 (price=59.2700, mid=55.5860, upper=60.7116, lower=50.4604; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=59.2700 vs SMA50=56.5054 dist=+4.89% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=55.59_50=56.51_80=54.32 on 2026-10-27: SMA20=55.5860 SMA50=56.5054 SMA80=54.3241 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-05@46.5316; S2[2026-08-28→2026-09-28] low=2026-09-25@51.1000; S3[2026-09-29→2026-10-27] low=2026-09-29@50.3900 | lows=[46.531587687321455, 51.099998474121094, 50.38999938964844] span=9.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.6431532511194792 wick_frac=0.3568467488805208 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.2991700639729145 wick_frac=0.7008299360270854 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8703640910439536 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.2500:wick=0.8500; 2026-10-22:RED:body=-1.8300:wick=1.2050; 2026-10-23:GREEN:body=+2.7700:wick=0.8400; 2026-10-26:GREEN:body=+1.5500:wick=0.8600; 2026-10-27:RED:body=-0.5400:wick=1.2650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.59 vs prior_export=70.59 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.4 vs prior=0.4 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 11.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+16**  ·  Consumer Electronics
price=17.8700008392334  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.28 on 2026-10-27; prev RSI=55.95 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=5.600 (G=0.2800 R=0.0500); 2026-10-26:RED:O=17.6400,C=17.5900,body=-0.0500,vol=971100.0; 2026-10-27:GREEN:O=17.5900,C=17.8700,body=+0.2800,vol=1748740.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.801 (Gvol=1748740 Rvol=971100); 2026-10-26:RED:O=17.6400,C=17.5900,body=-0.0500,vol=971100.0; 2026-10-27:GREEN:O=17.5900,C=17.8700,body=+0.2800,vol=1748740.0 | **GOOD** |
| `A07_rvol` | RVOL=1.074 on 2026-10-27: today_vol=1748740 / avg20=1627843 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.602 on 2026-10-27 (price=17.8700, mid=17.5033, upper=18.1120, lower=16.8945; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=17.8700 vs SMA50=16.5612 dist=+7.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=17.5033 SMA50=16.5612 SMA80=16.0315 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-30@14.0200; S2[2026-08-28→2026-09-28] low=2026-09-09@14.2000; S3[2026-09-29→2026-10-27] low=2026-10-08@16.7000 | lows=[14.020000457763672, 14.199999809265137, 16.700000762939453] span=19.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5834581983664872 wick_frac=0.41654180163351284 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.09999847412109375 wick_frac=0.9000015258789062 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.600099183642328 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.0700:wick=0.2700; 2026-10-22:GREEN:body=+0.2350:wick=0.0950; 2026-10-23:GREEN:body=+0.0100:wick=0.2700; 2026-10-26:RED:body=-0.0500:wick=0.4500; 2026-10-27:GREEN:body=+0.2800:wick=0.1999 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.43 vs prior=-4.43 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=85.4800033569336  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.90 on 2026-10-27; prev RSI=55.60 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=19.221 (G=1.7300 R=0.0900); 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0; 2026-10-27:GREEN:O=83.7500,C=85.4800,body=+1.7300,vol=2728589.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.002 (Gvol=2728589 Rvol=2724100); 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0; 2026-10-27:GREEN:O=83.7500,C=85.4800,body=+1.7300,vol=2728589.0 | **GOOD** |
| `A07_rvol` | RVOL=0.396 on 2026-10-27: today_vol=2728589 / avg20=6886518 (avg window 2026-09-29→2026-10-26, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.650 on 2026-10-27 (price=85.4800, mid=81.3120, upper=87.7265, lower=74.8975; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=85.4800 vs SMA50=79.2666 dist=+7.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=81.3120 SMA50=79.2666 SMA80=73.2195 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-29@57.5000; S2[2026-08-28→2026-09-28] low=2026-08-28@64.4700; S3[2026-09-29→2026-10-27] low=2026-10-13@73.7600 | lows=[57.5, 64.47000122070312, 73.76000213623047] span=28.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.7505436563496083 wick_frac=0.24945634365039174 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.040910809095606515 wick_frac=0.9590891909043935 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.221412223446638 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.4000:wick=1.4900; 2026-10-22:GREEN:body=+1.9400:wick=0.5700; 2026-10-23:GREEN:body=+1.4800:wick=1.2300; 2026-10-26:RED:body=-0.0900:wick=2.1100; 2026-10-27:GREEN:body=+1.7300:wick=0.5750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+15**  ·  Software - Infrastructure
price=401.70001220703125  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.40 on 2026-10-27; prev RSI=55.26 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.9800 R=0.0000); 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0; 2026-10-27:GREEN:O=390.9900,C=401.7000,body=+10.7100,vol=4768697.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9897297 Rvol=0); 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0; 2026-10-27:GREEN:O=390.9900,C=401.7000,body=+10.7100,vol=4768697.0 | **GOOD** |
| `A07_rvol` | RVOL=0.904 on 2026-10-27: today_vol=4768697 / avg20=5272812 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.325 on 2026-10-27 (price=401.7000, mid=384.8300, upper=436.7802, lower=332.8798; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=401.7000 vs SMA50=370.9942 dist=+8.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=384.8300 SMA50=370.9942 SMA80=362.7036 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-07-30@313.0500; S2[2026-08-28→2026-09-28] low=2026-09-02@321.3900; S3[2026-09-29→2026-10-27] low=2026-10-20@334.7500 | lows=[313.04998779296875, 321.3900146484375, 334.75] span=6.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5301433984108884 wick_frac=0.4698566015891116 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+3.8100:wick=11.0800; 2026-10-22:RED:body=-0.0800:wick=13.9101; 2026-10-23:GREEN:body=+26.2500:wick=3.9599; 2026-10-26:GREEN:body=+6.2700:wick=8.9500; 2026-10-27:GREEN:body=+10.7100:wick=5.8094 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.35 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.74 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11480.0 | **NEUTRAL** |
| `B04_income` | 307.0 | **GOOD** |
| `B05_profit_margin` | 2.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 403.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=403.5 vs prior_export=403.5 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.66 | **GOOD** |
| `B10_insider_transactions` | -2.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.01 vs prior=-2.01 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.96 | **GOOD** |
| `B13_short_float` | 2.7 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.35 (this export) | prior_export=4.35 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.74 (this export) | prior_export=1.74 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATRC  ·  score **+15**  ·  Medical Instruments & Supplies
price=61.5099983215332  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.84 on 2026-10-27; prev RSI=66.09 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.09@2026-10-26 → 66.84@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.09@2026-10-26 → 66.84@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.09@2026-10-26 → 66.84@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.3800 R=0.0000); 2026-10-26:GREEN:O=59.3500,C=61.0800,body=+1.7300,vol=815900.0; 2026-10-27:GREEN:O=60.8600,C=61.5100,body=+0.6500,vol=783200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1599100 Rvol=0); 2026-10-26:GREEN:O=59.3500,C=61.0800,body=+1.7300,vol=815900.0; 2026-10-27:GREEN:O=60.8600,C=61.5100,body=+0.6500,vol=783200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.528 on 2026-10-27: today_vol=783200 / avg20=1482680 (avg window 2026-09-16→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.788 on 2026-10-27 (price=61.5100, mid=57.3375, upper=62.6297, lower=52.0453; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=61.5100 vs SMA50=51.9742 dist=+18.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=57.3375 SMA50=51.9742 SMA80=44.9614 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-27 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@31.9000; S2[2026-08-17→2026-09-15] low=2026-08-17@43.4100; S3[2026-09-16→2026-10-27] low=2026-10-08@51.6300 | lows=[31.899999618530273, 43.40999984741211, 51.630001068115234] span=61.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5624212044148915 wick_frac=0.43757879558510837 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.2300:wick=1.6000; 2026-10-09:GREEN:body=+2.6700:wick=0.7850; 2026-10-12:GREEN:body=+0.9900:wick=0.6900; 2026-10-26:GREEN:body=+1.7300:wick=0.6000; 2026-10-27:GREEN:body=+0.6500:wick=1.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10688.24 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 569.62 | **NEUTRAL** |
| `B04_income` | 10.55 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.33 vs prior_export=53.33 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -3.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.89 vs prior=-3.89 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.44 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10688.24 (this export) | prior_export=10688.24 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=91.12999725341797  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.50 on 2026-10-27; prev RSI=59.00 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.00@2026-10-26 → 62.50@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.00@2026-10-26 → 62.50@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.00@2026-10-26 → 62.50@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=11.000 (G=1.3200 R=0.1200); 2026-10-26:RED:O=89.9400,C=89.8200,body=-0.1200,vol=1317000.0; 2026-10-27:GREEN:O=89.8100,C=91.1300,body=+1.3200,vol=2126600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.615 (Gvol=2126600 Rvol=1317000); 2026-10-26:RED:O=89.9400,C=89.8200,body=-0.1200,vol=1317000.0; 2026-10-27:GREEN:O=89.8100,C=91.1300,body=+1.3200,vol=2126600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.958 on 2026-10-27: today_vol=2126600 / avg20=2220383 (avg window 2026-09-16→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.171 on 2026-10-27 (price=91.1300, mid=86.6150, upper=90.4723, lower=82.7577; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=91.1300 vs SMA50=89.6134 dist=+1.69% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.61_50=89.61_80=89.42 on 2026-10-27: SMA20=86.6150 SMA50=89.6134 SMA80=89.4247 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-27 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-30@87.2000; S2[2026-08-12→2026-09-15] low=2026-09-14@88.0400; S3[2026-09-16→2026-10-27] low=2026-10-08@83.2500 | lows=[87.19999694824219, 88.04000091552734, 83.25] span=5.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.7457639538442308 wick_frac=0.2542360461557693 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.1153871547518615 wick_frac=0.8846128452481385 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.999745692669592 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+1.0800:wick=0.4100; 2026-10-09:RED:body=-0.0100:wick=0.7600; 2026-10-12:GREEN:body=+1.1300:wick=0.3200; 2026-10-26:RED:body=-0.1200:wick=0.9200; 2026-10-27:GREEN:body=+1.3200:wick=0.4500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.75 vs prior_export=100.75 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.2 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASC  ·  score **+15**  ·  Marine Shipping
price=20.420000076293945  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.17 on 2026-10-27; prev RSI=69.99 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.99@2026-10-26 → 66.17@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.99@2026-10-26 → 66.17@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.99@2026-10-26 → 66.17@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=3.154 (G=0.4100 R=0.1300); 2026-10-26:GREEN:O=20.2900,C=20.7000,body=+0.4100,vol=1067000.0; 2026-10-27:RED:O=20.5500,C=20.4200,body=-0.1300,vol=766100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.393 (Gvol=1067000 Rvol=766100); 2026-10-26:GREEN:O=20.2900,C=20.7000,body=+0.4100,vol=1067000.0; 2026-10-27:RED:O=20.5500,C=20.4200,body=-0.1300,vol=766100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.083 on 2026-10-27: today_vol=766100 / avg20=707096 (avg window 2026-09-16→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.901 on 2026-10-27 (price=20.4200, mid=18.8920, upper=20.5882, lower=17.1958; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=20.4200 vs SMA50=18.2146 dist=+12.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=18.8920 SMA50=18.2146 SMA80=17.4132 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-27 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-17@15.1700; S2[2026-08-17→2026-09-15] low=2026-08-28@16.6100; S3[2026-09-16→2026-10-27] low=2026-09-23@17.2000 | lows=[15.170000076293945, 16.610000610351562, 17.200000762939453] span=13.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5 wick_frac=0.5 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.17567459591260218 wick_frac=0.8243254040873979 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.1538653403172088 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.6100:wick=0.2000; 2026-10-09:RED:body=-0.2700:wick=0.2562; 2026-10-12:GREEN:body=+0.0300:wick=0.4700; 2026-10-26:GREEN:body=+0.4100:wick=0.4100; 2026-10-27:RED:body=-0.1300:wick=0.6100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.72 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=34.7 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 368.29 | **NEUTRAL** |
| `B04_income` | 105.58 | **GOOD** |
| `B05_profit_margin` | 28.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.0 vs prior_export=21.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.93 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.93 vs prior=-0.93 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 7.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.72 (this export) | prior_export=6.72 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=34.7 (this export) | prior_export=34.7 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+15**  ·  Biotechnology
price=17.25  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.16 on 2026-10-27; prev RSI=62.64 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.64@2026-10-26 → 65.16@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.64@2026-10-26 → 65.16@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.64@2026-10-26 → 65.16@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3300 R=0.0000); 2026-10-26:GREEN:O=16.9800,C=17.0400,body=+0.0600,vol=794700.0; 2026-10-27:GREEN:O=16.9800,C=17.2500,body=+0.2700,vol=494500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1289200 Rvol=0); 2026-10-26:GREEN:O=16.9800,C=17.0400,body=+0.0600,vol=794700.0; 2026-10-27:GREEN:O=16.9800,C=17.2500,body=+0.2700,vol=494500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.602 on 2026-10-27: today_vol=494500 / avg20=821979 (avg window 2026-09-16→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.934 on 2026-10-27 (price=17.2500, mid=16.4310, upper=17.3080, lower=15.5540; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=17.2500 vs SMA50=16.2139 dist=+6.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=16.4310 SMA50=16.2139 SMA80=16.1402 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-27 (63 bars); S1[2026-07-16→2026-08-14] low=2026-07-31@14.3630; S2[2026-08-17→2026-09-15] low=2026-08-17@15.0700; S3[2026-09-16→2026-10-27] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.069999694824219, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5579349961968874 wick_frac=0.44206500380311264 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-26:GREEN:body=+0.0600:wick=0.1850; 2026-10-27:GREEN:body=+0.2700:wick=0.0400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CON  ·  score **+15**  ·  Medical Care Facilities
price=40.0  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.21 on 2026-10-27; prev RSI=71.23 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 71.23@2026-10-26 → 69.21@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.23@2026-10-26 → 69.21@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 71.23@2026-10-26 → 69.21@2026-10-27 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=4.190 (G=0.8800 R=0.2100); 2026-10-26:GREEN:O=39.3200,C=40.2000,body=+0.8800,vol=1759400.0; 2026-10-27:RED:O=40.2100,C=40.0000,body=-0.2100,vol=1035600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.699 (Gvol=1759400 Rvol=1035600); 2026-10-26:GREEN:O=39.3200,C=40.2000,body=+0.8800,vol=1759400.0; 2026-10-27:RED:O=40.2100,C=40.0000,body=-0.2100,vol=1035600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.856 on 2026-10-27: today_vol=1035600 / avg20=1209216 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.684 on 2026-10-27 (price=40.0000, mid=37.6765, upper=41.0715, lower=34.2815; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=40.0000 vs SMA50=35.9806 dist=+11.17% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=37.6765 SMA50=35.9806 SMA80=34.3438 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-27 (63 bars); S1[2026-07-24→2026-08-27] low=2026-08-06@30.1250; S2[2026-08-28→2026-09-28] low=2026-09-09@33.3200; S3[2026-09-29→2026-10-27] low=2026-10-01@34.0520 | lows=[30.125, 33.31999969482422, 34.051998138427734] span=13.04% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.8627457580211454 wick_frac=0.1372542419788546 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.3043471049707263 wick_frac=0.6956528950292737 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.190499545867393 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.2500:wick=0.4600; 2026-10-22:GREEN:body=+0.9100:wick=0.1500; 2026-10-23:RED:body=-0.5200:wick=0.1900; 2026-10-26:GREEN:body=+0.8800:wick=0.1400; 2026-10-27:RED:body=-0.2100:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.34 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.41 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2287.47 | **NEUTRAL** |
| `B04_income` | 195.03 | **GOOD** |
| `B05_profit_margin` | 8.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.0 vs prior_export=41.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -4.09 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.09 vs prior=-4.09 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 2.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.34 (this export) | prior_export=23.34 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.41 (this export) | prior_export=2.41 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SNPS  ·  score **+15**  ·  Software - Infrastructure
price=474.4200134277344  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.06 on 2026-10-27; prev RSI=51.19 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=38.9300 R=0.0000); 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0; 2026-10-27:GREEN:O=458.6700,C=474.4200,body=+15.7500,vol=2153168.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4963368 Rvol=0); 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0; 2026-10-27:GREEN:O=458.6700,C=474.4200,body=+15.7500,vol=2153168.0 | **GOOD** |
| `A07_rvol` | RVOL=0.828 on 2026-10-27: today_vol=2153168 / avg20=2601318 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.032 on 2026-10-27 (price=474.4200, mid=472.6940, upper=527.2104, lower=418.1776; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=474.4200 vs SMA50=432.7542 dist=+9.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=472.6940 SMA50=432.7542 SMA80=422.2980 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-07-30@370.2000; S2[2026-08-28→2026-09-28] low=2026-09-15@362.5500; S3[2026-09-29→2026-10-27] low=2026-09-29@403.3300 | lows=[370.20001220703125, 362.54998779296875, 403.3299865722656] span=11.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.8419915003977608 wick_frac=0.15800849960223917 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:RED:body=-0.6100:wick=9.0900; 2026-10-22:RED:body=-19.0200:wick=2.9200; 2026-10-23:GREEN:body=+10.7200:wick=5.3800; 2026-10-26:GREEN:body=+23.1800:wick=3.3100; 2026-10-27:GREEN:body=+15.7500:wick=3.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.64 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9408.2 | **NEUTRAL** |
| `B04_income` | 1076.56 | **GOOD** |
| `B05_profit_margin` | 11.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 560.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=560.72 vs prior_export=560.72 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.3 | **GOOD** |
| `B10_insider_transactions` | -1.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.1 vs prior=-1.1 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.91 | **GOOD** |
| `B13_short_float` | 3.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.64 (this export) | prior_export=6.64 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR-A  ·  score **+14**  ·  Oil & Gas Integrated
price=19.760000228881836  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=40.34 on 2026-10-27; prev RSI=41.08 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.08@2026-10-26 → 40.34@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 41.08@2026-10-26 → 40.34@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 41.08@2026-10-26 → 40.34@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=3.030 (G=0.5000 R=0.1650); 2026-10-26:GREEN:O=19.4000,C=19.9000,body=+0.5000,vol=40419700.0; 2026-10-27:RED:O=19.9250,C=19.7600,body=-0.1650,vol=10761110.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=3.756 (Gvol=40419700 Rvol=10761110); 2026-10-26:GREEN:O=19.4000,C=19.9000,body=+0.5000,vol=40419700.0; 2026-10-27:RED:O=19.9250,C=19.7600,body=-0.1650,vol=10761110.0 | **GOOD** |
| `A07_rvol` | RVOL=0.887 on 2026-10-27: today_vol=10761110 / avg20=12125394 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.697 on 2026-10-27 (price=19.7600, mid=21.8515, upper=24.8519, lower=18.8511; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=19.7600 vs SMA50=19.6208 dist=+0.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=21.8515 SMA50=19.6208 SMA80=18.3340 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-13@15.8400; S2[2026-08-28→2026-09-28] low=2026-08-28@16.4600; S3[2026-09-29→2026-10-27] low=2026-09-29@18.3600 | lows=[15.84000015258789, 16.459999084472656, 18.360000610351562] span=15.91% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.38759719723035024 wick_frac=0.6124028027696498 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.3173055056303415 wick_frac=0.6826944943696586 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0303212456795405 need>1.4; red_wick_gt_green=False 5d trail=2026-10-21:RED:body=-0.2300:wick=0.1950; 2026-10-22:RED:body=-0.3500:wick=0.3600; 2026-10-23:GREEN:body=+0.2400:wick=0.3485; 2026-10-26:GREEN:body=+0.5000:wick=0.7900; 2026-10-27:RED:body=-0.1650:wick=0.3550 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.94 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.74 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.51 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.51 vs prior_export=22.51 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.42 | **GOOD** |
| `B13_short_float` | 0.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.94 (this export) | prior_export=12.94 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.74 (this export) | prior_export=6.74 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BKR  ·  score **+14**  ·  Oil & Gas Equipment & Services
price=59.0  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.58 on 2026-10-27; prev RSI=47.59 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.59@2026-10-26 → 58.58@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.59@2026-10-26 → 58.58@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.59@2026-10-26 → 58.58@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.0700 R=0.0000); 2026-10-26:GREEN:O=55.7000,C=56.0100,body=+0.3100,vol=9050300.0; 2026-10-27:GREEN:O=56.2400,C=59.0000,body=+2.7600,vol=13051875.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=22102175 Rvol=0); 2026-10-26:GREEN:O=55.7000,C=56.0100,body=+0.3100,vol=9050300.0; 2026-10-27:GREEN:O=56.2400,C=59.0000,body=+2.7600,vol=13051875.0 | **GOOD** |
| `A07_rvol` | RVOL=2.212 on 2026-10-27: today_vol=13051875 / avg20=5899541 (avg window 2026-09-29→2026-10-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.358 on 2026-10-27 (price=59.0000, mid=55.9745, upper=58.2023, lower=53.7467; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=59.0000 vs SMA50=58.6756 dist=+0.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-27: SMA20=55.9745 SMA50=58.6756 SMA80=58.9415 | **BAD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-07-30@58.5147; S2[2026-08-28→2026-09-28] low=2026-09-16@55.4400; S3[2026-09-29→2026-10-27] low=2026-10-22@52.6000 | lows=[58.51473311629339, 55.439998626708984, 52.599998474121094] span=11.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.6400728261574948 wick_frac=0.35992717384250517 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.3500:wick=0.5510; 2026-10-22:RED:body=-2.1500:wick=1.2700; 2026-10-23:GREEN:body=+2.2400:wick=0.3000; 2026-10-26:GREEN:body=+0.3100:wick=0.8700; 2026-10-27:GREEN:body=+2.7600:wick=-0.0473 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=27.57 (current export asof; earnings_date=7/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.35 (current export; earnings_date=7/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 27725.0 | **NEUTRAL** |
| `B04_income` | 3096.0 | **GOOD** |
| `B05_profit_margin` | 11.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 72.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=72.05 vs prior_export=72.05 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -14.61 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-14.61 vs prior=-14.61 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 2.49 | **NEUTRAL** |
| `B14_earnings_date` | 7/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.57 (this export) | prior_export=27.57 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.35 (this export) | prior_export=3.35 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MCHP  ·  score **+14**  ·  Semiconductors
price=80.16999816894531  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.81 on 2026-10-27; prev RSI=50.85 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.85@2026-10-26 → 59.81@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.85@2026-10-26 → 59.81@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.85@2026-10-26 → 59.81@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.6900 R=0.0000); 2026-10-26:GREEN:O=74.4500,C=75.9000,body=+1.4500,vol=7944900.0; 2026-10-27:GREEN:O=76.9300,C=80.1700,body=+3.2400,vol=11855001.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=19799901 Rvol=0); 2026-10-26:GREEN:O=74.4500,C=75.9000,body=+1.4500,vol=7944900.0; 2026-10-27:GREEN:O=76.9300,C=80.1700,body=+3.2400,vol=11855001.0 | **GOOD** |
| `A07_rvol` | RVOL=1.256 on 2026-10-27: today_vol=11855001 / avg20=9439762 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.639 on 2026-10-27 (price=80.1700, mid=76.4425, upper=82.2752, lower=70.6098; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=80.1700 vs SMA50=75.1234 dist=+6.72% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=76.44_50=75.12_80=77.23 on 2026-10-27: SMA20=76.4425 SMA50=75.1234 SMA80=77.2250 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-03@72.5800; S2[2026-08-28→2026-09-28] low=2026-09-16@68.7500; S3[2026-09-29→2026-10-27] low=2026-10-15@69.7600 | lows=[72.58000183105469, 68.75, 69.76000213623047] span=5.57% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.6957062310061521 wick_frac=0.3042937689938479 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.6000:wick=1.5750; 2026-10-22:RED:body=-0.6400:wick=1.6900; 2026-10-23:RED:body=-0.1600:wick=1.6700; 2026-10-26:GREEN:body=+1.4500:wick=1.0100; 2026-10-27:GREEN:body=+3.2400:wick=0.8000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.59 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.8 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5122.3 | **NEUTRAL** |
| `B04_income` | 367.2 | **GOOD** |
| `B05_profit_margin` | 7.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.26 vs prior_export=110.26 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | -5.62 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.62 vs prior=-5.62 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.19 | **GOOD** |
| `B13_short_float` | 6.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.59 (this export) | prior_export=8.59 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.8 (this export) | prior_export=1.8 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### JCI  ·  score **+14**  ·  Building Products & Equipment
price=155.8800048828125  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.16 on 2026-10-27; prev RSI=48.01 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.01@2026-10-26 → 58.16@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.01@2026-10-26 → 58.16@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.01@2026-10-26 → 58.16@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.7500 R=0.0000); 2026-10-26:GREEN:O=145.0200,C=147.5000,body=+2.4800,vol=4087400.0; 2026-10-27:GREEN:O=153.6100,C=155.8800,body=+2.2700,vol=6058910.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10146310 Rvol=0); 2026-10-26:GREEN:O=145.0200,C=147.5000,body=+2.4800,vol=4087400.0; 2026-10-27:GREEN:O=153.6100,C=155.8800,body=+2.2700,vol=6058910.0 | **GOOD** |
| `A07_rvol` | RVOL=1.767 on 2026-10-27: today_vol=6058910 / avg20=3429048 (avg window 2026-09-29→2026-10-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.425 on 2026-10-27 (price=155.8800, mid=151.7290, upper=161.4867, lower=141.9713; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=155.8800 vs SMA50=146.8524 dist=+6.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=151.7290 SMA50=146.8524 SMA80=146.5160 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-24@140.8000; S2[2026-08-28→2026-09-28] low=2026-09-14@135.7900; S3[2026-09-29→2026-10-27] low=2026-10-23@140.6900 | lows=[140.8000030517578, 135.7899932861328, 140.69000244140625] span=3.69% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5779952650843075 wick_frac=0.4220047349156924 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+1.0300:wick=1.0150; 2026-10-22:RED:body=-5.3800:wick=2.7200; 2026-10-23:GREEN:body=+2.0100:wick=2.2800; 2026-10-26:GREEN:body=+2.4800:wick=0.8600; 2026-10-27:GREEN:body=+2.2700:wick=3.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.02 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.33 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 24995.0 | **NEUTRAL** |
| `B04_income` | 3579.0 | **GOOD** |
| `B05_profit_margin` | 14.32 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 168.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=168.78 vs prior_export=168.78 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | -5.9 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.9 vs prior=-5.9 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.16 | **BAD** |
| `B13_short_float` | 1.66 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.02 (this export) | prior_export=9.02 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.33 (this export) | prior_export=2.33 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-28_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.