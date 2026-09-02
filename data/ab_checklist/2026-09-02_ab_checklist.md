# A+B1 Feature Checklist — 2026-09-02

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,683** names
- Export: `finviz_2026-09-02.csv` · prior export for Δ: `2026-09-01`
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
| 1 | SHEL | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Oil & Gas Integrated |
| 2 | SHAK | +15 | 17 | 2 | 2026-09-01→2026-09-02 | Restaurants |
| 3 | AUPH | +15 | 16 | 1 | 2026-09-01→2026-09-02 | Biotechnology |
| 4 | NICE | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Software - Application |
| 5 | AMCR | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Packaging & Containers |
| 6 | TAL | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Education & Training Services |
| 7 | ASH | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Specialty Chemicals |
| 8 | GMAB | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Biotechnology |
| 9 | WES | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Oil & Gas Midstream |
| 10 | BG | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Farm Products |
| 11 | CMC | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Metal Fabrication |
| 12 | ELV | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Healthcare Plans |
| 13 | DCTH | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Medical Devices |
| 14 | HASI | +14 | 15 | 1 | 2026-09-01→2026-09-02 | Asset Management |
| 15 | NUE | +14 | 16 | 2 | 2026-09-01→2026-09-02 | Steel |

## Full checklist — top 15

### SHEL  ·  score **+16**  ·  Oil & Gas Integrated
price=92.93000030517578  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.72 on 2026-09-02; prev RSI=64.79 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.79@2026-09-01 → 61.72@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.79@2026-09-01 → 61.72@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.79@2026-09-01 → 61.72@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=15.799 (G=0.7900 R=0.0500); 2026-09-01:GREEN:O=92.7200,C=93.5100,body=+0.7900,vol=11050227.0; 2026-09-02:RED:O=92.9800,C=92.9300,body=-0.0500,vol=1672180.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=6.608 (Gvol=11050227 Rvol=1672180); 2026-09-01:GREEN:O=92.7200,C=93.5100,body=+0.7900,vol=11050227.0; 2026-09-02:RED:O=92.9800,C=92.9300,body=-0.0500,vol=1672180.0 | **GOOD** |
| `A07_rvol` | RVOL=0.294 on 2026-09-02: today_vol=1672180 / avg20=5687891 (avg window 2026-08-05→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.496 on 2026-09-02 (price=92.9300, mid=91.1771, upper=94.7105, lower=87.6436; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=92.9300 vs SMA50=86.4645 dist=+7.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=91.1771 SMA50=86.4645 SMA80=85.4615 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-02 (63 bars); S1[2026-06-04→2026-07-06] low=2026-07-01@75.5975; S2[2026-07-07→2026-08-04] low=2026-07-07@79.3248; S3[2026-08-05→2026-09-02] low=2026-08-05@86.8885 | lows=[75.59747315421986, 79.32480078270552, 86.88852126928514] span=14.94% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.5683462319556507 wick_frac=0.4316537680443493 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.08065071864540264 wick_frac=0.9193492813545974 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.799054012816601 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.5300:wick=0.7700; 2026-08-28:GREEN:body=+0.1700:wick=0.4450; 2026-08-31:RED:body=-0.5900:wick=0.9400; 2026-09-01:GREEN:body=+0.7900:wick=0.6000; 2026-09-02:RED:body=-0.0500:wick=0.5700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.09 vs prior_export=100.09 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.31 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.31 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHAK  ·  score **+15**  ·  Restaurants
price=69.77269744873047  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.01 on 2026-09-02; prev RSI=45.78 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.78@2026-09-01 → 51.01@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.78@2026-09-01 → 51.01@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.78@2026-09-01 → 51.01@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.9027 R=0.0000); 2026-09-01:GREEN:O=67.2900,C=67.6200,body=+0.3300,vol=1015500.0; 2026-09-02:GREEN:O=67.2000,C=69.7727,body=+2.5727,vol=84635.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1100135 Rvol=0); 2026-09-01:GREEN:O=67.2900,C=67.6200,body=+0.3300,vol=1015500.0; 2026-09-02:GREEN:O=67.2000,C=69.7727,body=+2.5727,vol=84635.0 | **GOOD** |
| `A07_rvol` | RVOL=0.052 on 2026-09-02: today_vol=84635 / avg20=1624980 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.579 on 2026-09-02 (price=69.7727, mid=72.3831, upper=76.8919, lower=67.8743; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=69.7727 vs SMA50=63.8737 dist=+9.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=72.3831 SMA50=63.8737 SMA80=63.0953 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-08@51.6000; S2[2026-07-01→2026-08-03] low=2026-07-08@52.1200; S3[2026-08-04→2026-09-02] low=2026-08-05@64.0100 | lows=[51.599998474121094, 52.119998931884766, 64.01000213623047] span=24.05% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.579563661113557 wick_frac=0.42043633888644294 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:RED:body=-0.8300:wick=1.2200; 2026-08-27:RED:body=-1.9100:wick=0.9480; 2026-08-31:RED:body=-0.1100:wick=1.8990; 2026-09-01:GREEN:body=+0.3300:wick=2.1100; 2026-09-02:GREEN:body=+2.5727:wick=-0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=40.75 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1552.3 | **NEUTRAL** |
| `B04_income` | 39.72 | **GOOD** |
| `B05_profit_margin` | 2.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 81.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=81.73 vs prior_export=81.73 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.59 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.59 vs prior=2.59 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | -13.49 | **BAD** |
| `B13_short_float` | 10.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=40.75 (this export) | prior_export=40.75 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+15**  ·  Biotechnology
price=16.40999984741211  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.05 on 2026-09-02; prev RSI=50.99 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.99@2026-09-01 → 55.05@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.99@2026-09-01 → 55.05@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.99@2026-09-01 → 55.05@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.2400 R=0.0000); 2026-09-01:DOJI:O=16.0500,C=16.0500,body=+0.0000,vol=878800.0; 2026-09-02:GREEN:O=16.1700,C=16.4100,body=+0.2400,vol=18688.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.043 (Gvol=458088 Rvol=439400); 2026-09-01:DOJI:O=16.0500,C=16.0500,body=+0.0000,vol=878800.0; 2026-09-02:GREEN:O=16.1700,C=16.4100,body=+0.2400,vol=18688.0 | **GOOD** |
| `A07_rvol` | RVOL=0.016 on 2026-09-02: today_vol=18688 / avg20=1140095 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.344 on 2026-09-02 (price=16.4100, mid=15.9838, upper=17.2227, lower=14.7448; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=16.4100 vs SMA50=16.0039 dist=+2.54% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.98_50=16.00_80=15.88 on 2026-09-02: SMA20=15.9838 SMA50=16.0039 SMA80=15.8841 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-02@14.9950; S2[2026-07-02→2026-08-03] low=2026-07-31@14.3630; S3[2026-08-04→2026-09-02] low=2026-08-04@14.6000 | lows=[14.994999885559082, 14.36299991607666, 14.600000381469727] span=4.40% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.9795875470023588 wick_frac=0.020412452997641124 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:RED:body=-0.0600:wick=0.2500; 2026-08-27:GREEN:body=+0.0050:wick=0.3350; 2026-08-31:RED:body=-0.1000:wick=0.2550; 2026-09-01:DOJI:body=+0.0000:wick=0.3350; 2026-09-02:GREEN:body=+0.2400:wick=0.0050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 12.31 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=12.31 vs prior=12.31 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.65 | **GOOD** |
| `B13_short_float` | 8.18 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NICE  ·  score **+14**  ·  Software - Application
price=106.83000183105469  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.21 on 2026-09-02; prev RSI=58.90 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.90@2026-09-01 → 59.21@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.90@2026-09-01 → 59.21@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.90@2026-09-01 → 59.21@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=8.136 (G=1.7900 R=0.2200); 2026-09-01:RED:O=106.8100,C=106.5900,body=-0.2200,vol=405000.0; 2026-09-02:GREEN:O=105.0400,C=106.8300,body=+1.7900,vol=17875.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=0.044 (Gvol=17875 Rvol=405000); 2026-09-01:RED:O=106.8100,C=106.5900,body=-0.2200,vol=405000.0; 2026-09-02:GREEN:O=105.0400,C=106.8300,body=+1.7900,vol=17875.0 | **BAD** |
| `A07_rvol` | RVOL=0.032 on 2026-09-02: today_vol=17875 / avg20=560950 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.778 on 2026-09-02 (price=106.8300, mid=101.8470, upper=108.2543, lower=95.4397; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=106.8300 vs SMA50=98.0750 dist=+8.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=101.8470 SMA50=98.0750 SMA80=95.6338 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-17@83.1000; S2[2026-07-01→2026-08-03] low=2026-07-23@86.0100; S3[2026-08-04→2026-09-02] low=2026-08-05@93.4800 | lows=[83.0999984741211, 86.01000213623047, 93.4800033569336] span=12.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7885478249471487 wick_frac=0.21145217505285127 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.05379007767439431 wick_frac=0.9462099223256056 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.136322652240255 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+2.1400:wick=0.5400; 2026-08-27:GREEN:body=+0.1100:wick=2.9700; 2026-08-31:GREEN:body=+1.2400:wick=1.4200; 2026-09-01:RED:body=-0.2200:wick=3.8700; 2026-09-02:GREEN:body=+1.7900:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.27 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.09 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3089.11 | **NEUTRAL** |
| `B04_income` | 428.37 | **GOOD** |
| `B05_profit_margin` | 13.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.17 vs prior_export=122.17 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.99 | **GOOD** |
| `B13_short_float` | 2.56 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMCR  ·  score **+14**  ·  Packaging & Containers
price=46.65999984741211  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.90 on 2026-09-02; prev RSI=47.95 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.95@2026-09-01 → 52.90@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.95@2026-09-01 → 52.90@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.95@2026-09-01 → 52.90@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.941 (G=0.6700 R=0.1700); 2026-09-01:RED:O=45.9900,C=45.8200,body=-0.1700,vol=2605400.0; 2026-09-02:GREEN:O=45.9900,C=46.6600,body=+0.6700,vol=75080.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=0.029 (Gvol=75080 Rvol=2605400); 2026-09-01:RED:O=45.9900,C=45.8200,body=-0.1700,vol=2605400.0; 2026-09-02:GREEN:O=45.9900,C=46.6600,body=+0.6700,vol=75080.0 | **BAD** |
| `A07_rvol` | RVOL=0.020 on 2026-09-02: today_vol=75080 / avg20=3699740 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.160 on 2026-09-02 (price=46.6600, mid=46.9305, upper=48.6252, lower=45.2358; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=46.6600 vs SMA50=45.0268 dist=+3.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=46.9305 SMA50=45.0268 SMA80=42.6434 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-04@37.4600; S2[2026-07-02→2026-08-03] low=2026-07-08@41.4500; S3[2026-08-04→2026-09-02] low=2026-08-17@44.8100 | lows=[37.459999084472656, 41.45000076293945, 44.810001373291016] span=19.62% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8815740601315063 wick_frac=0.1184259398684937 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.253733552725223 wick_frac=0.746266447274777 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.941119712779087 need>1.4; red_wick_gt_green=False 5d trail=2026-08-26:RED:body=-0.0800:wick=0.5600; 2026-08-27:GREEN:body=+0.0400:wick=0.8700; 2026-08-31:RED:body=-0.4500:wick=0.2500; 2026-09-01:RED:body=-0.1700:wick=0.5000; 2026-09-02:GREEN:body=+0.6700:wick=0.0900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.19 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23506.0 | **NEUTRAL** |
| `B04_income` | 1106.0 | **GOOD** |
| `B05_profit_margin` | 4.71 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.48 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.48 vs prior_export=49.48 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.71 | **GOOD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.19 (this export) | prior_export=3.19 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TAL  ·  score **+14**  ·  Education & Training Services
price=12.460000038146973  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.55 on 2026-09-02; prev RSI=62.63 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.63@2026-09-01 → 65.55@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.63@2026-09-01 → 65.55@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.63@2026-09-01 → 65.55@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7300 R=0.0000); 2026-09-01:GREEN:O=11.7400,C=12.2300,body=+0.4900,vol=6351900.0; 2026-09-02:GREEN:O=12.2200,C=12.4600,body=+0.2400,vol=584718.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6936618 Rvol=0); 2026-09-01:GREEN:O=11.7400,C=12.2300,body=+0.4900,vol=6351900.0; 2026-09-02:GREEN:O=12.2200,C=12.4600,body=+0.2400,vol=584718.0 | **GOOD** |
| `A07_rvol` | RVOL=0.116 on 2026-09-02: today_vol=584718 / avg20=5022550 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.742 on 2026-09-02 (price=12.4600, mid=11.9120, upper=12.6505, lower=11.1735; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=12.4600 vs SMA50=10.9350 dist=+13.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=11.9120 SMA50=10.9350 SMA80=10.6256 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-25@8.8800; S2[2026-07-01→2026-08-03] low=2026-07-01@9.6600; S3[2026-08-04→2026-09-02] low=2026-08-21@11.1700 | lows=[8.880000114440918, 9.65999984741211, 11.170000076293945] span=25.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7321673147180081 wick_frac=0.26783268528199194 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+0.0100:wick=0.1900; 2026-08-27:GREEN:body=+0.1000:wick=0.1800; 2026-08-31:RED:body=-0.1500:wick=0.2100; 2026-09-01:GREEN:body=+0.4900:wick=0.2200; 2026-09-02:GREEN:body=+0.2400:wick=0.0700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=161.6 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.42 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3202.07 | **NEUTRAL** |
| `B04_income` | 910.22 | **GOOD** |
| `B05_profit_margin` | 28.43 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.78 vs prior_export=16.78 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | -45.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-45.15 vs prior=-45.15 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.54 | **GOOD** |
| `B13_short_float` | 6.14 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=161.6 (this export) | prior_export=161.6 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.42 (this export) | prior_export=5.42 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASH  ·  score **+14**  ·  Specialty Chemicals
price=75.94999694824219  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.29 on 2026-09-02; prev RSI=64.42 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.42@2026-09-01 → 65.29@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.42@2026-09-01 → 65.29@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.42@2026-09-01 → 65.29@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.4900 R=0.0000); 2026-09-01:GREEN:O=73.0500,C=75.6400,body=+2.5900,vol=722700.0; 2026-09-02:GREEN:O=73.0500,C=75.9500,body=+2.9000,vol=11226.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=733926 Rvol=0); 2026-09-01:GREEN:O=73.0500,C=75.6400,body=+2.5900,vol=722700.0; 2026-09-02:GREEN:O=73.0500,C=75.9500,body=+2.9000,vol=11226.0 | **GOOD** |
| `A07_rvol` | RVOL=0.017 on 2026-09-02: today_vol=11226 / avg20=644195 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.810 on 2026-09-02 (price=75.9500, mid=73.7704, upper=76.4610, lower=71.0798; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-02: price=75.9500 vs SMA50=69.7174 dist=+8.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=73.7704 SMA50=69.7174 SMA80=65.1906 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-08@54.9800; S2[2026-06-30→2026-08-03] low=2026-07-08@62.8200; S3[2026-08-04→2026-09-02] low=2026-08-04@70.9700 | lows=[54.97999954223633, 62.81999969482422, 70.97000122070312] span=29.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=3.9220290707268233 wick_frac=-2.9220290707268233 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+0.2088:wick=1.2428; 2026-08-27:GREEN:body=+0.3182:wick=0.8451; 2026-08-31:RED:body=-1.1235:wick=0.6363; 2026-09-01:GREEN:body=+2.5900:wick=0.7700; 2026-09-02:GREEN:body=+2.9000:wick=-2.4900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.55 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1843.0 | **NEUTRAL** |
| `B04_income` | 52.0 | **GOOD** |
| `B05_profit_margin` | 2.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=80.18 vs prior_export=80.18 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.56 | **GOOD** |
| `B13_short_float` | 9.43 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.55 (this export) | prior_export=3.55 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+14**  ·  Biotechnology
price=34.5  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.52 on 2026-09-02; prev RSI=61.71 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.71@2026-09-01 → 67.52@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.71@2026-09-01 → 67.52@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.71@2026-09-01 → 67.52@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=11.500 (G=0.4600 R=0.0400); 2026-09-01:RED:O=33.3300,C=33.2900,body=-0.0400,vol=1563200.0; 2026-09-02:GREEN:O=34.0400,C=34.5000,body=+0.4600,vol=120618.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=0.077 (Gvol=120618 Rvol=1563200); 2026-09-01:RED:O=33.3300,C=33.2900,body=-0.0400,vol=1563200.0; 2026-09-02:GREEN:O=34.0400,C=34.5000,body=+0.4600,vol=120618.0 | **BAD** |
| `A07_rvol` | RVOL=0.041 on 2026-09-02: today_vol=120618 / avg20=2933185 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.655 on 2026-09-02 (price=34.5000, mid=32.4855, upper=35.5618, lower=29.4092; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=34.5000 vs SMA50=29.7624 dist=+15.92% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=32.4855 SMA50=29.7624 SMA80=28.3440 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-03@23.6200; S2[2026-06-30→2026-08-03] low=2026-07-01@27.3300; S3[2026-08-04→2026-09-02] low=2026-08-06@28.4700 | lows=[23.6200008392334, 27.329999923706055, 28.469999313354492] span=20.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.9387704260768698 wick_frac=0.06122957392313022 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.07692533415496575 wick_frac=0.9230746658450343 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.49971390425329 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:RED:body=-0.0300:wick=0.5100; 2026-08-27:GREEN:body=+0.1900:wick=0.3050; 2026-08-31:GREEN:body=+0.4000:wick=0.3200; 2026-09-01:RED:body=-0.0400:wick=0.4800; 2026-09-02:GREEN:body=+0.4600:wick=0.0300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=38.46 vs prior_export=38.46 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.31 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 1.3 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WES  ·  score **+14**  ·  Oil & Gas Midstream
price=48.22999954223633  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.74 on 2026-09-02; prev RSI=60.94 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.94@2026-09-01 → 54.74@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.94@2026-09-01 → 54.74@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.94@2026-09-01 → 54.74@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=1.488 (G=0.6100 R=0.4100); 2026-09-01:GREEN:O=48.3800,C=48.9900,body=+0.6100,vol=891400.0; 2026-09-02:RED:O=48.6400,C=48.2300,body=-0.4100,vol=66328.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=13.439 (Gvol=891400 Rvol=66328); 2026-09-01:GREEN:O=48.3800,C=48.9900,body=+0.6100,vol=891400.0; 2026-09-02:RED:O=48.6400,C=48.2300,body=-0.4100,vol=66328.0 | **GOOD** |
| `A07_rvol` | RVOL=0.082 on 2026-09-02: today_vol=66328 / avg20=812315 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.042 on 2026-09-02 (price=48.2300, mid=48.1585, upper=49.8780, lower=46.4390; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=48.2300 vs SMA50=45.8668 dist=+5.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=48.1585 SMA50=45.8668 SMA80=45.0233 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-24@41.2859; S2[2026-07-01→2026-08-03] low=2026-07-01@42.1781; S3[2026-08-04→2026-09-02] low=2026-08-04@45.8000 | lows=[41.2858772277832, 42.17806755687263, 45.79999923706055] span=10.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.6630454613305027 wick_frac=0.33695453866949726 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.5257085000440214 wick_frac=0.4742914999559786 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4878069204216637 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+0.5300:wick=0.3000; 2026-08-27:RED:body=-0.4700:wick=0.4000; 2026-08-31:RED:body=-0.2100:wick=0.8200; 2026-09-01:GREEN:body=+0.6100:wick=0.3100; 2026-09-02:RED:body=-0.4100:wick=0.3699 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.19 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.58 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4332.26 | **NEUTRAL** |
| `B04_income` | 1256.18 | **GOOD** |
| `B05_profit_margin` | 29.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 48.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.28000000000000114 (now=48.64 vs prior_export=48.36 on finviz_2026-09-01) | **GOOD** |
| `B09_analyst_recom` | 2.8 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.65 | **GOOD** |
| `B13_short_float` | 2.75 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.19 (this export) | prior_export=9.19 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.58 (this export) | prior_export=8.58 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BG  ·  score **+14**  ·  Farm Products
price=120.29000091552734  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.37 on 2026-09-02; prev RSI=63.01 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.01@2026-09-01 → 61.37@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.01@2026-09-01 → 61.37@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.01@2026-09-01 → 61.37@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.3500 R=0.0000); 2026-09-01:GREEN:O=117.9900,C=121.0400,body=+3.0500,vol=1599200.0; 2026-09-02:GREEN:O=117.9900,C=120.2900,body=+2.3000,vol=40538.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1639738 Rvol=0); 2026-09-01:GREEN:O=117.9900,C=121.0400,body=+3.0500,vol=1599200.0; 2026-09-02:GREEN:O=117.9900,C=120.2900,body=+2.3000,vol=40538.0 | **GOOD** |
| `A07_rvol` | RVOL=0.029 on 2026-09-02: today_vol=40538 / avg20=1395015 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=1.008 on 2026-09-02 (price=120.2900, mid=113.1440, upper=120.2341, lower=106.0539; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-02: price=120.2900 vs SMA50=112.8780 dist=+6.57% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=113.14_50=112.88_80=117.27 on 2026-09-02: SMA20=113.1440 SMA50=112.8780 SMA80=117.2661 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-24@106.0000; S2[2026-06-30→2026-08-03] low=2026-07-30@102.8100; S3[2026-08-04→2026-09-02] low=2026-08-04@104.5300 | lows=[106.0, 102.80999755859375, 104.52999877929688] span=3.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=1.4608402659413668 wick_frac=-0.46084026594136696 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+2.4500:wick=1.3600; 2026-08-27:RED:body=-1.1800:wick=1.8700; 2026-08-31:RED:body=-0.6600:wick=7.8600; 2026-09-01:GREEN:body=+3.0500:wick=1.4500; 2026-09-02:GREEN:body=+2.3000:wick=-1.2750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.71 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.38 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 91812.0 | **NEUTRAL** |
| `B04_income` | 1010.0 | **GOOD** |
| `B05_profit_margin` | 1.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 144.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=144.2 vs prior_export=144.2 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.18 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 3.94 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.71 (this export) | prior_export=1.71 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.38 (this export) | prior_export=5.38 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CMC  ·  score **+14**  ·  Metal Fabrication
price=68.94999694824219  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.20 on 2026-09-02; prev RSI=45.23 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.23@2026-09-01 → 50.20@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.23@2026-09-01 → 50.20@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.23@2026-09-01 → 50.20@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.2700 R=0.0000); 2026-09-01:GREEN:O=66.4100,C=67.1400,body=+0.7300,vol=677200.0; 2026-09-02:GREEN:O=66.4100,C=68.9500,body=+2.5400,vol=66970.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=744170 Rvol=0); 2026-09-01:GREEN:O=66.4100,C=67.1400,body=+0.7300,vol=677200.0; 2026-09-02:GREEN:O=66.4100,C=68.9500,body=+2.5400,vol=66970.0 | **GOOD** |
| `A07_rvol` | RVOL=0.059 on 2026-09-02: today_vol=66970 / avg20=1135570 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.174 on 2026-09-02 (price=68.9500, mid=70.0875, upper=76.6207, lower=63.5543; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=68.9500 vs SMA50=68.2753 dist=+0.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=70.09_50=68.28_80=70.16 on 2026-09-02: SMA20=70.0875 SMA50=68.2753 SMA80=70.1631 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-29@63.8126; S2[2026-06-30→2026-08-03] low=2026-07-09@59.9600; S3[2026-08-04→2026-09-02] low=2026-08-24@64.3900 | lows=[63.81257663716338, 59.959999084472656, 64.38999938964844] span=7.39% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8519973357779611 wick_frac=0.1480026642220388 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+1.2100:wick=0.3900; 2026-08-27:GREEN:body=+0.5800:wick=0.4700; 2026-08-31:RED:body=-0.2700:wick=1.2200; 2026-09-01:GREEN:body=+0.7300:wick=1.5800; 2026-09-02:GREEN:body=+2.5400:wick=-0.7100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.84 (current export asof; earnings_date=6/25/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.4 (current export; earnings_date=6/25/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8850.09 | **NEUTRAL** |
| `B04_income` | 595.11 | **GOOD** |
| `B05_profit_margin` | 6.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 81.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=81.55 vs prior_export=81.55 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 1.05 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.05 vs prior=1.05 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.27 | **BAD** |
| `B13_short_float` | 4.47 | **NEUTRAL** |
| `B14_earnings_date` | 6/25/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.84 (this export) | prior_export=1.84 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.4 (this export) | prior_export=3.4 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+14**  ·  Healthcare Plans
price=405.927490234375  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.82 on 2026-09-02; prev RSI=55.04 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.04@2026-09-01 → 56.82@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.04@2026-09-01 → 56.82@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.04@2026-09-01 → 56.82@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.0875 R=0.0000); 2026-09-01:GREEN:O=396.4400,C=403.0400,body=+6.6000,vol=1101900.0; 2026-09-02:GREEN:O=396.4400,C=405.9275,body=+9.4875,vol=15323.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1117223 Rvol=0); 2026-09-01:GREEN:O=396.4400,C=403.0400,body=+6.6000,vol=1101900.0; 2026-09-02:GREEN:O=396.4400,C=405.9275,body=+9.4875,vol=15323.0 | **GOOD** |
| `A07_rvol` | RVOL=0.017 on 2026-09-02: today_vol=15323 / avg20=906555 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.917 on 2026-09-02 (price=405.9275, mid=397.5514, upper=406.6902, lower=388.4126; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-02: price=405.9275 vs SMA50=395.6098 dist=+2.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=397.5514 SMA50=395.6098 SMA80=394.9968 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-24@381.2400; S2[2026-06-30→2026-08-03] low=2026-07-28@362.9800; S3[2026-08-04→2026-09-02] low=2026-08-05@370.4700 | lows=[381.239990234375, 362.9800109863281, 370.4700012207031] span=5.03% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=1.216498778187207 wick_frac=-0.21649877818720697 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-26:GREEN:body=+3.5400:wick=7.7500; 2026-08-27:GREEN:body=+1.7500:wick=7.1200; 2026-08-31:RED:body=-1.4000:wick=3.1600; 2026-09-01:GREEN:body=+6.6000:wick=4.6400; 2026-09-02:GREEN:body=+9.4875:wick=-4.3475 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 445.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=445.57 vs prior_export=445.57 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.28 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.28 vs prior=0.28 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.36 | **GOOD** |
| `B13_short_float` | 2.5 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DCTH  ·  score **+14**  ·  Medical Devices
price=16.584999084472656  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.60 on 2026-09-02; prev RSI=59.03 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.03@2026-09-01 → 60.60@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.03@2026-09-01 → 60.60@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.03@2026-09-01 → 60.60@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.187 (G=0.1750 R=0.0800); 2026-09-01:RED:O=16.4800,C=16.4000,body=-0.0800,vol=360400.0; 2026-09-02:GREEN:O=16.4100,C=16.5850,body=+0.1750,vol=21187.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=0.059 (Gvol=21187 Rvol=360400); 2026-09-01:RED:O=16.4800,C=16.4000,body=-0.0800,vol=360400.0; 2026-09-02:GREEN:O=16.4100,C=16.5850,body=+0.1750,vol=21187.0 | **BAD** |
| `A07_rvol` | RVOL=0.029 on 2026-09-02: today_vol=21187 / avg20=739910 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.060 on 2026-09-02 (price=16.5850, mid=16.4577, upper=18.5730, lower=14.3425; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=16.5850 vs SMA50=14.1813 dist=+16.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=16.4577 SMA50=14.1813 SMA80=13.0576 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-03@10.2000; S2[2026-07-02→2026-08-03] low=2026-07-29@11.5000; S3[2026-08-04→2026-09-02] low=2026-08-05@12.4300 | lows=[10.199999809265137, 11.5, 12.430000305175781] span=21.86% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8749928474699117 wick_frac=0.12500715253008832 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.25396911898274294 wick_frac=0.746030881017257 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1874925494122976 need>1.4; red_wick_gt_green=False 5d trail=2026-08-26:RED:body=-0.5000:wick=0.0900; 2026-08-27:GREEN:body=+0.0100:wick=0.3000; 2026-08-31:GREEN:body=+0.1900:wick=0.1900; 2026-09-01:RED:body=-0.0800:wick=0.2350; 2026-09-02:GREEN:body=+0.1750:wick=0.0250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=178.56 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.97 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 95.42 | **NEUTRAL** |
| `B04_income` | 0.53 | **GOOD** |
| `B05_profit_margin` | 0.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.5 vs prior_export=24.5 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.21 vs prior=0.21 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.84 | **GOOD** |
| `B13_short_float` | 7.33 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=178.56 (this export) | prior_export=178.56 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.97 (this export) | prior_export=10.97 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HASI  ·  score **+14**  ·  Asset Management
price=39.564998626708984  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.46 on 2026-09-02; prev RSI=47.08 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.08@2026-09-01 → 47.46@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.08@2026-09-01 → 47.46@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.08@2026-09-01 → 47.46@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4650 R=0.0000); 2026-09-01:GREEN:O=39.3100,C=39.5200,body=+0.2100,vol=4727300.0; 2026-09-02:GREEN:O=39.3100,C=39.5650,body=+0.2550,vol=13212.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4740512 Rvol=0); 2026-09-01:GREEN:O=39.3100,C=39.5200,body=+0.2100,vol=4727300.0; 2026-09-02:GREEN:O=39.3100,C=39.5650,body=+0.2550,vol=13212.0 | **GOOD** |
| `A07_rvol` | RVOL=0.012 on 2026-09-02: today_vol=13212 / avg20=1125800 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.345 on 2026-09-02 (price=39.5650, mid=40.4293, upper=42.9325, lower=37.9260; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=39.5650 vs SMA50=39.1565 dist=+1.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=40.43_50=39.16_80=39.37 on 2026-09-02: SMA20=40.4293 SMA50=39.1565 SMA80=39.3749 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-29→2026-09-02 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-10@35.8512; S2[2026-06-30→2026-08-03] low=2026-07-08@37.1200; S3[2026-08-04→2026-09-02] low=2026-08-05@38.0100 | lows=[35.85118516259977, 37.119998931884766, 38.0099983215332] span=6.02% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8583236855963225 wick_frac=0.14167631440367748 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-26:GREEN:body=+0.5500:wick=0.1600; 2026-08-27:GREEN:body=+0.2500:wick=0.3200; 2026-08-31:RED:body=-0.2000:wick=0.3500; 2026-09-01:GREEN:body=+0.2100:wick=0.4900; 2026-09-02:GREEN:body=+0.2550:wick=-0.0750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.66 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 462.89 | **NEUTRAL** |
| `B04_income` | 83.73 | **GOOD** |
| `B05_profit_margin` | 18.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.79 vs prior_export=49.79 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.33 | **GOOD** |
| `B13_short_float` | 10.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.9 (this export) | prior_export=7.9 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NUE  ·  score **+14**  ·  Steel
price=259.375  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.57 on 2026-09-02; prev RSI=48.07 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.07@2026-09-01 → 54.57@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.07@2026-09-01 → 54.57@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.07@2026-09-01 → 54.57@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.6050 R=0.0000); 2026-09-01:GREEN:O=248.5800,C=251.9200,body=+3.3400,vol=1311400.0; 2026-09-02:GREEN:O=254.1100,C=259.3750,body=+5.2650,vol=70793.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1382193 Rvol=0); 2026-09-01:GREEN:O=248.5800,C=251.9200,body=+3.3400,vol=1311400.0; 2026-09-02:GREEN:O=254.1100,C=259.3750,body=+5.2650,vol=70793.0 | **GOOD** |
| `A07_rvol` | RVOL=0.047 on 2026-09-02: today_vol=70793 / avg20=1509945 (avg window 2026-08-04→2026-09-01, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.040 on 2026-09-02 (price=259.3750, mid=260.3478, upper=284.9398, lower=235.7557; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=259.3750 vs SMA50=247.6810 dist=+4.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=260.3478 SMA50=247.6810 SMA80=245.9675 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-07-01@214.6100; S2[2026-07-02→2026-08-03] low=2026-07-02@216.8800; S3[2026-08-04→2026-09-02] low=2026-08-19@238.6200 | lows=[214.61000061035156, 216.8800048828125, 238.6199951171875] span=11.19% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.578431631711068 wick_frac=0.42156836828893196 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-26:GREEN:body=+3.8900:wick=2.1100; 2026-08-27:GREEN:body=+0.3700:wick=3.0900; 2026-08-31:RED:body=-2.6200:wick=2.5400; 2026-09-01:GREEN:body=+3.3400:wick=3.2600; 2026-09-02:GREEN:body=+5.2650:wick=2.8250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.48 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.46 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 36101.0 | **NEUTRAL** |
| `B04_income` | 2873.0 | **GOOD** |
| `B05_profit_margin` | 7.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 287.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=287.93 vs prior_export=287.93 on finviz_2026-09-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -12.51 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-12.51 vs prior=-12.51 on finviz_2026-09-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.53 | **GOOD** |
| `B13_short_float` | 1.56 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.48 (this export) | prior_export=8.48 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.46 (this export) | prior_export=2.46 (finviz_2026-09-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-02_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.