# A+B1 Feature Checklist — 2026-10-02

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-02.csv` · prior export for Δ: `2026-10-01`
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
| 1 | EXLS | +17 | 18 | 1 | 2026-09-30→2026-10-01 | Information Technology Services |
| 2 | DINO | +17 | 17 | 0 | 2026-09-30→2026-10-01 | Oil & Gas Refining & Marketing |
| 3 | ARHS | +17 | 17 | 0 | 2026-09-30→2026-10-01 | Specialty Retail |
| 4 | ESTC | +16 | 17 | 1 | 2026-09-30→2026-10-01 | Software - Application |
| 5 | SU | +16 | 17 | 1 | 2026-09-30→2026-10-01 | Oil & Gas Integrated |
| 6 | SB | +16 | 16 | 0 | 2026-09-30→2026-10-01 | Marine Shipping |
| 7 | DSX | +16 | 16 | 0 | 2026-09-30→2026-10-01 | Marine Shipping |
| 8 | ET | +15 | 17 | 2 | 2026-09-30→2026-10-01 | Oil & Gas Midstream |
| 9 | ECO | +15 | 18 | 3 | 2026-09-30→2026-10-01 | Marine Shipping |
| 10 | SBLK | +15 | 17 | 2 | 2026-09-30→2026-10-01 | Marine Shipping |
| 11 | INOD | +15 | 16 | 1 | 2026-09-30→2026-10-01 | Information Technology Services |
| 12 | BP | +15 | 16 | 1 | 2026-09-30→2026-10-01 | Oil & Gas Integrated |
| 13 | CVX | +15 | 18 | 3 | 2026-09-30→2026-10-01 | Oil & Gas Integrated |
| 14 | SLDE | +15 | 18 | 3 | 2026-09-30→2026-10-01 | Insurance - Property & Casualty |
| 15 | SKY | +15 | 16 | 1 | 2026-09-30→2026-10-01 | Residential Construction |

## Full checklist — top 15

### EXLS  ·  score **+17**  ·  Information Technology Services
price=36.47999954223633  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.86 on 2026-10-01; prev RSI=42.19 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.19@2026-09-30 → 58.86@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.19@2026-09-30 → 58.86@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.19@2026-09-30 → 58.86@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=6.875 (G=1.1000 R=0.1600); 2026-09-30:RED:O=34.5400,C=34.3800,body=-0.1600,vol=1833100.0; 2026-10-01:GREEN:O=35.3800,C=36.4800,body=+1.1000,vol=3022900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.649 (Gvol=3022900 Rvol=1833100); 2026-09-30:RED:O=34.5400,C=34.3800,body=-0.1600,vol=1833100.0; 2026-10-01:GREEN:O=35.3800,C=36.4800,body=+1.1000,vol=3022900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.592 on 2026-10-01: today_vol=3022900 / avg20=1898945 (avg window 2026-09-02→2026-09-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.643 on 2026-10-01 (price=36.4800, mid=35.4290, upper=37.0630, lower=33.7950; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=36.4800 vs SMA50=35.0068 dist=+4.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=35.4290 SMA50=35.0068 SMA80=32.2259 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-06-30@24.8500; S2[2026-08-04→2026-09-01] low=2026-08-06@33.3300; S3[2026-09-02→2026-10-01] low=2026-09-29@34.0500 | lows=[24.850000381469727, 33.33000183105469, 34.04999923706055] span=37.02% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.670731026526112 wick_frac=0.32926897347388795 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.32652918233411965 wick_frac=0.6734708176658804 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.874997019764919 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:RED:body=-0.1800:wick=0.6450; 2026-09-28:GREEN:body=+0.0300:wick=0.4600; 2026-09-29:GREEN:body=+0.2600:wick=0.5300; 2026-09-30:RED:body=-0.1600:wick=0.3300; 2026-10-01:GREEN:body=+1.1000:wick=0.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.76 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.63 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2237.31 | **NEUTRAL** |
| `B04_income` | 250.0 | **GOOD** |
| `B05_profit_margin` | 11.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.62 vs prior_export=44.62 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.18 | **GOOD** |
| `B10_insider_transactions` | -1.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.07 vs prior=-1.07 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.76 (this export) | prior_export=7.76 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.63 (this export) | prior_export=3.63 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+17**  ·  Oil & Gas Refining & Marketing
price=112.69999694824219  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.74 on 2026-10-01; prev RSI=56.06 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.06@2026-09-30 → 64.74@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.06@2026-09-30 → 64.74@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.06@2026-09-30 → 64.74@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=260.544 (G=5.2100 R=0.0200); 2026-09-30:RED:O=107.3400,C=107.3200,body=-0.0200,vol=1812600.0; 2026-10-01:GREEN:O=107.4900,C=112.7000,body=+5.2100,vol=2463300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.359 (Gvol=2463300 Rvol=1812600); 2026-09-30:RED:O=107.3400,C=107.3200,body=-0.0200,vol=1812600.0; 2026-10-01:GREEN:O=107.4900,C=112.7000,body=+5.2100,vol=2463300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.881 on 2026-10-01: today_vol=2463300 / avg20=2795064 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.557 on 2026-10-01 (price=112.7000, mid=108.7875, upper=115.8109, lower=101.7641; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=112.7000 vs SMA50=98.7438 dist=+14.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=108.7875 SMA50=98.7438 SMA80=89.5964 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-06@71.6658; S2[2026-08-04→2026-09-01] low=2026-08-07@80.0041; S3[2026-09-02→2026-10-01] low=2026-09-23@103.0000 | lows=[71.66576745966339, 80.0040801936391, 103.0] span=43.72% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.9303572328723452 wick_frac=0.0696427671276548 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.0039055922537845206 wick_frac=0.9960944077462155 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=260.543685616177 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:GREEN:body=+2.3400:wick=2.3913; 2026-09-28:RED:body=-0.9300:wick=2.1050; 2026-09-29:GREEN:body=+1.4300:wick=1.7630; 2026-09-30:RED:body=-0.0200:wick=5.1000; 2026-10-01:GREEN:body=+5.2100:wick=0.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.43 vs prior_export=103.43 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.58 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.14 | **GOOD** |
| `B13_short_float` | 5.85 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARHS  ·  score **+17**  ·  Specialty Retail
price=9.869999885559082  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.62 on 2026-10-01; prev RSI=59.51 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.51@2026-09-30 → 63.62@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.51@2026-09-30 → 63.62@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.51@2026-09-30 → 63.62@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=8.000 (G=0.4800 R=0.0600); 2026-09-30:RED:O=9.5300,C=9.4700,body=-0.0600,vol=1419500.0; 2026-10-01:GREEN:O=9.3900,C=9.8700,body=+0.4800,vol=2416400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.702 (Gvol=2416400 Rvol=1419500); 2026-09-30:RED:O=9.5300,C=9.4700,body=-0.0600,vol=1419500.0; 2026-10-01:GREEN:O=9.3900,C=9.8700,body=+0.4800,vol=2416400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.816 on 2026-10-01: today_vol=2416400 / avg20=1330820 (avg window 2026-09-02→2026-09-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.742 on 2026-10-01 (price=9.8700, mid=8.6352, upper=10.2987, lower=6.9718; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=9.8700 vs SMA50=8.7382 dist=+12.95% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.64_50=8.74_80=8.31 on 2026-10-01: SMA20=8.6352 SMA50=8.7382 SMA80=8.3080 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-01 (63 bars); S1[2026-07-01→2026-08-03] low=2026-07-23@7.0600; S2[2026-08-04→2026-09-01] low=2026-08-04@7.8700; S3[2026-09-02→2026-10-01] low=2026-09-16@7.0670 | lows=[7.059999942779541, 7.869999885559082, 7.066999912261963] span=11.47% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.6620679857619788 wick_frac=0.3379320142380212 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.16666490060611197 wick_frac=0.8333350993938881 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.000063578853673 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:GREEN:body=+0.2300:wick=0.0978; 2026-09-28:GREEN:body=+0.2000:wick=0.1400; 2026-09-29:RED:body=-0.2100:wick=0.1350; 2026-09-30:RED:body=-0.0600:wick=0.3000; 2026-10-01:GREEN:body=+0.4800:wick=0.2450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.52 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.26 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1408.59 | **NEUTRAL** |
| `B04_income` | 69.18 | **GOOD** |
| `B05_profit_margin` | 4.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.54 vs prior_export=10.54 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 12.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.52 (this export) | prior_export=72.52 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESTC  ·  score **+16**  ·  Software - Application
price=92.30999755859375  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.71 on 2026-10-01; prev RSI=58.84 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.84@2026-09-30 → 59.71@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.84@2026-09-30 → 59.71@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.84@2026-09-30 → 59.71@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=2.240 (G=2.8000 R=1.2500); 2026-09-30:GREEN:O=88.9000,C=91.7000,body=+2.8000,vol=1879600.0; 2026-10-01:RED:O=93.5600,C=92.3100,body=-1.2500,vol=1365700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.376 (Gvol=1879600 Rvol=1365700); 2026-09-30:GREEN:O=88.9000,C=91.7000,body=+2.8000,vol=1879600.0; 2026-10-01:RED:O=93.5600,C=92.3100,body=-1.2500,vol=1365700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.652 on 2026-10-01: today_vol=1365700 / avg20=2095472 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.571 on 2026-10-01 (price=92.3100, mid=88.5700, upper=95.1251, lower=82.0149; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=92.3100 vs SMA50=81.8598 dist=+12.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=88.5700 SMA50=81.8598 SMA80=73.6544 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-06-30@56.0100; S2[2026-08-04→2026-09-01] low=2026-08-06@66.4700; S3[2026-09-02→2026-10-01] low=2026-09-11@82.1500 | lows=[56.0099983215332, 66.47000122070312, 82.1500015258789] span=46.67% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.7310689513671151 wick_frac=0.26893104863288486 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.34530360541307237 wick_frac=0.6546963945869276 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.239996337890625 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:RED:body=-2.7600:wick=2.5100; 2026-09-28:GREEN:body=+2.2800:wick=1.7950; 2026-09-29:GREEN:body=+1.3100:wick=1.0600; 2026-09-30:GREEN:body=+2.8000:wick=1.0300; 2026-10-01:RED:body=-1.2500:wick=2.3700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.84 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.78 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.16 | **NEUTRAL** |
| `B04_income` | 375.64 | **GOOD** |
| `B05_profit_margin` | 20.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.42 vs prior_export=110.42 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.05 vs prior=-8.05 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 4.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.84 (this export) | prior_export=19.84 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SU  ·  score **+16**  ·  Oil & Gas Integrated
price=69.0999984741211  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.26 on 2026-10-01; prev RSI=50.61 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.61@2026-09-30 → 55.26@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.61@2026-09-30 → 55.26@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.61@2026-09-30 → 55.26@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=19.143 (G=1.3400 R=0.0700); 2026-09-30:RED:O=67.9400,C=67.8700,body=-0.0700,vol=2973700.0; 2026-10-01:GREEN:O=67.7600,C=69.1000,body=+1.3400,vol=4978800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.674 (Gvol=4978800 Rvol=2973700); 2026-09-30:RED:O=67.9400,C=67.8700,body=-0.0700,vol=2973700.0; 2026-10-01:GREEN:O=67.7600,C=69.1000,body=+1.3400,vol=4978800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.074 on 2026-10-01: today_vol=4978800 / avg20=4633793 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.257 on 2026-10-01 (price=69.1000, mid=68.5465, upper=70.6969, lower=66.3961; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=69.1000 vs SMA50=66.7864 dist=+3.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=68.5465 SMA50=66.7864 SMA80=63.6511 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-06@54.4100; S2[2026-08-04→2026-09-01] low=2026-08-07@59.9500; S3[2026-09-02→2026-10-01] low=2026-09-22@65.8600 | lows=[54.40999984741211, 59.95000076293945, 65.86000061035156] span=21.04% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.6633656261212774 wick_frac=0.33663437387872264 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.06421966976740931 wick_frac=0.9357803302325907 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.142888283378745 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:GREEN:body=+0.1900:wick=0.9100; 2026-09-28:RED:body=-0.3200:wick=0.8400; 2026-09-29:RED:body=-0.2400:wick=0.9000; 2026-09-30:RED:body=-0.0700:wick=1.0200; 2026-10-01:GREEN:body=+1.3400:wick=0.6800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.59 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.91 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 40945.89 | **NEUTRAL** |
| `B04_income` | 6460.91 | **GOOD** |
| `B05_profit_margin` | 15.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.05999999999998806 (now=76.21 vs prior_export=76.15 on finviz_2026-10-01) | **GOOD** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.93 | **BAD** |
| `B13_short_float` | 1.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.59 (this export) | prior_export=6.59 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.91 (this export) | prior_export=7.91 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SB  ·  score **+16**  ·  Marine Shipping
price=8.9399995803833  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.52 on 2026-10-01; prev RSI=52.52 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.52@2026-09-30 → 59.52@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.52@2026-09-30 → 59.52@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.52@2026-09-30 → 59.52@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5900 R=0.0000); 2026-09-30:GREEN:O=8.4100,C=8.5400,body=+0.1300,vol=1925900.0; 2026-10-01:GREEN:O=8.4800,C=8.9400,body=+0.4600,vol=1684800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3610700 Rvol=0); 2026-09-30:GREEN:O=8.4100,C=8.5400,body=+0.1300,vol=1925900.0; 2026-10-01:GREEN:O=8.4800,C=8.9400,body=+0.4600,vol=1684800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.928 on 2026-10-01: today_vol=1684800 / avg20=1816054 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.481 on 2026-10-01 (price=8.9400, mid=8.6200, upper=9.2855, lower=7.9545; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=8.9400 vs SMA50=8.2171 dist=+8.80% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=8.6200 SMA50=8.2171 SMA80=7.6201 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-01 (63 bars); S1[2026-07-01→2026-08-03] low=2026-07-01@6.2169; S2[2026-08-04→2026-09-01] low=2026-08-11@7.1672; S3[2026-09-02→2026-10-01] low=2026-09-25@7.9917 | lows=[6.216863400547756, 7.167211325967238, 7.991700172424316] span=28.55% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.7007912147473065 wick_frac=0.2992087852526935 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:RED:body=-0.1000:wick=0.1393; 2026-09-28:GREEN:body=+0.0600:wick=0.0700; 2026-09-29:GREEN:body=+0.1200:wick=0.2016; 2026-09-30:GREEN:body=+0.1300:wick=0.1000; 2026-10-01:GREEN:body=+0.4600:wick=0.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.0 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.86 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 307.5 | **NEUTRAL** |
| `B04_income` | 78.98 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.71 vs prior_export=8.71 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.82 | **GOOD** |
| `B13_short_float` | 5.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.0 (this export) | prior_export=12.0 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.86 (this export) | prior_export=12.86 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DSX  ·  score **+16**  ·  Marine Shipping
price=2.819999933242798  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.83 on 2026-10-01; prev RSI=43.88 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.88@2026-09-30 → 49.83@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.88@2026-09-30 → 49.83@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.88@2026-09-30 → 49.83@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.0600 R=0.0300); 2026-09-30:RED:O=2.7800,C=2.7500,body=-0.0300,vol=854000.0; 2026-10-01:GREEN:O=2.7600,C=2.8200,body=+0.0600,vol=974100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.141 (Gvol=974100 Rvol=854000); 2026-09-30:RED:O=2.7800,C=2.7500,body=-0.0300,vol=854000.0; 2026-10-01:GREEN:O=2.7600,C=2.8200,body=+0.0600,vol=974100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.657 on 2026-10-01: today_vol=974100 / avg20=1482264 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.518 on 2026-10-01 (price=2.8200, mid=2.9225, upper=3.1205, lower=2.7245; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=2.8200 vs SMA50=2.6762 dist=+5.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=2.9225 SMA50=2.6762 SMA80=2.4907 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-06-30@2.0100; S2[2026-08-04→2026-09-01] low=2026-08-12@2.3000; S3[2026-09-02→2026-10-01] low=2026-09-30@2.7400 | lows=[2.009999990463257, 2.299999952316284, 2.740000009536743] span=36.32% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.5 wick_frac=0.5 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.375 wick_frac=0.625 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:RED:body=-0.1100:wick=0.0400; 2026-09-28:DOJI:body=+0.0000:wick=0.0750; 2026-09-29:RED:body=-0.0200:wick=0.0500; 2026-09-30:RED:body=-0.0300:wick=0.0500; 2026-10-01:GREEN:body=+0.0600:wick=0.0600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=638.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.92 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 215.94 | **NEUTRAL** |
| `B04_income` | 54.43 | **GOOD** |
| `B05_profit_margin` | 25.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 3.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=3.8 vs prior_export=3.8 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.07 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=638.0 (this export) | prior_export=638.0 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.92 (this export) | prior_export=4.92 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ET  ·  score **+15**  ·  Oil & Gas Midstream
price=20.100000381469727  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=36.67 on 2026-10-01; prev RSI=26.27 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | cross_up | RSI 26.27@2026-09-30 → 36.67@2026-10-01 vs 30 | rule: cross_up=GOOD | **GOOD** |
| `A03_rsi_cross_50` | below | RSI 26.27@2026-09-30 → 36.67@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 26.27@2026-09-30 → 36.67@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=1.538 (G=0.4000 R=0.2600); 2026-09-30:RED:O=20.0300,C=19.7700,body=-0.2600,vol=8840900.0; 2026-10-01:GREEN:O=19.7000,C=20.1000,body=+0.4000,vol=12617500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.427 (Gvol=12617500 Rvol=8840900); 2026-09-30:RED:O=20.0300,C=19.7700,body=-0.2600,vol=8840900.0; 2026-10-01:GREEN:O=19.7000,C=20.1000,body=+0.4000,vol=12617500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.487 on 2026-10-01: today_vol=12617500 / avg20=8486615 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.599 on 2026-10-01 (price=20.1000, mid=20.8860, upper=22.1984, lower=19.5736; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-01: price=20.1000 vs SMA50=20.7649 dist=-3.20% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=20.8860 SMA50=20.7649 SMA80=20.1477 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-06@18.9334; S2[2026-08-04→2026-09-01] low=2026-08-05@19.8579; S3[2026-09-02→2026-10-01] low=2026-10-01@19.5900 | lows=[18.93335723876953, 19.85789603332627, 19.59000015258789] span=4.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.7407413948395528 wick_frac=0.2592586051604472 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.6500011920940324 wick_frac=0.3499988079059676 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.538458716942376 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:RED:body=-0.1200:wick=0.1700; 2026-09-28:RED:body=-0.1700:wick=0.2900; 2026-09-29:RED:body=-0.0300:wick=0.2100; 2026-09-30:RED:body=-0.2600:wick=0.1400; 2026-10-01:GREEN:body=+0.4000:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=55.43 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.9 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 107379.0 | **NEUTRAL** |
| `B04_income` | 5048.0 | **GOOD** |
| `B05_profit_margin` | 4.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.75 vs prior_export=24.75 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | 0.25 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.25 vs prior=0.25 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.13 | **GOOD** |
| `B13_short_float` | 1.03 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=55.43 (this export) | prior_export=55.43 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.9 (this export) | prior_export=23.9 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ECO  ·  score **+15**  ·  Marine Shipping
price=85.73999786376953  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.96 on 2026-10-01; prev RSI=65.20 on 2026-09-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 65.20@2026-09-30 → 71.96@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.20@2026-09-30 → 71.96@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 65.20@2026-09-30 → 71.96@2026-10-01 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=6.000 (G=5.0400 R=0.8400); 2026-09-30:RED:O=81.7200,C=80.8800,body=-0.8400,vol=448300.0; 2026-10-01:GREEN:O=80.7000,C=85.7400,body=+5.0400,vol=731400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.631 (Gvol=731400 Rvol=448300); 2026-09-30:RED:O=81.7200,C=80.8800,body=-0.8400,vol=448300.0; 2026-10-01:GREEN:O=80.7000,C=85.7400,body=+5.0400,vol=731400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.950 on 2026-10-01: today_vol=731400 / avg20=769551 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.685 on 2026-10-01 (price=85.7400, mid=78.5240, upper=89.0602, lower=67.9878; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=85.7400 vs SMA50=67.2890 dist=+27.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=78.5240 SMA50=67.2890 SMA80=60.1267 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-01 (63 bars); S1[2026-07-01→2026-08-03] low=2026-07-01@45.3396; S2[2026-08-04→2026-09-01] low=2026-08-04@54.2847; S3[2026-09-02→2026-10-01] low=2026-09-02@67.4600 | lows=[45.339633350333564, 54.28474426269531, 67.45999908447266] span=48.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.9190384503017538 wick_frac=0.08096154969824623 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.2625014901175405 wick_frac=0.7374985098824596 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.999972752291078 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:RED:body=-0.1100:wick=3.2000; 2026-09-28:RED:body=-0.3800:wick=1.8100; 2026-09-29:GREEN:body=+1.8600:wick=0.6000; 2026-09-30:RED:body=-0.8400:wick=2.3600; 2026-10-01:GREEN:body=+5.0400:wick=0.4440 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.23 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.73 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 706.47 | **NEUTRAL** |
| `B04_income` | 402.14 | **GOOD** |
| `B05_profit_margin` | 56.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.21999999999999886 (now=70.88 vs prior_export=70.66 on finviz_2026-10-01) | **GOOD** |
| `B09_analyst_recom` | 2.25 | **GOOD** |
| `B10_insider_transactions` | -0.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.82 vs prior=-0.82 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.76 | **GOOD** |
| `B13_short_float` | 5.96 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.23 (this export) | prior_export=28.23 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.73 (this export) | prior_export=21.73 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+15**  ·  Marine Shipping
price=30.43000030517578  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.25 on 2026-10-01; prev RSI=42.61 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.61@2026-09-30 → 51.25@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.61@2026-09-30 → 51.25@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.61@2026-09-30 → 51.25@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=2.485 (G=0.8200 R=0.3300); 2026-09-30:RED:O=29.6900,C=29.3600,body=-0.3300,vol=2125200.0; 2026-10-01:GREEN:O=29.6100,C=30.4300,body=+0.8200,vol=2098000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=0.987 (Gvol=2098000 Rvol=2125200); 2026-09-30:RED:O=29.6900,C=29.3600,body=-0.3300,vol=2125200.0; 2026-10-01:GREEN:O=29.6100,C=30.4300,body=+0.8200,vol=2098000.0 | **BAD** |
| `A07_rvol` | RVOL=1.077 on 2026-10-01: today_vol=2098000 / avg20=1948531 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.196 on 2026-10-01 (price=30.4300, mid=30.8160, upper=32.7865, lower=28.8455; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=30.4300 vs SMA50=29.7557 dist=+2.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=30.8160 SMA50=29.7557 SMA80=28.3716 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-06-30@24.6600; S2[2026-08-04→2026-09-01] low=2026-08-12@27.1500; S3[2026-09-02→2026-10-01] low=2026-09-29@28.9400 | lows=[24.65999984741211, 27.149999618530273, 28.940000534057617] span=17.36% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.6507932903725838 wick_frac=0.3492067096274162 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.39759030607203344 wick_frac=0.6024096939279665 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4848481345548072 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:RED:body=-0.2700:wick=0.5000; 2026-09-28:RED:body=-0.3000:wick=0.1300; 2026-09-29:GREEN:body=+0.0500:wick=0.5899; 2026-09-30:RED:body=-0.3300:wick=0.5000; 2026-10-01:GREEN:body=+0.8200:wick=0.4400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.6200000000000045 (now=35.55 vs prior_export=36.17 on finviz_2026-10-01) | **BAD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.57 | **GOOD** |
| `B13_short_float` | 3.28 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INOD  ·  score **+15**  ·  Information Technology Services
price=71.8499984741211  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.82 on 2026-10-01; prev RSI=57.14 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.14@2026-09-30 → 65.82@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.14@2026-09-30 → 65.82@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.14@2026-09-30 → 65.82@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=4.612 (G=5.3500 R=1.1600); 2026-09-30:RED:O=66.9200,C=65.7600,body=-1.1600,vol=1644700.0; 2026-10-01:GREEN:O=66.5000,C=71.8500,body=+5.3500,vol=2437000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.482 (Gvol=2437000 Rvol=1644700); 2026-09-30:RED:O=66.9200,C=65.7600,body=-1.1600,vol=1644700.0; 2026-10-01:GREEN:O=66.5000,C=71.8500,body=+5.3500,vol=2437000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.639 on 2026-10-01: today_vol=2437000 / avg20=1486489 (avg window 2026-09-02→2026-09-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.745 on 2026-10-01 (price=71.8500, mid=61.1180, upper=75.5276, lower=46.7084; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=71.8500 vs SMA50=60.8716 dist=+18.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=61.12_50=60.87_80=67.64 on 2026-10-01: SMA20=61.1180 SMA50=60.8716 SMA80=67.6440 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-29@54.3600; S2[2026-08-04→2026-09-01] low=2026-09-01@54.1400; S3[2026-09-02→2026-10-01] low=2026-09-14@51.9700 | lows=[54.36000061035156, 54.13999938964844, 51.970001220703125] span=4.60% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.5355350883222214 wick_frac=0.46446491167777854 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.35258228264927755 wick_frac=0.6474177173507225 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.612083423768276 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:RED:body=-4.1700:wick=1.8300; 2026-09-28:RED:body=-1.7546:wick=2.0454; 2026-09-29:RED:body=-0.4500:wick=2.6600; 2026-09-30:RED:body=-1.1600:wick=2.1300; 2026-10-01:GREEN:body=+5.3500:wick=4.6400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=92.49 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 317.16 | **NEUTRAL** |
| `B04_income` | 46.48 | **GOOD** |
| `B05_profit_margin` | 14.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 123.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=123.67 vs prior_export=123.67 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | -49.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-49.78 vs prior=-49.78 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.2 | **GOOD** |
| `B13_short_float` | 14.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=92.49 (this export) | prior_export=92.49 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.76 (this export) | prior_export=6.76 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BP  ·  score **+15**  ·  Oil & Gas Integrated
price=44.5  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.31 on 2026-10-01; prev RSI=49.29 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.29@2026-09-30 → 52.31@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.29@2026-09-30 → 52.31@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.29@2026-09-30 → 52.31@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4200 R=0.0000); 2026-09-30:GREEN:O=43.5000,C=43.9900,body=+0.4900,vol=7325300.0; 2026-10-01:GREEN:O=43.5700,C=44.5000,body=+0.9300,vol=11632700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=99.000 (Gvol=18958000 Rvol=0); 2026-09-30:GREEN:O=43.5000,C=43.9900,body=+0.4900,vol=7325300.0; 2026-10-01:GREEN:O=43.5700,C=44.5000,body=+0.9300,vol=11632700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.299 on 2026-10-01: today_vol=11632700 / avg20=8953203 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.097 on 2026-10-01 (price=44.5000, mid=44.7075, upper=46.8542, lower=42.5608; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=44.5000 vs SMA50=43.6227 dist=+2.01% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=44.7075 SMA50=43.6227 SMA80=42.0859 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-06@36.8076; S2[2026-08-04→2026-09-01] low=2026-08-05@40.6603; S3[2026-09-02→2026-10-01] low=2026-09-22@42.4700 | lows=[36.80762387799942, 40.66027501103988, 42.470001220703125] span=15.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.7014579670043606 wick_frac=0.2985420329956395 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:GREEN:body=+0.1400:wick=0.5150; 2026-09-28:RED:body=-0.3650:wick=0.4100; 2026-09-29:GREEN:body=+0.1650:wick=0.5250; 2026-09-30:GREEN:body=+0.4900:wick=0.4900; 2026-10-01:GREEN:body=+0.9300:wick=0.1000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.35 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.04 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 216829.8 | **NEUTRAL** |
| `B04_income` | 5490.34 | **GOOD** |
| `B05_profit_margin` | 2.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-2.3100000000000023 (now=50.54 vs prior_export=52.85 on finviz_2026-10-01) | **BAD** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 60.28 | **GOOD** |
| `B13_short_float` | 0.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.35 (this export) | prior_export=18.35 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.04 (this export) | prior_export=11.04 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVX  ·  score **+15**  ·  Oil & Gas Integrated
price=207.10000610351562  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.44 on 2026-10-01; prev RSI=47.45 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.45@2026-09-30 → 52.44@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.45@2026-09-30 → 52.44@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.45@2026-09-30 → 52.44@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=4.284 (G=3.7700 R=0.8800); 2026-09-30:RED:O=205.0900,C=204.2100,body=-0.8800,vol=6390200.0; 2026-10-01:GREEN:O=203.3300,C=207.1000,body=+3.7700,vol=5999700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=0.939 (Gvol=5999700 Rvol=6390200); 2026-09-30:RED:O=205.0900,C=204.2100,body=-0.8800,vol=6390200.0; 2026-10-01:GREEN:O=203.3300,C=207.1000,body=+3.7700,vol=5999700.0 | **BAD** |
| `A07_rvol` | RVOL=0.607 on 2026-10-01: today_vol=5999700 / avg20=9889600 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.205 on 2026-10-01 (price=207.1000, mid=208.8330, upper=217.2877, lower=200.3783; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=207.1000 vs SMA50=202.2319 dist=+2.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=208.8330 SMA50=202.2319 SMA80=193.3365 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-01 (63 bars); S1[2026-07-06→2026-08-03] low=2026-07-06@167.1600; S2[2026-08-04→2026-09-01] low=2026-08-07@185.8700; S3[2026-09-02→2026-10-01] low=2026-09-22@200.7800 | lows=[167.16000366210938, 185.8699951171875, 200.77999877929688] span=20.11% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.8213523486586217 wick_frac=0.17864765134137828 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.33587452825793224 wick_frac=0.6641254717420677 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.284146278025351 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:GREEN:body=+0.6450:wick=2.4038; 2026-09-28:RED:body=-0.6300:wick=2.2343; 2026-09-29:GREEN:body=+1.3300:wick=1.4400; 2026-09-30:RED:body=-0.8800:wick=1.7400; 2026-10-01:GREEN:body=+3.7700:wick=0.8200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.18 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.69 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 211454.0 | **NEUTRAL** |
| `B04_income` | 20591.0 | **GOOD** |
| `B05_profit_margin` | 9.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 224.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.12999999999999545 (now=224.75 vs prior_export=224.62 on finviz_2026-10-01) | **GOOD** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -20.49 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.36999999999999744 (now=-20.49 vs prior=-20.12 on finviz_2026-10-01) | **BAD** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 1.05 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.18 (this export) | prior_export=9.18 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.69 (this export) | prior_export=11.69 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SLDE  ·  score **+15**  ·  Insurance - Property & Casualty
price=23.6299991607666  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.91 on 2026-10-01; prev RSI=39.61 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.61@2026-09-30 → 50.91@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 39.61@2026-09-30 → 50.91@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 39.61@2026-09-30 → 50.91@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=4.778 (G=1.2900 R=0.2700); 2026-09-30:RED:O=22.5800,C=22.3100,body=-0.2700,vol=905400.0; 2026-10-01:GREEN:O=22.3400,C=23.6300,body=+1.2900,vol=2405600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=2.657 (Gvol=2405600 Rvol=905400); 2026-09-30:RED:O=22.5800,C=22.3100,body=-0.2700,vol=905400.0; 2026-10-01:GREEN:O=22.3400,C=23.6300,body=+1.2900,vol=2405600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.605 on 2026-10-01: today_vol=2405600 / avg20=1498918 (avg window 2026-09-02→2026-09-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.270 on 2026-10-01 (price=23.6300, mid=24.2595, upper=26.5890, lower=21.9300; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=23.6300 vs SMA50=22.7861 dist=+3.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-01: SMA20=24.2595 SMA50=22.7861 SMA80=21.2343 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-07-23@19.1477; S2[2026-08-04→2026-09-01] low=2026-08-04@20.2342; S3[2026-09-02→2026-10-01] low=2026-10-01@22.2550 | lows=[19.14771451817286, 20.234182262062028, 22.2549991607666] span=16.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.9116594865933023 wick_frac=0.08834051340669768 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.529412424687812 wick_frac=0.470587575312188 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.777766004040746 need>1.4; red_wick_gt_green=True 5d trail=2026-09-25:RED:body=-0.4500:wick=0.3250; 2026-09-28:RED:body=-0.2100:wick=0.1650; 2026-09-29:RED:body=-0.2400:wick=0.4300; 2026-09-30:RED:body=-0.2700:wick=0.2400; 2026-10-01:GREEN:body=+1.2900:wick=0.1250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.75 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.66 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1388.8 | **NEUTRAL** |
| `B04_income` | 555.76 | **GOOD** |
| `B05_profit_margin` | 40.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -11.11 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.8099999999999987 (now=-11.11 vs prior=-10.3 on finviz_2026-10-01) | **BAD** |
| `B12_institutional_transactions` | 9.23 | **GOOD** |
| `B13_short_float` | 9.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.75 (this export) | prior_export=24.75 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.66 (this export) | prior_export=7.66 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKY  ·  score **+15**  ·  Residential Construction
price=87.4800033569336  pair=`2026-09-30→2026-10-01`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.74 on 2026-10-01; prev RSI=47.69 on 2026-09-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.69@2026-09-30 → 52.74@2026-10-01 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.69@2026-09-30 → 52.74@2026-10-01 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.69@2026-09-30 → 52.74@2026-10-01 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_body_sum/RED_body_sum=19.066 (G=2.8600 R=0.1500); 2026-09-30:RED:O=85.8500,C=85.7000,body=-0.1500,vol=428400.0; 2026-10-01:GREEN:O=84.6200,C=87.4800,body=+2.8600,vol=540600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-30 + 2026-10-01; ratio=GREEN_vol/RED_vol=1.262 (Gvol=540600 Rvol=428400); 2026-09-30:RED:O=85.8500,C=85.7000,body=-0.1500,vol=428400.0; 2026-10-01:GREEN:O=84.6200,C=87.4800,body=+2.8600,vol=540600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.015 on 2026-10-01: today_vol=540600 / avg20=532494 (avg window 2026-09-02→2026-09-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.549 on 2026-10-01 (price=87.4800, mid=85.5285, upper=89.0824, lower=81.9746; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-01: price=87.4800 vs SMA50=87.3022 dist=+0.20% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=85.53_50=87.30_80=85.30 on 2026-10-01: SMA20=85.5285 SMA50=87.3022 SMA80=85.3030 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-30→2026-10-01 (63 bars); S1[2026-06-30→2026-08-03] low=2026-07-08@78.2200; S2[2026-08-04→2026-09-01] low=2026-08-05@78.9300; S3[2026-09-02→2026-10-01] low=2026-09-10@80.3800 | lows=[78.22000122070312, 78.93000030517578, 80.37999725341797] span=2.76% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: GREEN body_frac=0.5777782572910855 wick_frac=0.42222174270891455 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-30+2026-10-01: RED body_frac=0.08108197275685305 wick_frac=0.918918027243147 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.066476781445502 need>1.4; red_wick_gt_green=False 5d trail=2026-09-25:GREEN:body=+1.8100:wick=0.9299; 2026-09-28:RED:body=-0.0100:wick=1.8950; 2026-09-29:RED:body=-0.7200:wick=1.5800; 2026-09-30:RED:body=-0.1500:wick=1.7000; 2026-10-01:GREEN:body=+2.8600:wick=2.0900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.84 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.18 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2672.55 | **NEUTRAL** |
| `B04_income` | 191.37 | **GOOD** |
| `B05_profit_margin` | 7.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.5 vs prior_export=103.5 on finviz_2026-10-01) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | -1.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.99 vs prior=-1.99 on finviz_2026-10-01) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.08 | **GOOD** |
| `B13_short_float` | 5.42 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.84 (this export) | prior_export=0.84 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.18 (this export) | prior_export=1.18 (finviz_2026-10-01) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-02_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.