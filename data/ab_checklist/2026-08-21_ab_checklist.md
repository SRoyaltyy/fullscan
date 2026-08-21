# A+B1 Feature Checklist — 2026-08-21

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,706** names
- Export: `finviz_2026-08-21.csv` · prior export for Δ: `2026-08-20`
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
| 1 | SON | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 2 | FLS | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Specialty Industrial Machinery |
| 3 | CXT | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Specialty Industrial Machinery |
| 4 | PLX | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Biotechnology |
| 5 | ASH | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Specialty Chemicals |
| 6 | COR | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Distribution |
| 7 | KBR | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 8 | RLI | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Insurance - Property & Casualty |
| 9 | OPCH | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Medical Care Facilities |
| 10 | EVTC | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 11 | RHI | +14 | 16 | 2 | 2026-08-20→2026-08-21 | Staffing & Employment Services |
| 12 | ERO | +14 | 16 | 2 | 2026-08-20→2026-08-21 | Copper |
| 13 | FA | +14 | 16 | 2 | 2026-08-20→2026-08-21 | Specialty Business Services |
| 14 | PAGP | +14 | 16 | 2 | 2026-08-20→2026-08-21 | Oil & Gas Midstream |
| 15 | WAT | +14 | 17 | 3 | 2026-08-20→2026-08-21 | Diagnostics & Research |

## Full checklist — top 15

### SON  ·  score **+17**  ·  Packaging & Containers
price=59.13999938964844  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.57 on 2026-08-21; prev RSI=57.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.59@2026-08-20 → 60.57@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.59@2026-08-20 → 60.57@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.59@2026-08-20 → 60.57@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7000 R=0.0000); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.1400,body=+0.1500,vol=122643.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=699843 Rvol=0); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.1400,body=+0.1500,vol=122643.0 | **GOOD** |
| `A07_rvol` | RVOL=0.121 on 2026-08-21: today_vol=122643 / avg20=1012315 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.702 on 2026-08-21 (price=59.1400, mid=57.8057, upper=59.7063, lower=55.9051; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=59.1400 vs SMA50=54.8980 dist=+7.73% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=57.8057 SMA50=54.8980 SMA80=52.5593 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@45.4827; S2[2026-06-18→2026-07-20] low=2026-06-22@49.4158; S3[2026-07-23→2026-08-21] low=2026-07-23@54.7457 | lows=[45.482707673306166, 49.41576037269584, 54.7456863407041] span=20.37% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5015536053822365 wick_frac=0.4984463946177636 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-1.0000:wick=0.1300; 2026-08-18:RED:body=-0.0700:wick=1.1000; 2026-08-19:GREEN:body=+0.2800:wick=0.5600; 2026-08-20:GREEN:body=+1.5500:wick=0.3800; 2026-08-21:GREEN:body=+0.1500:wick=0.6000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.23 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.29 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7458.91 | **NEUTRAL** |
| `B04_income` | 646.79 | **GOOD** |
| `B05_profit_margin` | 8.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=1.37 vs prior=1.36 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 4.61 | **GOOD** |
| `B13_short_float` | 11.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+17**  ·  Specialty Industrial Machinery
price=78.70999908447266  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.59 on 2026-08-21; prev RSI=53.87 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.87@2026-08-20 → 55.59@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.87@2026-08-20 → 55.59@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.87@2026-08-20 → 55.59@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.467 (G=1.1100 R=0.4500); 2026-08-20:GREEN:O=76.9900,C=78.1000,body=+1.1100,vol=1291900.0; 2026-08-21:RED:O=79.1600,C=78.7100,body=-0.4500,vol=146006.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=8.848 (Gvol=1291900 Rvol=146006); 2026-08-20:GREEN:O=76.9900,C=78.1000,body=+1.1100,vol=1291900.0; 2026-08-21:RED:O=79.1600,C=78.7100,body=-0.4500,vol=146006.0 | **GOOD** |
| `A07_rvol` | RVOL=0.085 on 2026-08-21: today_vol=146006 / avg20=1722760 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.146 on 2026-08-21 (price=78.7100, mid=77.7450, upper=84.3595, lower=71.1305; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=78.7100 vs SMA50=75.3083 dist=+4.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=77.7450 SMA50=75.3083 SMA80=74.3853 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@63.0844; S2[2026-06-18→2026-07-20] low=2026-07-20@66.3300; S3[2026-07-23→2026-08-21] low=2026-07-23@67.9500 | lows=[63.08438243991164, 66.33000183105469, 67.94999694824219] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6491235114864835 wick_frac=0.35087648851351655 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2821343155075098 wick_frac=0.7178656844924902 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4666429310140208 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.1800:wick=1.0900; 2026-08-18:RED:body=-0.5600:wick=1.4900; 2026-08-19:RED:body=-0.8800:wick=2.0000; 2026-08-20:GREEN:body=+1.1100:wick=0.6000; 2026-08-21:RED:body=-0.4500:wick=1.1450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.34 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+16**  ·  Specialty Industrial Machinery
price=49.599998474121094  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.27 on 2026-08-21; prev RSI=45.74 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.74@2026-08-20 → 46.27@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 45.74@2026-08-20 → 46.27@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 45.74@2026-08-20 → 46.27@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.075 (G=1.2300 R=0.4000); 2026-08-20:GREEN:O=48.2300,C=49.4600,body=+1.2300,vol=399700.0; 2026-08-21:RED:O=50.0000,C=49.6000,body=-0.4000,vol=38872.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=10.282 (Gvol=399700 Rvol=38872); 2026-08-20:GREEN:O=48.2300,C=49.4600,body=+1.2300,vol=399700.0; 2026-08-21:RED:O=50.0000,C=49.6000,body=-0.4000,vol=38872.0 | **GOOD** |
| `A07_rvol` | RVOL=0.067 on 2026-08-21: today_vol=38872 / avg20=578290 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.571 on 2026-08-21 (price=49.6000, mid=51.8605, upper=55.8167, lower=47.9043; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=49.6000 vs SMA50=49.1960 dist=+0.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=51.8605 SMA50=49.1960 SMA80=46.1193 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-03@35.7100; S2[2026-06-18→2026-07-20] low=2026-06-23@44.3000; S3[2026-07-23→2026-08-21] low=2026-08-20@47.5800 | lows=[35.709999084472656, 44.29999923706055, 47.58000183105469] span=33.24% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6212120336387018 wick_frac=0.37878796636129813 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2564110088153664 wick_frac=0.7435889911846336 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.074987125445841 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-1.0700:wick=0.7000; 2026-08-18:RED:body=-1.6800:wick=0.0900; 2026-08-19:GREEN:body=+0.5000:wick=0.4900; 2026-08-20:GREEN:body=+1.2300:wick=0.7500; 2026-08-21:RED:body=-0.4000:wick=1.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.7 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.9 | **NEUTRAL** |
| `B04_income` | 140.3 | **GOOD** |
| `B05_profit_margin` | 7.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.17 vs prior_export=70.17 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.93 | **GOOD** |
| `B13_short_float` | 14.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLX  ·  score **+15**  ·  Biotechnology
price=2.424999952316284  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.25 on 2026-08-21; prev RSI=51.64 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.64@2026-08-20 → 54.25@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.64@2026-08-20 → 54.25@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.64@2026-08-20 → 54.25@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.0950 R=0.0000); 2026-08-20:GREEN:O=2.3200,C=2.3800,body=+0.0600,vol=383500.0; 2026-08-21:GREEN:O=2.3900,C=2.4250,body=+0.0350,vol=177326.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=560826 Rvol=0); 2026-08-20:GREEN:O=2.3200,C=2.3800,body=+0.0600,vol=383500.0; 2026-08-21:GREEN:O=2.3900,C=2.4250,body=+0.0350,vol=177326.0 | **GOOD** |
| `A07_rvol` | RVOL=0.248 on 2026-08-21: today_vol=177326 / avg20=715215 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.260 on 2026-08-21 (price=2.4250, mid=2.3782, upper=2.5578, lower=2.1987; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=2.4250 vs SMA50=2.3025 dist=+5.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=2.3782 SMA50=2.3025 SMA80=2.1997 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@1.9000; S2[2026-06-22→2026-07-22] low=2026-06-23@2.1300; S3[2026-07-23→2026-08-21] low=2026-08-14@2.1700 | lows=[1.899999976158142, 2.130000114440918, 2.1700000762939453] span=14.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=1.0750013262080484 wick_frac=-0.07500132620804831 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.0500:wick=0.1000; 2026-08-18:GREEN:body=+0.0400:wick=0.0300; 2026-08-19:RED:body=-0.0200:wick=0.0500; 2026-08-20:GREEN:body=+0.0600:wick=0.0200; 2026-08-21:GREEN:body=+0.0350:wick=-0.0100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=600.0 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=53.64 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 80.62 | **NEUTRAL** |
| `B04_income` | 18.95 | **GOOD** |
| `B05_profit_margin` | 23.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.0 vs prior_export=11.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.99 | **GOOD** |
| `B13_short_float` | 5.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=600.0 (this export) | prior_export=600.0 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=53.64 (this export) | prior_export=53.64 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASH  ·  score **+15**  ·  Specialty Chemicals
price=73.36000061035156  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.63 on 2026-08-21; prev RSI=56.87 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.87@2026-08-20 → 58.63@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.87@2026-08-20 → 58.63@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.87@2026-08-20 → 58.63@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=41.335 (G=1.2400 R=0.0300); 2026-08-20:GREEN:O=71.5800,C=72.8200,body=+1.2400,vol=377300.0; 2026-08-21:RED:O=73.3900,C=73.3600,body=-0.0300,vol=47509.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=7.942 (Gvol=377300 Rvol=47509); 2026-08-20:GREEN:O=71.5800,C=72.8200,body=+1.2400,vol=377300.0; 2026-08-21:RED:O=73.3900,C=73.3600,body=-0.0300,vol=47509.0 | **GOOD** |
| `A07_rvol` | RVOL=0.057 on 2026-08-21: today_vol=47509 / avg20=838535 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.168 on 2026-08-21 (price=73.3600, mid=72.4610, upper=77.8226, lower=67.0994; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=73.3600 vs SMA50=68.2924 dist=+7.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=72.4610 SMA50=68.2924 SMA80=63.4347 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@53.7671; S2[2026-06-18→2026-07-20] low=2026-06-29@62.1100; S3[2026-07-23→2026-08-21] low=2026-07-27@65.6300 | lows=[53.76713122146996, 62.11000061035156, 65.62999725341797] span=22.06% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5821593721680761 wick_frac=0.4178406278319239 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.03211185247494835 wick_frac=0.9678881475250517 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=41.33494404883011 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.4800:wick=0.7500; 2026-08-18:RED:body=-0.3500:wick=1.1400; 2026-08-19:RED:body=-2.0500:wick=0.4600; 2026-08-20:GREEN:body=+1.2400:wick=0.8900; 2026-08-21:RED:body=-0.0300:wick=0.9042 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.55 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1843.0 | **NEUTRAL** |
| `B04_income` | 52.0 | **GOOD** |
| `B05_profit_margin` | 2.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=80.18 vs prior_export=80.18 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.74 | **GOOD** |
| `B13_short_float` | 10.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.55 (this export) | prior_export=3.55 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### COR  ·  score **+15**  ·  Medical Distribution
price=319.3500061035156  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.49 on 2026-08-21; prev RSI=53.24 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.24@2026-08-20 → 56.49@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.24@2026-08-20 → 56.49@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.24@2026-08-20 → 56.49@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=57.635 (G=4.6100 R=0.0800); 2026-08-20:RED:O=314.7700,C=314.6900,body=-0.0800,vol=1175800.0; 2026-08-21:GREEN:O=314.7400,C=319.3500,body=+4.6100,vol=202489.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.172 (Gvol=202489 Rvol=1175800); 2026-08-20:RED:O=314.7700,C=314.6900,body=-0.0800,vol=1175800.0; 2026-08-21:GREEN:O=314.7400,C=319.3500,body=+4.6100,vol=202489.0 | **BAD** |
| `A07_rvol` | RVOL=0.135 on 2026-08-21: today_vol=202489 / avg20=1499675 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.268 on 2026-08-21 (price=319.3500, mid=315.8441, upper=328.9241, lower=302.7641; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=319.3500 vs SMA50=300.3554 dist=+6.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=315.8441 SMA50=300.3554 SMA80=290.8305 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-03@260.7843; S2[2026-06-18→2026-07-20] low=2026-06-22@268.7788; S3[2026-07-23→2026-08-21] low=2026-07-23@297.8524 | lows=[260.7843028852483, 268.7787651178076, 297.8524010306813] span=14.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6809487961990452 wick_frac=0.31905120380095475 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.014232421249260142 wick_frac=0.9857675787507398 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=57.63487218618848 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:GREEN:body=+0.5200:wick=6.2800; 2026-08-18:GREEN:body=+0.1400:wick=5.9400; 2026-08-19:GREEN:body=+3.0700:wick=7.4100; 2026-08-20:RED:body=-0.0800:wick=5.5400; 2026-08-21:GREEN:body=+4.6100:wick=2.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.07 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.52 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 332771.32 | **NEUTRAL** |
| `B04_income` | 2624.79 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 368.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=368.92 vs prior_export=368.92 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.68 | **GOOD** |
| `B10_insider_transactions` | 0.5 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.14 (now=0.5 vs prior=0.36 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 0.98 | **GOOD** |
| `B13_short_float` | 2.72 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.07 (this export) | prior_export=3.07 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.52 (this export) | prior_export=0.52 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+15**  ·  Engineering & Construction
price=38.810001373291016  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.03 on 2026-08-21; prev RSI=55.61 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.61@2026-08-20 → 60.03@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.61@2026-08-20 → 60.03@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.61@2026-08-20 → 60.03@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8700 R=0.0000); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.8100,body=+0.8700,vol=175119.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.304 (Gvol=750769 Rvol=575650); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.8100,body=+0.8700,vol=175119.0 | **GOOD** |
| `A07_rvol` | RVOL=0.112 on 2026-08-21: today_vol=175119 / avg20=1561180 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.685 on 2026-08-21 (price=38.8100, mid=37.4320, upper=39.4427, lower=35.4213; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.8100 vs SMA50=35.8976 dist=+8.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=37.4320 SMA50=35.8976 SMA80=35.2052 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@30.9469; S2[2026-06-18→2026-07-20] low=2026-06-22@31.6100; S3[2026-07-23→2026-08-21] low=2026-07-30@32.1400 | lows=[30.946867052586228, 31.610000610351562, 32.13999938964844] span=3.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=1.0235344804372997 wick_frac=-0.023534480437299728 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.8200:wick=0.0900; 2026-08-18:RED:body=-0.2600:wick=0.5300; 2026-08-19:GREEN:body=+0.2700:wick=0.6400; 2026-08-20:DOJI:body=+0.0000:wick=0.9600; 2026-08-21:GREEN:body=+0.8700:wick=-0.0200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.1 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RLI  ·  score **+15**  ·  Insurance - Property & Casualty
price=65.77999877929688  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.12 on 2026-08-21; prev RSI=61.10 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.10@2026-08-20 → 63.12@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.10@2026-08-20 → 63.12@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.10@2026-08-20 → 63.12@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4400 R=0.0000); 2026-08-20:GREEN:O=64.0700,C=65.1000,body=+1.0300,vol=828700.0; 2026-08-21:GREEN:O=65.3700,C=65.7800,body=+0.4100,vol=100211.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=928911 Rvol=0); 2026-08-20:GREEN:O=64.0700,C=65.1000,body=+1.0300,vol=828700.0; 2026-08-21:GREEN:O=65.3700,C=65.7800,body=+0.4100,vol=100211.0 | **GOOD** |
| `A07_rvol` | RVOL=0.118 on 2026-08-21: today_vol=100211 / avg20=847885 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.789 on 2026-08-21 (price=65.7800, mid=63.6295, upper=66.3557, lower=60.9033; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=65.7800 vs SMA50=59.9316 dist=+9.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=63.6295 SMA50=59.9316 SMA80=55.9134 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-29@48.8900; S2[2026-06-18→2026-07-20] low=2026-06-18@51.7800; S3[2026-07-23→2026-08-21] low=2026-07-23@56.6100 | lows=[48.88999938964844, 51.779998779296875, 56.61000061035156] span=15.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6397012403193557 wick_frac=0.3602987596806443 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.8000:wick=1.0200; 2026-08-18:GREEN:body=+0.7100:wick=0.2800; 2026-08-19:GREEN:body=+0.0300:wick=1.1900; 2026-08-20:GREEN:body=+1.0300:wick=0.3900; 2026-08-21:GREEN:body=+0.4100:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.8 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.93 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1978.84 | **NEUTRAL** |
| `B04_income` | 438.7 | **GOOD** |
| `B05_profit_margin` | 22.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=60.5 vs prior_export=60.5 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 3.1 | **NEUTRAL** |
| `B10_insider_transactions` | 1.29 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.29 vs prior=1.29 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.3 | **GOOD** |
| `B13_short_float` | 8.78 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.8 (this export) | prior_export=14.8 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.93 (this export) | prior_export=1.93 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OPCH  ·  score **+15**  ·  Medical Care Facilities
price=23.920000076293945  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.50 on 2026-08-21; prev RSI=54.62 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.62@2026-08-20 → 58.50@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.62@2026-08-20 → 58.50@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.62@2026-08-20 → 58.50@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8700 R=0.0000); 2026-08-20:GREEN:O=23.0500,C=23.4900,body=+0.4400,vol=1466800.0; 2026-08-21:GREEN:O=23.4900,C=23.9200,body=+0.4300,vol=300353.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1767153 Rvol=0); 2026-08-20:GREEN:O=23.0500,C=23.4900,body=+0.4400,vol=1466800.0; 2026-08-21:GREEN:O=23.4900,C=23.9200,body=+0.4300,vol=300353.0 | **GOOD** |
| `A07_rvol` | RVOL=0.116 on 2026-08-21: today_vol=300353 / avg20=2578755 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.316 on 2026-08-21 (price=23.9200, mid=23.4415, upper=24.9564, lower=21.9266; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=23.9200 vs SMA50=22.2550 dist=+7.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=23.4415 SMA50=22.2550 SMA80=21.9960 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@19.4300; S2[2026-06-18→2026-07-20] low=2026-06-30@20.8200; S3[2026-07-23→2026-08-21] low=2026-07-23@20.6800 | lows=[19.43000030517578, 20.81999969482422, 20.68000030517578] span=7.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6454859364653609 wick_frac=0.3545140635346391 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.4200:wick=0.2400; 2026-08-18:RED:body=-0.3100:wick=0.4500; 2026-08-19:RED:body=-0.1800:wick=0.5900; 2026-08-20:GREEN:body=+0.4400:wick=0.3800; 2026-08-21:GREEN:body=+0.4300:wick=0.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.56 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.6 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5693.52 | **NEUTRAL** |
| `B04_income` | 209.58 | **GOOD** |
| `B05_profit_margin` | 3.68 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.58 vs prior_export=28.58 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 3.27 | **GOOD** |
| `B11_insider_tx_delta` | delta=1.3800000000000001 (now=3.27 vs prior=1.89 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | -27.69 | **BAD** |
| `B13_short_float` | 8.85 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.56 (this export) | prior_export=5.56 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.6 (this export) | prior_export=1.6 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVTC  ·  score **+15**  ·  Software - Infrastructure
price=30.18000030517578  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.04 on 2026-08-21; prev RSI=48.48 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.48@2026-08-20 → 51.04@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.48@2026-08-20 → 51.04@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.48@2026-08-20 → 51.04@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3600 R=0.0000); 2026-08-20:GREEN:O=29.6700,C=29.7300,body=+0.0600,vol=219100.0; 2026-08-21:GREEN:O=29.8800,C=30.1800,body=+0.3000,vol=102532.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=321632 Rvol=0); 2026-08-20:GREEN:O=29.6700,C=29.7300,body=+0.0600,vol=219100.0; 2026-08-21:GREEN:O=29.8800,C=30.1800,body=+0.3000,vol=102532.0 | **GOOD** |
| `A07_rvol` | RVOL=0.260 on 2026-08-21: today_vol=102532 / avg20=394905 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.267 on 2026-08-21 (price=30.1800, mid=30.6860, upper=32.5842, lower=28.7878; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=30.1800 vs SMA50=28.8052 dist=+4.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=30.6860 SMA50=28.8052 SMA80=27.5603 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-05@21.8100; S2[2026-06-18→2026-07-20] low=2026-06-24@24.9200; S3[2026-07-23→2026-08-21] low=2026-07-23@28.7000 | lows=[21.809999465942383, 24.920000076293945, 28.700000762939453] span=31.59% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.3669708844927697 wick_frac=0.6330291155072303 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.7300:wick=0.8200; 2026-08-18:RED:body=-1.1700:wick=0.2700; 2026-08-19:RED:body=-0.1200:wick=0.5800; 2026-08-20:GREEN:body=+0.0600:wick=0.4600; 2026-08-21:GREEN:body=+0.3000:wick=0.1850 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.8 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 996.16 | **NEUTRAL** |
| `B04_income` | 97.58 | **GOOD** |
| `B05_profit_margin` | 9.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.6 vs prior_export=35.6 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 3.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.10999999999999988 (now=3.55 vs prior=3.44 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 4.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.8 (this export) | prior_export=4.8 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RHI  ·  score **+14**  ·  Staffing & Employment Services
price=45.45000076293945  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.75 on 2026-08-21; prev RSI=63.12 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.12@2026-08-20 → 66.75@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.12@2026-08-20 → 66.75@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.12@2026-08-20 → 66.75@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2500 R=0.0000); 2026-08-20:GREEN:O=43.2000,C=43.8800,body=+0.6800,vol=1633100.0; 2026-08-21:GREEN:O=43.8800,C=45.4500,body=+1.5700,vol=360952.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1994052 Rvol=0); 2026-08-20:GREEN:O=43.2000,C=43.8800,body=+0.6800,vol=1633100.0; 2026-08-21:GREEN:O=43.8800,C=45.4500,body=+1.5700,vol=360952.0 | **GOOD** |
| `A07_rvol` | RVOL=0.159 on 2026-08-21: today_vol=360952 / avg20=2266990 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.812 on 2026-08-21 (price=45.4500, mid=41.4025, upper=46.3871, lower=36.4179; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=45.4500 vs SMA50=36.6196 dist=+24.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=41.4025 SMA50=36.6196 SMA80=32.9840 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@23.9682; S2[2026-06-18→2026-07-20] low=2026-06-24@28.6000; S3[2026-07-23→2026-08-21] low=2026-07-24@34.7700 | lows=[23.96817541848597, 28.600000381469727, 34.77000045776367] span=45.07% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.695488111550602 wick_frac=0.304511888449398 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.2200:wick=0.6300; 2026-08-18:GREEN:body=+0.8800:wick=0.4700; 2026-08-19:RED:body=-0.2200:wick=1.8200; 2026-08-20:GREEN:body=+0.6800:wick=0.7500; 2026-08-21:GREEN:body=+1.5700:wick=0.1450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.93 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5293.4 | **NEUTRAL** |
| `B04_income` | 114.78 | **GOOD** |
| `B05_profit_margin` | 2.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 34.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=34.78 vs prior_export=34.78 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.65 | **GOOD** |
| `B13_short_float` | 23.76 | **GOOD** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.93 (this export) | prior_export=0.93 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ERO  ·  score **+14**  ·  Copper
price=38.29999923706055  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.56 on 2026-08-21; prev RSI=66.11 on 2026-08-20 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 66.11@2026-08-20 → 70.56@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.11@2026-08-20 → 70.56@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 66.11@2026-08-20 → 70.56@2026-08-21 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.2600 R=0.0000); 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1677400.0; 2026-08-21:GREEN:O=37.6400,C=38.3000,body=+0.6600,vol=1411877.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3089277 Rvol=0); 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1677400.0; 2026-08-21:GREEN:O=37.6400,C=38.3000,body=+0.6600,vol=1411877.0 | **GOOD** |
| `A07_rvol` | RVOL=1.088 on 2026-08-21: today_vol=1411877 / avg20=1297140 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.756 on 2026-08-21 (price=38.3000, mid=31.7840, upper=40.4013, lower=23.1667; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.3000 vs SMA50=28.7334 dist=+33.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=31.7840 SMA50=28.7334 SMA80=28.4835 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-09@25.1510; S2[2026-06-22→2026-07-21] low=2026-07-08@22.9320; S3[2026-07-23→2026-08-21] low=2026-07-29@24.7020 | lows=[25.150999069213867, 22.93199920654297, 24.70199966430664] span=9.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7278785025918675 wick_frac=0.2721214974081324 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.6200:wick=0.6300; 2026-08-18:RED:body=-1.0100:wick=0.5720; 2026-08-19:RED:body=-0.3300:wick=0.8000; 2026-08-20:GREEN:body=+2.6000:wick=0.1800; 2026-08-21:GREEN:body=+0.6600:wick=0.6080 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.31 vs prior_export=36.31 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.21 | **GOOD** |
| `B13_short_float` | 5.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.7 (this export) | prior_export=12.7 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.4 (this export) | prior_export=2.4 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FA  ·  score **+14**  ·  Specialty Business Services
price=21.2450008392334  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.12 on 2026-08-21; prev RSI=52.05 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.05@2026-08-20 → 52.12@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.05@2026-08-20 → 52.12@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.05@2026-08-20 → 52.12@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=11.200 (G=0.2800 R=0.0250); 2026-08-20:GREEN:O=20.9500,C=21.2300,body=+0.2800,vol=1149500.0; 2026-08-21:RED:O=21.2700,C=21.2450,body=-0.0250,vol=305439.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=3.763 (Gvol=1149500 Rvol=305439); 2026-08-20:GREEN:O=20.9500,C=21.2300,body=+0.2800,vol=1149500.0; 2026-08-21:RED:O=21.2700,C=21.2450,body=-0.0250,vol=305439.0 | **GOOD** |
| `A07_rvol` | RVOL=0.119 on 2026-08-21: today_vol=305439 / avg20=2566385 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.026 on 2026-08-21 (price=21.2450, mid=21.3102, upper=23.8529, lower=18.7676; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=21.2450 vs SMA50=19.5760 dist=+8.53% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=21.3102 SMA50=19.5760 SMA80=17.7755 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@14.5100; S2[2026-06-18→2026-07-20] low=2026-06-18@15.5350; S3[2026-07-23→2026-08-21] low=2026-07-23@19.0450 | lows=[14.510000228881836, 15.53499984741211, 19.045000076293945] span=31.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6829241061044483 wick_frac=0.3170758938955517 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.07142740366537512 wick_frac=0.9285725963346249 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.200122072175173 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-1.0500:wick=0.5200; 2026-08-18:GREEN:body=+0.5600:wick=0.0990; 2026-08-19:GREEN:body=+0.1900:wick=0.5900; 2026-08-20:GREEN:body=+0.2800:wick=0.1300; 2026-08-21:RED:body=-0.0250:wick=0.3250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=20.07 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.13 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1663.13 | **NEUTRAL** |
| `B04_income` | 25.14 | **GOOD** |
| `B05_profit_margin` | 1.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.25 vs prior_export=26.25 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | -11.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-11.77 vs prior=-11.77 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.68 | **GOOD** |
| `B13_short_float` | 15.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.07 (this export) | prior_export=20.07 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.13 (this export) | prior_export=8.13 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+14**  ·  Oil & Gas Midstream
price=26.850000381469727  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.85 on 2026-08-21; prev RSI=66.44 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.44@2026-08-20 → 66.85@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.44@2026-08-20 → 66.85@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.44@2026-08-20 → 66.85@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=19.500 (G=0.3900 R=0.0200); 2026-08-20:GREEN:O=26.4100,C=26.8000,body=+0.3900,vol=1427400.0; 2026-08-21:RED:O=26.8700,C=26.8500,body=-0.0200,vol=567810.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=2.514 (Gvol=1427400 Rvol=567810); 2026-08-20:GREEN:O=26.4100,C=26.8000,body=+0.3900,vol=1427400.0; 2026-08-21:RED:O=26.8700,C=26.8500,body=-0.0200,vol=567810.0 | **GOOD** |
| `A07_rvol` | RVOL=0.340 on 2026-08-21: today_vol=567810 / avg20=1669655 (avg window 2026-07-24→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.921 on 2026-08-21 (price=26.8500, mid=25.7755, upper=26.9418, lower=24.6092; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=26.8500 vs SMA50=24.7931 dist=+8.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=25.7755 SMA50=24.7931 SMA80=24.4850 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-18@22.4514; S2[2026-06-22→2026-07-23] low=2026-06-22@22.6876; S3[2026-07-24→2026-08-21] low=2026-08-07@24.6700 | lows=[22.451422571353564, 22.6876479865078, 24.670000076293945] span=9.88% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6724128292232158 wick_frac=0.3275871707767843 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.05555673292925868 wick_frac=0.9444432670707413 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.499523173755485 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.0600:wick=0.2300; 2026-08-18:GREEN:body=+0.3200:wick=0.0600; 2026-08-19:RED:body=-0.1700:wick=0.3600; 2026-08-20:GREEN:body=+0.3900:wick=0.1900; 2026-08-21:RED:body=-0.0200:wick=0.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.0 vs prior_export=25.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 8.04 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+14**  ·  Diagnostics & Research
price=408.4100036621094  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.96 on 2026-08-21; prev RSI=56.08 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.08@2026-08-20 → 57.96@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.08@2026-08-20 → 57.96@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.08@2026-08-20 → 57.96@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.463 (G=7.7700 R=5.3100); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=408.4100,body=+7.7700,vol=278463.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.226 (Gvol=278463 Rvol=1233800); 2026-08-20:RED:O=410.0000,C=404.6900,body=-5.3100,vol=1233800.0; 2026-08-21:GREEN:O=400.6400,C=408.4100,body=+7.7700,vol=278463.0 | **BAD** |
| `A07_rvol` | RVOL=0.300 on 2026-08-21: today_vol=278463 / avg20=926920 (avg window 2026-07-23→2026-08-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.314 on 2026-08-21 (price=408.4100, mid=398.6735, upper=429.6723, lower=367.6747; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=408.4100 vs SMA50=380.8314 dist=+7.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=398.6735 SMA50=380.8314 SMA80=365.6343 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@323.8500; S2[2026-06-18→2026-07-20] low=2026-06-22@353.5300; S3[2026-07-23→2026-08-21] low=2026-07-27@367.6700 | lows=[323.8500061035156, 353.5299987792969, 367.6700134277344] span=13.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5764095157025392 wick_frac=0.42359048429746077 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2900053668276715 wick_frac=0.7099946331723285 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4632754399475856 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.7400:wick=9.1600; 2026-08-18:RED:body=-5.9900:wick=2.7200; 2026-08-19:GREEN:body=+10.1100:wick=5.0700; 2026-08-20:RED:body=-5.3100:wick=13.0000; 2026-08-21:GREEN:body=+7.7700:wick=5.7100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3770.58 | **NEUTRAL** |
| `B04_income` | 449.25 | **GOOD** |
| `B05_profit_margin` | 11.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 439.95 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=439.95 vs prior_export=439.95 on finviz_2026-08-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | -1.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.6000000000000001 (now=-1.52 vs prior=-2.12 on finviz_2026-08-20) | **GOOD** |
| `B12_institutional_transactions` | 26.0 | **GOOD** |
| `B13_short_float` | 4.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-08-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-21_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.