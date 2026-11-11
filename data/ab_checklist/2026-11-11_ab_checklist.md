# A+B1 Feature Checklist — 2026-11-11

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,614** names
- Export: `finviz_2026-11-11.csv` · prior export for Δ: `2026-11-10`
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
| 1 | MSCI | +17 | 17 | 0 | 2026-11-09→2026-11-10 | Financial Data & Stock Exchanges |
| 2 | DOCS | +17 | 18 | 1 | 2026-11-09→2026-11-10 | Health Information Services |
| 3 | VNT | +17 | 19 | 2 | 2026-11-09→2026-11-10 | Scientific & Technical Instruments |
| 4 | TTEK | +16 | 17 | 1 | 2026-11-09→2026-11-10 | Engineering & Construction |
| 5 | HONA | +16 | 16 | 0 | 2026-11-09→2026-11-10 | Aerospace & Defense |
| 6 | ED | +16 | 16 | 0 | 2026-11-09→2026-11-10 | Utilities - Regulated Electric |
| 7 | KMB | +16 | 16 | 0 | 2026-11-09→2026-11-10 | Household & Personal Products |
| 8 | AAPL | +16 | 17 | 1 | 2026-11-09→2026-11-10 | Consumer Electronics |
| 9 | CTSH | +16 | 17 | 1 | 2026-11-09→2026-11-10 | Information Technology Services |
| 10 | BSX | +16 | 16 | 0 | 2026-11-09→2026-11-10 | Medical Devices |
| 11 | IFF | +16 | 17 | 1 | 2026-11-09→2026-11-10 | Specialty Chemicals |
| 12 | CELH | +15 | 17 | 2 | 2026-11-09→2026-11-10 | Beverages - Non-Alcoholic |
| 13 | CXT | +15 | 16 | 1 | 2026-11-09→2026-11-10 | Business Equipment & Supplies |
| 14 | TEL | +15 | 16 | 1 | 2026-11-09→2026-11-10 | Electronic Components |
| 15 | BTE | +15 | 16 | 1 | 2026-11-09→2026-11-10 | Oil & Gas E&P |

## Full checklist — top 15

### MSCI  ·  score **+17**  ·  Financial Data & Stock Exchanges
price=656.8599853515625  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.65 on 2026-11-10; prev RSI=64.12 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.12@2026-11-09 → 66.65@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.12@2026-11-09 → 66.65@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.12@2026-11-09 → 66.65@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=7.959 (G=7.7200 R=0.9700); 2026-11-09:RED:O=647.4200,C=646.4500,body=-0.9700,vol=463400.0; 2026-11-10:GREEN:O=649.1400,C=656.8600,body=+7.7200,vol=471631.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.018 (Gvol=471631 Rvol=463400); 2026-11-09:RED:O=647.4200,C=646.4500,body=-0.9700,vol=463400.0; 2026-11-10:GREEN:O=649.1400,C=656.8600,body=+7.7200,vol=471631.0 | **GOOD** |
| `A07_rvol` | RVOL=0.680 on 2026-11-10: today_vol=471631 / avg20=694060 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.630 on 2026-11-10 (price=656.8600, mid=615.9000, upper=680.9036, lower=550.8965; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=656.8600 vs SMA50=578.6092 dist=+13.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=615.9000 SMA50=578.6092 SMA80=575.2866 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@545.1200; S2[2026-09-14→2026-10-12] low=2026-09-29@533.4600; S3[2026-10-13→2026-11-10] low=2026-10-14@557.0000 | lows=[545.1199951171875, 533.4600219726562, 557.0] span=4.41% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5482927083017248 wick_frac=0.45170729169827517 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.07834978356685762 wick_frac=0.9216502164331424 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.958973068210421 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:GREEN:body=+22.5900:wick=4.2401; 2026-11-05:RED:body=-24.0900:wick=6.8300; 2026-11-06:RED:body=-1.0100:wick=11.2000; 2026-11-09:RED:body=-0.9700:wick=11.4100; 2026-11-10:GREEN:body=+7.7200:wick=6.3600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.65 (current export asof; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.47 (current export; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3432.33 | **NEUTRAL** |
| `B04_income` | 1383.67 | **GOOD** |
| `B05_profit_margin` | 40.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 693.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=693.22 vs prior_export=693.22 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | 0.1 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.1 vs prior=0.1 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 2.12 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.65 (this export) | prior_export=1.65 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.47 (this export) | prior_export=0.47 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DOCS  ·  score **+17**  ·  Health Information Services
price=32.41999816894531  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.01 on 2026-11-10; prev RSI=57.77 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.77@2026-11-09 → 55.01@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.77@2026-11-09 → 55.01@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.77@2026-11-09 → 55.01@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=9.000 (G=0.6300 R=0.0700); 2026-11-09:GREEN:O=32.2400,C=32.8700,body=+0.6300,vol=3072600.0; 2026-11-10:RED:O=32.4900,C=32.4200,body=-0.0700,vol=2134197.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.440 (Gvol=3072600 Rvol=2134197); 2026-11-09:GREEN:O=32.2400,C=32.8700,body=+0.6300,vol=3072600.0; 2026-11-10:RED:O=32.4900,C=32.4200,body=-0.0700,vol=2134197.0 | **GOOD** |
| `A07_rvol` | RVOL=0.725 on 2026-11-10: today_vol=2134197 / avg20=2944665 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.158 on 2026-11-10 (price=32.4200, mid=32.7790, upper=35.0454, lower=30.5126; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=32.4200 vs SMA50=29.0738 dist=+11.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=32.7790 SMA50=29.0738 SMA80=26.9286 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-17@23.9510; S2[2026-09-09→2026-10-07] low=2026-09-09@23.9550; S3[2026-10-08→2026-11-10] low=2026-10-08@27.7200 | lows=[23.951000213623047, 23.954999923706055, 27.719999313354492] span=15.74% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5294092190612963 wick_frac=0.4705907809387036 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.07609076490893447 wick_frac=0.9239092350910655 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.999509563511525 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:GREEN:body=+1.5700:wick=0.4350; 2026-11-05:RED:body=-1.5800:wick=0.6100; 2026-11-06:GREEN:body=+2.2500:wick=0.5700; 2026-11-09:GREEN:body=+0.6300:wick=0.5600; 2026-11-10:RED:body=-0.0700:wick=0.8500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.36 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.17 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 661.58 | **NEUTRAL** |
| `B04_income` | 139.72 | **GOOD** |
| `B05_profit_margin` | 21.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 31.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.16000000000000014 (now=31.16 vs prior_export=31.0 on finviz_2026-11-10) | **GOOD** |
| `B09_analyst_recom` | 2.48 | **GOOD** |
| `B10_insider_transactions` | -0.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.07 vs prior=-0.07 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.89 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.17 (this export) | prior_export=2.17 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+17**  ·  Scientific & Technical Instruments
price=43.130001068115234  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=82.83 on 2026-11-10; prev RSI=88.75 on 2026-11-09 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 88.75@2026-11-09 → 82.83@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 88.75@2026-11-09 → 82.83@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 88.75@2026-11-09 → 82.83@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=18.334 (G=2.2000 R=0.1200); 2026-11-09:GREEN:O=41.6800,C=43.8800,body=+2.2000,vol=3140500.0; 2026-11-10:RED:O=43.2500,C=43.1300,body=-0.1200,vol=2810478.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.117 (Gvol=3140500 Rvol=2810478); 2026-11-09:GREEN:O=41.6800,C=43.8800,body=+2.2000,vol=3140500.0; 2026-11-10:RED:O=43.2500,C=43.1300,body=-0.1200,vol=2810478.0 | **GOOD** |
| `A07_rvol` | RVOL=1.784 on 2026-11-10: today_vol=2810478 / avg20=1575180 (avg window 2026-10-08→2026-11-09, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.992 on 2026-11-10 (price=43.1300, mid=36.1780, upper=43.1878, lower=29.1682; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-10: price=43.1300 vs SMA50=33.6434 dist=+28.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=36.1780 SMA50=33.6434 SMA80=33.1610 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-09-01@31.6000; S2[2026-09-09→2026-10-07] low=2026-09-24@30.4250; S3[2026-10-08→2026-11-10] low=2026-10-08@31.7800 | lows=[31.600000381469727, 30.424999237060547, 31.780000686645508] span=4.45% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.7333335876464844 wick_frac=0.2666664123535156 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.05589662580339072 wick_frac=0.9441033741966093 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.33350287694313 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:GREEN:body=+0.4500:wick=0.4700; 2026-11-05:RED:body=-2.1100:wick=1.4400; 2026-11-06:GREEN:body=+2.5800:wick=0.4900; 2026-11-09:GREEN:body=+2.2000:wick=0.8000; 2026-11-10:RED:body=-0.1200:wick=2.0268 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.2199999999999989 (now=46.11 vs prior_export=44.89 on finviz_2026-11-10) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.96 | **GOOD** |
| `B13_short_float` | 9.33 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTEK  ·  score **+16**  ·  Engineering & Construction
price=40.650001525878906  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.99 on 2026-11-10; prev RSI=76.44 on 2026-11-09 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.44@2026-11-09 → 71.99@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.44@2026-11-09 → 71.99@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.44@2026-11-09 → 71.99@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=11.334 (G=0.6800 R=0.0600); 2026-11-09:GREEN:O=40.4100,C=41.0900,body=+0.6800,vol=2664400.0; 2026-11-10:RED:O=40.7100,C=40.6500,body=-0.0600,vol=2217305.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.202 (Gvol=2664400 Rvol=2217305); 2026-11-09:GREEN:O=40.4100,C=41.0900,body=+0.6800,vol=2664400.0; 2026-11-10:RED:O=40.7100,C=40.6500,body=-0.0600,vol=2217305.0 | **GOOD** |
| `A07_rvol` | RVOL=1.033 on 2026-11-10: today_vol=2217305 / avg20=2146705 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.614 on 2026-11-10 (price=40.6500, mid=37.5005, upper=42.6304, lower=32.3706; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=40.6500 vs SMA50=36.0464 dist=+12.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=37.5005 SMA50=36.0464 SMA80=35.3400 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-11@34.2005; S2[2026-09-09→2026-10-07] low=2026-10-07@32.0200; S3[2026-10-08→2026-11-10] low=2026-10-08@32.2300 | lows=[34.20049350805924, 32.02000045776367, 32.22999954223633] span=6.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5573784844363148 wick_frac=0.44262151556368523 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.09999554953683394 wick_frac=0.900004450463166 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.3337995930824 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+0.9700:wick=0.6000; 2026-11-05:RED:body=-0.9300:wick=0.8500; 2026-11-06:GREEN:body=+0.7400:wick=1.1200; 2026-11-09:GREEN:body=+0.6800:wick=0.5400; 2026-11-10:RED:body=-0.0600:wick=0.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=11/11/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5069.48 | **NEUTRAL** |
| `B04_income` | 435.98 | **GOOD** |
| `B05_profit_margin` | 8.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.17 vs prior_export=40.17 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.32 | **GOOD** |
| `B13_short_float` | 5.01 | **NEUTRAL** |
| `B14_earnings_date` | 11/11/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HONA  ·  score **+16**  ·  Aerospace & Defense
price=166.47000122070312  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.50 on 2026-11-10; prev RSI=54.24 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.24@2026-11-09 → 55.50@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.24@2026-11-09 → 55.50@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.24@2026-11-09 → 55.50@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=11.9300 R=0.0000); 2026-11-09:GREEN:O=155.8700,C=164.7700,body=+8.9000,vol=3956200.0; 2026-11-10:GREEN:O=163.4400,C=166.4700,body=+3.0300,vol=2694076.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6650276 Rvol=0); 2026-11-09:GREEN:O=155.8700,C=164.7700,body=+8.9000,vol=3956200.0; 2026-11-10:GREEN:O=163.4400,C=166.4700,body=+3.0300,vol=2694076.0 | **GOOD** |
| `A07_rvol` | RVOL=0.770 on 2026-11-10: today_vol=2694076 / avg20=3497196 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.380 on 2026-11-10 (price=166.4700, mid=161.6230, upper=174.3829, lower=148.8631; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=166.4700 vs SMA50=159.4734 dist=+4.39% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=161.62_50=159.47_80=167.51 on 2026-11-10: SMA20=161.6230 SMA50=159.4734 SMA80=167.5122 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@149.1900; S2[2026-09-14→2026-10-12] low=2026-10-08@149.8800; S3[2026-10-13→2026-11-10] low=2026-11-06@147.4000 | lows=[149.19000244140625, 149.8800048828125, 147.39999389648438] span=1.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.6580387752328812 wick_frac=0.34196122476711877 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:RED:body=-2.1200:wick=5.8600; 2026-11-05:RED:body=-6.1800:wick=3.4600; 2026-11-06:GREEN:body=+3.6300:wick=3.2000; 2026-11-09:GREEN:body=+8.9000:wick=3.9500; 2026-11-10:GREEN:body=+3.0300:wick=1.8299 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.34 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18338.0 | **NEUTRAL** |
| `B04_income` | 1898.0 | **GOOD** |
| `B05_profit_margin` | 10.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 188.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=188.43 vs prior_export=188.43 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 2.39 | **GOOD** |
| `B10_insider_transactions` | 7.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.21 vs prior=7.21 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 213.36 | **GOOD** |
| `B13_short_float` | 2.37 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.34 (this export) | prior_export=18.34 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ED  ·  score **+16**  ·  Utilities - Regulated Electric
price=119.27999877929688  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.95 on 2026-11-10; prev RSI=63.20 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.20@2026-11-09 → 64.95@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.20@2026-11-09 → 64.95@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.20@2026-11-09 → 64.95@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=1.624 (G=1.5100 R=0.9300); 2026-11-09:RED:O=119.3200,C=118.3900,body=-0.9300,vol=2050400.0; 2026-11-10:GREEN:O=117.7700,C=119.2800,body=+1.5100,vol=2055524.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.002 (Gvol=2055524 Rvol=2050400); 2026-11-09:RED:O=119.3200,C=118.3900,body=-0.9300,vol=2050400.0; 2026-11-10:GREEN:O=117.7700,C=119.2800,body=+1.5100,vol=2055524.0 | **GOOD** |
| `A07_rvol` | RVOL=0.865 on 2026-11-10: today_vol=2055524 / avg20=2376596 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.550 on 2026-11-10 (price=119.2800, mid=115.3600, upper=122.4878, lower=108.2322; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=119.2800 vs SMA50=109.3628 dist=+9.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=115.3600 SMA50=109.3628 SMA80=109.1883 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-24@105.5100; S2[2026-09-14→2026-10-12] low=2026-09-29@102.0000; S3[2026-10-13→2026-11-10] low=2026-10-13@105.6800 | lows=[105.51000213623047, 102.0, 105.68000030517578] span=3.61% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.6508630753698778 wick_frac=0.3491369246301223 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.3457247067365507 wick_frac=0.6542752932634492 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6236576782037293 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:GREEN:body=+2.8600:wick=1.1600; 2026-11-05:RED:body=-0.5700:wick=2.0370; 2026-11-06:RED:body=-1.3700:wick=1.2200; 2026-11-09:RED:body=-0.9300:wick=1.7600; 2026-11-10:GREEN:body=+1.5100:wick=0.8100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.78 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.16 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18072.0 | **NEUTRAL** |
| `B04_income` | 2295.0 | **GOOD** |
| `B05_profit_margin` | 12.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=109.47 vs prior_export=109.47 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 3.32 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.15 | **GOOD** |
| `B13_short_float` | 3.34 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.16 (this export) | prior_export=3.16 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KMB  ·  score **+16**  ·  Household & Personal Products
price=110.76000213623047  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.76 on 2026-11-10; prev RSI=61.51 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.51@2026-11-09 → 58.76@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.51@2026-11-09 → 58.76@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.51@2026-11-09 → 58.76@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=3.077 (G=3.2000 R=1.0400); 2026-11-09:GREEN:O=108.7300,C=111.9300,body=+3.2000,vol=10788200.0; 2026-11-10:RED:O=111.8000,C=110.7600,body=-1.0400,vol=7529257.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.433 (Gvol=10788200 Rvol=7529257); 2026-11-09:GREEN:O=108.7300,C=111.9300,body=+3.2000,vol=10788200.0; 2026-11-10:RED:O=111.8000,C=110.7600,body=-1.0400,vol=7529257.0 | **GOOD** |
| `A07_rvol` | RVOL=1.068 on 2026-11-10: today_vol=7529257 / avg20=7048306 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.451 on 2026-11-10 (price=110.7600, mid=108.0033, upper=114.1192, lower=101.8873; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=110.7600 vs SMA50=102.6893 dist=+7.86% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=108.00_50=102.69_80=105.32 on 2026-11-10: SMA20=108.0033 SMA50=102.6893 SMA80=105.3177 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-11@97.3000; S2[2026-09-14→2026-10-12] low=2026-10-05@93.9450; S3[2026-10-13→2026-11-10] low=2026-10-13@96.8900 | lows=[97.30000305175781, 93.94499969482422, 96.88999938964844] span=3.57% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5734759674151128 wick_frac=0.42652403258488725 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.37681267587724393 wick_frac=0.6231873241227561 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.076917433884752 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+2.9300:wick=2.6300; 2026-11-05:GREEN:body=+2.1600:wick=2.2400; 2026-11-06:GREEN:body=+0.3300:wick=1.8200; 2026-11-09:GREEN:body=+3.2000:wick=2.3800; 2026-11-10:RED:body=-1.0400:wick=1.7200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.86 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16566.0 | **NEUTRAL** |
| `B04_income` | 3302.0 | **GOOD** |
| `B05_profit_margin` | 19.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 117.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.15000000000000568 (now=117.79 vs prior_export=117.64 on finviz_2026-11-10) | **GOOD** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.67 | **GOOD** |
| `B13_short_float` | 8.26 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.86 (this export) | prior_export=2.86 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.18 (this export) | prior_export=0.18 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+16**  ·  Consumer Electronics
price=348.510009765625  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.62 on 2026-11-10; prev RSI=57.61 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.61@2026-11-09 → 55.62@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.61@2026-11-09 → 55.62@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.61@2026-11-09 → 55.62@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=1.875 (G=5.6900 R=3.0350); 2026-11-09:GREEN:O=345.0000,C=350.6900,body=+5.6900,vol=48691200.0; 2026-11-10:RED:O=351.5450,C=348.5100,body=-3.0350,vol=28421074.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.713 (Gvol=48691200 Rvol=28421074); 2026-11-09:GREEN:O=345.0000,C=350.6900,body=+5.6900,vol=48691200.0; 2026-11-10:RED:O=351.5450,C=348.5100,body=-3.0350,vol=28421074.0 | **GOOD** |
| `A07_rvol` | RVOL=0.580 on 2026-11-10: today_vol=28421074 / avg20=49040874 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.486 on 2026-11-10 (price=348.5100, mid=344.2323, upper=353.0380, lower=335.4265; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=348.5100 vs SMA50=337.2667 dist=+3.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=344.2323 SMA50=337.2667 SMA80=329.2418 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-13@302.0500; S2[2026-09-14→2026-10-12] low=2026-10-01@325.8100; S3[2026-10-13→2026-11-10] low=2026-11-03@325.3300 | lows=[302.04998779296875, 325.80999755859375, 325.3299865722656] span=7.87% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.7536439259816167 wick_frac=0.24635607401838333 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.4096106164073247 wick_frac=0.5903893835926752 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8747926114367879 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+7.9450:wick=5.5350; 2026-11-05:RED:body=-9.7200:wick=3.6000; 2026-11-06:GREEN:body=+3.6400:wick=3.8500; 2026-11-09:GREEN:body=+5.6900:wick=1.8600; 2026-11-10:RED:body=-3.0350:wick=4.3745 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.56 (current export asof; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 478834.0 | **NEUTRAL** |
| `B04_income` | 132175.0 | **GOOD** |
| `B05_profit_margin` | 27.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 341.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.579999999999984 (now=341.3 vs prior_export=339.72 on finviz_2026-11-10) | **GOOD** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | -1.93 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.93 vs prior=-1.93 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.31 | **GOOD** |
| `B13_short_float` | 0.91 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.56 (this export) | prior_export=5.56 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CTSH  ·  score **+16**  ·  Information Technology Services
price=65.0  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.51 on 2026-11-10; prev RSI=48.18 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.18@2026-11-09 → 51.51@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.18@2026-11-09 → 51.51@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.18@2026-11-09 → 51.51@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=12.600 (G=1.2600 R=0.1000); 2026-11-09:RED:O=63.7000,C=63.6000,body=-0.1000,vol=4172600.0; 2026-11-10:GREEN:O=63.7400,C=65.0000,body=+1.2600,vol=4897913.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.174 (Gvol=4897913 Rvol=4172600); 2026-11-09:RED:O=63.7000,C=63.6000,body=-0.1000,vol=4172600.0; 2026-11-10:GREEN:O=63.7400,C=65.0000,body=+1.2600,vol=4897913.0 | **GOOD** |
| `A07_rvol` | RVOL=0.717 on 2026-11-10: today_vol=4897913 / avg20=6831194 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.244 on 2026-11-10 (price=65.0000, mid=66.3580, upper=71.9144, lower=60.8016; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=65.0000 vs SMA50=62.4624 dist=+4.06% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=66.3580 SMA50=62.4624 SMA80=60.1989 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-17@56.2204; S2[2026-09-14→2026-10-12] low=2026-10-08@54.7000; S3[2026-10-13→2026-11-10] low=2026-10-13@59.7500 | lows=[56.220350988128516, 54.70000076293945, 59.75] span=9.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5206600366652007 wick_frac=0.47933996333479933 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.06711572630403949 wick_frac=0.9328842736959605 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.599694831203509 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+1.6200:wick=1.2600; 2026-11-05:RED:body=-0.5800:wick=1.3137; 2026-11-06:RED:body=-0.3900:wick=1.0300; 2026-11-09:RED:body=-0.1000:wick=1.3900; 2026-11-10:GREEN:body=+1.2600:wick=1.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.78 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21891.0 | **NEUTRAL** |
| `B04_income` | 2573.0 | **GOOD** |
| `B05_profit_margin` | 11.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.35 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=69.35 vs prior_export=69.35 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.66 vs prior=-0.66 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.35 | **GOOD** |
| `B13_short_float` | 6.59 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BSX  ·  score **+16**  ·  Medical Devices
price=44.70000076293945  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.02 on 2026-11-10; prev RSI=49.62 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.62@2026-11-09 → 52.02@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.62@2026-11-09 → 52.02@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.62@2026-11-09 → 52.02@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7900 R=0.0000); 2026-11-09:GREEN:O=43.3500,C=44.0200,body=+0.6700,vol=16193400.0; 2026-11-10:GREEN:O=43.5800,C=44.7000,body=+1.1200,vol=11711242.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=99.000 (Gvol=27904642 Rvol=0); 2026-11-09:GREEN:O=43.3500,C=44.0200,body=+0.6700,vol=16193400.0; 2026-11-10:GREEN:O=43.5800,C=44.7000,body=+1.1200,vol=11711242.0 | **GOOD** |
| `A07_rvol` | RVOL=0.583 on 2026-11-10: today_vol=11711242 / avg20=20101980 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.167 on 2026-11-10 (price=44.7000, mid=44.2930, upper=46.7304, lower=41.8556; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=44.7000 vs SMA50=44.2044 dist=+1.12% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=44.29_50=44.20_80=45.72 on 2026-11-10: SMA20=44.2930 SMA50=44.2044 SMA80=45.7189 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@42.7800; S2[2026-09-14→2026-10-12] low=2026-10-08@41.3900; S3[2026-10-13→2026-11-10] low=2026-10-28@39.5700 | lows=[42.779998779296875, 41.38999938964844, 39.56999969482422] span=8.11% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.6654880291295019 wick_frac=0.3345119708704981 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+1.0150:wick=1.1400; 2026-11-05:RED:body=-1.9450:wick=0.9207; 2026-11-06:RED:body=-0.6000:wick=0.3400; 2026-11-09:GREEN:body=+0.6700:wick=0.6200; 2026-11-10:GREEN:body=+1.1200:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.43 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.99 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21069.0 | **NEUTRAL** |
| `B04_income` | 3502.0 | **GOOD** |
| `B05_profit_margin` | 16.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.07 vs prior_export=50.07 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | 2.24 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.24 vs prior=2.24 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 2.83 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.43 (this export) | prior_export=0.43 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.99 (this export) | prior_export=0.99 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IFF  ·  score **+16**  ·  Specialty Chemicals
price=83.16999816894531  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=38.33 on 2026-11-10; prev RSI=40.68 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 40.68@2026-11-09 → 38.33@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 40.68@2026-11-09 → 38.33@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 40.68@2026-11-09 → 38.33@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=5.346 (G=1.3900 R=0.2600); 2026-11-09:GREEN:O=83.0400,C=84.4300,body=+1.3900,vol=2702400.0; 2026-11-10:RED:O=83.4300,C=83.1700,body=-0.2600,vol=2326046.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.162 (Gvol=2702400 Rvol=2326046); 2026-11-09:GREEN:O=83.0400,C=84.4300,body=+1.3900,vol=2702400.0; 2026-11-10:RED:O=83.4300,C=83.1700,body=-0.2600,vol=2326046.0 | **GOOD** |
| `A07_rvol` | RVOL=0.937 on 2026-11-10: today_vol=2326046 / avg20=2481647 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.820 on 2026-11-10 (price=83.1700, mid=89.6910, upper=97.6400, lower=81.7420; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-11-10: price=83.1700 vs SMA50=86.8116 dist=-4.19% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=89.6910 SMA50=86.8116 SMA80=84.7410 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-18@80.8700; S2[2026-09-09→2026-10-07] low=2026-10-01@81.5400; S3[2026-10-08→2026-11-10] low=2026-11-10@82.3000 | lows=[80.87000274658203, 81.54000091552734, 82.30000305175781] span=1.77% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5148151706443173 wick_frac=0.4851848293556828 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.15204811428928358 wick_frac=0.8479518857107164 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.346107573579037 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+1.2300:wick=4.5700; 2026-11-05:RED:body=-1.7400:wick=1.5300; 2026-11-06:RED:body=-1.2100:wick=0.8300; 2026-11-09:GREEN:body=+1.3900:wick=1.3100; 2026-11-10:RED:body=-0.2600:wick=1.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.13 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.76 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9226.0 | **NEUTRAL** |
| `B04_income` | 182.0 | **GOOD** |
| `B05_profit_margin` | 1.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 98.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=98.44 vs prior_export=98.44 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.91 | **GOOD** |
| `B10_insider_transactions` | 5.97 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.97 vs prior=5.97 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.88 | **GOOD** |
| `B13_short_float` | 6.7 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.13 (this export) | prior_export=2.13 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.76 (this export) | prior_export=1.76 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CELH  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=32.79999923706055  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.45 on 2026-11-10; prev RSI=71.47 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 71.47@2026-11-09 → 68.45@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.47@2026-11-09 → 68.45@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 71.47@2026-11-09 → 68.45@2026-11-10 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=4.435 (G=1.0200 R=0.2300); 2026-11-09:GREEN:O=32.1700,C=33.1900,body=+1.0200,vol=6031800.0; 2026-11-10:RED:O=33.0300,C=32.8000,body=-0.2300,vol=4571803.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.319 (Gvol=6031800 Rvol=4571803); 2026-11-09:GREEN:O=32.1700,C=33.1900,body=+1.0200,vol=6031800.0; 2026-11-10:RED:O=33.0300,C=32.8000,body=-0.2300,vol=4571803.0 | **GOOD** |
| `A07_rvol` | RVOL=0.769 on 2026-11-10: today_vol=4571803 / avg20=5945243 (avg window 2026-10-13→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.989 on 2026-11-10 (price=32.8000, mid=29.4315, upper=32.8369, lower=26.0261; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-10: price=32.8000 vs SMA50=28.7782 dist=+13.98% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=29.43_50=28.78_80=29.20 on 2026-11-10: SMA20=29.4315 SMA50=28.7782 SMA80=29.1956 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-10 (63 bars); S1[2026-08-13→2026-09-11] low=2026-09-10@26.5900; S2[2026-09-14→2026-10-12] low=2026-10-02@26.4300; S3[2026-10-13→2026-11-10] low=2026-10-14@26.7900 | lows=[26.59000015258789, 26.43000030517578, 26.790000915527344] span=1.36% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.829268973473888 wick_frac=0.17073102652611208 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.3108944280012788 wick_frac=0.6891055719987212 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.434793425439105 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:GREEN:body=+0.0800:wick=0.8100; 2026-11-05:RED:body=-2.6400:wick=1.4827; 2026-11-06:GREEN:body=+3.1300:wick=0.5400; 2026-11-09:GREEN:body=+1.0200:wick=0.2100; 2026-11-10:RED:body=-0.2300:wick=0.5098 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.69 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.82 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3169.4 | **NEUTRAL** |
| `B04_income` | 230.17 | **GOOD** |
| `B05_profit_margin` | 7.26 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.75 vs prior_export=41.75 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.59 | **GOOD** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.32 | **GOOD** |
| `B13_short_float` | 16.34 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.69 (this export) | prior_export=20.69 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.82 (this export) | prior_export=5.82 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+15**  ·  Business Equipment & Supplies
price=58.52000045776367  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.01 on 2026-11-10; prev RSI=60.68 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.68@2026-11-09 → 57.01@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.68@2026-11-09 → 57.01@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.68@2026-11-09 → 57.01@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=8.067 (G=1.2100 R=0.1500); 2026-11-09:GREEN:O=58.4400,C=59.6500,body=+1.2100,vol=661000.0; 2026-11-10:RED:O=58.6700,C=58.5200,body=-0.1500,vol=814456.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=0.812 (Gvol=661000 Rvol=814456); 2026-11-09:GREEN:O=58.4400,C=59.6500,body=+1.2100,vol=661000.0; 2026-11-10:RED:O=58.6700,C=58.5200,body=-0.1500,vol=814456.0 | **BAD** |
| `A07_rvol` | RVOL=1.091 on 2026-11-10: today_vol=814456 / avg20=746650 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.178 on 2026-11-10 (price=58.5200, mid=57.5005, upper=63.2414, lower=51.7596; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=58.5200 vs SMA50=52.1492 dist=+12.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=57.5005 SMA50=52.1492 SMA80=51.9356 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-20@47.5800; S2[2026-09-09→2026-10-07] low=2026-09-18@44.8400; S3[2026-10-08→2026-11-10] low=2026-10-08@47.3100 | lows=[47.58000183105469, 44.84000015258789, 47.310001373291016] span=6.11% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.5330408172525111 wick_frac=0.46695918274748893 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.08068295605640276 wick_frac=0.9193170439435973 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.066809084204369 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:GREEN:body=+0.8400:wick=2.3400; 2026-11-05:GREEN:body=+1.2300:wick=5.2300; 2026-11-06:RED:body=-1.3900:wick=1.8400; 2026-11-09:GREEN:body=+1.2100:wick=1.0600; 2026-11-10:RED:body=-0.1500:wick=1.7091 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.14 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.42 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1854.9 | **NEUTRAL** |
| `B04_income` | 128.2 | **GOOD** |
| `B05_profit_margin` | 6.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 72.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=72.29 vs prior_export=72.29 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.73 | **GOOD** |
| `B13_short_float` | 19.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.14 (this export) | prior_export=6.14 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.42 (this export) | prior_export=0.42 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TEL  ·  score **+15**  ·  Electronic Components
price=217.75999450683594  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.67 on 2026-11-10; prev RSI=53.56 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.56@2026-11-09 → 52.67@2026-11-10 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.56@2026-11-09 → 52.67@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.56@2026-11-09 → 52.67@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=13.719 (G=7.8200 R=0.5700); 2026-11-09:GREEN:O=210.9300,C=218.7500,body=+7.8200,vol=1875600.0; 2026-11-10:RED:O=218.3300,C=217.7600,body=-0.5700,vol=1599867.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.172 (Gvol=1875600 Rvol=1599867); 2026-11-09:GREEN:O=210.9300,C=218.7500,body=+7.8200,vol=1875600.0; 2026-11-10:RED:O=218.3300,C=217.7600,body=-0.5700,vol=1599867.0 | **GOOD** |
| `A07_rvol` | RVOL=0.754 on 2026-11-10: today_vol=1599867 / avg20=2122319 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.218 on 2026-11-10 (price=217.7600, mid=215.4190, upper=226.1537, lower=204.6843; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-10: price=217.7600 vs SMA50=212.1610 dist=+2.64% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=215.4190 SMA50=212.1610 SMA80=210.6999 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-25@199.2800; S2[2026-09-09→2026-10-07] low=2026-09-16@200.4300; S3[2026-10-08→2026-11-10] low=2026-10-23@204.6300 | lows=[199.27999877929688, 200.42999267578125, 204.6300048828125] span=2.68% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.7476112535849431 wick_frac=0.25238874641505693 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.14430576319113675 wick_frac=0.8556942368088633 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.719134811007603 need>1.4; red_wick_gt_green=True 5d trail=2026-11-04:RED:body=-1.7800:wick=5.4100; 2026-11-05:RED:body=-13.6700:wick=0.8000; 2026-11-06:RED:body=-1.4300:wick=3.1100; 2026-11-09:GREEN:body=+7.8200:wick=2.6400; 2026-11-10:RED:body=-0.5700:wick=3.3800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.17 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.49 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 19885.0 | **NEUTRAL** |
| `B04_income` | 3253.0 | **GOOD** |
| `B05_profit_margin` | 16.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 263.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=263.88 vs prior_export=263.88 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -2.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.89 vs prior=-2.89 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.49 | **GOOD** |
| `B13_short_float` | 2.15 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.17 (this export) | prior_export=2.17 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.49 (this export) | prior_export=0.49 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BTE  ·  score **+15**  ·  Oil & Gas E&P
price=4.170000076293945  pair=`2026-11-09→2026-11-10`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=30.30 on 2026-11-10; prev RSI=26.86 on 2026-11-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | cross_up | RSI 26.86@2026-11-09 → 30.30@2026-11-10 vs 30 | rule: cross_up=GOOD | **GOOD** |
| `A03_rsi_cross_50` | below | RSI 26.86@2026-11-09 → 30.30@2026-11-10 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 26.86@2026-11-09 → 30.30@2026-11-10 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.1500 R=0.0300); 2026-11-09:RED:O=4.1200,C=4.0900,body=-0.0300,vol=17796000.0; 2026-11-10:GREEN:O=4.0200,C=4.1700,body=+0.1500,vol=17960130.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-09 + 2026-11-10; ratio=GREEN_vol/RED_vol=1.009 (Gvol=17960130 Rvol=17796000); 2026-11-09:RED:O=4.1200,C=4.0900,body=-0.0300,vol=17796000.0; 2026-11-10:GREEN:O=4.0200,C=4.1700,body=+0.1500,vol=17960130.0 | **GOOD** |
| `A07_rvol` | RVOL=0.907 on 2026-11-10: today_vol=17960130 / avg20=19800962 (avg window 2026-10-08→2026-11-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.791 on 2026-11-10 (price=4.1700, mid=4.9905, upper=6.0278, lower=3.9532; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-11-10: price=4.1700 vs SMA50=4.8642 dist=-14.27% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-10: SMA20=4.9905 SMA50=4.8642 SMA80=4.6785 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-10→2026-11-10 (63 bars); S1[2026-08-10→2026-09-08] low=2026-08-10@4.2400; S2[2026-09-09→2026-10-07] low=2026-09-29@4.4000; S3[2026-10-08→2026-11-10] low=2026-11-10@3.9550 | lows=[4.239999771118164, 4.400000095367432, 3.9549999237060547] span=11.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: GREEN body_frac=0.6666673730923857 wick_frac=0.33333262690761434 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-09+2026-11-10: RED body_frac=0.23076697355390088 wick_frac=0.7692330264460991 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.000047684140255 need>1.4; red_wick_gt_green=False 5d trail=2026-11-04:RED:body=-0.2300:wick=0.0800; 2026-11-05:RED:body=-0.2600:wick=0.0200; 2026-11-06:RED:body=-0.1300:wick=0.0200; 2026-11-09:RED:body=-0.0300:wick=0.1000; 2026-11-10:GREEN:body=+0.1500:wick=0.0750 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.06 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=36.65 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2185.03 | **NEUTRAL** |
| `B04_income` | 153.64 | **GOOD** |
| `B05_profit_margin` | 7.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 6.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=6.2 vs prior_export=6.2 on finviz_2026-11-10) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-10) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 3.5 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.06 (this export) | prior_export=14.06 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=36.65 (this export) | prior_export=36.65 (finviz_2026-11-10) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-11_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.