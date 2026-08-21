# A+B1 Feature Checklist — 2026-08-20

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,707** names
- Export: `finviz_2026-08-20.csv` · prior export for Δ: `2026-08-19`
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
| 1 | CLX | +18 | 18 | 0 | 2026-08-19→2026-08-20 | Household & Personal Products |
| 2 | ERO | +17 | 17 | 0 | 2026-08-19→2026-08-20 | Copper |
| 3 | SON | +17 | 17 | 0 | 2026-08-19→2026-08-20 | Packaging & Containers |
| 4 | AUPH | +17 | 18 | 1 | 2026-08-19→2026-08-20 | Biotechnology |
| 5 | ADSK | +17 | 17 | 0 | 2026-08-19→2026-08-20 | Software - Application |
| 6 | FLS | +16 | 17 | 1 | 2026-08-19→2026-08-20 | Specialty Industrial Machinery |
| 7 | DSGX | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Software - Application |
| 8 | AHR | +16 | 17 | 1 | 2026-08-19→2026-08-20 | REIT - Healthcare Facilities |
| 9 | WTW | +16 | 17 | 1 | 2026-08-19→2026-08-20 | Insurance Brokers |
| 10 | CXT | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Specialty Industrial Machinery |
| 11 | AOS | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Specialty Industrial Machinery |
| 12 | TTEK | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Engineering & Construction |
| 13 | AWK | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Utilities - Regulated Water |
| 14 | SONO | +16 | 16 | 0 | 2026-08-19→2026-08-20 | Consumer Electronics |
| 15 | GEO | +15 | 17 | 2 | 2026-08-19→2026-08-20 | Security & Protection Services |

## Full checklist — top 15

### CLX  ·  score **+18**  ·  Household & Personal Products
price=106.04000091552734  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.73 on 2026-08-20; prev RSI=66.61 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.61@2026-08-19 → 60.73@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.61@2026-08-19 → 60.73@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.61@2026-08-19 → 60.73@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=1.921 (G=1.4600 R=0.7600); 2026-08-19:GREEN:O=106.4400,C=107.9000,body=+1.4600,vol=2349400.0; 2026-08-20:RED:O=106.8000,C=106.0400,body=-0.7600,vol=2017800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.164 (Gvol=2349400 Rvol=2017800); 2026-08-19:GREEN:O=106.4400,C=107.9000,body=+1.4600,vol=2349400.0; 2026-08-20:RED:O=106.8000,C=106.0400,body=-0.7600,vol=2017800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-08-20: today_vol=2017800 / avg20=2708325 (avg window 2026-07-23→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.383 on 2026-08-20 (price=106.0400, mid=102.4410, upper=111.8346, lower=93.0474; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=106.0400 vs SMA50=97.9123 dist=+8.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=102.4410 SMA50=97.9123 SMA80=95.4114 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-20 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-01@86.8949; S2[2026-06-18→2026-07-20] low=2026-06-22@89.4550; S3[2026-07-23→2026-08-20] low=2026-07-23@91.9755 | lows=[86.89488181497894, 89.45496636119053, 91.97552602044338] span=5.85% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6431723539909858 wick_frac=0.3568276460090141 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.24127098693070073 wick_frac=0.7587290130692993 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9210460272047383 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-0.5300:wick=1.3300; 2026-08-17:GREEN:body=+0.9400:wick=1.8600; 2026-08-18:RED:body=-1.0700:wick=1.4500; 2026-08-19:GREEN:body=+1.4600:wick=0.8100; 2026-08-20:RED:body=-0.7600:wick=2.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.18 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.73 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6720.0 | **NEUTRAL** |
| `B04_income` | 587.0 | **GOOD** |
| `B05_profit_margin` | 8.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 98.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.25 (now=98.38 vs prior_export=97.13 on finviz_2026-08-19) | **GOOD** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.68 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.68 vs prior=0.68 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.92 | **GOOD** |
| `B13_short_float` | 11.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.18 (this export) | prior_export=1.18 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.73 (this export) | prior_export=1.73 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ERO  ·  score **+17**  ·  Copper
price=36.02000045776367  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.11 on 2026-08-20; prev RSI=61.53 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.53@2026-08-19 → 66.11@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.53@2026-08-19 → 66.11@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.53@2026-08-19 → 66.11@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=7.879 (G=2.6000 R=0.3300); 2026-08-19:RED:O=34.4100,C=34.0800,body=-0.3300,vol=1195100.0; 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1677400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.404 (Gvol=1677400 Rvol=1195100); 2026-08-19:RED:O=34.4100,C=34.0800,body=-0.3300,vol=1195100.0; 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1677400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.311 on 2026-08-20: today_vol=1677400 / avg20=1279515 (avg window 2026-07-21→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.578 on 2026-08-20 (price=36.0200, mid=31.1875, upper=39.5536, lower=22.8214; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=36.0200 vs SMA50=28.5054 dist=+26.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=31.1875 SMA50=28.5054 SMA80=28.3461 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-20 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-09@25.1510; S2[2026-06-18→2026-07-20] low=2026-07-08@22.9320; S3[2026-07-21→2026-08-20] low=2026-07-29@24.7020 | lows=[25.150999069213867, 22.93199920654297, 24.70199966430664] span=9.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.9352530325484384 wick_frac=0.06474696745156155 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.2920333667541008 wick_frac=0.7079666332458993 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.878842174621707 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-0.9100:wick=0.5100; 2026-08-17:GREEN:body=+0.6200:wick=0.6300; 2026-08-18:RED:body=-1.0100:wick=0.5720; 2026-08-19:RED:body=-0.3300:wick=0.8000; 2026-08-20:GREEN:body=+2.6000:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.31 vs prior_export=36.31 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.21 | **GOOD** |
| `B13_short_float` | 5.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.7 (this export) | prior_export=12.7 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.4 (this export) | prior_export=2.4 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SON  ·  score **+17**  ·  Packaging & Containers
price=58.33000183105469  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.59 on 2026-08-20; prev RSI=55.96 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.96@2026-08-19 → 57.59@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.96@2026-08-19 → 57.59@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.96@2026-08-19 → 57.59@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8300 R=0.0000); 2026-08-19:GREEN:O=57.6200,C=57.9000,body=+0.2800,vol=874600.0; 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1451800 Rvol=0); 2026-08-19:GREEN:O=57.6200,C=57.9000,body=+0.2800,vol=874600.0; 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.548 on 2026-08-20: today_vol=577200 / avg20=1053815 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.347 on 2026-08-20 (price=58.3300, mid=57.6385, upper=59.6311, lower=55.6460; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=58.3300 vs SMA50=54.6561 dist=+6.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=57.6385 SMA50=54.6561 SMA80=52.4228 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-19@45.4827; S2[2026-06-17→2026-07-17] low=2026-06-22@49.4158; S3[2026-07-20→2026-08-20] low=2026-07-20@53.9234 | lows=[45.482707673306166, 49.41576037269584, 53.92341372893777] span=18.56% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.5682233116962906 wick_frac=0.43177668830370947 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:GREEN:body=+0.0600:wick=0.5200; 2026-08-17:RED:body=-1.0000:wick=0.1300; 2026-08-18:RED:body=-0.0700:wick=1.1000; 2026-08-19:GREEN:body=+0.2800:wick=0.5600; 2026-08-20:GREEN:body=+1.5500:wick=0.3800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.23 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.29 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7458.91 | **NEUTRAL** |
| `B04_income` | 646.79 | **GOOD** |
| `B05_profit_margin` | 8.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.61 | **GOOD** |
| `B13_short_float` | 11.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+17**  ·  Biotechnology
price=17.270000457763672  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.64 on 2026-08-20; prev RSI=57.65 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.65@2026-08-19 → 66.64@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.65@2026-08-19 → 66.64@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.65@2026-08-19 → 66.64@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2700 R=0.0000); 2026-08-19:GREEN:O=15.5700,C=16.1700,body=+0.6000,vol=1646500.0; 2026-08-20:GREEN:O=16.6000,C=17.2700,body=+0.6700,vol=2637000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4283500 Rvol=0); 2026-08-19:GREEN:O=15.5700,C=16.1700,body=+0.6000,vol=1646500.0; 2026-08-20:GREEN:O=16.6000,C=17.2700,body=+0.6700,vol=2637000.0 | **GOOD** |
| `A07_rvol` | RVOL=2.247 on 2026-08-20: today_vol=2637000 / avg20=1173440 (avg window 2026-07-23→2026-08-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.582 on 2026-08-20 (price=17.2700, mid=15.4105, upper=16.5862, lower=14.2348; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-20: price=17.2700 vs SMA50=15.9314 dist=+8.40% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.41_50=15.93_80=15.82 on 2026-08-20: SMA20=15.4105 SMA50=15.9314 SMA80=15.8206 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-20 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-01@14.9650; S2[2026-06-22→2026-07-21] low=2026-07-21@15.1600; S3[2026-07-23→2026-08-20] low=2026-07-31@14.3630 | lows=[14.96500015258789, 15.15999984741211, 14.36299991607666] span=5.55% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6384490241721064 wick_frac=0.36155097582789364 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:GREEN:body=+0.3800:wick=0.2090; 2026-08-17:RED:body=-0.1300:wick=0.3900; 2026-08-18:GREEN:body=+0.0100:wick=0.2000; 2026-08-19:GREEN:body=+0.6000:wick=0.2350; 2026-08-20:GREEN:body=+0.6700:wick=0.5300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0 (now=18.0 vs prior_export=17.0 on finviz_2026-08-19) | **GOOD** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 11.91 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=11.91 vs prior=11.91 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.44 | **GOOD** |
| `B13_short_float` | 8.15 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+17**  ·  Software - Application
price=251.02000427246094  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.24 on 2026-08-20; prev RSI=60.46 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.46@2026-08-19 → 60.24@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.46@2026-08-19 → 60.24@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.46@2026-08-19 → 60.24@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=5.103 (G=7.4000 R=1.4500); 2026-08-19:GREEN:O=243.8900,C=251.2900,body=+7.4000,vol=1558300.0; 2026-08-20:RED:O=252.4700,C=251.0200,body=-1.4500,vol=1482600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.051 (Gvol=1558300 Rvol=1482600); 2026-08-19:GREEN:O=243.8900,C=251.2900,body=+7.4000,vol=1558300.0; 2026-08-20:RED:O=252.4700,C=251.0200,body=-1.4500,vol=1482600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.734 on 2026-08-20: today_vol=1482600 / avg20=2018730 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.359 on 2026-08-20 (price=251.0200, mid=240.9775, upper=268.9798, lower=212.9752; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=251.0200 vs SMA50=219.7220 dist=+14.24% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=240.98_50=219.72_80=226.98 on 2026-08-20: SMA20=240.9775 SMA50=219.7220 SMA80=226.9834 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-12@194.4700; S2[2026-06-17→2026-07-17] low=2026-06-22@185.5000; S3[2026-07-20→2026-08-20] low=2026-07-23@202.3700 | lows=[194.47000122070312, 185.5, 202.3699951171875] span=9.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.5385728484727336 wick_frac=0.4614271515272665 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.25573146675493624 wick_frac=0.7442685332450638 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.103454807581003 need>1.4; red_wick_gt_green=False 5d trail=2026-08-14:RED:body=-8.3400:wick=1.1100; 2026-08-17:RED:body=-6.5600:wick=2.0900; 2026-08-18:GREEN:body=+1.4800:wick=5.8200; 2026-08-19:GREEN:body=+7.4000:wick=6.3400; 2026-08-20:RED:body=-1.4500:wick=4.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.15 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.15 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7519.0 | **NEUTRAL** |
| `B04_income` | 1463.0 | **GOOD** |
| `B05_profit_margin` | 19.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 314.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.6299999999999955 (now=314.6 vs prior_export=312.97 on finviz_2026-08-19) | **GOOD** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | 1.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.35 vs prior=1.35 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.31 | **GOOD** |
| `B13_short_float` | 4.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.15 (this export) | prior_export=5.15 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.15 (this export) | prior_export=2.15 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+16**  ·  Specialty Industrial Machinery
price=78.0999984741211  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.87 on 2026-08-20; prev RSI=53.29 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.29@2026-08-19 → 53.87@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.29@2026-08-19 → 53.87@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.29@2026-08-19 → 53.87@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=1.261 (G=1.1100 R=0.8800); 2026-08-19:RED:O=78.7700,C=77.8900,body=-0.8800,vol=1385700.0; 2026-08-20:GREEN:O=76.9900,C=78.1000,body=+1.1100,vol=1291900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=0.932 (Gvol=1291900 Rvol=1385700); 2026-08-19:RED:O=78.7700,C=77.8900,body=-0.8800,vol=1385700.0; 2026-08-20:GREEN:O=76.9900,C=78.1000,body=+1.1100,vol=1291900.0 | **BAD** |
| `A07_rvol` | RVOL=0.720 on 2026-08-20: today_vol=1291900 / avg20=1795065 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.099 on 2026-08-20 (price=78.1000, mid=77.3925, upper=84.5219, lower=70.2631; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=78.1000 vs SMA50=75.1998 dist=+3.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=77.3925 SMA50=75.1998 SMA80=74.4223 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-19@63.0844; S2[2026-06-17→2026-07-17] low=2026-07-17@66.9300; S3[2026-07-20→2026-08-20] low=2026-07-20@66.3300 | lows=[63.08438243991164, 66.93000030517578, 66.33000183105469] span=6.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6491235114864835 wick_frac=0.35087648851351655 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.30555408383842664 wick_frac=0.6944459161615734 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2613682668215669 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-0.5100:wick=1.7000; 2026-08-17:RED:body=-0.1800:wick=1.0900; 2026-08-18:RED:body=-0.5600:wick=1.4900; 2026-08-19:RED:body=-0.8800:wick=2.0000; 2026-08-20:GREEN:body=+1.1100:wick=0.6000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=0.44 vs prior=0.43 on finviz_2026-08-19) | **GOOD** |
| `B12_institutional_transactions` | 4.34 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DSGX  ·  score **+16**  ·  Software - Application
price=77.83000183105469  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.95 on 2026-08-20; prev RSI=55.41 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.41@2026-08-19 → 55.95@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.41@2026-08-19 → 55.95@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.41@2026-08-19 → 55.95@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.9200 R=0.0000); 2026-08-19:GREEN:O=74.9100,C=77.5800,body=+2.6700,vol=318200.0; 2026-08-20:GREEN:O=77.5800,C=77.8300,body=+0.2500,vol=456600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=774800 Rvol=0); 2026-08-19:GREEN:O=74.9100,C=77.5800,body=+2.6700,vol=318200.0; 2026-08-20:GREEN:O=77.5800,C=77.8300,body=+0.2500,vol=456600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.959 on 2026-08-20: today_vol=456600 / avg20=476105 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.264 on 2026-08-20 (price=77.8300, mid=75.8995, upper=83.2253, lower=68.5737; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=77.8300 vs SMA50=73.3400 dist=+6.12% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=75.8995 SMA50=73.3400 SMA80=72.7836 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-18@67.0200; S2[2026-06-17→2026-07-17] low=2026-06-22@65.8100; S3[2026-07-20→2026-08-20] low=2026-07-23@65.6700 | lows=[67.0199966430664, 65.80999755859375, 65.66999816894531] span=2.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.5291417760525328 wick_frac=0.47085822394746724 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:DOJI:body=+0.0000:wick=2.3200; 2026-08-17:RED:body=-2.4000:wick=1.2300; 2026-08-18:RED:body=-0.6400:wick=1.0200; 2026-08-19:GREEN:body=+2.6700:wick=0.2200; 2026-08-20:GREEN:body=+0.2500:wick=1.6100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.99 (current export asof; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.04 (current export; earnings_date=9/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 753.87 | **NEUTRAL** |
| `B04_income` | 176.0 | **GOOD** |
| `B05_profit_margin` | 23.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.23 vs prior_export=100.23 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.93 | **GOOD** |
| `B13_short_float` | 3.21 | **NEUTRAL** |
| `B14_earnings_date` | 9/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.99 (this export) | prior_export=5.99 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.04 (this export) | prior_export=1.04 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AHR  ·  score **+16**  ·  REIT - Healthcare Facilities
price=55.77000045776367  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.93 on 2026-08-20; prev RSI=53.63 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.63@2026-08-19 → 55.93@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.63@2026-08-19 → 55.93@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.63@2026-08-19 → 55.93@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=19.000 (G=0.7600 R=0.0400); 2026-08-19:RED:O=55.2400,C=55.2000,body=-0.0400,vol=1856900.0; 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.232 (Gvol=2287500 Rvol=1856900); 2026-08-19:RED:O=55.2400,C=55.2000,body=-0.0400,vol=1856900.0; 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.780 on 2026-08-20: today_vol=2287500 / avg20=2934045 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.130 on 2026-08-20 (price=55.7700, mid=55.3755, upper=58.4139, lower=52.3371; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=55.7700 vs SMA50=52.7087 dist=+5.81% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=55.3755 SMA50=52.7087 SMA80=51.5634 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-08@44.5946; S2[2026-06-17→2026-07-17] low=2026-06-18@45.5400; S3[2026-07-20→2026-08-20] low=2026-08-11@52.3300 | lows=[44.59456890815638, 45.539996507535484, 52.33000183105469] span=17.35% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6608705459322113 wick_frac=0.3391294540677887 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.042106193055650366 wick_frac=0.9578938069443497 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.999618539004388 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:GREEN:body=+0.3000:wick=0.6800; 2026-08-17:GREEN:body=+0.2500:wick=0.4100; 2026-08-18:RED:body=-0.6800:wick=0.3300; 2026-08-19:RED:body=-0.0400:wick=0.9100; 2026-08-20:GREEN:body=+0.7600:wick=0.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.62 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.98 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2502.39 | **NEUTRAL** |
| `B04_income` | 121.02 | **GOOD** |
| `B05_profit_margin` | 4.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.67 vs prior_export=63.67 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.76 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.76 vs prior=-1.76 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 10.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WTW  ·  score **+16**  ·  Insurance Brokers
price=341.1499938964844  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.57 on 2026-08-20; prev RSI=64.41 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.41@2026-08-19 → 67.57@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.41@2026-08-19 → 67.57@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.41@2026-08-19 → 67.57@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=12.0000 R=0.0000); 2026-08-19:GREEN:O=329.1500,C=335.4600,body=+6.3100,vol=468600.0; 2026-08-20:GREEN:O=335.4600,C=341.1500,body=+5.6900,vol=458800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=927400 Rvol=0); 2026-08-19:GREEN:O=329.1500,C=335.4600,body=+6.3100,vol=468600.0; 2026-08-20:GREEN:O=335.4600,C=341.1500,body=+5.6900,vol=458800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.788 on 2026-08-20: today_vol=458800 / avg20=582595 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.362 on 2026-08-20 (price=341.1500, mid=329.0130, upper=362.5413, lower=295.4847; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=341.1500 vs SMA50=295.0781 dist=+15.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=329.0130 SMA50=295.0781 SMA80=281.6069 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-01@247.6386; S2[2026-06-17→2026-07-17] low=2026-06-22@249.3324; S3[2026-07-20→2026-08-20] low=2026-07-23@284.8000 | lows=[247.63860027759392, 249.33244136274328, 284.79998779296875] span=15.01% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6813358882239875 wick_frac=0.3186641117760125 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-3.2400:wick=2.3800; 2026-08-17:RED:body=-3.5400:wick=3.7100; 2026-08-18:RED:body=-1.9500:wick=3.9200; 2026-08-19:GREEN:body=+6.3100:wick=2.5200; 2026-08-20:GREEN:body=+5.6900:wick=3.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.64 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.05 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 10104.0 | **NEUTRAL** |
| `B04_income` | 1565.0 | **GOOD** |
| `B05_profit_margin` | 15.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 382.35 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.2000000000000455 (now=382.35 vs prior_export=379.15 on finviz_2026-08-19) | **GOOD** |
| `B09_analyst_recom` | 1.85 | **GOOD** |
| `B10_insider_transactions` | 0.04 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.04 vs prior=0.04 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.09 | **BAD** |
| `B13_short_float` | 3.82 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.64 (this export) | prior_export=7.64 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.05 (this export) | prior_export=2.05 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+16**  ·  Specialty Industrial Machinery
price=49.459999084472656  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=45.74 on 2026-08-20; prev RSI=42.33 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.33@2026-08-19 → 45.74@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.33@2026-08-19 → 45.74@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.33@2026-08-19 → 45.74@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7300 R=0.0000); 2026-08-19:GREEN:O=48.0600,C=48.5600,body=+0.5000,vol=499000.0; 2026-08-20:GREEN:O=48.2300,C=49.4600,body=+1.2300,vol=399700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=898700 Rvol=0); 2026-08-19:GREEN:O=48.0600,C=48.5600,body=+0.5000,vol=499000.0; 2026-08-20:GREEN:O=48.2300,C=49.4600,body=+1.2300,vol=399700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.691 on 2026-08-20: today_vol=399700 / avg20=578460 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.662 on 2026-08-20 (price=49.4600, mid=51.9830, upper=55.7935, lower=48.1725; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=49.4600 vs SMA50=48.9266 dist=+1.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=51.9830 SMA50=48.9266 SMA80=46.0637 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-03@35.7100; S2[2026-06-17→2026-07-17] low=2026-06-23@44.3000; S3[2026-07-20→2026-08-20] low=2026-08-20@47.5800 | lows=[35.709999084472656, 44.29999923706055, 47.58000183105469] span=33.24% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.5631318142469293 wick_frac=0.4368681857530707 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-14:GREEN:body=+0.3300:wick=0.7700; 2026-08-17:RED:body=-1.0700:wick=0.7000; 2026-08-18:RED:body=-1.6800:wick=0.0900; 2026-08-19:GREEN:body=+0.5000:wick=0.4900; 2026-08-20:GREEN:body=+1.2300:wick=0.7500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.7 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.9 | **NEUTRAL** |
| `B04_income` | 140.3 | **GOOD** |
| `B05_profit_margin` | 7.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.17 vs prior_export=70.17 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.93 | **GOOD** |
| `B13_short_float` | 14.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AOS  ·  score **+16**  ·  Specialty Industrial Machinery
price=62.43000030517578  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.58 on 2026-08-20; prev RSI=57.62 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.62@2026-08-19 → 52.58@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.62@2026-08-19 → 52.58@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.62@2026-08-19 → 52.58@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=7.250 (G=2.0300 R=0.2800); 2026-08-19:GREEN:O=61.7700,C=63.8000,body=+2.0300,vol=1629700.0; 2026-08-20:RED:O=62.7100,C=62.4300,body=-0.2800,vol=1195100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.364 (Gvol=1629700 Rvol=1195100); 2026-08-19:GREEN:O=61.7700,C=63.8000,body=+2.0300,vol=1629700.0; 2026-08-20:RED:O=62.7100,C=62.4300,body=-0.2800,vol=1195100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.761 on 2026-08-20: today_vol=1195100 / avg20=1571250 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.109 on 2026-08-20 (price=62.4300, mid=62.1216, upper=64.9557, lower=59.2874; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=62.4300 vs SMA50=60.5357 dist=+3.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=62.1216 SMA50=60.5357 SMA80=59.8024 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-20@53.8340; S2[2026-06-17→2026-07-17] low=2026-06-23@56.7861; S3[2026-07-20→2026-08-20] low=2026-07-20@57.6509 | lows=[53.83400803848822, 56.786133313205724, 57.65089612018473] span=7.09% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.8529402807808265 wick_frac=0.1470597192191735 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.2204714002937616 wick_frac=0.7795285997062383 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.250027247956403 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-0.2600:wick=0.4400; 2026-08-17:RED:body=-0.2500:wick=0.6900; 2026-08-18:RED:body=-0.7700:wick=0.3600; 2026-08-19:GREEN:body=+2.0300:wick=0.3500; 2026-08-20:RED:body=-0.2800:wick=0.9900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.67 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.9 | **NEUTRAL** |
| `B04_income` | 500.3 | **GOOD** |
| `B05_profit_margin` | 13.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=69.7 vs prior_export=69.7 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.62 | **NEUTRAL** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.48 | **GOOD** |
| `B13_short_float` | 9.78 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.67 (this export) | prior_export=7.67 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTEK  ·  score **+16**  ·  Engineering & Construction
price=36.56999969482422  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.64 on 2026-08-20; prev RSI=67.90 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.90@2026-08-19 → 67.64@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.90@2026-08-19 → 67.64@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.90@2026-08-19 → 67.64@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2000 R=0.0000); 2026-08-19:GREEN:O=35.6000,C=36.6000,body=+1.0000,vol=4643000.0; 2026-08-20:GREEN:O=36.3700,C=36.5700,body=+0.2000,vol=3856800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8499800 Rvol=0); 2026-08-19:GREEN:O=35.6000,C=36.6000,body=+1.0000,vol=4643000.0; 2026-08-20:GREEN:O=36.3700,C=36.5700,body=+0.2000,vol=3856800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.268 on 2026-08-20: today_vol=3856800 / avg20=3040875 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.590 on 2026-08-20 (price=36.5700, mid=34.4760, upper=38.0243, lower=30.9278; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=36.5700 vs SMA50=31.3641 dist=+16.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=34.4760 SMA50=31.3641 SMA80=30.4813 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-20@26.3265; S2[2026-06-17→2026-07-17] low=2026-06-22@26.6558; S3[2026-07-20→2026-08-20] low=2026-07-24@30.3483 | lows=[26.326495228866442, 26.655826330841563, 30.348322603720742] span=15.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.5164795527213368 wick_frac=0.48352044727866317 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-14:GREEN:body=+0.1100:wick=0.5400; 2026-08-17:RED:body=-0.4200:wick=0.3900; 2026-08-18:RED:body=-0.6700:wick=0.3600; 2026-08-19:GREEN:body=+1.0000:wick=0.3700; 2026-08-20:GREEN:body=+0.2000:wick=0.4600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5069.48 | **NEUTRAL** |
| `B04_income` | 435.98 | **GOOD** |
| `B05_profit_margin` | 8.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.33 vs prior_export=40.33 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.16 | **GOOD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AWK  ·  score **+16**  ·  Utilities - Regulated Water
price=137.2100067138672  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.81 on 2026-08-20; prev RSI=62.27 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.27@2026-08-19 → 57.81@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.27@2026-08-19 → 57.81@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.27@2026-08-19 → 57.81@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=3.067 (G=2.3000 R=0.7500); 2026-08-19:GREEN:O=136.4100,C=138.7100,body=+2.3000,vol=3029200.0; 2026-08-20:RED:O=137.9600,C=137.2100,body=-0.7500,vol=1661700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.823 (Gvol=3029200 Rvol=1661700); 2026-08-19:GREEN:O=136.4100,C=138.7100,body=+2.3000,vol=3029200.0; 2026-08-20:RED:O=137.9600,C=137.2100,body=-0.7500,vol=1661700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.679 on 2026-08-20: today_vol=1661700 / avg20=2446805 (avg window 2026-07-20→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.562 on 2026-08-20 (price=137.2100, mid=135.0542, upper=138.8911, lower=131.2174; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=137.2100 vs SMA50=131.2784 dist=+4.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=135.0542 SMA50=131.2784 SMA80=128.9069 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-02@119.7733; S2[2026-06-17→2026-07-17] low=2026-06-17@123.6575; S3[2026-07-20→2026-08-20] low=2026-08-04@128.8827 | lows=[119.77331916694077, 123.65748936872049, 128.88273703185902] span=7.61% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.7516355839234068 wick_frac=0.2483644160765932 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.3333333333333333 wick_frac=0.6666666666666666 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0666707356770835 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:RED:body=-0.3000:wick=1.6100; 2026-08-17:RED:body=-0.8400:wick=1.8500; 2026-08-18:GREEN:body=+0.3400:wick=2.0500; 2026-08-19:GREEN:body=+2.3000:wick=0.7600; 2026-08-20:RED:body=-0.7500:wick=1.5000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.21 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5284.0 | **NEUTRAL** |
| `B04_income` | 1128.0 | **GOOD** |
| `B05_profit_margin` | 21.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=141.91 vs prior_export=141.91 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 2.71 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.36 | **GOOD** |
| `B13_short_float` | 6.09 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.21 (this export) | prior_export=2.21 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+16**  ·  Consumer Electronics
price=16.049999237060547  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.53 on 2026-08-20; prev RSI=55.14 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.14@2026-08-19 → 53.53@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.14@2026-08-19 → 53.53@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.14@2026-08-19 → 53.53@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=11.500 (G=0.4600 R=0.0400); 2026-08-19:GREEN:O=15.8200,C=16.2800,body=+0.4600,vol=1384600.0; 2026-08-20:RED:O=16.0900,C=16.0500,body=-0.0400,vol=996000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.390 (Gvol=1384600 Rvol=996000); 2026-08-19:GREEN:O=15.8200,C=16.2800,body=+0.4600,vol=1384600.0; 2026-08-20:RED:O=16.0900,C=16.0500,body=-0.0400,vol=996000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.533 on 2026-08-20: today_vol=996000 / avg20=1867095 (avg window 2026-07-22→2026-08-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.177 on 2026-08-20 (price=16.0500, mid=15.7780, upper=17.3137, lower=14.2423; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=16.0500 vs SMA50=14.9498 dist=+7.36% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.78_50=14.95_80=15.02 on 2026-08-20: SMA20=15.7780 SMA50=14.9498 SMA80=15.0202 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-20 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-17@14.0700; S2[2026-06-18→2026-07-20] low=2026-06-29@13.0800; S3[2026-07-22→2026-08-20] low=2026-07-30@14.0200 | lows=[14.069999694824219, 13.079999923706055, 14.020000457763672] span=7.57% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.6301387401039897 wick_frac=0.36986125989601026 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.0888909610709979 wick_frac=0.9111090389290021 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.499761586877742 need>1.4; red_wick_gt_green=False 5d trail=2026-08-14:RED:body=-0.5200:wick=0.2000; 2026-08-17:RED:body=-0.6200:wick=0.1200; 2026-08-18:GREEN:body=+0.4100:wick=0.3300; 2026-08-19:GREEN:body=+0.4600:wick=0.2700; 2026-08-20:RED:body=-0.0400:wick=0.4100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 409.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=409.09 vs prior=409.09 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.32 | **GOOD** |
| `B13_short_float` | 8.35 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEO  ·  score **+15**  ·  Security & Protection Services
price=31.719999313354492  pair=`2026-08-19→2026-08-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.13 on 2026-08-20; prev RSI=52.17 on 2026-08-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.17@2026-08-19 → 59.13@2026-08-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.17@2026-08-19 → 59.13@2026-08-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.17@2026-08-19 → 59.13@2026-08-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_body_sum/RED_body_sum=2.842 (G=1.0800 R=0.3800); 2026-08-19:RED:O=31.0600,C=30.6800,body=-0.3800,vol=3231900.0; 2026-08-20:GREEN:O=30.6400,C=31.7200,body=+1.0800,vol=3763700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-19 + 2026-08-20; ratio=GREEN_vol/RED_vol=1.165 (Gvol=3763700 Rvol=3231900); 2026-08-19:RED:O=31.0600,C=30.6800,body=-0.3800,vol=3231900.0; 2026-08-20:GREEN:O=30.6400,C=31.7200,body=+1.0800,vol=3763700.0 | **GOOD** |
| `A07_rvol` | RVOL=2.077 on 2026-08-20: today_vol=3763700 / avg20=1811805 (avg window 2026-07-20→2026-08-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.793 on 2026-08-20 (price=31.7200, mid=30.9535, upper=31.9204, lower=29.9866; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-20: price=31.7200 vs SMA50=29.9068 dist=+6.06% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-20: SMA20=30.9535 SMA50=29.9068 SMA80=26.7794 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-20 (63 bars); S1[2026-05-18→2026-06-16] low=2026-05-26@22.2900; S2[2026-06-17→2026-07-17] low=2026-06-18@28.3900; S3[2026-07-20→2026-08-20] low=2026-08-06@28.8900 | lows=[22.290000915527344, 28.389999389648438, 28.889999389648438] span=29.61% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: GREEN body_frac=0.7248313470474532 wick_frac=0.2751686529525468 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-19+2026-08-20: RED body_frac=0.34862303446875975 wick_frac=0.6513769655312402 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8421113392126647 need>1.4; red_wick_gt_green=True 5d trail=2026-08-14:GREEN:body=+1.0700:wick=0.1600; 2026-08-17:RED:body=-0.1800:wick=0.6000; 2026-08-18:RED:body=-0.0500:wick=0.6600; 2026-08-19:RED:body=-0.3800:wick=0.7100; 2026-08-20:GREEN:body=+1.0800:wick=0.4100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.7 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.48 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2827.32 | **NEUTRAL** |
| `B04_income` | 291.54 | **GOOD** |
| `B05_profit_margin` | 10.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.75 vs prior_export=37.75 on finviz_2026-08-19) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.16 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.16 vs prior=-0.16 on finviz_2026-08-19) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.66 | **BAD** |
| `B13_short_float` | 7.64 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.7 (this export) | prior_export=28.7 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.48 (this export) | prior_export=1.48 (finviz_2026-08-19) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-20_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.