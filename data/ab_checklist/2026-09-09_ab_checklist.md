# A+B1 Feature Checklist — 2026-09-09

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,657** names
- Export: `finviz_2026-09-09.csv` · prior export for Δ: `2026-09-08`
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
| 1 | AVO | +18 | 18 | 0 | 2026-09-04→2026-09-08 | Food Distribution |
| 2 | SKM | +17 | 17 | 0 | 2026-09-04→2026-09-08 | Telecom Services |
| 3 | CFFN | +17 | 17 | 0 | 2026-09-04→2026-09-08 | Banks - Regional |
| 4 | RPRX | +17 | 18 | 1 | 2026-09-04→2026-09-08 | Biotechnology |
| 5 | HASI | +16 | 16 | 0 | 2026-09-04→2026-09-08 | Asset Management |
| 6 | HSBC | +16 | 17 | 1 | 2026-09-04→2026-09-08 | Banks - Diversified |
| 7 | SR | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Utilities - Regulated Gas |
| 8 | MMSI | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Medical Instruments & Supplies |
| 9 | APAM | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Asset Management |
| 10 | SHEL | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Oil & Gas Integrated |
| 11 | ALB | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Specialty Chemicals |
| 12 | KO | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Beverages - Non-Alcoholic |
| 13 | RSG | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Waste Management |
| 14 | LFST | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Medical Care Facilities |
| 15 | KMI | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Oil & Gas Midstream |

## Full checklist — top 15

### AVO  ·  score **+18**  ·  Food Distribution
price=12.869999885559082  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.81 on 2026-09-08; prev RSI=46.67 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.67@2026-09-04 → 50.81@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.67@2026-09-04 → 50.81@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.67@2026-09-04 → 50.81@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4000 R=0.0000); 2026-09-04:GREEN:O=12.4900,C=12.6800,body=+0.1900,vol=458400.0; 2026-09-08:GREEN:O=12.6600,C=12.8700,body=+0.2100,vol=990200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1448600 Rvol=0); 2026-09-04:GREEN:O=12.4900,C=12.6800,body=+0.1900,vol=458400.0; 2026-09-08:GREEN:O=12.6600,C=12.8700,body=+0.2100,vol=990200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.931 on 2026-09-08: today_vol=990200 / avg20=512800 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.106 on 2026-09-08 (price=12.8700, mid=12.9165, upper=13.3546, lower=12.4784; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=12.8700 vs SMA50=12.8596 dist=+0.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=12.9165 SMA50=12.8596 SMA80=12.3481 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-08@10.0700; S2[2026-07-07→2026-08-07] low=2026-08-05@11.6900; S3[2026-08-10→2026-09-08] low=2026-08-27@12.3300 | lows=[10.069999694824219, 11.6899995803833, 12.329999923706055] span=22.44% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.6716680659191323 wick_frac=0.32833193408086775 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.1000:wick=0.0800; 2026-09-02:RED:body=-0.1000:wick=0.1500; 2026-09-03:RED:body=-0.2200:wick=0.0900; 2026-09-04:GREEN:body=+0.1900:wick=0.0600; 2026-09-08:GREEN:body=+0.2100:wick=0.1500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=56.52 (current export asof; earnings_date=9/8/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=22.42 (current export; earnings_date=9/8/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1338.5 | **NEUTRAL** |
| `B04_income` | 1.6 | **GOOD** |
| `B05_profit_margin` | 0.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.5 vs prior_export=16.5 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 11.27 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=11.27 vs prior=11.27 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 18.52 | **GOOD** |
| `B13_short_float` | 8.91 | **NEUTRAL** |
| `B14_earnings_date` | 9/8/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=56.52 (this export) | prior_export=-81.24 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=22.42 (this export) | prior_export=13.5 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+17**  ·  Telecom Services
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
| `B07_target_price` | 43.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.060000000000002274 (now=43.92 vs prior_export=43.86 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 0.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=9.5 vs prior_export=9.5 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 5.45 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.76 (this export) | prior_export=11.76 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RPRX  ·  score **+17**  ·  Biotechnology
price=60.59000015258789  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.26 on 2026-09-08; prev RSI=69.68 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.68@2026-09-04 → 50.26@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.68@2026-09-04 → 50.26@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.68@2026-09-04 → 50.26@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7900 R=0.0000); 2026-09-04:GREEN:O=63.5500,C=63.9600,body=+0.4100,vol=3401300.0; 2026-09-08:GREEN:O=59.2100,C=60.5900,body=+1.3800,vol=8631500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12032800 Rvol=0); 2026-09-04:GREEN:O=63.5500,C=63.9600,body=+0.4100,vol=3401300.0; 2026-09-08:GREEN:O=59.2100,C=60.5900,body=+1.3800,vol=8631500.0 | **GOOD** |
| `A07_rvol` | RVOL=3.256 on 2026-09-08: today_vol=8631500 / avg20=2651020 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.108 on 2026-09-08 (price=60.5900, mid=60.9733, upper=64.5301, lower=57.4164; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=60.5900 vs SMA50=58.7532 dist=+3.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=60.9733 SMA50=58.7532 SMA80=56.9234 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-05→2026-09-08 (63 bars); S1[2026-06-05→2026-07-07] low=2026-06-18@50.9447; S2[2026-07-08→2026-08-07] low=2026-07-15@54.7295; S3[2026-08-10→2026-09-08] low=2026-08-10@56.3031 | lows=[50.94473680886078, 54.72948720381519, 56.30314205782566] span=10.52% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5252074299359974 wick_frac=0.4747925700640025 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-01:GREEN:body=+0.5050:wick=0.4650; 2026-09-02:GREEN:body=+0.4500:wick=0.8050; 2026-09-03:GREEN:body=+0.3150:wick=1.1250; 2026-09-04:GREEN:body=+0.4100:wick=0.5400; 2026-09-08:GREEN:body=+1.3800:wick=0.8500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.82 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.85 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2536.01 | **NEUTRAL** |
| `B04_income` | 814.79 | **GOOD** |
| `B05_profit_margin` | 32.13 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4199999999999875 (now=65.71 vs prior_export=65.29 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -1.63 | **BAD** |
| `B11_insider_tx_delta` | delta=0.4700000000000002 (now=-1.63 vs prior=-2.1 on finviz_2026-09-08) | **GOOD** |
| `B12_institutional_transactions` | 1.39 | **GOOD** |
| `B13_short_float` | 2.78 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.82 (this export) | prior_export=3.82 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.85 (this export) | prior_export=1.85 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HASI  ·  score **+16**  ·  Asset Management
price=39.369998931884766  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.27 on 2026-09-08; prev RSI=38.65 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 38.65@2026-09-04 → 47.27@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 38.65@2026-09-04 → 47.27@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 38.65@2026-09-04 → 47.27@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5200 R=0.0000); 2026-09-04:GREEN:O=38.0200,C=38.3400,body=+0.3200,vol=1905400.0; 2026-09-08:GREEN:O=38.1700,C=39.3700,body=+1.2000,vol=851400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2756800 Rvol=0); 2026-09-04:GREEN:O=38.0200,C=38.3400,body=+0.3200,vol=1905400.0; 2026-09-08:GREEN:O=38.1700,C=39.3700,body=+1.2000,vol=851400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.779 on 2026-09-08: today_vol=851400 / avg20=1092515 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.461 on 2026-09-08 (price=39.3700, mid=40.4370, upper=42.7501, lower=38.1239; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=39.3700 vs SMA50=39.2382 dist=+0.34% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=40.44_50=39.24_80=39.25 on 2026-09-08: SMA20=40.4370 SMA50=39.2382 SMA80=39.2485 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-10@35.8512; S2[2026-07-07→2026-08-07] low=2026-07-08@37.1200; S3[2026-08-10→2026-09-08] low=2026-09-04@37.6400 | lows=[35.85118516259977, 37.119998931884766, 37.63999938964844] span=4.99% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5493862047669705 wick_frac=0.45061379523302947 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.2100:wick=0.4900; 2026-09-02:GREEN:body=+0.2400:wick=0.7500; 2026-09-03:RED:body=-0.3000:wick=0.8200; 2026-09-04:GREEN:body=+0.3200:wick=0.8600; 2026-09-08:GREEN:body=+1.2000:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.66 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 462.89 | **NEUTRAL** |
| `B04_income` | 83.73 | **GOOD** |
| `B05_profit_margin` | 18.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.03999999999999915 (now=49.33 vs prior_export=49.29 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.26 | **GOOD** |
| `B13_short_float` | 10.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.9 (this export) | prior_export=7.9 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=108.62 vs prior_export=108.62 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.21 | **GOOD** |
| `B13_short_float` | 0.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.31 (this export) | prior_export=3.31 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SR  ·  score **+15**  ·  Utilities - Regulated Gas
price=83.44000244140625  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.56 on 2026-09-08; prev RSI=51.97 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.97@2026-09-04 → 55.56@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.97@2026-09-04 → 55.56@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.97@2026-09-04 → 55.56@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=3.250 (G=0.9100 R=0.2800); 2026-09-04:RED:O=82.9600,C=82.6800,body=-0.2800,vol=422800.0; 2026-09-08:GREEN:O=82.5300,C=83.4400,body=+0.9100,vol=456200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.079 (Gvol=456200 Rvol=422800); 2026-09-04:RED:O=82.9600,C=82.6800,body=-0.2800,vol=422800.0; 2026-09-08:GREEN:O=82.5300,C=83.4400,body=+0.9100,vol=456200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.973 on 2026-09-08: today_vol=456200 / avg20=469085 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.533 on 2026-09-08 (price=83.4400, mid=82.7635, upper=84.0317, lower=81.4953; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=83.4400 vs SMA50=81.5140 dist=+2.36% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=82.76_50=81.51_80=81.61 on 2026-09-08: SMA20=82.7635 SMA50=81.5140 SMA80=81.6076 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-22@75.8500; S2[2026-07-07→2026-08-07] low=2026-08-04@78.2800; S3[2026-08-10→2026-09-08] low=2026-08-24@79.9000 | lows=[75.8499984741211, 78.27999877929688, 79.9000015258789] span=5.34% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5759510174123827 wick_frac=0.4240489825876172 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.19580330037933555 wick_frac=0.8041966996206644 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.250027247956403 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.5400:wick=0.5200; 2026-09-02:RED:body=-0.0500:wick=2.0200; 2026-09-03:GREEN:body=+1.3100:wick=0.4900; 2026-09-04:RED:body=-0.2800:wick=1.1500; 2026-09-08:GREEN:body=+0.9100:wick=0.6700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=17.93 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.48 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2536.5 | **NEUTRAL** |
| `B04_income` | 531.0 | **GOOD** |
| `B05_profit_margin` | 20.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 92.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=92.38 vs prior_export=92.38 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -0.06 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.06 vs prior=-0.06 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 4.55 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.93 (this export) | prior_export=17.93 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.48 (this export) | prior_export=6.48 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MMSI  ·  score **+15**  ·  Medical Instruments & Supplies
price=87.37999725341797  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.08 on 2026-09-08; prev RSI=58.96 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.96@2026-09-04 → 50.08@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.96@2026-09-04 → 50.08@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.96@2026-09-04 → 50.08@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.761 (G=1.9200 R=1.0900); 2026-09-04:GREEN:O=87.6800,C=89.6000,body=+1.9200,vol=599600.0; 2026-09-08:RED:O=88.4700,C=87.3800,body=-1.0900,vol=1422100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.422 (Gvol=599600 Rvol=1422100); 2026-09-04:GREEN:O=87.6800,C=89.6000,body=+1.9200,vol=599600.0; 2026-09-08:RED:O=88.4700,C=87.3800,body=-1.0900,vol=1422100.0 | **BAD** |
| `A07_rvol` | RVOL=1.910 on 2026-09-08: today_vol=1422100 / avg20=744455 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.399 on 2026-09-08 (price=87.3800, mid=90.2275, upper=92.2630, lower=88.1920; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=True on 2026-09-08: price=87.3800 vs SMA50=81.7776 dist=+6.85% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=90.2275 SMA50=81.7776 SMA80=75.2042 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-04@61.8800; S2[2026-07-07→2026-08-07] low=2026-07-09@70.0000; S3[2026-08-10→2026-09-08] low=2026-09-08@86.7200 | lows=[61.880001068115234, 70.0, 86.72000122070312] span=40.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.7245271937375662 wick_frac=0.27547280626243376 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.4977181516680137 wick_frac=0.5022818483319863 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7614597988366965 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.2400:wick=0.9700; 2026-09-02:RED:body=-0.2900:wick=1.8200; 2026-09-03:RED:body=-2.2400:wick=0.9800; 2026-09-04:GREEN:body=+1.9200:wick=0.7300; 2026-09-08:RED:body=-1.0900:wick=1.1000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=24.09 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1580.33 | **NEUTRAL** |
| `B04_income` | 145.56 | **GOOD** |
| `B05_profit_margin` | 9.21 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 101.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=101.45 vs prior_export=101.45 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -1.56 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.56 vs prior=-1.56 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.96 | **GOOD** |
| `B13_short_float` | 5.94 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.09 (this export) | prior_export=24.09 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### APAM  ·  score **+15**  ·  Asset Management
price=41.060001373291016  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.05 on 2026-09-08; prev RSI=49.52 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.52@2026-09-04 → 46.05@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.52@2026-09-04 → 46.05@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.52@2026-09-04 → 46.05@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=6.308 (G=0.8200 R=0.1300); 2026-09-04:GREEN:O=40.7500,C=41.5700,body=+0.8200,vol=554000.0; 2026-09-08:RED:O=41.1900,C=41.0600,body=-0.1300,vol=649000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.854 (Gvol=554000 Rvol=649000); 2026-09-04:GREEN:O=40.7500,C=41.5700,body=+0.8200,vol=554000.0; 2026-09-08:RED:O=41.1900,C=41.0600,body=-0.1300,vol=649000.0 | **BAD** |
| `A07_rvol` | RVOL=0.845 on 2026-09-08: today_vol=649000 / avg20=767790 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.720 on 2026-09-08 (price=41.0600, mid=42.3155, upper=44.0593, lower=40.5717; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=41.0600 vs SMA50=39.9286 dist=+2.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=42.3155 SMA50=39.9286 SMA80=38.6381 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-07-01@33.9600; S2[2026-07-07→2026-08-07] low=2026-07-08@35.4000; S3[2026-08-10→2026-09-08] low=2026-09-03@40.4000 | lows=[33.959999084472656, 35.400001525878906, 40.400001525878906] span=18.96% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.8817198126286947 wick_frac=0.11828018737130529 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2166626400315349 wick_frac=0.7833373599684651 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.3078232290627385 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.6100:wick=0.8300; 2026-09-02:RED:body=-0.7700:wick=0.5200; 2026-09-03:RED:body=-0.6100:wick=1.4200; 2026-09-04:GREEN:body=+0.8200:wick=0.1100; 2026-09-08:RED:body=-0.1300:wick=0.4700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.01 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1318.13 | **NEUTRAL** |
| `B04_income` | 271.99 | **GOOD** |
| `B05_profit_margin` | 20.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.25 (now=39.5 vs prior_export=39.25 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | -0.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.04999999999999999 (now=-0.39 vs prior=-0.44 on finviz_2026-09-08) | **GOOD** |
| `B12_institutional_transactions` | 2.38 | **GOOD** |
| `B13_short_float` | 4.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHEL  ·  score **+15**  ·  Oil & Gas Integrated
price=95.31999969482422  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.85 on 2026-09-08; prev RSI=60.86 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.86@2026-09-04 → 67.85@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.86@2026-09-04 → 67.85@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.86@2026-09-04 → 67.85@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=5.875 (G=0.4700 R=0.0800); 2026-09-04:RED:O=93.0300,C=92.9500,body=-0.0800,vol=6849003.0; 2026-09-08:GREEN:O=94.8500,C=95.3200,body=+0.4700,vol=7422101.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.084 (Gvol=7422101 Rvol=6849003); 2026-09-04:RED:O=93.0300,C=92.9500,body=-0.0800,vol=6849003.0; 2026-09-08:GREEN:O=94.8500,C=95.3200,body=+0.4700,vol=7422101.0 | **GOOD** |
| `A07_rvol` | RVOL=1.289 on 2026-09-08: today_vol=7422101 / avg20=5756105 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.069 on 2026-09-08 (price=95.3200, mid=91.9145, upper=95.0990, lower=88.7300; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-08: price=95.3200 vs SMA50=87.4829 dist=+8.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=91.9145 SMA50=87.4829 SMA80=85.8281 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-07-01@75.5975; S2[2026-07-10→2026-08-07] low=2026-07-10@80.7667; S3[2026-08-10→2026-09-08] low=2026-08-10@87.9096 | lows=[75.59747315421986, 80.76667254889078, 87.9095701030474] span=16.29% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.34082622863750284 wick_frac=0.6591737713624972 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.05594562294592172 wick_frac=0.9440543770540782 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.874880793438871 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.7900:wick=0.6000; 2026-09-02:RED:body=-0.1800:wick=0.8572; 2026-09-03:RED:body=-0.2300:wick=0.9600; 2026-09-04:RED:body=-0.0800:wick=1.3500; 2026-09-08:GREEN:body=+0.4700:wick=0.9090 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.28000000000000114 (now=102.56 vs prior_export=102.28 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 2.28 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.53 | **GOOD** |
| `B13_short_float` | 0.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=176.0 vs prior_export=176.0 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | -7.86 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-7.86 vs prior=-7.86 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.45 | **GOOD** |
| `B13_short_float` | 8.7 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.49 (this export) | prior_export=8.49 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KO  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=88.36000061035156  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.65 on 2026-09-08; prev RSI=50.25 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.25@2026-09-04 → 51.65@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.25@2026-09-04 → 51.65@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.25@2026-09-04 → 51.65@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.558 (G=0.8100 R=0.5200); 2026-09-04:RED:O=88.5900,C=88.0700,body=-0.5200,vol=17290700.0; 2026-09-08:GREEN:O=87.5500,C=88.3600,body=+0.8100,vol=19409500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.123 (Gvol=19409500 Rvol=17290700); 2026-09-04:RED:O=88.5900,C=88.0700,body=-0.5200,vol=17290700.0; 2026-09-08:GREEN:O=87.5500,C=88.3600,body=+0.8100,vol=19409500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.347 on 2026-09-08: today_vol=19409500 / avg20=14405830 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.179 on 2026-09-08 (price=88.3600, mid=88.9325, upper=92.1368, lower=85.7282; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=88.3600 vs SMA50=86.1804 dist=+2.53% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=88.9325 SMA50=86.1804 SMA80=83.9248 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-18@78.7400; S2[2026-07-10→2026-08-07] low=2026-07-17@80.8300; S3[2026-08-10→2026-09-08] low=2026-08-12@85.6600 | lows=[78.73999786376953, 80.83000183105469, 85.66000366210938] span=8.79% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.80999755859375 wick_frac=0.19000244140625 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.49056046985324286 wick_frac=0.5094395301467571 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5576976686180437 need>1.4; red_wick_gt_green=False 5d trail=2026-09-01:RED:body=-1.5900:wick=0.3800; 2026-09-02:GREEN:body=+0.3400:wick=0.8800; 2026-09-03:GREEN:body=+0.7000:wick=0.5200; 2026-09-04:RED:body=-0.5200:wick=0.5400; 2026-09-08:GREEN:body=+0.8100:wick=0.1900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.07 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 50569.0 | **NEUTRAL** |
| `B04_income` | 14316.0 | **GOOD** |
| `B05_profit_margin` | 28.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 96.24 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=96.24 vs prior_export=96.24 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | -6.42 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.42 vs prior=-6.42 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.09 | **GOOD** |
| `B13_short_float` | 0.92 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.07 (this export) | prior_export=4.07 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+15**  ·  Waste Management
price=221.6300048828125  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.45 on 2026-09-08; prev RSI=56.69 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.69@2026-09-04 → 54.45@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.69@2026-09-04 → 54.45@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.69@2026-09-04 → 54.45@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=2.579 (G=0.4900 R=0.1900); 2026-09-04:RED:O=222.9000,C=222.7100,body=-0.1900,vol=1611000.0; 2026-09-08:GREEN:O=221.1400,C=221.6300,body=+0.4900,vol=1504000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.934 (Gvol=1504000 Rvol=1611000); 2026-09-04:RED:O=222.9000,C=222.7100,body=-0.1900,vol=1611000.0; 2026-09-08:GREEN:O=221.1400,C=221.6300,body=+0.4900,vol=1504000.0 | **BAD** |
| `A07_rvol` | RVOL=1.040 on 2026-09-08: today_vol=1504000 / avg20=1445955 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.268 on 2026-09-08 (price=221.6300, mid=219.8250, upper=226.5557, lower=213.0943; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=221.6300 vs SMA50=217.6139 dist=+1.85% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=219.8250 SMA50=217.6139 SMA80=213.4019 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-18@202.5586; S2[2026-07-07→2026-08-07] low=2026-08-05@204.8000; S3[2026-08-10→2026-09-08] low=2026-08-12@212.2200 | lows=[202.5586467956411, 204.8000030517578, 212.22000122070312] span=4.77% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.16388197109496203 wick_frac=0.836118028905038 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.04656546503756727 wick_frac=0.9534345349624327 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5791502690546944 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.6800:wick=3.3000; 2026-09-02:RED:body=-0.8500:wick=2.8700; 2026-09-03:GREEN:body=+1.4400:wick=2.1400; 2026-09-04:RED:body=-0.1900:wick=3.8900; 2026-09-08:GREEN:body=+0.4900:wick=2.5000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-09-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | 5.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.6600000000000001 (now=5.44 vs prior=4.78 on finviz_2026-09-08) | **GOOD** |
| `B12_institutional_transactions` | 0.74 | **GOOD** |
| `B13_short_float` | 2.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LFST  ·  score **+15**  ·  Medical Care Facilities
price=12.800000190734863  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.23 on 2026-09-08; prev RSI=63.30 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.30@2026-09-04 → 64.23@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.30@2026-09-04 → 64.23@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.30@2026-09-04 → 64.23@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.583 (G=0.1900 R=0.1200); 2026-09-04:RED:O=12.8500,C=12.7300,body=-0.1200,vol=2043800.0; 2026-09-08:GREEN:O=12.6100,C=12.8000,body=+0.1900,vol=3073000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.504 (Gvol=3073000 Rvol=2043800); 2026-09-04:RED:O=12.8500,C=12.7300,body=-0.1200,vol=2043800.0; 2026-09-08:GREEN:O=12.6100,C=12.8000,body=+0.1900,vol=3073000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.741 on 2026-09-08: today_vol=3073000 / avg20=4145990 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.565 on 2026-09-08 (price=12.8000, mid=12.4320, upper=13.0834, lower=11.7806; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=12.8000 vs SMA50=11.4587 dist=+11.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=12.4320 SMA50=11.4587 SMA80=10.1575 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-08@7.2900; S2[2026-07-07→2026-08-07] low=2026-08-05@10.0800; S3[2026-08-10→2026-09-08] low=2026-08-10@10.8500 | lows=[7.289999961853027, 10.079999923706055, 10.850000381469727] span=48.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.45238214177047337 wick_frac=0.5476178582295267 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2739746905999878 wick_frac=0.7260253094000122 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5833267106413416 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.3300:wick=0.1550; 2026-09-02:GREEN:body=+0.1500:wick=0.2600; 2026-09-03:RED:body=-0.0500:wick=0.2800; 2026-09-04:RED:body=-0.1200:wick=0.3180; 2026-09-08:GREEN:body=+0.1900:wick=0.2300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=82.93 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.01 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1584.83 | **NEUTRAL** |
| `B04_income` | 50.6 | **GOOD** |
| `B05_profit_margin` | 3.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.3999999999999986 (now=14.2 vs prior_export=13.8 on finviz_2026-09-08) | **GOOD** |
| `B09_analyst_recom` | 1.45 | **GOOD** |
| `B10_insider_transactions` | -26.65 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-26.65 vs prior=-26.65 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.73 | **GOOD** |
| `B13_short_float` | 4.69 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=82.93 (this export) | prior_export=82.93 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.01 (this export) | prior_export=5.01 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KMI  ·  score **+15**  ·  Oil & Gas Midstream
price=31.979999542236328  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.34 on 2026-09-08; prev RSI=46.43 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.43@2026-09-04 → 52.34@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.43@2026-09-04 → 52.34@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.43@2026-09-04 → 52.34@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=13.500 (G=0.5400 R=0.0400); 2026-09-04:RED:O=31.4400,C=31.4000,body=-0.0400,vol=7728100.0; 2026-09-08:GREEN:O=31.4400,C=31.9800,body=+0.5400,vol=17839400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=2.308 (Gvol=17839400 Rvol=7728100); 2026-09-04:RED:O=31.4400,C=31.4000,body=-0.0400,vol=7728100.0; 2026-09-08:GREEN:O=31.4400,C=31.9800,body=+0.5400,vol=17839400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.712 on 2026-09-08: today_vol=17839400 / avg20=10420825 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.152 on 2026-09-08 (price=31.9800, mid=31.8165, upper=32.8956, lower=30.7374; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=31.9800 vs SMA50=31.7948 dist=+0.58% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=31.82_50=31.79_80=31.86 on 2026-09-08: SMA20=31.8165 SMA50=31.7948 SMA80=31.8633 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-18@30.6039; S2[2026-07-10→2026-08-07] low=2026-08-04@30.6800; S3[2026-08-10→2026-09-08] low=2026-08-24@30.5100 | lows=[30.603945889190435, 30.68000030517578, 30.510000228881836] span=0.56% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.5294115447114944 wick_frac=0.47058845528850557 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.07843313250083213 wick_frac=0.9215668674991678 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.49966622162884 need>1.4; red_wick_gt_green=False 5d trail=2026-09-01:RED:body=-0.4100:wick=0.4000; 2026-09-02:RED:body=-0.0700:wick=0.5300; 2026-09-03:RED:body=-0.4200:wick=0.0900; 2026-09-04:RED:body=-0.0400:wick=0.4700; 2026-09-08:GREEN:body=+0.5400:wick=0.4800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=15.37 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.98 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17896.0 | **NEUTRAL** |
| `B04_income` | 3449.0 | **GOOD** |
| `B05_profit_margin` | 19.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.07000000000000028 (now=36.28 vs prior_export=36.35 on finviz_2026-09-08) | **BAD** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-09-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.43 | **GOOD** |
| `B13_short_float` | 2.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.37 (this export) | prior_export=15.37 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.98 (this export) | prior_export=5.98 (finviz_2026-09-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-09_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.