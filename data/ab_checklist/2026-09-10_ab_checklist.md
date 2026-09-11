# A+B1 Feature Checklist — 2026-09-10

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,654** names
- Export: `finviz_2026-09-10.csv` · prior export for Δ: `2026-09-09`
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
| 1 | AVO | +19 | 19 | 0 | 2026-09-04→2026-09-08 | Food Distribution |
| 2 | CFFN | +17 | 17 | 0 | 2026-09-04→2026-09-08 | Banks - Regional |
| 3 | SKM | +16 | 16 | 0 | 2026-09-04→2026-09-08 | Telecom Services |
| 4 | KO | +16 | 17 | 1 | 2026-09-04→2026-09-08 | Beverages - Non-Alcoholic |
| 5 | HSBC | +16 | 17 | 1 | 2026-09-04→2026-09-08 | Banks - Diversified |
| 6 | KMI | +16 | 17 | 1 | 2026-09-04→2026-09-08 | Oil & Gas Midstream |
| 7 | FCF | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Banks - Regional |
| 8 | SR | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Utilities - Regulated Gas |
| 9 | ALB | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Specialty Chemicals |
| 10 | RPRX | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Biotechnology |
| 11 | HRI | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Rental & Leasing Services |
| 12 | PBF | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Oil & Gas Refining & Marketing |
| 13 | HASI | +15 | 15 | 0 | 2026-09-04→2026-09-08 | Asset Management |
| 14 | NWBI | +15 | 16 | 1 | 2026-09-04→2026-09-08 | Banks - Regional |
| 15 | SOBO | +15 | 17 | 2 | 2026-09-04→2026-09-08 | Oil & Gas Midstream |

## Full checklist — top 15

### AVO  ·  score **+19**  ·  Food Distribution
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
| `B08_target_price_delta` | delta=0.0 (now=16.5 vs prior_export=16.5 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 11.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.08999999999999986 (now=11.36 vs prior=11.27 on finviz_2026-09-09) | **GOOD** |
| `B12_institutional_transactions` | 18.52 | **GOOD** |
| `B13_short_float` | 9.3 | **NEUTRAL** |
| `B14_earnings_date` | 9/8/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=56.52 (this export) | prior_export=56.52 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=22.42 (this export) | prior_export=22.42 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=9.5 vs prior_export=9.5 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 5.66 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.76 (this export) | prior_export=11.76 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+16**  ·  Telecom Services
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
| `B08_target_price_delta` | delta=0.0 (now=43.92 vs prior_export=43.92 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 0.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KO  ·  score **+16**  ·  Beverages - Non-Alcoholic
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
| `B08_target_price_delta` | delta=0.0 (now=96.24 vs prior_export=96.24 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | -6.35 | **BAD** |
| `B11_insider_tx_delta` | delta=0.07000000000000028 (now=-6.35 vs prior=-6.42 on finviz_2026-09-09) | **GOOD** |
| `B12_institutional_transactions` | 0.09 | **GOOD** |
| `B13_short_float` | 0.92 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.07 (this export) | prior_export=4.07 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=108.62 vs prior_export=108.62 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.21 | **GOOD** |
| `B13_short_float` | 0.15 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.31 (this export) | prior_export=3.31 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KMI  ·  score **+16**  ·  Oil & Gas Midstream
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
| `B08_target_price_delta` | delta=0.0 (now=36.28 vs prior_export=36.28 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.43 | **GOOD** |
| `B13_short_float` | 2.51 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.37 (this export) | prior_export=15.37 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.98 (this export) | prior_export=5.98 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FCF  ·  score **+15**  ·  Banks - Regional
price=21.100000381469727  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.99 on 2026-09-08; prev RSI=57.52 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.52@2026-09-04 → 51.99@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.52@2026-09-04 → 51.99@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.52@2026-09-04 → 51.99@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.818 (G=0.2000 R=0.1100); 2026-09-04:GREEN:O=21.1400,C=21.3400,body=+0.2000,vol=746900.0; 2026-09-08:RED:O=21.2100,C=21.1000,body=-0.1100,vol=918400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.813 (Gvol=746900 Rvol=918400); 2026-09-04:GREEN:O=21.1400,C=21.3400,body=+0.2000,vol=746900.0; 2026-09-08:RED:O=21.2100,C=21.1000,body=-0.1100,vol=918400.0 | **BAD** |
| `A07_rvol` | RVOL=1.390 on 2026-09-08: today_vol=918400 / avg20=660495 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.046 on 2026-09-08 (price=21.1000, mid=21.1310, upper=21.8088, lower=20.4532; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=21.1000 vs SMA50=20.9001 dist=+0.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=21.1310 SMA50=20.9001 SMA80=20.1287 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-04@18.7270; S2[2026-07-07→2026-08-07] low=2026-07-08@19.7204; S3[2026-08-10→2026-09-08] low=2026-09-01@20.4700 | lows=[18.726969140133388, 19.720443433739185, 20.469999313354492] span=9.31% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.6896556260029991 wick_frac=0.3103443739970009 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.4230715621905146 wick_frac=0.5769284378094853 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8182101922976885 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.0900:wick=0.2400; 2026-09-02:GREEN:body=+0.3000:wick=0.2200; 2026-09-03:DOJI:body=+0.0000:wick=0.2300; 2026-09-04:GREEN:body=+0.2000:wick=0.0900; 2026-09-08:RED:body=-0.1100:wick=0.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.95 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.41 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 743.09 | **NEUTRAL** |
| `B04_income` | 168.34 | **GOOD** |
| `B05_profit_margin` | 22.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.08 vs prior_export=24.08 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | -6.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.040000000000000036 (now=-6.83 vs prior=-6.87 on finviz_2026-09-09) | **GOOD** |
| `B12_institutional_transactions` | 1.2 | **GOOD** |
| `B13_short_float` | 3.29 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.95 (this export) | prior_export=3.95 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.41 (this export) | prior_export=1.41 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=92.38 vs prior_export=92.38 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -0.06 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.06 vs prior=-0.06 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 4.65 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.93 (this export) | prior_export=17.93 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.48 (this export) | prior_export=6.48 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=176.0 vs prior_export=176.0 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | -7.86 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-7.86 vs prior=-7.86 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.45 | **GOOD** |
| `B13_short_float` | 8.19 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.09 (this export) | prior_export=17.09 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.49 (this export) | prior_export=8.49 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RPRX  ·  score **+15**  ·  Biotechnology
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
| `B08_target_price_delta` | delta=0.0 (now=65.71 vs prior_export=65.71 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -1.63 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.63 vs prior=-1.63 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.39 | **GOOD** |
| `B13_short_float` | 2.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.82 (this export) | prior_export=3.82 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.85 (this export) | prior_export=1.85 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HRI  ·  score **+15**  ·  Rental & Leasing Services
price=146.27000427246094  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=42.23 on 2026-09-08; prev RSI=35.13 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 35.13@2026-09-04 → 42.23@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 35.13@2026-09-04 → 42.23@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 35.13@2026-09-04 → 42.23@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.255 (G=4.6700 R=3.7200); 2026-09-04:GREEN:O=136.2500,C=140.9200,body=+4.6700,vol=448100.0; 2026-09-08:RED:O=149.9900,C=146.2700,body=-3.7200,vol=1227400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.365 (Gvol=448100 Rvol=1227400); 2026-09-04:GREEN:O=136.2500,C=140.9200,body=+4.6700,vol=448100.0; 2026-09-08:RED:O=149.9900,C=146.2700,body=-3.7200,vol=1227400.0 | **BAD** |
| `A07_rvol` | RVOL=3.420 on 2026-09-08: today_vol=1227400 / avg20=358840 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.469 on 2026-09-08 (price=146.2700, mid=157.3525, upper=180.9890, lower=133.7160; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-09-08: price=146.2700 vs SMA50=153.9094 dist=-4.96% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=157.3525 SMA50=153.9094 SMA80=148.4035 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-05→2026-09-08 (63 bars); S1[2026-06-05→2026-07-07] low=2026-07-07@127.2800; S2[2026-07-08→2026-08-07] low=2026-07-08@131.7100; S3[2026-08-10→2026-09-08] low=2026-09-03@135.0700 | lows=[127.27999877929688, 131.7100067138672, 135.07000732421875] span=6.12% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.7262818672178492 wick_frac=0.2737181327821508 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.4217692253130044 wick_frac=0.5782307746869956 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2553754399205888 need>1.4; red_wick_gt_green=False 5d trail=2026-09-01:RED:body=-8.7500:wick=2.8500; 2026-09-02:RED:body=-2.6000:wick=3.9700; 2026-09-03:GREEN:body=+0.5200:wick=6.3800; 2026-09-04:GREEN:body=+4.6700:wick=1.7600; 2026-09-08:RED:body=-3.7200:wick=5.1000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=87.15 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.15 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4856.0 | **NEUTRAL** |
| `B04_income` | 49.0 | **GOOD** |
| `B05_profit_margin` | 1.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 197.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0 (now=197.3 vs prior_export=196.3 on finviz_2026-09-09) | **GOOD** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.43 vs prior=0.43 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.45 | **GOOD** |
| `B13_short_float` | 6.66 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=87.15 (this export) | prior_export=87.15 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.15 (this export) | prior_export=4.15 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBF  ·  score **+15**  ·  Oil & Gas Refining & Marketing
price=76.7699966430664  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.41 on 2026-09-08; prev RSI=59.04 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.04@2026-09-04 → 62.41@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.04@2026-09-04 → 62.41@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.04@2026-09-04 → 62.41@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=1.687 (G=0.2700 R=0.1600); 2026-09-04:RED:O=74.5000,C=74.3400,body=-0.1600,vol=1872500.0; 2026-09-08:GREEN:O=76.5000,C=76.7700,body=+0.2700,vol=2625300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=1.402 (Gvol=2625300 Rvol=1872500); 2026-09-04:RED:O=74.5000,C=74.3400,body=-0.1600,vol=1872500.0; 2026-09-08:GREEN:O=76.5000,C=76.7700,body=+0.2700,vol=2625300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.985 on 2026-09-08: today_vol=2625300 / avg20=2666480 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.754 on 2026-09-08 (price=76.7700, mid=72.3949, upper=78.1984, lower=66.5915; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=76.7700 vs SMA50=64.4303 dist=+19.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=72.3949 SMA50=64.4303 SMA80=55.5520 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-09→2026-09-08 (63 bars); S1[2026-06-09→2026-07-09] low=2026-06-18@36.2554; S2[2026-07-10→2026-08-07] low=2026-07-10@51.5088; S3[2026-08-10→2026-09-08] low=2026-08-10@62.6674 | lows=[36.255422636086145, 51.50880357211937, 62.66738317168613] span=72.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.14438360859063906 wick_frac=0.855616391409361 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.05387326476300079 wick_frac=0.9461267352369992 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6874403967194354 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.0100:wick=3.9200; 2026-09-02:RED:body=-0.4100:wick=2.8200; 2026-09-03:GREEN:body=+0.5800:wick=2.8400; 2026-09-04:RED:body=-0.1600:wick=2.8100; 2026-09-08:GREEN:body=+0.2700:wick=1.6000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=49.93 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.58 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 34373.2 | **NEUTRAL** |
| `B04_income` | 1353.1 | **GOOD** |
| `B05_profit_margin` | 3.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.8500000000000085 (now=69.62 vs prior_export=68.77 on finviz_2026-09-09) | **GOOD** |
| `B09_analyst_recom` | 3.4 | **NEUTRAL** |
| `B10_insider_transactions` | -46.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.020000000000003126 (now=-46.05 vs prior=-46.07 on finviz_2026-09-09) | **GOOD** |
| `B12_institutional_transactions` | 4.91 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=49.93 (this export) | prior_export=49.93 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.58 (this export) | prior_export=21.58 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HASI  ·  score **+15**  ·  Asset Management
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
| `B08_target_price_delta` | delta=0.0 (now=49.33 vs prior_export=49.33 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.26 | **GOOD** |
| `B13_short_float` | 11.0 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.9 (this export) | prior_export=7.9 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+15**  ·  Banks - Regional
price=15.5  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.88 on 2026-09-08; prev RSI=58.54 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.54@2026-09-04 → 53.88@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.54@2026-09-04 → 53.88@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.54@2026-09-04 → 53.88@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=0.1600 R=0.0600); 2026-09-04:GREEN:O=15.4600,C=15.6200,body=+0.1600,vol=688100.0; 2026-09-08:RED:O=15.5600,C=15.5000,body=-0.0600,vol=863700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.797 (Gvol=688100 Rvol=863700); 2026-09-04:GREEN:O=15.4600,C=15.6200,body=+0.1600,vol=688100.0; 2026-09-08:RED:O=15.5600,C=15.5000,body=-0.0600,vol=863700.0 | **BAD** |
| `A07_rvol` | RVOL=1.108 on 2026-09-08: today_vol=863700 / avg20=779375 (avg window 2026-08-10→2026-09-04, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.083 on 2026-09-08 (price=15.5000, mid=15.4685, upper=15.8469, lower=15.0901; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=15.5000 vs SMA50=15.3020 dist=+1.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=15.4685 SMA50=15.3020 SMA80=14.7929 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-04@13.7061; S2[2026-07-07→2026-08-07] low=2026-07-08@14.5257; S3[2026-08-10→2026-09-08] low=2026-09-01@15.0700 | lows=[13.706064819371372, 14.525664138598916, 15.069999694824219] span=9.95% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.6153834867769504 wick_frac=0.38461651322304957 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.42857629427792915 wick_frac=0.5714237057220708 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.666645474052293 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.1200:wick=0.1900; 2026-09-02:GREEN:body=+0.2400:wick=0.0800; 2026-09-03:GREEN:body=+0.0200:wick=0.1700; 2026-09-04:GREEN:body=+0.1600:wick=0.1000; 2026-09-08:RED:body=-0.0600:wick=0.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-09-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.89 | **GOOD** |
| `B13_short_float` | 5.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOBO  ·  score **+15**  ·  Oil & Gas Midstream
price=37.790000915527344  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.69 on 2026-09-08; prev RSI=49.15 on 2026-09-04 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.15@2026-09-04 → 55.69@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.15@2026-09-04 → 55.69@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.15@2026-09-04 → 55.69@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=4.091 (G=0.4500 R=0.1100); 2026-09-04:RED:O=37.1600,C=37.0500,body=-0.1100,vol=1054300.0; 2026-09-08:GREEN:O=37.3400,C=37.7900,body=+0.4500,vol=1021900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=0.969 (Gvol=1021900 Rvol=1054300); 2026-09-04:RED:O=37.1600,C=37.0500,body=-0.1100,vol=1054300.0; 2026-09-08:GREEN:O=37.3400,C=37.7900,body=+0.4500,vol=1021900.0 | **BAD** |
| `A07_rvol` | RVOL=1.981 on 2026-09-08: today_vol=1021900 / avg20=515895 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.463 on 2026-09-08 (price=37.7900, mid=37.2950, upper=38.3633, lower=36.2267; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-08: price=37.7900 vs SMA50=36.7780 dist=+2.75% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=37.2950 SMA50=36.7780 SMA80=36.6282 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-07-02@34.0700; S2[2026-07-07→2026-08-07] low=2026-07-07@34.6700; S3[2026-08-10→2026-09-08] low=2026-08-10@35.4700 | lows=[34.06999969482422, 34.66999816894531, 35.470001220703125] span=4.11% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.737705430622608 wick_frac=0.262294569377392 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2135936712986282 wick_frac=0.7864063287013718 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.0908933277847135 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:RED:body=-0.1900:wick=1.5790; 2026-09-02:RED:body=-0.0900:wick=0.4000; 2026-09-03:RED:body=-0.1500:wick=0.3200; 2026-09-04:RED:body=-0.1100:wick=0.4050; 2026-09-08:GREEN:body=+0.4500:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.84 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.89 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1529.5 | **NEUTRAL** |
| `B04_income` | 434.53 | **GOOD** |
| `B05_profit_margin` | 28.41 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.00999999999999801 (now=36.28 vs prior_export=36.27 on finviz_2026-09-09) | **GOOD** |
| `B09_analyst_recom` | 3.05 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-09) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.24 | **BAD** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.84 (this export) | prior_export=12.84 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.89 (this export) | prior_export=9.89 (finviz_2026-09-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-10_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.