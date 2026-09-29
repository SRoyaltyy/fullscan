# A+B1 Feature Checklist — 2026-09-29

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,662** names
- Export: `finviz_2026-09-29.csv` · prior export for Δ: `2026-09-28`
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
| 1 | FPI | +16 | 16 | 0 | 2026-09-25→2026-09-28 | REIT - Specialty |
| 2 | AVPT | +16 | 18 | 2 | 2026-09-25→2026-09-28 | Software - Infrastructure |
| 3 | WAY | +16 | 17 | 1 | 2026-09-25→2026-09-28 | Health Information Services |
| 4 | ERO | +16 | 16 | 0 | 2026-09-25→2026-09-28 | Copper |
| 5 | MSFT | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Software - Infrastructure |
| 6 | FUTU | +15 | 15 | 0 | 2026-09-25→2026-09-28 | Capital Markets |
| 7 | RTX | +15 | 17 | 2 | 2026-09-25→2026-09-28 | Aerospace & Defense |
| 8 | SMFG | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Banks - Diversified |
| 9 | FCX | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Copper |
| 10 | PM | +15 | 15 | 0 | 2026-09-25→2026-09-28 | Tobacco |
| 11 | MDB | +15 | 18 | 3 | 2026-09-25→2026-09-28 | Software - Infrastructure |
| 12 | TTEK | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Engineering & Construction |
| 13 | CON | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Medical Care Facilities |
| 14 | BIIB | +15 | 17 | 2 | 2026-09-25→2026-09-28 | Drug Manufacturers - General |
| 15 | DGX | +15 | 16 | 1 | 2026-09-25→2026-09-28 | Diagnostics & Research |

## Full checklist — top 15

### FPI  ·  score **+16**  ·  REIT - Specialty
price=10.739999771118164  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.22 on 2026-09-28; prev RSI=61.65 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.65@2026-09-25 → 56.22@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.65@2026-09-25 → 56.22@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.65@2026-09-25 → 56.22@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=1.556 (G=0.1400 R=0.0900); 2026-09-25:GREEN:O=10.7200,C=10.8600,body=+0.1400,vol=733652.0; 2026-09-28:RED:O=10.8300,C=10.7400,body=-0.0900,vol=478889.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.532 (Gvol=733652 Rvol=478889); 2026-09-25:GREEN:O=10.7200,C=10.8600,body=+0.1400,vol=733652.0; 2026-09-28:RED:O=10.8300,C=10.7400,body=-0.0900,vol=478889.0 | **GOOD** |
| `A07_rvol` | RVOL=0.770 on 2026-09-28: today_vol=478889 / avg20=621608 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.056 on 2026-09-28 (price=10.7400, mid=10.7210, upper=11.0591, lower=10.3829; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=10.7400 vs SMA50=10.1922 dist=+5.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=10.7210 SMA50=10.1922 SMA80=10.0305 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-28 (63 bars); S1[2026-06-26→2026-07-29] low=2026-07-09@9.4700; S2[2026-07-30→2026-08-27] low=2026-07-30@9.2100; S3[2026-08-28→2026-09-28] low=2026-08-31@10.1300 | lows=[9.470000267028809, 9.210000038146973, 10.130000114440918] span=9.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.583331346509946 wick_frac=0.41666865349005394 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.4864862078386696 wick_frac=0.5135137921613304 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5555461365659307 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-0.0800:wick=0.0950; 2026-09-23:RED:body=-0.0700:wick=0.1000; 2026-09-24:RED:body=-0.1400:wick=0.1790; 2026-09-25:GREEN:body=+0.1400:wick=0.1000; 2026-09-28:RED:body=-0.0900:wick=0.0950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=366.67 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 51.47 | **NEUTRAL** |
| `B04_income` | 24.17 | **GOOD** |
| `B05_profit_margin` | 46.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.5 vs prior_export=10.5 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 9.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=366.67 (this export) | prior_export=366.67 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+16**  ·  Software - Infrastructure
price=13.670000076293945  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.56 on 2026-09-28; prev RSI=46.14 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.14@2026-09-25 → 56.56@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.14@2026-09-25 → 56.56@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.14@2026-09-25 → 56.56@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=5.800 (G=0.8700 R=0.1500); 2026-09-25:RED:O=13.1500,C=13.0000,body=-0.1500,vol=2590498.0; 2026-09-28:GREEN:O=12.8000,C=13.6700,body=+0.8700,vol=2682870.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.036 (Gvol=2682870 Rvol=2590498); 2026-09-25:RED:O=13.1500,C=13.0000,body=-0.1500,vol=2590498.0; 2026-09-28:GREEN:O=12.8000,C=13.6700,body=+0.8700,vol=2682870.0 | **GOOD** |
| `A07_rvol` | RVOL=1.583 on 2026-09-28: today_vol=2682870 / avg20=1695145 (avg window 2026-08-28→2026-09-25, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.577 on 2026-09-28 (price=13.6700, mid=13.3045, upper=13.9377, lower=12.6713; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=13.6700 vs SMA50=13.1909 dist=+3.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=13.3045 SMA50=13.1909 SMA80=12.4960 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-29→2026-09-28 (63 bars); S1[2026-06-29→2026-07-28] low=2026-06-30@10.8500; S2[2026-07-29→2026-08-27] low=2026-08-07@12.4100; S3[2026-08-28→2026-09-28] low=2026-09-11@12.6100 | lows=[10.850000381469727, 12.40999984741211, 12.609999656677246] span=16.22% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.828571921627896 wick_frac=0.171428078372104 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.5999984741210938 wick_frac=0.40000152587890625 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.800013987258879 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-0.4200:wick=0.3000; 2026-09-23:RED:body=-0.0200:wick=0.2950; 2026-09-24:GREEN:body=+0.0800:wick=0.2000; 2026-09-25:RED:body=-0.1500:wick=0.1000; 2026-09-28:GREEN:body=+0.8700:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=101.09 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466.15 | **NEUTRAL** |
| `B04_income` | 71.48 | **GOOD** |
| `B05_profit_margin` | 15.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.18 vs prior=-0.18 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.33 | **GOOD** |
| `B13_short_float` | 7.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=101.09 (this export) | prior_export=101.09 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+16**  ·  Health Information Services
price=25.739999771118164  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.25 on 2026-09-28; prev RSI=48.23 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.23@2026-09-25 → 55.25@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.23@2026-09-25 → 55.25@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.23@2026-09-25 → 55.25@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.3500 R=0.0000); 2026-09-25:GREEN:O=24.3900,C=24.7000,body=+0.3100,vol=1260740.0; 2026-09-28:GREEN:O=24.7000,C=25.7400,body=+1.0400,vol=1928569.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3189309 Rvol=0); 2026-09-25:GREEN:O=24.3900,C=24.7000,body=+0.3100,vol=1260740.0; 2026-09-28:GREEN:O=24.7000,C=25.7400,body=+1.0400,vol=1928569.0 | **GOOD** |
| `A07_rvol` | RVOL=0.946 on 2026-09-28: today_vol=1928569 / avg20=2037792 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.239 on 2026-09-28 (price=25.7400, mid=25.2475, upper=27.3089, lower=23.1861; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=25.7400 vs SMA50=24.4251 dist=+5.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=25.2475 SMA50=24.4251 SMA80=22.8974 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-28 (63 bars); S1[2026-06-25→2026-07-28] low=2026-06-25@18.7300; S2[2026-07-29→2026-08-27] low=2026-07-30@20.1410; S3[2026-08-28→2026-09-28] low=2026-09-10@22.9550 | lows=[18.729999542236328, 20.141000747680664, 22.954999923706055] span=22.56% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.7781556229911241 wick_frac=0.22184437700887594 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-0.3500:wick=0.5910; 2026-09-23:RED:body=-0.7500:wick=0.6390; 2026-09-24:RED:body=-0.1200:wick=0.9500; 2026-09-25:GREEN:body=+0.3100:wick=0.0950; 2026-09-28:GREEN:body=+1.0400:wick=0.2750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.68 vs prior_export=33.68 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ERO  ·  score **+16**  ·  Copper
price=37.40999984741211  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.22 on 2026-09-28; prev RSI=58.98 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.98@2026-09-25 → 57.22@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.98@2026-09-25 → 57.22@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.98@2026-09-25 → 57.22@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0750 R=0.0000); 2026-09-25:GREEN:O=37.0600,C=37.8700,body=+0.8100,vol=783798.0; 2026-09-28:GREEN:O=36.1450,C=37.4100,body=+1.2650,vol=1153021.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1936819 Rvol=0); 2026-09-25:GREEN:O=37.0600,C=37.8700,body=+0.8100,vol=783798.0; 2026-09-28:GREEN:O=36.1450,C=37.4100,body=+1.2650,vol=1153021.0 | **GOOD** |
| `A07_rvol` | RVOL=0.830 on 2026-09-28: today_vol=1153021 / avg20=1389790 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.518 on 2026-09-28 (price=37.4100, mid=35.5170, upper=39.1749, lower=31.8591; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=37.4100 vs SMA50=33.4194 dist=+11.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=35.5170 SMA50=33.4194 SMA80=31.1448 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-28 (63 bars); S1[2026-06-26→2026-07-28] low=2026-07-08@22.9320; S2[2026-07-29→2026-08-27] low=2026-07-29@24.7020; S3[2026-08-28→2026-09-28] low=2026-09-16@31.6400 | lows=[22.93199920654297, 24.70199966430664, 31.639999389648438] span=37.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5646428785115041 wick_frac=0.4353571214884958 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-22:GREEN:body=+1.8200:wick=0.3450; 2026-09-23:GREEN:body=+0.2300:wick=1.3000; 2026-09-24:GREEN:body=+1.3500:wick=0.2700; 2026-09-25:GREEN:body=+0.8100:wick=0.5400; 2026-09-28:GREEN:body=+1.2650:wick=1.1250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.05000000000000426 (now=38.2 vs prior_export=38.15 on finviz_2026-09-28) | **GOOD** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.83 | **GOOD** |
| `B13_short_float` | 5.93 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.7 (this export) | prior_export=12.7 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.4 (this export) | prior_export=2.4 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSFT  ·  score **+15**  ·  Software - Infrastructure
price=509.2200012207031  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.57 on 2026-09-28; prev RSI=63.24 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.24@2026-09-25 → 58.57@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.24@2026-09-25 → 58.57@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.24@2026-09-25 → 58.57@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=20.8250 R=0.0000); 2026-09-25:GREEN:O=499.1400,C=516.1700,body=+17.0300,vol=37897460.0; 2026-09-28:GREEN:O=505.4250,C=509.2200,body=+3.7950,vol=19545869.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=57443329 Rvol=0); 2026-09-25:GREEN:O=499.1400,C=516.1700,body=+17.0300,vol=37897460.0; 2026-09-28:GREEN:O=505.4250,C=509.2200,body=+3.7950,vol=19545869.0 | **GOOD** |
| `A07_rvol` | RVOL=0.897 on 2026-09-28: today_vol=19545869 / avg20=21798168 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.690 on 2026-09-28 (price=509.2200, mid=499.8250, upper=513.4506, lower=486.1994; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=509.2200 vs SMA50=478.0652 dist=+6.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=499.8250 SMA50=478.0652 SMA80=444.2153 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-06-30@367.4500; S2[2026-07-30→2026-08-27] low=2026-07-30@432.4400; S3[2026-08-28→2026-09-28] low=2026-09-10@486.0000 | lows=[367.45001220703125, 432.44000244140625, 486.0] span=32.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5560852006943645 wick_frac=0.4439147993056355 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-9.3200:wick=5.5300; 2026-09-23:RED:body=-0.1700:wick=13.3900; 2026-09-24:GREEN:body=+2.8600:wick=4.8200; 2026-09-25:GREEN:body=+17.0300:wick=5.0701; 2026-09-28:GREEN:body=+3.7950:wick=7.3150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.81 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.72 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 331839.0 | **NEUTRAL** |
| `B04_income` | 133749.0 | **GOOD** |
| `B05_profit_margin` | 40.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 571.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=571.36 vs prior_export=571.36 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -0.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.15 vs prior=-0.15 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.92 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.81 (this export) | prior_export=11.81 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.72 (this export) | prior_export=2.72 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FUTU  ·  score **+15**  ·  Capital Markets
price=112.2699966430664  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.78 on 2026-09-28; prev RSI=48.06 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.06@2026-09-25 → 48.78@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 48.06@2026-09-25 → 48.78@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 48.06@2026-09-25 → 48.78@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.1300 R=0.0000); 2026-09-25:GREEN:O=109.6500,C=111.8700,body=+2.2200,vol=679230.0; 2026-09-28:GREEN:O=111.3600,C=112.2700,body=+0.9100,vol=695417.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1374647 Rvol=0); 2026-09-25:GREEN:O=109.6500,C=111.8700,body=+2.2200,vol=679230.0; 2026-09-28:GREEN:O=111.3600,C=112.2700,body=+0.9100,vol=695417.0 | **GOOD** |
| `A07_rvol` | RVOL=0.821 on 2026-09-28: today_vol=695417 / avg20=846802 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.246 on 2026-09-28 (price=112.2700, mid=114.4265, upper=123.1768, lower=105.6762; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=112.2700 vs SMA50=111.2308 dist=+0.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=114.4265 SMA50=111.2308 SMA80=105.6457 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-07-07@91.8600; S2[2026-07-30→2026-08-27] low=2026-07-30@102.0000; S3[2026-08-28→2026-09-28] low=2026-09-24@108.1000 | lows=[91.86000061035156, 102.0, 108.0999984741211] span=17.68% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5145352337517477 wick_frac=0.4854647662482524 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-22:RED:body=-0.6800:wick=2.7820; 2026-09-23:RED:body=-2.8600:wick=0.5870; 2026-09-24:GREEN:body=+0.2600:wick=2.1400; 2026-09-25:GREEN:body=+2.2200:wick=1.1000; 2026-09-28:GREEN:body=+0.9100:wick=1.6150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.72 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.86 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3314.94 | **NEUTRAL** |
| `B04_income` | 1422.92 | **GOOD** |
| `B05_profit_margin` | 42.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=153.21 vs prior_export=153.21 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.31 | **GOOD** |
| `B13_short_float` | 5.06 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.72 (this export) | prior_export=11.72 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.86 (this export) | prior_export=16.86 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RTX  ·  score **+15**  ·  Aerospace & Defense
price=187.66000366210938  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=27.35 on 2026-09-28; prev RSI=29.34 on 2026-09-25 | **GOOD** |
| `A02_rsi_cross_30` | below | RSI 29.34@2026-09-25 → 27.35@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 29.34@2026-09-25 → 27.35@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 29.34@2026-09-25 → 27.35@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=8.312 (G=1.3300 R=0.1600); 2026-09-25:GREEN:O=188.0700,C=189.4000,body=+1.3300,vol=3622938.0; 2026-09-28:RED:O=187.8200,C=187.6600,body=-0.1600,vol=3023638.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.198 (Gvol=3622938 Rvol=3023638); 2026-09-25:GREEN:O=188.0700,C=189.4000,body=+1.3300,vol=3622938.0; 2026-09-28:RED:O=187.8200,C=187.6600,body=-0.1600,vol=3023638.0 | **GOOD** |
| `A07_rvol` | RVOL=0.777 on 2026-09-28: today_vol=3023638 / avg20=3893757 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.815 on 2026-09-28 (price=187.6600, mid=196.3575, upper=207.0341, lower=185.6809; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-09-28: price=187.6600 vs SMA50=207.2549 dist=-9.45% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=196.36_50=207.25_80=200.20 on 2026-09-28: SMA20=196.3575 SMA50=207.2549 SMA80=200.2010 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-06-30@186.2114; S2[2026-07-30→2026-08-27] low=2026-08-25@207.4400; S3[2026-08-28→2026-09-28] low=2026-09-28@186.8100 | lows=[186.2114145180241, 207.44000244140625, 186.80999755859375] span=11.40% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.6683331160814925 wick_frac=0.33166688391850757 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.05755529941270102 wick_frac=0.942444700587299 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.312225824909403 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-3.7500:wick=4.0800; 2026-09-23:GREEN:body=+0.3100:wick=4.0600; 2026-09-24:RED:body=-3.3100:wick=1.4600; 2026-09-25:GREEN:body=+1.3300:wick=0.6600; 2026-09-28:RED:body=-0.1600:wick=2.6200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=13.9 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.93 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 93500.0 | **NEUTRAL** |
| `B04_income` | 7738.0 | **GOOD** |
| `B05_profit_margin` | 8.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 236.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.29000000000002046 (now=236.43 vs prior_export=236.14 on finviz_2026-09-28) | **GOOD** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -5.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.52 vs prior=-5.52 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.9 (this export) | prior_export=13.9 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.93 (this export) | prior_export=7.93 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SMFG  ·  score **+15**  ·  Banks - Diversified
price=26.43000030517578  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.91 on 2026-09-28; prev RSI=53.52 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.52@2026-09-25 → 51.91@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.52@2026-09-25 → 51.91@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.52@2026-09-25 → 51.91@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=0.778 (G=0.1400 R=0.1800); 2026-09-25:GREEN:O=26.4700,C=26.6100,body=+0.1400,vol=1923690.0; 2026-09-28:RED:O=26.6100,C=26.4300,body=-0.1800,vol=1335863.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.440 (Gvol=1923690 Rvol=1335863); 2026-09-25:GREEN:O=26.4700,C=26.6100,body=+0.1400,vol=1923690.0; 2026-09-28:RED:O=26.6100,C=26.4300,body=-0.1800,vol=1335863.0 | **GOOD** |
| `A07_rvol` | RVOL=0.947 on 2026-09-28: today_vol=1335863 / avg20=1411314 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.105 on 2026-09-28 (price=26.4300, mid=26.5435, upper=27.6236, lower=25.4634; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=26.4300 vs SMA50=25.9304 dist=+1.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=26.5435 SMA50=25.9304 SMA80=25.3351 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-28 (63 bars); S1[2026-06-26→2026-07-28] low=2026-06-29@23.5000; S2[2026-07-29→2026-08-27] low=2026-08-20@24.3200; S3[2026-08-28→2026-09-28] low=2026-09-24@25.2900 | lows=[23.5, 24.31999969482422, 25.290000915527344] span=7.62% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.500003405971349 wick_frac=0.49999659402865104 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.46153770913519143 wick_frac=0.5384622908648086 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=0.7777836646462933 need>1.4; red_wick_gt_green=False 5d trail=2026-09-22:RED:body=-0.4000:wick=0.1000; 2026-09-23:RED:body=-0.1300:wick=0.0700; 2026-09-24:GREEN:body=+0.0700:wick=0.2200; 2026-09-25:GREEN:body=+0.1400:wick=0.1400; 2026-09-28:RED:body=-0.1800:wick=0.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.18 (current export asof; earnings_date=7/31/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.82 (current export; earnings_date=7/31/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 67685.69 | **NEUTRAL** |
| `B04_income` | 11115.15 | **GOOD** |
| `B05_profit_margin` | 16.42 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 29.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=15.139999999999999 (now=29.08 vs prior_export=13.94 on finviz_2026-09-28) | **GOOD** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 0.12 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.18 (this export) | prior_export=18.18 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.82 (this export) | prior_export=10.82 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FCX  ·  score **+15**  ·  Copper
price=71.95999908447266  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.52 on 2026-09-28; prev RSI=51.52 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.52@2026-09-25 → 50.52@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.52@2026-09-25 → 50.52@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.52@2026-09-25 → 50.52@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.6550 R=0.0000); 2026-09-25:GREEN:O=71.8600,C=72.3100,body=+0.4500,vol=7496080.0; 2026-09-28:GREEN:O=69.7550,C=71.9600,body=+2.2050,vol=8832195.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=16328275 Rvol=0); 2026-09-25:GREEN:O=71.8600,C=72.3100,body=+0.4500,vol=7496080.0; 2026-09-28:GREEN:O=69.7550,C=71.9600,body=+2.2050,vol=8832195.0 | **GOOD** |
| `A07_rvol` | RVOL=0.751 on 2026-09-28: today_vol=8832195 / avg20=11762584 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.108 on 2026-09-28 (price=71.9600, mid=72.4175, upper=76.6502, lower=68.1848; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=71.9600 vs SMA50=69.8904 dist=+2.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=72.4175 SMA50=69.8904 SMA80=67.4104 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-07-08@55.8644; S2[2026-07-30→2026-08-27] low=2026-07-31@60.9700; S3[2026-08-28→2026-09-28] low=2026-09-14@67.0100 | lows=[55.86440695057745, 60.970001220703125, 67.01000213623047] span=19.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5307652982622193 wick_frac=0.4692347017377807 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:GREEN:body=+1.7000:wick=1.1000; 2026-09-23:RED:body=-0.3500:wick=1.3300; 2026-09-24:GREEN:body=+0.5300:wick=1.2000; 2026-09-25:GREEN:body=+0.4500:wick=0.8899; 2026-09-28:GREEN:body=+2.2050:wick=0.8335 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.25 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.71 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 25156.0 | **NEUTRAL** |
| `B04_income` | 2923.0 | **GOOD** |
| `B05_profit_margin` | 11.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 74.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=74.73 vs prior_export=74.73 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -1.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.43 vs prior=-1.43 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.97 | **GOOD** |
| `B13_short_float` | 2.4 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.25 (this export) | prior_export=20.25 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+15**  ·  Tobacco
price=193.75999450683594  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.40 on 2026-09-28; prev RSI=51.74 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.74@2026-09-25 → 57.40@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.74@2026-09-25 → 57.40@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.74@2026-09-25 → 57.40@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=45.709 (G=3.2000 R=0.0700); 2026-09-25:RED:O=190.5500,C=190.4800,body=-0.0700,vol=3443380.0; 2026-09-28:GREEN:O=190.5600,C=193.7600,body=+3.2000,vol=3898095.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.132 (Gvol=3898095 Rvol=3443380); 2026-09-25:RED:O=190.5500,C=190.4800,body=-0.0700,vol=3443380.0; 2026-09-28:GREEN:O=190.5600,C=193.7600,body=+3.2000,vol=3898095.0 | **GOOD** |
| `A07_rvol` | RVOL=0.887 on 2026-09-28: today_vol=3898095 / avg20=4393099 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.688 on 2026-09-28 (price=193.7600, mid=189.3215, upper=195.7708, lower=182.8722; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=193.7600 vs SMA50=190.0990 dist=+1.93% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=189.32_50=190.10_80=186.50 on 2026-09-28: SMA20=189.3215 SMA50=190.0990 SMA80=186.5025 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-07-15@175.7600; S2[2026-07-30→2026-08-27] low=2026-08-12@184.0200; S3[2026-08-28→2026-09-28] low=2026-09-08@181.0100 | lows=[175.75999450683594, 184.02000427246094, 181.00999450683594] span=4.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.7390290057828319 wick_frac=0.2609709942171681 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.019034027264957974 wick_frac=0.980965972735042 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=45.70945945945946 need>1.4; red_wick_gt_green=False 5d trail=2026-09-22:GREEN:body=+1.7700:wick=3.4800; 2026-09-23:GREEN:body=+0.7900:wick=3.6600; 2026-09-24:RED:body=-0.9100:wick=2.6600; 2026-09-25:RED:body=-0.0700:wick=3.6080; 2026-09-28:GREEN:body=+3.2000:wick=1.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDB  ·  score **+15**  ·  Software - Infrastructure
price=334.67999267578125  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=37.00 on 2026-09-28; prev RSI=53.68 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.68@2026-09-25 → 37.00@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_down | RSI 53.68@2026-09-25 → 37.00@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **BAD** |
| `A04_rsi_cross_70` | below | RSI 53.68@2026-09-25 → 37.00@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=3.274 (G=23.4440 R=7.1600); 2026-09-25:RED:O=417.6000,C=410.4400,body=-7.1600,vol=1352663.0; 2026-09-28:GREEN:O=311.2360,C=334.6800,body=+23.4440,vol=17806336.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=13.164 (Gvol=17806336 Rvol=1352663); 2026-09-25:RED:O=417.6000,C=410.4400,body=-7.1600,vol=1352663.0; 2026-09-28:GREEN:O=311.2360,C=334.6800,body=+23.4440,vol=17806336.0 | **GOOD** |
| `A07_rvol` | RVOL=8.773 on 2026-09-28: today_vol=17806336 / avg20=2029673 (avg window 2026-08-28→2026-09-25, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.908 on 2026-09-28 (price=334.6800, mid=390.4890, upper=451.9730, lower=329.0050; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-09-28: price=334.6800 vs SMA50=386.4666 dist=-13.40% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=390.4890 SMA50=386.4666 SMA80=369.2777 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-30→2026-09-28 (63 bars); S1[2026-06-30→2026-07-29] low=2026-07-24@291.7900; S2[2026-07-30→2026-08-27] low=2026-07-30@318.1700; S3[2026-08-28→2026-09-28] low=2026-09-28@300.0000 | lows=[291.7900085449219, 318.1700134277344, 300.0] span=9.04% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5291907623951642 wick_frac=0.4708092376048358 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.30236511303621644 wick_frac=0.6976348869637835 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.274300035376504 need>1.4; red_wick_gt_green=False 5d trail=2026-09-22:GREEN:body=+18.3800:wick=14.7100; 2026-09-23:RED:body=-0.9800:wick=13.1580; 2026-09-24:RED:body=-1.6400:wick=8.9700; 2026-09-25:RED:body=-7.1600:wick=16.5200; 2026-09-28:GREEN:body=+23.4440:wick=20.8576 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=17.62 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2782.77 | **NEUTRAL** |
| `B04_income` | 58.9 | **GOOD** |
| `B05_profit_margin` | 2.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 470.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.160000000000025 (now=470.69 vs prior_export=467.53 on finviz_2026-09-28) | **GOOD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -5.72 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.72 vs prior=-5.72 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.29 | **GOOD** |
| `B13_short_float` | 3.71 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.62 (this export) | prior_export=17.62 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.0 (this export) | prior_export=5.0 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTEK  ·  score **+15**  ·  Engineering & Construction
price=33.88999938964844  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=36.46 on 2026-09-28; prev RSI=36.59 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 36.59@2026-09-25 → 36.46@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 36.59@2026-09-25 → 36.46@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 36.59@2026-09-25 → 36.46@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7300 R=0.0000); 2026-09-25:GREEN:O=33.4900,C=33.9100,body=+0.4200,vol=2047447.0; 2026-09-28:GREEN:O=33.5800,C=33.8900,body=+0.3100,vol=1914386.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3961833 Rvol=0); 2026-09-25:GREEN:O=33.4900,C=33.9100,body=+0.4200,vol=2047447.0; 2026-09-28:GREEN:O=33.5800,C=33.8900,body=+0.3100,vol=1914386.0 | **GOOD** |
| `A07_rvol` | RVOL=0.911 on 2026-09-28: today_vol=1914386 / avg20=2102417 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.968 on 2026-09-28 (price=33.8900, mid=35.6400, upper=37.4484, lower=33.8316; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-09-28: price=33.8900 vs SMA50=34.9741 dist=-3.10% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=35.6400 SMA50=34.9741 SMA80=32.6550 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-28 (63 bars); S1[2026-06-25→2026-07-28] low=2026-06-30@28.0630; S2[2026-07-29→2026-08-27] low=2026-07-30@31.9750; S3[2026-08-28→2026-09-28] low=2026-09-28@33.2600 | lows=[28.062968775059517, 31.975017503782613, 33.2599983215332] span=18.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5537121441663935 wick_frac=0.44628785583360653 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:RED:body=-0.5100:wick=0.6000; 2026-09-23:RED:body=-0.5300:wick=0.7400; 2026-09-24:RED:body=-1.1100:wick=1.2700; 2026-09-25:GREEN:body=+0.4200:wick=0.1851; 2026-09-28:GREEN:body=+0.3100:wick=0.4400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5069.48 | **NEUTRAL** |
| `B04_income` | 435.98 | **GOOD** |
| `B05_profit_margin` | 8.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.33 vs prior_export=40.33 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.71 | **GOOD** |
| `B13_short_float` | 4.35 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CON  ·  score **+15**  ·  Medical Care Facilities
price=35.36000061035156  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.02 on 2026-09-28; prev RSI=55.18 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.18@2026-09-25 → 58.02@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.18@2026-09-25 → 58.02@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.18@2026-09-25 → 58.02@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2800 R=0.0000); 2026-09-25:GREEN:O=34.3200,C=35.0100,body=+0.6900,vol=568062.0; 2026-09-28:GREEN:O=34.7700,C=35.3600,body=+0.5900,vol=587780.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1155842 Rvol=0); 2026-09-25:GREEN:O=34.3200,C=35.0100,body=+0.6900,vol=568062.0; 2026-09-28:GREEN:O=34.7700,C=35.3600,body=+0.5900,vol=587780.0 | **GOOD** |
| `A07_rvol` | RVOL=0.616 on 2026-09-28: today_vol=587780 / avg20=954418 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.385 on 2026-09-28 (price=35.3600, mid=34.8060, upper=36.2453, lower=33.3667; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=35.3600 vs SMA50=33.5984 dist=+5.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=34.8060 SMA50=33.5984 SMA80=31.5374 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-22→2026-09-28 (63 bars); S1[2026-06-22→2026-07-23] low=2026-06-23@28.0800; S2[2026-07-24→2026-08-27] low=2026-08-06@30.1250; S3[2026-08-28→2026-09-28] low=2026-09-09@33.3200 | lows=[28.079999923706055, 30.125, 33.31999969482422] span=18.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.8570526323519838 wick_frac=0.14294736764801622 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:GREEN:body=+0.4000:wick=0.5600; 2026-09-23:RED:body=-0.0600:wick=0.6150; 2026-09-24:GREEN:body=+0.0100:wick=0.7800; 2026-09-25:GREEN:body=+0.6900:wick=0.0100; 2026-09-28:GREEN:body=+0.5900:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.34 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.41 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2287.47 | **NEUTRAL** |
| `B04_income` | 195.03 | **GOOD** |
| `B05_profit_margin` | 8.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.0 vs prior_export=41.0 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -4.09 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.09 vs prior=-4.09 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 2.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.34 (this export) | prior_export=23.34 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.41 (this export) | prior_export=2.41 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BIIB  ·  score **+15**  ·  Drug Manufacturers - General
price=228.69000244140625  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.77 on 2026-09-28; prev RSI=62.69 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.69@2026-09-25 → 63.77@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.69@2026-09-25 → 63.77@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.69@2026-09-25 → 63.77@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=4.209 (G=2.8200 R=0.6700); 2026-09-25:RED:O=228.2700,C=227.6000,body=-0.6700,vol=939192.0; 2026-09-28:GREEN:O=225.8700,C=228.6900,body=+2.8200,vol=1313729.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.399 (Gvol=1313729 Rvol=939192); 2026-09-25:RED:O=228.2700,C=227.6000,body=-0.6700,vol=939192.0; 2026-09-28:GREEN:O=225.8700,C=228.6900,body=+2.8200,vol=1313729.0 | **GOOD** |
| `A07_rvol` | RVOL=1.404 on 2026-09-28: today_vol=1313729 / avg20=935720 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.835 on 2026-09-28 (price=228.6900, mid=219.7605, upper=230.4606, lower=209.0604; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-28: price=228.6900 vs SMA50=213.5774 dist=+7.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=219.7605 SMA50=213.5774 SMA80=208.8718 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-28 (63 bars); S1[2026-06-25→2026-07-28] low=2026-07-15@186.0300; S2[2026-07-29→2026-08-27] low=2026-08-03@199.5800; S3[2026-08-28→2026-09-28] low=2026-09-10@208.9300 | lows=[186.02999877929688, 199.5800018310547, 208.92999267578125] span=12.31% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.5662652817354537 wick_frac=0.43373471826454635 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.12984451423265497 wick_frac=0.870155485767345 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.208977658338837 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:GREEN:body=+6.1400:wick=1.2900; 2026-09-23:GREEN:body=+0.5900:wick=6.2400; 2026-09-24:RED:body=-0.4400:wick=3.9700; 2026-09-25:RED:body=-0.6700:wick=4.4900; 2026-09-28:GREEN:body=+2.8200:wick=2.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.49 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.2 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9661.4 | **NEUTRAL** |
| `B04_income` | 834.6 | **GOOD** |
| `B05_profit_margin` | 8.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 239.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=239.28 vs prior_export=239.28 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -0.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.11 vs prior=-0.11 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.12 | **GOOD** |
| `B13_short_float` | 3.97 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.49 (this export) | prior_export=22.49 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.2 (this export) | prior_export=11.2 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DGX  ·  score **+15**  ·  Diagnostics & Research
price=237.6199951171875  pair=`2026-09-25→2026-09-28`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.37 on 2026-09-28; prev RSI=47.44 on 2026-09-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.44@2026-09-25 → 49.37@2026-09-28 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.44@2026-09-25 → 49.37@2026-09-28 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.44@2026-09-25 → 49.37@2026-09-28 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_body_sum/RED_body_sum=67.016 (G=3.3500 R=0.0500); 2026-09-25:RED:O=236.3700,C=236.3200,body=-0.0500,vol=739378.0; 2026-09-28:GREEN:O=234.2700,C=237.6200,body=+3.3500,vol=1030759.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-25 + 2026-09-28; ratio=GREEN_vol/RED_vol=1.394 (Gvol=1030759 Rvol=739378); 2026-09-25:RED:O=236.3700,C=236.3200,body=-0.0500,vol=739378.0; 2026-09-28:GREEN:O=234.2700,C=237.6200,body=+3.3500,vol=1030759.0 | **GOOD** |
| `A07_rvol` | RVOL=0.935 on 2026-09-28: today_vol=1030759 / avg20=1101934 (avg window 2026-08-28→2026-09-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.138 on 2026-09-28 (price=237.6200, mid=239.0050, upper=249.0662, lower=228.9438; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-28: price=237.6200 vs SMA50=235.6870 dist=+0.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-28: SMA20=239.0050 SMA50=235.6870 SMA80=223.4603 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-28 (63 bars); S1[2026-06-25→2026-07-28] low=2026-07-16@201.0000; S2[2026-07-29→2026-08-27] low=2026-07-30@229.0100; S3[2026-08-28→2026-09-28] low=2026-09-22@227.2400 | lows=[201.0, 229.00999450683594, 227.24000549316406] span=13.94% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: GREEN body_frac=0.7801744816190188 wick_frac=0.21982551838098116 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-25+2026-09-28: RED body_frac=0.019564750005972145 wick_frac=0.9804352499940279 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=67.01617826617827 need>1.4; red_wick_gt_green=True 5d trail=2026-09-22:GREEN:body=+6.1700:wick=3.8800; 2026-09-23:RED:body=-2.1200:wick=4.5800; 2026-09-24:GREEN:body=+2.5100:wick=1.2300; 2026-09-25:RED:body=-0.0500:wick=2.5050; 2026-09-28:GREEN:body=+3.3500:wick=0.9439 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.36 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11560.0 | **NEUTRAL** |
| `B04_income` | 1058.0 | **GOOD** |
| `B05_profit_margin` | 9.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 250.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=250.86 vs prior_export=250.86 on finviz_2026-09-28) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | -9.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.0 vs prior=-9.0 on finviz_2026-09-28) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.95 | **GOOD** |
| `B13_short_float` | 3.28 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.36 (this export) | prior_export=2.36 (finviz_2026-09-28) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-29_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.