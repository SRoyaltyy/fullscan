# A+B1 Feature Checklist — 2026-09-28

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,661** names
- Export: `finviz_2026-09-28.csv` · prior export for Δ: `2026-09-27`
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
| 1 | FIGS | +17 | 18 | 1 | 2026-09-24→2026-09-25 | Apparel Manufacturing |
| 2 | WAY | +16 | 17 | 1 | 2026-09-24→2026-09-25 | Health Information Services |
| 3 | AAPL | +16 | 17 | 1 | 2026-09-24→2026-09-25 | Consumer Electronics |
| 4 | GCT | +16 | 17 | 1 | 2026-09-24→2026-09-25 | Software - Infrastructure |
| 5 | RGEN | +16 | 18 | 2 | 2026-09-24→2026-09-25 | Medical Instruments & Supplies |
| 6 | AUPH | +16 | 16 | 0 | 2026-09-24→2026-09-25 | Biotechnology |
| 7 | ERO | +16 | 16 | 0 | 2026-09-24→2026-09-25 | Copper |
| 8 | GMAB | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Biotechnology |
| 9 | ICLR | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Diagnostics & Research |
| 10 | AMN | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Medical Care Facilities |
| 11 | CRSR | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Computer Hardware |
| 12 | DGX | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Diagnostics & Research |
| 13 | LLY | +15 | 17 | 2 | 2026-09-24→2026-09-25 | Drug Manufacturers - General |
| 14 | HUM | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Healthcare Plans |
| 15 | SN | +15 | 16 | 1 | 2026-09-24→2026-09-25 | Furnishings, Fixtures & Appliances |

## Full checklist — top 15

### FIGS  ·  score **+17**  ·  Apparel Manufacturing
price=13.420000076293945  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.49 on 2026-09-25; prev RSI=41.36 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.36@2026-09-24 → 50.49@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.36@2026-09-24 → 50.49@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.36@2026-09-24 → 50.49@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.3800 R=0.0000); 2026-09-24:GREEN:O=11.9500,C=12.4900,body=+0.5400,vol=2548900.0; 2026-09-25:GREEN:O=12.5800,C=13.4200,body=+0.8400,vol=4405576.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6954476 Rvol=0); 2026-09-24:GREEN:O=11.9500,C=12.4900,body=+0.5400,vol=2548900.0; 2026-09-25:GREEN:O=12.5800,C=13.4200,body=+0.8400,vol=4405576.0 | **GOOD** |
| `A07_rvol` | RVOL=1.753 on 2026-09-25: today_vol=4405576 / avg20=2513835 (avg window 2026-08-27→2026-09-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.137 on 2026-09-25 (price=13.4200, mid=13.7060, upper=15.7862, lower=11.6258; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=13.4200 vs SMA50=12.9552 dist=+3.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=13.7060 SMA50=12.9552 SMA80=12.2981 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-07-23@9.1700; S2[2026-07-28→2026-08-26] low=2026-07-28@9.7800; S3[2026-08-27→2026-09-25] low=2026-09-24@11.8100 | lows=[9.170000076293945, 9.779999732971191, 11.8100004196167] span=28.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.7974583429687019 wick_frac=0.2025416570312981 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.0700:wick=0.4300; 2026-09-22:RED:body=-0.3600:wick=0.1650; 2026-09-23:RED:body=-0.8300:wick=0.1850; 2026-09-24:GREEN:body=+0.5400:wick=0.2300; 2026-09-25:GREEN:body=+0.8400:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=122.88 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.61 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 710.08 | **NEUTRAL** |
| `B04_income` | 61.92 | **GOOD** |
| `B05_profit_margin` | 8.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.14 vs prior_export=18.14 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | -0.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.83 vs prior=-0.83 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.96 | **GOOD** |
| `B13_short_float` | 11.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=122.88 (this export) | prior_export=122.88 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.61 (this export) | prior_export=5.61 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+16**  ·  Health Information Services
price=24.700000762939453  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.23 on 2026-09-25; prev RSI=47.80 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.80@2026-09-24 → 48.23@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.80@2026-09-24 → 48.23@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.80@2026-09-24 → 48.23@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=2.583 (G=0.3100 R=0.1200); 2026-09-24:RED:O=24.7600,C=24.6400,body=-0.1200,vol=1124700.0; 2026-09-25:GREEN:O=24.3900,C=24.7000,body=+0.3100,vol=1260740.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.121 (Gvol=1260740 Rvol=1124700); 2026-09-24:RED:O=24.7600,C=24.6400,body=-0.1200,vol=1124700.0; 2026-09-25:GREEN:O=24.3900,C=24.7000,body=+0.3100,vol=1260740.0 | **GOOD** |
| `A07_rvol` | RVOL=0.603 on 2026-09-25: today_vol=1260740 / avg20=2089975 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.273 on 2026-09-25 (price=24.7000, mid=25.2725, upper=27.3709, lower=23.1741; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=24.7000 vs SMA50=24.3541 dist=+1.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=25.2725 SMA50=24.3541 SMA80=22.8245 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-24@18.3400; S2[2026-07-28→2026-08-26] low=2026-07-30@20.1410; S3[2026-08-27→2026-09-25] low=2026-09-10@22.9550 | lows=[18.34000015258789, 20.141000747680664, 22.954999923706055] span=25.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.7654377966995705 wick_frac=0.2345622033004295 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.11215034902707366 wick_frac=0.8878496509729263 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5833267106413413 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.2300:wick=0.7000; 2026-09-22:RED:body=-0.3500:wick=0.5910; 2026-09-23:RED:body=-0.7500:wick=0.6390; 2026-09-24:RED:body=-0.1200:wick=0.9500; 2026-09-25:GREEN:body=+0.3100:wick=0.0950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.25 (now=33.68 vs prior_export=33.43 on finviz_2026-09-27) | **GOOD** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+16**  ·  Consumer Electronics
price=341.07000732421875  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.71 on 2026-09-25; prev RSI=61.36 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.36@2026-09-24 → 65.71@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.36@2026-09-24 → 65.71@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.36@2026-09-24 → 65.71@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=6.288 (G=5.0300 R=0.8000); 2026-09-24:RED:O=336.7200,C=335.9200,body=-0.8000,vol=24733100.0; 2026-09-25:GREEN:O=336.0400,C=341.0700,body=+5.0300,vol=29403008.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.189 (Gvol=29403008 Rvol=24733100); 2026-09-24:RED:O=336.7200,C=335.9200,body=-0.8000,vol=24733100.0; 2026-09-25:GREEN:O=336.0400,C=341.0700,body=+5.0300,vol=29403008.0 | **GOOD** |
| `A07_rvol` | RVOL=0.684 on 2026-09-25: today_vol=29403008 / avg20=43013460 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.697 on 2026-09-25 (price=341.0700, mid=329.3960, upper=346.1456, lower=312.6464; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=341.0700 vs SMA50=321.7428 dist=+6.01% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=329.3960 SMA50=321.7428 SMA80=314.3352 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-29→2026-09-25 (63 bars); S1[2026-06-29→2026-07-28] low=2026-06-29@279.6089; S2[2026-07-29→2026-08-26] low=2026-07-31@299.7415; S3[2026-08-27→2026-09-25] low=2026-08-27@309.4000 | lows=[279.6088673150861, 299.7415030462749, 309.3999938964844] span=10.65% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.7044801764374006 wick_frac=0.29551982356259937 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.17353254645474345 wick_frac=0.8264674535452565 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.287594415197986 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:GREEN:body=+3.7000:wick=2.8900; 2026-09-22:RED:body=-0.3900:wick=6.2000; 2026-09-23:RED:body=-4.0600:wick=2.2400; 2026-09-24:RED:body=-0.8000:wick=3.8100; 2026-09-25:GREEN:body=+5.0300:wick=2.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=337.68 vs prior_export=337.68 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.28 vs prior=-2.28 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+16**  ·  Software - Infrastructure
price=53.97999954223633  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.14 on 2026-09-25; prev RSI=61.91 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.91@2026-09-24 → 62.14@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.91@2026-09-24 → 62.14@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.91@2026-09-24 → 62.14@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=79.670 (G=2.3900 R=0.0300); 2026-09-24:GREEN:O=51.5200,C=53.9100,body=+2.3900,vol=713500.0; 2026-09-25:RED:O=54.0100,C=53.9800,body=-0.0300,vol=582218.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.225 (Gvol=713500 Rvol=582218); 2026-09-24:GREEN:O=51.5200,C=53.9100,body=+2.3900,vol=713500.0; 2026-09-25:RED:O=54.0100,C=53.9800,body=-0.0300,vol=582218.0 | **GOOD** |
| `A07_rvol` | RVOL=0.872 on 2026-09-25: today_vol=582218 / avg20=667600 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.611 on 2026-09-25 (price=53.9800, mid=51.9683, upper=55.2599, lower=48.6766; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=53.9800 vs SMA50=48.5145 dist=+11.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=51.9683 SMA50=48.5145 SMA80=42.8418 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-25 (63 bars); S1[2026-06-25→2026-07-27] low=2026-06-30@31.1500; S2[2026-07-28→2026-08-26] low=2026-07-28@38.3000; S3[2026-08-27→2026-09-25] low=2026-09-01@46.0100 | lows=[31.149999618530273, 38.29999923706055, 46.0099983215332] span=47.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.617571072166798 wick_frac=0.3824289278332021 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.012840294752035686 wick_frac=0.9871597052479643 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=79.66988809766022 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.5700:wick=0.5600; 2026-09-22:RED:body=-0.7000:wick=4.6600; 2026-09-23:RED:body=-0.3050:wick=1.7050; 2026-09-24:GREEN:body=+2.3900:wick=1.4800; 2026-09-25:RED:body=-0.0300:wick=2.3063 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.02 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RGEN  ·  score **+16**  ·  Medical Instruments & Supplies
price=189.67999267578125  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.64 on 2026-09-25; prev RSI=69.10 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.10@2026-09-24 → 67.64@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.10@2026-09-24 → 67.64@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.10@2026-09-24 → 67.64@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=27.291 (G=6.5500 R=0.2400); 2026-09-24:GREEN:O=184.1000,C=190.6500,body=+6.5500,vol=1602600.0; 2026-09-25:RED:O=189.9200,C=189.6800,body=-0.2400,vol=1478604.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.084 (Gvol=1602600 Rvol=1478604); 2026-09-24:GREEN:O=184.1000,C=190.6500,body=+6.5500,vol=1602600.0; 2026-09-25:RED:O=189.9200,C=189.6800,body=-0.2400,vol=1478604.0 | **GOOD** |
| `A07_rvol` | RVOL=1.500 on 2026-09-25: today_vol=1478604 / avg20=985625 (avg window 2026-08-27→2026-09-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.882 on 2026-09-25 (price=189.6800, mid=175.1950, upper=191.6258, lower=158.7642; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-25: price=189.6800 vs SMA50=165.3274 dist=+14.73% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=175.1950 SMA50=165.3274 SMA80=153.7095 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-25 (63 bars); S1[2026-06-25→2026-07-27] low=2026-07-21@129.0000; S2[2026-07-28→2026-08-26] low=2026-07-28@137.3500; S3[2026-08-27→2026-09-25] low=2026-09-10@161.1400 | lows=[129.0, 137.35000610351562, 161.13999938964844] span=24.91% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.7494260499140166 wick_frac=0.2505739500859834 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.05211850506804333 wick_frac=0.9478814949319567 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=27.29099116282027 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:GREEN:body=+3.8200:wick=2.2100; 2026-09-22:GREEN:body=+6.0300:wick=2.1700; 2026-09-23:RED:body=-1.8500:wick=5.3900; 2026-09-24:GREEN:body=+6.5500:wick=2.1900; 2026-09-25:RED:body=-0.2400:wick=4.3650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.81 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.26 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 785.1 | **NEUTRAL** |
| `B04_income` | 41.54 | **GOOD** |
| `B05_profit_margin` | 5.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 189.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=189.0 vs prior_export=189.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.52 | **GOOD** |
| `B10_insider_transactions` | -1.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.01 vs prior=-1.01 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 12.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.81 (this export) | prior_export=20.81 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.26 (this export) | prior_export=1.26 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.440000534057617  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.72 on 2026-09-25; prev RSI=54.44 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.44@2026-09-24 → 51.72@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.44@2026-09-24 → 51.72@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.44@2026-09-24 → 51.72@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=1.531 (G=0.2450 R=0.1600); 2026-09-24:GREEN:O=16.3500,C=16.5950,body=+0.2450,vol=809200.0; 2026-09-25:RED:O=16.6000,C=16.4400,body=-0.1600,vol=740880.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.092 (Gvol=809200 Rvol=740880); 2026-09-24:GREEN:O=16.3500,C=16.5950,body=+0.2450,vol=809200.0; 2026-09-25:RED:O=16.6000,C=16.4400,body=-0.1600,vol=740880.0 | **GOOD** |
| `A07_rvol` | RVOL=0.903 on 2026-09-25: today_vol=740880 / avg20=820245 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.102 on 2026-09-25 (price=16.4400, mid=16.3773, upper=16.9952, lower=15.7593; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=16.4400 vs SMA50=15.9354 dist=+3.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.38_50=15.94_80=16.08 on 2026-09-25: SMA20=16.3773 SMA50=15.9354 SMA80=16.0754 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-26→2026-09-25 (63 bars); S1[2026-06-26→2026-07-28] low=2026-07-28@14.9000; S2[2026-07-29→2026-08-26] low=2026-07-31@14.3630; S3[2026-08-27→2026-09-25] low=2026-09-09@15.8500 | lows=[14.899999618530273, 14.36299991607666, 15.850000381469727] span=10.35% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.6447354551797179 wick_frac=0.3552645448202822 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.34042432309590287 wick_frac=0.6595756769040971 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5312447845886084 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.2900:wick=0.0650; 2026-09-22:RED:body=-0.0500:wick=0.2050; 2026-09-23:RED:body=-0.1800:wick=0.4400; 2026-09-24:GREEN:body=+0.2450:wick=0.1350; 2026-09-25:RED:body=-0.1600:wick=0.3100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ERO  ·  score **+16**  ·  Copper
price=37.869998931884766  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.98 on 2026-09-25; prev RSI=56.95 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.95@2026-09-24 → 58.98@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.95@2026-09-24 → 58.98@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.95@2026-09-24 → 58.98@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.1600 R=0.0000); 2026-09-24:GREEN:O=35.7600,C=37.1100,body=+1.3500,vol=1130300.0; 2026-09-25:GREEN:O=37.0600,C=37.8700,body=+0.8100,vol=783798.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1914098 Rvol=0); 2026-09-24:GREEN:O=35.7600,C=37.1100,body=+1.3500,vol=1130300.0; 2026-09-25:GREEN:O=37.0600,C=37.8700,body=+0.8100,vol=783798.0 | **GOOD** |
| `A07_rvol` | RVOL=0.559 on 2026-09-25: today_vol=783798 / avg20=1402715 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.591 on 2026-09-25 (price=37.8700, mid=35.5880, upper=39.4500, lower=31.7260; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=37.8700 vs SMA50=33.1782 dist=+14.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=35.5880 SMA50=33.1782 SMA80=31.0699 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-25 (63 bars); S1[2026-06-25→2026-07-27] low=2026-07-08@22.9320; S2[2026-07-28→2026-08-26] low=2026-07-29@24.7020; S3[2026-08-27→2026-09-25] low=2026-09-16@31.6400 | lows=[22.93199920654297, 24.70199966430664, 31.639999389648438] span=37.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.7166662349652859 wick_frac=0.28333376503471414 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-21:GREEN:body=+0.0400:wick=1.2820; 2026-09-22:GREEN:body=+1.8200:wick=0.3450; 2026-09-23:GREEN:body=+0.2300:wick=1.3000; 2026-09-24:GREEN:body=+1.3500:wick=0.2700; 2026-09-25:GREEN:body=+0.8100:wick=0.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.30000000000000426 (now=38.2 vs prior_export=37.9 on finviz_2026-09-27) | **GOOD** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.83 | **GOOD** |
| `B13_short_float` | 5.93 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.7 (this export) | prior_export=12.7 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.4 (this export) | prior_export=2.4 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+15**  ·  Biotechnology
price=35.31999969482422  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.72 on 2026-09-25; prev RSI=62.67 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.67@2026-09-24 → 65.72@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.67@2026-09-24 → 65.72@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.67@2026-09-24 → 65.72@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4600 R=0.0000); 2026-09-24:GREEN:O=34.6900,C=34.8300,body=+0.1400,vol=2514900.0; 2026-09-25:GREEN:O=35.0000,C=35.3200,body=+0.3200,vol=3582577.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6097477 Rvol=0); 2026-09-24:GREEN:O=34.6900,C=34.8300,body=+0.1400,vol=2514900.0; 2026-09-25:GREEN:O=35.0000,C=35.3200,body=+0.3200,vol=3582577.0 | **GOOD** |
| `A07_rvol` | RVOL=1.864 on 2026-09-25: today_vol=3582577 / avg20=1921610 (avg window 2026-08-27→2026-09-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.750 on 2026-09-25 (price=35.3200, mid=33.9230, upper=35.7852, lower=32.0608; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=35.3200 vs SMA50=32.0268 dist=+10.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=33.9230 SMA50=32.0268 SMA80=29.8631 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-26@25.3350; S2[2026-07-28→2026-08-26] low=2026-07-30@28.0300; S3[2026-08-27→2026-09-25] low=2026-09-10@32.2100 | lows=[25.334999084472656, 28.030000686645508, 32.209999084472656] span=27.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.312272174873264 wick_frac=0.687727825126736 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.0100:wick=0.4950; 2026-09-22:GREEN:body=+0.7600:wick=0.1650; 2026-09-23:RED:body=-0.6400:wick=0.4150; 2026-09-24:GREEN:body=+0.1400:wick=0.5300; 2026-09-25:GREEN:body=+0.3200:wick=0.4500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.39 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=39.39 vs prior_export=39.39 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICLR  ·  score **+15**  ·  Diagnostics & Research
price=172.3300018310547  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.00 on 2026-09-25; prev RSI=56.76 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.76@2026-09-24 → 55.00@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.76@2026-09-24 → 55.00@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.76@2026-09-24 → 55.00@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=6.878 (G=10.1100 R=1.4700); 2026-09-24:GREEN:O=163.7900,C=173.9000,body=+10.1100,vol=1619300.0; 2026-09-25:RED:O=173.8000,C=172.3300,body=-1.4700,vol=965902.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.676 (Gvol=1619300 Rvol=965902); 2026-09-24:GREEN:O=163.7900,C=173.9000,body=+10.1100,vol=1619300.0; 2026-09-25:RED:O=173.8000,C=172.3300,body=-1.4700,vol=965902.0 | **GOOD** |
| `A07_rvol` | RVOL=1.019 on 2026-09-25: today_vol=965902 / avg20=948110 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.590 on 2026-09-25 (price=172.3300, mid=166.6715, upper=176.2672, lower=157.0758; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=172.3300 vs SMA50=167.5776 dist=+2.84% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=166.67_50=167.58_80=162.96 on 2026-09-25: SMA20=166.6715 SMA50=167.5776 SMA80=162.9609 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-24@147.7100; S2[2026-07-28→2026-08-26] low=2026-08-04@150.8600; S3[2026-08-27→2026-09-25] low=2026-09-10@156.7000 | lows=[147.7100067138672, 150.86000061035156, 156.6999969482422] span=6.09% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.9018728416857344 wick_frac=0.09812715831426554 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.3314547981778897 wick_frac=0.6685452018221103 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.8775457244285745 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:GREEN:body=+4.9400:wick=1.8500; 2026-09-22:RED:body=-3.4400:wick=1.4900; 2026-09-23:RED:body=-3.8700:wick=2.0000; 2026-09-24:GREEN:body=+10.1100:wick=1.1000; 2026-09-25:RED:body=-1.4700:wick=2.9650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.57 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.95 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8294.42 | **NEUTRAL** |
| `B04_income` | 42.34 | **GOOD** |
| `B05_profit_margin` | 0.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 187.27 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=187.27 vs prior_export=187.27 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -2.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.84 vs prior=-2.84 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.74 | **GOOD** |
| `B13_short_float` | 3.44 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.57 (this export) | prior_export=0.57 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.95 (this export) | prior_export=2.95 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMN  ·  score **+15**  ·  Medical Care Facilities
price=34.560001373291016  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.39 on 2026-09-25; prev RSI=53.39 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.39@2026-09-24 → 53.39@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.39@2026-09-24 → 53.39@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.39@2026-09-24 → 53.39@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=3.364 (G=0.7400 R=0.2200); 2026-09-24:GREEN:O=33.8200,C=34.5600,body=+0.7400,vol=920400.0; 2026-09-25:RED:O=34.7800,C=34.5600,body=-0.2200,vol=450109.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=2.045 (Gvol=920400 Rvol=450109); 2026-09-24:GREEN:O=33.8200,C=34.5600,body=+0.7400,vol=920400.0; 2026-09-25:RED:O=34.7800,C=34.5600,body=-0.2200,vol=450109.0 | **GOOD** |
| `A07_rvol` | RVOL=0.768 on 2026-09-25: today_vol=450109 / avg20=586280 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.471 on 2026-09-25 (price=34.5600, mid=34.0695, upper=35.1108, lower=33.0282; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=34.5600 vs SMA50=34.0222 dist=+1.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=34.0695 SMA50=34.0222 SMA80=33.1357 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-25@30.4000; S2[2026-07-28→2026-08-26] low=2026-08-06@30.1700; S3[2026-08-27→2026-09-25] low=2026-09-03@32.0300 | lows=[30.399999618530273, 30.170000076293945, 32.029998779296875] span=6.17% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.5522399956728812 wick_frac=0.4477600043271188 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.28571074703618016 wick_frac=0.7142892529638198 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.363683653829481 need>1.4; red_wick_gt_green=False 5d trail=2026-09-21:RED:body=-0.2300:wick=0.4700; 2026-09-22:GREEN:body=+0.0400:wick=0.7500; 2026-09-23:RED:body=-0.8700:wick=0.2500; 2026-09-24:GREEN:body=+0.7400:wick=0.6000; 2026-09-25:RED:body=-0.2200:wick=0.5500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=307.84 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.13 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3434.32 | **NEUTRAL** |
| `B04_income` | 104.92 | **GOOD** |
| `B05_profit_margin` | 3.05 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.14 vs prior_export=36.14 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.96 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.96 vs prior=-0.96 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.83 | **GOOD** |
| `B13_short_float` | 9.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=307.84 (this export) | prior_export=307.84 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.13 (this export) | prior_export=7.13 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+15**  ·  Computer Hardware
price=13.65999984741211  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.27 on 2026-09-25; prev RSI=61.20 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.20@2026-09-24 → 62.27@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.20@2026-09-24 → 62.27@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.20@2026-09-24 → 62.27@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=14.000 (G=0.2800 R=0.0200); 2026-09-24:GREEN:O=13.2700,C=13.5500,body=+0.2800,vol=1078400.0; 2026-09-25:RED:O=13.6800,C=13.6600,body=-0.0200,vol=969755.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.112 (Gvol=1078400 Rvol=969755); 2026-09-24:GREEN:O=13.2700,C=13.5500,body=+0.2800,vol=1078400.0; 2026-09-25:RED:O=13.6800,C=13.6600,body=-0.0200,vol=969755.0 | **GOOD** |
| `A07_rvol` | RVOL=0.876 on 2026-09-25: today_vol=969755 / avg20=1106670 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.570 on 2026-09-25 (price=13.6600, mid=12.9165, upper=14.2206, lower=11.6124; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=13.6600 vs SMA50=11.9777 dist=+14.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=12.9165 SMA50=11.9777 SMA80=10.8877 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-29→2026-09-25 (63 bars); S1[2026-06-29→2026-07-28] low=2026-07-07@8.3000; S2[2026-07-29→2026-08-26] low=2026-07-29@10.0600; S3[2026-08-27→2026-09-25] low=2026-09-01@11.5500 | lows=[8.300000190734863, 10.0600004196167, 11.550000190734863] span=39.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.5185180606501587 wick_frac=0.4814819393498413 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.05000119209403238 wick_frac=0.9499988079059676 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.99966622162884 need>1.4; red_wick_gt_green=False 5d trail=2026-09-21:RED:body=-0.1700:wick=0.3900; 2026-09-22:GREEN:body=+0.0600:wick=0.4200; 2026-09-23:RED:body=-0.0500:wick=0.3800; 2026-09-24:GREEN:body=+0.2800:wick=0.2600; 2026-09-25:RED:body=-0.0200:wick=0.3800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 19.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DGX  ·  score **+15**  ·  Diagnostics & Research
price=236.32000732421875  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.44 on 2026-09-25; prev RSI=47.79 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.79@2026-09-24 → 47.44@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.79@2026-09-24 → 47.44@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.79@2026-09-24 → 47.44@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=50.212 (G=2.5100 R=0.0500); 2026-09-24:GREEN:O=234.0800,C=236.5900,body=+2.5100,vol=926900.0; 2026-09-25:RED:O=236.3700,C=236.3200,body=-0.0500,vol=739378.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.254 (Gvol=926900 Rvol=739378); 2026-09-24:GREEN:O=234.0800,C=236.5900,body=+2.5100,vol=926900.0; 2026-09-25:RED:O=236.3700,C=236.3200,body=-0.0500,vol=739378.0 | **GOOD** |
| `A07_rvol` | RVOL=0.668 on 2026-09-25: today_vol=739378 / avg20=1106955 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.289 on 2026-09-25 (price=236.3200, mid=239.2620, upper=249.4362, lower=229.0878; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=236.3200 vs SMA50=235.0546 dist=+0.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=239.2620 SMA50=235.0546 SMA80=222.9165 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-24@197.6066; S2[2026-07-28→2026-08-26] low=2026-07-30@229.0100; S3[2026-08-27→2026-09-25] low=2026-09-22@227.2400 | lows=[197.60657161725575, 229.00999450683594, 227.24000549316406] span=15.89% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.6711232782818722 wick_frac=0.3288767217181278 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.019564750005972145 wick_frac=0.9804352499940279 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=50.21214896214896 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-2.2500:wick=2.6600; 2026-09-22:GREEN:body=+6.1700:wick=3.8800; 2026-09-23:RED:body=-2.1200:wick=4.5800; 2026-09-24:GREEN:body=+2.5100:wick=1.2300; 2026-09-25:RED:body=-0.0500:wick=2.5050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.36 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11560.0 | **NEUTRAL** |
| `B04_income` | 1058.0 | **GOOD** |
| `B05_profit_margin` | 9.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 250.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=250.86 vs prior_export=250.86 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | -9.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.0 vs prior=-9.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.95 | **GOOD** |
| `B13_short_float` | 3.28 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.36 (this export) | prior_export=2.36 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LLY  ·  score **+15**  ·  Drug Manufacturers - General
price=1183.4599609375  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.76 on 2026-09-25; prev RSI=55.36 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.36@2026-09-24 → 55.76@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.36@2026-09-24 → 55.76@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.36@2026-09-24 → 55.76@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=13.559 (G=25.2200 R=1.8600); 2026-09-24:GREEN:O=1156.6700,C=1181.8900,body=+25.2200,vol=2417700.0; 2026-09-25:RED:O=1185.3199,C=1183.4600,body=-1.8600,vol=1904134.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=1.270 (Gvol=2417700 Rvol=1904134); 2026-09-24:GREEN:O=1156.6700,C=1181.8900,body=+25.2200,vol=2417700.0; 2026-09-25:RED:O=1185.3199,C=1183.4600,body=-1.8600,vol=1904134.0 | **GOOD** |
| `A07_rvol` | RVOL=0.799 on 2026-09-25: today_vol=1904134 / avg20=2384040 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.825 on 2026-09-25 (price=1183.4600, mid=1150.8075, upper=1190.3880, lower=1111.2270; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-25: price=1183.4600 vs SMA50=1176.7351 dist=+0.57% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=1150.81_50=1176.74_80=1169.04 on 2026-09-25: SMA20=1150.8075 SMA50=1176.7351 SMA80=1169.0440 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-29→2026-09-25 (63 bars); S1[2026-06-29→2026-07-28] low=2026-07-15@1132.7768; S2[2026-07-29→2026-08-26] low=2026-08-03@1107.5629; S3[2026-08-27→2026-09-25] low=2026-09-11@1113.2900 | lows=[1132.7768041389054, 1107.5628820454413, 1113.2900390625] span=2.28% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.5390029845762111 wick_frac=0.46099701542378896 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=0.06378543111784628 wick_frac=0.9362145688821537 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.559230819715166 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:GREEN:body=+20.3900:wick=16.1000; 2026-09-22:GREEN:body=+23.9900:wick=17.2800; 2026-09-23:RED:body=-23.7200:wick=19.1801; 2026-09-24:GREEN:body=+25.2200:wick=21.5701; 2026-09-25:RED:body=-1.8600:wick=27.3000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.92 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.33 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 79665.8 | **NEUTRAL** |
| `B04_income` | 26709.5 | **GOOD** |
| `B05_profit_margin` | 33.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 1363.03 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.5699999999999363 (now=1363.03 vs prior_export=1359.46 on finviz_2026-09-27) | **GOOD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -0.03 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.03 vs prior=-0.03 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.01 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.92 (this export) | prior_export=30.92 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.33 (this export) | prior_export=10.33 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+15**  ·  Healthcare Plans
price=397.92999267578125  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.97 on 2026-09-25; prev RSI=44.75 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.75@2026-09-24 → 55.97@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.75@2026-09-24 → 55.97@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.75@2026-09-24 → 55.97@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=14.0100 R=0.0000); 2026-09-24:GREEN:O=374.5400,C=380.3200,body=+5.7800,vol=899000.0; 2026-09-25:GREEN:O=389.7000,C=397.9300,body=+8.2300,vol=1933321.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2832321 Rvol=0); 2026-09-24:GREEN:O=374.5400,C=380.3200,body=+5.7800,vol=899000.0; 2026-09-25:GREEN:O=389.7000,C=397.9300,body=+8.2300,vol=1933321.0 | **GOOD** |
| `A07_rvol` | RVOL=2.106 on 2026-09-25: today_vol=1933321 / avg20=917980 (avg window 2026-08-27→2026-09-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.238 on 2026-09-25 (price=397.9300, mid=392.4515, upper=415.4985, lower=369.4045; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=397.9300 vs SMA50=387.4410 dist=+2.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=392.4515 SMA50=387.4410 SMA80=380.6938 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-24@352.2290; S2[2026-07-28→2026-08-26] low=2026-08-05@353.6900; S3[2026-08-27→2026-09-25] low=2026-09-23@369.1300 | lows=[352.22899615508356, 353.69000244140625, 369.1300048828125] span=4.80% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.43596426633856983 wick_frac=0.5640357336614301 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-21:RED:body=-7.6200:wick=4.7000; 2026-09-22:RED:body=-6.4200:wick=3.4900; 2026-09-23:GREEN:body=+4.7800:wick=3.7700; 2026-09-24:GREEN:body=+5.7800:wick=4.4400; 2026-09-25:GREEN:body=+8.2300:wick=18.6328 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 425.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=425.46 vs prior_export=425.46 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 2.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SN  ·  score **+15**  ·  Furnishings, Fixtures & Appliances
price=177.74000549316406  pair=`2026-09-24→2026-09-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.60 on 2026-09-25; prev RSI=55.82 on 2026-09-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.82@2026-09-24 → 56.60@2026-09-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.82@2026-09-24 → 56.60@2026-09-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.82@2026-09-24 → 56.60@2026-09-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.8600 R=0.0000); 2026-09-24:GREEN:O=168.4500,C=176.9400,body=+8.4900,vol=1528400.0; 2026-09-25:GREEN:O=175.3700,C=177.7400,body=+2.3700,vol=1294405.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-24 + 2026-09-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2822805 Rvol=0); 2026-09-24:GREEN:O=168.4500,C=176.9400,body=+8.4900,vol=1528400.0; 2026-09-25:GREEN:O=175.3700,C=177.7400,body=+2.3700,vol=1294405.0 | **GOOD** |
| `A07_rvol` | RVOL=0.574 on 2026-09-25: today_vol=1294405 / avg20=2254290 (avg window 2026-08-27→2026-09-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.325 on 2026-09-25 (price=177.7400, mid=172.4635, upper=188.6950, lower=156.2320; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-25: price=177.7400 vs SMA50=172.1480 dist=+3.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-25: SMA20=172.4635 SMA50=172.1480 SMA80=159.1010 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-25 (63 bars); S1[2026-06-24→2026-07-27] low=2026-06-24@136.6300; S2[2026-07-28→2026-08-26] low=2026-07-28@153.1950; S3[2026-08-27→2026-09-25] low=2026-09-11@157.0100 | lows=[136.6300048828125, 153.19500732421875, 157.00999450683594] span=14.92% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: GREEN body_frac=0.6166361275172447 wick_frac=0.3833638724827553 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-24+2026-09-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-21:RED:body=-0.2100:wick=3.5800; 2026-09-22:RED:body=-0.9900:wick=3.9990; 2026-09-23:RED:body=-0.8000:wick=4.4800; 2026-09-24:GREEN:body=+8.4900:wick=1.2070; 2026-09-25:GREEN:body=+2.3700:wick=4.2549 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.45 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6909.96 | **NEUTRAL** |
| `B04_income` | 695.22 | **GOOD** |
| `B05_profit_margin` | 10.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.56 vs prior_export=209.56 on finviz_2026-09-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | -6.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.41 vs prior=-6.41 on finviz_2026-09-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.92 | **GOOD** |
| `B13_short_float` | 8.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.45 (this export) | prior_export=13.45 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.82 (this export) | prior_export=6.82 (finviz_2026-09-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-28_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.