# A+B1 Feature Checklist — 2026-10-19

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-19.csv` · prior export for Δ: `2026-10-18`
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
| 1 | ARHS | +17 | 17 | 0 | 2026-10-15→2026-10-16 | Specialty Retail |
| 2 | SWKS | +16 | 16 | 0 | 2026-10-15→2026-10-16 | Semiconductors |
| 3 | AME | +16 | 17 | 1 | 2026-10-15→2026-10-16 | Specialty Industrial Machinery |
| 4 | EPAM | +16 | 17 | 1 | 2026-10-15→2026-10-16 | Information Technology Services |
| 5 | VNT | +15 | 16 | 1 | 2026-10-15→2026-10-16 | Scientific & Technical Instruments |
| 6 | TME | +15 | 16 | 1 | 2026-10-15→2026-10-16 | Internet Content & Information |
| 7 | TTC | +15 | 17 | 2 | 2026-10-15→2026-10-16 | Tools & Accessories |
| 8 | RPRX | +15 | 17 | 2 | 2026-10-15→2026-10-16 | Biotechnology |
| 9 | HLNE | +15 | 16 | 1 | 2026-10-15→2026-10-16 | Asset Management |
| 10 | TTEK | +14 | 15 | 1 | 2026-10-15→2026-10-16 | Engineering & Construction |
| 11 | IMO | +14 | 16 | 2 | 2026-10-15→2026-10-16 | Oil & Gas Integrated |
| 12 | PAGP | +14 | 15 | 1 | 2026-10-15→2026-10-16 | Oil & Gas Midstream |
| 13 | WMG | +14 | 16 | 2 | 2026-10-15→2026-10-16 | Entertainment |
| 14 | ESTC | +14 | 16 | 2 | 2026-10-15→2026-10-16 | Software - Application |
| 15 | HOMB | +14 | 15 | 1 | 2026-10-15→2026-10-16 | Banks - Regional |

## Full checklist — top 15

### ARHS  ·  score **+17**  ·  Specialty Retail
price=9.930000305175781  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.91 on 2026-10-16; prev RSI=61.23 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.23@2026-10-15 → 57.91@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.23@2026-10-15 → 57.91@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.23@2026-10-15 → 57.91@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=3.043 (G=0.7000 R=0.2300); 2026-10-15:GREEN:O=9.4300,C=10.1300,body=+0.7000,vol=2203938.0; 2026-10-16:RED:O=10.1600,C=9.9300,body=-0.2300,vol=1627388.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.354 (Gvol=2203938 Rvol=1627388); 2026-10-15:GREEN:O=9.4300,C=10.1300,body=+0.7000,vol=2203938.0; 2026-10-16:RED:O=10.1600,C=9.9300,body=-0.2300,vol=1627388.0 | **GOOD** |
| `A07_rvol` | RVOL=0.996 on 2026-10-16: today_vol=1627388 / avg20=1634094 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.397 on 2026-10-16 (price=9.9300, mid=9.6330, upper=10.3809, lower=8.8851; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=9.9300 vs SMA50=9.1514 dist=+8.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=9.6330 SMA50=9.1514 SMA80=8.7094 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-16 (63 bars); S1[2026-07-17→2026-08-18] low=2026-07-23@7.0600; S2[2026-08-19→2026-09-17] low=2026-09-16@7.0670; S3[2026-09-18→2026-10-16] low=2026-09-18@7.4500 | lows=[7.059999942779541, 7.066999912261963, 7.449999809265137] span=5.52% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8045975877517508 wick_frac=0.19540241224824914 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.42990580903691344 wick_frac=0.5700941909630866 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0434834889622344 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-0.0420:wick=0.2680; 2026-10-13:GREEN:body=+0.3180:wick=0.1700; 2026-10-14:RED:body=-0.1400:wick=0.0950; 2026-10-15:GREEN:body=+0.7000:wick=0.1700; 2026-10-16:RED:body=-0.2300:wick=0.3050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.52 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.26 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1408.59 | **NEUTRAL** |
| `B04_income` | 69.18 | **GOOD** |
| `B05_profit_margin` | 4.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.54 vs prior_export=10.54 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 12.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.52 (this export) | prior_export=72.52 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=79.69999694824219  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.58 on 2026-10-16; prev RSI=47.48 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.48@2026-10-15 → 48.58@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.48@2026-10-15 → 48.58@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.48@2026-10-15 → 48.58@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.9600 R=0.0000); 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0; 2026-10-16:GREEN:O=79.0300,C=79.7000,body=+0.6700,vol=7007503.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12776705 Rvol=0); 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0; 2026-10-16:GREEN:O=79.0300,C=79.7000,body=+0.6700,vol=7007503.0 | **GOOD** |
| `A07_rvol` | RVOL=0.897 on 2026-10-16: today_vol=7007503 / avg20=7813138 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.378 on 2026-10-16 (price=79.7000, mid=83.5390, upper=93.7083, lower=73.3697; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=79.7000 vs SMA50=77.3894 dist=+2.99% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=83.5390 SMA50=77.3894 SMA80=72.0424 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-16 (63 bars); S1[2026-07-17→2026-08-18] low=2026-07-17@56.7100; S2[2026-08-19→2026-09-17] low=2026-08-25@63.0600; S3[2026-09-18→2026-10-16] low=2026-10-13@73.7600 | lows=[56.709999084472656, 63.060001373291016, 73.76000213623047] span=30.07% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.5855250537770774 wick_frac=0.4144749462229225 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-1.4000:wick=1.9000; 2026-10-13:GREEN:body=+2.8000:wick=0.6850; 2026-10-14:GREEN:body=+1.3900:wick=0.3700; 2026-10-15:GREEN:body=+3.2900:wick=0.2380; 2026-10-16:GREEN:body=+0.6700:wick=2.1391 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+16**  ·  Specialty Industrial Machinery
price=248.1699981689453  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.83 on 2026-10-16; prev RSI=53.47 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.47@2026-10-15 → 52.83@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.47@2026-10-15 → 52.83@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.47@2026-10-15 → 52.83@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=17.392 (G=4.0000 R=0.2300); 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0; 2026-10-16:RED:O=248.4000,C=248.1700,body=-0.2300,vol=1568726.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.135 (Gvol=1780759 Rvol=1568726); 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0; 2026-10-16:RED:O=248.4000,C=248.1700,body=-0.2300,vol=1568726.0 | **GOOD** |
| `A07_rvol` | RVOL=1.156 on 2026-10-16: today_vol=1568726 / avg20=1356697 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.123 on 2026-10-16 (price=248.1700, mid=248.8510, upper=254.4020, lower=243.3000; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=248.1700 vs SMA50=244.7722 dist=+1.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=248.8510 SMA50=244.7722 SMA80=242.5328 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-18] low=2026-07-17@229.8900; S2[2026-08-19→2026-09-17] low=2026-09-01@229.6000; S3[2026-09-18→2026-10-16] low=2026-09-18@233.6200 | lows=[229.88999938964844, 229.60000610351562, 233.6199951171875] span=1.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.5730662533528914 wick_frac=0.4269337466471086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.04206598607940433 wick_frac=0.9579340139205956 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=17.391627413255488 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:GREEN:body=+1.3700:wick=2.8450; 2026-10-13:GREEN:body=+0.8400:wick=3.1900; 2026-10-14:RED:body=-2.0900:wick=2.8150; 2026-10-15:GREEN:body=+4.0000:wick=2.9800; 2026-10-16:RED:body=-0.2300:wick=5.2375 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+16**  ·  Information Technology Services
price=122.19000244140625  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.95 on 2026-10-16; prev RSI=65.09 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.09@2026-10-15 → 64.95@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.09@2026-10-15 → 64.95@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.09@2026-10-15 → 64.95@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=3.276 (G=6.2900 R=1.9200); 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0; 2026-10-16:RED:O=124.1100,C=122.1900,body=-1.9200,vol=1276200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.146 (Gvol=1462982 Rvol=1276200); 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0; 2026-10-16:RED:O=124.1100,C=122.1900,body=-1.9200,vol=1276200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.371 on 2026-10-16: today_vol=1276200 / avg20=930865 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.151 on 2026-10-16 (price=122.1900, mid=111.8970, upper=120.8371, lower=102.9569; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=122.1900 vs SMA50=111.2914 dist=+9.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=111.8970 SMA50=111.2914 SMA80=103.0789 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-18] low=2026-07-23@84.0300; S2[2026-08-19→2026-09-17] low=2026-08-19@99.8100; S3[2026-09-18→2026-10-16] low=2026-09-28@104.3300 | lows=[84.02999877929688, 99.80999755859375, 104.33000183105469] span=24.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=1.0413907222029242 wick_frac=-0.04139072220292418 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.3416365179025963 wick_frac=0.6583634820974037 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.276045267784056 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-1.9000:wick=2.3450; 2026-10-13:GREEN:body=+1.3200:wick=1.9100; 2026-10-14:RED:body=-1.5700:wick=1.2600; 2026-10-15:GREEN:body=+6.2900:wick=-0.2500; 2026-10-16:RED:body=-1.9200:wick=3.7000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+15**  ·  Scientific & Technical Instruments
price=33.34000015258789  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.41 on 2026-10-16; prev RSI=61.13 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.13@2026-10-15 → 61.41@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.13@2026-10-15 → 61.41@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.13@2026-10-15 → 61.41@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=10.857 (G=0.7600 R=0.0700); 2026-10-15:GREEN:O=32.5400,C=33.3000,body=+0.7600,vol=1423800.0; 2026-10-16:RED:O=33.4100,C=33.3400,body=-0.0700,vol=1173100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.214 (Gvol=1423800 Rvol=1173100); 2026-10-15:GREEN:O=32.5400,C=33.3000,body=+0.7600,vol=1423800.0; 2026-10-16:RED:O=33.4100,C=33.3400,body=-0.0700,vol=1173100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.887 on 2026-10-16: today_vol=1173100 / avg20=1322140 (avg window 2026-09-17→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.052 on 2026-10-16 (price=33.3400, mid=31.8675, upper=33.2673, lower=30.4677; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=33.3400 vs SMA50=32.4646 dist=+2.70% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=31.87_50=32.46_80=31.66 on 2026-10-16: SMA20=31.8675 SMA50=32.4646 SMA80=31.6558 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-15@28.9400; S2[2026-08-18→2026-09-16] low=2026-09-10@31.1100; S3[2026-09-17→2026-10-16] low=2026-09-24@30.4250 | lows=[28.940000534057617, 31.110000610351562, 30.424999237060547] span=7.50% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8216170963857409 wick_frac=0.1783829036142591 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.07368383011359758 wick_frac=0.9263161698864024 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.85716621253406 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.2900:wick=0.3500; 2026-10-12:RED:body=-0.7000:wick=0.1700; 2026-10-14:GREEN:body=+0.1600:wick=0.4400; 2026-10-15:GREEN:body=+0.7600:wick=0.1650; 2026-10-16:RED:body=-0.0700:wick=0.8800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.57 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.28 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3068.3 | **NEUTRAL** |
| `B04_income` | 348.0 | **GOOD** |
| `B05_profit_margin` | 11.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.22 vs prior_export=40.22 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.96 | **GOOD** |
| `B13_short_float` | 8.75 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.57 (this export) | prior_export=10.57 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.28 (this export) | prior_export=1.28 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TME  ·  score **+15**  ·  Internet Content & Information
price=8.630000114440918  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.15 on 2026-10-16; prev RSI=62.94 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.94@2026-10-15 → 62.15@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.94@2026-10-15 → 62.15@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.94@2026-10-15 → 62.15@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=3.571 (G=0.2500 R=0.0700); 2026-10-15:GREEN:O=8.4000,C=8.6500,body=+0.2500,vol=6162630.0; 2026-10-16:RED:O=8.7000,C=8.6300,body=-0.0700,vol=4218272.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.461 (Gvol=6162630 Rvol=4218272); 2026-10-15:GREEN:O=8.4000,C=8.6500,body=+0.2500,vol=6162630.0; 2026-10-16:RED:O=8.7000,C=8.6300,body=-0.0700,vol=4218272.0 | **GOOD** |
| `A07_rvol` | RVOL=0.623 on 2026-10-16: today_vol=4218272 / avg20=6766024 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.751 on 2026-10-16 (price=8.6300, mid=8.2005, upper=8.7724, lower=7.6286; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=8.6300 vs SMA50=8.3692 dist=+3.12% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-16: SMA20=8.2005 SMA50=8.3692 SMA80=8.5786 | **BAD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-16 (63 bars); S1[2026-07-17→2026-08-18] low=2026-08-11@8.2100; S2[2026-08-19→2026-09-17] low=2026-09-10@7.7700; S3[2026-09-18→2026-10-16] low=2026-10-02@7.6600 | lows=[8.210000038146973, 7.769999980926514, 7.659999847412109] span=7.18% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.7575759327225963 wick_frac=0.2424240672774037 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.43749850988245953 wick_frac=0.5625014901175405 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.571444141689373 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:GREEN:body=+0.1500:wick=0.1000; 2026-10-13:GREEN:body=+0.0300:wick=0.1450; 2026-10-14:RED:body=-0.0600:wick=0.2100; 2026-10-15:GREEN:body=+0.2500:wick=0.0800; 2026-10-16:RED:body=-0.0700:wick=0.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.44 (current export asof; earnings_date=8/11/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.46 (current export; earnings_date=8/11/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4853.58 | **NEUTRAL** |
| `B04_income` | 1276.49 | **GOOD** |
| `B05_profit_margin` | 26.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.98 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.98 vs prior_export=11.98 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 4.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/11/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.44 (this export) | prior_export=1.44 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.46 (this export) | prior_export=1.46 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTC  ·  score **+15**  ·  Tools & Accessories
price=100.58000183105469  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.27 on 2026-10-16; prev RSI=64.37 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.37@2026-10-15 → 63.27@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.37@2026-10-15 → 63.27@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.37@2026-10-15 → 63.27@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=1.7600 R=0.8800); 2026-10-15:GREEN:O=99.0600,C=100.8200,body=+1.7600,vol=922500.0; 2026-10-16:RED:O=101.4600,C=100.5800,body=-0.8800,vol=514200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.794 (Gvol=922500 Rvol=514200); 2026-10-15:GREEN:O=99.0600,C=100.8200,body=+1.7600,vol=922500.0; 2026-10-16:RED:O=101.4600,C=100.5800,body=-0.8800,vol=514200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.663 on 2026-10-16: today_vol=514200 / avg20=775110 (avg window 2026-09-17→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.878 on 2026-10-16 (price=100.5800, mid=97.1235, upper=101.0584, lower=93.1886; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=100.5800 vs SMA50=96.8792 dist=+3.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=97.1235 SMA50=96.8792 SMA80=96.0650 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@90.6200; S2[2026-08-18→2026-09-16] low=2026-09-04@91.0300; S3[2026-09-17→2026-10-16] low=2026-09-18@91.1100 | lows=[90.62000274658203, 91.02999877929688, 91.11000061035156] span=0.54% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8148144223569138 wick_frac=0.1851855776430862 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.31541109844103354 wick_frac=0.6845889015589665 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0000086697935724 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-1.4100:wick=1.2500; 2026-10-12:GREEN:body=+1.1500:wick=0.8700; 2026-10-14:GREEN:body=+0.7700:wick=0.3600; 2026-10-15:GREEN:body=+1.7600:wick=0.4000; 2026-10-16:RED:body=-0.8800:wick=1.9100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.75 (current export asof; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.91 (current export; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4756.5 | **NEUTRAL** |
| `B04_income` | 363.3 | **GOOD** |
| `B05_profit_margin` | 7.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=111.0 vs prior_export=111.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -3.79 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.79 vs prior=-3.79 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.73 | **GOOD** |
| `B13_short_float` | 3.92 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.75 (this export) | prior_export=1.75 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.91 (this export) | prior_export=2.91 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RPRX  ·  score **+15**  ·  Biotechnology
price=60.959999084472656  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.02 on 2026-10-16; prev RSI=58.79 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.79@2026-10-15 → 65.02@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.79@2026-10-15 → 65.02@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.79@2026-10-15 → 65.02@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=8.231 (G=1.0700 R=0.1300); 2026-10-15:RED:O=59.7500,C=59.6200,body=-0.1300,vol=3407000.0; 2026-10-16:GREEN:O=59.8900,C=60.9600,body=+1.0700,vol=5786400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.698 (Gvol=5786400 Rvol=3407000); 2026-10-15:RED:O=59.7500,C=59.6200,body=-0.1300,vol=3407000.0; 2026-10-16:GREEN:O=59.8900,C=60.9600,body=+1.0700,vol=5786400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.954 on 2026-10-16: today_vol=5786400 / avg20=2961597 (avg window 2026-09-17→2026-10-15, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.310 on 2026-10-16 (price=60.9600, mid=57.9805, upper=60.2545, lower=55.7065; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=60.9600 vs SMA50=59.2651 dist=+2.86% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=57.98_50=59.27_80=58.41 on 2026-10-16: SMA20=57.9805 SMA50=59.2651 SMA80=58.4085 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-04@56.1338; S2[2026-08-18→2026-09-16] low=2026-09-14@57.7500; S3[2026-09-17→2026-10-16] low=2026-10-08@55.1300 | lows=[56.13382804939311, 57.75, 55.130001068115234] span=4.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.6729557183266196 wick_frac=0.3270442816733804 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.14285834056449145 wick_frac=0.8571416594355086 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.230699257607323 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.9000:wick=0.4550; 2026-10-12:GREEN:body=+0.7400:wick=0.3100; 2026-10-14:GREEN:body=+2.0600:wick=0.2400; 2026-10-15:RED:body=-0.1300:wick=0.7800; 2026-10-16:GREEN:body=+1.0700:wick=0.5200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.82 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.85 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2536.01 | **NEUTRAL** |
| `B04_income` | 814.79 | **GOOD** |
| `B05_profit_margin` | 32.13 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.71 vs prior_export=65.71 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -1.81 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.81 vs prior=-1.81 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.37 | **GOOD** |
| `B13_short_float` | 2.79 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.82 (this export) | prior_export=3.82 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.85 (this export) | prior_export=1.85 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HLNE  ·  score **+15**  ·  Asset Management
price=81.83999633789062  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=33.17 on 2026-10-16; prev RSI=36.08 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 36.08@2026-10-15 → 33.17@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 36.08@2026-10-15 → 33.17@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 36.08@2026-10-15 → 33.17@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=1.148 (G=2.7100 R=2.3600); 2026-10-15:GREEN:O=80.7900,C=83.5000,body=+2.7100,vol=992000.0; 2026-10-16:RED:O=84.2000,C=81.8400,body=-2.3600,vol=766300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.295 (Gvol=992000 Rvol=766300); 2026-10-15:GREEN:O=80.7900,C=83.5000,body=+2.7100,vol=992000.0; 2026-10-16:RED:O=84.2000,C=81.8400,body=-2.3600,vol=766300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.117 on 2026-10-16: today_vol=766300 / avg20=686228 (avg window 2026-09-17→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-1.115 on 2026-10-16 (price=81.8400, mid=85.9425, upper=89.6211, lower=82.2639; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-16: price=81.8400 vs SMA50=95.3080 dist=-14.13% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=85.94_50=95.31_80=90.86 on 2026-10-16: SMA20=85.9425 SMA50=95.3080 SMA80=90.8581 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@79.3400; S2[2026-08-18→2026-09-16] low=2026-09-16@88.1500; S3[2026-09-17→2026-10-16] low=2026-10-15@79.6850 | lows=[79.33999633789062, 88.1500015258789, 79.68499755859375] span=11.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.6110475567903252 wick_frac=0.38895244320967476 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.45297524473373996 wick_frac=0.54702475526626 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1483043998318947 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.2600:wick=1.4400; 2026-10-12:GREEN:body=+0.9100:wick=1.4350; 2026-10-14:GREEN:body=+0.2600:wick=2.3100; 2026-10-15:GREEN:body=+2.7100:wick=1.7250; 2026-10-16:RED:body=-2.3600:wick=2.8500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=26.41 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=24.05 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 858.37 | **NEUTRAL** |
| `B04_income` | 275.89 | **GOOD** |
| `B05_profit_margin` | 32.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 135.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=135.0 vs prior_export=135.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 1.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.21 vs prior=1.21 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.77 | **GOOD** |
| `B13_short_float` | 11.31 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.41 (this export) | prior_export=26.41 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=24.05 (this export) | prior_export=24.05 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTEK  ·  score **+14**  ·  Engineering & Construction
price=34.720001220703125  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.22 on 2026-10-16; prev RSI=54.80 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.80@2026-10-15 → 54.22@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.80@2026-10-15 → 54.22@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.80@2026-10-15 → 54.22@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=5.810 (G=1.2200 R=0.2100); 2026-10-15:GREEN:O=33.5600,C=34.7800,body=+1.2200,vol=2312800.0; 2026-10-16:RED:O=34.9300,C=34.7200,body=-0.2100,vol=1407200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.644 (Gvol=2312800 Rvol=1407200); 2026-10-15:GREEN:O=33.5600,C=34.7800,body=+1.2200,vol=2312800.0; 2026-10-16:RED:O=34.9300,C=34.7200,body=-0.2100,vol=1407200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.647 on 2026-10-16: today_vol=1407200 / avg20=2174580 (avg window 2026-09-17→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.392 on 2026-10-16 (price=34.7200, mid=33.9860, upper=35.8568, lower=32.1152; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-16: price=34.7200 vs SMA50=35.2511 dist=-1.51% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=33.99_50=35.25_80=33.58 on 2026-10-16: SMA20=33.9860 SMA50=35.2511 SMA80=33.5839 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@30.3483; S2[2026-08-18→2026-09-16] low=2026-09-09@35.3000; S3[2026-09-17→2026-10-16] low=2026-10-07@32.0200 | lows=[30.348322603720742, 35.29999923706055, 32.02000045776367] span=16.32% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8652464808737551 wick_frac=0.13475351912624484 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.32307665220989124 wick_frac=0.6769233477901088 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.8095367847411445 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.0700:wick=0.4200; 2026-10-12:GREEN:body=+0.0400:wick=0.4200; 2026-10-14:RED:body=-0.0200:wick=0.7200; 2026-10-15:GREEN:body=+1.2200:wick=0.1900; 2026-10-16:RED:body=-0.2100:wick=0.4400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.87 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.68 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5069.48 | **NEUTRAL** |
| `B04_income` | 435.98 | **GOOD** |
| `B05_profit_margin` | 8.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.33 vs prior_export=40.33 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | 0.15 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.15 vs prior=0.15 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.71 | **GOOD** |
| `B13_short_float` | 4.35 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.87 (this export) | prior_export=5.87 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IMO  ·  score **+14**  ·  Oil & Gas Integrated
price=133.9199981689453  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.47 on 2026-10-16; prev RSI=57.63 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.63@2026-10-15 → 64.47@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.63@2026-10-15 → 64.47@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.63@2026-10-15 → 64.47@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=15.631 (G=2.9700 R=0.1900); 2026-10-15:RED:O=129.7800,C=129.5900,body=-0.1900,vol=773700.0; 2026-10-16:GREEN:O=130.9500,C=133.9200,body=+2.9700,vol=948400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.226 (Gvol=948400 Rvol=773700); 2026-10-15:RED:O=129.7800,C=129.5900,body=-0.1900,vol=773700.0; 2026-10-16:GREEN:O=130.9500,C=133.9200,body=+2.9700,vol=948400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.798 on 2026-10-16: today_vol=948400 / avg20=527356 (avg window 2026-09-17→2026-10-15, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.307 on 2026-10-16 (price=133.9200, mid=124.9140, upper=131.8053, lower=118.0227; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=133.9200 vs SMA50=129.1998 dist=+3.65% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=124.91_50=129.20_80=125.73 on 2026-10-16: SMA20=124.9140 SMA50=129.1998 SMA80=125.7323 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-16@118.7700; S2[2026-08-18→2026-09-16] low=2026-09-04@127.8700; S3[2026-09-17→2026-10-16] low=2026-10-07@119.1100 | lows=[118.7699966430664, 127.87000274658203, 119.11000061035156] span=7.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.9369094436074301 wick_frac=0.06309055639256989 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.07089662711517002 wick_frac=0.92910337288483 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.631384516543527 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.2200:wick=2.5600; 2026-10-12:GREEN:body=+0.1000:wick=1.3450; 2026-10-14:GREEN:body=+0.0500:wick=1.3900; 2026-10-15:RED:body=-0.1900:wick=2.4900; 2026-10-16:GREEN:body=+2.9700:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.12 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.34 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 36505.14 | **NEUTRAL** |
| `B04_income` | 3011.45 | **GOOD** |
| `B05_profit_margin` | 8.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 113.87 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=113.87 vs prior_export=113.87 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 3.85 | **BAD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.74 | **GOOD** |
| `B13_short_float` | 12.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.12 (this export) | prior_export=9.12 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.34 (this export) | prior_export=3.34 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+14**  ·  Oil & Gas Midstream
price=27.450000762939453  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.52 on 2026-10-16; prev RSI=59.03 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.03@2026-10-15 → 63.52@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.03@2026-10-15 → 63.52@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.03@2026-10-15 → 63.52@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=1.714 (G=0.1200 R=0.0700); 2026-10-15:RED:O=27.2400,C=27.1700,body=-0.0700,vol=2578800.0; 2026-10-16:GREEN:O=27.3300,C=27.4500,body=+0.1200,vol=3134700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.216 (Gvol=3134700 Rvol=2578800); 2026-10-15:RED:O=27.2400,C=27.1700,body=-0.0700,vol=2578800.0; 2026-10-16:GREEN:O=27.3300,C=27.4500,body=+0.1200,vol=3134700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.898 on 2026-10-16: today_vol=3134700 / avg20=1651841 (avg window 2026-09-17→2026-10-15, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.793 on 2026-10-16 (price=27.4500, mid=26.6383, upper=27.6624, lower=25.6141; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=27.4500 vs SMA50=26.8759 dist=+2.14% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=26.64_50=26.88_80=26.07 on 2026-10-16: SMA20=26.6383 SMA50=26.8759 SMA80=26.0712 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@24.6700; S2[2026-08-18→2026-09-16] low=2026-08-19@25.8500; S3[2026-09-17→2026-10-16] low=2026-10-02@25.6200 | lows=[24.670000076293945, 25.850000381469727, 25.6200008392334] span=4.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.22641638446340595 wick_frac=0.773583615536594 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.13461467923559403 wick_frac=0.865385320764406 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7143051771117166 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:RED:body=-0.0100:wick=0.4450; 2026-10-12:GREEN:body=+0.0250:wick=0.2200; 2026-10-14:RED:body=-0.2400:wick=0.1300; 2026-10-15:RED:body=-0.0700:wick=0.4500; 2026-10-16:GREEN:body=+0.1200:wick=0.4100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.08 vs prior_export=26.08 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 9.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMG  ·  score **+14**  ·  Entertainment
price=31.399999618530273  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=75.32 on 2026-10-16; prev RSI=72.20 on 2026-10-15 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.20@2026-10-15 → 75.32@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.20@2026-10-15 → 75.32@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.20@2026-10-15 → 75.32@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6200 R=0.0000); 2026-10-15:GREEN:O=29.4700,C=30.6900,body=+1.2200,vol=4762444.0; 2026-10-16:GREEN:O=31.0000,C=31.4000,body=+0.4000,vol=3492390.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8254834 Rvol=0); 2026-10-15:GREEN:O=29.4700,C=30.6900,body=+1.2200,vol=4762444.0; 2026-10-16:GREEN:O=31.0000,C=31.4000,body=+0.4000,vol=3492390.0 | **GOOD** |
| `A07_rvol` | RVOL=1.503 on 2026-10-16: today_vol=3492390 / avg20=2322892 (avg window 2026-09-18→2026-10-15, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.349 on 2026-10-16 (price=31.4000, mid=28.1247, upper=30.5520, lower=25.6975; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=31.4000 vs SMA50=27.7487 dist=+13.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=28.1247 SMA50=27.7487 SMA80=27.6790 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-18] low=2026-08-12@24.5150; S2[2026-08-19→2026-09-17] low=2026-08-19@26.2754; S3[2026-09-18→2026-10-16] low=2026-09-28@26.4500 | lows=[24.514999389648438, 26.275437746437767, 26.450000762939453] span=7.89% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.6732562346684655 wick_frac=0.3267437653315345 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-12:RED:body=-0.1500:wick=0.3000; 2026-10-13:GREEN:body=+0.2600:wick=0.3850; 2026-10-14:GREEN:body=+0.3100:wick=0.3400; 2026-10-15:GREEN:body=+1.2200:wick=0.3150; 2026-10-16:GREEN:body=+0.4000:wick=0.3250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESTC  ·  score **+14**  ·  Software - Application
price=91.73999786376953  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.05 on 2026-10-16; prev RSI=60.45 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.45@2026-10-15 → 51.05@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.45@2026-10-15 → 51.05@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.45@2026-10-15 → 51.05@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=1.083 (G=6.2400 R=5.7600); 2026-10-15:GREEN:O=91.0000,C=97.2400,body=+6.2400,vol=4181500.0; 2026-10-16:RED:O=97.5000,C=91.7400,body=-5.7600,vol=2953000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.416 (Gvol=4181500 Rvol=2953000); 2026-10-15:GREEN:O=91.0000,C=97.2400,body=+6.2400,vol=4181500.0; 2026-10-16:RED:O=97.5000,C=91.7400,body=-5.7600,vol=2953000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.601 on 2026-10-16: today_vol=2953000 / avg20=1844624 (avg window 2026-09-17→2026-10-15, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.072 on 2026-10-16 (price=91.7400, mid=92.1960, upper=98.5234, lower=85.8686; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=91.7400 vs SMA50=87.9858 dist=+4.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-16: SMA20=92.1960 SMA50=87.9858 SMA80=77.8294 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@56.0600; S2[2026-08-18→2026-09-16] low=2026-08-26@78.7900; S3[2026-09-17→2026-10-16] low=2026-09-17@83.8100 | lows=[56.060001373291016, 78.79000091552734, 83.80999755859375] span=49.50% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8606893605199354 wick_frac=0.13931063948006467 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.616702322226606 wick_frac=0.383297677773394 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0833325606808173 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.4300:wick=2.7324; 2026-10-12:RED:body=-3.9300:wick=2.4600; 2026-10-14:RED:body=-0.5600:wick=3.2700; 2026-10-15:GREEN:body=+6.2400:wick=1.0100; 2026-10-16:RED:body=-5.7600:wick=3.5800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=19.84 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.78 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.16 | **NEUTRAL** |
| `B04_income` | 375.64 | **GOOD** |
| `B05_profit_margin` | 20.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.42 vs prior_export=110.42 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.05 vs prior=-8.05 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 4.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.84 (this export) | prior_export=19.84 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HOMB  ·  score **+14**  ·  Banks - Regional
price=29.200000762939453  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.45 on 2026-10-16; prev RSI=48.83 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.83@2026-10-15 → 53.45@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.83@2026-10-15 → 53.45@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.83@2026-10-15 → 53.45@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2700 R=0.0000); 2026-10-15:GREEN:O=27.7500,C=28.8700,body=+1.1200,vol=4760300.0; 2026-10-16:GREEN:O=29.0500,C=29.2000,body=+0.1500,vol=2375800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7136100 Rvol=0); 2026-10-15:GREEN:O=27.7500,C=28.8700,body=+1.1200,vol=4760300.0; 2026-10-16:GREEN:O=29.0500,C=29.2000,body=+0.1500,vol=2375800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.378 on 2026-10-16: today_vol=2375800 / avg20=1724304 (avg window 2026-09-17→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.490 on 2026-10-16 (price=29.2000, mid=28.7550, upper=29.6640, lower=27.8460; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-16: price=29.2000 vs SMA50=29.6165 dist=-1.41% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=28.76_50=29.62_80=29.53 on 2026-10-16: SMA20=28.7550 SMA50=29.6165 SMA80=29.5303 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-16 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-15@28.8246; S2[2026-08-18→2026-09-16] low=2026-09-01@29.0900; S3[2026-09-17→2026-10-16] low=2026-10-15@27.6000 | lows=[28.824611964612252, 29.09000015258789, 27.600000381469727] span=5.40% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.5659466428219232 wick_frac=0.43405335717807675 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:RED:body=-0.1700:wick=0.0700; 2026-10-12:GREEN:body=+0.1700:wick=0.2000; 2026-10-14:GREEN:body=+0.2900:wick=0.0600; 2026-10-15:GREEN:body=+1.1200:wick=0.1500; 2026-10-16:GREEN:body=+0.1500:wick=0.4500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.14 (current export asof; earnings_date=10/14/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.21 (current export; earnings_date=10/14/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1491.58 | **NEUTRAL** |
| `B04_income` | 479.37 | **GOOD** |
| `B05_profit_margin` | 32.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.25 vs prior_export=33.25 on finviz_2026-10-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | 0.54 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.54 vs prior=0.54 on finviz_2026-10-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.53 | **GOOD** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 10/14/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.14 (this export) | prior_export=5.14 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.21 (this export) | prior_export=2.21 (finviz_2026-10-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-19_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.