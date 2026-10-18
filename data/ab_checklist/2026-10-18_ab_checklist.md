# A+B1 Feature Checklist — 2026-10-18

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-18.csv` · prior export for Δ: `2026-10-16`
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
| 2 | AME | +16 | 17 | 1 | 2026-10-15→2026-10-16 | Specialty Industrial Machinery |
| 3 | EPAM | +16 | 17 | 1 | 2026-10-15→2026-10-16 | Information Technology Services |
| 4 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 5 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 6 | SWKS | +16 | 16 | 0 | 2026-10-15→2026-10-16 | Semiconductors |
| 7 | TME | +15 | 16 | 1 | 2026-10-15→2026-10-16 | Internet Content & Information |
| 8 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 9 | ADSK | +14 | 15 | 1 | 2026-10-15→2026-10-16 | Software - Application |
| 10 | MTDR | +14 | 16 | 2 | 2026-10-15→2026-10-16 | Oil & Gas E&P |
| 11 | NWBI | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Banks - Regional |
| 12 | AFRM | +14 | 16 | 2 | 2026-10-15→2026-10-16 | Credit Services |
| 13 | PODD | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Devices |
| 14 | EMBC | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Medical Instruments & Supplies |
| 15 | SEZL | +14 | 17 | 3 | 2026-10-14→2026-10-15 | Credit Services |

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
| `B08_target_price_delta` | delta=0.0 (now=10.54 vs prior_export=10.54 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 12.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.52 (this export) | prior_export=72.52 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.549999237060547  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.72 on 2026-10-12; prev RSI=57.75 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=8.166 (G=0.4900 R=0.0600); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.297 (Gvol=849222 Rvol=654764); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A07_rvol` | RVOL=0.785 on 2026-10-12: today_vol=654764 / avg20=834281 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.229 on 2026-10-12 (price=16.5500, mid=16.3710, upper=17.1537, lower=15.5883; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=16.5500 vs SMA50=16.1181 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.37_50=16.12_80=16.12 on 2026-10-12: SMA20=16.3710 SMA50=16.1181 SMA80=16.1191 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-31@14.3630; S2[2026-08-13→2026-09-11] low=2026-08-14@14.9010; S3[2026-09-14→2026-10-12] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5764713538160505 wick_frac=0.42352864618394953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.2754882213854103 wick_frac=0.7245117786145897 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.16647593616886 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=422.0899963378906  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.14 on 2026-10-12; prev RSI=59.13 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=17.8700 R=0.0000); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2928672 Rvol=0); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A07_rvol` | RVOL=0.737 on 2026-10-12: today_vol=869447 / avg20=1180302 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.925 on 2026-10-12 (price=422.0900, mid=401.9415, upper=423.7173, lower=380.1657; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=422.0900 vs SMA50=400.5118 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=401.9415 SMA50=400.5118 SMA80=398.6746 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-28@362.9800; S2[2026-08-13→2026-09-11] low=2026-08-17@388.1200; S3[2026-09-14→2026-10-12] low=2026-10-01@375.7400 | lows=[362.9800109863281, 388.1199951171875, 375.739990234375] span=6.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6505804361528325 wick_frac=0.34941956384716766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+3.4100:wick=3.1374; 2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=11.98 vs prior_export=11.98 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 4.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/11/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.44 (this export) | prior_export=1.44 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.46 (this export) | prior_export=1.46 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
price=51.970001220703125  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.48 on 2026-10-12; prev RSI=42.64 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6400 R=0.0000); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1914338 Rvol=0); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A07_rvol` | RVOL=0.622 on 2026-10-12: today_vol=717503 / avg20=1153282 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.033 on 2026-10-12 (price=51.9700, mid=52.1205, upper=56.6668, lower=47.5742; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=51.9700 vs SMA50=51.7088 dist=+0.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=52.1205 SMA50=51.7088 SMA80=50.5664 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-08-12@47.8600; S2[2026-08-13→2026-09-11] low=2026-08-13@48.1100; S3[2026-09-14→2026-10-12] low=2026-10-02@48.3600 | lows=[47.86000061035156, 48.11000061035156, 48.36000061035156] span=1.04% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.37251635024746754 wick_frac=0.6274836497525325 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.1800:wick=1.0800; 2026-10-07:RED:body=-1.8300:wick=0.3050; 2026-10-08:RED:body=-0.2900:wick=0.9400; 2026-10-09:GREEN:body=+0.3600:wick=1.6550; 2026-10-12:GREEN:body=+1.2800:wick=0.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+14**  ·  Software - Application
price=239.89999389648438  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.86 on 2026-10-16; prev RSI=65.07 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.07@2026-10-15 → 59.86@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.07@2026-10-15 → 59.86@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.07@2026-10-15 → 59.86@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=1.352 (G=10.9800 R=8.1200); 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0; 2026-10-16:RED:O=248.0200,C=239.9000,body=-8.1200,vol=2039690.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.188 (Gvol=2422648 Rvol=2039690); 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0; 2026-10-16:RED:O=248.0200,C=239.9000,body=-8.1200,vol=2039690.0 | **GOOD** |
| `A07_rvol` | RVOL=0.980 on 2026-10-16: today_vol=2039690 / avg20=2081024 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.684 on 2026-10-16 (price=239.9000, mid=222.8030, upper=247.8048, lower=197.8012; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=239.9000 vs SMA50=233.3136 dist=+2.82% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=222.80_50=233.31_80=225.67 on 2026-10-16: SMA20=222.8030 SMA50=233.3136 SMA80=225.6742 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-16 (63 bars); S1[2026-07-16→2026-08-18] low=2026-07-23@202.3700; S2[2026-08-19→2026-09-17] low=2026-09-09@206.3300; S3[2026-09-18→2026-10-16] low=2026-09-29@199.1801 | lows=[202.3699951171875, 206.3300018310547, 199.1800994873047] span=3.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.8635470681910641 wick_frac=0.1364529318089359 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.5814538942385964 wick_frac=0.41854610576140366 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.3522163738624042 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-1.1630:wick=6.7920; 2026-10-13:RED:body=-2.3100:wick=3.4000; 2026-10-14:GREEN:body=+0.7900:wick=2.9300; 2026-10-15:GREEN:body=+10.9800:wick=1.7350; 2026-10-16:RED:body=-8.1200:wick=5.8450 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MTDR  ·  score **+14**  ·  Oil & Gas E&P
price=57.52000045776367  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.47 on 2026-10-16; prev RSI=46.72 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.72@2026-10-15 → 59.47@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.72@2026-10-15 → 59.47@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.72@2026-10-15 → 59.47@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=2.467 (G=3.3300 R=1.3500); 2026-10-15:RED:O=55.1200,C=53.7700,body=-1.3500,vol=1449960.0; 2026-10-16:GREEN:O=54.1900,C=57.5200,body=+3.3300,vol=1939641.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.338 (Gvol=1939641 Rvol=1449960); 2026-10-15:RED:O=55.1200,C=53.7700,body=-1.3500,vol=1449960.0; 2026-10-16:GREEN:O=54.1900,C=57.5200,body=+3.3300,vol=1939641.0 | **GOOD** |
| `A07_rvol` | RVOL=1.280 on 2026-10-16: today_vol=1939641 / avg20=1514823 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.217 on 2026-10-16 (price=57.5200, mid=53.3885, upper=56.7844, lower=49.9926; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-16: price=57.5200 vs SMA50=55.6773 dist=+3.31% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=53.39_50=55.68_80=53.55 on 2026-10-16: SMA20=53.3885 SMA50=55.6773 SMA80=53.5462 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-16 (63 bars); S1[2026-07-17→2026-08-18] low=2026-07-28@45.0927; S2[2026-08-19→2026-09-17] low=2026-08-26@54.2700; S3[2026-09-18→2026-10-16] low=2026-09-29@50.3900 | lows=[45.09266872932533, 54.27000045776367, 50.38999938964844] span=20.35% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=1.010622102076375 wick_frac=-0.01062210207637494 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.6094800817705706 wick_frac=0.3905199182294295 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4666708110338122 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-0.1300:wick=0.8750; 2026-10-13:GREEN:body=+0.4200:wick=0.5500; 2026-10-14:RED:body=-1.2000:wick=0.1600; 2026-10-15:RED:body=-1.3500:wick=0.8650; 2026-10-16:GREEN:body=+3.3300:wick=-0.0350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.59 vs prior_export=70.59 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.4 vs prior=0.4 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 11.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+14**  ·  Banks - Regional
price=14.859999656677246  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=43.30 on 2026-10-12; prev RSI=37.21 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=1.700 (G=0.1700 R=0.1000); 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0; 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.380 (Gvol=991454 Rvol=718370); 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0; 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0 | **GOOD** |
| `A07_rvol` | RVOL=0.846 on 2026-10-12: today_vol=991454 / avg20=1171580 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.466 on 2026-10-12 (price=14.8600, mid=15.1030, upper=15.6239, lower=14.5821; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=14.8600 vs SMA50=15.3452 dist=-3.16% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=15.10_50=15.35_80=15.19 on 2026-10-12: SMA20=15.1030 SMA50=15.3452 SMA80=15.1925 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-10@14.7133; S2[2026-08-13→2026-09-11] low=2026-09-01@15.0700; S3[2026-09-14→2026-10-12] low=2026-10-01@14.5400 | lows=[14.713282220409273, 15.069999694824219, 14.539999961853027] span=3.65% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.8947347287055163 wick_frac=0.10526527129448376 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.4878069204216638 wick_frac=0.5121930795783363 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6999942779759294 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.1500:wick=0.1150; 2026-10-07:RED:body=-0.0800:wick=0.1000; 2026-10-08:GREEN:body=+0.1500:wick=0.1500; 2026-10-09:RED:body=-0.1000:wick=0.1050; 2026-10-12:GREEN:body=+0.1700:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AFRM  ·  score **+14**  ·  Credit Services
price=74.01000213623047  pair=`2026-10-15→2026-10-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.34 on 2026-10-16; prev RSI=53.49 on 2026-10-15 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.49@2026-10-15 → 50.34@2026-10-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.49@2026-10-15 → 50.34@2026-10-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.49@2026-10-15 → 50.34@2026-10-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_body_sum/RED_body_sum=3.256 (G=7.2600 R=2.2300); 2026-10-15:GREEN:O=68.6600,C=75.9200,body=+7.2600,vol=5909777.0; 2026-10-16:RED:O=76.2400,C=74.0100,body=-2.2300,vol=3630160.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-15 + 2026-10-16; ratio=GREEN_vol/RED_vol=1.628 (Gvol=5909777 Rvol=3630160); 2026-10-15:GREEN:O=68.6600,C=75.9200,body=+7.2600,vol=5909777.0; 2026-10-16:RED:O=76.2400,C=74.0100,body=-2.2300,vol=3630160.0 | **GOOD** |
| `A07_rvol` | RVOL=0.859 on 2026-10-16: today_vol=3630160 / avg20=4225016 (avg window 2026-09-18→2026-10-15, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.130 on 2026-10-16 (price=74.0100, mid=73.0345, upper=80.5240, lower=65.5450; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-16: price=74.0100 vs SMA50=73.7122 dist=+0.40% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-16: SMA20=73.0345 SMA50=73.7122 SMA80=75.4533 | **BAD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-16 (63 bars); S1[2026-07-20→2026-08-18] low=2026-07-29@69.6200; S2[2026-08-19→2026-09-17] low=2026-09-10@66.0300; S3[2026-09-18→2026-10-16] low=2026-09-23@66.6250 | lows=[69.62000274658203, 66.02999877929688, 66.625] span=5.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: GREEN body_frac=0.9331613940808441 wick_frac=0.06683860591915586 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-15+2026-10-16: RED body_frac=0.4058230589803399 wick_frac=0.5941769410196601 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.255609155290978 need>1.4; red_wick_gt_green=True 5d trail=2026-10-12:RED:body=-1.8600:wick=1.3450; 2026-10-13:RED:body=-0.6500:wick=1.3400; 2026-10-14:RED:body=-5.2000:wick=2.1850; 2026-10-15:GREEN:body=+7.2600:wick=0.5200; 2026-10-16:RED:body=-2.2300:wick=3.2650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1230.26 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.32 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3949.22 | **NEUTRAL** |
| `B04_income` | 1929.79 | **GOOD** |
| `B05_profit_margin` | 48.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.84 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.84 vs prior_export=99.84 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | -0.63 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.63 vs prior=-0.63 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.37 | **GOOD** |
| `B13_short_float` | 5.74 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1230.26 (this export) | prior_export=1230.26 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+14**  ·  Medical Devices
price=139.80999755859375  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.20 on 2026-10-12; prev RSI=44.78 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.9300 R=0.0000); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2183896 Rvol=0); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A07_rvol` | RVOL=0.819 on 2026-10-12: today_vol=1319431 / avg20=1610443 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-10-12 (price=139.8100, mid=136.1735, upper=143.1581, lower=129.1889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=139.8100 vs SMA50=141.5730 dist=-1.25% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=136.1735 SMA50=141.5730 SMA80=147.5865 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-05@126.4000; S2[2026-08-13→2026-09-11] low=2026-09-11@131.2400; S3[2026-09-14→2026-10-12] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5660465898505131 wick_frac=0.43395341014948685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.0400:wick=1.3950; 2026-10-07:RED:body=-0.5800:wick=3.1000; 2026-10-08:GREEN:body=+0.5100:wick=1.2300; 2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBC  ·  score **+14**  ·  Medical Instruments & Supplies
price=5.989999771118164  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.09 on 2026-10-12; prev RSI=55.18 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.2200 R=0.0550); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.807 (Gvol=1053433 Rvol=583035); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A07_rvol` | RVOL=1.019 on 2026-10-12: today_vol=1053433 / avg20=1033961 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.624 on 2026-10-12 (price=5.9900, mid=5.6322, upper=6.2055, lower=5.0590; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=5.9900 vs SMA50=5.2233 dist=+14.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=5.6322 SMA50=5.2233 SMA80=4.4932 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-14@3.0700; S2[2026-08-13→2026-09-11] low=2026-08-13@4.7200; S3[2026-09-14→2026-10-12] low=2026-09-14@4.9800 | lows=[3.069999933242798, 4.71999979019165, 4.980000019073486] span=62.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6470579160542584 wick_frac=0.35294208394574156 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.19642814568070271 wick_frac=0.8035718543192972 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.000008669793572 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.0500:wick=0.2950; 2026-10-07:GREEN:body=+0.0500:wick=0.2500; 2026-10-08:RED:body=-0.1400:wick=0.1100; 2026-10-09:RED:body=-0.0550:wick=0.2250; 2026-10-12:GREEN:body=+0.2200:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=110.68 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.75 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1018.7 | **NEUTRAL** |
| `B04_income` | 87.5 | **GOOD** |
| `B05_profit_margin` | 8.59 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=4.0 vs prior_export=4.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | -16.39 | **BAD** |
| `B13_short_float` | 10.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=110.68 (this export) | prior_export=110.68 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.75 (this export) | prior_export=6.75 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SEZL  ·  score **+14**  ·  Credit Services
price=132.3699951171875  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.93 on 2026-10-15; prev RSI=54.94 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.94@2026-10-14 → 62.93@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=3.118 (G=11.1300 R=3.5700); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.009 (Gvol=462162 Rvol=457963); 2026-10-14:RED:O=126.6800,C=123.1100,body=-3.5700,vol=457963.0; 2026-10-15:GREEN:O=121.2400,C=132.3700,body=+11.1300,vol=462162.0 | **GOOD** |
| `A07_rvol` | RVOL=0.843 on 2026-10-15: today_vol=462162 / avg20=548541 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.975 on 2026-10-15 (price=132.3700, mid=115.5997, upper=132.7973, lower=98.4022; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=132.3700 vs SMA50=119.8667 dist=+10.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-15: SMA20=115.5997 SMA50=119.8667 SMA80=138.2212 | **BAD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@114.1600; S2[2026-08-18→2026-09-16] low=2026-09-01@112.4100; S3[2026-09-17→2026-10-15] low=2026-09-24@104.8700 | lows=[114.16000366210938, 112.41000366210938, 104.87000274658203] span=8.86% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.9823466866705229 wick_frac=0.017653313329477113 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.38867693830950656 wick_frac=0.6113230616904934 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.117646555979886 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+8.6400:wick=1.6700; 2026-10-12:GREEN:body=+2.7300:wick=3.1975; 2026-10-13:RED:body=-1.1400:wick=5.0900; 2026-10-14:RED:body=-3.5700:wick=5.6150; 2026-10-15:GREEN:body=+11.1300:wick=0.2000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.7 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.81 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 531.89 | **NEUTRAL** |
| `B04_income` | 161.43 | **GOOD** |
| `B05_profit_margin` | 30.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 168.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=168.0 vs prior_export=168.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -1.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.0 vs prior=-1.0 on finviz_2026-10-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.93 | **GOOD** |
| `B13_short_float` | 25.11 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.7 (this export) | prior_export=9.7 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.81 (this export) | prior_export=10.81 (finviz_2026-10-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-18_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.