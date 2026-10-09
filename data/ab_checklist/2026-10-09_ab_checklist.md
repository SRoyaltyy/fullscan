# A+B1 Feature Checklist — 2026-10-09

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-09.csv` · prior export for Δ: `2026-10-08`
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
| 1 | AAPL | +18 | 19 | 1 | 2026-10-07→2026-10-08 | Consumer Electronics |
| 2 | SB | +17 | 17 | 0 | 2026-10-07→2026-10-08 | Marine Shipping |
| 3 | ESTC | +16 | 18 | 2 | 2026-10-07→2026-10-08 | Software - Application |
| 4 | MDLN | +16 | 18 | 2 | 2026-10-07→2026-10-08 | Medical Instruments & Supplies |
| 5 | AUPH | +16 | 16 | 0 | 2026-10-07→2026-10-08 | Biotechnology |
| 6 | ANET | +16 | 18 | 2 | 2026-10-07→2026-10-08 | Computer Hardware |
| 7 | GCT | +15 | 17 | 2 | 2026-10-07→2026-10-08 | Software - Infrastructure |
| 8 | AMCR | +15 | 16 | 1 | 2026-10-07→2026-10-08 | Packaging & Containers |
| 9 | AXTA | +15 | 16 | 1 | 2026-10-07→2026-10-08 | Specialty Chemicals |
| 10 | OXY | +15 | 16 | 1 | 2026-10-07→2026-10-08 | Oil & Gas E&P |
| 11 | SU | +15 | 17 | 2 | 2026-10-07→2026-10-08 | Oil & Gas Integrated |
| 12 | SBLK | +15 | 17 | 2 | 2026-10-07→2026-10-08 | Marine Shipping |
| 13 | RYN | +15 | 18 | 3 | 2026-10-07→2026-10-08 | REIT - Specialty |
| 14 | CNP | +15 | 17 | 2 | 2026-10-07→2026-10-08 | Utilities - Regulated Electric |
| 15 | ARCO | +15 | 17 | 2 | 2026-10-07→2026-10-08 | Restaurants |

## Full checklist — top 15

### AAPL  ·  score **+18**  ·  Consumer Electronics
price=340.4200134277344  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.23 on 2026-10-08; prev RSI=57.70 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.70@2026-10-07 → 61.23@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.70@2026-10-07 → 61.23@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.70@2026-10-07 → 61.23@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=12.415 (G=3.6000 R=0.2900); 2026-10-07:RED:O=336.9600,C=336.6700,body=-0.2900,vol=33312459.0; 2026-10-08:GREEN:O=336.8200,C=340.4200,body=+3.6000,vol=35279900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.059 (Gvol=35279900 Rvol=33312459); 2026-10-07:RED:O=336.9600,C=336.6700,body=-0.2900,vol=33312459.0; 2026-10-08:GREEN:O=336.8200,C=340.4200,body=+3.6000,vol=35279900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.884 on 2026-10-08: today_vol=35279900 / avg20=39894214 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.763 on 2026-10-08 (price=340.4200, mid=335.1705, upper=342.0540, lower=328.2870; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=340.4200 vs SMA50=322.2866 dist=+5.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=335.1705 SMA50=322.2866 SMA80=318.2737 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-08 (63 bars); S1[2026-07-13→2026-08-10] low=2026-07-31@299.7415; S2[2026-08-11→2026-09-09] low=2026-08-12@300.5700; S3[2026-09-10→2026-10-08] low=2026-09-10@316.5100 | lows=[299.7415030462749, 300.57000732421875, 316.510009765625] span=5.59% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.63492020775586 wick_frac=0.36507979224414006 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.04931595009238307 wick_frac=0.9506840499076169 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.414754788465586 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+0.4300:wick=3.5000; 2026-10-05:GREEN:body=+0.0950:wick=4.4450; 2026-10-06:GREEN:body=+1.3500:wick=2.4100; 2026-10-07:RED:body=-0.2900:wick=5.5900; 2026-10-08:GREEN:body=+3.6000:wick=2.0700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.9300000000000068 (now=337.68 vs prior_export=335.75 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.9400000000000004 (now=-2.28 vs prior=-3.22 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SB  ·  score **+17**  ·  Marine Shipping
price=8.819999694824219  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.93 on 2026-10-08; prev RSI=48.79 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.79@2026-10-07 → 55.93@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.79@2026-10-07 → 55.93@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.79@2026-10-07 → 55.93@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=4.500 (G=0.2700 R=0.0600); 2026-10-07:RED:O=8.5100,C=8.4500,body=-0.0600,vol=939931.0; 2026-10-08:GREEN:O=8.5500,C=8.8200,body=+0.2700,vol=1414200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.505 (Gvol=1414200 Rvol=939931); 2026-10-07:RED:O=8.5100,C=8.4500,body=-0.0600,vol=939931.0; 2026-10-08:GREEN:O=8.5500,C=8.8200,body=+0.2700,vol=1414200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.866 on 2026-10-08: today_vol=1414200 / avg20=1633174 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.390 on 2026-10-08 (price=8.8200, mid=8.5980, upper=9.1677, lower=8.0283; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=8.8200 vs SMA50=8.3396 dist=+5.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=8.5980 SMA50=8.3396 SMA80=7.7586 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-08 (63 bars); S1[2026-07-09→2026-08-10] low=2026-07-09@6.4446; S2[2026-08-11→2026-09-09] low=2026-08-11@7.1672; S3[2026-09-10→2026-10-08] low=2026-09-25@7.9917 | lows=[6.444550970525013, 7.167211325967238, 7.991700172424316] span=24.01% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.8999977747613431 wick_frac=0.10000222523865684 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.2553213075502709 wick_frac=0.7446786924497291 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.499960263848049 need>1.4; red_wick_gt_green=False 5d trail=2026-10-02:RED:body=-0.0200:wick=0.1750; 2026-10-05:GREEN:body=+0.0300:wick=0.2841; 2026-10-06:RED:body=-0.1300:wick=0.0950; 2026-10-07:RED:body=-0.0600:wick=0.1750; 2026-10-08:GREEN:body=+0.2700:wick=0.0300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.0 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.86 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 307.5 | **NEUTRAL** |
| `B04_income` | 78.98 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.71 vs prior_export=8.71 on finviz_2026-10-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.82 | **GOOD** |
| `B13_short_float` | 5.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.0 (this export) | prior_export=12.0 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.86 (this export) | prior_export=12.86 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESTC  ·  score **+16**  ·  Software - Application
price=95.26000213623047  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.36 on 2026-10-08; prev RSI=58.90 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.90@2026-10-07 → 62.36@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.90@2026-10-07 → 62.36@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.90@2026-10-07 → 62.36@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=2.583 (G=1.8600 R=0.7200); 2026-10-07:RED:O=93.8200,C=93.1000,body=-0.7200,vol=945420.0; 2026-10-08:GREEN:O=93.4000,C=95.2600,body=+1.8600,vol=1610400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.703 (Gvol=1610400 Rvol=945420); 2026-10-07:RED:O=93.8200,C=93.1000,body=-0.7200,vol=945420.0; 2026-10-08:GREEN:O=93.4000,C=95.2600,body=+1.8600,vol=1610400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.876 on 2026-10-08: today_vol=1610400 / avg20=1838554 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.755 on 2026-10-08 (price=95.2600, mid=89.8395, upper=97.0214, lower=82.6576; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=95.2600 vs SMA50=85.2542 dist=+11.74% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=89.8395 SMA50=85.2542 SMA80=75.6638 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-08 (63 bars); S1[2026-07-08→2026-08-10] low=2026-07-23@56.0600; S2[2026-08-11→2026-09-09] low=2026-08-12@74.6400; S3[2026-09-10→2026-10-08] low=2026-09-11@82.1500 | lows=[56.060001373291016, 74.63999938964844, 82.1500015258789] span=46.54% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.744000244140625 wick_frac=0.255999755859375 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.46451600200824955 wick_frac=0.5354839979917504 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.583329801212224 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+1.3600:wick=0.5250; 2026-10-05:GREEN:body=+2.3800:wick=1.1167; 2026-10-06:RED:body=-1.4800:wick=2.8100; 2026-10-07:RED:body=-0.7200:wick=0.8300; 2026-10-08:GREEN:body=+1.8600:wick=0.6400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.84 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.78 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.16 | **NEUTRAL** |
| `B04_income` | 375.64 | **GOOD** |
| `B05_profit_margin` | 20.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.539999999999992 (now=110.42 vs prior_export=110.96 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=8.55 (now=-8.05 vs prior=-16.6 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 4.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.84 (this export) | prior_export=19.84 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDLN  ·  score **+16**  ·  Medical Instruments & Supplies
price=35.7400016784668  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.13 on 2026-10-08; prev RSI=55.27 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.27@2026-10-07 → 57.13@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.27@2026-10-07 → 57.13@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.27@2026-10-07 → 57.13@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=6.100 (G=0.6100 R=0.1000); 2026-10-07:RED:O=35.5500,C=35.4500,body=-0.1000,vol=4467389.0; 2026-10-08:GREEN:O=35.1300,C=35.7400,body=+0.6100,vol=10191300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=2.281 (Gvol=10191300 Rvol=4467389); 2026-10-07:RED:O=35.5500,C=35.4500,body=-0.1000,vol=4467389.0; 2026-10-08:GREEN:O=35.1300,C=35.7400,body=+0.6100,vol=10191300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.092 on 2026-10-08: today_vol=10191300 / avg20=9333183 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.568 on 2026-10-08 (price=35.7400, mid=34.0935, upper=36.9909, lower=31.1961; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=35.7400 vs SMA50=35.0201 dist=+2.06% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-08: SMA20=34.0935 SMA50=35.0201 SMA80=36.5149 | **BAD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-08 (63 bars); S1[2026-07-08→2026-08-10] low=2026-08-10@33.5300; S2[2026-08-11→2026-09-09] low=2026-08-20@32.4850; S3[2026-09-10→2026-10-08] low=2026-09-18@31.4900 | lows=[33.529998779296875, 32.48500061035156, 31.489999771118164] span=6.48% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.6354153835149945 wick_frac=0.3645846164850055 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.13157656979370577 wick_frac=0.8684234302062942 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.100099183642328 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+0.1800:wick=0.6600; 2026-10-05:GREEN:body=+0.3600:wick=0.7000; 2026-10-06:RED:body=-0.4000:wick=1.1750; 2026-10-07:RED:body=-0.1000:wick=0.6600; 2026-10-08:GREEN:body=+0.6100:wick=0.3500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=41.88 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.21 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 29939.0 | **NEUTRAL** |
| `B04_income` | 693.0 | **GOOD** |
| `B05_profit_margin` | 2.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.11999999999999744 (now=44.5 vs prior_export=44.38 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.59 | **GOOD** |
| `B10_insider_transactions` | -15.76 | **BAD** |
| `B11_insider_tx_delta` | delta=2.58 (now=-15.76 vs prior=-18.34 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 10.52 | **GOOD** |
| `B13_short_float` | 8.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=41.88 (this export) | prior_export=41.88 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.21 (this export) | prior_export=2.21 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.219999313354492  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.56 on 2026-10-08; prev RSI=46.42 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.42@2026-10-07 → 50.56@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.42@2026-10-07 → 50.56@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.42@2026-10-07 → 50.56@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7600 R=0.0000); 2026-10-07:GREEN:O=15.5600,C=16.0100,body=+0.4500,vol=682526.0; 2026-10-08:GREEN:O=15.9100,C=16.2200,body=+0.3100,vol=831000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1513526 Rvol=0); 2026-10-07:GREEN:O=15.5600,C=16.0100,body=+0.4500,vol=682526.0; 2026-10-08:GREEN:O=15.9100,C=16.2200,body=+0.3100,vol=831000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.011 on 2026-10-08: today_vol=831000 / avg20=821910 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.150 on 2026-10-08 (price=16.2200, mid=16.3365, upper=17.1113, lower=15.5617; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=16.2200 vs SMA50=16.0517 dist=+1.05% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.34_50=16.05_80=16.11 on 2026-10-08: SMA20=16.3365 SMA50=16.0517 SMA80=16.1068 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-08 (63 bars); S1[2026-07-10→2026-08-10] low=2026-07-31@14.3630; S2[2026-08-11→2026-09-09] low=2026-08-14@14.9010; S3[2026-09-10→2026-10-08] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.6561539598048802 wick_frac=0.34384604019511966 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-02:RED:body=-0.2300:wick=0.0900; 2026-10-05:RED:body=-0.0400:wick=0.1600; 2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+16**  ·  Computer Hardware
price=210.97000122070312  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.04 on 2026-10-08; prev RSI=67.17 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.17@2026-10-07 → 60.04@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.17@2026-10-07 → 60.04@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.17@2026-10-07 → 60.04@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=1.794 (G=4.5300 R=2.5250); 2026-10-07:GREEN:O=211.3000,C=215.8300,body=+4.5300,vol=4987941.0; 2026-10-08:RED:O=213.4950,C=210.9700,body=-2.5250,vol=4875100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.023 (Gvol=4987941 Rvol=4875100); 2026-10-07:GREEN:O=211.3000,C=215.8300,body=+4.5300,vol=4987941.0; 2026-10-08:RED:O=213.4950,C=210.9700,body=-2.5250,vol=4875100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.992 on 2026-10-08: today_vol=4875100 / avg20=4913433 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.542 on 2026-10-08 (price=210.9700, mid=203.7650, upper=217.0699, lower=190.4601; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=210.9700 vs SMA50=196.9494 dist=+7.12% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=203.7650 SMA50=196.9494 SMA80=187.0664 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-08 (63 bars); S1[2026-07-13→2026-08-10] low=2026-07-29@156.8400; S2[2026-08-11→2026-09-09] low=2026-09-03@181.2700; S3[2026-09-10→2026-10-08] low=2026-09-14@186.1100 | lows=[156.83999633789062, 181.27000427246094, 186.11000061035156] span=18.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.5532286798303481 wick_frac=0.446771320169652 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.32330214501882437 wick_frac=0.6766978549811756 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7940632591643602 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:RED:body=-0.9100:wick=3.0623; 2026-10-05:RED:body=-0.8600:wick=3.5379; 2026-10-06:GREEN:body=+7.3600:wick=1.4700; 2026-10-07:GREEN:body=+4.5300:wick=3.6583; 2026-10-08:RED:body=-2.5250:wick=5.2850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 248.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-1.5699999999999932 (now=248.86 vs prior_export=250.43 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -3.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.23999999999999977 (now=-3.24 vs prior=-3.48 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.66 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+15**  ·  Software - Infrastructure
price=56.41999816894531  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.89 on 2026-10-08; prev RSI=64.62 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.62@2026-10-07 → 65.89@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.62@2026-10-07 → 65.89@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.62@2026-10-07 → 65.89@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.8180 R=0.0000); 2026-10-07:GREEN:O=53.1920,C=55.9600,body=+2.7680,vol=1000534.0; 2026-10-08:GREEN:O=55.3700,C=56.4200,body=+1.0500,vol=1021100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2021634 Rvol=0); 2026-10-07:GREEN:O=53.1920,C=55.9600,body=+2.7680,vol=1000534.0; 2026-10-08:GREEN:O=55.3700,C=56.4200,body=+1.0500,vol=1021100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.528 on 2026-10-08: today_vol=1021100 / avg20=668279 (avg window 2026-09-10→2026-10-07, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.098 on 2026-10-08 (price=56.4200, mid=53.2482, upper=56.1379, lower=50.3586; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-08: price=56.4200 vs SMA50=51.3431 dist=+9.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=53.2482 SMA50=51.3431 SMA80=45.1761 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-08 (63 bars); S1[2026-07-09→2026-08-10] low=2026-07-09@33.7500; S2[2026-08-11→2026-09-09] low=2026-09-01@46.0100; S3[2026-09-10→2026-10-08] low=2026-09-22@49.3400 | lows=[33.75, 46.0099983215332, 49.34000015258789] span=46.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.6102533492495126 wick_frac=0.3897466507504875 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-02:RED:body=-0.1800:wick=1.0800; 2026-10-05:GREEN:body=+0.1600:wick=1.4150; 2026-10-06:GREEN:body=+0.1200:wick=1.6700; 2026-10-07:GREEN:body=+2.7680:wick=0.7350; 2026-10-08:GREEN:body=+1.0500:wick=1.3900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.170000000000002 (now=65.5 vs prior_export=61.33 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMCR  ·  score **+15**  ·  Packaging & Containers
price=41.880001068115234  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=41.11 on 2026-10-08; prev RSI=34.34 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 34.34@2026-10-07 → 41.11@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 34.34@2026-10-07 → 41.11@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 34.34@2026-10-07 → 41.11@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=3.259 (G=0.8800 R=0.2700); 2026-10-07:RED:O=41.5300,C=41.2600,body=-0.2700,vol=2854130.0; 2026-10-08:GREEN:O=41.0000,C=41.8800,body=+0.8800,vol=4923900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.725 (Gvol=4923900 Rvol=2854130); 2026-10-07:RED:O=41.5300,C=41.2600,body=-0.2700,vol=2854130.0; 2026-10-08:GREEN:O=41.0000,C=41.8800,body=+0.8800,vol=4923900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.520 on 2026-10-08: today_vol=4923900 / avg20=3239906 (avg window 2026-09-10→2026-10-07, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.328 on 2026-10-08 (price=41.8800, mid=42.1785, upper=43.0882, lower=41.2688; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-08: price=41.8800 vs SMA50=44.6714 dist=-6.25% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=42.18_50=44.67_80=44.09 on 2026-10-08: SMA20=42.1785 SMA50=44.6714 SMA80=44.0936 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-08 (63 bars); S1[2026-07-10→2026-08-10] low=2026-07-23@41.9400; S2[2026-08-11→2026-09-09] low=2026-09-09@43.0400; S3[2026-09-10→2026-10-08] low=2026-10-08@40.7900 | lows=[41.939998626708984, 43.040000915527344, 40.790000915527344] span=5.52% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.8073403164448426 wick_frac=0.19265968355515736 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.43206929810638894 wick_frac=0.5679307018936111 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2592576894276553 need>1.4; red_wick_gt_green=False 5d trail=2026-10-02:RED:body=-0.3200:wick=0.2500; 2026-10-05:GREEN:body=+0.3000:wick=0.7400; 2026-10-06:GREEN:body=+0.0300:wick=0.6200; 2026-10-07:RED:body=-0.2700:wick=0.3549; 2026-10-08:GREEN:body=+0.8800:wick=0.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.19 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23506.0 | **NEUTRAL** |
| `B04_income` | 1106.0 | **GOOD** |
| `B05_profit_margin` | 4.71 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.63 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.28000000000000114 (now=49.63 vs prior_export=49.35 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.09 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.84 | **GOOD** |
| `B13_short_float` | 4.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.19 (this export) | prior_export=3.19 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AXTA  ·  score **+15**  ·  Specialty Chemicals
price=32.810001373291016  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=45.29 on 2026-10-08; prev RSI=45.11 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.11@2026-10-07 → 45.29@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 45.11@2026-10-07 → 45.29@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 45.11@2026-10-07 → 45.29@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=1.778 (G=0.4800 R=0.2700); 2026-10-07:RED:O=33.0600,C=32.7900,body=-0.2700,vol=1393602.0; 2026-10-08:GREEN:O=32.3300,C=32.8100,body=+0.4800,vol=1985600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.425 (Gvol=1985600 Rvol=1393602); 2026-10-07:RED:O=33.0600,C=32.7900,body=-0.2700,vol=1393602.0; 2026-10-08:GREEN:O=32.3300,C=32.8100,body=+0.4800,vol=1985600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.905 on 2026-10-08: today_vol=1985600 / avg20=2194612 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.053 on 2026-10-08 (price=32.8100, mid=32.7465, upper=33.9503, lower=31.5427; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-08: price=32.8100 vs SMA50=34.9030 dist=-6.00% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=32.75_50=34.90_80=34.38 on 2026-10-08: SMA20=32.7465 SMA50=34.9030 SMA80=34.3834 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-08 (63 bars); S1[2026-07-08→2026-08-10] low=2026-07-23@31.5200; S2[2026-08-11→2026-09-09] low=2026-09-09@34.2900; S3[2026-09-10→2026-10-08] low=2026-10-01@31.2000 | lows=[31.520000457763672, 34.290000915527344, 31.200000762939453] span=9.90% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.7272722018322111 wick_frac=0.2727277981677889 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.47787807792804043 wick_frac=0.5221219220719595 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7777730682829653 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+0.0300:wick=0.5000; 2026-10-05:RED:body=-0.1500:wick=0.8150; 2026-10-06:GREEN:body=+0.2700:wick=0.3750; 2026-10-07:RED:body=-0.2700:wick=0.2950; 2026-10-08:GREEN:body=+0.4800:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.21 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.45 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5150.0 | **NEUTRAL** |
| `B04_income` | 349.0 | **GOOD** |
| `B05_profit_margin` | 6.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.19999999999999574 (now=39.33 vs prior_export=39.13 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 3.74 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.21 (this export) | prior_export=11.21 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OXY  ·  score **+15**  ·  Oil & Gas E&P
price=60.279998779296875  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.44 on 2026-10-08; prev RSI=50.06 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.06@2026-10-07 → 58.44@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.06@2026-10-07 → 58.44@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.06@2026-10-07 → 58.44@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=1.327 (G=0.6500 R=0.4900); 2026-10-07:RED:O=58.7000,C=58.2100,body=-0.4900,vol=7090183.0; 2026-10-08:GREEN:O=59.6300,C=60.2800,body=+0.6500,vol=11842000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.670 (Gvol=11842000 Rvol=7090183); 2026-10-07:RED:O=58.7000,C=58.2100,body=-0.4900,vol=7090183.0; 2026-10-08:GREEN:O=59.6300,C=60.2800,body=+0.6500,vol=11842000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.249 on 2026-10-08: today_vol=11842000 / avg20=9478324 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.440 on 2026-10-08 (price=60.2800, mid=58.3745, upper=62.7009, lower=54.0481; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=60.2800 vs SMA50=58.6422 dist=+2.79% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=58.37_50=58.64_80=56.52 on 2026-10-08: SMA20=58.3745 SMA50=58.6422 SMA80=56.5198 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-08 (63 bars); S1[2026-07-13→2026-08-10] low=2026-07-15@52.9500; S2[2026-08-11→2026-09-09] low=2026-08-13@57.1800; S3[2026-09-10→2026-10-08] low=2026-09-29@54.7600 | lows=[52.95000076293945, 57.18000030517578, 54.7599983215332] span=7.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.6190458890249263 wick_frac=0.38095411097507365 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.33222635184940913 wick_frac=0.6677736481505908 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.3265213972643264 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+1.1750:wick=0.1650; 2026-10-05:GREEN:body=+0.4000:wick=1.6050; 2026-10-06:GREEN:body=+0.8000:wick=0.6000; 2026-10-07:RED:body=-0.4900:wick=0.9849; 2026-10-08:GREEN:body=+0.6500:wick=0.4000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=31.47 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.0 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 24313.0 | **NEUTRAL** |
| `B04_income` | 3412.0 | **GOOD** |
| `B05_profit_margin` | 14.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.39000000000000057 (now=68.45 vs prior_export=68.84 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.13 vs prior=0.13 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.47 (this export) | prior_export=31.47 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.0 (this export) | prior_export=14.0 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SU  ·  score **+15**  ·  Oil & Gas Integrated
price=70.91000366210938  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.82 on 2026-10-08; prev RSI=50.52 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.52@2026-10-07 → 60.82@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.52@2026-10-07 → 60.82@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.52@2026-10-07 → 60.82@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=2.110 (G=1.5400 R=0.7300); 2026-10-07:RED:O=68.8700,C=68.1400,body=-0.7300,vol=2907711.0; 2026-10-08:GREEN:O=69.3700,C=70.9100,body=+1.5400,vol=3843900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.322 (Gvol=3843900 Rvol=2907711); 2026-10-07:RED:O=68.8700,C=68.1400,body=-0.7300,vol=2907711.0; 2026-10-08:GREEN:O=69.3700,C=70.9100,body=+1.5400,vol=3843900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.038 on 2026-10-08: today_vol=3843900 / avg20=3704891 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.927 on 2026-10-08 (price=70.9100, mid=68.7560, upper=71.0796, lower=66.4324; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-08: price=70.9100 vs SMA50=67.1800 dist=+5.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=68.7560 SMA50=67.1800 SMA80=64.1419 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-08 (63 bars); S1[2026-07-13→2026-08-10] low=2026-07-15@59.8300; S2[2026-08-11→2026-09-09] low=2026-08-12@62.5400; S3[2026-09-10→2026-10-08] low=2026-09-22@65.8600 | lows=[59.83000183105469, 62.540000915527344, 65.86000061035156] span=10.08% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.773870637534361 wick_frac=0.22612936246563894 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.43981264421706795 wick_frac=0.5601873557829321 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1095805942539427 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+1.3100:wick=0.4400; 2026-10-05:RED:body=-0.6200:wick=0.6650; 2026-10-06:RED:body=-0.0400:wick=1.4100; 2026-10-07:RED:body=-0.7300:wick=0.9298; 2026-10-08:GREEN:body=+1.5400:wick=0.4500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.59 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.91 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 40945.89 | **NEUTRAL** |
| `B04_income` | 6460.91 | **GOOD** |
| `B05_profit_margin` | 15.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4899999999999949 (now=76.21 vs prior_export=75.72 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.93 | **BAD** |
| `B13_short_float` | 1.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.59 (this export) | prior_export=6.59 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.91 (this export) | prior_export=7.91 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+15**  ·  Marine Shipping
price=30.469999313354492  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.50 on 2026-10-08; prev RSI=45.42 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.42@2026-10-07 → 51.50@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.42@2026-10-07 → 51.50@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.42@2026-10-07 → 51.50@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6300 R=0.0000); 2026-10-07:GREEN:O=29.6800,C=29.6900,body=+0.0100,vol=1354116.0; 2026-10-08:GREEN:O=29.8500,C=30.4700,body=+0.6200,vol=1437600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2791716 Rvol=0); 2026-10-07:GREEN:O=29.6800,C=29.6900,body=+0.0100,vol=1354116.0; 2026-10-08:GREEN:O=29.8500,C=30.4700,body=+0.6200,vol=1437600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.736 on 2026-10-08: today_vol=1437600 / avg20=1954475 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.037 on 2026-10-08 (price=30.4700, mid=30.5360, upper=32.3327, lower=28.7393; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=30.4700 vs SMA50=30.0583 dist=+1.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-08: SMA20=30.5360 SMA50=30.0583 SMA80=28.6163 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-08 (63 bars); S1[2026-07-08→2026-08-10] low=2026-07-17@24.8600; S2[2026-08-11→2026-09-09] low=2026-08-12@27.1500; S3[2026-09-10→2026-10-08] low=2026-09-29@28.9400 | lows=[24.860000610351562, 27.149999618530273, 28.940000534057617] span=16.41% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.47567301989173505 wick_frac=0.524326980108265 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-02:GREEN:body=+0.8000:wick=0.1150; 2026-10-05:GREEN:body=+0.0200:wick=0.9400; 2026-10-06:RED:body=-0.7400:wick=0.3750; 2026-10-07:GREEN:body=+0.0100:wick=0.3750; 2026-10-08:GREEN:body=+0.6200:wick=0.0500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.6200000000000045 (now=35.55 vs prior_export=36.17 on finviz_2026-10-08) | **BAD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.020000000000000018 (now=0.14 vs prior=0.12 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 2.57 | **GOOD** |
| `B13_short_float` | 3.28 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYN  ·  score **+15**  ·  REIT - Specialty
price=18.600000381469727  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=37.39 on 2026-10-08; prev RSI=21.76 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | cross_up | RSI 21.76@2026-10-07 → 37.39@2026-10-08 vs 30 | rule: cross_up=GOOD | **GOOD** |
| `A03_rsi_cross_50` | below | RSI 21.76@2026-10-07 → 37.39@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 21.76@2026-10-07 → 37.39@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=10.000 (G=0.6000 R=0.0600); 2026-10-07:RED:O=18.0500,C=17.9900,body=-0.0600,vol=5223620.0; 2026-10-08:GREEN:O=18.0000,C=18.6000,body=+0.6000,vol=6172500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.182 (Gvol=6172500 Rvol=5223620); 2026-10-07:RED:O=18.0500,C=17.9900,body=-0.0600,vol=5223620.0; 2026-10-08:GREEN:O=18.0000,C=18.6000,body=+0.6000,vol=6172500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.578 on 2026-10-08: today_vol=6172500 / avg20=3912808 (avg window 2026-09-10→2026-10-07, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.321 on 2026-10-08 (price=18.6000, mid=19.1920, upper=21.0375, lower=17.3465; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-08: price=18.6000 vs SMA50=20.3606 dist=-8.65% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-08: SMA20=19.1920 SMA50=20.3606 SMA80=20.7743 | **BAD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-08 (63 bars); S1[2026-07-09→2026-08-10] low=2026-07-09@20.9000; S2[2026-08-11→2026-09-09] low=2026-09-02@20.1700; S3[2026-09-10→2026-10-08] low=2026-10-06@17.7100 | lows=[20.899999618530273, 20.170000076293945, 17.709999084472656] span=18.01% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.8695650972055982 wick_frac=0.13043490279440179 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.19354699776655243 wick_frac=0.8064530022334476 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.00009536828051 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:RED:body=-0.3200:wick=0.1050; 2026-10-05:RED:body=-0.0900:wick=0.3500; 2026-10-06:GREEN:body=+0.1800:wick=0.2500; 2026-10-07:RED:body=-0.0600:wick=0.2500; 2026-10-08:GREEN:body=+0.6000:wick=0.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.94 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 968.3 | **NEUTRAL** |
| `B04_income` | 75.79 | **GOOD** |
| `B05_profit_margin` | 7.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0 (now=26.0 vs prior_export=25.0 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-0.12 vs prior=-0.13 on finviz_2026-10-08) | **GOOD** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 4.72 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.7 (this export) | prior_export=0.7 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.94 (this export) | prior_export=5.94 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNP  ·  score **+15**  ·  Utilities - Regulated Electric
price=38.41999816894531  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.12 on 2026-10-08; prev RSI=52.43 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.43@2026-10-07 → 51.12@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.43@2026-10-07 → 51.12@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.43@2026-10-07 → 51.12@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=2.571 (G=0.3600 R=0.1400); 2026-10-07:GREEN:O=38.1700,C=38.5300,body=+0.3600,vol=9916836.0; 2026-10-08:RED:O=38.5600,C=38.4200,body=-0.1400,vol=6532500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.518 (Gvol=9916836 Rvol=6532500); 2026-10-07:GREEN:O=38.1700,C=38.5300,body=+0.3600,vol=9916836.0; 2026-10-08:RED:O=38.5600,C=38.4200,body=-0.1400,vol=6532500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.906 on 2026-10-08: today_vol=6532500 / avg20=7206296 (avg window 2026-09-10→2026-10-07, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.407 on 2026-10-08 (price=38.4200, mid=37.8065, upper=39.3153, lower=36.2977; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-08: price=38.4200 vs SMA50=39.2216 dist=-2.04% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-08: SMA20=37.8065 SMA50=39.2216 SMA80=40.9006 | **BAD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-08 (63 bars); S1[2026-07-09→2026-08-10] low=2026-08-10@39.9500; S2[2026-08-11→2026-09-09] low=2026-08-24@38.4500; S3[2026-09-10→2026-10-08] low=2026-09-29@36.3700 | lows=[39.95000076293945, 38.45000076293945, 36.369998931884766] span=9.84% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.6605491744185232 wick_frac=0.3394508255814767 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.2745169904183465 wick_frac=0.7254830095816535 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.571374077000627 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+0.3600:wick=0.2300; 2026-10-05:RED:body=-0.0900:wick=0.6100; 2026-10-06:GREEN:body=+0.5000:wick=0.2350; 2026-10-07:GREEN:body=+0.3600:wick=0.1850; 2026-10-08:RED:body=-0.1400:wick=0.3700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.21 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9620.0 | **NEUTRAL** |
| `B04_income` | 1117.0 | **GOOD** |
| `B05_profit_margin` | 11.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7999999999999972 (now=45.47 vs prior_export=44.67 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 7.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.21 (this export) | prior_export=7.21 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARCO  ·  score **+15**  ·  Restaurants
price=8.119999885559082  pair=`2026-10-07→2026-10-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.89 on 2026-10-08; prev RSI=55.67 on 2026-10-07 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.67@2026-10-07 → 59.89@2026-10-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.67@2026-10-07 → 59.89@2026-10-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.67@2026-10-07 → 59.89@2026-10-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_body_sum/RED_body_sum=1.750 (G=0.2800 R=0.1600); 2026-10-07:RED:O=8.0600,C=7.9000,body=-0.1600,vol=2879593.0; 2026-10-08:GREEN:O=7.8400,C=8.1200,body=+0.2800,vol=3732900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-07 + 2026-10-08; ratio=GREEN_vol/RED_vol=1.296 (Gvol=3732900 Rvol=2879593); 2026-10-07:RED:O=8.0600,C=7.9000,body=-0.1600,vol=2879593.0; 2026-10-08:GREEN:O=7.8400,C=8.1200,body=+0.2800,vol=3732900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.847 on 2026-10-08: today_vol=3732900 / avg20=2020701 (avg window 2026-09-10→2026-10-07, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.715 on 2026-10-08 (price=8.1200, mid=7.5650, upper=8.3411, lower=6.7889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-08: price=8.1200 vs SMA50=7.8718 dist=+3.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-08: SMA20=7.5650 SMA50=7.8718 SMA80=8.0360 | **BAD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-08 (63 bars); S1[2026-07-08→2026-08-10] low=2026-08-10@8.0000; S2[2026-08-11→2026-09-09] low=2026-08-18@7.6800; S3[2026-09-10→2026-10-08] low=2026-10-01@6.6700 | lows=[8.0, 7.679999828338623, 6.670000076293945] span=19.94% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: GREEN body_frac=0.636362946723208 wick_frac=0.363637053276792 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-07+2026-10-08: RED body_frac=0.40020108749452854 wick_frac=0.5997989125054715 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7499947846041515 need>1.4; red_wick_gt_green=True 5d trail=2026-10-02:GREEN:body=+0.1200:wick=0.1450; 2026-10-05:GREEN:body=+0.3300:wick=0.2400; 2026-10-06:GREEN:body=+0.0200:wick=0.1900; 2026-10-07:RED:body=-0.1600:wick=0.2398; 2026-10-08:GREEN:body=+0.2800:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=49.15 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.11 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4980.97 | **NEUTRAL** |
| `B04_income` | 256.75 | **GOOD** |
| `B05_profit_margin` | 5.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.05999999999999872 (now=11.29 vs prior_export=11.23 on finviz_2026-10-08) | **GOOD** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | -0.06 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.06 vs prior=-0.06 on finviz_2026-10-08) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.07 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=49.15 (this export) | prior_export=49.15 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.11 (this export) | prior_export=2.11 (finviz_2026-10-08) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-09_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.