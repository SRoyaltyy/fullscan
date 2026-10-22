# A+B1 Feature Checklist — 2026-10-22

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-22.csv` · prior export for Δ: `2026-10-21`
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
| 1 | KMI | +17 | 18 | 1 | 2026-10-20→2026-10-21 | Oil & Gas Midstream |
| 2 | ISRG | +16 | 17 | 1 | 2026-10-20→2026-10-21 | Medical Instruments & Supplies |
| 3 | CRSR | +16 | 17 | 1 | 2026-10-20→2026-10-21 | Computer Hardware |
| 4 | ADSK | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Software - Application |
| 5 | CORT | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Biotechnology |
| 6 | NTNX | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Software - Infrastructure |
| 7 | INOD | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Information Technology Services |
| 8 | ASC | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Marine Shipping |
| 9 | PAA | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Oil & Gas Midstream |
| 10 | ACCO | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Business Equipment & Supplies |
| 11 | BGC | +15 | 16 | 1 | 2026-10-20→2026-10-21 | Capital Markets |
| 12 | P | +14 | 16 | 2 | 2026-10-20→2026-10-21 | Computer Hardware |
| 13 | TTC | +14 | 16 | 2 | 2026-10-20→2026-10-21 | Tools & Accessories |
| 14 | CCC | +14 | 15 | 1 | 2026-10-20→2026-10-21 | Software - Application |
| 15 | AMRX | +14 | 15 | 1 | 2026-10-20→2026-10-21 | Drug Manufacturers - Specialty & Gen |

## Full checklist — top 15

### KMI  ·  score **+17**  ·  Oil & Gas Midstream
price=33.970001220703125  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.17 on 2026-10-21; prev RSI=61.66 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.66@2026-10-20 → 64.17@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=18.500 (G=0.3700 R=0.0200); 2026-10-20:RED:O=33.5900,C=33.5700,body=-0.0200,vol=11940232.0; 2026-10-21:GREEN:O=33.6000,C=33.9700,body=+0.3700,vol=18340062.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.536 (Gvol=18340062 Rvol=11940232); 2026-10-20:RED:O=33.5900,C=33.5700,body=-0.0200,vol=11940232.0; 2026-10-21:GREEN:O=33.6000,C=33.9700,body=+0.3700,vol=18340062.0 | **GOOD** |
| `A07_rvol` | RVOL=1.578 on 2026-10-21: today_vol=18340062 / avg20=11621521 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.706 on 2026-10-21 (price=33.9700, mid=32.1385, upper=34.7341, lower=29.5429; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=33.9700 vs SMA50=31.8162 dist=+6.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=32.1385 SMA50=31.8162 SMA80=31.7966 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-08-04@30.6800; S2[2026-08-24→2026-09-22] low=2026-09-16@30.4100; S3[2026-09-23→2026-10-21] low=2026-10-01@29.4300 | lows=[30.68000030517578, 30.40999984741211, 29.43000030517578] span=4.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5967796516314011 wick_frac=0.4032203483685988 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.06375708344480385 wick_frac=0.9362429165551962 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.49971390425329 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:RED:body=-0.9300:wick=0.4400; 2026-10-16:GREEN:body=+1.1800:wick=0.0435; 2026-10-19:RED:body=-0.6800:wick=0.2950; 2026-10-20:RED:body=-0.0200:wick=0.2937; 2026-10-21:GREEN:body=+0.3700:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.37 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.98 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17896.0 | **NEUTRAL** |
| `B04_income` | 3449.0 | **GOOD** |
| `B05_profit_margin` | 19.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.3 vs prior_export=36.3 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.19 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.41 | **GOOD** |
| `B13_short_float` | 2.4 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.37 (this export) | prior_export=15.37 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.98 (this export) | prior_export=5.98 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ISRG  ·  score **+16**  ·  Medical Instruments & Supplies
price=428.1199951171875  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.19 on 2026-10-21; prev RSI=69.72 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.72@2026-10-20 → 61.19@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=30.1230 R=0.0000); 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0; 2026-10-21:GREEN:O=402.4950,C=428.1200,body=+25.6250,vol=4701233.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7841239 Rvol=0); 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0; 2026-10-21:GREEN:O=402.4950,C=428.1200,body=+25.6250,vol=4701233.0 | **GOOD** |
| `A07_rvol` | RVOL=2.348 on 2026-10-21: today_vol=4701233 / avg20=2002457 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.486 on 2026-10-21 (price=428.1200, mid=416.0210, upper=440.9124, lower=391.1296; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=428.1200 vs SMA50=394.1864 dist=+8.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=416.0210 SMA50=394.1864 SMA80=388.8721 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-24@332.1500; S2[2026-08-24→2026-09-22] low=2026-09-10@345.2500; S3[2026-09-23→2026-10-21] low=2026-10-05@389.5600 | lows=[332.1499938964844, 345.25, 389.55999755859375] span=17.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6472860820051523 wick_frac=0.35271391799484786 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+2.0200:wick=8.3300; 2026-10-16:RED:body=-1.7300:wick=7.6500; 2026-10-19:GREEN:body=+5.8400:wick=2.0820; 2026-10-20:GREEN:body=+4.4980:wick=3.9020; 2026-10-21:GREEN:body=+25.6250:wick=8.1322 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.5 (current export asof; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.37 (current export; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11034.4 | **NEUTRAL** |
| `B04_income` | 3138.8 | **GOOD** |
| `B05_profit_margin` | 28.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 479.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=479.86 vs prior_export=479.86 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.8 vs prior=-0.8 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.5 (this export) | prior_export=11.5 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.37 (this export) | prior_export=2.37 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=13.8100004196167  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.28 on 2026-10-21; prev RSI=49.53 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.53@2026-10-20 → 56.28@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6200 R=0.0000); 2026-10-20:GREEN:O=12.8700,C=13.1800,body=+0.3100,vol=1113572.0; 2026-10-21:GREEN:O=13.5000,C=13.8100,body=+0.3100,vol=1003664.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2117236 Rvol=0); 2026-10-20:GREEN:O=12.8700,C=13.1800,body=+0.3100,vol=1113572.0; 2026-10-21:GREEN:O=13.5000,C=13.8100,body=+0.3100,vol=1003664.0 | **GOOD** |
| `A07_rvol` | RVOL=0.898 on 2026-10-21: today_vol=1003664 / avg20=1118268 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.463 on 2026-10-21 (price=13.8100, mid=13.4970, upper=14.1735, lower=12.8205; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=13.8100 vs SMA50=12.8870 dist=+7.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=13.4970 SMA50=12.8870 SMA80=11.9204 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-24@9.7200; S2[2026-08-24→2026-09-22] low=2026-08-24@10.7260; S3[2026-09-23→2026-10-21] low=2026-10-15@12.4240 | lows=[9.720000267028809, 10.72599983215332, 12.423999786376953] span=27.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5596946233661975 wick_frac=0.44030537663380254 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+0.5100:wick=0.3610; 2026-10-16:RED:body=-0.3500:wick=0.4664; 2026-10-19:GREEN:body=+0.0300:wick=0.3100; 2026-10-20:GREEN:body=+0.3100:wick=0.2200; 2026-10-21:GREEN:body=+0.3100:wick=0.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 19.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+15**  ·  Software - Application
price=244.83999633789062  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.68 on 2026-10-21; prev RSI=63.49 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.49@2026-10-20 → 62.68@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.2200 R=0.0000); 2026-10-20:GREEN:O=241.1000,C=245.5800,body=+4.4800,vol=1472196.0; 2026-10-21:GREEN:O=243.1000,C=244.8400,body=+1.7400,vol=872468.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2344664 Rvol=0); 2026-10-20:GREEN:O=241.1000,C=245.5800,body=+4.4800,vol=1472196.0; 2026-10-21:GREEN:O=243.1000,C=244.8400,body=+1.7400,vol=872468.0 | **GOOD** |
| `A07_rvol` | RVOL=0.458 on 2026-10-21: today_vol=872468 / avg20=1906042 (avg window 2026-09-23→2026-10-20, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.633 on 2026-10-21 (price=244.8400, mid=226.5780, upper=255.4194, lower=197.7366; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=244.8400 vs SMA50=232.8012 dist=+5.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=226.58_50=232.80_80=227.70 on 2026-10-21: SMA20=226.5780 SMA50=232.8012 SMA80=227.6994 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-23@202.3700; S2[2026-08-24→2026-09-22] low=2026-09-09@206.3300; S3[2026-09-23→2026-10-21] low=2026-09-29@199.1801 | lows=[202.3699951171875, 206.3300018310547, 199.1800994873047] span=3.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5115603248467959 wick_frac=0.48843967515320413 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+10.9800:wick=1.7350; 2026-10-16:RED:body=-8.1200:wick=5.8450; 2026-10-19:GREEN:body=+0.3500:wick=4.8275; 2026-10-20:GREEN:body=+4.4800:wick=2.1000; 2026-10-21:GREEN:body=+1.7400:wick=3.3437 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CORT  ·  score **+15**  ·  Biotechnology
price=116.27999877929688  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.31 on 2026-10-21; prev RSI=51.59 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.59@2026-10-20 → 51.31@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=22.647 (G=3.8500 R=0.1700); 2026-10-20:GREEN:O=112.6000,C=116.4500,body=+3.8500,vol=549300.0; 2026-10-21:RED:O=116.4500,C=116.2800,body=-0.1700,vol=516745.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.063 (Gvol=549300 Rvol=516745); 2026-10-20:GREEN:O=112.6000,C=116.4500,body=+3.8500,vol=549300.0; 2026-10-21:RED:O=116.4500,C=116.2800,body=-0.1700,vol=516745.0 | **GOOD** |
| `A07_rvol` | RVOL=0.688 on 2026-10-21: today_vol=516745 / avg20=751001 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.075 on 2026-10-21 (price=116.2800, mid=116.7490, upper=122.9651, lower=110.5329; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=116.2800 vs SMA50=115.7982 dist=+0.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=116.7490 SMA50=115.7982 SMA80=108.5460 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-07-28@90.0000; S2[2026-08-24→2026-09-22] low=2026-09-08@108.2500; S3[2026-09-23→2026-10-21] low=2026-10-16@107.7900 | lows=[90.0, 108.25, 107.79000091552734] span=20.28% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7432429048844323 wick_frac=0.25675709511556766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.07456039083805986 wick_frac=0.9254396091619401 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=22.647293779732518 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:RED:body=-2.9400:wick=1.5200; 2026-10-16:RED:body=-3.0100:wick=2.8400; 2026-10-19:GREEN:body=+0.4000:wick=2.1900; 2026-10-20:GREEN:body=+3.8500:wick=1.3300; 2026-10-21:RED:body=-0.1700:wick=2.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1381.48 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.83 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 830.81 | **NEUTRAL** |
| `B04_income` | 54.2 | **GOOD** |
| `B05_profit_margin` | 6.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=141.0 vs prior_export=141.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -6.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.34 vs prior=-6.34 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.39 | **GOOD** |
| `B13_short_float` | 9.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1381.48 (this export) | prior_export=1381.48 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.83 (this export) | prior_export=15.83 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NTNX  ·  score **+15**  ·  Software - Infrastructure
price=70.11000061035156  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.64 on 2026-10-21; prev RSI=44.26 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.26@2026-10-20 → 50.64@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.26@2026-10-20 → 50.64@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.26@2026-10-20 → 50.64@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0300 R=0.0000); 2026-10-20:GREEN:O=67.3800,C=68.2000,body=+0.8200,vol=2189700.0; 2026-10-21:GREEN:O=68.9000,C=70.1100,body=+1.2100,vol=2611400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4801100 Rvol=0); 2026-10-20:GREEN:O=67.3800,C=68.2000,body=+0.8200,vol=2189700.0; 2026-10-21:GREEN:O=68.9000,C=70.1100,body=+1.2100,vol=2611400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.020 on 2026-10-21: today_vol=2611400 / avg20=2561396 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.158 on 2026-10-21 (price=70.1100, mid=70.8180, upper=75.2941, lower=66.3419; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=70.1100 vs SMA50=68.2779 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=70.8180 SMA50=68.2779 SMA80=62.8953 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-21 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@52.3100; S2[2026-08-18→2026-09-16] low=2026-08-19@64.7400; S3[2026-09-17→2026-10-21] low=2026-09-28@66.2500 | lows=[52.310001373291016, 64.73999786376953, 66.25] span=26.65% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5253568738002621 wick_frac=0.4746431261997378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.7200:wick=0.9500; 2026-10-12:RED:body=-1.7600:wick=0.2900; 2026-10-19:GREEN:body=+0.0300:wick=1.1550; 2026-10-20:GREEN:body=+0.8200:wick=1.1150; 2026-10-21:GREEN:body=+1.2100:wick=0.7200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=22.45 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.55 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2853.55 | **NEUTRAL** |
| `B04_income` | 1506.84 | **GOOD** |
| `B05_profit_margin` | 52.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=78.08 vs prior_export=78.08 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -1.4 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.4 vs prior=-1.4 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.51 | **GOOD** |
| `B13_short_float` | 4.38 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.45 (this export) | prior_export=22.45 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.55 (this export) | prior_export=2.55 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INOD  ·  score **+15**  ·  Information Technology Services
price=60.560001373291016  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.38 on 2026-10-21; prev RSI=41.03 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.03@2026-10-20 → 50.38@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.1100 R=0.0000); 2026-10-20:GREEN:O=53.3800,C=55.0450,body=+1.6650,vol=538422.0; 2026-10-21:GREEN:O=56.1150,C=60.5600,body=+4.4450,vol=1108698.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1647120 Rvol=0); 2026-10-20:GREEN:O=53.3800,C=55.0450,body=+1.6650,vol=538422.0; 2026-10-21:GREEN:O=56.1150,C=60.5600,body=+4.4450,vol=1108698.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-10-21: today_vol=1108698 / avg20=1382606 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.167 on 2026-10-21 (price=60.5600, mid=62.6390, upper=75.0729, lower=50.2051; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=60.5600 vs SMA50=60.3436 dist=+0.36% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=62.64_50=60.34_80=61.99 on 2026-10-21: SMA20=62.6390 SMA50=60.3436 SMA80=61.9860 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-29@54.3600; S2[2026-08-24→2026-09-22] low=2026-09-14@51.9700; S3[2026-09-23→2026-10-21] low=2026-10-15@50.6900 | lows=[54.36000061035156, 51.970001220703125, 50.689998626708984] span=7.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7067738789184911 wick_frac=0.2932261210815089 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-15:GREEN:body=+5.4200:wick=0.5590; 2026-10-16:RED:body=-2.6300:wick=1.2384; 2026-10-19:RED:body=-0.5100:wick=1.9200; 2026-10-20:GREEN:body=+1.6650:wick=1.3150; 2026-10-21:GREEN:body=+4.4450:wick=0.7549 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=92.49 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.76 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 317.16 | **NEUTRAL** |
| `B04_income` | 46.48 | **GOOD** |
| `B05_profit_margin` | 14.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 123.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=123.67 vs prior_export=123.67 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | -49.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-49.78 vs prior=-49.78 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.2 | **GOOD** |
| `B13_short_float` | 14.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=92.49 (this export) | prior_export=92.49 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.76 (this export) | prior_export=6.76 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASC  ·  score **+15**  ·  Marine Shipping
price=19.6200008392334  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.34 on 2026-10-21; prev RSI=52.37 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.37@2026-10-20 → 58.34@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.37@2026-10-20 → 58.34@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.37@2026-10-20 → 58.34@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0900 R=0.0000); 2026-10-20:GREEN:O=18.5200,C=18.8700,body=+0.3500,vol=830300.0; 2026-10-21:GREEN:O=18.8800,C=19.6200,body=+0.7400,vol=567600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1397900 Rvol=0); 2026-10-20:GREEN:O=18.5200,C=18.8700,body=+0.3500,vol=830300.0; 2026-10-21:GREEN:O=18.8800,C=19.6200,body=+0.7400,vol=567600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.807 on 2026-10-21: today_vol=567600 / avg20=702916 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.671 on 2026-10-21 (price=19.6200, mid=18.7355, upper=20.0530, lower=17.4180; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=19.6200 vs SMA50=18.2032 dist=+7.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=18.7355 SMA50=18.2032 SMA80=17.4041 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-21 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-17@15.1700; S2[2026-08-18→2026-09-16] low=2026-08-28@16.6100; S3[2026-09-17→2026-10-21] low=2026-09-23@17.2000 | lows=[15.170000076293945, 16.610000610351562, 17.200000762939453] span=13.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.7269626855239479 wick_frac=0.273037314476052 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.2700:wick=0.2562; 2026-10-12:GREEN:body=+0.0300:wick=0.4700; 2026-10-19:RED:body=-0.5500:wick=0.8300; 2026-10-20:GREEN:body=+0.3500:wick=0.2500; 2026-10-21:GREEN:body=+0.7400:wick=0.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.72 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=34.7 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 368.29 | **NEUTRAL** |
| `B04_income` | 105.58 | **GOOD** |
| `B05_profit_margin` | 28.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.0 vs prior_export=21.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.93 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.93 vs prior=-0.93 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 7.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.72 (this export) | prior_export=6.72 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=34.7 (this export) | prior_export=34.7 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAA  ·  score **+15**  ·  Oil & Gas Midstream
price=25.889999389648438  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.47 on 2026-10-21; prev RSI=64.33 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.33@2026-10-20 → 68.47@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.33@2026-10-20 → 68.47@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.33@2026-10-20 → 68.47@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=6.000 (G=0.3600 R=0.0600); 2026-10-20:RED:O=25.5500,C=25.4900,body=-0.0600,vol=1675300.0; 2026-10-21:GREEN:O=25.5300,C=25.8900,body=+0.3600,vol=2122800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.267 (Gvol=2122800 Rvol=1675300); 2026-10-20:RED:O=25.5500,C=25.4900,body=-0.0600,vol=1675300.0; 2026-10-21:GREEN:O=25.5300,C=25.8900,body=+0.3600,vol=2122800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.918 on 2026-10-21: today_vol=2122800 / avg20=2313498 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.115 on 2026-10-21 (price=25.8900, mid=24.4995, upper=25.7462, lower=23.2528; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-21: price=25.8900 vs SMA50=24.7056 dist=+4.79% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=24.50_50=24.71_80=24.02 on 2026-10-21: SMA20=24.4995 SMA50=24.7056 SMA80=24.0172 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-21 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@22.7300; S2[2026-08-18→2026-09-16] low=2026-08-19@23.7900; S3[2026-09-17→2026-10-21] low=2026-10-02@23.4400 | lows=[22.729999542236328, 23.790000915527344, 23.440000534057617] span=4.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.782607433698771 wick_frac=0.217392566301229 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.19999872843101102 wick_frac=0.8000012715689889 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.000031789426837 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.2200:wick=0.2000; 2026-10-12:GREEN:body=+0.0300:wick=0.2200; 2026-10-19:DOJI:body=+0.0000:wick=0.3600; 2026-10-20:RED:body=-0.0600:wick=0.2400; 2026-10-21:GREEN:body=+0.3600:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.44 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=39.05 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 2548.0 | **GOOD** |
| `B05_profit_margin` | 4.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.54 | **GOOD** |
| `B13_short_float` | 4.0 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.44 (this export) | prior_export=7.44 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=39.05 (this export) | prior_export=39.05 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ACCO  ·  score **+15**  ·  Business Equipment & Supplies
price=4.53000020980835  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.84 on 2026-10-21; prev RSI=67.88 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.88@2026-10-20 → 62.84@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.88@2026-10-20 → 62.84@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.88@2026-10-20 → 62.84@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=2.250 (G=0.0900 R=0.0400); 2026-10-20:GREEN:O=4.4900,C=4.5800,body=+0.0900,vol=653900.0; 2026-10-21:RED:O=4.5700,C=4.5300,body=-0.0400,vol=470400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.390 (Gvol=653900 Rvol=470400); 2026-10-20:GREEN:O=4.4900,C=4.5800,body=+0.0900,vol=653900.0; 2026-10-21:RED:O=4.5700,C=4.5300,body=-0.0400,vol=470400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.743 on 2026-10-21: today_vol=470400 / avg20=633459 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.681 on 2026-10-21 (price=4.5300, mid=4.3535, upper=4.6126, lower=4.0944; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=4.5300 vs SMA50=4.3097 dist=+5.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=4.3535 SMA50=4.3097 SMA80=4.2260 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-21 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@3.8900; S2[2026-08-18→2026-09-16] low=2026-08-25@4.0900; S3[2026-09-17→2026-10-21] low=2026-09-21@4.1300 | lows=[3.890000104904175, 4.090000152587891, 4.130000114440918] span=6.17% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5625014901175405 wick_frac=0.43749850988245953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.4 wick_frac=0.6 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.250005960470162 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.0100:wick=0.0680; 2026-10-12:GREEN:body=+0.0300:wick=0.0700; 2026-10-19:RED:body=-0.0200:wick=0.0900; 2026-10-20:GREEN:body=+0.0900:wick=0.0700; 2026-10-21:RED:body=-0.0400:wick=0.0600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.74 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.12 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1571.3 | **NEUTRAL** |
| `B04_income` | 58.8 | **GOOD** |
| `B05_profit_margin` | 3.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.0 vs prior_export=8.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -2.19 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.19 vs prior=-2.19 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.61 | **GOOD** |
| `B13_short_float` | 7.67 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.74 (this export) | prior_export=8.74 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.12 (this export) | prior_export=3.12 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BGC  ·  score **+15**  ·  Capital Markets
price=11.829999923706055  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.91 on 2026-10-21; prev RSI=48.41 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.41@2026-10-20 → 51.91@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.41@2026-10-20 → 51.91@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.41@2026-10-20 → 51.91@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3500 R=0.0000); 2026-10-20:GREEN:O=11.4100,C=11.6300,body=+0.2200,vol=2178400.0; 2026-10-21:GREEN:O=11.7000,C=11.8300,body=+0.1300,vol=2182000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4360400 Rvol=0); 2026-10-20:GREEN:O=11.4100,C=11.6300,body=+0.2200,vol=2178400.0; 2026-10-21:GREEN:O=11.7000,C=11.8300,body=+0.1300,vol=2182000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.546 on 2026-10-21: today_vol=2182000 / avg20=3994013 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.066 on 2026-10-21 (price=11.8300, mid=11.7835, upper=12.4929, lower=11.0741; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=11.8300 vs SMA50=11.6532 dist=+1.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=11.7835 SMA50=11.6532 SMA80=11.5511 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-21 (63 bars); S1[2026-07-15→2026-08-17] low=2026-08-12@10.1300; S2[2026-08-18→2026-09-16] low=2026-08-20@10.7400; S3[2026-09-17→2026-10-21] low=2026-10-08@11.0900 | lows=[10.130000114440918, 10.739999771118164, 11.09000015258789] span=9.48% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.5678878784336254 wick_frac=0.4321121215663746 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.2100:wick=0.1650; 2026-10-12:GREEN:body=+0.1100:wick=0.2000; 2026-10-19:RED:body=-0.1900:wick=0.2200; 2026-10-20:GREEN:body=+0.2200:wick=0.1000; 2026-10-21:GREEN:body=+0.1300:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.48 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.03 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3291.5 | **NEUTRAL** |
| `B04_income` | 190.87 | **GOOD** |
| `B05_profit_margin` | 5.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.5 vs prior_export=15.5 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | -16.88 | **BAD** |
| `B13_short_float` | 3.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.48 (this export) | prior_export=4.48 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.03 (this export) | prior_export=4.03 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### P  ·  score **+14**  ·  Computer Hardware
price=130.92999267578125  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.43 on 2026-10-21; prev RSI=47.71 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.71@2026-10-20 → 51.43@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.71@2026-10-20 → 51.43@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.71@2026-10-20 → 51.43@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=5.516 (G=1.7100 R=0.3100); 2026-10-20:GREEN:O=124.8100,C=126.5200,body=+1.7100,vol=2724704.0; 2026-10-21:RED:O=131.2400,C=130.9300,body=-0.3100,vol=2519877.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=1.081 (Gvol=2724704 Rvol=2519877); 2026-10-20:GREEN:O=124.8100,C=126.5200,body=+1.7100,vol=2724704.0; 2026-10-21:RED:O=131.2400,C=130.9300,body=-0.3100,vol=2519877.0 | **GOOD** |
| `A07_rvol` | RVOL=0.533 on 2026-10-21: today_vol=2519877 / avg20=4730279 (avg window 2026-09-23→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.298 on 2026-10-21 (price=130.9300, mid=137.8450, upper=161.0858, lower=114.6042; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=130.9300 vs SMA50=117.4234 dist=+11.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=137.8450 SMA50=117.4234 SMA80=102.8054 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-21 (63 bars); S1[2026-07-24→2026-08-21] low=2026-07-28@68.2300; S2[2026-08-24→2026-09-22] low=2026-09-01@89.6450; S3[2026-09-23→2026-10-21] low=2026-09-23@107.1000 | lows=[68.2300033569336, 89.6449966430664, 107.0999984741211] span=56.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.26635507354881033 wick_frac=0.7336449264511897 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=0.0948196201054744 wick_frac=0.9051803798945256 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.515898016439435 need>1.4; red_wick_gt_green=False 5d trail=2026-10-15:RED:body=-1.3350:wick=5.8750; 2026-10-16:RED:body=-7.9900:wick=1.6900; 2026-10-19:RED:body=-2.1100:wick=1.5600; 2026-10-20:GREEN:body=+1.7100:wick=4.7100; 2026-10-21:RED:body=-0.3100:wick=2.9595 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=20.67 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.04 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4262.15 | **NEUTRAL** |
| `B04_income` | 253.28 | **GOOD** |
| `B05_profit_margin` | 5.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 147.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=147.26 vs prior_export=147.26 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | -14.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-14.02 vs prior=-14.02 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 2.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.67 (this export) | prior_export=20.67 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTC  ·  score **+14**  ·  Tools & Accessories
price=103.3499984741211  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.38 on 2026-10-21; prev RSI=67.98 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.98@2026-10-20 → 69.38@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.98@2026-10-20 → 69.38@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.98@2026-10-20 → 69.38@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.8400 R=0.0000); 2026-10-20:GREEN:O=100.4400,C=102.6500,body=+2.2100,vol=479900.0; 2026-10-21:GREEN:O=102.7200,C=103.3500,body=+0.6300,vol=491400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=971300 Rvol=0); 2026-10-20:GREEN:O=100.4400,C=102.6500,body=+2.2100,vol=479900.0; 2026-10-21:GREEN:O=102.7200,C=103.3500,body=+0.6300,vol=491400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.648 on 2026-10-21: today_vol=491400 / avg20=758340 (avg window 2026-09-17→2026-10-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.150 on 2026-10-21 (price=103.3500, mid=97.4610, upper=102.5801, lower=92.3419; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-21: price=103.3500 vs SMA50=97.0142 dist=+6.53% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=97.4610 SMA50=97.0142 SMA80=96.1494 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-21 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@90.6200; S2[2026-08-18→2026-09-16] low=2026-09-04@91.0300; S3[2026-09-17→2026-10-21] low=2026-09-18@91.1100 | lows=[90.62000274658203, 91.02999877929688, 91.11000061035156] span=0.54% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6632911405041936 wick_frac=0.3367088594958064 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-1.4100:wick=1.2500; 2026-10-12:GREEN:body=+1.1500:wick=0.8700; 2026-10-19:GREEN:body=+0.1900:wick=0.9800; 2026-10-20:GREEN:body=+2.2100:wick=0.4900; 2026-10-21:GREEN:body=+0.6300:wick=0.6100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.75 (current export asof; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.91 (current export; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4756.5 | **NEUTRAL** |
| `B04_income` | 363.3 | **GOOD** |
| `B05_profit_margin` | 7.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=111.0 vs prior_export=111.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -3.79 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.79 vs prior=-3.79 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.73 | **GOOD** |
| `B13_short_float` | 3.92 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.75 (this export) | prior_export=1.75 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.91 (this export) | prior_export=2.91 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCC  ·  score **+14**  ·  Software - Application
price=7.639999866485596  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.64 on 2026-10-21; prev RSI=69.48 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.48@2026-10-20 → 69.64@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.48@2026-10-20 → 69.64@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.48@2026-10-20 → 69.64@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.0200 R=0.0000); 2026-10-20:GREEN:O=7.6200,C=7.6300,body=+0.0100,vol=37275000.0; 2026-10-21:GREEN:O=7.6300,C=7.6400,body=+0.0100,vol=26565100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=63840100 Rvol=0); 2026-10-20:GREEN:O=7.6200,C=7.6300,body=+0.0100,vol=37275000.0; 2026-10-21:GREEN:O=7.6300,C=7.6400,body=+0.0100,vol=26565100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.690 on 2026-10-21: today_vol=26565100 / avg20=15723332 (avg window 2026-09-17→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.947 on 2026-10-21 (price=7.6400, mid=6.8475, upper=7.6845, lower=6.0105; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-21: price=7.6400 vs SMA50=6.9862 dist=+9.36% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=6.85_50=6.99_80=6.48 on 2026-10-21: SMA20=6.8475 SMA50=6.9862 SMA80=6.4804 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-21 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-30@5.4040; S2[2026-08-18→2026-09-16] low=2026-09-16@6.2950; S3[2026-09-17→2026-10-21] low=2026-09-17@6.2400 | lows=[5.4039998054504395, 6.295000076293945, 6.239999771118164] span=16.49% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6666613685130732 wick_frac=0.33333863148692683 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.3600:wick=0.2200; 2026-10-12:RED:body=-0.0900:wick=0.1600; 2026-10-19:GREEN:body=+0.0200:wick=0.0000; 2026-10-20:GREEN:body=+0.0100:wick=0.0000; 2026-10-21:GREEN:body=+0.0100:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.83 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.65 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1112.19 | **NEUTRAL** |
| `B04_income` | 42.35 | **GOOD** |
| `B05_profit_margin` | 3.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.75 vs prior_export=8.75 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.93 | **GOOD** |
| `B13_short_float` | 8.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.83 (this export) | prior_export=1.83 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.65 (this export) | prior_export=0.65 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMRX  ·  score **+14**  ·  Drug Manufacturers - Specialty & Generic
price=20.200000762939453  pair=`2026-10-20→2026-10-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.78 on 2026-10-21; prev RSI=56.31 on 2026-10-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.31@2026-10-20 → 59.78@2026-10-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7300 R=0.0000); 2026-10-20:GREEN:O=19.4100,C=19.8400,body=+0.4300,vol=5203935.0; 2026-10-21:GREEN:O=19.9000,C=20.2000,body=+0.3000,vol=4657958.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-20 + 2026-10-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9861893 Rvol=0); 2026-10-20:GREEN:O=19.4100,C=19.8400,body=+0.4300,vol=5203935.0; 2026-10-21:GREEN:O=19.9000,C=20.2000,body=+0.3000,vol=4657958.0 | **GOOD** |
| `A07_rvol` | RVOL=1.505 on 2026-10-21: today_vol=4657958 / avg20=3095990 (avg window 2026-09-23→2026-10-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.403 on 2026-10-21 (price=20.2000, mid=19.8485, upper=20.7197, lower=18.9773; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-21: price=20.2000 vs SMA50=18.6890 dist=+8.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-21: SMA20=19.8485 SMA50=18.6890 SMA80=18.3155 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-21 (63 bars); S1[2026-07-23→2026-08-21] low=2026-08-12@16.7700; S2[2026-08-24→2026-09-22] low=2026-09-16@16.5600; S3[2026-09-23→2026-10-21] low=2026-09-23@18.7350 | lows=[16.770000457763672, 16.559999465942383, 18.735000610351562] span=13.13% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: GREEN body_frac=0.6215848937719632 wick_frac=0.3784151062280368 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-20+2026-10-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-15:GREEN:body=+0.2900:wick=0.6950; 2026-10-16:RED:body=-0.1000:wick=0.3450; 2026-10-19:GREEN:body=+0.2600:wick=0.2350; 2026-10-20:GREEN:body=+0.4300:wick=0.1700; 2026-10-21:GREEN:body=+0.3000:wick=0.2698 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=34.83 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.64 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3117.55 | **NEUTRAL** |
| `B04_income` | 157.36 | **GOOD** |
| `B05_profit_margin` | 5.05 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.71 vs prior_export=22.71 on finviz_2026-10-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -0.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.25 vs prior=-0.25 on finviz_2026-10-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.94 | **GOOD** |
| `B13_short_float` | 7.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=34.83 (this export) | prior_export=34.83 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.64 (this export) | prior_export=3.64 (finviz_2026-10-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-22_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.