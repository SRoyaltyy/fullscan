# A+B1 Feature Checklist — 2026-10-27

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-27.csv` · prior export for Δ: `2026-10-26`
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
| 1 | DINO | +16 | 16 | 0 | 2026-10-23→2026-10-26 | Oil & Gas Refining & Marketing |
| 2 | GMAB | +16 | 18 | 2 | 2026-10-23→2026-10-26 | Biotechnology |
| 3 | MCHP | +16 | 17 | 1 | 2026-10-23→2026-10-26 | Semiconductors |
| 4 | SNPS | +16 | 17 | 1 | 2026-10-23→2026-10-26 | Software - Infrastructure |
| 5 | PM | +16 | 16 | 0 | 2026-10-23→2026-10-26 | Tobacco |
| 6 | SWKS | +16 | 17 | 1 | 2026-10-23→2026-10-26 | Semiconductors |
| 7 | OXY | +15 | 16 | 1 | 2026-10-23→2026-10-26 | Oil & Gas E&P |
| 8 | CAE | +15 | 17 | 2 | 2026-10-12→2026-10-26 | Aerospace & Defense |
| 9 | MTDR | +15 | 16 | 1 | 2026-10-23→2026-10-26 | Oil & Gas E&P |
| 10 | LTH | +15 | 17 | 2 | 2026-10-12→2026-10-26 | Leisure |
| 11 | PANW | +15 | 16 | 1 | 2026-10-23→2026-10-26 | Software - Infrastructure |
| 12 | CSCO | +15 | 16 | 1 | 2026-10-23→2026-10-26 | Communication Equipment |
| 13 | KBR | +15 | 18 | 3 | 2026-10-12→2026-10-26 | Engineering & Construction |
| 14 | CP | +15 | 16 | 1 | 2026-10-12→2026-10-26 | Railroads |
| 15 | LDOS | +14 | 16 | 2 | 2026-10-12→2026-10-26 | Information Technology Services |

## Full checklist — top 15

### DINO  ·  score **+16**  ·  Oil & Gas Refining & Marketing
price=125.25  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.14 on 2026-10-26; prev RSI=59.54 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.54@2026-10-23 → 65.14@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.54@2026-10-23 → 65.14@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.54@2026-10-23 → 65.14@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=2.567 (G=3.4400 R=1.3400); 2026-10-23:RED:O=123.0000,C=121.6600,body=-1.3400,vol=1852390.0; 2026-10-26:GREEN:O=121.8100,C=125.2500,body=+3.4400,vol=2089600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.128 (Gvol=2089600 Rvol=1852390); 2026-10-23:RED:O=123.0000,C=121.6600,body=-1.3400,vol=1852390.0; 2026-10-26:GREEN:O=121.8100,C=125.2500,body=+3.4400,vol=2089600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.008 on 2026-10-26: today_vol=2089600 / avg20=2073370 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.396 on 2026-10-26 (price=125.2500, mid=119.9895, upper=133.2797, lower=106.6993; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=125.2500 vs SMA50=110.3714 dist=+13.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=119.9895 SMA50=110.3714 SMA80=101.0556 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-08-07@80.0041; S2[2026-08-27→2026-09-25] low=2026-08-27@94.6900; S3[2026-09-28→2026-10-26] low=2026-09-29@103.3570 | lows=[80.0040801936391, 94.69000244140625, 103.35700225830078] span=29.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.5910657425747766 wick_frac=0.40893425742522344 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=0.4802852681635589 wick_frac=0.5197147318364411 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5671730169213602 need>1.4; red_wick_gt_green=False 5d trail=2026-10-20:GREEN:body=+0.7800:wick=3.1100; 2026-10-21:GREEN:body=+0.2600:wick=4.1450; 2026-10-22:RED:body=-3.9900:wick=2.9382; 2026-10-23:RED:body=-1.3400:wick=1.4500; 2026-10-26:GREEN:body=+3.4400:wick=2.3800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.43 vs prior_export=103.43 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 2.58 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.14 | **GOOD** |
| `B13_short_float` | 5.85 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+16**  ·  Biotechnology
price=44.685001373291016  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.56 on 2026-10-26; prev RSI=78.02 on 2026-10-23 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 78.02@2026-10-23 → 79.56@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 78.02@2026-10-23 → 79.56@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 78.02@2026-10-23 → 79.56@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=37.762 (G=3.9650 R=0.1050); 2026-10-23:GREEN:O=39.8250,C=43.7900,body=+3.9650,vol=12896581.0; 2026-10-26:RED:O=44.7900,C=44.6850,body=-0.1050,vol=7473300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.726 (Gvol=12896581 Rvol=7473300); 2026-10-23:GREEN:O=39.8250,C=43.7900,body=+3.9650,vol=12896581.0; 2026-10-26:RED:O=44.7900,C=44.6850,body=-0.1050,vol=7473300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.513 on 2026-10-26: today_vol=7473300 / avg20=4940844 (avg window 2026-09-28→2026-10-23, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.293 on 2026-10-26 (price=44.6850, mid=37.8930, upper=43.1463, lower=32.6397; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=44.6850 vs SMA50=35.4736 dist=+25.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=37.8930 SMA50=35.4736 SMA80=33.1562 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-26 (63 bars); S1[2026-07-28→2026-08-26] low=2026-07-30@28.0300; S2[2026-08-27→2026-09-25] low=2026-09-10@32.2100; S3[2026-09-28→2026-10-26] low=2026-10-02@33.7250 | lows=[28.030000686645508, 32.209999084472656, 33.724998474121094] span=20.32% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.8427202145634626 wick_frac=0.1572797854365374 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=0.07692286194966269 wick_frac=0.9230771380503373 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=37.76207084468665 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:GREEN:body=+0.4700:wick=0.5150; 2026-10-21:GREEN:body=+0.4900:wick=0.1200; 2026-10-22:GREEN:body=+0.3800:wick=0.2830; 2026-10-23:GREEN:body=+3.9650:wick=0.7400; 2026-10-26:RED:body=-0.1050:wick=1.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.39 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=39.39 vs prior_export=39.39 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MCHP  ·  score **+16**  ·  Semiconductors
price=75.9000015258789  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.85 on 2026-10-26; prev RSI=46.01 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.01@2026-10-23 → 50.85@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.01@2026-10-23 → 50.85@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.01@2026-10-23 → 50.85@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=9.063 (G=1.4500 R=0.1600); 2026-10-23:RED:O=74.2100,C=74.0500,body=-0.1600,vol=7183297.0; 2026-10-26:GREEN:O=74.4500,C=75.9000,body=+1.4500,vol=7944900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.106 (Gvol=7944900 Rvol=7183297); 2026-10-23:RED:O=74.2100,C=74.0500,body=-0.1600,vol=7183297.0; 2026-10-26:GREEN:O=74.4500,C=75.9000,body=+1.4500,vol=7944900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.835 on 2026-10-26: today_vol=7944900 / avg20=9511561 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.083 on 2026-10-26 (price=75.9000, mid=76.3730, upper=82.0498, lower=70.6962; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=75.9000 vs SMA50=75.1252 dist=+1.03% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=76.37_50=75.13_80=77.32 on 2026-10-26: SMA20=76.3730 SMA50=75.1252 SMA80=77.3178 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-07-29@71.2400; S2[2026-08-27→2026-09-25] low=2026-09-16@68.7500; S3[2026-09-28→2026-10-26] low=2026-10-15@69.7600 | lows=[71.23999786376953, 68.75, 69.76000213623047] span=3.62% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.5894329745035465 wick_frac=0.4105670254964536 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=0.08742943859385814 wick_frac=0.9125705614061419 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.06275332602165 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:GREEN:body=+2.3800:wick=1.4800; 2026-10-21:GREEN:body=+0.6000:wick=1.5750; 2026-10-22:RED:body=-0.6400:wick=1.6900; 2026-10-23:RED:body=-0.1600:wick=1.6700; 2026-10-26:GREEN:body=+1.4500:wick=1.0100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.59 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.8 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5122.3 | **NEUTRAL** |
| `B04_income` | 367.2 | **GOOD** |
| `B05_profit_margin` | 7.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.26 vs prior_export=110.26 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | -5.62 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.62 vs prior=-5.62 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.19 | **GOOD** |
| `B13_short_float` | 6.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.59 (this export) | prior_export=8.59 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.8 (this export) | prior_export=1.8 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SNPS  ·  score **+16**  ·  Software - Infrastructure
price=454.3299865722656  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.19 on 2026-10-26; prev RSI=42.39 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.39@2026-10-23 → 51.19@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.39@2026-10-23 → 51.19@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.39@2026-10-23 → 51.19@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=33.9000 R=0.0000); 2026-10-23:GREEN:O=419.4200,C=430.1400,body=+10.7200,vol=1362437.0; 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4172637 Rvol=0); 2026-10-23:GREEN:O=419.4200,C=430.1400,body=+10.7200,vol=1362437.0; 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.107 on 2026-10-26: today_vol=2810200 / avg20=2538069 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.255 on 2026-10-26 (price=454.3300, mid=469.7275, upper=530.0013, lower=409.4537; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=454.3300 vs SMA50=431.5302 dist=+5.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=469.7275 SMA50=431.5302 SMA80=421.8961 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-07-30@370.2000; S2[2026-08-27→2026-09-25] low=2026-09-15@362.5500; S3[2026-09-28→2026-10-26] low=2026-09-29@403.3300 | lows=[370.20001220703125, 362.54998779296875, 403.3299865722656] span=11.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.7704422791722187 wick_frac=0.22955772082778123 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:RED:body=-3.2600:wick=7.6500; 2026-10-21:RED:body=-0.6100:wick=9.0900; 2026-10-22:RED:body=-19.0200:wick=2.9200; 2026-10-23:GREEN:body=+10.7200:wick=5.3800; 2026-10-26:GREEN:body=+23.1800:wick=3.3100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.64 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9408.2 | **NEUTRAL** |
| `B04_income` | 1076.56 | **GOOD** |
| `B05_profit_margin` | 11.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 560.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=560.72 vs prior_export=560.72 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.3 | **GOOD** |
| `B10_insider_transactions` | -1.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.1 vs prior=-1.1 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.91 | **GOOD** |
| `B13_short_float` | 3.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.64 (this export) | prior_export=6.64 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+16**  ·  Tobacco
price=201.8699951171875  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.81 on 2026-10-26; prev RSI=54.01 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.01@2026-10-23 → 56.81@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.01@2026-10-23 → 56.81@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.01@2026-10-23 → 56.81@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=2.641 (G=2.0600 R=0.7800); 2026-10-23:RED:O=200.2500,C=199.4700,body=-0.7800,vol=3536225.0; 2026-10-26:GREEN:O=199.8100,C=201.8700,body=+2.0600,vol=5037900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.425 (Gvol=5037900 Rvol=3536225); 2026-10-23:RED:O=200.2500,C=199.4700,body=-0.7800,vol=3536225.0; 2026-10-26:GREEN:O=199.8100,C=201.8700,body=+2.0600,vol=5037900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.118 on 2026-10-26: today_vol=5037900 / avg20=4504191 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.391 on 2026-10-26 (price=201.8700, mid=197.4065, upper=208.8116, lower=186.0014; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=201.8700 vs SMA50=192.7646 dist=+4.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=197.4065 SMA50=192.7646 SMA80=191.1916 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-08-12@184.0200; S2[2026-08-27→2026-09-25] low=2026-09-08@181.0100; S3[2026-09-28→2026-10-26] low=2026-10-21@184.0000 | lows=[184.02000427246094, 181.00999450683594, 184.0] span=1.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.5036654566618043 wick_frac=0.4963345433381957 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=0.3246349936175483 wick_frac=0.6753650063824518 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6410266442349073 need>1.4; red_wick_gt_green=False 5d trail=2026-10-20:RED:body=-1.9600:wick=1.7200; 2026-10-21:GREEN:body=+10.3200:wick=3.5100; 2026-10-22:GREEN:body=+4.7000:wick=0.1790; 2026-10-23:RED:body=-0.7800:wick=1.6227; 2026-10-26:GREEN:body=+2.0600:wick=2.0300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=83.33999633789062  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.60 on 2026-10-26; prev RSI=55.83 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.83@2026-10-23 → 55.60@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.83@2026-10-23 → 55.60@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.83@2026-10-23 → 55.60@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=16.444 (G=1.4800 R=0.0900); 2026-10-23:GREEN:O=81.9500,C=83.4300,body=+1.4800,vol=3509474.0; 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.288 (Gvol=3509474 Rvol=2724100); 2026-10-23:GREEN:O=81.9500,C=83.4300,body=+1.4800,vol=3509474.0; 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.394 on 2026-10-26: today_vol=2724100 / avg20=6918167 (avg window 2026-09-28→2026-10-23, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.277 on 2026-10-26 (price=83.3400, mid=81.4400, upper=88.2919, lower=74.5881; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=83.3400 vs SMA50=78.9116 dist=+5.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=81.4400 SMA50=78.9116 SMA80=72.9751 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-26 (63 bars); S1[2026-07-28→2026-08-26] low=2026-07-29@57.5000; S2[2026-08-27→2026-09-25] low=2026-08-28@64.4700; S3[2026-09-28→2026-10-26] low=2026-10-13@73.7600 | lows=[57.5, 64.47000122070312, 73.76000213623047] span=28.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.5461268844751622 wick_frac=0.4538731155248378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=0.040910809095606515 wick_frac=0.9590891909043935 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=16.44375688734424 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:GREEN:body=+1.5900:wick=1.7450; 2026-10-21:GREEN:body=+0.4000:wick=1.4900; 2026-10-22:GREEN:body=+1.9400:wick=0.5700; 2026-10-23:GREEN:body=+1.4800:wick=1.2300; 2026-10-26:RED:body=-0.0900:wick=2.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OXY  ·  score **+15**  ·  Oil & Gas E&P
price=64.06999969482422  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.80 on 2026-10-26; prev RSI=68.48 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.48@2026-10-23 → 67.80@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.48@2026-10-23 → 67.80@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.48@2026-10-23 → 67.80@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.3800 R=0.0000); 2026-10-23:GREEN:O=63.0500,C=64.1700,body=+1.1200,vol=11211462.0; 2026-10-26:GREEN:O=63.8100,C=64.0700,body=+0.2600,vol=7303700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=18515162 Rvol=0); 2026-10-23:GREEN:O=63.0500,C=64.1700,body=+1.1200,vol=11211462.0; 2026-10-26:GREEN:O=63.8100,C=64.0700,body=+0.2600,vol=7303700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.814 on 2026-10-26: today_vol=7303700 / avg20=8972636 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.685 on 2026-10-26 (price=64.0700, mid=60.3870, upper=65.7638, lower=55.0102; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=64.0700 vs SMA50=59.9376 dist=+6.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=60.3870 SMA50=59.9376 SMA80=58.2306 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-08-05@53.6600; S2[2026-08-27→2026-09-25] low=2026-09-22@56.1800; S3[2026-09-28→2026-10-26] low=2026-09-29@54.7600 | lows=[53.65999984741211, 56.18000030517578, 54.7599983215332] span=4.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.3468206252236181 wick_frac=0.6531793747763819 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:RED:body=-0.5800:wick=1.5870; 2026-10-21:RED:body=-0.2000:wick=0.9400; 2026-10-22:RED:body=-0.9000:wick=1.3800; 2026-10-23:GREEN:body=+1.1200:wick=1.1350; 2026-10-26:GREEN:body=+0.2600:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.47 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.0 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 24313.0 | **NEUTRAL** |
| `B04_income` | 3412.0 | **GOOD** |
| `B05_profit_margin` | 14.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.45 vs prior_export=68.45 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.13 vs prior=0.13 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.47 (this export) | prior_export=31.47 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.0 (this export) | prior_export=14.0 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CAE  ·  score **+15**  ·  Aerospace & Defense
price=26.09000015258789  pair=`2026-10-12→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.35 on 2026-10-26; prev RSI=44.59 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.59@2026-10-12 → 66.35@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.59@2026-10-12 → 66.35@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.59@2026-10-12 → 66.35@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=43.499 (G=0.8700 R=0.0200); 2026-10-12:RED:O=23.7400,C=23.7200,body=-0.0200,vol=239298.0; 2026-10-26:GREEN:O=25.2200,C=26.0900,body=+0.8700,vol=673000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_vol/RED_vol=2.812 (Gvol=673000 Rvol=239298); 2026-10-12:RED:O=23.7400,C=23.7200,body=-0.0200,vol=239298.0; 2026-10-26:GREEN:O=25.2200,C=26.0900,body=+0.8700,vol=673000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.843 on 2026-10-26: today_vol=673000 / avg20=797974 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.502 on 2026-10-26 (price=26.0900, mid=24.1362, upper=25.4370, lower=22.8355; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=26.0900 vs SMA50=24.8433 dist=+5.02% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-26: SMA20=24.1362 SMA50=24.8433 SMA80=25.0392 | **BAD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-26 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-15@23.8600; S2[2026-08-14→2026-09-14] low=2026-09-01@23.2600; S3[2026-09-15→2026-10-26] low=2026-10-08@22.9700 | lows=[23.860000610351562, 23.260000228881836, 22.969999313354492] span=3.87% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: GREEN body_frac=0.8529432114186767 wick_frac=0.1470567885813233 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: RED body_frac=0.037044756820919726 wick_frac=0.9629552431790803 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=43.49904634751097 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-0.4500:wick=0.1500; 2026-10-08:GREEN:body=+0.0300:wick=0.2700; 2026-10-09:GREEN:body=+0.2400:wick=0.1530; 2026-10-12:RED:body=-0.0200:wick=0.5199; 2026-10-26:GREEN:body=+0.8700:wick=0.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.89 (current export asof; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.52 (current export; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3610.72 | **NEUTRAL** |
| `B04_income` | 207.45 | **GOOD** |
| `B05_profit_margin` | 5.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 31.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=31.91 vs prior_export=31.91 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.02 | **GOOD** |
| `B13_short_float` | 1.87 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.89 (this export) | prior_export=12.89 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MTDR  ·  score **+15**  ·  Oil & Gas E&P
price=60.09000015258789  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.15 on 2026-10-26; prev RSI=64.83 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.83@2026-10-23 → 65.15@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.83@2026-10-23 → 65.15@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.83@2026-10-23 → 65.15@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.3200 R=0.0000); 2026-10-23:GREEN:O=57.1900,C=59.9600,body=+2.7700,vol=1922512.0; 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3946812 Rvol=0); 2026-10-23:GREEN:O=57.1900,C=59.9600,body=+2.7700,vol=1922512.0; 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.470 on 2026-10-26: today_vol=2024300 / avg20=1376948 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.938 on 2026-10-26 (price=60.0900, mid=55.1570, upper=60.4188, lower=49.8952; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=60.0900 vs SMA50=56.4388 dist=+6.47% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=55.16_50=56.44_80=54.19 on 2026-10-26: SMA20=55.1570 SMA50=56.4388 SMA80=54.1916 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-08-05@46.5316; S2[2026-08-27→2026-09-25] low=2026-09-25@51.1000; S3[2026-09-28→2026-10-26] low=2026-09-29@50.3900 | lows=[46.531587687321455, 51.099998474121094, 50.38999938964844] span=9.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.7052331337914534 wick_frac=0.29476686620854664 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:RED:body=-0.6500:wick=1.3050; 2026-10-21:GREEN:body=+0.2500:wick=0.8500; 2026-10-22:RED:body=-1.8300:wick=1.2050; 2026-10-23:GREEN:body=+2.7700:wick=0.8400; 2026-10-26:GREEN:body=+1.5500:wick=0.8600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.59 vs prior_export=70.59 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.4 vs prior=0.4 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 11.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LTH  ·  score **+15**  ·  Leisure
price=43.86000061035156  pair=`2026-10-12→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.63 on 2026-10-26; prev RSI=46.03 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.03@2026-10-12 → 65.63@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.03@2026-10-12 → 65.63@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.03@2026-10-12 → 65.63@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=5.609 (G=1.2900 R=0.2300); 2026-10-12:RED:O=40.6000,C=40.3700,body=-0.2300,vol=2086166.0; 2026-10-26:GREEN:O=42.5700,C=43.8600,body=+1.2900,vol=3648400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.749 (Gvol=3648400 Rvol=2086166); 2026-10-12:RED:O=40.6000,C=40.3700,body=-0.2300,vol=2086166.0; 2026-10-26:GREEN:O=42.5700,C=43.8600,body=+1.2900,vol=3648400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.052 on 2026-10-26: today_vol=3648400 / avg20=3466795 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.710 on 2026-10-26 (price=43.8600, mid=40.3460, upper=42.4014, lower=38.2906; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=43.8600 vs SMA50=42.3186 dist=+3.64% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=40.35_50=42.32_80=41.86 on 2026-10-26: SMA20=40.3460 SMA50=42.3186 SMA80=41.8575 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-26 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-13@40.8350; S2[2026-08-14→2026-09-14] low=2026-09-10@40.8500; S3[2026-09-15→2026-10-26] low=2026-09-23@36.5200 | lows=[40.834999084472656, 40.849998474121094, 36.52000045776367] span=11.86% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: GREEN body_frac=0.6482423619710697 wick_frac=0.3517576380289304 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: RED body_frac=0.24083771724845915 wick_frac=0.7591622827515409 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.608710795614748 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.0600:wick=0.6650; 2026-10-08:RED:body=-0.1000:wick=0.9650; 2026-10-09:GREEN:body=+0.5100:wick=0.7800; 2026-10-12:RED:body=-0.2300:wick=0.7250; 2026-10-26:GREEN:body=+1.2900:wick=0.7000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.48 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.39 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3182.44 | **NEUTRAL** |
| `B04_income` | 414.88 | **GOOD** |
| `B05_profit_margin` | 13.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 56.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=56.07 vs prior_export=56.07 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -64.3 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-64.3 vs prior=-64.3 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.58 | **GOOD** |
| `B13_short_float` | 5.99 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.48 (this export) | prior_export=20.48 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.39 (this export) | prior_export=2.39 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+15**  ·  Software - Infrastructure
price=390.4599914550781  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.26 on 2026-10-26; prev RSI=52.30 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.30@2026-10-23 → 55.26@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.30@2026-10-23 → 55.26@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.30@2026-10-23 → 55.26@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=32.5200 R=0.0000); 2026-10-23:GREEN:O=354.3000,C=380.5500,body=+26.2500,vol=6561456.0; 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=11690056 Rvol=0); 2026-10-23:GREEN:O=354.3000,C=380.5500,body=+26.2500,vol=6561456.0; 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.965 on 2026-10-26: today_vol=5128600 / avg20=5315909 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.123 on 2026-10-26 (price=390.4600, mid=384.1655, upper=435.5440, lower=332.7870; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=390.4600 vs SMA50=370.4754 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-26: SMA20=384.1655 SMA50=370.4754 SMA80=362.1515 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-07-29@310.4600; S2[2026-08-27→2026-09-25] low=2026-09-02@321.3900; S3[2026-09-28→2026-10-26] low=2026-10-20@334.7500 | lows=[310.4599914550781, 321.3900146484375, 334.75] span=7.82% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.6404388256139731 wick_frac=0.3595611743860269 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:RED:body=-5.0000:wick=6.3100; 2026-10-21:GREEN:body=+3.8100:wick=11.0800; 2026-10-22:RED:body=-0.0800:wick=13.9101; 2026-10-23:GREEN:body=+26.2500:wick=3.9599; 2026-10-26:GREEN:body=+6.2700:wick=8.9500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.35 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.74 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11480.0 | **NEUTRAL** |
| `B04_income` | 307.0 | **GOOD** |
| `B05_profit_margin` | 2.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 403.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=403.5 vs prior_export=403.5 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.66 | **GOOD** |
| `B10_insider_transactions` | -2.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.01 vs prior=-2.01 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.96 | **GOOD** |
| `B13_short_float` | 2.7 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.35 (this export) | prior_export=4.35 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.74 (this export) | prior_export=1.74 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CSCO  ·  score **+15**  ·  Communication Equipment
price=110.23999786376953  pair=`2026-10-23→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.32 on 2026-10-26; prev RSI=41.80 on 2026-10-23 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.80@2026-10-23 → 50.32@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.80@2026-10-23 → 50.32@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.80@2026-10-23 → 50.32@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.3500 R=0.0000); 2026-10-23:GREEN:O=104.2400,C=106.5000,body=+2.2600,vol=13287322.0; 2026-10-26:GREEN:O=108.1500,C=110.2400,body=+2.0900,vol=20455000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-23 + 2026-10-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=33742322 Rvol=0); 2026-10-23:GREEN:O=104.2400,C=106.5000,body=+2.2600,vol=13287322.0; 2026-10-26:GREEN:O=108.1500,C=110.2400,body=+2.0900,vol=20455000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.341 on 2026-10-26: today_vol=20455000 / avg20=15252762 (avg window 2026-09-28→2026-10-23, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.089 on 2026-10-26 (price=110.2400, mid=111.0175, upper=119.7699, lower=102.2651; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-26: price=110.2400 vs SMA50=110.1970 dist=+0.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=111.02_50=110.20_80=112.33 on 2026-10-26: SMA20=111.0175 SMA50=110.1970 SMA80=112.3301 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-26 (63 bars); S1[2026-07-29→2026-08-26] low=2026-08-20@109.2300; S2[2026-08-27→2026-09-25] low=2026-09-22@104.5100; S3[2026-09-28→2026-10-26] low=2026-10-22@103.5800 | lows=[109.2300033569336, 104.51000213623047, 103.58000183105469] span=5.45% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: GREEN body_frac=0.6853704252890807 wick_frac=0.3146295747109193 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-23+2026-10-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-20:GREEN:body=+0.6050:wick=1.1550; 2026-10-21:GREEN:body=+0.0300:wick=1.1800; 2026-10-22:RED:body=-0.8500:wick=1.8700; 2026-10-23:GREEN:body=+2.2600:wick=0.6550; 2026-10-26:GREEN:body=+2.0900:wick=1.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.52 (current export asof; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.47 (current export; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 63325.0 | **NEUTRAL** |
| `B04_income` | 13267.0 | **GOOD** |
| `B05_profit_margin` | 20.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 139.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=139.73 vs prior_export=139.73 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -1.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.15 vs prior=-1.15 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.24 | **GOOD** |
| `B13_short_float` | 1.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.47 (this export) | prior_export=2.47 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+15**  ·  Engineering & Construction
price=40.02000045776367  pair=`2026-10-12→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.65 on 2026-10-26; prev RSI=47.19 on 2026-10-12 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 47.19@2026-10-12 → 71.65@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.19@2026-10-12 → 71.65@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | cross_up | RSI 47.19@2026-10-12 → 71.65@2026-10-26 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.7200 R=0.1800); 2026-10-12:RED:O=35.3700,C=35.1900,body=-0.1800,vol=907134.0; 2026-10-26:GREEN:O=39.3000,C=40.0200,body=+0.7200,vol=1973600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_vol/RED_vol=2.176 (Gvol=1973600 Rvol=907134); 2026-10-12:RED:O=35.3700,C=35.1900,body=-0.1800,vol=907134.0; 2026-10-26:GREEN:O=39.3000,C=40.0200,body=+0.7200,vol=1973600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.222 on 2026-10-26: today_vol=1973600 / avg20=1615307 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.804 on 2026-10-26 (price=40.0200, mid=35.3725, upper=37.9487, lower=32.7963; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=40.0200 vs SMA50=36.6538 dist=+9.18% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=35.37_50=36.65_80=36.09 on 2026-10-26: SMA20=35.3725 SMA50=36.6538 SMA80=36.0939 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-26 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-30@32.1400; S2[2026-08-14→2026-09-14] low=2026-09-11@35.7500; S3[2026-09-15→2026-10-26] low=2026-10-07@33.3100 | lows=[32.13999938964844, 35.75, 33.310001373291016] span=11.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: GREEN body_frac=0.7912102653101434 wick_frac=0.20878973468985668 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: RED body_frac=0.22015686164728057 wick_frac=0.7798431383527195 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-0.6100:wick=0.5800; 2026-10-08:GREEN:body=+1.2400:wick=0.3200; 2026-10-09:GREEN:body=+0.1500:wick=0.7300; 2026-10-12:RED:body=-0.1800:wick=0.6376; 2026-10-26:GREEN:body=+0.7200:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.17 vs prior_export=44.17 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.86 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=89.81999969482422  pair=`2026-10-12→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.00 on 2026-10-26; prev RSI=42.80 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.80@2026-10-12 → 59.00@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.80@2026-10-12 → 59.00@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.80@2026-10-12 → 59.00@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=9.416 (G=1.1300 R=0.1200); 2026-10-12:GREEN:O=84.4000,C=85.5300,body=+1.1300,vol=1729371.0; 2026-10-26:RED:O=89.9400,C=89.8200,body=-0.1200,vol=1317000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.313 (Gvol=1729371 Rvol=1317000); 2026-10-12:GREEN:O=84.4000,C=85.5300,body=+1.1300,vol=1729371.0; 2026-10-26:RED:O=89.9400,C=89.8200,body=-0.1200,vol=1317000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.590 on 2026-10-26: today_vol=1317000 / avg20=2233588 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.033 on 2026-10-26 (price=89.8200, mid=86.4425, upper=89.7137, lower=83.1713; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=89.8200 vs SMA50=89.6166 dist=+0.23% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.44_50=89.62_80=89.40 on 2026-10-26: SMA20=86.4425 SMA50=89.6166 SMA80=89.3996 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-26 (63 bars); S1[2026-07-08→2026-08-10] low=2026-07-30@87.2000; S2[2026-08-11→2026-09-14] low=2026-09-14@88.0400; S3[2026-09-15→2026-10-26] low=2026-10-08@83.2500 | lows=[87.19999694824219, 88.04000091552734, 83.25] span=5.75% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: GREEN body_frac=0.7793059903712083 wick_frac=0.22069400962879165 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: RED body_frac=0.1153871547518615 wick_frac=0.8846128452481385 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.416428253544408 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-1.2200:wick=0.6450; 2026-10-08:GREEN:body=+1.0800:wick=0.4100; 2026-10-09:RED:body=-0.0100:wick=0.7600; 2026-10-12:GREEN:body=+1.1300:wick=0.3200; 2026-10-26:RED:body=-0.1200:wick=0.9200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.75 vs prior_export=100.75 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.2 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LDOS  ·  score **+14**  ·  Information Technology Services
price=139.86000061035156  pair=`2026-10-12→2026-10-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.11 on 2026-10-26; prev RSI=45.64 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.64@2026-10-12 → 67.11@2026-10-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.64@2026-10-12 → 67.11@2026-10-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.64@2026-10-12 → 67.11@2026-10-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_body_sum/RED_body_sum=3.341 (G=2.8400 R=0.8500); 2026-10-12:RED:O=122.4500,C=121.6000,body=-0.8500,vol=643473.0; 2026-10-26:GREEN:O=137.0200,C=139.8600,body=+2.8400,vol=807300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-26; ratio=GREEN_vol/RED_vol=1.255 (Gvol=807300 Rvol=643473); 2026-10-12:RED:O=122.4500,C=121.6000,body=-0.8500,vol=643473.0; 2026-10-26:GREEN:O=137.0200,C=139.8600,body=+2.8400,vol=807300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.633 on 2026-10-26: today_vol=807300 / avg20=1276070 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.424 on 2026-10-26 (price=139.8600, mid=123.5115, upper=134.9892, lower=112.0338; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-26: price=139.8600 vs SMA50=131.3896 dist=+6.45% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=123.51_50=131.39_80=122.70 on 2026-10-26: SMA20=123.5115 SMA50=131.3896 SMA80=122.6969 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-26 (63 bars); S1[2026-07-14→2026-08-13] low=2026-07-14@104.0100; S2[2026-08-14→2026-09-14] low=2026-09-11@126.7600; S3[2026-09-15→2026-10-26] low=2026-10-07@112.0700 | lows=[104.01000213623047, 126.76000213623047, 112.06999969482422] span=21.87% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: GREEN body_frac=0.6528740954325262 wick_frac=0.34712590456747383 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-26: RED body_frac=0.3427408562753223 wick_frac=0.6572591437246776 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3411781601457666 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-2.4000:wick=2.1400; 2026-10-08:GREEN:body=+6.7700:wick=0.6100; 2026-10-09:GREEN:body=+2.4800:wick=1.5250; 2026-10-12:RED:body=-0.8500:wick=1.6300; 2026-10-26:GREEN:body=+2.8400:wick=1.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.1 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.74 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17634.0 | **NEUTRAL** |
| `B04_income` | 1376.0 | **GOOD** |
| `B05_profit_margin` | 7.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 157.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=157.73 vs prior_export=157.73 on finviz_2026-10-26) | **NEUTRAL** |
| `B09_analyst_recom` | 2.3 | **GOOD** |
| `B10_insider_transactions` | -1.37 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.37 vs prior=-1.37 on finviz_2026-10-26) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.33 | **GOOD** |
| `B13_short_float` | 4.21 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.1 (this export) | prior_export=12.1 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-10-26) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-27_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.