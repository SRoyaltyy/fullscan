# A+B1 Feature Checklist — 2026-10-23

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-23.csv` · prior export for Δ: `2026-10-22`
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
| 1 | ASC | +16 | 17 | 1 | 2026-10-21→2026-10-22 | Marine Shipping |
| 2 | HSIC | +16 | 17 | 1 | 2026-10-21→2026-10-22 | Medical Distribution |
| 3 | NWBI | +16 | 17 | 1 | 2026-10-21→2026-10-22 | Banks - Regional |
| 4 | TRI | +15 | 17 | 2 | 2026-10-21→2026-10-22 | Specialty Business Services |
| 5 | RCI | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Telecom Services |
| 6 | PAGP | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Oil & Gas Midstream |
| 7 | AUPH | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Biotechnology |
| 8 | PM | +15 | 15 | 0 | 2026-10-21→2026-10-22 | Tobacco |
| 9 | ICE | +15 | 17 | 2 | 2026-10-21→2026-10-22 | Financial Data & Stock Exchanges |
| 10 | GCT | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Software - Infrastructure |
| 11 | CP | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Railroads |
| 12 | ALC | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Medical Instruments & Supplies |
| 13 | UHS | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Medical Care Facilities |
| 14 | CURI | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Broadcasting |
| 15 | INSW | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Oil & Gas Midstream |

## Full checklist — top 15

### ASC  ·  score **+16**  ·  Marine Shipping
price=19.34000015258789  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.77 on 2026-10-22; prev RSI=58.74 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.74@2026-10-21 → 55.77@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.74@2026-10-21 → 55.77@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.74@2026-10-21 → 55.77@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=10.571 (G=0.7400 R=0.0700); 2026-10-21:GREEN:O=18.8800,C=19.6200,body=+0.7400,vol=567600.0; 2026-10-22:RED:O=19.4100,C=19.3400,body=-0.0700,vol=418600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.356 (Gvol=567600 Rvol=418600); 2026-10-21:GREEN:O=18.8800,C=19.6200,body=+0.7400,vol=567600.0; 2026-10-22:RED:O=19.4100,C=19.3400,body=-0.0700,vol=418600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.609 on 2026-10-22: today_vol=418600 / avg20=687716 (avg window 2026-09-17→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.420 on 2026-10-22 (price=19.3400, mid=18.7775, upper=20.1168, lower=17.4382; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=19.3400 vs SMA50=18.2200 dist=+6.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=18.7775 SMA50=18.2200 SMA80=17.4146 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-22 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-17@15.1700; S2[2026-08-18→2026-09-16] low=2026-08-28@16.6100; S3[2026-09-17→2026-10-22] low=2026-09-23@17.2000 | lows=[15.170000076293945, 16.610000610351562, 17.200000762939453] span=13.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8705917728051987 wick_frac=0.12940822719480125 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.08045951811983426 wick_frac=0.9195404818801658 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.57149863760218 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.2700:wick=0.2562; 2026-10-12:GREEN:body=+0.0300:wick=0.4700; 2026-10-20:GREEN:body=+0.3500:wick=0.2500; 2026-10-21:GREEN:body=+0.7400:wick=0.1100; 2026-10-22:RED:body=-0.0700:wick=0.8000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.72 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=34.7 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 368.29 | **NEUTRAL** |
| `B04_income` | 105.58 | **GOOD** |
| `B05_profit_margin` | 28.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.0 vs prior_export=21.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.93 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.93 vs prior=-0.93 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 7.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.72 (this export) | prior_export=6.72 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=34.7 (this export) | prior_export=34.7 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSIC  ·  score **+16**  ·  Medical Distribution
price=86.0  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.80 on 2026-10-22; prev RSI=46.91 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.91@2026-10-21 → 51.80@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.91@2026-10-21 → 51.80@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.91@2026-10-21 → 51.80@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5200 R=0.0000); 2026-10-21:GREEN:O=84.6800,C=85.0100,body=+0.3300,vol=1263900.0; 2026-10-22:GREEN:O=84.8100,C=86.0000,body=+1.1900,vol=2075100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3339000 Rvol=0); 2026-10-21:GREEN:O=84.6800,C=85.0100,body=+0.3300,vol=1263900.0; 2026-10-22:GREEN:O=84.8100,C=86.0000,body=+1.1900,vol=2075100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.539 on 2026-10-22: today_vol=2075100 / avg20=1348272 (avg window 2026-09-17→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.543 on 2026-10-22 (price=86.0000, mid=85.0225, upper=86.8236, lower=83.2214; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-22: price=86.0000 vs SMA50=87.3834 dist=-1.58% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=85.02_50=87.38_80=86.52 on 2026-10-22: SMA20=85.0225 SMA50=87.3834 SMA80=86.5211 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-22 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@83.1900; S2[2026-08-18→2026-09-16] low=2026-09-16@86.4400; S3[2026-09-17→2026-10-22] low=2026-10-08@79.3700 | lows=[83.19000244140625, 86.44000244140625, 79.37000274658203] span=8.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.5210834022462827 wick_frac=0.47891659775371725 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1000:wick=1.5150; 2026-10-12:GREEN:body=+0.0700:wick=0.9600; 2026-10-20:GREEN:body=+0.1300:wick=1.2800; 2026-10-21:GREEN:body=+0.3300:wick=1.1600; 2026-10-22:GREEN:body=+1.1900:wick=0.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.45 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13602.0 | **NEUTRAL** |
| `B04_income` | 403.0 | **GOOD** |
| `B05_profit_margin` | 2.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.64 vs prior_export=99.64 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.49 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+16**  ·  Banks - Regional
price=15.4399995803833  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.84 on 2026-10-22; prev RSI=48.90 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.90@2026-10-21 → 57.84@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.90@2026-10-21 → 57.84@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.90@2026-10-21 → 57.84@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=6.500 (G=0.3900 R=0.0600); 2026-10-21:RED:O=15.1200,C=15.0600,body=-0.0600,vol=779600.0; 2026-10-22:GREEN:O=15.0500,C=15.4400,body=+0.3900,vol=974200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.250 (Gvol=974200 Rvol=779600); 2026-10-21:RED:O=15.1200,C=15.0600,body=-0.0600,vol=779600.0; 2026-10-22:GREEN:O=15.0500,C=15.4400,body=+0.3900,vol=974200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.871 on 2026-10-22: today_vol=974200 / avg20=1117917 (avg window 2026-09-17→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.859 on 2026-10-22 (price=15.4400, mid=15.0610, upper=15.5023, lower=14.6197; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=15.4400 vs SMA50=15.3120 dist=+0.84% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.06_50=15.31_80=15.23 on 2026-10-22: SMA20=15.0610 SMA50=15.3120 SMA80=15.2252 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-22 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@14.8812; S2[2026-08-18→2026-09-16] low=2026-09-01@15.0700; S3[2026-09-17→2026-10-22] low=2026-10-01@14.5400 | lows=[14.881151984844093, 15.069999694824219, 14.539999961853027] span=3.65% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.9069766926081257 wick_frac=0.09302330739187424 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.2857116906826036 wick_frac=0.7142883093173964 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.500047684140255 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1000:wick=0.1050; 2026-10-12:GREEN:body=+0.1700:wick=0.0200; 2026-10-20:GREEN:body=+0.2000:wick=0.1400; 2026-10-21:RED:body=-0.0600:wick=0.1500; 2026-10-22:GREEN:body=+0.3900:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TRI  ·  score **+15**  ·  Specialty Business Services
price=123.26000213623047  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.13 on 2026-10-22; prev RSI=72.53 on 2026-10-21 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.8100 R=0.0000); 2026-10-21:GREEN:O=112.3700,C=115.2600,body=+2.8900,vol=1596597.0; 2026-10-22:GREEN:O=117.3400,C=123.2600,body=+5.9200,vol=2662214.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4258811 Rvol=0); 2026-10-21:GREEN:O=112.3700,C=115.2600,body=+2.8900,vol=1596597.0; 2026-10-22:GREEN:O=117.3400,C=123.2600,body=+5.9200,vol=2662214.0 | **GOOD** |
| `A07_rvol` | RVOL=1.883 on 2026-10-22: today_vol=2662214 / avg20=1413955 (avg window 2026-09-24→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.347 on 2026-10-22 (price=123.2600, mid=103.8670, upper=118.2663, lower=89.4677; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=123.2600 vs SMA50=102.9558 dist=+19.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=103.8670 SMA50=102.9558 SMA80=100.0015 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@86.0700; S2[2026-08-25→2026-09-23] low=2026-09-21@93.6500; S3[2026-09-24→2026-10-22] low=2026-09-29@92.5700 | lows=[86.06999969482422, 93.6500015258789, 92.56999969482422] span=8.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.7585544374069635 wick_frac=0.2414455625930365 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-0.4300:wick=3.3200; 2026-10-19:RED:body=-0.5700:wick=1.5200; 2026-10-20:GREEN:body=+3.2600:wick=0.5000; 2026-10-21:GREEN:body=+2.8900:wick=1.7300; 2026-10-22:GREEN:body=+5.9200:wick=0.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.32 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7832.0 | **NEUTRAL** |
| `B04_income` | 1658.0 | **GOOD** |
| `B05_profit_margin` | 21.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.64 vs prior_export=122.64 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 9.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.32 (this export) | prior_export=3.32 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.86 (this export) | prior_export=1.86 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RCI  ·  score **+15**  ·  Telecom Services
price=33.20000076293945  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.22 on 2026-10-22; prev RSI=44.95 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.95@2026-10-21 → 54.22@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.95@2026-10-21 → 54.22@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.95@2026-10-21 → 54.22@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=4.296 (G=1.1600 R=0.2700); 2026-10-21:RED:O=32.0200,C=31.7500,body=-0.2700,vol=1318200.0; 2026-10-22:GREEN:O=32.0400,C=33.2000,body=+1.1600,vol=2431100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.844 (Gvol=2431100 Rvol=1318200); 2026-10-21:RED:O=32.0200,C=31.7500,body=-0.2700,vol=1318200.0; 2026-10-22:GREEN:O=32.0400,C=33.2000,body=+1.1600,vol=2431100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.614 on 2026-10-22: today_vol=2431100 / avg20=1506302 (avg window 2026-09-17→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.501 on 2026-10-22 (price=33.2000, mid=31.7205, upper=34.6732, lower=28.7678; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-22: price=33.2000 vs SMA50=34.4142 dist=-3.53% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=31.72_50=34.41_80=34.21 on 2026-10-22: SMA20=31.7205 SMA50=34.4142 SMA80=34.2144 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-22 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@32.2600; S2[2026-08-18→2026-09-16] low=2026-09-10@35.6300; S3[2026-09-17→2026-10-22] low=2026-10-09@29.2550 | lows=[32.2599983215332, 35.630001068115234, 29.2549991607666] span=21.79% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8226939033908829 wick_frac=0.17730609660911711 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.3552645448202822 wick_frac=0.6447354551797179 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.296288447138275 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.7300:wick=0.5350; 2026-10-12:GREEN:body=+0.0900:wick=0.2650; 2026-10-20:GREEN:body=+0.1200:wick=0.5500; 2026-10-21:RED:body=-0.2700:wick=0.4900; 2026-10-22:GREEN:body=+1.1600:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.94 (current export asof; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.28 (current export; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16362.29 | **NEUTRAL** |
| `B04_income` | 4477.33 | **GOOD** |
| `B05_profit_margin` | 27.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 42.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=42.47 vs prior_export=42.47 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.55 | **GOOD** |
| `B13_short_float` | 3.58 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.28 (this export) | prior_export=1.28 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+15**  ·  Oil & Gas Midstream
price=27.93000030517578  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.45 on 2026-10-22; prev RSI=68.35 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.35@2026-10-21 → 68.45@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.35@2026-10-21 → 68.45@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.35@2026-10-21 → 68.45@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=5.714 (G=0.4000 R=0.0700); 2026-10-21:GREEN:O=27.5200,C=27.9200,body=+0.4000,vol=1715300.0; 2026-10-22:RED:O=28.0000,C=27.9300,body=-0.0700,vol=1590800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.078 (Gvol=1715300 Rvol=1590800); 2026-10-21:GREEN:O=27.5200,C=27.9200,body=+0.4000,vol=1715300.0; 2026-10-22:RED:O=28.0000,C=27.9300,body=-0.0700,vol=1590800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.992 on 2026-10-22: today_vol=1590800 / avg20=1603576 (avg window 2026-09-17→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.947 on 2026-10-22 (price=27.9300, mid=26.7198, upper=27.9976, lower=25.4419; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=27.9300 vs SMA50=26.9085 dist=+3.80% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=26.72_50=26.91_80=26.09 on 2026-10-22: SMA20=26.7198 SMA50=26.9085 SMA80=26.0916 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-22 (63 bars); S1[2026-07-16→2026-08-17] low=2026-08-07@24.6700; S2[2026-08-18→2026-09-16] low=2026-08-19@25.8500; S3[2026-09-17→2026-10-22] low=2026-10-02@25.6200 | lows=[24.670000076293945, 25.850000381469727, 25.6200008392334] span=4.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8888903017433211 wick_frac=0.11110969825667892 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.10769190139295219 wick_frac=0.8923080986070479 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.714305177111717 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.0100:wick=0.4450; 2026-10-12:GREEN:body=+0.0250:wick=0.2200; 2026-10-20:GREEN:body=+0.1700:wick=0.0800; 2026-10-21:GREEN:body=+0.4000:wick=0.0500; 2026-10-22:RED:body=-0.0700:wick=0.5800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.08 vs prior_export=26.08 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 9.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+15**  ·  Biotechnology
price=17.329999923706055  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.60 on 2026-10-22; prev RSI=61.06 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.06@2026-10-21 → 66.60@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.06@2026-10-21 → 66.60@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.06@2026-10-21 → 66.60@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5000 R=0.0000); 2026-10-21:GREEN:O=16.7300,C=16.9000,body=+0.1700,vol=783200.0; 2026-10-22:GREEN:O=17.0000,C=17.3300,body=+0.3300,vol=780400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1563600 Rvol=0); 2026-10-21:GREEN:O=16.7300,C=16.9000,body=+0.1700,vol=783200.0; 2026-10-22:GREEN:O=17.0000,C=17.3300,body=+0.3300,vol=780400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.985 on 2026-10-22: today_vol=780400 / avg20=792514 (avg window 2026-09-17→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.071 on 2026-10-22 (price=17.3300, mid=16.4170, upper=17.2694, lower=15.5646; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=17.3300 vs SMA50=16.2437 dist=+6.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=16.4170 SMA50=16.2437 SMA80=16.1296 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-22 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-31@14.3630; S2[2026-08-18→2026-09-16] low=2026-08-18@15.1500; S3[2026-09-17→2026-10-22] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.149999618530273, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.5106397568249299 wick_frac=0.4893602431750702 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-20:GREEN:body=+0.1400:wick=0.1290; 2026-10-21:GREEN:body=+0.1700:wick=0.2100; 2026-10-22:GREEN:body=+0.3300:wick=0.2450 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+15**  ·  Tobacco
price=201.74000549316406  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.27 on 2026-10-22; prev RSI=52.22 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=15.0200 R=0.0000); 2026-10-21:GREEN:O=187.1400,C=197.4600,body=+10.3200,vol=5768726.0; 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10867148 Rvol=0); 2026-10-21:GREEN:O=187.1400,C=197.4600,body=+10.3200,vol=5768726.0; 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0 | **GOOD** |
| `A07_rvol` | RVOL=1.158 on 2026-10-22: today_vol=5098422 / avg20=4402108 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.447 on 2026-10-22 (price=201.7400, mid=196.5515, upper=208.1622, lower=184.9408; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=201.7400 vs SMA50=192.3238 dist=+4.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=196.5515 SMA50=192.3238 SMA80=190.6744 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-08-12@184.0200; S2[2026-08-25→2026-09-23] low=2026-09-08@181.0100; S3[2026-09-24→2026-10-22] low=2026-10-21@184.0000 | lows=[184.02000427246094, 181.00999450683594, 184.0] span=1.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8547597126315387 wick_frac=0.14524028736846134 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-16:GREEN:body=+2.2200:wick=1.8800; 2026-10-19:RED:body=-1.0900:wick=1.2100; 2026-10-20:RED:body=-1.9600:wick=1.7200; 2026-10-21:GREEN:body=+10.3200:wick=3.5100; 2026-10-22:GREEN:body=+4.7000:wick=0.1790 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICE  ·  score **+15**  ·  Financial Data & Stock Exchanges
price=163.83999633789062  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.36 on 2026-10-22; prev RSI=53.93 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=9.372 (G=4.0300 R=0.4300); 2026-10-21:RED:O=158.1200,C=157.6900,body=-0.4300,vol=2767634.0; 2026-10-22:GREEN:O=159.8100,C=163.8400,body=+4.0300,vol=4734792.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.711 (Gvol=4734792 Rvol=2767634); 2026-10-21:RED:O=158.1200,C=157.6900,body=-0.4300,vol=2767634.0; 2026-10-22:GREEN:O=159.8100,C=163.8400,body=+4.0300,vol=4734792.0 | **GOOD** |
| `A07_rvol` | RVOL=1.741 on 2026-10-22: today_vol=4734792 / avg20=2720032 (avg window 2026-09-24→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.166 on 2026-10-22 (price=163.8400, mid=155.5125, upper=162.6543, lower=148.3707; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=163.8400 vs SMA50=156.9728 dist=+4.37% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=155.51_50=156.97_80=151.61 on 2026-10-22: SMA20=155.5125 SMA50=156.9728 SMA80=151.6109 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-27@144.8900; S2[2026-08-25→2026-09-23] low=2026-09-23@151.4200; S3[2026-09-24→2026-10-22] low=2026-10-05@147.5100 | lows=[144.88999938964844, 151.4199981689453, 147.50999450683594] span=4.51% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8732381327099775 wick_frac=0.12676186729002245 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.16667060966665878 wick_frac=0.8333293903333412 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.372249822569199 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:GREEN:body=+2.2200:wick=1.0800; 2026-10-19:RED:body=-1.9400:wick=0.4550; 2026-10-20:RED:body=-1.0700:wick=3.6700; 2026-10-21:RED:body=-0.4300:wick=2.1499; 2026-10-22:GREEN:body=+4.0300:wick=0.5850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.49 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.33 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13138.0 | **NEUTRAL** |
| `B04_income` | 4038.0 | **GOOD** |
| `B05_profit_margin` | 30.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 186.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=186.86 vs prior_export=186.86 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | -2.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.02 vs prior=-2.02 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.15 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.49 (this export) | prior_export=3.49 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.33 (this export) | prior_export=1.33 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+15**  ·  Software - Infrastructure
price=59.119998931884766  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.03 on 2026-10-22; prev RSI=69.22 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=1.782 (G=1.3900 R=0.7800); 2026-10-21:GREEN:O=59.0000,C=60.3900,body=+1.3900,vol=619879.0; 2026-10-22:RED:O=59.9000,C=59.1200,body=-0.7800,vol=525628.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.179 (Gvol=619879 Rvol=525628); 2026-10-21:GREEN:O=59.0000,C=60.3900,body=+1.3900,vol=619879.0; 2026-10-22:RED:O=59.9000,C=59.1200,body=-0.7800,vol=525628.0 | **GOOD** |
| `A07_rvol` | RVOL=0.772 on 2026-10-22: today_vol=525628 / avg20=681066 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.562 on 2026-10-22 (price=59.1200, mid=56.0150, upper=61.5406, lower=50.4894; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=59.1200 vs SMA50=53.3621 dist=+10.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=56.0150 SMA50=53.3621 SMA80=48.2992 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@35.6100; S2[2026-08-25→2026-09-23] low=2026-09-01@46.0100; S3[2026-09-24→2026-10-22] low=2026-09-29@49.4300 | lows=[35.61000061035156, 46.0099983215332, 49.43000030517578] span=38.81% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.5537841460708651 wick_frac=0.4462158539291348 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.3000126183335583 wick_frac=0.6999873816664417 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7820445731221237 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-1.9000:wick=1.3099; 2026-10-19:GREEN:body=+0.3600:wick=0.6800; 2026-10-20:GREEN:body=+1.0500:wick=0.9899; 2026-10-21:GREEN:body=+1.3900:wick=1.1200; 2026-10-22:RED:body=-0.7800:wick=1.8199 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=90.62000274658203  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.16 on 2026-10-22; prev RSI=53.95 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.95@2026-10-21 → 62.16@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.95@2026-10-21 → 62.16@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.95@2026-10-21 → 62.16@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.3000 R=0.0000); 2026-10-21:GREEN:O=87.3300,C=88.1000,body=+0.7700,vol=1280000.0; 2026-10-22:GREEN:O=88.0900,C=90.6200,body=+2.5300,vol=4199200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5479200 Rvol=0); 2026-10-21:GREEN:O=87.3300,C=88.1000,body=+0.7700,vol=1280000.0; 2026-10-22:GREEN:O=88.0900,C=90.6200,body=+2.5300,vol=4199200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.931 on 2026-10-22: today_vol=4199200 / avg20=2174303 (avg window 2026-09-17→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.249 on 2026-10-22 (price=90.6200, mid=86.4310, upper=89.7842, lower=83.0778; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=90.6200 vs SMA50=89.5522 dist=+1.19% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=86.43_50=89.55_80=89.36 on 2026-10-22: SMA20=86.4310 SMA50=89.5522 SMA80=89.3649 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-22 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-30@87.2000; S2[2026-08-13→2026-09-16] low=2026-09-16@87.4900; S3[2026-09-17→2026-10-22] low=2026-10-08@83.2500 | lows=[87.19999694824219, 87.48999786376953, 83.25] span=5.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.6920610594677346 wick_frac=0.30793894053226545 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.0100:wick=0.7600; 2026-10-12:GREEN:body=+1.1300:wick=0.3200; 2026-10-20:GREEN:body=+0.3500:wick=0.8600; 2026-10-21:GREEN:body=+0.7700:wick=0.4500; 2026-10-22:GREEN:body=+2.5300:wick=0.8300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.75 vs prior_export=100.75 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.2 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ALC  ·  score **+14**  ·  Medical Instruments & Supplies
price=70.62999725341797  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.81 on 2026-10-22; prev RSI=57.63 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.63@2026-10-21 → 67.81@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.63@2026-10-21 → 67.81@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.63@2026-10-21 → 67.81@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=20.880 (G=2.6100 R=0.1250); 2026-10-21:RED:O=67.5550,C=67.4300,body=-0.1250,vol=1073916.0; 2026-10-22:GREEN:O=68.0200,C=70.6300,body=+2.6100,vol=2197779.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=2.047 (Gvol=2197779 Rvol=1073916); 2026-10-21:RED:O=67.5550,C=67.4300,body=-0.1250,vol=1073916.0; 2026-10-22:GREEN:O=68.0200,C=70.6300,body=+2.6100,vol=2197779.0 | **GOOD** |
| `A07_rvol` | RVOL=1.289 on 2026-10-22: today_vol=2197779 / avg20=1705203 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.432 on 2026-10-22 (price=70.6300, mid=65.0690, upper=68.9531, lower=61.1849; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=70.6300 vs SMA50=68.0236 dist=+3.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-22: SMA20=65.0690 SMA50=68.0236 SMA80=68.4826 | **BAD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-27@67.2300; S2[2026-08-25→2026-09-23] low=2026-09-18@64.5400; S3[2026-09-24→2026-10-22] low=2026-10-13@62.1050 | lows=[67.2300033569336, 64.54000091552734, 62.10499954223633] span=8.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.9738807138607294 wick_frac=0.026119286139270597 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.125 wick_frac=0.875 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.8800048828125 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-0.7700:wick=0.2100; 2026-10-19:GREEN:body=+0.6300:wick=0.0900; 2026-10-20:GREEN:body=+0.4400:wick=0.3650; 2026-10-21:RED:body=-0.1250:wick=0.8750; 2026-10-22:GREEN:body=+2.6100:wick=0.0700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.36 (current export asof; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.72 (current export; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10861.0 | **NEUTRAL** |
| `B04_income` | 643.0 | **GOOD** |
| `B05_profit_margin` | 5.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 85.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=85.54 vs prior_export=85.54 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.82 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 3.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.36 (this export) | prior_export=11.36 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.72 (this export) | prior_export=0.72 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UHS  ·  score **+14**  ·  Medical Care Facilities
price=188.1300048828125  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.41 on 2026-10-22; prev RSI=60.45 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.45@2026-10-21 → 66.41@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.45@2026-10-21 → 66.41@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.45@2026-10-21 → 66.41@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=2.459 (G=4.7700 R=1.9400); 2026-10-21:RED:O=184.8100,C=182.8700,body=-1.9400,vol=338500.0; 2026-10-22:GREEN:O=183.3600,C=188.1300,body=+4.7700,vol=489100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.445 (Gvol=489100 Rvol=338500); 2026-10-21:RED:O=184.8100,C=182.8700,body=-1.9400,vol=338500.0; 2026-10-22:GREEN:O=183.3600,C=188.1300,body=+4.7700,vol=489100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.710 on 2026-10-22: today_vol=489100 / avg20=688444 (avg window 2026-09-17→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.335 on 2026-10-22 (price=188.1300, mid=178.1240, upper=185.6182, lower=170.6298; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=188.1300 vs SMA50=175.1352 dist=+7.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=178.1240 SMA50=175.1352 SMA80=167.3255 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-22 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-15@143.6000; S2[2026-08-18→2026-09-16] low=2026-09-03@166.4300; S3[2026-09-17→2026-10-22] low=2026-10-08@173.3900 | lows=[143.60000610351562, 166.42999267578125, 173.38999938964844] span=20.75% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8057442141592327 wick_frac=0.19425578584076728 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.39271285073575746 wick_frac=0.6072871492642425 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.458761994651565 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+2.0400:wick=1.8500; 2026-10-12:GREEN:body=+1.2700:wick=2.6950; 2026-10-20:GREEN:body=+1.5100:wick=2.7800; 2026-10-21:RED:body=-1.9400:wick=3.0000; 2026-10-22:GREEN:body=+4.7700:wick=1.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.67 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.29 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18114.49 | **NEUTRAL** |
| `B04_income` | 1526.03 | **GOOD** |
| `B05_profit_margin` | 8.42 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 193.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=193.25 vs prior_export=193.25 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.58 | **BAD** |
| `B13_short_float` | 5.58 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.67 (this export) | prior_export=0.67 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.29 (this export) | prior_export=1.29 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CURI  ·  score **+14**  ·  Broadcasting
price=2.880000114440918  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.24 on 2026-10-22; prev RSI=48.45 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.45@2026-10-21 → 54.24@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.45@2026-10-21 → 54.24@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.45@2026-10-21 → 54.24@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=10.000 (G=0.1000 R=0.0100); 2026-10-21:RED:O=2.7900,C=2.7800,body=-0.0100,vol=564021.0; 2026-10-22:GREEN:O=2.7800,C=2.8800,body=+0.1000,vol=428036.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=0.759 (Gvol=428036 Rvol=564021); 2026-10-21:RED:O=2.7900,C=2.7800,body=-0.0100,vol=564021.0; 2026-10-22:GREEN:O=2.7800,C=2.8800,body=+0.1000,vol=428036.0 | **BAD** |
| `A07_rvol` | RVOL=0.837 on 2026-10-22: today_vol=428036 / avg20=511376 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.138 on 2026-10-22 (price=2.8800, mid=2.8455, upper=3.0954, lower=2.5956; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=2.8800 vs SMA50=2.8531 dist=+0.94% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=2.85_50=2.85_80=2.77 on 2026-10-22: SMA20=2.8455 SMA50=2.8531 SMA80=2.7689 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@2.3450; S2[2026-08-25→2026-09-23] low=2026-08-27@2.5100; S3[2026-09-24→2026-10-22] low=2026-10-19@2.5800 | lows=[2.3450000286102295, 2.509999990463257, 2.5799999237060547] span=10.02% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.7142862008538785 wick_frac=0.28571379914612155 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.1 wick_frac=0.9 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.000023841880648 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-0.1200:wick=0.0500; 2026-10-19:DOJI:body=+0.0000:wick=0.0750; 2026-10-20:GREEN:body=+0.2000:wick=0.0000; 2026-10-21:RED:body=-0.0100:wick=0.0900; 2026-10-22:GREEN:body=+0.1000:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=300.0 (current export asof; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.01 (current export; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 75.96 | **NEUTRAL** |
| `B04_income` | 0.02 | **GOOD** |
| `B05_profit_margin` | 0.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.38 vs prior_export=5.38 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.73 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.73 vs prior=0.73 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.23 | **BAD** |
| `B13_short_float` | 6.09 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=300.0 (this export) | prior_export=300.0 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.01 (this export) | prior_export=9.01 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INSW  ·  score **+14**  ·  Oil & Gas Midstream
price=108.41999816894531  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.58 on 2026-10-22; prev RSI=50.05 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.05@2026-10-21 → 46.58@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_down | RSI 50.05@2026-10-21 → 46.58@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **BAD** |
| `A04_rsi_cross_70` | below | RSI 50.05@2026-10-21 → 46.58@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=7.541 (G=4.6000 R=0.6100); 2026-10-21:GREEN:O=106.5500,C=111.1500,body=+4.6000,vol=1220973.0; 2026-10-22:RED:O=109.0300,C=108.4200,body=-0.6100,vol=843468.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.448 (Gvol=1220973 Rvol=843468); 2026-10-21:GREEN:O=106.5500,C=111.1500,body=+4.6000,vol=1220973.0; 2026-10-22:RED:O=109.0300,C=108.4200,body=-0.6100,vol=843468.0 | **GOOD** |
| `A07_rvol` | RVOL=1.062 on 2026-10-22: today_vol=843468 / avg20=794324 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.438 on 2026-10-22 (price=108.4200, mid=113.8320, upper=126.1885, lower=101.4755; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=108.4200 vs SMA50=106.8070 dist=+1.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=113.8320 SMA50=106.8070 SMA80=100.0155 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-08-11@86.1300; S2[2026-08-25→2026-09-23] low=2026-08-26@93.1400; S3[2026-09-24→2026-10-22] low=2026-09-24@100.7700 | lows=[86.12999725341797, 93.13999938964844, 100.7699966430664] span=17.00% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.7244093825956047 wick_frac=0.2755906174043953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.11652571088786838 wick_frac=0.8834742891121317 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.540973559796883 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-5.5100:wick=2.5085; 2026-10-19:RED:body=-5.9100:wick=4.5100; 2026-10-20:GREEN:body=+0.8500:wick=2.5350; 2026-10-21:GREEN:body=+4.6000:wick=1.7500; 2026-10-22:RED:body=-0.6100:wick=4.6249 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.4 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.5 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1259.47 | **NEUTRAL** |
| `B04_income` | 779.12 | **GOOD** |
| `B05_profit_margin` | 61.86 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=111.33 vs prior_export=111.33 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -0.65 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.65 vs prior=-0.65 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.26 | **GOOD** |
| `B13_short_float` | 6.6 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.4 (this export) | prior_export=6.4 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.5 (this export) | prior_export=6.5 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-23_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.