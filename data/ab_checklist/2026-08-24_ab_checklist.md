# A+B1 Feature Checklist — 2026-08-24

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,702** names
- Export: `finviz_2026-08-24.csv` · prior export for Δ: `2026-08-21`
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
| 1 | HUM | +18 | 18 | 0 | 2026-08-21→2026-08-24 | Healthcare Plans |
| 2 | DINO | +17 | 17 | 0 | 2026-08-21→2026-08-24 | Oil & Gas Refining & Marketing |
| 3 | SGHC | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Gambling |
| 4 | EMBJ | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Aerospace & Defense |
| 5 | CRSR | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Computer Hardware |
| 6 | DE | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Farm & Heavy Construction Machinery |
| 7 | AMH | +16 | 17 | 1 | 2026-08-21→2026-08-24 | REIT - Residential |
| 8 | SKWD | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Insurance - Property & Casualty |
| 9 | SYY | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Food Distribution |
| 10 | CBRL | +16 | 16 | 0 | 2026-08-21→2026-08-24 | Restaurants |
| 11 | IBKR | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Capital Markets |
| 12 | RUSHA | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Auto & Truck Dealerships |
| 13 | FLS | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Specialty Industrial Machinery |
| 14 | ALC | +15 | 15 | 0 | 2026-08-21→2026-08-24 | Medical Instruments & Supplies |
| 15 | PLTR | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Software - Infrastructure |

## Full checklist — top 15

### HUM  ·  score **+18**  ·  Healthcare Plans
price=386.6099853515625  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.88 on 2026-08-24; prev RSI=49.50 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.50@2026-08-21 → 53.88@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.178 (G=6.1200 R=2.8100); 2026-08-21:RED:O=381.6900,C=378.8800,body=-2.8100,vol=1143600.0; 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1454977.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.272 (Gvol=1454977 Rvol=1143600); 2026-08-21:RED:O=381.6900,C=378.8800,body=-2.8100,vol=1143600.0; 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1454977.0 | **GOOD** |
| `A07_rvol` | RVOL=1.046 on 2026-08-24: today_vol=1454977 / avg20=1390370 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.458 on 2026-08-24 (price=386.6100, mid=378.3285, upper=396.3976, lower=360.2594; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=386.6100 vs SMA50=382.0114 dist=+1.20% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=378.33_50=382.01_80=347.01 on 2026-08-24: SMA20=378.3285 SMA50=382.0114 SMA80=347.0056 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@299.2939; S2[2026-06-22→2026-07-23] low=2026-06-24@352.2290; S3[2026-07-24→2026-08-24] low=2026-08-05@353.6900 | lows=[299.2938790186219, 352.22899615508356, 353.69000244140625] span=18.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5899467241688705 wick_frac=0.41005327583112944 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.3005343020618119 wick_frac=0.699465697938188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.177936097656335 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-2.0000:wick=8.8400; 2026-08-19:RED:body=-6.9800:wick=9.1300; 2026-08-20:GREEN:body=+6.4400:wick=7.0600; 2026-08-21:RED:body=-2.8100:wick=6.5400; 2026-08-24:GREEN:body=+6.1200:wick=4.2538 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 420.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.080000000000041 (now=420.54 vs prior_export=418.46 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.23 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.23 vs prior=0.23 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.22 | **GOOD** |
| `B13_short_float` | 3.06 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+17**  ·  Oil & Gas Refining & Marketing
price=95.18000030517578  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.65 on 2026-08-24; prev RSI=67.90 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.90@2026-08-21 → 62.65@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=3.860 (G=3.3200 R=0.8600); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=95.1800,body=-0.8600,vol=1953657.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.485 (Gvol=2901400 Rvol=1953657); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=95.1800,body=-0.8600,vol=1953657.0 | **GOOD** |
| `A07_rvol` | RVOL=0.663 on 2026-08-24: today_vol=1953657 / avg20=2947205 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.550 on 2026-08-24 (price=95.1800, mid=89.8407, upper=99.5445, lower=80.1369; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=95.1800 vs SMA50=81.6974 dist=+16.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=89.8407 SMA50=81.6974 SMA80=77.3487 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-22@63.4268; S2[2026-06-25→2026-07-24] low=2026-06-25@64.8977; S3[2026-07-27→2026-08-24] low=2026-08-07@80.0041 | lows=[63.42683480953448, 64.8977242324992, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7562642725310477 wick_frac=0.24373572746895236 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.1915412064570943 wick_frac=0.8084587935429057 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.8604620216106884 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.2900:wick=1.7800; 2026-08-19:RED:body=-1.6300:wick=1.0200; 2026-08-20:RED:body=-2.4600:wick=1.7500; 2026-08-21:GREEN:body=+3.3200:wick=1.0700; 2026-08-24:RED:body=-0.8600:wick=3.6299 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 92.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=92.79 vs prior_export=92.79 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.63 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.37 | **GOOD** |
| `B13_short_float` | 5.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SGHC  ·  score **+16**  ·  Gambling
price=13.989999771118164  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.58 on 2026-08-24; prev RSI=46.31 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.31@2026-08-21 → 55.58@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7500 R=0.0000); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4298829.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6352829 Rvol=0); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4298829.0 | **GOOD** |
| `A07_rvol` | RVOL=1.964 on 2026-08-24: today_vol=4298829 / avg20=2188330 (avg window 2026-07-24→2026-08-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.353 on 2026-08-24 (price=13.9900, mid=13.4995, upper=14.8889, lower=12.1101; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=13.9900 vs SMA50=13.9203 dist=+0.50% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=13.50_50=13.92_80=13.58 on 2026-08-24: SMA20=13.4995 SMA50=13.9203 SMA80=13.5774 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-03@12.1657; S2[2026-06-23→2026-07-23] low=2026-06-30@13.2350; S3[2026-07-24→2026-08-24] low=2026-08-12@12.7410 | lows=[12.165696572387432, 13.234999656677246, 12.741000175476074] span=8.79% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6277677185930743 wick_frac=0.37223228140692566 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.2100:wick=0.0650; 2026-08-19:GREEN:body=+0.2800:wick=0.1500; 2026-08-20:RED:body=-0.0200:wick=0.4170; 2026-08-21:GREEN:body=+0.0900:wick=0.1200; 2026-08-24:GREEN:body=+0.6600:wick=0.1381 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.51 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.65 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2431.0 | **NEUTRAL** |
| `B04_income` | 370.0 | **GOOD** |
| `B05_profit_margin` | 15.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.5 vs prior_export=19.5 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.13 vs prior=-0.13 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.32 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.65 (this export) | prior_export=3.65 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBJ  ·  score **+16**  ·  Aerospace & Defense
price=76.26000213623047  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.50 on 2026-08-24; prev RSI=63.83 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.83@2026-08-21 → 64.50@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=4.571 (G=1.2800 R=0.2800); 2026-08-21:RED:O=76.2300,C=75.9500,body=-0.2800,vol=871800.0; 2026-08-24:GREEN:O=74.9800,C=76.2600,body=+1.2800,vol=1093956.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.255 (Gvol=1093956 Rvol=871800); 2026-08-21:RED:O=76.2300,C=75.9500,body=-0.2800,vol=871800.0; 2026-08-24:GREEN:O=74.9800,C=76.2600,body=+1.2800,vol=1093956.0 | **GOOD** |
| `A07_rvol` | RVOL=0.801 on 2026-08-24: today_vol=1093956 / avg20=1366410 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.507 on 2026-08-24 (price=76.2600, mid=73.0025, upper=79.4228, lower=66.5822; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=76.2600 vs SMA50=66.6958 dist=+14.34% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=73.0025 SMA50=66.6958 SMA80=63.8784 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-11@53.3748; S2[2026-06-22→2026-07-23] low=2026-06-23@59.4139; S3[2026-07-24→2026-08-24] low=2026-07-24@64.6100 | lows=[53.3748138669873, 59.41386146883149, 64.61000061035156] span=21.05% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.795029996303773 wick_frac=0.204970003696227 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.12785666508737215 wick_frac=0.8721433349126279 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.571319582572682 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.1900:wick=1.1200; 2026-08-19:RED:body=-1.2200:wick=0.8100; 2026-08-20:RED:body=-2.4400:wick=0.5000; 2026-08-21:RED:body=-0.2800:wick=1.9100; 2026-08-24:GREEN:body=+1.2800:wick=0.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=91.21 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.12 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8338.75 | **NEUTRAL** |
| `B04_income` | 445.04 | **GOOD** |
| `B05_profit_margin` | 5.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.54 vs prior_export=89.54 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.07 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.45 | **GOOD** |
| `B13_short_float` | 1.33 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=91.21 (this export) | prior_export=91.21 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.12 (this export) | prior_export=11.12 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=10.90999984741211  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.11 on 2026-08-24; prev RSI=47.25 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.25@2026-08-21 → 47.11@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=1.143 (G=0.1600 R=0.1400); 2026-08-21:RED:O=11.0700,C=10.9300,body=-0.1400,vol=1193100.0; 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1812214.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.519 (Gvol=1812214 Rvol=1193100); 2026-08-21:RED:O=11.0700,C=10.9300,body=-0.1400,vol=1193100.0; 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1812214.0 | **GOOD** |
| `A07_rvol` | RVOL=0.785 on 2026-08-24: today_vol=1812214 / avg20=2308520 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.338 on 2026-08-24 (price=10.9100, mid=11.7377, upper=14.1862, lower=9.2893; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=10.9100 vs SMA50=10.2589 dist=+6.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=11.7377 SMA50=10.2589 SMA80=9.5505 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-26@7.5500; S2[2026-06-25→2026-07-24] low=2026-06-26@8.1950; S3[2026-07-27→2026-08-24] low=2026-07-27@9.9100 | lows=[7.550000190734863, 8.194999694824219, 9.90999984741211] span=31.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6056007565858342 wick_frac=0.39439924341416577 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.42424067277403693 wick_frac=0.575759327225963 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1428610354223434 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0400:wick=0.3400; 2026-08-19:GREEN:body=+0.0100:wick=0.4680; 2026-08-20:RED:body=-0.3300:wick=0.2800; 2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1042 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 25.27 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DE  ·  score **+16**  ·  Farm & Heavy Construction Machinery
price=648.6400146484375  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.42 on 2026-08-24; prev RSI=62.14 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.14@2026-08-21 → 62.42@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=5.502 (G=24.2100 R=4.4000); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=648.6400,body=-4.4000,vol=1228915.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.947 (Gvol=2392500 Rvol=1228915); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=648.6400,body=-4.4000,vol=1228915.0 | **GOOD** |
| `A07_rvol` | RVOL=1.023 on 2026-08-24: today_vol=1228915 / avg20=1201820 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.984 on 2026-08-24 (price=648.6400, mid=614.9965, upper=649.1845, lower=580.8085; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-24: price=648.6400 vs SMA50=604.8177 dist=+7.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=614.9965 SMA50=604.8177 SMA80=589.9240 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@513.8182; S2[2026-06-22→2026-07-23] low=2026-07-15@576.4500; S3[2026-07-24→2026-08-24] low=2026-08-19@579.3100 | lows=[513.8182476448627, 576.4500122070312, 579.3099975585938] span=12.75% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5219913408519654 wick_frac=0.4780086591480346 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.2946870567267168 wick_frac=0.7053129432732832 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.502309645022126 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-15.2100:wick=0.8300; 2026-08-19:RED:body=-8.9500:wick=7.8000; 2026-08-20:GREEN:body=+9.8200:wick=42.7000; 2026-08-21:GREEN:body=+24.2100:wick=22.1700; 2026-08-24:RED:body=-4.4000:wick=10.5310 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.65 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 47976.0 | **NEUTRAL** |
| `B04_income` | 4873.0 | **GOOD** |
| `B05_profit_margin` | 10.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 671.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=6.040000000000077 (now=671.33 vs prior_export=665.29 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 2.52 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.65 (this export) | prior_export=8.65 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMH  ·  score **+16**  ·  REIT - Residential
price=34.810001373291016  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.98 on 2026-08-24; prev RSI=56.11 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.11@2026-08-21 → 60.98@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=0.3200 R=0.1200); 2026-08-21:RED:O=34.4700,C=34.3500,body=-0.1200,vol=1411400.0; 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283885.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=0.910 (Gvol=1283885 Rvol=1411400); 2026-08-21:RED:O=34.4700,C=34.3500,body=-0.1200,vol=1411400.0; 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283885.0 | **BAD** |
| `A07_rvol` | RVOL=0.597 on 2026-08-24: today_vol=1283885 / avg20=2152050 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.781 on 2026-08-24 (price=34.8100, mid=34.1135, upper=35.0056, lower=33.2214; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=34.8100 vs SMA50=33.5818 dist=+3.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=34.1135 SMA50=33.5818 SMA80=32.8572 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@31.1085; S2[2026-06-22→2026-07-23] low=2026-06-22@31.4700; S3[2026-07-24→2026-08-24] low=2026-07-30@32.8600 | lows=[31.108537337013935, 31.469999313354492, 32.86000061035156] span=5.63% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7111092273131862 wick_frac=0.2888907726868139 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.30000572202407066 wick_frac=0.6999942779759294 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.6666030898340645 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.2500:wick=0.2200; 2026-08-19:GREEN:body=+0.2900:wick=0.2400; 2026-08-20:GREEN:body=+0.1000:wick=0.2300; 2026-08-21:RED:body=-0.1200:wick=0.2800; 2026-08-24:GREEN:body=+0.3200:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.77 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1891.62 | **NEUTRAL** |
| `B04_income` | 463.49 | **GOOD** |
| `B05_profit_margin` | 24.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.16 vs prior_export=37.16 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.41 (this export) | prior_export=72.41 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.77 (this export) | prior_export=0.77 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKWD  ·  score **+16**  ·  Insurance - Property & Casualty
price=57.91999816894531  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.15 on 2026-08-24; prev RSI=43.96 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.96@2026-08-21 → 47.15@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8700 R=0.0000); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=57.9200,body=+1.0200,vol=370042.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=893242 Rvol=0); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=57.9200,body=+1.0200,vol=370042.0 | **GOOD** |
| `A07_rvol` | RVOL=0.726 on 2026-08-24: today_vol=370042 / avg20=509590 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.462 on 2026-08-24 (price=57.9200, mid=60.3520, upper=65.6152, lower=55.0888; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=57.9200 vs SMA50=57.8120 dist=+0.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=60.3520 SMA50=57.8120 SMA80=53.1741 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-03@42.5000; S2[2026-06-22→2026-07-23] low=2026-06-22@50.7550; S3[2026-07-24→2026-08-24] low=2026-08-20@54.0200 | lows=[42.5, 50.755001068115234, 54.02000045776367] span=27.11% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.514937798906614 wick_frac=0.48506220109338605 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:GREEN:body=+0.0400:wick=1.5800; 2026-08-19:RED:body=-2.0400:wick=0.6900; 2026-08-20:GREEN:body=+0.9500:wick=1.4900; 2026-08-21:GREEN:body=+0.8500:wick=0.7850; 2026-08-24:GREEN:body=+1.0200:wick=0.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.67 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.81 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1735.66 | **NEUTRAL** |
| `B04_income` | 187.9 | **GOOD** |
| `B05_profit_margin` | 10.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.92 vs prior_export=70.92 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.98 | **GOOD** |
| `B13_short_float` | 4.74 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.67 (this export) | prior_export=10.67 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.81 (this export) | prior_export=4.81 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SYY  ·  score **+16**  ·  Food Distribution
price=83.86000061035156  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.11 on 2026-08-24; prev RSI=55.45 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.45@2026-08-21 → 54.11@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.130 (G=0.4900 R=0.2300); 2026-08-21:GREEN:O=83.6100,C=84.1000,body=+0.4900,vol=2171400.0; 2026-08-24:RED:O=84.0900,C=83.8600,body=-0.2300,vol=1746824.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.243 (Gvol=2171400 Rvol=1746824); 2026-08-21:GREEN:O=83.6100,C=84.1000,body=+0.4900,vol=2171400.0; 2026-08-24:RED:O=84.0900,C=83.8600,body=-0.2300,vol=1746824.0 | **GOOD** |
| `A07_rvol` | RVOL=0.482 on 2026-08-24: today_vol=1746824 / avg20=3624650 (avg window 2026-07-27→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.013 on 2026-08-24 (price=83.8600, mid=83.8870, upper=86.0143, lower=81.7597; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=83.8600 vs SMA50=82.3886 dist=+1.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=83.8870 SMA50=82.3886 SMA80=79.2079 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-02@72.5491; S2[2026-06-23→2026-07-24] low=2026-06-23@77.7348; S3[2026-07-27→2026-08-24] low=2026-08-04@80.4300 | lows=[72.5491367244224, 77.73476358499943, 80.43000030517578] span=10.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5505597750612924 wick_frac=0.4494402249387076 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.15231483586719818 wick_frac=0.8476851641328018 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1304650699927024 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.9800:wick=0.1300; 2026-08-19:RED:body=-0.0400:wick=1.1800; 2026-08-20:GREEN:body=+0.6600:wick=1.6800; 2026-08-21:GREEN:body=+0.4900:wick=0.4000; 2026-08-24:RED:body=-0.2300:wick=1.2800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.27 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.81 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 84553.0 | **NEUTRAL** |
| `B04_income` | 1756.0 | **GOOD** |
| `B05_profit_margin` | 2.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 90.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=90.67 vs prior_export=90.67 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | 0.64 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.64 vs prior=0.64 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.08 | **GOOD** |
| `B13_short_float` | 3.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.27 (this export) | prior_export=1.27 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.81 (this export) | prior_export=0.81 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=58.02000045776367  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.43 on 2026-08-24; prev RSI=57.52 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.52@2026-08-21 → 58.43@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7200 R=0.0000); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:GREEN:O=57.6000,C=58.0200,body=+0.4200,vol=870127.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1416827 Rvol=0); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:GREEN:O=57.6000,C=58.0200,body=+0.4200,vol=870127.0 | **GOOD** |
| `A07_rvol` | RVOL=1.085 on 2026-08-24: today_vol=870127 / avg20=802260 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.285 on 2026-08-24 (price=58.0200, mid=57.0410, upper=60.4751, lower=53.6069; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=58.0200 vs SMA50=52.2704 dist=+11.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=57.0410 SMA50=52.2704 SMA80=44.4682 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@28.0918; S2[2026-06-22→2026-07-23] low=2026-06-23@45.2175; S3[2026-07-24→2026-08-24] low=2026-07-27@50.2000 | lows=[28.091751487486153, 45.21746996696082, 50.20000076293945] span=78.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.570175581250165 wick_frac=0.429824418749835 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.7800:wick=1.3000; 2026-08-19:RED:body=-1.8300:wick=1.7800; 2026-08-20:RED:body=-0.4300:wick=1.4000; 2026-08-21:GREEN:body=+2.3000:wick=0.3300; 2026-08-24:GREEN:body=+0.4200:wick=1.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=160.4 (current export asof; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.66 (current export; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3337.38 | **NEUTRAL** |
| `B04_income` | 26.23 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 23.97 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IBKR  ·  score **+15**  ·  Capital Markets
price=93.05000305175781  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.44 on 2026-08-24; prev RSI=56.35 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.35@2026-08-21 → 54.44@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.35@2026-08-21 → 54.44@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.35@2026-08-21 → 54.44@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=3.549 (G=3.0700 R=0.8650); 2026-08-21:GREEN:O=90.8000,C=93.8700,body=+3.0700,vol=5760300.0; 2026-08-24:RED:O=93.9150,C=93.0500,body=-0.8650,vol=3692234.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.560 (Gvol=5760300 Rvol=3692234); 2026-08-21:GREEN:O=90.8000,C=93.8700,body=+3.0700,vol=5760300.0; 2026-08-24:RED:O=93.9150,C=93.0500,body=-0.8650,vol=3692234.0 | **GOOD** |
| `A07_rvol` | RVOL=0.795 on 2026-08-24: today_vol=3692234 / avg20=4645120 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.635 on 2026-08-24 (price=93.0500, mid=90.1195, upper=94.7314, lower=85.5076; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=93.0500 vs SMA50=91.8856 dist=+1.27% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=90.12_50=91.89_80=89.24 on 2026-08-24: SMA20=90.1195 SMA50=91.8856 SMA80=89.2446 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-28@79.0399; S2[2026-06-25→2026-07-24] low=2026-06-30@86.6600; S3[2026-07-27→2026-08-24] low=2026-08-07@85.3700 | lows=[79.03994866332086, 86.66000366210938, 85.37000274658203] span=9.64% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7851654269725186 wick_frac=0.2148345730274814 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.37772817377688195 wick_frac=0.622271826223118 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.549141360240613 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-2.4400:wick=0.3700; 2026-08-19:RED:body=-0.4500:wick=2.0600; 2026-08-20:RED:body=-1.5600:wick=0.4800; 2026-08-21:GREEN:body=+3.0700:wick=0.8400; 2026-08-24:RED:body=-0.8650:wick=1.4250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.81 (current export asof; earnings_date=7/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.47 (current export; earnings_date=7/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11297.0 | **NEUTRAL** |
| `B04_income` | 1126.0 | **GOOD** |
| `B05_profit_margin` | 9.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=109.42 vs prior_export=109.42 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -0.17 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.17 vs prior=-0.17 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.08 | **GOOD** |
| `B13_short_float` | 2.11 | **NEUTRAL** |
| `B14_earnings_date` | 7/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.81 (this export) | prior_export=7.81 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.47 (this export) | prior_export=5.47 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RUSHA  ·  score **+15**  ·  Auto & Truck Dealerships
price=78.7300033569336  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.26 on 2026-08-24; prev RSI=51.42 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.42@2026-08-21 → 51.26@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.42@2026-08-21 → 51.26@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.42@2026-08-21 → 51.26@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=1.750 (G=1.3300 R=0.7600); 2026-08-21:GREEN:O=77.4500,C=78.7800,body=+1.3300,vol=390400.0; 2026-08-24:RED:O=79.4900,C=78.7300,body=-0.7600,vol=250536.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.558 (Gvol=390400 Rvol=250536); 2026-08-21:GREEN:O=77.4500,C=78.7800,body=+1.3300,vol=390400.0; 2026-08-24:RED:O=79.4900,C=78.7300,body=-0.7600,vol=250536.0 | **GOOD** |
| `A07_rvol` | RVOL=0.520 on 2026-08-24: today_vol=250536 / avg20=481385 (avg window 2026-07-20→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.255 on 2026-08-24 (price=78.7300, mid=79.7710, upper=83.8568, lower=75.6852; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=78.7300 vs SMA50=75.2002 dist=+4.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=79.7710 SMA50=75.2002 SMA80=73.5509 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-18→2026-08-24 (63 bars); S1[2026-05-18→2026-06-16] low=2026-06-05@65.6800; S2[2026-06-17→2026-07-17] low=2026-06-17@67.3300; S3[2026-07-20→2026-08-24] low=2026-07-20@74.9200 | lows=[65.68000030517578, 67.33000183105469, 74.91999816894531] span=14.07% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6425130565865274 wick_frac=0.3574869434134727 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.36363304507174904 wick_frac=0.636366954928251 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.75001505812436 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-2.7400:wick=0.1000; 2026-08-19:RED:body=-2.0900:wick=0.1500; 2026-08-20:RED:body=-0.3700:wick=1.6800; 2026-08-21:GREEN:body=+1.3300:wick=0.7400; 2026-08-24:RED:body=-0.7600:wick=1.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.46 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.36 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7236.52 | **NEUTRAL** |
| `B04_income` | 265.23 | **GOOD** |
| `B05_profit_margin` | 3.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 91.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=91.0 vs prior_export=91.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -0.74 | **BAD** |
| `B11_insider_tx_delta` | delta=0.13 (now=-0.74 vs prior=-0.87 on finviz_2026-08-21) | **GOOD** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 7.74 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.46 (this export) | prior_export=6.46 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.36 (this export) | prior_export=0.36 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+15**  ·  Specialty Industrial Machinery
price=80.45999908447266  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.08 on 2026-08-24; prev RSI=57.37 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.37@2026-08-21 → 60.08@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.37@2026-08-21 → 60.08@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.37@2026-08-21 → 60.08@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2200 R=0.0000); 2026-08-21:GREEN:O=79.1600,C=79.3900,body=+0.2300,vol=853800.0; 2026-08-24:GREEN:O=79.4700,C=80.4600,body=+0.9900,vol=1831347.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2685147 Rvol=0); 2026-08-21:GREEN:O=79.1600,C=79.3900,body=+0.2300,vol=853800.0; 2026-08-24:GREEN:O=79.4700,C=80.4600,body=+0.9900,vol=1831347.0 | **GOOD** |
| `A07_rvol` | RVOL=1.097 on 2026-08-24: today_vol=1831347 / avg20=1669440 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.365 on 2026-08-24 (price=80.4600, mid=78.1820, upper=84.4157, lower=71.9483; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=80.4600 vs SMA50=75.4383 dist=+6.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=78.1820 SMA50=75.4383 SMA80=74.3624 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@64.0515; S2[2026-06-22→2026-07-23] low=2026-07-20@66.3300; S3[2026-07-24→2026-08-24] low=2026-07-29@69.1400 | lows=[64.05153288904764, 66.33000183105469, 69.13999938964844] span=7.94% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.30973479393680436 wick_frac=0.6902652060631956 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.5600:wick=1.4900; 2026-08-19:RED:body=-0.8800:wick=2.0000; 2026-08-20:GREEN:body=+1.1100:wick=0.6000; 2026-08-21:GREEN:body=+0.2300:wick=1.6300; 2026-08-24:GREEN:body=+0.9900:wick=1.0067 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.33 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ALC  ·  score **+15**  ·  Medical Instruments & Supplies
price=73.69999694824219  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.11 on 2026-08-24; prev RSI=57.90 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.90@2026-08-21 → 58.11@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.90@2026-08-21 → 58.11@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.90@2026-08-21 → 58.11@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-21:GREEN:O=72.8000,C=73.6300,body=+0.8300,vol=994200.0; 2026-08-24:GREEN:O=73.3400,C=73.7000,body=+0.3600,vol=1159810.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2154010 Rvol=0); 2026-08-21:GREEN:O=72.8000,C=73.6300,body=+0.8300,vol=994200.0; 2026-08-24:GREEN:O=73.3400,C=73.7000,body=+0.3600,vol=1159810.0 | **GOOD** |
| `A07_rvol` | RVOL=0.549 on 2026-08-24: today_vol=1159810 / avg20=2111865 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.390 on 2026-08-24 (price=73.7000, mid=71.9830, upper=76.3870, lower=67.5790; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=73.7000 vs SMA50=69.2288 dist=+6.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=71.9830 SMA50=69.2288 SMA80=68.5505 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-17@63.8100; S2[2026-06-22→2026-07-23] low=2026-06-22@64.9000; S3[2026-07-24→2026-08-24] low=2026-07-27@67.2300 | lows=[63.810001373291016, 64.9000015258789, 67.2300033569336] span=5.36% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6029734083897074 wick_frac=0.39702659161029263 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0500:wick=0.8500; 2026-08-19:GREEN:body=+0.5900:wick=0.8050; 2026-08-20:RED:body=-1.5300:wick=0.2800; 2026-08-21:GREEN:body=+0.8300:wick=0.3700; 2026-08-24:GREEN:body=+0.3600:wick=0.3400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.36 (current export asof; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.72 (current export; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10861.0 | **NEUTRAL** |
| `B04_income` | 643.0 | **GOOD** |
| `B05_profit_margin` | 5.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 87.37 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=87.37 vs prior_export=87.37 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.74 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.28 | **GOOD** |
| `B13_short_float` | 2.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.36 (this export) | prior_export=11.36 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.72 (this export) | prior_export=0.72 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLTR  ·  score **+15**  ·  Software - Infrastructure
price=175.88999938964844  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.03 on 2026-08-24; prev RSI=69.41 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.41@2026-08-21 → 65.03@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.41@2026-08-21 → 65.03@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.41@2026-08-21 → 65.03@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=3.821 (G=5.9600 R=1.5600); 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0; 2026-08-24:RED:O=177.4500,C=175.8900,body=-1.5600,vol=35003354.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=1.171 (Gvol=40986600 Rvol=35003354); 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0; 2026-08-24:RED:O=177.4500,C=175.8900,body=-1.5600,vol=35003354.0 | **GOOD** |
| `A07_rvol` | RVOL=0.735 on 2026-08-24: today_vol=35003354 / avg20=47632880 (avg window 2026-07-27→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.370 on 2026-08-24 (price=175.8900, mid=159.4925, upper=203.8239, lower=115.1611; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=175.8900 vs SMA50=139.8280 dist=+25.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=159.4925 SMA50=139.8280 SMA80=139.3592 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-24@112.2500; S2[2026-06-25→2026-07-24] low=2026-06-25@106.3700; S3[2026-07-27→2026-08-24] low=2026-07-28@117.8900 | lows=[112.25, 106.37000274658203, 117.88999938964844] span=10.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6026296341438955 wick_frac=0.3973703658561045 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.2096770224041609 wick_frac=0.7903229775958391 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.8205231034078015 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.4300:wick=3.9400; 2026-08-19:GREEN:body=+2.5800:wick=4.4690; 2026-08-20:RED:body=-1.9200:wick=2.5100; 2026-08-21:GREEN:body=+5.9600:wick=3.9300; 2026-08-24:RED:body=-1.5600:wick=5.8800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.98 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.8 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6155.94 | **NEUTRAL** |
| `B04_income` | 3016.69 | **GOOD** |
| `B05_profit_margin` | 49.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 199.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=199.08 vs prior_export=199.08 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.91 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.91 | **GOOD** |
| `B13_short_float` | 3.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-24_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.