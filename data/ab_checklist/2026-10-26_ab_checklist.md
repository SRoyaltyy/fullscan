# A+B1 Feature Checklist — 2026-10-26

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-26.csv` · prior export for Δ: `2026-10-23`
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
| 1 | ADSK | +17 | 17 | 0 | 2026-10-22→2026-10-23 | Software - Application |
| 2 | ELV | +16 | 18 | 2 | 2026-10-22→2026-10-23 | Healthcare Plans |
| 3 | SN | +16 | 17 | 1 | 2026-10-22→2026-10-23 | Furnishings, Fixtures & Appliances |
| 4 | PM | +16 | 16 | 0 | 2026-10-22→2026-10-23 | Tobacco |
| 5 | SWKS | +15 | 15 | 0 | 2026-10-22→2026-10-23 | Semiconductors |
| 6 | V | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Credit Services |
| 7 | CURI | +15 | 16 | 1 | 2026-10-22→2026-10-23 | Broadcasting |
| 8 | CRWD | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Software - Infrastructure |
| 9 | SHEL | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Oil & Gas Integrated |
| 10 | TOST | +15 | 16 | 1 | 2026-10-22→2026-10-23 | Software - Infrastructure |
| 11 | KBR | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Engineering & Construction |
| 12 | OXY | +15 | 16 | 1 | 2026-10-22→2026-10-23 | Oil & Gas E&P |
| 13 | AAPL | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Consumer Electronics |
| 14 | RSG | +15 | 18 | 3 | 2026-10-22→2026-10-23 | Waste Management |
| 15 | BP | +15 | 17 | 2 | 2026-10-22→2026-10-23 | Oil & Gas Integrated |

## Full checklist — top 15

### ADSK  ·  score **+17**  ·  Software - Application
price=252.1999969482422  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.53 on 2026-10-23; prev RSI=68.54 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.54@2026-10-22 → 65.53@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.54@2026-10-22 → 65.53@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.54@2026-10-22 → 65.53@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=3.028 (G=5.6330 R=1.8600); 2026-10-22:GREEN:O=249.3170,C=254.9500,body=+5.6330,vol=2163006.0; 2026-10-23:RED:O=254.0600,C=252.2000,body=-1.8600,vol=1319876.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.639 (Gvol=2163006 Rvol=1319876); 2026-10-22:GREEN:O=249.3170,C=254.9500,body=+5.6330,vol=2163006.0; 2026-10-23:RED:O=254.0600,C=252.2000,body=-1.8600,vol=1319876.0 | **GOOD** |
| `A07_rvol` | RVOL=0.703 on 2026-10-23: today_vol=1319876 / avg20=1878776 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.691 on 2026-10-23 (price=252.2000, mid=230.8980, upper=261.7197, lower=200.0763; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=252.2000 vs SMA50=232.7948 dist=+8.34% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=230.90_50=232.79_80=229.21 on 2026-10-23: SMA20=230.8980 SMA50=232.7948 SMA80=229.2139 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-23 (63 bars); S1[2026-07-27→2026-08-25] low=2026-07-27@212.1900; S2[2026-08-26→2026-09-24] low=2026-09-09@206.3300; S3[2026-09-25→2026-10-23] low=2026-09-29@199.1801 | lows=[212.19000244140625, 206.3300018310547, 199.1800994873047] span=6.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.602781687496122 wick_frac=0.39721831250387796 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.30567020908457165 wick_frac=0.6943297909154283 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.028491267217405 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:GREEN:body=+0.3500:wick=4.8275; 2026-10-20:GREEN:body=+4.4800:wick=2.1000; 2026-10-21:GREEN:body=+1.7400:wick=3.3437; 2026-10-22:GREEN:body=+5.6330:wick=3.7120; 2026-10-23:RED:body=-1.8600:wick=4.2250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=439.95001220703125  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.74 on 2026-10-23; prev RSI=73.20 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 73.20@2026-10-22 → 68.74@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 73.20@2026-10-22 → 68.74@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 73.20@2026-10-22 → 68.74@2026-10-23 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=5.483 (G=12.7200 R=2.3200); 2026-10-22:GREEN:O=432.5600,C=445.2800,body=+12.7200,vol=1428100.0; 2026-10-23:RED:O=442.2700,C=439.9500,body=-2.3200,vol=1173800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.217 (Gvol=1428100 Rvol=1173800); 2026-10-22:GREEN:O=432.5600,C=445.2800,body=+12.7200,vol=1428100.0; 2026-10-23:RED:O=442.2700,C=439.9500,body=-2.3200,vol=1173800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.952 on 2026-10-23: today_vol=1173800 / avg20=1233369 (avg window 2026-09-17→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.979 on 2026-10-23 (price=439.9500, mid=405.6915, upper=440.7022, lower=370.6808; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=439.9500 vs SMA50=403.8588 dist=+8.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=405.6915 SMA50=403.8588 SMA80=400.2796 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-23 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-28@362.9800; S2[2026-08-18→2026-09-16] low=2026-08-31@390.8000; S3[2026-09-17→2026-10-23] low=2026-10-01@375.7400 | lows=[362.9800109863281, 390.79998779296875, 375.739990234375] span=7.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.6784000651041666 wick_frac=0.32159993489583333 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.3613663480232542 wick_frac=0.6386336519767458 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.48281395930072 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950; 2026-10-21:RED:body=-10.8700:wick=13.6900; 2026-10-22:GREEN:body=+12.7200:wick=6.0300; 2026-10-23:RED:body=-2.3200:wick=4.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SN  ·  score **+16**  ·  Furnishings, Fixtures & Appliances
price=196.3800048828125  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.79 on 2026-10-23; prev RSI=67.07 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.07@2026-10-22 → 62.79@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.07@2026-10-22 → 62.79@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.07@2026-10-22 → 62.79@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=2.605 (G=4.2200 R=1.6200); 2026-10-22:GREEN:O=194.9400,C=199.1600,body=+4.2200,vol=1356751.0; 2026-10-23:RED:O=198.0000,C=196.3800,body=-1.6200,vol=1015670.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.336 (Gvol=1356751 Rvol=1015670); 2026-10-22:GREEN:O=194.9400,C=199.1600,body=+4.2200,vol=1356751.0; 2026-10-23:RED:O=198.0000,C=196.3800,body=-1.6200,vol=1015670.0 | **GOOD** |
| `A07_rvol` | RVOL=0.876 on 2026-10-23: today_vol=1015670 / avg20=1159930 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.692 on 2026-10-23 (price=196.3800, mid=187.7710, upper=200.2113, lower=175.3307; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=196.3800 vs SMA50=180.9282 dist=+8.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=187.7710 SMA50=180.9282 SMA80=173.2798 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-23 (63 bars); S1[2026-07-27→2026-08-25] low=2026-07-27@151.1600; S2[2026-08-26→2026-09-24] low=2026-09-11@157.0100; S3[2026-09-25→2026-10-23] low=2026-09-25@174.4451 | lows=[151.16000366210938, 157.00999450683594, 174.44509887695312] span=15.40% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.7785982742360046 wick_frac=0.22140172576399544 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.3302743160763282 wick_frac=0.6697256839236718 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.604946876648331 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:RED:body=-0.4400:wick=4.6400; 2026-10-20:GREEN:body=+8.9900:wick=0.9500; 2026-10-21:RED:body=-1.7600:wick=2.3400; 2026-10-22:GREEN:body=+4.2200:wick=1.2000; 2026-10-23:RED:body=-1.6200:wick=3.2850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.45 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6909.96 | **NEUTRAL** |
| `B04_income` | 695.22 | **GOOD** |
| `B05_profit_margin` | 10.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.56 vs prior_export=209.56 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | -6.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.41 vs prior=-6.41 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.92 | **GOOD** |
| `B13_short_float` | 8.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.45 (this export) | prior_export=13.45 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.82 (this export) | prior_export=6.82 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+16**  ·  Tobacco
price=199.47000122070312  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.01 on 2026-10-23; prev RSI=57.27 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.27@2026-10-22 → 54.01@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.27@2026-10-22 → 54.01@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.27@2026-10-22 → 54.01@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=6.026 (G=4.7000 R=0.7800); 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0; 2026-10-23:RED:O=200.2500,C=199.4700,body=-0.7800,vol=3536225.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.442 (Gvol=5098422 Rvol=3536225); 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0; 2026-10-23:RED:O=200.2500,C=199.4700,body=-0.7800,vol=3536225.0 | **GOOD** |
| `A07_rvol` | RVOL=0.786 on 2026-10-23: today_vol=3536225 / avg20=4499549 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.218 on 2026-10-23 (price=199.4700, mid=197.0010, upper=208.3142, lower=185.6878; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=199.4700 vs SMA50=192.5350 dist=+3.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=197.0010 SMA50=192.5350 SMA80=190.9466 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-08-12@184.0200; S2[2026-08-26→2026-09-24] low=2026-09-08@181.0100; S3[2026-09-25→2026-10-23] low=2026-10-21@184.0000 | lows=[184.02000427246094, 181.00999450683594, 184.0] span=1.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.9633150899139953 wick_frac=0.03668491008600469 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.3246349936175483 wick_frac=0.6753650063824518 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.025666105872687 need>1.4; red_wick_gt_green=False 5d trail=2026-10-19:RED:body=-1.0900:wick=1.2100; 2026-10-20:RED:body=-1.9600:wick=1.7200; 2026-10-21:GREEN:body=+10.3200:wick=3.5100; 2026-10-22:GREEN:body=+4.7000:wick=0.1790; 2026-10-23:RED:body=-0.7800:wick=1.6227 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+15**  ·  Semiconductors
price=83.43000030517578  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.83 on 2026-10-23; prev RSI=53.74 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.74@2026-10-22 → 55.83@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.74@2026-10-22 → 55.83@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.74@2026-10-22 → 55.83@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.4200 R=0.0000); 2026-10-22:GREEN:O=80.4500,C=82.3900,body=+1.9400,vol=3515525.0; 2026-10-23:GREEN:O=81.9500,C=83.4300,body=+1.4800,vol=3509474.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7024999 Rvol=0); 2026-10-22:GREEN:O=80.4500,C=82.3900,body=+1.9400,vol=3515525.0; 2026-10-23:GREEN:O=81.9500,C=83.4300,body=+1.4800,vol=3509474.0 | **GOOD** |
| `A07_rvol` | RVOL=0.504 on 2026-10-23: today_vol=3509474 / avg20=6970078 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.242 on 2026-10-23 (price=83.4300, mid=81.6525, upper=88.9984, lower=74.3066; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=83.4300 vs SMA50=78.6372 dist=+6.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=81.6525 SMA50=78.6372 SMA80=72.7809 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-23 (63 bars); S1[2026-07-27→2026-08-25] low=2026-07-29@57.5000; S2[2026-08-26→2026-09-24] low=2026-08-28@64.4700; S3[2026-09-25→2026-10-23] low=2026-10-13@73.7600 | lows=[57.5, 64.47000122070312, 73.76000213623047] span=28.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.6595177829338312 wick_frac=0.3404822170661688 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-19:RED:body=-2.0600:wick=0.5900; 2026-10-20:GREEN:body=+1.5900:wick=1.7450; 2026-10-21:GREEN:body=+0.4000:wick=1.4900; 2026-10-22:GREEN:body=+1.9400:wick=0.5700; 2026-10-23:GREEN:body=+1.4800:wick=1.2300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+15**  ·  Credit Services
price=407.9200134277344  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.02 on 2026-10-23; prev RSI=79.13 on 2026-10-22 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.13@2026-10-22 → 70.02@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.13@2026-10-22 → 70.02@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.13@2026-10-22 → 70.02@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=2.393 (G=6.3900 R=2.6700); 2026-10-22:GREEN:O=407.9500,C=414.3400,body=+6.3900,vol=7087098.0; 2026-10-23:RED:O=410.5900,C=407.9200,body=-2.6700,vol=5071142.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.398 (Gvol=7087098 Rvol=5071142); 2026-10-22:GREEN:O=407.9500,C=414.3400,body=+6.3900,vol=7087098.0; 2026-10-23:RED:O=410.5900,C=407.9200,body=-2.6700,vol=5071142.0 | **GOOD** |
| `A07_rvol` | RVOL=0.861 on 2026-10-23: today_vol=5071142 / avg20=5892730 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.605 on 2026-10-23 (price=407.9200, mid=385.2590, upper=422.6894, lower=347.8286; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=407.9200 vs SMA50=377.0662 dist=+8.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=385.2590 SMA50=377.0662 SMA80=370.4671 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-07-29@354.8208; S2[2026-08-26→2026-09-24] low=2026-09-22@359.8000; S3[2026-09-25→2026-10-23] low=2026-10-02@356.7500 | lows=[354.82083449278736, 359.79998779296875, 356.75] span=1.40% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.8729477783058592 wick_frac=0.1270522216941408 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.4619251015031441 wick_frac=0.5380748984968559 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.393267802034518 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:GREEN:body=+3.4000:wick=2.3820; 2026-10-20:GREEN:body=+2.7050:wick=2.3399; 2026-10-21:GREEN:body=+1.8800:wick=3.5499; 2026-10-22:GREEN:body=+6.3900:wick=0.9300; 2026-10-23:RED:body=-2.6700:wick=3.1101 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.68 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.04 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 44488.0 | **NEUTRAL** |
| `B04_income` | 22399.0 | **GOOD** |
| `B05_profit_margin` | 50.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 423.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=423.54 vs prior_export=423.54 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -0.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.1 vs prior=-0.1 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.87 | **GOOD** |
| `B13_short_float` | 1.09 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.68 (this export) | prior_export=2.68 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.04 (this export) | prior_export=2.04 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CURI  ·  score **+15**  ·  Broadcasting
price=2.8499999046325684  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.34 on 2026-10-23; prev RSI=54.24 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.24@2026-10-22 → 52.34@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.24@2026-10-22 → 52.34@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.24@2026-10-22 → 52.34@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1000 R=0.0000); 2026-10-22:GREEN:O=2.7800,C=2.8800,body=+0.1000,vol=428036.0; 2026-10-23:DOJI:O=2.8500,C=2.8500,body=+0.0000,vol=419524.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=3.041 (Gvol=637798 Rvol=209762); 2026-10-22:GREEN:O=2.7800,C=2.8800,body=+0.1000,vol=428036.0; 2026-10-23:DOJI:O=2.8500,C=2.8500,body=+0.0000,vol=419524.0 | **GOOD** |
| `A07_rvol` | RVOL=0.862 on 2026-10-23: today_vol=419524 / avg20=486447 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.077 on 2026-10-23 (price=2.8500, mid=2.8330, upper=3.0525, lower=2.6135; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=2.8500 vs SMA50=2.8303 dist=+0.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=2.8330 SMA50=2.8303 SMA80=2.7713 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-23 (63 bars); S1[2026-07-27→2026-08-25] low=2026-07-27@2.3700; S2[2026-08-26→2026-09-24] low=2026-08-27@2.5100; S3[2026-09-25→2026-10-23] low=2026-10-19@2.5800 | lows=[2.369999885559082, 2.509999990463257, 2.5799999237060547] span=8.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.7142862008538785 wick_frac=0.28571379914612155 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:DOJI:body=+0.0000:wick=0.0750; 2026-10-20:GREEN:body=+0.2000:wick=0.0000; 2026-10-21:RED:body=-0.0100:wick=0.0900; 2026-10-22:GREEN:body=+0.1000:wick=0.0400; 2026-10-23:DOJI:body=+0.0000:wick=0.0850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=300.0 (current export asof; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.01 (current export; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 75.96 | **NEUTRAL** |
| `B04_income` | 0.02 | **GOOD** |
| `B05_profit_margin` | 0.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.38 vs prior_export=5.38 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.73 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.73 vs prior=0.73 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.23 | **BAD** |
| `B13_short_float` | 6.09 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=300.0 (this export) | prior_export=300.0 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.01 (this export) | prior_export=9.01 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRWD  ·  score **+15**  ·  Software - Infrastructure
price=257.57000732421875  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.92 on 2026-10-23; prev RSI=45.84 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.84@2026-10-22 → 55.92@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.84@2026-10-22 → 55.92@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.84@2026-10-22 → 55.92@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=3.323 (G=19.0700 R=5.7390); 2026-10-22:RED:O=243.6790,C=237.9400,body=-5.7390,vol=10067471.0; 2026-10-23:GREEN:O=238.5000,C=257.5700,body=+19.0700,vol=13342948.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.325 (Gvol=13342948 Rvol=10067471); 2026-10-22:RED:O=243.6790,C=237.9400,body=-5.7390,vol=10067471.0; 2026-10-23:GREEN:O=238.5000,C=257.5700,body=+19.0700,vol=13342948.0 | **GOOD** |
| `A07_rvol` | RVOL=1.612 on 2026-10-23: today_vol=13342948 / avg20=8279674 (avg window 2026-09-25→2026-10-22, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.058 on 2026-10-23 (price=257.5700, mid=255.6840, upper=288.1461, lower=223.2219; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=257.5700 vs SMA50=234.8046 dist=+9.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=255.6840 SMA50=234.8046 SMA80=221.4886 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-07-28@174.1400; S2[2026-08-26→2026-09-24] low=2026-08-26@181.2400; S3[2026-09-25→2026-10-23] low=2026-10-20@221.3600 | lows=[174.13999938964844, 181.24000549316406, 221.36000061035156] span=27.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.8247382133307288 wick_frac=0.1752617866692711 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.5128683789960564 wick_frac=0.4871316210039436 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.322880745311889 need>1.4; red_wick_gt_green=False 5d trail=2026-10-19:RED:body=-5.8050:wick=1.6569; 2026-10-20:GREEN:body=+1.1150:wick=6.7650; 2026-10-21:GREEN:body=+8.4500:wick=5.7476; 2026-10-22:RED:body=-5.7390:wick=5.4510; 2026-10-23:GREEN:body=+19.0700:wick=4.0525 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.31 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.14 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5396.15 | **NEUTRAL** |
| `B04_income` | 37.77 | **GOOD** |
| `B05_profit_margin` | 0.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 239.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=239.46 vs prior_export=239.46 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.65 | **GOOD** |
| `B10_insider_transactions` | -5.3 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.3 vs prior=-5.3 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.02 | **GOOD** |
| `B13_short_float` | 2.82 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.31 (this export) | prior_export=6.31 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.14 (this export) | prior_export=2.14 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHEL  ·  score **+15**  ·  Oil & Gas Integrated
price=108.18000030517578  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=76.89 on 2026-10-23; prev RSI=72.58 on 2026-10-22 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.58@2026-10-22 → 76.89@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.58@2026-10-22 → 76.89@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.58@2026-10-22 → 76.89@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=2.200 (G=1.3200 R=0.6000); 2026-10-22:RED:O=106.4000,C=105.8000,body=-0.6000,vol=4095288.0; 2026-10-23:GREEN:O=106.8600,C=108.1800,body=+1.3200,vol=4656592.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.137 (Gvol=4656592 Rvol=4095288); 2026-10-22:RED:O=106.4000,C=105.8000,body=-0.6000,vol=4095288.0; 2026-10-23:GREEN:O=106.8600,C=108.1800,body=+1.3200,vol=4656592.0 | **GOOD** |
| `A07_rvol` | RVOL=0.707 on 2026-10-23: today_vol=4656592 / avg20=6586427 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.825 on 2026-10-23 (price=108.1800, mid=100.8970, upper=109.7251, lower=92.0689; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=108.1800 vs SMA50=96.6374 dist=+11.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=100.8970 SMA50=96.6374 SMA80=92.5859 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-07-28@85.0744; S2[2026-08-26→2026-09-24] low=2026-08-27@90.0500; S3[2026-09-25→2026-10-23] low=2026-10-01@94.3400 | lows=[85.07441491675512, 90.05000305175781, 94.33999633789062] span=10.89% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.8000471663213968 wick_frac=0.19995283367860314 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.24096344934721128 wick_frac=0.7590365506527887 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.2000050862759557 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:RED:body=-0.3600:wick=0.9500; 2026-10-20:RED:body=-0.8200:wick=1.3700; 2026-10-21:RED:body=-0.1800:wick=1.0700; 2026-10-22:RED:body=-0.6000:wick=1.8900; 2026-10-23:GREEN:body=+1.3200:wick=0.3299 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.64 vs prior_export=103.64 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.45 | **GOOD** |
| `B13_short_float` | 0.7 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TOST  ·  score **+15**  ·  Software - Infrastructure
price=32.709999084472656  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.10 on 2026-10-23; prev RSI=62.24 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.24@2026-10-22 → 57.10@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.24@2026-10-22 → 57.10@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.24@2026-10-22 → 57.10@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=1.837 (G=0.7900 R=0.4300); 2026-10-22:GREEN:O=32.6300,C=33.4200,body=+0.7900,vol=9246432.0; 2026-10-23:RED:O=33.1400,C=32.7100,body=-0.4300,vol=5281614.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.751 (Gvol=9246432 Rvol=5281614); 2026-10-22:GREEN:O=32.6300,C=33.4200,body=+0.7900,vol=9246432.0; 2026-10-23:RED:O=33.1400,C=32.7100,body=-0.4300,vol=5281614.0 | **GOOD** |
| `A07_rvol` | RVOL=0.654 on 2026-10-23: today_vol=5281614 / avg20=8074788 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.692 on 2026-10-23 (price=32.7100, mid=31.0100, upper=33.4681, lower=28.5519; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=32.7100 vs SMA50=32.3630 dist=+1.07% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=31.01_50=32.36_80=32.03 on 2026-10-23: SMA20=31.0100 SMA50=32.3630 SMA80=32.0348 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-07-30@30.8600; S2[2026-08-26→2026-09-24] low=2026-09-23@28.7750; S3[2026-09-25→2026-10-23] low=2026-10-01@28.6950 | lows=[30.860000610351562, 28.774999618530273, 28.69499969482422] span=7.54% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.9187106562504159 wick_frac=0.0812893437495841 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.36440694404034524 wick_frac=0.6355930559596548 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8372012561877895 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:RED:body=-0.3800:wick=0.1949; 2026-10-20:GREEN:body=+1.0000:wick=0.1900; 2026-10-21:RED:body=-0.1800:wick=0.7080; 2026-10-22:GREEN:body=+0.7900:wick=0.0699; 2026-10-23:RED:body=-0.4300:wick=0.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.73 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.87 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6804.0 | **NEUTRAL** |
| `B04_income` | 486.0 | **GOOD** |
| `B05_profit_margin` | 7.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=38.96 vs prior_export=38.96 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | -0.7 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.7 vs prior=-0.7 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.41 | **GOOD** |
| `B13_short_float` | 6.54 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.73 (this export) | prior_export=11.73 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.87 (this export) | prior_export=1.87 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+15**  ·  Engineering & Construction
price=39.22999954223633  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.40 on 2026-10-23; prev RSI=70.75 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 70.75@2026-10-22 → 68.40@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.75@2026-10-22 → 68.40@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 70.75@2026-10-22 → 68.40@2026-10-23 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=43.999 (G=1.7600 R=0.0400); 2026-10-22:GREEN:O=37.7700,C=39.5300,body=+1.7600,vol=3040800.0; 2026-10-23:RED:O=39.2700,C=39.2300,body=-0.0400,vol=1359500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=2.237 (Gvol=3040800 Rvol=1359500); 2026-10-22:GREEN:O=37.7700,C=39.5300,body=+1.7600,vol=3040800.0; 2026-10-23:RED:O=39.2700,C=39.2300,body=-0.0400,vol=1359500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.806 on 2026-10-23: today_vol=1359500 / avg20=1687352 (avg window 2026-09-17→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.248 on 2026-10-23 (price=39.2300, mid=35.5100, upper=38.4905, lower=32.5295; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=39.2300 vs SMA50=36.6916 dist=+6.92% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=35.51_50=36.69_80=36.18 on 2026-10-23: SMA20=35.5100 SMA50=36.6916 SMA80=36.1826 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-23 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@32.1400; S2[2026-08-18→2026-09-16] low=2026-09-11@35.7500; S3[2026-09-17→2026-10-23] low=2026-10-07@33.3100 | lows=[32.13999938964844, 35.75, 33.310001373291016] span=11.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.6901956267100695 wick_frac=0.30980437328993055 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.038835886344108325 wick_frac=0.9611641136558917 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=43.998950982262066 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.1500:wick=0.7300; 2026-10-12:RED:body=-0.1800:wick=0.6376; 2026-10-21:GREEN:body=+0.4800:wick=0.2400; 2026-10-22:GREEN:body=+1.7600:wick=0.7900; 2026-10-23:RED:body=-0.0400:wick=0.9900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.17 vs prior_export=44.17 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.86 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OXY  ·  score **+15**  ·  Oil & Gas E&P
price=64.16999816894531  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.48 on 2026-10-23; prev RSI=62.60 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.60@2026-10-22 → 68.48@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.60@2026-10-22 → 68.48@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.60@2026-10-22 → 68.48@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=1.244 (G=1.1200 R=0.9000); 2026-10-22:RED:O=63.4000,C=62.5000,body=-0.9000,vol=8081150.0; 2026-10-23:GREEN:O=63.0500,C=64.1700,body=+1.1200,vol=11211462.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.387 (Gvol=11211462 Rvol=8081150); 2026-10-22:RED:O=63.4000,C=62.5000,body=-0.9000,vol=8081150.0; 2026-10-23:GREEN:O=63.0500,C=64.1700,body=+1.1200,vol=11211462.0 | **GOOD** |
| `A07_rvol` | RVOL=1.269 on 2026-10-23: today_vol=11211462 / avg20=8833485 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.773 on 2026-10-23 (price=64.1700, mid=59.9885, upper=65.3973, lower=54.5797; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-23: price=64.1700 vs SMA50=59.8234 dist=+7.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=59.9885 SMA50=59.8234 SMA80=58.0411 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-08-05@53.6600; S2[2026-08-26→2026-09-24] low=2026-09-22@56.1800; S3[2026-09-25→2026-10-23] low=2026-09-29@54.7600 | lows=[53.65999984741211, 56.18000030517578, 54.7599983215332] span=4.70% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.49667334872744806 wick_frac=0.5033266512725519 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.39473706225143845 wick_frac=0.6052629377485615 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2444411477980757 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:GREEN:body=+0.7200:wick=0.1900; 2026-10-20:RED:body=-0.5800:wick=1.5870; 2026-10-21:RED:body=-0.2000:wick=0.9400; 2026-10-22:RED:body=-0.9000:wick=1.3800; 2026-10-23:GREEN:body=+1.1200:wick=1.1350 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=31.47 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.0 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 24313.0 | **NEUTRAL** |
| `B04_income` | 3412.0 | **GOOD** |
| `B05_profit_margin` | 14.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.45 vs prior_export=68.45 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.13 vs prior=0.13 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.47 (this export) | prior_export=31.47 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.0 (this export) | prior_export=14.0 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+15**  ·  Consumer Electronics
price=347.7250061035156  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.92 on 2026-10-23; prev RSI=64.76 on 2026-10-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.76@2026-10-22 → 63.92@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.76@2026-10-22 → 63.92@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.76@2026-10-22 → 63.92@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=2.363 (G=3.2850 R=1.3900); 2026-10-22:GREEN:O=344.9350,C=348.2200,body=+3.2850,vol=46659852.0; 2026-10-23:RED:O=349.1150,C=347.7250,body=-1.3900,vol=39450857.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.183 (Gvol=46659852 Rvol=39450857); 2026-10-22:GREEN:O=344.9350,C=348.2200,body=+3.2850,vol=46659852.0; 2026-10-23:RED:O=349.1150,C=347.7250,body=-1.3900,vol=39450857.0 | **GOOD** |
| `A07_rvol` | RVOL=1.048 on 2026-10-23: today_vol=39450857 / avg20=37653718 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.808 on 2026-10-23 (price=347.7250, mid=338.9058, upper=349.8240, lower=327.9875; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=347.7250 vs SMA50=329.4667 dist=+5.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=338.9058 SMA50=329.4667 SMA80=325.4042 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-07-31@299.7415; S2[2026-08-26→2026-09-24] low=2026-08-26@308.8000; S3[2026-09-25→2026-10-23] low=2026-10-01@325.8100 | lows=[299.7415030462749, 308.79998779296875, 325.80999755859375] span=8.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.5402978482048296 wick_frac=0.4597021517951704 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.23243839083861947 wick_frac=0.7675616091613805 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3633389685379935 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:GREEN:body=+1.9650:wick=3.9100; 2026-10-20:RED:body=-0.1080:wick=4.2520; 2026-10-21:GREEN:body=+4.7850:wick=1.6050; 2026-10-22:GREEN:body=+3.2850:wick=2.7950; 2026-10-23:RED:body=-1.3900:wick=4.5900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=337.68 vs prior_export=337.68 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.28 vs prior=-2.28 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+15**  ·  Waste Management
price=240.4499969482422  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.77 on 2026-10-23; prev RSI=75.56 on 2026-10-22 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 75.56@2026-10-22 → 72.77@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.56@2026-10-22 → 72.77@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 75.56@2026-10-22 → 72.77@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=8.148 (G=9.9400 R=1.2200); 2026-10-22:GREEN:O=231.9600,C=241.9000,body=+9.9400,vol=1382699.0; 2026-10-23:RED:O=241.6700,C=240.4500,body=-1.2200,vol=1881914.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=0.735 (Gvol=1382699 Rvol=1881914); 2026-10-22:GREEN:O=231.9600,C=241.9000,body=+9.9400,vol=1382699.0; 2026-10-23:RED:O=241.6700,C=240.4500,body=-1.2200,vol=1881914.0 | **BAD** |
| `A07_rvol` | RVOL=1.574 on 2026-10-23: today_vol=1881914 / avg20=1195396 (avg window 2026-09-25→2026-10-22, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.911 on 2026-10-23 (price=240.4500, mid=221.4350, upper=242.3056, lower=200.5644; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=240.4500 vs SMA50=220.3670 dist=+9.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=221.4350 SMA50=220.3670 SMA80=218.8189 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-23 (63 bars); S1[2026-07-27→2026-08-25] low=2026-08-05@204.8000; S2[2026-08-26→2026-09-24] low=2026-09-23@212.6100; S3[2026-09-25→2026-10-23] low=2026-10-02@205.9700 | lows=[204.8000030517578, 212.61000061035156, 205.97000122070312] span=3.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.8484840196208682 wick_frac=0.15151598037913186 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.280459657221431 wick_frac=0.7195403427785689 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.14752232533707 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:RED:body=-0.7400:wick=1.8600; 2026-10-20:GREEN:body=+1.3800:wick=2.1150; 2026-10-21:RED:body=-1.6200:wick=1.8200; 2026-10-22:GREEN:body=+9.9400:wick=1.7750; 2026-10-23:RED:body=-1.2200:wick=3.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 6.57 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=6.57 vs prior=6.57 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.56 | **GOOD** |
| `B13_short_float` | 2.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BP  ·  score **+15**  ·  Oil & Gas Integrated
price=51.31999969482422  pair=`2026-10-22→2026-10-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=74.11 on 2026-10-23; prev RSI=70.03 on 2026-10-22 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 70.03@2026-10-22 → 74.11@2026-10-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.03@2026-10-22 → 74.11@2026-10-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 70.03@2026-10-22 → 74.11@2026-10-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_body_sum/RED_body_sum=2.937 (G=0.7050 R=0.2400); 2026-10-22:RED:O=50.2900,C=50.0500,body=-0.2400,vol=7577423.0; 2026-10-23:GREEN:O=50.6150,C=51.3200,body=+0.7050,vol=9154101.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-22 + 2026-10-23; ratio=GREEN_vol/RED_vol=1.208 (Gvol=9154101 Rvol=7577423); 2026-10-22:RED:O=50.2900,C=50.0500,body=-0.2400,vol=7577423.0; 2026-10-23:GREEN:O=50.6150,C=51.3200,body=+0.7050,vol=9154101.0 | **GOOD** |
| `A07_rvol` | RVOL=1.028 on 2026-10-23: today_vol=9154101 / avg20=8902765 (avg window 2026-09-25→2026-10-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.847 on 2026-10-23 (price=51.3200, mid=47.0375, upper=52.0950, lower=41.9800; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-23: price=51.3200 vs SMA50=45.3148 dist=+13.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-23: SMA20=47.0375 SMA50=45.3148 SMA80=43.7946 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-28→2026-10-23 (63 bars); S1[2026-07-28→2026-08-25] low=2026-08-05@40.6603; S2[2026-08-26→2026-09-24] low=2026-08-28@41.7200; S3[2026-09-25→2026-10-23] low=2026-09-29@43.3500 | lows=[40.66027501103988, 41.720001220703125, 43.349998474121094] span=6.62% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: GREEN body_frac=0.7540074661879602 wick_frac=0.24599253381203973 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-22+2026-10-23: RED body_frac=0.19672310554539343 wick_frac=0.8032768944546066 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.9374711912898355 need>1.4; red_wick_gt_green=True 5d trail=2026-10-19:RED:body=-0.0100:wick=0.7700; 2026-10-20:RED:body=-0.5100:wick=0.9800; 2026-10-21:GREEN:body=+0.1200:wick=0.5300; 2026-10-22:RED:body=-0.2400:wick=0.9800; 2026-10-23:GREEN:body=+0.7050:wick=0.2300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.35 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.04 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 216829.8 | **NEUTRAL** |
| `B04_income` | 5490.34 | **GOOD** |
| `B05_profit_margin` | 2.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.54 vs prior_export=50.54 on finviz_2026-10-23) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-23) | **NEUTRAL** |
| `B12_institutional_transactions` | 60.28 | **GOOD** |
| `B13_short_float` | 0.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.35 (this export) | prior_export=18.35 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.04 (this export) | prior_export=11.04 (finviz_2026-10-23) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-26_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.