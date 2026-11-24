# A+B1 Feature Checklist — 2026-11-23

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,590** names
- Export: `finviz_2026-11-23.csv` · prior export for Δ: `2026-11-20`
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
| 1 | SR | +18 | 19 | 1 | 2026-11-20→2026-11-23 | Utilities - Regulated Gas |
| 2 | GLOB | +18 | 19 | 1 | 2026-11-20→2026-11-23 | Information Technology Services |
| 3 | UPBD | +18 | 18 | 0 | 2026-11-20→2026-11-23 | Software - Application |
| 4 | LNT | +17 | 17 | 0 | 2026-11-20→2026-11-23 | Utilities - Regulated Electric |
| 5 | MDLN | +17 | 18 | 1 | 2026-11-20→2026-11-23 | Medical Instruments & Supplies |
| 6 | KMB | +16 | 16 | 0 | 2026-11-20→2026-11-23 | Household & Personal Products |
| 7 | KO | +16 | 18 | 2 | 2026-11-20→2026-11-23 | Beverages - Non-Alcoholic |
| 8 | NVST | +16 | 17 | 1 | 2026-11-20→2026-11-23 | Medical Instruments & Supplies |
| 9 | V | +16 | 16 | 0 | 2026-11-20→2026-11-23 | Credit Services |
| 10 | ARDT | +16 | 16 | 0 | 2026-11-20→2026-11-23 | Medical Care Facilities |
| 11 | ICLR | +16 | 18 | 2 | 2026-11-20→2026-11-23 | Diagnostics & Research |
| 12 | OCTV | +16 | 17 | 1 | 2026-11-20→2026-11-23 | Software - Application |
| 13 | AMH | +15 | 17 | 2 | 2026-11-20→2026-11-23 | REIT - Residential |
| 14 | CON | +15 | 16 | 1 | 2026-11-20→2026-11-23 | Medical Care Facilities |
| 15 | GPK | +15 | 15 | 0 | 2026-11-20→2026-11-23 | Packaging & Containers |

## Full checklist — top 15

### SR  ·  score **+18**  ·  Utilities - Regulated Gas
price=86.48999786376953  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.47 on 2026-11-23; prev RSI=48.05 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.05@2026-11-20 → 51.47@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.05@2026-11-20 → 51.47@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.05@2026-11-20 → 51.47@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=1.981 (G=1.0500 R=0.5300); 2026-11-20:RED:O=86.2300,C=85.7000,body=-0.5300,vol=777000.0; 2026-11-23:GREEN:O=85.4400,C=86.4900,body=+1.0500,vol=812400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.046 (Gvol=812400 Rvol=777000); 2026-11-20:RED:O=86.2300,C=85.7000,body=-0.5300,vol=777000.0; 2026-11-23:GREEN:O=85.4400,C=86.4900,body=+1.0500,vol=812400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.376 on 2026-11-23: today_vol=812400 / avg20=590386 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.466 on 2026-11-23 (price=86.4900, mid=87.8110, upper=90.6435, lower=84.9785; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=86.4900 vs SMA50=83.4806 dist=+3.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=87.8110 SMA50=83.4806 SMA80=82.9430 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-13→2026-11-23 (63 bars); S1[2026-08-13→2026-09-11] low=2026-08-24@79.9000; S2[2026-09-14→2026-10-23] low=2026-10-01@74.8400; S3[2026-10-26→2026-11-23] low=2026-11-19@83.4700 | lows=[79.9000015258789, 74.83999633789062, 83.47000122070312] span=11.53% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.7553927218837477 wick_frac=0.24460727811625227 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.3581116260367963 wick_frac=0.6418883739632038 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9810994832227324 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.0400:wick=2.4600; 2026-11-18:GREEN:body=+1.4900:wick=0.3000; 2026-11-19:RED:body=-0.6800:wick=2.2300; 2026-11-20:RED:body=-0.5300:wick=0.9500; 2026-11-23:GREEN:body=+1.0500:wick=0.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.06 (current export asof; earnings_date=11/17/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.57 (current export; earnings_date=11/17/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2541.8 | **NEUTRAL** |
| `B04_income` | 476.8 | **GOOD** |
| `B05_profit_margin` | 18.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 92.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.9099999999999966 (now=92.64 vs prior_export=91.73 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.2 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.2 vs prior=-0.2 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.54 | **GOOD** |
| `B13_short_float` | 4.86 | **NEUTRAL** |
| `B14_earnings_date` | 11/17/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.06 (this export) | prior_export=4.06 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.57 (this export) | prior_export=23.57 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GLOB  ·  score **+18**  ·  Information Technology Services
price=45.56999969482422  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.25 on 2026-11-23; prev RSI=44.56 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.56@2026-11-20 → 55.25@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.56@2026-11-20 → 55.25@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.56@2026-11-20 → 55.25@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=2.121 (G=3.8500 R=1.8150); 2026-11-20:RED:O=43.4450,C=41.6300,body=-1.8150,vol=3946700.0; 2026-11-23:GREEN:O=41.7200,C=45.5700,body=+3.8500,vol=1503500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=0.381 (Gvol=1503500 Rvol=3946700); 2026-11-20:RED:O=43.4450,C=41.6300,body=-1.8150,vol=3946700.0; 2026-11-23:GREEN:O=41.7200,C=45.5700,body=+3.8500,vol=1503500.0 | **BAD** |
| `A07_rvol` | RVOL=1.201 on 2026-11-23: today_vol=1503500 / avg20=1251515 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.055 on 2026-11-23 (price=45.5700, mid=45.2925, upper=50.3677, lower=40.2173; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=45.5700 vs SMA50=40.7402 dist=+11.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=45.2925 SMA50=40.7402 SMA80=39.9798 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-24@33.2600; S2[2026-09-25→2026-10-23] low=2026-09-29@33.0000; S3[2026-10-26→2026-11-23] low=2026-11-20@39.1200 | lows=[33.2599983215332, 33.0, 39.119998931884766] span=18.55% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.9101652224022856 wick_frac=0.08983477759771442 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.3134712158406761 wick_frac=0.6865287841593238 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.121212885489637 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-1.4700:wick=1.2850; 2026-11-18:GREEN:body=+0.4400:wick=0.5900; 2026-11-19:RED:body=-2.4300:wick=0.4400; 2026-11-20:RED:body=-1.8150:wick=3.9750; 2026-11-23:GREEN:body=+3.8500:wick=0.3800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.07 (current export asof; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.06 (current export; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2445.11 | **NEUTRAL** |
| `B04_income` | 94.23 | **GOOD** |
| `B05_profit_margin` | 3.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 52.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.3999999999999986 (now=52.05 vs prior_export=51.65 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.94 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.94 vs prior=0.94 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.74 | **GOOD** |
| `B13_short_float` | 26.32 | **GOOD** |
| `B14_earnings_date` | 11/19/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.07 (this export) | prior_export=0.07 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.06 (this export) | prior_export=0.06 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UPBD  ·  score **+18**  ·  Software - Application
price=26.600000381469727  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.96 on 2026-11-23; prev RSI=66.38 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.38@2026-11-20 → 69.96@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.38@2026-11-20 → 69.96@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.38@2026-11-20 → 69.96@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0900 R=0.0000); 2026-11-20:GREEN:O=25.0400,C=25.4400,body=+0.4000,vol=1699900.0; 2026-11-23:GREEN:O=25.9100,C=26.6000,body=+0.6900,vol=1726500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3426400 Rvol=0); 2026-11-20:GREEN:O=25.0400,C=25.4400,body=+0.4000,vol=1699900.0; 2026-11-23:GREEN:O=25.9100,C=26.6000,body=+0.6900,vol=1726500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.044 on 2026-11-23: today_vol=1726500 / avg20=1653621 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.562 on 2026-11-23 (price=26.6000, mid=23.4805, upper=29.0345, lower=17.9265; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=26.6000 vs SMA50=19.1403 dist=+38.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=23.4805 SMA50=19.1403 SMA80=19.1296 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-24@15.7600; S2[2026-09-25→2026-10-23] low=2026-10-07@14.9800; S3[2026-10-26→2026-11-23] low=2026-10-28@16.5000 | lows=[15.760000228881836, 14.979999542236328, 16.5] span=10.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.5200577924377406 wick_frac=0.47994220756225936 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.0300:wick=1.0600; 2026-11-18:GREEN:body=+1.6400:wick=0.2300; 2026-11-19:RED:body=-0.4900:wick=0.8000; 2026-11-20:GREEN:body=+0.4000:wick=0.4600; 2026-11-23:GREEN:body=+0.6900:wick=0.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.29 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.99 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4725.76 | **NEUTRAL** |
| `B04_income` | 111.39 | **GOOD** |
| `B05_profit_margin` | 2.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 27.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=27.33 vs prior_export=27.33 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 3.76 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.6599999999999997 (now=3.76 vs prior=3.1 on finviz_2026-11-20) | **GOOD** |
| `B12_institutional_transactions` | 7.83 | **GOOD** |
| `B13_short_float` | 14.95 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.29 (this export) | prior_export=11.29 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.99 (this export) | prior_export=1.99 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNT  ·  score **+17**  ·  Utilities - Regulated Electric
price=72.48999786376953  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.04 on 2026-11-23; prev RSI=53.06 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.06@2026-11-20 → 57.04@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.06@2026-11-20 → 57.04@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.06@2026-11-20 → 57.04@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=3.321 (G=0.9300 R=0.2800); 2026-11-20:RED:O=71.9000,C=71.6200,body=-0.2800,vol=2793100.0; 2026-11-23:GREEN:O=71.5600,C=72.4900,body=+0.9300,vol=2869900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.027 (Gvol=2869900 Rvol=2793100); 2026-11-20:RED:O=71.9000,C=71.6200,body=-0.2800,vol=2793100.0; 2026-11-23:GREEN:O=71.5600,C=72.4900,body=+0.9300,vol=2869900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.879 on 2026-11-23: today_vol=2869900 / avg20=3263698 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.089 on 2026-11-23 (price=72.4900, mid=72.2685, upper=74.7559, lower=69.7812; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=72.4900 vs SMA50=68.4864 dist=+5.85% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=72.27_50=68.49_80=68.77 on 2026-11-23: SMA20=72.2685 SMA50=68.4864 SMA80=68.7695 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-23 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-21@64.7600; S2[2026-09-22→2026-10-23] low=2026-10-01@62.5400; S3[2026-10-26→2026-11-23] low=2026-10-28@69.1900 | lows=[64.76000213623047, 62.540000915527344, 69.19000244140625] span=10.63% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.8773544844066016 wick_frac=0.12264551559339845 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.24561309579579982 wick_frac=0.7543869042042002 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.321444141689373 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-1.1200:wick=0.3500; 2026-11-18:GREEN:body=+1.1500:wick=0.4700; 2026-11-19:RED:body=-0.3700:wick=0.4200; 2026-11-20:RED:body=-0.2800:wick=0.8600; 2026-11-23:GREEN:body=+0.9300:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.41 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.93 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4504.0 | **NEUTRAL** |
| `B04_income` | 934.0 | **GOOD** |
| `B05_profit_margin` | 20.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 77.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.25 (now=77.33 vs prior_export=77.08 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.93 | **GOOD** |
| `B13_short_float` | 11.02 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.41 (this export) | prior_export=16.41 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.93 (this export) | prior_export=1.93 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDLN  ·  score **+17**  ·  Medical Instruments & Supplies
price=38.529998779296875  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.31 on 2026-11-23; prev RSI=48.92 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.92@2026-11-20 → 54.31@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.92@2026-11-20 → 54.31@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.92@2026-11-20 → 54.31@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=1.475 (G=0.9000 R=0.6100); 2026-11-20:RED:O=37.6500,C=37.0400,body=-0.6100,vol=9330900.0; 2026-11-23:GREEN:O=37.6300,C=38.5300,body=+0.9000,vol=14974400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.605 (Gvol=14974400 Rvol=9330900); 2026-11-20:RED:O=37.6500,C=37.0400,body=-0.6100,vol=9330900.0; 2026-11-23:GREEN:O=37.6300,C=38.5300,body=+0.9000,vol=14974400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.207 on 2026-11-23: today_vol=14974400 / avg20=12410461 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.160 on 2026-11-23 (price=38.5300, mid=37.4495, upper=44.2044, lower=30.6946; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=38.5300 vs SMA50=35.5970 dist=+8.24% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=37.45_50=35.60_80=35.71 on 2026-11-23: SMA20=37.4495 SMA50=35.5970 SMA80=35.7138 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-23 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-18@31.4900; S2[2026-09-22→2026-10-23] low=2026-09-22@32.3850; S3[2026-10-26→2026-11-23] low=2026-10-30@31.3700 | lows=[31.489999771118164, 32.3849983215332, 31.3700008392334] span=3.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.5817693576665015 wick_frac=0.41823064233349855 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.4265737623577492 wick_frac=0.5734262376422509 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4754046076493985 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-1.4900:wick=0.4040; 2026-11-18:GREEN:body=+2.3300:wick=0.3300; 2026-11-19:RED:body=-1.3100:wick=0.5350; 2026-11-20:RED:body=-0.6100:wick=0.8200; 2026-11-23:GREEN:body=+0.9000:wick=0.6470 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.21 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.74 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 30680.0 | **NEUTRAL** |
| `B04_income` | 569.0 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.14000000000000057 (now=46.14 vs prior_export=46.0 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | -19.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-19.27 vs prior=-19.27 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.36 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.21 (this export) | prior_export=28.21 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.74 (this export) | prior_export=1.74 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KMB  ·  score **+16**  ·  Household & Personal Products
price=107.72000122070312  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.25 on 2026-11-23; prev RSI=48.54 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.54@2026-11-20 → 51.25@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.54@2026-11-20 → 51.25@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.54@2026-11-20 → 51.25@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=3.711 (G=1.6700 R=0.4500); 2026-11-20:RED:O=106.8300,C=106.3800,body=-0.4500,vol=5755200.0; 2026-11-23:GREEN:O=106.0500,C=107.7200,body=+1.6700,vol=5949100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.034 (Gvol=5949100 Rvol=5755200); 2026-11-20:RED:O=106.8300,C=106.3800,body=-0.4500,vol=5755200.0; 2026-11-23:GREEN:O=106.0500,C=107.7200,body=+1.6700,vol=5949100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.788 on 2026-11-23: today_vol=5949100 / avg20=7546578 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.207 on 2026-11-23 (price=107.7200, mid=108.5050, upper=112.2979, lower=104.7121; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=107.7200 vs SMA50=103.5879 dist=+3.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=108.50_50=103.59_80=105.08 on 2026-11-23: SMA20=108.5050 SMA50=103.5879 SMA80=105.0774 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-21@96.8200; S2[2026-09-25→2026-10-23] low=2026-10-05@93.9450; S3[2026-10-26→2026-11-23] low=2026-11-03@104.4200 | lows=[96.81999969482422, 93.94499969482422, 104.41999816894531] span=11.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.6844269343214046 wick_frac=0.31557306567859544 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.14331358761413723 wick_frac=0.8566864123858627 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.711069291151688 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-1.6700:wick=1.1900; 2026-11-18:GREEN:body=+0.0500:wick=1.3700; 2026-11-19:RED:body=-1.7400:wick=0.5700; 2026-11-20:RED:body=-0.4500:wick=2.6900; 2026-11-23:GREEN:body=+1.6700:wick=0.7700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.86 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16566.0 | **NEUTRAL** |
| `B04_income` | 3302.0 | **GOOD** |
| `B05_profit_margin` | 19.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 117.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=117.47 vs prior_export=117.47 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.37 | **GOOD** |
| `B13_short_float` | 8.63 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.86 (this export) | prior_export=2.86 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.18 (this export) | prior_export=0.18 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KO  ·  score **+16**  ·  Beverages - Non-Alcoholic
price=95.87999725341797  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.89 on 2026-11-23; prev RSI=57.97 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.97@2026-11-20 → 52.89@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.97@2026-11-20 → 52.89@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.97@2026-11-20 → 52.89@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=127.973 (G=1.2800 R=0.0100); 2026-11-20:RED:O=97.0000,C=96.9900,body=-0.0100,vol=26046900.0; 2026-11-23:GREEN:O=94.6000,C=95.8800,body=+1.2800,vol=30102600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.156 (Gvol=30102600 Rvol=26046900); 2026-11-20:RED:O=97.0000,C=96.9900,body=-0.0100,vol=26046900.0; 2026-11-23:GREEN:O=94.6000,C=95.8800,body=+1.2800,vol=30102600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.577 on 2026-11-23: today_vol=30102600 / avg20=19092558 (avg window 2026-10-26→2026-11-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.028 on 2026-11-23 (price=95.8800, mid=96.0080, upper=100.5536, lower=91.4624; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=95.8800 vs SMA50=91.9374 dist=+4.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=96.0080 SMA50=91.9374 SMA80=90.6195 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-21@87.1200; S2[2026-09-25→2026-10-23] low=2026-10-05@85.1300; S3[2026-10-26→2026-11-23] low=2026-10-28@90.5800 | lows=[87.12000274658203, 85.12999725341797, 90.58000183105469] span=6.40% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.6632116978760243 wick_frac=0.33678830212397565 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.009617430216777318 wick_frac=0.9903825697832227 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=127.97254004576659 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:DOJI:body=+0.0000:wick=1.0800; 2026-11-18:GREEN:body=+0.1200:wick=1.5900; 2026-11-19:RED:body=-0.1200:wick=1.8300; 2026-11-20:RED:body=-0.0100:wick=1.0300; 2026-11-23:GREEN:body=+1.2800:wick=0.6500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.34 (current export asof; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 51447.0 | **NEUTRAL** |
| `B04_income` | 14629.0 | **GOOD** |
| `B05_profit_margin` | 28.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 99.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=99.91 vs prior_export=99.91 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.66 | **GOOD** |
| `B10_insider_transactions` | -6.16 | **BAD** |
| `B11_insider_tx_delta` | delta=0.09999999999999964 (now=-6.16 vs prior=-6.26 on finviz_2026-11-20) | **GOOD** |
| `B12_institutional_transactions` | -0.35 | **BAD** |
| `B13_short_float` | 1.11 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.34 (this export) | prior_export=3.34 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NVST  ·  score **+16**  ·  Medical Instruments & Supplies
price=24.31999969482422  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.41 on 2026-11-23; prev RSI=39.82 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.82@2026-11-20 → 50.41@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 39.82@2026-11-20 → 50.41@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 39.82@2026-11-20 → 50.41@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=3.100 (G=0.9300 R=0.3000); 2026-11-20:RED:O=23.2100,C=22.9100,body=-0.3000,vol=2314900.0; 2026-11-23:GREEN:O=23.3900,C=24.3200,body=+0.9300,vol=2957000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.277 (Gvol=2957000 Rvol=2314900); 2026-11-20:RED:O=23.2100,C=22.9100,body=-0.3000,vol=2314900.0; 2026-11-23:GREEN:O=23.3900,C=24.3200,body=+0.9300,vol=2957000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.932 on 2026-11-23: today_vol=2957000 / avg20=3174347 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.104 on 2026-11-23 (price=24.3200, mid=24.5350, upper=26.6019, lower=22.4681; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=24.3200 vs SMA50=24.0448 dist=+1.14% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=24.54_50=24.04_80=25.38 on 2026-11-23: SMA20=24.5350 SMA50=24.0448 SMA80=25.3754 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-23 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-18@23.1050; S2[2026-09-22→2026-10-23] low=2026-10-08@22.2900; S3[2026-10-26→2026-11-23] low=2026-11-20@22.7700 | lows=[23.104999542236328, 22.290000915527344, 22.770000457763672] span=3.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.744000244140625 wick_frac=0.255999755859375 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.30302960244294813 wick_frac=0.6969703975570519 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.100008900982923 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.1700:wick=0.7660; 2026-11-18:GREEN:body=+0.2600:wick=0.4000; 2026-11-19:RED:body=-0.4800:wick=0.3150; 2026-11-20:RED:body=-0.3000:wick=0.6900; 2026-11-23:GREEN:body=+0.9300:wick=0.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.42 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2878.1 | **NEUTRAL** |
| `B04_income` | 633.6 | **GOOD** |
| `B05_profit_margin` | 22.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.92 vs prior_export=28.92 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.19 | **GOOD** |
| `B10_insider_transactions` | -0.21 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.21 vs prior=-0.21 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.42 | **GOOD** |
| `B13_short_float` | 6.21 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.42 (this export) | prior_export=12.42 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+16**  ·  Credit Services
price=409.3999938964844  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.50 on 2026-11-23; prev RSI=43.57 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.57@2026-11-20 → 49.50@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.57@2026-11-20 → 49.50@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.57@2026-11-20 → 49.50@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=3.189 (G=5.3900 R=1.6900); 2026-11-20:RED:O=403.5600,C=401.8700,body=-1.6900,vol=8426600.0; 2026-11-23:GREEN:O=404.0100,C=409.4000,body=+5.3900,vol=8676200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.030 (Gvol=8676200 Rvol=8426600); 2026-11-20:RED:O=403.5600,C=401.8700,body=-1.6900,vol=8426600.0; 2026-11-23:GREEN:O=404.0100,C=409.4000,body=+5.3900,vol=8676200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.189 on 2026-11-23: today_vol=8676200 / avg20=7298968 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.388 on 2026-11-23 (price=409.4000, mid=417.1540, upper=437.1630, lower=397.1450; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=409.4000 vs SMA50=395.3212 dist=+3.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=417.1540 SMA50=395.3212 SMA80=386.0282 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-22@359.8000; S2[2026-09-25→2026-10-23] low=2026-10-02@356.7500; S3[2026-10-26→2026-11-23] low=2026-10-28@394.0000 | lows=[359.79998779296875, 356.75, 394.0] span=10.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.7283768346646982 wick_frac=0.27162316533530184 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.2238417448807185 wick_frac=0.7761582551192815 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.189335115027628 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-6.5000:wick=2.2800; 2026-11-18:RED:body=-0.7600:wick=5.3100; 2026-11-19:RED:body=-8.7900:wick=3.9800; 2026-11-20:RED:body=-1.6900:wick=5.8600; 2026-11-23:GREEN:body=+5.3900:wick=2.0100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.2 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 45903.0 | **NEUTRAL** |
| `B04_income` | 23313.0 | **GOOD** |
| `B05_profit_margin` | 50.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 457.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=457.86 vs prior_export=457.86 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.63 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.2 (this export) | prior_export=1.2 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARDT  ·  score **+16**  ·  Medical Care Facilities
price=15.59000015258789  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.90 on 2026-11-23; prev RSI=53.11 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.11@2026-11-20 → 55.90@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.11@2026-11-20 → 55.90@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.11@2026-11-20 → 55.90@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1100 R=0.0000); 2026-11-20:GREEN:O=14.4900,C=15.0800,body=+0.5900,vol=1540900.0; 2026-11-23:GREEN:O=15.0700,C=15.5900,body=+0.5200,vol=1458000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2998900 Rvol=0); 2026-11-20:GREEN:O=14.4900,C=15.0800,body=+0.5900,vol=1540900.0; 2026-11-23:GREEN:O=15.0700,C=15.5900,body=+0.5200,vol=1458000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.111 on 2026-11-23: today_vol=1458000 / avg20=1312889 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.088 on 2026-11-23 (price=15.5900, mid=15.1355, upper=20.3188, lower=9.9522; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=15.5900 vs SMA50=12.7508 dist=+22.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=15.1355 SMA50=12.7508 SMA80=11.8004 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-11-23 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-15@9.5200; S2[2026-08-18→2026-10-23] low=2026-08-21@10.3500; S3[2026-10-26→2026-11-23] low=2026-11-03@11.5500 | lows=[9.520000457763672, 10.350000381469727, 11.550000190734863] span=21.32% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.6820462474337889 wick_frac=0.31795375256621106 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-17:RED:body=-0.3400:wick=0.3500; 2026-11-18:GREEN:body=+0.6500:wick=0.4200; 2026-11-19:GREEN:body=+0.1700:wick=0.4000; 2026-11-20:GREEN:body=+0.5900:wick=0.2200; 2026-11-23:GREEN:body=+0.5200:wick=0.2980 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=33.62 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.31 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6454.05 | **NEUTRAL** |
| `B04_income` | 146.22 | **GOOD** |
| `B05_profit_margin` | 2.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.77 vs prior_export=17.77 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.78 | **GOOD** |
| `B13_short_float` | 15.69 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.62 (this export) | prior_export=33.62 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.31 (this export) | prior_export=1.31 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICLR  ·  score **+16**  ·  Diagnostics & Research
price=195.4499969482422  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.23 on 2026-11-23; prev RSI=44.67 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.67@2026-11-20 → 53.23@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.67@2026-11-20 → 53.23@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.67@2026-11-20 → 53.23@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=2.411 (G=8.0300 R=3.3300); 2026-11-20:RED:O=190.0400,C=186.7100,body=-3.3300,vol=735400.0; 2026-11-23:GREEN:O=187.4200,C=195.4500,body=+8.0300,vol=866700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.179 (Gvol=866700 Rvol=735400); 2026-11-20:RED:O=190.0400,C=186.7100,body=-3.3300,vol=735400.0; 2026-11-23:GREEN:O=187.4200,C=195.4500,body=+8.0300,vol=866700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.721 on 2026-11-23: today_vol=866700 / avg20=1202074 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.270 on 2026-11-23 (price=195.4500, mid=200.1785, upper=217.6755, lower=182.6815; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=195.4500 vs SMA50=182.2738 dist=+7.23% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=200.1785 SMA50=182.2738 SMA80=176.3822 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-23 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-10@156.7000; S2[2026-09-22→2026-10-23] low=2026-10-07@157.3750; S3[2026-10-26→2026-11-23] low=2026-10-26@179.1600 | lows=[156.6999969482422, 157.375, 179.16000366210938] span=14.33% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.6886789242949682 wick_frac=0.31132107570503176 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.46314122970093635 wick_frac=0.5368587702990637 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4114207685328592 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:GREEN:body=+3.7800:wick=3.2500; 2026-11-18:GREEN:body=+1.4100:wick=4.5900; 2026-11-19:RED:body=-3.9000:wick=5.4700; 2026-11-20:RED:body=-3.3300:wick=3.8600; 2026-11-23:GREEN:body=+8.0300:wick=3.6300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.65 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.61 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8234.76 | **NEUTRAL** |
| `B04_income` | 448.66 | **GOOD** |
| `B05_profit_margin` | 5.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 234.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.060000000000002274 (now=234.56 vs prior_export=234.5 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -5.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.34 vs prior=-5.34 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.88 | **BAD** |
| `B13_short_float` | 3.2 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.65 (this export) | prior_export=1.65 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.61 (this export) | prior_export=0.61 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OCTV  ·  score **+16**  ·  Software - Application
price=21.260000228881836  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.91 on 2026-11-23; prev RSI=43.47 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.47@2026-11-20 → 52.91@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.47@2026-11-20 → 52.91@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.47@2026-11-20 → 52.91@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=3.571 (G=0.7500 R=0.2100); 2026-11-20:RED:O=20.4500,C=20.2400,body=-0.2100,vol=2738100.0; 2026-11-23:GREEN:O=20.5100,C=21.2600,body=+0.7500,vol=3742000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.367 (Gvol=3742000 Rvol=2738100); 2026-11-20:RED:O=20.4500,C=20.2400,body=-0.2100,vol=2738100.0; 2026-11-23:GREEN:O=20.5100,C=21.2600,body=+0.7500,vol=3742000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.848 on 2026-11-23: today_vol=3742000 / avg20=2024923 (avg window 2026-10-26→2026-11-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.020 on 2026-11-23 (price=21.2600, mid=21.2775, upper=22.1605, lower=20.3945; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=21.2600 vs SMA50=20.3496 dist=+4.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=21.2775 SMA50=20.3496 SMA80=19.8134 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-10@17.0400; S2[2026-09-25→2026-10-23] low=2026-09-29@18.1400; S3[2026-10-26→2026-11-23] low=2026-11-23@20.1300 | lows=[17.040000915527344, 18.139999389648438, 20.1299991607666] span=18.13% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.5474449201211236 wick_frac=0.4525550798788765 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.2500011353263609 wick_frac=0.7499988646736391 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.571411703799239 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:GREEN:body=+0.1200:wick=0.5800; 2026-11-18:GREEN:body=+0.7700:wick=0.1400; 2026-11-19:GREEN:body=+0.0300:wick=0.7700; 2026-11-20:RED:body=-0.2100:wick=0.6300; 2026-11-23:GREEN:body=+0.7500:wick=0.6200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.81 (current export asof; earnings_date=11/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.86 (current export; earnings_date=11/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | nan | **NEUTRAL** |
| `B04_income` | nan | **NEUTRAL** |
| `B05_profit_margin` | nan | **NEUTRAL** |
| `B06_profitable` | False | **BAD** |
| `B07_target_price` | 24.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.2699999999999996 (now=24.16 vs prior_export=23.89 on finviz_2026-11-20) | **GOOD** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 329.06 | **GOOD** |
| `B13_short_float` | 1.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.81 (this export) | prior_export=1.81 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.86 (this export) | prior_export=2.86 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMH  ·  score **+15**  ·  REIT - Residential
price=32.099998474121094  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.23 on 2026-11-23; prev RSI=49.59 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.59@2026-11-20 → 51.23@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.59@2026-11-20 → 51.23@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.59@2026-11-20 → 51.23@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=12.000 (G=0.1200 R=0.0100); 2026-11-20:RED:O=31.9900,C=31.9800,body=-0.0100,vol=2160800.0; 2026-11-23:GREEN:O=31.9800,C=32.1000,body=+0.1200,vol=2826400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.308 (Gvol=2826400 Rvol=2160800); 2026-11-20:RED:O=31.9900,C=31.9800,body=-0.0100,vol=2160800.0; 2026-11-23:GREEN:O=31.9800,C=32.1000,body=+0.1200,vol=2826400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.102 on 2026-11-23: today_vol=2826400 / avg20=2564170 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.084 on 2026-11-23 (price=32.1000, mid=32.1870, upper=33.2276, lower=31.1464; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=32.1000 vs SMA50=31.4734 dist=+1.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=32.19_50=31.47_80=32.35 on 2026-11-23: SMA20=32.1870 SMA50=31.4734 SMA80=32.3452 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-23 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-18@30.9000; S2[2026-09-22→2026-10-23] low=2026-10-08@29.9600; S3[2026-10-26→2026-11-23] low=2026-10-28@31.1200 | lows=[30.899999618530273, 29.959999084472656, 31.1200008392334] span=3.87% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.23076697355390088 wick_frac=0.7692330264460991 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.016393852696089926 wick_frac=0.98360614730391 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.999618539004388 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.1300:wick=0.4600; 2026-11-18:GREEN:body=+0.3000:wick=0.3300; 2026-11-19:RED:body=-0.0700:wick=0.3500; 2026-11-20:RED:body=-0.0100:wick=0.6000; 2026-11-23:GREEN:body=+0.1200:wick=0.4000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.0 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.91 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1910.86 | **NEUTRAL** |
| `B04_income` | 446.04 | **GOOD** |
| `B05_profit_margin` | 23.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.060000000000002274 (now=36.58 vs prior_export=36.64 on finviz_2026-11-20) | **BAD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.54 | **GOOD** |
| `B13_short_float` | 4.28 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.0 (this export) | prior_export=25.0 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.91 (this export) | prior_export=1.91 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CON  ·  score **+15**  ·  Medical Care Facilities
price=43.16999816894531  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.98 on 2026-11-23; prev RSI=55.72 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.72@2026-11-20 → 61.98@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.72@2026-11-20 → 61.98@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.72@2026-11-20 → 61.98@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5300 R=0.0000); 2026-11-20:GREEN:O=41.2700,C=41.8400,body=+0.5700,vol=876500.0; 2026-11-23:GREEN:O=42.2100,C=43.1700,body=+0.9600,vol=718200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1594700 Rvol=0); 2026-11-20:GREEN:O=41.2700,C=41.8400,body=+0.5700,vol=876500.0; 2026-11-23:GREEN:O=42.2100,C=43.1700,body=+0.9600,vol=718200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.653 on 2026-11-23: today_vol=718200 / avg20=1099345 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.525 on 2026-11-23 (price=43.1700, mid=41.9475, upper=44.2756, lower=39.6194; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=43.1700 vs SMA50=38.8048 dist=+11.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-23: SMA20=41.9475 SMA50=38.8048 SMA80=36.9219 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-09@33.3200; S2[2026-09-25→2026-10-23] low=2026-10-01@34.0520; S3[2026-10-26→2026-11-23] low=2026-10-26@39.3200 | lows=[33.31999969482422, 34.051998138427734, 39.31999969482422] span=18.01% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.6066986989127993 wick_frac=0.39330130108720074 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.3400:wick=0.5300; 2026-11-18:RED:body=-0.1000:wick=1.0900; 2026-11-19:RED:body=-0.5900:wick=0.8100; 2026-11-20:GREEN:body=+0.5700:wick=0.3650; 2026-11-23:GREEN:body=+0.9600:wick=0.6300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.29 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.74 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2336.79 | **NEUTRAL** |
| `B04_income` | 211.85 | **GOOD** |
| `B05_profit_margin` | 9.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 48.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=48.33 vs prior_export=48.33 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -4.62 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.62 vs prior=-4.62 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.5 | **GOOD** |
| `B13_short_float` | 1.93 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.29 (this export) | prior_export=18.29 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPK  ·  score **+15**  ·  Packaging & Containers
price=10.859999656677246  pair=`2026-11-20→2026-11-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.81 on 2026-11-23; prev RSI=50.22 on 2026-11-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.22@2026-11-20 → 58.81@2026-11-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.22@2026-11-20 → 58.81@2026-11-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.22@2026-11-20 → 58.81@2026-11-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_body_sum/RED_body_sum=2.333 (G=0.5600 R=0.2400); 2026-11-20:RED:O=10.4000,C=10.1600,body=-0.2400,vol=5311700.0; 2026-11-23:GREEN:O=10.3000,C=10.8600,body=+0.5600,vol=7905800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-20 + 2026-11-23; ratio=GREEN_vol/RED_vol=1.488 (Gvol=7905800 Rvol=5311700); 2026-11-20:RED:O=10.4000,C=10.1600,body=-0.2400,vol=5311700.0; 2026-11-23:GREEN:O=10.3000,C=10.8600,body=+0.5600,vol=7905800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.041 on 2026-11-23: today_vol=7905800 / avg20=7596757 (avg window 2026-10-26→2026-11-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.225 on 2026-11-23 (price=10.8600, mid=10.4100, upper=12.4083, lower=8.4117; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-23: price=10.8600 vs SMA50=9.5196 dist=+14.08% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=10.41_50=9.52_80=10.13 on 2026-11-23: SMA20=10.4100 SMA50=9.5196 SMA80=10.1254 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-26→2026-11-23 (63 bars); S1[2026-08-26→2026-09-24] low=2026-09-24@9.0800; S2[2026-09-25→2026-10-23] low=2026-10-12@8.1550; S3[2026-10-26→2026-11-23] low=2026-10-28@8.2450 | lows=[9.079999923706055, 8.154999732971191, 8.244999885559082] span=11.34% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: GREEN body_frac=0.8615381455278517 wick_frac=0.1384618544721483 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-20+2026-11-23: RED body_frac=0.4897956800479562 wick_frac=0.5102043199520437 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3333333333333335 need>1.4; red_wick_gt_green=True 5d trail=2026-11-17:RED:body=-0.3200:wick=0.2400; 2026-11-18:GREEN:body=+0.1900:wick=0.1600; 2026-11-19:RED:body=-0.0700:wick=0.2500; 2026-11-20:RED:body=-0.2400:wick=0.2500; 2026-11-23:GREEN:body=+0.5600:wick=0.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.6 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.94 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8639.0 | **NEUTRAL** |
| `B04_income` | 74.0 | **GOOD** |
| `B05_profit_margin` | 0.86 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.1 vs prior_export=12.1 on finviz_2026-11-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.85 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.83 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.6 (this export) | prior_export=3.6 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.94 (this export) | prior_export=0.94 (finviz_2026-11-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-23_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.