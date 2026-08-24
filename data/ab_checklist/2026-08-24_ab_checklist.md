# A+B1 Feature Checklist — 2026-08-24

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,700** names
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
| 1 | CBRL | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Restaurants |
| 2 | RELY | +16 | 18 | 2 | 2026-08-21→2026-08-24 | Software - Infrastructure |
| 3 | DINO | +16 | 17 | 1 | 2026-08-21→2026-08-24 | Oil & Gas Refining & Marketing |
| 4 | BLMN | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Restaurants |
| 5 | SGHC | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Gambling |
| 6 | WPM | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Gold |
| 7 | WWW | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Footwear & Accessories |
| 8 | DE | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Farm & Heavy Construction Machinery |
| 9 | WAT | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Diagnostics & Research |
| 10 | ZBH | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Medical Devices |
| 11 | SKWD | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Insurance - Property & Casualty |
| 12 | NOMD | +15 | 17 | 2 | 2026-08-21→2026-08-24 | Packaged Foods |
| 13 | DCTH | +15 | 16 | 1 | 2026-08-21→2026-08-24 | Medical Devices |
| 14 | MSI | +14 | 16 | 2 | 2026-08-21→2026-08-24 | Communication Equipment |
| 15 | PLTR | +14 | 16 | 2 | 2026-08-21→2026-08-24 | Software - Infrastructure |

## Full checklist — top 15

### CBRL  ·  score **+16**  ·  Restaurants
price=57.560001373291016  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.21 on 2026-08-24; prev RSI=57.52 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.52@2026-08-21 → 57.21@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.52@2026-08-21 → 57.21@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.52@2026-08-21 → 57.21@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=57.504 (G=2.3000 R=0.0400); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:RED:O=57.6000,C=57.5600,body=-0.0400,vol=181562.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=3.011 (Gvol=546700 Rvol=181562); 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0; 2026-08-24:RED:O=57.6000,C=57.5600,body=-0.0400,vol=181562.0 | **GOOD** |
| `A07_rvol` | RVOL=0.226 on 2026-08-24: today_vol=181562 / avg20=802260 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.159 on 2026-08-24 (price=57.5600, mid=57.0180, upper=60.4306, lower=53.6054; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=57.5600 vs SMA50=52.2612 dist=+10.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=57.0180 SMA50=52.2612 SMA80=44.4624 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@28.0918; S2[2026-06-22→2026-07-23] low=2026-06-23@45.2175; S3[2026-07-24→2026-08-24] low=2026-07-27@50.2000 | lows=[28.091751487486153, 45.21746996696082, 50.20000076293945] span=78.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.8745267884856941 wick_frac=0.12547321151430585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.027025076487281332 wick_frac=0.9729749235127186 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=57.50424415832141 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.7800:wick=1.3000; 2026-08-19:RED:body=-1.8300:wick=1.7800; 2026-08-20:RED:body=-0.4300:wick=1.4000; 2026-08-21:GREEN:body=+2.3000:wick=0.3300; 2026-08-24:RED:body=-0.0400:wick=1.4400 | **GOOD** |
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

### RELY  ·  score **+16**  ·  Software - Infrastructure
price=26.510000228881836  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.08 on 2026-08-24; prev RSI=62.13 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.13@2026-08-21 → 61.08@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.13@2026-08-21 → 61.08@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.13@2026-08-21 → 61.08@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=27.286 (G=1.9100 R=0.0700); 2026-08-21:GREEN:O=24.7600,C=26.6700,body=+1.9100,vol=2748900.0; 2026-08-24:RED:O=26.5800,C=26.5100,body=-0.0700,vol=578348.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=4.753 (Gvol=2748900 Rvol=578348); 2026-08-21:GREEN:O=24.7600,C=26.6700,body=+1.9100,vol=2748900.0; 2026-08-24:RED:O=26.5800,C=26.5100,body=-0.0700,vol=578348.0 | **GOOD** |
| `A07_rvol` | RVOL=0.183 on 2026-08-24: today_vol=578348 / avg20=3164740 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.737 on 2026-08-24 (price=26.5100, mid=24.6675, upper=27.1690, lower=22.1660; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=26.5100 vs SMA50=23.3554 dist=+13.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=24.6675 SMA50=23.3554 SMA80=22.6504 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-24 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-11@17.7000; S2[2026-06-24→2026-07-23] low=2026-06-24@20.3000; S3[2026-07-24→2026-08-24] low=2026-07-28@22.3200 | lows=[17.700000762939453, 20.299999237060547, 22.31999969482422] span=26.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.8470068994450501 wick_frac=0.15299310055494986 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.1368254264143909 wick_frac=0.8631745735856091 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=27.2858310626703 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.6500:wick=0.6250; 2026-08-19:RED:body=-0.6300:wick=0.2250; 2026-08-20:RED:body=-0.1900:wick=0.7330; 2026-08-21:GREEN:body=+1.9100:wick=0.3450; 2026-08-24:RED:body=-0.0700:wick=0.4416 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=651.21 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.83 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1809.63 | **NEUTRAL** |
| `B04_income` | 305.01 | **GOOD** |
| `B05_profit_margin` | 16.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 32.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=32.05 vs prior_export=32.05 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.15 | **GOOD** |
| `B10_insider_transactions` | -42.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.00999999999999801 (now=-42.6 vs prior=-42.61 on finviz_2026-08-21) | **GOOD** |
| `B12_institutional_transactions` | 3.73 | **GOOD** |
| `B13_short_float` | 7.27 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=651.21 (this export) | prior_export=651.21 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.83 (this export) | prior_export=1.83 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+16**  ·  Oil & Gas Refining & Marketing
price=94.57499694824219  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.32 on 2026-08-24; prev RSI=67.90 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.90@2026-08-21 → 61.32@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.90@2026-08-21 → 61.32@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.90@2026-08-21 → 61.32@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.266 (G=3.3200 R=1.4650); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=94.5750,body=-1.4650,vol=478182.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=6.068 (Gvol=2901400 Rvol=478182); 2026-08-21:GREEN:O=94.0000,C=97.3200,body=+3.3200,vol=2901400.0; 2026-08-24:RED:O=96.0400,C=94.5750,body=-1.4650,vol=478182.0 | **GOOD** |
| `A07_rvol` | RVOL=0.162 on 2026-08-24: today_vol=478182 / avg20=2947205 (avg window 2026-07-27→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.494 on 2026-08-24 (price=94.5750, mid=89.8105, upper=99.4477, lower=80.1732; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=94.5750 vs SMA50=81.6853 dist=+15.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=89.8105 SMA50=81.6853 SMA80=77.3411 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-22@63.4268; S2[2026-06-25→2026-07-24] low=2026-06-25@64.8977; S3[2026-07-27→2026-08-24] low=2026-08-07@80.0041 | lows=[63.42683480953448, 64.8977242324992, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7562642725310477 wick_frac=0.24373572746895236 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.35472274614050286 wick_frac=0.6452772538594971 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.2662052587998187 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.2900:wick=1.7800; 2026-08-19:RED:body=-1.6300:wick=1.0200; 2026-08-20:RED:body=-2.4600:wick=1.7500; 2026-08-21:GREEN:body=+3.3200:wick=1.0700; 2026-08-24:RED:body=-1.4650:wick=2.6650 | **GOOD** |
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

### BLMN  ·  score **+15**  ·  Restaurants
price=11.600000381469727  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.45 on 2026-08-24; prev RSI=67.89 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.89@2026-08-21 → 67.45@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.89@2026-08-21 → 67.45@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.89@2026-08-21 → 67.45@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=34.000 (G=1.0200 R=0.0300); 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0; 2026-08-24:RED:O=11.6300,C=11.6000,body=-0.0300,vol=476760.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=3.879 (Gvol=1849400 Rvol=476760); 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0; 2026-08-24:RED:O=11.6300,C=11.6000,body=-0.0300,vol=476760.0 | **GOOD** |
| `A07_rvol` | RVOL=0.214 on 2026-08-24: today_vol=476760 / avg20=2230645 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.563 on 2026-08-24 (price=11.6000, mid=10.3495, upper=12.5708, lower=8.1282; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=11.6000 vs SMA50=9.1158 dist=+27.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=10.3495 SMA50=9.1158 SMA80=8.5076 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-08@7.0300; S2[2026-06-23→2026-07-23] low=2026-07-08@7.6600; S3[2026-07-24→2026-08-24] low=2026-07-24@7.9100 | lows=[7.03000020980835, 7.659999847412109, 7.909999847412109] span=12.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.9272736257762696 wick_frac=0.07272637422373038 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.10344736868067363 wick_frac=0.8965526313193264 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=34.00031789426836 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0100:wick=0.4200; 2026-08-19:GREEN:body=+0.0100:wick=0.5000; 2026-08-20:GREEN:body=+0.0300:wick=0.3700; 2026-08-21:GREEN:body=+1.0200:wick=0.0800; 2026-08-24:RED:body=-0.0300:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=35.18 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3979.52 | **NEUTRAL** |
| `B04_income` | 29.38 | **GOOD** |
| `B05_profit_margin` | 0.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.62 | **GOOD** |
| `B13_short_float` | 10.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SGHC  ·  score **+15**  ·  Gambling
price=13.920000076293945  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.78 on 2026-08-24; prev RSI=46.31 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.31@2026-08-21 → 54.78@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.31@2026-08-21 → 54.78@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.31@2026-08-21 → 54.78@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6800 R=0.0000); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9200,body=+0.5900,vol=1595391.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3649391 Rvol=0); 2026-08-21:GREEN:O=13.2200,C=13.3100,body=+0.0900,vol=2054000.0; 2026-08-24:GREEN:O=13.3300,C=13.9200,body=+0.5900,vol=1595391.0 | **GOOD** |
| `A07_rvol` | RVOL=0.729 on 2026-08-24: today_vol=1595391 / avg20=2188330 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.306 on 2026-08-24 (price=13.9200, mid=13.4960, upper=14.8806, lower=12.1114; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=13.9200 vs SMA50=13.9189 dist=+0.01% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=13.50_50=13.92_80=13.58 on 2026-08-24: SMA20=13.4960 SMA50=13.9189 SMA80=13.5765 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-03@12.1657; S2[2026-06-23→2026-07-23] low=2026-06-30@13.2350; S3[2026-07-24→2026-08-24] low=2026-08-12@12.7410 | lows=[12.165696572387432, 13.234999656677246, 12.741000175476074] span=8.79% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.7229939176917602 wick_frac=0.27700608230823975 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.2100:wick=0.0650; 2026-08-19:GREEN:body=+0.2800:wick=0.1500; 2026-08-20:RED:body=-0.0200:wick=0.4170; 2026-08-21:GREEN:body=+0.0900:wick=0.1200; 2026-08-24:GREEN:body=+0.5900:wick=-0.0101 | **GOOD** |
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

### WPM  ·  score **+15**  ·  Gold
price=159.0  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=80.00 on 2026-08-24; prev RSI=79.50 on 2026-08-21 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.50@2026-08-21 → 80.00@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.50@2026-08-21 → 80.00@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.50@2026-08-21 → 80.00@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=6.160 (G=3.0800 R=0.5000); 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0; 2026-08-24:RED:O=159.5000,C=159.0000,body=-0.5000,vol=1190027.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=2.772 (Gvol=3299300 Rvol=1190027); 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0; 2026-08-24:RED:O=159.5000,C=159.0000,body=-0.5000,vol=1190027.0 | **GOOD** |
| `A07_rvol` | RVOL=0.525 on 2026-08-24: today_vol=1190027 / avg20=2268755 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.931 on 2026-08-24 (price=159.0000, mid=130.2994, upper=161.1254, lower=99.4735; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-24: price=159.0000 vs SMA50=119.8858 dist=+32.63% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=130.30_50=119.89_80=123.26 on 2026-08-24: SMA20=130.2994 SMA50=119.8858 SMA80=123.2569 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-10@106.8200; S2[2026-06-23→2026-07-23] low=2026-07-17@101.5900; S3[2026-07-24→2026-08-24] low=2026-07-29@106.3000 | lows=[106.81999969482422, 101.58999633789062, 106.30000305175781] span=5.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.655319964028193 wick_frac=0.3446800359718069 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.19383614315291334 wick_frac=0.8061638568470867 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.160003662109375 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-1.3382:wick=2.1871; 2026-08-19:GREEN:body=+7.8496:wick=1.3283; 2026-08-20:GREEN:body=+5.7100:wick=0.4500; 2026-08-21:GREEN:body=+3.0800:wick=1.6200; 2026-08-24:RED:body=-0.5000:wick=2.0795 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.52 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3171.64 | **NEUTRAL** |
| `B04_income` | 2050.75 | **GOOD** |
| `B05_profit_margin` | 64.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 170.81 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.09999999999999432 (now=170.81 vs prior_export=170.71 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 1.06 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.97 | **GOOD** |
| `B13_short_float` | 0.93 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WWW  ·  score **+15**  ·  Footwear & Accessories
price=20.924999237060547  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.32 on 2026-08-24; prev RSI=60.59 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.59@2026-08-21 → 60.32@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.59@2026-08-21 → 60.32@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.59@2026-08-21 → 60.32@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.327 (G=0.6400 R=0.2750); 2026-08-21:GREEN:O=20.3200,C=20.9600,body=+0.6400,vol=662700.0; 2026-08-24:RED:O=21.2000,C=20.9250,body=-0.2750,vol=156486.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=4.235 (Gvol=662700 Rvol=156486); 2026-08-21:GREEN:O=20.3200,C=20.9600,body=+0.6400,vol=662700.0; 2026-08-24:RED:O=21.2000,C=20.9250,body=-0.2750,vol=156486.0 | **GOOD** |
| `A07_rvol` | RVOL=0.176 on 2026-08-24: today_vol=156486 / avg20=888850 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.611 on 2026-08-24 (price=20.9250, mid=19.8068, upper=21.6377, lower=17.9758; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=20.9250 vs SMA50=18.4314 dist=+13.53% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=19.8068 SMA50=18.4314 SMA80=17.6713 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-24 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@14.8496; S2[2026-06-23→2026-07-23] low=2026-06-30@15.9628; S3[2026-07-24→2026-08-24] low=2026-08-12@17.6900 | lows=[14.849618413984873, 15.962842417770247, 17.690000534057617] span=19.13% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.8421042064553693 wick_frac=0.15789579354463068 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.423079518878361 wick_frac=0.5769204811216391 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.327257594673325 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+0.0400:wick=0.5800; 2026-08-19:GREEN:body=+0.7700:wick=0.0000; 2026-08-20:GREEN:body=+0.3000:wick=0.4000; 2026-08-21:GREEN:body=+0.6400:wick=0.1200; 2026-08-24:RED:body=-0.2750:wick=0.3750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.99 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.13 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1951.8 | **NEUTRAL** |
| `B04_income` | 105.5 | **GOOD** |
| `B05_profit_margin` | 5.41 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.3 vs prior_export=24.3 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.88 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.88 vs prior=-1.88 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.35 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.99 (this export) | prior_export=4.99 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.13 (this export) | prior_export=1.13 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DE  ·  score **+15**  ·  Farm & Heavy Construction Machinery
price=652.5800170898438  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.32 on 2026-08-24; prev RSI=62.14 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.14@2026-08-21 → 63.32@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.14@2026-08-21 → 63.32@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.14@2026-08-21 → 63.32@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=52.635 (G=24.2100 R=0.4600); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=652.5800,body=-0.4600,vol=467086.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=5.122 (Gvol=2392500 Rvol=467086); 2026-08-21:GREEN:O=623.2600,C=647.4700,body=+24.2100,vol=2392500.0; 2026-08-24:RED:O=653.0400,C=652.5800,body=-0.4600,vol=467086.0 | **GOOD** |
| `A07_rvol` | RVOL=0.389 on 2026-08-24: today_vol=467086 / avg20=1201820 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=1.067 on 2026-08-24 (price=652.5800, mid=615.1935, upper=650.2326, lower=580.1544; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-24: price=652.5800 vs SMA50=604.8965 dist=+7.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=615.1935 SMA50=604.8965 SMA80=589.9733 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@513.8182; S2[2026-06-22→2026-07-23] low=2026-07-15@576.4500; S3[2026-07-24→2026-08-24] low=2026-08-19@579.3100 | lows=[513.8182476448627, 576.4500122070312, 579.3099975585938] span=12.75% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5219913408519654 wick_frac=0.4780086591480346 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.03080583250554922 wick_frac=0.9691941674944508 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=52.63481953290871 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-15.2100:wick=0.8300; 2026-08-19:RED:body=-8.9500:wick=7.8000; 2026-08-20:GREEN:body=+9.8200:wick=42.7000; 2026-08-21:GREEN:body=+24.2100:wick=22.1700; 2026-08-24:RED:body=-0.4600:wick=14.4710 | **NEUTRAL** |
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

### WAT  ·  score **+15**  ·  Diagnostics & Research
price=405.5249938964844  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.57 on 2026-08-24; prev RSI=59.04 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.04@2026-08-21 → 55.57@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.04@2026-08-21 → 55.57@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.04@2026-08-21 → 55.57@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=10.875 (G=10.0600 R=0.9250); 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=816900.0; 2026-08-24:RED:O=406.4500,C=405.5250,body=-0.9250,vol=128982.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=6.333 (Gvol=816900 Rvol=128982); 2026-08-21:GREEN:O=400.6400,C=410.7000,body=+10.0600,vol=816900.0; 2026-08-24:RED:O=406.4500,C=405.5250,body=-0.9250,vol=128982.0 | **GOOD** |
| `A07_rvol` | RVOL=0.144 on 2026-08-24: today_vol=128982 / avg20=894830 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.178 on 2026-08-24 (price=405.5250, mid=400.3302, upper=429.4600, lower=371.2005; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=405.5250 vs SMA50=381.7209 dist=+6.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=400.3302 SMA50=381.7209 SMA80=366.8586 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-20@323.8500; S2[2026-06-22→2026-07-23] low=2026-06-22@353.5300; S3[2026-07-24→2026-08-24] low=2026-07-27@367.6700 | lows=[323.8500061035156, 353.5299987792969, 367.6700134277344] span=13.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5935097530148464 wick_frac=0.4064902469851535 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.10583745356015531 wick_frac=0.8941625464398447 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.875457754610537 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-5.9900:wick=2.7200; 2026-08-19:GREEN:body=+10.1100:wick=5.0700; 2026-08-20:RED:body=-5.3100:wick=13.0000; 2026-08-21:GREEN:body=+10.0600:wick=6.8900; 2026-08-24:RED:body=-0.9250:wick=7.8150 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3770.58 | **NEUTRAL** |
| `B04_income` | 449.25 | **GOOD** |
| `B05_profit_margin` | 11.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 439.95 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=439.95 vs prior_export=439.95 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.96 | **GOOD** |
| `B10_insider_transactions` | -1.52 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.52 vs prior=-1.52 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.12 | **GOOD** |
| `B13_short_float` | 4.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZBH  ·  score **+15**  ·  Medical Devices
price=101.03089904785156  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.74 on 2026-08-24; prev RSI=61.65 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.65@2026-08-21 → 61.74@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.65@2026-08-21 → 61.74@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.65@2026-08-21 → 61.74@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=7.990 (G=2.3100 R=0.2891); 2026-08-21:GREEN:O=98.6700,C=100.9800,body=+2.3100,vol=1159800.0; 2026-08-24:RED:O=101.3200,C=101.0309,body=-0.2891,vol=307880.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=3.767 (Gvol=1159800 Rvol=307880); 2026-08-21:GREEN:O=98.6700,C=100.9800,body=+2.3100,vol=1159800.0; 2026-08-24:RED:O=101.3200,C=101.0309,body=-0.2891,vol=307880.0 | **GOOD** |
| `A07_rvol` | RVOL=0.125 on 2026-08-24: today_vol=307880 / avg20=2463765 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.708 on 2026-08-24 (price=101.0309, mid=97.8405, upper=102.3437, lower=93.3374; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=101.0309 vs SMA50=92.7371 dist=+8.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=97.8405 SMA50=92.7371 SMA80=89.3711 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-29@81.3638; S2[2026-06-22→2026-07-23] low=2026-06-30@83.7200; S3[2026-07-24→2026-08-24] low=2026-07-24@90.4300 | lows=[81.36379644583408, 83.72000122070312, 90.43000030517578] span=11.14% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.9314524440178552 wick_frac=0.06854755598214478 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.21414765919931278 wick_frac=0.7858523408006872 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.99031483387433 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:RED:body=-0.4300:wick=1.6800; 2026-08-19:GREEN:body=+2.0200:wick=1.8400; 2026-08-20:RED:body=-1.0400:wick=1.6200; 2026-08-21:GREEN:body=+2.3100:wick=0.1700; 2026-08-24:RED:body=-0.2891:wick=1.0609 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.97 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.97 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8508.9 | **NEUTRAL** |
| `B04_income` | 806.6 | **GOOD** |
| `B05_profit_margin` | 9.48 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 106.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=106.45 vs prior_export=106.45 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | -1.79 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.79 vs prior=-1.79 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.03 | **GOOD** |
| `B13_short_float` | 5.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.97 (this export) | prior_export=2.97 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.97 (this export) | prior_export=1.97 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKWD  ·  score **+15**  ·  Insurance - Property & Casualty
price=58.26499938964844  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.15 on 2026-08-24; prev RSI=43.96 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.96@2026-08-21 → 48.15@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.96@2026-08-21 → 48.15@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.96@2026-08-21 → 48.15@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2150 R=0.0000); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=58.2650,body=+1.3650,vol=135098.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=658298 Rvol=0); 2026-08-21:GREEN:O=56.0400,C=56.8900,body=+0.8500,vol=523200.0; 2026-08-24:GREEN:O=56.9000,C=58.2650,body=+1.3650,vol=135098.0 | **GOOD** |
| `A07_rvol` | RVOL=0.265 on 2026-08-24: today_vol=135098 / avg20=509590 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.402 on 2026-08-24 (price=58.2650, mid=60.3692, upper=65.6011, lower=55.1374; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=58.2650 vs SMA50=57.8189 dist=+0.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=60.3692 SMA50=57.8189 SMA80=53.1784 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-03@42.5000; S2[2026-06-22→2026-07-23] low=2026-06-22@50.7550; S3[2026-07-24→2026-08-24] low=2026-08-20@54.0200 | lows=[42.5, 50.755001068115234, 54.02000045776367] span=27.11% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6011881040823952 wick_frac=0.3988118959176048 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:GREEN:body=+0.0400:wick=1.5800; 2026-08-19:RED:body=-2.0400:wick=0.6900; 2026-08-20:GREEN:body=+0.9500:wick=1.4900; 2026-08-21:GREEN:body=+0.8500:wick=0.7850; 2026-08-24:GREEN:body=+1.3650:wick=0.6350 | **NEUTRAL** |
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

### NOMD  ·  score **+15**  ·  Packaged Foods
price=12.520000457763672  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.92 on 2026-08-24; prev RSI=54.45 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.45@2026-08-21 → 62.92@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.45@2026-08-21 → 62.92@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.45@2026-08-21 → 62.92@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4400 R=0.0000); 2026-08-21:GREEN:O=11.7100,C=11.8100,body=+0.1000,vol=1569900.0; 2026-08-24:GREEN:O=12.1800,C=12.5200,body=+0.3400,vol=1286987.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2856887 Rvol=0); 2026-08-21:GREEN:O=11.7100,C=11.8100,body=+0.1000,vol=1569900.0; 2026-08-24:GREEN:O=12.1800,C=12.5200,body=+0.3400,vol=1286987.0 | **GOOD** |
| `A07_rvol` | RVOL=0.669 on 2026-08-24: today_vol=1286987 / avg20=1922515 (avg window 2026-07-24→2026-08-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.067 on 2026-08-24 (price=12.5200, mid=11.7932, upper=12.4746, lower=11.1118; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-24: price=12.5200 vs SMA50=11.2062 dist=+11.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=11.7932 SMA50=11.2062 SMA80=10.6208 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-04@9.5634; S2[2026-06-22→2026-07-23] low=2026-06-22@9.6866; S3[2026-07-24→2026-08-24] low=2026-08-13@10.9300 | lows=[9.563381707387485, 9.686557825639353, 10.930000305175781] span=14.29% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6139015963215539 wick_frac=0.386098403678446 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0550:wick=0.2150; 2026-08-19:GREEN:body=+0.3500:wick=0.2450; 2026-08-20:RED:body=-0.2300:wick=0.1000; 2026-08-21:GREEN:body=+0.1000:wick=0.2500; 2026-08-24:GREEN:body=+0.3400:wick=0.0209 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.0 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.04 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3457.3 | **NEUTRAL** |
| `B04_income` | 145.06 | **GOOD** |
| `B05_profit_margin` | 4.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.17999999999999972 (now=13.76 vs prior_export=13.58 on finviz_2026-08-21) | **GOOD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 3.6 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.10000000000000009 (now=3.6 vs prior=3.5 on finviz_2026-08-21) | **GOOD** |
| `B12_institutional_transactions` | -1.38 | **BAD** |
| `B13_short_float` | 2.68 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.04 (this export) | prior_export=0.04 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DCTH  ·  score **+15**  ·  Medical Devices
price=16.989999771118164  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.37 on 2026-08-24; prev RSI=68.07 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.07@2026-08-21 → 66.37@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.07@2026-08-21 → 66.37@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.07@2026-08-21 → 66.37@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=2.500 (G=0.3000 R=0.1200); 2026-08-21:GREEN:O=16.8400,C=17.1400,body=+0.3000,vol=509100.0; 2026-08-24:RED:O=17.1100,C=16.9900,body=-0.1200,vol=80308.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=6.339 (Gvol=509100 Rvol=80308); 2026-08-21:GREEN:O=16.8400,C=17.1400,body=+0.3000,vol=509100.0; 2026-08-24:RED:O=17.1100,C=16.9900,body=-0.1200,vol=80308.0 | **GOOD** |
| `A07_rvol` | RVOL=0.107 on 2026-08-24: today_vol=80308 / avg20=748475 (avg window 2026-07-27→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.398 on 2026-08-24 (price=16.9900, mid=15.1905, upper=19.7126, lower=10.6684; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=16.9900 vs SMA50=13.6117 dist=+24.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=15.1905 SMA50=13.6117 SMA80=12.6362 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-24 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@10.2000; S2[2026-06-24→2026-07-24] low=2026-07-24@12.1230; S3[2026-07-27→2026-08-24] low=2026-07-29@11.5000 | lows=[10.199999809265137, 12.123000144958496, 11.5] span=18.85% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5454539149251971 wick_frac=0.4545460850748029 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.3934449808639968 wick_frac=0.6065550191360032 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4999761583088294 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.0900:wick=0.4600; 2026-08-19:GREEN:body=+0.0100:wick=0.4380; 2026-08-20:RED:body=-0.3000:wick=0.2400; 2026-08-21:GREEN:body=+0.3000:wick=0.2500; 2026-08-24:RED:body=-0.1200:wick=0.1850 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=178.56 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.97 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 95.42 | **NEUTRAL** |
| `B04_income` | 0.53 | **GOOD** |
| `B05_profit_margin` | 0.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.5 vs prior_export=24.5 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.21 vs prior=0.21 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.84 | **GOOD** |
| `B13_short_float` | 7.9 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=178.56 (this export) | prior_export=178.56 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.97 (this export) | prior_export=10.97 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSI  ·  score **+14**  ·  Communication Equipment
price=482.94000244140625  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.70 on 2026-08-24; prev RSI=65.83 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.83@2026-08-21 → 66.70@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.83@2026-08-21 → 66.70@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.83@2026-08-21 → 66.70@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.7600 R=0.0000); 2026-08-21:GREEN:O=471.5500,C=480.4700,body=+8.9200,vol=1298100.0; 2026-08-24:GREEN:O=481.1000,C=482.9400,body=+1.8400,vol=227203.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1525303 Rvol=0); 2026-08-21:GREEN:O=471.5500,C=480.4700,body=+8.9200,vol=1298100.0; 2026-08-24:GREEN:O=481.1000,C=482.9400,body=+1.8400,vol=227203.0 | **GOOD** |
| `A07_rvol` | RVOL=0.219 on 2026-08-24: today_vol=227203 / avg20=1038110 (avg window 2026-07-24→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.666 on 2026-08-24 (price=482.9400, mid=458.1760, upper=495.3773, lower=420.9747; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=482.9400 vs SMA50=429.5609 dist=+12.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=458.1760 SMA50=429.5609 SMA80=422.7320 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-24 (63 bars); S1[2026-05-20→2026-06-18] low=2026-05-21@389.6322; S2[2026-06-22→2026-07-23] low=2026-06-24@389.0200; S3[2026-07-24→2026-08-24] low=2026-07-24@404.2400 | lows=[389.6322122089833, 389.0199890136719, 404.239990234375] span=3.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.5208802718351041 wick_frac=0.4791197281648958 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-18:GREEN:body=+10.6100:wick=3.2000; 2026-08-19:GREEN:body=+11.1000:wick=4.1400; 2026-08-20:RED:body=-4.2900:wick=9.4900; 2026-08-21:GREEN:body=+8.9200:wick=3.2400; 2026-08-24:GREEN:body=+1.8400:wick=4.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.51 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.37 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 12236.0 | **NEUTRAL** |
| `B04_income` | 2134.0 | **GOOD** |
| `B05_profit_margin` | 17.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 526.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=526.0 vs prior_export=526.0 on finviz_2026-08-21) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -21.96 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.96 vs prior=-21.96 on finviz_2026-08-21) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 2.35 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.51 (this export) | prior_export=14.51 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.37 (this export) | prior_export=4.37 (finviz_2026-08-21) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLTR  ·  score **+14**  ·  Software - Infrastructure
price=177.30999755859375  pair=`2026-08-21→2026-08-24`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.50 on 2026-08-24; prev RSI=69.41 on 2026-08-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.41@2026-08-21 → 66.50@2026-08-24 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.41@2026-08-21 → 66.50@2026-08-24 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.41@2026-08-21 → 66.50@2026-08-24 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_body_sum/RED_body_sum=42.572 (G=5.9600 R=0.1400); 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0; 2026-08-24:RED:O=177.4500,C=177.3100,body=-0.1400,vol=12481940.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-21 + 2026-08-24; ratio=GREEN_vol/RED_vol=3.284 (Gvol=40986600 Rvol=12481940); 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0; 2026-08-24:RED:O=177.4500,C=177.3100,body=-0.1400,vol=12481940.0 | **GOOD** |
| `A07_rvol` | RVOL=0.262 on 2026-08-24: today_vol=12481940 / avg20=47632880 (avg window 2026-07-27→2026-08-21, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.399 on 2026-08-24 (price=177.3100, mid=159.5635, upper=204.0099, lower=115.1171; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-24: price=177.3100 vs SMA50=139.8564 dist=+26.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-24: SMA20=159.5635 SMA50=139.8564 SMA80=139.3769 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-24 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-24@112.2500; S2[2026-06-25→2026-07-24] low=2026-06-25@106.3700; S3[2026-07-27→2026-08-24] low=2026-07-28@117.8900 | lows=[112.25, 106.37000274658203, 117.88999938964844] span=10.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: GREEN body_frac=0.6026296341438955 wick_frac=0.3973703658561045 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-21+2026-08-24: RED body_frac=0.021891993137725094 wick_frac=0.978108006862275 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=42.5716621253406 need>1.4; red_wick_gt_green=False 5d trail=2026-08-18:RED:body=-0.4300:wick=3.9400; 2026-08-19:GREEN:body=+2.5800:wick=4.4690; 2026-08-20:RED:body=-1.9200:wick=2.5100; 2026-08-21:GREEN:body=+5.9600:wick=3.9300; 2026-08-24:RED:body=-0.1400:wick=6.2550 | **NEUTRAL** |
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