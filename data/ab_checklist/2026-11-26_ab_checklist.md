# A+B1 Feature Checklist — 2026-11-26

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,607** names
- Export: `finviz_2026-11-26.csv` · prior export for Δ: `2026-11-25`
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
| 1 | GLOB | +17 | 18 | 1 | 2026-11-24→2026-11-25 | Information Technology Services |
| 2 | MICC | +17 | 17 | 0 | 2026-11-24→2026-11-25 | Packaged Foods |
| 3 | TRIN | +17 | 17 | 0 | 2026-11-24→2026-11-25 | Asset Management |
| 4 | CNM | +17 | 17 | 0 | 2026-11-24→2026-11-25 | Industrial Distribution |
| 5 | UHS | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Medical Care Facilities |
| 6 | VFC | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Apparel Manufacturing |
| 7 | HII | +16 | 17 | 1 | 2026-11-24→2026-11-25 | Aerospace & Defense |
| 8 | EMBC | +16 | 18 | 2 | 2026-11-24→2026-11-25 | Medical Instruments & Supplies |
| 9 | V | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Credit Services |
| 10 | PRCH | +16 | 18 | 2 | 2026-11-24→2026-11-25 | Insurance - Property & Casualty |
| 11 | GEHC | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Medical Devices |
| 12 | RSG | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Waste Management |
| 13 | FLOC | +16 | 16 | 0 | 2026-11-24→2026-11-25 | Oil & Gas Equipment & Services |
| 14 | OCFC | +15 | 16 | 1 | 2026-11-24→2026-11-25 | Banks - Regional |
| 15 | HGTY | +15 | 16 | 1 | 2026-11-24→2026-11-25 | Insurance - Property & Casualty |

## Full checklist — top 15

### GLOB  ·  score **+17**  ·  Information Technology Services
price=50.189998626708984  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.03 on 2026-11-25; prev RSI=63.80 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.80@2026-11-24 → 64.03@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.80@2026-11-24 → 64.03@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.80@2026-11-24 → 64.03@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=95.998 (G=3.8400 R=0.0400); 2026-11-24:GREEN:O=46.2100,C=50.0500,body=+3.8400,vol=2559300.0; 2026-11-25:RED:O=50.2300,C=50.1900,body=-0.0400,vol=2027800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=1.262 (Gvol=2559300 Rvol=2027800); 2026-11-24:GREEN:O=46.2100,C=50.0500,body=+3.8400,vol=2559300.0; 2026-11-25:RED:O=50.2300,C=50.1900,body=-0.0400,vol=2027800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.520 on 2026-11-25: today_vol=2027800 / avg20=1334158 (avg window 2026-10-28→2026-11-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.748 on 2026-11-25 (price=50.1900, mid=45.9920, upper=51.6066, lower=40.3774; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=50.1900 vs SMA50=41.1756 dist=+21.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=45.9920 SMA50=41.1756 SMA80=40.2919 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-28@33.1700; S2[2026-09-29→2026-10-27] low=2026-09-29@33.0000; S3[2026-10-28→2026-11-25] low=2026-11-20@39.1200 | lows=[33.16999816894531, 33.0, 39.119998931884766] span=18.55% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.6857144998044967 wick_frac=0.3142855001955033 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.022472439864899618 wick_frac=0.9775275601351003 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=95.99780659927522 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-2.4300:wick=0.4400; 2026-11-20:RED:body=-1.8150:wick=3.9750; 2026-11-23:GREEN:body=+3.8500:wick=0.3800; 2026-11-24:GREEN:body=+3.8400:wick=1.7600; 2026-11-25:RED:body=-0.0400:wick=1.7400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.07 (current export asof; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.06 (current export; earnings_date=11/19/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2445.11 | **NEUTRAL** |
| `B04_income` | 94.23 | **GOOD** |
| `B05_profit_margin` | 3.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.0 vs prior_export=53.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | 0.94 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.94 vs prior=0.94 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | -11.67 | **BAD** |
| `B13_short_float` | 23.0 | **GOOD** |
| `B14_earnings_date` | 11/19/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.07 (this export) | prior_export=0.07 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.06 (this export) | prior_export=0.06 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MICC  ·  score **+17**  ·  Packaged Foods
price=19.489999771118164  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.68 on 2026-11-25; prev RSI=44.65 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.65@2026-11-24 → 52.68@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.65@2026-11-24 → 52.68@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.65@2026-11-24 → 52.68@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=16.000 (G=0.3200 R=0.0200); 2026-11-24:RED:O=18.9400,C=18.9200,body=-0.0200,vol=1039700.0; 2026-11-25:GREEN:O=19.1700,C=19.4900,body=+0.3200,vol=1167400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=1.123 (Gvol=1167400 Rvol=1039700); 2026-11-24:RED:O=18.9400,C=18.9200,body=-0.0200,vol=1039700.0; 2026-11-25:GREEN:O=19.1700,C=19.4900,body=+0.3200,vol=1167400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.232 on 2026-11-25: today_vol=1167400 / avg20=947572 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.085 on 2026-11-25 (price=19.4900, mid=19.5605, upper=20.3918, lower=18.7292; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=19.4900 vs SMA50=18.8972 dist=+3.14% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=19.56_50=18.90_80=19.16 on 2026-11-25: SMA20=19.5605 SMA50=18.8972 SMA80=19.1562 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-28@18.7310; S2[2026-09-29→2026-10-27] low=2026-10-07@16.8600; S3[2026-10-28→2026-11-25] low=2026-11-24@18.8200 | lows=[18.731000900268555, 16.860000610351562, 18.81999969482422] span=11.63% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.6736830270200813 wick_frac=0.32631697297991863 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.08333532015672063 wick_frac=0.9166646798432794 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.999618539004388 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.0900:wick=0.2000; 2026-11-20:DOJI:body=+0.0000:wick=0.3150; 2026-11-23:GREEN:body=+0.1200:wick=0.1400; 2026-11-24:RED:body=-0.0200:wick=0.2200; 2026-11-25:GREEN:body=+0.3200:wick=0.1550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.59 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9445.44 | **NEUTRAL** |
| `B04_income` | 211.17 | **GOOD** |
| `B05_profit_margin` | 2.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.9 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.2099999999999973 (now=19.9 vs prior_export=19.69 on finviz_2026-11-25) | **GOOD** |
| `B09_analyst_recom` | 2.53 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.27 | **GOOD** |
| `B13_short_float` | 1.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.0 (this export) | prior_export=6.0 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.59 (this export) | prior_export=2.59 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TRIN  ·  score **+17**  ·  Asset Management
price=17.309999465942383  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.01 on 2026-11-25; prev RSI=48.41 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.41@2026-11-24 → 51.01@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.41@2026-11-24 → 51.01@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.41@2026-11-24 → 51.01@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3500 R=0.0000); 2026-11-24:GREEN:O=17.1100,C=17.1700,body=+0.0600,vol=842600.0; 2026-11-25:GREEN:O=17.0200,C=17.3100,body=+0.2900,vol=1157300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1999900 Rvol=0); 2026-11-24:GREEN:O=17.1100,C=17.1700,body=+0.0600,vol=842600.0; 2026-11-25:GREEN:O=17.0200,C=17.3100,body=+0.2900,vol=1157300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.230 on 2026-11-25: today_vol=1157300 / avg20=940977 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.128 on 2026-11-25 (price=17.3100, mid=17.4200, upper=18.2823, lower=16.5577; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=17.3100 vs SMA50=17.2198 dist=+0.52% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=17.42_50=17.22_80=17.57 on 2026-11-25: SMA20=17.4200 SMA50=17.2198 SMA80=17.5663 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-25@17.3450; S2[2026-09-29→2026-10-27] low=2026-10-19@15.7450; S3[2026-10-28→2026-11-25] low=2026-11-20@16.6100 | lows=[17.344999313354492, 15.744999885559082, 16.610000610351562] span=10.16% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.5173772410287865 wick_frac=0.4826227589712135 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.2400:wick=0.3050; 2026-11-20:RED:body=-0.1600:wick=0.2150; 2026-11-23:DOJI:body=+0.0000:wick=0.3450; 2026-11-24:GREEN:body=+0.0600:wick=0.2700; 2026-11-25:GREEN:body=+0.2900:wick=0.0500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.42 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.78 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.92 | **NEUTRAL** |
| `B04_income` | 160.62 | **GOOD** |
| `B05_profit_margin` | 51.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.0 vs prior_export=19.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.18 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.03 (now=0.18 vs prior=0.15 on finviz_2026-11-25) | **GOOD** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.42 (this export) | prior_export=0.42 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.78 (this export) | prior_export=4.78 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNM  ·  score **+17**  ·  Industrial Distribution
price=44.33000183105469  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.23 on 2026-11-25; prev RSI=46.74 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.74@2026-11-24 → 53.23@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.74@2026-11-24 → 53.23@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.74@2026-11-24 → 53.23@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.5100 R=0.0000); 2026-11-24:GREEN:O=41.2500,C=42.6400,body=+1.3900,vol=2235700.0; 2026-11-25:GREEN:O=42.2100,C=44.3300,body=+2.1200,vol=2695800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4931500 Rvol=0); 2026-11-24:GREEN:O=41.2500,C=42.6400,body=+1.3900,vol=2235700.0; 2026-11-25:GREEN:O=42.2100,C=44.3300,body=+2.1200,vol=2695800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.165 on 2026-11-25: today_vol=2695800 / avg20=2313719 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.043 on 2026-11-25 (price=44.3300, mid=44.5220, upper=48.9588, lower=40.0852; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=44.3300 vs SMA50=42.9162 dist=+3.29% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=44.52_50=42.92_80=43.49 on 2026-11-25: SMA20=44.5220 SMA50=42.9162 SMA80=43.4917 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-25 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-14@40.0300; S2[2026-09-24→2026-10-27] low=2026-10-07@39.4150; S3[2026-10-28→2026-11-25] low=2026-11-20@40.3700 | lows=[40.029998779296875, 39.415000915527344, 40.369998931884766] span=2.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.8434009920111665 wick_frac=0.15659900798883347 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.3800:wick=1.3450; 2026-11-20:RED:body=-1.0700:wick=0.8400; 2026-11-23:GREEN:body=+0.2100:wick=1.0700; 2026-11-24:GREEN:body=+1.3900:wick=0.3400; 2026-11-25:GREEN:body=+2.1200:wick=0.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.43 (current export asof; earnings_date=12/8/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.43 (current export; earnings_date=12/8/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7698.0 | **NEUTRAL** |
| `B04_income` | 459.0 | **GOOD** |
| `B05_profit_margin` | 5.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 54.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=54.29 vs prior_export=54.29 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | 0.22 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.22 vs prior=0.22 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.37 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 12/8/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.43 (this export) | prior_export=3.43 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.43 (this export) | prior_export=0.43 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UHS  ·  score **+16**  ·  Medical Care Facilities
price=190.57000732421875  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.32 on 2026-11-25; prev RSI=47.64 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.64@2026-11-24 → 54.32@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.64@2026-11-24 → 54.32@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.64@2026-11-24 → 54.32@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=3.441 (G=6.9500 R=2.0200); 2026-11-24:RED:O=185.2200,C=183.2000,body=-2.0200,vol=428400.0; 2026-11-25:GREEN:O=183.6200,C=190.5700,body=+6.9500,vol=642700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=1.500 (Gvol=642700 Rvol=428400); 2026-11-24:RED:O=185.2200,C=183.2000,body=-2.0200,vol=428400.0; 2026-11-25:GREEN:O=183.6200,C=190.5700,body=+6.9500,vol=642700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.814 on 2026-11-25: today_vol=642700 / avg20=789483 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.134 on 2026-11-25 (price=190.5700, mid=188.2800, upper=205.3237, lower=171.2363; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=190.5700 vs SMA50=182.5926 dist=+4.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=188.2800 SMA50=182.5926 SMA80=178.5464 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-25 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-03@166.4300; S2[2026-09-24→2026-10-27] low=2026-10-08@173.3900; S3[2026-10-28→2026-11-25] low=2026-10-28@169.1300 | lows=[166.42999267578125, 173.38999938964844, 169.1300048828125] span=4.18% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.6569008510595338 wick_frac=0.3430991489404662 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.38697390806143267 wick_frac=0.6130260919385673 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.4405928253627733 need>1.4; red_wick_gt_green=False 5d trail=2026-11-19:RED:body=-9.4000:wick=1.2700; 2026-11-20:GREEN:body=+2.5100:wick=1.6400; 2026-11-23:GREEN:body=+5.2800:wick=1.1100; 2026-11-24:RED:body=-2.0200:wick=3.2000; 2026-11-25:GREEN:body=+6.9500:wick=3.6300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.54 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18352.59 | **NEUTRAL** |
| `B04_income` | 1461.75 | **GOOD** |
| `B05_profit_margin` | 7.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 194.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=194.0 vs prior_export=194.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.52 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.44 | **GOOD** |
| `B13_short_float` | 6.49 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.54 (this export) | prior_export=0.54 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VFC  ·  score **+16**  ·  Apparel Manufacturing
price=21.469999313354492  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.80 on 2026-11-25; prev RSI=62.97 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.97@2026-11-24 → 65.80@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.97@2026-11-24 → 65.80@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.97@2026-11-24 → 65.80@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.7300 R=0.0000); 2026-11-24:GREEN:O=20.0400,C=20.9100,body=+0.8700,vol=7730400.0; 2026-11-25:GREEN:O=20.6100,C=21.4700,body=+0.8600,vol=8085800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=15816200 Rvol=0); 2026-11-24:GREEN:O=20.0400,C=20.9100,body=+0.8700,vol=7730400.0; 2026-11-25:GREEN:O=20.6100,C=21.4700,body=+0.8600,vol=8085800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.774 on 2026-11-25: today_vol=8085800 / avg20=10448930 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.771 on 2026-11-25 (price=21.4700, mid=20.1860, upper=21.8524, lower=18.5196; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=21.4700 vs SMA50=17.2328 dist=+24.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=20.1860 SMA50=17.2328 SMA80=15.9756 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-10@12.4600; S2[2026-09-29→2026-10-27] low=2026-09-30@13.9300; S3[2026-10-28→2026-11-25] low=2026-10-28@15.9510 | lows=[12.460000038146973, 13.930000305175781, 15.951000213623047] span=28.02% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.7152585731735936 wick_frac=0.2847414268264063 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-19:RED:body=-0.7900:wick=0.3500; 2026-11-20:RED:body=-0.5400:wick=0.2400; 2026-11-23:GREEN:body=+0.1400:wick=0.4700; 2026-11-24:GREEN:body=+0.8700:wick=0.2200; 2026-11-25:GREEN:body=+0.8600:wick=0.5000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.82 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.13 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9403.78 | **NEUTRAL** |
| `B04_income` | 369.66 | **GOOD** |
| `B05_profit_margin` | 3.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.62 vs prior_export=19.62 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.26 | **GOOD** |
| `B10_insider_transactions` | 0.33 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.33 vs prior=0.33 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.85 | **GOOD** |
| `B13_short_float` | 8.39 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.82 (this export) | prior_export=30.82 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.13 (this export) | prior_export=1.13 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HII  ·  score **+16**  ·  Aerospace & Defense
price=299.3399963378906  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.25 on 2026-11-25; prev RSI=48.25 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.25@2026-11-24 → 52.25@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.25@2026-11-24 → 52.25@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.25@2026-11-24 → 52.25@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=14.5500 R=0.0000); 2026-11-24:GREEN:O=287.7800,C=292.8100,body=+5.0300,vol=357300.0; 2026-11-25:GREEN:O=289.8200,C=299.3400,body=+9.5200,vol=272600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=629900 Rvol=0); 2026-11-24:GREEN:O=287.7800,C=292.8100,body=+5.0300,vol=357300.0; 2026-11-25:GREEN:O=289.8200,C=299.3400,body=+9.5200,vol=272600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.503 on 2026-11-25: today_vol=272600 / avg20=541719 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.081 on 2026-11-25 (price=299.3400, mid=301.2680, upper=325.1462, lower=277.3898; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=299.3400 vs SMA50=298.0868 dist=+0.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=301.2680 SMA50=298.0868 SMA80=295.3819 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-11-25 (63 bars); S1[2026-07-17→2026-08-19] low=2026-07-17@268.2500; S2[2026-08-20→2026-10-27] low=2026-10-19@267.0700; S3[2026-10-28→2026-11-25] low=2026-10-28@275.6000 | lows=[268.25, 267.07000732421875, 275.6000061035156] span=3.19% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.6097772917531161 wick_frac=0.3902227082468839 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-1.3900:wick=6.2500; 2026-11-20:RED:body=-5.3700:wick=5.6000; 2026-11-23:GREEN:body=+1.2200:wick=4.8800; 2026-11-24:GREEN:body=+5.0300:wick=4.8500; 2026-11-25:GREEN:body=+9.5200:wick=3.8800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.6 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.72 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13530.0 | **NEUTRAL** |
| `B04_income` | 733.0 | **GOOD** |
| `B05_profit_margin` | 5.42 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 357.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=357.83 vs prior_export=357.83 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -5.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.39 vs prior=-5.39 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 3.64 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.6 (this export) | prior_export=25.6 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.72 (this export) | prior_export=4.72 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBC  ·  score **+16**  ·  Medical Instruments & Supplies
price=7.920000076293945  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.44 on 2026-11-25; prev RSI=72.24 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 72.24@2026-11-24 → 69.44@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.24@2026-11-24 → 69.44@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 72.24@2026-11-24 → 69.44@2026-11-25 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=48.000 (G=1.2000 R=0.0250); 2026-11-24:GREEN:O=6.8800,C=8.0800,body=+1.2000,vol=6981700.0; 2026-11-25:RED:O=7.9450,C=7.9200,body=-0.0250,vol=2043700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=3.416 (Gvol=6981700 Rvol=2043700); 2026-11-24:GREEN:O=6.8800,C=8.0800,body=+1.2000,vol=6981700.0; 2026-11-25:RED:O=7.9450,C=7.9200,body=-0.0250,vol=2043700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.816 on 2026-11-25: today_vol=2043700 / avg20=1125637 (avg window 2026-10-28→2026-11-24, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.297 on 2026-11-25 (price=7.9200, mid=6.4412, upper=7.5817, lower=5.3008; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-25: price=7.9200 vs SMA50=6.1134 dist=+29.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=6.4412 SMA50=6.1134 SMA80=5.6576 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-25 (63 bars); S1[2026-08-25→2026-09-23] low=2026-08-27@4.8000; S2[2026-09-24→2026-10-27] low=2026-09-24@5.4500; S3[2026-10-28→2026-11-25] low=2026-11-23@5.5550 | lows=[4.800000190734863, 5.449999809265137, 5.554999828338623] span=15.73% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.7228917254646363 wick_frac=0.2771082745353637 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.021276682089139756 wick_frac=0.9787233179108602 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=47.999809265864315 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:GREEN:body=+0.1200:wick=0.3100; 2026-11-20:GREEN:body=+0.0500:wick=0.4500; 2026-11-23:RED:body=-0.4250:wick=0.1900; 2026-11-24:GREEN:body=+1.2000:wick=0.4600; 2026-11-25:RED:body=-0.0250:wick=1.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=148.53 (current export asof; earnings_date=11/24/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.72 (current export; earnings_date=11/24/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1053.4 | **NEUTRAL** |
| `B04_income` | 127.1 | **GOOD** |
| `B05_profit_margin` | 12.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.0 vs prior_export=5.0 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.18 | **GOOD** |
| `B13_short_float` | 10.16 | **NEUTRAL** |
| `B14_earnings_date` | 11/24/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=148.53 (this export) | prior_export=148.53 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.72 (this export) | prior_export=11.72 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+16**  ·  Credit Services
price=424.3900146484375  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.09 on 2026-11-25; prev RSI=54.55 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.55@2026-11-24 → 59.09@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.55@2026-11-24 → 59.09@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.55@2026-11-24 → 59.09@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.7500 R=0.0000); 2026-11-24:GREEN:O=409.0000,C=416.7800,body=+7.7800,vol=6253600.0; 2026-11-25:GREEN:O=415.4200,C=424.3900,body=+8.9700,vol=6591500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12845100 Rvol=0); 2026-11-24:GREEN:O=409.0000,C=416.7800,body=+7.7800,vol=6253600.0; 2026-11-25:GREEN:O=415.4200,C=424.3900,body=+8.9700,vol=6591500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.894 on 2026-11-25: today_vol=6591500 / avg20=7373310 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.312 on 2026-11-25 (price=424.3900, mid=419.0895, upper=436.0808, lower=402.0982; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=424.3900 vs SMA50=397.2136 dist=+6.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=419.0895 SMA50=397.2136 SMA80=387.3691 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-22@359.8000; S2[2026-09-29→2026-10-27] low=2026-10-02@356.7500; S3[2026-10-28→2026-11-25] low=2026-10-28@394.0000 | lows=[359.79998779296875, 356.75, 394.0] span=10.44% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.7505431972307195 wick_frac=0.24945680276928048 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-8.7900:wick=3.9800; 2026-11-20:RED:body=-1.6900:wick=5.8600; 2026-11-23:GREEN:body=+5.3900:wick=2.0100; 2026-11-24:GREEN:body=+7.7800:wick=1.6900; 2026-11-25:GREEN:body=+8.9700:wick=4.2300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.2 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 45903.0 | **NEUTRAL** |
| `B04_income` | 23313.0 | **GOOD** |
| `B05_profit_margin` | 50.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 458.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=458.67 vs prior_export=458.67 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.39 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.08 | **GOOD** |
| `B13_short_float` | 1.27 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.2 (this export) | prior_export=1.2 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PRCH  ·  score **+16**  ·  Insurance - Property & Casualty
price=21.489999771118164  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.21 on 2026-11-25; prev RSI=61.88 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.88@2026-11-24 → 58.21@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.88@2026-11-24 → 58.21@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.88@2026-11-24 → 58.21@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=48.999 (G=0.9800 R=0.0200); 2026-11-24:GREEN:O=21.0400,C=22.0200,body=+0.9800,vol=2353500.0; 2026-11-25:RED:O=21.5100,C=21.4900,body=-0.0200,vol=938800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=2.507 (Gvol=2353500 Rvol=938800); 2026-11-24:GREEN:O=21.0400,C=22.0200,body=+0.9800,vol=2353500.0; 2026-11-25:RED:O=21.5100,C=21.4900,body=-0.0200,vol=938800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.431 on 2026-11-25: today_vol=938800 / avg20=2177967 (avg window 2026-10-28→2026-11-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.389 on 2026-11-25 (price=21.4900, mid=20.9300, upper=22.3702, lower=19.4898; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=21.4900 vs SMA50=18.6536 dist=+15.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=20.9300 SMA50=18.6536 SMA80=17.9382 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-18@15.4900; S2[2026-09-29→2026-10-27] low=2026-09-29@16.0700; S3[2026-10-28→2026-11-25] low=2026-10-28@16.1400 | lows=[15.489999771118164, 16.06999969482422, 16.139999389648438] span=4.20% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.6049383878890346 wick_frac=0.39506161211096535 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.014545787464488636 wick_frac=0.9854542125355114 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=48.99885561701316 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.8800:wick=0.2750; 2026-11-20:GREEN:body=+0.4600:wick=0.6700; 2026-11-23:GREEN:body=+0.4900:wick=0.6900; 2026-11-24:GREEN:body=+0.9800:wick=0.6400; 2026-11-25:RED:body=-0.0200:wick=1.3550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=599.3 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.56 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 530.17 | **NEUTRAL** |
| `B04_income` | 9.6 | **GOOD** |
| `B05_profit_margin` | 1.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.379999999999999 (now=25.5 vs prior_export=25.12 on finviz_2026-11-25) | **GOOD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | -4.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.66 vs prior=-4.66 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.08 | **GOOD** |
| `B13_short_float` | 18.8 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=599.3 (this export) | prior_export=599.3 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.56 (this export) | prior_export=4.56 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEHC  ·  score **+16**  ·  Medical Devices
price=83.33000183105469  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.91 on 2026-11-25; prev RSI=61.07 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.07@2026-11-24 → 62.91@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.07@2026-11-24 → 62.91@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.07@2026-11-24 → 62.91@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.2000 R=0.0000); 2026-11-24:GREEN:O=80.7200,C=82.5200,body=+1.8000,vol=2354500.0; 2026-11-25:GREEN:O=81.9300,C=83.3300,body=+1.4000,vol=2617000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4971500 Rvol=0); 2026-11-24:GREEN:O=80.7200,C=82.5200,body=+1.8000,vol=2354500.0; 2026-11-25:GREEN:O=81.9300,C=83.3300,body=+1.4000,vol=2617000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.604 on 2026-11-25: today_vol=2617000 / avg20=4333607 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.303 on 2026-11-25 (price=83.3300, mid=81.9215, upper=86.5638, lower=77.2792; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=83.3300 vs SMA50=73.2981 dist=+13.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=81.9215 SMA50=73.2981 SMA80=72.2438 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-17@62.9950; S2[2026-09-29→2026-10-27] low=2026-10-08@63.3150; S3[2026-10-28→2026-11-25] low=2026-10-28@74.6400 | lows=[62.994998931884766, 63.314998626708984, 74.63999938964844] span=18.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.7262036074620865 wick_frac=0.27379639253791355 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-1.7700:wick=0.7500; 2026-11-20:RED:body=-1.8600:wick=0.9200; 2026-11-23:GREEN:body=+0.6700:wick=0.6100; 2026-11-24:GREEN:body=+1.8000:wick=0.3980; 2026-11-25:GREEN:body=+1.4000:wick=0.8100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.28 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.02 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21637.0 | **NEUTRAL** |
| `B04_income` | 2154.0 | **GOOD** |
| `B05_profit_margin` | 9.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 88.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=88.26 vs prior_export=88.26 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.76 | **GOOD** |
| `B10_insider_transactions` | 0.7 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.7 vs prior=0.7 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.64 | **GOOD** |
| `B13_short_float` | 3.99 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.28 (this export) | prior_export=11.28 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.02 (this export) | prior_export=3.02 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+16**  ·  Waste Management
price=254.16000366210938  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.63 on 2026-11-25; prev RSI=63.51 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.51@2026-11-24 → 65.63@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.51@2026-11-24 → 65.63@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.51@2026-11-24 → 65.63@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=11.233 (G=3.3700 R=0.3000); 2026-11-24:GREEN:O=248.7800,C=252.1500,body=+3.3700,vol=1420800.0; 2026-11-25:RED:O=254.4600,C=254.1600,body=-0.3000,vol=1178200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=1.206 (Gvol=1420800 Rvol=1178200); 2026-11-24:GREEN:O=248.7800,C=252.1500,body=+3.3700,vol=1420800.0; 2026-11-25:RED:O=254.4600,C=254.1600,body=-0.3000,vol=1178200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.775 on 2026-11-25: today_vol=1178200 / avg20=1520649 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.652 on 2026-11-25 (price=254.1600, mid=248.2650, upper=257.3004, lower=239.2296; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=254.1600 vs SMA50=232.1420 dist=+9.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=248.2650 SMA50=232.1420 SMA80=227.3046 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-28→2026-11-25 (63 bars); S1[2026-08-28→2026-09-28] low=2026-09-25@211.2300; S2[2026-09-29→2026-10-27] low=2026-10-02@205.9700; S3[2026-10-28→2026-11-25] low=2026-10-28@230.8800 | lows=[211.22999572753906, 205.97000122070312, 230.8800048828125] span=12.09% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.596459957113304 wick_frac=0.4035400428866959 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=0.07500076293945312 wick_frac=0.9249992370605469 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.233202787243782 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.6700:wick=2.7300; 2026-11-20:RED:body=-4.9700:wick=2.0300; 2026-11-23:GREEN:body=+3.6900:wick=0.9600; 2026-11-24:GREEN:body=+3.3700:wick=2.2800; 2026-11-25:RED:body=-0.3000:wick=3.7000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.16 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17238.0 | **NEUTRAL** |
| `B04_income` | 2227.56 | **GOOD** |
| `B05_profit_margin` | 12.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 261.95 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=261.95 vs prior_export=261.95 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.93 | **GOOD** |
| `B10_insider_transactions` | 5.65 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.65 vs prior=5.65 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 2.52 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.16 (this export) | prior_export=3.16 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLOC  ·  score **+16**  ·  Oil & Gas Equipment & Services
price=20.559999465942383  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.05 on 2026-11-25; prev RSI=48.21 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.21@2026-11-24 → 54.05@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.21@2026-11-24 → 54.05@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.21@2026-11-24 → 54.05@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2900 R=0.0000); 2026-11-24:GREEN:O=19.6200,C=19.8400,body=+0.2200,vol=692400.0; 2026-11-25:GREEN:O=19.4900,C=20.5600,body=+1.0700,vol=510400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1202800 Rvol=0); 2026-11-24:GREEN:O=19.6200,C=19.8400,body=+0.2200,vol=692400.0; 2026-11-25:GREEN:O=19.4900,C=20.5600,body=+1.0700,vol=510400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.709 on 2026-11-25: today_vol=510400 / avg20=719819 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.254 on 2026-11-25 (price=20.5600, mid=20.1875, upper=21.6513, lower=18.7237; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=20.5600 vs SMA50=19.6516 dist=+4.62% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=20.19_50=19.65_80=20.39 on 2026-11-25: SMA20=20.1875 SMA50=19.6516 SMA80=20.3949 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-25 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-23@19.0000; S2[2026-09-24→2026-10-27] low=2026-09-30@17.8300; S3[2026-10-28→2026-11-25] low=2026-11-05@18.6700 | lows=[19.0, 17.829999923706055, 18.670000076293945] span=6.56% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.5979215654623768 wick_frac=0.4020784345376231 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-19:RED:body=-0.3900:wick=0.2100; 2026-11-20:RED:body=-1.3500:wick=0.2900; 2026-11-23:GREEN:body=+0.1700:wick=0.9050; 2026-11-24:GREEN:body=+0.2200:wick=0.4550; 2026-11-25:GREEN:body=+1.0700:wick=0.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.45 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.03 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 901.22 | **NEUTRAL** |
| `B04_income` | 57.32 | **GOOD** |
| `B05_profit_margin` | 6.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 29.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=29.78 vs prior_export=29.78 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | 0.41 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.41 vs prior=0.41 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.94 | **GOOD** |
| `B13_short_float` | 11.85 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.45 (this export) | prior_export=30.45 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.03 (this export) | prior_export=6.03 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OCFC  ·  score **+15**  ·  Banks - Regional
price=17.670000076293945  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.60 on 2026-11-25; prev RSI=49.81 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.81@2026-11-24 → 50.60@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.81@2026-11-24 → 50.60@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.81@2026-11-24 → 50.60@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4500 R=0.0000); 2026-11-24:GREEN:O=17.3400,C=17.6200,body=+0.2800,vol=722100.0; 2026-11-25:GREEN:O=17.5000,C=17.6700,body=+0.1700,vol=866100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1588200 Rvol=0); 2026-11-24:GREEN:O=17.3400,C=17.6200,body=+0.2800,vol=722100.0; 2026-11-25:GREEN:O=17.5000,C=17.6700,body=+0.1700,vol=866100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.719 on 2026-11-25: today_vol=866100 / avg20=1204309 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.347 on 2026-11-25 (price=17.6700, mid=17.9785, upper=18.8663, lower=17.0907; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=17.6700 vs SMA50=17.3366 dist=+1.92% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=17.98_50=17.34_80=18.02 on 2026-11-25: SMA20=17.9785 SMA50=17.3366 SMA80=18.0181 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-25→2026-11-25 (63 bars); S1[2026-08-25→2026-09-23] low=2026-09-23@16.9900; S2[2026-09-24→2026-10-27] low=2026-10-19@15.6000; S3[2026-10-28→2026-11-25] low=2026-10-28@16.9000 | lows=[16.989999771118164, 15.600000381469727, 16.899999618530273] span=8.91% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.5268071251812398 wick_frac=0.47319287481876016 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:RED:body=-0.2100:wick=0.4300; 2026-11-20:RED:body=-0.1800:wick=0.2600; 2026-11-23:RED:body=-0.0600:wick=0.4800; 2026-11-24:GREEN:body=+0.2800:wick=0.2400; 2026-11-25:GREEN:body=+0.1700:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.72 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.13 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 890.83 | **NEUTRAL** |
| `B04_income` | 47.03 | **GOOD** |
| `B05_profit_margin` | 5.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.38 vs prior_export=21.38 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | -0.74 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.74 vs prior=-0.74 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.68 | **GOOD** |
| `B13_short_float` | 5.93 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.72 (this export) | prior_export=18.72 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.13 (this export) | prior_export=4.13 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HGTY  ·  score **+15**  ·  Insurance - Property & Casualty
price=16.579999923706055  pair=`2026-11-24→2026-11-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.15 on 2026-11-25; prev RSI=58.10 on 2026-11-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.10@2026-11-24 → 60.15@2026-11-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.10@2026-11-24 → 60.15@2026-11-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.10@2026-11-24 → 60.15@2026-11-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5000 R=0.0000); 2026-11-24:GREEN:O=16.2100,C=16.3800,body=+0.1700,vol=570300.0; 2026-11-25:GREEN:O=16.2500,C=16.5800,body=+0.3300,vol=865400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-24 + 2026-11-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1435700 Rvol=0); 2026-11-24:GREEN:O=16.2100,C=16.3800,body=+0.1700,vol=570300.0; 2026-11-25:GREEN:O=16.2500,C=16.5800,body=+0.3300,vol=865400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.729 on 2026-11-25: today_vol=865400 / avg20=1186635 (avg window 2026-10-28→2026-11-24, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.008 on 2026-11-25 (price=16.5800, mid=16.5665, upper=18.3489, lower=14.7841; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-25: price=16.5800 vs SMA50=14.5940 dist=+13.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-25: SMA20=16.5665 SMA50=14.5940 SMA80=13.5117 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-11-25 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-23@11.2500; S2[2026-08-12→2026-10-27] low=2026-08-12@12.6400; S3[2026-10-28→2026-11-25] low=2026-10-28@15.2450 | lows=[11.25, 12.640000343322754, 15.244999885559082] span=35.51% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: GREEN body_frac=0.5428562935755055 wick_frac=0.45714370642449453 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-24+2026-11-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-19:GREEN:body=+0.0100:wick=0.4500; 2026-11-20:RED:body=-0.0600:wick=0.5050; 2026-11-23:GREEN:body=+0.0800:wick=0.3800; 2026-11-24:GREEN:body=+0.1700:wick=0.1800; 2026-11-25:GREEN:body=+0.3300:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=834.58 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.5 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1378.22 | **NEUTRAL** |
| `B04_income` | 6.71 | **GOOD** |
| `B05_profit_margin` | 0.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.57 vs prior_export=17.57 on finviz_2026-11-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | -67.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-67.66 vs prior=-67.66 on finviz_2026-11-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.1 | **GOOD** |
| `B13_short_float` | 0.9 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=834.58 (this export) | prior_export=834.58 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.5 (this export) | prior_export=4.5 (finviz_2026-11-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-26_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.