# A+B1 Feature Checklist — 2026-08-25

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,705** names
- Export: `finviz_2026-08-25.csv` · prior export for Δ: `2026-08-24`
- score = sum of flags over **30** features

## Framing (per asof trading day)

- **A05/A06/A12/A13** use **exactly two connected sessions**: `pair_day_a` (prev) + `pair_day_b` (asof).
  No multi-day green/red sums.
- **RSI crosses**: cross **up** through 30 or 50 → GOOD; cross **down** through 50 or 70 → BAD.
- **A11 downside structure**: last ~63 sessions split into 3 equal sections; lowest **low** in each;
  GOOD if rising lows or span(highest low − lowest low)/lowest ≤ 12%.
- **B17/B18**: current export EPS/Rev surprise vs **prior export** snapshot (proxy for last 2 prints).
- Analyst last-2 rating actions (upgrade/downgrade) come from quote scrape → merge step (B19).

## Ranked (top 20)

| Rank | Ticker | score | good | bad | pair | Industry |
|-----:|--------|------:|-----:|----:|------|----------|
| 1 | AHR | +17 | 18 | 1 | 2026-08-20→2026-08-21 | REIT - Healthcare Facilities |
| 2 | PLTR | +17 | 18 | 1 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 3 | FCX | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Copper |
| 4 | KBR | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Engineering & Construction |
| 5 | SBLK | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Marine Shipping |
| 6 | BLMN | +16 | 16 | 0 | 2026-08-20→2026-08-21 | Restaurants |
| 7 | LEU | +16 | 18 | 2 | 2026-08-20→2026-08-21 | Uranium |
| 8 | SON | +16 | 17 | 1 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 9 | WPM | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Gold |
| 10 | CBRL | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Restaurants |
| 11 | GPK | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Packaging & Containers |
| 12 | NWBI | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Banks - Regional |
| 13 | AVNT | +15 | 15 | 0 | 2026-08-20→2026-08-21 | Specialty Chemicals |
| 14 | CP | +15 | 16 | 1 | 2026-08-13→2026-08-14 | Railroads |
| 15 | ANET | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Computer Hardware |
| 16 | DRH | +15 | 16 | 1 | 2026-08-20→2026-08-21 | REIT - Hotel & Motel |
| 17 | ERO | +15 | 18 | 3 | 2026-08-20→2026-08-21 | Copper |
| 18 | SOLV | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Medical Instruments & Supplies |
| 19 | PANW | +15 | 17 | 2 | 2026-08-20→2026-08-21 | Software - Infrastructure |
| 20 | BJ | +15 | 16 | 1 | 2026-08-20→2026-08-21 | Discount Stores |

## Full checklist — top 20

### AHR  ·  score **+17**  ·  REIT - Healthcare Facilities
price=55.93000030517578  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.58 on 2026-08-21; prev RSI=55.93 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.93@2026-08-20 → 56.58@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=4.750 (G=0.7600 R=0.1600); 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0; 2026-08-21:RED:O=56.0900,C=55.9300,body=-0.1600,vol=1992700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.148 (Gvol=2287500 Rvol=1992700); 2026-08-20:GREEN:O=55.0100,C=55.7700,body=+0.7600,vol=2287500.0; 2026-08-21:RED:O=56.0900,C=55.9300,body=-0.1600,vol=1992700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.673 on 2026-08-21: today_vol=1992700 / avg20=2960060 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.191 on 2026-08-21 (price=55.9300, mid=55.3530, upper=58.3667, lower=52.3393; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=55.9300 vs SMA50=52.8823 dist=+5.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=55.3530 SMA50=52.8823 SMA80=51.6333 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-06-08@44.5946; S2[2026-06-18→2026-07-20] low=2026-06-18@45.5400; S3[2026-07-23→2026-08-21] low=2026-08-11@52.3300 | lows=[44.59456890815638, 45.539996507535484, 52.33000183105469] span=17.35% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6608705459322113 wick_frac=0.3391294540677887 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.131147284688696 wick_frac=0.868852715311304 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.750017881410486 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.2500:wick=0.4100; 2026-08-18:RED:body=-0.6800:wick=0.3300; 2026-08-19:RED:body=-0.0400:wick=0.9100; 2026-08-20:GREEN:body=+0.7600:wick=0.3900; 2026-08-21:RED:body=-0.1600:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=16.62 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.98 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2502.39 | **NEUTRAL** |
| `B04_income` | 121.02 | **GOOD** |
| `B05_profit_margin` | 4.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.67 vs prior_export=63.67 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -0.06 | **BAD** |
| `B11_insider_tx_delta` | delta=1.7 (now=-0.06 vs prior=-1.76 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 17.27 | **GOOD** |
| `B13_short_float` | 10.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLTR  ·  score **+17**  ·  Software - Infrastructure
price=179.94000244140625  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.41 on 2026-08-21; prev RSI=66.30 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.30@2026-08-20 → 69.41@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.104 (G=5.9600 R=1.9200); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.517 (Gvol=40986600 Rvol=27018400); 2026-08-20:RED:O=175.8800,C=173.9600,body=-1.9200,vol=27018400.0; 2026-08-21:GREEN:O=173.9800,C=179.9400,body=+5.9600,vol=40986600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.879 on 2026-08-21: today_vol=40986600 / avg20=46642400 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.500 on 2026-08-21 (price=179.9400, mid=157.2745, upper=202.5798, lower=111.9692; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=179.9400 vs SMA50=138.9318 dist=+29.52% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=157.2745 SMA50=138.9318 SMA80=138.8852 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-23@116.1800; S2[2026-06-24→2026-07-23] low=2026-06-25@106.3700; S3[2026-07-24→2026-08-21] low=2026-07-28@117.8900 | lows=[116.18000030517578, 106.37000274658203, 117.88999938964844] span=10.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6026296341438955 wick_frac=0.3973703658561045 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.4334073882717644 wick_frac=0.5665926117282356 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.1041731238426755 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-1.5100:wick=2.4640; 2026-08-18:RED:body=-0.4300:wick=3.9400; 2026-08-19:GREEN:body=+2.5800:wick=4.4690; 2026-08-20:RED:body=-1.9200:wick=2.5100; 2026-08-21:GREEN:body=+5.9600:wick=3.9300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.98 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.8 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6155.94 | **NEUTRAL** |
| `B04_income` | 3016.69 | **GOOD** |
| `B05_profit_margin` | 49.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 200.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.799999999999983 (now=200.88 vs prior_export=199.08 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -1.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.24999999999999978 (now=-1.8 vs prior=-2.05 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 3.91 | **GOOD** |
| `B13_short_float` | 3.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.98 (this export) | prior_export=18.98 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.8 (this export) | prior_export=6.8 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FCX  ·  score **+16**  ·  Copper
price=76.66000366210938  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.30 on 2026-08-21; prev RSI=61.63 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.63@2026-08-20 → 69.30@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.6000 R=0.0000); 2026-08-20:GREEN:O=67.8200,C=71.2200,body=+3.4000,vol=17647400.0; 2026-08-21:GREEN:O=74.4600,C=76.6600,body=+2.2000,vol=28496500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=46143900 Rvol=0); 2026-08-20:GREEN:O=67.8200,C=71.2200,body=+3.4000,vol=17647400.0; 2026-08-21:GREEN:O=74.4600,C=76.6600,body=+2.2000,vol=28496500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.245 on 2026-08-21: today_vol=28496500 / avg20=12691485 (avg window 2026-07-24→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.218 on 2026-08-21 (price=76.6600, mid=67.1070, upper=74.9471, lower=59.2669; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=76.6600 vs SMA50=64.6077 dist=+18.65% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=67.1070 SMA50=64.6077 SMA80=63.9602 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-22@61.3611; S2[2026-06-24→2026-07-23] low=2026-07-08@55.8644; S3[2026-07-24→2026-08-21] low=2026-07-29@58.1900 | lows=[61.36106366423981, 55.86440695057745, 58.189998626708984] span=9.84% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.8227965239697523 wick_frac=0.17720347603024772 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.7600:wick=0.5900; 2026-08-18:RED:body=-0.7600:wick=1.5600; 2026-08-19:GREEN:body=+0.1900:wick=2.4100; 2026-08-20:GREEN:body=+3.4000:wick=0.4000; 2026-08-21:GREEN:body=+2.2000:wick=0.7300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.25 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.71 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 25156.0 | **NEUTRAL** |
| `B04_income` | 2923.0 | **GOOD** |
| `B05_profit_margin` | 11.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 73.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=73.68 vs prior_export=73.68 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.54 | **GOOD** |
| `B10_insider_transactions` | -0.3 | **BAD** |
| `B11_insider_tx_delta` | delta=0.33 (now=-0.3 vs prior=-0.63 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 1.95 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.25 (this export) | prior_export=20.25 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.71 (this export) | prior_export=4.71 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KBR  ·  score **+16**  ·  Engineering & Construction
price=38.58000183105469  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.00 on 2026-08-21; prev RSI=55.61 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.61@2026-08-20 → 59.00@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6400 R=0.0000); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1163800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=3.022 (Gvol=1739450 Rvol=575650); 2026-08-20:DOJI:O=37.9000,C=37.9000,body=+0.0000,vol=1151300.0; 2026-08-21:GREEN:O=37.9400,C=38.5800,body=+0.6400,vol=1163800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-08-21: today_vol=1163800 / avg20=1561180 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.586 on 2026-08-21 (price=38.5800, mid=37.4205, upper=39.4004, lower=35.4406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=38.5800 vs SMA50=35.8930 dist=+7.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=37.4205 SMA50=35.8930 SMA80=35.2023 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@30.9469; S2[2026-06-18→2026-07-20] low=2026-06-22@31.6100; S3[2026-07-23→2026-08-21] low=2026-07-30@32.1400 | lows=[30.946867052586228, 31.610000610351562, 32.13999938964844] span=3.86% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.633665198703761 wick_frac=0.36633480129623897 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.8200:wick=0.0900; 2026-08-18:RED:body=-0.2600:wick=0.5300; 2026-08-19:GREEN:body=+0.2700:wick=0.6400; 2026-08-20:DOJI:body=+0.0000:wick=0.9600; 2026-08-21:GREEN:body=+0.6400:wick=0.3700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.44 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.1 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.64 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.44 (this export) | prior_export=10.44 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+16**  ·  Marine Shipping
price=30.5  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.24 on 2026-08-21; prev RSI=59.81 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.81@2026-08-20 → 67.24@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.426 (G=1.0000 R=0.1843); 2026-08-20:RED:O=29.3443,C=29.1600,body=-0.1843,vol=1605200.0; 2026-08-21:GREEN:O=29.5000,C=30.5000,body=+1.0000,vol=2808200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.749 (Gvol=2808200 Rvol=1605200); 2026-08-20:RED:O=29.3443,C=29.1600,body=-0.1843,vol=1605200.0; 2026-08-21:GREEN:O=29.5000,C=30.5000,body=+1.0000,vol=2808200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.868 on 2026-08-21: today_vol=2808200 / avg20=1503535 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.220 on 2026-08-21 (price=30.5000, mid=28.5647, upper=30.1513, lower=26.9781; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=30.5000 vs SMA50=27.0602 dist=+12.71% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=28.5647 SMA50=27.0602 SMA80=26.6572 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@25.4230; S2[2026-06-18→2026-07-20] low=2026-06-26@23.8600; S3[2026-07-23→2026-08-21] low=2026-07-23@26.4700 | lows=[25.423019363615396, 23.860000610351562, 26.469999313354492] span=10.94% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6451616078833834 wick_frac=0.3548383921166166 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.2923086854838944 wick_frac=0.7076913145161056 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.425585721237033 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.0089:wick=0.1358; 2026-08-18:GREEN:body=+0.0970:wick=0.5238; 2026-08-19:RED:body=-0.1261:wick=0.7178; 2026-08-20:RED:body=-0.1843:wick=0.4462; 2026-08-21:GREEN:body=+1.0000:wick=0.5500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.46 vs prior_export=35.46 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.48 vs prior=-0.48 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.37 | **GOOD** |
| `B13_short_float` | 2.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BLMN  ·  score **+16**  ·  Restaurants
price=11.630000114440918  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.89 on 2026-08-21; prev RSI=58.18 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.18@2026-08-20 → 67.89@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0500 R=0.0000); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3319300 Rvol=0); 2026-08-20:GREEN:O=10.4500,C=10.4800,body=+0.0300,vol=1469900.0; 2026-08-21:GREEN:O=10.6100,C=11.6300,body=+1.0200,vol=1849400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.839 on 2026-08-21: today_vol=1849400 / avg20=2203390 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.614 on 2026-08-21 (price=11.6300, mid=10.1665, upper=12.5510, lower=7.7820; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=11.6300 vs SMA50=9.0324 dist=+28.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=10.1665 SMA50=9.0324 SMA80=8.4386 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-08@7.0300; S2[2026-06-22→2026-07-22] low=2026-07-08@7.6600; S3[2026-07-23→2026-08-21] low=2026-07-24@7.9100 | lows=[7.03000020980835, 7.659999847412109, 7.909999847412109] span=12.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5011365148646267 wick_frac=0.4988634851353733 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.2700:wick=0.3000; 2026-08-18:RED:body=-0.0100:wick=0.4200; 2026-08-19:GREEN:body=+0.0100:wick=0.5000; 2026-08-20:GREEN:body=+0.0300:wick=0.3700; 2026-08-21:GREEN:body=+1.0200:wick=0.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=35.18 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3979.52 | **NEUTRAL** |
| `B04_income` | 29.38 | **GOOD** |
| `B05_profit_margin` | 0.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 11.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=11.86 vs prior_export=11.86 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.73 | **NEUTRAL** |
| `B10_insider_transactions` | 1.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.01 vs prior=1.01 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.62 | **GOOD** |
| `B13_short_float` | 10.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.18 (this export) | prior_export=35.18 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LEU  ·  score **+16**  ·  Uranium
price=186.25999450683594  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.85 on 2026-08-21; prev RSI=48.28 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.28@2026-08-20 → 53.85@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.724 (G=7.8600 R=4.5600); 2026-08-20:RED:O=180.6900,C=176.1300,body=-4.5600,vol=425200.0; 2026-08-21:GREEN:O=178.4000,C=186.2600,body=+7.8600,vol=458200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.078 (Gvol=458200 Rvol=425200); 2026-08-20:RED:O=180.6900,C=176.1300,body=-4.5600,vol=425200.0; 2026-08-21:GREEN:O=178.4000,C=186.2600,body=+7.8600,vol=458200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.759 on 2026-08-21: today_vol=458200 / avg20=603300 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.252 on 2026-08-21 (price=186.2600, mid=182.0740, upper=198.6958, lower=165.4522; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=186.2600 vs SMA50=173.0796 dist=+7.62% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=182.07_50=173.08_80=178.60 on 2026-08-21: SMA20=182.0740 SMA50=173.0796 SMA80=178.6011 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-10@144.6500; S2[2026-06-24→2026-07-23] low=2026-07-17@142.1300; S3[2026-07-24→2026-08-21] low=2026-07-28@157.8900 | lows=[144.64999389648438, 142.1300048828125, 157.88999938964844] span=11.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7987808471112187 wick_frac=0.20121915288878137 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.41988925435669316 wick_frac=0.5801107456433069 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7236852672297251 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-4.8800:wick=2.6600; 2026-08-18:RED:body=-2.1500:wick=7.0700; 2026-08-19:GREEN:body=+6.8200:wick=4.8000; 2026-08-20:RED:body=-4.5600:wick=6.3000; 2026-08-21:GREEN:body=+7.8600:wick=1.9800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=17.52 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 473.9 | **NEUTRAL** |
| `B04_income` | 48.5 | **GOOD** |
| `B05_profit_margin` | 10.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 250.13 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-2.0 (now=250.13 vs prior_export=252.13 on finviz_2026-08-24) | **BAD** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -0.04 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.04 vs prior=-0.04 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.06 | **GOOD** |
| `B13_short_float` | 26.55 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.25 (this export) | prior_export=5.25 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=17.52 (this export) | prior_export=17.52 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SON  ·  score **+16**  ·  Packaging & Containers
price=59.47999954223633  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.69 on 2026-08-21; prev RSI=57.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.59@2026-08-20 → 61.69@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0400 R=0.0000); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=833100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1410300 Rvol=0); 2026-08-20:GREEN:O=56.7800,C=58.3300,body=+1.5500,vol=577200.0; 2026-08-21:GREEN:O=58.9900,C=59.4800,body=+0.4900,vol=833100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.823 on 2026-08-21: today_vol=833100 / avg20=1012315 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.847 on 2026-08-21 (price=59.4800, mid=57.8227, upper=59.7788, lower=55.8666; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=59.4800 vs SMA50=54.9048 dist=+8.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=57.8227 SMA50=54.9048 SMA80=52.5635 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@45.4827; S2[2026-06-18→2026-07-20] low=2026-06-22@49.4158; S3[2026-07-23→2026-08-21] low=2026-07-23@54.7457 | lows=[45.482707673306166, 49.41576037269584, 54.7456863407041] span=20.37% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6465540631459084 wick_frac=0.3534459368540917 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-1.0000:wick=0.1300; 2026-08-18:RED:body=-0.0700:wick=1.1000; 2026-08-19:GREEN:body=+0.2800:wick=0.5600; 2026-08-20:GREEN:body=+1.5500:wick=0.3800; 2026-08-21:GREEN:body=+0.4900:wick=0.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.23 (current export asof; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.29 (current export; earnings_date=7/22/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7458.91 | **NEUTRAL** |
| `B04_income` | 646.79 | **GOOD** |
| `B05_profit_margin` | 8.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.89 vs prior_export=63.89 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.37 vs prior=1.37 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.88 | **GOOD** |
| `B13_short_float` | 11.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.23 (this export) | prior_export=2.23 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.29 (this export) | prior_export=0.29 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WPM  ·  score **+15**  ·  Gold
price=157.77999877929688  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.50 on 2026-08-21; prev RSI=76.11 on 2026-08-20 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.11@2026-08-20 → 79.50@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.7900 R=0.0000); 2026-08-20:GREEN:O=144.5400,C=150.2500,body=+5.7100,vol=2584200.0; 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5883500 Rvol=0); 2026-08-20:GREEN:O=144.5400,C=150.2500,body=+5.7100,vol=2584200.0; 2026-08-21:GREEN:O=154.7000,C=157.7800,body=+3.0800,vol=3299300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.514 on 2026-08-21: today_vol=3299300 / avg20=2179180 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.038 on 2026-08-21 (price=157.7800, mid=127.9234, upper=156.6917, lower=99.1551; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=157.7800 vs SMA50=118.9574 dist=+32.64% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=127.92_50=118.96_80=122.97 on 2026-08-21: SMA20=127.9234 SMA50=118.9574 SMA80=122.9732 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-10@106.8200; S2[2026-06-22→2026-07-21] low=2026-07-17@101.5900; S3[2026-07-23→2026-08-21] low=2026-07-29@106.3000 | lows=[106.81999969482422, 101.58999633789062, 106.30000305175781] span=5.15% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7911342774102055 wick_frac=0.20886572258979444 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.2483:wick=1.9774; 2026-08-18:RED:body=-1.3382:wick=2.1871; 2026-08-19:GREEN:body=+7.8496:wick=1.3283; 2026-08-20:GREEN:body=+5.7100:wick=0.4500; 2026-08-21:GREEN:body=+3.0800:wick=1.6200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.52 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3171.64 | **NEUTRAL** |
| `B04_income` | 2050.75 | **GOOD** |
| `B05_profit_margin` | 64.66 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 170.99 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.09000000000000341 (now=170.99 vs prior_export=170.9 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.06 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.97 | **GOOD** |
| `B13_short_float` | 0.93 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.52 (this export) | prior_export=4.52 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+15**  ·  Restaurants
price=57.650001525878906  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.52 on 2026-08-21; prev RSI=50.63 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.63@2026-08-20 → 57.52@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=5.349 (G=2.3000 R=0.4300); 2026-08-20:RED:O=55.5600,C=55.1300,body=-0.4300,vol=546800.0; 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.000 (Gvol=546700 Rvol=546800); 2026-08-20:RED:O=55.5600,C=55.1300,body=-0.4300,vol=546800.0; 2026-08-21:GREEN:O=55.3500,C=57.6500,body=+2.3000,vol=546700.0 | **BAD** |
| `A07_rvol` | RVOL=0.678 on 2026-08-21: today_vol=546700 / avg20=805905 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.223 on 2026-08-21 (price=57.6500, mid=56.8255, upper=60.5311, lower=53.1199; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=57.6500 vs SMA50=51.7908 dist=+11.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=56.8255 SMA50=51.7908 SMA80=44.1081 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@28.0918; S2[2026-06-18→2026-07-20] low=2026-06-18@44.3219; S3[2026-07-23→2026-08-21] low=2026-07-27@50.2000 | lows=[28.091751487486153, 44.32188230619164, 50.20000076293945] span=78.70% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.8745267884856941 wick_frac=0.12547321151430585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.23497309905924876 wick_frac=0.7650269009407512 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.348840510281932 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.7900:wick=1.7700; 2026-08-18:RED:body=-0.7800:wick=1.3000; 2026-08-19:RED:body=-1.8300:wick=1.7800; 2026-08-20:RED:body=-0.4300:wick=1.4000; 2026-08-21:GREEN:body=+2.3000:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=160.4 (current export asof; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.66 (current export; earnings_date=6/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3337.38 | **NEUTRAL** |
| `B04_income` | 26.23 | **GOOD** |
| `B05_profit_margin` | 0.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.0 vs prior_export=45.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 3.18 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 23.97 | **GOOD** |
| `B14_earnings_date` | 6/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=160.4 (this export) | prior_export=160.4 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.66 (this export) | prior_export=2.66 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPK  ·  score **+15**  ·  Packaging & Containers
price=11.960000038146973  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.53 on 2026-08-21; prev RSI=55.27 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.27@2026-08-20 → 59.53@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6200 R=0.0000); 2026-08-20:GREEN:O=11.2600,C=11.6300,body=+0.3700,vol=4254600.0; 2026-08-21:GREEN:O=11.7100,C=11.9600,body=+0.2500,vol=4464100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8718700 Rvol=0); 2026-08-20:GREEN:O=11.2600,C=11.6300,body=+0.3700,vol=4254600.0; 2026-08-21:GREEN:O=11.7100,C=11.9600,body=+0.2500,vol=4464100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.904 on 2026-08-21: today_vol=4464100 / avg20=4935460 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.536 on 2026-08-21 (price=11.9600, mid=11.5490, upper=12.3154, lower=10.7826; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=11.9600 vs SMA50=10.9839 dist=+8.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=11.5490 SMA50=10.9839 SMA80=10.6719 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@9.2143; S2[2026-06-18→2026-07-20] low=2026-07-08@9.8600; S3[2026-07-23→2026-08-21] low=2026-07-24@10.5000 | lows=[9.214289930855712, 9.859999656677246, 10.5] span=13.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.666736810290141 wick_frac=0.3332631897098591 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.2300:wick=0.1500; 2026-08-18:RED:body=-0.1400:wick=0.1300; 2026-08-19:GREEN:body=+0.2700:wick=0.1900; 2026-08-20:GREEN:body=+0.3700:wick=0.0400; 2026-08-21:GREEN:body=+0.2500:wick=0.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.85 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.59 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8637.0 | **NEUTRAL** |
| `B04_income` | 194.0 | **GOOD** |
| `B05_profit_margin` | 2.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.58 vs prior_export=12.58 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 3.15 | **NEUTRAL** |
| `B10_insider_transactions` | 2.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.06 vs prior=2.06 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.85 (this export) | prior_export=14.85 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+15**  ·  Banks - Regional
price=15.420000076293945  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.05 on 2026-08-21; prev RSI=47.68 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.68@2026-08-20 → 51.05@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.500 (G=0.0900 R=0.0600); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=864700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=0.771 (Gvol=667000 Rvol=864700); 2026-08-20:GREEN:O=15.2200,C=15.3100,body=+0.0900,vol=667000.0; 2026-08-21:RED:O=15.4800,C=15.4200,body=-0.0600,vol=864700.0 | **BAD** |
| `A07_rvol` | RVOL=0.891 on 2026-08-21: today_vol=864700 / avg20=970710 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.394 on 2026-08-21 (price=15.4200, mid=15.5735, upper=15.9625, lower=15.1844; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=15.4200 vs SMA50=15.0801 dist=+2.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=15.5735 SMA50=15.0801 SMA80=14.5336 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@13.3506; S2[2026-06-18→2026-07-20] low=2026-06-18@14.1504; S3[2026-07-23→2026-08-21] low=2026-07-23@14.8812 | lows=[13.350576971196979, 14.150425914132185, 14.881151984844093] span=11.46% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5624981373641766 wick_frac=0.4375018626358234 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.22221986747481598 wick_frac=0.7777801325251841 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5000158947134183 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.0300:wick=0.1400; 2026-08-18:RED:body=-0.0700:wick=0.1200; 2026-08-19:RED:body=-0.3800:wick=0.0400; 2026-08-20:GREEN:body=+0.0900:wick=0.0700; 2026-08-21:RED:body=-0.0600:wick=0.2100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.56 | **GOOD** |
| `B13_short_float` | 5.31 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVNT  ·  score **+15**  ·  Specialty Chemicals
price=44.65999984741211  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.72 on 2026-08-21; prev RSI=59.90 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.90@2026-08-20 → 62.72@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1900 R=0.0000); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=800300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1444200 Rvol=0); 2026-08-20:GREEN:O=43.1200,C=43.7300,body=+0.6100,vol=643900.0; 2026-08-21:GREEN:O=44.0800,C=44.6600,body=+0.5800,vol=800300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.885 on 2026-08-21: today_vol=800300 / avg20=904395 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.356 on 2026-08-21 (price=44.6600, mid=41.8005, upper=49.8272, lower=33.7738; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=44.6600 vs SMA50=38.7045 dist=+15.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=41.8005 SMA50=38.7045 SMA80=37.3873 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-19@31.9895; S2[2026-06-18→2026-07-20] low=2026-07-08@34.9500; S3[2026-07-23→2026-08-21] low=2026-07-29@35.5700 | lows=[31.989543385452947, 34.95000076293945, 35.56999969482422] span=11.19% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6157398137259762 wick_frac=0.3842601862740238 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-0.6700:wick=0.2300; 2026-08-18:RED:body=-1.7400:wick=0.0300; 2026-08-19:GREEN:body=+0.0800:wick=0.7700; 2026-08-20:GREEN:body=+0.6100:wick=0.4700; 2026-08-21:GREEN:body=+0.5800:wick=0.2900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.51 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.06 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3331.5 | **NEUTRAL** |
| `B04_income` | 170.0 | **GOOD** |
| `B05_profit_margin` | 5.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.75 vs prior_export=50.75 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.36 | **GOOD** |
| `B13_short_float` | 4.48 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.51 (this export) | prior_export=7.51 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.06 (this export) | prior_export=2.06 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CP  ·  score **+15**  ·  Railroads
price=93.5  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.49 on 2026-08-14; prev RSI=61.30 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.30@2026-08-13 → 59.49@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=2.469 (G=0.7900 R=0.3200); 2026-08-13:GREEN:O=93.1300,C=93.9200,body=+0.7900,vol=2037600.0; 2026-08-14:RED:O=93.8200,C=93.5000,body=-0.3200,vol=2714700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=0.751 (Gvol=2037600 Rvol=2714700); 2026-08-13:GREEN:O=93.1300,C=93.9200,body=+0.7900,vol=2037600.0; 2026-08-14:RED:O=93.8200,C=93.5000,body=-0.3200,vol=2714700.0 | **BAD** |
| `A07_rvol` | RVOL=0.915 on 2026-08-14: today_vol=2714700 / avg20=2966730 (avg window 2026-07-14→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.673 on 2026-08-14 (price=93.5000, mid=91.6560, upper=94.3945, lower=88.9175; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=93.5000 vs SMA50=89.6680 dist=+4.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=91.6560 SMA50=89.6680 SMA80=88.1804 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-12→2026-08-14 (63 bars); S1[2026-05-12→2026-06-10] low=2026-05-12@83.9473; S2[2026-06-11→2026-07-13] low=2026-06-24@84.2666; S3[2026-07-14→2026-08-14] low=2026-07-30@87.2000 | lows=[83.94731893206304, 84.26662384723396, 87.19999694824219] span=3.87% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.9186050637852415 wick_frac=0.08139493621475843 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.2499985098913413 wick_frac=0.7500014901086587 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4687552154113916 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:RED:body=-0.4200:wick=1.1600; 2026-08-11:GREEN:body=+1.3200:wick=0.3100; 2026-08-12:GREEN:body=+1.5000:wick=0.4000; 2026-08-13:GREEN:body=+0.7900:wick=0.0700; 2026-08-14:RED:body=-0.3200:wick=0.9600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.77 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.38 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11177.85 | **NEUTRAL** |
| `B04_income` | 2796.89 | **GOOD** |
| `B05_profit_margin` | 25.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.78 vs prior_export=102.78 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.14 | **GOOD** |
| `B13_short_float` | 2.07 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.38 (this export) | prior_export=1.38 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+15**  ·  Computer Hardware
price=188.64999389648438  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.73 on 2026-08-21; prev RSI=48.51 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.51@2026-08-20 → 51.73@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=3.128 (G=4.8800 R=1.5600); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8123600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.931 (Gvol=8123600 Rvol=4207300); 2026-08-20:RED:O=185.3100,C=183.7500,body=-1.5600,vol=4207300.0; 2026-08-21:GREEN:O=183.7700,C=188.6500,body=+4.8800,vol=8123600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.017 on 2026-08-21: today_vol=8123600 / avg20=7987355 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.026 on 2026-08-21 (price=188.6500, mid=187.9825, upper=214.0069, lower=161.9581; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=188.6500 vs SMA50=177.3556 dist=+6.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=187.9825 SMA50=177.3556 SMA80=168.9896 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-09@145.3200; S2[2026-06-24→2026-07-23] low=2026-06-26@154.7400; S3[2026-07-24→2026-08-21] low=2026-07-29@156.8400 | lows=[145.32000732421875, 154.74000549316406, 156.83999633789062] span=7.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.7124066096114693 wick_frac=0.28759339038853077 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.3505614895331493 wick_frac=0.6494385104668506 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.128203372588912 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.6400:wick=3.2850; 2026-08-18:RED:body=-2.8200:wick=5.5000; 2026-08-19:RED:body=-8.5900:wick=2.4000; 2026-08-20:RED:body=-1.5600:wick=2.8900; 2026-08-21:GREEN:body=+4.8800:wick=1.9700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 249.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=249.97 vs prior_export=249.97 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.03000000000000025 (now=-2.97 vs prior=-2.94 on finviz_2026-08-24) | **BAD** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 1.22 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DRH  ·  score **+15**  ·  REIT - Hotel & Motel
price=12.819999694824219  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.65 on 2026-08-21; prev RSI=56.59 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.59@2026-08-20 → 57.65@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.1200 R=0.0600); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1831800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.020 (Gvol=1869100 Rvol=1831800); 2026-08-20:GREEN:O=12.6500,C=12.7700,body=+0.1200,vol=1869100.0; 2026-08-21:RED:O=12.8800,C=12.8200,body=-0.0600,vol=1831800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.723 on 2026-08-21: today_vol=1831800 / avg20=2534385 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.110 on 2026-08-21 (price=12.8200, mid=12.7280, upper=13.5676, lower=11.8884; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=12.8200 vs SMA50=12.3671 dist=+3.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=12.7280 SMA50=12.3671 SMA80=11.7526 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-21 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@10.4726; S2[2026-06-23→2026-07-23] low=2026-07-08@11.5700; S3[2026-07-24→2026-08-21] low=2026-08-11@11.9900 | lows=[10.472552720822522, 11.569999694824219, 11.989999771118164] span=14.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.5454576980930872 wick_frac=0.4545423019069128 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.18750149011754047 wick_frac=0.8124985098824595 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.1100:wick=0.1700; 2026-08-18:GREEN:body=+0.1200:wick=0.1100; 2026-08-19:RED:body=-0.1800:wick=0.2300; 2026-08-20:GREEN:body=+0.1200:wick=0.1000; 2026-08-21:RED:body=-0.0600:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=103.61 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.05 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1136.37 | **NEUTRAL** |
| `B04_income` | 148.78 | **GOOD** |
| `B05_profit_margin` | 13.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.56 vs prior_export=13.56 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.07 | **GOOD** |
| `B10_insider_transactions` | -1.6 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.6 vs prior=-1.6 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.35 | **GOOD** |
| `B13_short_float` | 6.11 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=103.61 (this export) | prior_export=103.61 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.05 (this export) | prior_export=1.05 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ERO  ·  score **+15**  ·  Copper
price=39.41999816894531  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.34 on 2026-08-21; prev RSI=66.11 on 2026-08-20 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 66.11@2026-08-20 → 72.34@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.11@2026-08-20 → 72.34@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 66.11@2026-08-20 → 72.34@2026-08-21 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.3800 R=0.0000); 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1681900.0; 2026-08-21:GREEN:O=37.6400,C=39.4200,body=+1.7800,vol=3835700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5517600 Rvol=0); 2026-08-20:GREEN:O=33.4200,C=36.0200,body=+2.6000,vol=1681900.0; 2026-08-21:GREEN:O=37.6400,C=39.4200,body=+1.7800,vol=3835700.0 | **GOOD** |
| `A07_rvol` | RVOL=2.957 on 2026-08-21: today_vol=3835700 / avg20=1297365 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.861 on 2026-08-21 (price=39.4200, mid=31.8400, upper=40.6480, lower=23.0320; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-21: price=39.4200 vs SMA50=28.7558 dist=+37.09% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=31.8400 SMA50=28.7558 SMA80=28.4975 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-21 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-09@25.1510; S2[2026-06-22→2026-07-21] low=2026-07-08@22.9320; S3[2026-07-23→2026-08-21] low=2026-07-29@24.7020 | lows=[25.150999069213867, 22.93199920654297, 24.70199966430664] span=9.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.9370354997257558 wick_frac=0.06296450027424413 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+0.6200:wick=0.6300; 2026-08-18:RED:body=-1.0100:wick=0.5720; 2026-08-19:RED:body=-0.3300:wick=0.8000; 2026-08-20:GREEN:body=+2.6000:wick=0.1800; 2026-08-21:GREEN:body=+1.7800:wick=0.1160 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.7 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.4 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.01999999999999602 (now=36.33 vs prior_export=36.31 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.43 | **GOOD** |
| `B13_short_float` | 5.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.7 (this export) | prior_export=12.7 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.4 (this export) | prior_export=2.4 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLV  ·  score **+15**  ·  Medical Instruments & Supplies
price=89.56999969482422  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.96 on 2026-08-21; prev RSI=62.09 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.09@2026-08-20 → 63.96@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.8000 R=0.0000); 2026-08-20:GREEN:O=87.0900,C=88.6100,body=+1.5200,vol=805100.0; 2026-08-21:GREEN:O=88.2900,C=89.5700,body=+1.2800,vol=925200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1730300 Rvol=0); 2026-08-20:GREEN:O=87.0900,C=88.6100,body=+1.5200,vol=805100.0; 2026-08-21:GREEN:O=88.2900,C=89.5700,body=+1.2800,vol=925200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.713 on 2026-08-21: today_vol=925200 / avg20=1298110 (avg window 2026-07-23→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.607 on 2026-08-21 (price=89.5700, mid=85.9675, upper=91.9047, lower=80.0303; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=89.5700 vs SMA50=80.9856 dist=+10.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=85.9675 SMA50=80.9856 SMA80=78.2257 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-20@72.7420; S2[2026-06-18→2026-07-20] low=2026-06-23@73.0400; S3[2026-07-23→2026-08-21] low=2026-07-23@76.7700 | lows=[72.74199676513672, 73.04000091552734, 76.7699966430664] span=5.54% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.6842287034422322 wick_frac=0.3157712965577678 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:RED:body=-0.9300:wick=0.5550; 2026-08-18:RED:body=-0.0500:wick=1.3900; 2026-08-19:RED:body=-0.0700:wick=2.0050; 2026-08-20:GREEN:body=+1.5200:wick=0.8900; 2026-08-21:GREEN:body=+1.2800:wick=0.4550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=33.89 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.55 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8310.0 | **NEUTRAL** |
| `B04_income` | 1434.0 | **GOOD** |
| `B05_profit_margin` | 17.26 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 95.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=95.83 vs prior_export=95.83 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.38 | **BAD** |
| `B13_short_float` | 2.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.89 (this export) | prior_export=33.89 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.55 (this export) | prior_export=2.55 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+15**  ·  Software - Infrastructure
price=357.8699951171875  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.58 on 2026-08-21; prev RSI=47.92 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.92@2026-08-20 → 51.58@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=1.200 (G=5.4700 R=4.5600); 2026-08-20:RED:O=354.1200,C=349.5600,body=-4.5600,vol=4736200.0; 2026-08-21:GREEN:O=352.4000,C=357.8700,body=+5.4700,vol=5136100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=1.084 (Gvol=5136100 Rvol=4736200); 2026-08-20:RED:O=354.1200,C=349.5600,body=-4.5600,vol=4736200.0; 2026-08-21:GREEN:O=352.4000,C=357.8700,body=+5.4700,vol=5136100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.909 on 2026-08-21: today_vol=5136100 / avg20=5652170 (avg window 2026-07-24→2026-08-20, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.003 on 2026-08-21 (price=357.8700, mid=358.0335, upper=408.4984, lower=307.5686; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=357.8700 vs SMA50=335.8386 dist=+6.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-21: SMA20=358.0335 SMA50=335.8386 SMA80=299.2571 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-21 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-27@243.0400; S2[2026-06-24→2026-07-23] low=2026-06-24@284.2800; S3[2026-07-24→2026-08-21] low=2026-07-28@308.5400 | lows=[243.0399932861328, 284.2799987792969, 308.5400085449219] span=26.95% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.3129294812704265 wick_frac=0.6870705187295735 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=0.35022970185636604 wick_frac=0.649770298143634 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1995623134478188 need>1.4; red_wick_gt_green=False 5d trail=2026-08-17:RED:body=-7.4900:wick=6.2100; 2026-08-18:GREEN:body=+3.8000:wick=6.2400; 2026-08-19:RED:body=-15.5000:wick=9.4700; 2026-08-20:RED:body=-4.5600:wick=8.4600; 2026-08-21:GREEN:body=+5.4700:wick=12.0100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.2 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10606.3 | **NEUTRAL** |
| `B04_income` | 842.8 | **GOOD** |
| `B05_profit_margin` | 7.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 365.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.2399999999999523 (now=365.21 vs prior_export=363.97 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.65 | **GOOD** |
| `B10_insider_transactions` | -0.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.82 vs prior=-0.82 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.66 | **GOOD** |
| `B13_short_float` | 2.6 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.2 (this export) | prior_export=7.2 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BJ  ·  score **+15**  ·  Discount Stores
price=96.41999816894531  pair=`2026-08-20→2026-08-21`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.86 on 2026-08-21; prev RSI=43.28 on 2026-08-20 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.28@2026-08-20 → 57.86@2026-08-21 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.28@2026-08-20 → 57.86@2026-08-21 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.28@2026-08-20 → 57.86@2026-08-21 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.8300 R=0.0000); 2026-08-20:GREEN:O=88.9100,C=91.3000,body=+2.3900,vol=5166200.0; 2026-08-21:GREEN:O=93.9800,C=96.4200,body=+2.4400,vol=4179600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-20 + 2026-08-21; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9345800 Rvol=0); 2026-08-20:GREEN:O=88.9100,C=91.3000,body=+2.3900,vol=5166200.0; 2026-08-21:GREEN:O=93.9800,C=96.4200,body=+2.4400,vol=4179600.0 | **GOOD** |
| `A07_rvol` | RVOL=2.515 on 2026-08-21: today_vol=4179600 / avg20=1661970 (avg window 2026-07-23→2026-08-20, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.260 on 2026-08-21 (price=96.4200, mid=95.1900, upper=99.9168, lower=90.4632; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-21: price=96.4200 vs SMA50=91.4606 dist=+5.42% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=95.19_50=91.46_80=91.57 on 2026-08-21: SMA20=95.1900 SMA50=91.4606 SMA80=91.5660 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-19→2026-08-21 (63 bars); S1[2026-05-19→2026-06-17] low=2026-05-27@83.6500; S2[2026-06-18→2026-07-20] low=2026-06-22@83.2120; S3[2026-07-23→2026-08-21] low=2026-08-20@88.2200 | lows=[83.6500015258789, 83.21199798583984, 88.22000122070312] span=6.02% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: GREEN body_frac=0.608485743654422 wick_frac=0.391514256345578 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-20+2026-08-21: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-17:GREEN:body=+1.8300:wick=0.3910; 2026-08-18:RED:body=-2.7500:wick=0.1600; 2026-08-19:RED:body=-0.7600:wick=2.9000; 2026-08-20:GREEN:body=+2.3900:wick=1.2530; 2026-08-21:GREEN:body=+2.4400:wick=1.9100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.31 (current export asof; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.33 (current export; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 22811.63 | **NEUTRAL** |
| `B04_income` | 594.5 | **GOOD** |
| `B05_profit_margin` | 2.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 107.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=107.25 vs prior_export=107.25 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | -14.61 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-14.61 vs prior=-14.61 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.24 | **GOOD** |
| `B13_short_float` | 7.53 | **NEUTRAL** |
| `B14_earnings_date` | 8/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.31 (this export) | prior_export=16.31 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.33 (this export) | prior_export=4.33 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-25_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.