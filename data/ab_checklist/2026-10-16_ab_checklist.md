# A+B1 Feature Checklist — 2026-10-16

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-16.csv` · prior export for Δ: `2026-10-15`
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
| 1 | FIVN | +17 | 18 | 1 | 2026-10-14→2026-10-15 | Software - Infrastructure |
| 2 | HTHT | +16 | 17 | 1 | 2026-10-14→2026-10-15 | Lodging |
| 3 | VFF | +16 | 17 | 1 | 2026-10-14→2026-10-15 | Farm Products |
| 4 | SWKS | +16 | 16 | 0 | 2026-10-14→2026-10-15 | Semiconductors |
| 5 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 6 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 7 | ADSK | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Software - Application |
| 8 | WAY | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Health Information Services |
| 9 | TENB | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Software - Infrastructure |
| 10 | FRSH | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Software - Application |
| 11 | ARHS | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Specialty Retail |
| 12 | AME | +15 | 16 | 1 | 2026-10-14→2026-10-15 | Specialty Industrial Machinery |
| 13 | CVLT | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Software - Application |
| 14 | EPAM | +15 | 17 | 2 | 2026-10-14→2026-10-15 | Information Technology Services |
| 15 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |

## Full checklist — top 15

### FIVN  ·  score **+17**  ·  Software - Infrastructure
price=35.099998474121094  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.73 on 2026-10-15; prev RSI=45.85 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.85@2026-10-14 → 52.73@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=2.221 (G=2.5100 R=1.1300); 2026-10-14:RED:O=34.2100,C=33.0800,body=-1.1300,vol=1631365.0; 2026-10-15:GREEN:O=32.5900,C=35.1000,body=+2.5100,vol=1774710.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.088 (Gvol=1774710 Rvol=1631365); 2026-10-14:RED:O=34.2100,C=33.0800,body=-1.1300,vol=1631365.0; 2026-10-15:GREEN:O=32.5900,C=35.1000,body=+2.5100,vol=1774710.0 | **GOOD** |
| `A07_rvol` | RVOL=0.738 on 2026-10-15: today_vol=1774710 / avg20=2404949 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.020 on 2026-10-15 (price=35.1000, mid=35.1640, upper=38.3399, lower=31.9881; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=35.1000 vs SMA50=33.6785 dist=+4.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=35.1640 SMA50=33.6785 SMA80=30.3294 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-15 (63 bars); S1[2026-07-20→2026-08-17] low=2026-07-23@20.7600; S2[2026-08-18→2026-09-16] low=2026-09-10@30.1000; S3[2026-09-17→2026-10-15] low=2026-09-18@32.1400 | lows=[20.760000228881836, 30.100000381469727, 32.13999938964844] span=54.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7699376309107291 wick_frac=0.23006236908927089 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.39718691338160367 wick_frac=0.6028130866183964 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.22124285164505 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+1.2900:wick=0.4450; 2026-10-12:RED:body=-1.0900:wick=0.7400; 2026-10-13:GREEN:body=+0.0580:wick=1.0019; 2026-10-14:RED:body=-1.1300:wick=1.7150; 2026-10-15:GREEN:body=+2.5100:wick=0.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.74 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.88 | **NEUTRAL** |
| `B04_income` | 59.47 | **GOOD** |
| `B05_profit_margin` | 4.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.8 vs prior_export=35.8 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -4.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.84 vs prior=-4.84 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.75 | **GOOD** |
| `B13_short_float` | 12.24 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.9 (this export) | prior_export=1.9 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+16**  ·  Lodging
price=45.939998626708984  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.11 on 2026-10-15; prev RSI=55.77 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.77@2026-10-14 → 65.11@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=7.737 (G=1.4700 R=0.1900); 2026-10-14:RED:O=44.3900,C=44.2000,body=-0.1900,vol=1243111.0; 2026-10-15:GREEN:O=44.4700,C=45.9400,body=+1.4700,vol=1929322.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.552 (Gvol=1929322 Rvol=1243111); 2026-10-14:RED:O=44.3900,C=44.2000,body=-0.1900,vol=1243111.0; 2026-10-15:GREEN:O=44.4700,C=45.9400,body=+1.4700,vol=1929322.0 | **GOOD** |
| `A07_rvol` | RVOL=1.563 on 2026-10-15: today_vol=1929322 / avg20=1234228 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.269 on 2026-10-15 (price=45.9400, mid=43.0840, upper=45.3354, lower=40.8326; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=45.9400 vs SMA50=44.3390 dist=+3.61% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=43.08_50=44.34_80=43.46 on 2026-10-15: SMA20=43.0840 SMA50=44.3390 SMA80=43.4629 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-24@40.0800; S2[2026-08-18→2026-09-16] low=2026-09-16@42.5100; S3[2026-09-17→2026-10-15] low=2026-10-02@41.2900 | lows=[40.08000183105469, 42.5099983215332, 41.290000915527344] span=6.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7538445286503215 wick_frac=0.2461554713496785 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.23312645098479742 wick_frac=0.7668735490152025 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.736884373682414 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.9600:wick=0.3300; 2026-10-12:GREEN:body=+0.2950:wick=0.6150; 2026-10-13:RED:body=-0.4100:wick=0.9100; 2026-10-14:RED:body=-0.1900:wick=0.6250; 2026-10-15:GREEN:body=+1.4700:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.56 (current export asof; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.03 (current export; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.86 | **NEUTRAL** |
| `B04_income` | 720.34 | **GOOD** |
| `B05_profit_margin` | 18.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.99 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=60.99 vs prior_export=60.99 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.21 | **GOOD** |
| `B13_short_float` | 3.59 | **NEUTRAL** |
| `B14_earnings_date` | 8/17/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.56 (this export) | prior_export=3.56 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.03 (this export) | prior_export=5.03 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VFF  ·  score **+16**  ·  Farm Products
price=2.5999999046325684  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=38.54 on 2026-10-15; prev RSI=35.18 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 35.18@2026-10-14 → 38.54@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.667 (G=0.0500 R=0.0300); 2026-10-14:RED:O=2.5900,C=2.5600,body=-0.0300,vol=737285.0; 2026-10-15:GREEN:O=2.5500,C=2.6000,body=+0.0500,vol=835664.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.133 (Gvol=835664 Rvol=737285); 2026-10-14:RED:O=2.5900,C=2.5600,body=-0.0300,vol=737285.0; 2026-10-15:GREEN:O=2.5500,C=2.6000,body=+0.0500,vol=835664.0 | **GOOD** |
| `A07_rvol` | RVOL=1.077 on 2026-10-15: today_vol=835664 / avg20=775784 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.624 on 2026-10-15 (price=2.6000, mid=2.8430, upper=3.2322, lower=2.4538; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-15: price=2.6000 vs SMA50=2.7609 dist=-5.83% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=2.8430 SMA50=2.7609 SMA80=2.4623 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-15 (63 bars); S1[2026-07-17→2026-08-17] low=2026-07-17@1.9000; S2[2026-08-18→2026-09-16] low=2026-08-19@2.4700; S3[2026-09-17→2026-10-15] low=2026-10-15@2.5300 | lows=[1.899999976158142, 2.4700000286102295, 2.5299999713897705] span=33.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.7142857142857143 wick_frac=0.2857142857142857 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.28571396133532545 wick_frac=0.7142860386646745 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6666666666666667 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.0100:wick=0.0650; 2026-10-12:GREEN:body=+0.0193:wick=0.0507; 2026-10-13:GREEN:body=+0.0400:wick=0.0199; 2026-10-14:RED:body=-0.0300:wick=0.0750; 2026-10-15:GREEN:body=+0.0500:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=294.74 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=13.63 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 230.57 | **NEUTRAL** |
| `B04_income` | 22.71 | **GOOD** |
| `B05_profit_margin` | 9.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.83 vs prior_export=5.83 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.67 vs prior=0.67 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.12 | **GOOD** |
| `B13_short_float` | 4.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=294.74 (this export) | prior_export=294.74 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=13.63 (this export) | prior_export=13.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=79.0999984741211  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.48 on 2026-10-15; prev RSI=43.33 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.33@2026-10-14 → 47.48@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.6800 R=0.0000); 2026-10-14:GREEN:O=75.5000,C=76.8900,body=+1.3900,vol=5128746.0; 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10897948 Rvol=0); 2026-10-14:GREEN:O=75.5000,C=76.8900,body=+1.3900,vol=5128746.0; 2026-10-15:GREEN:O=75.8100,C=79.1000,body=+3.2900,vol=5769202.0 | **GOOD** |
| `A07_rvol` | RVOL=0.731 on 2026-10-15: today_vol=5769202 / avg20=7892153 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.477 on 2026-10-15 (price=79.1000, mid=83.9920, upper=94.2480, lower=73.7360; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=79.1000 vs SMA50=77.1322 dist=+2.55% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=83.9920 SMA50=77.1322 SMA80=71.9984 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-16@55.5500; S2[2026-08-18→2026-09-16] low=2026-08-25@63.0600; S3[2026-09-17→2026-10-15] low=2026-10-13@73.7600 | lows=[55.54999923706055, 63.060001373291016, 73.76000213623047] span=32.78% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8611556980962559 wick_frac=0.13884430190374406 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-3.0000:wick=2.6489; 2026-10-12:RED:body=-1.4000:wick=1.9000; 2026-10-13:GREEN:body=+2.8000:wick=0.6850; 2026-10-14:GREEN:body=+1.3900:wick=0.3700; 2026-10-15:GREEN:body=+3.2900:wick=0.2380 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+16**  ·  Biotechnology
price=16.549999237060547  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.72 on 2026-10-12; prev RSI=57.75 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.75@2026-10-09 → 55.72@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=8.166 (G=0.4900 R=0.0600); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.297 (Gvol=849222 Rvol=654764); 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0; 2026-10-12:RED:O=16.6100,C=16.5500,body=-0.0600,vol=654764.0 | **GOOD** |
| `A07_rvol` | RVOL=0.785 on 2026-10-12: today_vol=654764 / avg20=834281 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.229 on 2026-10-12 (price=16.5500, mid=16.3710, upper=17.1537, lower=15.5883; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=16.5500 vs SMA50=16.1181 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.37_50=16.12_80=16.12 on 2026-10-12: SMA20=16.3710 SMA50=16.1181 SMA80=16.1191 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-31@14.3630; S2[2026-08-13→2026-09-11] low=2026-08-14@14.9010; S3[2026-09-14→2026-10-12] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5764713538160505 wick_frac=0.42352864618394953 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.2754882213854103 wick_frac=0.7245117786145897 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.16647593616886 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=422.0899963378906  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.14 on 2026-10-12; prev RSI=59.13 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=17.8700 R=0.0000); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2928672 Rvol=0); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A07_rvol` | RVOL=0.737 on 2026-10-12: today_vol=869447 / avg20=1180302 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.925 on 2026-10-12 (price=422.0900, mid=401.9415, upper=423.7173, lower=380.1657; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=422.0900 vs SMA50=400.5118 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=401.9415 SMA50=400.5118 SMA80=398.6746 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-28@362.9800; S2[2026-08-13→2026-09-11] low=2026-08-17@388.1200; S3[2026-09-14→2026-10-12] low=2026-10-01@375.7400 | lows=[362.9800109863281, 388.1199951171875, 375.739990234375] span=6.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6505804361528325 wick_frac=0.34941956384716766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+3.4100:wick=3.1374; 2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+15**  ·  Software - Application
price=245.16000366210938  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.07 on 2026-10-15; prev RSI=55.48 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.48@2026-10-14 → 65.07@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=11.7700 R=0.0000); 2026-10-14:GREEN:O=230.3500,C=231.1400,body=+0.7900,vol=1371131.0; 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3793779 Rvol=0); 2026-10-14:GREEN:O=230.3500,C=231.1400,body=+0.7900,vol=1371131.0; 2026-10-15:GREEN:O=234.1800,C=245.1600,body=+10.9800,vol=2422648.0 | **GOOD** |
| `A07_rvol` | RVOL=1.187 on 2026-10-15: today_vol=2422648 / avg20=2040941 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.989 on 2026-10-15 (price=245.1600, mid=221.6555, upper=245.4298, lower=197.8812; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=245.1600 vs SMA50=233.3650 dist=+5.05% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=221.66_50=233.36_80=225.10 on 2026-10-15: SMA20=221.6555 SMA50=233.3650 SMA80=225.0982 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@202.3700; S2[2026-08-18→2026-09-16] low=2026-09-09@206.3300; S3[2026-09-17→2026-10-15] low=2026-09-29@199.1801 | lows=[202.3699951171875, 206.3300018310547, 199.1800994873047] span=3.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.5379553925498008 wick_frac=0.4620446074501992 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.1500:wick=4.9200; 2026-10-12:RED:body=-1.1630:wick=6.7920; 2026-10-13:RED:body=-2.3100:wick=3.4000; 2026-10-14:GREEN:body=+0.7900:wick=2.9300; 2026-10-15:GREEN:body=+10.9800:wick=1.7350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.0 | **GOOD** |
| `B13_short_float` | 4.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+15**  ·  Health Information Services
price=28.239999771118164  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.34 on 2026-10-15; prev RSI=59.22 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.22@2026-10-14 → 66.34@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.1600 R=0.0000); 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0; 2026-10-15:GREEN:O=26.9800,C=28.2400,body=+1.2600,vol=3547027.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7272027 Rvol=0); 2026-10-14:GREEN:O=25.9700,C=26.8700,body=+0.9000,vol=3725000.0; 2026-10-15:GREEN:O=26.9800,C=28.2400,body=+1.2600,vol=3547027.0 | **GOOD** |
| `A07_rvol` | RVOL=1.833 on 2026-10-15: today_vol=3547027 / avg20=1934585 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.308 on 2026-10-15 (price=28.2400, mid=25.8640, upper=27.6805, lower=24.0475; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=28.2400 vs SMA50=25.3060 dist=+11.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=25.8640 SMA50=25.3060 SMA80=24.0052 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@20.1410; S2[2026-08-18→2026-09-16] low=2026-09-10@22.9550; S3[2026-09-17→2026-10-15] low=2026-09-25@24.3250 | lows=[20.141000747680664, 22.954999923706055, 24.325000762939453] span=20.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8391256238221849 wick_frac=0.16087437617781514 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.3000:wick=0.4850; 2026-10-12:RED:body=-0.3500:wick=0.4125; 2026-10-13:RED:body=-0.5700:wick=1.0700; 2026-10-14:GREEN:body=+0.9000:wick=0.2000; 2026-10-15:GREEN:body=+1.2600:wick=0.2050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=33.68 vs prior_export=33.68 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TENB  ·  score **+15**  ·  Software - Infrastructure
price=39.31999969482422  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.84 on 2026-10-15; prev RSI=52.46 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.46@2026-10-14 → 55.84@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.005 (G=1.9700 R=1.9600); 2026-10-14:RED:O=40.0800,C=38.1200,body=-1.9600,vol=2410560.0; 2026-10-15:GREEN:O=37.3500,C=39.3200,body=+1.9700,vol=4873720.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=2.022 (Gvol=4873720 Rvol=2410560); 2026-10-14:RED:O=40.0800,C=38.1200,body=-1.9600,vol=2410560.0; 2026-10-15:GREEN:O=37.3500,C=39.3200,body=+1.9700,vol=4873720.0 | **GOOD** |
| `A07_rvol` | RVOL=1.526 on 2026-10-15: today_vol=4873720 / avg20=3193561 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.403 on 2026-10-15 (price=39.3200, mid=37.4483, upper=42.0947, lower=32.8018; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=39.3200 vs SMA50=36.2841 dist=+8.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=37.4483 SMA50=36.2841 SMA80=35.9376 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-30@27.5000; S2[2026-08-18→2026-09-16] low=2026-09-11@29.9700; S3[2026-09-17→2026-10-15] low=2026-09-28@32.5000 | lows=[27.5, 29.969999313354492, 32.5] span=18.18% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.6852178158967391 wick_frac=0.31478218410326086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.7025097691767093 wick_frac=0.2974902308232907 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0051011769102165 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+2.4700:wick=0.1200; 2026-10-12:RED:body=-1.7100:wick=0.2660; 2026-10-13:GREEN:body=+0.9400:wick=0.9000; 2026-10-14:RED:body=-1.9600:wick=0.8300; 2026-10-15:GREEN:body=+1.9700:wick=0.9050 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.28 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1043.54 | **NEUTRAL** |
| `B04_income` | 6.74 | **GOOD** |
| `B05_profit_margin` | 0.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.0 vs prior_export=36.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 16.77 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.28 (this export) | prior_export=8.28 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+15**  ·  Software - Application
price=14.25  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.85 on 2026-10-15; prev RSI=60.32 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.32@2026-10-14 → 66.85@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=4.462 (G=0.5800 R=0.1300); 2026-10-14:RED:O=13.8200,C=13.6900,body=-0.1300,vol=4573770.0; 2026-10-15:GREEN:O=13.6700,C=14.2500,body=+0.5800,vol=5580073.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.220 (Gvol=5580073 Rvol=4573770); 2026-10-14:RED:O=13.8200,C=13.6900,body=-0.1300,vol=4573770.0; 2026-10-15:GREEN:O=13.6700,C=14.2500,body=+0.5800,vol=5580073.0 | **GOOD** |
| `A07_rvol` | RVOL=0.732 on 2026-10-15: today_vol=5580073 / avg20=7621307 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.876 on 2026-10-15 (price=14.2500, mid=13.1535, upper=14.4054, lower=11.9016; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=14.2500 vs SMA50=12.8653 dist=+10.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=13.1535 SMA50=12.8653 SMA80=11.9758 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-23@9.7650; S2[2026-08-18→2026-09-16] low=2026-09-10@11.7040; S3[2026-09-17→2026-10-15] low=2026-09-28@11.9600 | lows=[9.765000343322754, 11.704000473022461, 11.960000038146973] span=22.48% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.659090736680802 wick_frac=0.340909263319198 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.24074096967492065 wick_frac=0.7592590303250794 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.461533947107802 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+0.2000:wick=0.0750; 2026-10-12:RED:body=-0.0800:wick=0.3450; 2026-10-13:DOJI:body=+0.0000:wick=0.3000; 2026-10-14:RED:body=-0.1300:wick=0.4100; 2026-10-15:GREEN:body=+0.5800:wick=0.3000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.64 vs prior_export=14.64 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.42 | **GOOD** |
| `B13_short_float` | 11.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARHS  ·  score **+15**  ·  Specialty Retail
price=10.130000114440918  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.23 on 2026-10-15; prev RSI=50.25 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.25@2026-10-14 → 61.23@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.25@2026-10-14 → 61.23@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.25@2026-10-14 → 61.23@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.7000 R=0.1400); 2026-10-14:RED:O=9.4400,C=9.3000,body=-0.1400,vol=1134837.0; 2026-10-15:GREEN:O=9.4300,C=10.1300,body=+0.7000,vol=2203938.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.942 (Gvol=2203938 Rvol=1134837); 2026-10-14:RED:O=9.4400,C=9.3000,body=-0.1400,vol=1134837.0; 2026-10-15:GREEN:O=9.4300,C=10.1300,body=+0.7000,vol=2203938.0 | **GOOD** |
| `A07_rvol` | RVOL=1.365 on 2026-10-15: today_vol=2203938 / avg20=1614982 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.550 on 2026-10-15 (price=10.1300, mid=9.5260, upper=10.6249, lower=8.4271; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=10.1300 vs SMA50=9.1464 dist=+10.75% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=9.5260 SMA50=9.1464 SMA80=8.6721 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-15 (63 bars); S1[2026-07-16→2026-08-17] low=2026-07-23@7.0600; S2[2026-08-18→2026-09-16] low=2026-09-16@7.0670; S3[2026-09-17→2026-10-15] low=2026-09-17@7.2200 | lows=[7.059999942779541, 7.066999912261963, 7.21999979019165] span=2.27% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.8045975877517508 wick_frac=0.19540241224824914 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.5957405363288099 wick_frac=0.4042594636711902 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.000020435967302 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-0.0600:wick=0.2600; 2026-10-12:RED:body=-0.0420:wick=0.2680; 2026-10-13:GREEN:body=+0.3180:wick=0.1700; 2026-10-14:RED:body=-0.1400:wick=0.0950; 2026-10-15:GREEN:body=+0.7000:wick=0.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.52 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.26 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1408.59 | **NEUTRAL** |
| `B04_income` | 69.18 | **GOOD** |
| `B05_profit_margin` | 4.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.54 vs prior_export=10.54 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 12.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.52 (this export) | prior_export=72.52 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+15**  ·  Specialty Industrial Machinery
price=248.52000427246094  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.47 on 2026-10-15; prev RSI=50.77 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.77@2026-10-14 → 53.47@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=1.914 (G=4.0000 R=2.0900); 2026-10-14:RED:O=248.9200,C=246.8300,body=-2.0900,vol=1378770.0; 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.292 (Gvol=1780759 Rvol=1378770); 2026-10-14:RED:O=248.9200,C=246.8300,body=-2.0900,vol=1378770.0; 2026-10-15:GREEN:O=244.5200,C=248.5200,body=+4.0000,vol=1780759.0 | **GOOD** |
| `A07_rvol` | RVOL=1.308 on 2026-10-15: today_vol=1780759 / avg20=1361144 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.026 on 2026-10-15 (price=248.5200, mid=248.3245, upper=255.8084, lower=240.8406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-15: price=248.5200 vs SMA50=244.8364 dist=+1.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=248.3245 SMA50=244.8364 SMA80=242.3984 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-17@229.8900; S2[2026-08-18→2026-09-16] low=2026-09-01@229.6000; S3[2026-09-17→2026-10-15] low=2026-09-17@231.8700 | lows=[229.88999938964844, 229.60000610351562, 231.8699951171875] span=0.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.5730662533528914 wick_frac=0.4269337466471086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.42609518002575797 wick_frac=0.573904819974242 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.91387895159524 need>1.4; red_wick_gt_green=False 5d trail=2026-10-09:GREEN:body=+0.2400:wick=3.1900; 2026-10-12:GREEN:body=+1.3700:wick=2.8450; 2026-10-13:GREEN:body=+0.8400:wick=3.1900; 2026-10-14:RED:body=-2.0900:wick=2.8150; 2026-10-15:GREEN:body=+4.0000:wick=2.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVLT  ·  score **+15**  ·  Software - Application
price=158.91000366210938  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.71 on 2026-10-15; prev RSI=57.37 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.37@2026-10-14 → 61.71@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.37@2026-10-14 → 61.71@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.37@2026-10-14 → 61.71@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=2.029 (G=5.5600 R=2.7400); 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0; 2026-10-15:GREEN:O=153.3500,C=158.9100,body=+5.5600,vol=658358.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=1.207 (Gvol=658358 Rvol=545597); 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0; 2026-10-15:GREEN:O=153.3500,C=158.9100,body=+5.5600,vol=658358.0 | **GOOD** |
| `A07_rvol` | RVOL=1.080 on 2026-10-15: today_vol=658358 / avg20=609433 (avg window 2026-09-17→2026-10-14, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.887 on 2026-10-15 (price=158.9100, mid=150.9370, upper=159.9211, lower=141.9529; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=158.9100 vs SMA50=142.9074 dist=+11.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=150.9370 SMA50=142.9074 SMA80=141.6819 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-30@115.3100; S2[2026-08-18→2026-09-16] low=2026-09-02@125.9000; S3[2026-09-17→2026-10-15] low=2026-09-18@141.0000 | lows=[115.30999755859375, 125.9000015258789, 141.0] span=22.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=0.9258939279978858 wick_frac=0.07410607200211412 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.4528932414948057 wick_frac=0.5471067585051943 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.029192121134494 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:GREEN:body=+4.4700:wick=2.8850; 2026-10-12:RED:body=-3.2300:wick=1.2300; 2026-10-13:GREEN:body=+4.5000:wick=1.2000; 2026-10-14:RED:body=-2.7400:wick=3.3100; 2026-10-15:GREEN:body=+5.5600:wick=0.4450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.16 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1215.84 | **NEUTRAL** |
| `B04_income` | 68.3 | **GOOD** |
| `B05_profit_margin` | 5.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 160.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=160.75 vs prior_export=160.75 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -15.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.45 vs prior=-15.45 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 8.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.16 (this export) | prior_export=22.16 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=122.26000213623047  pair=`2026-10-14→2026-10-15`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.09 on 2026-10-15; prev RSI=55.38 on 2026-10-14 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.38@2026-10-14 → 65.09@2026-10-15 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_body_sum/RED_body_sum=4.006 (G=6.2900 R=1.5700); 2026-10-14:RED:O=116.2000,C=114.6300,body=-1.5700,vol=559019.0; 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-14 + 2026-10-15; ratio=GREEN_vol/RED_vol=2.617 (Gvol=1462982 Rvol=559019); 2026-10-14:RED:O=116.2000,C=114.6300,body=-1.5700,vol=559019.0; 2026-10-15:GREEN:O=115.9700,C=122.2600,body=+6.2900,vol=1462982.0 | **GOOD** |
| `A07_rvol` | RVOL=1.633 on 2026-10-15: today_vol=1462982 / avg20=895921 (avg window 2026-09-17→2026-10-14, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.416 on 2026-10-15 (price=122.2600, mid=111.4875, upper=119.0931, lower=103.8819; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-15: price=122.2600 vs SMA50=110.7090 dist=+10.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-15: SMA20=111.4875 SMA50=110.7090 SMA80=102.5095 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-15 (63 bars); S1[2026-07-15→2026-08-17] low=2026-07-23@84.0300; S2[2026-08-18→2026-09-16] low=2026-08-18@99.4700; S3[2026-09-17→2026-10-15] low=2026-09-28@104.3300 | lows=[84.02999877929688, 99.47000122070312, 104.33000183105469] span=24.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: GREEN body_frac=1.0413907222029242 wick_frac=-0.04139072220292418 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-14+2026-10-15: RED body_frac=0.5547698512403824 wick_frac=0.44523014875961764 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.006370788646293 need>1.4; red_wick_gt_green=True 5d trail=2026-10-09:RED:body=-1.4600:wick=2.4457; 2026-10-12:RED:body=-1.9000:wick=2.3450; 2026-10-13:GREEN:body=+1.3200:wick=1.9100; 2026-10-14:RED:body=-1.5700:wick=1.2600; 2026-10-15:GREEN:body=+6.2900:wick=-0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
price=51.970001220703125  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.48 on 2026-10-12; prev RSI=42.64 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.64@2026-10-09 → 51.48@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6400 R=0.0000); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1914338 Rvol=0); 2026-10-09:GREEN:O=49.6800,C=50.0400,body=+0.3600,vol=1196835.0; 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0 | **GOOD** |
| `A07_rvol` | RVOL=0.622 on 2026-10-12: today_vol=717503 / avg20=1153282 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.033 on 2026-10-12 (price=51.9700, mid=52.1205, upper=56.6668, lower=47.5742; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=51.9700 vs SMA50=51.7088 dist=+0.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=52.1205 SMA50=51.7088 SMA80=50.5664 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-08-12@47.8600; S2[2026-08-13→2026-09-11] low=2026-08-13@48.1100; S3[2026-09-14→2026-10-12] low=2026-10-02@48.3600 | lows=[47.86000061035156, 48.11000061035156, 48.36000061035156] span=1.04% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.37251635024746754 wick_frac=0.6274836497525325 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.1800:wick=1.0800; 2026-10-07:RED:body=-1.8300:wick=0.3050; 2026-10-08:RED:body=-0.2900:wick=0.9400; 2026-10-09:GREEN:body=+0.3600:wick=1.6550; 2026-10-12:GREEN:body=+1.2800:wick=0.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-15) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-15) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-15) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-16_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.