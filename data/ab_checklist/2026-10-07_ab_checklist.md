# A+B1 Feature Checklist — 2026-10-07

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-07.csv` · prior export for Δ: `2026-10-06`
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
| 1 | MH | +17 | 18 | 1 | 2026-10-05→2026-10-06 | Education & Training Services |
| 2 | ESTC | +17 | 18 | 1 | 2026-10-05→2026-10-06 | Software - Application |
| 3 | ELV | +16 | 17 | 1 | 2026-10-05→2026-10-06 | Healthcare Plans |
| 4 | HUM | +16 | 17 | 1 | 2026-10-05→2026-10-06 | Healthcare Plans |
| 5 | VEEV | +16 | 17 | 1 | 2026-10-05→2026-10-06 | Health Information Services |
| 6 | FUTU | +16 | 16 | 0 | 2026-10-05→2026-10-06 | Capital Markets |
| 7 | CORT | +16 | 18 | 2 | 2026-10-05→2026-10-06 | Biotechnology |
| 8 | SOLS | +16 | 17 | 1 | 2026-10-05→2026-10-06 | Specialty Chemicals |
| 9 | ANET | +15 | 18 | 3 | 2026-10-05→2026-10-06 | Computer Hardware |
| 10 | ASH | +15 | 16 | 1 | 2026-10-05→2026-10-06 | Specialty Chemicals |
| 11 | PD | +15 | 16 | 1 | 2026-10-05→2026-10-06 | Software - Application |
| 12 | PODD | +15 | 17 | 2 | 2026-10-05→2026-10-06 | Medical Devices |
| 13 | ABBV | +15 | 17 | 2 | 2026-10-05→2026-10-06 | Drug Manufacturers - General |
| 14 | ETN | +15 | 17 | 2 | 2026-10-05→2026-10-06 | Specialty Industrial Machinery |
| 15 | AVPT | +15 | 18 | 3 | 2026-10-05→2026-10-06 | Software - Infrastructure |

## Full checklist — top 15

### MH  ·  score **+17**  ·  Education & Training Services
price=12.979999542236328  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.50 on 2026-10-06; prev RSI=46.45 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.45@2026-10-05 → 51.50@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.45@2026-10-05 → 51.50@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.45@2026-10-05 → 51.50@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=4.143 (G=0.2900 R=0.0700); 2026-10-05:RED:O=12.6600,C=12.5900,body=-0.0700,vol=386252.0; 2026-10-06:GREEN:O=12.6900,C=12.9800,body=+0.2900,vol=495261.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.282 (Gvol=495261 Rvol=386252); 2026-10-05:RED:O=12.6600,C=12.5900,body=-0.0700,vol=386252.0; 2026-10-06:GREEN:O=12.6900,C=12.9800,body=+0.2900,vol=495261.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-10-06: today_vol=495261 / avg20=617581 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.028 on 2026-10-06 (price=12.9800, mid=12.9540, upper=13.8808, lower=12.0272; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=12.9800 vs SMA50=12.6282 dist=+2.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=12.9540 SMA50=12.6282 SMA80=11.7203 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-07-23@8.9700; S2[2026-08-07→2026-09-04] low=2026-08-11@11.2100; S3[2026-09-08→2026-10-06] low=2026-09-10@11.9950 | lows=[8.970000267028809, 11.210000038146973, 11.994999885559082] span=33.72% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.5647524436198447 wick_frac=0.4352475563801554 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.14622853887009565 wick_frac=0.8537714611299043 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.142874659400545 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+0.4900:wick=0.1900; 2026-10-01:RED:body=-0.2500:wick=0.4400; 2026-10-02:RED:body=-0.2600:wick=0.1700; 2026-10-05:RED:body=-0.0700:wick=0.4087; 2026-10-06:GREEN:body=+0.2900:wick=0.2235 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.59 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2116.97 | **NEUTRAL** |
| `B04_income` | 92.68 | **GOOD** |
| `B05_profit_margin` | 4.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.92 vs prior_export=17.92 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.23 | **BAD** |
| `B13_short_float` | 9.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.59 (this export) | prior_export=23.59 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESTC  ·  score **+17**  ·  Software - Application
price=94.5999984741211  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.61 on 2026-10-06; prev RSI=63.37 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.37@2026-10-05 → 62.61@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.37@2026-10-05 → 62.61@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.37@2026-10-05 → 62.61@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=1.608 (G=2.3800 R=1.4800); 2026-10-05:GREEN:O=92.5300,C=94.9100,body=+2.3800,vol=2572276.0; 2026-10-06:RED:O=96.0800,C=94.6000,body=-1.4800,vol=1524497.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.687 (Gvol=2572276 Rvol=1524497); 2026-10-05:GREEN:O=92.5300,C=94.9100,body=+2.3800,vol=2572276.0; 2026-10-06:RED:O=96.0800,C=94.6000,body=-1.4800,vol=1524497.0 | **GOOD** |
| `A07_rvol` | RVOL=0.774 on 2026-10-06: today_vol=1524497 / avg20=1968738 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.800 on 2026-10-06 (price=94.6000, mid=88.9245, upper=96.0225, lower=81.8265; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=94.6000 vs SMA50=83.9538 dist=+12.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=88.9245 SMA50=83.9538 SMA80=74.8348 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-07-23@56.0600; S2[2026-08-07→2026-09-04] low=2026-08-07@71.2100; S3[2026-09-08→2026-10-06] low=2026-09-11@82.1500 | lows=[56.060001373291016, 71.20999908447266, 82.1500015258789] span=46.54% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.6806423459591552 wick_frac=0.3193576540408448 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.34498905386635936 wick_frac=0.6550109461336406 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.608107759798337 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+2.8000:wick=1.0300; 2026-10-01:RED:body=-1.2500:wick=2.3700; 2026-10-02:GREEN:body=+1.3600:wick=0.5250; 2026-10-05:GREEN:body=+2.3800:wick=1.1167; 2026-10-06:RED:body=-1.4800:wick=2.8100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.84 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.78 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.16 | **NEUTRAL** |
| `B04_income` | 375.64 | **GOOD** |
| `B05_profit_margin` | 20.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.42 vs prior_export=110.42 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | -8.05 | **BAD** |
| `B11_insider_tx_delta` | delta=8.55 (now=-8.05 vs prior=-16.6 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 3.19 | **GOOD** |
| `B13_short_float` | 4.61 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.84 (this export) | prior_export=19.84 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=400.0899963378906  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.89 on 2026-10-06; prev RSI=46.44 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.44@2026-10-05 → 50.89@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.44@2026-10-05 → 50.89@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.44@2026-10-05 → 50.89@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.2500 R=0.0000); 2026-10-05:GREEN:O=387.6700,C=394.5100,body=+6.8400,vol=712036.0; 2026-10-06:GREEN:O=396.6800,C=400.0900,body=+3.4100,vol=1005651.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1717687 Rvol=0); 2026-10-05:GREEN:O=387.6700,C=394.5100,body=+6.8400,vol=712036.0; 2026-10-06:GREEN:O=396.6800,C=400.0900,body=+3.4100,vol=1005651.0 | **GOOD** |
| `A07_rvol` | RVOL=0.821 on 2026-10-06: today_vol=1005651 / avg20=1224879 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.111 on 2026-10-06 (price=400.0900, mid=402.7595, upper=426.7242, lower=378.7948; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=400.0900 vs SMA50=398.0404 dist=+0.51% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=402.76_50=398.04_80=398.56 on 2026-10-06: SMA20=402.7595 SMA50=398.0404 SMA80=398.5556 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-07-28@362.9800; S2[2026-08-07→2026-09-04] low=2026-08-12@385.8400; S3[2026-09-08→2026-10-06] low=2026-10-01@375.7400 | lows=[362.9800109863281, 385.8399963378906, 375.739990234375] span=6.30% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.5715304978927573 wick_frac=0.4284695021072427 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:RED:body=-3.0900:wick=10.0100; 2026-10-01:RED:body=-6.3300:wick=8.3000; 2026-10-02:GREEN:body=+4.3400:wick=3.0500; 2026-10-05:GREEN:body=+6.8400:wick=4.1525; 2026-10-06:GREEN:body=+3.4100:wick=3.1374 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.08999999999997499 (now=447.86 vs prior_export=447.95 on finviz_2026-10-06) | **BAD** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+16**  ·  Healthcare Plans
price=404.2699890136719  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.10 on 2026-10-06; prev RSI=59.00 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.00@2026-10-05 → 59.10@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.00@2026-10-05 → 59.10@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.00@2026-10-05 → 59.10@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=18.787 (G=14.0900 R=0.7500); 2026-10-05:GREEN:O=389.9800,C=404.0700,body=+14.0900,vol=1517892.0; 2026-10-06:RED:O=405.0200,C=404.2700,body=-0.7500,vol=1982822.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=0.766 (Gvol=1517892 Rvol=1982822); 2026-10-05:GREEN:O=389.9800,C=404.0700,body=+14.0900,vol=1517892.0; 2026-10-06:RED:O=405.0200,C=404.2700,body=-0.7500,vol=1982822.0 | **BAD** |
| `A07_rvol` | RVOL=1.847 on 2026-10-06: today_vol=1982822 / avg20=1073798 (avg window 2026-09-08→2026-10-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.612 on 2026-10-06 (price=404.2700, mid=390.3855, upper=413.0721, lower=367.6989; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=404.2700 vs SMA50=386.4690 dist=+4.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=390.3855 SMA50=386.4690 SMA80=385.7181 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-08-05@353.6900; S2[2026-08-07→2026-09-04] low=2026-08-07@363.7800; S3[2026-09-08→2026-10-06] low=2026-09-23@369.1300 | lows=[353.69000244140625, 363.7799987792969, 369.1300048828125] span=4.37% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.591271685997892 wick_frac=0.4087283140021079 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.0625 wick_frac=0.9375 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=18.786661783854168 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:RED:body=-3.9700:wick=8.8600; 2026-10-01:RED:body=-0.3200:wick=10.3100; 2026-10-02:GREEN:body=+9.8200:wick=1.6700; 2026-10-05:GREEN:body=+14.0900:wick=9.7400; 2026-10-06:RED:body=-0.7500:wick=11.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 425.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=425.46 vs prior_export=425.46 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 2.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+16**  ·  Health Information Services
price=283.5  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.41 on 2026-10-06; prev RSI=62.38 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.38@2026-10-05 → 62.41@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.38@2026-10-05 → 62.41@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.38@2026-10-05 → 62.41@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=4.592 (G=8.4500 R=1.8400); 2026-10-05:GREEN:O=275.0000,C=283.4500,body=+8.4500,vol=1324143.0; 2026-10-06:RED:O=285.3400,C=283.5000,body=-1.8400,vol=1174105.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.128 (Gvol=1324143 Rvol=1174105); 2026-10-05:GREEN:O=275.0000,C=283.4500,body=+8.4500,vol=1324143.0; 2026-10-06:RED:O=285.3400,C=283.5000,body=-1.8400,vol=1174105.0 | **GOOD** |
| `A07_rvol` | RVOL=0.774 on 2026-10-06: today_vol=1174105 / avg20=1517210 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.677 on 2026-10-06 (price=283.5000, mid=271.0005, upper=289.4621, lower=252.5389; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=283.5000 vs SMA50=254.4592 dist=+11.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=271.0005 SMA50=254.4592 SMA80=226.0951 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-06 (63 bars); S1[2026-07-08→2026-08-06] low=2026-07-23@179.2500; S2[2026-08-07→2026-09-04] low=2026-08-07@223.3100; S3[2026-09-08→2026-10-06] low=2026-09-21@257.8700 | lows=[179.25, 223.30999755859375, 257.8699951171875] span=43.86% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.9027775513597579 wick_frac=0.09722244864024206 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.24187537358638922 wick_frac=0.7581246264136108 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.592407078765363 need>1.4; red_wick_gt_green=False 5d trail=2026-09-30:GREEN:body=+7.6000:wick=5.1100; 2026-10-01:RED:body=-9.4700:wick=0.1400; 2026-10-02:RED:body=-9.7700:wick=1.9850; 2026-10-05:GREEN:body=+8.4500:wick=0.9100; 2026-10-06:RED:body=-1.8400:wick=5.7672 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.02999999999999997 (now=-0.38 vs prior=-0.41 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 3.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FUTU  ·  score **+16**  ·  Capital Markets
price=113.20999908447266  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.28 on 2026-10-06; prev RSI=47.49 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.49@2026-10-05 → 52.28@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.49@2026-10-05 → 52.28@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.49@2026-10-05 → 52.28@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.2900 R=0.0000); 2026-10-05:GREEN:O=103.1200,C=109.7000,body=+6.5800,vol=2000485.0; 2026-10-06:GREEN:O=109.5000,C=113.2100,body=+3.7100,vol=1210077.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3210562 Rvol=0); 2026-10-05:GREEN:O=103.1200,C=109.7000,body=+6.5800,vol=2000485.0; 2026-10-06:GREEN:O=109.5000,C=113.2100,body=+3.7100,vol=1210077.0 | **GOOD** |
| `A07_rvol` | RVOL=1.295 on 2026-10-06: today_vol=1210077 / avg20=934660 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.331 on 2026-10-06 (price=113.2100, mid=111.1405, upper=117.3957, lower=104.8853; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=113.2100 vs SMA50=112.1192 dist=+0.97% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=111.14_50=112.12_80=106.85 on 2026-10-06: SMA20=111.1405 SMA50=112.1192 SMA80=106.8467 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-06 (63 bars); S1[2026-07-09→2026-08-06] low=2026-07-17@92.5410; S2[2026-08-07→2026-09-04] low=2026-08-13@102.8500; S3[2026-09-08→2026-10-06] low=2026-10-02@101.1100 | lows=[92.54100036621094, 102.8499984741211, 101.11000061035156] span=11.14% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.7592741174119091 wick_frac=0.24072588258809086 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-30:RED:body=-3.9100:wick=0.4200; 2026-10-01:RED:body=-4.1700:wick=0.9500; 2026-10-02:RED:body=-4.4300:wick=1.2800; 2026-10-05:GREEN:body=+6.5800:wick=1.3700; 2026-10-06:GREEN:body=+3.7100:wick=1.6600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.72 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.86 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3314.94 | **NEUTRAL** |
| `B04_income` | 1422.92 | **GOOD** |
| `B05_profit_margin` | 42.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.09000000000000341 (now=153.21 vs prior_export=153.12 on finviz_2026-10-06) | **GOOD** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.31 | **GOOD** |
| `B13_short_float` | 5.06 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.72 (this export) | prior_export=11.72 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.86 (this export) | prior_export=16.86 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CORT  ·  score **+16**  ·  Biotechnology
price=119.56999969482422  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.52 on 2026-10-06; prev RSI=61.94 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.94@2026-10-05 → 57.52@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.94@2026-10-05 → 57.52@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.94@2026-10-05 → 57.52@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=2.449 (G=6.4400 R=2.6300); 2026-10-05:GREEN:O=115.5900,C=122.0300,body=+6.4400,vol=995820.0; 2026-10-06:RED:O=122.2000,C=119.5700,body=-2.6300,vol=786536.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.266 (Gvol=995820 Rvol=786536); 2026-10-05:GREEN:O=115.5900,C=122.0300,body=+6.4400,vol=995820.0; 2026-10-06:RED:O=122.2000,C=119.5700,body=-2.6300,vol=786536.0 | **GOOD** |
| `A07_rvol` | RVOL=0.442 on 2026-10-06: today_vol=786536 / avg20=1781084 (avg window 2026-09-08→2026-10-05, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.783 on 2026-10-06 (price=119.5700, mid=114.9635, upper=120.8454, lower=109.0816; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=119.5700 vs SMA50=113.6336 dist=+5.22% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=114.9635 SMA50=113.6336 SMA80=103.6519 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-07-13@84.8800; S2[2026-08-07→2026-09-04] low=2026-08-07@108.0800; S3[2026-09-08→2026-10-06] low=2026-09-08@108.2500 | lows=[84.87999725341797, 108.08000183105469, 108.25] span=27.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.8809855803351507 wick_frac=0.11901441966484926 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.3539747312733352 wick_frac=0.6460252687266649 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.4486726870291453 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+0.4300:wick=3.6600; 2026-10-01:RED:body=-0.2600:wick=3.7800; 2026-10-02:GREEN:body=+1.8500:wick=1.5650; 2026-10-05:GREEN:body=+6.4400:wick=0.8700; 2026-10-06:RED:body=-2.6300:wick=4.7999 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1381.48 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.83 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 830.81 | **NEUTRAL** |
| `B04_income` | 54.2 | **GOOD** |
| `B05_profit_margin` | 6.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=141.0 vs prior_export=141.0 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -6.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.3100000000000005 (now=-6.34 vs prior=-6.65 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 2.39 | **GOOD** |
| `B13_short_float` | 9.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1381.48 (this export) | prior_export=1381.48 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.83 (this export) | prior_export=15.83 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLS  ·  score **+16**  ·  Specialty Chemicals
price=61.27000045776367  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.13 on 2026-10-06; prev RSI=47.83 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.83@2026-10-05 → 58.13@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.83@2026-10-05 → 58.13@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.83@2026-10-05 → 58.13@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.7600 R=0.0000); 2026-10-05:GREEN:O=57.3300,C=58.0600,body=+0.7300,vol=2508919.0; 2026-10-06:GREEN:O=58.2400,C=61.2700,body=+3.0300,vol=2599801.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5108720 Rvol=0); 2026-10-05:GREEN:O=57.3300,C=58.0600,body=+0.7300,vol=2508919.0; 2026-10-06:GREEN:O=58.2400,C=61.2700,body=+3.0300,vol=2599801.0 | **GOOD** |
| `A07_rvol` | RVOL=1.234 on 2026-10-06: today_vol=2599801 / avg20=2106461 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.722 on 2026-10-06 (price=61.2700, mid=58.3185, upper=62.4044, lower=54.2326; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=61.2700 vs SMA50=59.4642 dist=+3.04% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-06: SMA20=58.3185 SMA50=59.4642 SMA80=64.6690 | **BAD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-06 (63 bars); S1[2026-07-07→2026-08-06] low=2026-07-29@53.8700; S2[2026-08-07→2026-09-04] low=2026-08-24@54.5600; S3[2026-09-08→2026-10-06] low=2026-10-01@54.7850 | lows=[53.869998931884766, 54.560001373291016, 54.78499984741211] span=1.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.6551683079470723 wick_frac=0.3448316920529278 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+0.6300:wick=1.6200; 2026-10-01:GREEN:body=+0.9800:wick=1.2550; 2026-10-02:RED:body=-0.0200:wick=1.3100; 2026-10-05:GREEN:body=+0.7300:wick=1.2950; 2026-10-06:GREEN:body=+3.0300:wick=0.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.74 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.43 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4095.0 | **NEUTRAL** |
| `B04_income` | 210.0 | **GOOD** |
| `B05_profit_margin` | 5.13 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.1299999999999955 (now=80.25 vs prior_export=79.12 on finviz_2026-10-06) | **GOOD** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.91 | **GOOD** |
| `B13_short_float` | 3.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.74 (this export) | prior_export=13.74 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.43 (this export) | prior_export=6.43 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+15**  ·  Computer Hardware
price=215.36000061035156  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.82 on 2026-10-06; prev RSI=59.52 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.52@2026-10-05 → 66.82@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.52@2026-10-05 → 66.82@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.52@2026-10-05 → 66.82@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=8.558 (G=7.3600 R=0.8600); 2026-10-05:RED:O=207.7600,C=206.9000,body=-0.8600,vol=3493550.0; 2026-10-06:GREEN:O=208.0000,C=215.3600,body=+7.3600,vol=6854425.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.962 (Gvol=6854425 Rvol=3493550); 2026-10-05:RED:O=207.7600,C=206.9000,body=-0.8600,vol=3493550.0; 2026-10-06:GREEN:O=208.0000,C=215.3600,body=+7.3600,vol=6854425.0 | **GOOD** |
| `A07_rvol` | RVOL=1.445 on 2026-10-06: today_vol=6854425 / avg20=4742089 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.018 on 2026-10-06 (price=215.3600, mid=201.5210, upper=215.1194, lower=187.9226; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-06: price=215.3600 vs SMA50=194.9670 dist=+10.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=201.5210 SMA50=194.9670 SMA80=185.8855 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-06 (63 bars); S1[2026-07-09→2026-08-06] low=2026-07-29@156.8400; S2[2026-08-07→2026-09-04] low=2026-09-03@181.2700; S3[2026-09-08→2026-10-06] low=2026-09-14@186.1100 | lows=[156.83999633789062, 181.27000427246094, 186.11000061035156] span=18.66% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.8335219800823594 wick_frac=0.16647801991764058 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.19554786084289486 wick_frac=0.8044521391571051 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.55813417079186 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:RED:body=-0.6500:wick=3.6900; 2026-10-01:RED:body=-0.2050:wick=5.3740; 2026-10-02:RED:body=-0.9100:wick=3.0623; 2026-10-05:RED:body=-0.8600:wick=3.5379; 2026-10-06:GREEN:body=+7.3600:wick=1.4700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 248.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.38999999999998636 (now=248.86 vs prior_export=249.25 on finviz_2026-10-06) | **BAD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -3.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.029999999999999805 (now=-3.24 vs prior=-3.27 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 0.66 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ASH  ·  score **+15**  ·  Specialty Chemicals
price=70.97000122070312  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.46 on 2026-10-06; prev RSI=42.72 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.72@2026-10-05 → 54.46@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.72@2026-10-05 → 54.46@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.72@2026-10-05 → 54.46@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=8.526 (G=1.6200 R=0.1900); 2026-10-05:RED:O=68.9700,C=68.7800,body=-0.1900,vol=539101.0; 2026-10-06:GREEN:O=69.3500,C=70.9700,body=+1.6200,vol=1078295.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=2.000 (Gvol=1078295 Rvol=539101); 2026-10-05:RED:O=68.9700,C=68.7800,body=-0.1900,vol=539101.0; 2026-10-06:GREEN:O=69.3500,C=70.9700,body=+1.6200,vol=1078295.0 | **GOOD** |
| `A07_rvol` | RVOL=1.623 on 2026-10-06: today_vol=1078295 / avg20=664501 (avg window 2026-09-08→2026-10-05, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.684 on 2026-10-06 (price=70.9700, mid=69.5445, upper=71.6278, lower=67.4612; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-06: price=70.9700 vs SMA50=71.7010 dist=-1.02% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=69.54_50=71.70_80=69.49 on 2026-10-06: SMA20=69.5445 SMA50=71.7010 SMA80=69.4924 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-07-08@62.8200; S2[2026-08-07→2026-09-04] low=2026-09-04@71.0000; S3[2026-09-08→2026-10-06] low=2026-10-01@67.0000 | lows=[62.81999969482422, 71.0, 67.0] span=13.02% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.7943533302407709 wick_frac=0.20564666975922904 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.188120831224553 wick_frac=0.811879168775447 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.52622068743977 need>1.4; red_wick_gt_green=False 5d trail=2026-09-30:RED:body=-0.7800:wick=0.4400; 2026-10-01:GREEN:body=+1.3600:wick=0.6100; 2026-10-02:GREEN:body=+0.3500:wick=0.7837; 2026-10-05:RED:body=-0.1900:wick=0.8200; 2026-10-06:GREEN:body=+1.6200:wick=0.4194 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.55 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.27 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1843.0 | **NEUTRAL** |
| `B04_income` | 52.0 | **GOOD** |
| `B05_profit_margin` | 2.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 80.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.27000000000001023 (now=80.18 vs prior_export=79.91 on finviz_2026-10-06) | **GOOD** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.08 | **GOOD** |
| `B13_short_float` | 9.74 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.55 (this export) | prior_export=3.55 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PD  ·  score **+15**  ·  Software - Application
price=15.4399995803833  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.06 on 2026-10-06; prev RSI=67.56 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.56@2026-10-05 → 65.06@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.56@2026-10-05 → 65.06@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.56@2026-10-05 → 65.06@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=1.222 (G=0.3300 R=0.2700); 2026-10-05:GREEN:O=15.2600,C=15.5900,body=+0.3300,vol=1444811.0; 2026-10-06:RED:O=15.7100,C=15.4400,body=-0.2700,vol=1200076.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.204 (Gvol=1444811 Rvol=1200076); 2026-10-05:GREEN:O=15.2600,C=15.5900,body=+0.3300,vol=1444811.0; 2026-10-06:RED:O=15.7100,C=15.4400,body=-0.2700,vol=1200076.0 | **GOOD** |
| `A07_rvol` | RVOL=0.717 on 2026-10-06: today_vol=1200076 / avg20=1673261 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.677 on 2026-10-06 (price=15.4400, mid=14.6635, upper=15.8103, lower=13.5167; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=15.4400 vs SMA50=13.1286 dist=+17.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=14.6635 SMA50=13.1286 SMA80=11.7774 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-06 (63 bars); S1[2026-07-08→2026-08-06] low=2026-07-23@8.5450; S2[2026-08-07→2026-09-04] low=2026-08-13@11.2100; S3[2026-09-08→2026-10-06] low=2026-09-08@13.0100 | lows=[8.545000076293945, 11.210000038146973, 13.010000228881836] span=52.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.7857139613353255 wick_frac=0.21428603866467455 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.4655180918618685 wick_frac=0.5344819081381316 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.222219867474816 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+0.1400:wick=0.4900; 2026-10-01:GREEN:body=+0.1800:wick=0.2900; 2026-10-02:RED:body=-0.2400:wick=0.3600; 2026-10-05:GREEN:body=+0.3300:wick=0.0900; 2026-10-06:RED:body=-0.2700:wick=0.3100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.27 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 494.73 | **NEUTRAL** |
| `B04_income` | 185.55 | **GOOD** |
| `B05_profit_margin` | 37.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.64 vs prior_export=12.64 on finviz_2026-10-06) | **NEUTRAL** |
| `B09_analyst_recom` | 2.8 | **NEUTRAL** |
| `B10_insider_transactions` | -9.99 | **BAD** |
| `B11_insider_tx_delta` | delta=1.2400000000000002 (now=-9.99 vs prior=-11.23 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 11.91 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.27 (this export) | prior_export=4.27 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+15**  ·  Medical Devices
price=134.72999572753906  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=43.18 on 2026-10-06; prev RSI=44.59 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.59@2026-10-05 → 43.18@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 44.59@2026-10-05 → 43.18@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 44.59@2026-10-05 → 43.18@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=3.779 (G=3.9300 R=1.0400); 2026-10-05:GREEN:O=131.6900,C=135.6200,body=+3.9300,vol=1940303.0; 2026-10-06:RED:O=135.7700,C=134.7300,body=-1.0400,vol=1712351.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.133 (Gvol=1940303 Rvol=1712351); 2026-10-05:GREEN:O=131.6900,C=135.6200,body=+3.9300,vol=1940303.0; 2026-10-06:RED:O=135.7700,C=134.7300,body=-1.0400,vol=1712351.0 | **GOOD** |
| `A07_rvol` | RVOL=0.979 on 2026-10-06: today_vol=1712351 / avg20=1749586 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.193 on 2026-10-06 (price=134.7300, mid=136.0955, upper=143.1602, lower=129.0308; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-06: price=134.7300 vs SMA50=144.0700 dist=-6.48% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-06: SMA20=136.0955 SMA50=144.0700 SMA80=148.4026 | **BAD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-06 (63 bars); S1[2026-07-06→2026-08-06] low=2026-08-05@126.4000; S2[2026-08-07→2026-09-04] low=2026-08-07@136.4600; S3[2026-09-08→2026-10-06] low=2026-10-02@128.0100 | lows=[126.4000015258789, 136.4600067138672, 128.00999450683594] span=7.96% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.8762524283075165 wick_frac=0.12374757169248356 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.4271086602331119 wick_frac=0.5728913397668881 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.77880806361689 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:RED:body=-2.4600:wick=0.5900; 2026-10-01:RED:body=-0.9900:wick=2.2800; 2026-10-02:RED:body=-0.6200:wick=3.9000; 2026-10-05:GREEN:body=+3.9300:wick=0.5550; 2026-10-06:RED:body=-1.0400:wick=1.3950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.910000000000025 (now=169.86 vs prior_export=168.95 on finviz_2026-10-06) | **GOOD** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ABBV  ·  score **+15**  ·  Drug Manufacturers - General
price=266.69000244140625  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.76 on 2026-10-06; prev RSI=58.40 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.40@2026-10-05 → 59.76@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.40@2026-10-05 → 59.76@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.40@2026-10-05 → 59.76@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=4.276 (G=4.4900 R=1.0500); 2026-10-05:GREEN:O=261.2700,C=265.7600,body=+4.4900,vol=7540753.0; 2026-10-06:RED:O=267.7400,C=266.6900,body=-1.0500,vol=4410846.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.710 (Gvol=7540753 Rvol=4410846); 2026-10-05:GREEN:O=261.2700,C=265.7600,body=+4.4900,vol=7540753.0; 2026-10-06:RED:O=267.7400,C=266.6900,body=-1.0500,vol=4410846.0 | **GOOD** |
| `A07_rvol` | RVOL=1.004 on 2026-10-06: today_vol=4410846 / avg20=4392782 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.531 on 2026-10-06 (price=266.6900, mid=262.4460, upper=270.4399, lower=254.4521; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=266.6900 vs SMA50=258.1662 dist=+3.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=262.4460 SMA50=258.1662 SMA80=252.9756 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-06 (63 bars); S1[2026-07-09→2026-08-06] low=2026-08-06@241.2000; S2[2026-08-07→2026-09-04] low=2026-08-07@241.9500; S3[2026-09-08→2026-10-06] low=2026-09-09@246.7700 | lows=[241.1999969482422, 241.9499969482422, 246.77000427246094] span=2.31% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.6368834789233553 wick_frac=0.3631165210766447 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.12994576465789434 wick_frac=0.8700542353421057 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.276259954659071 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:RED:body=-3.0100:wick=0.7600; 2026-10-01:RED:body=-0.5800:wick=2.7400; 2026-10-02:GREEN:body=+3.0800:wick=3.1100; 2026-10-05:GREEN:body=+4.4900:wick=2.5600; 2026-10-06:RED:body=-1.0500:wick=7.0302 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.24 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 64386.0 | **NEUTRAL** |
| `B04_income` | 6268.0 | **GOOD** |
| `B05_profit_margin` | 9.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 279.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-2.349999999999966 (now=279.73 vs prior_export=282.08 on finviz_2026-10-06) | **BAD** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -1.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.82 vs prior=-1.82 on finviz_2026-10-06) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.56 | **GOOD** |
| `B13_short_float` | 1.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ETN  ·  score **+15**  ·  Specialty Industrial Machinery
price=445.0899963378906  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.90 on 2026-10-06; prev RSI=54.18 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.18@2026-10-05 → 59.90@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.18@2026-10-05 → 59.90@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.18@2026-10-05 → 59.90@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=2.240 (G=8.6900 R=3.8800); 2026-10-05:RED:O=436.4800,C=432.6000,body=-3.8800,vol=1426410.0; 2026-10-06:GREEN:O=436.4000,C=445.0900,body=+8.6900,vol=1589973.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=1.115 (Gvol=1589973 Rvol=1426410); 2026-10-05:RED:O=436.4800,C=432.6000,body=-3.8800,vol=1426410.0; 2026-10-06:GREEN:O=436.4000,C=445.0900,body=+8.6900,vol=1589973.0 | **GOOD** |
| `A07_rvol` | RVOL=0.765 on 2026-10-06: today_vol=1589973 / avg20=2079279 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.586 on 2026-10-06 (price=445.0900, mid=425.4730, upper=458.9430, lower=392.0030; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-06: price=445.0900 vs SMA50=423.2656 dist=+5.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=425.4730 SMA50=423.2656 SMA80=416.5244 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-06 (63 bars); S1[2026-07-07→2026-08-06] low=2026-07-29@357.2510; S2[2026-08-07→2026-09-04] low=2026-09-03@384.7700; S3[2026-09-08→2026-10-06] low=2026-09-14@387.0000 | lows=[357.2510251368342, 384.7699890136719, 387.0] span=8.33% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.685330169266352 wick_frac=0.31466983073364796 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.4630074727963991 wick_frac=0.5369925272036009 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.239688532326569 need>1.4; red_wick_gt_green=False 5d trail=2026-09-30:RED:body=-4.2900:wick=2.4600; 2026-10-01:GREEN:body=+7.6500:wick=8.9900; 2026-10-02:RED:body=-10.4500:wick=7.8500; 2026-10-05:RED:body=-3.8800:wick=4.5000; 2026-10-06:GREEN:body=+8.6900:wick=3.9900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.53 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.61 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 30025.0 | **NEUTRAL** |
| `B04_income` | 3829.0 | **GOOD** |
| `B05_profit_margin` | 12.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 487.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-1.2199999999999704 (now=487.72 vs prior_export=488.94 on finviz_2026-10-06) | **BAD** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -4.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.2400000000000002 (now=-4.71 vs prior=-4.95 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 2.0 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.53 (this export) | prior_export=2.53 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.61 (this export) | prior_export=4.61 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+15**  ·  Software - Infrastructure
price=14.59000015258789  pair=`2026-10-05→2026-10-06`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.25 on 2026-10-06; prev RSI=67.69 on 2026-10-05 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.69@2026-10-05 → 66.25@2026-10-06 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.69@2026-10-05 → 66.25@2026-10-06 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.69@2026-10-05 → 66.25@2026-10-06 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_body_sum/RED_body_sum=4.167 (G=0.7500 R=0.1800); 2026-10-05:GREEN:O=13.9100,C=14.6600,body=+0.7500,vol=2906554.0; 2026-10-06:RED:O=14.7700,C=14.5900,body=-0.1800,vol=3152034.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-05 + 2026-10-06; ratio=GREEN_vol/RED_vol=0.922 (Gvol=2906554 Rvol=3152034); 2026-10-05:GREEN:O=13.9100,C=14.6600,body=+0.7500,vol=2906554.0; 2026-10-06:RED:O=14.7700,C=14.5900,body=-0.1800,vol=3152034.0 | **BAD** |
| `A07_rvol` | RVOL=1.471 on 2026-10-06: today_vol=3152034 / avg20=2142820 (avg window 2026-09-08→2026-10-05, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.970 on 2026-10-06 (price=14.5900, mid=13.5097, upper=14.6232, lower=12.3963; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-06: price=14.5900 vs SMA50=13.3816 dist=+9.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-06: SMA20=13.5097 SMA50=13.3816 SMA80=12.7552 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-06 (63 bars); S1[2026-07-08→2026-08-06] low=2026-07-23@11.7350; S2[2026-08-07→2026-09-04] low=2026-08-07@12.4100; S3[2026-09-08→2026-10-06] low=2026-09-11@12.6100 | lows=[11.734999656677246, 12.40999984741211, 12.609999656677246] span=7.46% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: GREEN body_frac=0.9493671344499773 wick_frac=0.05063286555002264 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-05+2026-10-06: RED body_frac=0.1945948843889217 wick_frac=0.8054051156110783 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.166659602424448 need>1.4; red_wick_gt_green=True 5d trail=2026-09-30:GREEN:body=+0.4400:wick=0.1850; 2026-10-01:RED:body=-0.1900:wick=0.2100; 2026-10-02:RED:body=-0.1500:wick=0.1100; 2026-10-05:GREEN:body=+0.7500:wick=0.0400; 2026-10-06:RED:body=-0.1800:wick=0.7450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=101.09 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466.15 | **NEUTRAL** |
| `B04_income` | 71.48 | **GOOD** |
| `B05_profit_margin` | 15.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.07000000000000028 (now=16.8 vs prior_export=16.73 on finviz_2026-10-06) | **GOOD** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.020000000000000018 (now=-0.18 vs prior=-0.2 on finviz_2026-10-06) | **GOOD** |
| `B12_institutional_transactions` | 5.33 | **GOOD** |
| `B13_short_float` | 7.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=101.09 (this export) | prior_export=101.09 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-10-06) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-07_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.