# A+B1 Feature Checklist — 2026-11-10

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,615** names
- Export: `finviz_2026-11-10.csv` · prior export for Δ: `2026-11-09`
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
| 1 | CNM | +17 | 18 | 1 | 2026-11-06→2026-11-09 | Industrial Distribution |
| 2 | TEL | +17 | 18 | 1 | 2026-11-06→2026-11-09 | Electronic Components |
| 3 | WCN | +17 | 17 | 0 | 2026-11-06→2026-11-09 | Waste Management |
| 4 | SOLS | +17 | 17 | 0 | 2026-11-06→2026-11-09 | Specialty Chemicals |
| 5 | GPN | +17 | 17 | 0 | 2026-11-06→2026-11-09 | Software - Infrastructure |
| 6 | TAK | +16 | 16 | 0 | 2026-11-06→2026-11-09 | Drug Manufacturers - Specialty & Gen |
| 7 | V | +16 | 16 | 0 | 2026-11-06→2026-11-09 | Credit Services |
| 8 | TXN | +16 | 17 | 1 | 2026-11-06→2026-11-09 | Semiconductors |
| 9 | HUM | +16 | 17 | 1 | 2026-11-06→2026-11-09 | Healthcare Plans |
| 10 | TEVA | +16 | 18 | 2 | 2026-11-06→2026-11-09 | Drug Manufacturers - Specialty & Gen |
| 11 | MEOH | +16 | 16 | 0 | 2026-11-06→2026-11-09 | Chemicals |
| 12 | VNT | +16 | 18 | 2 | 2026-11-06→2026-11-09 | Scientific & Technical Instruments |
| 13 | Q | +16 | 18 | 2 | 2026-11-06→2026-11-09 | Semiconductor Equipment & Materials |
| 14 | HNGE | +16 | 18 | 2 | 2026-11-06→2026-11-09 | Health Information Services |
| 15 | HONA | +16 | 16 | 0 | 2026-11-06→2026-11-09 | Aerospace & Defense |

## Full checklist — top 15

### CNM  ·  score **+17**  ·  Industrial Distribution
price=47.900001525878906  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.85 on 2026-11-09; prev RSI=60.99 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.99@2026-11-06 → 66.85@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.99@2026-11-06 → 66.85@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.99@2026-11-06 → 66.85@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=14.167 (G=0.8500 R=0.0600); 2026-11-06:RED:O=46.0300,C=45.9700,body=-0.0600,vol=2358000.0; 2026-11-09:GREEN:O=47.0500,C=47.9000,body=+0.8500,vol=4605700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.953 (Gvol=4605700 Rvol=2358000); 2026-11-06:RED:O=46.0300,C=45.9700,body=-0.0600,vol=2358000.0; 2026-11-09:GREEN:O=47.0500,C=47.9000,body=+0.8500,vol=4605700.0 | **GOOD** |
| `A07_rvol` | RVOL=2.331 on 2026-11-09: today_vol=4605700 / avg20=1975576 (avg window 2026-10-07→2026-11-06, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.911 on 2026-11-09 (price=47.9000, mid=43.3520, upper=48.3446, lower=38.3594; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-09: price=47.9000 vs SMA50=42.7518 dist=+12.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=43.35_50=42.75_80=43.61 on 2026-11-09: SMA20=43.3520 SMA50=42.7518 SMA80=43.6104 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-02@41.5500; S2[2026-09-08→2026-10-06] low=2026-09-14@40.0300; S3[2026-10-07→2026-11-09] low=2026-10-07@39.4150 | lows=[41.54999923706055, 40.029998779296875, 39.415000915527344] span=5.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.5629160487776534 wick_frac=0.43708395122234656 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.05660139704109431 wick_frac=0.9433986029589057 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.167281281790437 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:RED:body=-0.2800:wick=0.4850; 2026-11-04:GREEN:body=+2.2700:wick=0.8200; 2026-11-05:RED:body=-1.6200:wick=1.7700; 2026-11-06:RED:body=-0.0600:wick=1.0000; 2026-11-09:GREEN:body=+0.8500:wick=0.6600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.43 (current export asof; earnings_date=9/9/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.43 (current export; earnings_date=9/9/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7698.0 | **NEUTRAL** |
| `B04_income` | 459.0 | **GOOD** |
| `B05_profit_margin` | 5.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 54.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=54.43 vs prior_export=54.43 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | 0.22 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.22 vs prior=0.22 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.88 | **GOOD** |
| `B13_short_float` | 6.34 | **NEUTRAL** |
| `B14_earnings_date` | 9/9/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.43 (this export) | prior_export=3.43 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.43 (this export) | prior_export=0.43 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TEL  ·  score **+17**  ·  Electronic Components
price=218.75  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.56 on 2026-11-09; prev RSI=42.96 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.96@2026-11-06 → 53.56@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.96@2026-11-06 → 53.56@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.96@2026-11-06 → 53.56@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=5.469 (G=7.8200 R=1.4300); 2026-11-06:RED:O=208.4400,C=207.0100,body=-1.4300,vol=1809200.0; 2026-11-09:GREEN:O=210.9300,C=218.7500,body=+7.8200,vol=1875600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.037 (Gvol=1875600 Rvol=1809200); 2026-11-06:RED:O=208.4400,C=207.0100,body=-1.4300,vol=1809200.0; 2026-11-09:GREEN:O=210.9300,C=218.7500,body=+7.8200,vol=1875600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.884 on 2026-11-09: today_vol=1875600 / avg20=2120836 (avg window 2026-10-07→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.328 on 2026-11-09 (price=218.7500, mid=215.2465, upper=225.9336, lower=204.5594; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=218.7500 vs SMA50=211.9064 dist=+3.23% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=215.2465 SMA50=211.9064 SMA80=210.4902 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-25@199.2800; S2[2026-09-08→2026-10-06] low=2026-09-16@200.4300; S3[2026-10-07→2026-11-09] low=2026-10-23@204.6300 | lows=[199.27999877929688, 200.42999267578125, 204.6300048828125] span=2.68% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.7476112535849431 wick_frac=0.25238874641505693 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.3149801870716862 wick_frac=0.6850198129283138 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.468506247532464 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:RED:body=-0.2300:wick=4.1500; 2026-11-04:RED:body=-1.7800:wick=5.4100; 2026-11-05:RED:body=-13.6700:wick=0.8000; 2026-11-06:RED:body=-1.4300:wick=3.1100; 2026-11-09:GREEN:body=+7.8200:wick=2.6400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.17 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.49 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 19885.0 | **NEUTRAL** |
| `B04_income` | 3253.0 | **GOOD** |
| `B05_profit_margin` | 16.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 263.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=263.88 vs prior_export=263.88 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | -2.89 | **BAD** |
| `B11_insider_tx_delta` | delta=2.72 (now=-2.89 vs prior=-5.61 on finviz_2026-11-09) | **GOOD** |
| `B12_institutional_transactions` | 0.49 | **GOOD** |
| `B13_short_float` | 2.15 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.17 (this export) | prior_export=2.17 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.49 (this export) | prior_export=0.49 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WCN  ·  score **+17**  ·  Waste Management
price=190.02000427246094  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.53 on 2026-11-09; prev RSI=68.72 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.72@2026-11-06 → 69.53@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.72@2026-11-06 → 69.53@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.72@2026-11-06 → 69.53@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.4600 R=0.0000); 2026-11-06:GREEN:O=184.6000,C=189.0700,body=+4.4700,vol=1761800.0; 2026-11-09:GREEN:O=189.0300,C=190.0200,body=+0.9900,vol=1917500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3679300 Rvol=0); 2026-11-06:GREEN:O=184.6000,C=189.0700,body=+4.4700,vol=1761800.0; 2026-11-09:GREEN:O=189.0300,C=190.0200,body=+0.9900,vol=1917500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.856 on 2026-11-09: today_vol=1917500 / avg20=2240371 (avg window 2026-10-07→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.465 on 2026-11-09 (price=190.0200, mid=177.7975, upper=204.0688, lower=151.5262; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=190.0200 vs SMA50=166.4584 dist=+14.15% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=177.80_50=166.46_80=167.07 on 2026-11-09: SMA20=177.7975 SMA50=166.4584 SMA80=167.0724 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-02@162.3700; S2[2026-09-08→2026-10-06] low=2026-10-01@149.3900; S3[2026-10-07→2026-11-09] low=2026-10-09@152.3300 | lows=[162.3699951171875, 149.38999938964844, 152.3300018310547] span=8.69% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.6209835131996791 wick_frac=0.3790164868003209 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:GREEN:body=+0.0700:wick=3.0400; 2026-11-04:GREEN:body=+3.0000:wick=3.4000; 2026-11-05:RED:body=-1.7400:wick=3.7200; 2026-11-06:GREEN:body=+4.4700:wick=0.2800; 2026-11-09:GREEN:body=+0.9900:wick=2.3000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.93 (current export asof; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9943.62 | **NEUTRAL** |
| `B04_income` | 1133.47 | **GOOD** |
| `B05_profit_margin` | 11.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 208.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0800000000000125 (now=208.08 vs prior_export=208.0 on finviz_2026-11-09) | **GOOD** |
| `B09_analyst_recom` | 1.27 | **GOOD** |
| `B10_insider_transactions` | 0.68 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.68 vs prior=0.68 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.26 | **GOOD** |
| `B13_short_float` | 2.11 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.93 (this export) | prior_export=3.93 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLS  ·  score **+17**  ·  Specialty Chemicals
price=63.650001525878906  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.50 on 2026-11-09; prev RSI=45.95 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.95@2026-11-06 → 56.50@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.95@2026-11-06 → 56.50@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.95@2026-11-06 → 56.50@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.2700 R=0.0000); 2026-11-06:GREEN:O=56.0900,C=58.0000,body=+1.9100,vol=3138800.0; 2026-11-09:GREEN:O=59.2900,C=63.6500,body=+4.3600,vol=2921300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6060100 Rvol=0); 2026-11-06:GREEN:O=56.0900,C=58.0000,body=+1.9100,vol=3138800.0; 2026-11-09:GREEN:O=59.2900,C=63.6500,body=+4.3600,vol=2921300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.152 on 2026-11-09: today_vol=2921300 / avg20=2536682 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.638 on 2026-11-09 (price=63.6500, mid=60.6360, upper=65.3590, lower=55.9130; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=63.6500 vs SMA50=59.9496 dist=+6.17% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=60.6360 SMA50=59.9496 SMA80=59.7896 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-08-24@54.5600; S2[2026-09-11→2026-10-09] low=2026-10-01@54.7850; S3[2026-10-12→2026-11-09] low=2026-11-05@54.8050 | lows=[54.560001373291016, 54.78499984741211, 54.80500030517578] span=0.45% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.7443928429247202 wick_frac=0.2556071570752797 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:GREEN:body=+2.1400:wick=0.7950; 2026-11-04:RED:body=-2.7000:wick=2.9800; 2026-11-05:RED:body=-1.4400:wick=1.5950; 2026-11-06:GREEN:body=+1.9100:wick=1.4300; 2026-11-09:GREEN:body=+4.3600:wick=0.3950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.78 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.77 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4172.0 | **NEUTRAL** |
| `B04_income` | 325.0 | **GOOD** |
| `B05_profit_margin` | 7.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.9 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=78.9 vs prior_export=78.9 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.39 | **GOOD** |
| `B13_short_float` | 3.16 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.78 (this export) | prior_export=14.78 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPN  ·  score **+17**  ·  Software - Infrastructure
price=97.7699966430664  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.63 on 2026-11-09; prev RSI=66.17 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.17@2026-11-06 → 66.63@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.17@2026-11-06 → 66.63@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.17@2026-11-06 → 66.63@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=5.712 (G=3.3700 R=0.5900); 2026-11-06:GREEN:O=94.0300,C=97.4000,body=+3.3700,vol=3648100.0; 2026-11-09:RED:O=98.3600,C=97.7700,body=-0.5900,vol=2742000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.330 (Gvol=3648100 Rvol=2742000); 2026-11-06:GREEN:O=94.0300,C=97.4000,body=+3.3700,vol=3648100.0; 2026-11-09:RED:O=98.3600,C=97.7700,body=-0.5900,vol=2742000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.836 on 2026-11-09: today_vol=2742000 / avg20=3280763 (avg window 2026-10-07→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.688 on 2026-11-09 (price=97.7700, mid=88.2805, upper=102.0741, lower=74.4869; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=97.7700 vs SMA50=87.4010 dist=+11.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=88.2805 SMA50=87.4010 SMA80=86.9206 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-12@84.2000; S2[2026-09-08→2026-10-06] low=2026-10-02@77.9300; S3[2026-10-07→2026-11-09] low=2026-10-08@80.3200 | lows=[84.19999694824219, 77.93000030517578, 80.31999969482422] span=8.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.6419052850632441 wick_frac=0.35809471493675593 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.2994942140566666 wick_frac=0.7005057859433333 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.711830654442476 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:RED:body=-1.3400:wick=1.3900; 2026-11-04:RED:body=-2.0300:wick=1.7600; 2026-11-05:RED:body=-1.9600:wick=2.8000; 2026-11-06:GREEN:body=+3.3700:wick=1.8800; 2026-11-09:RED:body=-0.5900:wick=1.3800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.09 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.01 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11493.52 | **NEUTRAL** |
| `B04_income` | 197.89 | **GOOD** |
| `B05_profit_margin` | 1.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 108.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.539999999999992 (now=108.88 vs prior_export=107.34 on finviz_2026-11-09) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.29 | **GOOD** |
| `B13_short_float` | 6.62 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.09 (this export) | prior_export=0.09 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.01 (this export) | prior_export=0.01 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TAK  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=21.09000015258789  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.33 on 2026-11-09; prev RSI=68.60 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.60@2026-11-06 → 69.33@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.60@2026-11-06 → 69.33@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.60@2026-11-06 → 69.33@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=8.000 (G=0.4000 R=0.0500); 2026-11-06:GREEN:O=20.6200,C=21.0200,body=+0.4000,vol=2965900.0; 2026-11-09:RED:O=21.1400,C=21.0900,body=-0.0500,vol=2162200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.372 (Gvol=2965900 Rvol=2162200); 2026-11-06:GREEN:O=20.6200,C=21.0200,body=+0.4000,vol=2965900.0; 2026-11-09:RED:O=21.1400,C=21.0900,body=-0.0500,vol=2162200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.706 on 2026-11-09: today_vol=2162200 / avg20=3064260 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.590 on 2026-11-09 (price=21.0900, mid=20.2310, upper=21.6864, lower=18.7756; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=21.0900 vs SMA50=19.1770 dist=+9.98% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=20.2310 SMA50=19.1770 SMA80=18.5406 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-08-17@17.2800; S2[2026-09-11→2026-10-09] low=2026-10-05@17.7500; S3[2026-10-12→2026-11-09] low=2026-10-13@18.5200 | lows=[17.280000686645508, 17.75, 18.520000457763672] span=7.18% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.769229358471188 wick_frac=0.23077064152881194 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.16128813934743955 wick_frac=0.8387118606525604 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.000114442664225 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:RED:body=-0.0300:wick=0.1700; 2026-11-04:GREEN:body=+0.1600:wick=0.2750; 2026-11-05:RED:body=-0.3400:wick=0.1950; 2026-11-06:GREEN:body=+0.4000:wick=0.1200; 2026-11-09:RED:body=-0.0500:wick=0.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=164.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.72 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 29820.42 | **NEUTRAL** |
| `B04_income` | 242.88 | **GOOD** |
| `B05_profit_margin` | 0.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.17 vs prior_export=21.17 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.07 | **GOOD** |
| `B13_short_float` | 0.1 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=164.69 (this export) | prior_export=164.69 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.72 (this export) | prior_export=2.72 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+16**  ·  Credit Services
price=422.1600036621094  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.86 on 2026-11-09; prev RSI=63.28 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.28@2026-11-06 → 62.86@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.28@2026-11-06 → 62.86@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.28@2026-11-06 → 62.86@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=4.392 (G=8.1700 R=1.8600); 2026-11-06:GREEN:O=414.4800,C=422.6500,body=+8.1700,vol=9219400.0; 2026-11-09:RED:O=424.0200,C=422.1600,body=-1.8600,vol=6764700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.363 (Gvol=9219400 Rvol=6764700); 2026-11-06:GREEN:O=414.4800,C=422.6500,body=+8.1700,vol=9219400.0; 2026-11-09:RED:O=424.0200,C=422.1600,body=-1.8600,vol=6764700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.918 on 2026-11-09: today_vol=6764700 / avg20=7369624 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.507 on 2026-11-09 (price=422.1600, mid=410.5050, upper=433.4779, lower=387.5321; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=422.1600 vs SMA50=386.6540 dist=+9.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=410.5050 SMA50=386.6540 SMA80=378.9547 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-08-18@358.0500; S2[2026-09-11→2026-10-09] low=2026-10-02@356.7500; S3[2026-10-12→2026-11-09] low=2026-10-12@384.6493 | lows=[358.04998779296875, 356.75, 384.6492919921875] span=7.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.7166655512575123 wick_frac=0.2833344487424877 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.2120852544584602 wick_frac=0.7879147455415398 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.392498523331365 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:RED:body=-1.9800:wick=3.4700; 2026-11-04:GREEN:body=+9.1500:wick=4.2400; 2026-11-05:RED:body=-18.2100:wick=2.0754; 2026-11-06:GREEN:body=+8.1700:wick=3.2300; 2026-11-09:RED:body=-1.8600:wick=6.9100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.2 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 45903.0 | **NEUTRAL** |
| `B04_income` | 23313.0 | **GOOD** |
| `B05_profit_margin` | 50.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 454.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=454.97 vs prior_export=454.97 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.58 | **GOOD** |
| `B13_short_float` | 1.21 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.2 (this export) | prior_export=1.2 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TXN  ·  score **+16**  ·  Semiconductors
price=295.7300109863281  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.76 on 2026-11-09; prev RSI=42.55 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.55@2026-11-06 → 52.76@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.55@2026-11-06 → 52.76@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.55@2026-11-06 → 52.76@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=2.749 (G=14.9000 R=5.4200); 2026-11-06:RED:O=282.7800,C=277.3600,body=-5.4200,vol=10094200.0; 2026-11-09:GREEN:O=280.8300,C=295.7300,body=+14.9000,vol=10278700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.018 (Gvol=10278700 Rvol=10094200); 2026-11-06:RED:O=282.7800,C=277.3600,body=-5.4200,vol=10094200.0; 2026-11-09:GREEN:O=280.8300,C=295.7300,body=+14.9000,vol=10278700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.147 on 2026-11-09: today_vol=10278700 / avg20=8961079 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.142 on 2026-11-09 (price=295.7300, mid=291.7375, upper=319.8226, lower=263.6524; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=295.7300 vs SMA50=279.8002 dist=+5.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=291.7375 SMA50=279.8002 SMA80=277.9766 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-03@248.1300; S2[2026-09-11→2026-10-09] low=2026-09-14@256.7800; S3[2026-10-12→2026-11-09] low=2026-11-06@271.2300 | lows=[248.1300048828125, 256.7799987792969, 271.2300109863281] span=9.31% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.8370805973219949 wick_frac=0.16291940267800503 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.39077255485245066 wick_frac=0.6092274451475493 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.7490751845408017 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:GREEN:body=+2.6600:wick=8.9100; 2026-11-04:RED:body=-1.9450:wick=11.3100; 2026-11-05:RED:body=-23.7700:wick=5.2300; 2026-11-06:RED:body=-5.4200:wick=8.4500; 2026-11-09:GREEN:body=+14.9000:wick=2.9000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.56 (current export asof; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.91 (current export; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 20766.0 | **NEUTRAL** |
| `B04_income` | 7072.0 | **GOOD** |
| `B05_profit_margin` | 34.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 336.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=336.62 vs prior_export=336.62 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.03 | **GOOD** |
| `B10_insider_transactions` | -3.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.45 vs prior=-3.45 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.1 | **GOOD** |
| `B13_short_float` | 2.22 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.56 (this export) | prior_export=7.56 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.91 (this export) | prior_export=1.91 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+16**  ·  Healthcare Plans
price=423.2200012207031  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.82 on 2026-11-09; prev RSI=61.06 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.06@2026-11-06 → 54.82@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.06@2026-11-06 → 54.82@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.06@2026-11-06 → 54.82@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=1.221 (G=18.7400 R=15.3500); 2026-11-06:GREEN:O=415.0000,C=433.7400,body=+18.7400,vol=2576200.0; 2026-11-09:RED:O=438.5700,C=423.2200,body=-15.3500,vol=2450900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.051 (Gvol=2576200 Rvol=2450900); 2026-11-06:GREEN:O=415.0000,C=433.7400,body=+18.7400,vol=2576200.0; 2026-11-09:RED:O=438.5700,C=423.2200,body=-15.3500,vol=2450900.0 | **GOOD** |
| `A07_rvol` | RVOL=1.784 on 2026-11-09: today_vol=2450900 / avg20=1373574 (avg window 2026-10-12→2026-11-06, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.259 on 2026-11-09 (price=423.2200, mid=419.7205, upper=433.2503, lower=406.1908; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=423.2200 vs SMA50=404.8270 dist=+4.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=419.7205 SMA50=404.8270 SMA80=396.7893 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-08-12@370.3800; S2[2026-09-11→2026-10-09] low=2026-09-23@369.1300; S3[2026-10-12→2026-11-09] low=2026-11-02@399.3500 | lows=[370.3800048828125, 369.1300048828125, 399.3500061035156] span=8.19% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.5776815297170068 wick_frac=0.42231847028299313 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.5878972337836748 wick_frac=0.4121027662163252 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.220845783903823 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:GREEN:body=+10.0000:wick=7.2000; 2026-11-04:GREEN:body=+3.0400:wick=19.0450; 2026-11-05:RED:body=-5.6800:wick=12.8350; 2026-11-06:GREEN:body=+18.7400:wick=13.7000; 2026-11-09:RED:body=-15.3500:wick=10.7600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.06 (current export asof; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 154185.0 | **NEUTRAL** |
| `B04_income` | 1246.0 | **GOOD** |
| `B05_profit_margin` | 0.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 486.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=17.74000000000001 (now=486.26 vs prior_export=468.52 on finviz_2026-11-09) | **GOOD** |
| `B09_analyst_recom` | 2.03 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.53 | **GOOD** |
| `B13_short_float` | 3.06 | **NEUTRAL** |
| `B14_earnings_date` | 11/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.06 (this export) | prior_export=30.06 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TEVA  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=42.970001220703125  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.39 on 2026-11-09; prev RSI=62.73 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.73@2026-11-06 → 64.39@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.73@2026-11-06 → 64.39@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.73@2026-11-06 → 64.39@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=28.502 (G=1.1400 R=0.0400); 2026-11-06:GREEN:O=41.4700,C=42.6100,body=+1.1400,vol=6196100.0; 2026-11-09:RED:O=43.0100,C=42.9700,body=-0.0400,vol=4578100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.353 (Gvol=6196100 Rvol=4578100); 2026-11-06:GREEN:O=41.4700,C=42.6100,body=+1.1400,vol=6196100.0; 2026-11-09:RED:O=43.0100,C=42.9700,body=-0.0400,vol=4578100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.645 on 2026-11-09: today_vol=4578100 / avg20=7095938 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.997 on 2026-11-09 (price=42.9700, mid=41.1335, upper=42.9762, lower=39.2908; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-09: price=42.9700 vs SMA50=39.6344 dist=+8.42% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=41.1335 SMA50=39.6344 SMA80=37.8910 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-08@35.4200; S2[2026-09-11→2026-10-09] low=2026-09-11@36.0600; S3[2026-10-12→2026-11-09] low=2026-10-23@39.1050 | lows=[35.41999816894531, 36.060001373291016, 39.10499954223633] span=10.40% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.9344247942566976 wick_frac=0.0655752057433024 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.07691743388475222 wick_frac=0.9230825661152477 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=28.50205054840248 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:DOJI:body=+0.0000:wick=1.8000; 2026-11-04:RED:body=-0.0400:wick=1.2600; 2026-11-05:GREEN:body=+0.2900:wick=1.1653; 2026-11-06:GREEN:body=+1.1400:wick=0.0800; 2026-11-09:RED:body=-0.0400:wick=0.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.31 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.85 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17239.0 | **NEUTRAL** |
| `B04_income` | 506.0 | **GOOD** |
| `B05_profit_margin` | 2.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.42 vs prior_export=50.42 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.31 | **GOOD** |
| `B10_insider_transactions` | -1.46 | **BAD** |
| `B11_insider_tx_delta` | delta=0.07000000000000006 (now=-1.46 vs prior=-1.53 on finviz_2026-11-09) | **GOOD** |
| `B12_institutional_transactions` | 8.62 | **GOOD** |
| `B13_short_float` | 2.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.31 (this export) | prior_export=19.31 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.85 (this export) | prior_export=3.85 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MEOH  ·  score **+16**  ·  Chemicals
price=66.48999786376953  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.94 on 2026-11-09; prev RSI=55.62 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.62@2026-11-06 → 55.94@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.62@2026-11-06 → 55.94@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.62@2026-11-06 → 55.94@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2900 R=0.0000); 2026-11-06:GREEN:O=65.0000,C=66.3300,body=+1.3300,vol=997600.0; 2026-11-09:GREEN:O=65.5300,C=66.4900,body=+0.9600,vol=1294400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2292000 Rvol=0); 2026-11-06:GREEN:O=65.0000,C=66.3300,body=+1.3300,vol=997600.0; 2026-11-09:GREEN:O=65.5300,C=66.4900,body=+0.9600,vol=1294400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.598 on 2026-11-09: today_vol=1294400 / avg20=809982 (avg window 2026-10-07→2026-11-06, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.328 on 2026-11-09 (price=66.4900, mid=64.6640, upper=70.2301, lower=59.0979; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=66.4900 vs SMA50=61.8787 dist=+7.45% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=64.6640 SMA50=61.8787 SMA80=59.2906 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-07@52.2600; S2[2026-09-08→2026-10-06] low=2026-10-01@57.0100; S3[2026-10-07→2026-11-09] low=2026-10-22@59.7700 | lows=[52.2599983215332, 57.0099983215332, 59.77000045776367] span=14.37% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.528975047213464 wick_frac=0.471024952786536 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:GREEN:body=+1.0700:wick=2.1400; 2026-11-04:RED:body=-4.6700:wick=0.6800; 2026-11-05:RED:body=-0.7900:wick=1.5800; 2026-11-06:GREEN:body=+1.3300:wick=0.9000; 2026-11-09:GREEN:body=+0.9600:wick=1.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=25.87 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.9 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4725.29 | **NEUTRAL** |
| `B04_income` | 360.44 | **GOOD** |
| `B05_profit_margin` | 7.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 79.82 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=79.82 vs prior_export=79.82 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 3.08 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.87 (this export) | prior_export=25.87 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.9 (this export) | prior_export=5.9 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+16**  ·  Scientific & Technical Instruments
price=43.880001068115234  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=88.75 on 2026-11-09; prev RSI=86.83 on 2026-11-06 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 86.83@2026-11-06 → 88.75@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 86.83@2026-11-06 → 88.75@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 86.83@2026-11-06 → 88.75@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.7800 R=0.0000); 2026-11-06:GREEN:O=39.6500,C=42.2300,body=+2.5800,vol=4229800.0; 2026-11-09:GREEN:O=41.6800,C=43.8800,body=+2.2000,vol=3140500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7370300 Rvol=0); 2026-11-06:GREEN:O=39.6500,C=42.2300,body=+2.5800,vol=4229800.0; 2026-11-09:GREEN:O=41.6800,C=43.8800,body=+2.2000,vol=3140500.0 | **GOOD** |
| `A07_rvol` | RVOL=2.108 on 2026-11-09: today_vol=3140500 / avg20=1489664 (avg window 2026-10-07→2026-11-06, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.288 on 2026-11-09 (price=43.8800, mid=35.6315, upper=42.0374, lower=29.2256; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-09: price=43.8800 vs SMA50=33.4546 dist=+31.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=35.6315 SMA50=33.4546 SMA80=32.9896 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-09 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-01@31.6000; S2[2026-09-08→2026-10-06] low=2026-09-24@30.4250; S3[2026-10-07→2026-11-09] low=2026-10-07@31.6950 | lows=[31.600000381469727, 30.424999237060547, 31.69499969482422] span=4.17% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.7868614301413552 wick_frac=0.2131385698586447 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:RED:body=-0.2400:wick=0.4200; 2026-11-04:GREEN:body=+0.4500:wick=0.4700; 2026-11-05:RED:body=-2.1100:wick=1.4400; 2026-11-06:GREEN:body=+2.5800:wick=0.4900; 2026-11-09:GREEN:body=+2.2000:wick=0.8000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.89 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.4500000000000028 (now=44.89 vs prior_export=43.44 on finviz_2026-11-09) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.96 | **GOOD** |
| `B13_short_float` | 9.33 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### Q  ·  score **+16**  ·  Semiconductor Equipment & Materials
price=125.08999633789062  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.48 on 2026-11-09; prev RSI=47.46 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.46@2026-11-06 → 55.48@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.46@2026-11-06 → 55.48@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.46@2026-11-06 → 55.48@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=6.862 (G=5.4900 R=0.8000); 2026-11-06:RED:O=117.8000,C=117.0000,body=-0.8000,vol=2531500.0; 2026-11-09:GREEN:O=119.6000,C=125.0900,body=+5.4900,vol=2638200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.042 (Gvol=2638200 Rvol=2531500); 2026-11-06:RED:O=117.8000,C=117.0000,body=-0.8000,vol=2531500.0; 2026-11-09:GREEN:O=119.6000,C=125.0900,body=+5.4900,vol=2638200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.258 on 2026-11-09: today_vol=2638200 / avg20=2096338 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.807 on 2026-11-09 (price=125.0900, mid=117.7905, upper=126.8381, lower=108.7429; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-09: price=125.0900 vs SMA50=121.1086 dist=+3.29% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-09: SMA20=117.7905 SMA50=121.1086 SMA80=125.7151 | **BAD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-01@113.3300; S2[2026-09-11→2026-10-09] low=2026-09-15@111.7500; S3[2026-10-12→2026-11-09] low=2026-10-23@105.0050 | lows=[113.33000183105469, 111.75, 105.00499725341797] span=7.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.6574848942529177 wick_frac=0.3425151057470823 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.16326587808854143 wick_frac=0.8367341219114586 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.862471151461977 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:GREEN:body=+0.2900:wick=5.2300; 2026-11-04:RED:body=-1.2200:wick=6.2750; 2026-11-05:RED:body=-2.8000:wick=5.1300; 2026-11-06:RED:body=-0.8000:wick=4.1000; 2026-11-09:GREEN:body=+5.4900:wick=2.8600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=21.45 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.73 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5490.0 | **NEUTRAL** |
| `B04_income` | 558.0 | **GOOD** |
| `B05_profit_margin` | 10.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 175.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=175.8 vs prior_export=175.8 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.63 | **GOOD** |
| `B13_short_float` | 2.0 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=21.45 (this export) | prior_export=21.45 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.73 (this export) | prior_export=4.73 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HNGE  ·  score **+16**  ·  Health Information Services
price=122.19000244140625  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.69 on 2026-11-09; prev RSI=62.55 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.55@2026-11-06 → 69.69@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.55@2026-11-06 → 69.69@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.55@2026-11-06 → 69.69@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=7.960 (G=5.9700 R=0.7500); 2026-11-06:RED:O=116.5100,C=115.7600,body=-0.7500,vol=1540200.0; 2026-11-09:GREEN:O=116.2200,C=122.1900,body=+5.9700,vol=2014600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=1.308 (Gvol=2014600 Rvol=1540200); 2026-11-06:RED:O=116.5100,C=115.7600,body=-0.7500,vol=1540200.0; 2026-11-09:GREEN:O=116.2200,C=122.1900,body=+5.9700,vol=2014600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.041 on 2026-11-09: today_vol=2014600 / avg20=1935448 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.851 on 2026-11-09 (price=122.1900, mid=111.9205, upper=123.9865, lower=99.8545; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-09: price=122.1900 vs SMA50=101.5314 dist=+20.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-09: SMA20=111.9205 SMA50=101.5314 SMA80=95.1608 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-08-21@84.2300; S2[2026-09-11→2026-10-09] low=2026-09-11@86.2900; S3[2026-10-12→2026-11-09] low=2026-10-16@97.4400 | lows=[84.2300033569336, 86.29000091552734, 97.44000244140625] span=15.68% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.9644598100661256 wick_frac=0.035540189933874415 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=0.09745323327352215 wick_frac=0.9025467667264778 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.960001627604167 need>1.4; red_wick_gt_green=True 5d trail=2026-11-03:GREEN:body=+3.7700:wick=3.0800; 2026-11-04:GREEN:body=+4.0800:wick=4.8500; 2026-11-05:RED:body=-4.8000:wick=2.3000; 2026-11-06:RED:body=-0.7500:wick=6.9460; 2026-11-09:GREEN:body=+5.9700:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.33 (current export asof; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.53 (current export; earnings_date=11/2/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 800.82 | **NEUTRAL** |
| `B04_income` | 165.6 | **GOOD** |
| `B05_profit_margin` | 20.68 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 135.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=135.38 vs prior_export=135.38 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | -14.72 | **BAD** |
| `B11_insider_tx_delta` | delta=0.5299999999999994 (now=-14.72 vs prior=-15.25 on finviz_2026-11-09) | **GOOD** |
| `B12_institutional_transactions` | 17.07 | **GOOD** |
| `B13_short_float` | 11.86 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.33 (this export) | prior_export=23.33 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.53 (this export) | prior_export=4.53 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HONA  ·  score **+16**  ·  Aerospace & Defense
price=164.77000427246094  pair=`2026-11-06→2026-11-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.24 on 2026-11-09; prev RSI=43.96 on 2026-11-06 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.96@2026-11-06 → 54.24@2026-11-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.96@2026-11-06 → 54.24@2026-11-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.96@2026-11-06 → 54.24@2026-11-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=12.5300 R=0.0000); 2026-11-06:GREEN:O=149.2900,C=152.9200,body=+3.6300,vol=2611600.0; 2026-11-09:GREEN:O=155.8700,C=164.7700,body=+8.9000,vol=3956200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-06 + 2026-11-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6567800 Rvol=0); 2026-11-06:GREEN:O=149.2900,C=152.9200,body=+3.6300,vol=2611600.0; 2026-11-09:GREEN:O=155.8700,C=164.7700,body=+8.9000,vol=3956200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.171 on 2026-11-09: today_vol=3956200 / avg20=3379612 (avg window 2026-10-12→2026-11-06, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.282 on 2026-11-09 (price=164.7700, mid=161.2085, upper=173.8434, lower=148.5736; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-09: price=164.7700 vs SMA50=159.3062 dist=+3.43% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=161.21_50=159.31_80=167.96 on 2026-11-09: SMA20=161.2085 SMA50=159.3062 SMA80=167.9622 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-12→2026-11-09 (63 bars); S1[2026-08-12→2026-09-10] low=2026-09-10@149.1900; S2[2026-09-11→2026-10-09] low=2026-10-08@149.8800; S3[2026-10-12→2026-11-09] low=2026-11-06@147.4000 | lows=[149.19000244140625, 149.8800048828125, 147.39999389648438] span=1.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: GREEN body_frac=0.6120433649709764 wick_frac=0.38795663502902367 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-06+2026-11-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-03:RED:body=-2.2500:wick=2.8200; 2026-11-04:RED:body=-2.1200:wick=5.8600; 2026-11-05:RED:body=-6.1800:wick=3.4600; 2026-11-06:GREEN:body=+3.6300:wick=3.2000; 2026-11-09:GREEN:body=+8.9000:wick=3.9500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.34 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18338.0 | **NEUTRAL** |
| `B04_income` | 1898.0 | **GOOD** |
| `B05_profit_margin` | 10.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 188.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=188.43 vs prior_export=188.43 on finviz_2026-11-09) | **NEUTRAL** |
| `B09_analyst_recom` | 2.39 | **GOOD** |
| `B10_insider_transactions` | 7.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.21 vs prior=7.21 on finviz_2026-11-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 213.36 | **GOOD** |
| `B13_short_float` | 2.37 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.34 (this export) | prior_export=18.34 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-10_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.