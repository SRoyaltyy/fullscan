# A+B1 Feature Checklist — 2026-10-12

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-12.csv` · prior export for Δ: `2026-10-09`
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
| 1 | ELV | +18 | 19 | 1 | 2026-10-08→2026-10-09 | Healthcare Plans |
| 2 | AAPL | +17 | 18 | 1 | 2026-10-08→2026-10-09 | Consumer Electronics |
| 3 | AVPT | +17 | 18 | 1 | 2026-10-08→2026-10-09 | Software - Infrastructure |
| 4 | ATRC | +16 | 19 | 3 | 2026-10-08→2026-10-09 | Medical Instruments & Supplies |
| 5 | MH | +16 | 17 | 1 | 2026-10-08→2026-10-09 | Education & Training Services |
| 6 | HTHT | +16 | 17 | 1 | 2026-10-08→2026-10-09 | Lodging |
| 7 | CXT | +16 | 17 | 1 | 2026-10-08→2026-10-09 | Business Equipment & Supplies |
| 8 | NWBI | +15 | 16 | 1 | 2026-10-08→2026-10-09 | Banks - Regional |
| 9 | WCC | +15 | 18 | 3 | 2026-10-08→2026-10-09 | Industrial Distribution |
| 10 | HHH | +15 | 16 | 1 | 2026-10-08→2026-10-09 | Real Estate - Diversified |
| 11 | BEKE | +15 | 18 | 3 | 2026-10-08→2026-10-09 | Real Estate Services |
| 12 | MICC | +15 | 16 | 1 | 2026-10-08→2026-10-09 | Packaged Foods |
| 13 | MDLN | +15 | 17 | 2 | 2026-10-08→2026-10-09 | Medical Instruments & Supplies |
| 14 | AUPH | +15 | 15 | 0 | 2026-10-08→2026-10-09 | Biotechnology |
| 15 | VEEV | +15 | 17 | 2 | 2026-10-08→2026-10-09 | Health Information Services |

## Full checklist — top 15

### ELV  ·  score **+18**  ·  Healthcare Plans
price=412.70001220703125  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.13 on 2026-10-09; prev RSI=51.21 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.21@2026-10-08 → 59.13@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.21@2026-10-08 → 59.13@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.21@2026-10-08 → 59.13@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=3.730 (G=10.7800 R=2.8900); 2026-10-08:RED:O=403.8300,C=400.9400,body=-2.8900,vol=785800.0; 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=2.621 (Gvol=2059225 Rvol=785800); 2026-10-08:RED:O=403.8300,C=400.9400,body=-2.8900,vol=785800.0; 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0 | **GOOD** |
| `A07_rvol` | RVOL=1.798 on 2026-10-09: today_vol=2059225 / avg20=1145310 (avg window 2026-09-11→2026-10-08, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.480 on 2026-10-09 (price=412.7000, mid=402.0490, upper=424.2605, lower=379.8375; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=412.7000 vs SMA50=399.5980 dist=+3.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=402.0490 SMA50=399.5980 SMA80=398.4494 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-28@362.9800; S2[2026-08-12→2026-09-10] low=2026-08-12@385.8400; S3[2026-09-11→2026-10-09] low=2026-10-01@375.7400 | lows=[362.9800109863281, 385.8399963378906, 375.739990234375] span=6.30% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6118080465005915 wick_frac=0.38819195349940855 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.28194967130335363 wick_frac=0.7180503286966463 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.73012386614431 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+6.8400:wick=4.1525; 2026-10-06:GREEN:body=+3.4100:wick=3.1374; 2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.08999999999997499 (now=447.86 vs prior_export=447.95 on finviz_2026-10-09) | **BAD** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+17**  ·  Consumer Electronics
price=336.6400146484375  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.14 on 2026-10-09; prev RSI=61.23 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.23@2026-10-08 → 56.14@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.23@2026-10-08 → 56.14@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.23@2026-10-08 → 56.14@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.5450 R=0.0000); 2026-10-08:GREEN:O=336.8200,C=340.4200,body=+3.6000,vol=35332400.0; 2026-10-09:GREEN:O=331.6950,C=336.6400,body=+4.9450,vol=37750739.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=73083139 Rvol=0); 2026-10-08:GREEN:O=336.8200,C=340.4200,body=+3.6000,vol=35332400.0; 2026-10-09:GREEN:O=331.6950,C=336.6400,body=+4.9450,vol=37750739.0 | **GOOD** |
| `A07_rvol` | RVOL=0.989 on 2026-10-09: today_vol=37750739 / avg20=38160239 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.185 on 2026-10-09 (price=336.6400, mid=335.3890, upper=342.1614, lower=328.6166; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=336.6400 vs SMA50=322.3566 dist=+4.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=335.3890 SMA50=322.3566 SMA80=318.7445 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-09 (63 bars); S1[2026-07-14→2026-08-11] low=2026-07-31@299.7415; S2[2026-08-12→2026-09-10] low=2026-08-12@300.5700; S3[2026-09-11→2026-10-09] low=2026-10-01@325.8100 | lows=[299.7415030462749, 300.57000732421875, 325.80999755859375] span=8.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6300406420076707 wick_frac=0.3699593579923294 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+0.0950:wick=4.4450; 2026-10-06:GREEN:body=+1.3500:wick=2.4100; 2026-10-07:RED:body=-0.2900:wick=5.5900; 2026-10-08:GREEN:body=+3.6000:wick=2.0700; 2026-10-09:GREEN:body=+4.9450:wick=2.9650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.9300000000000068 (now=337.68 vs prior_export=335.75 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.9400000000000004 (now=-2.28 vs prior=-3.22 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+17**  ·  Software - Infrastructure
price=14.630000114440918  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.34 on 2026-10-09; prev RSI=61.74 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.74@2026-10-08 → 64.34@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.74@2026-10-08 → 64.34@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.74@2026-10-08 → 64.34@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.3700 R=0.0000); 2026-10-08:GREEN:O=14.2300,C=14.4100,body=+0.1800,vol=2339600.0; 2026-10-09:GREEN:O=14.4400,C=14.6300,body=+0.1900,vol=1430542.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3770142 Rvol=0); 2026-10-08:GREEN:O=14.2300,C=14.4100,body=+0.1800,vol=2339600.0; 2026-10-09:GREEN:O=14.4400,C=14.6300,body=+0.1900,vol=1430542.0 | **GOOD** |
| `A07_rvol` | RVOL=0.618 on 2026-10-09: today_vol=1430542 / avg20=2313470 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.793 on 2026-10-09 (price=14.6300, mid=13.7557, upper=14.8589, lower=12.6526; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=14.6300 vs SMA50=13.4732 dist=+8.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=13.7557 SMA50=13.4732 SMA80=12.8890 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-09 (63 bars); S1[2026-07-13→2026-08-11] low=2026-07-23@11.7350; S2[2026-08-12→2026-09-10] low=2026-09-10@12.7400; S3[2026-09-11→2026-10-09] low=2026-09-11@12.6100 | lows=[11.734999656677246, 12.739999771118164, 12.609999656677246] span=8.56% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.5015889749043232 wick_frac=0.49841102509567686 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+0.7500:wick=0.0400; 2026-10-06:RED:body=-0.1800:wick=0.7450; 2026-10-07:RED:body=-0.2300:wick=0.2445; 2026-10-08:GREEN:body=+0.1800:wick=0.2700; 2026-10-09:GREEN:body=+0.1900:wick=0.1250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=101.09 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466.15 | **NEUTRAL** |
| `B04_income` | 71.48 | **GOOD** |
| `B05_profit_margin` | 15.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.07000000000000028 (now=16.8 vs prior_export=16.73 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.06 (now=-0.18 vs prior=-0.24 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 5.33 | **GOOD** |
| `B13_short_float` | 7.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=101.09 (this export) | prior_export=101.09 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATRC  ·  score **+16**  ·  Medical Instruments & Supplies
price=55.810001373291016  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.68 on 2026-10-09; prev RSI=45.60 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.60@2026-10-08 → 54.68@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.60@2026-10-08 → 54.68@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.60@2026-10-08 → 54.68@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=11.609 (G=2.6700 R=0.2300); 2026-10-08:RED:O=53.0700,C=52.8400,body=-0.2300,vol=595600.0; 2026-10-09:GREEN:O=53.1400,C=55.8100,body=+2.6700,vol=712491.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=1.196 (Gvol=712491 Rvol=595600); 2026-10-08:RED:O=53.0700,C=52.8400,body=-0.2300,vol=595600.0; 2026-10-09:GREEN:O=53.1400,C=55.8100,body=+2.6700,vol=712491.0 | **GOOD** |
| `A07_rvol` | RVOL=0.457 on 2026-10-09: today_vol=712491 / avg20=1558277 (avg window 2026-09-11→2026-10-08, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.217 on 2026-10-09 (price=55.8100, mid=56.8015, upper=61.3796, lower=52.2234; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=55.8100 vs SMA50=50.7342 dist=+10.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=56.8015 SMA50=50.7342 SMA80=43.7724 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-23@31.9000; S2[2026-08-12→2026-09-10] low=2026-08-12@41.8800; S3[2026-09-11→2026-10-09] low=2026-10-08@51.6300 | lows=[31.899999618530273, 41.880001068115234, 51.630001068115234] span=61.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.7727940713718675 wick_frac=0.2272059286281325 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.12568294620020304 wick_frac=0.874317053799797 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.608727381288043 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+2.2300:wick=0.5100; 2026-10-06:RED:body=-2.3100:wick=0.8400; 2026-10-07:RED:body=-0.0400:wick=1.8600; 2026-10-08:RED:body=-0.2300:wick=1.6000; 2026-10-09:GREEN:body=+2.6700:wick=0.7850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10688.24 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 569.62 | **NEUTRAL** |
| `B04_income` | 10.55 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-1.5600000000000023 (now=53.33 vs prior_export=54.89 on finviz_2026-10-09) | **BAD** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -3.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.10000000000000009 (now=-3.89 vs prior=-3.99 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 4.44 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10688.24 (this export) | prior_export=10688.24 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MH  ·  score **+16**  ·  Education & Training Services
price=13.369999885559082  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.19 on 2026-10-09; prev RSI=56.19 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.19@2026-10-08 → 56.19@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.19@2026-10-08 → 56.19@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.19@2026-10-08 → 56.19@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=4.333 (G=0.3900 R=0.0900); 2026-10-08:GREEN:O=12.9800,C=13.3700,body=+0.3900,vol=866100.0; 2026-10-09:RED:O=13.4600,C=13.3700,body=-0.0900,vol=345885.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=2.504 (Gvol=866100 Rvol=345885); 2026-10-08:GREEN:O=12.9800,C=13.3700,body=+0.3900,vol=866100.0; 2026-10-09:RED:O=13.4600,C=13.3700,body=-0.0900,vol=345885.0 | **GOOD** |
| `A07_rvol` | RVOL=0.533 on 2026-10-09: today_vol=345885 / avg20=648904 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.366 on 2026-10-09 (price=13.3700, mid=13.1105, upper=13.8197, lower=12.4013; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=13.3700 vs SMA50=12.7752 dist=+4.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=13.1105 SMA50=12.7752 SMA80=11.7416 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-23@8.9700; S2[2026-08-12→2026-09-10] low=2026-08-12@11.2100; S3[2026-09-11→2026-10-09] low=2026-09-11@12.0050 | lows=[8.970000267028809, 11.210000038146973, 12.005000114440918] span=33.84% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6141744700343172 wick_frac=0.3858255299656827 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.26087052670569827 wick_frac=0.7391294732943017 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.333329801212224 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:RED:body=-0.0700:wick=0.4087; 2026-10-06:GREEN:body=+0.2900:wick=0.2235; 2026-10-07:GREEN:body=+0.1200:wick=0.2950; 2026-10-08:GREEN:body=+0.3900:wick=0.2450; 2026-10-09:RED:body=-0.0900:wick=0.2550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.59 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2116.97 | **NEUTRAL** |
| `B04_income` | 92.68 | **GOOD** |
| `B05_profit_margin` | 4.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.92 vs prior_export=17.92 on finviz_2026-10-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.23 | **BAD** |
| `B13_short_float` | 9.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.59 (this export) | prior_export=23.59 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+16**  ·  Lodging
price=44.15999984741211  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.32 on 2026-10-09; prev RSI=46.08 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.08@2026-10-08 → 56.32@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.08@2026-10-08 → 56.32@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.08@2026-10-08 → 56.32@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5600 R=0.0000); 2026-10-08:GREEN:O=42.0700,C=42.6700,body=+0.6000,vol=1826300.0; 2026-10-09:GREEN:O=43.2000,C=44.1600,body=+0.9600,vol=1381043.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3207343 Rvol=0); 2026-10-08:GREEN:O=42.0700,C=42.6700,body=+0.6000,vol=1826300.0; 2026-10-09:GREEN:O=43.2000,C=44.1600,body=+0.9600,vol=1381043.0 | **GOOD** |
| `A07_rvol` | RVOL=1.095 on 2026-10-09: today_vol=1381043 / avg20=1261240 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.991 on 2026-10-09 (price=44.1600, mid=42.7605, upper=44.1723, lower=41.3487; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-09: price=44.1600 vs SMA50=44.1524 dist=+0.02% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=42.76_50=44.15_80=43.43 on 2026-10-09: SMA20=42.7605 SMA50=44.1524 SMA80=43.4334 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-24@40.0800; S2[2026-08-12→2026-09-10] low=2026-08-13@40.5100; S3[2026-09-11→2026-10-09] low=2026-10-02@41.2900 | lows=[40.08000183105469, 40.5099983215332, 41.290000915527344] span=3.02% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6946721232651798 wick_frac=0.3053278767348203 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:RED:body=-0.2800:wick=0.6300; 2026-10-06:GREEN:body=+0.1900:wick=0.6800; 2026-10-07:RED:body=-0.0800:wick=0.4150; 2026-10-08:GREEN:body=+0.6000:wick=0.3300; 2026-10-09:GREEN:body=+0.9600:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.56 (current export asof; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.03 (current export; earnings_date=8/17/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.86 | **NEUTRAL** |
| `B04_income` | 720.34 | **GOOD** |
| `B05_profit_margin` | 18.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.99 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.6700000000000017 (now=60.99 vs prior_export=60.32 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.21 | **GOOD** |
| `B13_short_float` | 3.59 | **NEUTRAL** |
| `B14_earnings_date` | 8/17/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.56 (this export) | prior_export=3.56 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.03 (this export) | prior_export=5.03 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+16**  ·  Business Equipment & Supplies
price=50.45000076293945  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.77 on 2026-10-09; prev RSI=48.28 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.28@2026-10-08 → 57.77@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.28@2026-10-08 → 57.77@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.28@2026-10-08 → 57.77@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2400 R=0.0000); 2026-10-08:GREEN:O=47.7700,C=48.1800,body=+0.4100,vol=655400.0; 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1074413 Rvol=0); 2026-10-08:GREEN:O=47.7700,C=48.1800,body=+0.4100,vol=655400.0; 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0 | **GOOD** |
| `A07_rvol` | RVOL=0.742 on 2026-10-09: today_vol=419013 / avg20=564372 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.941 on 2026-10-09 (price=50.4500, mid=47.8625, upper=50.6137, lower=45.1113; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-09: price=50.4500 vs SMA50=49.6532 dist=+1.60% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=47.86_50=49.65_80=49.57 on 2026-10-09: SMA20=47.8625 SMA50=49.6532 SMA80=49.5674 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-09@49.2800; S2[2026-08-12→2026-09-10] low=2026-08-20@47.5800; S3[2026-09-11→2026-10-09] low=2026-09-18@44.8400 | lows=[49.279998779296875, 47.58000183105469, 44.84000015258789] span=9.90% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6109289224897627 wick_frac=0.38907107751023723 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+0.6900:wick=1.1000; 2026-10-06:GREEN:body=+0.3400:wick=0.6200; 2026-10-07:RED:body=-0.2500:wick=1.7600; 2026-10-08:GREEN:body=+0.4100:wick=0.7600; 2026-10-09:GREEN:body=+1.8300:wick=0.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.7 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.9 | **NEUTRAL** |
| `B04_income` | 140.3 | **GOOD** |
| `B05_profit_margin` | 7.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.71 vs prior_export=68.71 on finviz_2026-10-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.45 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+15**  ·  Banks - Regional
price=14.670000076293945  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=37.21 on 2026-10-09; prev RSI=41.09 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.09@2026-10-08 → 37.21@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 41.09@2026-10-08 → 37.21@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 41.09@2026-10-08 → 37.21@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=1.500 (G=0.1500 R=0.1000); 2026-10-08:GREEN:O=14.7000,C=14.8500,body=+0.1500,vol=1019600.0; 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=1.419 (Gvol=1019600 Rvol=718370); 2026-10-08:GREEN:O=14.7000,C=14.8500,body=+0.1500,vol=1019600.0; 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0 | **GOOD** |
| `A07_rvol` | RVOL=0.609 on 2026-10-09: today_vol=718370 / avg20=1179411 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.859 on 2026-10-09 (price=14.6700, mid=15.1375, upper=15.6816, lower=14.5934; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-09: price=14.6700 vs SMA50=15.3579 dist=-4.48% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=15.14_50=15.36_80=15.19 on 2026-10-09: SMA20=15.1375 SMA50=15.3579 SMA80=15.1895 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-09@14.6047; S2[2026-08-12→2026-09-10] low=2026-09-01@15.0700; S3[2026-09-11→2026-10-09] low=2026-10-01@14.5400 | lows=[14.604661377643726, 15.069999694824219, 14.539999961853027] span=3.65% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.5000031789224725 wick_frac=0.4999968210775276 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.4878069204216638 wick_frac=0.5121930795783363 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5 need>1.4; red_wick_gt_green=False 5d trail=2026-10-05:RED:body=-0.1300:wick=0.0700; 2026-10-06:RED:body=-0.1500:wick=0.1150; 2026-10-07:RED:body=-0.0800:wick=0.1000; 2026-10-08:GREEN:body=+0.1500:wick=0.1500; 2026-10-09:RED:body=-0.1000:wick=0.1050 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.28000000000000114 (now=16.57 vs prior_export=16.29 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WCC  ·  score **+15**  ·  Industrial Distribution
price=365.69000244140625  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.72 on 2026-10-09; prev RSI=48.17 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.17@2026-10-08 → 52.72@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.17@2026-10-08 → 52.72@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.17@2026-10-08 → 52.72@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=1.711 (G=10.4700 R=6.1200); 2026-10-08:RED:O=362.0500,C=355.9300,body=-6.1200,vol=797200.0; 2026-10-09:GREEN:O=355.2200,C=365.6900,body=+10.4700,vol=835380.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=1.048 (Gvol=835380 Rvol=797200); 2026-10-08:RED:O=362.0500,C=355.9300,body=-6.1200,vol=797200.0; 2026-10-09:GREEN:O=355.2200,C=365.6900,body=+10.4700,vol=835380.0 | **GOOD** |
| `A07_rvol` | RVOL=1.660 on 2026-10-09: today_vol=835380 / avg20=503194 (avg window 2026-09-11→2026-10-08, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.203 on 2026-10-09 (price=365.6900, mid=359.6620, upper=389.4081, lower=329.9159; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=365.6900 vs SMA50=355.4628 dist=+2.88% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=359.6620 SMA50=355.4628 SMA80=348.3160 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-07-29@308.5400; S2[2026-08-12→2026-09-10] low=2026-09-02@327.8400; S3[2026-09-11→2026-10-09] low=2026-09-15@330.3900 | lows=[308.5400085449219, 327.8399963378906, 330.3900146484375] span=7.08% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.595563664264722 wick_frac=0.40443633573527804 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.48494316052165126 wick_frac=0.5150568394783488 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7107858781290515 need>1.4; red_wick_gt_green=False 5d trail=2026-10-05:RED:body=-6.2700:wick=4.0268; 2026-10-06:GREEN:body=+4.0700:wick=7.9400; 2026-10-07:RED:body=-10.2900:wick=7.5300; 2026-10-08:RED:body=-6.1200:wick=6.5000; 2026-10-09:GREEN:body=+10.4700:wick=7.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.84 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.52 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 25012.9 | **NEUTRAL** |
| `B04_income` | 715.5 | **GOOD** |
| `B05_profit_margin` | 2.86 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 415.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.020000000000038654 (now=415.58 vs prior_export=415.6 on finviz_2026-10-09) | **BAD** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | -12.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.10999999999999943 (now=-12.57 vs prior=-12.68 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | -5.43 | **BAD** |
| `B13_short_float` | 3.02 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.84 (this export) | prior_export=14.84 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.52 (this export) | prior_export=3.52 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HHH  ·  score **+15**  ·  Real Estate - Diversified
price=73.7300033569336  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.63 on 2026-10-09; prev RSI=59.94 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.94@2026-10-08 → 63.63@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.94@2026-10-08 → 63.63@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.94@2026-10-08 → 63.63@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.5900 R=0.0000); 2026-10-08:GREEN:O=71.1200,C=71.9300,body=+0.8100,vol=509700.0; 2026-10-09:GREEN:O=71.9500,C=73.7300,body=+1.7800,vol=432771.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=942471 Rvol=0); 2026-10-08:GREEN:O=71.1200,C=71.9300,body=+0.8100,vol=509700.0; 2026-10-09:GREEN:O=71.9500,C=73.7300,body=+1.7800,vol=432771.0 | **GOOD** |
| `A07_rvol` | RVOL=0.465 on 2026-10-09: today_vol=432771 / avg20=929827 (avg window 2026-09-11→2026-10-08, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.540 on 2026-10-09 (price=73.7300, mid=68.1895, upper=78.4471, lower=57.9319; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=73.7300 vs SMA50=66.5302 dist=+10.82% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=68.19_50=66.53_80=67.77 on 2026-10-09: SMA20=68.1895 SMA50=66.5302 SMA80=67.7691 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-08-03@63.8300; S2[2026-08-12→2026-09-10] low=2026-09-10@61.4500; S3[2026-09-11→2026-10-09] low=2026-09-18@60.0900 | lows=[63.83000183105469, 61.45000076293945, 60.09000015258789] span=6.22% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.7066573666669992 wick_frac=0.2933426333330008 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-05:GREEN:body=+0.1400:wick=1.3247; 2026-10-06:GREEN:body=+1.3500:wick=0.9449; 2026-10-07:RED:body=-0.4500:wick=0.8450; 2026-10-08:GREEN:body=+0.8100:wick=0.8900; 2026-10-09:GREEN:body=+1.7800:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=170.05 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=139.3 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2334.65 | **NEUTRAL** |
| `B04_income` | 292.1 | **GOOD** |
| `B05_profit_margin` | 12.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.67 vs prior_export=89.67 on finviz_2026-10-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 1.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.06999999999999984 (now=1.67 vs prior=1.6 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=170.05 (this export) | prior_export=170.05 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=139.3 (this export) | prior_export=139.3 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BEKE  ·  score **+15**  ·  Real Estate Services
price=17.84000015258789  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.81 on 2026-10-09; prev RSI=54.56 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.56@2026-10-08 → 62.81@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.56@2026-10-08 → 62.81@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.56@2026-10-08 → 62.81@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=6.750 (G=0.5400 R=0.0800); 2026-10-08:RED:O=17.1700,C=17.0900,body=-0.0800,vol=5202100.0; 2026-10-09:GREEN:O=17.3000,C=17.8400,body=+0.5400,vol=9966491.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=1.916 (Gvol=9966491 Rvol=5202100); 2026-10-08:RED:O=17.1700,C=17.0900,body=-0.0800,vol=5202100.0; 2026-10-09:GREEN:O=17.3000,C=17.8400,body=+0.5400,vol=9966491.0 | **GOOD** |
| `A07_rvol` | RVOL=1.878 on 2026-10-09: today_vol=9966491 / avg20=5308292 (avg window 2026-09-11→2026-10-08, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.475 on 2026-10-09 (price=17.8400, mid=16.6150, upper=17.4458, lower=15.7842; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-09: price=17.8400 vs SMA50=17.0502 dist=+4.63% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.62_50=17.05_80=16.54 on 2026-10-09: SMA20=16.6150 SMA50=17.0502 SMA80=16.5418 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-09 (63 bars); S1[2026-07-10→2026-08-11] low=2026-07-10@15.5250; S2[2026-08-12→2026-09-10] low=2026-09-10@16.6500; S3[2026-09-11→2026-10-09] low=2026-09-16@15.8600 | lows=[15.524999618530273, 16.649999618530273, 15.859999656677246] span=7.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.75 wick_frac=0.25 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.262294569377392 wick_frac=0.737705430622608 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.750017881410486 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+0.0700:wick=0.1100; 2026-10-06:GREEN:body=+0.1600:wick=0.1950; 2026-10-07:GREEN:body=+0.0900:wick=0.0750; 2026-10-08:RED:body=-0.0800:wick=0.2250; 2026-10-09:GREEN:body=+0.5400:wick=0.1800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.18 (current export asof; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=8/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 12683.5 | **NEUTRAL** |
| `B04_income` | 683.71 | **GOOD** |
| `B05_profit_margin` | 5.39 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.030000000000001137 (now=24.08 vs prior_export=24.11 on finviz_2026-10-09) | **BAD** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | -27.52 | **BAD** |
| `B11_insider_tx_delta` | delta=41.33 (now=-27.52 vs prior=-68.85 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 0.7 | **GOOD** |
| `B13_short_float` | 1.98 | **NEUTRAL** |
| `B14_earnings_date` | 8/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.18 (this export) | prior_export=31.18 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MICC  ·  score **+15**  ·  Packaged Foods
price=17.280000686645508  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=35.84 on 2026-10-09; prev RSI=37.30 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 37.30@2026-10-08 → 35.84@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 37.30@2026-10-08 → 35.84@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 37.30@2026-10-08 → 35.84@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.4500 R=0.0900); 2026-10-08:GREEN:O=16.9900,C=17.4400,body=+0.4500,vol=1160100.0; 2026-10-09:RED:O=17.3700,C=17.2800,body=-0.0900,vol=796920.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=1.456 (Gvol=1160100 Rvol=796920); 2026-10-08:GREEN:O=16.9900,C=17.4400,body=+0.4500,vol=1160100.0; 2026-10-09:RED:O=17.3700,C=17.2800,body=-0.0900,vol=796920.0 | **GOOD** |
| `A07_rvol` | RVOL=1.002 on 2026-10-09: today_vol=796920 / avg20=795228 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.576 on 2026-10-09 (price=17.2800, mid=18.3725, upper=20.2682, lower=16.4768; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-09: price=17.2800 vs SMA50=19.0494 dist=-9.29% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=18.37_50=19.05_80=18.66 on 2026-10-09: SMA20=18.3725 SMA50=19.0494 SMA80=18.6594 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-09 (63 bars); S1[2026-07-14→2026-08-11] low=2026-07-23@17.3900; S2[2026-08-12→2026-09-10] low=2026-08-18@18.8770; S3[2026-09-11→2026-10-09] low=2026-10-07@16.8600 | lows=[17.389999389648438, 18.87700080871582, 16.860000610351562] span=11.96% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=1.0 wick_frac=0.0 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=0.37515901284823816 wick_frac=0.6248409871517618 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:RED:body=-0.0900:wick=0.0900; 2026-10-06:RED:body=-0.0600:wick=0.1900; 2026-10-07:GREEN:body=+0.0600:wick=0.1550; 2026-10-08:GREEN:body=+0.4500:wick=0.0000; 2026-10-09:RED:body=-0.0900:wick=0.1499 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.07 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9445.44 | **NEUTRAL** |
| `B04_income` | 211.17 | **GOOD** |
| `B05_profit_margin` | 2.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.16999999999999815 (now=18.86 vs prior_export=18.69 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.3 | **GOOD** |
| `B13_short_float` | 2.03 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.0 (this export) | prior_export=6.0 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.07 (this export) | prior_export=2.59 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDLN  ·  score **+15**  ·  Medical Instruments & Supplies
price=36.2599983215332  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.31 on 2026-10-09; prev RSI=57.13 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.13@2026-10-08 → 60.31@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.13@2026-10-08 → 60.31@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.13@2026-10-08 → 60.31@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.2500 R=0.0000); 2026-10-08:GREEN:O=35.1300,C=35.7400,body=+0.6100,vol=10191300.0; 2026-10-09:GREEN:O=35.6200,C=36.2600,body=+0.6400,vol=5330878.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=15522178 Rvol=0); 2026-10-08:GREEN:O=35.1300,C=35.7400,body=+0.6100,vol=10191300.0; 2026-10-09:GREEN:O=35.6200,C=36.2600,body=+0.6400,vol=5330878.0 | **GOOD** |
| `A07_rvol` | RVOL=0.563 on 2026-10-09: today_vol=5330878 / avg20=9476523 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.670 on 2026-10-09 (price=36.2600, mid=34.2800, upper=37.2333, lower=31.3267; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=36.2600 vs SMA50=34.9437 dist=+3.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-09: SMA20=34.2800 SMA50=34.9437 SMA80=36.5041 | **BAD** |
| `A11_three_section_lows` | window=2026-07-09→2026-10-09 (63 bars); S1[2026-07-09→2026-08-11] low=2026-08-11@33.1000; S2[2026-08-12→2026-09-10] low=2026-09-10@32.2900; S3[2026-09-11→2026-10-09] low=2026-09-18@31.4900 | lows=[33.099998474121094, 32.290000915527344, 31.489999771118164] span=5.11% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6459117365008568 wick_frac=0.35408826349914324 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+0.3600:wick=0.7000; 2026-10-06:RED:body=-0.4000:wick=1.1750; 2026-10-07:RED:body=-0.1000:wick=0.6600; 2026-10-08:GREEN:body=+0.6100:wick=0.3500; 2026-10-09:GREEN:body=+0.6400:wick=0.3350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=41.88 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.21 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 29939.0 | **NEUTRAL** |
| `B04_income` | 693.0 | **GOOD** |
| `B05_profit_margin` | 2.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.11999999999999744 (now=44.5 vs prior_export=44.38 on finviz_2026-10-09) | **GOOD** |
| `B09_analyst_recom` | 1.59 | **GOOD** |
| `B10_insider_transactions` | -15.76 | **BAD** |
| `B11_insider_tx_delta` | delta=2.58 (now=-15.76 vs prior=-18.34 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 10.52 | **GOOD** |
| `B13_short_float` | 8.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=41.88 (this export) | prior_export=41.88 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.21 (this export) | prior_export=2.21 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AUPH  ·  score **+15**  ·  Biotechnology
price=16.649999618530273  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.75 on 2026-10-09; prev RSI=50.56 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.56@2026-10-08 → 57.75@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.56@2026-10-08 → 57.75@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.56@2026-10-08 → 57.75@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8000 R=0.0000); 2026-10-08:GREEN:O=15.9100,C=16.2200,body=+0.3100,vol=831000.0; 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1680222 Rvol=0); 2026-10-08:GREEN:O=15.9100,C=16.2200,body=+0.3100,vol=831000.0; 2026-10-09:GREEN:O=16.1600,C=16.6500,body=+0.4900,vol=849222.0 | **GOOD** |
| `A07_rvol` | RVOL=1.024 on 2026-10-09: today_vol=849222 / avg20=829090 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.367 on 2026-10-09 (price=16.6500, mid=16.3640, upper=17.1425, lower=15.5855; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-09: price=16.6500 vs SMA50=16.0847 dist=+3.51% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.36_50=16.08_80=16.11 on 2026-10-09: SMA20=16.3640 SMA50=16.0847 SMA80=16.1149 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-09 (63 bars); S1[2026-07-13→2026-08-11] low=2026-07-31@14.3630; S2[2026-08-12→2026-09-10] low=2026-08-14@14.9010; S3[2026-09-11→2026-10-09] low=2026-10-06@15.4700 | lows=[14.36299991607666, 14.901000022888184, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.5982357341285933 wick_frac=0.4017642658714067 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-05:RED:body=-0.0400:wick=0.1600; 2026-10-06:RED:body=-0.0800:wick=0.2600; 2026-10-07:GREEN:body=+0.4500:wick=0.2000; 2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-09) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+15**  ·  Health Information Services
price=296.1300048828125  pair=`2026-10-08→2026-10-09`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.30 on 2026-10-09; prev RSI=62.92 on 2026-10-08 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.92@2026-10-08 → 69.30@2026-10-09 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.92@2026-10-08 → 69.30@2026-10-09 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.92@2026-10-08 → 69.30@2026-10-09 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=13.1300 R=0.0000); 2026-10-08:GREEN:O=282.1300,C=284.8400,body=+2.7100,vol=921700.0; 2026-10-09:GREEN:O=285.7100,C=296.1300,body=+10.4200,vol=1844546.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-08 + 2026-10-09; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2766246 Rvol=0); 2026-10-08:GREEN:O=282.1300,C=284.8400,body=+2.7100,vol=921700.0; 2026-10-09:GREEN:O=285.7100,C=296.1300,body=+10.4200,vol=1844546.0 | **GOOD** |
| `A07_rvol` | RVOL=1.223 on 2026-10-09: today_vol=1844546 / avg20=1508020 (avg window 2026-09-11→2026-10-08, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.036 on 2026-10-09 (price=296.1300, mid=274.9650, upper=295.3891, lower=254.5409; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-09: price=296.1300 vs SMA50=259.5152 dist=+14.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-09: SMA20=274.9650 SMA50=259.5152 SMA80=230.8478 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-09 (63 bars); S1[2026-07-13→2026-08-11] low=2026-07-23@179.2500; S2[2026-08-12→2026-09-10] low=2026-08-12@230.1700; S3[2026-09-11→2026-10-09] low=2026-09-21@257.8700 | lows=[179.25, 230.1699981689453, 257.8699951171875] span=43.86% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: GREEN body_frac=0.6048556602326612 wick_frac=0.39514433976733876 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-08+2026-10-09: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-05:GREEN:body=+8.4500:wick=0.9100; 2026-10-06:RED:body=-1.8400:wick=5.7672; 2026-10-07:RED:body=-1.7300:wick=5.4550; 2026-10-08:GREEN:body=+2.7100:wick=3.5200; 2026-10-09:GREEN:body=+10.4200:wick=3.0300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-10-09) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.02999999999999997 (now=-0.38 vs prior=-0.41 on finviz_2026-10-09) | **GOOD** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 3.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-09) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-12_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.