# A+B1 Feature Checklist — 2026-10-05

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-05.csv` · prior export for Δ: `2026-10-02`
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
| 1 | SB | +17 | 17 | 0 | 2026-10-01→2026-10-02 | Marine Shipping |
| 2 | SKM | +17 | 17 | 0 | 2026-10-01→2026-10-02 | Telecom Services |
| 3 | EXLS | +17 | 18 | 1 | 2026-10-01→2026-10-02 | Information Technology Services |
| 4 | CORT | +17 | 18 | 1 | 2026-10-01→2026-10-02 | Biotechnology |
| 5 | DSX | +16 | 16 | 0 | 2026-10-01→2026-10-02 | Marine Shipping |
| 6 | HUM | +16 | 17 | 1 | 2026-10-01→2026-10-02 | Healthcare Plans |
| 7 | PBR | +16 | 17 | 1 | 2026-10-01→2026-10-02 | Oil & Gas Integrated |
| 8 | CBRL | +16 | 16 | 0 | 2026-10-01→2026-10-02 | Restaurants |
| 9 | SLDE | +15 | 17 | 2 | 2026-10-01→2026-10-02 | Insurance - Property & Casualty |
| 10 | SBLK | +15 | 16 | 1 | 2026-10-01→2026-10-02 | Marine Shipping |
| 11 | RNG | +15 | 16 | 1 | 2026-10-01→2026-10-02 | Software - Application |
| 12 | MICC | +15 | 16 | 1 | 2026-10-01→2026-10-02 | Packaged Foods |
| 13 | PBR-A | +15 | 16 | 1 | 2026-10-01→2026-10-02 | Oil & Gas Integrated |
| 14 | PARR | +15 | 17 | 2 | 2026-10-01→2026-10-02 | Oil & Gas Refining & Marketing |
| 15 | DINO | +15 | 16 | 1 | 2026-10-01→2026-10-02 | Oil & Gas Refining & Marketing |

## Full checklist — top 15

### SB  ·  score **+17**  ·  Marine Shipping
price=8.8100004196167  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.60 on 2026-10-02; prev RSI=59.52 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.52@2026-10-01 → 56.60@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.52@2026-10-01 → 56.60@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.52@2026-10-01 → 56.60@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=23.001 (G=0.4600 R=0.0200); 2026-10-01:GREEN:O=8.4800,C=8.9400,body=+0.4600,vol=1684800.0; 2026-10-02:RED:O=8.8300,C=8.8100,body=-0.0200,vol=1211922.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.390 (Gvol=1684800 Rvol=1211922); 2026-10-01:GREEN:O=8.4800,C=8.9400,body=+0.4600,vol=1684800.0; 2026-10-02:RED:O=8.8300,C=8.8100,body=-0.0200,vol=1211922.0 | **GOOD** |
| `A07_rvol` | RVOL=0.659 on 2026-10-02: today_vol=1211922 / avg20=1838579 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.309 on 2026-10-02 (price=8.8100, mid=8.6100, upper=9.2565, lower=7.9635; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=8.8100 vs SMA50=8.2458 dist=+6.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=8.6100 SMA50=8.2458 SMA80=7.6506 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-02 (63 bars); S1[2026-07-02→2026-08-04] low=2026-07-02@6.3060; S2[2026-08-05→2026-09-02] low=2026-08-11@7.1672; S3[2026-09-03→2026-10-02] low=2026-09-25@7.9917 | lows=[6.305958045143694, 7.167211325967238, 7.991700172424316] span=26.73% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.8363634156787472 wick_frac=0.1636365843212529 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.1025612183515672 wick_frac=0.8974387816484328 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=23.000572218778313 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:GREEN:body=+0.0600:wick=0.0700; 2026-09-29:GREEN:body=+0.1200:wick=0.2016; 2026-09-30:GREEN:body=+0.1300:wick=0.1000; 2026-10-01:GREEN:body=+0.4600:wick=0.0900; 2026-10-02:RED:body=-0.0200:wick=0.1750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.0 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=12.86 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 307.5 | **NEUTRAL** |
| `B04_income` | 78.98 | **GOOD** |
| `B05_profit_margin` | 25.69 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.71 vs prior_export=8.71 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.82 | **GOOD** |
| `B13_short_float` | 5.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.0 (this export) | prior_export=12.0 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=12.86 (this export) | prior_export=12.86 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+17**  ·  Telecom Services
price=36.790000915527344  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.60 on 2026-10-02; prev RSI=45.85 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.85@2026-10-01 → 52.60@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.85@2026-10-01 → 52.60@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.85@2026-10-01 → 52.60@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0500 R=0.0000); 2026-10-01:GREEN:O=34.9400,C=35.5900,body=+0.6500,vol=1957100.0; 2026-10-02:GREEN:O=36.3900,C=36.7900,body=+0.4000,vol=1124409.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3081509 Rvol=0); 2026-10-01:GREEN:O=34.9400,C=35.5900,body=+0.6500,vol=1957100.0; 2026-10-02:GREEN:O=36.3900,C=36.7900,body=+0.4000,vol=1124409.0 | **GOOD** |
| `A07_rvol` | RVOL=0.955 on 2026-10-02: today_vol=1124409 / avg20=1177446 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.217 on 2026-10-02 (price=36.7900, mid=36.3365, upper=38.4268, lower=34.2462; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=36.7900 vs SMA50=36.2998 dist=+1.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=36.3365 SMA50=36.2998 SMA80=35.2805 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-02 (63 bars); S1[2026-07-07→2026-08-04] low=2026-07-29@29.5300; S2[2026-08-05→2026-09-02] low=2026-08-10@32.8400; S3[2026-09-03→2026-10-02] low=2026-10-01@34.5800 | lows=[29.530000686645508, 32.84000015258789, 34.58000183105469] span=17.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5291482278760609 wick_frac=0.47085177212393914 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-28:RED:body=-0.3200:wick=0.4260; 2026-09-29:GREEN:body=+0.2400:wick=0.4600; 2026-09-30:RED:body=-0.6000:wick=0.1900; 2026-10-01:GREEN:body=+0.6500:wick=1.0700; 2026-10-02:GREEN:body=+0.4000:wick=0.1879 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=44.43 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.38 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11760.23 | **NEUTRAL** |
| `B04_income` | 483.22 | **GOOD** |
| `B05_profit_margin` | 4.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.030000000000001137 (now=45.14 vs prior_export=45.11 on finviz_2026-10-02) | **GOOD** |
| `B09_analyst_recom` | 1.85 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EXLS  ·  score **+17**  ·  Information Technology Services
price=35.75  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.12 on 2026-10-02; prev RSI=58.86 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.86@2026-10-01 → 53.12@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.86@2026-10-01 → 53.12@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.86@2026-10-01 → 53.12@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=1.692 (G=1.1000 R=0.6500); 2026-10-01:GREEN:O=35.3800,C=36.4800,body=+1.1000,vol=3022900.0; 2026-10-02:RED:O=36.4000,C=35.7500,body=-0.6500,vol=2914822.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.037 (Gvol=3022900 Rvol=2914822); 2026-10-01:GREEN:O=35.3800,C=36.4800,body=+1.1000,vol=3022900.0; 2026-10-02:RED:O=36.4000,C=35.7500,body=-0.6500,vol=2914822.0 | **GOOD** |
| `A07_rvol` | RVOL=1.472 on 2026-10-02: today_vol=2914822 / avg20=1980380 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.289 on 2026-10-02 (price=35.7500, mid=35.3505, upper=36.7335, lower=33.9675; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=35.7500 vs SMA50=35.1580 dist=+1.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=35.3505 SMA50=35.1580 SMA80=32.3055 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-01@26.2000; S2[2026-08-05→2026-09-02] low=2026-08-06@33.3300; S3[2026-09-03→2026-10-02] low=2026-09-29@34.0500 | lows=[26.200000762939453, 33.33000183105469, 34.04999923706055] span=29.96% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.670731026526112 wick_frac=0.32926897347388795 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.3641465442259156 wick_frac=0.6358534557740844 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6923013721140416 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:GREEN:body=+0.0300:wick=0.4600; 2026-09-29:GREEN:body=+0.2600:wick=0.5300; 2026-09-30:RED:body=-0.1600:wick=0.3300; 2026-10-01:GREEN:body=+1.1000:wick=0.5400; 2026-10-02:RED:body=-0.6500:wick=1.1350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.76 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.63 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2237.31 | **NEUTRAL** |
| `B04_income` | 250.0 | **GOOD** |
| `B05_profit_margin` | 11.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.62 vs prior_export=44.62 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.18 | **GOOD** |
| `B10_insider_transactions` | -1.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.20999999999999996 (now=-1.07 vs prior=-1.28 on finviz_2026-10-02) | **GOOD** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.76 (this export) | prior_export=7.76 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.63 (this export) | prior_export=3.63 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CORT  ·  score **+17**  ·  Biotechnology
price=116.2300033569336  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.23 on 2026-10-02; prev RSI=50.72 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.72@2026-10-01 → 54.23@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.72@2026-10-01 → 54.23@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.72@2026-10-01 → 54.23@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=7.115 (G=1.8500 R=0.2600); 2026-10-01:RED:O=114.2900,C=114.0300,body=-0.2600,vol=879000.0; 2026-10-02:GREEN:O=114.3800,C=116.2300,body=+1.8500,vol=930247.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.058 (Gvol=930247 Rvol=879000); 2026-10-01:RED:O=114.2900,C=114.0300,body=-0.2600,vol=879000.0; 2026-10-02:GREEN:O=114.3800,C=116.2300,body=+1.8500,vol=930247.0 | **GOOD** |
| `A07_rvol` | RVOL=0.533 on 2026-10-02: today_vol=930247 / avg20=1745560 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.468 on 2026-10-02 (price=116.2300, mid=114.1635, upper=118.5770, lower=109.7500; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=116.2300 vs SMA50=112.6420 dist=+3.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=114.1635 SMA50=112.6420 SMA80=102.4563 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-13@84.8800; S2[2026-08-05→2026-09-02] low=2026-08-06@107.3200; S3[2026-09-03→2026-10-02] low=2026-09-08@108.2500 | lows=[84.87999725341797, 107.31999969482422, 108.25] span=27.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5417293140695827 wick_frac=0.45827068593041725 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.06435694982918846 wick_frac=0.9356430501708115 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.115349628803662 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:RED:body=-0.3700:wick=2.3400; 2026-09-29:RED:body=-1.1800:wick=6.1294; 2026-09-30:GREEN:body=+0.4300:wick=3.6600; 2026-10-01:RED:body=-0.2600:wick=3.7800; 2026-10-02:GREEN:body=+1.8500:wick=1.5650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1381.48 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.83 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 830.81 | **NEUTRAL** |
| `B04_income` | 54.2 | **GOOD** |
| `B05_profit_margin` | 6.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 141.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=141.0 vs prior_export=141.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -6.34 | **BAD** |
| `B11_insider_tx_delta` | delta=0.009999999999999787 (now=-6.34 vs prior=-6.35 on finviz_2026-10-02) | **GOOD** |
| `B12_institutional_transactions` | 2.39 | **GOOD** |
| `B13_short_float` | 9.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1381.48 (this export) | prior_export=1381.48 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.83 (this export) | prior_export=15.83 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DSX  ·  score **+16**  ·  Marine Shipping
price=2.859999895095825  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.91 on 2026-10-02; prev RSI=49.83 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.83@2026-10-01 → 52.91@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.83@2026-10-01 → 52.91@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.83@2026-10-01 → 52.91@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1000 R=0.0000); 2026-10-01:GREEN:O=2.7600,C=2.8200,body=+0.0600,vol=974100.0; 2026-10-02:GREEN:O=2.8200,C=2.8600,body=+0.0400,vol=828236.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1802336 Rvol=0); 2026-10-01:GREEN:O=2.7600,C=2.8200,body=+0.0600,vol=974100.0; 2026-10-02:GREEN:O=2.8200,C=2.8600,body=+0.0400,vol=828236.0 | **GOOD** |
| `A07_rvol` | RVOL=0.603 on 2026-10-02: today_vol=828236 / avg20=1372989 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.287 on 2026-10-02 (price=2.8600, mid=2.9170, upper=3.1155, lower=2.7185; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=2.8600 vs SMA50=2.6902 dist=+6.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=2.9170 SMA50=2.6902 SMA80=2.4969 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-17@2.0400; S2[2026-08-05→2026-09-02] low=2026-08-12@2.3000; S3[2026-09-03→2026-10-02] low=2026-09-30@2.7400 | lows=[2.0399999618530273, 2.299999952316284, 2.740000009536743] span=34.31% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5357142857142857 wick_frac=0.4642857142857143 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-28:DOJI:body=+0.0000:wick=0.0750; 2026-09-29:RED:body=-0.0200:wick=0.0500; 2026-09-30:RED:body=-0.0300:wick=0.0500; 2026-10-01:GREEN:body=+0.0600:wick=0.0600; 2026-10-02:GREEN:body=+0.0400:wick=0.0300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=638.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.92 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 215.94 | **NEUTRAL** |
| `B04_income` | 54.43 | **GOOD** |
| `B05_profit_margin` | 25.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 3.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=3.8 vs prior_export=3.8 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.07 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=638.0 (this export) | prior_export=638.0 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.92 (this export) | prior_export=4.92 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+16**  ·  Healthcare Plans
price=388.3599853515625  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.67 on 2026-10-02; prev RSI=44.93 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.93@2026-10-01 → 50.67@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.93@2026-10-01 → 50.67@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.93@2026-10-01 → 50.67@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=30.687 (G=9.8200 R=0.3200); 2026-10-01:RED:O=380.0000,C=379.6800,body=-0.3200,vol=891900.0; 2026-10-02:GREEN:O=378.5400,C=388.3600,body=+9.8200,vol=755960.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=0.848 (Gvol=755960 Rvol=891900); 2026-10-01:RED:O=380.0000,C=379.6800,body=-0.3200,vol=891900.0; 2026-10-02:GREEN:O=378.5400,C=388.3600,body=+9.8200,vol=755960.0 | **BAD** |
| `A07_rvol` | RVOL=0.728 on 2026-10-02: today_vol=755960 / avg20=1037735 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.083 on 2026-10-02 (price=388.3600, mid=390.2000, upper=412.4419, lower=367.9581; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=388.3600 vs SMA50=385.9786 dist=+0.62% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=390.2000 SMA50=385.9786 SMA80=384.4189 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-29@355.8100; S2[2026-08-05→2026-09-02] low=2026-08-05@353.6900; S3[2026-09-03→2026-10-02] low=2026-09-23@369.1300 | lows=[355.80999755859375, 353.69000244140625, 369.1300048828125] span=4.37% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.854654930624907 wick_frac=0.14534506937509295 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.030104242326805867 wick_frac=0.9698957576731941 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=30.68672515735266 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:RED:body=-7.8000:wick=11.1425; 2026-09-29:GREEN:body=+0.0900:wick=9.4250; 2026-09-30:RED:body=-3.9700:wick=8.8600; 2026-10-01:RED:body=-0.3200:wick=10.3100; 2026-10-02:GREEN:body=+9.8200:wick=1.6700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 425.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=425.46 vs prior_export=425.46 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 2.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+16**  ·  Oil & Gas Integrated
price=21.649999618530273  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.15 on 2026-10-02; prev RSI=58.00 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.00@2026-10-01 → 64.15@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.00@2026-10-01 → 64.15@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.00@2026-10-01 → 64.15@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.9750 R=0.0000); 2026-10-01:GREEN:O=20.8400,C=20.9800,body=+0.1400,vol=20427300.0; 2026-10-02:GREEN:O=20.8150,C=21.6500,body=+0.8350,vol=21041146.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=41468446 Rvol=0); 2026-10-01:GREEN:O=20.8400,C=20.9800,body=+0.1400,vol=20427300.0; 2026-10-02:GREEN:O=20.8150,C=21.6500,body=+0.8350,vol=21041146.0 | **GOOD** |
| `A07_rvol` | RVOL=0.983 on 2026-10-02: today_vol=21041146 / avg20=21402344 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.927 on 2026-10-02 (price=21.6500, mid=20.9285, upper=21.7070, lower=20.1500; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-02: price=21.6500 vs SMA50=19.5620 dist=+10.67% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=20.9285 SMA50=19.5620 SMA80=18.7104 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-02 (63 bars); S1[2026-07-07→2026-08-04] low=2026-07-07@16.3400; S2[2026-08-05→2026-09-02] low=2026-08-27@17.5400; S3[2026-09-03→2026-10-02] low=2026-09-04@20.1200 | lows=[16.34000015258789, 17.540000915527344, 20.1200008392334] span=23.13% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.6586409558901504 wick_frac=0.34135904410984963 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:GREEN:body=+0.0550:wick=0.3400; 2026-09-29:GREEN:body=+0.3600:wick=0.0600; 2026-09-30:RED:body=-0.1200:wick=0.3800; 2026-10-01:GREEN:body=+0.1400:wick=0.2400; 2026-10-02:GREEN:body=+0.8350:wick=0.0450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=55.7599983215332  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.14 on 2026-10-02; prev RSI=51.91 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.91@2026-10-01 → 61.14@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.91@2026-10-01 → 61.14@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.91@2026-10-01 → 61.14@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=8.458 (G=4.0600 R=0.4800); 2026-10-01:RED:O=52.2000,C=51.7200,body=-0.4800,vol=763800.0; 2026-10-02:GREEN:O=51.7000,C=55.7600,body=+4.0600,vol=1048277.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.372 (Gvol=1048277 Rvol=763800); 2026-10-01:RED:O=52.2000,C=51.7200,body=-0.4800,vol=763800.0; 2026-10-02:GREEN:O=51.7000,C=55.7600,body=+4.0600,vol=1048277.0 | **GOOD** |
| `A07_rvol` | RVOL=0.901 on 2026-10-02: today_vol=1048277 / avg20=1163201 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.789 on 2026-10-02 (price=55.7600, mid=49.6535, upper=57.3975, lower=41.9095; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=55.7600 vs SMA50=53.7054 dist=+3.83% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=49.65_50=53.71_80=51.50 on 2026-10-02: SMA20=49.6535 SMA50=53.7054 SMA80=51.4982 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-08@46.8395; S2[2026-08-05→2026-09-02] low=2026-09-02@53.2000; S3[2026-09-03→2026-10-02] low=2026-09-17@42.8700 | lows=[46.8394908080273, 53.20000076293945, 42.869998931884766] span=24.10% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.9759609873830717 wick_frac=0.02403901261692827 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.31578986994865205 wick_frac=0.6842101300513479 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.458336313568415 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:GREEN:body=+1.4800:wick=0.9000; 2026-09-29:RED:body=-1.5800:wick=2.0750; 2026-09-30:RED:body=-0.6600:wick=1.0800; 2026-10-01:RED:body=-0.4800:wick=1.0400; 2026-10-02:GREEN:body=+4.0600:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=469.95 (current export asof; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3318.71 | **NEUTRAL** |
| `B04_income` | 31.68 | **GOOD** |
| `B05_profit_margin` | 0.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.71 vs prior_export=50.71 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.64 (now=0.0 vs prior=-0.64 on finviz_2026-10-02) | **GOOD** |
| `B12_institutional_transactions` | 5.15 | **GOOD** |
| `B13_short_float` | 26.09 | **GOOD** |
| `B14_earnings_date` | 9/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=469.95 (this export) | prior_export=469.95 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SLDE  ·  score **+15**  ·  Insurance - Property & Casualty
price=23.610000610351562  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.76 on 2026-10-02; prev RSI=50.91 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.91@2026-10-01 → 50.76@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.91@2026-10-01 → 50.76@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.91@2026-10-01 → 50.76@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=32.251 (G=1.2900 R=0.0400); 2026-10-01:GREEN:O=22.3400,C=23.6300,body=+1.2900,vol=2405600.0; 2026-10-02:RED:O=23.6500,C=23.6100,body=-0.0400,vol=1110484.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=2.166 (Gvol=2405600 Rvol=1110484); 2026-10-01:GREEN:O=22.3400,C=23.6300,body=+1.2900,vol=2405600.0; 2026-10-02:RED:O=23.6500,C=23.6100,body=-0.0400,vol=1110484.0 | **GOOD** |
| `A07_rvol` | RVOL=0.736 on 2026-10-02: today_vol=1110484 / avg20=1507873 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.254 on 2026-10-02 (price=23.6100, mid=24.2035, upper=26.5392, lower=21.8678; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=23.6100 vs SMA50=22.8389 dist=+3.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=24.2035 SMA50=22.8389 SMA80=21.3297 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-23@19.1477; S2[2026-08-05→2026-09-02] low=2026-08-11@20.3139; S3[2026-09-03→2026-10-02] low=2026-10-01@22.2550 | lows=[19.14771451817286, 20.313922820890404, 22.2549991607666] span=16.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.9116594865933023 wick_frac=0.08834051340669768 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.08695453866949729 wick_frac=0.9130454613305027 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=32.25077487959563 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:RED:body=-0.2100:wick=0.1650; 2026-09-29:RED:body=-0.2400:wick=0.4300; 2026-09-30:RED:body=-0.2700:wick=0.2400; 2026-10-01:GREEN:body=+1.2900:wick=0.1250; 2026-10-02:RED:body=-0.0400:wick=0.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.75 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.66 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1388.8 | **NEUTRAL** |
| `B04_income` | 555.76 | **GOOD** |
| `B05_profit_margin` | 40.02 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.0 vs prior_export=26.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -11.11 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.8099999999999987 (now=-11.11 vs prior=-10.3 on finviz_2026-10-02) | **BAD** |
| `B12_institutional_transactions` | 9.23 | **GOOD** |
| `B13_short_float` | 9.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.75 (this export) | prior_export=24.75 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.66 (this export) | prior_export=7.66 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SBLK  ·  score **+15**  ·  Marine Shipping
price=30.75  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.51 on 2026-10-02; prev RSI=51.25 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.25@2026-10-01 → 53.51@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.25@2026-10-01 → 53.51@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.25@2026-10-01 → 53.51@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.6200 R=0.0000); 2026-10-01:GREEN:O=29.6100,C=30.4300,body=+0.8200,vol=2098000.0; 2026-10-02:GREEN:O=29.9500,C=30.7500,body=+0.8000,vol=1412498.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3510498 Rvol=0); 2026-10-01:GREEN:O=29.6100,C=30.4300,body=+0.8200,vol=2098000.0; 2026-10-02:GREEN:O=29.9500,C=30.7500,body=+0.8000,vol=1412498.0 | **GOOD** |
| `A07_rvol` | RVOL=0.718 on 2026-10-02: today_vol=1412498 / avg20=1967631 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.005 on 2026-10-02 (price=30.7500, mid=30.7600, upper=32.6670, lower=28.8530; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=30.7500 vs SMA50=29.8489 dist=+3.02% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=30.7600 SMA50=29.8489 SMA80=28.4228 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-01→2026-10-02 (63 bars); S1[2026-07-01→2026-08-04] low=2026-07-01@24.7800; S2[2026-08-05→2026-09-02] low=2026-08-12@27.1500; S3[2026-09-03→2026-10-02] low=2026-09-29@28.9400 | lows=[24.780000686645508, 27.149999618530273, 28.940000534057617] span=16.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.7625551720861904 wick_frac=0.2374448279138096 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-28:RED:body=-0.3000:wick=0.1300; 2026-09-29:GREEN:body=+0.0500:wick=0.5899; 2026-09-30:RED:body=-0.3300:wick=0.5000; 2026-10-01:GREEN:body=+0.8200:wick=0.4400; 2026-10-02:GREEN:body=+0.8000:wick=0.1150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=26.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.01 | **NEUTRAL** |
| `B04_income` | 287.15 | **GOOD** |
| `B05_profit_margin` | 23.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.6200000000000045 (now=35.55 vs prior_export=36.17 on finviz_2026-10-02) | **BAD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.57 | **GOOD** |
| `B13_short_float` | 3.28 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.91 (this export) | prior_export=26.91 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=0.11 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RNG  ·  score **+15**  ·  Software - Application
price=78.27999877929688  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.70 on 2026-10-02; prev RSI=62.78 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.78@2026-10-01 → 62.70@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.78@2026-10-01 → 62.70@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.78@2026-10-01 → 62.70@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=4.436 (G=1.7300 R=0.3900); 2026-10-01:GREEN:O=76.5800,C=78.3100,body=+1.7300,vol=1318200.0; 2026-10-02:RED:O=78.6700,C=78.2800,body=-0.3900,vol=1207296.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.092 (Gvol=1318200 Rvol=1207296); 2026-10-01:GREEN:O=76.5800,C=78.3100,body=+1.7300,vol=1318200.0; 2026-10-02:RED:O=78.6700,C=78.2800,body=-0.3900,vol=1207296.0 | **GOOD** |
| `A07_rvol` | RVOL=0.770 on 2026-10-02: today_vol=1207296 / avg20=1567311 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.529 on 2026-10-02 (price=78.2800, mid=74.5100, upper=81.6405, lower=67.3795; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=78.2800 vs SMA50=67.9111 dist=+15.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=74.5100 SMA50=67.9111 SMA80=57.0099 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-02 (63 bars); S1[2026-07-02→2026-08-04] low=2026-07-23@37.0146; S2[2026-08-05→2026-09-02] low=2026-08-06@59.6500; S3[2026-09-03→2026-10-02] low=2026-09-11@67.9700 | lows=[37.01458919963156, 59.650001525878906, 67.97000122070312] span=83.63% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5133514295481455 wick_frac=0.4866485704518545 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.14606728730547888 wick_frac=0.8539327126945211 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.43589342306037 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:GREEN:body=+1.7300:wick=1.4400; 2026-09-29:RED:body=-1.0000:wick=2.8300; 2026-09-30:GREEN:body=+0.7100:wick=1.6500; 2026-10-01:GREEN:body=+1.7300:wick=1.6400; 2026-10-02:RED:body=-0.3900:wick=2.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.76 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.0 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2583.9 | **NEUTRAL** |
| `B04_income` | 110.26 | **GOOD** |
| `B05_profit_margin` | 4.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 57.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=57.46 vs prior_export=57.46 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | -1.51 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.51 vs prior=-1.51 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.62 | **GOOD** |
| `B13_short_float` | 9.15 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.76 (this export) | prior_export=4.76 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.0 (this export) | prior_export=1.0 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MICC  ·  score **+15**  ·  Packaged Foods
price=17.479999542236328  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=32.50 on 2026-10-02; prev RSI=27.38 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | cross_up | RSI 27.38@2026-10-01 → 32.50@2026-10-02 vs 30 | rule: cross_up=GOOD | **GOOD** |
| `A03_rsi_cross_50` | below | RSI 27.38@2026-10-01 → 32.50@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 27.38@2026-10-01 → 32.50@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.2900 R=0.0000); 2026-10-01:GREEN:O=17.1200,C=17.1800,body=+0.0600,vol=1151800.0; 2026-10-02:GREEN:O=17.2500,C=17.4800,body=+0.2300,vol=660693.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1812493 Rvol=0); 2026-10-01:GREEN:O=17.1200,C=17.1800,body=+0.0600,vol=1151800.0; 2026-10-02:GREEN:O=17.2500,C=17.4800,body=+0.2300,vol=660693.0 | **GOOD** |
| `A07_rvol` | RVOL=0.820 on 2026-10-02: today_vol=660693 / avg20=805506 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-1.031 on 2026-10-02 (price=17.4800, mid=18.9205, upper=20.3183, lower=17.5227; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-02: price=17.4800 vs SMA50=19.1572 dist=-8.75% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=18.92_50=19.16_80=18.70 on 2026-10-02: SMA20=18.9205 SMA50=19.1572 SMA80=18.6967 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-02 (63 bars); S1[2026-07-07→2026-08-04] low=2026-07-23@17.3900; S2[2026-08-05→2026-09-02] low=2026-08-05@18.7800; S3[2026-09-03→2026-10-02] low=2026-10-01@17.0700 | lows=[17.389999389648438, 18.780000686645508, 17.06999969482422] span=10.02% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5848435661630547 wick_frac=0.4151564338369453 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-28:GREEN:body=+0.0400:wick=0.1940; 2026-09-29:GREEN:body=+0.1300:wick=0.0950; 2026-09-30:RED:body=-0.4900:wick=0.0460; 2026-10-01:GREEN:body=+0.0600:wick=0.1200; 2026-10-02:GREEN:body=+0.2300:wick=0.0450 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.0 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.07 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9445.44 | **NEUTRAL** |
| `B04_income` | 211.17 | **GOOD** |
| `B05_profit_margin` | 2.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.05000000000000071 (now=18.86 vs prior_export=18.81 on finviz_2026-10-02) | **GOOD** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.3 | **GOOD** |
| `B13_short_float` | 2.03 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.0 (this export) | prior_export=6.0 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.07 (this export) | prior_export=3.07 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR-A  ·  score **+15**  ·  Oil & Gas Integrated
price=19.600000381469727  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.27 on 2026-10-02; prev RSI=60.32 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.32@2026-10-01 → 65.27@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.32@2026-10-01 → 65.27@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.32@2026-10-01 → 65.27@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.7500 R=0.0000); 2026-10-01:GREEN:O=19.0400,C=19.1200,body=+0.0800,vol=6395900.0; 2026-10-02:GREEN:O=18.9300,C=19.6000,body=+0.6700,vol=7941830.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=14337730 Rvol=0); 2026-10-01:GREEN:O=19.0400,C=19.1200,body=+0.0800,vol=6395900.0; 2026-10-02:GREEN:O=18.9300,C=19.6000,body=+0.6700,vol=7941830.0 | **GOOD** |
| `A07_rvol` | RVOL=1.105 on 2026-10-02: today_vol=7941830 / avg20=7184215 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.009 on 2026-10-02 (price=19.6000, mid=18.9640, upper=19.5941, lower=18.3339; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-02: price=19.6000 vs SMA50=17.6022 dist=+11.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=18.9640 SMA50=17.6022 SMA80=16.8078 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-02 (63 bars); S1[2026-07-07→2026-08-04] low=2026-07-07@14.7300; S2[2026-08-05→2026-09-02] low=2026-08-13@15.8400; S3[2026-09-03→2026-10-02] low=2026-09-04@18.2400 | lows=[14.729999542236328, 15.84000015258789, 18.239999771118164] span=23.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.5602692567039143 wick_frac=0.4397307432960857 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:DOJI:body=+0.0000:wick=0.2900; 2026-09-29:GREEN:body=+0.3600:wick=0.1000; 2026-09-30:RED:body=-0.1000:wick=0.2600; 2026-10-01:GREEN:body=+0.0800:wick=0.2500; 2026-10-02:GREEN:body=+0.6700:wick=0.0930 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.94 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.74 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.51 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.51 vs prior_export=22.51 on finviz_2026-10-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.42 | **GOOD** |
| `B13_short_float` | 0.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.94 (this export) | prior_export=12.94 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.74 (this export) | prior_export=6.74 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PARR  ·  score **+15**  ·  Oil & Gas Refining & Marketing
price=84.0  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.11 on 2026-10-02; prev RSI=62.20 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.20@2026-10-01 → 57.11@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.20@2026-10-01 → 57.11@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.20@2026-10-01 → 57.11@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=3.502 (G=4.5700 R=1.3050); 2026-10-01:GREEN:O=81.6400,C=86.2100,body=+4.5700,vol=984600.0; 2026-10-02:RED:O=85.3050,C=84.0000,body=-1.3050,vol=821212.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=1.199 (Gvol=984600 Rvol=821212); 2026-10-01:GREEN:O=81.6400,C=86.2100,body=+4.5700,vol=984600.0; 2026-10-02:RED:O=85.3050,C=84.0000,body=-1.3050,vol=821212.0 | **GOOD** |
| `A07_rvol` | RVOL=0.848 on 2026-10-02: today_vol=821212 / avg20=968296 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.355 on 2026-10-02 (price=84.0000, mid=81.8125, upper=87.9816, lower=75.6434; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=84.0000 vs SMA50=79.3890 dist=+5.81% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=81.8125 SMA50=79.3890 SMA80=72.4695 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-02 (63 bars); S1[2026-07-02→2026-08-04] low=2026-07-02@57.8600; S2[2026-08-05→2026-09-02] low=2026-08-07@64.9800; S3[2026-09-03→2026-10-02] low=2026-09-25@74.2250 | lows=[57.86000061035156, 64.9800033569336, 74.2249984741211] span=28.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.8059955946010315 wick_frac=0.1940044053989685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=0.26588147881018703 wick_frac=0.734118521189813 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.501914656034236 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:RED:body=-0.2900:wick=1.6950; 2026-09-29:GREEN:body=+1.9400:wick=1.3100; 2026-09-30:GREEN:body=+1.7500:wick=3.3800; 2026-10-01:GREEN:body=+4.5700:wick=1.1000; 2026-10-02:RED:body=-1.3050:wick=3.6032 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.8 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.66 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8618.8 | **NEUTRAL** |
| `B04_income` | 856.91 | **GOOD** |
| `B05_profit_margin` | 9.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 86.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7099999999999937 (now=86.57 vs prior_export=85.86 on finviz_2026-10-02) | **GOOD** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | -12.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-12.82 vs prior=-12.82 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.45 | **BAD** |
| `B13_short_float` | 9.39 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.8 (this export) | prior_export=22.8 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.66 (this export) | prior_export=23.66 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+15**  ·  Oil & Gas Refining & Marketing
price=113.36000061035156  pair=`2026-10-01→2026-10-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.63 on 2026-10-02; prev RSI=64.74 on 2026-10-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.74@2026-10-01 → 65.63@2026-10-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.74@2026-10-01 → 65.63@2026-10-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.74@2026-10-01 → 65.63@2026-10-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=7.4900 R=0.0000); 2026-10-01:GREEN:O=107.4900,C=112.7000,body=+5.2100,vol=2463300.0; 2026-10-02:GREEN:O=111.0800,C=113.3600,body=+2.2800,vol=2340114.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-01 + 2026-10-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4803414 Rvol=0); 2026-10-01:GREEN:O=107.4900,C=112.7000,body=+5.2100,vol=2463300.0; 2026-10-02:GREEN:O=111.0800,C=113.3600,body=+2.2800,vol=2340114.0 | **GOOD** |
| `A07_rvol` | RVOL=0.838 on 2026-10-02: today_vol=2340114 / avg20=2793344 (avg window 2026-09-03→2026-10-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.586 on 2026-10-02 (price=113.3600, mid=109.1480, upper=116.3395, lower=101.9565; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-02: price=113.3600 vs SMA50=99.2402 dist=+14.23% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-02: SMA20=109.1480 SMA50=99.2402 SMA80=90.1486 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-02 (63 bars); S1[2026-07-07→2026-08-04] low=2026-07-07@72.0633; S2[2026-08-05→2026-09-02] low=2026-08-07@80.0041; S3[2026-09-03→2026-10-02] low=2026-09-23@103.0000 | lows=[72.063306243067, 80.0040801936391, 103.0] span=42.93% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: GREEN body_frac=0.685149438526 wick_frac=0.3148505614740001 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-01+2026-10-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-28:RED:body=-0.9300:wick=2.1050; 2026-09-29:GREEN:body=+1.4300:wick=1.7630; 2026-09-30:RED:body=-0.0200:wick=5.1000; 2026-10-01:GREEN:body=+5.2100:wick=0.3900; 2026-10-02:GREEN:body=+2.2800:wick=2.9025 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-2.5 (now=103.43 vs prior_export=105.93 on finviz_2026-10-02) | **BAD** |
| `B09_analyst_recom` | 2.58 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-10-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.14 | **GOOD** |
| `B13_short_float` | 5.85 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-10-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-05_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.