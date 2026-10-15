# A+B1 Feature Checklist — 2026-10-15

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-15.csv` · prior export for Δ: `2026-10-14`
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
| 1 | RSG | +16 | 17 | 1 | 2026-10-13→2026-10-14 | Waste Management |
| 2 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 3 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 4 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 5 | GCT | +15 | 17 | 2 | 2026-10-13→2026-10-14 | Software - Infrastructure |
| 6 | CVLT | +15 | 16 | 1 | 2026-10-13→2026-10-14 | Software - Application |
| 7 | SONO | +15 | 16 | 1 | 2026-10-13→2026-10-14 | Consumer Electronics |
| 8 | MRK | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Drug Manufacturers - General |
| 9 | SKM | +14 | 15 | 1 | 2026-10-13→2026-10-14 | Telecom Services |
| 10 | PFE | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Drug Manufacturers - General |
| 11 | IDXX | +14 | 17 | 3 | 2026-10-13→2026-10-14 | Diagnostics & Research |
| 12 | PODD | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Devices |
| 13 | NWBI | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Banks - Regional |
| 14 | PBR | +14 | 16 | 2 | 2026-10-13→2026-10-14 | Oil & Gas Integrated |
| 15 | AMCR | +14 | 15 | 1 | 2026-10-13→2026-10-14 | Packaging & Containers |

## Full checklist — top 15

### RSG  ·  score **+16**  ·  Waste Management
price=224.44000244140625  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.32 on 2026-10-14; prev RSI=59.00 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.00@2026-10-13 → 66.32@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.327 (G=3.7000 R=1.5900); 2026-10-13:RED:O=221.5000,C=219.9100,body=-1.5900,vol=955255.0; 2026-10-14:GREEN:O=220.7400,C=224.4400,body=+3.7000,vol=1226703.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.284 (Gvol=1226703 Rvol=955255); 2026-10-13:RED:O=221.5000,C=219.9100,body=-1.5900,vol=955255.0; 2026-10-14:GREEN:O=220.7400,C=224.4400,body=+3.7000,vol=1226703.0 | **GOOD** |
| `A07_rvol` | RVOL=0.858 on 2026-10-14: today_vol=1226703 / avg20=1429824 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.166 on 2026-10-14 (price=224.4400, mid=214.7780, upper=223.0672, lower=206.4888; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=224.4400 vs SMA50=217.4566 dist=+3.21% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=214.78_50=217.46_80=216.68 on 2026-10-14: SMA20=214.7780 SMA50=217.4566 SMA80=216.6833 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-08-05@204.8000; S2[2026-08-17→2026-09-15] low=2026-08-17@213.0000; S3[2026-09-16→2026-10-14] low=2026-10-02@205.9700 | lows=[204.8000030517578, 213.0, 205.97000122070312] span=4.00% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=1.0164828485313413 wick_frac=-0.016482848531341306 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4291503644825172 wick_frac=0.5708496355174828 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3270474654997026 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.2300:wick=3.1200; 2026-10-09:GREEN:body=+0.9100:wick=2.2800; 2026-10-12:GREEN:body=+1.8200:wick=2.8700; 2026-10-13:RED:body=-1.5900:wick=2.1150; 2026-10-14:GREEN:body=+3.7000:wick=-0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 6.57 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=6.57 vs prior=6.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.56 | **GOOD** |
| `B13_short_float` | 2.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+15**  ·  Software - Infrastructure
price=57.75  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.41 on 2026-10-14; prev RSI=65.32 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.32@2026-10-13 → 67.41@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=3.553 (G=1.6700 R=0.4700); 2026-10-13:RED:O=57.5000,C=57.0300,body=-0.4700,vol=633969.0; 2026-10-14:GREEN:O=56.0800,C=57.7500,body=+1.6700,vol=819507.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.293 (Gvol=819507 Rvol=633969); 2026-10-13:RED:O=57.5000,C=57.0300,body=-0.4700,vol=633969.0; 2026-10-14:GREEN:O=56.0800,C=57.7500,body=+1.6700,vol=819507.0 | **GOOD** |
| `A07_rvol` | RVOL=1.114 on 2026-10-14: today_vol=819507 / avg20=735712 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.824 on 2026-10-14 (price=57.7500, mid=54.2905, upper=58.4886, lower=50.0924; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=57.7500 vs SMA50=52.4347 dist=+10.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=54.2905 SMA50=52.4347 SMA80=46.3573 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-14 (63 bars); S1[2026-07-15→2026-08-14] low=2026-07-23@35.4000; S2[2026-08-17→2026-09-15] low=2026-09-01@46.0100; S3[2026-09-16→2026-10-14] low=2026-09-22@49.3400 | lows=[35.400001525878906, 46.0099983215332, 49.34000015258789] span=39.38% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.5502465413200236 wick_frac=0.4497534586799764 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.21159761281181572 wick_frac=0.7884023871881842 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.553178365041231 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+1.0500:wick=1.3900; 2026-10-09:GREEN:body=+0.0700:wick=2.0300; 2026-10-12:GREEN:body=+0.3400:wick=1.1400; 2026-10-13:RED:body=-0.4700:wick=1.7512; 2026-10-14:GREEN:body=+1.6700:wick=1.3650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVLT  ·  score **+15**  ·  Software - Application
price=154.3300018310547  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.37 on 2026-10-14; prev RSI=64.01 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.01@2026-10-13 → 57.37@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.642 (G=4.5000 R=2.7400); 2026-10-13:GREEN:O=154.3500,C=158.8500,body=+4.5000,vol=597026.0; 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.094 (Gvol=597026 Rvol=545597); 2026-10-13:GREEN:O=154.3500,C=158.8500,body=+4.5000,vol=597026.0; 2026-10-14:RED:O=157.0700,C=154.3300,body=-2.7400,vol=545597.0 | **GOOD** |
| `A07_rvol` | RVOL=0.901 on 2026-10-14: today_vol=545597 / avg20=605488 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.476 on 2026-10-14 (price=154.3300, mid=150.4310, upper=158.6300, lower=142.2320; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=154.3300 vs SMA50=142.3926 dist=+8.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=150.4310 SMA50=142.3926 SMA80=141.2843 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-30@115.3100; S2[2026-08-17→2026-09-15] low=2026-09-02@125.9000; S3[2026-09-16→2026-10-14] low=2026-09-18@141.0000 | lows=[115.30999755859375, 125.9000015258789, 141.0] span=22.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7894719934895973 wick_frac=0.21052800651040274 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4528932414948057 wick_frac=0.5471067585051943 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6423324738679839 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.6200:wick=3.3100; 2026-10-09:GREEN:body=+4.4700:wick=2.8850; 2026-10-12:RED:body=-3.2300:wick=1.2300; 2026-10-13:GREEN:body=+4.5000:wick=1.2000; 2026-10-14:RED:body=-2.7400:wick=3.3100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=22.16 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1215.84 | **NEUTRAL** |
| `B04_income` | 68.3 | **GOOD** |
| `B05_profit_margin` | 5.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 160.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=160.75 vs prior_export=160.75 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -15.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.45 vs prior=-15.45 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 8.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.16 (this export) | prior_export=22.16 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+15**  ·  Consumer Electronics
price=17.600000381469727  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.34 on 2026-10-14; prev RSI=60.06 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.06@2026-10-13 → 58.34@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=5.400 (G=0.5400 R=0.1000); 2026-10-13:GREEN:O=17.1800,C=17.7200,body=+0.5400,vol=2387607.0; 2026-10-14:RED:O=17.7000,C=17.6000,body=-0.1000,vol=1507052.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.584 (Gvol=2387607 Rvol=1507052); 2026-10-13:GREEN:O=17.1800,C=17.7200,body=+0.5400,vol=2387607.0; 2026-10-14:RED:O=17.7000,C=17.6000,body=-0.1000,vol=1507052.0 | **GOOD** |
| `A07_rvol` | RVOL=0.698 on 2026-10-14: today_vol=1507052 / avg20=2157867 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.205 on 2026-10-14 (price=17.6000, mid=17.3153, upper=18.7075, lower=15.9230; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=17.6000 vs SMA50=16.2435 dist=+8.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=17.3153 SMA50=16.2435 SMA80=15.6261 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-14 (63 bars); S1[2026-07-15→2026-08-14] low=2026-07-15@13.7600; S2[2026-08-17→2026-09-15] low=2026-09-09@14.2000; S3[2026-09-16→2026-10-14] low=2026-09-18@15.2800 | lows=[13.760000228881836, 14.199999809265137, 15.279999732971191] span=11.05% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7346919180385725 wick_frac=0.26530808196142747 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.17857167185509634 wick_frac=0.8214283281449036 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.39996948253829 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:RED:body=-0.0300:wick=0.3600; 2026-10-09:GREEN:body=+0.2800:wick=0.1600; 2026-10-12:GREEN:body=+0.0900:wick=0.7750; 2026-10-13:GREEN:body=+0.5400:wick=0.1950; 2026-10-14:RED:body=-0.1000:wick=0.4600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.43 vs prior=-4.43 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MRK  ·  score **+14**  ·  Drug Manufacturers - General
price=147.3699951171875  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.63 on 2026-10-14; prev RSI=44.26 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.26@2026-10-13 → 54.63@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.103 (G=4.2700 R=3.8700); 2026-10-13:RED:O=145.8500,C=141.9800,body=-3.8700,vol=8378020.0; 2026-10-14:GREEN:O=143.1000,C=147.3700,body=+4.2700,vol=9784632.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.168 (Gvol=9784632 Rvol=8378020); 2026-10-13:RED:O=145.8500,C=141.9800,body=-3.8700,vol=8378020.0; 2026-10-14:GREEN:O=143.1000,C=147.3700,body=+4.2700,vol=9784632.0 | **GOOD** |
| `A07_rvol` | RVOL=1.010 on 2026-10-14: today_vol=9784632 / avg20=9684545 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.241 on 2026-10-14 (price=147.3700, mid=145.8855, upper=152.0487, lower=139.7223; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-14: price=147.3700 vs SMA50=144.4346 dist=+2.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=145.8855 SMA50=144.4346 SMA80=137.8877 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-21@124.1000; S2[2026-08-17→2026-09-15] low=2026-08-17@134.5500; S3[2026-09-16→2026-10-14] low=2026-10-05@138.8000 | lows=[124.0999984741211, 134.5500030517578, 138.8000030517578] span=11.85% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7806193895369921 wick_frac=0.2193806104630079 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.8889422419745471 wick_frac=0.11105775802545292 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.103353376047314 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.4800:wick=3.4000; 2026-10-09:GREEN:body=+4.2300:wick=1.0900; 2026-10-12:RED:body=-0.3000:wick=1.4199; 2026-10-13:RED:body=-3.8700:wick=0.4835; 2026-10-14:GREEN:body=+4.2700:wick=1.2000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=51.94 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.45 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 66287.0 | **NEUTRAL** |
| `B04_income` | 3173.0 | **GOOD** |
| `B05_profit_margin` | 4.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=153.76 vs prior_export=153.76 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -8.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.47 vs prior=-8.47 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.51 | **GOOD** |
| `B13_short_float` | 1.4 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=51.94 (this export) | prior_export=51.94 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.45 (this export) | prior_export=1.45 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SKM  ·  score **+14**  ·  Telecom Services
price=33.88999938964844  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=40.62 on 2026-10-14; prev RSI=42.97 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.97@2026-10-13 → 40.62@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=132.971 (G=1.3300 R=0.0100); 2026-10-13:GREEN:O=33.0900,C=34.4200,body=+1.3300,vol=1991125.0; 2026-10-14:RED:O=33.9000,C=33.8900,body=-0.0100,vol=1393508.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.429 (Gvol=1991125 Rvol=1393508); 2026-10-13:GREEN:O=33.0900,C=34.4200,body=+1.3300,vol=1991125.0; 2026-10-14:RED:O=33.9000,C=33.8900,body=-0.0100,vol=1393508.0 | **GOOD** |
| `A07_rvol` | RVOL=1.257 on 2026-10-14: today_vol=1393508 / avg20=1108773 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.803 on 2026-10-14 (price=33.8900, mid=35.4260, upper=37.3391, lower=33.5129; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-14: price=33.8900 vs SMA50=36.6332 dist=-7.49% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=35.43_50=36.63_80=35.06 on 2026-10-14: SMA20=35.4260 SMA50=36.6332 SMA80=35.0587 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-29@29.5300; S2[2026-08-17→2026-09-15] low=2026-09-15@35.5700; S3[2026-09-16→2026-10-14] low=2026-10-12@32.8500 | lows=[29.530000686645508, 35.56999969482422, 32.849998474121094] span=20.45% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.9109581166880046 wick_frac=0.0890418833119954 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.009825266147798684 wick_frac=0.9901747338522013 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=132.97139588100686 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-1.0100:wick=0.3000; 2026-10-09:RED:body=-0.3600:wick=0.4800; 2026-10-12:GREEN:body=+0.1900:wick=0.5400; 2026-10-13:GREEN:body=+1.3300:wick=0.1300; 2026-10-14:RED:body=-0.0100:wick=1.0080 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=44.43 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.38 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11760.23 | **NEUTRAL** |
| `B04_income` | 483.22 | **GOOD** |
| `B05_profit_margin` | 4.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.14 vs prior_export=45.14 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.85 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=44.43 (this export) | prior_export=44.43 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.38 (this export) | prior_export=3.38 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PFE  ·  score **+14**  ·  Drug Manufacturers - General
price=29.209999084472656  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.53 on 2026-10-14; prev RSI=56.97 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.97@2026-10-13 → 66.53@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.542 (G=0.6100 R=0.2400); 2026-10-13:RED:O=28.6100,C=28.3700,body=-0.2400,vol=26826046.0; 2026-10-14:GREEN:O=28.6000,C=29.2100,body=+0.6100,vol=39302193.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.465 (Gvol=39302193 Rvol=26826046); 2026-10-13:RED:O=28.6100,C=28.3700,body=-0.2400,vol=26826046.0; 2026-10-14:GREEN:O=28.6000,C=29.2100,body=+0.6100,vol=39302193.0 | **GOOD** |
| `A07_rvol` | RVOL=1.236 on 2026-10-14: today_vol=39302193 / avg20=31794297 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.107 on 2026-10-14 (price=29.2100, mid=28.1550, upper=29.1085, lower=27.2015; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=29.2100 vs SMA50=27.8288 dist=+4.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=28.1550 SMA50=27.8288 SMA80=26.4884 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-07-21@24.2262; S2[2026-08-17→2026-09-15] low=2026-08-17@26.5500; S3[2026-09-16→2026-10-14] low=2026-10-05@27.1200 | lows=[24.226188874908235, 26.549999237060547, 27.1200008392334] span=11.94% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.6489344217388923 wick_frac=0.35106557826110774 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4999980131845076 wick_frac=0.5000019868154923 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5416636864315856 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:RED:body=-0.0800:wick=0.4400; 2026-10-09:GREEN:body=+0.6700:wick=0.0250; 2026-10-12:RED:body=-0.0200:wick=0.1950; 2026-10-13:RED:body=-0.2400:wick=0.2400; 2026-10-14:GREEN:body=+0.6100:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.41 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 63696.0 | **NEUTRAL** |
| `B04_income` | 4332.0 | **GOOD** |
| `B05_profit_margin` | 6.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.57 vs prior_export=28.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | 2.18 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.18 vs prior=2.18 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.47 | **BAD** |
| `B13_short_float` | 2.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.87 (this export) | prior_export=12.87 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.41 (this export) | prior_export=4.41 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IDXX  ·  score **+14**  ·  Diagnostics & Research
price=529.010009765625  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.11 on 2026-10-14; prev RSI=48.25 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.25@2026-10-13 → 53.11@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.25@2026-10-13 → 53.11@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.25@2026-10-13 → 53.11@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=1.625 (G=8.8100 R=5.4200); 2026-10-13:RED:O=525.7300,C=520.3100,body=-5.4200,vol=445410.0; 2026-10-14:GREEN:O=520.2000,C=529.0100,body=+8.8100,vol=824319.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.851 (Gvol=824319 Rvol=445410); 2026-10-13:RED:O=525.7300,C=520.3100,body=-5.4200,vol=445410.0; 2026-10-14:GREEN:O=520.2000,C=529.0100,body=+8.8100,vol=824319.0 | **GOOD** |
| `A07_rvol` | RVOL=1.638 on 2026-10-14: today_vol=824319 / avg20=503182 (avg window 2026-09-16→2026-10-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.682 on 2026-10-14 (price=529.0100, mid=519.1990, upper=533.5928, lower=504.8052; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-14: price=529.0100 vs SMA50=536.5948 dist=-1.41% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-14: SMA20=519.1990 SMA50=536.5948 SMA80=543.9318 | **BAD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-14 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@534.3600; S2[2026-08-17→2026-09-15] low=2026-09-11@500.6100; S3[2026-09-16→2026-10-14] low=2026-10-07@501.1700 | lows=[534.3599853515625, 500.6099853515625, 501.1700134277344] span=6.74% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.867977967263587 wick_frac=0.13202203273641294 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.4151655266885779 wick_frac=0.584834473311422 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6254659294377316 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+6.4700:wick=1.9200; 2026-10-09:GREEN:body=+4.3300:wick=4.5099; 2026-10-12:GREEN:body=+5.9400:wick=4.6901; 2026-10-13:RED:body=-5.4200:wick=7.6350; 2026-10-14:GREEN:body=+8.8100:wick=1.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.28 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.36 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4553.22 | **NEUTRAL** |
| `B04_income` | 1139.66 | **GOOD** |
| `B05_profit_margin` | 25.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 722.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=722.36 vs prior_export=722.36 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.73 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.73 vs prior=-0.73 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.62 | **GOOD** |
| `B13_short_float` | 2.44 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.28 (this export) | prior_export=8.28 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.36 (this export) | prior_export=1.36 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+14**  ·  Medical Devices
price=139.80999755859375  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.20 on 2026-10-12; prev RSI=44.78 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.9300 R=0.0000); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2183896 Rvol=0); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A07_rvol` | RVOL=0.819 on 2026-10-12: today_vol=1319431 / avg20=1610443 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-10-12 (price=139.8100, mid=136.1735, upper=143.1581, lower=129.1889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=139.8100 vs SMA50=141.5730 dist=-1.25% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=136.1735 SMA50=141.5730 SMA80=147.5865 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-05@126.4000; S2[2026-08-13→2026-09-11] low=2026-09-11@131.2400; S3[2026-09-14→2026-10-12] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5660465898505131 wick_frac=0.43395341014948685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.0400:wick=1.3950; 2026-10-07:RED:body=-0.5800:wick=3.1000; 2026-10-08:GREEN:body=+0.5100:wick=1.2300; 2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NWBI  ·  score **+14**  ·  Banks - Regional
price=14.859999656677246  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=43.30 on 2026-10-12; prev RSI=37.21 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 37.21@2026-10-09 → 43.30@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=1.700 (G=0.1700 R=0.1000); 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0; 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.380 (Gvol=991454 Rvol=718370); 2026-10-09:RED:O=14.7700,C=14.6700,body=-0.1000,vol=718370.0; 2026-10-12:GREEN:O=14.6900,C=14.8600,body=+0.1700,vol=991454.0 | **GOOD** |
| `A07_rvol` | RVOL=0.846 on 2026-10-12: today_vol=991454 / avg20=1171580 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.466 on 2026-10-12 (price=14.8600, mid=15.1030, upper=15.6239, lower=14.5821; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=14.8600 vs SMA50=15.3452 dist=-3.16% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=15.10_50=15.35_80=15.19 on 2026-10-12: SMA20=15.1030 SMA50=15.3452 SMA80=15.1925 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-10@14.7133; S2[2026-08-13→2026-09-11] low=2026-09-01@15.0700; S3[2026-09-14→2026-10-12] low=2026-10-01@14.5400 | lows=[14.713282220409273, 15.069999694824219, 14.539999961853027] span=3.65% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.8947347287055163 wick_frac=0.10526527129448376 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.4878069204216638 wick_frac=0.5121930795783363 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6999942779759294 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.1500:wick=0.1150; 2026-10-07:RED:body=-0.0800:wick=0.1000; 2026-10-08:GREEN:body=+0.1500:wick=0.1500; 2026-10-09:RED:body=-0.1000:wick=0.1050; 2026-10-12:GREEN:body=+0.1700:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+14**  ·  Oil & Gas Integrated
price=25.940000534057617  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=81.22 on 2026-10-14; prev RSI=79.73 on 2026-10-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.73@2026-10-13 → 81.22@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5400 R=0.0000); 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0; 2026-10-14:GREEN:O=25.6400,C=25.9400,body=+0.3000,vol=19676375.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=99.000 (Gvol=37240359 Rvol=0); 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0; 2026-10-14:GREEN:O=25.6400,C=25.9400,body=+0.3000,vol=19676375.0 | **GOOD** |
| `A07_rvol` | RVOL=0.815 on 2026-10-14: today_vol=19676375 / avg20=24132281 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.839 on 2026-10-14 (price=25.9400, mid=22.4530, upper=26.6098, lower=18.2962; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-14: price=25.9400 vs SMA50=20.5430 dist=+26.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-14: SMA20=22.4530 SMA50=20.5430 SMA80=19.4493 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-17→2026-10-14 (63 bars); S1[2026-07-17→2026-08-14] low=2026-08-13@17.6000; S2[2026-08-17→2026-09-15] low=2026-08-27@17.5400; S3[2026-09-16→2026-10-14] low=2026-09-25@20.2000 | lows=[17.600000381469727, 17.540000915527344, 20.200000762939453] span=15.17% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.6239204341911745 wick_frac=0.3760795658088255 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.2900:wick=0.3000; 2026-10-09:GREEN:body=+0.6760:wick=0.3140; 2026-10-12:RED:body=-0.0200:wick=0.5000; 2026-10-13:GREEN:body=+0.2400:wick=0.2250; 2026-10-14:GREEN:body=+0.3000:wick=0.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMCR  ·  score **+14**  ·  Packaging & Containers
price=42.279998779296875  pair=`2026-10-13→2026-10-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.70 on 2026-10-14; prev RSI=41.15 on 2026-10-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.15@2026-10-13 → 46.70@2026-10-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 41.15@2026-10-13 → 46.70@2026-10-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 41.15@2026-10-13 → 46.70@2026-10-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_body_sum/RED_body_sum=2.115 (G=0.5500 R=0.2600); 2026-10-13:RED:O=42.0300,C=41.7700,body=-0.2600,vol=2236722.0; 2026-10-14:GREEN:O=41.7300,C=42.2800,body=+0.5500,vol=3037382.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-13 + 2026-10-14; ratio=GREEN_vol/RED_vol=1.358 (Gvol=3037382 Rvol=2236722); 2026-10-13:RED:O=42.0300,C=41.7700,body=-0.2600,vol=2236722.0; 2026-10-14:GREEN:O=41.7300,C=42.2800,body=+0.5500,vol=3037382.0 | **GOOD** |
| `A07_rvol` | RVOL=0.974 on 2026-10-14: today_vol=3037382 / avg20=3117231 (avg window 2026-09-16→2026-10-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.191 on 2026-10-14 (price=42.2800, mid=42.0960, upper=43.0576, lower=41.1344; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-14: price=42.2800 vs SMA50=44.3546 dist=-4.68% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=42.10_50=44.35_80=44.13 on 2026-10-14: SMA20=42.0960 SMA50=44.3546 SMA80=44.1308 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-14 (63 bars); S1[2026-07-16→2026-08-14] low=2026-07-23@41.9400; S2[2026-08-17→2026-09-15] low=2026-09-15@41.9800; S3[2026-09-16→2026-10-14] low=2026-10-08@40.7900 | lows=[41.939998626708984, 41.97999954223633, 40.790000915527344] span=2.92% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: GREEN body_frac=0.7801429568586286 wick_frac=0.21985704314137144 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-13+2026-10-14: RED body_frac=0.45217039400794784 wick_frac=0.5478296059920522 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1153953372360874 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.8800:wick=0.2100; 2026-10-09:RED:body=-0.1800:wick=0.4700; 2026-10-12:GREEN:body=+0.0600:wick=0.5200; 2026-10-13:RED:body=-0.2600:wick=0.3150; 2026-10-14:GREEN:body=+0.5500:wick=0.1550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.19 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23506.0 | **NEUTRAL** |
| `B04_income` | 1106.0 | **GOOD** |
| `B05_profit_margin` | 4.71 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.63 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.63 vs prior_export=49.63 on finviz_2026-10-14) | **NEUTRAL** |
| `B09_analyst_recom` | 2.09 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-14) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.84 | **GOOD** |
| `B13_short_float` | 4.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.19 (this export) | prior_export=3.19 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-10-14) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-15_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.