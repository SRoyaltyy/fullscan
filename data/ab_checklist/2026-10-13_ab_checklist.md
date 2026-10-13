# A+B1 Feature Checklist — 2026-10-13

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-13.csv` · prior export for Δ: `2026-10-12`
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
| 1 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 2 | CRSR | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Computer Hardware |
| 3 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 4 | MUFG | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Banks - Diversified |
| 5 | VEEV | +15 | 17 | 2 | 2026-10-09→2026-10-12 | Health Information Services |
| 6 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 7 | AVPT | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Software - Infrastructure |
| 8 | PFE | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Drug Manufacturers - General |
| 9 | PBR | +15 | 17 | 2 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 10 | AAPL | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Consumer Electronics |
| 11 | A | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Diagnostics & Research |
| 12 | MRK | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Drug Manufacturers - General |
| 13 | OMER | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Biotechnology |
| 14 | OXY | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Oil & Gas E&P |
| 15 | CXT | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Business Equipment & Supplies |

## Full checklist — top 15

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=13.550000190734863  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.54 on 2026-10-12; prev RSI=56.86 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.86@2026-10-09 → 55.54@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.86@2026-10-09 → 55.54@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.86@2026-10-09 → 55.54@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=3.231 (G=0.4200 R=0.1300); 2026-10-09:GREEN:O=13.2100,C=13.6300,body=+0.4200,vol=880955.0; 2026-10-12:RED:O=13.6800,C=13.5500,body=-0.1300,vol=842642.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.045 (Gvol=880955 Rvol=842642); 2026-10-09:GREEN:O=13.2100,C=13.6300,body=+0.4200,vol=880955.0; 2026-10-12:RED:O=13.6800,C=13.5500,body=-0.1300,vol=842642.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-10-12: today_vol=842642 / avg20=1050524 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.088 on 2026-10-12 (price=13.5500, mid=13.5185, upper=13.8746, lower=13.1624; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=13.5500 vs SMA50=12.7124 dist=+6.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=13.5185 SMA50=12.7124 SMA80=11.5281 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-17@9.0200; S2[2026-08-13→2026-09-11] low=2026-08-24@10.7260; S3[2026-09-14→2026-10-12] low=2026-09-14@12.6700 | lows=[9.020000457763672, 10.72599983215332, 12.670000076293945] span=40.47% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6774196029330105 wick_frac=0.3225803970669895 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.3252440851697382 wick_frac=0.6747559148302618 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.230766973553901 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.0100:wick=0.3700; 2026-10-07:GREEN:body=+0.2500:wick=0.2650; 2026-10-08:RED:body=-0.2600:wick=0.3900; 2026-10-09:GREEN:body=+0.4200:wick=0.2000; 2026-10-12:RED:body=-0.1300:wick=0.2697 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 19.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MUFG  ·  score **+15**  ·  Banks - Diversified
price=22.350000381469727  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=42.46 on 2026-10-12; prev RSI=42.80 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.80@2026-10-09 → 42.46@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.80@2026-10-09 → 42.46@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.80@2026-10-09 → 42.46@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5600 R=0.0000); 2026-10-09:GREEN:O=21.9500,C=22.3800,body=+0.4300,vol=2473579.0; 2026-10-12:GREEN:O=22.2200,C=22.3500,body=+0.1300,vol=2407507.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4881086 Rvol=0); 2026-10-09:GREEN:O=21.9500,C=22.3800,body=+0.4300,vol=2473579.0; 2026-10-12:GREEN:O=22.2200,C=22.3500,body=+0.1300,vol=2407507.0 | **GOOD** |
| `A07_rvol` | RVOL=0.832 on 2026-10-12: today_vol=2407507 / avg20=2895226 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.813 on 2026-10-12 (price=22.3500, mid=22.9970, upper=23.7933, lower=22.2007; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-10-12: price=22.3500 vs SMA50=22.8576 dist=-2.22% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=22.9970 SMA50=22.8576 SMA80=22.2897 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-12 (63 bars); S1[2026-07-13→2026-08-12] low=2026-07-17@21.1900; S2[2026-08-13→2026-09-11] low=2026-08-19@21.4900; S3[2026-09-14→2026-10-12] low=2026-10-09@21.9200 | lows=[21.190000534057617, 21.489999771118164, 21.920000076293945] span=3.45% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6605714242984082 wick_frac=0.33942857570159185 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.0400:wick=0.0900; 2026-10-07:RED:body=-0.0900:wick=0.2100; 2026-10-08:GREEN:body=+0.2300:wick=0.1000; 2026-10-09:GREEN:body=+0.4300:wick=0.0400; 2026-10-12:GREEN:body=+0.1300:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.6 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=146.44 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 86521.46 | **NEUTRAL** |
| `B04_income` | 17435.86 | **GOOD** |
| `B05_profit_margin` | 20.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.28 vs prior_export=26.28 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.25 | **GOOD** |
| `B13_short_float` | 0.05 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.6 (this export) | prior_export=20.6 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=146.44 (this export) | prior_export=146.44 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+15**  ·  Health Information Services
price=295.8500061035156  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.98 on 2026-10-12; prev RSI=69.30 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.30@2026-10-09 → 68.98@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.30@2026-10-09 → 68.98@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.30@2026-10-09 → 68.98@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=15.324 (G=10.4200 R=0.6800); 2026-10-09:GREEN:O=285.7100,C=296.1300,body=+10.4200,vol=1844546.0; 2026-10-12:RED:O=296.5300,C=295.8500,body=-0.6800,vol=1137660.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.621 (Gvol=1844546 Rvol=1137660); 2026-10-09:GREEN:O=285.7100,C=296.1300,body=+10.4200,vol=1844546.0; 2026-10-12:RED:O=296.5300,C=295.8500,body=-0.6800,vol=1137660.0 | **GOOD** |
| `A07_rvol` | RVOL=0.731 on 2026-10-12: today_vol=1137660 / avg20=1557062 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.883 on 2026-10-12 (price=295.8500, mid=276.4740, upper=298.4101, lower=254.5379; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=295.8500 vs SMA50=261.3566 dist=+13.20% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=276.4740 SMA50=261.3566 SMA80=232.5036 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-23@179.2500; S2[2026-08-13→2026-09-11] low=2026-08-17@238.3200; S3[2026-09-14→2026-10-12] low=2026-09-21@257.8700 | lows=[179.25, 238.32000732421875, 257.8699951171875] span=43.86% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.7747214848092937 wick_frac=0.22527851519070632 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.09763001196167008 wick_frac=0.9023699880383299 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.323714208778386 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.8400:wick=5.7672; 2026-10-07:RED:body=-1.7300:wick=5.4550; 2026-10-08:GREEN:body=+2.7100:wick=3.5200; 2026-10-09:GREEN:body=+10.4200:wick=3.0300; 2026-10-12:RED:body=-0.6800:wick=6.2850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.38 vs prior=-0.38 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 3.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVPT  ·  score **+15**  ·  Software - Infrastructure
price=14.569999694824219  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.08 on 2026-10-12; prev RSI=64.34 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.34@2026-10-09 → 63.08@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.34@2026-10-09 → 63.08@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.34@2026-10-09 → 63.08@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=3.167 (G=0.1900 R=0.0600); 2026-10-09:GREEN:O=14.4400,C=14.6300,body=+0.1900,vol=1430542.0; 2026-10-12:RED:O=14.6300,C=14.5700,body=-0.0600,vol=1194004.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.198 (Gvol=1430542 Rvol=1194004); 2026-10-09:GREEN:O=14.4400,C=14.6300,body=+0.1900,vol=1430542.0; 2026-10-12:RED:O=14.6300,C=14.5700,body=-0.0600,vol=1194004.0 | **GOOD** |
| `A07_rvol` | RVOL=0.524 on 2026-10-12: today_vol=1194004 / avg20=2280272 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.661 on 2026-10-12 (price=14.5700, mid=13.8042, upper=14.9625, lower=12.6460; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=14.5700 vs SMA50=13.5082 dist=+7.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=13.8042 SMA50=13.5082 SMA80=12.9355 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-23@11.7350; S2[2026-08-13→2026-09-11] low=2026-09-11@12.6100; S3[2026-09-14→2026-10-12] low=2026-09-28@12.6400 | lows=[11.734999656677246, 12.609999656677246, 12.640000343322754] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6031771020977835 wick_frac=0.3968228979022165 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.23077064152881194 wick_frac=0.769229358471188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.166653421282683 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.1800:wick=0.7450; 2026-10-07:RED:body=-0.2300:wick=0.2445; 2026-10-08:GREEN:body=+0.1800:wick=0.2700; 2026-10-09:GREEN:body=+0.1900:wick=0.1250; 2026-10-12:RED:body=-0.0600:wick=0.2000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=101.09 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466.15 | **NEUTRAL** |
| `B04_income` | 71.48 | **GOOD** |
| `B05_profit_margin` | 15.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.18 vs prior=-0.18 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.33 | **GOOD** |
| `B13_short_float` | 7.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=101.09 (this export) | prior_export=101.09 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PFE  ·  score **+15**  ·  Drug Manufacturers - General
price=28.399999618530273  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.52 on 2026-10-12; prev RSI=55.95 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.95@2026-10-09 → 57.52@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.95@2026-10-09 → 57.52@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.95@2026-10-09 → 57.52@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=33.499 (G=0.6700 R=0.0200); 2026-10-09:GREEN:O=27.6100,C=28.2800,body=+0.6700,vol=22581615.0; 2026-10-12:RED:O=28.4200,C=28.4000,body=-0.0200,vol=17930455.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.259 (Gvol=22581615 Rvol=17930455); 2026-10-09:GREEN:O=27.6100,C=28.2800,body=+0.6700,vol=22581615.0; 2026-10-12:RED:O=28.4200,C=28.4000,body=-0.0200,vol=17930455.0 | **GOOD** |
| `A07_rvol` | RVOL=0.543 on 2026-10-12: today_vol=17930455 / avg20=33006382 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.424 on 2026-10-12 (price=28.4000, mid=28.0265, upper=28.9064, lower=27.1466; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=28.4000 vs SMA50=27.6860 dist=+2.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=28.0265 SMA50=27.6860 SMA80=26.3864 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-15@23.8920; S2[2026-08-13→2026-09-11] low=2026-08-13@26.4100; S3[2026-09-14→2026-10-12] low=2026-10-05@27.1200 | lows=[23.892034939667635, 26.40999984741211, 27.1200008392334] span=13.51% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.9640293100609254 wick_frac=0.03597068993907459 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.09302531892620784 wick_frac=0.9069746810737922 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=33.499237078008775 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.0200:wick=0.4900; 2026-10-07:GREEN:body=+0.3800:wick=0.2388; 2026-10-08:RED:body=-0.0800:wick=0.4400; 2026-10-09:GREEN:body=+0.6700:wick=0.0250; 2026-10-12:RED:body=-0.0200:wick=0.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.41 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 63696.0 | **NEUTRAL** |
| `B04_income` | 4332.0 | **GOOD** |
| `B05_profit_margin` | 6.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.57 vs prior_export=28.57 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.54 | **NEUTRAL** |
| `B10_insider_transactions` | 2.18 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.18 vs prior=2.18 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.47 | **BAD** |
| `B13_short_float` | 2.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.87 (this export) | prior_export=12.87 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.41 (this export) | prior_export=4.41 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+15**  ·  Oil & Gas Integrated
price=25.420000076293945  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.45 on 2026-10-12; prev RSI=79.06 on 2026-10-09 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.06@2026-10-09 → 79.45@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.06@2026-10-09 → 79.45@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.06@2026-10-09 → 79.45@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=33.799 (G=0.6760 R=0.0200); 2026-10-09:GREEN:O=24.6240,C=25.3000,body=+0.6760,vol=25919401.0; 2026-10-12:RED:O=25.4400,C=25.4200,body=-0.0200,vol=13275058.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.952 (Gvol=25919401 Rvol=13275058); 2026-10-09:GREEN:O=24.6240,C=25.3000,body=+0.6760,vol=25919401.0; 2026-10-12:RED:O=25.4400,C=25.4200,body=-0.0200,vol=13275058.0 | **GOOD** |
| `A07_rvol` | RVOL=0.543 on 2026-10-12: today_vol=13275058 / avg20=24464229 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.961 on 2026-10-12 (price=25.4200, mid=22.0150, upper=25.5585, lower=18.4715; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=25.4200 vs SMA50=20.2698 dist=+25.41% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=22.0150 SMA50=20.2698 SMA80=19.2283 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-16@17.4400; S2[2026-08-13→2026-09-11] low=2026-08-27@17.5400; S3[2026-09-14→2026-10-12] low=2026-09-25@20.2000 | lows=[17.440000534057617, 17.540000915527344, 20.200000762939453] span=15.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6828271151826912 wick_frac=0.3171728848173087 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.03846252599686754 wick_frac=0.9615374740031325 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=33.799160785809654 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.2500:wick=0.1350; 2026-10-07:GREEN:body=+0.1700:wick=0.4650; 2026-10-08:GREEN:body=+0.2900:wick=0.3000; 2026-10-09:GREEN:body=+0.6760:wick=0.3140; 2026-10-12:RED:body=-0.0200:wick=0.5000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+15**  ·  Consumer Electronics
price=340.9700012207031  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.22 on 2026-10-12; prev RSI=56.14 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.14@2026-10-09 → 60.22@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.14@2026-10-09 → 60.22@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.14@2026-10-09 → 60.22@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=7.7550 R=0.0000); 2026-10-09:GREEN:O=331.6950,C=336.6400,body=+4.9450,vol=37750739.0; 2026-10-12:GREEN:O=338.1600,C=340.9700,body=+2.8100,vol=25796997.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=63547736 Rvol=0); 2026-10-09:GREEN:O=331.6950,C=336.6400,body=+4.9450,vol=37750739.0; 2026-10-12:GREEN:O=338.1600,C=340.9700,body=+2.8100,vol=25796997.0 | **GOOD** |
| `A07_rvol` | RVOL=0.688 on 2026-10-12: today_vol=25796997 / avg20=37511931 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.729 on 2026-10-12 (price=340.9700, mid=335.7835, upper=342.9000, lower=328.6670; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=340.9700 vs SMA50=323.0031 dist=+5.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=335.7835 SMA50=323.0031 SMA80=319.3104 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-31@299.7415; S2[2026-08-13→2026-09-11] low=2026-08-13@302.0500; S3[2026-09-14→2026-10-12] low=2026-10-01@325.8100 | lows=[299.7415030462749, 302.04998779296875, 325.80999755859375] span=8.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5919044188988081 wick_frac=0.40809558110119193 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+1.3500:wick=2.4100; 2026-10-07:RED:body=-0.2900:wick=5.5900; 2026-10-08:GREEN:body=+3.6000:wick=2.0700; 2026-10-09:GREEN:body=+4.9450:wick=2.9650; 2026-10-12:GREEN:body=+2.8100:wick=2.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=337.68 vs prior_export=337.68 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.28 vs prior=-2.28 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### A  ·  score **+15**  ·  Diagnostics & Research
price=171.3300018310547  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.10 on 2026-10-12; prev RSI=62.14 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.14@2026-10-09 → 63.10@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.14@2026-10-09 → 63.10@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.14@2026-10-09 → 63.10@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=24.624 (G=1.9700 R=0.0800); 2026-10-09:GREEN:O=168.5700,C=170.5400,body=+1.9700,vol=1537128.0; 2026-10-12:RED:O=171.4100,C=171.3300,body=-0.0800,vol=1513841.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.015 (Gvol=1537128 Rvol=1513841); 2026-10-09:GREEN:O=168.5700,C=170.5400,body=+1.9700,vol=1537128.0; 2026-10-12:RED:O=171.4100,C=171.3300,body=-0.0800,vol=1513841.0 | **GOOD** |
| `A07_rvol` | RVOL=0.596 on 2026-10-12: today_vol=1513841 / avg20=2540772 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.321 on 2026-10-12 (price=171.3300, mid=166.6840, upper=181.1742, lower=152.1938; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=171.3300 vs SMA50=156.2390 dist=+9.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=166.6840 SMA50=156.2390 SMA80=147.6258 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-12 (63 bars); S1[2026-07-13→2026-08-12] low=2026-07-20@130.1600; S2[2026-08-13→2026-09-11] low=2026-09-10@141.2500; S3[2026-09-14→2026-10-12] low=2026-09-14@144.6500 | lows=[130.16000366210938, 141.25, 144.64999389648438] span=11.13% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.544195751138088 wick_frac=0.455804248861912 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.043479342543910574 wick_frac=0.9565206574560894 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=24.624260919321 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-3.4400:wick=4.9300; 2026-10-07:RED:body=-0.7900:wick=3.9600; 2026-10-08:GREEN:body=+0.6900:wick=4.6500; 2026-10-09:GREEN:body=+1.9700:wick=1.6500; 2026-10-12:RED:body=-0.0800:wick=1.7600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.85 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.98 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7372.0 | **NEUTRAL** |
| `B04_income` | 1440.0 | **GOOD** |
| `B05_profit_margin` | 19.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 176.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=176.11 vs prior_export=176.11 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.23 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.23 vs prior=-0.23 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.11 | **GOOD** |
| `B13_short_float` | 1.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.85 (this export) | prior_export=8.85 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MRK  ·  score **+15**  ·  Drug Manufacturers - General
price=145.4499969482422  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.26 on 2026-10-12; prev RSI=51.59 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.59@2026-10-09 → 51.26@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.59@2026-10-09 → 51.26@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.59@2026-10-09 → 51.26@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=14.100 (G=4.2300 R=0.3000); 2026-10-09:GREEN:O=141.3700,C=145.6000,body=+4.2300,vol=7990067.0; 2026-10-12:RED:O=145.7500,C=145.4500,body=-0.3000,vol=6624466.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.206 (Gvol=7990067 Rvol=6624466); 2026-10-09:GREEN:O=141.3700,C=145.6000,body=+4.2300,vol=7990067.0; 2026-10-12:RED:O=145.7500,C=145.4500,body=-0.3000,vol=6624466.0 | **GOOD** |
| `A07_rvol` | RVOL=0.682 on 2026-10-12: today_vol=6624466 / avg20=9719626 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.068 on 2026-10-12 (price=145.4500, mid=145.8530, upper=151.8054, lower=139.9006; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=145.4500 vs SMA50=143.7630 dist=+1.17% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=145.8530 SMA50=143.7630 SMA80=137.1377 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-15@122.7500; S2[2026-08-13→2026-09-11] low=2026-08-13@133.0600; S3[2026-09-14→2026-10-12] low=2026-10-05@138.8000 | lows=[122.75, 133.05999755859375, 138.8000030517578] span=13.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.7951160329383825 wick_frac=0.2048839670616175 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.17443108725546733 wick_frac=0.8255689127445327 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.099893189563094 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+1.8100:wick=0.5950; 2026-10-07:GREEN:body=+0.7650:wick=1.8650; 2026-10-08:GREEN:body=+0.4800:wick=3.4000; 2026-10-09:GREEN:body=+4.2300:wick=1.0900; 2026-10-12:RED:body=-0.3000:wick=1.4199 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=51.94 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.45 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 66287.0 | **NEUTRAL** |
| `B04_income` | 3173.0 | **GOOD** |
| `B05_profit_margin` | 4.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 153.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=153.76 vs prior_export=153.76 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -8.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.47 vs prior=-8.47 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.51 | **GOOD** |
| `B13_short_float` | 1.4 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=51.94 (this export) | prior_export=51.94 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.45 (this export) | prior_export=1.45 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OMER  ·  score **+14**  ·  Biotechnology
price=19.6299991607666  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.81 on 2026-10-12; prev RSI=59.69 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.69@2026-10-09 → 57.81@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.69@2026-10-09 → 57.81@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.69@2026-10-09 → 57.81@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=4.846 (G=0.6300 R=0.1300); 2026-10-09:GREEN:O=19.2000,C=19.8300,body=+0.6300,vol=793623.0; 2026-10-12:RED:O=19.7600,C=19.6300,body=-0.1300,vol=1065188.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=0.745 (Gvol=793623 Rvol=1065188); 2026-10-09:GREEN:O=19.2000,C=19.8300,body=+0.6300,vol=793623.0; 2026-10-12:RED:O=19.7600,C=19.6300,body=-0.1300,vol=1065188.0 | **BAD** |
| `A07_rvol` | RVOL=0.756 on 2026-10-12: today_vol=1065188 / avg20=1409439 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.288 on 2026-10-12 (price=19.6300, mid=19.0430, upper=21.0782, lower=17.0078; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=19.6300 vs SMA50=17.8740 dist=+9.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=19.0430 SMA50=17.8740 SMA80=15.0131 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-12 (63 bars); S1[2026-07-14→2026-08-12] low=2026-07-17@9.1200; S2[2026-08-13→2026-09-11] low=2026-08-13@16.2700; S3[2026-09-14→2026-10-12] low=2026-09-15@16.5000 | lows=[9.119999885559082, 16.270000457763672, 16.5] span=80.92% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.8811175193269062 wick_frac=0.11888248067309386 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.16883245562320723 wick_frac=0.8311675443767927 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.846107573579037 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.4400:wick=0.2000; 2026-10-07:GREEN:body=+0.6400:wick=0.1764; 2026-10-08:GREEN:body=+0.4900:wick=0.2400; 2026-10-09:GREEN:body=+0.6300:wick=0.0850; 2026-10-12:RED:body=-0.1300:wick=0.6400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=155.91 (current export asof; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=124.79 (current export; earnings_date=8/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 38.42 | **NEUTRAL** |
| `B04_income` | 124.83 | **GOOD** |
| `B05_profit_margin` | 324.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.67 vs prior_export=36.67 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -3.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.82 vs prior=-3.82 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.41 | **GOOD** |
| `B13_short_float` | 18.03 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=155.91 (this export) | prior_export=155.91 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=124.79 (this export) | prior_export=124.79 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OXY  ·  score **+14**  ·  Oil & Gas E&P
price=61.130001068115234  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.24 on 2026-10-12; prev RSI=57.58 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.58@2026-10-09 → 61.24@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.58@2026-10-09 → 61.24@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.58@2026-10-09 → 61.24@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4300 R=0.0000); 2026-10-09:GREEN:O=59.9100,C=60.1100,body=+0.2000,vol=6676794.0; 2026-10-12:GREEN:O=60.9000,C=61.1300,body=+0.2300,vol=6392430.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=13069224 Rvol=0); 2026-10-09:GREEN:O=59.9100,C=60.1100,body=+0.2000,vol=6676794.0; 2026-10-12:GREEN:O=60.9000,C=61.1300,body=+0.2300,vol=6392430.0 | **GOOD** |
| `A07_rvol` | RVOL=0.669 on 2026-10-12: today_vol=6392430 / avg20=9558114 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.704 on 2026-10-12 (price=61.1300, mid=58.2745, upper=62.3319, lower=54.2171; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=61.1300 vs SMA50=58.8066 dist=+3.95% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=58.27_50=58.81_80=56.70 on 2026-10-12: SMA20=58.2745 SMA50=58.8066 SMA80=56.7014 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-12 (63 bars); S1[2026-07-15→2026-08-12] low=2026-07-15@52.9500; S2[2026-08-13→2026-09-11] low=2026-08-13@57.1800; S3[2026-09-14→2026-10-12] low=2026-09-29@54.7600 | lows=[52.95000076293945, 57.18000030517578, 54.7599983215332] span=7.99% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.2284407941915827 wick_frac=0.7715592058084173 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.8000:wick=0.6000; 2026-10-07:RED:body=-0.4900:wick=0.9849; 2026-10-08:GREEN:body=+0.6500:wick=0.4000; 2026-10-09:GREEN:body=+0.2000:wick=0.9450; 2026-10-12:GREEN:body=+0.2300:wick=0.5850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.47 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.0 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 24313.0 | **NEUTRAL** |
| `B04_income` | 3412.0 | **GOOD** |
| `B05_profit_margin` | 14.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.45 vs prior_export=68.45 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.13 vs prior=0.13 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.47 (this export) | prior_export=31.47 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.0 (this export) | prior_export=14.0 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CXT  ·  score **+14**  ·  Business Equipment & Supplies
price=51.11000061035156  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.07 on 2026-10-12; prev RSI=57.77 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.77@2026-10-09 → 60.07@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.5300 R=0.0000); 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0; 2026-10-12:GREEN:O=50.4100,C=51.1100,body=+0.7000,vol=467391.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=886404 Rvol=0); 2026-10-09:GREEN:O=48.6200,C=50.4500,body=+1.8300,vol=419013.0; 2026-10-12:GREEN:O=50.4100,C=51.1100,body=+0.7000,vol=467391.0 | **GOOD** |
| `A07_rvol` | RVOL=0.835 on 2026-10-12: today_vol=467391 / avg20=559837 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.062 on 2026-10-12 (price=51.1100, mid=47.9275, upper=50.9230, lower=44.9320; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=51.1100 vs SMA50=49.6434 dist=+2.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=47.9275 SMA50=49.6434 SMA80=49.6754 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-10@49.7400; S2[2026-08-13→2026-09-11] low=2026-08-20@47.5800; S3[2026-09-14→2026-10-12] low=2026-09-18@44.8400 | lows=[49.7400016784668, 47.58000183105469, 44.84000015258789] span=10.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6434306991572664 wick_frac=0.35656930084273364 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+0.3400:wick=0.6200; 2026-10-07:RED:body=-0.2500:wick=1.7600; 2026-10-08:GREEN:body=+0.4100:wick=0.7600; 2026-10-09:GREEN:body=+1.8300:wick=0.2700; 2026-10-12:GREEN:body=+0.7000:wick=0.9850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.25 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.7 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1802.9 | **NEUTRAL** |
| `B04_income` | 140.3 | **GOOD** |
| `B05_profit_margin` | 7.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.71 vs prior_export=68.71 on finviz_2026-10-12) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-10-12) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.45 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-10-12) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-13_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.