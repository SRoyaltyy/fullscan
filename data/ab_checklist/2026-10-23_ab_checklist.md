# A+B1 Feature Checklist — 2026-10-23

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-23.csv` · prior export for Δ: `2026-10-22`
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
| 1 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 2 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 3 | TRI | +15 | 17 | 2 | 2026-10-21→2026-10-22 | Specialty Business Services |
| 4 | GCT | +15 | 16 | 1 | 2026-10-21→2026-10-22 | Software - Infrastructure |
| 5 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 6 | PM | +15 | 15 | 0 | 2026-10-21→2026-10-22 | Tobacco |
| 7 | ICE | +15 | 17 | 2 | 2026-10-21→2026-10-22 | Financial Data & Stock Exchanges |
| 8 | GMAB | +14 | 15 | 1 | 2026-10-21→2026-10-22 | Biotechnology |
| 9 | A | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Diagnostics & Research |
| 10 | AAPL | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Consumer Electronics |
| 11 | ZBRA | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Communication Equipment |
| 12 | GEHC | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Medical Devices |
| 13 | TOST | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Software - Infrastructure |
| 14 | NWBI | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Banks - Regional |
| 15 | WMG | +14 | 16 | 2 | 2026-10-21→2026-10-22 | Entertainment |

## Full checklist — top 15

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TRI  ·  score **+15**  ·  Specialty Business Services
price=123.26000213623047  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.13 on 2026-10-22; prev RSI=72.53 on 2026-10-21 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.53@2026-10-21 → 79.13@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.8100 R=0.0000); 2026-10-21:GREEN:O=112.3700,C=115.2600,body=+2.8900,vol=1596597.0; 2026-10-22:GREEN:O=117.3400,C=123.2600,body=+5.9200,vol=2662214.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4258811 Rvol=0); 2026-10-21:GREEN:O=112.3700,C=115.2600,body=+2.8900,vol=1596597.0; 2026-10-22:GREEN:O=117.3400,C=123.2600,body=+5.9200,vol=2662214.0 | **GOOD** |
| `A07_rvol` | RVOL=1.883 on 2026-10-22: today_vol=2662214 / avg20=1413955 (avg window 2026-09-24→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.347 on 2026-10-22 (price=123.2600, mid=103.8670, upper=118.2663, lower=89.4677; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=123.2600 vs SMA50=102.9558 dist=+19.72% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=103.8670 SMA50=102.9558 SMA80=100.0015 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@86.0700; S2[2026-08-25→2026-09-23] low=2026-09-21@93.6500; S3[2026-09-24→2026-10-22] low=2026-09-29@92.5700 | lows=[86.06999969482422, 93.6500015258789, 92.56999969482422] span=8.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.7585544374069635 wick_frac=0.2414455625930365 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-0.4300:wick=3.3200; 2026-10-19:RED:body=-0.5700:wick=1.5200; 2026-10-20:GREEN:body=+3.2600:wick=0.5000; 2026-10-21:GREEN:body=+2.8900:wick=1.7300; 2026-10-22:GREEN:body=+5.9200:wick=0.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.32 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7832.0 | **NEUTRAL** |
| `B04_income` | 1658.0 | **GOOD** |
| `B05_profit_margin` | 21.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.64 vs prior_export=122.64 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 9.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.32 (this export) | prior_export=3.32 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.86 (this export) | prior_export=1.86 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+15**  ·  Software - Infrastructure
price=59.119998931884766  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.03 on 2026-10-22; prev RSI=69.22 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.22@2026-10-21 → 63.03@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=1.782 (G=1.3900 R=0.7800); 2026-10-21:GREEN:O=59.0000,C=60.3900,body=+1.3900,vol=619879.0; 2026-10-22:RED:O=59.9000,C=59.1200,body=-0.7800,vol=525628.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.179 (Gvol=619879 Rvol=525628); 2026-10-21:GREEN:O=59.0000,C=60.3900,body=+1.3900,vol=619879.0; 2026-10-22:RED:O=59.9000,C=59.1200,body=-0.7800,vol=525628.0 | **GOOD** |
| `A07_rvol` | RVOL=0.772 on 2026-10-22: today_vol=525628 / avg20=681066 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.562 on 2026-10-22 (price=59.1200, mid=56.0150, upper=61.5406, lower=50.4894; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=59.1200 vs SMA50=53.3621 dist=+10.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=56.0150 SMA50=53.3621 SMA80=48.2992 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@35.6100; S2[2026-08-25→2026-09-23] low=2026-09-01@46.0100; S3[2026-09-24→2026-10-22] low=2026-09-29@49.4300 | lows=[35.61000061035156, 46.0099983215332, 49.43000030517578] span=38.81% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.5537841460708651 wick_frac=0.4462158539291348 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.3000126183335583 wick_frac=0.6999873816664417 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7820445731221237 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-1.9000:wick=1.3099; 2026-10-19:GREEN:body=+0.3600:wick=0.6800; 2026-10-20:GREEN:body=+1.0500:wick=0.9899; 2026-10-21:GREEN:body=+1.3900:wick=1.1200; 2026-10-22:RED:body=-0.7800:wick=1.8199 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+15**  ·  Tobacco
price=201.74000549316406  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.27 on 2026-10-22; prev RSI=52.22 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.22@2026-10-21 → 57.27@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=15.0200 R=0.0000); 2026-10-21:GREEN:O=187.1400,C=197.4600,body=+10.3200,vol=5768726.0; 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10867148 Rvol=0); 2026-10-21:GREEN:O=187.1400,C=197.4600,body=+10.3200,vol=5768726.0; 2026-10-22:GREEN:O=197.0400,C=201.7400,body=+4.7000,vol=5098422.0 | **GOOD** |
| `A07_rvol` | RVOL=1.158 on 2026-10-22: today_vol=5098422 / avg20=4402108 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.447 on 2026-10-22 (price=201.7400, mid=196.5515, upper=208.1622, lower=184.9408; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=201.7400 vs SMA50=192.3238 dist=+4.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=196.5515 SMA50=192.3238 SMA80=190.6744 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-08-12@184.0200; S2[2026-08-25→2026-09-23] low=2026-09-08@181.0100; S3[2026-09-24→2026-10-22] low=2026-10-21@184.0000 | lows=[184.02000427246094, 181.00999450683594, 184.0] span=1.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8547597126315387 wick_frac=0.14524028736846134 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-16:GREEN:body=+2.2200:wick=1.8800; 2026-10-19:RED:body=-1.0900:wick=1.2100; 2026-10-20:RED:body=-1.9600:wick=1.7200; 2026-10-21:GREEN:body=+10.3200:wick=3.5100; 2026-10-22:GREEN:body=+4.7000:wick=0.1790 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.38 | **GOOD** |
| `B13_short_float` | 0.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICE  ·  score **+15**  ·  Financial Data & Stock Exchanges
price=163.83999633789062  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.36 on 2026-10-22; prev RSI=53.93 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.93@2026-10-21 → 64.36@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=9.372 (G=4.0300 R=0.4300); 2026-10-21:RED:O=158.1200,C=157.6900,body=-0.4300,vol=2767634.0; 2026-10-22:GREEN:O=159.8100,C=163.8400,body=+4.0300,vol=4734792.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.711 (Gvol=4734792 Rvol=2767634); 2026-10-21:RED:O=158.1200,C=157.6900,body=-0.4300,vol=2767634.0; 2026-10-22:GREEN:O=159.8100,C=163.8400,body=+4.0300,vol=4734792.0 | **GOOD** |
| `A07_rvol` | RVOL=1.741 on 2026-10-22: today_vol=4734792 / avg20=2720032 (avg window 2026-09-24→2026-10-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.166 on 2026-10-22 (price=163.8400, mid=155.5125, upper=162.6543, lower=148.3707; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=163.8400 vs SMA50=156.9728 dist=+4.37% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=155.51_50=156.97_80=151.61 on 2026-10-22: SMA20=155.5125 SMA50=156.9728 SMA80=151.6109 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-27@144.8900; S2[2026-08-25→2026-09-23] low=2026-09-23@151.4200; S3[2026-09-24→2026-10-22] low=2026-10-05@147.5100 | lows=[144.88999938964844, 151.4199981689453, 147.50999450683594] span=4.51% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8732381327099775 wick_frac=0.12676186729002245 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.16667060966665878 wick_frac=0.8333293903333412 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.372249822569199 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:GREEN:body=+2.2200:wick=1.0800; 2026-10-19:RED:body=-1.9400:wick=0.4550; 2026-10-20:RED:body=-1.0700:wick=3.6700; 2026-10-21:RED:body=-0.4300:wick=2.1499; 2026-10-22:GREEN:body=+4.0300:wick=0.5850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.49 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.33 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13138.0 | **NEUTRAL** |
| `B04_income` | 4038.0 | **GOOD** |
| `B05_profit_margin` | 30.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 186.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=186.86 vs prior_export=186.86 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | -2.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.02 vs prior=-2.02 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 1.15 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.49 (this export) | prior_export=3.49 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.33 (this export) | prior_export=1.33 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+14**  ·  Biotechnology
price=40.0  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.79 on 2026-10-22; prev RSI=67.34 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.34@2026-10-21 → 68.79@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.34@2026-10-21 → 68.79@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.34@2026-10-21 → 68.79@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8700 R=0.0000); 2026-10-21:GREEN:O=39.0800,C=39.5700,body=+0.4900,vol=2972768.0; 2026-10-22:GREEN:O=39.6200,C=40.0000,body=+0.3800,vol=4604442.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7577210 Rvol=0); 2026-10-21:GREEN:O=39.0800,C=39.5700,body=+0.4900,vol=2972768.0; 2026-10-22:GREEN:O=39.6200,C=40.0000,body=+0.3800,vol=4604442.0 | **GOOD** |
| `A07_rvol` | RVOL=1.053 on 2026-10-22: today_vol=4604442 / avg20=4370667 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.949 on 2026-10-22 (price=40.0000, mid=37.0032, upper=40.1617, lower=33.8448; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=40.0000 vs SMA50=34.9697 dist=+14.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=37.0032 SMA50=34.9697 SMA80=32.7020 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-30@28.0300; S2[2026-08-25→2026-09-23] low=2026-09-10@32.2100; S3[2026-09-24→2026-10-22] low=2026-10-02@33.7250 | lows=[28.030000686645508, 32.209999084472656, 33.724998474121094] span=20.32% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.6882132952273496 wick_frac=0.3117867047726503 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-16:RED:body=-1.1450:wick=0.3150; 2026-10-19:GREEN:body=+0.6600:wick=0.5800; 2026-10-20:GREEN:body=+0.4700:wick=0.5150; 2026-10-21:GREEN:body=+0.4900:wick=0.1200; 2026-10-22:GREEN:body=+0.3800:wick=0.2830 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.39 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=39.39 vs prior_export=39.39 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### A  ·  score **+14**  ·  Diagnostics & Research
price=173.88999938964844  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.68 on 2026-10-22; prev RSI=65.88 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.88@2026-10-21 → 58.68@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.88@2026-10-21 → 58.68@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.88@2026-10-21 → 58.68@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=1.945 (G=7.0200 R=3.6100); 2026-10-21:GREEN:O=170.7300,C=177.7500,body=+7.0200,vol=2134320.0; 2026-10-22:RED:O=177.5000,C=173.8900,body=-3.6100,vol=2654464.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=0.804 (Gvol=2134320 Rvol=2654464); 2026-10-21:GREEN:O=170.7300,C=177.7500,body=+7.0200,vol=2134320.0; 2026-10-22:RED:O=177.5000,C=173.8900,body=-3.6100,vol=2654464.0 | **BAD** |
| `A07_rvol` | RVOL=1.199 on 2026-10-22: today_vol=2654464 / avg20=2213028 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.342 on 2026-10-22 (price=173.8900, mid=171.9110, upper=177.7017, lower=166.1203; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=173.8900 vs SMA50=160.9350 dist=+8.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=171.9110 SMA50=160.9350 SMA80=152.0586 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-31@135.7800; S2[2026-08-25→2026-09-23] low=2026-09-10@141.2500; S3[2026-09-24→2026-10-22] low=2026-10-08@163.3200 | lows=[135.77999877929688, 141.25, 163.32000732421875] span=20.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.9385018665469901 wick_frac=0.06149813345300993 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.46520659353192056 wick_frac=0.5347934064680794 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.944599192679164 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-2.0700:wick=3.2400; 2026-10-19:RED:body=-3.9200:wick=1.4900; 2026-10-20:GREEN:body=+2.0700:wick=2.3550; 2026-10-21:GREEN:body=+7.0200:wick=0.4600; 2026-10-22:RED:body=-3.6100:wick=4.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.85 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.98 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7372.0 | **NEUTRAL** |
| `B04_income` | 1440.0 | **GOOD** |
| `B05_profit_margin` | 19.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 176.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=176.11 vs prior_export=176.11 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.23 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.23 vs prior=-0.23 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.11 | **GOOD** |
| `B13_short_float` | 1.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.85 (this export) | prior_export=8.85 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AAPL  ·  score **+14**  ·  Consumer Electronics
price=348.2200012207031  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.76 on 2026-10-22; prev RSI=60.65 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.65@2026-10-21 → 64.76@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.65@2026-10-21 → 64.76@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.65@2026-10-21 → 64.76@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.0700 R=0.0000); 2026-10-21:GREEN:O=339.2050,C=343.9900,body=+4.7850,vol=35131228.0; 2026-10-22:GREEN:O=344.9350,C=348.2200,body=+3.2850,vol=46659852.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=81791080 Rvol=0); 2026-10-21:GREEN:O=339.2050,C=343.9900,body=+4.7850,vol=35131228.0; 2026-10-22:GREEN:O=344.9350,C=348.2200,body=+3.2850,vol=46659852.0 | **GOOD** |
| `A07_rvol` | RVOL=1.276 on 2026-10-22: today_vol=46659852 / avg20=36557380 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.949 on 2026-10-22 (price=348.2200, mid=338.5730, upper=348.7393, lower=328.4067; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=348.2200 vs SMA50=328.6174 dist=+5.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=338.5730 SMA50=328.6174 SMA80=324.7342 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-31@299.7415; S2[2026-08-25→2026-09-23] low=2026-08-25@308.2100; S3[2026-09-24→2026-10-22] low=2026-10-01@325.8100 | lows=[299.7415030462749, 308.2099914550781, 325.80999755859375] span=8.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.6445614978888783 wick_frac=0.35543850211112177 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-2.6250:wick=4.7350; 2026-10-19:GREEN:body=+1.9650:wick=3.9100; 2026-10-20:RED:body=-0.1080:wick=4.2520; 2026-10-21:GREEN:body=+4.7850:wick=1.6050; 2026-10-22:GREEN:body=+3.2850:wick=2.7950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 337.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=337.68 vs prior_export=337.68 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -2.28 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.28 vs prior=-2.28 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZBRA  ·  score **+14**  ·  Communication Equipment
price=384.1600036621094  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.64 on 2026-10-22; prev RSI=63.63 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.63@2026-10-21 → 57.64@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.63@2026-10-21 → 57.64@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.63@2026-10-21 → 57.64@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=2.095 (G=4.8600 R=2.3200); 2026-10-21:GREEN:O=386.1400,C=391.0000,body=+4.8600,vol=299420.0; 2026-10-22:RED:O=386.4800,C=384.1600,body=-2.3200,vol=585527.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=0.511 (Gvol=299420 Rvol=585527); 2026-10-21:GREEN:O=386.1400,C=391.0000,body=+4.8600,vol=299420.0; 2026-10-22:RED:O=386.4800,C=384.1600,body=-2.3200,vol=585527.0 | **BAD** |
| `A07_rvol` | RVOL=1.297 on 2026-10-22: today_vol=585527 / avg20=451325 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.344 on 2026-10-22 (price=384.1600, mid=379.6205, upper=392.7986, lower=366.4424; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-22: price=384.1600 vs SMA50=366.1946 dist=+4.91% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=379.6205 SMA50=366.1946 SMA80=338.5752 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-07-24@256.6700; S2[2026-08-25→2026-09-23] low=2026-09-10@336.4000; S3[2026-09-24→2026-10-22] low=2026-09-24@362.7600 | lows=[256.6700134277344, 336.3999938964844, 362.760009765625] span=41.33% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.5586182221255639 wick_frac=0.4413817778744361 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.16518370282491032 wick_frac=0.8348162971750896 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.0948146589145247 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-9.1000:wick=1.5250; 2026-10-19:GREEN:body=+2.4200:wick=6.6600; 2026-10-20:GREEN:body=+10.0600:wick=1.3800; 2026-10-21:GREEN:body=+4.8600:wick=3.8400; 2026-10-22:RED:body=-2.3200:wick=11.7250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=45.54 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.99 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5887.0 | **NEUTRAL** |
| `B04_income` | 539.0 | **GOOD** |
| `B05_profit_margin` | 9.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 419.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=419.23 vs prior_export=419.23 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.59 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.59 vs prior=-1.59 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.31 | **GOOD** |
| `B13_short_float` | 3.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=45.54 (this export) | prior_export=45.54 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.99 (this export) | prior_export=3.99 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEHC  ·  score **+14**  ·  Medical Devices
price=71.41999816894531  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.48 on 2026-10-22; prev RSI=56.69 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.69@2026-10-21 → 64.48@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.69@2026-10-21 → 64.48@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.69@2026-10-21 → 64.48@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=209.115 (G=2.0900 R=0.0100); 2026-10-21:RED:O=69.0200,C=69.0100,body=-0.0100,vol=2170877.0; 2026-10-22:GREEN:O=69.3300,C=71.4200,body=+2.0900,vol=3129640.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.442 (Gvol=3129640 Rvol=2170877); 2026-10-21:RED:O=69.0200,C=69.0100,body=-0.0100,vol=2170877.0; 2026-10-22:GREEN:O=69.3300,C=71.4200,body=+2.0900,vol=3129640.0 | **GOOD** |
| `A07_rvol` | RVOL=0.907 on 2026-10-22: today_vol=3129640 / avg20=3449177 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.896 on 2026-10-22 (price=71.4200, mid=67.1995, upper=71.9117, lower=62.4873; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=71.4200 vs SMA50=68.4085 dist=+4.40% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=67.20_50=68.41_80=67.59 on 2026-10-22: SMA20=67.1995 SMA50=68.4085 SMA80=67.5905 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-27@60.9300; S2[2026-08-25→2026-09-23] low=2026-09-17@62.9950; S3[2026-09-24→2026-10-22] low=2026-10-08@63.3150 | lows=[60.93000030517578, 62.994998931884766, 63.314998626708984] span=3.91% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.8744756785055321 wick_frac=0.12552432149446788 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.006448056230988078 wick_frac=0.9935519437690119 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=209.1145038167939 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-1.5300:wick=0.3400; 2026-10-19:GREEN:body=+0.4700:wick=0.4900; 2026-10-20:RED:body=-0.0200:wick=1.3700; 2026-10-21:RED:body=-0.0100:wick=1.5400; 2026-10-22:GREEN:body=+2.0900:wick=0.3000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.08 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.59 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21267.0 | **NEUTRAL** |
| `B04_income` | 1985.0 | **GOOD** |
| `B05_profit_margin` | 9.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 82.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=82.78 vs prior_export=82.78 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | 7.47 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.47 vs prior=7.47 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.22 | **BAD** |
| `B13_short_float` | 3.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.08 (this export) | prior_export=9.08 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TOST  ·  score **+14**  ·  Software - Infrastructure
price=33.41999816894531  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.24 on 2026-10-22; prev RSI=57.88 on 2026-10-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.88@2026-10-21 → 62.24@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.88@2026-10-21 → 62.24@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.88@2026-10-21 → 62.24@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=4.389 (G=0.7900 R=0.1800); 2026-10-21:RED:O=32.7200,C=32.5400,body=-0.1800,vol=6836899.0; 2026-10-22:GREEN:O=32.6300,C=33.4200,body=+0.7900,vol=9246432.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=1.352 (Gvol=9246432 Rvol=6836899); 2026-10-21:RED:O=32.7200,C=32.5400,body=-0.1800,vol=6836899.0; 2026-10-22:GREEN:O=32.6300,C=33.4200,body=+0.7900,vol=9246432.0 | **GOOD** |
| `A07_rvol` | RVOL=1.149 on 2026-10-22: today_vol=9246432 / avg20=8047966 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.080 on 2026-10-22 (price=33.4200, mid=30.9045, upper=33.2331, lower=28.5759; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=33.4200 vs SMA50=32.4132 dist=+3.11% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=30.90_50=32.41_80=31.98 on 2026-10-22: SMA20=30.9045 SMA50=32.4132 SMA80=31.9848 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-27→2026-10-22 (63 bars); S1[2026-07-27→2026-08-24] low=2026-07-27@29.6790; S2[2026-08-25→2026-09-23] low=2026-09-23@28.7750; S3[2026-09-24→2026-10-22] low=2026-10-01@28.6950 | lows=[29.679000854492188, 28.774999618530273, 28.69499969482422] span=3.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.9187106562504159 wick_frac=0.0812893437495841 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=0.20270293490961577 wick_frac=0.7972970650903842 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.388865341414826 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:RED:body=-0.9700:wick=0.5350; 2026-10-19:RED:body=-0.3800:wick=0.1949; 2026-10-20:GREEN:body=+1.0000:wick=0.1900; 2026-10-21:RED:body=-0.1800:wick=0.7080; 2026-10-22:GREEN:body=+0.7900:wick=0.0699 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.73 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.87 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6804.0 | **NEUTRAL** |
| `B04_income` | 486.0 | **GOOD** |
| `B05_profit_margin` | 7.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 38.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=38.96 vs prior_export=38.96 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | -0.7 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.7 vs prior=-0.7 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.41 | **GOOD** |
| `B13_short_float` | 6.54 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.73 (this export) | prior_export=11.73 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.87 (this export) | prior_export=1.87 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMG  ·  score **+14**  ·  Entertainment
price=31.690000534057617  pair=`2026-10-21→2026-10-22`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=74.10 on 2026-10-22; prev RSI=72.88 on 2026-10-21 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.88@2026-10-21 → 74.10@2026-10-22 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.88@2026-10-21 → 74.10@2026-10-22 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.88@2026-10-21 → 74.10@2026-10-22 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6400 R=0.0000); 2026-10-21:GREEN:O=31.0100,C=31.4400,body=+0.4300,vol=1864888.0; 2026-10-22:GREEN:O=31.4800,C=31.6900,body=+0.2100,vol=2126690.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-21 + 2026-10-22; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3991578 Rvol=0); 2026-10-21:GREEN:O=31.0100,C=31.4400,body=+0.4300,vol=1864888.0; 2026-10-22:GREEN:O=31.4800,C=31.6900,body=+0.2100,vol=2126690.0 | **GOOD** |
| `A07_rvol` | RVOL=0.894 on 2026-10-22: today_vol=2126690 / avg20=2378168 (avg window 2026-09-24→2026-10-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.811 on 2026-10-22 (price=31.6900, mid=28.9233, upper=32.3369, lower=25.5096; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-22: price=31.6900 vs SMA50=28.2053 dist=+12.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-22: SMA20=28.9233 SMA50=28.2053 SMA80=27.8949 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-24→2026-10-22 (63 bars); S1[2026-07-24→2026-08-24] low=2026-08-12@24.5150; S2[2026-08-25→2026-09-23] low=2026-09-23@26.9500; S3[2026-09-24→2026-10-22] low=2026-09-28@26.4500 | lows=[24.514999389648438, 26.950000762939453, 26.450000762939453] span=9.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: GREEN body_frac=0.6449482272195433 wick_frac=0.35505177278045674 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-21+2026-10-22: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-16:GREEN:body=+0.4000:wick=0.3250; 2026-10-19:RED:body=-0.2200:wick=0.4500; 2026-10-20:GREEN:body=+0.2000:wick=0.4450; 2026-10-21:GREEN:body=+0.4300:wick=0.0400; 2026-10-22:GREEN:body=+0.2100:wick=0.3500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-23_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.