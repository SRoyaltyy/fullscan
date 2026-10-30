# A+B1 Feature Checklist — 2026-10-30

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-30.csv` · prior export for Δ: `2026-10-29`
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
| 1 | WMG | +16 | 16 | 0 | 2026-10-28→2026-10-29 | Entertainment |
| 2 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 3 | AME | +16 | 17 | 1 | 2026-10-28→2026-10-29 | Specialty Industrial Machinery |
| 4 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 5 | SWKS | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Semiconductors |
| 6 | EPAM | +15 | 15 | 0 | 2026-10-28→2026-10-29 | Information Technology Services |
| 7 | ZM | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Software - Application |
| 8 | FRSH | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Software - Application |
| 9 | TJX | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Apparel Retail |
| 10 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 11 | TLYS | +15 | 16 | 1 | 2026-10-28→2026-10-29 | Apparel Retail |
| 12 | GCT | +14 | 16 | 2 | 2026-10-28→2026-10-29 | Software - Infrastructure |
| 13 | SU | +14 | 15 | 1 | 2026-10-28→2026-10-29 | Oil & Gas Integrated |
| 14 | VEEV | +14 | 16 | 2 | 2026-10-28→2026-10-29 | Health Information Services |
| 15 | THC | +14 | 16 | 2 | 2026-10-28→2026-10-29 | Medical Care Facilities |

## Full checklist — top 15

### WMG  ·  score **+16**  ·  Entertainment
price=30.469999313354492  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.78 on 2026-10-29; prev RSI=51.11 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.11@2026-10-28 → 57.78@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.093 (G=0.9000 R=0.4300); 2026-10-28:RED:O=30.0100,C=29.5800,body=-0.4300,vol=1561033.0; 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=2.076 (Gvol=3240998 Rvol=1561033); 2026-10-28:RED:O=30.0100,C=29.5800,body=-0.4300,vol=1561033.0; 2026-10-29:GREEN:O=29.5700,C=30.4700,body=+0.9000,vol=3240998.0 | **GOOD** |
| `A07_rvol` | RVOL=1.432 on 2026-10-29: today_vol=3240998 / avg20=2263832 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.271 on 2026-10-29 (price=30.4700, mid=29.6708, upper=32.6176, lower=26.7239; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=30.4700 vs SMA50=28.6298 dist=+6.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=29.6708 SMA50=28.6298 SMA80=28.0813 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-12@24.5150; S2[2026-09-01→2026-09-30] low=2026-09-28@26.4500; S3[2026-10-01→2026-10-29] low=2026-10-05@26.7000 | lows=[24.514999389648438, 26.450000762939453, 26.700000762939453] span=8.91% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6498200066653997 wick_frac=0.3501799933346003 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.477778319370829 wick_frac=0.522221680629171 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.093020883234861 need>1.4; red_wick_gt_green=False 5d trail=2026-10-23:RED:body=-0.5000:wick=0.5200; 2026-10-26:RED:body=-0.4600:wick=0.3050; 2026-10-27:RED:body=-0.2700:wick=0.1900; 2026-10-28:RED:body=-0.4300:wick=0.4700; 2026-10-29:GREEN:body=+0.9000:wick=0.4850 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.63 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.01 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7304.0 | **NEUTRAL** |
| `B04_income` | 665.0 | **GOOD** |
| `B05_profit_margin` | 9.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.6 vs prior_export=37.6 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.47 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.76 | **GOOD** |
| `B13_short_float` | 6.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.63 (this export) | prior_export=10.63 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.01 (this export) | prior_export=3.01 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AME  ·  score **+16**  ·  Specialty Industrial Machinery
price=251.77999877929688  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.95 on 2026-10-29; prev RSI=53.32 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.32@2026-10-28 → 57.95@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=4.099 (G=3.7300 R=0.9100); 2026-10-28:RED:O=249.0400,C=248.1300,body=-0.9100,vol=1138431.0; 2026-10-29:GREEN:O=248.0500,C=251.7800,body=+3.7300,vol=1840954.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.617 (Gvol=1840954 Rvol=1138431); 2026-10-28:RED:O=249.0400,C=248.1300,body=-0.9100,vol=1138431.0; 2026-10-29:GREEN:O=248.0500,C=251.7800,body=+3.7300,vol=1840954.0 | **GOOD** |
| `A07_rvol` | RVOL=1.477 on 2026-10-29: today_vol=1840954 / avg20=1246038 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.505 on 2026-10-29 (price=251.7800, mid=247.9415, upper=255.5396, lower=240.3434; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=251.7800 vs SMA50=243.4302 dist=+3.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=247.9415 SMA50=243.4302 SMA80=243.4298 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-31@234.3900; S2[2026-09-01→2026-09-30] low=2026-09-01@229.6000; S3[2026-10-01→2026-10-29] low=2026-10-22@234.4300 | lows=[234.38999938964844, 229.60000610351562, 234.42999267578125] span=2.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7159291594790346 wick_frac=0.28407084052096543 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.28173052848389796 wick_frac=0.7182694715161021 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.098948639267569 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+3.8700:wick=2.3900; 2026-10-26:GREEN:body=+0.8200:wick=2.1900; 2026-10-27:GREEN:body=+3.2100:wick=2.0000; 2026-10-28:RED:body=-0.9100:wick=2.3200; 2026-10-29:GREEN:body=+3.7300:wick=1.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.87 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.63 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7863.92 | **NEUTRAL** |
| `B04_income` | 1576.27 | **GOOD** |
| `B05_profit_margin` | 20.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 281.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=281.94 vs prior_export=281.94 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.78 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.87 (this export) | prior_export=4.87 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.63 (this export) | prior_export=4.63 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+15**  ·  Semiconductors
price=89.43000030517578  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.62 on 2026-10-29; prev RSI=63.06 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.06@2026-10-28 → 66.62@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.1800 R=0.0000); 2026-10-28:GREEN:O=85.4800,C=87.2300,body=+1.7500,vol=3198881.0; 2026-10-29:GREEN:O=88.0000,C=89.4300,body=+1.4300,vol=4330962.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=7529843 Rvol=0); 2026-10-28:GREEN:O=85.4800,C=87.2300,body=+1.7500,vol=3198881.0; 2026-10-29:GREEN:O=88.0000,C=89.4300,body=+1.4300,vol=4330962.0 | **GOOD** |
| `A07_rvol` | RVOL=0.643 on 2026-10-29: today_vol=4330962 / avg20=6734475 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.019 on 2026-10-29 (price=89.4300, mid=81.7130, upper=89.2853, lower=74.1407; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-29: price=89.4300 vs SMA50=80.0816 dist=+11.67% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=81.7130 SMA50=80.0816 SMA80=73.8719 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-03@59.9200; S2[2026-09-01→2026-09-30] low=2026-09-01@64.6500; S3[2026-10-01→2026-10-29] low=2026-10-13@73.7600 | lows=[59.91999816894531, 64.6500015258789, 73.76000213623047] span=23.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6065945528108387 wick_frac=0.3934054471891612 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+1.4800:wick=1.2300; 2026-10-26:RED:body=-0.0900:wick=2.1100; 2026-10-27:GREEN:body=+1.7300:wick=0.5750; 2026-10-28:GREEN:body=+1.7500:wick=0.3150; 2026-10-29:GREEN:body=+1.4300:wick=2.4800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=134.6999969482422  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.92 on 2026-10-29; prev RSI=68.72 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.72@2026-10-28 → 65.92@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.2800 R=0.0000); 2026-10-28:GREEN:O=132.3600,C=136.2300,body=+3.8700,vol=440777.0; 2026-10-29:GREEN:O=132.2900,C=134.7000,body=+2.4100,vol=702187.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1142964 Rvol=0); 2026-10-28:GREEN:O=132.3600,C=136.2300,body=+3.8700,vol=440777.0; 2026-10-29:GREEN:O=132.2900,C=134.7000,body=+2.4100,vol=702187.0 | **GOOD** |
| `A07_rvol` | RVOL=0.750 on 2026-10-29: today_vol=702187 / avg20=936768 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.517 on 2026-10-29 (price=134.7000, mid=122.9725, upper=145.6707, lower=100.2743; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=134.7000 vs SMA50=117.4008 dist=+14.74% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=122.9725 SMA50=117.4008 SMA80=109.1614 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-06@87.0000; S2[2026-09-01→2026-09-30] low=2026-09-28@104.3300; S3[2026-10-01→2026-10-29] low=2026-10-08@106.1200 | lows=[87.0, 104.33000183105469, 106.12000274658203] span=21.98% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6943269757186539 wick_frac=0.3056730242813462 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-23:GREEN:body=+0.5100:wick=6.1700; 2026-10-26:GREEN:body=+1.3100:wick=2.5800; 2026-10-27:RED:body=-0.8400:wick=3.0650; 2026-10-28:GREEN:body=+3.8700:wick=0.4900; 2026-10-29:GREEN:body=+2.4100:wick=2.4000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZM  ·  score **+15**  ·  Software - Application
price=97.66000366210938  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.76 on 2026-10-29; prev RSI=58.36 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.36@2026-10-28 → 52.76@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=4.500 (G=3.6000 R=0.8000); 2026-10-28:GREEN:O=96.6600,C=100.2600,body=+3.6000,vol=2347456.0; 2026-10-29:RED:O=98.4600,C=97.6600,body=-0.8000,vol=1863368.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.260 (Gvol=2347456 Rvol=1863368); 2026-10-28:GREEN:O=96.6600,C=100.2600,body=+3.6000,vol=2347456.0; 2026-10-29:RED:O=98.4600,C=97.6600,body=-0.8000,vol=1863368.0 | **GOOD** |
| `A07_rvol` | RVOL=0.700 on 2026-10-29: today_vol=1863368 / avg20=2663389 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.266 on 2026-10-29 (price=97.6600, mid=96.5485, upper=100.7258, lower=92.3712; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=97.6600 vs SMA50=95.7775 dist=+1.97% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=96.55_50=95.78_80=96.11 on 2026-10-29: SMA20=96.5485 SMA50=95.7775 SMA80=96.1128 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-26@92.7200; S2[2026-09-01→2026-09-30] low=2026-09-28@85.4900; S3[2026-10-01→2026-10-29] low=2026-10-01@90.5800 | lows=[92.72000122070312, 85.48999786376953, 90.58000183105469] span=8.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.8181811876535632 wick_frac=0.18181881234643682 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.2816881355021558 wick_frac=0.7183118644978441 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.500023841994335 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.2300:wick=2.2313; 2026-10-26:GREEN:body=+2.2700:wick=0.5400; 2026-10-27:RED:body=-0.2200:wick=2.0100; 2026-10-28:GREEN:body=+3.6000:wick=0.8000; 2026-10-29:RED:body=-0.8000:wick=2.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.83 (current export asof; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.83 (current export; earnings_date=8/25/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4993.05 | **NEUTRAL** |
| `B04_income` | 3255.07 | **GOOD** |
| `B05_profit_margin` | 65.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 118.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=118.17 vs prior_export=118.17 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.01 vs prior=-1.01 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.1 | **GOOD** |
| `B13_short_float` | 2.69 | **NEUTRAL** |
| `B14_earnings_date` | 8/25/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.83 (this export) | prior_export=4.83 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.83 (this export) | prior_export=0.83 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+15**  ·  Software - Application
price=15.010000228881836  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.04 on 2026-10-29; prev RSI=66.75 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.75@2026-10-28 → 61.04@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.6900 R=0.0000); 2026-10-28:GREEN:O=14.6900,C=15.3800,body=+0.6900,vol=5075048.0; 2026-10-29:DOJI:O=15.0100,C=15.0100,body=+0.0000,vol=3723214.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=3.726 (Gvol=6936655 Rvol=1861607); 2026-10-28:GREEN:O=14.6900,C=15.3800,body=+0.6900,vol=5075048.0; 2026-10-29:DOJI:O=15.0100,C=15.0100,body=+0.0000,vol=3723214.0 | **GOOD** |
| `A07_rvol` | RVOL=0.514 on 2026-10-29: today_vol=3723214 / avg20=7240706 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.456 on 2026-10-29 (price=15.0100, mid=14.2842, upper=15.8766, lower=12.6919; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=15.0100 vs SMA50=13.3955 dist=+12.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=14.2842 SMA50=13.3955 SMA80=12.6164 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-06@10.8720; S2[2026-09-01→2026-09-30] low=2026-09-10@11.7040; S3[2026-10-01→2026-10-29] low=2026-10-02@12.6914 | lows=[10.871999740600586, 11.704000473022461, 12.691399574279785] span=16.73% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.9078951661275062 wick_frac=0.09210483387249378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+0.2000:wick=0.1850; 2026-10-26:RED:body=-0.1000:wick=0.2450; 2026-10-27:RED:body=-0.3100:wick=0.2300; 2026-10-28:GREEN:body=+0.6900:wick=0.0700; 2026-10-29:DOJI:body=+0.0000:wick=0.5700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.64 vs prior_export=14.64 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.42 | **GOOD** |
| `B13_short_float` | 11.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TJX  ·  score **+15**  ·  Apparel Retail
price=141.38999938964844  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.22 on 2026-10-29; prev RSI=45.38 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.38@2026-10-28 → 52.22@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.475 (G=1.4600 R=0.5900); 2026-10-28:RED:O=138.9800,C=138.3900,body=-0.5900,vol=4065949.0; 2026-10-29:GREEN:O=139.9300,C=141.3900,body=+1.4600,vol=4238070.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.042 (Gvol=4238070 Rvol=4065949); 2026-10-28:RED:O=138.9800,C=138.3900,body=-0.5900,vol=4065949.0; 2026-10-29:GREEN:O=139.9300,C=141.3900,body=+1.4600,vol=4238070.0 | **GOOD** |
| `A07_rvol` | RVOL=0.650 on 2026-10-29: today_vol=4238070 / avg20=6525016 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.039 on 2026-10-29 (price=141.3900, mid=141.7285, upper=150.4207, lower=133.0363; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=141.3900 vs SMA50=135.6918 dist=+4.20% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=141.73_50=135.69_80=142.74 on 2026-10-29: SMA20=141.7285 SMA50=135.6918 SMA80=142.7383 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-31@133.0800; S2[2026-09-01→2026-09-30] low=2026-09-16@122.7800; S3[2026-10-01→2026-10-29] low=2026-10-01@131.4400 | lows=[133.0800018310547, 122.77999877929688, 131.44000244140625] span=8.39% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6854475901197776 wick_frac=0.3145524098802224 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.32113817762015895 wick_frac=0.678861822379841 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.474603010396731 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-1.6400:wick=1.0100; 2026-10-26:RED:body=-1.1300:wick=1.3900; 2026-10-27:RED:body=-2.0700:wick=0.8600; 2026-10-28:RED:body=-0.5900:wick=1.2472; 2026-10-29:GREEN:body=+1.4600:wick=0.6700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.6 (current export asof; earnings_date=8/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.1 (current export; earnings_date=8/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 62363.0 | **NEUTRAL** |
| `B04_income` | 6067.0 | **GOOD** |
| `B05_profit_margin` | 9.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 171.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=171.58 vs prior_export=171.58 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.68 | **GOOD** |
| `B10_insider_transactions` | -8.46 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.46 vs prior=-8.46 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 1.68 | **NEUTRAL** |
| `B14_earnings_date` | 8/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.6 (this export) | prior_export=2.6 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.1 (this export) | prior_export=0.1 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TLYS  ·  score **+15**  ·  Apparel Retail
price=4.340000152587891  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.56 on 2026-10-29; prev RSI=48.20 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 48.20@2026-10-28 → 47.56@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=5.000 (G=0.1000 R=0.0200); 2026-10-28:GREEN:O=4.2600,C=4.3600,body=+0.1000,vol=103707.0; 2026-10-29:RED:O=4.3600,C=4.3400,body=-0.0200,vol=83313.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.245 (Gvol=103707 Rvol=83313); 2026-10-28:GREEN:O=4.2600,C=4.3600,body=+0.1000,vol=103707.0; 2026-10-29:RED:O=4.3600,C=4.3400,body=-0.0200,vol=83313.0 | **GOOD** |
| `A07_rvol` | RVOL=0.596 on 2026-10-29: today_vol=83313 / avg20=139831 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.421 on 2026-10-29 (price=4.3400, mid=4.4780, upper=4.8055, lower=4.1505; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=4.3400 vs SMA50=4.3194 dist=+0.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=4.4780 SMA50=4.3194 SMA80=4.2071 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-18@3.6000; S2[2026-09-01→2026-09-30] low=2026-09-02@3.8000; S3[2026-10-01→2026-10-29] low=2026-10-26@4.1100 | lows=[3.5999999046325684, 3.799999952316284, 4.110000133514404] span=14.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.769229358471188 wick_frac=0.23077064152881194 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.1333331214058422 wick_frac=0.8666668785941578 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.3200:wick=0.0700; 2026-10-26:RED:body=-0.0800:wick=0.1500; 2026-10-27:GREEN:body=+0.1000:wick=0.0150; 2026-10-28:GREEN:body=+0.1000:wick=0.0300; 2026-10-29:RED:body=-0.0200:wick=0.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=63.64 (current export asof; earnings_date=9/2/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.14 (current export; earnings_date=9/2/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 582.94 | **NEUTRAL** |
| `B04_income` | 1.96 | **GOOD** |
| `B05_profit_margin` | 0.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 6.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=6.65 vs prior_export=6.65 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -21.5 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.5 vs prior=-21.5 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.73 | **GOOD** |
| `B13_short_float` | 3.32 | **NEUTRAL** |
| `B14_earnings_date` | 9/2/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=63.64 (this export) | prior_export=63.64 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.14 (this export) | prior_export=4.14 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GCT  ·  score **+14**  ·  Software - Infrastructure
price=60.619998931884766  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.76 on 2026-10-29; prev RSI=65.81 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.81@2026-10-28 → 65.76@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=1.508 (G=0.9500 R=0.6300); 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0; 2026-10-29:RED:O=61.2500,C=60.6200,body=-0.6300,vol=480148.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=0.842 (Gvol=404220 Rvol=480148); 2026-10-28:GREEN:O=59.6800,C=60.6300,body=+0.9500,vol=404220.0; 2026-10-29:RED:O=61.2500,C=60.6200,body=-0.6300,vol=480148.0 | **BAD** |
| `A07_rvol` | RVOL=0.754 on 2026-10-29: today_vol=480148 / avg20=636471 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.694 on 2026-10-29 (price=60.6200, mid=57.8455, upper=61.8435, lower=53.8475; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=60.6200 vs SMA50=54.2015 dist=+11.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=57.8455 SMA50=54.2015 SMA80=49.9751 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-03@43.1820; S2[2026-09-01→2026-09-30] low=2026-09-01@46.0100; S3[2026-10-01→2026-10-29] low=2026-10-01@51.9500 | lows=[43.18199920654297, 46.0099983215332, 51.95000076293945] span=20.30% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.6506858620959945 wick_frac=0.34931413790400545 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.2636430248763208 wick_frac=0.7363569751236791 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5079351623665616 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.1950:wick=1.8050; 2026-10-26:GREEN:body=+0.6500:wick=1.3300; 2026-10-27:RED:body=-0.3600:wick=1.1000; 2026-10-28:GREEN:body=+0.9500:wick=0.5100; 2026-10-29:RED:body=-0.6300:wick=1.7596 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.89 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.15 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1466.52 | **NEUTRAL** |
| `B04_income` | 156.13 | **GOOD** |
| `B05_profit_margin` | 10.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.5 vs prior_export=65.5 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -1.48 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.48 vs prior=-1.48 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.72 | **GOOD** |
| `B13_short_float` | 13.01 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.89 (this export) | prior_export=28.89 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.15 (this export) | prior_export=8.15 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SU  ·  score **+14**  ·  Oil & Gas Integrated
price=77.62999725341797  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.78 on 2026-10-29; prev RSI=61.55 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.55@2026-10-28 → 61.78@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.55@2026-10-28 → 61.78@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.55@2026-10-28 → 61.78@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=2.826 (G=0.6500 R=0.2300); 2026-10-28:GREEN:O=76.8900,C=77.5400,body=+0.6500,vol=3748362.0; 2026-10-29:RED:O=77.8600,C=77.6300,body=-0.2300,vol=2259427.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=1.659 (Gvol=3748362 Rvol=2259427); 2026-10-28:GREEN:O=76.8900,C=77.5400,body=+0.6500,vol=3748362.0; 2026-10-29:RED:O=77.8600,C=77.6300,body=-0.2300,vol=2259427.0 | **GOOD** |
| `A07_rvol` | RVOL=0.668 on 2026-10-29: today_vol=2259427 / avg20=3380086 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.334 on 2026-10-29 (price=77.6300, mid=74.9815, upper=82.9021, lower=67.0609; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=77.6300 vs SMA50=70.8274 dist=+9.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=74.9815 SMA50=70.8274 SMA80=68.1678 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-07@59.9500; S2[2026-09-01→2026-09-30] low=2026-09-22@65.8600; S3[2026-10-01→2026-10-29] low=2026-10-06@67.4400 | lows=[59.95000076293945, 65.86000061035156, 67.44000244140625] span=12.49% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.5508486082824169 wick_frac=0.44915139171758317 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.15054531290573878 wick_frac=0.8494546870942612 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8260523435167677 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+1.1300:wick=0.5869; 2026-10-26:GREEN:body=+0.8800:wick=0.7800; 2026-10-27:RED:body=-2.8300:wick=0.5500; 2026-10-28:GREEN:body=+0.6500:wick=0.5300; 2026-10-29:RED:body=-0.2300:wick=1.2978 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.59 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.91 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 40945.89 | **NEUTRAL** |
| `B04_income` | 6460.91 | **GOOD** |
| `B05_profit_margin` | 15.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=76.21 vs prior_export=76.21 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.93 | **BAD** |
| `B13_short_float` | 1.77 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.59 (this export) | prior_export=6.59 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.91 (this export) | prior_export=7.91 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+14**  ·  Health Information Services
price=310.3399963378906  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.19 on 2026-10-29; prev RSI=67.08 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.08@2026-10-28 → 59.19@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.08@2026-10-28 → 59.19@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.08@2026-10-28 → 59.19@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=4.418 (G=7.8200 R=1.7700); 2026-10-28:GREEN:O=310.0000,C=317.8200,body=+7.8200,vol=1395807.0; 2026-10-29:RED:O=312.1100,C=310.3400,body=-1.7700,vol=1791079.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=0.779 (Gvol=1395807 Rvol=1791079); 2026-10-28:GREEN:O=310.0000,C=317.8200,body=+7.8200,vol=1395807.0; 2026-10-29:RED:O=312.1100,C=310.3400,body=-1.7700,vol=1791079.0 | **BAD** |
| `A07_rvol` | RVOL=1.237 on 2026-10-29: today_vol=1791079 / avg20=1447408 (avg window 2026-10-01→2026-10-28, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.194 on 2026-10-29 (price=310.3400, mid=304.1690, upper=335.9363, lower=272.4017; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=310.3400 vs SMA50=282.6092 dist=+9.81% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=304.1690 SMA50=282.6092 SMA80=255.7162 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-04@204.6800; S2[2026-09-01→2026-09-30] low=2026-09-21@257.8700; S3[2026-10-01→2026-10-29] low=2026-10-02@272.8350 | lows=[204.67999267578125, 257.8699951171875, 272.8349914550781] span=33.30% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7447626023065477 wick_frac=0.2552373976934524 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=0.20263216736308118 wick_frac=0.7973678326369188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.418110657080295 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:GREEN:body=+2.3700:wick=3.3900; 2026-10-26:RED:body=-4.8600:wick=4.1000; 2026-10-27:RED:body=-2.0400:wick=5.8900; 2026-10-28:GREEN:body=+7.8200:wick=2.6800; 2026-10-29:RED:body=-1.7700:wick=6.9650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.38 vs prior=-0.38 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.03 | **GOOD** |
| `B13_short_float` | 3.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

### THC  ·  score **+14**  ·  Medical Care Facilities
price=270.57000732421875  pair=`2026-10-28→2026-10-29`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.69 on 2026-10-29; prev RSI=53.71 on 2026-10-28 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.71@2026-10-28 → 55.69@2026-10-29 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.71@2026-10-28 → 55.69@2026-10-29 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.71@2026-10-28 → 55.69@2026-10-29 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=19.0600 R=0.0000); 2026-10-28:GREEN:O=261.0000,C=268.5000,body=+7.5000,vol=1090205.0; 2026-10-29:GREEN:O=259.0100,C=270.5700,body=+11.5600,vol=1460134.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-28 + 2026-10-29; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2550339 Rvol=0); 2026-10-28:GREEN:O=261.0000,C=268.5000,body=+7.5000,vol=1090205.0; 2026-10-29:GREEN:O=259.0100,C=270.5700,body=+11.5600,vol=1460134.0 | **GOOD** |
| `A07_rvol` | RVOL=2.381 on 2026-10-29: today_vol=1460134 / avg20=613251 (avg window 2026-10-01→2026-10-28, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.419 on 2026-10-29 (price=270.5700, mid=266.3690, upper=276.3907, lower=256.3473; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-29: price=270.5700 vs SMA50=264.8576 dist=+2.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-29: SMA20=266.3690 SMA50=264.8576 SMA80=253.6412 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-03→2026-10-29 (63 bars); S1[2026-08-03→2026-08-31] low=2026-08-04@244.4400; S2[2026-09-01→2026-09-30] low=2026-09-25@248.8600; S3[2026-10-01→2026-10-29] low=2026-10-01@252.1200 | lows=[244.44000244140625, 248.86000061035156, 252.1199951171875] span=3.14% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: GREEN body_frac=0.7473193430345794 wick_frac=0.2526806569654206 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-28+2026-10-29: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-23:RED:body=-0.2400:wick=5.5950; 2026-10-26:GREEN:body=+0.0200:wick=7.6300; 2026-10-27:RED:body=-9.2300:wick=10.1500; 2026-10-28:GREEN:body=+7.5000:wick=2.5350; 2026-10-29:GREEN:body=+11.5600:wick=3.9100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=43.63 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.62 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 21812.0 | **NEUTRAL** |
| `B04_income` | 2241.0 | **GOOD** |
| `B05_profit_margin` | 10.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 284.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=284.62 vs prior_export=284.62 on finviz_2026-10-29) | **NEUTRAL** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | -20.3 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-20.3 vs prior=-20.3 on finviz_2026-10-29) | **NEUTRAL** |
| `B12_institutional_transactions` | -4.84 | **BAD** |
| `B13_short_float` | 2.53 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=43.63 (this export) | prior_export=43.63 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.62 (this export) | prior_export=3.62 (finviz_2026-10-29) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-30_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.