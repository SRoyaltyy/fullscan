# A+B1 Feature Checklist — 2026-11-04

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-11-04.csv` · prior export for Δ: `2026-11-03`
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
| 1 | TRI | +17 | 17 | 0 | 2026-10-30→2026-11-02 | Specialty Business Services |
| 2 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 3 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 4 | HHH | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Real Estate - Diversified |
| 5 | EPAM | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Information Technology Services |
| 6 | CDNA | +15 | 18 | 3 | 2026-10-30→2026-11-02 | Diagnostics & Research |
| 7 | DEO | +15 | 16 | 1 | 2026-10-30→2026-11-02 | Beverages - Wineries & Distilleries |
| 8 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 9 | CXT | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Business Equipment & Supplies |
| 10 | NWBI | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Banks - Regional |
| 11 | PODD | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Devices |
| 12 | ATRC | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Instruments & Supplies |
| 13 | POOL | +14 | 16 | 2 | 2026-10-30→2026-11-02 | Industrial Distribution |
| 14 | EMBC | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Medical Instruments & Supplies |
| 15 | GEHC | +14 | 17 | 3 | 2026-10-30→2026-11-02 | Medical Devices |

## Full checklist — top 15

### TRI  ·  score **+17**  ·  Specialty Business Services
price=122.23999786376953  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.70 on 2026-11-02; prev RSI=65.23 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.23@2026-10-30 → 69.70@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=2.962 (G=4.6800 R=1.5800); 2026-10-30:RED:O=119.8300,C=118.2500,body=-1.5800,vol=2377280.0; 2026-11-02:GREEN:O=117.5600,C=122.2400,body=+4.6800,vol=2500561.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.052 (Gvol=2500561 Rvol=2377280); 2026-10-30:RED:O=119.8300,C=118.2500,body=-1.5800,vol=2377280.0; 2026-11-02:GREEN:O=117.5600,C=122.2400,body=+4.6800,vol=2500561.0 | **GOOD** |
| `A07_rvol` | RVOL=1.603 on 2026-11-02: today_vol=2500561 / avg20=1560185 (avg window 2026-10-05→2026-10-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.658 on 2026-11-02 (price=122.2400, mid=111.3990, upper=127.8706, lower=94.9274; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=122.2400 vs SMA50=105.1538 dist=+16.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=111.3990 SMA50=105.1538 SMA80=102.8594 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@96.3300; S2[2026-09-03→2026-10-02] low=2026-09-29@92.5700; S3[2026-10-05→2026-11-02] low=2026-10-05@95.1425 | lows=[96.33000183105469, 92.56999969482422, 95.14250183105469] span=4.06% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.7279514206170445 wick_frac=0.27204857938295546 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.48840966282012277 wick_frac=0.5115903371798772 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.9620220769312486 need>1.4; red_wick_gt_green=False 5d trail=2026-10-27:RED:body=-4.1900:wick=1.4950; 2026-10-28:GREEN:body=+0.9900:wick=1.8500; 2026-10-29:RED:body=-1.1100:wick=1.6800; 2026-10-30:RED:body=-1.5800:wick=1.6550; 2026-11-02:GREEN:body=+4.6800:wick=1.7490 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.32 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7832.0 | **NEUTRAL** |
| `B04_income` | 1658.0 | **GOOD** |
| `B05_profit_margin` | 21.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.64 vs prior_export=122.64 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 9.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.32 (this export) | prior_export=3.32 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.86 (this export) | prior_export=1.86 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HHH  ·  score **+15**  ·  Real Estate - Diversified
price=77.91000366210938  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.60 on 2026-11-02; prev RSI=65.58 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.58@2026-10-30 → 68.60@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7500 R=0.0000); 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0; 2026-11-02:GREEN:O=77.0400,C=77.9100,body=+0.8700,vol=720915.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1613971 Rvol=0); 2026-10-30:GREEN:O=74.3500,C=76.2300,body=+1.8800,vol=893056.0; 2026-11-02:GREEN:O=77.0400,C=77.9100,body=+0.8700,vol=720915.0 | **GOOD** |
| `A07_rvol` | RVOL=0.768 on 2026-11-02: today_vol=720915 / avg20=938184 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.040 on 2026-11-02 (price=77.9100, mid=71.8310, upper=77.6755, lower=65.9865; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=77.9100 vs SMA50=68.1356 dist=+14.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=71.8310 SMA50=68.1356 SMA80=68.0869 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-09-01@63.0200; S2[2026-09-03→2026-10-02] low=2026-09-18@60.0900; S3[2026-10-05→2026-11-02] low=2026-10-19@66.1400 | lows=[63.02000045776367, 60.09000015258789, 66.13999938964844] span=10.07% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.5853732169820749 wick_frac=0.414626783017925 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-27:GREEN:body=+0.2900:wick=1.6200; 2026-10-28:GREEN:body=+0.4400:wick=1.2550; 2026-10-29:GREEN:body=+1.5600:wick=0.5550; 2026-10-30:GREEN:body=+1.8800:wick=0.9600; 2026-11-02:GREEN:body=+0.8700:wick=0.8400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=170.05 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=139.3 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2334.65 | **NEUTRAL** |
| `B04_income` | 292.1 | **GOOD** |
| `B05_profit_margin` | 12.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.67 vs prior_export=89.67 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 1.67 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.67 vs prior=1.67 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.18 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=170.05 (this export) | prior_export=170.05 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=139.3 (this export) | prior_export=139.3 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EPAM  ·  score **+15**  ·  Information Technology Services
price=143.8000030517578  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=73.13 on 2026-11-02; prev RSI=70.79 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 70.79@2026-10-30 → 73.13@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.1000 R=0.0000); 2026-10-30:GREEN:O=137.2100,C=140.5100,body=+3.3000,vol=1291536.0; 2026-11-02:GREEN:O=141.0000,C=143.8000,body=+2.8000,vol=1330402.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2621938 Rvol=0); 2026-10-30:GREEN:O=137.2100,C=140.5100,body=+3.3000,vol=1291536.0; 2026-11-02:GREEN:O=141.0000,C=143.8000,body=+2.8000,vol=1330402.0 | **GOOD** |
| `A07_rvol` | RVOL=1.462 on 2026-11-02: today_vol=1330402 / avg20=909862 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.751 on 2026-11-02 (price=143.8000, mid=126.2760, upper=149.6051, lower=102.9469; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=143.8000 vs SMA50=118.7450 dist=+21.10% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=126.2760 SMA50=118.7450 SMA80=110.5102 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@87.0000; S2[2026-09-03→2026-10-02] low=2026-09-28@104.3300; S3[2026-10-05→2026-11-02] low=2026-10-08@106.1200 | lows=[87.0, 104.33000183105469, 106.12000274658203] span=21.98% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.6239463412015004 wick_frac=0.37605365879849956 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:RED:body=-0.8400:wick=3.0650; 2026-10-28:GREEN:body=+3.8700:wick=0.4900; 2026-10-29:GREEN:body=+2.4100:wick=2.4000; 2026-10-30:GREEN:body=+3.3000:wick=0.8500; 2026-11-02:GREEN:body=+2.8000:wick=3.3849 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.09 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5616.75 | **NEUTRAL** |
| `B04_income` | 401.67 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 120.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=120.69 vs prior_export=120.69 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.14 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.18 | **GOOD** |
| `B13_short_float` | 12.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.09 (this export) | prior_export=7.09 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CDNA  ·  score **+15**  ·  Diagnostics & Research
price=85.87999725341797  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.19 on 2026-11-02; prev RSI=81.90 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 81.90@2026-10-30 → 78.19@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=5.310 (G=5.3100 R=1.0000); 2026-10-30:GREEN:O=82.1000,C=87.4100,body=+5.3100,vol=3552531.0; 2026-11-02:RED:O=86.8800,C=85.8800,body=-1.0000,vol=2190573.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.622 (Gvol=3552531 Rvol=2190573); 2026-10-30:GREEN:O=82.1000,C=87.4100,body=+5.3100,vol=3552531.0; 2026-11-02:RED:O=86.8800,C=85.8800,body=-1.0000,vol=2190573.0 | **GOOD** |
| `A07_rvol` | RVOL=1.538 on 2026-11-02: today_vol=2190573 / avg20=1424559 (avg window 2026-10-05→2026-10-30, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.244 on 2026-11-02 (price=85.8800, mid=68.9290, upper=82.5605, lower=55.2975; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=85.8800 vs SMA50=60.6746 dist=+41.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=68.9290 SMA50=60.6746 SMA80=53.2779 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-07@44.5700; S2[2026-09-03→2026-10-02] low=2026-09-03@48.3860; S3[2026-10-05→2026-11-02] low=2026-10-08@61.1900 | lows=[44.56999969482422, 48.38600158691406, 61.189998626708984] span=37.29% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.5502595179646518 wick_frac=0.4497404820353482 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.16454135753649932 wick_frac=0.8354586424635007 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.310005187988281 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:GREEN:body=+1.4300:wick=2.4500; 2026-10-28:GREEN:body=+2.2200:wick=2.2100; 2026-10-29:RED:body=-0.5000:wick=2.6300; 2026-10-30:GREEN:body=+5.3100:wick=4.3400; 2026-11-02:RED:body=-1.0000:wick=5.0775 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=59.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.69 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 458.09 | **NEUTRAL** |
| `B04_income` | 111.01 | **GOOD** |
| `B05_profit_margin` | 24.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 56.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=56.14 vs prior_export=56.14 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.99 vs prior=-6.99 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.52 | **GOOD** |
| `B13_short_float` | 13.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=59.41 (this export) | prior_export=59.41 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.69 (this export) | prior_export=15.69 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DEO  ·  score **+15**  ·  Beverages - Wineries & Distilleries
price=97.62999725341797  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.74 on 2026-11-02; prev RSI=57.02 on 2026-10-30 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.02@2026-10-30 → 65.74@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=52.624 (G=2.1050 R=0.0400); 2026-10-30:RED:O=93.4800,C=93.4400,body=-0.0400,vol=928094.0; 2026-11-02:GREEN:O=95.5250,C=97.6300,body=+2.1050,vol=1312683.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.414 (Gvol=1312683 Rvol=928094); 2026-10-30:RED:O=93.4800,C=93.4400,body=-0.0400,vol=928094.0; 2026-11-02:GREEN:O=95.5250,C=97.6300,body=+2.1050,vol=1312683.0 | **GOOD** |
| `A07_rvol` | RVOL=1.213 on 2026-11-02: today_vol=1312683 / avg20=1082088 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.765 on 2026-11-02 (price=97.6300, mid=91.8850, upper=99.3995, lower=84.3705; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-02: price=97.6300 vs SMA50=89.7156 dist=+8.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=91.8850 SMA50=89.7156 SMA80=89.2676 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-05@87.2600; S2[2026-09-03→2026-10-02] low=2026-10-01@84.0000; S3[2026-10-05→2026-11-02] low=2026-10-05@83.2000 | lows=[87.26000213623047, 84.0, 83.19999694824219] span=4.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.9152143193593971 wick_frac=0.08478568064060292 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.043479342543910574 wick_frac=0.9565206574560894 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=52.62368872782758 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:GREEN:body=+0.0200:wick=0.6650; 2026-10-28:RED:body=-0.5800:wick=1.1050; 2026-10-29:RED:body=-0.6000:wick=1.1330; 2026-10-30:RED:body=-0.0400:wick=0.8800; 2026-11-02:GREEN:body=+2.1050:wick=0.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.73 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.38 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 19602.05 | **NEUTRAL** |
| `B04_income` | 1729.57 | **GOOD** |
| `B05_profit_margin` | 8.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 107.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=107.71 vs prior_export=107.71 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.08 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | -43.51 | **BAD** |
| `B13_short_float` | 0.56 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.73 (this export) | prior_export=9.73 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.38 (this export) | prior_export=0.38 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=68.71 vs prior_export=68.71 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.45 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.14 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.14 vs prior=0.14 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.7 | **GOOD** |
| `B13_short_float` | 6.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ATRC  ·  score **+14**  ·  Medical Instruments & Supplies
price=57.279998779296875  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.38 on 2026-10-12; prev RSI=54.68 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.68@2026-10-09 → 58.38@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.68@2026-10-09 → 58.38@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.68@2026-10-09 → 58.38@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.6600 R=0.0000); 2026-10-09:GREEN:O=53.1400,C=55.8100,body=+2.6700,vol=712491.0; 2026-10-12:GREEN:O=56.2900,C=57.2800,body=+0.9900,vol=645569.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1358060 Rvol=0); 2026-10-09:GREEN:O=53.1400,C=55.8100,body=+2.6700,vol=712491.0; 2026-10-12:GREEN:O=56.2900,C=57.2800,body=+0.9900,vol=645569.0 | **GOOD** |
| `A07_rvol` | RVOL=0.419 on 2026-10-12: today_vol=645569 / avg20=1538956 (avg window 2026-09-14→2026-10-09, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.089 on 2026-10-12 (price=57.2800, mid=56.8760, upper=61.4332, lower=52.3188; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=57.2800 vs SMA50=51.0768 dist=+12.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=56.8760 SMA50=51.0768 SMA80=44.1285 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-23@31.9000; S2[2026-08-13→2026-09-11] low=2026-08-13@43.0300; S3[2026-09-14→2026-10-12] low=2026-10-08@51.6300 | lows=[31.899999618530273, 43.029998779296875, 51.630001068115234] span=61.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6810392035235003 wick_frac=0.31896079647649966 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-2.3100:wick=0.8400; 2026-10-07:RED:body=-0.0400:wick=1.8600; 2026-10-08:RED:body=-0.2300:wick=1.6000; 2026-10-09:GREEN:body=+2.6700:wick=0.7850; 2026-10-12:GREEN:body=+0.9900:wick=0.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10688.24 (current export asof; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 569.62 | **NEUTRAL** |
| `B04_income` | 10.55 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 53.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=53.33 vs prior_export=53.33 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.4 | **GOOD** |
| `B10_insider_transactions` | -3.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.89 vs prior=-3.89 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.44 | **GOOD** |
| `B13_short_float` | 8.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10688.24 (this export) | prior_export=10688.24 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### POOL  ·  score **+14**  ·  Industrial Distribution
price=214.75999450683594  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.26 on 2026-11-02; prev RSI=75.61 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 75.61@2026-10-30 → 79.26@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.61@2026-10-30 → 79.26@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 75.61@2026-10-30 → 79.26@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=117.793 (G=5.8900 R=0.0500); 2026-10-30:RED:O=206.0000,C=205.9500,body=-0.0500,vol=733324.0; 2026-11-02:GREEN:O=208.8700,C=214.7600,body=+5.8900,vol=1046742.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.427 (Gvol=1046742 Rvol=733324); 2026-10-30:RED:O=206.0000,C=205.9500,body=-0.0500,vol=733324.0; 2026-11-02:GREEN:O=208.8700,C=214.7600,body=+5.8900,vol=1046742.0 | **GOOD** |
| `A07_rvol` | RVOL=1.200 on 2026-11-02: today_vol=1046742 / avg20=872414 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.911 on 2026-11-02 (price=214.7600, mid=178.4120, upper=218.3042, lower=138.5198; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=214.7600 vs SMA50=175.4369 dist=+22.41% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=178.41_50=175.44_80=183.22 on 2026-11-02: SMA20=178.4120 SMA50=175.4369 SMA80=183.2175 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-09-02@180.7300; S2[2026-09-03→2026-10-02] low=2026-10-01@158.4000; S3[2026-10-05→2026-11-02] low=2026-10-08@152.7000 | lows=[180.72999572753906, 158.39999389648438, 152.6999969482422] span=18.36% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.588411850338788 wick_frac=0.411588149661212 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.009506265954977952 wick_frac=0.9904937340450221 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=117.79279829111992 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:RED:body=-0.3300:wick=3.3123; 2026-10-28:GREEN:body=+3.1100:wick=2.1000; 2026-10-29:GREEN:body=+7.6000:wick=0.5400; 2026-10-30:RED:body=-0.0500:wick=5.2100; 2026-11-02:GREEN:body=+5.8900:wick=4.1200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.98 (current export asof; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.28 (current export; earnings_date=7/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5394.29 | **NEUTRAL** |
| `B04_income` | 397.74 | **GOOD** |
| `B05_profit_margin` | 7.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 217.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=217.0 vs prior_export=217.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 2.47 | **GOOD** |
| `B10_insider_transactions` | 1.75 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.75 vs prior=1.75 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | 7.91 | **GOOD** |
| `B13_short_float` | 9.44 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.98 (this export) | prior_export=0.98 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.28 (this export) | prior_export=0.28 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBC  ·  score **+14**  ·  Medical Instruments & Supplies
price=5.989999771118164  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.09 on 2026-10-12; prev RSI=55.18 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.2200 R=0.0550); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.807 (Gvol=1053433 Rvol=583035); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A07_rvol` | RVOL=1.019 on 2026-10-12: today_vol=1053433 / avg20=1033961 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.624 on 2026-10-12 (price=5.9900, mid=5.6322, upper=6.2055, lower=5.0590; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=5.9900 vs SMA50=5.2233 dist=+14.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=5.6322 SMA50=5.2233 SMA80=4.4932 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-14@3.0700; S2[2026-08-13→2026-09-11] low=2026-08-13@4.7200; S3[2026-09-14→2026-10-12] low=2026-09-14@4.9800 | lows=[3.069999933242798, 4.71999979019165, 4.980000019073486] span=62.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6470579160542584 wick_frac=0.35294208394574156 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.19642814568070271 wick_frac=0.8035718543192972 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.000008669793572 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.0500:wick=0.2950; 2026-10-07:GREEN:body=+0.0500:wick=0.2500; 2026-10-08:RED:body=-0.1400:wick=0.1100; 2026-10-09:RED:body=-0.0550:wick=0.2250; 2026-10-12:GREEN:body=+0.2200:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=110.68 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.75 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1018.7 | **NEUTRAL** |
| `B04_income` | 87.5 | **GOOD** |
| `B05_profit_margin` | 8.59 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=4.0 vs prior_export=4.0 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | -16.39 | **BAD** |
| `B13_short_float` | 10.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=110.68 (this export) | prior_export=110.68 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.75 (this export) | prior_export=6.75 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEHC  ·  score **+14**  ·  Medical Devices
price=79.55000305175781  pair=`2026-10-30→2026-11-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.80 on 2026-11-02; prev RSI=74.10 on 2026-10-30 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 74.10@2026-10-30 → 78.80@2026-11-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 74.10@2026-10-30 → 78.80@2026-11-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 74.10@2026-10-30 → 78.80@2026-11-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_body_sum/RED_body_sum=2.337 (G=2.0100 R=0.8600); 2026-10-30:RED:O=77.4300,C=76.5700,body=-0.8600,vol=3967429.0; 2026-11-02:GREEN:O=77.5400,C=79.5500,body=+2.0100,vol=4768671.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-30 + 2026-11-02; ratio=GREEN_vol/RED_vol=1.202 (Gvol=4768671 Rvol=3967429); 2026-10-30:RED:O=77.4300,C=76.5700,body=-0.8600,vol=3967429.0; 2026-11-02:GREEN:O=77.5400,C=79.5500,body=+2.0100,vol=4768671.0 | **GOOD** |
| `A07_rvol` | RVOL=1.243 on 2026-11-02: today_vol=4768671 / avg20=3835994 (avg window 2026-10-05→2026-10-30, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.094 on 2026-11-02 (price=79.5500, mid=70.3820, upper=78.7604, lower=62.0036; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-02: price=79.5500 vs SMA50=68.5469 dist=+16.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-02: SMA20=70.3820 SMA50=68.5469 SMA80=68.4521 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-05→2026-11-02 (63 bars); S1[2026-08-05→2026-09-02] low=2026-08-06@69.5850; S2[2026-09-03→2026-10-02] low=2026-09-17@62.9950; S3[2026-10-05→2026-11-02] low=2026-10-08@63.3150 | lows=[69.58499908447266, 62.994998931884766, 63.314998626708984] span=10.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: GREEN body_frac=0.6779104953837604 wick_frac=0.3220895046162397 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-30+2026-11-02: RED body_frac=0.4278605454441935 wick_frac=0.5721394545558065 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3372101275704833 need>1.4; red_wick_gt_green=True 5d trail=2026-10-27:RED:body=-0.6200:wick=1.0125; 2026-10-28:RED:body=-0.1800:wick=3.0600; 2026-10-29:GREEN:body=+0.2300:wick=1.7000; 2026-10-30:RED:body=-0.8600:wick=1.1500; 2026-11-02:GREEN:body=+2.0100:wick=0.9550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.08 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.59 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21267.0 | **NEUTRAL** |
| `B04_income` | 1985.0 | **GOOD** |
| `B05_profit_margin` | 9.33 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 82.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=82.78 vs prior_export=82.78 on finviz_2026-11-03) | **NEUTRAL** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | 7.47 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.47 vs prior=7.47 on finviz_2026-11-03) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.22 | **BAD** |
| `B13_short_float` | 3.76 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.08 (this export) | prior_export=9.08 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.59 (this export) | prior_export=0.59 (finviz_2026-11-03) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-04_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.