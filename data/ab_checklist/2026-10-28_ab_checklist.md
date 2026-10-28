# A+B1 Feature Checklist — 2026-10-28

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-28.csv` · prior export for Δ: `2026-10-27`
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
| 1 | MTDR | +17 | 17 | 0 | 2026-10-26→2026-10-27 | Oil & Gas E&P |
| 2 | PBR | +17 | 17 | 0 | 2026-10-26→2026-10-27 | Oil & Gas Integrated |
| 3 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 4 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 5 | SONO | +16 | 17 | 1 | 2026-10-26→2026-10-27 | Consumer Electronics |
| 6 | SWKS | +16 | 17 | 1 | 2026-10-26→2026-10-27 | Semiconductors |
| 7 | PANW | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Software - Infrastructure |
| 8 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 9 | SNPS | +15 | 16 | 1 | 2026-10-26→2026-10-27 | Software - Infrastructure |
| 10 | WAT | +14 | 16 | 2 | 2026-10-26→2026-10-27 | Diagnostics & Research |
| 11 | CVLT | +14 | 17 | 3 | 2026-10-26→2026-10-27 | Software - Application |
| 12 | CDNS | +14 | 15 | 1 | 2026-10-26→2026-10-27 | Software - Application |
| 13 | DVN | +14 | 16 | 2 | 2026-10-26→2026-10-27 | Oil & Gas E&P |
| 14 | CCEP | +14 | 15 | 1 | 2026-10-26→2026-10-27 | Beverages - Non-Alcoholic |
| 15 | CXT | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Business Equipment & Supplies |

## Full checklist — top 15

### MTDR  ·  score **+17**  ·  Oil & Gas E&P
price=59.27000045776367  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.37 on 2026-10-27; prev RSI=65.15 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.15@2026-10-26 → 61.37@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=2.870 (G=1.5500 R=0.5400); 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0; 2026-10-27:RED:O=59.8100,C=59.2700,body=-0.5400,vol=956924.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=2.115 (Gvol=2024300 Rvol=956924); 2026-10-26:GREEN:O=58.5400,C=60.0900,body=+1.5500,vol=2024300.0; 2026-10-27:RED:O=59.8100,C=59.2700,body=-0.5400,vol=956924.0 | **GOOD** |
| `A07_rvol` | RVOL=0.685 on 2026-10-27: today_vol=956924 / avg20=1397966 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.719 on 2026-10-27 (price=59.2700, mid=55.5860, upper=60.7116, lower=50.4604; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=59.2700 vs SMA50=56.5054 dist=+4.89% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=55.59_50=56.51_80=54.32 on 2026-10-27: SMA20=55.5860 SMA50=56.5054 SMA80=54.3241 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-05@46.5316; S2[2026-08-28→2026-09-28] low=2026-09-25@51.1000; S3[2026-09-29→2026-10-27] low=2026-09-29@50.3900 | lows=[46.531587687321455, 51.099998474121094, 50.38999938964844] span=9.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.6431532511194792 wick_frac=0.3568467488805208 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.2991700639729145 wick_frac=0.7008299360270854 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8703640910439536 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.2500:wick=0.8500; 2026-10-22:RED:body=-1.8300:wick=1.2050; 2026-10-23:GREEN:body=+2.7700:wick=0.8400; 2026-10-26:GREEN:body=+1.5500:wick=0.8600; 2026-10-27:RED:body=-0.5400:wick=1.2650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 70.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=70.59 vs prior_export=70.59 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.4 vs prior=0.4 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.68 | **GOOD** |
| `B13_short_float` | 11.81 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+17**  ·  Oil & Gas Integrated
price=22.299999237060547  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=41.62 on 2026-10-27; prev RSI=42.64 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.64@2026-10-26 → 41.62@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=2.633 (G=0.7900 R=0.3000); 2026-10-26:GREEN:O=21.7100,C=22.5000,body=+0.7900,vol=102623400.0; 2026-10-27:RED:O=22.6000,C=22.3000,body=-0.3000,vol=37103285.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=2.766 (Gvol=102623400 Rvol=37103285); 2026-10-26:GREEN:O=21.7100,C=22.5000,body=+0.7900,vol=102623400.0; 2026-10-27:RED:O=22.6000,C=22.3000,body=-0.3000,vol=37103285.0 | **GOOD** |
| `A07_rvol` | RVOL=1.313 on 2026-10-27: today_vol=37103285 / avg20=28267864 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.586 on 2026-10-27 (price=22.3000, mid=24.3540, upper=27.8600, lower=20.8480; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=22.3000 vs SMA50=21.7814 dist=+2.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=24.3540 SMA50=21.7814 SMA80=20.4206 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-27@17.5400; S2[2026-08-28→2026-09-28] low=2026-08-28@18.1800; S3[2026-09-29→2026-10-27] low=2026-09-29@20.2400 | lows=[17.540000915527344, 18.18000030517578, 20.239999771118164] span=15.39% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5642865219896921 wick_frac=0.43571347801030785 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.3773598938602573 wick_frac=0.6226401061397426 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.633326339748358 need>1.4; red_wick_gt_green=False 5d trail=2026-10-21:RED:body=-0.2300:wick=0.1500; 2026-10-22:RED:body=-0.3200:wick=0.5845; 2026-10-23:GREEN:body=+0.2000:wick=0.4000; 2026-10-26:GREEN:body=+0.7900:wick=0.6100; 2026-10-27:RED:body=-0.3000:wick=0.4950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+16**  ·  Consumer Electronics
price=17.8700008392334  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.28 on 2026-10-27; prev RSI=55.95 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.95@2026-10-26 → 59.28@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=5.600 (G=0.2800 R=0.0500); 2026-10-26:RED:O=17.6400,C=17.5900,body=-0.0500,vol=971100.0; 2026-10-27:GREEN:O=17.5900,C=17.8700,body=+0.2800,vol=1748740.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.801 (Gvol=1748740 Rvol=971100); 2026-10-26:RED:O=17.6400,C=17.5900,body=-0.0500,vol=971100.0; 2026-10-27:GREEN:O=17.5900,C=17.8700,body=+0.2800,vol=1748740.0 | **GOOD** |
| `A07_rvol` | RVOL=1.074 on 2026-10-27: today_vol=1748740 / avg20=1627843 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.602 on 2026-10-27 (price=17.8700, mid=17.5033, upper=18.1120, lower=16.8945; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=17.8700 vs SMA50=16.5612 dist=+7.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=17.5033 SMA50=16.5612 SMA80=16.0315 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-30@14.0200; S2[2026-08-28→2026-09-28] low=2026-09-09@14.2000; S3[2026-09-29→2026-10-27] low=2026-10-08@16.7000 | lows=[14.020000457763672, 14.199999809265137, 16.700000762939453] span=19.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5834581983664872 wick_frac=0.41654180163351284 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.09999847412109375 wick_frac=0.9000015258789062 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.600099183642328 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.0700:wick=0.2700; 2026-10-22:GREEN:body=+0.2350:wick=0.0950; 2026-10-23:GREEN:body=+0.0100:wick=0.2700; 2026-10-26:RED:body=-0.0500:wick=0.4500; 2026-10-27:GREEN:body=+0.2800:wick=0.1999 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.43 vs prior=-4.43 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=85.4800033569336  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.90 on 2026-10-27; prev RSI=55.60 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.60@2026-10-26 → 59.90@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=19.221 (G=1.7300 R=0.0900); 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0; 2026-10-27:GREEN:O=83.7500,C=85.4800,body=+1.7300,vol=2728589.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.002 (Gvol=2728589 Rvol=2724100); 2026-10-26:RED:O=83.4300,C=83.3400,body=-0.0900,vol=2724100.0; 2026-10-27:GREEN:O=83.7500,C=85.4800,body=+1.7300,vol=2728589.0 | **GOOD** |
| `A07_rvol` | RVOL=0.396 on 2026-10-27: today_vol=2728589 / avg20=6886518 (avg window 2026-09-29→2026-10-26, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.650 on 2026-10-27 (price=85.4800, mid=81.3120, upper=87.7265, lower=74.8975; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=85.4800 vs SMA50=79.2666 dist=+7.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=81.3120 SMA50=79.2666 SMA80=73.2195 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-29@57.5000; S2[2026-08-28→2026-09-28] low=2026-08-28@64.4700; S3[2026-09-29→2026-10-27] low=2026-10-13@73.7600 | lows=[57.5, 64.47000122070312, 73.76000213623047] span=28.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.7505436563496083 wick_frac=0.24945634365039174 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.040910809095606515 wick_frac=0.9590891909043935 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.221412223446638 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+0.4000:wick=1.4900; 2026-10-22:GREEN:body=+1.9400:wick=0.5700; 2026-10-23:GREEN:body=+1.4800:wick=1.2300; 2026-10-26:RED:body=-0.0900:wick=2.1100; 2026-10-27:GREEN:body=+1.7300:wick=0.5750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.9 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.95 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4014.1 | **NEUTRAL** |
| `B04_income` | 290.1 | **GOOD** |
| `B05_profit_margin` | 7.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.07 vs prior_export=68.07 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 3.08 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.4 | **GOOD** |
| `B13_short_float` | 21.83 | **GOOD** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.9 (this export) | prior_export=4.9 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.95 (this export) | prior_export=0.95 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PANW  ·  score **+15**  ·  Software - Infrastructure
price=401.70001220703125  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.40 on 2026-10-27; prev RSI=55.26 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.26@2026-10-26 → 58.40@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.9800 R=0.0000); 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0; 2026-10-27:GREEN:O=390.9900,C=401.7000,body=+10.7100,vol=4768697.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9897297 Rvol=0); 2026-10-26:GREEN:O=384.1900,C=390.4600,body=+6.2700,vol=5128600.0; 2026-10-27:GREEN:O=390.9900,C=401.7000,body=+10.7100,vol=4768697.0 | **GOOD** |
| `A07_rvol` | RVOL=0.904 on 2026-10-27: today_vol=4768697 / avg20=5272812 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.325 on 2026-10-27 (price=401.7000, mid=384.8300, upper=436.7802, lower=332.8798; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=401.7000 vs SMA50=370.9942 dist=+8.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=384.8300 SMA50=370.9942 SMA80=362.7036 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-07-30@313.0500; S2[2026-08-28→2026-09-28] low=2026-09-02@321.3900; S3[2026-09-29→2026-10-27] low=2026-10-20@334.7500 | lows=[313.04998779296875, 321.3900146484375, 334.75] span=6.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5301433984108884 wick_frac=0.4698566015891116 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+3.8100:wick=11.0800; 2026-10-22:RED:body=-0.0800:wick=13.9101; 2026-10-23:GREEN:body=+26.2500:wick=3.9599; 2026-10-26:GREEN:body=+6.2700:wick=8.9500; 2026-10-27:GREEN:body=+10.7100:wick=5.8094 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.35 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.74 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11480.0 | **NEUTRAL** |
| `B04_income` | 307.0 | **GOOD** |
| `B05_profit_margin` | 2.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 403.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=403.5 vs prior_export=403.5 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.66 | **GOOD** |
| `B10_insider_transactions` | -2.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.01 vs prior=-2.01 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.96 | **GOOD** |
| `B13_short_float` | 2.7 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.35 (this export) | prior_export=4.35 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.74 (this export) | prior_export=1.74 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SNPS  ·  score **+15**  ·  Software - Infrastructure
price=474.4200134277344  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.06 on 2026-10-27; prev RSI=51.19 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.19@2026-10-26 → 57.06@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=38.9300 R=0.0000); 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0; 2026-10-27:GREEN:O=458.6700,C=474.4200,body=+15.7500,vol=2153168.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4963368 Rvol=0); 2026-10-26:GREEN:O=431.1500,C=454.3300,body=+23.1800,vol=2810200.0; 2026-10-27:GREEN:O=458.6700,C=474.4200,body=+15.7500,vol=2153168.0 | **GOOD** |
| `A07_rvol` | RVOL=0.828 on 2026-10-27: today_vol=2153168 / avg20=2601318 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.032 on 2026-10-27 (price=474.4200, mid=472.6940, upper=527.2104, lower=418.1776; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=474.4200 vs SMA50=432.7542 dist=+9.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=472.6940 SMA50=432.7542 SMA80=422.2980 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-07-30@370.2000; S2[2026-08-28→2026-09-28] low=2026-09-15@362.5500; S3[2026-09-29→2026-10-27] low=2026-09-29@403.3300 | lows=[370.20001220703125, 362.54998779296875, 403.3299865722656] span=11.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.8419915003977608 wick_frac=0.15800849960223917 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:RED:body=-0.6100:wick=9.0900; 2026-10-22:RED:body=-19.0200:wick=2.9200; 2026-10-23:GREEN:body=+10.7200:wick=5.3800; 2026-10-26:GREEN:body=+23.1800:wick=3.3100; 2026-10-27:GREEN:body=+15.7500:wick=3.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.64 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9408.2 | **NEUTRAL** |
| `B04_income` | 1076.56 | **GOOD** |
| `B05_profit_margin` | 11.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 560.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=560.72 vs prior_export=560.72 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.3 | **GOOD** |
| `B10_insider_transactions` | -1.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.1 vs prior=-1.1 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.91 | **GOOD** |
| `B13_short_float` | 3.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.64 (this export) | prior_export=6.64 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+14**  ·  Diagnostics & Research
price=468.54998779296875  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.49 on 2026-10-27; prev RSI=63.97 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.97@2026-10-26 → 69.49@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.97@2026-10-26 → 69.49@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.97@2026-10-26 → 69.49@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=13.7200 R=0.0000); 2026-10-26:DOJI:O=455.9800,C=455.9800,body=+0.0000,vol=909100.0; 2026-10-27:GREEN:O=454.8300,C=468.5500,body=+13.7200,vol=797335.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=2.754 (Gvol=1251885 Rvol=454550); 2026-10-26:DOJI:O=455.9800,C=455.9800,body=+0.0000,vol=909100.0; 2026-10-27:GREEN:O=454.8300,C=468.5500,body=+13.7200,vol=797335.0 | **GOOD** |
| `A07_rvol` | RVOL=0.901 on 2026-10-27: today_vol=797335 / avg20=884949 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.177 on 2026-10-27 (price=468.5500, mid=440.6455, upper=464.3548, lower=416.9362; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=468.5500 vs SMA50=426.2274 dist=+9.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=440.6455 SMA50=426.2274 SMA80=411.2404 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-30@370.4000; S2[2026-08-28→2026-09-28] low=2026-09-10@392.9500; S3[2026-09-29→2026-10-27] low=2026-10-08@421.6100 | lows=[370.3999938964844, 392.95001220703125, 421.6099853515625] span=13.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.7413807603829125 wick_frac=0.2586192396170876 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+20.5700:wick=3.9800; 2026-10-22:RED:body=-3.8900:wick=11.9625; 2026-10-23:RED:body=-4.4900:wick=4.4200; 2026-10-26:DOJI:body=+0.0000:wick=11.7000; 2026-10-27:GREEN:body=+13.7200:wick=4.7860 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4644.25 | **NEUTRAL** |
| `B04_income` | 166.14 | **GOOD** |
| `B05_profit_margin` | 3.58 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 448.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=448.96 vs prior_export=448.96 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -2.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.57 vs prior=-2.57 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.29 | **GOOD** |
| `B13_short_float` | 3.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVLT  ·  score **+14**  ·  Software - Application
price=172.5500030517578  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.54 on 2026-10-27; prev RSI=64.22 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.22@2026-10-26 → 67.54@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.22@2026-10-26 → 67.54@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.22@2026-10-26 → 67.54@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=8.291 (G=18.8200 R=2.2700); 2026-10-26:RED:O=170.0000,C=167.7300,body=-2.2700,vol=1842300.0; 2026-10-27:GREEN:O=153.7300,C=172.5500,body=+18.8200,vol=1289979.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=0.700 (Gvol=1289979 Rvol=1842300); 2026-10-26:RED:O=170.0000,C=167.7300,body=-2.2700,vol=1842300.0; 2026-10-27:GREEN:O=153.7300,C=172.5500,body=+18.8200,vol=1289979.0 | **BAD** |
| `A07_rvol` | RVOL=1.906 on 2026-10-27: today_vol=1289979 / avg20=676883 (avg window 2026-09-29→2026-10-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.044 on 2026-10-27 (price=172.5500, mid=157.0712, upper=171.8978, lower=142.2447; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-27: price=172.5500 vs SMA50=146.2477 dist=+17.98% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=157.0712 SMA50=146.2477 SMA80=144.5173 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-07-30@115.3100; S2[2026-08-28→2026-09-28] low=2026-09-02@125.9000; S3[2026-09-29→2026-10-27] low=2026-09-29@143.9600 | lows=[115.30999755859375, 125.9000015258789, 143.9600067138672] span=24.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.9433589074246449 wick_frac=0.0566410925753551 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.30026521237173326 wick_frac=0.6997347876282667 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.290736520868203 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:GREEN:body=+5.8050:wick=2.1950; 2026-10-22:GREEN:body=+3.0500:wick=2.8825; 2026-10-23:GREEN:body=+1.5600:wick=5.1949; 2026-10-26:RED:body=-2.2700:wick=5.2900; 2026-10-27:GREEN:body=+18.8200:wick=1.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=22.16 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.19 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1215.84 | **NEUTRAL** |
| `B04_income` | 68.3 | **GOOD** |
| `B05_profit_margin` | 5.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 160.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=160.75 vs prior_export=160.75 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -15.45 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.45 vs prior=-15.45 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.71 | **GOOD** |
| `B13_short_float` | 8.25 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.16 (this export) | prior_export=22.16 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.19 (this export) | prior_export=1.19 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CDNS  ·  score **+14**  ·  Software - Application
price=343.0799865722656  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.05 on 2026-10-27; prev RSI=48.06 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.06@2026-10-26 → 58.05@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.06@2026-10-26 → 58.05@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.06@2026-10-26 → 58.05@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=22.3100 R=0.0000); 2026-10-26:GREEN:O=316.9800,C=322.1700,body=+5.1900,vol=4501000.0; 2026-10-27:GREEN:O=325.9600,C=343.0800,body=+17.1200,vol=4627993.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9128993 Rvol=0); 2026-10-26:GREEN:O=316.9800,C=322.1700,body=+5.1900,vol=4501000.0; 2026-10-27:GREEN:O=325.9600,C=343.0800,body=+17.1200,vol=4627993.0 | **GOOD** |
| `A07_rvol` | RVOL=2.017 on 2026-10-27: today_vol=4627993 / avg20=2294649 (avg window 2026-09-29→2026-10-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.224 on 2026-10-27 (price=343.0800, mid=336.1130, upper=367.1806, lower=305.0454; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=343.0800 vs SMA50=319.1686 dist=+7.49% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=336.11_50=319.17_80=328.72 on 2026-10-27: SMA20=336.1130 SMA50=319.1686 SMA80=328.7249 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-24@311.4300; S2[2026-08-28→2026-09-28] low=2026-09-15@273.0700; S3[2026-09-29→2026-10-27] low=2026-10-23@305.1854 | lows=[311.42999267578125, 273.07000732421875, 305.1853942871094] span=14.05% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.5002387391266938 wick_frac=0.4997612608733062 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-21:GREEN:body=+1.5700:wick=7.0850; 2026-10-22:RED:body=-8.7700:wick=4.3050; 2026-10-23:GREEN:body=+8.9200:wick=4.0446; 2026-10-26:GREEN:body=+5.1900:wick=11.1100; 2026-10-27:GREEN:body=+17.1200:wick=7.9800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.75 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.48 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5837.62 | **NEUTRAL** |
| `B04_income` | 1377.99 | **GOOD** |
| `B05_profit_margin` | 23.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 410.48 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=410.48 vs prior_export=410.48 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.39 | **GOOD** |
| `B10_insider_transactions` | -15.09 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.09 vs prior=-15.09 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 1.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.75 (this export) | prior_export=2.75 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.48 (this export) | prior_export=0.48 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DVN  ·  score **+14**  ·  Oil & Gas E&P
price=51.22999954223633  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.89 on 2026-10-27; prev RSI=62.40 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.40@2026-10-26 → 59.89@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.40@2026-10-26 → 59.89@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.40@2026-10-26 → 59.89@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=1.471 (G=0.2500 R=0.1700); 2026-10-26:GREEN:O=51.3900,C=51.6400,body=+0.2500,vol=8413400.0; 2026-10-27:RED:O=51.4000,C=51.2300,body=-0.1700,vol=7836296.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.074 (Gvol=8413400 Rvol=7836296); 2026-10-26:GREEN:O=51.3900,C=51.6400,body=+0.2500,vol=8413400.0; 2026-10-27:RED:O=51.4000,C=51.2300,body=-0.1700,vol=7836296.0 | **GOOD** |
| `A07_rvol` | RVOL=0.914 on 2026-10-27: today_vol=7836296 / avg20=8574060 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.630 on 2026-10-27 (price=51.2300, mid=49.2790, upper=52.3780, lower=46.1800; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=51.2300 vs SMA50=48.7236 dist=+5.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-27: SMA20=49.2790 SMA50=48.7236 SMA80=46.9496 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-30→2026-10-27 (63 bars); S1[2026-07-30→2026-08-27] low=2026-08-05@42.0400; S2[2026-08-28→2026-09-28] low=2026-09-22@46.4900; S3[2026-09-29→2026-10-27] low=2026-09-29@45.5600 | lows=[42.040000915527344, 46.4900016784668, 45.560001373291016] span=10.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.2427178353314149 wick_frac=0.7572821646685851 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.1465534534524659 wick_frac=0.853446546547534 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.470571075956468 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:RED:body=-0.1300:wick=1.1250; 2026-10-22:RED:body=-1.2400:wick=0.8700; 2026-10-23:GREEN:body=+1.7900:wick=0.6500; 2026-10-26:GREEN:body=+0.2500:wick=0.7800; 2026-10-27:RED:body=-0.1700:wick=0.9900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.01 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.4 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 19817.0 | **NEUTRAL** |
| `B04_income` | 3280.0 | **GOOD** |
| `B05_profit_margin` | 16.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=60.86 vs prior_export=60.86 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.34 | **GOOD** |
| `B10_insider_transactions` | -0.65 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.65 vs prior=-0.65 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 74.58 | **GOOD** |
| `B13_short_float` | 2.92 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.01 (this export) | prior_export=12.01 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.4 (this export) | prior_export=23.4 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCEP  ·  score **+14**  ·  Beverages - Non-Alcoholic
price=110.88999938964844  pair=`2026-10-26→2026-10-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.74 on 2026-10-27; prev RSI=61.44 on 2026-10-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.44@2026-10-26 → 57.74@2026-10-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.44@2026-10-26 → 57.74@2026-10-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.44@2026-10-26 → 57.74@2026-10-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_body_sum/RED_body_sum=4.069 (G=1.1800 R=0.2900); 2026-10-26:GREEN:O=111.1100,C=112.2900,body=+1.1800,vol=1322000.0; 2026-10-27:RED:O=111.1800,C=110.8900,body=-0.2900,vol=1190458.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-26 + 2026-10-27; ratio=GREEN_vol/RED_vol=1.110 (Gvol=1322000 Rvol=1190458); 2026-10-26:GREEN:O=111.1100,C=112.2900,body=+1.1800,vol=1322000.0; 2026-10-27:RED:O=111.1800,C=110.8900,body=-0.2900,vol=1190458.0 | **GOOD** |
| `A07_rvol` | RVOL=0.699 on 2026-10-27: today_vol=1190458 / avg20=1703980 (avg window 2026-09-29→2026-10-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.406 on 2026-10-27 (price=110.8900, mid=106.9580, upper=116.6465, lower=97.2695; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-27: price=110.8900 vs SMA50=105.7658 dist=+4.84% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=106.96_50=105.77_80=106.20 on 2026-10-27: SMA20=106.9580 SMA50=105.7658 SMA80=106.1954 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-29→2026-10-27 (63 bars); S1[2026-07-29→2026-08-27] low=2026-08-04@102.5900; S2[2026-08-28→2026-09-28] low=2026-09-23@99.0000; S3[2026-09-29→2026-10-27] low=2026-10-05@100.0600 | lows=[102.58999633789062, 99.0, 100.05999755859375] span=3.63% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: GREEN body_frac=0.7283940151739923 wick_frac=0.27160598482600773 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-26+2026-10-27: RED body_frac=0.23387067003014828 wick_frac=0.7661293299698517 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.068953723922022 need>1.4; red_wick_gt_green=True 5d trail=2026-10-21:RED:body=-2.2800:wick=0.5300; 2026-10-22:GREEN:body=+2.9700:wick=0.4075; 2026-10-23:RED:body=-2.7300:wick=0.6600; 2026-10-26:GREEN:body=+1.1800:wick=0.4400; 2026-10-27:RED:body=-0.2900:wick=0.9500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.67 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.0 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 24903.24 | **NEUTRAL** |
| `B04_income` | 2328.07 | **GOOD** |
| `B05_profit_margin` | 9.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.21 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=109.21 vs prior_export=109.21 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.32 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | -7.1 | **BAD** |
| `B13_short_float` | 5.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.67 (this export) | prior_export=2.67 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.0 (this export) | prior_export=1.0 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B08_target_price_delta` | delta=0.0 (now=68.71 vs prior_export=68.71 on finviz_2026-10-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-10-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.45 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.25 (this export) | prior_export=6.25 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.7 (this export) | prior_export=3.7 (finviz_2026-10-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-28_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.