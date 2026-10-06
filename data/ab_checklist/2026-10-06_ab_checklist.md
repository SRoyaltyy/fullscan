# A+B1 Feature Checklist — 2026-10-06

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-06.csv` · prior export for Δ: `2026-10-05`
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
| 1 | SONO | +17 | 18 | 1 | 2026-10-02→2026-10-05 | Consumer Electronics |
| 2 | LPG | +16 | 17 | 1 | 2026-10-02→2026-10-05 | Oil & Gas Midstream |
| 3 | HUM | +16 | 16 | 0 | 2026-10-02→2026-10-05 | Healthcare Plans |
| 4 | KNTK | +16 | 17 | 1 | 2026-10-02→2026-10-05 | Oil & Gas Midstream |
| 5 | CVX | +16 | 18 | 2 | 2026-10-02→2026-10-05 | Oil & Gas Integrated |
| 6 | WAT | +15 | 17 | 2 | 2026-10-02→2026-10-05 | Diagnostics & Research |
| 7 | WAY | +15 | 17 | 2 | 2026-10-02→2026-10-05 | Health Information Services |
| 8 | VLTO | +15 | 17 | 2 | 2026-10-02→2026-10-05 | Pollution & Treatment Controls |
| 9 | ANF | +15 | 16 | 1 | 2026-10-02→2026-10-05 | Apparel Retail |
| 10 | GDDY | +15 | 17 | 2 | 2026-10-02→2026-10-05 | Software - Infrastructure |
| 11 | ET | +15 | 17 | 2 | 2026-10-02→2026-10-05 | Oil & Gas Midstream |
| 12 | ESI | +15 | 16 | 1 | 2026-10-02→2026-10-05 | Specialty Chemicals |
| 13 | ELV | +15 | 16 | 1 | 2026-10-02→2026-10-05 | Healthcare Plans |
| 14 | EC | +15 | 15 | 0 | 2026-10-02→2026-10-05 | Oil & Gas Integrated |
| 15 | DE | +15 | 16 | 1 | 2026-10-02→2026-10-05 | Farm & Heavy Construction Machinery |

## Full checklist — top 15

### SONO  ·  score **+17**  ·  Consumer Electronics
price=17.940000534057617  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.74 on 2026-10-05; prev RSI=62.81 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.81@2026-10-02 → 64.74@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.81@2026-10-02 → 64.74@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.81@2026-10-02 → 64.74@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=2.333 (G=0.4200 R=0.1800); 2026-10-02:RED:O=17.8800,C=17.7000,body=-0.1800,vol=2525664.0; 2026-10-05:GREEN:O=17.5200,C=17.9400,body=+0.4200,vol=2683993.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.063 (Gvol=2683993 Rvol=2525664); 2026-10-02:RED:O=17.8800,C=17.7000,body=-0.1800,vol=2525664.0; 2026-10-05:GREEN:O=17.5200,C=17.9400,body=+0.4200,vol=2683993.0 | **GOOD** |
| `A07_rvol` | RVOL=1.258 on 2026-10-05: today_vol=2683993 / avg20=2132824 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.535 on 2026-10-05 (price=17.9400, mid=16.5328, upper=19.1607, lower=13.9048; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=17.9400 vs SMA50=16.0437 dist=+11.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=16.5328 SMA50=16.0437 SMA80=15.4211 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-05 (63 bars); S1[2026-07-06→2026-08-05] low=2026-07-06@13.5400; S2[2026-08-06→2026-09-03] low=2026-08-12@14.9000; S3[2026-09-04→2026-10-05] low=2026-09-09@14.2000 | lows=[13.539999961853027, 14.899999618530273, 14.199999809265137] span=10.04% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.6131392755410888 wick_frac=0.38686072445891123 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.25531690398054235 wick_frac=0.7446830960194577 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3333545262845576 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.0900:wick=0.3950; 2026-09-30:GREEN:body=+0.0800:wick=0.4500; 2026-10-01:RED:body=-0.3400:wick=0.3300; 2026-10-02:RED:body=-0.1800:wick=0.5250; 2026-10-05:GREEN:body=+0.4200:wick=0.2650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.43 | **BAD** |
| `B11_insider_tx_delta` | delta=0.020000000000000462 (now=-4.43 vs prior=-4.45 on finviz_2026-10-05) | **GOOD** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 8.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LPG  ·  score **+16**  ·  Oil & Gas Midstream
price=57.63999938964844  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.40 on 2026-10-05; prev RSI=64.20 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.20@2026-10-02 → 65.40@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.20@2026-10-02 → 65.40@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.20@2026-10-02 → 65.40@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=4.200 (G=1.4700 R=0.3500); 2026-10-02:GREEN:O=55.7400,C=57.2100,body=+1.4700,vol=789598.0; 2026-10-05:RED:O=57.9900,C=57.6400,body=-0.3500,vol=649423.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.216 (Gvol=789598 Rvol=649423); 2026-10-02:GREEN:O=55.7400,C=57.2100,body=+1.4700,vol=789598.0; 2026-10-05:RED:O=57.9900,C=57.6400,body=-0.3500,vol=649423.0 | **GOOD** |
| `A07_rvol` | RVOL=1.016 on 2026-10-05: today_vol=649423 / avg20=639150 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.615 on 2026-10-05 (price=57.6400, mid=55.4570, upper=59.0063, lower=51.9077; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=57.6400 vs SMA50=50.8642 dist=+13.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=55.4570 SMA50=50.8642 SMA80=46.5884 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-05 (63 bars); S1[2026-07-06→2026-08-05] low=2026-07-06@35.4501; S2[2026-08-06→2026-09-03] low=2026-08-11@42.1200; S3[2026-09-04→2026-10-05] low=2026-09-23@52.0900 | lows=[35.45013037924837, 42.119998931884766, 52.09000015258789] span=46.94% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.5833321980069724 wick_frac=0.4166678019930276 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.1107602233288064 wick_frac=0.8892397766711936 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.199965122995935 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.6900:wick=1.1150; 2026-09-30:GREEN:body=+0.1200:wick=1.5000; 2026-10-01:GREEN:body=+2.2000:wick=0.4500; 2026-10-02:GREEN:body=+1.4700:wick=1.0500; 2026-10-05:RED:body=-0.3500:wick=2.8100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.15 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=16.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 585.18 | **NEUTRAL** |
| `B04_income` | 321.87 | **GOOD** |
| `B05_profit_margin` | 55.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 54.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=54.12 vs prior_export=54.12 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -3.23 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.23 vs prior=-3.23 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.81 | **GOOD** |
| `B13_short_float` | 5.76 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.15 (this export) | prior_export=19.15 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=16.86 (this export) | prior_export=16.86 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+16**  ·  Healthcare Plans
price=404.07000732421875  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.00 on 2026-10-05; prev RSI=50.67 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.67@2026-10-02 → 59.00@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.67@2026-10-02 → 59.00@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.67@2026-10-02 → 59.00@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=23.9100 R=0.0000); 2026-10-02:GREEN:O=378.5400,C=388.3600,body=+9.8200,vol=755960.0; 2026-10-05:GREEN:O=389.9800,C=404.0700,body=+14.0900,vol=1517892.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2273852 Rvol=0); 2026-10-02:GREEN:O=378.5400,C=388.3600,body=+9.8200,vol=755960.0; 2026-10-05:GREEN:O=389.9800,C=404.0700,body=+14.0900,vol=1517892.0 | **GOOD** |
| `A07_rvol` | RVOL=1.464 on 2026-10-05: today_vol=1517892 / avg20=1037058 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.610 on 2026-10-05 (price=404.0700, mid=390.3265, upper=412.8667, lower=367.7862; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=404.0700 vs SMA50=386.1700 dist=+4.64% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=390.3265 SMA50=386.1700 SMA80=385.1040 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-08-05@353.6900; S2[2026-08-06→2026-09-03] low=2026-08-06@355.7100; S3[2026-09-04→2026-10-05] low=2026-09-23@369.1300 | lows=[353.69000244140625, 355.7099914550781, 369.1300048828125] span=4.37% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.7229633083113995 wick_frac=0.27703669168860046 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.0900:wick=9.4250; 2026-09-30:RED:body=-3.9700:wick=8.8600; 2026-10-01:RED:body=-0.3200:wick=10.3100; 2026-10-02:GREEN:body=+9.8200:wick=1.6700; 2026-10-05:GREEN:body=+14.0900:wick=9.7400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 425.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=425.46 vs prior_export=425.46 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 2.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KNTK  ·  score **+16**  ·  Oil & Gas Midstream
price=53.08000183105469  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.87 on 2026-10-05; prev RSI=49.60 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.60@2026-10-02 → 52.87@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.60@2026-10-02 → 52.87@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.60@2026-10-02 → 52.87@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0000 R=0.0000); 2026-10-02:GREEN:O=50.9400,C=52.5300,body=+1.5900,vol=1745020.0; 2026-10-05:GREEN:O=52.6700,C=53.0800,body=+0.4100,vol=1083204.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2828224 Rvol=0); 2026-10-02:GREEN:O=50.9400,C=52.5300,body=+1.5900,vol=1745020.0; 2026-10-05:GREEN:O=52.6700,C=53.0800,body=+0.4100,vol=1083204.0 | **GOOD** |
| `A07_rvol` | RVOL=0.780 on 2026-10-05: today_vol=1083204 / avg20=1388420 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.023 on 2026-10-05 (price=53.0800, mid=53.0120, upper=55.9140, lower=50.1100; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=53.0800 vs SMA50=52.6018 dist=+0.91% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=53.0120 SMA50=52.6018 SMA80=50.6716 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-02@46.5197; S2[2026-08-06→2026-09-03] low=2026-08-07@48.8000; S3[2026-09-04→2026-10-05] low=2026-10-01@49.2000 | lows=[46.51970327639811, 48.79999923706055, 49.20000076293945] span=5.76% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.5422807269337758 wick_frac=0.4577192730662242 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-29:GREEN:body=+0.6200:wick=0.8950; 2026-09-30:RED:body=-1.1400:wick=0.4700; 2026-10-01:GREEN:body=+1.0100:wick=1.3600; 2026-10-02:GREEN:body=+1.5900:wick=0.3050; 2026-10-05:GREEN:body=+0.4100:wick=1.2600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=148.54 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=30.44 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1826.32 | **NEUTRAL** |
| `B04_income` | 185.44 | **GOOD** |
| `B05_profit_margin` | 10.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 57.53 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=57.53 vs prior_export=57.53 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.11000000000000032 (now=-6.77 vs prior=-6.88 on finviz_2026-10-05) | **GOOD** |
| `B12_institutional_transactions` | 1.05 | **GOOD** |
| `B13_short_float` | 9.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=148.54 (this export) | prior_export=148.54 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=30.44 (this export) | prior_export=30.44 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CVX  ·  score **+16**  ·  Oil & Gas Integrated
price=206.47000122070312  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.27 on 2026-10-05; prev RSI=51.69 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.69@2026-10-02 → 51.27@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.69@2026-10-02 → 51.27@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.69@2026-10-02 → 51.27@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=5.250 (G=1.8900 R=0.3600); 2026-10-02:GREEN:O=204.8000,C=206.6900,body=+1.8900,vol=6641824.0; 2026-10-05:RED:O=206.8300,C=206.4700,body=-0.3600,vol=6403501.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.037 (Gvol=6641824 Rvol=6403501); 2026-10-02:GREEN:O=204.8000,C=206.6900,body=+1.8900,vol=6641824.0; 2026-10-05:RED:O=206.8300,C=206.4700,body=-0.3600,vol=6403501.0 | **GOOD** |
| `A07_rvol` | RVOL=0.653 on 2026-10-05: today_vol=6403501 / avg20=9808501 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.239 on 2026-10-05 (price=206.4700, mid=208.4950, upper=216.9702, lower=200.0198; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=206.4700 vs SMA50=202.7109 dist=+1.85% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=208.4950 SMA50=202.7109 SMA80=193.7940 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-05 (63 bars); S1[2026-07-08→2026-08-05] low=2026-07-10@173.5100; S2[2026-08-06→2026-09-03] low=2026-08-07@185.8700; S3[2026-09-04→2026-10-05] low=2026-09-22@200.7800 | lows=[173.50999450683594, 185.8699951171875, 200.77999877929688] span=15.72% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.6631562603732774 wick_frac=0.33684373962672265 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.06831145367051371 wick_frac=0.9316885463294863 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.249989403636672 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+1.3300:wick=1.4400; 2026-09-30:RED:body=-0.8800:wick=1.7400; 2026-10-01:GREEN:body=+3.7700:wick=0.8200; 2026-10-02:GREEN:body=+1.8900:wick=0.9600; 2026-10-05:RED:body=-0.3600:wick=4.9100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.18 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.69 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 211454.0 | **NEUTRAL** |
| `B04_income` | 20591.0 | **GOOD** |
| `B05_profit_margin` | 9.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 224.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.12999999999999545 (now=224.75 vs prior_export=224.62 on finviz_2026-10-05) | **GOOD** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -20.49 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.36999999999999744 (now=-20.49 vs prior=-20.12 on finviz_2026-10-05) | **BAD** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 1.05 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.18 (this export) | prior_export=9.18 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.69 (this export) | prior_export=11.69 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+15**  ·  Diagnostics & Research
price=440.1199951171875  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.57 on 2026-10-05; prev RSI=53.14 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.14@2026-10-02 → 61.57@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.14@2026-10-02 → 61.57@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.14@2026-10-02 → 61.57@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=9.242 (G=14.5100 R=1.5700); 2026-10-02:RED:O=426.7800,C=425.2100,body=-1.5700,vol=589699.0; 2026-10-05:GREEN:O=425.6100,C=440.1200,body=+14.5100,vol=1007404.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.708 (Gvol=1007404 Rvol=589699); 2026-10-02:RED:O=426.7800,C=425.2100,body=-1.5700,vol=589699.0; 2026-10-05:GREEN:O=425.6100,C=440.1200,body=+14.5100,vol=1007404.0 | **GOOD** |
| `A07_rvol` | RVOL=1.428 on 2026-10-05: today_vol=1007404 / avg20=705695 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.646 on 2026-10-05 (price=440.1200, mid=423.2655, upper=449.3571, lower=397.1739; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=440.1200 vs SMA50=411.4398 dist=+6.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=423.2655 SMA50=411.4398 SMA80=395.5011 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-20@357.0400; S2[2026-08-06→2026-09-03] low=2026-08-06@388.7400; S3[2026-09-04→2026-10-05] low=2026-09-10@392.9500 | lows=[357.0400085449219, 388.739990234375, 392.95001220703125] span=10.06% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.8174653389084507 wick_frac=0.18253466109154928 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.15858669617728566 wick_frac=0.8414133038227143 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.242001321774287 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+1.8000:wick=4.0650; 2026-09-30:RED:body=-3.8700:wick=5.1500; 2026-10-01:RED:body=-11.6800:wick=1.6500; 2026-10-02:RED:body=-1.5700:wick=8.3300; 2026-10-05:GREEN:body=+14.5100:wick=3.2400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.32 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.4 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4644.25 | **NEUTRAL** |
| `B04_income` | 166.14 | **GOOD** |
| `B05_profit_margin` | 3.58 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 448.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-7.220000000000027 (now=448.96 vs prior_export=456.18 on finviz_2026-10-05) | **BAD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -2.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.57 vs prior=-2.57 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.29 | **GOOD** |
| `B13_short_float` | 3.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.32 (this export) | prior_export=1.32 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.4 (this export) | prior_export=1.4 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAY  ·  score **+15**  ·  Health Information Services
price=26.479999542236328  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.86 on 2026-10-05; prev RSI=48.32 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.32@2026-10-02 → 58.86@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.32@2026-10-02 → 58.86@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.32@2026-10-02 → 58.86@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=3.152 (G=1.4500 R=0.4600); 2026-10-02:RED:O=25.2700,C=24.8100,body=-0.4600,vol=1688455.0; 2026-10-05:GREEN:O=25.0300,C=26.4800,body=+1.4500,vol=3533612.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=2.093 (Gvol=3533612 Rvol=1688455); 2026-10-02:RED:O=25.2700,C=24.8100,body=-0.4600,vol=1688455.0; 2026-10-05:GREEN:O=25.0300,C=26.4800,body=+1.4500,vol=3533612.0 | **GOOD** |
| `A07_rvol` | RVOL=1.736 on 2026-10-05: today_vol=3533612 / avg20=2035774 (avg window 2026-09-04→2026-10-02, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.648 on 2026-10-05 (price=26.4800, mid=25.1595, upper=27.1966, lower=23.1224; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=26.4800 vs SMA50=24.7301 dist=+7.08% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=25.1595 SMA50=24.7301 SMA80=23.2044 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-30@20.1410; S2[2026-08-06→2026-09-03] low=2026-08-06@23.0200; S3[2026-09-04→2026-10-05] low=2026-09-10@22.9550 | lows=[20.141000747680664, 23.020000457763672, 22.954999923706055] span=14.29% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.7249994277954102 wick_frac=0.27500057220458984 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.48936247922715903 wick_frac=0.510637520772841 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.1521646287105107 need>1.4; red_wick_gt_green=False 5d trail=2026-09-29:RED:body=-0.3000:wick=0.2200; 2026-09-30:GREEN:body=+0.3100:wick=0.7450; 2026-10-01:RED:body=-0.5200:wick=1.0500; 2026-10-02:RED:body=-0.4600:wick=0.4800; 2026-10-05:GREEN:body=+1.4500:wick=0.5500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.04 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.1 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1205.74 | **NEUTRAL** |
| `B04_income` | 134.79 | **GOOD** |
| `B05_profit_margin` | 11.18 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 33.68 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.09000000000000341 (now=33.68 vs prior_export=33.77 on finviz_2026-10-05) | **BAD** |
| `B09_analyst_recom` | 1.21 | **GOOD** |
| `B10_insider_transactions` | -0.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.41 vs prior=-0.41 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.76 | **GOOD** |
| `B13_short_float` | 11.27 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VLTO  ·  score **+15**  ·  Pollution & Treatment Controls
price=96.56999969482422  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.29 on 2026-10-05; prev RSI=44.59 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.59@2026-10-02 → 53.29@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.59@2026-10-02 → 53.29@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.59@2026-10-02 → 53.29@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=116.019 (G=2.3200 R=0.0200); 2026-10-02:RED:O=94.7000,C=94.6800,body=-0.0200,vol=1536264.0; 2026-10-05:GREEN:O=94.2500,C=96.5700,body=+2.3200,vol=2269733.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.477 (Gvol=2269733 Rvol=1536264); 2026-10-02:RED:O=94.7000,C=94.6800,body=-0.0200,vol=1536264.0; 2026-10-05:GREEN:O=94.2500,C=96.5700,body=+2.3200,vol=2269733.0 | **GOOD** |
| `A07_rvol` | RVOL=1.308 on 2026-10-05: today_vol=2269733 / avg20=1735013 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.598 on 2026-10-05 (price=96.5700, mid=95.4140, upper=97.3472, lower=93.4808; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-05: price=96.5700 vs SMA50=96.6056 dist=-0.04% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=95.41_50=96.61_80=93.55 on 2026-10-05: SMA20=95.4140 SMA50=96.6056 SMA80=93.5455 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-23@89.9000; S2[2026-08-06→2026-09-03] low=2026-08-12@95.7600; S3[2026-09-04→2026-10-05] low=2026-09-10@92.9200 | lows=[89.9000015258789, 95.76000213623047, 92.91999816894531] span=6.52% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.7918086870933051 wick_frac=0.20819131290669485 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.019798614625744997 wick_frac=0.980201385374255 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=116.01945822205265 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.7800:wick=0.6300; 2026-09-30:RED:body=-1.6600:wick=0.5600; 2026-10-01:RED:body=-0.0400:wick=1.4850; 2026-10-02:RED:body=-0.0200:wick=0.9900; 2026-10-05:GREEN:body=+2.3200:wick=0.6100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.27 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.31 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5696.0 | **NEUTRAL** |
| `B04_income` | 988.0 | **GOOD** |
| `B05_profit_margin` | 17.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 114.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.06999999999999318 (now=114.57 vs prior_export=114.5 on finviz_2026-10-05) | **GOOD** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -2.81 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.81 vs prior=-2.81 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.27 | **GOOD** |
| `B13_short_float` | 2.73 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.27 (this export) | prior_export=10.27 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.31 (this export) | prior_export=1.31 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANF  ·  score **+15**  ·  Apparel Retail
price=141.39999389648438  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.64 on 2026-10-05; prev RSI=53.46 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.46@2026-10-02 → 59.64@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.46@2026-10-02 → 59.64@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.46@2026-10-02 → 59.64@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=7.638 (G=4.4300 R=0.5800); 2026-10-02:RED:O=136.4900,C=135.9100,body=-0.5800,vol=1053618.0; 2026-10-05:GREEN:O=136.9700,C=141.4000,body=+4.4300,vol=1468838.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.394 (Gvol=1468838 Rvol=1053618); 2026-10-02:RED:O=136.4900,C=135.9100,body=-0.5800,vol=1053618.0; 2026-10-05:GREEN:O=136.9700,C=141.4000,body=+4.4300,vol=1468838.0 | **GOOD** |
| `A07_rvol` | RVOL=1.118 on 2026-10-05: today_vol=1468838 / avg20=1313586 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.246 on 2026-10-05 (price=141.4000, mid=138.6535, upper=149.8062, lower=127.5007; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=141.4000 vs SMA50=125.8832 dist=+12.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=138.6535 SMA50=125.8832 SMA80=112.3457 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-08@85.0400; S2[2026-08-06→2026-09-03] low=2026-08-18@100.4300; S3[2026-09-04→2026-10-05] low=2026-09-23@130.8100 | lows=[85.04000091552734, 100.43000030517578, 130.80999755859375] span=53.82% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.815835396404776 wick_frac=0.184164603595224 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.15782349643961885 wick_frac=0.8421765035603812 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.63789429375707 need>1.4; red_wick_gt_green=False 5d trail=2026-09-29:GREEN:body=+0.4600:wick=3.2200; 2026-09-30:GREEN:body=+0.4700:wick=4.3000; 2026-10-01:GREEN:body=+1.7200:wick=3.6700; 2026-10-02:RED:body=-0.5800:wick=3.0950; 2026-10-05:GREEN:body=+4.4300:wick=1.0000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=109.68 (current export asof; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.5 (current export; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5340.93 | **NEUTRAL** |
| `B04_income` | 535.98 | **GOOD** |
| `B05_profit_margin` | 10.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 163.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=163.55 vs prior_export=163.55 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | -8.32 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.32 vs prior=-8.32 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.62 | **GOOD** |
| `B13_short_float` | 12.69 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=109.68 (this export) | prior_export=109.68 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.5 (this export) | prior_export=1.5 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GDDY  ·  score **+15**  ·  Software - Infrastructure
price=99.73999786376953  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.34 on 2026-10-05; prev RSI=49.87 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.87@2026-10-02 → 53.34@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.87@2026-10-02 → 53.34@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.87@2026-10-02 → 53.34@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.0700 R=0.0000); 2026-10-02:GREEN:O=96.8800,C=97.2100,body=+0.3300,vol=1715926.0; 2026-10-05:GREEN:O=97.0000,C=99.7400,body=+2.7400,vol=1556638.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3272564 Rvol=0); 2026-10-02:GREEN:O=96.8800,C=97.2100,body=+0.3300,vol=1715926.0; 2026-10-05:GREEN:O=97.0000,C=99.7400,body=+2.7400,vol=1556638.0 | **GOOD** |
| `A07_rvol` | RVOL=0.578 on 2026-10-05: today_vol=1556638 / avg20=2693760 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.283 on 2026-10-05 (price=99.7400, mid=97.8510, upper=104.5366, lower=91.1654; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=99.7400 vs SMA50=96.7950 dist=+3.04% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=97.8510 SMA50=96.7950 SMA80=92.2792 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-06→2026-10-05 (63 bars); S1[2026-07-06→2026-08-05] low=2026-07-31@73.1000; S2[2026-08-06→2026-09-03] low=2026-08-06@87.6750; S3[2026-09-04→2026-10-05] low=2026-09-09@91.6000 | lows=[73.0999984741211, 87.67500305175781, 91.5999984741211] span=25.31% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.5023376995083493 wick_frac=0.4976623004916507 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.2600:wick=2.5200; 2026-09-30:GREEN:body=+1.0500:wick=1.7150; 2026-10-01:RED:body=-0.5200:wick=2.9000; 2026-10-02:GREEN:body=+0.3300:wick=1.3604; 2026-10-05:GREEN:body=+2.7400:wick=0.6450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.01 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.27 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5105.1 | **NEUTRAL** |
| `B04_income` | 910.3 | **GOOD** |
| `B05_profit_margin` | 17.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 106.2 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=106.2 vs prior_export=106.2 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | -4.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.4400000000000004 (now=-4.38 vs prior=-4.82 on finviz_2026-10-05) | **GOOD** |
| `B12_institutional_transactions` | -6.6 | **BAD** |
| `B13_short_float` | 6.21 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.01 (this export) | prior_export=8.01 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.27 (this export) | prior_export=0.27 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ET  ·  score **+15**  ·  Oil & Gas Midstream
price=20.719999313354492  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.07 on 2026-10-05; prev RSI=45.89 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.89@2026-10-02 → 51.07@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.89@2026-10-02 → 51.07@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.89@2026-10-02 → 51.07@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8400 R=0.0000); 2026-10-02:GREEN:O=20.0100,C=20.4700,body=+0.4600,vol=10963503.0; 2026-10-05:GREEN:O=20.3400,C=20.7200,body=+0.3800,vol=7204304.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=18167807 Rvol=0); 2026-10-02:GREEN:O=20.0100,C=20.4700,body=+0.4600,vol=10963503.0; 2026-10-05:GREEN:O=20.3400,C=20.7200,body=+0.3800,vol=7204304.0 | **GOOD** |
| `A07_rvol` | RVOL=0.798 on 2026-10-05: today_vol=7204304 / avg20=9023590 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.062 on 2026-10-05 (price=20.7200, mid=20.7985, upper=22.0588, lower=19.5382; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-05: price=20.7200 vs SMA50=20.7866 dist=-0.32% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=20.7985 SMA50=20.7866 SMA80=20.1941 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-08→2026-10-05 (63 bars); S1[2026-07-08→2026-08-05] low=2026-07-10@19.1792; S2[2026-08-06→2026-09-03] low=2026-08-07@20.1100; S3[2026-09-04→2026-10-05] low=2026-10-01@19.5900 | lows=[19.179244689935544, 20.110000610351562, 19.59000015258789] span=4.85% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.9444844023920321 wick_frac=0.055515597607967836 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:RED:body=-0.0300:wick=0.2100; 2026-09-30:RED:body=-0.2600:wick=0.1400; 2026-10-01:GREEN:body=+0.4000:wick=0.1400; 2026-10-02:GREEN:body=+0.4600:wick=0.0500; 2026-10-05:GREEN:body=+0.3800:wick=0.0050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=55.43 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=23.9 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 107379.0 | **NEUTRAL** |
| `B04_income` | 5048.0 | **GOOD** |
| `B05_profit_margin` | 4.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=-0.019999999999999574 (now=24.75 vs prior_export=24.77 on finviz_2026-10-05) | **BAD** |
| `B09_analyst_recom` | 1.58 | **GOOD** |
| `B10_insider_transactions` | 0.25 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.25 vs prior=0.25 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.13 | **GOOD** |
| `B13_short_float` | 1.03 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=55.43 (this export) | prior_export=55.43 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=23.9 (this export) | prior_export=23.9 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ESI  ·  score **+15**  ·  Specialty Chemicals
price=37.540000915527344  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.71 on 2026-10-05; prev RSI=61.96 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.96@2026-10-02 → 59.71@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.96@2026-10-02 → 59.71@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.96@2026-10-02 → 59.71@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=1.974 (G=0.7700 R=0.3900); 2026-10-02:GREEN:O=37.1600,C=37.9300,body=+0.7700,vol=3681609.0; 2026-10-05:RED:O=37.9300,C=37.5400,body=-0.3900,vol=3356351.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.097 (Gvol=3681609 Rvol=3356351); 2026-10-02:GREEN:O=37.1600,C=37.9300,body=+0.7700,vol=3681609.0; 2026-10-05:RED:O=37.9300,C=37.5400,body=-0.3900,vol=3356351.0 | **GOOD** |
| `A07_rvol` | RVOL=0.755 on 2026-10-05: today_vol=3356351 / avg20=4447269 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.768 on 2026-10-05 (price=37.5400, mid=34.7935, upper=38.3695, lower=31.2175; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=37.5400 vs SMA50=35.9460 dist=+4.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-05: SMA20=34.7935 SMA50=35.9460 SMA80=38.4163 | **BAD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-29@34.1000; S2[2026-08-06→2026-09-03] low=2026-09-03@33.4900; S3[2026-09-04→2026-10-05] low=2026-09-15@31.4400 | lows=[34.099998474121094, 33.4900016784668, 31.440000534057617] span=8.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.5500012261546972 wick_frac=0.44999877384530285 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.33050787185206737 wick_frac=0.6694921281479326 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9743632379983567 need>1.4; red_wick_gt_green=False 5d trail=2026-09-29:GREEN:body=+0.5100:wick=0.6500; 2026-09-30:GREEN:body=+0.3000:wick=0.7100; 2026-10-01:GREEN:body=+0.2700:wick=0.9400; 2026-10-02:GREEN:body=+0.7700:wick=0.6300; 2026-10-05:RED:body=-0.3900:wick=0.7900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.7 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.99 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3150.2 | **NEUTRAL** |
| `B04_income` | 178.6 | **GOOD** |
| `B05_profit_margin` | 5.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 47.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.1700000000000017 (now=47.0 vs prior_export=46.83 on finviz_2026-10-05) | **GOOD** |
| `B09_analyst_recom` | 1.15 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.62 | **GOOD** |
| `B13_short_float` | 3.01 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.7 (this export) | prior_export=8.7 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.99 (this export) | prior_export=11.99 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+15**  ·  Healthcare Plans
price=394.510009765625  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.44 on 2026-10-05; prev RSI=39.03 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.03@2026-10-02 → 46.44@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 39.03@2026-10-02 → 46.44@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 39.03@2026-10-02 → 46.44@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=11.1800 R=0.0000); 2026-10-02:GREEN:O=382.0900,C=386.4300,body=+4.3400,vol=888661.0; 2026-10-05:GREEN:O=387.6700,C=394.5100,body=+6.8400,vol=712036.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1600697 Rvol=0); 2026-10-02:GREEN:O=382.0900,C=386.4300,body=+4.3400,vol=888661.0; 2026-10-05:GREEN:O=387.6700,C=394.5100,body=+6.8400,vol=712036.0 | **GOOD** |
| `A07_rvol` | RVOL=0.571 on 2026-10-05: today_vol=712036 / avg20=1245922 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.349 on 2026-10-05 (price=394.5100, mid=402.8520, upper=426.7876, lower=378.9164; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-05: price=394.5100 vs SMA50=397.5922 dist=-0.78% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=402.85_50=397.59_80=398.76 on 2026-10-05: SMA20=402.8520 SMA50=397.5922 SMA80=398.7602 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-28@362.9800; S2[2026-08-06→2026-09-03] low=2026-08-12@385.8400; S3[2026-09-04→2026-10-05] low=2026-10-01@375.7400 | lows=[362.9800109863281, 385.8399963378906, 375.739990234375] span=6.30% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.6047604840392815 wick_frac=0.3952395159607185 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+1.7300:wick=3.7050; 2026-09-30:RED:body=-3.0900:wick=10.0100; 2026-10-01:RED:body=-6.3300:wick=8.3000; 2026-10-02:GREEN:body=+4.3400:wick=3.0500; 2026-10-05:GREEN:body=+6.8400:wick=4.1525 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4800000000000182 (now=447.86 vs prior_export=447.38 on finviz_2026-10-05) | **GOOD** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EC  ·  score **+15**  ·  Oil & Gas Integrated
price=17.25  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.12 on 2026-10-05; prev RSI=45.10 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.10@2026-10-02 → 54.12@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.10@2026-10-02 → 54.12@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.10@2026-10-02 → 54.12@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1000 R=0.0000); 2026-10-02:GREEN:O=16.0500,C=16.6000,body=+0.5500,vol=1106734.0; 2026-10-05:GREEN:O=16.7000,C=17.2500,body=+0.5500,vol=2063405.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3170139 Rvol=0); 2026-10-02:GREEN:O=16.0500,C=16.6000,body=+0.5500,vol=1106734.0; 2026-10-05:GREEN:O=16.7000,C=17.2500,body=+0.5500,vol=2063405.0 | **GOOD** |
| `A07_rvol` | RVOL=1.355 on 2026-10-05: today_vol=2063405 / avg20=1522944 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.078 on 2026-10-05 (price=17.2500, mid=17.1530, upper=18.4023, lower=15.9037; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=17.2500 vs SMA50=17.0244 dist=+1.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=17.1530 SMA50=17.0244 SMA80=16.4778 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-07→2026-10-05 (63 bars); S1[2026-07-07→2026-08-05] low=2026-07-07@14.2700; S2[2026-08-06→2026-09-03] low=2026-08-27@16.2900; S3[2026-09-04→2026-10-05] low=2026-09-25@16.0700 | lows=[14.270000457763672, 16.290000915527344, 16.06999969482422] span=14.16% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.8200504166162104 wick_frac=0.1799495833837896 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-09-29:GREEN:body=+0.1100:wick=0.3100; 2026-09-30:RED:body=-0.0600:wick=0.1600; 2026-10-01:RED:body=-0.1100:wick=0.3700; 2026-10-02:GREEN:body=+0.5500:wick=0.0300; 2026-10-05:GREEN:body=+0.5500:wick=0.2450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=27.29 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.82 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 32293.12 | **NEUTRAL** |
| `B04_income` | 3501.13 | **GOOD** |
| `B05_profit_margin` | 10.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.3 vs prior_export=14.3 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 3.31 | **NEUTRAL** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 216.14 | **GOOD** |
| `B13_short_float` | 0.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.29 (this export) | prior_export=27.29 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.82 (this export) | prior_export=7.82 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DE  ·  score **+15**  ·  Farm & Heavy Construction Machinery
price=682.2000122070312  pair=`2026-10-02→2026-10-05`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.78 on 2026-10-05; prev RSI=55.93 on 2026-10-02 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.93@2026-10-02 → 53.78@2026-10-05 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.93@2026-10-02 → 53.78@2026-10-05 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.93@2026-10-02 → 53.78@2026-10-05 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_body_sum/RED_body_sum=2.328 (G=15.5500 R=6.6800); 2026-10-02:GREEN:O=671.4500,C=687.0000,body=+15.5500,vol=1063543.0; 2026-10-05:RED:O=688.8800,C=682.2000,body=-6.6800,vol=751245.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-02 + 2026-10-05; ratio=GREEN_vol/RED_vol=1.416 (Gvol=1063543 Rvol=751245); 2026-10-02:GREEN:O=671.4500,C=687.0000,body=+15.5500,vol=1063543.0; 2026-10-05:RED:O=688.8800,C=682.2000,body=-6.6800,vol=751245.0 | **GOOD** |
| `A07_rvol` | RVOL=0.593 on 2026-10-05: today_vol=751245 / avg20=1266023 (avg window 2026-09-04→2026-10-02, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.080 on 2026-10-05 (price=682.2000, mid=683.8520, upper=704.6227, lower=663.0813; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-05: price=682.2000 vs SMA50=650.8124 dist=+4.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-05: SMA20=683.8520 SMA50=650.8124 SMA80=630.3199 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-02→2026-10-05 (63 bars); S1[2026-07-02→2026-08-05] low=2026-07-15@576.4500; S2[2026-08-06→2026-09-03] low=2026-08-19@579.3100; S3[2026-09-04→2026-10-05] low=2026-10-01@644.0600 | lows=[576.4500122070312, 579.3099975585938, 644.0599975585938] span=11.73% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: GREEN body_frac=0.7406585867160496 wick_frac=0.2593414132839505 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-02+2026-10-05: RED body_frac=0.47578369871886833 wick_frac=0.5242163012811317 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3278450363196126 need>1.4; red_wick_gt_green=False 5d trail=2026-09-29:RED:body=-5.0900:wick=7.4595; 2026-09-30:RED:body=-3.5600:wick=13.4300; 2026-10-01:GREEN:body=+2.8200:wick=20.6100; 2026-10-02:GREEN:body=+15.5500:wick=5.4448; 2026-10-05:RED:body=-6.6800:wick=7.3600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.65 (current export asof; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=8/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 47981.0 | **NEUTRAL** |
| `B04_income` | 4873.0 | **GOOD** |
| `B05_profit_margin` | 10.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 698.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=698.73 vs prior_export=698.73 on finviz_2026-10-05) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.18 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.18 vs prior=-0.18 on finviz_2026-10-05) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.08 | **GOOD** |
| `B13_short_float` | 2.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.65 (this export) | prior_export=8.65 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-10-05) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-06_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.