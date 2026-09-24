# A+B1 Feature Checklist — 2026-09-23

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,663** names
- Export: `finviz_2026-09-23.csv` · prior export for Δ: `2026-09-22`
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
| 1 | NICE | +18 | 19 | 1 | 2026-09-21→2026-09-23 | Software - Application |
| 2 | FLS | +18 | 18 | 0 | 2026-09-21→2026-09-23 | Specialty Industrial Machinery |
| 3 | FIVN | +16 | 18 | 2 | 2026-09-21→2026-09-23 | Software - Infrastructure |
| 4 | BP | +16 | 16 | 0 | 2026-09-22→2026-09-23 | Oil & Gas Integrated |
| 5 | ICE | +16 | 17 | 1 | 2026-09-21→2026-09-23 | Financial Data & Stock Exchanges |
| 6 | AMGN | +16 | 17 | 1 | 2026-09-21→2026-09-23 | Drug Manufacturers - General |
| 7 | P | +16 | 18 | 2 | 2026-09-21→2026-09-23 | Computer Hardware |
| 8 | FRSH | +16 | 17 | 1 | 2026-09-21→2026-09-23 | Software - Application |
| 9 | AMRX | +15 | 18 | 3 | 2026-09-21→2026-09-23 | Drug Manufacturers - Specialty & Gen |
| 10 | TTC | +15 | 17 | 2 | 2026-09-21→2026-09-23 | Tools & Accessories |
| 11 | PM | +15 | 16 | 1 | 2026-09-21→2026-09-23 | Tobacco |
| 12 | MDB | +15 | 16 | 1 | 2026-09-21→2026-09-23 | Software - Infrastructure |
| 13 | SHEL | +15 | 15 | 0 | 2026-09-22→2026-09-23 | Oil & Gas Integrated |
| 14 | SPNT | +14 | 16 | 2 | 2026-09-21→2026-09-23 | Insurance - Reinsurance |
| 15 | VEEV | +14 | 15 | 1 | 2026-09-21→2026-09-23 | Health Information Services |

## Full checklist — top 15

### NICE  ·  score **+18**  ·  Software - Application
price=117.79000091552734  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.69 on 2026-09-23; prev RSI=65.75 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.75@2026-09-21 → 68.69@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.75@2026-09-21 → 68.69@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.75@2026-09-21 → 68.69@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=27.143 (G=9.5000 R=0.3500); 2026-09-21:GREEN:O=105.0400,C=114.5400,body=+9.5000,vol=1173500.0; 2026-09-23:RED:O=118.1400,C=117.7900,body=-0.3500,vol=1102400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.064 (Gvol=1173500 Rvol=1102400); 2026-09-21:GREEN:O=105.0400,C=114.5400,body=+9.5000,vol=1173500.0; 2026-09-23:RED:O=118.1400,C=117.7900,body=-0.3500,vol=1102400.0 | **GOOD** |
| `A07_rvol` | RVOL=2.541 on 2026-09-23: today_vol=1102400 / avg20=433880 (avg window 2026-08-24→2026-09-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.228 on 2026-09-23 (price=117.7900, mid=105.3560, upper=115.4786, lower=95.2334; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-23: price=117.7900 vs SMA50=101.8830 dist=+15.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=105.3560 SMA50=101.8830 SMA80=97.9538 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-22→2026-09-23 (63 bars); S1[2026-06-22→2026-07-22] low=2026-06-22@83.2700; S2[2026-07-23→2026-08-21] low=2026-07-23@86.0100; S3[2026-08-24→2026-09-23] low=2026-09-11@97.4400 | lows=[83.2699966430664, 86.01000213623047, 97.44000244140625] span=17.02% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.7704783932856388 wick_frac=0.2295216067143612 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.029940002519202393 wick_frac=0.9700599974807976 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=27.142975476839236 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:GREEN:body=+4.0600:wick=0.1900; 2026-09-17:RED:body=-1.5900:wick=1.9500; 2026-09-18:GREEN:body=+0.0400:wick=3.0200; 2026-09-21:GREEN:body=+9.5000:wick=2.8300; 2026-09-23:RED:body=-0.3500:wick=11.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.27 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.09 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3089.11 | **NEUTRAL** |
| `B04_income` | 428.37 | **GOOD** |
| `B05_profit_margin` | 13.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 125.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.3699999999999903 (now=125.55 vs prior_export=124.18 on finviz_2026-09-22) | **GOOD** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.69 | **GOOD** |
| `B13_short_float` | 2.13 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+18**  ·  Specialty Industrial Machinery
price=77.18000030517578  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.02 on 2026-09-23; prev RSI=46.74 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.74@2026-09-21 → 54.02@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.74@2026-09-21 → 54.02@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.74@2026-09-21 → 54.02@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=3.289 (G=1.4800 R=0.4500); 2026-09-21:RED:O=75.1200,C=74.6700,body=-0.4500,vol=1347100.0; 2026-09-23:GREEN:O=75.7000,C=77.1800,body=+1.4800,vol=1426500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.059 (Gvol=1426500 Rvol=1347100); 2026-09-21:RED:O=75.1200,C=74.6700,body=-0.4500,vol=1347100.0; 2026-09-23:GREEN:O=75.7000,C=77.1800,body=+1.4800,vol=1426500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.841 on 2026-09-23: today_vol=1426500 / avg20=1696160 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.124 on 2026-09-23 (price=77.1800, mid=76.3325, upper=83.1497, lower=69.5153; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=77.1800 vs SMA50=75.7176 dist=+1.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=76.3325 SMA50=75.7176 SMA80=75.5499 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-18→2026-09-23 (63 bars); S1[2026-06-18→2026-07-20] low=2026-07-20@66.3300; S2[2026-07-23→2026-08-21] low=2026-07-23@67.9500; S3[2026-08-24→2026-09-23] low=2026-09-14@68.3600 | lows=[66.33000183105469, 67.94999694824219, 68.36000061035156] span=3.06% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.6851879978100772 wick_frac=0.31481200218992283 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.3879337560179948 wick_frac=0.6120662439820052 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.288862892697896 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:GREEN:body=+0.1200:wick=2.7800; 2026-09-17:RED:body=-0.4000:wick=1.6300; 2026-09-18:RED:body=-0.3800:wick=0.7000; 2026-09-21:RED:body=-0.4500:wick=0.7100; 2026-09-23:GREEN:body=+1.4800:wick=0.6800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.79 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.41 | **GOOD** |
| `B13_short_float` | 6.32 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FIVN  ·  score **+16**  ·  Software - Infrastructure
price=37.68000030517578  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.37 on 2026-09-23; prev RSI=64.93 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.93@2026-09-21 → 66.37@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.93@2026-09-21 → 66.37@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.93@2026-09-21 → 66.37@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=3.273 (G=4.0100 R=1.2250); 2026-09-21:GREEN:O=33.0000,C=37.0100,body=+4.0100,vol=4044200.0; 2026-09-23:RED:O=38.9050,C=37.6800,body=-1.2250,vol=2958400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.367 (Gvol=4044200 Rvol=2958400); 2026-09-21:GREEN:O=33.0000,C=37.0100,body=+4.0100,vol=4044200.0; 2026-09-23:RED:O=38.9050,C=37.6800,body=-1.2250,vol=2958400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.509 on 2026-09-23: today_vol=2958400 / avg20=1960255 (avg window 2026-08-24→2026-09-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.109 on 2026-09-23 (price=37.6800, mid=33.2738, upper=37.2483, lower=29.2992; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-23: price=37.6800 vs SMA50=30.6235 dist=+23.04% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=33.2738 SMA50=30.6235 SMA80=27.5503 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-23 (63 bars); S1[2026-06-24→2026-07-23] low=2026-06-24@19.0500; S2[2026-07-24→2026-08-21] low=2026-07-24@22.2500; S3[2026-08-24→2026-09-23] low=2026-09-10@30.1000 | lows=[19.049999237060547, 22.25, 30.100000381469727] span=58.01% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.9271670784186958 wick_frac=0.0728329215813042 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.47480527314987314 wick_frac=0.5251947268501269 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2734720950654883 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:GREEN:body=+1.0600:wick=0.8350; 2026-09-17:RED:body=-0.2500:wick=1.8800; 2026-09-18:RED:body=-1.9700:wick=0.6400; 2026-09-21:GREEN:body=+4.0100:wick=0.3150; 2026-09-23:RED:body=-1.2250:wick=1.3550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.74 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.88 | **NEUTRAL** |
| `B04_income` | 59.47 | **GOOD** |
| `B05_profit_margin` | 4.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.55 vs prior_export=35.55 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -4.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.83 vs prior=-4.83 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.72 | **GOOD** |
| `B13_short_float` | 11.97 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.9 (this export) | prior_export=1.9 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BP  ·  score **+16**  ·  Oil & Gas Integrated
price=44.4900016784668  pair=`2026-09-22→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.03 on 2026-09-23; prev RSI=44.20 on 2026-09-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.20@2026-09-22 → 52.03@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.20@2026-09-22 → 52.03@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.20@2026-09-22 → 52.03@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-22 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0600 R=0.0000); 2026-09-22:GREEN:O=42.7100,C=43.1000,body=+0.3900,vol=8741800.0; 2026-09-23:GREEN:O=43.8200,C=44.4900,body=+0.6700,vol=8702200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-22 + 2026-09-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=17444000 Rvol=0); 2026-09-22:GREEN:O=42.7100,C=43.1000,body=+0.3900,vol=8741800.0; 2026-09-23:GREEN:O=43.8200,C=44.4900,body=+0.6700,vol=8702200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.861 on 2026-09-23: today_vol=8702200 / avg20=10112300 (avg window 2026-08-25→2026-09-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.042 on 2026-09-23 (price=44.4900, mid=44.3705, upper=47.2075, lower=41.5335; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=44.4900 vs SMA50=43.3092 dist=+2.73% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=44.3705 SMA50=43.3092 SMA80=41.9932 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-23 (63 bars); S1[2026-06-25→2026-07-24] low=2026-07-01@35.6024; S2[2026-07-27→2026-08-24] low=2026-08-05@40.6603; S3[2026-08-25→2026-09-23] low=2026-08-28@41.7200 | lows=[35.60243644763844, 40.66027501103988, 41.720001220703125] span=17.18% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-22+2026-09-23: GREEN body_frac=0.5289510755668523 wick_frac=0.4710489244331477 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-22+2026-09-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-17:GREEN:body=+0.3600:wick=0.3600; 2026-09-18:RED:body=-0.1200:wick=0.4100; 2026-09-21:RED:body=-1.5500:wick=0.0400; 2026-09-22:GREEN:body=+0.3900:wick=0.7400; 2026-09-23:GREEN:body=+0.6700:wick=0.2700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.35 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.04 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 216829.8 | **NEUTRAL** |
| `B04_income` | 5490.34 | **GOOD** |
| `B05_profit_margin` | 2.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.08 vs prior_export=50.08 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 60.93 | **GOOD** |
| `B13_short_float` | 0.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.35 (this export) | prior_export=18.35 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.04 (this export) | prior_export=11.04 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICE  ·  score **+16**  ·  Financial Data & Stock Exchanges
price=156.35000610351562  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.69 on 2026-09-23; prev RSI=48.38 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.38@2026-09-21 → 50.69@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.38@2026-09-21 → 50.69@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.38@2026-09-21 → 50.69@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=4.776 (G=4.0600 R=0.8500); 2026-09-21:RED:O=156.2200,C=155.3700,body=-0.8500,vol=2986900.0; 2026-09-23:GREEN:O=152.2900,C=156.3500,body=+4.0600,vol=3042100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.018 (Gvol=3042100 Rvol=2986900); 2026-09-21:RED:O=156.2200,C=155.3700,body=-0.8500,vol=2986900.0; 2026-09-23:GREEN:O=152.2900,C=156.3500,body=+4.0600,vol=3042100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.950 on 2026-09-23: today_vol=3042100 / avg20=3202920 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.331 on 2026-09-23 (price=156.3500, mid=158.3990, upper=164.5842, lower=152.2138; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=156.3500 vs SMA50=153.1876 dist=+2.06% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=158.3990 SMA50=153.1876 SMA80=146.8765 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-22→2026-09-23 (63 bars); S1[2026-06-22→2026-07-23] low=2026-06-29@121.7900; S2[2026-07-24→2026-08-21] low=2026-07-24@142.7800; S3[2026-08-24→2026-09-23] low=2026-09-23@151.4200 | lows=[121.79000091552734, 142.77999877929688, 151.4199981689453] span=24.33% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.63937148267228 wick_frac=0.36062851732772 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.28716357281673516 wick_frac=0.7128364271832649 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.776451369690877 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:RED:body=-1.6900:wick=2.5900; 2026-09-17:RED:body=-1.0000:wick=0.6900; 2026-09-18:GREEN:body=+3.0000:wick=0.9700; 2026-09-21:RED:body=-0.8500:wick=2.1100; 2026-09-23:GREEN:body=+4.0600:wick=2.2900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.49 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.33 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 13138.0 | **NEUTRAL** |
| `B04_income` | 4038.0 | **GOOD** |
| `B05_profit_margin` | 30.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 186.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=186.86 vs prior_export=186.86 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | -2.02 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.02 vs prior=-2.02 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 0.96 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.49 (this export) | prior_export=3.49 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.33 (this export) | prior_export=1.33 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMGN  ·  score **+16**  ·  Drug Manufacturers - General
price=406.0899963378906  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.34 on 2026-09-23; prev RSI=44.82 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.82@2026-09-21 → 52.34@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.82@2026-09-21 → 52.34@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.82@2026-09-21 → 52.34@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=1.863 (G=7.4500 R=4.0000); 2026-09-21:GREEN:O=385.7100,C=393.1600,body=+7.4500,vol=2889000.0; 2026-09-23:RED:O=410.0900,C=406.0900,body=-4.0000,vol=2822500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.024 (Gvol=2889000 Rvol=2822500); 2026-09-21:GREEN:O=385.7100,C=393.1600,body=+7.4500,vol=2889000.0; 2026-09-23:RED:O=410.0900,C=406.0900,body=-4.0000,vol=2822500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.962 on 2026-09-23: today_vol=2822500 / avg20=2932535 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.058 on 2026-09-23 (price=406.0900, mid=409.3310, upper=465.0364, lower=353.6256; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=406.0900 vs SMA50=401.5152 dist=+1.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=409.3310 SMA50=401.5152 SMA80=382.4593 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-23→2026-09-23 (63 bars); S1[2026-06-23→2026-07-23] low=2026-06-23@345.4900; S2[2026-07-24→2026-08-21] low=2026-07-24@372.1600; S3[2026-08-24→2026-09-23] low=2026-09-16@374.4600 | lows=[345.489990234375, 372.1600036621094, 374.4599914550781] span=8.39% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.5912705236668547 wick_frac=0.4087294763331452 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.3502622851813549 wick_frac=0.6497377148186451 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.8625030517578125 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:RED:body=-0.6600:wick=3.9900; 2026-09-17:GREEN:body=+0.6300:wick=5.7800; 2026-09-18:GREEN:body=+7.1400:wick=0.8500; 2026-09-21:GREEN:body=+7.4500:wick=5.1500; 2026-09-23:RED:body=-4.0000:wick=7.4200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.97 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.66 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 38202.0 | **NEUTRAL** |
| `B04_income` | 8743.0 | **GOOD** |
| `B05_profit_margin` | 22.89 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 396.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=396.93 vs prior_export=396.93 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.62 | **NEUTRAL** |
| `B10_insider_transactions` | -0.9 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.9 vs prior=-0.9 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 2.41 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.97 (this export) | prior_export=11.97 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.66 (this export) | prior_export=6.66 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### P  ·  score **+16**  ·  Computer Hardware
price=109.6500015258789  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.36 on 2026-09-23; prev RSI=65.44 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.44@2026-09-21 → 60.36@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.44@2026-09-21 → 60.36@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.44@2026-09-21 → 60.36@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=5.477 (G=8.1600 R=1.4900); 2026-09-21:GREEN:O=105.5000,C=113.6600,body=+8.1600,vol=7342600.0; 2026-09-23:RED:O=111.1400,C=109.6500,body=-1.4900,vol=3818500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.923 (Gvol=7342600 Rvol=3818500); 2026-09-21:GREEN:O=105.5000,C=113.6600,body=+8.1600,vol=7342600.0; 2026-09-23:RED:O=111.1400,C=109.6500,body=-1.4900,vol=3818500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.553 on 2026-09-23: today_vol=3818500 / avg20=6909835 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.835 on 2026-09-23 (price=109.6500, mid=99.5725, upper=111.6370, lower=87.5080; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-23: price=109.6500 vs SMA50=93.1728 dist=+17.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=99.5725 SMA50=93.1728 SMA80=86.4708 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-23 (63 bars); S1[2026-06-24→2026-07-23] low=2026-07-17@65.7500; S2[2026-07-24→2026-08-21] low=2026-07-28@68.2300; S3[2026-08-24→2026-09-23] low=2026-09-01@89.6450 | lows=[65.75, 68.2300033569336, 89.6449966430664] span=36.34% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.8680853550549195 wick_frac=0.13191464494508054 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.304081101995631 wick_frac=0.695918898004369 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.476520376657091 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:RED:body=-0.3900:wick=4.0250; 2026-09-17:GREEN:body=+3.9400:wick=0.8600; 2026-09-18:RED:body=-1.2100:wick=4.9700; 2026-09-21:GREEN:body=+8.1600:wick=1.2400; 2026-09-23:RED:body=-1.4900:wick=3.4100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.67 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.04 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4262.15 | **NEUTRAL** |
| `B04_income` | 253.28 | **GOOD** |
| `B05_profit_margin` | 5.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 136.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.789999999999992 (now=136.26 vs prior_export=132.47 on finviz_2026-09-22) | **GOOD** |
| `B09_analyst_recom` | 1.48 | **GOOD** |
| `B10_insider_transactions` | -12.94 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-12.94 vs prior=-12.94 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 2.41 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.67 (this export) | prior_export=20.67 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.04 (this export) | prior_export=8.04 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FRSH  ·  score **+16**  ·  Software - Application
price=12.869999885559082  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.21 on 2026-09-23; prev RSI=49.81 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.81@2026-09-21 → 55.21@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.81@2026-09-21 → 55.21@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.81@2026-09-21 → 55.21@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.5900 R=0.0000); 2026-09-21:GREEN:O=12.2700,C=12.4500,body=+0.1800,vol=3072200.0; 2026-09-23:GREEN:O=12.4600,C=12.8700,body=+0.4100,vol=6236700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9308900 Rvol=0); 2026-09-21:GREEN:O=12.2700,C=12.4500,body=+0.1800,vol=3072200.0; 2026-09-23:GREEN:O=12.4600,C=12.8700,body=+0.4100,vol=6236700.0 | **GOOD** |
| `A07_rvol` | RVOL=1.575 on 2026-09-23: today_vol=6236700 / avg20=3958555 (avg window 2026-08-24→2026-09-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.052 on 2026-09-23 (price=12.8700, mid=12.8045, upper=14.0626, lower=11.5464; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=12.8700 vs SMA50=12.0771 dist=+6.57% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=12.8045 SMA50=12.0771 SMA80=11.1672 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-22→2026-09-23 (63 bars); S1[2026-06-22→2026-07-21] low=2026-06-22@8.6200; S2[2026-07-23→2026-08-21] low=2026-07-23@9.7650; S3[2026-08-24→2026-09-23] low=2026-09-10@11.7040 | lows=[8.619999885559082, 9.765000343322754, 11.704000473022461] span=35.78% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.6550824354467273 wick_frac=0.34491756455327266 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:GREEN:body=+0.3100:wick=0.0700; 2026-09-17:GREEN:body=+0.1400:wick=0.3450; 2026-09-18:RED:body=-0.5000:wick=0.0150; 2026-09-21:GREEN:body=+0.1800:wick=0.1350; 2026-09-23:GREEN:body=+0.4100:wick=0.1450 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.37 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.61 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 903.87 | **NEUTRAL** |
| `B04_income` | 185.19 | **GOOD** |
| `B05_profit_margin` | 20.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.64 vs prior_export=14.64 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.05 | **GOOD** |
| `B10_insider_transactions` | -0.12 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.12 vs prior=-0.12 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.58 | **GOOD** |
| `B13_short_float` | 12.02 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.37 (this export) | prior_export=30.37 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.61 (this export) | prior_export=1.61 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMRX  ·  score **+15**  ·  Drug Manufacturers - Specialty & Generic
price=19.329999923706055  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.36 on 2026-09-23; prev RSI=70.28 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 70.28@2026-09-21 → 61.36@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.28@2026-09-21 → 61.36@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 70.28@2026-09-21 → 61.36@2026-09-23 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=3.243 (G=1.2000 R=0.3700); 2026-09-21:GREEN:O=18.9600,C=20.1600,body=+1.2000,vol=5905200.0; 2026-09-23:RED:O=19.7000,C=19.3300,body=-0.3700,vol=3575100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.652 (Gvol=5905200 Rvol=3575100); 2026-09-21:GREEN:O=18.9600,C=20.1600,body=+1.2000,vol=5905200.0; 2026-09-23:RED:O=19.7000,C=19.3300,body=-0.3700,vol=3575100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.559 on 2026-09-23: today_vol=3575100 / avg20=2292590 (avg window 2026-08-24→2026-09-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.889 on 2026-09-23 (price=19.3300, mid=17.8295, upper=19.5173, lower=16.1417; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-23: price=19.3300 vs SMA50=17.8974 dist=+8.00% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=17.83_50=17.90_80=17.02 on 2026-09-23: SMA20=17.8295 SMA50=17.8974 SMA80=17.0246 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-22→2026-09-23 (63 bars); S1[2026-06-22→2026-07-21] low=2026-06-23@15.9300; S2[2026-07-23→2026-08-21] low=2026-08-12@16.7700; S3[2026-08-24→2026-09-23] low=2026-09-16@16.5600 | lows=[15.930000305175781, 16.770000457763672, 16.559999465942383] span=5.27% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.5000003973643616 wick_frac=0.49999960263563836 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.2578404020453165 wick_frac=0.7421595979546836 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2432379489347225 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:GREEN:body=+1.4300:wick=0.3100; 2026-09-17:RED:body=-0.2800:wick=0.7800; 2026-09-18:GREEN:body=+0.5900:wick=0.1600; 2026-09-21:GREEN:body=+1.2000:wick=1.2000; 2026-09-23:RED:body=-0.3700:wick=1.0650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=34.83 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.64 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3117.55 | **NEUTRAL** |
| `B04_income` | 157.36 | **GOOD** |
| `B05_profit_margin` | 5.05 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7100000000000009 (now=22.71 vs prior_export=22.0 on finviz_2026-09-22) | **GOOD** |
| `B09_analyst_recom` | 1.12 | **GOOD** |
| `B10_insider_transactions` | -0.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.25 vs prior=-0.25 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.9 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=34.83 (this export) | prior_export=34.83 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.64 (this export) | prior_export=3.64 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TTC  ·  score **+15**  ·  Tools & Accessories
price=96.58000183105469  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.85 on 2026-09-23; prev RSI=43.74 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.74@2026-09-21 → 54.85@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.74@2026-09-21 → 54.85@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.74@2026-09-21 → 54.85@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=7.818 (G=1.7200 R=0.2200); 2026-09-21:RED:O=93.6100,C=93.3900,body=-0.2200,vol=843100.0; 2026-09-23:GREEN:O=94.8600,C=96.5800,body=+1.7200,vol=782700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=0.928 (Gvol=782700 Rvol=843100); 2026-09-21:RED:O=93.6100,C=93.3900,body=-0.2200,vol=843100.0; 2026-09-23:GREEN:O=94.8600,C=96.5800,body=+1.7200,vol=782700.0 | **BAD** |
| `A07_rvol` | RVOL=0.755 on 2026-09-23: today_vol=782700 / avg20=1036290 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.196 on 2026-09-23 (price=96.5800, mid=95.4315, upper=101.3044, lower=89.5586; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=96.5800 vs SMA50=95.7608 dist=+0.86% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=95.43_50=95.76_80=94.46 on 2026-09-23: SMA20=95.4315 SMA50=95.7608 SMA80=94.4610 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-18→2026-09-23 (63 bars); S1[2026-06-18→2026-07-20] low=2026-06-23@90.6000; S2[2026-07-23→2026-08-21] low=2026-07-30@90.6200; S3[2026-08-24→2026-09-23] low=2026-09-04@91.0300 | lows=[90.5999984741211, 90.62000274658203, 91.02999877929688] span=0.47% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.5771809674983038 wick_frac=0.4228190325016961 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.18965562600299912 wick_frac=0.8103443739970009 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.818143986683313 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:RED:body=-1.2800:wick=0.9300; 2026-09-17:RED:body=-0.2100:wick=2.1900; 2026-09-18:GREEN:body=+1.4800:wick=1.0500; 2026-09-21:RED:body=-0.2200:wick=0.9400; 2026-09-23:GREEN:body=+1.7200:wick=1.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.75 (current export asof; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.91 (current export; earnings_date=9/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4756.5 | **NEUTRAL** |
| `B04_income` | 363.3 | **GOOD** |
| `B05_profit_margin` | 7.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.25 (now=111.0 vs prior_export=108.75 on finviz_2026-09-22) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -4.64 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.64 vs prior=-4.64 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.73 | **GOOD** |
| `B13_short_float` | 3.98 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.75 (this export) | prior_export=1.75 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.91 (this export) | prior_export=2.91 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PM  ·  score **+15**  ·  Tobacco
price=190.83999633789062  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.60 on 2026-09-23; prev RSI=46.86 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 46.86@2026-09-21 → 52.60@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 46.86@2026-09-21 → 52.60@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 46.86@2026-09-21 → 52.60@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=1.519 (G=0.7900 R=0.5200); 2026-09-21:RED:O=188.0000,C=187.4800,body=-0.5200,vol=5693200.0; 2026-09-23:GREEN:O=190.0500,C=190.8400,body=+0.7900,vol=7307800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.284 (Gvol=7307800 Rvol=5693200); 2026-09-21:RED:O=188.0000,C=187.4800,body=-0.5200,vol=5693200.0; 2026-09-23:GREEN:O=190.0500,C=190.8400,body=+0.7900,vol=7307800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.780 on 2026-09-23: today_vol=7307800 / avg20=4105370 (avg window 2026-08-24→2026-09-21, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.190 on 2026-09-23 (price=190.8400, mid=189.5425, upper=196.3774, lower=182.7076; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=190.8400 vs SMA50=189.5726 dist=+0.67% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=189.54_50=189.57_80=185.60 on 2026-09-23: SMA20=189.5425 SMA50=189.5726 SMA80=185.6015 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-23 (63 bars); S1[2026-06-24→2026-07-23] low=2026-06-24@175.1083; S2[2026-07-24→2026-08-21] low=2026-08-12@184.0200; S3[2026-08-24→2026-09-23] low=2026-09-08@181.0100 | lows=[175.1082500828659, 184.02000427246094, 181.00999450683594] span=5.09% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.1775267028991719 wick_frac=0.8224732971008281 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.1969713608646649 wick_frac=0.8030286391353351 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5192053757445934 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:RED:body=-0.7100:wick=3.2400; 2026-09-17:RED:body=-2.3200:wick=1.7800; 2026-09-18:GREEN:body=+0.6200:wick=3.8200; 2026-09-21:RED:body=-0.5200:wick=2.1200; 2026-09-23:GREEN:body=+0.7900:wick=3.6600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=7.28 (current export asof; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.57 (current export; earnings_date=7/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 42447.0 | **NEUTRAL** |
| `B04_income` | 10844.0 | **GOOD** |
| `B05_profit_margin` | 25.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.77 vs prior_export=209.77 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 1.07 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.28 (this export) | prior_export=7.28 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.57 (this export) | prior_export=5.57 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDB  ·  score **+15**  ·  Software - Infrastructure
price=428.3599853515625  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.39 on 2026-09-23; prev RSI=54.72 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.72@2026-09-21 → 59.39@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.72@2026-09-21 → 59.39@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.72@2026-09-21 → 59.39@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=20.765 (G=20.3500 R=0.9800); 2026-09-21:GREEN:O=386.9500,C=407.3000,body=+20.3500,vol=1249700.0; 2026-09-23:RED:O=429.3400,C=428.3600,body=-0.9800,vol=1192100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.048 (Gvol=1249700 Rvol=1192100); 2026-09-21:GREEN:O=386.9500,C=407.3000,body=+20.3500,vol=1249700.0; 2026-09-23:RED:O=429.3400,C=428.3600,body=-0.9800,vol=1192100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.598 on 2026-09-23: today_vol=1192100 / avg20=1994105 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.536 on 2026-09-23 (price=428.3600, mid=395.6900, upper=456.6910, lower=334.6890; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=428.3600 vs SMA50=380.9552 dist=+12.44% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=395.6900 SMA50=380.9552 SMA80=368.1777 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-24→2026-09-23 (63 bars); S1[2026-06-24→2026-07-23] low=2026-06-25@290.5700; S2[2026-07-24→2026-08-21] low=2026-07-24@291.7900; S3[2026-08-24→2026-09-23] low=2026-09-08@354.1100 | lows=[290.57000732421875, 291.7900085449219, 354.1099853515625] span=21.87% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.7112892696684345 wick_frac=0.28871073033156547 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.06931750972426685 wick_frac=0.9306824902757331 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.76504842275714 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:GREEN:body=+3.5280:wick=10.6400; 2026-09-17:GREEN:body=+17.5000:wick=13.5000; 2026-09-18:RED:body=-12.9000:wick=3.0150; 2026-09-21:GREEN:body=+20.3500:wick=8.2600; 2026-09-23:RED:body=-0.9800:wick=13.1580 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=17.62 (current export asof; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.0 (current export; earnings_date=9/1/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2782.77 | **NEUTRAL** |
| `B04_income` | 58.9 | **GOOD** |
| `B05_profit_margin` | 2.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 470.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=470.69 vs prior_export=470.69 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -5.72 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.72 vs prior=-5.72 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.29 | **GOOD** |
| `B13_short_float` | 3.8 | **NEUTRAL** |
| `B14_earnings_date` | 9/1/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=17.62 (this export) | prior_export=17.62 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.0 (this export) | prior_export=5.0 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHEL  ·  score **+15**  ·  Oil & Gas Integrated
price=95.41000366210938  pair=`2026-09-22→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.99 on 2026-09-23; prev RSI=52.27 on 2026-09-22 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.27@2026-09-22 → 56.99@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.27@2026-09-22 → 56.99@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.27@2026-09-22 → 56.99@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-22 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.1700 R=0.0000); 2026-09-22:GREEN:O=92.9900,C=93.9300,body=+0.9400,vol=5967765.0; 2026-09-23:GREEN:O=94.1800,C=95.4100,body=+1.2300,vol=5253044.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-22 + 2026-09-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=11220809 Rvol=0); 2026-09-22:GREEN:O=92.9900,C=93.9300,body=+0.9400,vol=5967765.0; 2026-09-23:GREEN:O=94.1800,C=95.4100,body=+1.2300,vol=5253044.0 | **GOOD** |
| `A07_rvol` | RVOL=0.807 on 2026-09-23: today_vol=5253044 / avg20=6508453 (avg window 2026-08-25→2026-09-22, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.276 on 2026-09-23 (price=95.4100, mid=94.1740, upper=98.6602, lower=89.6878; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=95.4100 vs SMA50=91.0162 dist=+4.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=94.1740 SMA50=91.0162 SMA80=87.3551 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-25→2026-09-23 (63 bars); S1[2026-06-25→2026-07-24] low=2026-07-01@75.5975; S2[2026-07-27→2026-08-24] low=2026-07-28@85.0744; S3[2026-08-25→2026-09-23] low=2026-08-27@90.0500 | lows=[75.59747315421986, 85.07441491675512, 90.05000305175781] span=19.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-22+2026-09-23: GREEN body_frac=0.7141410965935355 wick_frac=0.28585890340646447 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-22+2026-09-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-17:GREEN:body=+0.3400:wick=0.4550; 2026-09-18:GREEN:body=+0.0450:wick=0.9650; 2026-09-21:RED:body=-1.4500:wick=0.3900; 2026-09-22:GREEN:body=+0.9400:wick=0.7700; 2026-09-23:GREEN:body=+1.2300:wick=0.1700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 102.85 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=102.85 vs prior_export=102.85 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.42 | **GOOD** |
| `B13_short_float` | 0.81 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SPNT  ·  score **+14**  ·  Insurance - Reinsurance
price=25.420000076293945  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.57 on 2026-09-23; prev RSI=63.88 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.88@2026-09-21 → 66.57@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.88@2026-09-21 → 66.57@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.88@2026-09-21 → 66.57@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=6.429 (G=0.4500 R=0.0700); 2026-09-21:GREEN:O=24.7200,C=25.1700,body=+0.4500,vol=1979400.0; 2026-09-23:RED:O=25.4900,C=25.4200,body=-0.0700,vol=1049100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=1.887 (Gvol=1979400 Rvol=1049100); 2026-09-21:GREEN:O=24.7200,C=25.1700,body=+0.4500,vol=1979400.0; 2026-09-23:RED:O=25.4900,C=25.4200,body=-0.0700,vol=1049100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.230 on 2026-09-23: today_vol=1049100 / avg20=852725 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.281 on 2026-09-23 (price=25.4200, mid=24.4465, upper=25.2062, lower=23.6868; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-23: price=25.4200 vs SMA50=24.2514 dist=+4.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=24.4465 SMA50=24.2514 SMA80=23.7417 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-18→2026-09-23 (63 bars); S1[2026-06-18→2026-07-20] low=2026-06-18@22.5900; S2[2026-07-23→2026-08-21] low=2026-08-12@22.3600; S3[2026-08-24→2026-09-23] low=2026-08-27@23.5300 | lows=[22.59000015258789, 22.360000610351562, 23.530000686645508] span=5.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.8490599988483907 wick_frac=0.15094000115160938 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=0.1372542419788546 wick_frac=0.8627457580211454 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.428610354223434 need>1.4; red_wick_gt_green=True 5d trail=2026-09-16:RED:body=-0.1400:wick=0.5400; 2026-09-17:RED:body=-0.2500:wick=0.2500; 2026-09-18:GREEN:body=+0.4000:wick=0.2400; 2026-09-21:GREEN:body=+0.4500:wick=0.0800; 2026-09-23:RED:body=-0.0700:wick=0.4400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.68 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=22.44 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3248.3 | **NEUTRAL** |
| `B04_income` | 494.7 | **GOOD** |
| `B05_profit_margin` | 15.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 28.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=28.25 vs prior_export=28.25 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.45 | **BAD** |
| `B13_short_float` | 5.24 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.68 (this export) | prior_export=3.68 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=22.44 (this export) | prior_export=22.44 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VEEV  ·  score **+14**  ·  Health Information Services
price=270.75  pair=`2026-09-21→2026-09-23`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.00 on 2026-09-23; prev RSI=54.49 on 2026-09-21 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.49@2026-09-21 → 61.00@2026-09-23 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.49@2026-09-21 → 61.00@2026-09-23 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.49@2026-09-21 → 61.00@2026-09-23 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=8.8100 R=0.0000); 2026-09-21:GREEN:O=260.0000,C=261.3100,body=+1.3100,vol=1199100.0; 2026-09-23:GREEN:O=263.2500,C=270.7500,body=+7.5000,vol=1426700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-21 + 2026-09-23; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2625800 Rvol=0); 2026-09-21:GREEN:O=260.0000,C=261.3100,body=+1.3100,vol=1199100.0; 2026-09-23:GREEN:O=263.2500,C=270.7500,body=+7.5000,vol=1426700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.726 on 2026-09-23: today_vol=1426700 / avg20=1965675 (avg window 2026-08-24→2026-09-21, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.125 on 2026-09-23 (price=270.7500, mid=267.8925, upper=290.6880, lower=245.0970; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-23: price=270.7500 vs SMA50=237.2718 dist=+14.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-23: SMA20=267.8925 SMA50=237.2718 SMA80=213.0490 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-23→2026-09-23 (63 bars); S1[2026-06-23→2026-07-23] low=2026-06-23@155.7400; S2[2026-07-24→2026-08-21] low=2026-07-24@182.0000; S3[2026-08-24→2026-09-23] low=2026-08-26@238.9300 | lows=[155.74000549316406, 182.0, 238.92999267578125] span=53.42% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: GREEN body_frac=0.5084965920276316 wick_frac=0.4915034079723684 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-21+2026-09-23: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-09-16:RED:body=-0.3700:wick=5.1500; 2026-09-17:GREEN:body=+0.3100:wick=5.9500; 2026-09-18:RED:body=-3.1900:wick=1.4700; 2026-09-21:GREEN:body=+1.3100:wick=5.2400; 2026-09-23:GREEN:body=+7.5000:wick=1.6800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=5.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.49 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3458.1 | **NEUTRAL** |
| `B04_income` | 1014.77 | **GOOD** |
| `B05_profit_margin` | 29.34 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 297.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=297.42 vs prior_export=297.42 on finviz_2026-09-22) | **NEUTRAL** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -0.38 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.38 vs prior=-0.38 on finviz_2026-09-22) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.11 | **GOOD** |
| `B13_short_float` | 3.27 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.49 (this export) | prior_export=5.49 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.49 (this export) | prior_export=2.49 (finviz_2026-09-22) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-23_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.