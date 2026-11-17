# A+B1 Feature Checklist — 2026-11-17

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,593** names
- Export: `finviz_2026-11-17.csv` · prior export for Δ: `2026-11-16`
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
| 1 | ABNB | +16 | 17 | 1 | 2026-11-13→2026-11-16 | Travel Services |
| 2 | WCN | +16 | 18 | 2 | 2026-11-13→2026-11-16 | Waste Management |
| 3 | CBRL | +16 | 17 | 1 | 2026-11-13→2026-11-16 | Restaurants |
| 4 | SO | +16 | 17 | 1 | 2026-11-13→2026-11-16 | Utilities - Regulated Electric |
| 5 | FOXA | +16 | 16 | 0 | 2026-11-13→2026-11-16 | Entertainment |
| 6 | LFST | +15 | 17 | 2 | 2026-11-13→2026-11-16 | Medical Care Facilities |
| 7 | PPL | +15 | 16 | 1 | 2026-11-13→2026-11-16 | Utilities - Regulated Electric |
| 8 | GXO | +15 | 17 | 2 | 2026-11-13→2026-11-16 | Integrated Freight & Logistics |
| 9 | PBH | +15 | 16 | 1 | 2026-11-13→2026-11-16 | Drug Manufacturers - Specialty & Gen |
| 10 | RPD | +15 | 16 | 1 | 2026-11-13→2026-11-16 | Software - Infrastructure |
| 11 | AMN | +14 | 15 | 1 | 2026-11-13→2026-11-16 | Medical Care Facilities |
| 12 | ELV | +14 | 17 | 3 | 2026-11-13→2026-11-16 | Healthcare Plans |
| 13 | VNT | +14 | 16 | 2 | 2026-11-13→2026-11-16 | Scientific & Technical Instruments |
| 14 | CLOV | +14 | 16 | 2 | 2026-11-13→2026-11-16 | Healthcare Plans |
| 15 | HTHT | +14 | 17 | 3 | 2026-11-13→2026-11-16 | Lodging |

## Full checklist — top 15

### ABNB  ·  score **+16**  ·  Travel Services
price=186.19000244140625  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.23 on 2026-11-16; prev RSI=60.88 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.88@2026-11-13 → 60.23@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.0250 R=0.0000); 2026-11-13:GREEN:O=182.6650,C=186.7800,body=+4.1150,vol=4949070.0; 2026-11-16:GREEN:O=184.2800,C=186.1900,body=+1.9100,vol=7216300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=12165370 Rvol=0); 2026-11-13:GREEN:O=182.6650,C=186.7800,body=+4.1150,vol=4949070.0; 2026-11-16:GREEN:O=184.2800,C=186.1900,body=+1.9100,vol=7216300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.585 on 2026-11-16: today_vol=7216300 / avg20=4553408 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.597 on 2026-11-16 (price=186.1900, mid=176.1068, upper=192.9950, lower=159.2185; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=186.1900 vs SMA50=169.2045 dist=+10.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=176.11_50=169.20_80=171.23 on 2026-11-16: SMA20=176.1068 SMA50=169.2045 SMA80=171.2323 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@165.0000; S2[2026-09-18→2026-10-16] low=2026-09-23@149.0000; S3[2026-10-19→2026-11-16] low=2026-10-30@164.1000 | lows=[165.0, 149.0, 164.10000610351562] span=10.74% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.507175531102692 wick_frac=0.49282446889730813 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.1000:wick=5.0900; 2026-11-11:RED:body=-1.5700:wick=3.5769; 2026-11-12:GREEN:body=+1.5250:wick=3.5850; 2026-11-13:GREEN:body=+4.1150:wick=1.1050; 2026-11-16:GREEN:body=+1.9100:wick=6.5400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.8 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 13958.0 | **NEUTRAL** |
| `B04_income` | 3207.0 | **GOOD** |
| `B05_profit_margin` | 22.98 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 207.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.3499999999999943 (now=207.92 vs prior_export=206.57 on finviz_2026-11-16) | **GOOD** |
| `B09_analyst_recom` | 1.91 | **GOOD** |
| `B10_insider_transactions` | -3.29 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.29 vs prior=-3.29 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.65 | **GOOD** |
| `B13_short_float` | 3.21 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.8 (this export) | prior_export=10.8 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WCN  ·  score **+16**  ·  Waste Management
price=197.3000030517578  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=75.75 on 2026-11-16; prev RSI=72.94 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.94@2026-11-13 → 75.75@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.94@2026-11-13 → 75.75@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.94@2026-11-13 → 75.75@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=12.160 (G=3.0400 R=0.2500); 2026-11-13:RED:O=194.2200,C=193.9700,body=-0.2500,vol=1054000.0; 2026-11-16:GREEN:O=194.2600,C=197.3000,body=+3.0400,vol=2406000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=2.283 (Gvol=2406000 Rvol=1054000); 2026-11-13:RED:O=194.2200,C=193.9700,body=-0.2500,vol=1054000.0; 2026-11-16:GREEN:O=194.2600,C=197.3000,body=+3.0400,vol=2406000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.065 on 2026-11-16: today_vol=2406000 / avg20=2258379 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.719 on 2026-11-16 (price=197.3000, mid=186.4845, upper=201.5293, lower=171.4397; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=197.3000 vs SMA50=169.2702 dist=+16.56% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=186.4845 SMA50=169.2702 SMA80=168.4989 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@158.8500; S2[2026-09-15→2026-10-16] low=2026-10-01@149.3900; S3[2026-10-19→2026-11-16] low=2026-10-19@165.1600 | lows=[158.85000610351562, 149.38999938964844, 165.16000366210938] span=10.56% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.9020769094934732 wick_frac=0.09792309050652685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.08389273772766606 wick_frac=0.916107262272334 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.1600341796875 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+2.1600:wick=1.3700; 2026-11-11:GREEN:body=+2.8300:wick=0.8150; 2026-11-12:RED:body=-1.4700:wick=2.6100; 2026-11-13:RED:body=-0.2500:wick=2.7300; 2026-11-16:GREEN:body=+3.0400:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.93 (current export asof; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.14 (current export; earnings_date=10/21/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9943.62 | **NEUTRAL** |
| `B04_income` | 1133.47 | **GOOD** |
| `B05_profit_margin` | 11.4 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 208.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=208.08 vs prior_export=208.08 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.3 | **GOOD** |
| `B10_insider_transactions` | -1.63 | **BAD** |
| `B11_insider_tx_delta` | delta=0.030000000000000027 (now=-1.63 vs prior=-1.66 on finviz_2026-11-16) | **GOOD** |
| `B12_institutional_transactions` | 1.87 | **GOOD** |
| `B13_short_float` | 1.83 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.93 (this export) | prior_export=3.93 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.14 (this export) | prior_export=1.14 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+16**  ·  Restaurants
price=67.9800033569336  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.57 on 2026-11-16; prev RSI=54.50 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.50@2026-11-13 → 56.57@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=4.429 (G=1.2400 R=0.2800); 2026-11-13:GREEN:O=65.7100,C=66.9500,body=+1.2400,vol=549900.0; 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.209 (Gvol=549900 Rvol=454700); 2026-11-13:GREEN:O=65.7100,C=66.9500,body=+1.2400,vol=549900.0; 2026-11-16:RED:O=68.2600,C=67.9800,body=-0.2800,vol=454700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.639 on 2026-11-16: today_vol=454700 / avg20=711850 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.033 on 2026-11-16 (price=67.9800, mid=67.8435, upper=72.0056, lower=63.6814; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=67.9800 vs SMA50=59.3040 dist=+14.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=67.8435 SMA50=59.3040 SMA80=58.2534 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@42.8700; S2[2026-09-18→2026-10-16] low=2026-09-18@43.7000; S3[2026-10-19→2026-11-16] low=2026-11-11@63.4800 | lows=[42.869998931884766, 43.70000076293945, 63.47999954223633] span=48.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.590475602268475 wick_frac=0.40952439773152505 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.10937462747061488 wick_frac=0.8906253725293851 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.42858310626703 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-1.1700:wick=0.8800; 2026-11-11:GREEN:body=+1.7900:wick=1.2400; 2026-11-12:RED:body=-1.1600:wick=2.3400; 2026-11-13:GREEN:body=+1.2400:wick=0.8600; 2026-11-16:RED:body=-0.2800:wick=2.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=469.95 (current export asof; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=9/23/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3318.71 | **NEUTRAL** |
| `B04_income` | 31.68 | **GOOD** |
| `B05_profit_margin` | 0.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.71 vs prior_export=50.71 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | -0.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.71 vs prior=-0.71 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.15 | **GOOD** |
| `B13_short_float` | 23.32 | **GOOD** |
| `B14_earnings_date` | 9/23/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=469.95 (this export) | prior_export=469.95 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SO  ·  score **+16**  ·  Utilities - Regulated Electric
price=94.63999938964844  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.46 on 2026-11-16; prev RSI=58.89 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.89@2026-11-13 → 62.46@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.89@2026-11-13 → 62.46@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.89@2026-11-13 → 62.46@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.2700 R=0.0000); 2026-11-13:GREEN:O=92.4700,C=93.5900,body=+1.1200,vol=22081900.0; 2026-11-16:GREEN:O=93.4900,C=94.6400,body=+1.1500,vol=13104000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=35185900 Rvol=0); 2026-11-13:GREEN:O=92.4700,C=93.5900,body=+1.1200,vol=22081900.0; 2026-11-16:GREEN:O=93.4900,C=94.6400,body=+1.1500,vol=13104000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.649 on 2026-11-16: today_vol=13104000 / avg20=7944465 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.722 on 2026-11-16 (price=94.6400, mid=93.0085, upper=95.2672, lower=90.7498; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=94.6400 vs SMA50=88.8152 dist=+6.56% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=93.01_50=88.82_80=89.88 on 2026-11-16: SMA20=93.0085 SMA50=88.8152 SMA80=89.8816 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-16@85.8700; S2[2026-09-18→2026-10-16] low=2026-09-29@81.6900; S3[2026-10-19→2026-11-16] low=2026-10-19@89.2500 | lows=[85.87000274658203, 81.69000244140625, 89.25] span=9.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5912254282445406 wick_frac=0.4087745717554594 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+0.7300:wick=0.9154; 2026-11-11:GREEN:body=+0.7500:wick=0.2900; 2026-11-12:RED:body=-1.0700:wick=0.9300; 2026-11-13:GREEN:body=+1.1200:wick=1.2500; 2026-11-16:GREEN:body=+1.1500:wick=0.4700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.39 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.59 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31198.0 | **NEUTRAL** |
| `B04_income` | 4992.0 | **GOOD** |
| `B05_profit_margin` | 16.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 96.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.1799999999999926 (now=96.86 vs prior_export=96.68 on finviz_2026-11-16) | **GOOD** |
| `B09_analyst_recom` | 2.7 | **NEUTRAL** |
| `B10_insider_transactions` | -1.0 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-1.0 vs prior=-1.01 on finviz_2026-11-16) | **GOOD** |
| `B12_institutional_transactions` | 2.75 | **GOOD** |
| `B13_short_float` | 2.61 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.39 (this export) | prior_export=13.39 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.59 (this export) | prior_export=5.59 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FOXA  ·  score **+16**  ·  Entertainment
price=67.94999694824219  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.36 on 2026-11-16; prev RSI=56.76 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.76@2026-11-13 → 55.36@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=2.646 (G=1.6800 R=0.6350); 2026-11-13:GREEN:O=66.7400,C=68.4200,body=+1.6800,vol=6667302.0; 2026-11-16:RED:O=68.5850,C=67.9500,body=-0.6350,vol=5675600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.175 (Gvol=6667302 Rvol=5675600); 2026-11-13:GREEN:O=66.7400,C=68.4200,body=+1.6800,vol=6667302.0; 2026-11-16:RED:O=68.5850,C=67.9500,body=-0.6350,vol=5675600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.593 on 2026-11-16: today_vol=5675600 / avg20=9576184 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.410 on 2026-11-16 (price=67.9500, mid=66.3825, upper=70.2075, lower=62.5575; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=67.9500 vs SMA50=65.1592 dist=+4.28% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=66.3825 SMA50=65.1592 SMA80=64.5067 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@62.9300; S2[2026-09-15→2026-10-16] low=2026-10-01@60.2600; S3[2026-10-19→2026-11-16] low=2026-11-03@62.1000 | lows=[62.93000030517578, 60.2599983215332, 62.099998474121094] span=4.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.7691241036531482 wick_frac=0.23087589634685174 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.31909689341456027 wick_frac=0.6809031065854397 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.645660871550264 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.2200:wick=1.6200; 2026-11-11:GREEN:body=+0.7300:wick=0.8199; 2026-11-12:RED:body=-1.8700:wick=0.6100; 2026-11-13:GREEN:body=+1.6800:wick=0.5043; 2026-11-16:RED:body=-0.6350:wick=1.3550 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=23.12 (current export asof; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.74 (current export; earnings_date=11/2/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17959.0 | **NEUTRAL** |
| `B04_income` | 2029.0 | **GOOD** |
| `B05_profit_margin` | 11.3 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 81.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=81.72 vs prior_export=81.72 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.68 | **GOOD** |
| `B13_short_float` | 14.53 | **NEUTRAL** |
| `B14_earnings_date` | 11/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.12 (this export) | prior_export=23.12 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.74 (this export) | prior_export=5.74 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LFST  ·  score **+15**  ·  Medical Care Facilities
price=14.479999542236328  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.19 on 2026-11-16; prev RSI=60.16 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.16@2026-11-13 → 64.19@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=5.286 (G=0.3700 R=0.0700); 2026-11-13:RED:O=14.1500,C=14.0800,body=-0.0700,vol=3728704.0; 2026-11-16:GREEN:O=14.1100,C=14.4800,body=+0.3700,vol=3768600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.011 (Gvol=3768600 Rvol=3728704); 2026-11-13:RED:O=14.1500,C=14.0800,body=-0.0700,vol=3728704.0; 2026-11-16:GREEN:O=14.1100,C=14.4800,body=+0.3700,vol=3768600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.568 on 2026-11-16: today_vol=3768600 / avg20=6638183 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.643 on 2026-11-16 (price=14.4800, mid=13.5453, upper=14.9987, lower=12.0918; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=14.4800 vs SMA50=12.8255 dist=+12.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=13.5453 SMA50=12.8255 SMA80=12.4538 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-08-25@11.9150; S2[2026-09-18→2026-10-16] low=2026-10-02@11.4700; S3[2026-10-19→2026-11-16] low=2026-10-19@11.6900 | lows=[11.914999961853027, 11.470000267028809, 11.6899995803833] span=3.88% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.7872333517980808 wick_frac=0.21276664820191912 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.11232309877087313 wick_frac=0.8876769012291269 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.285735694822888 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+0.1700:wick=0.4200; 2026-11-11:RED:body=-0.3100:wick=0.3510; 2026-11-12:RED:body=-0.1900:wick=0.3192; 2026-11-13:RED:body=-0.0700:wick=0.5532; 2026-11-16:GREEN:body=+0.3700:wick=0.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=62.6 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.62 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1676.89 | **NEUTRAL** |
| `B04_income` | 71.91 | **GOOD** |
| `B05_profit_margin` | 4.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.8 vs prior_export=16.8 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.45 | **GOOD** |
| `B10_insider_transactions` | -37.66 | **BAD** |
| `B11_insider_tx_delta` | delta=-1.6399999999999935 (now=-37.66 vs prior=-36.02 on finviz_2026-11-16) | **BAD** |
| `B12_institutional_transactions` | 3.6 | **GOOD** |
| `B13_short_float` | 5.3 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=62.6 (this export) | prior_export=62.6 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.62 (this export) | prior_export=5.62 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PPL  ·  score **+15**  ·  Utilities - Regulated Electric
price=35.79999923706055  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.67 on 2026-11-16; prev RSI=58.19 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.19@2026-11-13 → 59.67@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=3.875 (G=0.3100 R=0.0800); 2026-11-13:GREEN:O=35.3600,C=35.6700,body=+0.3100,vol=5891200.0; 2026-11-16:RED:O=35.8800,C=35.8000,body=-0.0800,vol=5731400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.028 (Gvol=5891200 Rvol=5731400); 2026-11-13:GREEN:O=35.3600,C=35.6700,body=+0.3100,vol=5891200.0; 2026-11-16:RED:O=35.8800,C=35.8000,body=-0.0800,vol=5731400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.652 on 2026-11-16: today_vol=5731400 / avg20=8787981 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.408 on 2026-11-16 (price=35.8000, mid=35.4420, upper=36.3196, lower=34.5644; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=35.8000 vs SMA50=34.3396 dist=+4.25% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=35.44_50=34.34_80=34.69 on 2026-11-16: SMA20=35.4420 SMA50=34.3396 SMA80=34.6931 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-14@33.6700; S2[2026-09-15→2026-10-16] low=2026-09-25@31.5550; S3[2026-10-19→2026-11-16] low=2026-10-20@33.9100 | lows=[33.66999816894531, 31.55500030517578, 33.90999984741211] span=7.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.6739090773389945 wick_frac=0.3260909226610054 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.2285776566757493 wick_frac=0.7714223433242506 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.8748807934388707 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.2200:wick=0.3600; 2026-11-11:RED:body=-0.1900:wick=0.1850; 2026-11-12:RED:body=-0.3300:wick=0.2000; 2026-11-13:GREEN:body=+0.3100:wick=0.1500; 2026-11-16:RED:body=-0.0800:wick=0.2700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.73 (current export asof; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.5 (current export; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9657.0 | **NEUTRAL** |
| `B04_income` | 1314.0 | **GOOD** |
| `B05_profit_margin` | 13.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.2600000000000051 (now=40.38 vs prior_export=40.12 on finviz_2026-11-16) | **GOOD** |
| `B09_analyst_recom` | 1.57 | **GOOD** |
| `B10_insider_transactions` | -0.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.78 vs prior=-0.78 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.02 | **GOOD** |
| `B13_short_float` | 4.58 | **NEUTRAL** |
| `B14_earnings_date` | 11/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.73 (this export) | prior_export=1.73 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.5 (this export) | prior_export=3.5 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GXO  ·  score **+15**  ·  Integrated Freight & Logistics
price=57.15999984741211  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.15 on 2026-11-16; prev RSI=65.33 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 65.33@2026-11-13 → 72.15@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.33@2026-11-13 → 72.15@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 65.33@2026-11-13 → 72.15@2026-11-16 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=6.833 (G=1.6400 R=0.2400); 2026-11-13:RED:O=54.8700,C=54.6300,body=-0.2400,vol=1058337.0; 2026-11-16:GREEN:O=55.5200,C=57.1600,body=+1.6400,vol=2554300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=2.414 (Gvol=2554300 Rvol=1058337); 2026-11-13:RED:O=54.8700,C=54.6300,body=-0.2400,vol=1058337.0; 2026-11-16:GREEN:O=55.5200,C=57.1600,body=+1.6400,vol=2554300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.727 on 2026-11-16: today_vol=2554300 / avg20=1478629 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.755 on 2026-11-16 (price=57.1600, mid=52.3390, upper=58.7230, lower=45.9550; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=57.1600 vs SMA50=48.6008 dist=+17.61% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=52.34_50=48.60_80=48.63 on 2026-11-16: SMA20=52.3390 SMA50=48.6008 SMA80=48.6342 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-17→2026-11-16 (63 bars); S1[2026-08-17→2026-09-15] low=2026-09-14@44.9900; S2[2026-09-16→2026-10-16] low=2026-09-24@43.6100; S3[2026-10-19→2026-11-16] low=2026-10-19@47.0000 | lows=[44.9900016784668, 43.61000061035156, 47.0] span=7.77% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5222929007741164 wick_frac=0.47770709922588356 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.2181801788055126 wick_frac=0.7818198211944875 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.833391613949201 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.3950:wick=1.0350; 2026-11-11:RED:body=-0.3050:wick=0.6550; 2026-11-12:RED:body=-0.9700:wick=1.2800; 2026-11-13:RED:body=-0.2400:wick=0.8600; 2026-11-16:GREEN:body=+1.6400:wick=1.5000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.08 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.41 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 13836.0 | **NEUTRAL** |
| `B04_income` | 136.0 | **GOOD** |
| `B05_profit_margin` | 0.98 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 71.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4099999999999966 (now=71.47 vs prior_export=71.06 on finviz_2026-11-16) | **GOOD** |
| `B09_analyst_recom` | 1.06 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.44 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.08 (this export) | prior_export=4.08 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.41 (this export) | prior_export=1.41 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBH  ·  score **+15**  ·  Drug Manufacturers - Specialty & Generic
price=48.0  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.44 on 2026-11-16; prev RSI=47.48 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.48@2026-11-13 → 52.44@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.8000 R=0.0000); 2026-11-13:GREEN:O=45.6900,C=46.1900,body=+0.5000,vol=500400.0; 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1031500 Rvol=0); 2026-11-13:GREEN:O=45.6900,C=46.1900,body=+0.5000,vol=500400.0; 2026-11-16:GREEN:O=46.7000,C=48.0000,body=+1.3000,vol=531100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.838 on 2026-11-16: today_vol=531100 / avg20=633628 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.119 on 2026-11-16 (price=48.0000, mid=47.3925, upper=52.4980, lower=42.2870; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=48.0000 vs SMA50=47.5500 dist=+0.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-11-16: SMA20=47.3925 SMA50=47.5500 SMA80=49.0785 | **BAD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-11@46.4800; S2[2026-09-15→2026-10-16] low=2026-09-21@44.4600; S3[2026-10-19→2026-11-16] low=2026-11-06@41.7700 | lows=[46.47999954223633, 44.459999084472656, 41.77000045776367] span=11.28% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.6130989489588102 wick_frac=0.38690105104118977 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+1.4800:wick=0.6000; 2026-11-11:RED:body=-0.1000:wick=0.9927; 2026-11-12:GREEN:body=+0.6000:wick=0.8700; 2026-11-13:GREEN:body=+0.5000:wick=0.6800; 2026-11-16:GREEN:body=+1.3000:wick=0.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.32 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1167.08 | **NEUTRAL** |
| `B04_income` | 155.1 | **GOOD** |
| `B05_profit_margin` | 13.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.25 vs prior_export=65.25 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 9.09 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RPD  ·  score **+15**  ·  Software - Infrastructure
price=14.6899995803833  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.30 on 2026-11-16; prev RSI=49.14 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.14@2026-11-13 → 51.30@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=1.708 (G=0.4100 R=0.2400); 2026-11-13:RED:O=14.6500,C=14.4100,body=-0.2400,vol=2061580.0; 2026-11-16:GREEN:O=14.2800,C=14.6900,body=+0.4100,vol=1067400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=0.518 (Gvol=1067400 Rvol=2061580); 2026-11-13:RED:O=14.6500,C=14.4100,body=-0.2400,vol=2061580.0; 2026-11-16:GREEN:O=14.2800,C=14.6900,body=+0.4100,vol=1067400.0 | **BAD** |
| `A07_rvol` | RVOL=0.507 on 2026-11-16: today_vol=1067400 / avg20=2106540 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.223 on 2026-11-16 (price=14.6900, mid=15.2907, upper=17.9867, lower=12.5948; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=14.6900 vs SMA50=13.4710 dist=+9.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=15.2907 SMA50=13.4710 SMA80=12.7715 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-11@10.1200; S2[2026-09-18→2026-10-16] low=2026-09-28@11.1300; S3[2026-10-19→2026-11-16] low=2026-10-19@12.5900 | lows=[10.119999885559082, 11.130000114440918, 12.59000015258789] span=24.41% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5222929007741164 wick_frac=0.47770709922588356 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.23837864104339557 wick_frac=0.7616213589566044 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.708334326745027 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-0.0300:wick=1.2100; 2026-11-11:RED:body=-0.1700:wick=0.5898; 2026-11-12:GREEN:body=+0.0060:wick=0.8653; 2026-11-13:RED:body=-0.2400:wick=0.7668; 2026-11-16:GREEN:body=+0.4100:wick=0.3750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=31.43 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.6 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 850.37 | **NEUTRAL** |
| `B04_income` | 13.84 | **GOOD** |
| `B05_profit_margin` | 1.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 15.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=15.97 vs prior_export=15.97 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 3.32 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.11 | **GOOD** |
| `B13_short_float` | 10.46 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.43 (this export) | prior_export=31.43 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.6 (this export) | prior_export=1.6 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMN  ·  score **+14**  ·  Medical Care Facilities
price=39.63999938964844  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=46.78 on 2026-11-16; prev RSI=41.74 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.74@2026-11-13 → 46.78@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 41.74@2026-11-13 → 46.78@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 41.74@2026-11-13 → 46.78@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0300 R=0.0000); 2026-11-13:GREEN:O=37.5000,C=38.0200,body=+0.5200,vol=574500.0; 2026-11-16:GREEN:O=38.1300,C=39.6400,body=+1.5100,vol=657300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1231800 Rvol=0); 2026-11-13:GREEN:O=37.5000,C=38.0200,body=+0.5200,vol=574500.0; 2026-11-16:GREEN:O=38.1300,C=39.6400,body=+1.5100,vol=657300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.671 on 2026-11-16: today_vol=657300 / avg20=978956 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.412 on 2026-11-16 (price=39.6400, mid=43.3480, upper=52.3561, lower=34.3399; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=39.6400 vs SMA50=38.7284 dist=+2.35% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=43.3480 SMA50=38.7284 SMA80=36.9784 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-03@32.0300; S2[2026-09-18→2026-10-16] low=2026-09-24@33.4100; S3[2026-10-19→2026-11-16] low=2026-11-09@35.3000 | lows=[32.029998779296875, 33.40999984741211, 35.29999923706055] span=10.21% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.5553425037821524 wick_frac=0.44465749621784756 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.2700:wick=1.0800; 2026-11-11:GREEN:body=+0.1900:wick=1.1700; 2026-11-12:RED:body=-0.5000:wick=0.6441; 2026-11-13:GREEN:body=+0.5200:wick=0.8700; 2026-11-16:GREEN:body=+1.5100:wick=0.5400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=406.11 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.43 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3517.09 | **NEUTRAL** |
| `B04_income` | 109.02 | **GOOD** |
| `B05_profit_margin` | 3.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 44.79 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=44.79 vs prior_export=44.79 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.94 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.94 vs prior=-0.94 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.13 | **GOOD** |
| `B13_short_float` | 11.19 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=406.11 (this export) | prior_export=406.11 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.43 (this export) | prior_export=10.43 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+14**  ·  Healthcare Plans
price=446.2799987792969  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.73 on 2026-11-16; prev RSI=55.45 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.45@2026-11-13 → 63.73@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.45@2026-11-13 → 63.73@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.45@2026-11-13 → 63.73@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=3.217 (G=8.0100 R=2.4900); 2026-11-13:RED:O=432.1800,C=429.6900,body=-2.4900,vol=779100.0; 2026-11-16:GREEN:O=438.2700,C=446.2800,body=+8.0100,vol=2280400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=2.927 (Gvol=2280400 Rvol=779100); 2026-11-13:RED:O=432.1800,C=429.6900,body=-2.4900,vol=779100.0; 2026-11-16:GREEN:O=438.2700,C=446.2800,body=+8.0100,vol=2280400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.676 on 2026-11-16: today_vol=2280400 / avg20=1360631 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.893 on 2026-11-16 (price=446.2800, mid=428.5625, upper=448.3966, lower=408.7284; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-16: price=446.2800 vs SMA50=415.1006 dist=+7.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=428.5625 SMA50=415.1006 SMA80=406.0516 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-17@388.1200; S2[2026-09-15→2026-10-16] low=2026-10-01@375.7400; S3[2026-10-19→2026-11-16] low=2026-11-02@406.9000 | lows=[388.1199951171875, 375.739990234375, 406.8999938964844] span=8.29% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.8754085675787452 wick_frac=0.12459143242125485 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.19362264088258815 wick_frac=0.8063773591174118 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.2168840082361014 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:GREEN:body=+9.9200:wick=1.8400; 2026-11-11:GREEN:body=+0.5400:wick=8.0000; 2026-11-12:RED:body=-3.8800:wick=3.9700; 2026-11-13:RED:body=-2.4900:wick=10.3700; 2026-11-16:GREEN:body=+8.0100:wick=1.1400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=48.28 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.35 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201831.0 | **NEUTRAL** |
| `B04_income` | 5139.0 | **GOOD** |
| `B05_profit_margin` | 2.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 479.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=479.62 vs prior_export=479.62 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | -2.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.01 vs prior=-2.01 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.12 | **BAD** |
| `B13_short_float` | 2.11 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=48.28 (this export) | prior_export=48.28 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.35 (this export) | prior_export=2.35 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+14**  ·  Scientific & Technical Instruments
price=43.5  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.00 on 2026-11-16; prev RSI=80.13 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 80.13@2026-11-13 → 79.00@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=3.600 (G=0.1800 R=0.0500); 2026-11-13:GREEN:O=43.4600,C=43.6400,body=+0.1800,vol=1515428.0; 2026-11-16:RED:O=43.5500,C=43.5000,body=-0.0500,vol=1513300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.001 (Gvol=1515428 Rvol=1513300); 2026-11-13:GREEN:O=43.4600,C=43.6400,body=+0.1800,vol=1515428.0; 2026-11-16:RED:O=43.5500,C=43.5000,body=-0.0500,vol=1513300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.876 on 2026-11-16: today_vol=1513300 / avg20=1728385 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.644 on 2026-11-16 (price=43.5000, mid=38.3535, upper=46.3460, lower=30.3610; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=43.5000 vs SMA50=34.5268 dist=+25.99% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=38.3535 SMA50=34.5268 SMA80=33.8500 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-16 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@31.1100; S2[2026-09-15→2026-10-16] low=2026-09-24@30.4250; S3[2026-10-19→2026-11-16] low=2026-10-19@32.9300 | lows=[31.110000610351562, 30.424999237060547, 32.93000030517578] span=8.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.3050851841075874 wick_frac=0.6949148158924127 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.0709211031810877 wick_frac=0.9290788968189123 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.600061036087587 need>1.4; red_wick_gt_green=True 5d trail=2026-11-10:RED:body=-0.1200:wick=2.0268; 2026-11-11:GREEN:body=+0.7900:wick=0.5350; 2026-11-12:RED:body=-0.5800:wick=0.1500; 2026-11-13:GREEN:body=+0.1800:wick=0.4100; 2026-11-16:RED:body=-0.0500:wick=0.6550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=46.56 vs prior_export=46.56 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.48 | **GOOD** |
| `B13_short_float` | 9.18 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CLOV  ·  score **+14**  ·  Healthcare Plans
price=4.949999809265137  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.28 on 2026-11-16; prev RSI=58.64 on 2026-11-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.64@2026-11-13 → 58.28@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.64@2026-11-13 → 58.28@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.64@2026-11-13 → 58.28@2026-11-16 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=2.727 (G=0.0600 R=0.0220); 2026-11-13:GREEN:O=4.9000,C=4.9600,body=+0.0600,vol=4736267.0; 2026-11-16:RED:O=4.9720,C=4.9500,body=-0.0220,vol=4063300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=1.166 (Gvol=4736267 Rvol=4063300); 2026-11-13:GREEN:O=4.9000,C=4.9600,body=+0.0600,vol=4736267.0; 2026-11-16:RED:O=4.9720,C=4.9500,body=-0.0220,vol=4063300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.886 on 2026-11-16: today_vol=4063300 / avg20=4584883 (avg window 2026-10-19→2026-11-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.597 on 2026-11-16 (price=4.9500, mid=4.7020, upper=5.1175, lower=4.2865; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-16: price=4.9500 vs SMA50=4.6213 dist=+7.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=4.7020 SMA50=4.6213 SMA80=4.5043 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-08-25@4.0350; S2[2026-09-18→2026-10-16] low=2026-10-01@4.1100; S3[2026-10-19→2026-11-16] low=2026-11-03@4.1900 | lows=[4.034999847412109, 4.110000133514404, 4.190000057220459] span=3.84% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.1764703407420705 wick_frac=0.8235296592579295 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=0.11000140666760444 wick_frac=0.8899985933323955 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.727231349429971 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.1100:wick=0.1200; 2026-11-11:RED:body=-0.0400:wick=0.1550; 2026-11-12:RED:body=-0.0400:wick=0.1234; 2026-11-13:GREEN:body=+0.0600:wick=0.2800; 2026-11-16:RED:body=-0.0220:wick=0.1780 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=296.08 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.6 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2739.37 | **NEUTRAL** |
| `B04_income` | 12.45 | **GOOD** |
| `B05_profit_margin` | 0.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 5.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=5.6 vs prior_export=5.6 on finviz_2026-11-16) | **NEUTRAL** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -1.42 | **BAD** |
| `B11_insider_tx_delta` | delta=0.08000000000000007 (now=-1.42 vs prior=-1.5 on finviz_2026-11-16) | **GOOD** |
| `B12_institutional_transactions` | 6.79 | **GOOD** |
| `B13_short_float` | 5.61 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=296.08 (this export) | prior_export=296.08 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.6 (this export) | prior_export=2.6 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HTHT  ·  score **+14**  ·  Lodging
price=54.880001068115234  pair=`2026-11-13→2026-11-16`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=77.60 on 2026-11-16; prev RSI=65.62 on 2026-11-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 65.62@2026-11-13 → 77.60@2026-11-16 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.62@2026-11-13 → 77.60@2026-11-16 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 65.62@2026-11-13 → 77.60@2026-11-16 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=3.5400 R=0.0000); 2026-11-13:GREEN:O=49.6500,C=50.3900,body=+0.7400,vol=1841800.0; 2026-11-16:GREEN:O=52.0800,C=54.8800,body=+2.8000,vol=3736700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-13 + 2026-11-16; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5578500 Rvol=0); 2026-11-13:GREEN:O=49.6500,C=50.3900,body=+0.7400,vol=1841800.0; 2026-11-16:GREEN:O=52.0800,C=54.8800,body=+2.8000,vol=3736700.0 | **GOOD** |
| `A07_rvol` | RVOL=2.346 on 2026-11-16: today_vol=3736700 / avg20=1592719 (avg window 2026-10-19→2026-11-13, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.300 on 2026-11-16 (price=54.8800, mid=48.6610, upper=53.4453, lower=43.8767; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-16: price=54.8800 vs SMA50=45.4814 dist=+20.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-16: SMA20=48.6610 SMA50=45.4814 SMA80=45.2631 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-16 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-16@42.5100; S2[2026-09-18→2026-10-16] low=2026-10-02@41.2900; S3[2026-10-19→2026-11-16] low=2026-10-21@44.2000 | lows=[42.5099983215332, 41.290000915527344, 44.20000076293945] span=7.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: GREEN body_frac=0.6631649197748106 wick_frac=0.3368350802251894 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-13+2026-11-16: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-10:GREEN:body=+0.3200:wick=0.8750; 2026-11-11:RED:body=-0.4600:wick=0.5500; 2026-11-12:GREEN:body=+0.4500:wick=0.2400; 2026-11-13:GREEN:body=+0.7400:wick=0.5500; 2026-11-16:GREEN:body=+2.8000:wick=0.9200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=12.36 (current export asof; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.29 (current export; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3804.86 | **NEUTRAL** |
| `B04_income` | 720.34 | **GOOD** |
| `B05_profit_margin` | 18.93 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 62.02 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.7000000000000028 (now=62.02 vs prior_export=61.32 on finviz_2026-11-16) | **GOOD** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-16) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.91 | **GOOD** |
| `B13_short_float` | 3.98 | **NEUTRAL** |
| `B14_earnings_date` | 11/16/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.36 (this export) | prior_export=3.56 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.29 (this export) | prior_export=5.03 (finviz_2026-11-16) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-17_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.