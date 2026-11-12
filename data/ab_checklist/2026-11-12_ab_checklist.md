# A+B1 Feature Checklist — 2026-11-12

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,611** names
- Export: `finviz_2026-11-12.csv` · prior export for Δ: `2026-11-11`
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
| 1 | ZM | +17 | 18 | 1 | 2026-11-10→2026-11-11 | Software - Application |
| 2 | RSG | +17 | 17 | 0 | 2026-11-10→2026-11-11 | Waste Management |
| 3 | MSCI | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Financial Data & Stock Exchanges |
| 4 | ABBV | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Drug Manufacturers - General |
| 5 | V | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Credit Services |
| 6 | WMT | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Discount Stores |
| 7 | HONA | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Aerospace & Defense |
| 8 | SOLS | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Specialty Chemicals |
| 9 | GPN | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Software - Infrastructure |
| 10 | RMD | +15 | 17 | 2 | 2026-11-10→2026-11-11 | Medical Instruments & Supplies |
| 11 | TEVA | +15 | 17 | 2 | 2026-11-10→2026-11-11 | Drug Manufacturers - Specialty & Gen |
| 12 | TAK | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Drug Manufacturers - Specialty & Gen |
| 13 | HUM | +15 | 17 | 2 | 2026-11-10→2026-11-11 | Healthcare Plans |
| 14 | PBA | +15 | 15 | 0 | 2026-11-10→2026-11-11 | Oil & Gas Midstream |
| 15 | HSY | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Confectioners |

## Full checklist — top 15

### ZM  ·  score **+17**  ·  Software - Application
price=98.08000183105469  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.69 on 2026-11-11; prev RSI=43.01 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.01@2026-11-10 → 56.69@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 43.01@2026-11-10 → 56.69@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 43.01@2026-11-10 → 56.69@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=9.397 (G=5.4500 R=0.5800); 2026-11-10:RED:O=91.4900,C=90.9100,body=-0.5800,vol=3115602.0; 2026-11-11:GREEN:O=92.6300,C=98.0800,body=+5.4500,vol=4291208.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.377 (Gvol=4291208 Rvol=3115602); 2026-11-10:RED:O=91.4900,C=90.9100,body=-0.5800,vol=3115602.0; 2026-11-11:GREEN:O=92.6300,C=98.0800,body=+5.4500,vol=4291208.0 | **GOOD** |
| `A07_rvol` | RVOL=1.698 on 2026-11-11: today_vol=4291208 / avg20=2526920 (avg window 2026-10-14→2026-11-10, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.423 on 2026-11-11 (price=98.0800, mid=95.1540, upper=102.0752, lower=88.2328; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=98.0800 vs SMA50=94.3169 dist=+3.99% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=95.15_50=94.32_80=96.24 on 2026-11-11: SMA20=95.1540 SMA50=94.3169 SMA80=96.2448 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-26@92.7200; S2[2026-09-15→2026-10-13] low=2026-09-28@85.4900; S3[2026-10-14→2026-11-11] low=2026-11-06@87.7300 | lows=[92.72000122070312, 85.48999786376953, 87.7300033569336] span=8.46% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.8790334290699764 wick_frac=0.12096657093002365 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.1613335200889636 wick_frac=0.8386664799110364 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.396653556254194 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-2.5000:wick=0.7800; 2026-11-06:GREEN:body=+1.6800:wick=0.9400; 2026-11-09:GREEN:body=+1.6600:wick=1.8680; 2026-11-10:RED:body=-0.5800:wick=3.0150; 2026-11-11:GREEN:body=+5.4500:wick=0.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.83 (current export asof; earnings_date=11/23/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.83 (current export; earnings_date=11/23/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4993.05 | **NEUTRAL** |
| `B04_income` | 3255.07 | **GOOD** |
| `B05_profit_margin` | 65.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 118.52 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=118.52 vs prior_export=118.52 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.8 vs prior=-0.8 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.49 | **GOOD** |
| `B13_short_float` | 2.09 | **NEUTRAL** |
| `B14_earnings_date` | 11/23/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.83 (this export) | prior_export=4.83 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.83 (this export) | prior_export=0.83 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+17**  ·  Waste Management
price=249.33999633789062  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.35 on 2026-11-11; prev RSI=66.82 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.82@2026-11-10 → 68.35@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.82@2026-11-10 → 68.35@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.82@2026-11-10 → 68.35@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.8600 R=0.0000); 2026-11-10:GREEN:O=245.2400,C=247.6200,body=+2.3800,vol=799144.0; 2026-11-11:GREEN:O=246.8600,C=249.3400,body=+2.4800,vol=1500158.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2299302 Rvol=0); 2026-11-10:GREEN:O=245.2400,C=247.6200,body=+2.3800,vol=799144.0; 2026-11-11:GREEN:O=246.8600,C=249.3400,body=+2.4800,vol=1500158.0 | **GOOD** |
| `A07_rvol` | RVOL=1.041 on 2026-11-11: today_vol=1500158 / avg20=1441751 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.661 on 2026-11-11 (price=249.3400, mid=240.0410, upper=254.1096, lower=225.9724; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=249.3400 vs SMA50=226.4668 dist=+10.10% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=240.0410 SMA50=226.4668 SMA80=222.8984 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-17@213.0000; S2[2026-09-15→2026-10-13] low=2026-10-02@205.9700; S3[2026-10-14→2026-11-11] low=2026-10-14@221.0100 | lows=[213.0, 205.97000122070312, 221.00999450683594] span=7.30% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6537305972425143 wick_frac=0.34626940275748574 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-05:GREEN:body=+1.2200:wick=6.1841; 2026-11-06:GREEN:body=+3.6600:wick=1.1700; 2026-11-09:GREEN:body=+0.0900:wick=3.4900; 2026-11-10:GREEN:body=+2.3800:wick=2.3100; 2026-11-11:GREEN:body=+2.4800:wick=0.6200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.16 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 17238.0 | **NEUTRAL** |
| `B04_income` | 2227.56 | **GOOD** |
| `B05_profit_margin` | 12.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 260.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.2999999999999545 (now=260.78 vs prior_export=259.48 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 1.97 | **GOOD** |
| `B10_insider_transactions` | 6.6 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=6.6 vs prior=6.6 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.12 | **GOOD** |
| `B13_short_float` | 2.35 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.16 (this export) | prior_export=3.16 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSCI  ·  score **+16**  ·  Financial Data & Stock Exchanges
price=661.530029296875  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.75 on 2026-11-11; prev RSI=66.65 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.65@2026-11-10 → 67.75@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.65@2026-11-10 → 67.75@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.65@2026-11-10 → 67.75@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=16.9300 R=0.0000); 2026-11-10:GREEN:O=649.1400,C=656.8600,body=+7.7200,vol=471631.0; 2026-11-11:GREEN:O=652.3200,C=661.5300,body=+9.2100,vol=427494.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=899125 Rvol=0); 2026-11-10:GREEN:O=649.1400,C=656.8600,body=+7.7200,vol=471631.0; 2026-11-11:GREEN:O=652.3200,C=661.5300,body=+9.2100,vol=427494.0 | **GOOD** |
| `A07_rvol` | RVOL=0.616 on 2026-11-11: today_vol=427494 / avg20=694148 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.649 on 2026-11-11 (price=661.5300, mid=620.9505, upper=683.4863, lower=558.4147; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=661.5300 vs SMA50=580.5780 dist=+13.94% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=620.9505 SMA50=580.5780 SMA80=575.7254 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-14@542.6300; S2[2026-09-15→2026-10-13] low=2026-09-29@533.4600; S3[2026-10-14→2026-11-11] low=2026-10-14@557.0000 | lows=[542.6300048828125, 533.4600219726562, 557.0] span=4.41% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.5601831586638508 wick_frac=0.43981684133614907 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-24.0900:wick=6.8300; 2026-11-06:RED:body=-1.0100:wick=11.2000; 2026-11-09:RED:body=-0.9700:wick=11.4100; 2026-11-10:GREEN:body=+7.7200:wick=6.3600; 2026-11-11:GREEN:body=+9.2100:wick=6.8893 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.65 (current export asof; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.47 (current export; earnings_date=10/20/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3432.33 | **NEUTRAL** |
| `B04_income` | 1383.67 | **GOOD** |
| `B05_profit_margin` | 40.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 693.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=693.22 vs prior_export=693.22 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | 0.1 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.1 vs prior=0.1 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.25 | **GOOD** |
| `B13_short_float` | 1.99 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.65 (this export) | prior_export=1.65 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.47 (this export) | prior_export=0.47 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ABBV  ·  score **+16**  ·  Drug Manufacturers - General
price=294.9800109863281  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.84 on 2026-11-11; prev RSI=61.87 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.87@2026-11-10 → 62.84@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.87@2026-11-10 → 62.84@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.87@2026-11-10 → 62.84@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.0100 R=0.0000); 2026-11-10:GREEN:O=292.5000,C=293.8900,body=+1.3900,vol=3471430.0; 2026-11-11:GREEN:O=291.3600,C=294.9800,body=+3.6200,vol=3145456.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=6616886 Rvol=0); 2026-11-10:GREEN:O=292.5000,C=293.8900,body=+1.3900,vol=3471430.0; 2026-11-11:GREEN:O=291.3600,C=294.9800,body=+3.6200,vol=3145456.0 | **GOOD** |
| `A07_rvol` | RVOL=0.576 on 2026-11-11: today_vol=3145456 / avg20=5457732 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.786 on 2026-11-11 (price=294.9800, mid=288.6200, upper=296.7070, lower=280.5330; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=294.9800 vs SMA50=274.1540 dist=+7.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=288.6200 SMA50=274.1540 SMA80=267.0801 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-09@246.7700; S2[2026-09-15→2026-10-13] low=2026-09-15@257.8000; S3[2026-10-14→2026-11-11] low=2026-10-14@275.9700 | lows=[246.77000427246094, 257.79998779296875, 275.9700012207031] span=11.83% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.5164786345066694 wick_frac=0.4835213654933306 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-10.4950:wick=4.7050; 2026-11-06:GREEN:body=+9.0400:wick=1.6200; 2026-11-09:RED:body=-0.5400:wick=4.4400; 2026-11-10:GREEN:body=+1.3900:wick=2.0700; 2026-11-11:GREEN:body=+3.6200:wick=2.1150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.51 (current export asof; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 66014.0 | **NEUTRAL** |
| `B04_income` | 10297.0 | **GOOD** |
| `B05_profit_margin` | 15.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 314.81 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.160000000000025 (now=314.81 vs prior_export=313.65 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 1.61 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.89 | **GOOD** |
| `B13_short_float` | 0.9 | **NEUTRAL** |
| `B14_earnings_date` | 10/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.51 (this export) | prior_export=2.51 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### V  ·  score **+16**  ·  Credit Services
price=425.54998779296875  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.71 on 2026-11-11; prev RSI=63.31 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.31@2026-11-10 → 64.71@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.31@2026-11-10 → 64.71@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.31@2026-11-10 → 64.71@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=9.8975 R=0.0000); 2026-11-10:GREEN:O=421.4000,C=423.0000,body=+1.6000,vol=5581730.0; 2026-11-11:GREEN:O=417.2525,C=425.5500,body=+8.2975,vol=4464848.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10046578 Rvol=0); 2026-11-10:GREEN:O=421.4000,C=423.0000,body=+1.6000,vol=5581730.0; 2026-11-11:GREEN:O=417.2525,C=425.5500,body=+8.2975,vol=4464848.0 | **GOOD** |
| `A07_rvol` | RVOL=0.606 on 2026-11-11: today_vol=4464848 / avg20=7363829 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.566 on 2026-11-11 (price=425.5500, mid=413.5580, upper=434.7358, lower=392.3802; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=425.5500 vs SMA50=388.5842 dist=+9.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=413.5580 SMA50=388.5842 SMA80=380.6233 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-18@358.0500; S2[2026-09-15→2026-10-13] low=2026-10-02@356.7500; S3[2026-10-14→2026-11-11] low=2026-10-14@389.5600 | lows=[358.04998779296875, 356.75, 389.55999755859375] span=9.20% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6329267689318276 wick_frac=0.36707323106817236 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-18.2100:wick=2.0754; 2026-11-06:GREEN:body=+8.1700:wick=3.2300; 2026-11-09:RED:body=-1.8600:wick=6.9100; 2026-11-10:GREEN:body=+1.6000:wick=4.1300; 2026-11-11:GREEN:body=+8.2975:wick=0.1125 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.2 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.4 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 45903.0 | **NEUTRAL** |
| `B04_income` | 23313.0 | **GOOD** |
| `B05_profit_margin` | 50.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 457.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.3199999999999932 (now=457.86 vs prior_export=457.54 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.58 | **GOOD** |
| `B13_short_float` | 1.26 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.2 (this export) | prior_export=1.2 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.4 (this export) | prior_export=0.4 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WMT  ·  score **+16**  ·  Discount Stores
price=125.66999816894531  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.69 on 2026-11-11; prev RSI=63.48 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.48@2026-11-10 → 63.69@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.48@2026-11-10 → 63.69@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.48@2026-11-10 → 63.69@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=4.433 (G=1.3300 R=0.3000); 2026-11-10:RED:O=125.8500,C=125.5500,body=-0.3000,vol=17716792.0; 2026-11-11:GREEN:O=124.3400,C=125.6700,body=+1.3300,vol=19918341.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.124 (Gvol=19918341 Rvol=17716792); 2026-11-10:RED:O=125.8500,C=125.5500,body=-0.3000,vol=17716792.0; 2026-11-11:GREEN:O=124.3400,C=125.6700,body=+1.3300,vol=19918341.0 | **GOOD** |
| `A07_rvol` | RVOL=0.878 on 2026-11-11: today_vol=19918341 / avg20=22681653 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.436 on 2026-11-11 (price=125.6700, mid=122.3510, upper=129.9691, lower=114.7329; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=125.6700 vs SMA50=113.7422 dist=+10.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=122.3510 SMA50=113.7422 SMA80=112.3797 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-21@102.1500; S2[2026-09-15→2026-10-13] low=2026-10-05@103.3900; S3[2026-10-14→2026-11-11] low=2026-10-14@113.3800 | lows=[102.1500015258789, 103.38999938964844, 113.37999725341797] span=10.99% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.5964124793442152 wick_frac=0.4035875206557848 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.17543523071020015 wick_frac=0.8245647692897998 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.433407085272501 need>1.4; red_wick_gt_green=False 5d trail=2026-11-05:RED:body=-3.1750:wick=1.2450; 2026-11-06:GREEN:body=+1.5200:wick=2.0900; 2026-11-09:GREEN:body=+0.9400:wick=1.4700; 2026-11-10:RED:body=-0.3000:wick=1.4100; 2026-11-11:GREEN:body=+1.3300:wick=0.9000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.19 (current export asof; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 735840.0 | **NEUTRAL** |
| `B04_income` | 22076.0 | **GOOD** |
| `B05_profit_margin` | 3.0 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 131.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.1800000000000068 (now=131.8 vs prior_export=130.62 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 1.48 | **GOOD** |
| `B10_insider_transactions` | -0.26 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.26 vs prior=-0.26 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.29 | **GOOD** |
| `B13_short_float` | 1.65 | **NEUTRAL** |
| `B14_earnings_date` | 11/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.19 (this export) | prior_export=9.19 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HONA  ·  score **+16**  ·  Aerospace & Defense
price=169.52999877929688  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.76 on 2026-11-11; prev RSI=55.50 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.50@2026-11-10 → 57.76@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.50@2026-11-10 → 57.76@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.50@2026-11-10 → 57.76@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.0900 R=0.0000); 2026-11-10:GREEN:O=163.4400,C=166.4700,body=+3.0300,vol=2694076.0; 2026-11-11:GREEN:O=166.4700,C=169.5300,body=+3.0600,vol=2154159.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4848235 Rvol=0); 2026-11-10:GREEN:O=163.4400,C=166.4700,body=+3.0300,vol=2694076.0; 2026-11-11:GREEN:O=166.4700,C=169.5300,body=+3.0600,vol=2154159.0 | **GOOD** |
| `A07_rvol` | RVOL=0.605 on 2026-11-11: today_vol=2154159 / avg20=3560232 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.559 on 2026-11-11 (price=169.5300, mid=162.2630, upper=175.2711, lower=149.2549; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=169.5300 vs SMA50=159.7792 dist=+6.10% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=162.26_50=159.78_80=167.13 on 2026-11-11: SMA20=162.2630 SMA50=159.7792 SMA80=167.1296 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@149.1900; S2[2026-09-15→2026-10-13] low=2026-10-08@149.8800; S3[2026-10-14→2026-11-11] low=2026-11-06@147.4000 | lows=[149.19000244140625, 149.8800048828125, 147.39999389648438] span=1.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6692204551926545 wick_frac=0.3307795448073455 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-6.1800:wick=3.4600; 2026-11-06:GREEN:body=+3.6300:wick=3.2000; 2026-11-09:GREEN:body=+8.9000:wick=3.9500; 2026-11-10:GREEN:body=+3.0300:wick=1.8299; 2026-11-11:GREEN:body=+3.0600:wick=1.2199 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.34 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18338.0 | **NEUTRAL** |
| `B04_income` | 1898.0 | **GOOD** |
| `B05_profit_margin` | 10.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 188.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=188.43 vs prior_export=188.43 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.39 | **GOOD** |
| `B10_insider_transactions` | 7.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=7.21 vs prior=7.21 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 213.36 | **GOOD** |
| `B13_short_float` | 2.75 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.34 (this export) | prior_export=18.34 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOLS  ·  score **+16**  ·  Specialty Chemicals
price=67.1500015258789  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.71 on 2026-11-11; prev RSI=58.60 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.60@2026-11-10 → 61.71@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.60@2026-11-10 → 61.71@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.60@2026-11-10 → 61.71@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=32.714 (G=2.2900 R=0.0700); 2026-11-10:GREEN:O=62.7300,C=65.0200,body=+2.2900,vol=2735893.0; 2026-11-11:RED:O=67.2200,C=67.1500,body=-0.0700,vol=1964515.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.393 (Gvol=2735893 Rvol=1964515); 2026-11-10:GREEN:O=62.7300,C=65.0200,body=+2.2900,vol=2735893.0; 2026-11-11:RED:O=67.2200,C=67.1500,body=-0.0700,vol=1964515.0 | **GOOD** |
| `A07_rvol` | RVOL=0.736 on 2026-11-11: today_vol=1964515 / avg20=2669547 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.023 on 2026-11-11 (price=67.1500, mid=61.1925, upper=67.0159, lower=55.3691; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-11: price=67.1500 vs SMA50=60.0906 dist=+11.75% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=61.1925 SMA50=60.0906 SMA80=59.9840 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-24@54.5600; S2[2026-09-15→2026-10-13] low=2026-10-01@54.7850; S3[2026-10-14→2026-11-11] low=2026-11-05@54.8050 | lows=[54.560001373291016, 54.78499984741211, 54.80500030517578] span=0.45% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.9068221368234057 wick_frac=0.09317786317659428 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.03373484230110231 wick_frac=0.9662651576988976 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=32.71438692098093 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-1.4400:wick=1.5950; 2026-11-06:GREEN:body=+1.9100:wick=1.4300; 2026-11-09:GREEN:body=+4.3600:wick=0.3950; 2026-11-10:GREEN:body=+2.2900:wick=0.2353; 2026-11-11:RED:body=-0.0700:wick=2.0050 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.78 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.77 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4172.0 | **NEUTRAL** |
| `B04_income` | 325.0 | **GOOD** |
| `B05_profit_margin` | 7.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.9 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=78.9 vs prior_export=78.9 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.39 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.78 (this export) | prior_export=14.78 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GPN  ·  score **+16**  ·  Software - Infrastructure
price=100.2300033569336  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.61 on 2026-11-11; prev RSI=71.01 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 71.01@2026-11-10 → 67.61@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.01@2026-11-10 → 67.61@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 71.01@2026-11-10 → 67.61@2026-11-11 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=6.956 (G=3.1300 R=0.4500); 2026-11-10:GREEN:O=98.4600,C=101.5900,body=+3.1300,vol=3526850.0; 2026-11-11:RED:O=100.6800,C=100.2300,body=-0.4500,vol=3184024.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.108 (Gvol=3526850 Rvol=3184024); 2026-11-10:GREEN:O=98.4600,C=101.5900,body=+3.1300,vol=3526850.0; 2026-11-11:RED:O=100.6800,C=100.2300,body=-0.4500,vol=3184024.0 | **GOOD** |
| `A07_rvol` | RVOL=0.934 on 2026-11-11: today_vol=3184024 / avg20=3410636 (avg window 2026-10-09→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.668 on 2026-11-11 (price=100.2300, mid=90.1230, upper=105.2606, lower=74.9854; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=100.2300 vs SMA50=87.7234 dist=+14.26% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=90.1230 SMA50=87.7234 SMA80=87.5322 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-11 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-12@84.2000; S2[2026-09-10→2026-10-08] low=2026-10-02@77.9300; S3[2026-10-09→2026-11-11] low=2026-10-28@80.8500 | lows=[84.19999694824219, 77.93000030517578, 80.8499984741211] span=8.05% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6071771085549879 wick_frac=0.3928228914450121 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.25294947614901986 wick_frac=0.7470505238509801 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.95559662269845 need>1.4; red_wick_gt_green=False 5d trail=2026-11-05:RED:body=-1.9600:wick=2.8000; 2026-11-06:GREEN:body=+3.3700:wick=1.8800; 2026-11-09:RED:body=-0.5900:wick=1.3800; 2026-11-10:GREEN:body=+3.1300:wick=2.0250; 2026-11-11:RED:body=-0.4500:wick=1.3290 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.09 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.01 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11493.52 | **NEUTRAL** |
| `B04_income` | 197.89 | **GOOD** |
| `B05_profit_margin` | 1.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4399999999999977 (now=109.47 vs prior_export=109.03 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 2.24 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.29 | **GOOD** |
| `B13_short_float` | 6.52 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.09 (this export) | prior_export=0.09 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.01 (this export) | prior_export=0.01 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RMD  ·  score **+15**  ·  Medical Instruments & Supplies
price=238.52999877929688  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.47 on 2026-11-11; prev RSI=47.22 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.22@2026-11-10 → 51.47@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.22@2026-11-10 → 51.47@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.22@2026-11-10 → 51.47@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=20.789 (G=7.9000 R=0.3800); 2026-11-10:RED:O=234.4900,C=234.1100,body=-0.3800,vol=1157918.0; 2026-11-11:GREEN:O=230.6300,C=238.5300,body=+7.9000,vol=927625.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=0.801 (Gvol=927625 Rvol=1157918); 2026-11-10:RED:O=234.4900,C=234.1100,body=-0.3800,vol=1157918.0; 2026-11-11:GREEN:O=230.6300,C=238.5300,body=+7.9000,vol=927625.0 | **BAD** |
| `A07_rvol` | RVOL=0.621 on 2026-11-11: today_vol=927625 / avg20=1493314 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.208 on 2026-11-11 (price=238.5300, mid=241.9525, upper=258.4270, lower=225.4780; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=238.5300 vs SMA50=231.7896 dist=+2.91% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=241.9525 SMA50=231.7896 SMA80=227.6260 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@215.8000; S2[2026-09-15→2026-10-13] low=2026-10-02@218.0300; S3[2026-10-14→2026-11-11] low=2026-11-03@221.5900 | lows=[215.8000030517578, 218.02999877929688, 221.58999633789062] span=2.68% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.9196737211210863 wick_frac=0.08032627887891373 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.0804772276331862 wick_frac=0.9195227723668138 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.78919049148731 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-5.9800:wick=7.2100; 2026-11-06:GREEN:body=+1.3000:wick=3.4200; 2026-11-09:RED:body=-0.8300:wick=5.7000; 2026-11-10:RED:body=-0.3800:wick=4.3419; 2026-11-11:GREEN:body=+7.9000:wick=0.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.33 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.47 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5732.68 | **NEUTRAL** |
| `B04_income` | 1593.17 | **GOOD** |
| `B05_profit_margin` | 27.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 272.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=272.76 vs prior_export=272.76 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | -2.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.89 vs prior=-2.89 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.4 | **GOOD** |
| `B13_short_float` | 7.52 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.33 (this export) | prior_export=8.33 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.47 (this export) | prior_export=1.47 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TEVA  ·  score **+15**  ·  Drug Manufacturers - Specialty & Generic
price=43.2400016784668  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.89 on 2026-11-11; prev RSI=59.84 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.84@2026-11-10 → 63.89@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.84@2026-11-10 → 63.89@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.84@2026-11-10 → 63.89@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=8.834 (G=0.5300 R=0.0600); 2026-11-10:RED:O=42.4600,C=42.4000,body=-0.0600,vol=6216390.0; 2026-11-11:GREEN:O=42.7100,C=43.2400,body=+0.5300,vol=9546939.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.536 (Gvol=9546939 Rvol=6216390); 2026-11-10:RED:O=42.4600,C=42.4000,body=-0.0600,vol=6216390.0; 2026-11-11:GREEN:O=42.7100,C=43.2400,body=+0.5300,vol=9546939.0 | **GOOD** |
| `A07_rvol` | RVOL=1.377 on 2026-11-11: today_vol=9546939 / avg20=6935640 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.908 on 2026-11-11 (price=43.2400, mid=41.3170, upper=43.4352, lower=39.1988; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-11: price=43.2400 vs SMA50=39.9024 dist=+8.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=41.3170 SMA50=39.9024 SMA80=38.1584 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-08@35.4200; S2[2026-09-15→2026-10-13] low=2026-09-15@38.1100; S3[2026-10-14→2026-11-11] low=2026-10-23@39.1050 | lows=[35.41999816894531, 38.11000061035156, 39.10499954223633] span=10.40% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6235307845240392 wick_frac=0.37646921547596074 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.057690121006936164 wick_frac=0.9423098789930638 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.833736012207527 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:GREEN:body=+0.2900:wick=1.1653; 2026-11-06:GREEN:body=+1.1400:wick=0.0800; 2026-11-09:RED:body=-0.0400:wick=0.4800; 2026-11-10:RED:body=-0.0600:wick=0.9800; 2026-11-11:GREEN:body=+0.5300:wick=0.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.31 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.85 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17239.0 | **NEUTRAL** |
| `B04_income` | 506.0 | **GOOD** |
| `B05_profit_margin` | 2.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.42 vs prior_export=50.42 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.31 | **GOOD** |
| `B10_insider_transactions` | -1.46 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.46 vs prior=-1.46 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.62 | **GOOD** |
| `B13_short_float` | 2.5 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.31 (this export) | prior_export=19.31 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.85 (this export) | prior_export=3.85 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TAK  ·  score **+15**  ·  Drug Manufacturers - Specialty & Generic
price=20.84000015258789  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.49 on 2026-11-11; prev RSI=65.58 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.58@2026-11-10 → 63.49@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.58@2026-11-10 → 63.49@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.58@2026-11-10 → 63.49@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1000 R=0.0000); 2026-11-10:GREEN:O=20.8800,C=20.9300,body=+0.0500,vol=2950636.0; 2026-11-11:GREEN:O=20.7900,C=20.8400,body=+0.0500,vol=1961348.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4911984 Rvol=0); 2026-11-10:GREEN:O=20.8800,C=20.9300,body=+0.0500,vol=2950636.0; 2026-11-11:GREEN:O=20.7900,C=20.8400,body=+0.0500,vol=1961348.0 | **GOOD** |
| `A07_rvol` | RVOL=0.636 on 2026-11-11: today_vol=1961348 / avg20=3086041 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.366 on 2026-11-11 (price=20.8400, mid=20.4430, upper=21.5291, lower=19.3569; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=20.8400 vs SMA50=19.2882 dist=+8.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=20.4430 SMA50=19.2882 SMA80=18.6480 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-17@17.2800; S2[2026-09-15→2026-10-13] low=2026-10-05@17.7500; S3[2026-10-14→2026-11-11] low=2026-10-14@18.5700 | lows=[17.280000686645508, 17.75, 18.56999969482422] span=7.47% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.24305568433738342 wick_frac=0.7569443156626166 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-0.3400:wick=0.1950; 2026-11-06:GREEN:body=+0.4000:wick=0.1200; 2026-11-09:RED:body=-0.0500:wick=0.2600; 2026-11-10:GREEN:body=+0.0500:wick=0.1900; 2026-11-11:GREEN:body=+0.0500:wick=0.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=164.69 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.72 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 29820.42 | **NEUTRAL** |
| `B04_income` | 242.88 | **GOOD** |
| `B05_profit_margin` | 0.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.08999999999999986 (now=21.26 vs prior_export=21.17 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.07 | **GOOD** |
| `B13_short_float` | 0.1 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=164.69 (this export) | prior_export=164.69 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.72 (this export) | prior_export=2.72 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+15**  ·  Healthcare Plans
price=446.30999755859375  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.80 on 2026-11-11; prev RSI=60.89 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.89@2026-11-10 → 63.80@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.89@2026-11-10 → 63.80@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.89@2026-11-10 → 63.80@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=15.9900 R=0.0000); 2026-11-10:GREEN:O=425.3600,C=438.0600,body=+12.7000,vol=2085418.0; 2026-11-11:GREEN:O=443.0200,C=446.3100,body=+3.2900,vol=2497258.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4582676 Rvol=0); 2026-11-10:GREEN:O=425.3600,C=438.0600,body=+12.7000,vol=2085418.0; 2026-11-11:GREEN:O=443.0200,C=446.3100,body=+3.2900,vol=2497258.0 | **GOOD** |
| `A07_rvol` | RVOL=1.758 on 2026-11-11: today_vol=2497258 / avg20=1420405 (avg window 2026-10-14→2026-11-10, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.277 on 2026-11-11 (price=446.3100, mid=421.5500, upper=440.9434, lower=402.1566; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-11: price=446.3100 vs SMA50=406.9522 dist=+9.67% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=421.5500 SMA50=406.9522 SMA80=397.9156 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-19@371.5000; S2[2026-09-15→2026-10-13] low=2026-09-23@369.1300; S3[2026-10-14→2026-11-11] low=2026-11-02@399.3500 | lows=[371.5, 369.1300048828125, 399.3500061035156] span=8.19% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.4535166637556983 wick_frac=0.5464833362443017 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-5.6800:wick=12.8350; 2026-11-06:GREEN:body=+18.7400:wick=13.7000; 2026-11-09:RED:body=-15.3500:wick=10.7600; 2026-11-10:GREEN:body=+12.7000:wick=7.6400; 2026-11-11:GREEN:body=+3.2900:wick=8.3500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.06 (current export asof; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.69 (current export; earnings_date=11/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 154185.0 | **NEUTRAL** |
| `B04_income` | 1246.0 | **GOOD** |
| `B05_profit_margin` | 0.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 495.87 | **NEUTRAL** |
| `B08_target_price_delta` | delta=5.649999999999977 (now=495.87 vs prior_export=490.22 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 2.03 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.53 | **GOOD** |
| `B13_short_float` | 2.32 | **NEUTRAL** |
| `B14_earnings_date` | 11/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.06 (this export) | prior_export=30.06 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.69 (this export) | prior_export=0.69 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBA  ·  score **+15**  ·  Oil & Gas Midstream
price=49.900001525878906  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.20 on 2026-11-11; prev RSI=51.48 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.48@2026-11-10 → 56.20@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.48@2026-11-10 → 56.20@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.48@2026-11-10 → 56.20@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.4700 R=0.0000); 2026-11-10:GREEN:O=48.3100,C=49.0300,body=+0.7200,vol=486409.0; 2026-11-11:GREEN:O=49.1500,C=49.9000,body=+0.7500,vol=638668.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1125077 Rvol=0); 2026-11-10:GREEN:O=48.3100,C=49.0300,body=+0.7200,vol=486409.0; 2026-11-11:GREEN:O=49.1500,C=49.9000,body=+0.7500,vol=638668.0 | **GOOD** |
| `A07_rvol` | RVOL=0.701 on 2026-11-11: today_vol=638668 / avg20=911258 (avg window 2026-10-09→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.129 on 2026-11-11 (price=49.9000, mid=49.6410, upper=51.6515, lower=47.6305; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=49.9000 vs SMA50=48.0528 dist=+3.84% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=49.64_50=48.05_80=48.50 on 2026-11-11: SMA20=49.6410 SMA50=48.0528 SMA80=48.5026 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-11 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-11@47.8100; S2[2026-09-10→2026-10-08] low=2026-10-01@44.1300; S3[2026-10-09→2026-11-11] low=2026-10-09@46.9900 | lows=[47.810001373291016, 44.130001068115234, 46.9900016784668] span=8.34% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.8939955844900895 wick_frac=0.10600441550991045 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-1.3700:wick=0.6300; 2026-11-06:GREEN:body=+0.8900:wick=0.7700; 2026-11-09:GREEN:body=+0.4000:wick=0.7200; 2026-11-10:GREEN:body=+0.7200:wick=0.5250; 2026-11-11:GREEN:body=+0.7500:wick=-0.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=21.66 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.39 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5931.4 | **NEUTRAL** |
| `B04_income` | 1397.06 | **GOOD** |
| `B05_profit_margin` | 23.55 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 52.49 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.010000000000005116 (now=52.49 vs prior_export=52.48 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.0 | **GOOD** |
| `B13_short_float` | 1.84 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=21.66 (this export) | prior_export=21.66 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.39 (this export) | prior_export=10.39 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSY  ·  score **+15**  ·  Confectioners
price=176.42999267578125  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.26 on 2026-11-11; prev RSI=55.57 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.57@2026-11-10 → 58.26@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.57@2026-11-10 → 58.26@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.57@2026-11-10 → 58.26@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.9800 R=0.0000); 2026-11-10:GREEN:O=172.8200,C=173.9500,body=+1.1300,vol=1544354.0; 2026-11-11:GREEN:O=171.5800,C=176.4300,body=+4.8500,vol=3191243.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4735597 Rvol=0); 2026-11-10:GREEN:O=172.8200,C=173.9500,body=+1.1300,vol=1544354.0; 2026-11-11:GREEN:O=171.5800,C=176.4300,body=+4.8500,vol=3191243.0 | **GOOD** |
| `A07_rvol` | RVOL=1.671 on 2026-11-11: today_vol=3191243 / avg20=1909367 (avg window 2026-10-14→2026-11-10, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.638 on 2026-11-11 (price=176.4300, mid=170.2120, upper=179.9528, lower=160.4712; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=176.4300 vs SMA50=168.3112 dist=+4.82% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=170.21_50=168.31_80=172.88 on 2026-11-11: SMA20=170.2120 SMA50=168.3112 SMA80=172.8765 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-09@169.7100; S2[2026-09-15→2026-10-13] low=2026-10-01@156.1600; S3[2026-10-14→2026-11-11] low=2026-10-29@159.8900 | lows=[169.7100067138672, 156.16000366210938, 159.88999938964844] span=8.68% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6787964827396387 wick_frac=0.3212035172603613 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:GREEN:body=+0.3100:wick=5.2500; 2026-11-06:GREEN:body=+6.8500:wick=1.7500; 2026-11-09:RED:body=-3.4200:wick=3.8100; 2026-11-10:GREEN:body=+1.1300:wick=2.0300; 2026-11-11:GREEN:body=+4.8500:wick=0.0000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.04 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.79 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 12258.0 | **NEUTRAL** |
| `B04_income` | 1793.6 | **GOOD** |
| `B05_profit_margin` | 14.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 178.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=178.11 vs prior_export=178.11 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.46 | **GOOD** |
| `B10_insider_transactions` | -2.25 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.25 vs prior=-2.25 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.88 | **GOOD** |
| `B13_short_float` | 6.6 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.04 (this export) | prior_export=25.04 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.79 (this export) | prior_export=0.79 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-12_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.