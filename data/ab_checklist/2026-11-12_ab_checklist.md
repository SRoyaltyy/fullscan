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
| 1 | RSG | +17 | 17 | 0 | 2026-11-10→2026-11-11 | Waste Management |
| 2 | ZM | +17 | 18 | 1 | 2026-11-10→2026-11-11 | Software - Application |
| 3 | MSCI | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Financial Data & Stock Exchanges |
| 4 | ABBV | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Drug Manufacturers - General |
| 5 | V | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Credit Services |
| 6 | WMT | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Discount Stores |
| 7 | GPN | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Software - Infrastructure |
| 8 | SOLS | +16 | 17 | 1 | 2026-11-10→2026-11-11 | Specialty Chemicals |
| 9 | HONA | +16 | 16 | 0 | 2026-11-10→2026-11-11 | Aerospace & Defense |
| 10 | PRCH | +15 | 17 | 2 | 2026-11-10→2026-11-11 | Insurance - Property & Casualty |
| 11 | UHS | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Medical Care Facilities |
| 12 | HAE | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Medical Devices |
| 13 | DOCS | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Health Information Services |
| 14 | UPBD | +15 | 16 | 1 | 2026-11-10→2026-11-11 | Software - Application |
| 15 | ED | +15 | 15 | 0 | 2026-11-10→2026-11-11 | Utilities - Regulated Electric |

## Full checklist — top 15

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

### PRCH  ·  score **+15**  ·  Insurance - Property & Casualty
price=22.139999389648438  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.67 on 2026-11-11; prev RSI=65.01 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.01@2026-11-10 → 65.67@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.01@2026-11-10 → 65.67@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.01@2026-11-10 → 65.67@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=45.999 (G=1.3800 R=0.0300); 2026-11-10:GREEN:O=20.5700,C=21.9500,body=+1.3800,vol=4078000.0; 2026-11-11:RED:O=22.1700,C=22.1400,body=-0.0300,vol=1207569.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=3.377 (Gvol=4078000 Rvol=1207569); 2026-11-10:GREEN:O=20.5700,C=21.9500,body=+1.3800,vol=4078000.0; 2026-11-11:RED:O=22.1700,C=22.1400,body=-0.0300,vol=1207569.0 | **GOOD** |
| `A07_rvol` | RVOL=0.509 on 2026-11-11: today_vol=1207569 / avg20=2370116 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.777 on 2026-11-11 (price=22.1400, mid=19.2985, upper=22.9536, lower=15.6434; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=22.1400 vs SMA50=17.8322 dist=+24.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=19.2985 SMA50=17.8322 SMA80=17.0076 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@15.9000; S2[2026-09-15→2026-10-13] low=2026-09-18@15.4900; S3[2026-10-14→2026-11-11] low=2026-10-14@16.0750 | lows=[15.899999618530273, 15.489999771118164, 16.075000762939453] span=3.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.7540989757641642 wick_frac=0.24590102423583585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.044118637031718075 wick_frac=0.955881362968282 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=45.99898277067837 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-1.2500:wick=0.4200; 2026-11-06:RED:body=-0.4900:wick=0.4000; 2026-11-09:GREEN:body=+0.8800:wick=0.0300; 2026-11-10:GREEN:body=+1.3800:wick=0.4500; 2026-11-11:RED:body=-0.0300:wick=0.6500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=599.3 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.56 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 530.17 | **NEUTRAL** |
| `B04_income` | 9.6 | **GOOD** |
| `B05_profit_margin` | 1.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.12 vs prior_export=25.12 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | -7.26 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.3899999999999997 (now=-7.26 vs prior=-6.87 on finviz_2026-11-11) | **BAD** |
| `B12_institutional_transactions` | 3.14 | **GOOD** |
| `B13_short_float` | 19.16 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=599.3 (this export) | prior_export=599.3 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.56 (this export) | prior_export=4.56 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UHS  ·  score **+15**  ·  Medical Care Facilities
price=196.77000427246094  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.86 on 2026-11-11; prev RSI=61.80 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.80@2026-11-10 → 62.86@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.80@2026-11-10 → 62.86@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.80@2026-11-10 → 62.86@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=1.199 (G=3.7400 R=3.1200); 2026-11-10:RED:O=198.5300,C=195.4100,body=-3.1200,vol=533498.0; 2026-11-11:GREEN:O=193.0300,C=196.7700,body=+3.7400,vol=539727.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.012 (Gvol=539727 Rvol=533498); 2026-11-10:RED:O=198.5300,C=195.4100,body=-3.1200,vol=533498.0; 2026-11-11:GREEN:O=193.0300,C=196.7700,body=+3.7400,vol=539727.0 | **GOOD** |
| `A07_rvol` | RVOL=0.702 on 2026-11-11: today_vol=539727 / avg20=768368 (avg window 2026-10-09→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.658 on 2026-11-11 (price=196.7700, mid=185.7630, upper=202.4854, lower=169.0406; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=196.7700 vs SMA50=179.3734 dist=+9.70% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=185.7630 SMA50=179.3734 SMA80=174.4593 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-11 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-12@164.9000; S2[2026-09-10→2026-10-08] low=2026-09-15@169.0400; S3[2026-10-09→2026-11-11] low=2026-10-28@169.1300 | lows=[164.89999389648438, 169.0399932861328, 169.1300048828125] span=2.57% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6493071284574038 wick_frac=0.35069287154259615 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.8763999674251743 wick_frac=0.12360003257482566 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.1987215853515396 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-1.4100:wick=9.3300; 2026-11-06:GREEN:body=+2.9400:wick=2.6700; 2026-11-09:GREEN:body=+0.6600:wick=2.3000; 2026-11-10:RED:body=-3.1200:wick=0.4400; 2026-11-11:GREEN:body=+3.7400:wick=2.0200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.54 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.51 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18352.59 | **NEUTRAL** |
| `B04_income` | 1461.75 | **GOOD** |
| `B05_profit_margin` | 7.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 195.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.0600000000000023 (now=195.75 vs prior_export=192.69 on finviz_2026-11-11) | **GOOD** |
| `B09_analyst_recom` | 2.48 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 5.22 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.54 (this export) | prior_export=0.54 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAE  ·  score **+15**  ·  Medical Devices
price=120.54000091552734  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.09 on 2026-11-11; prev RSI=58.74 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.74@2026-11-10 → 55.09@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.74@2026-11-10 → 55.09@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.74@2026-11-10 → 55.09@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=1.749 (G=4.4600 R=2.5500); 2026-11-10:GREEN:O=117.9000,C=122.3600,body=+4.4600,vol=787799.0; 2026-11-11:RED:O=123.0900,C=120.5400,body=-2.5500,vol=390120.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=2.019 (Gvol=787799 Rvol=390120); 2026-11-10:GREEN:O=117.9000,C=122.3600,body=+4.4600,vol=787799.0; 2026-11-11:RED:O=123.0900,C=120.5400,body=-2.5500,vol=390120.0 | **GOOD** |
| `A07_rvol` | RVOL=0.555 on 2026-11-11: today_vol=390120 / avg20=702668 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.126 on 2026-11-11 (price=120.5400, mid=119.9100, upper=124.9228, lower=114.8972; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=120.5400 vs SMA50=112.5132 dist=+7.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=119.9100 SMA50=112.5132 SMA80=105.0399 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-17@89.0600; S2[2026-09-15→2026-10-13] low=2026-10-07@101.3300; S3[2026-10-14→2026-11-11] low=2026-10-23@115.4900 | lows=[89.05999755859375, 101.33000183105469, 115.48999786376953] span=29.68% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.6990597185732786 wick_frac=0.3009402814267213 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.838814031054638 wick_frac=0.161185968945362 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7490223885732408 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-1.9500:wick=8.8300; 2026-11-06:RED:body=-1.2300:wick=5.7300; 2026-11-09:RED:body=-2.9500:wick=2.5400; 2026-11-10:GREEN:body=+4.4600:wick=1.9200; 2026-11-11:RED:body=-2.5500:wick=0.4900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.56 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.65 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1382.46 | **NEUTRAL** |
| `B04_income` | 103.91 | **GOOD** |
| `B05_profit_margin` | 7.52 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 139.4 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=139.4 vs prior_export=139.4 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.35 | **GOOD** |
| `B13_short_float` | 8.06 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.56 (this export) | prior_export=4.56 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.65 (this export) | prior_export=3.65 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DOCS  ·  score **+15**  ·  Health Information Services
price=33.220001220703125  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.78 on 2026-11-11; prev RSI=55.01 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.01@2026-11-10 → 58.78@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.01@2026-11-10 → 58.78@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.01@2026-11-10 → 58.78@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=12.285 (G=0.8600 R=0.0700); 2026-11-10:RED:O=32.4900,C=32.4200,body=-0.0700,vol=2134197.0; 2026-11-11:GREEN:O=32.3600,C=33.2200,body=+0.8600,vol=2277616.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.067 (Gvol=2277616 Rvol=2134197); 2026-11-10:RED:O=32.4900,C=32.4200,body=-0.0700,vol=2134197.0; 2026-11-11:GREEN:O=32.3600,C=33.2200,body=+0.8600,vol=2277616.0 | **GOOD** |
| `A07_rvol` | RVOL=0.816 on 2026-11-11: today_vol=2277616 / avg20=2791560 (avg window 2026-10-09→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.152 on 2026-11-11 (price=33.2200, mid=32.9150, upper=34.9166, lower=30.9134; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=33.2200 vs SMA50=29.2264 dist=+13.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=32.9150 SMA50=29.2264 SMA80=27.0652 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-11→2026-11-11 (63 bars); S1[2026-08-11→2026-09-09] low=2026-08-17@23.9510; S2[2026-09-10→2026-10-08] low=2026-09-10@24.0700; S3[2026-10-09→2026-11-11] low=2026-10-09@29.1650 | lows=[23.951000213623047, 24.06999969482422, 29.165000915527344] span=21.77% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.5149698136739602 wick_frac=0.4850301863260397 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.07609076490893447 wick_frac=0.9239092350910655 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.285107078633317 need>1.4; red_wick_gt_green=False 5d trail=2026-11-05:RED:body=-1.5800:wick=0.6100; 2026-11-06:GREEN:body=+2.2500:wick=0.5700; 2026-11-09:GREEN:body=+0.6300:wick=0.5600; 2026-11-10:RED:body=-0.0700:wick=0.8500; 2026-11-11:GREEN:body=+0.8600:wick=0.8100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.36 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.17 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 661.58 | **NEUTRAL** |
| `B04_income` | 139.72 | **GOOD** |
| `B05_profit_margin` | 21.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 31.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=31.16 vs prior_export=31.16 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 2.48 | **GOOD** |
| `B10_insider_transactions` | -0.07 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.07 vs prior=-0.07 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.89 | **GOOD** |
| `B13_short_float` | 5.67 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.17 (this export) | prior_export=2.17 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UPBD  ·  score **+15**  ·  Software - Application
price=25.270000457763672  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=81.37 on 2026-11-11; prev RSI=79.93 on 2026-11-10 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.93@2026-11-10 → 81.37@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.93@2026-11-10 → 81.37@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.93@2026-11-10 → 81.37@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=10.000 (G=0.5000 R=0.0500); 2026-11-10:RED:O=24.7100,C=24.6600,body=-0.0500,vol=1450016.0; 2026-11-11:GREEN:O=24.7700,C=25.2700,body=+0.5000,vol=1751140.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=1.208 (Gvol=1751140 Rvol=1450016); 2026-11-10:RED:O=24.7100,C=24.6600,body=-0.0500,vol=1450016.0; 2026-11-11:GREEN:O=24.7700,C=25.2700,body=+0.5000,vol=1751140.0 | **GOOD** |
| `A07_rvol` | RVOL=1.204 on 2026-11-11: today_vol=1751140 / avg20=1455027 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.711 on 2026-11-11 (price=25.2700, mid=20.0220, upper=27.3980, lower=12.6460; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=25.2700 vs SMA50=18.0577 dist=+39.94% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=20.02_50=18.06_80=18.71 on 2026-11-11: SMA20=20.0220 SMA50=18.0577 SMA80=18.7141 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-09-10@17.7900; S2[2026-09-15→2026-10-13] low=2026-10-07@14.9800; S3[2026-10-14→2026-11-11] low=2026-10-14@15.1500 | lows=[17.790000915527344, 14.979999542236328, 15.149999618530273] span=18.76% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.537634232179627 wick_frac=0.4623657678203729 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=0.07673303554179131 wick_frac=0.9232669644582087 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.000152590218967 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-0.0100:wick=0.9549; 2026-11-06:RED:body=-0.2600:wick=0.9000; 2026-11-09:GREEN:body=+0.1400:wick=0.9600; 2026-11-10:RED:body=-0.0500:wick=0.6016; 2026-11-11:GREEN:body=+0.5000:wick=0.4300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.29 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.99 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4725.76 | **NEUTRAL** |
| `B04_income` | 111.39 | **GOOD** |
| `B05_profit_margin` | 2.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 27.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=27.33 vs prior_export=27.33 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 3.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.01 vs prior=3.01 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 14.95 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.29 (this export) | prior_export=11.29 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.99 (this export) | prior_export=1.99 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ED  ·  score **+15**  ·  Utilities - Regulated Electric
price=119.19000244140625  pair=`2026-11-10→2026-11-11`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.62 on 2026-11-11; prev RSI=64.95 on 2026-11-10 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.95@2026-11-10 → 64.62@2026-11-11 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.95@2026-11-10 → 64.62@2026-11-11 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.95@2026-11-10 → 64.62@2026-11-11 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.4700 R=0.0000); 2026-11-10:GREEN:O=117.7700,C=119.2800,body=+1.5100,vol=2055524.0; 2026-11-11:GREEN:O=118.2300,C=119.1900,body=+0.9600,vol=1724200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-10 + 2026-11-11; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3779724 Rvol=0); 2026-11-10:GREEN:O=117.7700,C=119.2800,body=+1.5100,vol=2055524.0; 2026-11-11:GREEN:O=118.2300,C=119.1900,body=+0.9600,vol=1724200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.718 on 2026-11-11: today_vol=1724200 / avg20=2400416 (avg window 2026-10-14→2026-11-10, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.494 on 2026-11-11 (price=119.1900, mid=115.8395, upper=122.6174, lower=109.0616; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-11: price=119.1900 vs SMA50=109.5926 dist=+8.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-11: SMA20=115.8395 SMA50=109.5926 SMA80=109.2736 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-14→2026-11-11 (63 bars); S1[2026-08-14→2026-09-14] low=2026-08-24@105.5100; S2[2026-09-15→2026-10-13] low=2026-09-29@102.0000; S3[2026-10-14→2026-11-11] low=2026-10-14@107.2000 | lows=[105.51000213623047, 102.0, 107.19999694824219] span=5.10% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: GREEN body_frac=0.5648331335814225 wick_frac=0.4351668664185775 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-10+2026-11-11: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-05:RED:body=-0.5700:wick=2.0370; 2026-11-06:RED:body=-1.3700:wick=1.2200; 2026-11-09:RED:body=-0.9300:wick=1.7600; 2026-11-10:GREEN:body=+1.5100:wick=0.8100; 2026-11-11:GREEN:body=+0.9600:wick=1.0450 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.78 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.16 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 18072.0 | **NEUTRAL** |
| `B04_income` | 2295.0 | **GOOD** |
| `B05_profit_margin` | 12.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 109.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=109.47 vs prior_export=109.47 on finviz_2026-11-11) | **NEUTRAL** |
| `B09_analyst_recom` | 3.32 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-11) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.15 | **GOOD** |
| `B13_short_float` | 3.1 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=1.78 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.16 (this export) | prior_export=3.16 (finviz_2026-11-11) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-12_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.