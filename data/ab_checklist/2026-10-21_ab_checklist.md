# A+B1 Feature Checklist — 2026-10-21

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-21.csv` · prior export for Δ: `2026-10-20`
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
| 1 | AUPH | +17 | 17 | 0 | 2026-10-19→2026-10-20 | Biotechnology |
| 2 | PAGP | +17 | 17 | 0 | 2026-10-19→2026-10-20 | Oil & Gas Midstream |
| 3 | ICLR | +16 | 17 | 1 | 2026-10-19→2026-10-20 | Diagnostics & Research |
| 4 | OXY | +16 | 16 | 0 | 2026-10-19→2026-10-20 | Oil & Gas E&P |
| 5 | BB | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 6 | HAS | +15 | 18 | 3 | 2026-10-19→2026-10-20 | Leisure |
| 7 | FIVN | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 8 | MH | +15 | 17 | 2 | 2026-10-19→2026-10-20 | Education & Training Services |
| 9 | SN | +15 | 17 | 2 | 2026-10-19→2026-10-20 | Furnishings, Fixtures & Appliances |
| 10 | MSFT | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 11 | TRI | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Specialty Business Services |
| 12 | ISRG | +15 | 17 | 2 | 2026-10-19→2026-10-20 | Medical Instruments & Supplies |
| 13 | FOXA | +14 | 15 | 1 | 2026-10-19→2026-10-20 | Entertainment |
| 14 | AMZN | +14 | 16 | 2 | 2026-10-19→2026-10-20 | Internet Retail |
| 15 | ANF | +14 | 17 | 3 | 2026-10-19→2026-10-20 | Apparel Retail |

## Full checklist — top 15

### AUPH  ·  score **+17**  ·  Biotechnology
price=16.739999771118164  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.60 on 2026-10-20; prev RSI=54.48 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.48@2026-10-19 → 58.60@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.48@2026-10-19 → 58.60@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.48@2026-10-19 → 58.60@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=2.800 (G=0.1400 R=0.0500); 2026-10-19:RED:O=16.5400,C=16.4900,body=-0.0500,vol=552200.0; 2026-10-20:GREEN:O=16.6000,C=16.7400,body=+0.1400,vol=593600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.075 (Gvol=593600 Rvol=552200); 2026-10-19:RED:O=16.5400,C=16.4900,body=-0.0500,vol=552200.0; 2026-10-20:GREEN:O=16.6000,C=16.7400,body=+0.1400,vol=593600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.733 on 2026-10-20: today_vol=593600 / avg20=809854 (avg window 2026-09-16→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.484 on 2026-10-20 (price=16.7400, mid=16.3780, upper=17.1256, lower=15.6304; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=16.7400 vs SMA50=16.1927 dist=+3.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=16.3780 SMA50=16.1927 SMA80=16.1269 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-20 (63 bars); S1[2026-07-16→2026-08-14] low=2026-07-31@14.3630; S2[2026-08-17→2026-09-15] low=2026-08-17@15.0700; S3[2026-09-16→2026-10-20] low=2026-10-06@15.4700 | lows=[14.36299991607666, 15.069999694824219, 15.470000267028809] span=7.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.5204418792631564 wick_frac=0.4795581207368436 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.17241832902534152 wick_frac=0.8275816709746585 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.799923707800877 need>1.4; red_wick_gt_green=False 5d trail=2026-10-08:GREEN:body=+0.3100:wick=0.1900; 2026-10-09:GREEN:body=+0.4900:wick=0.3600; 2026-10-12:RED:body=-0.0600:wick=0.1578; 2026-10-19:RED:body=-0.0500:wick=0.2400; 2026-10-20:GREEN:body=+0.1400:wick=0.1290 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.67 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.45 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 311.51 | **NEUTRAL** |
| `B04_income` | 314.11 | **GOOD** |
| `B05_profit_margin` | 100.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=18.0 vs prior_export=18.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.53 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.53 vs prior=5.53 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 7.26 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.67 (this export) | prior_export=16.67 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.45 (this export) | prior_export=3.45 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAGP  ·  score **+17**  ·  Oil & Gas Midstream
price=27.510000228881836  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.83 on 2026-10-20; prev RSI=61.03 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.03@2026-10-19 → 63.83@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.03@2026-10-19 → 63.83@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.03@2026-10-19 → 63.83@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=17.000 (G=0.1700 R=0.0100); 2026-10-19:RED:O=27.3200,C=27.3100,body=-0.0100,vol=1711200.0; 2026-10-20:GREEN:O=27.3400,C=27.5100,body=+0.1700,vol=2625300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.534 (Gvol=2625300 Rvol=1711200); 2026-10-19:RED:O=27.3200,C=27.3100,body=-0.0100,vol=1711200.0; 2026-10-20:GREEN:O=27.3400,C=27.5100,body=+0.1700,vol=2625300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.690 on 2026-10-20: today_vol=2625300 / avg20=1553756 (avg window 2026-09-16→2026-10-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.728 on 2026-10-20 (price=27.5100, mid=26.6792, upper=27.8206, lower=25.5379; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=27.5100 vs SMA50=26.8393 dist=+2.50% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=26.68_50=26.84_80=26.02 on 2026-10-20: SMA20=26.6792 SMA50=26.8393 SMA80=26.0209 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-20 (63 bars); S1[2026-07-15→2026-08-14] low=2026-08-07@24.6700; S2[2026-08-17→2026-09-15] low=2026-08-17@25.6900; S3[2026-09-16→2026-10-20] low=2026-10-02@25.6200 | lows=[24.670000076293945, 25.690000534057617, 25.6200008392334] span=4.13% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.6800003051757812 wick_frac=0.31999969482421875 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.023810064441124247 wick_frac=0.9761899355588758 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=16.999618539004388 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.1800:wick=0.1900; 2026-10-09:RED:body=-0.0100:wick=0.4450; 2026-10-12:GREEN:body=+0.0250:wick=0.2200; 2026-10-19:RED:body=-0.0100:wick=0.4100; 2026-10-20:GREEN:body=+0.1700:wick=0.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=352.15 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=37.53 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 554.0 | **GOOD** |
| `B05_profit_margin` | 1.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.08 vs prior_export=26.08 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.38 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.39 | **GOOD** |
| `B13_short_float` | 9.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=352.15 (this export) | prior_export=352.15 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=37.53 (this export) | prior_export=37.53 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ICLR  ·  score **+16**  ·  Diagnostics & Research
price=174.19000244140625  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.04 on 2026-10-20; prev RSI=51.90 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.90@2026-10-19 → 58.04@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.90@2026-10-19 → 58.04@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.90@2026-10-19 → 58.04@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=4.441 (G=4.9300 R=1.1100); 2026-10-19:RED:O=169.2500,C=168.1400,body=-1.1100,vol=728500.0; 2026-10-20:GREEN:O=169.2600,C=174.1900,body=+4.9300,vol=936300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.285 (Gvol=936300 Rvol=728500); 2026-10-19:RED:O=169.2500,C=168.1400,body=-1.1100,vol=728500.0; 2026-10-20:GREEN:O=169.2600,C=174.1900,body=+4.9300,vol=936300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.945 on 2026-10-20: today_vol=936300 / avg20=991149 (avg window 2026-09-16→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.769 on 2026-10-20 (price=174.1900, mid=167.4945, upper=176.1993, lower=158.7897; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=174.1900 vs SMA50=167.1578 dist=+4.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=167.4945 SMA50=167.1578 SMA80=166.4048 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-20 (63 bars); S1[2026-07-14→2026-08-14] low=2026-08-04@150.8600; S2[2026-08-17→2026-09-15] low=2026-09-10@156.7000; S3[2026-09-16→2026-10-20] low=2026-10-07@157.3750 | lows=[150.86000061035156, 156.6999969482422, 157.375] span=4.32% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.6093955706110685 wick_frac=0.3906044293889314 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.1859297124600639 wick_frac=0.8140702875399362 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.441446147501546 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+2.8800:wick=2.9100; 2026-10-09:RED:body=-2.8700:wick=3.4900; 2026-10-12:RED:body=-0.7400:wick=5.1900; 2026-10-19:RED:body=-1.1100:wick=4.8600; 2026-10-20:GREEN:body=+4.9300:wick=3.1600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.57 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.95 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 8294.42 | **NEUTRAL** |
| `B04_income` | 42.34 | **GOOD** |
| `B05_profit_margin` | 0.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 187.27 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=187.27 vs prior_export=187.27 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -2.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.84 vs prior=-2.84 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.74 | **GOOD** |
| `B13_short_float` | 3.44 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.57 (this export) | prior_export=0.57 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.95 (this export) | prior_export=2.95 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OXY  ·  score **+16**  ·  Oil & Gas E&P
price=62.91999816894531  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.34 on 2026-10-20; prev RSI=64.68 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.68@2026-10-19 → 65.34@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.68@2026-10-19 → 65.34@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.68@2026-10-19 → 65.34@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=1.241 (G=0.7200 R=0.5800); 2026-10-19:GREEN:O=62.0000,C=62.7200,body=+0.7200,vol=8482707.0; 2026-10-20:RED:O=63.5000,C=62.9200,body=-0.5800,vol=8034537.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.056 (Gvol=8482707 Rvol=8034537); 2026-10-19:GREEN:O=62.0000,C=62.7200,body=+0.7200,vol=8482707.0; 2026-10-20:RED:O=63.5000,C=62.9200,body=-0.5800,vol=8034537.0 | **GOOD** |
| `A07_rvol` | RVOL=0.887 on 2026-10-20: today_vol=8034537 / avg20=9058042 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.774 on 2026-10-20 (price=62.9200, mid=59.1335, upper=64.0249, lower=54.2421; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=62.9200 vs SMA50=59.5422 dist=+5.67% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=59.13_50=59.54_80=57.49 on 2026-10-20: SMA20=59.1335 SMA50=59.5422 SMA80=57.4940 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-08-05@53.6600; S2[2026-08-21→2026-09-21] low=2026-09-21@57.0000; S3[2026-09-22→2026-10-20] low=2026-09-29@54.7600 | lows=[53.65999984741211, 57.0, 54.7599983215332] span=6.22% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.7912102653101434 wick_frac=0.20878973468985668 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.2676519981833097 wick_frac=0.7323480018166904 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2413774959880035 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-0.8300:wick=0.1700; 2026-10-15:RED:body=-1.1300:wick=0.4250; 2026-10-16:GREEN:body=+1.2400:wick=0.5208; 2026-10-19:GREEN:body=+0.7200:wick=0.1900; 2026-10-20:RED:body=-0.5800:wick=1.5870 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=31.47 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.0 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 24313.0 | **NEUTRAL** |
| `B04_income` | 3412.0 | **GOOD** |
| `B05_profit_margin` | 14.03 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 68.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=68.45 vs prior_export=68.45 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.13 vs prior=0.13 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.37 | **GOOD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=31.47 (this export) | prior_export=31.47 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.0 (this export) | prior_export=14.0 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BB  ·  score **+15**  ·  Software - Infrastructure
price=8.869999885559082  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.90 on 2026-10-20; prev RSI=45.43 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.43@2026-10-19 → 53.90@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.43@2026-10-19 → 53.90@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.43@2026-10-19 → 53.90@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=8.778 (G=0.7900 R=0.0900); 2026-10-19:RED:O=8.2500,C=8.1600,body=-0.0900,vol=7718420.0; 2026-10-20:GREEN:O=8.0800,C=8.8700,body=+0.7900,vol=15833479.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=2.051 (Gvol=15833479 Rvol=7718420); 2026-10-19:RED:O=8.2500,C=8.1600,body=-0.0900,vol=7718420.0; 2026-10-20:GREEN:O=8.0800,C=8.8700,body=+0.7900,vol=15833479.0 | **GOOD** |
| `A07_rvol` | RVOL=0.808 on 2026-10-20: today_vol=15833479 / avg20=19606015 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.194 on 2026-10-20 (price=8.8700, mid=8.7135, upper=9.5207, lower=7.9063; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=8.8700 vs SMA50=8.3574 dist=+6.13% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=8.71_50=8.36_80=8.90 on 2026-10-20: SMA20=8.7135 SMA50=8.3574 SMA80=8.8967 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-07-28@7.7100; S2[2026-08-21→2026-09-21] low=2026-09-14@7.4600; S3[2026-09-22→2026-10-20] low=2026-09-28@7.8500 | lows=[7.710000038146973, 7.460000038146973, 7.849999904632568] span=5.23% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.9132949952481648 wick_frac=0.08670500475183514 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.3339520120031919 wick_frac=0.6660479879968081 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.777762471919637 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-0.0500:wick=0.2900; 2026-10-15:GREEN:body=+0.6900:wick=0.0950; 2026-10-16:RED:body=-0.5400:wick=0.1350; 2026-10-19:RED:body=-0.0900:wick=0.1795; 2026-10-20:GREEN:body=+0.7900:wick=0.0750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=66.67 (current export asof; earnings_date=9/24/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.56 (current export; earnings_date=9/24/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 614.0 | **NEUTRAL** |
| `B04_income` | 80.4 | **GOOD** |
| `B05_profit_margin` | 13.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.6 vs prior_export=10.6 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.8 | **NEUTRAL** |
| `B10_insider_transactions` | -21.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.83 vs prior=-21.83 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 11.06 | **GOOD** |
| `B13_short_float` | 6.43 | **NEUTRAL** |
| `B14_earnings_date` | 9/24/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=66.67 (this export) | prior_export=66.67 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.56 (this export) | prior_export=14.56 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HAS  ·  score **+15**  ·  Leisure
price=109.5199966430664  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=83.19 on 2026-10-20; prev RSI=70.05 on 2026-10-19 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 70.05@2026-10-19 → 83.19@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.05@2026-10-19 → 83.19@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 70.05@2026-10-19 → 83.19@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=17.580 (G=7.1200 R=0.4050); 2026-10-19:RED:O=98.8950,C=98.4900,body=-0.4050,vol=3686460.0; 2026-10-20:GREEN:O=102.4000,C=109.5200,body=+7.1200,vol=8007351.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=2.172 (Gvol=8007351 Rvol=3686460); 2026-10-19:RED:O=98.8950,C=98.4900,body=-0.4050,vol=3686460.0; 2026-10-20:GREEN:O=102.4000,C=109.5200,body=+7.1200,vol=8007351.0 | **GOOD** |
| `A07_rvol` | RVOL=4.258 on 2026-10-20: today_vol=8007351 / avg20=1880363 (avg window 2026-09-22→2026-10-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.504 on 2026-10-20 (price=109.5200, mid=92.8810, upper=103.9450, lower=81.8170; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=109.5200 vs SMA50=92.7280 dist=+18.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=92.8810 SMA50=92.7280 SMA80=89.9472 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-20 (63 bars); S1[2026-07-20→2026-08-20] low=2026-07-20@81.0000; S2[2026-08-21→2026-09-21] low=2026-09-21@86.3500; S3[2026-09-22→2026-10-20] low=2026-09-24@85.3700 | lows=[81.0, 86.3499984741211, 85.37000274658203] span=6.60% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.8578414610381254 wick_frac=0.1421585389618746 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.1594482808337063 wick_frac=0.8405517191662937 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=17.580287845678548 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:GREEN:body=+0.2300:wick=2.0400; 2026-10-15:GREEN:body=+1.8300:wick=1.6300; 2026-10-16:GREEN:body=+0.2300:wick=2.2700; 2026-10-19:RED:body=-0.4050:wick=2.1350; 2026-10-20:GREEN:body=+7.1200:wick=1.1799 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.67 (current export asof; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.62 (current export; earnings_date=7/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4971.2 | **NEUTRAL** |
| `B04_income` | 794.1 | **GOOD** |
| `B05_profit_margin` | 15.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 110.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=110.23 vs prior_export=110.23 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | -7.1 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-7.1 vs prior=-7.1 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.14 | **GOOD** |
| `B13_short_float` | 5.18 | **NEUTRAL** |
| `B14_earnings_date` | 7/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.67 (this export) | prior_export=10.67 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.62 (this export) | prior_export=6.62 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FIVN  ·  score **+15**  ·  Software - Infrastructure
price=34.310001373291016  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.64 on 2026-10-20; prev RSI=47.70 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.70@2026-10-19 → 50.64@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.70@2026-10-19 → 50.64@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.70@2026-10-19 → 50.64@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.5900 R=0.0000); 2026-10-19:GREEN:O=32.4300,C=33.3400,body=+0.9100,vol=1249036.0; 2026-10-20:GREEN:O=33.6300,C=34.3100,body=+0.6800,vol=2305426.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3554462 Rvol=0); 2026-10-19:GREEN:O=32.4300,C=33.3400,body=+0.9100,vol=1249036.0; 2026-10-20:GREEN:O=33.6300,C=34.3100,body=+0.6800,vol=2305426.0 | **GOOD** |
| `A07_rvol` | RVOL=1.107 on 2026-10-20: today_vol=2305426 / avg20=2083206 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.164 on 2026-10-20 (price=34.3100, mid=34.7515, upper=37.4408, lower=32.0622; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=34.3100 vs SMA50=33.7393 dist=+1.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=34.7515 SMA50=33.7393 SMA80=30.8240 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-07-23@20.7600; S2[2026-08-21→2026-09-21] low=2026-09-10@30.1000; S3[2026-09-22→2026-10-20] low=2026-10-16@31.9100 | lows=[20.760000228881836, 30.100000381469727, 31.90999984741211] span=53.71% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.5108915525883144 wick_frac=0.4891084474116856 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-1.1300:wick=1.7150; 2026-10-15:GREEN:body=+2.5100:wick=0.7500; 2026-10-16:RED:body=-3.2400:wick=1.1414; 2026-10-19:GREEN:body=+0.9100:wick=0.3165; 2026-10-20:GREEN:body=+0.6800:wick=1.7500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.74 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.9 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1203.88 | **NEUTRAL** |
| `B04_income` | 59.47 | **GOOD** |
| `B05_profit_margin` | 4.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.8 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.8 vs prior_export=35.8 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -4.84 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.84 vs prior=-4.84 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.75 | **GOOD** |
| `B13_short_float` | 12.24 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.74 (this export) | prior_export=2.74 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.9 (this export) | prior_export=1.9 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MH  ·  score **+15**  ·  Education & Training Services
price=14.270000457763672  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.04 on 2026-10-20; prev RSI=62.02 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.02@2026-10-19 → 66.04@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.02@2026-10-19 → 66.04@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.02@2026-10-19 → 66.04@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=20.000 (G=0.4000 R=0.0200); 2026-10-19:RED:O=13.8900,C=13.8700,body=-0.0200,vol=246200.0; 2026-10-20:GREEN:O=13.8700,C=14.2700,body=+0.4000,vol=375800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.526 (Gvol=375800 Rvol=246200); 2026-10-19:RED:O=13.8900,C=13.8700,body=-0.0200,vol=246200.0; 2026-10-20:GREEN:O=13.8700,C=14.2700,body=+0.4000,vol=375800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.605 on 2026-10-20: today_vol=375800 / avg20=620722 (avg window 2026-09-16→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.122 on 2026-10-20 (price=14.2700, mid=13.2060, upper=14.1539, lower=12.2581; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=14.2700 vs SMA50=12.9322 dist=+10.34% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=13.2060 SMA50=12.9322 SMA80=11.8586 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-20 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-23@8.9700; S2[2026-08-17→2026-09-15] low=2026-09-10@11.9950; S3[2026-09-16→2026-10-20] low=2026-09-28@12.1700 | lows=[8.970000267028809, 11.994999885559082, 12.170000076293945] span=35.67% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.784314495469114 wick_frac=0.21568550453088595 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.07017708235735032 wick_frac=0.9298229176426497 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=19.999570856379936 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.3900:wick=0.2450; 2026-10-09:RED:body=-0.0900:wick=0.2550; 2026-10-12:GREEN:body=+0.1200:wick=0.3050; 2026-10-19:RED:body=-0.0200:wick=0.2650; 2026-10-20:GREEN:body=+0.4000:wick=0.1100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.59 (current export asof; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.36 (current export; earnings_date=8/13/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2116.97 | **NEUTRAL** |
| `B04_income` | 92.68 | **GOOD** |
| `B05_profit_margin` | 4.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.92 vs prior_export=17.92 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.01 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.01 vs prior=0.01 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.23 | **BAD** |
| `B13_short_float` | 9.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.59 (this export) | prior_export=23.59 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.36 (this export) | prior_export=3.36 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SN  ·  score **+15**  ·  Furnishings, Fixtures & Appliances
price=198.72000122070312  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.53 on 2026-10-20; prev RSI=59.19 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.19@2026-10-19 → 67.53@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.19@2026-10-19 → 67.53@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.19@2026-10-19 → 67.53@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=20.432 (G=8.9900 R=0.4400); 2026-10-19:RED:O=189.3800,C=188.9400,body=-0.4400,vol=769907.0; 2026-10-20:GREEN:O=189.7300,C=198.7200,body=+8.9900,vol=1306541.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.697 (Gvol=1306541 Rvol=769907); 2026-10-19:RED:O=189.3800,C=188.9400,body=-0.4400,vol=769907.0; 2026-10-20:GREEN:O=189.7300,C=198.7200,body=+8.9900,vol=1306541.0 | **GOOD** |
| `A07_rvol` | RVOL=1.063 on 2026-10-20: today_vol=1306541 / avg20=1229466 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.127 on 2026-10-20 (price=198.7200, mid=184.2735, upper=197.0889, lower=171.4581; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=198.7200 vs SMA50=180.3138 dist=+10.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=184.2735 SMA50=180.3138 SMA80=171.2175 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-20 (63 bars); S1[2026-07-20→2026-08-20] low=2026-07-23@141.8800; S2[2026-08-21→2026-09-21] low=2026-09-11@157.0100; S3[2026-09-22→2026-10-20] low=2026-09-23@165.3000 | lows=[141.8800048828125, 157.00999450683594, 165.3000030517578] span=16.51% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.904426889848149 wick_frac=0.095573110151851 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.08661488276533243 wick_frac=0.9133851172346675 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.431717297822168 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-0.3500:wick=4.4397; 2026-10-15:GREEN:body=+6.1300:wick=5.1100; 2026-10-16:RED:body=-7.1700:wick=2.9470; 2026-10-19:RED:body=-0.4400:wick=4.6400; 2026-10-20:GREEN:body=+8.9900:wick=0.9500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.45 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6909.96 | **NEUTRAL** |
| `B04_income` | 695.22 | **GOOD** |
| `B05_profit_margin` | 10.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.56 vs prior_export=209.56 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | -6.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.41 vs prior=-6.41 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.92 | **GOOD** |
| `B13_short_float` | 8.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.45 (this export) | prior_export=13.45 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.82 (this export) | prior_export=6.82 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSFT  ·  score **+15**  ·  Software - Infrastructure
price=531.8699951171875  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.35 on 2026-10-20; prev RSI=59.56 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.56@2026-10-19 → 58.35@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.56@2026-10-19 → 58.35@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.56@2026-10-19 → 58.35@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=2.363 (G=4.8200 R=2.0400); 2026-10-19:GREEN:O=528.7000,C=533.5200,body=+4.8200,vol=17469801.0; 2026-10-20:RED:O=533.9100,C=531.8700,body=-2.0400,vol=14432432.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.210 (Gvol=17469801 Rvol=14432432); 2026-10-19:GREEN:O=528.7000,C=533.5200,body=+4.8200,vol=17469801.0; 2026-10-20:RED:O=533.9100,C=531.8700,body=-2.0400,vol=14432432.0 | **GOOD** |
| `A07_rvol` | RVOL=0.672 on 2026-10-20: today_vol=14432432 / avg20=21472296 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.313 on 2026-10-20 (price=531.8700, mid=523.5095, upper=550.2214, lower=496.7976; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=531.8700 vs SMA50=506.6648 dist=+4.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=523.5095 SMA50=506.6648 SMA80=472.0094 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-07-23@377.3900; S2[2026-08-21→2026-09-21] low=2026-08-21@478.5300; S3[2026-09-22→2026-10-20] low=2026-09-24@491.2200 | lows=[377.3900146484375, 478.5299987792969, 491.2200012207031] span=30.16% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.6799521275679772 wick_frac=0.3200478724320229 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.2443076743149108 wick_frac=0.7556923256850893 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3627741375699367 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-11.5800:wick=1.6200; 2026-10-15:GREEN:body=+6.1250:wick=3.7881; 2026-10-16:RED:body=-12.0100:wick=7.3200; 2026-10-19:GREEN:body=+4.8200:wick=2.2687; 2026-10-20:RED:body=-2.0400:wick=6.3101 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.81 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.72 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 331839.0 | **NEUTRAL** |
| `B04_income` | 133749.0 | **GOOD** |
| `B05_profit_margin` | 40.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 571.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=571.36 vs prior_export=571.36 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -0.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.15 vs prior=-0.15 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 0.92 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.81 (this export) | prior_export=11.81 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.72 (this export) | prior_export=2.72 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TRI  ·  score **+15**  ·  Specialty Business Services
price=112.81999969482422  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.83 on 2026-10-20; prev RSI=63.57 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.57@2026-10-19 → 69.83@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.57@2026-10-19 → 69.83@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.57@2026-10-19 → 69.83@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=5.719 (G=3.2600 R=0.5700); 2026-10-19:RED:O=108.8000,C=108.2300,body=-0.5700,vol=1566895.0; 2026-10-20:GREEN:O=109.5600,C=112.8200,body=+3.2600,vol=2061447.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.316 (Gvol=2061447 Rvol=1566895); 2026-10-19:RED:O=108.8000,C=108.2300,body=-0.5700,vol=1566895.0; 2026-10-20:GREEN:O=109.5600,C=112.8200,body=+3.2600,vol=2061447.0 | **GOOD** |
| `A07_rvol` | RVOL=1.488 on 2026-10-20: today_vol=2061447 / avg20=1385363 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.134 on 2026-10-20 (price=112.8200, mid=101.8795, upper=111.5248, lower=92.2342; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=112.8200 vs SMA50=102.3344 dist=+10.25% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=101.88_50=102.33_80=99.08 on 2026-10-20: SMA20=101.8795 SMA50=102.3344 SMA80=99.0810 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-21→2026-10-20 (63 bars); S1[2026-07-21→2026-08-20] low=2026-07-23@84.6300; S2[2026-08-21→2026-09-21] low=2026-09-21@93.6500; S3[2026-09-22→2026-10-20] low=2026-09-29@92.5700 | lows=[84.62999725341797, 93.6500015258789, 92.56999969482422] span=10.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.8670231114177303 wick_frac=0.13297688858226975 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.2727276045849456 wick_frac=0.7272723954150544 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.719305055480452 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:GREEN:body=+0.1200:wick=2.5300; 2026-10-15:RED:body=-0.0900:wick=3.6950; 2026-10-16:RED:body=-0.4300:wick=3.3200; 2026-10-19:RED:body=-0.5700:wick=1.5200; 2026-10-20:GREEN:body=+3.2600:wick=0.5000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.32 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.86 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 7832.0 | **NEUTRAL** |
| `B04_income` | 1658.0 | **GOOD** |
| `B05_profit_margin` | 21.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 122.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=122.64 vs prior_export=122.64 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.02 | **GOOD** |
| `B13_short_float` | 9.88 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.32 (this export) | prior_export=3.32 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.86 (this export) | prior_export=1.86 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ISRG  ·  score **+15**  ·  Medical Instruments & Supplies
price=438.0  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.72 on 2026-10-20; prev RSI=67.68 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.68@2026-10-19 → 69.72@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.68@2026-10-19 → 69.72@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.68@2026-10-19 → 69.72@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=10.3380 R=0.0000); 2026-10-19:GREEN:O=427.3400,C=433.1800,body=+5.8400,vol=2470673.0; 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5610679 Rvol=0); 2026-10-19:GREEN:O=427.3400,C=433.1800,body=+5.8400,vol=2470673.0; 2026-10-20:GREEN:O=433.5020,C=438.0000,body=+4.4980,vol=3140006.0 | **GOOD** |
| `A07_rvol` | RVOL=1.620 on 2026-10-20: today_vol=3140006 / avg20=1938632 (avg window 2026-09-22→2026-10-19, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.924 on 2026-10-20 (price=438.0000, mid=414.5300, upper=439.9370, lower=389.1230; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=438.0000 vs SMA50=393.6486 dist=+11.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=414.5300 SMA50=393.6486 SMA80=388.5952 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-07-23@328.5700; S2[2026-08-21→2026-09-21] low=2026-09-10@345.2500; S3[2026-09-22→2026-10-20] low=2026-10-05@389.5600 | lows=[328.57000732421875, 345.25, 389.55999755859375] span=18.56% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.6363311415358797 wick_frac=0.3636688584641204 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-4.5700:wick=6.1750; 2026-10-15:GREEN:body=+2.0200:wick=8.3300; 2026-10-16:RED:body=-1.7300:wick=7.6500; 2026-10-19:GREEN:body=+5.8400:wick=2.0820; 2026-10-20:GREEN:body=+4.4980:wick=3.9020 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.5 (current export asof; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.37 (current export; earnings_date=10/20/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 11034.4 | **NEUTRAL** |
| `B04_income` | 3138.8 | **GOOD** |
| `B05_profit_margin` | 28.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 479.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=479.86 vs prior_export=479.86 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.8 vs prior=-0.8 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.23 | **GOOD** |
| `B13_short_float` | 2.31 | **NEUTRAL** |
| `B14_earnings_date` | 10/20/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.5 (this export) | prior_export=11.5 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.37 (this export) | prior_export=2.37 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FOXA  ·  score **+14**  ·  Entertainment
price=67.0  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.14 on 2026-10-20; prev RSI=55.84 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.84@2026-10-19 → 61.14@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.84@2026-10-19 → 61.14@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.84@2026-10-19 → 61.14@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7700 R=0.0000); 2026-10-19:GREEN:O=64.0000,C=65.2700,body=+1.2700,vol=5154800.0; 2026-10-20:GREEN:O=65.5000,C=67.0000,body=+1.5000,vol=4853600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10008400 Rvol=0); 2026-10-19:GREEN:O=64.0000,C=65.2700,body=+1.2700,vol=5154800.0; 2026-10-20:GREEN:O=65.5000,C=67.0000,body=+1.5000,vol=4853600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.059 on 2026-10-20: today_vol=4853600 / avg20=4583518 (avg window 2026-09-16→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.302 on 2026-10-20 (price=67.0000, mid=63.5020, upper=66.1890, lower=60.8150; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=67.0000 vs SMA50=65.2300 dist=+2.71% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=63.50_50=65.23_80=61.27 on 2026-10-20: SMA20=63.5020 SMA50=65.2300 SMA80=61.2662 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-20 (63 bars); S1[2026-07-15→2026-08-14] low=2026-07-23@54.4000; S2[2026-08-17→2026-09-15] low=2026-09-10@62.9300; S3[2026-09-16→2026-10-20] low=2026-10-01@60.2600 | lows=[54.400001525878906, 62.93000030517578, 60.2599983215332] span=15.68% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.7440222574140649 wick_frac=0.25597774258593503 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+0.7400:wick=0.7300; 2026-10-09:RED:body=-1.3500:wick=0.5600; 2026-10-12:RED:body=-0.1700:wick=0.8600; 2026-10-19:GREEN:body=+1.2700:wick=0.8450; 2026-10-20:GREEN:body=+1.5000:wick=0.1900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.49 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.61 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 17126.0 | **NEUTRAL** |
| `B04_income` | 1685.0 | **GOOD** |
| `B05_profit_margin` | 9.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 77.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=77.94 vs prior_export=77.94 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 12.07 | **GOOD** |
| `B13_short_float` | 22.96 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.49 (this export) | prior_export=24.49 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.61 (this export) | prior_export=15.61 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMZN  ·  score **+14**  ·  Internet Retail
price=262.6300048828125  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.41 on 2026-10-20; prev RSI=57.26 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.26@2026-10-19 → 57.41@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.26@2026-10-19 → 57.41@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.26@2026-10-19 → 57.41@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=5.079 (G=7.1100 R=1.4000); 2026-10-19:GREEN:O=255.3400,C=262.4500,body=+7.1100,vol=54534560.0; 2026-10-20:RED:O=264.0300,C=262.6300,body=-1.4000,vol=34498734.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.581 (Gvol=54534560 Rvol=34498734); 2026-10-19:GREEN:O=255.3400,C=262.4500,body=+7.1100,vol=54534560.0; 2026-10-20:RED:O=264.0300,C=262.6300,body=-1.4000,vol=34498734.0 | **GOOD** |
| `A07_rvol` | RVOL=0.969 on 2026-10-20: today_vol=34498734 / avg20=35598610 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.804 on 2026-10-20 (price=262.6300, mid=253.8085, upper=264.7840, lower=242.8330; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=262.6300 vs SMA50=256.5262 dist=+2.38% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=253.81_50=256.53_80=254.07 on 2026-10-20: SMA20=253.8085 SMA50=256.5262 SMA80=254.0655 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-23→2026-10-20 (63 bars); S1[2026-07-23→2026-08-20] low=2026-07-29@226.1600; S2[2026-08-21→2026-09-21] low=2026-09-16@244.3000; S3[2026-09-22→2026-10-20] low=2026-09-28@244.7250 | lows=[226.16000366210938, 244.3000030517578, 244.72500610351562] span=8.21% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.8374586628324946 wick_frac=0.16254133716750538 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.28457024465287084 wick_frac=0.7154297553471292 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.078604904632153 need>1.4; red_wick_gt_green=True 5d trail=2026-10-14:RED:body=-3.9300:wick=3.0600; 2026-10-15:GREEN:body=+4.2600:wick=2.6900; 2026-10-16:RED:body=-5.0950:wick=1.8450; 2026-10-19:GREEN:body=+7.1100:wick=1.3800; 2026-10-20:RED:body=-1.4000:wick=3.5197 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=215.36 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.81 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 775680.0 | **NEUTRAL** |
| `B04_income` | 135281.0 | **GOOD** |
| `B05_profit_margin` | 17.44 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 330.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=330.94 vs prior_export=330.94 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -0.15 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.15 vs prior=-0.15 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.02 | **GOOD** |
| `B13_short_float` | 0.83 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=215.36 (this export) | prior_export=215.36 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.81 (this export) | prior_export=1.81 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANF  ·  score **+14**  ·  Apparel Retail
price=161.72999572753906  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=75.11 on 2026-10-20; prev RSI=75.57 on 2026-10-19 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 75.57@2026-10-19 → 75.11@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.57@2026-10-19 → 75.11@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 75.57@2026-10-19 → 75.11@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=27.814 (G=7.5100 R=0.2700); 2026-10-19:GREEN:O=154.5000,C=162.0100,body=+7.5100,vol=1578400.0; 2026-10-20:RED:O=162.0000,C=161.7300,body=-0.2700,vol=1207200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=1.307 (Gvol=1578400 Rvol=1207200); 2026-10-19:GREEN:O=154.5000,C=162.0100,body=+7.5100,vol=1578400.0; 2026-10-20:RED:O=162.0000,C=161.7300,body=-0.2700,vol=1207200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.002 on 2026-10-20: today_vol=1207200 / avg20=1204868 (avg window 2026-09-16→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.316 on 2026-10-20 (price=161.7300, mid=140.0095, upper=156.5102, lower=123.5088; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=161.7300 vs SMA50=132.2108 dist=+22.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=140.0095 SMA50=132.2108 SMA80=117.6532 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-20 (63 bars); S1[2026-07-14→2026-08-14] low=2026-07-24@90.0700; S2[2026-08-17→2026-09-15] low=2026-08-18@100.4300; S3[2026-09-16→2026-10-20] low=2026-09-23@130.8100 | lows=[90.06999969482422, 100.43000030517578, 130.80999755859375] span=45.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.8929835001941367 wick_frac=0.1070164998058633 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.06413392918628379 wick_frac=0.9358660708137162 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=27.814354337383442 need>1.4; red_wick_gt_green=True 5d trail=2026-10-08:GREEN:body=+3.6900:wick=1.8100; 2026-10-09:GREEN:body=+1.5800:wick=3.3700; 2026-10-12:GREEN:body=+1.9100:wick=3.4800; 2026-10-19:GREEN:body=+7.5100:wick=0.9000; 2026-10-20:RED:body=-0.2700:wick=3.9400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=109.68 (current export asof; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.5 (current export; earnings_date=8/26/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5340.93 | **NEUTRAL** |
| `B04_income` | 535.98 | **GOOD** |
| `B05_profit_margin` | 10.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 163.55 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=163.55 vs prior_export=163.55 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | -8.32 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.32 vs prior=-8.32 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.62 | **GOOD** |
| `B13_short_float` | 12.69 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=109.68 (this export) | prior_export=109.68 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.5 (this export) | prior_export=1.5 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-21_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.