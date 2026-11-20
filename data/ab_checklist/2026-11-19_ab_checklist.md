# A+B1 Feature Checklist — 2026-11-19

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,604** names
- Export: `finviz_2026-11-19.csv` · prior export for Δ: `2026-11-18`
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
| 1 | CXT | +17 | 17 | 0 | 2026-11-18→2026-11-19 | Business Equipment & Supplies |
| 2 | FNKO | +16 | 18 | 2 | 2026-11-18→2026-11-19 | Leisure |
| 3 | PLX | +16 | 16 | 0 | 2026-11-18→2026-11-19 | Biotechnology |
| 4 | BEKE | +16 | 17 | 1 | 2026-11-18→2026-11-19 | Real Estate Services |
| 5 | PBH | +16 | 16 | 0 | 2026-11-18→2026-11-19 | Drug Manufacturers - Specialty & Gen |
| 6 | TENB | +16 | 17 | 1 | 2026-11-18→2026-11-19 | Software - Infrastructure |
| 7 | CCC | +15 | 15 | 0 | 2026-11-18→2026-11-19 | Software - Application |
| 8 | CBRL | +15 | 17 | 2 | 2026-11-18→2026-11-19 | Restaurants |
| 9 | TDC | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Software - Infrastructure |
| 10 | LFTO | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Advertising Agencies |
| 11 | LNT | +15 | 15 | 0 | 2026-11-18→2026-11-19 | Utilities - Regulated Electric |
| 12 | BOOT | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Apparel Retail |
| 13 | ARDT | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Medical Care Facilities |
| 14 | CCEP | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Beverages - Non-Alcoholic |
| 15 | DGX | +15 | 16 | 1 | 2026-11-18→2026-11-19 | Diagnostics & Research |

## Full checklist — top 15

### CXT  ·  score **+17**  ·  Business Equipment & Supplies
price=54.20000076293945  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=44.65 on 2026-11-19; prev RSI=47.93 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.93@2026-11-18 → 44.65@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.93@2026-11-18 → 44.65@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.93@2026-11-18 → 44.65@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=1.420 (G=1.4200 R=1.0000); 2026-11-18:GREEN:O=53.9700,C=55.3900,body=+1.4200,vol=773200.0; 2026-11-19:RED:O=55.2000,C=54.2000,body=-1.0000,vol=418200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.849 (Gvol=773200 Rvol=418200); 2026-11-18:GREEN:O=53.9700,C=55.3900,body=+1.4200,vol=773200.0; 2026-11-19:RED:O=55.2000,C=54.2000,body=-1.0000,vol=418200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.581 on 2026-11-19: today_vol=418200 / avg20=719895 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.889 on 2026-11-19 (price=54.2000, mid=57.8920, upper=62.0452, lower=53.7388; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=True on 2026-11-19: price=54.2000 vs SMA50=53.0120 dist=+2.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=57.8920 SMA50=53.0120 SMA80=52.3378 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@45.2000; S2[2026-09-18→2026-10-21] low=2026-09-18@44.8400; S3[2026-10-22→2026-11-19] low=2026-11-17@53.4800 | lows=[45.20000076293945, 44.84000015258789, 53.47999954223633] span=19.27% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.5546864871872434 wick_frac=0.4453135128127566 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.48309185866083837 wick_frac=0.5169081413391616 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.4199981689453125 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:RED:body=-0.0700:wick=0.8000; 2026-11-16:RED:body=-0.6000:wick=1.2400; 2026-11-17:RED:body=-1.3800:wick=1.1100; 2026-11-18:GREEN:body=+1.4200:wick=1.1400; 2026-11-19:RED:body=-1.0000:wick=1.0700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.14 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.42 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1854.9 | **NEUTRAL** |
| `B04_income` | 128.2 | **GOOD** |
| `B05_profit_margin` | 6.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 72.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=72.71 vs prior_export=72.71 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 0.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.35 vs prior=0.35 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.39 | **GOOD** |
| `B13_short_float` | 18.16 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.14 (this export) | prior_export=6.14 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.42 (this export) | prior_export=0.42 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FNKO  ·  score **+16**  ·  Leisure
price=6.730000019073486  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.97 on 2026-11-19; prev RSI=54.70 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.70@2026-11-18 → 51.97@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.70@2026-11-18 → 51.97@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.70@2026-11-18 → 51.97@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=3.333 (G=0.3000 R=0.0900); 2026-11-18:GREEN:O=6.5600,C=6.8600,body=+0.3000,vol=752900.0; 2026-11-19:RED:O=6.8200,C=6.7300,body=-0.0900,vol=1735000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=0.434 (Gvol=752900 Rvol=1735000); 2026-11-18:GREEN:O=6.5600,C=6.8600,body=+0.3000,vol=752900.0; 2026-11-19:RED:O=6.8200,C=6.7300,body=-0.0900,vol=1735000.0 | **BAD** |
| `A07_rvol` | RVOL=1.666 on 2026-11-19: today_vol=1735000 / avg20=1041256 (avg window 2026-10-22→2026-11-18, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.035 on 2026-11-19 (price=6.7300, mid=6.7092, upper=7.2942, lower=6.1243; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=6.7300 vs SMA50=6.2314 dist=+8.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=6.7092 SMA50=6.2314 SMA80=6.1850 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-19 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-15@5.2150; S2[2026-09-22→2026-10-21] low=2026-09-23@5.2500; S3[2026-10-22→2026-11-19] low=2026-11-06@5.2000 | lows=[5.215000152587891, 5.25, 5.199999809265137] span=0.96% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.8108115770942823 wick_frac=0.18918842290571766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.3272731686425058 wick_frac=0.6727268313574942 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.333329801212224 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:RED:body=-0.0100:wick=0.3138; 2026-11-16:GREEN:body=+0.0300:wick=0.1500; 2026-11-17:RED:body=-0.1700:wick=0.0650; 2026-11-18:GREEN:body=+0.3000:wick=0.0700; 2026-11-19:RED:body=-0.0900:wick=0.1850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=53.99 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.05 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.08 | **NEUTRAL** |
| `B04_income` | 4.61 | **GOOD** |
| `B05_profit_margin` | 0.49 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 8.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=8.0 vs prior_export=8.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -1.09 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.09 vs prior=-1.09 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.95 | **GOOD** |
| `B13_short_float` | 20.41 | **GOOD** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=53.99 (this export) | prior_export=53.99 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.05 (this export) | prior_export=3.05 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PLX  ·  score **+16**  ·  Biotechnology
price=3.059999942779541  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.88 on 2026-11-19; prev RSI=60.62 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.62@2026-11-18 → 61.88@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.62@2026-11-18 → 61.88@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.62@2026-11-18 → 61.88@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.2200 R=0.0000); 2026-11-18:GREEN:O=2.8900,C=3.0200,body=+0.1300,vol=962600.0; 2026-11-19:GREEN:O=2.9700,C=3.0600,body=+0.0900,vol=1571600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2534200 Rvol=0); 2026-11-18:GREEN:O=2.8900,C=3.0200,body=+0.1300,vol=962600.0; 2026-11-19:GREEN:O=2.9700,C=3.0600,body=+0.0900,vol=1571600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.565 on 2026-11-19: today_vol=1571600 / avg20=1004045 (avg window 2026-10-22→2026-11-18, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.703 on 2026-11-19 (price=3.0600, mid=2.7410, upper=3.1949, lower=2.2871; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=3.0600 vs SMA50=2.7154 dist=+12.69% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=2.7410 SMA50=2.7154 SMA80=2.6096 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-08-19@2.3100; S2[2026-09-18→2026-10-21] low=2026-10-21@2.5300; S3[2026-10-22→2026-11-19] low=2026-11-10@2.4500 | lows=[2.309999942779541, 2.5299999713897705, 2.450000047683716] span=9.52% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.6688309471610312 wick_frac=0.33116905283896886 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:GREEN:body=+0.1100:wick=0.0900; 2026-11-16:GREEN:body=+0.0300:wick=0.0900; 2026-11-17:RED:body=-0.1300:wick=0.0900; 2026-11-18:GREEN:body=+0.1300:wick=0.0100; 2026-11-19:GREEN:body=+0.0900:wick=0.1300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=350.0 (current export asof; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=76.53 (current export; earnings_date=11/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 84.69 | **NEUTRAL** |
| `B04_income` | 20.55 | **GOOD** |
| `B05_profit_margin` | 24.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.0 vs prior_export=12.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.95 | **GOOD** |
| `B13_short_float` | 4.54 | **NEUTRAL** |
| `B14_earnings_date` | 11/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=350.0 (this export) | prior_export=350.0 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=76.53 (this export) | prior_export=76.53 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BEKE  ·  score **+16**  ·  Real Estate Services
price=18.239999771118164  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.07 on 2026-11-19; prev RSI=57.62 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.62@2026-11-18 → 54.07@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.62@2026-11-18 → 54.07@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.62@2026-11-18 → 54.07@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=3.300 (G=0.3300 R=0.1000); 2026-11-18:GREEN:O=18.1600,C=18.4900,body=+0.3300,vol=7167000.0; 2026-11-19:RED:O=18.3400,C=18.2400,body=-0.1000,vol=4482200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.599 (Gvol=7167000 Rvol=4482200); 2026-11-18:GREEN:O=18.1600,C=18.4900,body=+0.3300,vol=7167000.0; 2026-11-19:RED:O=18.3400,C=18.2400,body=-0.1000,vol=4482200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.789 on 2026-11-19: today_vol=4482200 / avg20=5680194 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.218 on 2026-11-19 (price=18.2400, mid=18.0160, upper=19.0457, lower=16.9863; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=18.2400 vs SMA50=17.3760 dist=+4.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=18.0160 SMA50=17.3760 SMA80=17.3594 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-16@15.8600; S2[2026-09-23→2026-10-21] low=2026-09-29@16.2000; S3[2026-10-22→2026-11-19] low=2026-10-22@17.0500 | lows=[15.859999656677246, 16.200000762939453, 17.049999237060547] span=7.50% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.6470583835414586 wick_frac=0.3529416164585414 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.2941186370317181 wick_frac=0.705881362968282 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.299986648610502 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:GREEN:body=+0.3600:wick=0.1150; 2026-11-16:GREEN:body=+0.1400:wick=0.7500; 2026-11-17:RED:body=-0.5900:wick=0.0650; 2026-11-18:GREEN:body=+0.3300:wick=0.1800; 2026-11-19:RED:body=-0.1000:wick=0.2400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=35.72 (current export asof; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.89 (current export; earnings_date=11/16/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 12576.09 | **NEUTRAL** |
| `B04_income` | 904.44 | **GOOD** |
| `B05_profit_margin` | 7.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.87 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.019999999999999574 (now=24.87 vs prior_export=24.85 on finviz_2026-11-18) | **GOOD** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | -68.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-68.97 vs prior=-68.97 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 28.81 | **GOOD** |
| `B13_short_float` | 2.19 | **NEUTRAL** |
| `B14_earnings_date` | 11/16/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.72 (this export) | prior_export=35.72 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.89 (this export) | prior_export=0.89 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBH  ·  score **+16**  ·  Drug Manufacturers - Specialty & Generic
price=49.599998474121094  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.71 on 2026-11-19; prev RSI=58.91 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.91@2026-11-18 → 55.71@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.91@2026-11-18 → 55.71@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.91@2026-11-18 → 55.71@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=5.109 (G=2.3500 R=0.4600); 2026-11-18:GREEN:O=48.2700,C=50.6200,body=+2.3500,vol=618500.0; 2026-11-19:RED:O=50.0600,C=49.6000,body=-0.4600,vol=510800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.211 (Gvol=618500 Rvol=510800); 2026-11-18:GREEN:O=48.2700,C=50.6200,body=+2.3500,vol=618500.0; 2026-11-19:RED:O=50.0600,C=49.6000,body=-0.4600,vol=510800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.800 on 2026-11-19: today_vol=510800 / avg20=638843 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.404 on 2026-11-19 (price=49.6000, mid=47.4775, upper=52.7335, lower=42.2215; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=49.6000 vs SMA50=47.3658 dist=+4.72% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=47.48_50=47.37_80=49.07 on 2026-11-19: SMA20=47.4775 SMA50=47.3658 SMA80=49.0681 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@45.9000; S2[2026-09-18→2026-10-21] low=2026-09-21@44.4600; S3[2026-10-22→2026-11-19] low=2026-11-06@41.7700 | lows=[45.900001525878906, 44.459999084472656, 41.77000045776367] span=9.89% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.6041122206521068 wick_frac=0.39588777934789326 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.2690070872939565 wick_frac=0.7309929127060435 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.108660137494091 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:GREEN:body=+0.5000:wick=0.6800; 2026-11-16:GREEN:body=+1.3000:wick=0.3200; 2026-11-17:GREEN:body=+0.6300:wick=0.7300; 2026-11-18:GREEN:body=+2.3500:wick=1.5400; 2026-11-19:RED:body=-0.4600:wick=1.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.32 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.18 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1167.08 | **NEUTRAL** |
| `B04_income` | 155.1 | **GOOD** |
| `B05_profit_margin` | 13.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=65.25 vs prior_export=65.25 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.71 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 9.09 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.32 (this export) | prior_export=5.32 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.18 (this export) | prior_export=2.18 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TENB  ·  score **+16**  ·  Software - Infrastructure
price=44.09000015258789  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.31 on 2026-11-19; prev RSI=54.67 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.67@2026-11-18 → 53.31@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.67@2026-11-18 → 53.31@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.67@2026-11-18 → 53.31@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=3.586 (G=5.6300 R=1.5700); 2026-11-18:GREEN:O=39.0500,C=44.6800,body=+5.6300,vol=4921700.0; 2026-11-19:RED:O=45.6600,C=44.0900,body=-1.5700,vol=3611900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.363 (Gvol=4921700 Rvol=3611900); 2026-11-18:GREEN:O=39.0500,C=44.6800,body=+5.6300,vol=4921700.0; 2026-11-19:RED:O=45.6600,C=44.0900,body=-1.5700,vol=3611900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.976 on 2026-11-19: today_vol=3611900 / avg20=3699574 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.123 on 2026-11-19 (price=44.0900, mid=44.9790, upper=52.2007, lower=37.7573; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=44.0900 vs SMA50=40.4407 dist=+9.02% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=44.9790 SMA50=40.4407 SMA80=38.5313 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-11@29.9700; S2[2026-09-23→2026-10-21] low=2026-09-28@32.5000; S3[2026-10-22→2026-11-19] low=2026-11-17@37.9400 | lows=[29.969999313354492, 32.5, 37.939998626708984] span=26.59% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.916938375858307 wick_frac=0.08306162414169303 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.39949098547309037 wick_frac=0.6005090145269096 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5859886385172732 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:RED:body=-0.7800:wick=2.1100; 2026-11-16:GREEN:body=+0.0400:wick=1.5400; 2026-11-17:RED:body=-2.0100:wick=1.2900; 2026-11-18:GREEN:body=+5.6300:wick=0.5100; 2026-11-19:RED:body=-1.5700:wick=2.3600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.04 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1067.49 | **NEUTRAL** |
| `B04_income` | 15.78 | **GOOD** |
| `B05_profit_margin` | 1.48 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.05 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.05 vs prior_export=49.05 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.46 | **GOOD** |
| `B10_insider_transactions` | -2.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.77 vs prior=-2.77 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.09 | **GOOD** |
| `B13_short_float` | 15.36 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.04 (this export) | prior_export=24.04 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCC  ·  score **+15**  ·  Software - Application
price=7.690000057220459  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.51 on 2026-11-19; prev RSI=66.51 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.51@2026-11-18 → 66.51@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.51@2026-11-18 → 66.51@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.51@2026-11-18 → 66.51@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=1.000 (G=0.0100 R=0.0100); 2026-11-18:GREEN:O=7.6800,C=7.6900,body=+0.0100,vol=16891700.0; 2026-11-19:RED:O=7.7000,C=7.6900,body=-0.0100,vol=13979700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.208 (Gvol=16891700 Rvol=13979700); 2026-11-18:GREEN:O=7.6800,C=7.6900,body=+0.0100,vol=16891700.0; 2026-11-19:RED:O=7.7000,C=7.6900,body=-0.0100,vol=13979700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.717 on 2026-11-19: today_vol=13979700 / avg20=19494875 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.174 on 2026-11-19 (price=7.6900, mid=7.6850, upper=7.7137, lower=7.6563; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=7.6900 vs SMA50=7.1960 dist=+6.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=7.6850 SMA50=7.1960 SMA80=7.0921 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@6.2400; S2[2026-09-18→2026-10-21] low=2026-09-21@6.4000; S3[2026-10-22→2026-11-19] low=2026-10-22@7.6200 | lows=[6.239999771118164, 6.400000095367432, 7.619999885559082] span=22.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.5000119209403238 wick_frac=0.49998807905967624 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.3333280350955272 wick_frac=0.6666719649044728 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0000476848981927 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:RED:body=-0.0100:wick=0.0100; 2026-11-16:RED:body=-0.0100:wick=0.0100; 2026-11-17:DOJI:body=+0.0000:wick=0.0200; 2026-11-18:GREEN:body=+0.0100:wick=0.0100; 2026-11-19:RED:body=-0.0100:wick=0.0200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=18.94 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.21 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1139.16 | **NEUTRAL** |
| `B04_income` | 74.56 | **GOOD** |
| `B05_profit_margin` | 6.54 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 7.75 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=7.75 vs prior_export=7.75 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 3.1 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.93 | **GOOD** |
| `B13_short_float` | 8.13 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.94 (this export) | prior_export=18.94 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.21 (this export) | prior_export=1.21 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CBRL  ·  score **+15**  ·  Restaurants
price=71.66999816894531  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.37 on 2026-11-19; prev RSI=67.12 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.12@2026-11-18 → 60.37@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.12@2026-11-18 → 60.37@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.12@2026-11-18 → 60.37@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=3.326 (G=4.7900 R=1.4400); 2026-11-18:GREEN:O=69.5500,C=74.3400,body=+4.7900,vol=1074300.0; 2026-11-19:RED:O=73.1100,C=71.6700,body=-1.4400,vol=1074900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=0.999 (Gvol=1074300 Rvol=1074900); 2026-11-18:GREEN:O=69.5500,C=74.3400,body=+4.7900,vol=1074300.0; 2026-11-19:RED:O=73.1100,C=71.6700,body=-1.4400,vol=1074900.0 | **BAD** |
| `A07_rvol` | RVOL=1.559 on 2026-11-19: today_vol=1074900 / avg20=689692 (avg window 2026-10-22→2026-11-18, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.595 on 2026-11-19 (price=71.6700, mid=68.6350, upper=73.7358, lower=63.5342; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=71.6700 vs SMA50=60.5486 dist=+18.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=68.6350 SMA50=60.5486 SMA80=58.9487 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-17@42.8700; S2[2026-09-23→2026-10-21] low=2026-09-23@44.5200; S3[2026-10-22→2026-11-19] low=2026-11-11@63.4800 | lows=[42.869998931884766, 44.52000045776367, 63.47999954223633] span=48.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.9410602617678624 wick_frac=0.05893973823213761 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.4615399663523612 wick_frac=0.5384600336476388 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3263785868689864 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:GREEN:body=+1.2400:wick=0.8600; 2026-11-16:RED:body=-0.2800:wick=2.2800; 2026-11-17:GREEN:body=+1.5800:wick=1.0600; 2026-11-18:GREEN:body=+4.7900:wick=0.3000; 2026-11-19:RED:body=-1.4400:wick=1.6800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=469.95 (current export asof; earnings_date=12/2/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.77 (current export; earnings_date=12/2/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3318.71 | **NEUTRAL** |
| `B04_income` | 31.68 | **GOOD** |
| `B05_profit_margin` | 0.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 50.71 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=50.71 vs prior_export=50.71 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 3.2 | **NEUTRAL** |
| `B10_insider_transactions` | -0.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.71 vs prior=-0.71 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.15 | **GOOD** |
| `B13_short_float` | 23.32 | **GOOD** |
| `B14_earnings_date` | 12/2/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=469.95 (this export) | prior_export=469.95 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.77 (this export) | prior_export=1.77 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TDC  ·  score **+15**  ·  Software - Infrastructure
price=35.61000061035156  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.77 on 2026-11-19; prev RSI=67.14 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.14@2026-11-18 → 63.77@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.14@2026-11-18 → 63.77@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.14@2026-11-18 → 63.77@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=1.727 (G=0.9500 R=0.5500); 2026-11-18:GREEN:O=35.0900,C=36.0400,body=+0.9500,vol=2256400.0; 2026-11-19:RED:O=36.1600,C=35.6100,body=-0.5500,vol=2077200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.086 (Gvol=2256400 Rvol=2077200); 2026-11-18:GREEN:O=35.0900,C=36.0400,body=+0.9500,vol=2256400.0; 2026-11-19:RED:O=36.1600,C=35.6100,body=-0.5500,vol=2077200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.945 on 2026-11-19: today_vol=2077200 / avg20=2197554 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.663 on 2026-11-19 (price=35.6100, mid=34.1405, upper=36.3572, lower=31.9238; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=35.6100 vs SMA50=31.6592 dist=+12.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=34.1405 SMA50=31.6592 SMA80=30.3995 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-08-26@26.7500; S2[2026-09-23→2026-10-21] low=2026-09-28@27.9700; S3[2026-10-22→2026-11-19] low=2026-11-10@31.2300 | lows=[26.75, 27.969999313354492, 31.229999542236328] span=16.75% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.6089743119490396 wick_frac=0.3910256880509604 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.305555261211506 wick_frac=0.694444738788494 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.727276510448817 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:GREEN:body=+0.3000:wick=0.6000; 2026-11-16:GREEN:body=+0.4500:wick=0.5400; 2026-11-17:GREEN:body=+0.3200:wick=0.5100; 2026-11-18:GREEN:body=+0.9500:wick=0.6100; 2026-11-19:RED:body=-0.5500:wick=1.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=20.94 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.84 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1674.0 | **NEUTRAL** |
| `B04_income` | 456.0 | **GOOD** |
| `B05_profit_margin` | 27.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.38 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.38 vs prior_export=35.38 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.78 | **NEUTRAL** |
| `B10_insider_transactions` | -5.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.01 vs prior=-5.01 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.15 | **GOOD** |
| `B13_short_float` | 15.31 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.94 (this export) | prior_export=20.94 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.84 (this export) | prior_export=0.84 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LFTO  ·  score **+15**  ·  Advertising Agencies
price=23.65999984741211  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.41 on 2026-11-19; prev RSI=71.30 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 71.30@2026-11-18 → 69.41@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.30@2026-11-18 → 69.41@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 71.30@2026-11-18 → 69.41@2026-11-19 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=1.659 (G=0.7300 R=0.4400); 2026-11-18:GREEN:O=23.2700,C=24.0000,body=+0.7300,vol=1243600.0; 2026-11-19:RED:O=24.1000,C=23.6600,body=-0.4400,vol=1070400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.162 (Gvol=1243600 Rvol=1070400); 2026-11-18:GREEN:O=23.2700,C=24.0000,body=+0.7300,vol=1243600.0; 2026-11-19:RED:O=24.1000,C=23.6600,body=-0.4400,vol=1070400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.724 on 2026-11-19: today_vol=1070400 / avg20=1478458 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.747 on 2026-11-19 (price=23.6600, mid=18.5255, upper=25.3963, lower=11.6547; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=23.6600 vs SMA50=17.2528 dist=+37.14% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=18.53_50=17.25_80=18.55 on 2026-11-19: SMA20=18.5255 SMA50=17.2528 SMA80=18.5492 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-22@16.1400; S2[2026-09-23→2026-10-21] low=2026-10-05@15.0200; S3[2026-10-22→2026-11-19] low=2026-11-02@13.9550 | lows=[16.139999389648438, 15.020000457763672, 13.954999923706055] span=15.66% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.5572517861380833 wick_frac=0.44274821386191665 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.31428618139162917 wick_frac=0.6857138186083708 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6590878549723218 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:GREEN:body=+0.7000:wick=2.8099; 2026-11-16:RED:body=-0.4700:wick=1.4700; 2026-11-17:RED:body=-2.2500:wick=0.6000; 2026-11-18:GREEN:body=+0.7300:wick=0.5800; 2026-11-19:RED:body=-0.4400:wick=0.9600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=161.69 (current export asof; earnings_date=11/12/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.14 (current export; earnings_date=11/12/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 856.83 | **NEUTRAL** |
| `B04_income` | 134.31 | **GOOD** |
| `B05_profit_margin` | 15.68 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 30.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.5500000000000007 (now=30.94 vs prior_export=30.39 on finviz_2026-11-18) | **GOOD** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 1.92 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.92 vs prior=1.92 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 18.3 | **GOOD** |
| `B13_short_float` | 11.21 | **NEUTRAL** |
| `B14_earnings_date` | 11/12/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=161.69 (this export) | prior_export=161.69 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.14 (this export) | prior_export=7.14 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNT  ·  score **+15**  ·  Utilities - Regulated Electric
price=71.4000015258789  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.02 on 2026-11-19; prev RSI=55.56 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.56@2026-11-18 → 52.02@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.56@2026-11-18 → 52.02@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.56@2026-11-18 → 52.02@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=3.108 (G=1.1500 R=0.3700); 2026-11-18:GREEN:O=70.9300,C=72.0800,body=+1.1500,vol=2978400.0; 2026-11-19:RED:O=71.7700,C=71.4000,body=-0.3700,vol=2573100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.158 (Gvol=2978400 Rvol=2573100); 2026-11-18:GREEN:O=70.9300,C=72.0800,body=+1.1500,vol=2978400.0; 2026-11-19:RED:O=71.7700,C=71.4000,body=-0.3700,vol=2573100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.781 on 2026-11-19: today_vol=2573100 / avg20=3295473 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.208 on 2026-11-19 (price=71.4000, mid=72.0210, upper=75.0003, lower=69.0417; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=71.4000 vs SMA50=68.3336 dist=+4.49% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=72.02_50=68.33_80=68.80 on 2026-11-19: SMA20=72.0210 SMA50=68.3336 SMA80=68.8007 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-16@65.8000; S2[2026-09-18→2026-10-21] low=2026-10-01@62.5400; S3[2026-10-22→2026-11-19] low=2026-10-22@68.5100 | lows=[65.80000305175781, 62.540000915527344, 68.51000213623047] span=9.55% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.7098762815712758 wick_frac=0.29012371842872414 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.4683477068384405 wick_frac=0.5316522931615595 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.108153249752557 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:GREEN:body=+0.0400:wick=0.6900; 2026-11-16:RED:body=-1.1300:wick=0.4700; 2026-11-17:RED:body=-1.1200:wick=0.3500; 2026-11-18:GREEN:body=+1.1500:wick=0.4700; 2026-11-19:RED:body=-0.3700:wick=0.4200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.41 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.93 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4504.0 | **NEUTRAL** |
| `B04_income` | 934.0 | **GOOD** |
| `B05_profit_margin` | 20.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 77.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=77.08 vs prior_export=77.08 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.93 | **GOOD** |
| `B13_short_float` | 11.02 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.41 (this export) | prior_export=16.41 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.93 (this export) | prior_export=1.93 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BOOT  ·  score **+15**  ·  Apparel Retail
price=139.3699951171875  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.23 on 2026-11-19; prev RSI=59.58 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.58@2026-11-18 → 57.23@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.58@2026-11-18 → 57.23@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.58@2026-11-18 → 57.23@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=46.331 (G=6.9500 R=0.1500); 2026-11-18:GREEN:O=134.5400,C=141.4900,body=+6.9500,vol=880200.0; 2026-11-19:RED:O=139.5200,C=139.3700,body=-0.1500,vol=796400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.105 (Gvol=880200 Rvol=796400); 2026-11-18:GREEN:O=134.5400,C=141.4900,body=+6.9500,vol=880200.0; 2026-11-19:RED:O=139.5200,C=139.3700,body=-0.1500,vol=796400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.851 on 2026-11-19: today_vol=796400 / avg20=936125 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.405 on 2026-11-19 (price=139.3700, mid=131.4155, upper=151.0737, lower=111.7573; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=139.3700 vs SMA50=128.2168 dist=+8.70% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=131.42_50=128.22_80=139.62 on 2026-11-19: SMA20=131.4155 SMA50=128.2168 SMA80=139.6246 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-19→2026-11-19 (63 bars); S1[2026-08-19→2026-09-17] low=2026-09-17@120.6400; S2[2026-09-18→2026-10-21] low=2026-10-21@111.6000; S3[2026-10-22→2026-11-19] low=2026-10-27@110.9050 | lows=[120.63999938964844, 111.5999984741211, 110.90499877929688] span=8.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.847562779705803 wick_frac=0.152437220294197 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.0350488780508674 wick_frac=0.9649511219491326 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=46.33058691892992 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:RED:body=-0.5400:wick=3.6200; 2026-11-16:RED:body=-3.3700:wick=4.2700; 2026-11-17:RED:body=-1.2600:wick=7.3100; 2026-11-18:GREEN:body=+6.9500:wick=1.2500; 2026-11-19:RED:body=-0.1500:wick=4.1300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.78 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.17 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2419.72 | **NEUTRAL** |
| `B04_income` | 252.86 | **GOOD** |
| `B05_profit_margin` | 10.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 180.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=180.67 vs prior_export=180.67 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | 0.76 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.76 vs prior=0.76 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.16 | **BAD** |
| `B13_short_float` | 6.78 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.78 (this export) | prior_export=5.78 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.17 (this export) | prior_export=0.17 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARDT  ·  score **+15**  ·  Medical Care Facilities
price=14.5  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.75 on 2026-11-19; prev RSI=49.17 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.17@2026-11-18 → 49.75@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.17@2026-11-18 → 49.75@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.17@2026-11-18 → 49.75@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.8200 R=0.0000); 2026-11-18:GREEN:O=13.7500,C=14.4000,body=+0.6500,vol=7269200.0; 2026-11-19:GREEN:O=14.3300,C=14.5000,body=+0.1700,vol=2193200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9462400 Rvol=0); 2026-11-18:GREEN:O=13.7500,C=14.4000,body=+0.6500,vol=7269200.0; 2026-11-19:GREEN:O=14.3300,C=14.5000,body=+0.1700,vol=2193200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.890 on 2026-11-19: today_vol=2193200 / avg20=1160654 (avg window 2026-10-22→2026-11-18, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.059 on 2026-11-19 (price=14.5000, mid=14.8245, upper=20.3001, lower=9.3489; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=14.5000 vs SMA50=12.5756 dist=+15.30% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=14.8245 SMA50=12.5756 SMA80=11.6462 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-11-19 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-14@9.0100; S2[2026-08-14→2026-10-21] low=2026-08-21@10.3500; S3[2026-10-22→2026-11-19] low=2026-11-03@11.5500 | lows=[9.010000228881836, 10.350000381469727, 11.550000190734863] span=28.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.45286117991152053 wick_frac=0.5471388200884796 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:RED:body=-0.2700:wick=0.5599; 2026-11-16:GREEN:body=+0.0500:wick=0.4500; 2026-11-17:RED:body=-0.3400:wick=0.3500; 2026-11-18:GREEN:body=+0.6500:wick=0.4200; 2026-11-19:GREEN:body=+0.1700:wick=0.4000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=33.62 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.31 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6454.05 | **NEUTRAL** |
| `B04_income` | 146.22 | **GOOD** |
| `B05_profit_margin` | 2.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.77 vs prior_export=17.77 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.78 | **GOOD** |
| `B13_short_float` | 15.69 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.62 (this export) | prior_export=33.62 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.31 (this export) | prior_export=1.31 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CCEP  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=116.26000213623047  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.62 on 2026-11-19; prev RSI=63.79 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.79@2026-11-18 → 55.62@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.79@2026-11-18 → 55.62@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.79@2026-11-18 → 55.62@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=1.253 (G=1.0400 R=0.8300); 2026-11-18:GREEN:O=117.8200,C=118.8600,body=+1.0400,vol=1829600.0; 2026-11-19:RED:O=117.0900,C=116.2600,body=-0.8300,vol=1467900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.246 (Gvol=1829600 Rvol=1467900); 2026-11-18:GREEN:O=117.8200,C=118.8600,body=+1.0400,vol=1829600.0; 2026-11-19:RED:O=117.0900,C=116.2600,body=-0.8300,vol=1467900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.838 on 2026-11-19: today_vol=1467900 / avg20=1750830 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.151 on 2026-11-19 (price=116.2600, mid=115.3420, upper=121.4369, lower=109.2471; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=116.2600 vs SMA50=108.7028 dist=+6.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=115.3420 SMA50=108.7028 SMA80=108.2959 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-18@99.3600; S2[2026-09-23→2026-10-21] low=2026-09-23@99.0000; S3[2026-10-22→2026-11-19] low=2026-10-29@109.6700 | lows=[99.36000061035156, 99.0, 109.66999816894531] span=10.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.5252520971166332 wick_frac=0.47474790288336677 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.37899236364649813 wick_frac=0.6210076363535019 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2530219047881679 need>1.4; red_wick_gt_green=True 5d trail=2026-11-13:GREEN:body=+0.6500:wick=0.3400; 2026-11-16:RED:body=-0.3800:wick=1.0900; 2026-11-17:GREEN:body=+0.3300:wick=1.1800; 2026-11-18:GREEN:body=+1.0400:wick=0.9400; 2026-11-19:RED:body=-0.8300:wick=1.3600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.67 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.0 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 24903.24 | **NEUTRAL** |
| `B04_income` | 2328.07 | **GOOD** |
| `B05_profit_margin` | 9.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 111.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.1700000000000017 (now=111.06 vs prior_export=110.89 on finviz_2026-11-18) | **GOOD** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | -3.48 | **BAD** |
| `B13_short_float` | 6.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.67 (this export) | prior_export=2.67 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.0 (this export) | prior_export=1.0 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DGX  ·  score **+15**  ·  Diagnostics & Research
price=259.0899963378906  pair=`2026-11-18→2026-11-19`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.32 on 2026-11-19; prev RSI=63.44 on 2026-11-18 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.44@2026-11-18 → 62.32@2026-11-19 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.44@2026-11-18 → 62.32@2026-11-19 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.44@2026-11-18 → 62.32@2026-11-19 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_body_sum/RED_body_sum=5.835 (G=5.3100 R=0.9100); 2026-11-18:GREEN:O=254.3800,C=259.6900,body=+5.3100,vol=1301900.0; 2026-11-19:RED:O=260.0000,C=259.0900,body=-0.9100,vol=1108500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-18 + 2026-11-19; ratio=GREEN_vol/RED_vol=1.174 (Gvol=1301900 Rvol=1108500); 2026-11-18:GREEN:O=254.3800,C=259.6900,body=+5.3100,vol=1301900.0; 2026-11-19:RED:O=260.0000,C=259.0900,body=-0.9100,vol=1108500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.157 on 2026-11-19: today_vol=1108500 / avg20=957669 (avg window 2026-10-22→2026-11-18, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.409 on 2026-11-19 (price=259.0900, mid=253.7535, upper=266.7901, lower=240.7169; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-19: price=259.0900 vs SMA50=243.2606 dist=+6.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-19: SMA20=253.7535 SMA50=243.2606 SMA80=241.3772 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-24→2026-11-19 (63 bars); S1[2026-08-24→2026-09-22] low=2026-09-22@227.2400; S2[2026-09-23→2026-10-21] low=2026-10-21@223.5000; S3[2026-10-22→2026-11-19] low=2026-10-22@226.5000 | lows=[227.24000549316406, 223.5, 226.5] span=1.67% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: GREEN body_frac=0.6389890636131942 wick_frac=0.3610109363868058 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-18+2026-11-19: RED body_frac=0.25 wick_frac=0.75 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.835138669975519 need>1.4; red_wick_gt_green=False 5d trail=2026-11-13:RED:body=-0.5700:wick=1.7800; 2026-11-16:RED:body=-0.0700:wick=2.3600; 2026-11-17:RED:body=-4.3000:wick=2.5500; 2026-11-18:GREEN:body=+5.3100:wick=3.0000; 2026-11-19:RED:body=-0.9100:wick=2.7300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.35 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.47 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 11841.0 | **NEUTRAL** |
| `B04_income` | 1125.0 | **GOOD** |
| `B05_profit_margin` | 9.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 258.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=258.29 vs prior_export=258.29 on finviz_2026-11-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.11 | **GOOD** |
| `B10_insider_transactions` | -15.82 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-15.82 vs prior=-15.82 on finviz_2026-11-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.96 | **GOOD** |
| `B13_short_float` | 3.41 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.35 (this export) | prior_export=4.35 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.47 (this export) | prior_export=1.47 (finviz_2026-11-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-19_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.