# A+B1 Feature Checklist — 2026-09-03

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,677** names
- Export: `finviz_2026-09-03.csv` · prior export for Δ: `2026-09-02`
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
| 1 | NWBI | +19 | 19 | 0 | 2026-09-01→2026-09-02 | Banks - Regional |
| 2 | BIIB | +17 | 18 | 1 | 2026-09-01→2026-09-02 | Drug Manufacturers - General |
| 3 | EVTC | +17 | 18 | 1 | 2026-09-01→2026-09-02 | Software - Infrastructure |
| 4 | AMCR | +17 | 17 | 0 | 2026-09-01→2026-09-02 | Packaging & Containers |
| 5 | SONO | +17 | 18 | 1 | 2026-09-01→2026-09-02 | Consumer Electronics |
| 6 | SHEL | +17 | 17 | 0 | 2026-09-01→2026-09-02 | Oil & Gas Integrated |
| 7 | INSW | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Oil & Gas Midstream |
| 8 | BMRN | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Biotechnology |
| 9 | FCF | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Banks - Regional |
| 10 | RELY | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Software - Infrastructure |
| 11 | ARVN | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Biotechnology |
| 12 | AHR | +16 | 17 | 1 | 2026-09-01→2026-09-02 | REIT - Healthcare Facilities |
| 13 | NUE | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Steel |
| 14 | PAY | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Software - Infrastructure |
| 15 | HSBC | +16 | 17 | 1 | 2026-09-01→2026-09-02 | Banks - Diversified |

## Full checklist — top 15

### NWBI  ·  score **+19**  ·  Banks - Regional
price=15.430000305175781  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.70 on 2026-09-02; prev RSI=41.53 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 41.53@2026-09-01 → 52.70@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 41.53@2026-09-01 → 52.70@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 41.53@2026-09-01 → 52.70@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.000 (G=0.2400 R=0.1200); 2026-09-01:RED:O=15.2600,C=15.1400,body=-0.1200,vol=926000.0; 2026-09-02:GREEN:O=15.1900,C=15.4300,body=+0.2400,vol=1245000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.344 (Gvol=1245000 Rvol=926000); 2026-09-01:RED:O=15.2600,C=15.1400,body=-0.1200,vol=926000.0; 2026-09-02:GREEN:O=15.1900,C=15.4300,body=+0.2400,vol=1245000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.608 on 2026-09-02: today_vol=1245000 / avg20=774145 (avg window 2026-08-05→2026-09-01, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.109 on 2026-09-02 (price=15.4300, mid=15.4715, upper=15.8538, lower=15.0892; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=15.4300 vs SMA50=15.2370 dist=+1.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=15.4715 SMA50=15.2370 SMA80=14.7235 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-03@13.6073; S2[2026-07-01→2026-08-04] low=2026-07-08@14.5257; S3[2026-08-05→2026-09-02] low=2026-09-01@15.0700 | lows=[13.607317740879719, 14.525664138598916, 15.069999694824219] span=10.75% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7500029802350809 wick_frac=0.24999701976491906 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.3870958810554392 wick_frac=0.6129041189445609 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.000007947293549 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:GREEN:body=+0.1500:wick=0.0700; 2026-08-28:RED:body=-0.0400:wick=0.1200; 2026-08-31:RED:body=-0.0600:wick=0.0800; 2026-09-01:RED:body=-0.1200:wick=0.1900; 2026-09-02:GREEN:body=+0.2400:wick=0.0800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.02 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.12 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 940.9 | **NEUTRAL** |
| `B04_income` | 152.92 | **GOOD** |
| `B05_profit_margin` | 16.25 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 16.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=16.57 vs prior_export=16.57 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.75 | **NEUTRAL** |
| `B10_insider_transactions` | 0.19 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.19 vs prior=0.19 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.57 | **GOOD** |
| `B13_short_float` | 5.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.02 (this export) | prior_export=10.02 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.12 (this export) | prior_export=1.12 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BIIB  ·  score **+17**  ·  Drug Manufacturers - General
price=222.6699981689453  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.57 on 2026-09-02; prev RSI=54.68 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.68@2026-09-01 → 61.57@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.68@2026-09-01 → 61.57@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.68@2026-09-01 → 61.57@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.159 (G=4.4700 R=2.0700); 2026-09-01:RED:O=218.1100,C=216.0400,body=-2.0700,vol=785500.0; 2026-09-02:GREEN:O=218.2000,C=222.6700,body=+4.4700,vol=1268200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.615 (Gvol=1268200 Rvol=785500); 2026-09-01:RED:O=218.1100,C=216.0400,body=-2.0700,vol=785500.0; 2026-09-02:GREEN:O=218.2000,C=222.6700,body=+4.4700,vol=1268200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.737 on 2026-09-02: today_vol=1268200 / avg20=730295 (avg window 2026-08-05→2026-09-01, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.714 on 2026-09-02 (price=222.6700, mid=214.2150, upper=226.0561, lower=202.3739; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=222.6700 vs SMA50=208.4438 dist=+6.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=214.2150 SMA50=208.4438 SMA80=203.3438 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-02@184.9000; S2[2026-07-01→2026-08-04] low=2026-07-15@186.0300; S3[2026-08-05→2026-09-02] low=2026-08-12@201.0000 | lows=[184.89999389648438, 186.02999877929688, 201.0] span=8.71% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.518561920271897 wick_frac=0.4814380797281031 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.3631593741216153 wick_frac=0.6368406258783847 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1594132389798024 need>1.4; red_wick_gt_green=False 5d trail=2026-08-27:GREEN:body=+2.1100:wick=2.4700; 2026-08-28:RED:body=-2.6700:wick=2.5500; 2026-08-31:GREEN:body=+0.1800:wick=5.1300; 2026-09-01:RED:body=-2.0700:wick=3.6300; 2026-09-02:GREEN:body=+4.4700:wick=4.1500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=22.49 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.2 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9661.4 | **NEUTRAL** |
| `B04_income` | 834.6 | **GOOD** |
| `B05_profit_margin` | 8.64 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 238.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.6899999999999977 (now=238.93 vs prior_export=238.24 on finviz_2026-09-02) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | -0.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.11 vs prior=-0.11 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.02 | **GOOD** |
| `B13_short_float` | 3.54 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=22.49 (this export) | prior_export=22.49 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.2 (this export) | prior_export=11.2 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EVTC  ·  score **+17**  ·  Software - Infrastructure
price=30.020000457763672  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.20 on 2026-09-02; prev RSI=44.98 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.98@2026-09-01 → 51.20@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.98@2026-09-01 → 51.20@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.98@2026-09-01 → 51.20@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.773 (G=0.8300 R=0.2200); 2026-09-01:RED:O=29.3800,C=29.1600,body=-0.2200,vol=364300.0; 2026-09-02:GREEN:O=29.1900,C=30.0200,body=+0.8300,vol=382700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.051 (Gvol=382700 Rvol=364300); 2026-09-01:RED:O=29.3800,C=29.1600,body=-0.2200,vol=364300.0; 2026-09-02:GREEN:O=29.1900,C=30.0200,body=+0.8300,vol=382700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.998 on 2026-09-02: today_vol=382700 / avg20=383495 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.012 on 2026-09-02 (price=30.0200, mid=30.0370, upper=31.4628, lower=28.6112; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=30.0200 vs SMA50=29.4918 dist=+1.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=30.0370 SMA50=29.4918 SMA80=27.5844 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-05@21.8100; S2[2026-07-01→2026-08-04] low=2026-07-09@27.4100; S3[2026-08-05→2026-09-02] low=2026-09-01@28.9800 | lows=[21.809999465942383, 27.40999984741211, 28.979999542236328] span=32.87% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.6535430114470914 wick_frac=0.34645698855290863 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.4399986267089844 wick_frac=0.5600013732910156 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.772738701091527 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.2900:wick=0.4600; 2026-08-28:GREEN:body=+0.5700:wick=0.2000; 2026-08-31:RED:body=-0.1800:wick=0.6200; 2026-09-01:RED:body=-0.2200:wick=0.2800; 2026-09-02:GREEN:body=+0.8300:wick=0.4400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.53 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.8 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 996.16 | **NEUTRAL** |
| `B04_income` | 97.58 | **GOOD** |
| `B05_profit_margin` | 9.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.6 vs prior_export=35.6 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 3.4 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.4 vs prior=3.4 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -2.01 | **BAD** |
| `B13_short_float` | 4.44 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.53 (this export) | prior_export=10.53 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.8 (this export) | prior_export=4.8 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMCR  ·  score **+17**  ·  Packaging & Containers
price=46.68000030517578  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.94 on 2026-09-02; prev RSI=47.68 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.68@2026-09-01 → 52.94@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.68@2026-09-01 → 52.94@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.68@2026-09-01 → 52.94@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.6800 R=0.1700); 2026-09-01:RED:O=45.9900,C=45.8200,body=-0.1700,vol=2605400.0; 2026-09-02:GREEN:O=46.0000,C=46.6800,body=+0.6800,vol=3134800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.203 (Gvol=3134800 Rvol=2605400); 2026-09-01:RED:O=45.9900,C=45.8200,body=-0.1700,vol=2605400.0; 2026-09-02:GREEN:O=46.0000,C=46.6800,body=+0.6800,vol=3134800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.848 on 2026-09-02: today_vol=3134800 / avg20=3698440 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.120 on 2026-09-02 (price=46.6800, mid=46.8775, upper=48.5300, lower=45.2250; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=46.6800 vs SMA50=45.1470 dist=+3.40% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=46.8775 SMA50=45.1470 SMA80=42.7362 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-03→2026-09-02 (63 bars); S1[2026-06-03→2026-07-02] low=2026-06-04@37.4600; S2[2026-07-06→2026-08-04] low=2026-07-08@41.4500; S3[2026-08-05→2026-09-02] low=2026-08-17@44.8100 | lows=[37.459999084472656, 41.45000076293945, 44.810001373291016] span=19.62% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.6732662048752484 wick_frac=0.32673379512475165 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.253733552725223 wick_frac=0.746266447274777 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.999955121732301 need>1.4; red_wick_gt_green=False 5d trail=2026-08-27:GREEN:body=+0.0400:wick=0.8700; 2026-08-28:RED:body=-0.3300:wick=0.3600; 2026-08-31:RED:body=-0.4500:wick=0.2500; 2026-09-01:RED:body=-0.1700:wick=0.5000; 2026-09-02:GREEN:body=+0.6800:wick=0.3300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.19 (current export asof; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.68 (current export; earnings_date=8/12/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 23506.0 | **NEUTRAL** |
| `B04_income` | 1106.0 | **GOOD** |
| `B05_profit_margin` | 4.71 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.48 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.48 vs prior_export=49.48 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.71 | **GOOD** |
| `B13_short_float` | 4.64 | **NEUTRAL** |
| `B14_earnings_date` | 8/12/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.19 (this export) | prior_export=3.19 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.68 (this export) | prior_export=5.68 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SONO  ·  score **+17**  ·  Consumer Electronics
price=15.979999542236328  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.12 on 2026-09-02; prev RSI=47.14 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.14@2026-09-01 → 54.12@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.14@2026-09-01 → 54.12@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.14@2026-09-01 → 54.12@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.320 (G=0.8300 R=0.2500); 2026-09-01:RED:O=15.3900,C=15.1400,body=-0.2500,vol=1350200.0; 2026-09-02:GREEN:O=15.1500,C=15.9800,body=+0.8300,vol=2161100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.601 (Gvol=2161100 Rvol=1350200); 2026-09-01:RED:O=15.3900,C=15.1400,body=-0.2500,vol=1350200.0; 2026-09-02:GREEN:O=15.1500,C=15.9800,body=+0.8300,vol=2161100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.613 on 2026-09-02: today_vol=2161100 / avg20=1339520 (avg window 2026-08-05→2026-09-01, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.397 on 2026-09-02 (price=15.9800, mid=15.6400, upper=16.4963, lower=14.7837; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=15.9800 vs SMA50=15.0390 dist=+6.26% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.64_50=15.04_80=15.11 on 2026-09-02: SMA20=15.6400 SMA50=15.0390 SMA80=15.1094 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-29@13.0800; S2[2026-07-02→2026-08-04] low=2026-07-02@13.3600; S3[2026-08-05→2026-09-02] low=2026-08-12@14.9000 | lows=[13.079999923706055, 13.359999656677246, 14.899999618530273] span=13.91% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.5354840853212838 wick_frac=0.46451591467871617 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.2604166563186385 wick_frac=0.7395833436813616 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.3199996948242188 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.2600:wick=0.2600; 2026-08-28:GREEN:body=+0.1800:wick=0.1700; 2026-08-31:GREEN:body=+0.2700:wick=0.0700; 2026-09-01:RED:body=-0.2500:wick=0.7100; 2026-09-02:GREEN:body=+0.8300:wick=0.7200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=525.0 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.63 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 102.79 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=102.79 vs prior=102.79 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.34 | **BAD** |
| `B13_short_float` | 6.74 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=525.0 (this export) | prior_export=525.0 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.63 (this export) | prior_export=2.63 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SHEL  ·  score **+17**  ·  Oil & Gas Integrated
price=92.80000305175781  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.07 on 2026-09-02; prev RSI=64.79 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.79@2026-09-01 → 61.07@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.79@2026-09-01 → 61.07@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.79@2026-09-01 → 61.07@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=4.389 (G=0.7900 R=0.1800); 2026-09-01:GREEN:O=92.7200,C=93.5100,body=+0.7900,vol=11105545.0; 2026-09-02:RED:O=92.9800,C=92.8000,body=-0.1800,vol=8104060.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.370 (Gvol=11105545 Rvol=8104060); 2026-09-01:GREEN:O=92.7200,C=93.5100,body=+0.7900,vol=11105545.0; 2026-09-02:RED:O=92.9800,C=92.8000,body=-0.1800,vol=8104060.0 | **GOOD** |
| `A07_rvol` | RVOL=1.424 on 2026-09-02: today_vol=8104060 / avg20=5690657 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.463 on 2026-09-02 (price=92.8000, mid=91.1706, upper=94.6909, lower=87.6502; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=92.8000 vs SMA50=86.4619 dist=+7.33% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=91.1706 SMA50=86.4619 SMA80=85.4599 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-02 (63 bars); S1[2026-06-04→2026-07-06] low=2026-07-01@75.5975; S2[2026-07-07→2026-08-04] low=2026-07-07@79.3248; S3[2026-08-05→2026-09-02] low=2026-08-05@86.8885 | lows=[75.59747315421986, 79.32480078270552, 86.88852126928514] span=14.94% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.5683462319556507 wick_frac=0.4316537680443493 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.17354429634860388 wick_frac=0.8264557036513961 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.388886534141482 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.5300:wick=0.7700; 2026-08-28:GREEN:body=+0.1700:wick=0.4450; 2026-08-31:RED:body=-0.5900:wick=0.9400; 2026-09-01:GREEN:body=+0.7900:wick=0.6000; 2026-09-02:RED:body=-0.1800:wick=0.8572 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.9 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.79 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 296116.54 | **NEUTRAL** |
| `B04_income` | 25941.85 | **GOOD** |
| `B05_profit_margin` | 8.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.09 vs prior_export=100.09 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.34 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.31 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.9 (this export) | prior_export=14.9 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.79 (this export) | prior_export=9.79 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### INSW  ·  score **+16**  ·  Oil & Gas Midstream
price=101.19000244140625  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.36 on 2026-09-02; prev RSI=59.93 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.93@2026-09-01 → 62.36@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.93@2026-09-01 → 62.36@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.93@2026-09-01 → 62.36@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.283 (G=1.3700 R=0.6000); 2026-09-01:RED:O=100.3600,C=99.7600,body=-0.6000,vol=577700.0; 2026-09-02:GREEN:O=99.8200,C=101.1900,body=+1.3700,vol=603000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.044 (Gvol=603000 Rvol=577700); 2026-09-01:RED:O=100.3600,C=99.7600,body=-0.6000,vol=577700.0; 2026-09-02:GREEN:O=99.8200,C=101.1900,body=+1.3700,vol=603000.0 | **GOOD** |
| `A07_rvol` | RVOL=1.129 on 2026-09-02: today_vol=603000 / avg20=534005 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.607 on 2026-09-02 (price=101.1900, mid=96.7855, upper=104.0457, lower=89.5253; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=101.1900 vs SMA50=91.3250 dist=+10.80% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=96.7855 SMA50=91.3250 SMA80=86.7781 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-02@72.5391; S2[2026-07-02→2026-08-04] low=2026-07-02@79.3700; S3[2026-08-05→2026-09-02] low=2026-08-11@86.1300 | lows=[72.53909666054166, 79.37000274658203, 86.12999725341797] span=18.74% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.5637870677069434 wick_frac=0.43621293229305663 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.18662493236765418 wick_frac=0.8133750676323458 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.28334371781341 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:GREEN:body=+4.1800:wick=1.6100; 2026-08-28:RED:body=-0.4700:wick=2.3200; 2026-08-31:RED:body=-3.0200:wick=1.0000; 2026-09-01:RED:body=-0.6000:wick=2.6150; 2026-09-02:GREEN:body=+1.3700:wick=1.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.4 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.5 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1259.47 | **NEUTRAL** |
| `B04_income` | 779.12 | **GOOD** |
| `B05_profit_margin` | 61.86 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.11 vs prior_export=103.11 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | -1.29 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.29 vs prior=-1.29 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.09 | **GOOD** |
| `B13_short_float` | 6.51 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.4 (this export) | prior_export=6.4 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.5 (this export) | prior_export=6.5 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BMRN  ·  score **+16**  ·  Biotechnology
price=66.97000122070312  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.17 on 2026-09-02; prev RSI=51.24 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.24@2026-09-01 → 58.17@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.24@2026-09-01 → 58.17@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.24@2026-09-01 → 58.17@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.814 (G=1.9700 R=0.7000); 2026-09-01:RED:O=65.5400,C=64.8400,body=-0.7000,vol=1782400.0; 2026-09-02:GREEN:O=65.0000,C=66.9700,body=+1.9700,vol=2520800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.414 (Gvol=2520800 Rvol=1782400); 2026-09-01:RED:O=65.5400,C=64.8400,body=-0.7000,vol=1782400.0; 2026-09-02:GREEN:O=65.0000,C=66.9700,body=+1.9700,vol=2520800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.095 on 2026-09-02: today_vol=2520800 / avg20=2301855 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.098 on 2026-09-02 (price=66.9700, mid=66.5485, upper=70.8647, lower=62.2323; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=66.9700 vs SMA50=62.0748 dist=+7.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=66.5485 SMA50=62.0748 SMA80=59.0999 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-02→2026-09-02 (63 bars); S1[2026-06-02→2026-07-01] low=2026-06-03@53.0100; S2[2026-07-02→2026-08-04] low=2026-07-23@57.4200; S3[2026-08-05→2026-09-02] low=2026-08-06@58.8900 | lows=[53.0099983215332, 57.41999816894531, 58.88999938964844] span=11.09% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8914036365644926 wick_frac=0.10859636343550742 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.4575196968185898 wick_frac=0.5424803031814102 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8142690542882365 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:GREEN:body=+0.0100:wick=1.0800; 2026-08-28:RED:body=-0.3200:wick=0.6900; 2026-08-31:RED:body=-0.7200:wick=1.2500; 2026-09-01:RED:body=-0.7000:wick=0.8300; 2026-09-02:GREEN:body=+1.9700:wick=0.2400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.66 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.19 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3456.79 | **NEUTRAL** |
| `B04_income` | 72.97 | **GOOD** |
| `B05_profit_margin` | 2.11 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 91.48 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=91.48 vs prior_export=91.48 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | -0.78 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.78 vs prior=-0.78 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.86 | **GOOD** |
| `B13_short_float` | 5.34 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.66 (this export) | prior_export=26.66 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.19 (this export) | prior_export=6.19 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FCF  ·  score **+16**  ·  Banks - Regional
price=20.989999771118164  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.67 on 2026-09-02; prev RSI=39.13 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 39.13@2026-09-01 → 50.67@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 39.13@2026-09-01 → 50.67@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 39.13@2026-09-01 → 50.67@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.333 (G=0.3000 R=0.0900); 2026-09-01:RED:O=20.6200,C=20.5300,body=-0.0900,vol=838700.0; 2026-09-02:GREEN:O=20.6900,C=20.9900,body=+0.3000,vol=854800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.019 (Gvol=854800 Rvol=838700); 2026-09-01:RED:O=20.6200,C=20.5300,body=-0.0900,vol=838700.0; 2026-09-02:GREEN:O=20.6900,C=20.9900,body=+0.3000,vol=854800.0 | **GOOD** |
| `A07_rvol` | RVOL=1.324 on 2026-09-02: today_vol=854800 / avg20=645410 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.245 on 2026-09-02 (price=20.9900, mid=21.1635, upper=21.8715, lower=20.4555; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=20.9900 vs SMA50=20.8049 dist=+0.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=21.1635 SMA50=20.8049 SMA80=20.0226 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-01@18.3793; S2[2026-07-01→2026-08-04] low=2026-07-08@19.7204; S3[2026-08-05→2026-09-02] low=2026-09-01@20.4700 | lows=[18.379253937243217, 19.720443433739185, 20.469999313354492] span=11.38% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.5769232179995525 wick_frac=0.4230767820004475 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.2727277981677889 wick_frac=0.7272722018322111 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.333319204848896 need>1.4; red_wick_gt_green=False 5d trail=2026-08-27:GREEN:body=+0.1400:wick=0.2100; 2026-08-28:DOJI:body=+0.0000:wick=0.2200; 2026-08-31:RED:body=-0.1400:wick=0.1300; 2026-09-01:RED:body=-0.0900:wick=0.2400; 2026-09-02:GREEN:body=+0.3000:wick=0.2200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.95 (current export asof; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.41 (current export; earnings_date=7/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 743.09 | **NEUTRAL** |
| `B04_income` | 168.34 | **GOOD** |
| `B05_profit_margin` | 22.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.08 vs prior_export=24.08 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | -6.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.66 vs prior=-6.66 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.08 | **GOOD** |
| `B13_short_float` | 3.05 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.95 (this export) | prior_export=3.95 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.41 (this export) | prior_export=1.41 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RELY  ·  score **+16**  ·  Software - Infrastructure
price=26.81999969482422  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.33 on 2026-09-02; prev RSI=53.63 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.63@2026-09-01 → 59.33@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.63@2026-09-01 → 59.33@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.63@2026-09-01 → 59.33@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=2.102 (G=1.1350 R=0.5400); 2026-09-01:RED:O=26.2500,C=25.7100,body=-0.5400,vol=4507700.0; 2026-09-02:GREEN:O=25.6850,C=26.8200,body=+1.1350,vol=5310400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.178 (Gvol=5310400 Rvol=4507700); 2026-09-01:RED:O=26.2500,C=25.7100,body=-0.5400,vol=4507700.0; 2026-09-02:GREEN:O=25.6850,C=26.8200,body=+1.1350,vol=5310400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.497 on 2026-09-02: today_vol=5310400 / avg20=3547015 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.573 on 2026-09-02 (price=26.8200, mid=25.5897, upper=27.7350, lower=23.4445; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=26.8200 vs SMA50=24.2315 dist=+10.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=25.5897 SMA50=24.2315 SMA80=22.9409 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-03→2026-09-02 (63 bars); S1[2026-06-03→2026-07-02] low=2026-06-11@17.7000; S2[2026-07-06→2026-08-04] low=2026-07-28@22.3200; S3[2026-08-05→2026-09-02] low=2026-08-10@22.7500 | lows=[17.700000762939453, 22.31999969482422, 22.75] span=28.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7516556667824549 wick_frac=0.24834433321754507 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.4218754889440894 wick_frac=0.5781245110559106 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1018487121886436 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:GREEN:body=+0.2350:wick=0.6550; 2026-08-28:GREEN:body=+0.7750:wick=0.4430; 2026-08-31:GREEN:body=+0.2300:wick=0.5300; 2026-09-01:RED:body=-0.5400:wick=0.7400; 2026-09-02:GREEN:body=+1.1350:wick=0.3750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=651.21 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.83 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1809.63 | **NEUTRAL** |
| `B04_income` | 305.01 | **GOOD** |
| `B05_profit_margin` | 16.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 32.15 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=32.15 vs prior_export=32.15 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.15 | **GOOD** |
| `B10_insider_transactions` | -42.88 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-42.88 vs prior=-42.88 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.78 | **GOOD** |
| `B13_short_float` | 7.45 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=651.21 (this export) | prior_export=651.21 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.83 (this export) | prior_export=1.83 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARVN  ·  score **+16**  ·  Biotechnology
price=9.59000015258789  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.41 on 2026-09-02; prev RSI=56.32 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.32@2026-09-01 → 61.41@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.32@2026-09-01 → 61.41@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.32@2026-09-01 → 61.41@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4500 R=0.0000); 2026-09-01:GREEN:O=9.1400,C=9.2800,body=+0.1400,vol=491100.0; 2026-09-02:GREEN:O=9.2800,C=9.5900,body=+0.3100,vol=548800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=1039900 Rvol=0); 2026-09-01:GREEN:O=9.1400,C=9.2800,body=+0.1400,vol=491100.0; 2026-09-02:GREEN:O=9.2800,C=9.5900,body=+0.3100,vol=548800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.705 on 2026-09-02: today_vol=548800 / avg20=778310 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.761 on 2026-09-02 (price=9.5900, mid=9.1775, upper=9.7195, lower=8.6355; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=9.5900 vs SMA50=8.6008 dist=+11.50% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=9.1775 SMA50=8.6008 SMA80=8.5805 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-10@6.9700; S2[2026-07-01→2026-08-04] low=2026-07-17@7.9000; S3[2026-08-05→2026-09-02] low=2026-08-05@8.2100 | lows=[6.96999979019165, 7.900000095367432, 8.210000038146973] span=17.79% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7105852887952296 wick_frac=0.2894147112047703 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.0300:wick=0.2500; 2026-08-28:GREEN:body=+0.0100:wick=0.2000; 2026-08-31:GREEN:body=+0.0800:wick=0.2300; 2026-09-01:GREEN:body=+0.1400:wick=0.1000; 2026-09-02:GREEN:body=+0.3100:wick=0.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=789.66 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=311.31 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 316.7 | **NEUTRAL** |
| `B04_income` | 9.3 | **GOOD** |
| `B05_profit_margin` | 2.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.93 vs prior_export=13.93 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.31 | **GOOD** |
| `B10_insider_transactions` | -0.58 | **BAD** |
| `B11_insider_tx_delta` | delta=0.5100000000000001 (now=-0.58 vs prior=-1.09 on finviz_2026-09-02) | **GOOD** |
| `B12_institutional_transactions` | 1.23 | **GOOD** |
| `B13_short_float` | 6.92 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=789.66 (this export) | prior_export=789.66 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=311.31 (this export) | prior_export=311.31 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AHR  ·  score **+16**  ·  REIT - Healthcare Facilities
price=56.209999084472656  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.72 on 2026-09-02; prev RSI=56.61 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.61@2026-09-01 → 54.72@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.61@2026-09-01 → 54.72@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.61@2026-09-01 → 54.72@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.026 (G=1.1500 R=0.3800); 2026-09-01:GREEN:O=55.3900,C=56.5400,body=+1.1500,vol=3166400.0; 2026-09-02:RED:O=56.5900,C=56.2100,body=-0.3800,vol=1901100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.666 (Gvol=3166400 Rvol=1901100); 2026-09-01:GREEN:O=55.3900,C=56.5400,body=+1.1500,vol=3166400.0; 2026-09-02:RED:O=56.5900,C=56.2100,body=-0.3800,vol=1901100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.612 on 2026-09-02: today_vol=1901100 / avg20=3108335 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.334 on 2026-09-02 (price=56.2100, mid=55.3240, upper=57.9751, lower=52.6729; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=56.2100 vs SMA50=54.4765 dist=+3.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=55.3240 SMA50=54.4765 SMA80=52.2659 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-08@44.5946; S2[2026-07-01→2026-08-04] low=2026-07-01@52.0250; S3[2026-08-05→2026-09-02] low=2026-08-11@52.3300 | lows=[44.59456890815638, 52.025001525878906, 52.33000183105469] span=17.35% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.8041966996206644 wick_frac=0.19580330037933555 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.3486247843296458 wick_frac=0.6513752156703542 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.026311298499222 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.0200:wick=0.6800; 2026-08-28:RED:body=-1.2200:wick=0.1250; 2026-08-31:RED:body=-0.1600:wick=0.4350; 2026-09-01:GREEN:body=+1.1500:wick=0.2800; 2026-09-02:RED:body=-0.3800:wick=0.7100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.62 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.98 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2502.39 | **NEUTRAL** |
| `B04_income` | 121.02 | **GOOD** |
| `B05_profit_margin` | 4.84 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 64.4 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.4000000000000057 (now=64.4 vs prior_export=64.0 on finviz_2026-09-02) | **GOOD** |
| `B09_analyst_recom` | 1.13 | **GOOD** |
| `B10_insider_transactions` | -1.87 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.87 vs prior=-1.87 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 17.06 | **GOOD** |
| `B13_short_float` | 13.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.62 (this export) | prior_export=16.62 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.98 (this export) | prior_export=3.98 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### NUE  ·  score **+16**  ·  Steel
price=263.9700012207031  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.48 on 2026-09-02; prev RSI=48.18 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.18@2026-09-01 → 58.48@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.18@2026-09-01 → 58.48@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.18@2026-09-01 → 58.48@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=13.2000 R=0.0000); 2026-09-01:GREEN:O=248.5800,C=251.9200,body=+3.3400,vol=1311400.0; 2026-09-02:GREEN:O=254.1100,C=263.9700,body=+9.8600,vol=1155500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2466900 Rvol=0); 2026-09-01:GREEN:O=248.5800,C=251.9200,body=+3.3400,vol=1311400.0; 2026-09-02:GREEN:O=254.1100,C=263.9700,body=+9.8600,vol=1155500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.791 on 2026-09-02: today_vol=1155500 / avg20=1461430 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.191 on 2026-09-02 (price=263.9700, mid=259.3655, upper=283.4503, lower=235.2807; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=263.9700 vs SMA50=247.8963 dist=+6.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=259.3655 SMA50=247.8963 SMA80=246.3294 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-03→2026-09-02 (63 bars); S1[2026-06-03→2026-07-02] low=2026-07-01@214.6100; S2[2026-07-06→2026-08-04] low=2026-07-06@220.0000; S3[2026-08-05→2026-09-02] low=2026-08-19@238.6200 | lows=[214.61000061035156, 220.0, 238.6199951171875] span=11.19% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.6722471565048633 wick_frac=0.32775284349513656 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:GREEN:body=+0.3700:wick=3.0900; 2026-08-28:RED:body=-2.2600:wick=5.7900; 2026-08-31:RED:body=-2.6200:wick=2.5400; 2026-09-01:GREEN:body=+3.3400:wick=3.2600; 2026-09-02:GREEN:body=+9.8600:wick=1.9000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.48 (current export asof; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.46 (current export; earnings_date=7/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 36101.0 | **NEUTRAL** |
| `B04_income` | 2873.0 | **GOOD** |
| `B05_profit_margin` | 7.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 287.93 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=287.93 vs prior_export=287.93 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -12.51 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-12.51 vs prior=-12.51 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.53 | **GOOD** |
| `B13_short_float` | 1.56 | **NEUTRAL** |
| `B14_earnings_date` | 7/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.48 (this export) | prior_export=8.48 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.46 (this export) | prior_export=2.46 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAY  ·  score **+16**  ·  Software - Infrastructure
price=36.279998779296875  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.23 on 2026-09-02; prev RSI=45.53 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.53@2026-09-01 → 49.23@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 45.53@2026-09-01 → 49.23@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 45.53@2026-09-01 → 49.23@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=12.000 (G=1.4400 R=0.1200); 2026-09-01:RED:O=35.2500,C=35.1300,body=-0.1200,vol=1136200.0; 2026-09-02:GREEN:O=34.8400,C=36.2800,body=+1.4400,vol=1980500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.743 (Gvol=1980500 Rvol=1136200); 2026-09-01:RED:O=35.2500,C=35.1300,body=-0.1200,vol=1136200.0; 2026-09-02:GREEN:O=34.8400,C=36.2800,body=+1.4400,vol=1980500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.749 on 2026-09-02: today_vol=1980500 / avg20=1132455 (avg window 2026-08-05→2026-09-01, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.610 on 2026-09-02 (price=36.2800, mid=38.8495, upper=43.0631, lower=34.6359; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-09-02: price=36.2800 vs SMA50=32.7266 dist=+10.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=38.8495 SMA50=32.7266 SMA80=29.2050 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-22@20.1100; S2[2026-07-01→2026-08-04] low=2026-07-01@24.5500; S3[2026-08-05→2026-09-02] low=2026-09-02@34.6500 | lows=[20.110000610351562, 24.549999237060547, 34.650001525878906] span=72.30% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.7239817876349242 wick_frac=0.27601821236507584 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.12499900658830636 wick_frac=0.8750009934116937 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=12.00009536828051 need>1.4; red_wick_gt_green=False 5d trail=2026-08-27:RED:body=-3.1200:wick=0.7750; 2026-08-28:GREEN:body=+0.3300:wick=1.1400; 2026-08-31:RED:body=-0.0800:wick=1.0200; 2026-09-01:RED:body=-0.1200:wick=0.8400; 2026-09-02:GREEN:body=+1.4400:wick=0.5490 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=32.56 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.43 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1360.37 | **NEUTRAL** |
| `B04_income` | 84.86 | **GOOD** |
| `B05_profit_margin` | 6.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 40.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=40.0 vs prior_export=40.0 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.23 | **GOOD** |
| `B13_short_float` | 5.73 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=32.56 (this export) | prior_export=32.56 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.43 (this export) | prior_export=4.43 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HSBC  ·  score **+16**  ·  Banks - Diversified
price=104.94000244140625  pair=`2026-09-01→2026-09-02`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.51 on 2026-09-02; prev RSI=54.37 on 2026-09-01 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.37@2026-09-01 → 60.51@2026-09-02 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.37@2026-09-01 → 60.51@2026-09-02 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.37@2026-09-01 → 60.51@2026-09-02 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_body_sum/RED_body_sum=3.000 (G=0.9300 R=0.3100); 2026-09-01:RED:O=103.7100,C=103.4000,body=-0.3100,vol=1180900.0; 2026-09-02:GREEN:O=104.0100,C=104.9400,body=+0.9300,vol=1387400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-01 + 2026-09-02; ratio=GREEN_vol/RED_vol=1.175 (Gvol=1387400 Rvol=1180900); 2026-09-01:RED:O=103.7100,C=103.4000,body=-0.3100,vol=1180900.0; 2026-09-02:GREEN:O=104.0100,C=104.9400,body=+0.9300,vol=1387400.0 | **GOOD** |
| `A07_rvol` | RVOL=1.258 on 2026-09-02: today_vol=1387400 / avg20=1103185 (avg window 2026-08-05→2026-09-01, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.067 on 2026-09-02 (price=104.9400, mid=103.3768, upper=104.8424, lower=101.9112; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-02: price=104.9400 vs SMA50=100.6416 dist=+4.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-02: SMA20=103.3768 SMA50=100.6416 SMA80=97.0553 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-01→2026-09-02 (63 bars); S1[2026-06-01→2026-06-30] low=2026-06-10@85.7062; S2[2026-07-01→2026-08-04] low=2026-07-01@94.2051; S3[2026-08-05→2026-09-02] low=2026-08-05@101.2412 | lows=[85.70616298395647, 94.20512637735261, 101.24115261531507] span=18.13% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: GREEN body_frac=0.861109933737408 wick_frac=0.138890066262592 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-01+2026-09-02: RED body_frac=0.2561965232633656 wick_frac=0.7438034767366344 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.000024611143926 need>1.4; red_wick_gt_green=True 5d trail=2026-08-27:RED:body=-0.4800:wick=0.8000; 2026-08-28:RED:body=-0.1100:wick=0.8300; 2026-08-31:GREEN:body=+0.1800:wick=0.7500; 2026-09-01:RED:body=-0.3100:wick=0.9000; 2026-09-02:GREEN:body=+0.9300:wick=0.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=3.31 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.09 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 135929.32 | **NEUTRAL** |
| `B04_income` | 24184.21 | **GOOD** |
| `B05_profit_margin` | 17.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 108.62 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=108.62 vs prior_export=108.62 on finviz_2026-09-02) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-09-02) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.04 | **GOOD** |
| `B13_short_float` | 0.16 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.31 (this export) | prior_export=3.31 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-09-02) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-09-03_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.