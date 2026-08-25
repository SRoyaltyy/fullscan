# A+B1 Feature Checklist — 2026-08-25

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,705** names
- Export: `finviz_2026-08-25.csv` · prior export for Δ: `2026-08-24`
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
| 1 | EVER | +17 | 19 | 2 | 2026-08-24→2026-08-25 | Internet Content & Information |
| 2 | RYAN | +16 | 18 | 2 | 2026-08-24→2026-08-25 | Insurance - Specialty |
| 3 | ALC | +15 | 16 | 1 | 2026-08-24→2026-08-25 | Medical Instruments & Supplies |
| 4 | GOOD | +15 | 17 | 2 | 2026-08-24→2026-08-25 | REIT - Diversified |
| 5 | CRSR | +15 | 17 | 2 | 2026-08-24→2026-08-25 | Computer Hardware |
| 6 | CMBT | +15 | 17 | 2 | 2026-08-24→2026-08-25 | Oil & Gas Midstream |
| 7 | CNK | +15 | 17 | 2 | 2026-08-24→2026-08-25 | Entertainment |
| 8 | HUM | +15 | 16 | 1 | 2026-08-24→2026-08-25 | Healthcare Plans |
| 9 | MUFG | +15 | 16 | 1 | 2026-08-24→2026-08-25 | Banks - Diversified |
| 10 | FLS | +15 | 17 | 2 | 2026-08-24→2026-08-25 | Specialty Industrial Machinery |
| 11 | AMH | +14 | 16 | 2 | 2026-08-24→2026-08-25 | REIT - Residential |
| 12 | OTF | +14 | 15 | 1 | 2026-08-24→2026-08-25 | Asset Management |
| 13 | TRV | +14 | 16 | 2 | 2026-08-24→2026-08-25 | Insurance - Property & Casualty |
| 14 | SGHC | +14 | 16 | 2 | 2026-08-24→2026-08-25 | Gambling |
| 15 | EXPE | +14 | 16 | 2 | 2026-08-24→2026-08-25 | Travel Services |

## Full checklist — top 15

### EVER  ·  score **+17**  ·  Internet Content & Information
price=25.915000915527344  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.51 on 2026-08-25; prev RSI=61.35 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.35@2026-08-24 → 56.51@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.35@2026-08-24 → 56.51@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.35@2026-08-24 → 56.51@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.108 (G=1.1700 R=0.5550); 2026-08-24:GREEN:O=25.4500,C=26.6200,body=+1.1700,vol=861900.0; 2026-08-25:RED:O=26.4700,C=25.9150,body=-0.5550,vol=161711.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=5.330 (Gvol=861900 Rvol=161711); 2026-08-24:GREEN:O=25.4500,C=26.6200,body=+1.1700,vol=861900.0; 2026-08-25:RED:O=26.4700,C=25.9150,body=-0.5550,vol=161711.0 | **GOOD** |
| `A07_rvol` | RVOL=0.246 on 2026-08-25: today_vol=161711 / avg20=657735 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.435 on 2026-08-25 (price=25.9150, mid=25.0078, upper=27.0924, lower=22.9231; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=25.9150 vs SMA50=24.1675 dist=+7.23% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=25.0078 SMA50=24.1675 SMA80=22.1143 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@17.6000; S2[2026-06-24→2026-07-24] low=2026-06-24@20.5550; S3[2026-07-27→2026-08-25] low=2026-08-04@22.0000 | lows=[17.600000381469727, 20.55500030517578, 22.0] span=25.00% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6464090726594666 wick_frac=0.3535909273405335 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.3425918294782103 wick_frac=0.6574081705217897 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.1081143312747654 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.5600:wick=0.4500; 2026-08-20:GREEN:body=+0.2950:wick=0.9450; 2026-08-21:GREEN:body=+0.1500:wick=0.8100; 2026-08-24:GREEN:body=+1.1700:wick=0.6400; 2026-08-25:RED:body=-0.5550:wick=1.0650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.36 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.11 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 755.2 | **NEUTRAL** |
| `B04_income` | 114.48 | **GOOD** |
| `B05_profit_margin` | 15.16 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 29.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=29.5 vs prior_export=29.5 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -0.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.16999999999999993 (now=-0.8 vs prior=-0.97 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 2.19 | **GOOD** |
| `B13_short_float` | 22.07 | **GOOD** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.36 (this export) | prior_export=1.36 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.11 (this export) | prior_export=2.11 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RYAN  ·  score **+16**  ·  Insurance - Specialty
price=43.68000030517578  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.68 on 2026-08-25; prev RSI=60.40 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.40@2026-08-24 → 56.68@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.40@2026-08-24 → 56.68@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.40@2026-08-24 → 56.68@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.690 (G=0.7800 R=0.2900); 2026-08-24:GREEN:O=43.5600,C=44.3400,body=+0.7800,vol=1453600.0; 2026-08-25:RED:O=43.9700,C=43.6800,body=-0.2900,vol=209935.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=6.924 (Gvol=1453600 Rvol=209935); 2026-08-24:GREEN:O=43.5600,C=44.3400,body=+0.7800,vol=1453600.0; 2026-08-25:RED:O=43.9700,C=43.6800,body=-0.2900,vol=209935.0 | **GOOD** |
| `A07_rvol` | RVOL=0.103 on 2026-08-25: today_vol=209935 / avg20=2031680 (avg window 2026-07-28→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.159 on 2026-08-25 (price=43.6800, mid=43.2831, upper=45.7816, lower=40.7846; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=43.6800 vs SMA50=40.7193 dist=+7.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=43.2831 SMA50=40.7193 SMA80=37.4603 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@30.5761; S2[2026-06-24→2026-07-27] low=2026-06-24@33.8261; S3[2026-07-28→2026-08-25] low=2026-08-11@41.0300 | lows=[30.57607472607481, 33.826090954123764, 41.029998779296875] span=34.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5397916567669312 wick_frac=0.4602083432330688 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.19863088861599562 wick_frac=0.8013691113840044 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.689642471916024 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.8500:wick=0.7300; 2026-08-20:GREEN:body=+0.9500:wick=0.3000; 2026-08-21:GREEN:body=+0.2900:wick=0.8800; 2026-08-24:GREEN:body=+0.7800:wick=0.6650; 2026-08-25:RED:body=-0.2900:wick=1.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=23.79 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.89 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3224.94 | **NEUTRAL** |
| `B04_income` | 99.03 | **GOOD** |
| `B05_profit_margin` | 3.07 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 49.91 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=49.91 vs prior_export=49.91 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.27 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=-0.08000000000000002 (now=0.43 vs prior=0.51 on finviz_2026-08-24) | **BAD** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 12.37 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=23.79 (this export) | prior_export=23.79 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.89 (this export) | prior_export=4.89 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ALC  ·  score **+15**  ·  Medical Instruments & Supplies
price=73.05999755859375  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.40 on 2026-08-25; prev RSI=58.11 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.11@2026-08-24 → 55.40@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.11@2026-08-24 → 55.40@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.11@2026-08-24 → 55.40@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.241 (G=0.3600 R=0.2900); 2026-08-24:GREEN:O=73.3400,C=73.7000,body=+0.3600,vol=1159800.0; 2026-08-25:RED:O=73.3500,C=73.0600,body=-0.2900,vol=394596.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=2.939 (Gvol=1159800 Rvol=394596); 2026-08-24:GREEN:O=73.3400,C=73.7000,body=+0.3600,vol=1159800.0; 2026-08-25:RED:O=73.3500,C=73.0600,body=-0.2900,vol=394596.0 | **GOOD** |
| `A07_rvol` | RVOL=0.187 on 2026-08-25: today_vol=394596 / avg20=2107015 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.206 on 2026-08-25 (price=73.0600, mid=72.2720, upper=76.0971, lower=68.4469; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=73.0600 vs SMA50=69.3320 dist=+5.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=72.2720 SMA50=69.3320 SMA80=68.5183 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-17@63.8100; S2[2026-06-23→2026-07-24] low=2026-06-23@65.3150; S3[2026-07-27→2026-08-25] low=2026-07-27@67.2300 | lows=[63.810001373291016, 65.31500244140625, 67.2300033569336] span=5.36% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5142832230711382 wick_frac=0.48571677692886184 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.3946119906566312 wick_frac=0.6053880093433688 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2413774959880035 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.5900:wick=0.8050; 2026-08-20:RED:body=-1.5300:wick=0.2800; 2026-08-21:GREEN:body=+0.8300:wick=0.3700; 2026-08-24:GREEN:body=+0.3600:wick=0.3400; 2026-08-25:RED:body=-0.2900:wick=0.4449 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=11.36 (current export asof; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.72 (current export; earnings_date=8/10/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10861.0 | **NEUTRAL** |
| `B04_income` | 643.0 | **GOOD** |
| `B05_profit_margin` | 5.92 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 87.37 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=87.37 vs prior_export=87.37 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.74 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.28 | **GOOD** |
| `B13_short_float` | 2.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.36 (this export) | prior_export=11.36 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.72 (this export) | prior_export=0.72 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GOOD  ·  score **+15**  ·  REIT - Diversified
price=13.069999694824219  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.81 on 2026-08-25; prev RSI=58.64 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.64@2026-08-24 → 55.81@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.64@2026-08-24 → 55.81@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.64@2026-08-24 → 55.81@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.667 (G=0.2000 R=0.1200); 2026-08-24:GREEN:O=12.9800,C=13.1800,body=+0.2000,vol=369000.0; 2026-08-25:RED:O=13.1900,C=13.0700,body=-0.1200,vol=71871.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=5.134 (Gvol=369000 Rvol=71871); 2026-08-24:GREEN:O=12.9800,C=13.1800,body=+0.2000,vol=369000.0; 2026-08-25:RED:O=13.1900,C=13.0700,body=-0.1200,vol=71871.0 | **GOOD** |
| `A07_rvol` | RVOL=0.168 on 2026-08-25: today_vol=71871 / avg20=428355 (avg window 2026-07-23→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.417 on 2026-08-25 (price=13.0700, mid=12.8640, upper=13.3583, lower=12.3697; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=13.0700 vs SMA50=12.5797 dist=+3.90% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=12.8640 SMA50=12.5797 SMA80=12.5029 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-20→2026-08-25 (63 bars); S1[2026-05-20→2026-06-18] low=2026-06-17@11.6884; S2[2026-06-22→2026-07-22] low=2026-06-22@11.7179; S3[2026-07-23→2026-08-25] low=2026-08-05@12.1100 | lows=[11.688406380704969, 11.717921671402111, 12.109999656677246] span=3.61% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8695666596177848 wick_frac=0.13043334038221527 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.8048317150861573 wick_frac=0.19516828491384272 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6666746139602158 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.1600:wick=0.0500; 2026-08-20:RED:body=-0.0300:wick=0.0600; 2026-08-21:RED:body=-0.1300:wick=0.0800; 2026-08-24:GREEN:body=+0.2000:wick=0.0300; 2026-08-25:RED:body=-0.1200:wick=0.0291 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=144.44 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.29 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 170.2 | **NEUTRAL** |
| `B04_income` | 12.18 | **GOOD** |
| `B05_profit_margin` | 7.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 14.36 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=14.36 vs prior_export=14.36 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.17 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.17 vs prior=0.17 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.1 | **GOOD** |
| `B13_short_float` | 7.44 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=144.44 (this export) | prior_export=144.44 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.29 (this export) | prior_export=4.29 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+15**  ·  Computer Hardware
price=11.319999694824219  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.38 on 2026-08-25; prev RSI=47.11 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.11@2026-08-24 → 50.38@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.11@2026-08-24 → 50.38@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.11@2026-08-24 → 50.38@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.4500 R=0.0000); 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0; 2026-08-25:GREEN:O=11.0300,C=11.3200,body=+0.2900,vol=294968.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2110168 Rvol=0); 2026-08-24:GREEN:O=10.7500,C=10.9100,body=+0.1600,vol=1815200.0; 2026-08-25:GREEN:O=11.0300,C=11.3200,body=+0.2900,vol=294968.0 | **GOOD** |
| `A07_rvol` | RVOL=0.128 on 2026-08-25: today_vol=294968 / avg20=2308740 (avg window 2026-07-28→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.185 on 2026-08-25 (price=11.3200, mid=11.7667, upper=14.1789, lower=9.3546; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=11.3200 vs SMA50=10.3167 dist=+9.73% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=11.7667 SMA50=10.3167 SMA80=9.6071 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-25 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-09@8.0500; S2[2026-06-26→2026-07-27] low=2026-06-26@8.1950; S3[2026-07-28→2026-08-25] low=2026-07-28@9.9200 | lows=[8.050000190734863, 8.194999694824219, 9.920000076293945] span=23.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6402398506236228 wick_frac=0.35976014937637724 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.0100:wick=0.4680; 2026-08-20:RED:body=-0.3300:wick=0.2800; 2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1040; 2026-08-25:GREEN:body=+0.2900:wick=0.1400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 25.27 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CMBT  ·  score **+15**  ·  Oil & Gas Midstream
price=18.045000076293945  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.43 on 2026-08-25; prev RSI=72.97 on 2026-08-24 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 72.97@2026-08-24 → 70.43@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.97@2026-08-24 → 70.43@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 72.97@2026-08-24 → 70.43@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.2200 R=0.0550); 2026-08-24:GREEN:O=17.9500,C=18.1700,body=+0.2200,vol=817900.0; 2026-08-25:RED:O=18.1000,C=18.0450,body=-0.0550,vol=438494.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=1.865 (Gvol=817900 Rvol=438494); 2026-08-24:GREEN:O=17.9500,C=18.1700,body=+0.2200,vol=817900.0; 2026-08-25:RED:O=18.1000,C=18.0450,body=-0.0550,vol=438494.0 | **GOOD** |
| `A07_rvol` | RVOL=0.489 on 2026-08-25: today_vol=438494 / avg20=896000 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.711 on 2026-08-25 (price=18.0450, mid=16.7868, upper=18.5563, lower=15.0172; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=18.0450 vs SMA50=15.7643 dist=+14.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=16.7868 SMA50=15.7643 SMA80=15.3279 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-18@13.9880; S2[2026-06-23→2026-07-24] low=2026-06-30@13.8950; S3[2026-07-27→2026-08-25] low=2026-07-27@15.1200 | lows=[13.98799991607666, 13.895000457763672, 15.119999885559082] span=8.82% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8627452447024152 wick_frac=0.13725475529758477 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.18965687338450307 wick_frac=0.8103431266154969 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.99996532112637 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.0400:wick=0.2100; 2026-08-20:RED:body=-0.2300:wick=0.1300; 2026-08-21:GREEN:body=+0.0500:wick=0.4800; 2026-08-24:GREEN:body=+0.2200:wick=0.0350; 2026-08-25:RED:body=-0.0550:wick=0.2350 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=30.1 (current export asof; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.46 (current export; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1950.67 | **NEUTRAL** |
| `B04_income` | 485.53 | **GOOD** |
| `B05_profit_margin` | 24.89 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.10000000000000142 (now=18.78 vs prior_export=18.68 on finviz_2026-08-24) | **GOOD** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 1.24 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.1 (this export) | prior_export=30.1 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.46 (this export) | prior_export=21.46 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNK  ·  score **+15**  ·  Entertainment
price=38.20000076293945  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.97 on 2026-08-25; prev RSI=65.52 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.52@2026-08-24 → 63.97@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.52@2026-08-24 → 63.97@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.52@2026-08-24 → 63.97@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=14.600 (G=1.4600 R=0.1000); 2026-08-24:GREEN:O=36.9400,C=38.4000,body=+1.4600,vol=2975900.0; 2026-08-25:RED:O=38.3000,C=38.2000,body=-0.1000,vol=365456.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=8.143 (Gvol=2975900 Rvol=365456); 2026-08-24:GREEN:O=36.9400,C=38.4000,body=+1.4600,vol=2975900.0; 2026-08-25:RED:O=38.3000,C=38.2000,body=-0.1000,vol=365456.0 | **GOOD** |
| `A07_rvol` | RVOL=0.138 on 2026-08-25: today_vol=365456 / avg20=2654155 (avg window 2026-07-28→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.577 on 2026-08-25 (price=38.2000, mid=37.2460, upper=38.8988, lower=35.5932; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=38.2000 vs SMA50=34.0258 dist=+12.27% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=37.2460 SMA50=34.0258 SMA80=31.8255 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-22@26.1730; S2[2026-06-24→2026-07-27] low=2026-07-16@28.5000; S3[2026-07-28→2026-08-25] low=2026-07-28@33.9200 | lows=[26.173046359836217, 28.5, 33.91999816894531] span=29.60% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.7192123317917967 wick_frac=0.2807876682082033 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.23809048055875967 wick_frac=0.7619095194412403 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.600251773861295 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.3000:wick=0.7500; 2026-08-20:RED:body=-0.1900:wick=1.0100; 2026-08-21:RED:body=-0.3800:wick=0.4100; 2026-08-24:GREEN:body=+1.4600:wick=0.5700; 2026-08-25:RED:body=-0.1000:wick=0.3200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=15.29 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.0 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3363.3 | **NEUTRAL** |
| `B04_income` | 214.3 | **GOOD** |
| `B05_profit_margin` | 6.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=39.77 vs prior_export=39.77 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.77 | **GOOD** |
| `B10_insider_transactions` | -1.17 | **BAD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=-1.17 vs prior=-1.18 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 2.19 | **GOOD** |
| `B13_short_float` | 7.7 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.29 (this export) | prior_export=15.29 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.0 (this export) | prior_export=5.0 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HUM  ·  score **+15**  ·  Healthcare Plans
price=385.1600036621094  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.95 on 2026-08-25; prev RSI=53.88 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.88@2026-08-24 → 52.95@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.88@2026-08-24 → 52.95@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.88@2026-08-24 → 52.95@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.598 (G=6.1200 R=3.8300); 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1455000.0; 2026-08-25:RED:O=388.9900,C=385.1600,body=-3.8300,vol=228716.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=6.362 (Gvol=1455000 Rvol=228716); 2026-08-24:GREEN:O=380.4900,C=386.6100,body=+6.1200,vol=1455000.0; 2026-08-25:RED:O=388.9900,C=385.1600,body=-3.8300,vol=228716.0 | **GOOD** |
| `A07_rvol` | RVOL=0.160 on 2026-08-25: today_vol=228716 / avg20=1426105 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.363 on 2026-08-25 (price=385.1600, mid=378.5290, upper=396.8176, lower=360.2404; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=385.1600 vs SMA50=382.4681 dist=+0.70% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=378.53_50=382.47_80=349.03 on 2026-08-25: SMA20=378.5290 SMA50=382.4681 SMA80=349.0315 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@299.2939; S2[2026-06-23→2026-07-24] low=2026-06-24@352.2290; S3[2026-07-27→2026-08-25] low=2026-08-05@353.6900 | lows=[299.2938790186219, 352.22899615508356, 353.69000244140625] span=18.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5784485096023491 wick_frac=0.4215514903976509 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.49611217184714335 wick_frac=0.5038878281528567 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5979155544577335 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:RED:body=-6.9800:wick=9.1300; 2026-08-20:GREEN:body=+6.4400:wick=7.0600; 2026-08-21:RED:body=-2.8100:wick=6.5400; 2026-08-24:GREEN:body=+6.1200:wick=4.4600; 2026-08-25:RED:body=-3.8300:wick=3.8900 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=4.7 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.71 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 145679.0 | **NEUTRAL** |
| `B04_income` | 1279.0 | **GOOD** |
| `B05_profit_margin` | 0.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 420.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=420.54 vs prior_export=420.54 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.23 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.23 vs prior=0.23 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.22 | **GOOD** |
| `B13_short_float` | 3.06 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.7 (this export) | prior_export=4.7 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.71 (this export) | prior_export=0.71 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MUFG  ·  score **+15**  ·  Banks - Diversified
price=22.19499969482422  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.13 on 2026-08-25; prev RSI=48.48 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.48@2026-08-24 → 51.13@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.48@2026-08-24 → 51.13@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.48@2026-08-24 → 51.13@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.1850 R=0.0000); 2026-08-24:GREEN:O=21.8700,C=21.9600,body=+0.0900,vol=2120100.0; 2026-08-25:GREEN:O=22.1000,C=22.1950,body=+0.0950,vol=479542.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2599642 Rvol=0); 2026-08-24:GREEN:O=21.8700,C=21.9600,body=+0.0900,vol=2120100.0; 2026-08-25:GREEN:O=22.1000,C=22.1950,body=+0.0950,vol=479542.0 | **GOOD** |
| `A07_rvol` | RVOL=0.159 on 2026-08-25: today_vol=479542 / avg20=3008105 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.168 on 2026-08-25 (price=22.1950, mid=22.3483, upper=23.2613, lower=21.4352; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=22.1950 vs SMA50=21.6163 dist=+2.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=22.3483 SMA50=21.6163 SMA80=20.5737 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-05-28@18.5900; S2[2026-06-24→2026-07-24] low=2026-06-24@19.7300; S3[2026-07-27→2026-08-25] low=2026-08-19@21.4900 | lows=[18.59000015258789, 19.729999542236328, 21.489999771118164] span=15.60% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.5162464415284015 wick_frac=0.4837535584715985 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-0.7700:wick=0.0700; 2026-08-20:RED:body=-0.0200:wick=0.2600; 2026-08-21:RED:body=-0.0600:wick=0.1700; 2026-08-24:GREEN:body=+0.0900:wick=0.1000; 2026-08-25:GREEN:body=+0.0950:wick=0.0750 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=20.6 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=146.44 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 86521.46 | **NEUTRAL** |
| `B04_income` | 17435.86 | **GOOD** |
| `B05_profit_margin` | 20.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.59 vs prior_export=25.59 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.53 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.49 | **GOOD** |
| `B13_short_float` | 0.05 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=20.6 (this export) | prior_export=20.6 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=146.44 (this export) | prior_export=146.44 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+15**  ·  Specialty Industrial Machinery
price=81.17500305175781  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.83 on 2026-08-25; prev RSI=60.08 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.08@2026-08-24 → 61.83@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.08@2026-08-24 → 61.83@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.08@2026-08-24 → 61.83@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.664 (G=0.9900 R=0.5950); 2026-08-24:GREEN:O=79.4700,C=80.4600,body=+0.9900,vol=1836700.0; 2026-08-25:RED:O=81.7700,C=81.1750,body=-0.5950,vol=582968.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=3.151 (Gvol=1836700 Rvol=582968); 2026-08-24:GREEN:O=79.4700,C=80.4600,body=+0.9900,vol=1836700.0; 2026-08-25:RED:O=81.7700,C=81.1750,body=-0.5950,vol=582968.0 | **GOOD** |
| `A07_rvol` | RVOL=0.345 on 2026-08-25: today_vol=582968 / avg20=1689060 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.443 on 2026-08-25 (price=81.1750, mid=78.6068, upper=84.4043, lower=72.8092; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=81.1750 vs SMA50=75.5469 dist=+7.45% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=78.6068 SMA50=75.5469 SMA80=74.2813 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@65.9659; S2[2026-06-23→2026-07-24] low=2026-07-20@66.3300; S3[2026-07-27→2026-08-25] low=2026-07-29@69.1400 | lows=[65.96590615385797, 66.33000183105469, 69.13999938964844] span=4.81% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.4949989318847656 wick_frac=0.5050010681152344 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.4027026887467146 wick_frac=0.5972973112532854 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.6638798774154666 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:RED:body=-0.8800:wick=2.0000; 2026-08-20:GREEN:body=+1.1100:wick=0.6000; 2026-08-21:GREEN:body=+0.2300:wick=1.6300; 2026-08-24:GREEN:body=+0.9900:wick=1.0100; 2026-08-25:RED:body=-0.5950:wick=0.8825 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.33 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMH  ·  score **+14**  ·  REIT - Residential
price=34.44499969482422  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.69 on 2026-08-25; prev RSI=60.98 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.98@2026-08-24 → 55.69@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.98@2026-08-24 → 55.69@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.98@2026-08-24 → 55.69@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=1.016 (G=0.3200 R=0.3150); 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0; 2026-08-25:RED:O=34.7600,C=34.4450,body=-0.3150,vol=311915.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=4.116 (Gvol=1283900 Rvol=311915); 2026-08-24:GREEN:O=34.4900,C=34.8100,body=+0.3200,vol=1283900.0; 2026-08-25:RED:O=34.7600,C=34.4450,body=-0.3150,vol=311915.0 | **GOOD** |
| `A07_rvol` | RVOL=0.147 on 2026-08-25: today_vol=311915 / avg20=2124830 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.333 on 2026-08-25 (price=34.4450, mid=34.1632, upper=35.0093, lower=33.3172; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=34.4450 vs SMA50=33.6080 dist=+2.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=34.1632 SMA50=33.6080 SMA80=32.9128 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-06-22@31.4700; S2[2026-06-23→2026-07-24] low=2026-06-23@31.8300; S3[2026-07-27→2026-08-25] low=2026-07-30@32.8600 | lows=[31.469999313354492, 31.829999923706055, 32.86000061035156] span=4.42% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.7111092273131862 wick_frac=0.2888907726868139 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.7499931880727696 wick_frac=0.25000681192723045 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.0158764759309717 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+0.2900:wick=0.2400; 2026-08-20:GREEN:body=+0.1000:wick=0.2300; 2026-08-21:RED:body=-0.1200:wick=0.2800; 2026-08-24:GREEN:body=+0.3200:wick=0.1300; 2026-08-25:RED:body=-0.3150:wick=0.1050 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=72.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.77 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1891.62 | **NEUTRAL** |
| `B04_income` | 463.49 | **GOOD** |
| `B05_profit_margin` | 24.5 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 37.16 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=37.16 vs prior_export=37.16 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.41 (this export) | prior_export=72.41 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.77 (this export) | prior_export=0.77 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### OTF  ·  score **+14**  ·  Asset Management
price=11.03499984741211  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.39 on 2026-08-25; prev RSI=54.91 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.91@2026-08-24 → 53.39@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.91@2026-08-24 → 53.39@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.91@2026-08-24 → 53.39@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.889 (G=0.1300 R=0.0450); 2026-08-24:GREEN:O=10.9700,C=11.1000,body=+0.1300,vol=2990500.0; 2026-08-25:RED:O=11.0800,C=11.0350,body=-0.0450,vol=527725.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=5.667 (Gvol=2990500 Rvol=527725); 2026-08-24:GREEN:O=10.9700,C=11.1000,body=+0.1300,vol=2990500.0; 2026-08-25:RED:O=11.0800,C=11.0350,body=-0.0450,vol=527725.0 | **GOOD** |
| `A07_rvol` | RVOL=0.181 on 2026-08-25: today_vol=527725 / avg20=2916820 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.050 on 2026-08-25 (price=11.0350, mid=10.9777, upper=12.1225, lower=9.8330; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=11.0350 vs SMA50=10.5323 dist=+4.77% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=10.98_50=10.53_80=10.59 on 2026-08-25: SMA20=10.9777 SMA50=10.5323 SMA80=10.5891 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-25 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-24@9.8280; S2[2026-06-25→2026-07-24] low=2026-07-23@9.7800; S3[2026-07-27→2026-08-25] low=2026-07-30@9.7900 | lows=[9.827973411172369, 9.779999732971191, 9.789999961853027] span=0.49% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.509804141562604 wick_frac=0.490195858437396 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.16666666666666666 wick_frac=0.8333333333333334 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8888865341414824 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.0100:wick=0.1450; 2026-08-20:RED:body=-0.0500:wick=0.1650; 2026-08-21:RED:body=-0.2600:wick=0.1510; 2026-08-24:GREEN:body=+0.1300:wick=0.1250; 2026-08-25:RED:body=-0.0450:wick=0.2250 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=0.91 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.33 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1456.25 | **NEUTRAL** |
| `B04_income` | 375.08 | **GOOD** |
| `B05_profit_margin` | 25.76 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.61 vs prior_export=13.61 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 0.03 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.03 vs prior=0.03 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 1.39 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.33 (this export) | prior_export=0.33 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TRV  ·  score **+14**  ·  Insurance - Property & Casualty
price=368.07501220703125  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.12 on 2026-08-25; prev RSI=54.44 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.44@2026-08-24 → 52.12@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.44@2026-08-24 → 52.12@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.44@2026-08-24 → 52.12@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=2.341 (G=4.7400 R=2.0250); 2026-08-24:GREEN:O=365.8100,C=370.5500,body=+4.7400,vol=1553900.0; 2026-08-25:RED:O=370.1000,C=368.0750,body=-2.0250,vol=272174.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=5.709 (Gvol=1553900 Rvol=272174); 2026-08-24:GREEN:O=365.8100,C=370.5500,body=+4.7400,vol=1553900.0; 2026-08-25:RED:O=370.1000,C=368.0750,body=-2.0250,vol=272174.0 | **GOOD** |
| `A07_rvol` | RVOL=0.146 on 2026-08-25: today_vol=272174 / avg20=1868795 (avg window 2026-07-28→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=-0.342 on 2026-08-25 (price=368.0750, mid=373.3097, upper=388.6132, lower=358.0063; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=368.0750 vs SMA50=352.6463 dist=+4.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=373.3097 SMA50=352.6463 SMA80=332.3996 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-25 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-02@285.9147; S2[2026-06-25→2026-07-27] low=2026-06-25@318.1600; S3[2026-07-28→2026-08-25] low=2026-08-20@358.8800 | lows=[285.91467292783994, 318.1600036621094, 358.8800048828125] span=25.52% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.6061362909079556 wick_frac=0.3938637090920444 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.47312955000819984 wick_frac=0.5268704499918002 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.340742973400648 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:RED:body=-5.5800:wick=3.8500; 2026-08-20:GREEN:body=+4.2100:wick=3.9900; 2026-08-21:RED:body=-1.7100:wick=2.1000; 2026-08-24:GREEN:body=+4.7400:wick=3.0800; 2026-08-25:RED:body=-2.0250:wick=2.2550 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=85.74 (current export asof; earnings_date=7/17/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.41 (current export; earnings_date=7/17/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 48979.0 | **NEUTRAL** |
| `B04_income` | 8244.0 | **GOOD** |
| `B05_profit_margin` | 16.83 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 366.08 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=366.08 vs prior_export=366.08 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.97 | **NEUTRAL** |
| `B10_insider_transactions` | -9.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.379999999999999 (now=-9.8 vs prior=-10.18 on finviz_2026-08-24) | **GOOD** |
| `B12_institutional_transactions` | 1.71 | **GOOD** |
| `B13_short_float` | 3.59 | **NEUTRAL** |
| `B14_earnings_date` | 7/17/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=85.74 (this export) | prior_export=85.74 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.41 (this export) | prior_export=2.41 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SGHC  ·  score **+14**  ·  Gambling
price=13.930000305175781  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.68 on 2026-08-25; prev RSI=55.58 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.58@2026-08-24 → 54.68@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.58@2026-08-24 → 54.68@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.58@2026-08-24 → 54.68@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=7.333 (G=0.6600 R=0.0900); 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4299300.0; 2026-08-25:RED:O=14.0200,C=13.9300,body=-0.0900,vol=505160.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=8.511 (Gvol=4299300 Rvol=505160); 2026-08-24:GREEN:O=13.3300,C=13.9900,body=+0.6600,vol=4299300.0; 2026-08-25:RED:O=14.0200,C=13.9300,body=-0.0900,vol=505160.0 | **GOOD** |
| `A07_rvol` | RVOL=0.215 on 2026-08-25: today_vol=505160 / avg20=2346830 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.360 on 2026-08-25 (price=13.9300, mid=13.4630, upper=14.7593, lower=12.1667; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=13.9300 vs SMA50=13.9255 dist=+0.03% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=13.46_50=13.93_80=13.60 on 2026-08-25: SMA20=13.4630 SMA50=13.9255 SMA80=13.5979 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-25 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@12.1657; S2[2026-06-24→2026-07-24] low=2026-06-30@13.2350; S3[2026-07-27→2026-08-25] low=2026-08-12@12.7410 | lows=[12.165696572387432, 13.234999656677246, 12.741000175476074] span=8.79% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.7875890370737914 wick_frac=0.21241096292620862 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.264706212343906 wick_frac=0.735293787656094 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.3333192048488955 need>1.4; red_wick_gt_green=True 5d trail=2026-08-19:GREEN:body=+0.2800:wick=0.1500; 2026-08-20:RED:body=-0.0200:wick=0.4170; 2026-08-21:GREEN:body=+0.0900:wick=0.1200; 2026-08-24:GREEN:body=+0.6600:wick=0.1780; 2026-08-25:RED:body=-0.0900:wick=0.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.51 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.65 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2431.0 | **NEUTRAL** |
| `B04_income` | 370.0 | **GOOD** |
| `B05_profit_margin` | 15.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.5 vs prior_export=19.5 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -0.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.13 vs prior=-0.13 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.32 | **GOOD** |
| `B13_short_float` | 16.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.51 (this export) | prior_export=0.51 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.65 (this export) | prior_export=3.65 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EXPE  ·  score **+14**  ·  Travel Services
price=335.5299987792969  pair=`2026-08-24→2026-08-25`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=66.20 on 2026-08-25; prev RSI=68.86 on 2026-08-24 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.86@2026-08-24 → 66.20@2026-08-25 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.86@2026-08-24 → 66.20@2026-08-25 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.86@2026-08-24 → 66.20@2026-08-25 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_body_sum/RED_body_sum=10.599 (G=15.5800 R=1.4700); 2026-08-24:GREEN:O=323.5500,C=339.1300,body=+15.5800,vol=1563400.0; 2026-08-25:RED:O=337.0000,C=335.5300,body=-1.4700,vol=238186.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-24 + 2026-08-25; ratio=GREEN_vol/RED_vol=6.564 (Gvol=1563400 Rvol=238186); 2026-08-24:GREEN:O=323.5500,C=339.1300,body=+15.5800,vol=1563400.0; 2026-08-25:RED:O=337.0000,C=335.5300,body=-1.4700,vol=238186.0 | **GOOD** |
| `A07_rvol` | RVOL=0.143 on 2026-08-25: today_vol=238186 / avg20=1660530 (avg window 2026-07-27→2026-08-24, excludes asof) | **BAD** |
| `A08_bollinger_position` | pos=0.696 on 2026-08-25 (price=335.5300, mid=317.4145, upper=343.4291, lower=291.3999; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-25: price=335.5300 vs SMA50=280.5272 dist=+19.61% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-25: SMA20=317.4145 SMA50=280.5272 SMA80=261.3176 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-21→2026-08-25 (63 bars); S1[2026-05-21→2026-06-22] low=2026-05-21@209.5651; S2[2026-06-23→2026-07-24] low=2026-06-23@237.9800; S3[2026-07-27→2026-08-25] low=2026-07-27@265.0100 | lows=[209.5650640539776, 237.97999572753906, 265.010009765625] span=26.46% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: GREEN body_frac=0.8882559577973766 wick_frac=0.11174404220262339 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-24+2026-08-25: RED body_frac=0.20940221100634263 wick_frac=0.7905977889936574 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.59864228030476 need>1.4; red_wick_gt_green=False 5d trail=2026-08-19:GREEN:body=+2.1400:wick=9.4900; 2026-08-20:RED:body=-2.5100:wick=3.3500; 2026-08-21:RED:body=-6.8200:wick=1.5000; 2026-08-24:GREEN:body=+15.5800:wick=1.9600; 2026-08-25:RED:body=-1.4700:wick=5.5500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.4 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.37 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 15700.0 | **NEUTRAL** |
| `B04_income` | 2036.0 | **GOOD** |
| `B05_profit_margin` | 12.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 343.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=343.97 vs prior_export=343.97 on finviz_2026-08-24) | **NEUTRAL** |
| `B09_analyst_recom` | 2.2 | **GOOD** |
| `B10_insider_transactions` | -0.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.39 vs prior=-0.39 on finviz_2026-08-24) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.77 | **GOOD** |
| `B13_short_float` | 6.89 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.4 (this export) | prior_export=10.4 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.37 (this export) | prior_export=3.37 (finviz_2026-08-24) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-25_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.