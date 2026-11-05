# A+B1 Feature Checklist — 2026-11-05

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,615** names
- Export: `finviz_2026-11-05.csv` · prior export for Δ: `2026-11-04`
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
| 1 | IFF | +18 | 18 | 0 | 2026-10-09→2026-10-12 | Specialty Chemicals |
| 2 | MNTK | +17 | 19 | 2 | 2026-09-04→2026-09-08 | Specialty Chemicals |
| 3 | ZBRA | +17 | 19 | 2 | 2026-11-03→2026-11-04 | Communication Equipment |
| 4 | MSFT | +17 | 18 | 1 | 2026-11-03→2026-11-04 | Software - Infrastructure |
| 5 | GLOB | +17 | 19 | 2 | 2026-11-03→2026-11-04 | Information Technology Services |
| 6 | PRCH | +16 | 18 | 2 | 2026-11-03→2026-11-04 | Insurance - Property & Casualty |
| 7 | ADSK | +16 | 17 | 1 | 2026-11-03→2026-11-04 | Software - Application |
| 8 | CTSH | +16 | 18 | 2 | 2026-11-03→2026-11-04 | Information Technology Services |
| 9 | CNC | +16 | 18 | 2 | 2026-11-03→2026-11-04 | Healthcare Plans |
| 10 | SWKS | +16 | 17 | 1 | 2026-11-03→2026-11-04 | Semiconductors |
| 11 | YPF | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 12 | AUPH | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Biotechnology |
| 13 | RMD | +16 | 18 | 2 | 2026-11-03→2026-11-04 | Medical Instruments & Supplies |
| 14 | LTC | +15 | 17 | 2 | 2026-10-09→2026-10-12 | REIT - Healthcare Facilities |
| 15 | NICE | +15 | 17 | 2 | 2026-11-03→2026-11-04 | Software - Application |

## Full checklist — top 15

### IFF  ·  score **+18**  ·  Specialty Chemicals
price=84.95999908447266  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.76 on 2026-10-12; prev RSI=51.57 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.57@2026-10-09 → 53.76@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=3.571 (G=0.7500 R=0.2100); 2026-10-09:RED:O=84.7600,C=84.5500,body=-0.2100,vol=1111114.0; 2026-10-12:GREEN:O=84.2100,C=84.9600,body=+0.7500,vol=1289930.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.161 (Gvol=1289930 Rvol=1111114); 2026-10-09:RED:O=84.7600,C=84.5500,body=-0.2100,vol=1111114.0; 2026-10-12:GREEN:O=84.2100,C=84.9600,body=+0.7500,vol=1289930.0 | **GOOD** |
| `A07_rvol` | RVOL=0.711 on 2026-10-12: today_vol=1289930 / avg20=1813615 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.251 on 2026-10-12 (price=84.9600, mid=84.4295, upper=86.5461, lower=82.3129; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=84.9600 vs SMA50=84.6470 dist=+0.37% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=84.43_50=84.65_80=82.01 on 2026-10-12: SMA20=84.4295 SMA50=84.6470 SMA80=82.0119 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-23@72.1100; S2[2026-08-13→2026-09-11] low=2026-08-18@80.8700; S3[2026-09-14→2026-10-12] low=2026-10-01@81.5400 | lows=[72.11000061035156, 80.87000274658203, 81.54000091552734] span=13.08% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.7614975250400874 wick_frac=0.2385024749599126 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.24561419163707102 wick_frac=0.754385808362929 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.571444141689373 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-0.0900:wick=0.9750; 2026-10-07:GREEN:body=+0.6700:wick=0.4730; 2026-10-08:GREEN:body=+0.9500:wick=1.1000; 2026-10-09:RED:body=-0.2100:wick=0.6450; 2026-10-12:GREEN:body=+0.7500:wick=0.2349 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.13 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.76 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 9226.0 | **NEUTRAL** |
| `B04_income` | 182.0 | **GOOD** |
| `B05_profit_margin` | 1.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 98.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.8700000000000045 (now=98.25 vs prior_export=96.38 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 5.85 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=5.85 vs prior=5.85 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.8 | **GOOD** |
| `B13_short_float` | 6.58 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.13 (this export) | prior_export=-23.03 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.76 (this export) | prior_export=-25.45 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MNTK  ·  score **+17**  ·  Specialty Chemicals
price=2.5  pair=`2026-09-04→2026-09-08`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.23 on 2026-09-08; prev RSI=71.31 on 2026-09-04 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 71.31@2026-09-04 → 78.23@2026-09-08 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_body_sum/RED_body_sum=14.500 (G=0.2900 R=0.0200); 2026-09-04:RED:O=2.2500,C=2.2300,body=-0.0200,vol=305800.0; 2026-09-08:GREEN:O=2.2100,C=2.5000,body=+0.2900,vol=703900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-09-04 + 2026-09-08; ratio=GREEN_vol/RED_vol=2.302 (Gvol=703900 Rvol=305800); 2026-09-04:RED:O=2.2500,C=2.2300,body=-0.0200,vol=305800.0; 2026-09-08:GREEN:O=2.2100,C=2.5000,body=+0.2900,vol=703900.0 | **GOOD** |
| `A07_rvol` | RVOL=3.481 on 2026-09-08: today_vol=703900 / avg20=202225 (avg window 2026-08-10→2026-09-04, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.451 on 2026-09-08 (price=2.5000, mid=1.9230, upper=2.3207, lower=1.5253; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-09-08: price=2.5000 vs SMA50=1.7735 dist=+40.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-09-08: SMA20=1.9230 SMA50=1.7735 SMA80=1.7129 | **GOOD** |
| `A11_three_section_lows` | window=2026-06-04→2026-09-08 (63 bars); S1[2026-06-04→2026-07-06] low=2026-06-26@1.4550; S2[2026-07-07→2026-08-07] low=2026-07-09@1.5250; S3[2026-08-10→2026-09-08] low=2026-08-11@1.7000 | lows=[1.4550000429153442, 1.524999976158142, 1.7000000476837158] span=16.84% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: GREEN body_frac=0.8787879663612982 wick_frac=0.12121203363870185 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-09-04+2026-09-08: RED body_frac=0.2222222222222222 wick_frac=0.7777777777777778 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.500011920940324 need>1.4; red_wick_gt_green=True 5d trail=2026-09-01:GREEN:body=+0.0400:wick=0.0600; 2026-09-02:GREEN:body=+0.2900:wick=0.0800; 2026-09-03:GREEN:body=+0.1200:wick=0.0500; 2026-09-04:RED:body=-0.0200:wick=0.0700; 2026-09-08:GREEN:body=+0.2900:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=542.86 (current export asof; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.47 (current export; earnings_date=11/9/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 189.1 | **NEUTRAL** |
| `B04_income` | 7.93 | **GOOD** |
| `B05_profit_margin` | 4.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 2.03 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=2.03 vs prior_export=2.03 on finviz_2026-11-04) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.3 | **GOOD** |
| `B13_short_float` | 3.52 | **NEUTRAL** |
| `B14_earnings_date` | 11/9/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=542.86 (this export) | prior_export=542.86 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.47 (this export) | prior_export=9.47 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZBRA  ·  score **+17**  ·  Communication Equipment
price=332.44000244140625  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=29.46 on 2026-11-04; prev RSI=27.19 on 2026-11-03 | **GOOD** |
| `A02_rsi_cross_30` | below | RSI 27.19@2026-11-03 → 29.46@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 27.19@2026-11-03 → 29.46@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 27.19@2026-11-03 → 29.46@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=58.624 (G=17.8800 R=0.3050); 2026-11-03:GREEN:O=311.6200,C=329.5000,body=+17.8800,vol=2063100.0; 2026-11-04:RED:O=332.7450,C=332.4400,body=-0.3050,vol=978539.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=2.108 (Gvol=2063100 Rvol=978539); 2026-11-03:GREEN:O=311.6200,C=329.5000,body=+17.8800,vol=2063100.0; 2026-11-04:RED:O=332.7450,C=332.4400,body=-0.3050,vol=978539.0 | **GOOD** |
| `A07_rvol` | RVOL=1.636 on 2026-11-04: today_vol=978539 / avg20=597995 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.242 on 2026-11-04 (price=332.4400, mid=375.0550, upper=409.3778, lower=340.7322; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-11-04: price=332.4400 vs SMA50=366.1514 dist=-9.21% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=375.0550 SMA50=366.1514 SMA80=350.0895 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-02@341.9700; S2[2026-09-08→2026-10-06] low=2026-09-10@336.4000; S3[2026-10-07→2026-11-04] low=2026-11-03@311.0700 | lows=[341.9700012207031, 336.3999938964844, 311.07000732421875] span=9.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.6180438786554119 wick_frac=0.3819561213445882 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.024546658053804193 wick_frac=0.9754533419461958 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=58.624374624774866 need>1.4; red_wick_gt_green=False 5d trail=2026-10-29:RED:body=-7.5500:wick=7.1300; 2026-10-30:RED:body=-8.2450:wick=21.7650; 2026-11-02:RED:body=-5.1000:wick=9.3100; 2026-11-03:GREEN:body=+17.8800:wick=11.0500; 2026-11-04:RED:body=-0.3050:wick=12.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1.18 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.85 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6155.0 | **NEUTRAL** |
| `B04_income` | 602.0 | **GOOD** |
| `B05_profit_margin` | 9.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 423.85 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.6200000000000045 (now=423.85 vs prior_export=419.23 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -1.59 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.59 vs prior=-1.59 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.31 | **GOOD** |
| `B13_short_float` | 3.1 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.18 (this export) | prior_export=45.54 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.85 (this export) | prior_export=3.99 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MSFT  ·  score **+17**  ·  Software - Infrastructure
price=555.7999877929688  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.32 on 2026-11-04; prev RSI=49.40 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.40@2026-11-03 → 64.32@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.40@2026-11-03 → 64.32@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.40@2026-11-03 → 64.32@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=24.0850 R=0.0000); 2026-11-03:GREEN:O=509.0100,C=520.9900,body=+11.9800,vol=20010200.0; 2026-11-04:GREEN:O=543.6950,C=555.8000,body=+12.1050,vol=50590883.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=99.000 (Gvol=70601083 Rvol=0); 2026-11-03:GREEN:O=509.0100,C=520.9900,body=+11.9800,vol=20010200.0; 2026-11-04:GREEN:O=543.6950,C=555.8000,body=+12.1050,vol=50590883.0 | **GOOD** |
| `A07_rvol` | RVOL=2.311 on 2026-11-04: today_vol=50590883 / avg20=21891875 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.226 on 2026-11-04 (price=555.8000, mid=530.9645, upper=551.2270, lower=510.7020; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-04: price=555.8000 vs SMA50=515.3436 dist=+7.85% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=530.9645 SMA50=515.3436 SMA80=491.8692 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-18@476.2534; S2[2026-09-08→2026-10-06] low=2026-09-10@486.0000; S3[2026-10-07→2026-11-04] low=2026-11-02@504.9900 | lows=[476.2534434403709, 486.0, 504.989990234375] span=6.03% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.743556138244351 wick_frac=0.25644386175564904 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:RED:body=-10.0800:wick=11.3300; 2026-10-30:RED:body=-3.3600:wick=8.2900; 2026-11-02:RED:body=-16.9800:wick=7.6600; 2026-11-03:GREEN:body=+11.9800:wick=3.6400; 2026-11-04:GREEN:body=+12.1050:wick=4.7040 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=3.61 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.83 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 346445.0 | **NEUTRAL** |
| `B04_income` | 142631.0 | **GOOD** |
| `B05_profit_margin` | 41.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 635.28 | **NEUTRAL** |
| `B08_target_price_delta` | delta=63.91999999999996 (now=635.28 vs prior_export=571.36 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.26 | **GOOD** |
| `B13_short_float` | 0.9 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=3.61 (this export) | prior_export=11.81 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.83 (this export) | prior_export=2.72 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GLOB  ·  score **+17**  ·  Information Technology Services
price=48.040000915527344  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.99 on 2026-11-04; prev RSI=64.35 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.35@2026-11-03 → 68.99@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.35@2026-11-03 → 68.99@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.35@2026-11-03 → 68.99@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=1.466 (G=1.0700 R=0.7300); 2026-11-03:RED:O=46.5000,C=45.7700,body=-0.7300,vol=1241900.0; 2026-11-04:GREEN:O=46.9700,C=48.0400,body=+1.0700,vol=1533377.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=1.235 (Gvol=1533377 Rvol=1241900); 2026-11-03:RED:O=46.5000,C=45.7700,body=-0.7300,vol=1241900.0; 2026-11-04:GREEN:O=46.9700,C=48.0400,body=+1.0700,vol=1533377.0 | **GOOD** |
| `A07_rvol` | RVOL=1.649 on 2026-11-04: today_vol=1533377 / avg20=930018 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.749 on 2026-11-04 (price=48.0400, mid=42.0835, upper=50.0333, lower=34.1337; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-04: price=48.0400 vs SMA50=39.1166 dist=+22.81% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=42.0835 SMA50=39.1166 SMA80=37.9673 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-14@35.5100; S2[2026-09-08→2026-10-06] low=2026-09-29@33.0000; S3[2026-10-07→2026-11-04] low=2026-10-08@34.6000 | lows=[35.5099983215332, 33.0, 34.599998474121094] span=7.61% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.5066287605120944 wick_frac=0.4933712394879057 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.26353769732792204 wick_frac=0.7364623026720779 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.465753925743997 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:GREEN:body=+1.3600:wick=0.1500; 2026-10-30:GREEN:body=+2.8900:wick=0.1000; 2026-11-02:GREEN:body=+1.2800:wick=1.0941; 2026-11-03:RED:body=-0.7300:wick=2.0400; 2026-11-04:GREEN:body=+1.0700:wick=1.0420 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=-6.55 (current export asof; earnings_date=8/13/2026 4:30:00 PM) | **BAD** |
| `B02_revenue_surprise` | Revenue surprise=0.25 (current export; earnings_date=8/13/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2451.11 | **NEUTRAL** |
| `B04_income` | 113.41 | **GOOD** |
| `B05_profit_margin` | 4.63 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 51.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.8200000000000003 (now=51.57 vs prior_export=50.75 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.12 | **GOOD** |
| `B10_insider_transactions` | 0.94 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.94 vs prior=0.94 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 18.99 | **GOOD** |
| `B13_short_float` | 20.39 | **GOOD** |
| `B14_earnings_date` | 8/13/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-6.55 (this export) | prior_export=-6.55 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.25 (this export) | prior_export=0.25 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PRCH  ·  score **+16**  ·  Insurance - Property & Casualty
price=21.559999465942383  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.20 on 2026-11-04; prev RSI=68.06 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.06@2026-11-03 → 68.20@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.06@2026-11-03 → 68.20@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.06@2026-11-03 → 68.20@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=2.882 (G=0.9800 R=0.3400); 2026-11-03:GREEN:O=20.5400,C=21.5200,body=+0.9800,vol=2750500.0; 2026-11-04:RED:O=21.9000,C=21.5600,body=-0.3400,vol=1871453.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=1.470 (Gvol=2750500 Rvol=1871453); 2026-11-03:GREEN:O=20.5400,C=21.5200,body=+0.9800,vol=2750500.0; 2026-11-04:RED:O=21.9000,C=21.5600,body=-0.3400,vol=1871453.0 | **GOOD** |
| `A07_rvol` | RVOL=0.850 on 2026-11-04: today_vol=1871453 / avg20=2202079 (avg window 2026-10-07→2026-11-03, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.982 on 2026-11-04 (price=21.5600, mid=18.3820, upper=21.6191, lower=15.1449; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-04: price=21.5600 vs SMA50=17.4657 dist=+23.44% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=18.3820 SMA50=17.4657 SMA80=16.5674 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-10@15.2200; S2[2026-09-08→2026-10-06] low=2026-09-18@15.4900; S3[2026-10-07→2026-11-04] low=2026-10-14@16.0750 | lows=[15.220000267028809, 15.489999771118164, 16.075000762939453] span=5.62% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.7025084019021524 wick_frac=0.29749159809784764 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.37362660395470987 wick_frac=0.6263733960452901 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.882350301248752 need>1.4; red_wick_gt_green=False 5d trail=2026-10-29:GREEN:body=+1.4300:wick=1.1300; 2026-10-30:GREEN:body=+0.0600:wick=1.1280; 2026-11-02:RED:body=-0.0500:wick=1.2900; 2026-11-03:GREEN:body=+0.9800:wick=0.4150; 2026-11-04:RED:body=-0.3400:wick=0.5700 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=599.3 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.56 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 530.17 | **NEUTRAL** |
| `B04_income` | 9.6 | **GOOD** |
| `B05_profit_margin` | 1.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.43 (now=25.12 vs prior_export=20.69 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | -6.31 | **BAD** |
| `B11_insider_tx_delta` | delta=3.8899999999999997 (now=-6.31 vs prior=-10.2 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 2.1 | **GOOD** |
| `B13_short_float` | 16.93 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=599.3 (this export) | prior_export=266.67 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.56 (this export) | prior_export=7.78 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+16**  ·  Software - Application
price=270.0899963378906  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.31 on 2026-11-04; prev RSI=60.27 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.27@2026-11-03 → 69.31@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.27@2026-11-03 → 69.31@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.27@2026-11-03 → 69.31@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=2.578 (G=11.6000 R=4.5000); 2026-11-03:RED:O=256.8200,C=252.3200,body=-4.5000,vol=2764900.0; 2026-11-04:GREEN:O=258.4900,C=270.0900,body=+11.6000,vol=3823430.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=1.383 (Gvol=3823430 Rvol=2764900); 2026-11-03:RED:O=256.8200,C=252.3200,body=-4.5000,vol=2764900.0; 2026-11-04:GREEN:O=258.4900,C=270.0900,body=+11.6000,vol=3823430.0 | **GOOD** |
| `A07_rvol` | RVOL=2.212 on 2026-11-04: today_vol=3823430 / avg20=1728362 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.283 on 2026-11-04 (price=270.0900, mid=244.6490, upper=264.4843, lower=224.8136; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-04: price=270.0900 vs SMA50=232.8694 dist=+15.98% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=244.65_50=232.87_80=233.89 on 2026-11-04: SMA20=244.6490 SMA50=232.8694 SMA80=233.8907 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-09-04@216.2000; S2[2026-09-08→2026-10-06] low=2026-09-29@199.1801; S3[2026-10-07→2026-11-04] low=2026-10-14@228.5900 | lows=[216.1999969482422, 199.1800994873047, 228.58999633789062] span=14.77% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.7211695533635758 wick_frac=0.2788304466364242 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.45500508369191744 wick_frac=0.5449949163080825 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5777791341145835 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:GREEN:body=+3.0900:wick=4.6300; 2026-10-30:GREEN:body=+4.2500:wick=3.5800; 2026-11-02:GREEN:body=+7.7900:wick=5.4553; 2026-11-03:RED:body=-4.5000:wick=5.3900; 2026-11-04:GREEN:body=+11.6000:wick=4.4850 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7814.0 | **NEUTRAL** |
| `B04_income` | 1642.0 | **GOOD** |
| `B05_profit_margin` | 21.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 311.09 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=311.09 vs prior_export=311.09 on finviz_2026-11-04) | **NEUTRAL** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 1.36 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.36 vs prior=1.36 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.22 | **GOOD** |
| `B13_short_float` | 4.34 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.63 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=1.69 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CTSH  ·  score **+16**  ·  Information Technology Services
price=65.77999877929688  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.66 on 2026-11-04; prev RSI=49.39 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.39@2026-11-03 → 53.66@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.39@2026-11-03 → 53.66@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.39@2026-11-03 → 53.66@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.0100 R=0.0000); 2026-11-03:GREEN:O=63.4400,C=63.8300,body=+0.3900,vol=4068800.0; 2026-11-04:GREEN:O=64.1600,C=65.7800,body=+1.6200,vol=10844604.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=99.000 (Gvol=14913404 Rvol=0); 2026-11-03:GREEN:O=63.4400,C=63.8300,body=+0.3900,vol=4068800.0; 2026-11-04:GREEN:O=64.1600,C=65.7800,body=+1.6200,vol=10844604.0 | **GOOD** |
| `A07_rvol` | RVOL=1.644 on 2026-11-04: today_vol=10844604 / avg20=6595060 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.053 on 2026-11-04 (price=65.7800, mid=65.3760, upper=73.0071, lower=57.7449; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-04: price=65.7800 vs SMA50=62.3546 dist=+5.49% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=65.3760 SMA50=62.3546 SMA80=59.1372 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-17@56.2204; S2[2026-09-08→2026-10-06] low=2026-09-28@55.6300; S3[2026-10-07→2026-11-04] low=2026-10-08@54.7000 | lows=[56.220350988128516, 55.630001068115234, 54.70000076293945] span=2.78% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.3872283236208901 wick_frac=0.6127716763791099 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:RED:body=-0.0850:wick=3.6300; 2026-10-30:GREEN:body=+1.4100:wick=1.0350; 2026-11-02:GREEN:body=+0.1900:wick=1.8550; 2026-11-03:GREEN:body=+0.3900:wick=1.4500; 2026-11-04:GREEN:body=+1.6200:wick=1.2600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.78 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21891.0 | **NEUTRAL** |
| `B04_income` | 2573.0 | **GOOD** |
| `B05_profit_margin` | 11.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.35 | **NEUTRAL** |
| `B08_target_price_delta` | delta=5.089999999999989 (now=69.35 vs prior_export=64.26 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.16 | **GOOD** |
| `B10_insider_transactions` | -0.66 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.66 vs prior=-0.66 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.35 | **GOOD** |
| `B13_short_float` | 6.59 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.78 (this export) | prior_export=-0.54 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.0 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNC  ·  score **+16**  ·  Healthcare Plans
price=76.01000213623047  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.40 on 2026-11-04; prev RSI=50.16 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.16@2026-11-03 → 68.40@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.16@2026-11-03 → 68.40@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.16@2026-11-03 → 68.40@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=6.7400 R=0.0000); 2026-11-03:GREEN:O=66.1200,C=67.4800,body=+1.3600,vol=4739800.0; 2026-11-04:GREEN:O=70.6300,C=76.0100,body=+5.3800,vol=14968384.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=99.000 (Gvol=19708184 Rvol=0); 2026-11-03:GREEN:O=66.1200,C=67.4800,body=+1.3600,vol=4739800.0; 2026-11-04:GREEN:O=70.6300,C=76.0100,body=+5.3800,vol=14968384.0 | **GOOD** |
| `A07_rvol` | RVOL=3.089 on 2026-11-04: today_vol=14968384 / avg20=4845856 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.452 on 2026-11-04 (price=76.0100, mid=68.8790, upper=73.7909, lower=63.9671; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-11-04: price=76.0100 vs SMA50=66.3940 dist=+14.48% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=68.8790 SMA50=66.3940 SMA80=65.8509 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-31@63.4200; S2[2026-09-08→2026-10-06] low=2026-10-01@59.4600; S3[2026-10-07→2026-11-04] low=2026-10-08@63.3500 | lows=[63.41999816894531, 59.459999084472656, 63.349998474121094] span=6.66% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.8755460595655828 wick_frac=0.12445394043441715 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:GREEN:body=+0.3700:wick=1.4150; 2026-10-30:RED:body=-0.8600:wick=2.4900; 2026-11-02:RED:body=-2.5900:wick=2.2200; 2026-11-03:GREEN:body=+1.3600:wick=0.0500; 2026-11-04:GREEN:body=+5.3800:wick=1.4600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=506.06 (current export asof; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.49 (current export; earnings_date=10/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 203895.0 | **NEUTRAL** |
| `B04_income` | 1460.0 | **GOOD** |
| `B05_profit_margin` | 0.72 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 75.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.8299999999999983 (now=75.61 vs prior_export=71.78 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | -2.05 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.05 vs prior=-2.05 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.8 | **GOOD** |
| `B13_short_float` | 2.25 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=506.06 (this export) | prior_export=131.29 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.49 (this export) | prior_export=12.47 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SWKS  ·  score **+16**  ·  Semiconductors
price=82.91999816894531  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.61 on 2026-11-04; prev RSI=55.54 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.54@2026-11-03 → 49.61@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_down | RSI 55.54@2026-11-03 → 49.61@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **BAD** |
| `A04_rsi_cross_70` | below | RSI 55.54@2026-11-03 → 49.61@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=20.957 (G=9.6400 R=0.4600); 2026-11-03:RED:O=85.8600,C=85.4000,body=-0.4600,vol=5012800.0; 2026-11-04:GREEN:O=73.2800,C=82.9200,body=+9.6400,vol=10012442.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=1.997 (Gvol=10012442 Rvol=5012800); 2026-11-03:RED:O=85.8600,C=85.4000,body=-0.4600,vol=5012800.0; 2026-11-04:GREEN:O=73.2800,C=82.9200,body=+9.6400,vol=10012442.0 | **GOOD** |
| `A07_rvol` | RVOL=2.018 on 2026-11-04: today_vol=10012442 / avg20=4960579 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.112 on 2026-11-04 (price=82.9200, mid=82.0085, upper=90.1144, lower=73.9026; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-04: price=82.9200 vs SMA50=81.5502 dist=+1.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=82.0085 SMA50=81.5502 SMA80=75.1530 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-25@63.0600; S2[2026-09-08→2026-10-06] low=2026-09-08@73.5000; S3[2026-10-07→2026-11-04] low=2026-11-04@72.8900 | lows=[63.060001373291016, 73.5, 72.88999938964844] span=16.56% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.8239317866742483 wick_frac=0.17606821332575176 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.12201553399630066 wick_frac=0.8779844660036994 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=20.95656212163933 need>1.4; red_wick_gt_green=True 5d trail=2026-10-29:GREEN:body=+1.4300:wick=2.4800; 2026-10-30:RED:body=-0.1500:wick=3.4100; 2026-11-02:RED:body=-2.9900:wick=2.9300; 2026-11-03:RED:body=-0.4600:wick=3.3100; 2026-11-04:GREEN:body=+9.6400:wick=2.0600 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=0.52 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.93 (current export; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3959.6 | **NEUTRAL** |
| `B04_income` | 172.1 | **GOOD** |
| `B05_profit_margin` | 4.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=10.050000000000011 (now=78.12 vs prior_export=68.07 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 3.04 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.38 | **GOOD** |
| `B13_short_float` | 9.52 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=0.52 (this export) | prior_export=4.9 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.93 (this export) | prior_export=0.95 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+16**  ·  Oil & Gas Integrated
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
| `B01_eps_surprise` | EPS surprise=26.56 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.76 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 16088.78 | **NEUTRAL** |
| `B04_income` | 1023.59 | **GOOD** |
| `B05_profit_margin` | 6.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 65.17 | **NEUTRAL** |
| `B08_target_price_delta` | delta=4.109999999999999 (now=65.17 vs prior_export=61.06 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.46 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 44.47 | **GOOD** |
| `B13_short_float` | 1.35 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.56 (this export) | prior_export=27.28 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.76 (this export) | prior_export=6.07 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

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
| `B01_eps_surprise` | EPS surprise=4.55 (current export asof; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.11 (current export; earnings_date=10/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 321.74 | **NEUTRAL** |
| `B04_income` | 314.27 | **GOOD** |
| `B05_profit_margin` | 97.68 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 18.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.5 (now=18.5 vs prior_export=18.0 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 5.49 | **GOOD** |
| `B11_insider_tx_delta` | delta=-0.040000000000000036 (now=5.49 vs prior=5.53 on finviz_2026-11-04) | **BAD** |
| `B12_institutional_transactions` | 3.71 | **GOOD** |
| `B13_short_float` | 6.54 | **NEUTRAL** |
| `B14_earnings_date` | 10/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.55 (this export) | prior_export=16.67 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.11 (this export) | prior_export=3.45 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RMD  ·  score **+16**  ·  Medical Instruments & Supplies
price=240.50999450683594  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.74 on 2026-11-04; prev RSI=40.27 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 40.27@2026-11-03 → 52.74@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 40.27@2026-11-03 → 52.74@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 40.27@2026-11-03 → 52.74@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=5.1900 R=0.0000); 2026-11-03:GREEN:O=227.0100,C=227.0800,body=+0.0700,vol=2161200.0; 2026-11-04:GREEN:O=235.3900,C=240.5100,body=+5.1200,vol=2228576.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4389776 Rvol=0); 2026-11-03:GREEN:O=227.0100,C=227.0800,body=+0.0700,vol=2161200.0; 2026-11-04:GREEN:O=235.3900,C=240.5100,body=+5.1200,vol=2228576.0 | **GOOD** |
| `A07_rvol` | RVOL=1.678 on 2026-11-04: today_vol=2228576 / avg20=1328183 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.020 on 2026-11-04 (price=240.5100, mid=240.8885, upper=259.3739, lower=222.4031; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-04: price=240.5100 vs SMA50=231.7946 dist=+3.76% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=240.8885 SMA50=231.7946 SMA80=225.2975 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-07@202.0000; S2[2026-09-08→2026-10-06] low=2026-09-10@215.8000; S3[2026-10-07→2026-11-04] low=2026-11-03@221.5900 | lows=[202.0, 215.8000030517578, 221.58999633789062] span=9.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.31966722316931684 wick_frac=0.6803327768306832 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-29:RED:body=-0.7400:wick=3.9300; 2026-10-30:RED:body=-16.6200:wick=2.8850; 2026-11-02:GREEN:body=+1.6700:wick=4.3590; 2026-11-03:GREEN:body=+0.0700:wick=8.7500; 2026-11-04:GREEN:body=+5.1200:wick=2.9890 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.33 (current export asof; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.47 (current export; earnings_date=10/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5732.68 | **NEUTRAL** |
| `B04_income` | 1593.17 | **GOOD** |
| `B05_profit_margin` | 27.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 272.88 | **NEUTRAL** |
| `B08_target_price_delta` | delta=10.569999999999993 (now=272.88 vs prior_export=262.31 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | -2.89 | **BAD** |
| `B11_insider_tx_delta` | delta=0.08000000000000007 (now=-2.89 vs prior=-2.97 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 1.07 | **GOOD** |
| `B13_short_float` | 8.56 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.33 (this export) | prior_export=1.98 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.47 (this export) | prior_export=0.16 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LTC  ·  score **+15**  ·  REIT - Healthcare Facilities
price=43.04999923706055  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.20 on 2026-10-12; prev RSI=52.07 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.07@2026-10-09 → 57.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.07@2026-10-09 → 57.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.07@2026-10-09 → 57.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=6.000 (G=0.4800 R=0.0800); 2026-10-09:RED:O=42.5400,C=42.4600,body=-0.0800,vol=307451.0; 2026-10-12:GREEN:O=42.5700,C=43.0500,body=+0.4800,vol=564016.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.834 (Gvol=564016 Rvol=307451); 2026-10-09:RED:O=42.5400,C=42.4600,body=-0.0800,vol=307451.0; 2026-10-12:GREEN:O=42.5700,C=43.0500,body=+0.4800,vol=564016.0 | **GOOD** |
| `A07_rvol` | RVOL=0.832 on 2026-10-12: today_vol=564016 / avg20=677849 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.294 on 2026-10-12 (price=43.0500, mid=42.7855, upper=43.6837, lower=41.8873; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=43.0500 vs SMA50=41.3825 dist=+4.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=42.7855 SMA50=41.3825 SMA80=40.6004 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-11@37.7200; S2[2026-08-13→2026-09-11] low=2026-08-13@38.8800; S3[2026-09-14→2026-10-12] low=2026-10-08@41.3800 | lows=[37.720001220703125, 38.880001068115234, 41.380001068115234] span=9.70% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5714279226706509 wick_frac=0.4285720773293491 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.12700681302043906 wick_frac=0.872993186979561 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.999856952126645 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:RED:body=-0.0100:wick=0.4800; 2026-10-07:RED:body=-0.7400:wick=0.2668; 2026-10-08:GREEN:body=+0.3000:wick=0.5000; 2026-10-09:RED:body=-0.0800:wick=0.5499; 2026-10-12:GREEN:body=+0.4800:wick=0.3600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=1114.6 (current export asof; earnings_date=11/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=-68.13 (current export; earnings_date=11/3/2026 4:30:00 PM) | **BAD** |
| `B03_sales` | 394.32 | **NEUTRAL** |
| `B04_income` | 501.26 | **GOOD** |
| `B05_profit_margin` | 127.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0600000000000023 (now=46.31 vs prior_export=45.25 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 2.11 | **GOOD** |
| `B10_insider_transactions` | 1.3 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.06000000000000005 (now=1.3 vs prior=1.24 on finviz_2026-11-04) | **GOOD** |
| `B12_institutional_transactions` | 10.56 | **GOOD** |
| `B13_short_float` | 6.12 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1114.6 (this export) | prior_export=16.67 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=-68.13 (this export) | prior_export=-67.11 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **BAD** |

### NICE  ·  score **+15**  ·  Software - Application
price=99.58999633789062  pair=`2026-11-03→2026-11-04`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=31.15 on 2026-11-04; prev RSI=37.82 on 2026-11-03 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 37.82@2026-11-03 → 31.15@2026-11-04 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 37.82@2026-11-03 → 31.15@2026-11-04 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 37.82@2026-11-03 → 31.15@2026-11-04 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_body_sum/RED_body_sum=2.415 (G=3.5500 R=1.4700); 2026-11-03:RED:O=107.7400,C=106.2700,body=-1.4700,vol=1345400.0; 2026-11-04:GREEN:O=96.0400,C=99.5900,body=+3.5500,vol=1820544.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-03 + 2026-11-04; ratio=GREEN_vol/RED_vol=1.353 (Gvol=1820544 Rvol=1345400); 2026-11-03:RED:O=107.7400,C=106.2700,body=-1.4700,vol=1345400.0; 2026-11-04:GREEN:O=96.0400,C=99.5900,body=+3.5500,vol=1820544.0 | **GOOD** |
| `A07_rvol` | RVOL=3.400 on 2026-11-04: today_vol=1820544 / avg20=535430 (avg window 2026-10-07→2026-11-03, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-1.459 on 2026-11-04 (price=99.5900, mid=114.6777, upper=125.0164, lower=104.3391; 20d BB) | **GOOD** |
| `A09_above_sma50` | above=False on 2026-11-04: price=99.5900 vs SMA50=111.0021 dist=-10.28% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-04: SMA20=114.6777 SMA50=111.0021 SMA80=106.7228 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-07→2026-11-04 (63 bars); S1[2026-08-07→2026-09-04] low=2026-08-07@97.2100; S2[2026-09-08→2026-10-06] low=2026-09-11@97.4400; S3[2026-10-07→2026-11-04] low=2026-11-04@96.9500 | lows=[97.20999908447266, 97.44000244140625, 96.94999694824219] span=0.51% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: GREEN body_frac=0.47019787913022915 wick_frac=0.5298021208697709 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-03+2026-11-04: RED body_frac=0.3434583270943407 wick_frac=0.6565416729056593 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.414960866947622 need>1.4; red_wick_gt_green=False 5d trail=2026-10-29:GREEN:body=+0.8900:wick=2.3500; 2026-10-30:RED:body=-1.0900:wick=1.5700; 2026-11-02:RED:body=-2.6100:wick=1.3300; 2026-11-03:RED:body=-1.4700:wick=2.8100; 2026-11-04:GREEN:body=+3.5500:wick=4.0000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.13 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.64 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3089.11 | **NEUTRAL** |
| `B04_income` | 428.37 | **GOOD** |
| `B05_profit_margin` | 13.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 128.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0100000000000193 (now=128.83 vs prior_export=127.82 on finviz_2026-11-04) | **GOOD** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-11-04) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.95 | **GOOD** |
| `B13_short_float` | 2.22 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.13 (this export) | prior_export=2.27 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.64 (this export) | prior_export=2.09 (finviz_2026-11-04) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-05_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.