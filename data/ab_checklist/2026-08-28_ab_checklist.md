# A+B1 Feature Checklist — 2026-08-28

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,696** names
- Export: `finviz_2026-08-28.csv` · prior export for Δ: `2026-08-27`
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
| 1 | CMBT | +19 | 19 | 0 | 2026-08-26→2026-08-27 | Oil & Gas Midstream |
| 2 | A | +17 | 18 | 1 | 2026-08-26→2026-08-27 | Diagnostics & Research |
| 3 | FLS | +17 | 17 | 0 | 2026-08-26→2026-08-27 | Specialty Industrial Machinery |
| 4 | MTDR | +17 | 17 | 0 | 2026-08-26→2026-08-27 | Oil & Gas E&P |
| 5 | CRSR | +17 | 18 | 1 | 2026-08-26→2026-08-27 | Computer Hardware |
| 6 | CRWD | +17 | 19 | 2 | 2026-08-26→2026-08-27 | Software - Infrastructure |
| 7 | LNC | +16 | 16 | 0 | 2026-08-26→2026-08-27 | Insurance - Life |
| 8 | PWP | +16 | 17 | 1 | 2026-08-26→2026-08-27 | Capital Markets |
| 9 | ADSK | +16 | 17 | 1 | 2026-08-26→2026-08-27 | Software - Application |
| 10 | KNTK | +16 | 17 | 1 | 2026-08-26→2026-08-27 | Oil & Gas Midstream |
| 11 | ROAD | +16 | 16 | 0 | 2026-08-26→2026-08-27 | Engineering & Construction |
| 12 | ZBH | +16 | 17 | 1 | 2026-08-26→2026-08-27 | Medical Devices |
| 13 | DINO | +16 | 16 | 0 | 2026-08-26→2026-08-27 | Oil & Gas Refining & Marketing |
| 14 | TPG | +16 | 16 | 0 | 2026-08-26→2026-08-27 | Asset Management |
| 15 | TENB | +16 | 17 | 1 | 2026-08-26→2026-08-27 | Software - Infrastructure |

## Full checklist — top 15

### CMBT  ·  score **+19**  ·  Oil & Gas Midstream
price=18.280000686645508  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.53 on 2026-08-27; prev RSI=62.82 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.82@2026-08-26 → 68.53@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.82@2026-08-26 → 68.53@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.82@2026-08-26 → 68.53@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=1.923 (G=0.5000 R=0.2600); 2026-08-26:RED:O=17.9100,C=17.6500,body=-0.2600,vol=1610700.0; 2026-08-27:GREEN:O=17.7800,C=18.2800,body=+0.5000,vol=1656200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.028 (Gvol=1656200 Rvol=1610700); 2026-08-26:RED:O=17.9100,C=17.6500,body=-0.2600,vol=1610700.0; 2026-08-27:GREEN:O=17.7800,C=18.2800,body=+0.5000,vol=1656200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.724 on 2026-08-27: today_vol=1656200 / avg20=960910 (avg window 2026-07-29→2026-08-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.734 on 2026-08-27 (price=18.2800, mid=17.0400, upper=18.7284, lower=15.3516; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=18.2800 vs SMA50=15.8892 dist=+15.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=17.0400 SMA50=15.8892 SMA80=15.4587 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-18@13.9880; S2[2026-06-25→2026-07-28] low=2026-06-30@13.8950; S3[2026-07-29→2026-08-27] low=2026-07-29@15.4900 | lows=[13.98799991607666, 13.895000457763672, 15.489999771118164] span=11.48% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.7874012909892077 wick_frac=0.21259870901079228 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.4905658340320938 wick_frac=0.5094341659679062 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9230752301654257 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+0.0500:wick=0.4800; 2026-08-24:GREEN:body=+0.2200:wick=0.0350; 2026-08-25:GREEN:body=+0.0700:wick=0.2400; 2026-08-26:RED:body=-0.2600:wick=0.2700; 2026-08-27:GREEN:body=+0.5000:wick=0.1350 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=42.02 (current export asof; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.0 (current export; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2266.8 | **NEUTRAL** |
| `B04_income` | 842.15 | **GOOD** |
| `B05_profit_margin` | 37.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 20.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.2199999999999989 (now=20.06 vs prior_export=18.84 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 1.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=42.02 (this export) | prior_export=30.1 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.0 (this export) | prior_export=21.46 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### A  ·  score **+17**  ·  Diagnostics & Research
price=157.69000244140625  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.93 on 2026-08-27; prev RSI=66.83 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.83@2026-08-26 → 69.93@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.83@2026-08-26 → 69.93@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.83@2026-08-26 → 69.93@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=1.584 (G=2.6300 R=1.6600); 2026-08-26:GREEN:O=152.4500,C=155.0800,body=+2.6300,vol=3202500.0; 2026-08-27:RED:O=159.3500,C=157.6900,body=-1.6600,vol=3202300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.000 (Gvol=3202500 Rvol=3202300); 2026-08-26:GREEN:O=152.4500,C=155.0800,body=+2.6300,vol=3202500.0; 2026-08-27:RED:O=159.3500,C=157.6900,body=-1.6600,vol=3202300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.673 on 2026-08-27: today_vol=3202300 / avg20=1913960 (avg window 2026-07-30→2026-08-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.681 on 2026-08-27 (price=157.6900, mid=148.9315, upper=161.7893, lower=136.0737; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=157.6900 vs SMA50=139.3743 dist=+13.14% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=148.9315 SMA50=139.3743 SMA80=132.8673 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-27 (63 bars); S1[2026-05-27→2026-06-25] low=2026-05-27@114.7992; S2[2026-06-26→2026-07-29] low=2026-07-08@127.4600; S3[2026-07-30→2026-08-27] low=2026-07-31@135.7800 | lows=[114.79918922981902, 127.45999908447266, 135.77999877929688] span=18.28% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.8825532394250808 wick_frac=0.11744676057491922 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.20024185844470663 wick_frac=0.7997581415552933 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5843367956613659 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+2.5000:wick=2.7700; 2026-08-24:RED:body=-3.7000:wick=1.7900; 2026-08-25:GREEN:body=+0.8700:wick=2.0200; 2026-08-26:GREEN:body=+2.6300:wick=0.3500; 2026-08-27:RED:body=-1.6600:wick=6.6300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.85 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.98 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7372.0 | **NEUTRAL** |
| `B04_income` | 1440.0 | **GOOD** |
| `B05_profit_margin` | 19.53 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 172.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=11.830000000000013 (now=172.83 vs prior_export=161.0 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.8 | **GOOD** |
| `B10_insider_transactions` | -0.17 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.17 vs prior=-0.17 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.93 | **GOOD** |
| `B13_short_float` | 1.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.85 (this export) | prior_export=8.85 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.98 (this export) | prior_export=1.98 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### FLS  ·  score **+17**  ·  Specialty Industrial Machinery
price=82.0  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.42 on 2026-08-27; prev RSI=65.61 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.61@2026-08-26 → 62.42@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.61@2026-08-26 → 62.42@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.61@2026-08-26 → 62.42@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=2.333 (G=2.1000 R=0.9000); 2026-08-26:GREEN:O=80.7000,C=82.8000,body=+2.1000,vol=1721900.0; 2026-08-27:RED:O=82.9000,C=82.0000,body=-0.9000,vol=1353900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.272 (Gvol=1721900 Rvol=1353900); 2026-08-26:GREEN:O=80.7000,C=82.8000,body=+2.1000,vol=1721900.0; 2026-08-27:RED:O=82.9000,C=82.0000,body=-0.9000,vol=1353900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.822 on 2026-08-27: today_vol=1353900 / avg20=1647400 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.649 on 2026-08-27 (price=82.0000, mid=79.6475, upper=83.2719, lower=76.0231; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=82.0000 vs SMA50=75.8202 dist=+8.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=79.6475 SMA50=75.8202 SMA80=74.2234 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-26@70.4926; S2[2026-06-25→2026-07-28] low=2026-07-20@66.3300; S3[2026-07-29→2026-08-27] low=2026-07-29@69.1400 | lows=[70.4925841915484, 66.33000183105469, 69.13999938964844] span=6.28% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.7070724715118012 wick_frac=0.29292752848819886 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.43269424749384694 wick_frac=0.567305752506153 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3333361590302206 need>1.4; red_wick_gt_green=False 5d trail=2026-08-21:GREEN:body=+0.2300:wick=1.6300; 2026-08-24:GREEN:body=+0.9900:wick=1.0100; 2026-08-25:RED:body=-1.2100:wick=0.3900; 2026-08-26:GREEN:body=+2.1000:wick=0.8700; 2026-08-27:RED:body=-0.9000:wick=1.1800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=10.45 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.91 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.44 vs prior=0.44 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.33 | **GOOD** |
| `B13_short_float` | 6.07 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=10.45 (this export) | prior_export=10.45 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.91 (this export) | prior_export=0.91 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MTDR  ·  score **+17**  ·  Oil & Gas E&P
price=56.7400016784668  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.54 on 2026-08-27; prev RSI=56.32 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.32@2026-08-26 → 60.54@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.32@2026-08-26 → 60.54@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.32@2026-08-26 → 60.54@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.7000 R=0.0000); 2026-08-26:GREEN:O=54.7900,C=55.2500,body=+0.4600,vol=1743100.0; 2026-08-27:GREEN:O=54.5000,C=56.7400,body=+2.2400,vol=1588700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3331800 Rvol=0); 2026-08-26:GREEN:O=54.7900,C=55.2500,body=+0.4600,vol=1743100.0; 2026-08-27:GREEN:O=54.5000,C=56.7400,body=+2.2400,vol=1588700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.727 on 2026-08-27: today_vol=1588700 / avg20=2184825 (avg window 2026-07-30→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.441 on 2026-08-27 (price=56.7400, mid=53.2853, upper=61.1279, lower=45.4427; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=56.7400 vs SMA50=51.5091 dist=+10.16% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=53.29_50=51.51_80=53.31 on 2026-08-27: SMA20=53.2853 SMA50=51.5091 SMA80=53.3086 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-27 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-24@48.0102; S2[2026-06-26→2026-07-29] low=2026-07-28@45.0927; S3[2026-07-30→2026-08-27] low=2026-08-05@46.5316 | lows=[48.01019827347788, 45.09266872932533, 46.531587687321455] span=6.47% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.5963972953042322 wick_frac=0.4036027046957678 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:RED:body=-0.6100:wick=0.9800; 2026-08-24:RED:body=-0.3100:wick=1.6800; 2026-08-25:RED:body=-0.3500:wick=0.9400; 2026-08-26:GREEN:body=+0.4600:wick=1.3400; 2026-08-27:GREEN:body=+2.2400:wick=0.1500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=25.72 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.71 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 3839.69 | **NEUTRAL** |
| `B04_income` | 723.69 | **GOOD** |
| `B05_profit_margin` | 18.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 69.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.14999999999999147 (now=69.1 vs prior_export=68.95 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.37 | **GOOD** |
| `B13_short_float` | 12.2 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=25.72 (this export) | prior_export=25.72 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.71 (this export) | prior_export=11.71 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+17**  ·  Computer Hardware
price=11.979999542236328  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.00 on 2026-08-27; prev RSI=53.98 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.98@2026-08-26 → 55.00@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.98@2026-08-26 → 55.00@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.98@2026-08-26 → 55.00@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=3.556 (G=0.3200 R=0.0900); 2026-08-26:GREEN:O=11.5200,C=11.8400,body=+0.3200,vol=1206000.0; 2026-08-27:RED:O=12.0700,C=11.9800,body=-0.0900,vol=1142300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.056 (Gvol=1206000 Rvol=1142300); 2026-08-26:GREEN:O=11.5200,C=11.8400,body=+0.3200,vol=1206000.0; 2026-08-27:RED:O=12.0700,C=11.9800,body=-0.0900,vol=1142300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.501 on 2026-08-27: today_vol=1142300 / avg20=2281175 (avg window 2026-07-30→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.019 on 2026-08-27 (price=11.9800, mid=11.9385, upper=14.1574, lower=9.7196; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=11.9800 vs SMA50=10.4653 dist=+14.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=11.9385 SMA50=10.4653 SMA80=9.7382 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-27 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-09@8.0500; S2[2026-06-30→2026-07-29] low=2026-07-07@8.3000; S3[2026-07-30→2026-08-27] low=2026-07-30@10.1950 | lows=[8.050000190734863, 8.300000190734863, 10.194999694824219] span=26.65% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.8205133221174538 wick_frac=0.17948667788254627 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.18000030517578125 wick_frac=0.8199996948242188 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.5555461365659307 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1040; 2026-08-25:GREEN:body=+0.7400:wick=0.1000; 2026-08-26:GREEN:body=+0.3200:wick=0.0700; 2026-08-27:RED:body=-0.0900:wick=0.4100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 23.38 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRWD  ·  score **+17**  ·  Software - Infrastructure
price=227.9600067138672  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.05 on 2026-08-27; prev RSI=42.21 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.21@2026-08-26 → 64.05@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 42.21@2026-08-26 → 64.05@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 42.21@2026-08-26 → 64.05@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=26.1400 R=0.0000); 2026-08-26:GREEN:O=182.7500,C=189.1800,body=+6.4300,vol=13619900.0; 2026-08-27:GREEN:O=208.2500,C=227.9600,body=+19.7100,vol=23398800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=37018700 Rvol=0); 2026-08-26:GREEN:O=182.7500,C=189.1800,body=+6.4300,vol=13619900.0; 2026-08-27:GREEN:O=208.2500,C=227.9600,body=+19.7100,vol=23398800.0 | **GOOD** |
| `A07_rvol` | RVOL=2.925 on 2026-08-27: today_vol=23398800 / avg20=7999610 (avg window 2026-07-30→2026-08-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.733 on 2026-08-27 (price=227.9600, mid=207.5775, upper=235.3878, lower=179.7672; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=227.9600 vs SMA50=195.2736 dist=+16.74% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=207.5775 SMA50=195.2736 SMA80=181.7541 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-27 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-09@154.4350; S2[2026-06-30→2026-07-29] low=2026-07-28@174.1400; S3[2026-07-30→2026-08-27] low=2026-07-30@177.7470 | lows=[154.43499755859375, 174.13999938964844, 177.7469940185547] span=15.10% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.7477993922040911 wick_frac=0.25220060779590897 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+1.0100:wick=4.8300; 2026-08-24:GREEN:body=+1.1600:wick=5.8100; 2026-08-25:RED:body=-6.2400:wick=5.9100; 2026-08-26:GREEN:body=+6.4300:wick=3.6500; 2026-08-27:GREEN:body=+19.7100:wick=3.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.31 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.14 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 5396.15 | **NEUTRAL** |
| `B04_income` | 37.77 | **GOOD** |
| `B05_profit_margin` | 0.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 231.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=16.569999999999993 (now=231.0 vs prior_export=214.43 on finviz_2026-08-27) | **GOOD** |
| `B09_analyst_recom` | 1.63 | **GOOD** |
| `B10_insider_transactions` | -3.75 | **BAD** |
| `B11_insider_tx_delta` | delta=-0.10000000000000009 (now=-3.75 vs prior=-3.65 on finviz_2026-08-27) | **BAD** |
| `B12_institutional_transactions` | 4.04 | **GOOD** |
| `B13_short_float` | 2.43 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.31 (this export) | prior_export=6.31 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.14 (this export) | prior_export=2.14 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNC  ·  score **+16**  ·  Insurance - Life
price=43.5099983215332  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.69 on 2026-08-27; prev RSI=51.74 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.74@2026-08-26 → 52.69@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.74@2026-08-26 → 52.69@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.74@2026-08-26 → 52.69@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1700 R=0.0000); 2026-08-26:GREEN:O=42.5100,C=43.3200,body=+0.8100,vol=1809700.0; 2026-08-27:GREEN:O=43.1500,C=43.5100,body=+0.3600,vol=1410600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=3220300 Rvol=0); 2026-08-26:GREEN:O=42.5100,C=43.3200,body=+0.8100,vol=1809700.0; 2026-08-27:GREEN:O=43.1500,C=43.5100,body=+0.3600,vol=1410600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.648 on 2026-08-27: today_vol=1410600 / avg20=2178265 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.348 on 2026-08-27 (price=43.5100, mid=44.6425, upper=47.8981, lower=41.3869; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=43.5100 vs SMA50=41.0975 dist=+5.87% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=44.6425 SMA50=41.0975 SMA80=38.8462 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-03@32.8415; S2[2026-06-25→2026-07-28] low=2026-06-30@34.8781; S3[2026-07-29→2026-08-27] low=2026-07-29@41.0700 | lows=[32.84154432414021, 34.878074838954085, 41.06999969482422] span=25.06% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.5921719066135004 wick_frac=0.4078280933864996 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:RED:body=-0.3600:wick=0.6300; 2026-08-24:RED:body=-0.2700:wick=0.5000; 2026-08-25:RED:body=-0.3500:wick=0.8300; 2026-08-26:GREEN:body=+0.8100:wick=0.3400; 2026-08-27:GREEN:body=+0.3600:wick=0.3900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.42 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.8 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20681.0 | **NEUTRAL** |
| `B04_income` | 2269.0 | **GOOD** |
| `B05_profit_margin` | 10.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 48.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=48.33 vs prior_export=48.33 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 232.75 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=232.75 vs prior=232.75 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.86 | **GOOD** |
| `B13_short_float` | 5.46 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.42 (this export) | prior_export=14.42 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.8 (this export) | prior_export=0.8 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PWP  ·  score **+16**  ·  Capital Markets
price=16.950000762939453  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.89 on 2026-08-27; prev RSI=49.42 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.42@2026-08-26 → 52.89@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.42@2026-08-26 → 52.89@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.42@2026-08-26 → 52.89@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=6.857 (G=0.4800 R=0.0700); 2026-08-26:RED:O=16.5600,C=16.4900,body=-0.0700,vol=608900.0; 2026-08-27:GREEN:O=16.4700,C=16.9500,body=+0.4800,vol=941500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.546 (Gvol=941500 Rvol=608900); 2026-08-26:RED:O=16.5600,C=16.4900,body=-0.0700,vol=608900.0; 2026-08-27:GREEN:O=16.4700,C=16.9500,body=+0.4800,vol=941500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.890 on 2026-08-27: today_vol=941500 / avg20=1057350 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.071 on 2026-08-27 (price=16.9500, mid=16.8532, upper=18.2249, lower=15.4816; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=16.9500 vs SMA50=16.2586 dist=+4.25% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=16.85_50=16.26_80=16.79 on 2026-08-27: SMA20=16.8532 SMA50=16.2586 SMA80=16.7895 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-11@14.5400; S2[2026-06-25→2026-07-28] low=2026-07-13@14.6100; S3[2026-07-29→2026-08-27] low=2026-07-30@14.1100 | lows=[14.539999961853027, 14.609999656677246, 14.109999656677246] span=3.54% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.6857156091792416 wick_frac=0.3142843908207585 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.21538443480659417 wick_frac=0.7846155651934058 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.857193460490463 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+0.3800:wick=0.0900; 2026-08-24:RED:body=-0.2000:wick=0.3700; 2026-08-25:GREEN:body=+0.1000:wick=0.3900; 2026-08-26:RED:body=-0.0700:wick=0.2550; 2026-08-27:GREEN:body=+0.4800:wick=0.2200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=263.64 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=8.13 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 689.25 | **NEUTRAL** |
| `B04_income` | 22.19 | **GOOD** |
| `B05_profit_margin` | 3.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.25 vs prior_export=22.25 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -9.19 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.19 vs prior=-9.19 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.98 | **GOOD** |
| `B13_short_float` | 10.75 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=263.64 (this export) | prior_export=263.64 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=8.13 (this export) | prior_export=8.13 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ADSK  ·  score **+16**  ·  Software - Application
price=270.5799865722656  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.52 on 2026-08-27; prev RSI=61.36 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.36@2026-08-26 → 69.52@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.36@2026-08-26 → 69.52@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.36@2026-08-26 → 69.52@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=18.6100 R=0.0000); 2026-08-26:GREEN:O=245.2700,C=254.7700,body=+9.5000,vol=1737100.0; 2026-08-27:GREEN:O=261.4700,C=270.5800,body=+9.1100,vol=3952200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=5689300 Rvol=0); 2026-08-26:GREEN:O=245.2700,C=254.7700,body=+9.5000,vol=1737100.0; 2026-08-27:GREEN:O=261.4700,C=270.5800,body=+9.1100,vol=3952200.0 | **GOOD** |
| `A07_rvol` | RVOL=2.229 on 2026-08-27: today_vol=3952200 / avg20=1773380 (avg window 2026-07-29→2026-08-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=1.240 on 2026-08-27 (price=270.5800, mid=249.0405, upper=266.4059, lower=231.6751; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-27: price=270.5800 vs SMA50=223.2996 dist=+21.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=249.04_50=223.30_80=228.35 on 2026-08-27: SMA20=249.0405 SMA50=223.2996 SMA80=228.3539 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-22@185.5000; S2[2026-06-25→2026-07-28] low=2026-06-25@189.0000; S3[2026-07-29→2026-08-27] low=2026-08-04@229.7000 | lows=[185.5, 189.0, 229.6999969482422] span=23.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.8104816664065233 wick_frac=0.1895183335934767 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+2.8100:wick=6.4600; 2026-08-24:GREEN:body=+0.8400:wick=3.9200; 2026-08-25:RED:body=-0.2100:wick=4.7700; 2026-08-26:GREEN:body=+9.5000:wick=2.0300; 2026-08-27:GREEN:body=+9.1100:wick=2.3200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.63 (current export asof; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.69 (current export; earnings_date=8/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 7519.0 | **NEUTRAL** |
| `B04_income` | 1463.0 | **GOOD** |
| `B05_profit_margin` | 19.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 315.03 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=315.03 vs prior_export=315.03 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.43 | **GOOD** |
| `B10_insider_transactions` | 1.35 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.35 vs prior=1.35 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.96 | **GOOD** |
| `B13_short_float` | 3.53 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.63 (this export) | prior_export=5.15 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.69 (this export) | prior_export=2.15 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### KNTK  ·  score **+16**  ·  Oil & Gas Midstream
price=54.380001068115234  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.78 on 2026-08-27; prev RSI=67.73 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 67.73@2026-08-26 → 62.78@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.73@2026-08-26 → 62.78@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 67.73@2026-08-26 → 62.78@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=1.672 (G=1.0200 R=0.6100); 2026-08-26:GREEN:O=54.0800,C=55.1000,body=+1.0200,vol=1227700.0; 2026-08-27:RED:O=54.9900,C=54.3800,body=-0.6100,vol=990400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.240 (Gvol=1227700 Rvol=990400); 2026-08-26:GREEN:O=54.0800,C=55.1000,body=+1.0200,vol=1227700.0; 2026-08-27:RED:O=54.9900,C=54.3800,body=-0.6100,vol=990400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.854 on 2026-08-27: today_vol=990400 / avg20=1160250 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.466 on 2026-08-27 (price=54.3800, mid=52.2025, upper=56.8792, lower=47.5258; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=54.3800 vs SMA50=49.6764 dist=+9.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=52.2025 SMA50=49.6764 SMA80=48.8840 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-18@43.5372; S2[2026-06-25→2026-07-28] low=2026-07-02@46.5197; S3[2026-07-29→2026-08-27] low=2026-08-04@47.4000 | lows=[43.53716842744477, 46.51970327639811, 47.400001525878906] span=8.87% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.5204066936290633 wick_frac=0.47959330637093667 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.2675442705893376 wick_frac=0.7324557294106624 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.672123971283488 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:RED:body=-0.4000:wick=1.1800; 2026-08-24:GREEN:body=+0.3000:wick=0.6000; 2026-08-25:GREEN:body=+0.0400:wick=0.7700; 2026-08-26:GREEN:body=+1.0200:wick=0.9400; 2026-08-27:RED:body=-0.6100:wick=1.6700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=148.54 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=30.44 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1826.32 | **NEUTRAL** |
| `B04_income` | 185.44 | **GOOD** |
| `B05_profit_margin` | 10.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 56.59 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=56.59 vs prior_export=56.59 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.74 | **GOOD** |
| `B10_insider_transactions` | -11.19 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-11.19 vs prior=-11.19 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.53 | **GOOD** |
| `B13_short_float` | 10.85 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=148.54 (this export) | prior_export=148.54 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=30.44 (this export) | prior_export=30.44 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ROAD  ·  score **+16**  ·  Engineering & Construction
price=114.75  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.20 on 2026-08-27; prev RSI=52.98 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.98@2026-08-26 → 53.20@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.98@2026-08-26 → 53.20@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.98@2026-08-26 → 53.20@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=41.818 (G=4.6000 R=0.1100); 2026-08-26:GREEN:O=109.9500,C=114.5500,body=+4.6000,vol=717200.0; 2026-08-27:RED:O=114.8600,C=114.7500,body=-0.1100,vol=397900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.802 (Gvol=717200 Rvol=397900); 2026-08-26:GREEN:O=109.9500,C=114.5500,body=+4.6000,vol=717200.0; 2026-08-27:RED:O=114.8600,C=114.7500,body=-0.1100,vol=397900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.516 on 2026-08-27: today_vol=397900 / avg20=771005 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.115 on 2026-08-27 (price=114.7500, mid=113.2283, upper=126.4323, lower=100.0242; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=114.7500 vs SMA50=111.4979 dist=+2.92% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=113.23_50=111.50_80=114.21 on 2026-08-27: SMA20=113.2283 SMA50=111.4979 SMA80=114.2078 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-11@103.0000; S2[2026-06-25→2026-07-28] low=2026-07-13@93.4200; S3[2026-07-29→2026-08-27] low=2026-08-06@98.9550 | lows=[103.0, 93.41999816894531, 98.95500183105469] span=10.25% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.896686496133254 wick_frac=0.10331350386674598 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.023789056506021512 wick_frac=0.9762109434939785 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=41.818005271188795 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:RED:body=-2.2400:wick=1.3800; 2026-08-24:RED:body=-3.6800:wick=0.1400; 2026-08-25:GREEN:body=+0.1100:wick=2.5550; 2026-08-26:GREEN:body=+4.6000:wick=0.5300; 2026-08-27:RED:body=-0.1100:wick=4.5140 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=6.59 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.34 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3477.93 | **NEUTRAL** |
| `B04_income` | 142.51 | **GOOD** |
| `B05_profit_margin` | 4.1 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 143.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=143.83 vs prior_export=143.83 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.74 | **GOOD** |
| `B13_short_float` | 7.3 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.59 (this export) | prior_export=6.59 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.34 (this export) | prior_export=5.34 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ZBH  ·  score **+16**  ·  Medical Devices
price=99.4800033569336  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.87 on 2026-08-27; prev RSI=61.72 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.72@2026-08-26 → 55.87@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.72@2026-08-26 → 55.87@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.72@2026-08-26 → 55.87@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=4.133 (G=1.8600 R=0.4500); 2026-08-26:GREEN:O=99.5200,C=101.3800,body=+1.8600,vol=1352800.0; 2026-08-27:RED:O=99.9300,C=99.4800,body=-0.4500,vol=1351500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.001 (Gvol=1352800 Rvol=1351500); 2026-08-26:GREEN:O=99.5200,C=101.3800,body=+1.8600,vol=1352800.0; 2026-08-27:RED:O=99.9300,C=99.4800,body=-0.4500,vol=1351500.0 | **GOOD** |
| `A07_rvol` | RVOL=0.577 on 2026-08-27: today_vol=1351500 / avg20=2342135 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.224 on 2026-08-27 (price=99.4800, mid=98.6445, upper=102.3692, lower=94.9198; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=99.4800 vs SMA50=93.4734 dist=+6.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=98.6445 SMA50=93.4734 SMA80=89.9494 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-29@81.3638; S2[2026-06-25→2026-07-28] low=2026-06-30@83.7200; S3[2026-07-29→2026-08-27] low=2026-07-30@93.4900 | lows=[81.36379644583408, 83.72000122070312, 93.48999786376953] span=14.90% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.6690634253706972 wick_frac=0.33093657462930287 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.21738986211802344 wick_frac=0.7826101378819765 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.133362720830084 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+2.3100:wick=0.1700; 2026-08-24:RED:body=-0.3400:wick=1.0900; 2026-08-25:GREEN:body=+0.1000:wick=1.4900; 2026-08-26:GREEN:body=+1.8600:wick=0.9200; 2026-08-27:RED:body=-0.4500:wick=1.6200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.97 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.97 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8508.9 | **NEUTRAL** |
| `B04_income` | 806.6 | **GOOD** |
| `B05_profit_margin` | 9.48 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 106.45 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=106.45 vs prior_export=106.45 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.41 | **GOOD** |
| `B10_insider_transactions` | -1.79 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-1.79 vs prior=-1.79 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.03 | **GOOD** |
| `B13_short_float` | 4.62 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.97 (this export) | prior_export=2.97 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.97 (this export) | prior_export=1.97 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+16**  ·  Oil & Gas Refining & Marketing
price=97.0  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.63 on 2026-08-27; prev RSI=62.99 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.99@2026-08-26 → 63.63@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.99@2026-08-26 → 63.63@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.99@2026-08-26 → 63.63@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.5200 R=0.0000); 2026-08-26:GREEN:O=92.0000,C=96.5200,body=+4.5200,vol=2409900.0; 2026-08-27:DOJI:O=97.0000,C=97.0000,body=+0.0000,vol=1840400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=3.619 (Gvol=3330100 Rvol=920200); 2026-08-26:GREEN:O=92.0000,C=96.5200,body=+4.5200,vol=2409900.0; 2026-08-27:DOJI:O=97.0000,C=97.0000,body=+0.0000,vol=1840400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.698 on 2026-08-27: today_vol=1840400 / avg20=2637180 (avg window 2026-07-30→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.596 on 2026-08-27 (price=97.0000, mid=90.6754, upper=101.2912, lower=80.0596; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=97.0000 vs SMA50=83.3561 dist=+16.37% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=90.6754 SMA50=83.3561 SMA80=78.3650 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-29→2026-08-27 (63 bars); S1[2026-05-29→2026-06-29] low=2026-06-22@63.4268; S2[2026-06-30→2026-07-29] low=2026-06-30@69.0718; S3[2026-07-30→2026-08-27] low=2026-08-07@80.0041 | lows=[63.42683480953448, 69.07184693233171, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.6827786658245908 wick_frac=0.31722133417540915 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-21:GREEN:body=+3.3200:wick=1.0700; 2026-08-24:RED:body=-0.8600:wick=3.6300; 2026-08-25:RED:body=-0.8900:wick=1.8000; 2026-08-26:GREEN:body=+4.5200:wick=2.1000; 2026-08-27:DOJI:body=+0.0000:wick=3.7500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 93.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=93.29 vs prior_export=93.29 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.37 | **GOOD** |
| `B13_short_float` | 5.02 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TPG  ·  score **+16**  ·  Asset Management
price=53.939998626708984  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.27 on 2026-08-27; prev RSI=65.34 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.34@2026-08-26 → 68.27@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.34@2026-08-26 → 68.27@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.34@2026-08-26 → 68.27@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=37.997 (G=1.1400 R=0.0300); 2026-08-26:RED:O=52.8300,C=52.8000,body=-0.0300,vol=1691600.0; 2026-08-27:GREEN:O=52.8000,C=53.9400,body=+1.1400,vol=3187900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=1.885 (Gvol=3187900 Rvol=1691600); 2026-08-26:RED:O=52.8300,C=52.8000,body=-0.0300,vol=1691600.0; 2026-08-27:GREEN:O=52.8000,C=53.9400,body=+1.1400,vol=3187900.0 | **GOOD** |
| `A07_rvol` | RVOL=0.961 on 2026-08-27: today_vol=3187900 / avg20=3318225 (avg window 2026-07-29→2026-08-26, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.546 on 2026-08-27 (price=53.9400, mid=50.5349, upper=56.7689, lower=44.3010; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=53.9400 vs SMA50=45.2074 dist=+19.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=50.5349 SMA50=45.2074 SMA80=44.0284 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-27 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-24@38.0490; S2[2026-06-25→2026-07-28] low=2026-06-29@38.0490; S3[2026-07-29→2026-08-27] low=2026-07-30@40.5915 | lows=[38.04899748812517, 38.048999194432454, 40.59153485476492] span=6.68% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.8539327126945211 wick_frac=0.14606728730547888 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=0.042257229129280795 wick_frac=0.9577427708707192 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=37.99669421487603 need>1.4; red_wick_gt_green=False 5d trail=2026-08-21:GREEN:body=+0.0700:wick=1.4800; 2026-08-24:GREEN:body=+0.8200:wick=0.9050; 2026-08-25:GREEN:body=+0.0400:wick=1.2700; 2026-08-26:RED:body=-0.0300:wick=0.6800; 2026-08-27:GREEN:body=+1.1400:wick=0.1950 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.95 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.26 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5048.29 | **NEUTRAL** |
| `B04_income` | 179.83 | **GOOD** |
| `B05_profit_margin` | 3.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 60.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=60.94 vs prior_export=60.94 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 1.44 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.91 | **GOOD** |
| `B13_short_float` | 7.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.95 (this export) | prior_export=16.95 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TENB  ·  score **+16**  ·  Software - Infrastructure
price=37.619998931884766  pair=`2026-08-26→2026-08-27`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.77 on 2026-08-27; prev RSI=45.25 on 2026-08-26 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.25@2026-08-26 → 56.77@2026-08-27 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.25@2026-08-26 → 56.77@2026-08-27 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.25@2026-08-26 → 56.77@2026-08-27 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=2.6100 R=0.0000); 2026-08-26:GREEN:O=32.3200,C=33.6600,body=+1.3400,vol=2409400.0; 2026-08-27:GREEN:O=36.3500,C=37.6200,body=+1.2700,vol=8236800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-26 + 2026-08-27; ratio=GREEN_vol/RED_vol=99.000 (Gvol=10646200 Rvol=0); 2026-08-26:GREEN:O=32.3200,C=33.6600,body=+1.3400,vol=2409400.0; 2026-08-27:GREEN:O=36.3500,C=37.6200,body=+1.2700,vol=8236800.0 | **GOOD** |
| `A07_rvol` | RVOL=2.895 on 2026-08-27: today_vol=8236800 / avg20=2845170 (avg window 2026-07-29→2026-08-26, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.488 on 2026-08-27 (price=37.6200, mid=35.7660, upper=39.5655, lower=31.9665; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-27: price=37.6200 vs SMA50=34.9010 dist=+7.79% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-27: SMA20=35.7660 SMA50=34.9010 SMA80=31.1026 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-27 (63 bars); S1[2026-05-27→2026-06-25] low=2026-05-27@24.3000; S2[2026-06-26→2026-07-28] low=2026-06-26@27.4300; S3[2026-07-29→2026-08-27] low=2026-07-30@27.5000 | lows=[24.299999237060547, 27.43000030517578, 27.5] span=13.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: GREEN body_frac=0.5558017617706188 wick_frac=0.4441982382293812 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-26+2026-08-27: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-21:GREEN:body=+0.4400:wick=1.5000; 2026-08-24:RED:body=-0.5000:wick=1.2500; 2026-08-25:RED:body=-1.1500:wick=0.9200; 2026-08-26:GREEN:body=+1.3400:wick=0.8800; 2026-08-27:GREEN:body=+1.2700:wick=1.2300 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=8.28 (current export asof; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=7/29/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1043.54 | **NEUTRAL** |
| `B04_income` | 6.74 | **GOOD** |
| `B05_profit_margin` | 0.65 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=36.0 vs prior_export=36.0 on finviz_2026-08-27) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | 0.37 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.37 vs prior=0.37 on finviz_2026-08-27) | **NEUTRAL** |
| `B12_institutional_transactions` | -4.27 | **BAD** |
| `B13_short_float` | 9.68 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.28 (this export) | prior_export=8.28 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-27) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-28_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.