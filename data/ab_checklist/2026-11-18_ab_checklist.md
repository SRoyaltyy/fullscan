# A+B1 Feature Checklist — 2026-11-18

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,596** names
- Export: `finviz_2026-11-18.csv` · prior export for Δ: `2026-11-17`
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
| 1 | SOLS | +17 | 17 | 0 | 2026-11-17→2026-11-18 | Specialty Chemicals |
| 2 | TMO | +17 | 18 | 1 | 2026-11-17→2026-11-18 | Diagnostics & Research |
| 3 | WAT | +17 | 18 | 1 | 2026-11-17→2026-11-18 | Diagnostics & Research |
| 4 | WSM | +17 | 19 | 2 | 2026-11-17→2026-11-18 | Specialty Retail |
| 5 | LIVN | +16 | 17 | 1 | 2026-11-17→2026-11-18 | Medical Devices |
| 6 | TREX | +16 | 17 | 1 | 2026-11-17→2026-11-18 | Building Products & Equipment |
| 7 | SG | +16 | 17 | 1 | 2026-11-17→2026-11-18 | Restaurants |
| 8 | ARDT | +16 | 18 | 2 | 2026-11-17→2026-11-18 | Medical Care Facilities |
| 9 | GEHC | +16 | 17 | 1 | 2026-11-17→2026-11-18 | Medical Devices |
| 10 | STAG | +16 | 16 | 0 | 2026-11-17→2026-11-18 | REIT - Industrial |
| 11 | UPBD | +16 | 17 | 1 | 2026-11-17→2026-11-18 | Software - Application |
| 12 | BOOT | +15 | 16 | 1 | 2026-11-17→2026-11-18 | Apparel Retail |
| 13 | MDLN | +15 | 17 | 2 | 2026-11-17→2026-11-18 | Medical Instruments & Supplies |
| 14 | VNT | +15 | 16 | 1 | 2026-11-17→2026-11-18 | Scientific & Technical Instruments |
| 15 | COCO | +15 | 16 | 1 | 2026-11-17→2026-11-18 | Beverages - Non-Alcoholic |

## Full checklist — top 15

### SOLS  ·  score **+17**  ·  Specialty Chemicals
price=65.58999633789062  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.26 on 2026-11-18; prev RSI=53.91 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.91@2026-11-17 → 57.26@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.91@2026-11-17 → 57.26@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.91@2026-11-17 → 57.26@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=1.990 (G=2.0300 R=1.0200); 2026-11-17:RED:O=64.8500,C=63.8300,body=-1.0200,vol=2090700.0; 2026-11-18:GREEN:O=63.5600,C=65.5900,body=+2.0300,vol=2248800.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.076 (Gvol=2248800 Rvol=2090700); 2026-11-17:RED:O=64.8500,C=63.8300,body=-1.0200,vol=2090700.0; 2026-11-18:GREEN:O=63.5600,C=65.5900,body=+2.0300,vol=2248800.0 | **GOOD** |
| `A07_rvol` | RVOL=0.897 on 2026-11-18: today_vol=2248800 / avg20=2505917 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.515 on 2026-11-18 (price=65.5900, mid=62.1565, upper=68.8280, lower=55.4850; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=65.5900 vs SMA50=60.3524 dist=+8.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=62.1565 SMA50=60.3524 SMA80=60.3391 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-08-24@54.5600; S2[2026-09-22→2026-10-20] low=2026-10-01@54.7850; S3[2026-10-21→2026-11-18] low=2026-11-05@54.8050 | lows=[54.560001373291016, 54.78499984741211, 54.80500030517578] span=0.45% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6353670053931173 wick_frac=0.3646329946068827 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.39859218336796726 wick_frac=0.6014078166320328 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.9901976917265676 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:GREEN:body=+0.3300:wick=1.0300; 2026-11-13:RED:body=-0.7600:wick=2.1800; 2026-11-16:GREEN:body=+0.3300:wick=0.8850; 2026-11-17:RED:body=-1.0200:wick=1.5390; 2026-11-18:GREEN:body=+2.0300:wick=1.1650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.78 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.77 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4172.0 | **NEUTRAL** |
| `B04_income` | 325.0 | **GOOD** |
| `B05_profit_margin` | 7.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 78.9 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=78.9 vs prior_export=78.9 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.42 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.48 | **GOOD** |
| `B13_short_float` | 2.98 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.78 (this export) | prior_export=14.78 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.77 (this export) | prior_export=2.77 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TMO  ·  score **+17**  ·  Diagnostics & Research
price=692.739990234375  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.12 on 2026-11-18; prev RSI=48.96 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.96@2026-11-17 → 51.12@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.96@2026-11-17 → 51.12@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.96@2026-11-17 → 51.12@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=3.714 (G=11.9600 R=3.2200); 2026-11-17:RED:O=690.0100,C=686.7900,body=-3.2200,vol=1583700.0; 2026-11-18:GREEN:O=680.7800,C=692.7400,body=+11.9600,vol=2075600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.311 (Gvol=2075600 Rvol=1583700); 2026-11-17:RED:O=690.0100,C=686.7900,body=-3.2200,vol=1583700.0; 2026-11-18:GREEN:O=680.7800,C=692.7400,body=+11.9600,vol=2075600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.986 on 2026-11-18: today_vol=2075600 / avg20=2105091 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.127 on 2026-11-18 (price=692.7400, mid=696.2755, upper=724.0425, lower=668.5085; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=692.7400 vs SMA50=675.5880 dist=+2.54% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=696.2755 SMA50=675.5880 SMA80=648.0611 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-10@594.8200; S2[2026-09-22→2026-10-20] low=2026-10-08@635.3200; S3[2026-10-21→2026-11-18] low=2026-11-06@661.6000 | lows=[594.8200073242188, 635.3200073242188, 661.5999755859375] span=11.23% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.8647828024943621 wick_frac=0.1352171975056379 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.282210525189631 wick_frac=0.717789474810369 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.7142369732926435 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-9.8800:wick=5.4000; 2026-11-13:RED:body=-1.2000:wick=11.1200; 2026-11-16:RED:body=-7.7500:wick=5.8400; 2026-11-17:RED:body=-3.2200:wick=8.1900; 2026-11-18:GREEN:body=+11.9600:wick=1.8701 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=4.32 (current export asof; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.54 (current export; earnings_date=10/21/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 47351.0 | **NEUTRAL** |
| `B04_income` | 7072.0 | **GOOD** |
| `B05_profit_margin` | 14.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 766.12 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=766.12 vs prior_export=766.12 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.52 | **GOOD** |
| `B10_insider_transactions` | -10.24 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-10.24 vs prior=-10.24 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.51 | **GOOD** |
| `B13_short_float` | 1.29 | **NEUTRAL** |
| `B14_earnings_date` | 10/21/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=4.32 (this export) | prior_export=4.32 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.54 (this export) | prior_export=1.54 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WAT  ·  score **+17**  ·  Diagnostics & Research
price=466.1000061035156  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.25 on 2026-11-18; prev RSI=47.29 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.29@2026-11-17 → 55.25@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.29@2026-11-17 → 55.25@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.29@2026-11-17 → 55.25@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=5.734 (G=12.9600 R=2.2600); 2026-11-17:RED:O=452.0400,C=449.7800,body=-2.2600,vol=585000.0; 2026-11-18:GREEN:O=453.1400,C=466.1000,body=+12.9600,vol=734200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.255 (Gvol=734200 Rvol=585000); 2026-11-17:RED:O=452.0400,C=449.7800,body=-2.2600,vol=585000.0; 2026-11-18:GREEN:O=453.1400,C=466.1000,body=+12.9600,vol=734200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.752 on 2026-11-18: today_vol=734200 / avg20=975751 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.172 on 2026-11-18 (price=466.1000, mid=462.8020, upper=481.9213, lower=443.6827; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=466.1000 vs SMA50=443.2500 dist=+5.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=462.8020 SMA50=443.2500 SMA80=429.1378 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-10@392.9500; S2[2026-09-22→2026-10-20] low=2026-09-24@418.0000; S3[2026-10-21→2026-11-18] low=2026-10-21@435.0700 | lows=[392.95001220703125, 418.0, 435.07000732421875] span=10.72% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.7637000897364195 wick_frac=0.2362999102635805 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.21421918299575934 wick_frac=0.7857808170042406 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.734484714270282 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-2.8000:wick=7.3150; 2026-11-13:RED:body=-3.9200:wick=6.4000; 2026-11-16:RED:body=-1.7000:wick=6.7800; 2026-11-17:RED:body=-2.2600:wick=8.2900; 2026-11-18:GREEN:body=+12.9600:wick=4.0100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.45 (current export asof; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.75 (current export; earnings_date=11/3/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 5611.36 | **NEUTRAL** |
| `B04_income` | 98.21 | **GOOD** |
| `B05_profit_margin` | 1.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 509.76 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=509.76 vs prior_export=509.76 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | -2.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.57 vs prior=-2.57 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.08 | **GOOD** |
| `B13_short_float` | 3.39 | **NEUTRAL** |
| `B14_earnings_date` | 11/3/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.45 (this export) | prior_export=2.45 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.75 (this export) | prior_export=0.75 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### WSM  ·  score **+17**  ·  Specialty Retail
price=260.0899963378906  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.62 on 2026-11-18; prev RSI=48.47 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.47@2026-11-17 → 55.62@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.47@2026-11-17 → 55.62@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.47@2026-11-17 → 55.62@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=21.617 (G=10.1600 R=0.4700); 2026-11-17:RED:O=250.5200,C=250.0500,body=-0.4700,vol=1043100.0; 2026-11-18:GREEN:O=249.9300,C=260.0900,body=+10.1600,vol=1934200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.854 (Gvol=1934200 Rvol=1043100); 2026-11-17:RED:O=250.5200,C=250.0500,body=-0.4700,vol=1043100.0; 2026-11-18:GREEN:O=249.9300,C=260.0900,body=+10.1600,vol=1934200.0 | **GOOD** |
| `A07_rvol` | RVOL=2.053 on 2026-11-18: today_vol=1934200 / avg20=942027 (avg window 2026-10-21→2026-11-17, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.348 on 2026-11-18 (price=260.0900, mid=256.2095, upper=267.3713, lower=245.0477; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=260.0900 vs SMA50=243.4336 dist=+6.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=256.2095 SMA50=243.4336 SMA80=241.2469 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-16@216.6800; S2[2026-09-22→2026-10-20] low=2026-10-01@225.2200; S3[2026-10-21→2026-11-18] low=2026-11-13@242.8100 | lows=[216.67999267578125, 225.22000122070312, 242.80999755859375] span=12.06% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6846365343209735 wick_frac=0.31536346567902657 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.054524534447355374 wick_frac=0.9454754655526446 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=21.616972923836116 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-8.3400:wick=1.8800; 2026-11-13:GREEN:body=+9.0500:wick=2.4400; 2026-11-16:RED:body=-1.4000:wick=2.4500; 2026-11-17:RED:body=-0.4700:wick=8.1500; 2026-11-18:GREEN:body=+10.1600:wick=4.6800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=1.1 (current export asof; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.7 (current export; earnings_date=11/19/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 8005.16 | **NEUTRAL** |
| `B04_income` | 1179.08 | **GOOD** |
| `B05_profit_margin` | 14.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 264.65 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=264.65 vs prior_export=264.65 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.26 | **GOOD** |
| `B10_insider_transactions` | -5.92 | **BAD** |
| `B11_insider_tx_delta` | delta=-1.3399999999999999 (now=-5.92 vs prior=-4.58 on finviz_2026-11-17) | **BAD** |
| `B12_institutional_transactions` | 0.28 | **GOOD** |
| `B13_short_float` | 5.14 | **NEUTRAL** |
| `B14_earnings_date` | 11/19/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=1.1 (this export) | prior_export=1.1 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.7 (this export) | prior_export=1.7 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LIVN  ·  score **+16**  ·  Medical Devices
price=94.54000091552734  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.68 on 2026-11-18; prev RSI=65.74 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.74@2026-11-17 → 69.68@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.74@2026-11-17 → 69.68@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.74@2026-11-17 → 69.68@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=4.037 (G=3.2500 R=0.8050); 2026-11-17:RED:O=92.7250,C=91.9200,body=-0.8050,vol=573600.0; 2026-11-18:GREEN:O=91.2900,C=94.5400,body=+3.2500,vol=1321100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=2.303 (Gvol=1321100 Rvol=573600); 2026-11-17:RED:O=92.7250,C=91.9200,body=-0.8050,vol=573600.0; 2026-11-18:GREEN:O=91.2900,C=94.5400,body=+3.2500,vol=1321100.0 | **GOOD** |
| `A07_rvol` | RVOL=1.688 on 2026-11-18: today_vol=1321100 / avg20=782857 (avg window 2026-10-21→2026-11-17, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.658 on 2026-11-18 (price=94.5400, mid=86.7315, upper=98.6007, lower=74.8623; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=94.5400 vs SMA50=81.2808 dist=+16.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=86.7315 SMA50=81.2808 SMA80=80.8109 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-18 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-11@77.1500; S2[2026-09-17→2026-10-20] low=2026-09-24@74.1100; S3[2026-10-21→2026-11-18] low=2026-10-21@78.0050 | lows=[77.1500015258789, 74.11000061035156, 78.00499725341797] span=5.26% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6253609539902023 wick_frac=0.3746390460097977 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.4004972386176007 wick_frac=0.5995027613823992 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.037265550216561 need>1.4; red_wick_gt_green=False 5d trail=2026-11-12:RED:body=-0.7100:wick=0.7300; 2026-11-13:GREEN:body=+0.8500:wick=2.8300; 2026-11-16:GREEN:body=+1.6700:wick=2.4300; 2026-11-17:RED:body=-0.8050:wick=1.2050; 2026-11-18:GREEN:body=+3.2500:wick=1.9470 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=16.08 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.14 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1506.89 | **NEUTRAL** |
| `B04_income` | 196.05 | **GOOD** |
| `B05_profit_margin` | 13.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 108.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=108.1 vs prior_export=108.1 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.47 vs prior=-6.47 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.2 | **GOOD** |
| `B13_short_float` | 11.6 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=16.08 (this export) | prior_export=16.08 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.14 (this export) | prior_export=3.14 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### TREX  ·  score **+16**  ·  Building Products & Equipment
price=52.2599983215332  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.52 on 2026-11-18; prev RSI=49.60 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.60@2026-11-17 → 53.52@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.60@2026-11-17 → 53.52@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.60@2026-11-17 → 53.52@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=17.417 (G=2.0900 R=0.1200); 2026-11-17:RED:O=50.9200,C=50.8000,body=-0.1200,vol=1469900.0; 2026-11-18:GREEN:O=50.1700,C=52.2600,body=+2.0900,vol=2144300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.459 (Gvol=2144300 Rvol=1469900); 2026-11-17:RED:O=50.9200,C=50.8000,body=-0.1200,vol=1469900.0; 2026-11-18:GREEN:O=50.1700,C=52.2600,body=+2.0900,vol=2144300.0 | **GOOD** |
| `A07_rvol` | RVOL=0.864 on 2026-11-18: today_vol=2144300 / avg20=2481466 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.030 on 2026-11-18 (price=52.2600, mid=52.4185, upper=57.7410, lower=47.0960; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=52.2600 vs SMA50=47.6922 dist=+9.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=52.4185 SMA50=47.6922 SMA80=47.2350 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-18 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-16@42.2500; S2[2026-09-17→2026-10-20] low=2026-10-01@42.1000; S3[2026-10-21→2026-11-18] low=2026-10-21@48.3100 | lows=[42.25, 42.099998474121094, 48.310001373291016] span=14.75% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.8600822593051914 wick_frac=0.13991774069480856 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.060605535165544414 wick_frac=0.9393944648344555 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=17.41682296468195 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-1.5300:wick=0.4350; 2026-11-13:RED:body=-0.5200:wick=1.1700; 2026-11-16:RED:body=-1.2400:wick=0.6000; 2026-11-17:RED:body=-0.1200:wick=1.8600; 2026-11-18:GREEN:body=+2.0900:wick=0.3400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.89 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=14.18 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1282.05 | **NEUTRAL** |
| `B04_income` | 178.07 | **GOOD** |
| `B05_profit_margin` | 13.89 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 63.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=63.94 vs prior_export=63.94 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.95 | **GOOD** |
| `B10_insider_transactions` | -0.69 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.69 vs prior=-0.69 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.44 | **GOOD** |
| `B13_short_float` | 9.57 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.89 (this export) | prior_export=14.89 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=14.18 (this export) | prior_export=14.18 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SG  ·  score **+16**  ·  Restaurants
price=13.0600004196167  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.17 on 2026-11-18; prev RSI=61.45 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.45@2026-11-17 → 68.17@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.45@2026-11-17 → 68.17@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.45@2026-11-17 → 68.17@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1100 R=0.0000); 2026-11-17:GREEN:O=11.9500,C=12.1800,body=+0.2300,vol=3226500.0; 2026-11-18:GREEN:O=12.1800,C=13.0600,body=+0.8800,vol=6045600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=99.000 (Gvol=9272100 Rvol=0); 2026-11-17:GREEN:O=11.9500,C=12.1800,body=+0.2300,vol=3226500.0; 2026-11-18:GREEN:O=12.1800,C=13.0600,body=+0.8800,vol=6045600.0 | **GOOD** |
| `A07_rvol` | RVOL=1.219 on 2026-11-18: today_vol=6045600 / avg20=4957474 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.648 on 2026-11-18 (price=13.0600, mid=11.6830, upper=13.8088, lower=9.5572; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=13.0600 vs SMA50=9.8706 dist=+32.31% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=11.6830 SMA50=9.8706 SMA80=8.5556 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-02@6.3800; S2[2026-09-22→2026-10-20] low=2026-09-23@7.4000; S3[2026-10-21→2026-11-18] low=2026-10-26@9.5400 | lows=[6.380000114440918, 7.400000095367432, 9.539999961853027] span=49.53% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.5742758177485418 wick_frac=0.4257241822514583 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-0.3600:wick=0.5288; 2026-11-13:RED:body=-0.0600:wick=0.6250; 2026-11-16:RED:body=-0.0500:wick=0.8590; 2026-11-17:GREEN:body=+0.2300:wick=0.3700; 2026-11-18:GREEN:body=+0.8800:wick=0.2700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=8.78 (current export asof; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.5 (current export; earnings_date=11/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 685.39 | **NEUTRAL** |
| `B04_income` | 19.07 | **GOOD** |
| `B05_profit_margin` | 2.78 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.18 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.18 vs prior_export=12.18 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | -0.13 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.13 vs prior=-0.13 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.27 | **GOOD** |
| `B13_short_float` | 21.72 | **GOOD** |
| `B14_earnings_date` | 11/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=8.78 (this export) | prior_export=8.78 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.5 (this export) | prior_export=3.5 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ARDT  ·  score **+16**  ·  Medical Care Facilities
price=14.399999618530273  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.17 on 2026-11-18; prev RSI=76.89 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 76.89@2026-11-17 → 49.17@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_down | RSI 76.89@2026-11-17 → 49.17@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **BAD** |
| `A04_rsi_cross_70` | cross_down | RSI 76.89@2026-11-17 → 49.17@2026-11-18 vs 70 | rule: cross_down=BAD | **BAD** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=1.912 (G=0.6500 R=0.3400); 2026-11-17:RED:O=18.0900,C=17.7500,body=-0.3400,vol=471200.0; 2026-11-18:GREEN:O=13.7500,C=14.4000,body=+0.6500,vol=7269200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=15.427 (Gvol=7269200 Rvol=471200); 2026-11-17:RED:O=18.0900,C=17.7500,body=-0.3400,vol=471200.0; 2026-11-18:GREEN:O=13.7500,C=14.4000,body=+0.6500,vol=7269200.0 | **GOOD** |
| `A07_rvol` | RVOL=8.942 on 2026-11-18: today_vol=7269200 / avg20=812899 (avg window 2026-10-21→2026-11-17, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=-0.057 on 2026-11-18 (price=14.4000, mid=14.7175, upper=20.3024, lower=9.1326; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=14.4000 vs SMA50=12.5006 dist=+15.19% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=14.7175 SMA50=12.5006 SMA80=11.5837 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-11-18 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-14@9.0100; S2[2026-08-13→2026-09-11] low=2026-08-21@10.3500; S3[2026-10-21→2026-11-18] low=2026-11-03@11.5500 | lows=[9.010000228881836, 10.350000381469727, 11.550000190734863] span=28.19% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6074764522592284 wick_frac=0.39252354774077164 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.49275482504878954 wick_frac=0.5072451749512105 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.911762725936564 need>1.4; red_wick_gt_green=False 5d trail=2026-11-12:RED:body=-0.6300:wick=0.2100; 2026-11-13:RED:body=-0.2700:wick=0.5599; 2026-11-16:GREEN:body=+0.0500:wick=0.4500; 2026-11-17:RED:body=-0.3400:wick=0.3500; 2026-11-18:GREEN:body=+0.6500:wick=0.4200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=33.62 (current export asof; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.31 (current export; earnings_date=11/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 6454.05 | **NEUTRAL** |
| `B04_income` | 146.22 | **GOOD** |
| `B05_profit_margin` | 2.27 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.77 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.77 vs prior_export=17.77 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.92 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.78 | **GOOD** |
| `B13_short_float` | 15.69 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=33.62 (this export) | prior_export=33.62 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.31 (this export) | prior_export=1.31 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GEHC  ·  score **+16**  ·  Medical Devices
price=83.66000366210938  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.17 on 2026-11-18; prev RSI=63.45 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.45@2026-11-17 → 67.17@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.45@2026-11-17 → 67.17@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.45@2026-11-17 → 67.17@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=3.615 (G=2.3500 R=0.6500); 2026-11-17:RED:O=82.4900,C=81.8400,body=-0.6500,vol=2920200.0; 2026-11-18:GREEN:O=81.3100,C=83.6600,body=+2.3500,vol=3651200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.250 (Gvol=3651200 Rvol=2920200); 2026-11-17:RED:O=82.4900,C=81.8400,body=-0.6500,vol=2920200.0; 2026-11-18:GREEN:O=81.3100,C=83.6600,body=+2.3500,vol=3651200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.842 on 2026-11-18: today_vol=3651200 / avg20=4338091 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.422 on 2026-11-18 (price=83.6600, mid=79.5260, upper=89.3253, lower=69.7267; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=83.6600 vs SMA50=71.5039 dist=+17.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=79.5260 SMA50=71.5039 SMA80=71.4958 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-17@62.9950; S2[2026-09-22→2026-10-20] low=2026-10-08@63.3150; S3[2026-10-21→2026-11-18] low=2026-10-21@68.7100 | lows=[62.994998931884766, 63.314998626708984, 68.70999908447266] span=9.07% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.8216802804202035 wick_frac=0.1783197195797965 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.34946307128149173 wick_frac=0.6505369287185082 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.615385518269423 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-0.9500:wick=0.4300; 2026-11-13:GREEN:body=+0.5900:wick=0.6500; 2026-11-16:RED:body=-0.5000:wick=0.8800; 2026-11-17:RED:body=-0.6500:wick=1.2100; 2026-11-18:GREEN:body=+2.3500:wick=0.5100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.28 (current export asof; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.02 (current export; earnings_date=10/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 21637.0 | **NEUTRAL** |
| `B04_income` | 2154.0 | **GOOD** |
| `B05_profit_margin` | 9.96 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 87.84 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=87.84 vs prior_export=87.84 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | 0.7 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.7 vs prior=0.7 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.3 | **BAD** |
| `B13_short_float` | 4.05 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.28 (this export) | prior_export=11.28 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.02 (this export) | prior_export=3.02 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### STAG  ·  score **+16**  ·  REIT - Industrial
price=36.68000030517578  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=48.97 on 2026-11-18; prev RSI=42.85 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 42.85@2026-11-17 → 48.97@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 42.85@2026-11-17 → 48.97@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 42.85@2026-11-17 → 48.97@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=5.571 (G=0.7800 R=0.1400); 2026-11-17:RED:O=36.3000,C=36.1600,body=-0.1400,vol=856700.0; 2026-11-18:GREEN:O=35.9000,C=36.6800,body=+0.7800,vol=1215000.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.418 (Gvol=1215000 Rvol=856700); 2026-11-17:RED:O=36.3000,C=36.1600,body=-0.1400,vol=856700.0; 2026-11-18:GREEN:O=35.9000,C=36.6800,body=+0.7800,vol=1215000.0 | **GOOD** |
| `A07_rvol` | RVOL=0.903 on 2026-11-18: today_vol=1215000 / avg20=1346162 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.193 on 2026-11-18 (price=36.6800, mid=36.8690, upper=37.8504, lower=35.8876; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=36.6800 vs SMA50=36.6670 dist=+0.04% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.87_50=36.67_80=36.93 on 2026-11-18: SMA20=36.8690 SMA50=36.6670 SMA80=36.9284 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-08-21@36.5400; S2[2026-09-22→2026-10-20] low=2026-10-08@35.1400; S3[2026-10-21→2026-11-18] low=2026-11-18@35.8800 | lows=[36.540000915527344, 35.13999938964844, 35.880001068115234] span=3.98% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.9512183775435201 wick_frac=0.04878162245647987 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.24561309579579982 wick_frac=0.7543869042042002 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.571444141689374 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-0.2600:wick=0.1150; 2026-11-13:RED:body=-0.3300:wick=0.0800; 2026-11-16:RED:body=-0.5500:wick=0.2400; 2026-11-17:RED:body=-0.1400:wick=0.4300; 2026-11-18:GREEN:body=+0.7800:wick=0.0400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=35.98 (current export asof; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.68 (current export; earnings_date=10/27/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 900.52 | **NEUTRAL** |
| `B04_income` | 268.95 | **GOOD** |
| `B05_profit_margin` | 29.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=41.25 vs prior_export=41.25 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.5 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.63 | **GOOD** |
| `B13_short_float` | 3.89 | **NEUTRAL** |
| `B14_earnings_date` | 10/27/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=35.98 (this export) | prior_export=35.98 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.68 (this export) | prior_export=1.68 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### UPBD  ·  score **+16**  ·  Software - Application
price=26.3799991607666  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=75.07 on 2026-11-18; prev RSI=70.02 on 2026-11-17 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 70.02@2026-11-17 → 75.07@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.02@2026-11-17 → 75.07@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 70.02@2026-11-17 → 75.07@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=54.665 (G=1.6400 R=0.0300); 2026-11-17:RED:O=24.7500,C=24.7200,body=-0.0300,vol=1155300.0; 2026-11-18:GREEN:O=24.7400,C=26.3800,body=+1.6400,vol=1353200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.171 (Gvol=1353200 Rvol=1155300); 2026-11-17:RED:O=24.7500,C=24.7200,body=-0.0300,vol=1155300.0; 2026-11-18:GREEN:O=24.7400,C=26.3800,body=+1.6400,vol=1353200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.857 on 2026-11-18: today_vol=1353200 / avg20=1578336 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.615 on 2026-11-18 (price=26.3800, mid=22.2070, upper=28.9974, lower=15.4166; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=26.3800 vs SMA50=18.6973 dist=+41.09% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=22.21_50=18.70_80=18.95 on 2026-11-18: SMA20=22.2070 SMA50=18.6973 SMA80=18.9514 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-21@16.8600; S2[2026-09-22→2026-10-20] low=2026-10-07@14.9800; S3[2026-10-21→2026-11-18] low=2026-10-21@15.8900 | lows=[16.860000610351562, 14.979999542236328, 15.890000343322754] span=12.55% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.8770055221344365 wick_frac=0.12299447786556346 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.027523610039319444 wick_frac=0.9724763899606805 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=54.66539513001462 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-1.4000:wick=0.2800; 2026-11-13:GREEN:body=+1.2600:wick=0.2600; 2026-11-16:RED:body=-0.4000:wick=0.3700; 2026-11-17:RED:body=-0.0300:wick=1.0600; 2026-11-18:GREEN:body=+1.6400:wick=0.2300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.29 (current export asof; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.99 (current export; earnings_date=10/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 4725.76 | **NEUTRAL** |
| `B04_income` | 111.39 | **GOOD** |
| `B05_profit_margin` | 2.36 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 27.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=27.33 vs prior_export=27.33 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 3.1 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0900000000000003 (now=3.1 vs prior=3.01 on finviz_2026-11-17) | **GOOD** |
| `B12_institutional_transactions` | 7.83 | **GOOD** |
| `B13_short_float` | 14.95 | **NEUTRAL** |
| `B14_earnings_date` | 10/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.29 (this export) | prior_export=11.29 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.99 (this export) | prior_export=1.99 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### BOOT  ·  score **+15**  ·  Apparel Retail
price=141.49000549316406  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.58 on 2026-11-18; prev RSI=52.36 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.36@2026-11-17 → 59.58@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.36@2026-11-17 → 59.58@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.36@2026-11-17 → 59.58@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=5.516 (G=6.9500 R=1.2600); 2026-11-17:RED:O=134.3200,C=133.0600,body=-1.2600,vol=775000.0; 2026-11-18:GREEN:O=134.5400,C=141.4900,body=+6.9500,vol=880200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.136 (Gvol=880200 Rvol=775000); 2026-11-17:RED:O=134.3200,C=133.0600,body=-1.2600,vol=775000.0; 2026-11-18:GREEN:O=134.5400,C=141.4900,body=+6.9500,vol=880200.0 | **GOOD** |
| `A07_rvol` | RVOL=0.935 on 2026-11-18: today_vol=880200 / avg20=941815 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.554 on 2026-11-18 (price=141.4900, mid=130.4350, upper=150.3769, lower=110.4931; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=141.4900 vs SMA50=128.5152 dist=+10.10% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=130.44_50=128.52_80=139.73 on 2026-11-18: SMA20=130.4350 SMA50=128.5152 SMA80=139.7295 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-18 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-16@122.4600; S2[2026-09-17→2026-10-20] low=2026-10-20@112.2900; S3[2026-10-21→2026-11-18] low=2026-10-27@110.9050 | lows=[122.45999908447266, 112.29000091552734, 110.90499877929688] span=10.42% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.847562779705803 wick_frac=0.152437220294197 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.14702577972128203 wick_frac=0.8529742202787179 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.515839953497384 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-2.7000:wick=0.6050; 2026-11-13:RED:body=-0.5400:wick=3.6200; 2026-11-16:RED:body=-3.3700:wick=4.2700; 2026-11-17:RED:body=-1.2600:wick=7.3100; 2026-11-18:GREEN:body=+6.9500:wick=1.2500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.78 (current export asof; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.17 (current export; earnings_date=10/28/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 2419.72 | **NEUTRAL** |
| `B04_income` | 252.86 | **GOOD** |
| `B05_profit_margin` | 10.45 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 180.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=180.67 vs prior_export=180.67 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.22 | **GOOD** |
| `B10_insider_transactions` | 0.76 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.76 vs prior=0.76 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.16 | **BAD** |
| `B13_short_float` | 6.78 | **NEUTRAL** |
| `B14_earnings_date` | 10/28/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.78 (this export) | prior_export=5.78 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.17 (this export) | prior_export=0.17 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MDLN  ·  score **+15**  ·  Medical Instruments & Supplies
price=39.15999984741211  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.49 on 2026-11-18; prev RSI=52.22 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.22@2026-11-17 → 57.49@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.22@2026-11-17 → 57.49@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.22@2026-11-17 → 57.49@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=1.564 (G=2.3300 R=1.4900); 2026-11-17:RED:O=39.1700,C=37.6800,body=-1.4900,vol=9072900.0; 2026-11-18:GREEN:O=36.8300,C=39.1600,body=+2.3300,vol=40688500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=4.485 (Gvol=40688500 Rvol=9072900); 2026-11-17:RED:O=39.1700,C=37.6800,body=-1.4900,vol=9072900.0; 2026-11-18:GREEN:O=36.8300,C=39.1600,body=+2.3300,vol=40688500.0 | **GOOD** |
| `A07_rvol` | RVOL=3.722 on 2026-11-18: today_vol=40688500 / avg20=10931916 (avg window 2026-10-21→2026-11-17, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.315 on 2026-11-18 (price=39.1600, mid=36.9120, upper=44.0422, lower=29.7818; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=39.1600 vs SMA50=35.4354 dist=+10.51% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=36.91_50=35.44_80=35.75 on 2026-11-18: SMA20=36.9120 SMA50=35.4354 SMA80=35.7472 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-18 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-15@31.6250; S2[2026-09-17→2026-10-20] low=2026-09-18@31.4900; S3[2026-10-21→2026-11-18] low=2026-10-30@31.3700 | lows=[31.625, 31.489999771118164, 31.3700008392334] span=0.81% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.8759391541410262 wick_frac=0.12406084585897378 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.7866948640483383 wick_frac=0.21330513595166165 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.5637592999380432 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-2.0200:wick=0.2800; 2026-11-13:RED:body=-0.7500:wick=0.8700; 2026-11-16:GREEN:body=+0.8950:wick=0.4500; 2026-11-17:RED:body=-1.4900:wick=0.4040; 2026-11-18:GREEN:body=+2.3300:wick=0.3300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=28.21 (current export asof; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.74 (current export; earnings_date=11/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 30680.0 | **NEUTRAL** |
| `B04_income` | 569.0 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.35999999999999943 (now=45.57 vs prior_export=45.21 on finviz_2026-11-17) | **GOOD** |
| `B09_analyst_recom` | 1.56 | **GOOD** |
| `B10_insider_transactions` | -19.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-19.27 vs prior=-19.27 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.36 | **GOOD** |
| `B13_short_float` | 7.41 | **NEUTRAL** |
| `B14_earnings_date` | 11/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=28.21 (this export) | prior_export=28.21 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.74 (this export) | prior_export=1.74 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VNT  ·  score **+15**  ·  Scientific & Technical Instruments
price=43.959999084472656  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=78.11 on 2026-11-18; prev RSI=76.12 on 2026-11-17 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.12@2026-11-17 → 78.11@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.12@2026-11-17 → 78.11@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.12@2026-11-17 → 78.11@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=6.187 (G=0.9900 R=0.1600); 2026-11-17:RED:O=43.3100,C=43.1500,body=-0.1600,vol=1269100.0; 2026-11-18:GREEN:O=42.9700,C=43.9600,body=+0.9900,vol=1630100.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.284 (Gvol=1630100 Rvol=1269100); 2026-11-17:RED:O=43.3100,C=43.1500,body=-0.1600,vol=1269100.0; 2026-11-18:GREEN:O=42.9700,C=43.9600,body=+0.9900,vol=1630100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.922 on 2026-11-18: today_vol=1630100 / avg20=1768745 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.583 on 2026-11-18 (price=43.9600, mid=39.3020, upper=47.2910, lower=31.3130; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=43.9600 vs SMA50=34.9590 dist=+25.75% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-11-18: SMA20=39.3020 SMA50=34.9590 SMA80=34.1871 | **GOOD** |
| `A11_three_section_lows` | window=2026-08-18→2026-11-18 (63 bars); S1[2026-08-18→2026-09-16] low=2026-09-10@31.1100; S2[2026-09-17→2026-10-20] low=2026-09-24@30.4250; S3[2026-10-21→2026-11-18] low=2026-10-22@33.2000 | lows=[31.110000610351562, 30.424999237060547, 33.20000076293945] span=9.12% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6226401061397426 wick_frac=0.3773598938602573 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.17582403762717405 wick_frac=0.824175962372826 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.187492549412298 need>1.4; red_wick_gt_green=False 5d trail=2026-11-12:RED:body=-0.5800:wick=0.1500; 2026-11-13:GREEN:body=+0.1800:wick=0.4100; 2026-11-16:RED:body=-0.0500:wick=0.6550; 2026-11-17:RED:body=-0.1600:wick=0.7500; 2026-11-18:GREEN:body=+0.9900:wick=0.6000 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=9.07 (current export asof; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.23 (current export; earnings_date=11/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3052.7 | **NEUTRAL** |
| `B04_income` | 344.2 | **GOOD** |
| `B05_profit_margin` | 11.28 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 46.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=46.56 vs prior_export=46.56 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 6.48 | **GOOD** |
| `B13_short_float` | 9.18 | **NEUTRAL** |
| `B14_earnings_date` | 11/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=9.07 (this export) | prior_export=9.07 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.23 (this export) | prior_export=1.23 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

### COCO  ·  score **+15**  ·  Beverages - Non-Alcoholic
price=67.6050033569336  pair=`2026-11-17→2026-11-18`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.18 on 2026-11-18; prev RSI=57.32 on 2026-11-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.32@2026-11-17 → 64.18@2026-11-18 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.32@2026-11-17 → 64.18@2026-11-18 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.32@2026-11-17 → 64.18@2026-11-18 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_body_sum/RED_body_sum=15.340 (G=4.2950 R=0.2800); 2026-11-17:RED:O=64.2700,C=63.9900,body=-0.2800,vol=1648900.0; 2026-11-18:GREEN:O=63.3100,C=67.6050,body=+4.2950,vol=1790300.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-11-17 + 2026-11-18; ratio=GREEN_vol/RED_vol=1.086 (Gvol=1790300 Rvol=1648900); 2026-11-17:RED:O=64.2700,C=63.9900,body=-0.2800,vol=1648900.0; 2026-11-18:GREEN:O=63.3100,C=67.6050,body=+4.2950,vol=1790300.0 | **GOOD** |
| `A07_rvol` | RVOL=1.306 on 2026-11-18: today_vol=1790300 / avg20=1371076 (avg window 2026-10-21→2026-11-17, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.775 on 2026-11-18 (price=67.6050, mid=61.3055, upper=69.4296, lower=53.1814; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-11-18: price=67.6050 vs SMA50=58.6983 dist=+15.17% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=61.31_50=58.70_80=60.12 on 2026-11-18: SMA20=61.3055 SMA50=58.6983 SMA80=60.1214 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-08-21→2026-11-18 (63 bars); S1[2026-08-21→2026-09-21] low=2026-09-10@50.6200; S2[2026-09-22→2026-10-20] low=2026-10-07@50.4600; S3[2026-10-21→2026-11-18] low=2026-10-26@54.0500 | lows=[50.619998931884766, 50.459999084472656, 54.04999923706055] span=7.11% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: GREEN body_frac=0.6972405990313394 wick_frac=0.30275940096866066 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-11-17+2026-11-18: RED body_frac=0.13207316305139946 wick_frac=0.8679268369486005 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=15.339568658973555 need>1.4; red_wick_gt_green=True 5d trail=2026-11-12:RED:body=-0.3300:wick=2.1700; 2026-11-13:RED:body=-1.6500:wick=1.0600; 2026-11-16:GREEN:body=+1.4600:wick=0.7500; 2026-11-17:RED:body=-0.2800:wick=1.8400; 2026-11-18:GREEN:body=+4.2950:wick=1.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=24.28 (current export asof; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.97 (current export; earnings_date=10/22/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 767.53 | **NEUTRAL** |
| `B04_income` | 120.47 | **GOOD** |
| `B05_profit_margin` | 15.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 79.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=79.1 vs prior_export=79.1 on finviz_2026-11-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -4.19 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.19 vs prior=-4.19 on finviz_2026-11-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.72 | **GOOD** |
| `B13_short_float` | 14.77 | **NEUTRAL** |
| `B14_earnings_date` | 10/22/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=24.28 (this export) | prior_export=24.28 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.97 (this export) | prior_export=5.97 (finviz_2026-11-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-11-18_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.