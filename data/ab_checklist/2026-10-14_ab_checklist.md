# A+B1 Feature Checklist — 2026-10-14

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,629** names
- Export: `finviz_2026-10-14.csv` · prior export for Δ: `2026-10-13`
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
| 1 | ARHS | +17 | 17 | 0 | 2026-10-12→2026-10-13 | Specialty Retail |
| 2 | ETN | +17 | 18 | 1 | 2026-10-12→2026-10-13 | Specialty Industrial Machinery |
| 3 | CDNA | +16 | 17 | 1 | 2026-10-12→2026-10-13 | Diagnostics & Research |
| 4 | AMD | +16 | 17 | 1 | 2026-10-12→2026-10-13 | Semiconductors |
| 5 | PBR-A | +16 | 17 | 1 | 2026-10-12→2026-10-13 | Oil & Gas Integrated |
| 6 | PBR | +16 | 18 | 2 | 2026-10-12→2026-10-13 | Oil & Gas Integrated |
| 7 | ELV | +16 | 18 | 2 | 2026-10-12→2026-10-13 | Healthcare Plans |
| 8 | CNP | +15 | 17 | 2 | 2026-10-12→2026-10-13 | Utilities - Regulated Electric |
| 9 | AVT | +15 | 17 | 2 | 2026-10-12→2026-10-13 | Electronics & Computer Distribution |
| 10 | SN | +15 | 16 | 1 | 2026-10-12→2026-10-13 | Furnishings, Fixtures & Appliances |
| 11 | YPF | +15 | 16 | 1 | 2026-10-12→2026-10-13 | Oil & Gas Integrated |
| 12 | CRSR | +15 | 17 | 2 | 2026-10-12→2026-10-13 | Computer Hardware |
| 13 | DINO | +15 | 17 | 2 | 2026-10-12→2026-10-13 | Oil & Gas Refining & Marketing |
| 14 | RNW | +15 | 16 | 1 | 2026-10-12→2026-10-13 | Utilities - Renewable |
| 15 | CMBT | +14 | 15 | 1 | 2026-10-12→2026-10-13 | Oil & Gas Midstream |

## Full checklist — top 15

### ARHS  ·  score **+17**  ·  Specialty Retail
price=9.600000381469727  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.53 on 2026-10-13; prev RSI=54.04 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.04@2026-10-12 → 55.53@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.04@2026-10-12 → 55.53@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.04@2026-10-12 → 55.53@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=7.571 (G=0.3180 R=0.0420); 2026-10-12:RED:O=9.5420,C=9.5000,body=-0.0420,vol=713858.0; 2026-10-13:GREEN:O=9.2820,C=9.6000,body=+0.3180,vol=1180919.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.654 (Gvol=1180919 Rvol=713858); 2026-10-12:RED:O=9.5420,C=9.5000,body=-0.0420,vol=713858.0; 2026-10-13:GREEN:O=9.2820,C=9.6000,body=+0.3180,vol=1180919.0 | **GOOD** |
| `A07_rvol` | RVOL=0.727 on 2026-10-13: today_vol=1180919 / avg20=1624575 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.182 on 2026-10-13 (price=9.6000, mid=9.2940, upper=10.9738, lower=7.6142; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=9.6000 vs SMA50=9.0880 dist=+5.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=9.2940 SMA50=9.0880 SMA80=8.6060 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-13 (63 bars); S1[2026-07-14→2026-08-13] low=2026-07-23@7.0600; S2[2026-08-14→2026-09-14] low=2026-09-10@7.8850; S3[2026-09-15→2026-10-13] low=2026-09-16@7.0670 | lows=[7.059999942779541, 7.885000228881836, 7.066999912261963] span=11.69% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.6516398087964573 wick_frac=0.34836019120354267 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.13548309691471394 wick_frac=0.8645169030852861 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.571480472297911 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.1200:wick=0.1400; 2026-10-08:GREEN:body=+0.0400:wick=0.2200; 2026-10-09:RED:body=-0.0600:wick=0.2600; 2026-10-12:RED:body=-0.0420:wick=0.2680; 2026-10-13:GREEN:body=+0.3180:wick=0.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=72.52 (current export asof; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.26 (current export; earnings_date=8/6/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1408.59 | **NEUTRAL** |
| `B04_income` | 69.18 | **GOOD** |
| `B05_profit_margin` | 4.91 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.54 vs prior_export=10.54 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.13 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.06 | **GOOD** |
| `B13_short_float` | 12.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=72.52 (this export) | prior_export=72.52 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.26 (this export) | prior_export=5.26 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ETN  ·  score **+17**  ·  Specialty Industrial Machinery
price=428.30999755859375  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.69 on 2026-10-13; prev RSI=49.82 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.82@2026-10-12 → 50.69@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.82@2026-10-12 → 50.69@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.82@2026-10-12 → 50.69@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=7.536 (G=7.3100 R=0.9700); 2026-10-12:RED:O=427.6600,C=426.6900,body=-0.9700,vol=1292386.0; 2026-10-13:GREEN:O=421.0000,C=428.3100,body=+7.3100,vol=1762784.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.364 (Gvol=1762784 Rvol=1292386); 2026-10-12:RED:O=427.6600,C=426.6900,body=-0.9700,vol=1292386.0; 2026-10-13:GREEN:O=421.0000,C=428.3100,body=+7.3100,vol=1762784.0 | **GOOD** |
| `A07_rvol` | RVOL=0.877 on 2026-10-13: today_vol=1762784 / avg20=2009022 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.110 on 2026-10-13 (price=428.3100, mid=430.7325, upper=452.8355, lower=408.6295; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=428.3100 vs SMA50=426.4038 dist=+0.45% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=430.7325 SMA50=426.4038 SMA80=418.6504 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-13 (63 bars); S1[2026-07-14→2026-08-13] low=2026-07-29@357.2510; S2[2026-08-14→2026-09-14] low=2026-09-03@384.7700; S3[2026-09-15→2026-10-13] low=2026-09-15@389.4000 | lows=[357.2510251368342, 384.7699890136719, 389.3999938964844] span=9.00% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.523639114115263 wick_frac=0.4763608858847371 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.10430100117147892 wick_frac=0.8956989988285211 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.536070473493786 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-3.7700:wick=11.8952; 2026-10-08:RED:body=-2.2900:wick=15.3000; 2026-10-09:GREEN:body=+4.5200:wick=6.6000; 2026-10-12:RED:body=-0.9700:wick=8.3300; 2026-10-13:GREEN:body=+7.3100:wick=6.6500 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.53 (current export asof; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=4.61 (current export; earnings_date=7/31/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 30025.0 | **NEUTRAL** |
| `B04_income` | 3829.0 | **GOOD** |
| `B05_profit_margin` | 12.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 487.72 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=487.72 vs prior_export=487.72 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.62 | **GOOD** |
| `B10_insider_transactions` | -4.71 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-4.71 vs prior=-4.71 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.15 | **GOOD** |
| `B13_short_float` | 2.0 | **NEUTRAL** |
| `B14_earnings_date` | 7/31/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.53 (this export) | prior_export=2.53 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=4.61 (this export) | prior_export=4.61 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CDNA  ·  score **+16**  ·  Diagnostics & Research
price=65.43000030517578  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.26 on 2026-10-13; prev RSI=60.15 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.15@2026-10-12 → 64.26@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.15@2026-10-12 → 64.26@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.15@2026-10-12 → 64.26@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=17.875 (G=2.8600 R=0.1600); 2026-10-12:RED:O=63.2800,C=63.1200,body=-0.1600,vol=898940.0; 2026-10-13:GREEN:O=62.5700,C=65.4300,body=+2.8600,vol=1497698.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.666 (Gvol=1497698 Rvol=898940); 2026-10-12:RED:O=63.2800,C=63.1200,body=-0.1600,vol=898940.0; 2026-10-13:GREEN:O=62.5700,C=65.4300,body=+2.8600,vol=1497698.0 | **GOOD** |
| `A07_rvol` | RVOL=1.003 on 2026-10-13: today_vol=1497698 / avg20=1492654 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.387 on 2026-10-13 (price=65.4300, mid=61.4973, upper=71.6565, lower=51.3380; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=65.4300 vs SMA50=53.7895 dist=+21.64% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=61.4973 SMA50=53.7895 SMA80=45.6209 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-13 (63 bars); S1[2026-07-14→2026-08-13] low=2026-07-14@28.2500; S2[2026-08-14→2026-09-14] low=2026-08-18@44.8000; S3[2026-09-15→2026-10-13] low=2026-09-15@51.7300 | lows=[28.25, 44.79999923706055, 51.72999954223633] span=83.12% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.5569619124737206 wick_frac=0.44303808752627943 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.05161281589669315 wick_frac=0.9483871841033068 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=17.875020861645567 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+1.6580:wick=2.0599; 2026-10-08:GREEN:body=+0.3200:wick=1.9400; 2026-10-09:GREEN:body=+1.4600:wick=1.4600; 2026-10-12:RED:body=-0.1600:wick=2.9400; 2026-10-13:GREEN:body=+2.8600:wick=2.2750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=59.41 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=15.69 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 458.09 | **NEUTRAL** |
| `B04_income` | 111.01 | **GOOD** |
| `B05_profit_margin` | 24.23 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 56.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=56.14 vs prior_export=56.14 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.75 | **GOOD** |
| `B10_insider_transactions` | -6.99 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.99 vs prior=-6.99 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.52 | **GOOD** |
| `B13_short_float` | 13.38 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=59.41 (this export) | prior_export=59.41 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=15.69 (this export) | prior_export=15.69 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AMD  ·  score **+16**  ·  Semiconductors
price=605.530029296875  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.83 on 2026-10-13; prev RSI=52.98 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 52.98@2026-10-12 → 55.83@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 52.98@2026-10-12 → 55.83@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 52.98@2026-10-12 → 55.83@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=4.988 (G=28.0341 R=5.6200); 2026-10-12:RED:O=599.8600,C=594.2400,body=-5.6200,vol=15252527.0; 2026-10-13:GREEN:O=577.4960,C=605.5300,body=+28.0341,vol=17775022.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.165 (Gvol=17775022 Rvol=15252527); 2026-10-12:RED:O=599.8600,C=594.2400,body=-5.6200,vol=15252527.0; 2026-10-13:GREEN:O=577.4960,C=605.5300,body=+28.0341,vol=17775022.0 | **GOOD** |
| `A07_rvol` | RVOL=0.825 on 2026-10-13: today_vol=17775022 / avg20=21557219 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.039 on 2026-10-13 (price=605.5300, mid=608.1810, upper=675.5001, lower=540.8619; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=605.5300 vs SMA50=533.7006 dist=+13.46% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=608.1810 SMA50=533.7006 SMA80=528.6915 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-13 (63 bars); S1[2026-07-16→2026-08-13] low=2026-07-29@424.0300; S2[2026-08-14→2026-09-14] low=2026-09-03@440.5000; S3[2026-09-15→2026-10-13] low=2026-09-15@497.5100 | lows=[424.0299987792969, 440.5, 497.510009765625] span=17.33% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.7874727185133498 wick_frac=0.2125272814866503 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.3690072095828541 wick_frac=0.630992790417146 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.988270813875193 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+11.0900:wick=4.2300; 2026-10-08:RED:body=-18.6800:wick=11.8300; 2026-10-09:RED:body=-18.7900:wick=1.7000; 2026-10-12:RED:body=-5.6200:wick=9.6100; 2026-10-13:GREEN:body=+28.0341:wick=7.5660 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.71 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.0 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 41305.0 | **NEUTRAL** |
| `B04_income` | 6434.0 | **GOOD** |
| `B05_profit_margin` | 15.58 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 635.54 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=635.54 vs prior_export=635.54 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.49 | **GOOD** |
| `B10_insider_transactions` | -6.76 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.76 vs prior=-6.76 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 3.6 | **GOOD** |
| `B13_short_float` | 2.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.71 (this export) | prior_export=2.71 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.0 (this export) | prior_export=2.0 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR-A  ·  score **+16**  ·  Oil & Gas Integrated
price=22.610000610351562  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=77.43 on 2026-10-13; prev RSI=77.58 on 2026-10-12 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 77.58@2026-10-12 → 77.43@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 77.58@2026-10-12 → 77.43@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 77.58@2026-10-12 → 77.43@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=2.500 (G=0.2000 R=0.0800); 2026-10-12:RED:O=22.7000,C=22.6200,body=-0.0800,vol=4932988.0; 2026-10-13:GREEN:O=22.4100,C=22.6100,body=+0.2000,vol=8034907.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.629 (Gvol=8034907 Rvol=4932988); 2026-10-12:RED:O=22.7000,C=22.6200,body=-0.0800,vol=4932988.0; 2026-10-13:GREEN:O=22.4100,C=22.6100,body=+0.2000,vol=8034907.0 | **GOOD** |
| `A07_rvol` | RVOL=0.844 on 2026-10-13: today_vol=8034907 / avg20=9522937 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.785 on 2026-10-13 (price=22.6100, mid=20.0725, upper=23.3054, lower=16.8396; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=22.6100 vs SMA50=18.3864 dist=+22.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=20.0725 SMA50=18.3864 SMA80=17.3816 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-13 (63 bars); S1[2026-07-16→2026-08-13] low=2026-07-16@15.6700; S2[2026-08-14→2026-09-14] low=2026-08-26@15.8600; S3[2026-09-15→2026-10-13] low=2026-09-29@18.3600 | lows=[15.670000076293945, 15.859999656677246, 18.360000610351562] span=17.17% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.5633831572839321 wick_frac=0.43661684271606793 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.17817302872484134 wick_frac=0.8218269712751587 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.5000119209403238 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.0200:wick=0.4350; 2026-10-08:GREEN:body=+0.1100:wick=0.3000; 2026-10-09:GREEN:body=+0.3700:wick=0.3700; 2026-10-12:RED:body=-0.0800:wick=0.3690; 2026-10-13:GREEN:body=+0.2000:wick=0.1550 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.94 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.74 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.51 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.51 vs prior_export=22.51 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.25 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.42 | **GOOD** |
| `B13_short_float` | 0.95 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.94 (this export) | prior_export=12.94 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.74 (this export) | prior_export=6.74 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PBR  ·  score **+16**  ·  Oil & Gas Integrated
price=25.5  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=79.73 on 2026-10-13; prev RSI=79.45 on 2026-10-12 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 79.45@2026-10-12 → 79.73@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 79.45@2026-10-12 → 79.73@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 79.45@2026-10-12 → 79.73@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=12.000 (G=0.2400 R=0.0200); 2026-10-12:RED:O=25.4400,C=25.4200,body=-0.0200,vol=13275058.0; 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.323 (Gvol=17563984 Rvol=13275058); 2026-10-12:RED:O=25.4400,C=25.4200,body=-0.0200,vol=13275058.0; 2026-10-13:GREEN:O=25.2600,C=25.5000,body=+0.2400,vol=17563984.0 | **GOOD** |
| `A07_rvol` | RVOL=0.718 on 2026-10-13: today_vol=17563984 / avg20=24446047 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.853 on 2026-10-13 (price=25.5000, mid=22.2015, upper=26.0686, lower=18.3344; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-13: price=25.5000 vs SMA50=20.3986 dist=+25.01% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=22.2015 SMA50=20.3986 SMA80=19.3376 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-13 (63 bars); S1[2026-07-16→2026-08-13] low=2026-07-16@17.4400; S2[2026-08-14→2026-09-14] low=2026-08-27@17.5400; S3[2026-09-15→2026-10-13] low=2026-09-25@20.2000 | lows=[17.440000534057617, 17.540000915527344, 20.200000762939453] span=15.83% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.5161304877498534 wick_frac=0.4838695122501466 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.03846252599686754 wick_frac=0.9615374740031325 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=11.99971390425329 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.1700:wick=0.4650; 2026-10-08:GREEN:body=+0.2900:wick=0.3000; 2026-10-09:GREEN:body=+0.6760:wick=0.3140; 2026-10-12:RED:body=-0.0200:wick=0.5000; 2026-10-13:GREEN:body=+0.2400:wick=0.2250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=14.2 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.84 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 104163.37 | **NEUTRAL** |
| `B04_income` | 25479.57 | **GOOD** |
| `B05_profit_margin` | 24.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 22.44 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=22.44 vs prior_export=22.44 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.69 | **GOOD** |
| `B10_insider_transactions` | 0.26 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.26 vs prior=0.26 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.98 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.2 (this export) | prior_export=14.2 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.84 (this export) | prior_export=6.84 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ELV  ·  score **+16**  ·  Healthcare Plans
price=425.2699890136719  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.68 on 2026-10-13; prev RSI=64.14 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.14@2026-10-12 → 65.68@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.14@2026-10-12 → 65.68@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.14@2026-10-12 → 65.68@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=4.098 (G=7.0900 R=1.7300); 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0; 2026-10-13:RED:O=427.0000,C=425.2700,body=-1.7300,vol=1842700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=0.472 (Gvol=869447 Rvol=1842700); 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0; 2026-10-13:RED:O=427.0000,C=425.2700,body=-1.7300,vol=1842700.0 | **BAD** |
| `A07_rvol` | RVOL=1.578 on 2026-10-13: today_vol=1842700 / avg20=1167929 (avg window 2026-09-15→2026-10-12, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.990 on 2026-10-13 (price=425.2700, mid=402.3280, upper=425.4992, lower=379.1568; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-13: price=425.2700 vs SMA50=401.3618 dist=+5.96% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=402.3280 SMA50=401.3618 SMA80=398.9669 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-13 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-28@362.9800; S2[2026-08-14→2026-09-14] low=2026-08-17@388.1200; S3[2026-09-15→2026-10-13] low=2026-10-01@375.7400 | lows=[362.9800109863281, 388.1199951171875, 375.739990234375] span=6.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.6893528258050733 wick_frac=0.3106471741949267 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.13410912497516017 wick_frac=0.8658908750248399 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.09823775335603 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950; 2026-10-13:RED:body=-1.7300:wick=11.1700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CNP  ·  score **+15**  ·  Utilities - Regulated Electric
price=39.36000061035156  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.91 on 2026-10-13; prev RSI=57.75 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.75@2026-10-12 → 60.91@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.75@2026-10-12 → 60.91@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.75@2026-10-12 → 60.91@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=2.143 (G=0.3000 R=0.1400); 2026-10-12:GREEN:O=38.7300,C=39.0300,body=+0.3000,vol=6142721.0; 2026-10-13:RED:O=39.5000,C=39.3600,body=-0.1400,vol=5365400.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.145 (Gvol=6142721 Rvol=5365400); 2026-10-12:GREEN:O=38.7300,C=39.0300,body=+0.3000,vol=6142721.0; 2026-10-13:RED:O=39.5000,C=39.3600,body=-0.1400,vol=5365400.0 | **GOOD** |
| `A07_rvol` | RVOL=0.777 on 2026-10-13: today_vol=5365400 / avg20=6904964 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.907 on 2026-10-13 (price=39.3600, mid=37.8690, upper=39.5127, lower=36.2253; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-13: price=39.3600 vs SMA50=39.0392 dist=+0.82% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-13: SMA20=37.8690 SMA50=39.0392 SMA80=40.7465 | **BAD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-13 (63 bars); S1[2026-07-14→2026-08-13] low=2026-08-11@39.8000; S2[2026-08-14→2026-09-14] low=2026-09-14@38.3700; S3[2026-09-15→2026-10-13] low=2026-09-29@36.3700 | lows=[39.79999923706055, 38.369998931884766, 36.369998931884766] span=9.43% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.5940566386922792 wick_frac=0.4059433613077208 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.16867359132273188 wick_frac=0.8313264086772681 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.142861035422343 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.3600:wick=0.1850; 2026-10-08:RED:body=-0.1400:wick=0.3700; 2026-10-09:GREEN:body=+0.2600:wick=0.2100; 2026-10-12:GREEN:body=+0.3000:wick=0.2050; 2026-10-13:RED:body=-0.1400:wick=0.6900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.21 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.11 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9620.0 | **NEUTRAL** |
| `B04_income` | 1117.0 | **GOOD** |
| `B05_profit_margin` | 11.61 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.47 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.47 vs prior_export=45.47 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.23 | **GOOD** |
| `B10_insider_transactions` | 0.06 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.06 vs prior=0.06 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.48 | **GOOD** |
| `B13_short_float` | 7.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.21 (this export) | prior_export=7.21 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.11 (this export) | prior_export=1.11 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### AVT  ·  score **+15**  ·  Electronics & Computer Distribution
price=99.5999984741211  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.68 on 2026-10-13; prev RSI=47.04 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.04@2026-10-12 → 51.68@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 47.04@2026-10-12 → 51.68@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 47.04@2026-10-12 → 51.68@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=2.885 (G=3.7500 R=1.3000); 2026-10-12:RED:O=98.4500,C=97.1500,body=-1.3000,vol=797324.0; 2026-10-13:GREEN:O=95.8500,C=99.6000,body=+3.7500,vol=1694200.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=2.125 (Gvol=1694200 Rvol=797324); 2026-10-12:RED:O=98.4500,C=97.1500,body=-1.3000,vol=797324.0; 2026-10-13:GREEN:O=95.8500,C=99.6000,body=+3.7500,vol=1694200.0 | **GOOD** |
| `A07_rvol` | RVOL=1.237 on 2026-10-13: today_vol=1694200 / avg20=1369135 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.079 on 2026-10-13 (price=99.6000, mid=100.2335, upper=108.2453, lower=92.2217; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=99.6000 vs SMA50=95.6770 dist=+4.10% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=100.2335 SMA50=95.6770 SMA80=92.4517 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-13 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-17@82.5000; S2[2026-08-14→2026-09-14] low=2026-08-24@85.5200; S3[2026-09-15→2026-10-13] low=2026-09-16@90.9000 | lows=[82.5, 85.5199966430664, 90.9000015258789] span=10.18% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.816992757899076 wick_frac=0.183007242100924 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.5508453754889601 wick_frac=0.44915462451104 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8846255421290783 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-2.1500:wick=1.6600; 2026-10-08:RED:body=-1.7300:wick=1.6900; 2026-10-09:RED:body=-0.5600:wick=2.3400; 2026-10-12:RED:body=-1.3000:wick=1.0600; 2026-10-13:GREEN:body=+3.7500:wick=0.8400 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.95 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.77 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 27632.68 | **NEUTRAL** |
| `B04_income` | 334.39 | **GOOD** |
| `B05_profit_margin` | 1.21 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 106.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=106.25 vs prior_export=106.25 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.43 | **GOOD** |
| `B10_insider_transactions` | -9.57 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-9.57 vs prior=-9.57 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.33 | **GOOD** |
| `B13_short_float` | 10.47 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.95 (this export) | prior_export=26.95 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.77 (this export) | prior_export=9.77 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SN  ·  score **+15**  ·  Furnishings, Fixtures & Appliances
price=188.8000030517578  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.70 on 2026-10-13; prev RSI=68.24 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.24@2026-10-12 → 65.70@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.24@2026-10-12 → 65.70@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.24@2026-10-12 → 65.70@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=2.310 (G=5.3600 R=2.3200); 2026-10-12:GREEN:O=184.6200,C=189.9800,body=+5.3600,vol=1507628.0; 2026-10-13:RED:O=191.1200,C=188.8000,body=-2.3200,vol=1030578.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.463 (Gvol=1507628 Rvol=1030578); 2026-10-12:GREEN:O=184.6200,C=189.9800,body=+5.3600,vol=1507628.0; 2026-10-13:RED:O=191.1200,C=188.8000,body=-2.3200,vol=1030578.0 | **GOOD** |
| `A07_rvol` | RVOL=0.566 on 2026-10-13: today_vol=1030578 / avg20=1822105 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.658 on 2026-10-13 (price=188.8000, mid=178.5200, upper=194.1488, lower=162.8912; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=188.8000 vs SMA50=179.2688 dist=+5.32% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=178.52_50=179.27_80=167.84 on 2026-10-13: SMA20=178.5200 SMA50=179.2688 SMA80=167.8421 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-13 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-23@141.8800; S2[2026-08-14→2026-09-14] low=2026-09-11@157.0100; S3[2026-09-15→2026-10-13] low=2026-09-18@164.0400 | lows=[141.8800048828125, 157.00999450683594, 164.0399932861328] span=15.62% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.6473515151180084 wick_frac=0.35264848488199163 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.38474171394446105 wick_frac=0.615258286055539 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.3103529922456145 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+2.1900:wick=1.7200; 2026-10-08:GREEN:body=+1.8000:wick=2.8300; 2026-10-09:RED:body=-0.2200:wick=3.1750; 2026-10-12:GREEN:body=+5.3600:wick=2.9199; 2026-10-13:RED:body=-2.3200:wick=3.7100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.45 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 6909.96 | **NEUTRAL** |
| `B04_income` | 695.22 | **GOOD** |
| `B05_profit_margin` | 10.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.56 vs prior_export=209.56 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.2 | **GOOD** |
| `B10_insider_transactions` | -6.41 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-6.41 vs prior=-6.41 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 9.92 | **GOOD** |
| `B13_short_float` | 8.08 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.45 (this export) | prior_export=13.45 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.82 (this export) | prior_export=6.82 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
price=51.790000915527344  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.70 on 2026-10-13; prev RSI=51.48 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.48@2026-10-12 → 50.70@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.48@2026-10-12 → 50.70@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.48@2026-10-12 → 50.70@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=6.737 (G=1.2800 R=0.1900); 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0; 2026-10-13:RED:O=51.9800,C=51.7900,body=-0.1900,vol=1015600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=0.706 (Gvol=717503 Rvol=1015600); 2026-10-12:GREEN:O=50.6900,C=51.9700,body=+1.2800,vol=717503.0; 2026-10-13:RED:O=51.9800,C=51.7900,body=-0.1900,vol=1015600.0 | **BAD** |
| `A07_rvol` | RVOL=0.906 on 2026-10-13: today_vol=1015600 / avg20=1121407 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.011 on 2026-10-13 (price=51.7900, mid=51.8295, upper=55.5699, lower=48.0891; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=51.7900 vs SMA50=51.7234 dist=+0.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=51.8295 SMA50=51.7234 SMA80=50.5781 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-15→2026-10-13 (63 bars); S1[2026-07-15→2026-08-13] low=2026-08-12@47.8600; S2[2026-08-14→2026-09-14] low=2026-08-26@49.4100; S3[2026-09-15→2026-10-13] low=2026-10-02@48.3600 | lows=[47.86000061035156, 49.40999984741211, 48.36000061035156] span=3.24% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.5663722938461903 wick_frac=0.4336277061538098 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.12925065913761963 wick_frac=0.8707493408623803 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=6.736904451181561 need>1.4; red_wick_gt_green=False 5d trail=2026-10-07:RED:body=-1.8300:wick=0.3050; 2026-10-08:RED:body=-0.2900:wick=0.9400; 2026-10-09:GREEN:body=+0.3600:wick=1.6550; 2026-10-12:GREEN:body=+1.2800:wick=0.9800; 2026-10-13:RED:body=-0.1900:wick=1.2800 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+15**  ·  Computer Hardware
price=14.319999694824219  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.15 on 2026-10-13; prev RSI=55.54 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.54@2026-10-12 → 64.15@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.54@2026-10-12 → 64.15@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.54@2026-10-12 → 64.15@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=7.385 (G=0.9600 R=0.1300); 2026-10-12:RED:O=13.6800,C=13.5500,body=-0.1300,vol=842642.0; 2026-10-13:GREEN:O=13.3600,C=14.3200,body=+0.9600,vol=1114777.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.323 (Gvol=1114777 Rvol=842642); 2026-10-12:RED:O=13.6800,C=13.5500,body=-0.1300,vol=842642.0; 2026-10-13:GREEN:O=13.3600,C=14.3200,body=+0.9600,vol=1114777.0 | **GOOD** |
| `A07_rvol` | RVOL=1.088 on 2026-10-13: today_vol=1114777 / avg20=1025036 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.516 on 2026-10-13 (price=14.3200, mid=13.5665, upper=14.0636, lower=13.0694; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-13: price=14.3200 vs SMA50=12.7828 dist=+12.03% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=13.5665 SMA50=12.7828 SMA80=11.5952 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-13 (63 bars); S1[2026-07-16→2026-08-13] low=2026-07-17@9.0200; S2[2026-08-14→2026-09-14] low=2026-08-24@10.7260; S3[2026-09-15→2026-10-13] low=2026-09-16@12.8320 | lows=[9.020000457763672, 10.72599983215332, 12.831999778747559] span=42.26% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.7741937220531073 wick_frac=0.22580627794689265 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.3252440851697382 wick_frac=0.6747559148302618 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=7.384609177273227 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.2500:wick=0.2650; 2026-10-08:RED:body=-0.2600:wick=0.3900; 2026-10-09:GREEN:body=+0.4200:wick=0.2000; 2026-10-12:RED:body=-0.1300:wick=0.2697; 2026-10-13:GREEN:body=+0.9600:wick=0.2800 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.81 | **GOOD** |
| `B13_short_float` | 19.58 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+15**  ·  Oil & Gas Refining & Marketing
price=128.22999572753906  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=80.37 on 2026-10-13; prev RSI=76.00 on 2026-10-12 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.00@2026-10-12 → 80.37@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.00@2026-10-12 → 80.37@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.00@2026-10-12 → 80.37@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=14.000 (G=4.3400 R=0.3100); 2026-10-12:RED:O=123.2200,C=122.9100,body=-0.3100,vol=2052442.0; 2026-10-13:GREEN:O=123.8900,C=128.2300,body=+4.3400,vol=2210995.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.077 (Gvol=2210995 Rvol=2052442); 2026-10-12:RED:O=123.2200,C=122.9100,body=-0.3100,vol=2052442.0; 2026-10-13:GREEN:O=123.8900,C=128.2300,body=+4.3400,vol=2210995.0 | **GOOD** |
| `A07_rvol` | RVOL=0.799 on 2026-10-13: today_vol=2210995 / avg20=2765701 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.141 on 2026-10-13 (price=128.2300, mid=113.2495, upper=126.3792, lower=100.1198; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-13: price=128.2300 vs SMA50=103.4714 dist=+23.93% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=113.2495 SMA50=103.4714 SMA80=94.7159 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-16→2026-10-13 (63 bars); S1[2026-07-16→2026-08-13] low=2026-08-07@80.0041; S2[2026-08-14→2026-09-14] low=2026-08-14@91.2200; S3[2026-09-15→2026-10-13] low=2026-09-23@103.0000 | lows=[80.0040801936391, 91.22000122070312, 103.0] span=28.74% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.8964708626324963 wick_frac=0.1035291373675037 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.0789802082968872 wick_frac=0.9210197917031128 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.000098444575704 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:RED:body=-0.6650:wick=1.7950; 2026-10-08:GREEN:body=+2.7500:wick=0.8100; 2026-10-09:GREEN:body=+1.3000:wick=3.0250; 2026-10-12:RED:body=-0.3100:wick=3.6150; 2026-10-13:GREEN:body=+4.3400:wick=0.5012 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 103.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=103.43 vs prior_export=103.43 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.58 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.14 | **GOOD** |
| `B13_short_float` | 5.85 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RNW  ·  score **+15**  ·  Utilities - Renewable
price=6.880000114440918  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.18 on 2026-10-13; prev RSI=63.95 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.95@2026-10-12 → 61.18@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.95@2026-10-12 → 61.18@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.95@2026-10-12 → 61.18@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=0.0100 R=0.0000); 2026-10-12:GREEN:O=6.8800,C=6.8900,body=+0.0100,vol=2205218.0; 2026-10-13:DOJI:O=6.8800,C=6.8800,body=+0.0000,vol=2457500.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=2.795 (Gvol=3433968 Rvol=1228750); 2026-10-12:GREEN:O=6.8800,C=6.8900,body=+0.0100,vol=2205218.0; 2026-10-13:DOJI:O=6.8800,C=6.8800,body=+0.0000,vol=2457500.0 | **GOOD** |
| `A07_rvol` | RVOL=1.937 on 2026-10-13: today_vol=2457500 / avg20=1268725 (avg window 2026-09-15→2026-10-12, excludes asof) | **GOOD** |
| `A08_bollinger_position` | pos=0.532 on 2026-10-13 (price=6.8800, mid=6.8565, upper=6.9006, lower=6.8124; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=6.8800 vs SMA50=6.7726 dist=+1.59% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=6.8565 SMA50=6.7726 SMA80=6.5550 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-14→2026-10-13 (63 bars); S1[2026-07-14→2026-08-13] low=2026-07-27@5.9450; S2[2026-08-14→2026-09-14] low=2026-08-31@6.7800; S3[2026-09-15→2026-10-13] low=2026-09-15@6.8300 | lows=[5.945000171661377, 6.78000020980835, 6.829999923706055] span=14.89% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.6666560701910544 wick_frac=0.33334392980894556 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-07:GREEN:body=+0.0300:wick=0.0100; 2026-10-08:GREEN:body=+0.0200:wick=0.0100; 2026-10-09:RED:body=-0.0200:wick=0.0100; 2026-10-12:GREEN:body=+0.0100:wick=0.0050; 2026-10-13:DOJI:body=+0.0000:wick=0.0200 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=40.31 (current export asof; earnings_date=8/18/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=10.06 (current export; earnings_date=8/18/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1515.69 | **NEUTRAL** |
| `B04_income` | 122.68 | **GOOD** |
| `B05_profit_margin` | 8.09 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 7.66 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=7.66 vs prior_export=7.66 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 2.25 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.93 | **BAD** |
| `B13_short_float` | 1.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/18/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=40.31 (this export) | prior_export=40.31 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=10.06 (this export) | prior_export=10.06 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CMBT  ·  score **+14**  ·  Oil & Gas Midstream
price=20.790000915527344  pair=`2026-10-12→2026-10-13`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.58 on 2026-10-13; prev RSI=69.99 on 2026-10-12 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 69.99@2026-10-12 → 67.58@2026-10-13 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 69.99@2026-10-12 → 67.58@2026-10-13 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 69.99@2026-10-12 → 67.58@2026-10-13 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_body_sum/RED_body_sum=1.333 (G=0.2000 R=0.1500); 2026-10-12:GREEN:O=20.7200,C=20.9200,body=+0.2000,vol=1109735.0; 2026-10-13:RED:O=20.9400,C=20.7900,body=-0.1500,vol=842112.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-12 + 2026-10-13; ratio=GREEN_vol/RED_vol=1.318 (Gvol=1109735 Rvol=842112); 2026-10-12:GREEN:O=20.7200,C=20.9200,body=+0.2000,vol=1109735.0; 2026-10-13:RED:O=20.9400,C=20.7900,body=-0.1500,vol=842112.0 | **GOOD** |
| `A07_rvol` | RVOL=0.802 on 2026-10-13: today_vol=842112 / avg20=1049634 (avg window 2026-09-15→2026-10-12, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.750 on 2026-10-13 (price=20.7900, mid=19.7850, upper=21.1241, lower=18.4459; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-13: price=20.7900 vs SMA50=18.6500 dist=+11.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-13: SMA20=19.7850 SMA50=18.6500 SMA80=17.3436 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-13→2026-10-13 (63 bars); S1[2026-07-13→2026-08-13] low=2026-07-17@14.8300; S2[2026-08-14→2026-09-14] low=2026-08-14@17.0300; S3[2026-09-15→2026-10-13] low=2026-09-25@18.6900 | lows=[14.829999923706055, 17.030000686645508, 18.690000534057617] span=26.03% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: GREEN body_frac=0.4149390400740779 wick_frac=0.5850609599259221 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-12+2026-10-13: RED body_frac=0.48387057079044354 wick_frac=0.5161294292095564 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.3333418104599266 need>1.4; red_wick_gt_green=False 5d trail=2026-10-07:GREEN:body=+0.1800:wick=0.3200; 2026-10-08:GREEN:body=+0.3000:wick=0.2250; 2026-10-09:RED:body=-0.0700:wick=0.2800; 2026-10-12:GREEN:body=+0.2000:wick=0.2820; 2026-10-13:RED:body=-0.1500:wick=0.1600 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=42.02 (current export asof; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=21.0 (current export; earnings_date=8/27/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2266.8 | **NEUTRAL** |
| `B04_income` | 842.15 | **GOOD** |
| `B05_profit_margin` | 37.15 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 20.73 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=20.73 vs prior_export=20.73 on finviz_2026-10-13) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-13) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.05 | **GOOD** |
| `B13_short_float` | 1.38 | **NEUTRAL** |
| `B14_earnings_date` | 8/27/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=42.02 (this export) | prior_export=42.02 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=21.0 (this export) | prior_export=21.0 (finviz_2026-10-13) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-14_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.