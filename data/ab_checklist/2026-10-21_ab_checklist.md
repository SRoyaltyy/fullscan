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
| 1 | ELV | +16 | 17 | 1 | 2026-10-09→2026-10-12 | Healthcare Plans |
| 2 | AUPH | +16 | 16 | 0 | 2026-10-09→2026-10-12 | Biotechnology |
| 3 | OXY | +16 | 16 | 0 | 2026-10-19→2026-10-20 | Oil & Gas E&P |
| 4 | TRI | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Specialty Business Services |
| 5 | ISRG | +15 | 17 | 2 | 2026-10-19→2026-10-20 | Medical Instruments & Supplies |
| 6 | BB | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 7 | FIVN | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 8 | YPF | +15 | 16 | 1 | 2026-10-09→2026-10-12 | Oil & Gas Integrated |
| 9 | HAS | +15 | 18 | 3 | 2026-10-19→2026-10-20 | Leisure |
| 10 | MSFT | +15 | 16 | 1 | 2026-10-19→2026-10-20 | Software - Infrastructure |
| 11 | SN | +15 | 17 | 2 | 2026-10-19→2026-10-20 | Furnishings, Fixtures & Appliances |
| 12 | NICE | +14 | 15 | 1 | 2026-10-19→2026-10-20 | Software - Application |
| 13 | GMAB | +14 | 15 | 1 | 2026-10-19→2026-10-20 | Biotechnology |
| 14 | EMBC | +14 | 15 | 1 | 2026-10-09→2026-10-12 | Medical Instruments & Supplies |
| 15 | PODD | +14 | 16 | 2 | 2026-10-09→2026-10-12 | Medical Devices |

## Full checklist — top 15

### ELV  ·  score **+16**  ·  Healthcare Plans
price=422.0899963378906  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.14 on 2026-10-12; prev RSI=59.13 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 59.13@2026-10-09 → 64.14@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=17.8700 R=0.0000); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2928672 Rvol=0); 2026-10-09:GREEN:O=401.9200,C=412.7000,body=+10.7800,vol=2059225.0; 2026-10-12:GREEN:O=415.0000,C=422.0900,body=+7.0900,vol=869447.0 | **GOOD** |
| `A07_rvol` | RVOL=0.737 on 2026-10-12: today_vol=869447 / avg20=1180302 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.925 on 2026-10-12 (price=422.0900, mid=401.9415, upper=423.7173, lower=380.1657; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-12: price=422.0900 vs SMA50=400.5118 dist=+5.39% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=401.9415 SMA50=400.5118 SMA80=398.6746 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-28@362.9800; S2[2026-08-13→2026-09-11] low=2026-08-17@388.1200; S3[2026-09-14→2026-10-12] low=2026-10-01@375.7400 | lows=[362.9800109863281, 388.1199951171875, 375.739990234375] span=6.93% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6505804361528325 wick_frac=0.34941956384716766 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:GREEN:body=+3.4100:wick=3.1374; 2026-10-07:GREEN:body=+3.3100:wick=7.0450; 2026-10-08:RED:body=-2.8900:wick=7.3600; 2026-10-09:GREEN:body=+10.7800:wick=6.8399; 2026-10-12:GREEN:body=+7.0900:wick=3.1950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=19.94 (current export asof; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.94 (current export; earnings_date=7/15/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 447.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=447.86 vs prior_export=447.86 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.34 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.34 vs prior=0.34 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.67 | **GOOD** |
| `B13_short_float` | 2.47 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=19.94 (this export) | prior_export=19.94 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.94 (this export) | prior_export=1.94 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

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

### YPF  ·  score **+15**  ·  Oil & Gas Integrated
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
| `B01_eps_surprise` | EPS surprise=27.28 (current export asof; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.07 (current export; earnings_date=8/10/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20762.28 | **NEUTRAL** |
| `B04_income` | 828.88 | **GOOD** |
| `B05_profit_margin` | 3.99 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 61.06 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=61.06 vs prior_export=61.06 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.55 | **GOOD** |
| `B10_insider_transactions` | 3.82 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.82 vs prior=3.82 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 56.14 | **GOOD** |
| `B13_short_float` | 1.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/10/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=27.28 (this export) | prior_export=27.28 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.07 (this export) | prior_export=6.07 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

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

### NICE  ·  score **+14**  ·  Software - Application
price=114.48999786376953  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.29 on 2026-10-20; prev RSI=53.31 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.31@2026-10-19 → 54.29@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.31@2026-10-19 → 54.29@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.31@2026-10-19 → 54.29@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=3.078 (G=1.5700 R=0.5100); 2026-10-19:GREEN:O=112.2700,C=113.8400,body=+1.5700,vol=390352.0; 2026-10-20:RED:O=115.0000,C=114.4900,body=-0.5100,vol=547743.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=0.713 (Gvol=390352 Rvol=547743); 2026-10-19:GREEN:O=112.2700,C=113.8400,body=+1.5700,vol=390352.0; 2026-10-20:RED:O=115.0000,C=114.4900,body=-0.5100,vol=547743.0 | **BAD** |
| `A07_rvol` | RVOL=1.018 on 2026-10-20: today_vol=547743 / avg20=538216 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.081 on 2026-10-20 (price=114.4900, mid=114.0277, upper=119.7378, lower=108.3177; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-20: price=114.4900 vs SMA50=107.9003 dist=+6.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=114.0277 SMA50=107.9003 SMA80=103.9934 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-22→2026-10-20 (63 bars); S1[2026-07-22→2026-08-20] low=2026-07-23@86.0100; S2[2026-08-21→2026-09-21] low=2026-09-11@97.4400; S3[2026-09-22→2026-10-20] low=2026-09-29@105.6600 | lows=[86.01000213623047, 97.44000244140625, 105.66000366210938] span=22.85% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.6977776421440972 wick_frac=0.3022223578559028 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=0.12259655943953343 wick_frac=0.8774034405604666 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0784178796355857 need>1.4; red_wick_gt_green=False 5d trail=2026-10-14:RED:body=-3.1400:wick=0.4900; 2026-10-15:GREEN:body=+4.3100:wick=2.8200; 2026-10-16:RED:body=-4.3150:wick=1.8650; 2026-10-19:GREEN:body=+1.5700:wick=0.6800; 2026-10-20:RED:body=-0.5100:wick=3.6500 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.27 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.09 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3089.11 | **NEUTRAL** |
| `B04_income` | 428.37 | **GOOD** |
| `B05_profit_margin` | 13.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 127.82 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=127.82 vs prior_export=127.82 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.94 | **GOOD** |
| `B10_insider_transactions` | nan | **NEUTRAL** |
| `B11_insider_tx_delta` | n/a (now=nan, prior_export_date=2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.64 | **GOOD** |
| `B13_short_float` | 2.1 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.27 (this export) | prior_export=2.27 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.09 (this export) | prior_export=2.09 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### GMAB  ·  score **+14**  ·  Biotechnology
price=39.06999969482422  pair=`2026-10-19→2026-10-20`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.62 on 2026-10-20; prev RSI=63.59 on 2026-10-19 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.59@2026-10-19 → 65.62@2026-10-20 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.59@2026-10-19 → 65.62@2026-10-20 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.59@2026-10-19 → 65.62@2026-10-20 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.1300 R=0.0000); 2026-10-19:GREEN:O=37.8400,C=38.5000,body=+0.6600,vol=4775871.0; 2026-10-20:GREEN:O=38.6000,C=39.0700,body=+0.4700,vol=3777521.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-19 + 2026-10-20; ratio=GREEN_vol/RED_vol=99.000 (Gvol=8553392 Rvol=0); 2026-10-19:GREEN:O=37.8400,C=38.5000,body=+0.6600,vol=4775871.0; 2026-10-20:GREEN:O=38.6000,C=39.0700,body=+0.4700,vol=3777521.0 | **GOOD** |
| `A07_rvol` | RVOL=0.884 on 2026-10-20: today_vol=3777521 / avg20=4273518 (avg window 2026-09-22→2026-10-19, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.924 on 2026-10-20 (price=39.0700, mid=36.5037, upper=39.2805, lower=33.7270; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-10-20: price=39.0700 vs SMA50=34.6237 dist=+12.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-20: SMA20=36.5037 SMA50=34.6237 SMA80=32.3574 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-20→2026-10-20 (63 bars); S1[2026-07-20→2026-08-20] low=2026-07-20@27.8650; S2[2026-08-21→2026-09-21] low=2026-09-10@32.2100; S3[2026-09-22→2026-10-20] low=2026-10-02@33.7250 | lows=[27.864999771118164, 32.209999084472656, 33.724998474121094] span=21.03% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: GREEN body_frac=0.5047077625130254 wick_frac=0.4952922374869745 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-19+2026-10-20: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-10-14:GREEN:body=+0.5050:wick=0.6150; 2026-10-15:GREEN:body=+0.6200:wick=0.6600; 2026-10-16:RED:body=-1.1450:wick=0.3150; 2026-10-19:GREEN:body=+0.6600:wick=0.5800; 2026-10-20:GREEN:body=+0.4700:wick=0.5150 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=37.92 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.46 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 4127.25 | **NEUTRAL** |
| `B04_income` | 785.93 | **GOOD** |
| `B05_profit_margin` | 19.04 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 39.39 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=39.39 vs prior_export=39.39 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 0.84 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=37.92 (this export) | prior_export=37.92 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.46 (this export) | prior_export=3.46 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### EMBC  ·  score **+14**  ·  Medical Instruments & Supplies
price=5.989999771118164  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.09 on 2026-10-12; prev RSI=55.18 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.18@2026-10-09 → 62.09@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=4.000 (G=0.2200 R=0.0550); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=1.807 (Gvol=1053433 Rvol=583035); 2026-10-09:RED:O=5.7400,C=5.6850,body=-0.0550,vol=583035.0; 2026-10-12:GREEN:O=5.7700,C=5.9900,body=+0.2200,vol=1053433.0 | **GOOD** |
| `A07_rvol` | RVOL=1.019 on 2026-10-12: today_vol=1053433 / avg20=1033961 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.624 on 2026-10-12 (price=5.9900, mid=5.6322, upper=6.2055, lower=5.0590; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-10-12: price=5.9900 vs SMA50=5.2233 dist=+14.68% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-10-12: SMA20=5.6322 SMA50=5.2233 SMA80=4.4932 | **GOOD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-07-14@3.0700; S2[2026-08-13→2026-09-11] low=2026-08-13@4.7200; S3[2026-09-14→2026-10-12] low=2026-09-14@4.9800 | lows=[3.069999933242798, 4.71999979019165, 4.980000019073486] span=62.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.6470579160542584 wick_frac=0.35294208394574156 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=0.19642814568070271 wick_frac=0.8035718543192972 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.000008669793572 need>1.4; red_wick_gt_green=False 5d trail=2026-10-06:GREEN:body=+0.0500:wick=0.2950; 2026-10-07:GREEN:body=+0.0500:wick=0.2500; 2026-10-08:RED:body=-0.1400:wick=0.1100; 2026-10-09:RED:body=-0.0550:wick=0.2250; 2026-10-12:GREEN:body=+0.2200:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=110.68 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=6.75 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 1018.7 | **NEUTRAL** |
| `B04_income` | 87.5 | **GOOD** |
| `B05_profit_margin` | 8.59 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=4.0 vs prior_export=4.0 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | -16.39 | **BAD** |
| `B13_short_float` | 10.67 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=110.68 (this export) | prior_export=110.68 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=6.75 (this export) | prior_export=6.75 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PODD  ·  score **+14**  ·  Medical Devices
price=139.80999755859375  pair=`2026-10-09→2026-10-12`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.20 on 2026-10-12; prev RSI=44.78 on 2026-10-09 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.78@2026-10-09 → 54.20@2026-10-12 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=4.9300 R=0.0000); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-10-09 + 2026-10-12; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2183896 Rvol=0); 2026-10-09:GREEN:O=134.1900,C=135.2800,body=+1.0900,vol=864465.0; 2026-10-12:GREEN:O=135.9700,C=139.8100,body=+3.8400,vol=1319431.0 | **GOOD** |
| `A07_rvol` | RVOL=0.819 on 2026-10-12: today_vol=1319431 / avg20=1610443 (avg window 2026-09-14→2026-10-09, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.521 on 2026-10-12 (price=139.8100, mid=136.1735, upper=143.1581, lower=129.1889; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-10-12: price=139.8100 vs SMA50=141.5730 dist=-1.25% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-10-12: SMA20=136.1735 SMA50=141.5730 SMA80=147.5865 | **BAD** |
| `A11_three_section_lows` | window=2026-07-10→2026-10-12 (63 bars); S1[2026-07-10→2026-08-12] low=2026-08-05@126.4000; S2[2026-08-13→2026-09-11] low=2026-09-11@131.2400; S3[2026-09-14→2026-10-12] low=2026-10-02@128.0100 | lows=[126.4000015258789, 131.24000549316406, 128.00999450683594] span=3.83% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: GREEN body_frac=0.5660465898505131 wick_frac=0.43395341014948685 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-10-09+2026-10-12: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-10-06:RED:body=-1.0400:wick=1.3950; 2026-10-07:RED:body=-0.5800:wick=3.1000; 2026-10-08:GREEN:body=+0.5100:wick=1.2300; 2026-10-09:GREEN:body=+1.0900:wick=2.3600; 2026-10-12:GREEN:body=+3.8400:wick=0.8650 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=13.11 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.82 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 3053.5 | **NEUTRAL** |
| `B04_income` | 375.3 | **GOOD** |
| `B05_profit_margin` | 12.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 169.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=169.86 vs prior_export=169.86 on finviz_2026-10-20) | **NEUTRAL** |
| `B09_analyst_recom` | 2.04 | **GOOD** |
| `B10_insider_transactions` | 2.38 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=2.38 vs prior=2.38 on finviz_2026-10-20) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 7.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.11 (this export) | prior_export=13.11 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.82 (this export) | prior_export=1.82 (finviz_2026-10-20) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-10-21_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.