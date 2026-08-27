# A+B1 Feature Checklist — 2026-08-27

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,698** names
- Export: `finviz_2026-08-27.csv` · prior export for Δ: `2026-08-25`
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
| 1 | OKE | +17 | 17 | 0 | 2026-08-25→2026-08-26 | Oil & Gas Midstream |
| 2 | RSG | +17 | 18 | 1 | 2026-08-25→2026-08-26 | Waste Management |
| 3 | DINO | +17 | 17 | 0 | 2026-08-25→2026-08-26 | Oil & Gas Refining & Marketing |
| 4 | ANET | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Computer Hardware |
| 5 | LNC | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Insurance - Life |
| 6 | MRK | +16 | 18 | 2 | 2026-08-25→2026-08-26 | Drug Manufacturers - General |
| 7 | CRSR | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Computer Hardware |
| 8 | SOBO | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Oil & Gas Midstream |
| 9 | ABM | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Specialty Business Services |
| 10 | CFFN | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Banks - Regional |
| 11 | PAA | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Oil & Gas Midstream |
| 12 | HLNE | +16 | 17 | 1 | 2026-08-25→2026-08-26 | Asset Management |
| 13 | IOT | +15 | 17 | 2 | 2026-08-25→2026-08-26 | Software - Infrastructure |
| 14 | VYX | +15 | 16 | 1 | 2026-08-25→2026-08-26 | Information Technology Services |
| 15 | CRM | +15 | 16 | 1 | 2026-08-25→2026-08-26 | Software - Application |

## Full checklist — top 15

### OKE  ·  score **+17**  ·  Oil & Gas Midstream
price=94.9000015258789  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.80 on 2026-08-26; prev RSI=50.34 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 50.34@2026-08-25 → 58.80@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 50.34@2026-08-25 → 58.80@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 50.34@2026-08-25 → 58.80@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=13.461 (G=3.5000 R=0.2600); 2026-08-25:RED:O=91.9600,C=91.7000,body=-0.2600,vol=2982800.0; 2026-08-26:GREEN:O=91.4000,C=94.9000,body=+3.5000,vol=3349143.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.123 (Gvol=3349143 Rvol=2982800); 2026-08-25:RED:O=91.9600,C=91.7000,body=-0.2600,vol=2982800.0; 2026-08-26:GREEN:O=91.4000,C=94.9000,body=+3.5000,vol=3349143.0 | **GOOD** |
| `A07_rvol` | RVOL=0.893 on 2026-08-26: today_vol=3349143 / avg20=3750385 (avg window 2026-07-29→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.493 on 2026-08-26 (price=94.9000, mid=91.6575, upper=98.2342, lower=85.0808; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=94.9000 vs SMA50=89.6931 dist=+5.81% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=91.6575 SMA50=89.6931 SMA80=89.1021 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-26 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-18@82.1011; S2[2026-06-25→2026-07-28] low=2026-07-01@84.4432; S3[2026-07-29→2026-08-26] low=2026-08-04@83.4800 | lows=[82.10108617947638, 84.44315674236297, 83.4800033569336] span=2.85% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.9006704689355938 wick_frac=0.09932953106440624 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.2031268626469256 wick_frac=0.7968731373530744 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=13.461427858798674 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-2.4400:wick=0.7000; 2026-08-21:RED:body=-1.7200:wick=0.7100; 2026-08-24:RED:body=-0.2700:wick=1.5200; 2026-08-25:RED:body=-0.2600:wick=1.0200; 2026-08-26:GREEN:body=+3.5000:wick=0.3860 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=5.1 (current export asof; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=34.63 (current export; earnings_date=8/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 39646.0 | **NEUTRAL** |
| `B04_income` | 3656.0 | **GOOD** |
| `B05_profit_margin` | 9.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 96.42 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=96.42 vs prior_export=96.42 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.4 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.16 | **GOOD** |
| `B13_short_float` | 3.99 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=5.1 (this export) | prior_export=5.1 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=34.63 (this export) | prior_export=34.63 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### RSG  ·  score **+17**  ·  Waste Management
price=222.17999267578125  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.32 on 2026-08-26; prev RSI=58.45 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.45@2026-08-25 → 60.32@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.45@2026-08-25 → 60.32@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.45@2026-08-25 → 60.32@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=2.841 (G=1.7900 R=0.6300); 2026-08-25:RED:O=221.5900,C=220.9600,body=-0.6300,vol=1344000.0; 2026-08-26:GREEN:O=220.3900,C=222.1800,body=+1.7900,vol=1439048.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.071 (Gvol=1439048 Rvol=1344000); 2026-08-25:RED:O=221.5900,C=220.9600,body=-0.6300,vol=1344000.0; 2026-08-26:GREEN:O=220.3900,C=222.1800,body=+1.7900,vol=1439048.0 | **GOOD** |
| `A07_rvol` | RVOL=1.003 on 2026-08-26: today_vol=1439048 / avg20=1434610 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.680 on 2026-08-26 (price=222.1800, mid=215.6120, upper=225.2720, lower=205.9520; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=222.1800 vs SMA50=215.1840 dist=+3.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=215.6120 SMA50=215.1840 SMA80=211.5700 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-02@197.0447; S2[2026-06-24→2026-07-27] low=2026-06-24@207.2848; S3[2026-07-28→2026-08-26] low=2026-08-05@204.8000 | lows=[197.04474803654423, 207.28484330604536, 204.8000030517578] span=5.20% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.6629837064332178 wick_frac=0.33701629356678214 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.17646731975859534 wick_frac=0.8235326802414047 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.8413059800905853 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-1.4000:wick=1.3400; 2026-08-21:GREEN:body=+1.0100:wick=1.3500; 2026-08-24:GREEN:body=+1.5300:wick=1.0900; 2026-08-25:RED:body=-0.6300:wick=2.9400; 2026-08-26:GREEN:body=+1.7900:wick=0.9099 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=2.01 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.53 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 16891.0 | **NEUTRAL** |
| `B04_income` | 2186.0 | **GOOD** |
| `B05_profit_margin` | 12.94 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 247.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=247.57 vs prior_export=247.57 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.93 | **GOOD** |
| `B10_insider_transactions` | 3.27 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.52 (now=3.27 vs prior=2.75 on finviz_2026-08-25) | **GOOD** |
| `B12_institutional_transactions` | -0.24 | **BAD** |
| `B13_short_float` | 2.17 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.01 (this export) | prior_export=2.01 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.53 (this export) | prior_export=1.53 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### DINO  ·  score **+17**  ·  Oil & Gas Refining & Marketing
price=96.5199966430664  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.99 on 2026-08-26; prev RSI=58.26 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.26@2026-08-25 → 62.99@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.26@2026-08-25 → 62.99@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.26@2026-08-25 → 62.99@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=5.079 (G=4.5200 R=0.8900); 2026-08-25:RED:O=94.1300,C=93.2400,body=-0.8900,vol=1368600.0; 2026-08-26:GREEN:O=92.0000,C=96.5200,body=+4.5200,vol=2409414.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.760 (Gvol=2409414 Rvol=1368600); 2026-08-25:RED:O=94.1300,C=93.2400,body=-0.8900,vol=1368600.0; 2026-08-26:GREEN:O=92.0000,C=96.5200,body=+4.5200,vol=2409414.0 | **GOOD** |
| `A07_rvol` | RVOL=0.902 on 2026-08-26: today_vol=2409414 / avg20=2670135 (avg window 2026-07-29→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.597 on 2026-08-26 (price=96.5200, mid=90.4189, upper=100.6316, lower=80.2063; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=96.5200 vs SMA50=82.7395 dist=+16.66% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=90.4189 SMA50=82.7395 SMA80=78.0382 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-26 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-22@63.4268; S2[2026-06-29→2026-07-28] low=2026-06-29@68.2271; S3[2026-07-29→2026-08-26] low=2026-08-07@80.0041 | lows=[63.42683480953448, 68.22708231781405, 80.0040801936391] span=26.14% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.6900755018508553 wick_frac=0.3099244981491447 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.3308554297853271 wick_frac=0.6691445702146729 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=5.078651396437327 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-2.4600:wick=1.7500; 2026-08-21:GREEN:body=+3.3200:wick=1.0700; 2026-08-24:RED:body=-0.8600:wick=3.6300; 2026-08-25:RED:body=-0.8900:wick=1.8000; 2026-08-26:GREEN:body=+4.5200:wick=2.0300 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=18.26 (current export asof; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=19.68 (current export; earnings_date=7/28/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 31228.0 | **NEUTRAL** |
| `B04_income` | 1899.0 | **GOOD** |
| `B05_profit_margin` | 6.08 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 93.29 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=93.29 vs prior_export=93.29 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 0.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.09 vs prior=0.09 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.37 | **GOOD** |
| `B13_short_float` | 5.02 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=18.26 (this export) | prior_export=18.26 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=19.68 (this export) | prior_export=19.68 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ANET  ·  score **+16**  ·  Computer Hardware
price=202.25  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.16 on 2026-08-26; prev RSI=53.30 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.30@2026-08-25 → 60.16@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.30@2026-08-25 → 60.16@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.30@2026-08-25 → 60.16@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=8.104 (G=11.3450 R=1.4000); 2026-08-25:RED:O=192.3400,C=190.9400,body=-1.4000,vol=4385100.0; 2026-08-26:GREEN:O=190.9050,C=202.2500,body=+11.3450,vol=7610378.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.736 (Gvol=7610378 Rvol=4385100); 2026-08-25:RED:O=192.3400,C=190.9400,body=-1.4000,vol=4385100.0; 2026-08-26:GREEN:O=190.9050,C=202.2500,body=+11.3450,vol=7610378.0 | **GOOD** |
| `A07_rvol` | RVOL=0.962 on 2026-08-26: today_vol=7610378 / avg20=7912805 (avg window 2026-07-29→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.560 on 2026-08-26 (price=202.2500, mid=192.1275, upper=210.2178, lower=174.0372; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=202.2500 vs SMA50=179.2078 dist=+12.86% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=192.1275 SMA50=179.2078 SMA80=169.8302 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-26 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-09@145.3200; S2[2026-06-29→2026-07-28] low=2026-06-29@155.2200; S3[2026-07-29→2026-08-26] low=2026-07-29@156.8400 | lows=[145.32000732421875, 155.22000122070312, 156.83999633789062] span=7.93% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.9116106503710171 wick_frac=0.08838934962898298 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.27504811468382206 wick_frac=0.7249518853161779 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=8.103607629427794 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-1.5600:wick=2.8900; 2026-08-21:GREEN:body=+4.8800:wick=1.9700; 2026-08-24:GREEN:body=+2.1500:wick=2.1000; 2026-08-25:RED:body=-1.4000:wick=3.6900; 2026-08-26:GREEN:body=+11.3450:wick=1.1000 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=15.14 (current export asof; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=7.26 (current export; earnings_date=8/4/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 10540.8 | **NEUTRAL** |
| `B04_income` | 4044.6 | **GOOD** |
| `B05_profit_margin` | 38.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 249.97 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=249.97 vs prior_export=249.97 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.09 | **GOOD** |
| `B10_insider_transactions` | -2.97 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.97 vs prior=-2.97 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 1.23 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=15.14 (this export) | prior_export=15.14 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=7.26 (this export) | prior_export=7.26 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### LNC  ·  score **+16**  ·  Insurance - Life
price=43.31999969482422  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.74 on 2026-08-26; prev RSI=48.11 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.11@2026-08-25 → 51.74@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.11@2026-08-25 → 51.74@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.11@2026-08-25 → 51.74@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=2.314 (G=0.8100 R=0.3500); 2026-08-25:RED:O=42.9600,C=42.6100,body=-0.3500,vol=1701200.0; 2026-08-26:GREEN:O=42.5100,C=43.3200,body=+0.8100,vol=1809715.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.064 (Gvol=1809715 Rvol=1701200); 2026-08-25:RED:O=42.9600,C=42.6100,body=-0.3500,vol=1701200.0; 2026-08-26:GREEN:O=42.5100,C=43.3200,body=+0.8100,vol=1809715.0 | **GOOD** |
| `A07_rvol` | RVOL=0.814 on 2026-08-26: today_vol=1809715 / avg20=2223655 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.345 on 2026-08-26 (price=43.3200, mid=44.5385, upper=48.0678, lower=41.0092; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=43.3200 vs SMA50=40.9510 dist=+5.78% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=44.5385 SMA50=40.9510 SMA80=38.7593 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-03@32.8415; S2[2026-06-24→2026-07-27] low=2026-06-30@34.8781; S3[2026-07-28→2026-08-26] low=2026-07-29@41.0700 | lows=[32.84154432414021, 34.878074838954085, 41.06999969482422] span=25.06% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.7043480856879383 wick_frac=0.2956519143120617 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.2966087996637895 wick_frac=0.7033912003362105 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.314299727520436 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-1.4700:wick=0.7400; 2026-08-21:RED:body=-0.3600:wick=0.6300; 2026-08-24:RED:body=-0.2700:wick=0.5000; 2026-08-25:RED:body=-0.3500:wick=0.8300; 2026-08-26:GREEN:body=+0.8100:wick=0.3400 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=14.42 (current export asof; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.8 (current export; earnings_date=7/30/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 20681.0 | **NEUTRAL** |
| `B04_income` | 2269.0 | **GOOD** |
| `B05_profit_margin` | 10.97 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 48.33 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=48.33 vs prior_export=48.33 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.56 | **NEUTRAL** |
| `B10_insider_transactions` | 232.75 | **GOOD** |
| `B11_insider_tx_delta` | delta=-2.3499999999999943 (now=232.75 vs prior=235.1 on finviz_2026-08-25) | **BAD** |
| `B12_institutional_transactions` | 2.86 | **GOOD** |
| `B13_short_float` | 5.46 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=14.42 (this export) | prior_export=14.42 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.8 (this export) | prior_export=0.8 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### MRK  ·  score **+16**  ·  Drug Manufacturers - General
price=153.10000610351562  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.97 on 2026-08-26; prev RSI=77.68 on 2026-08-25 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 77.68@2026-08-25 → 70.97@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 77.68@2026-08-25 → 70.97@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 77.68@2026-08-25 → 70.97@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=4.360 (G=5.4500 R=1.2500); 2026-08-25:GREEN:O=151.0000,C=156.4500,body=+5.4500,vol=14005100.0; 2026-08-26:RED:O=154.3500,C=153.1000,body=-1.2500,vol=8622014.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.624 (Gvol=14005100 Rvol=8622014); 2026-08-25:GREEN:O=151.0000,C=156.4500,body=+5.4500,vol=14005100.0; 2026-08-26:RED:O=154.3500,C=153.1000,body=-1.2500,vol=8622014.0 | **GOOD** |
| `A07_rvol` | RVOL=0.776 on 2026-08-26: today_vol=8622014 / avg20=11114845 (avg window 2026-07-29→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.752 on 2026-08-26 (price=153.1000, mid=137.5890, upper=158.2288, lower=116.9492; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=153.1000 vs SMA50=130.1054 dist=+17.67% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=137.5890 SMA50=130.1054 SMA80=124.5347 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-26 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-18@111.5700; S2[2026-06-29→2026-07-28] low=2026-07-14@120.1600; S3[2026-07-29→2026-08-26] low=2026-08-04@126.2200 | lows=[111.56999969482422, 120.16000366210938, 126.22000122070312] span=13.13% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.8706073052126116 wick_frac=0.12939269478738832 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.48560724617062645 wick_frac=0.5143927538293736 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=4.35999755859375 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-1.7900:wick=2.2100; 2026-08-21:GREEN:body=+3.4300:wick=2.4700; 2026-08-24:RED:body=-0.0600:wick=2.3200; 2026-08-25:GREEN:body=+5.4500:wick=0.8100; 2026-08-26:RED:body=-1.2500:wick=1.3241 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=51.94 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.45 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 66287.0 | **NEUTRAL** |
| `B04_income` | 3173.0 | **GOOD** |
| `B05_profit_margin` | 4.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 148.64 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.0 (now=148.64 vs prior_export=147.64 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 1.81 | **GOOD** |
| `B10_insider_transactions` | -8.47 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-8.47 vs prior=-8.47 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.48 | **GOOD** |
| `B13_short_float` | 0.99 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=51.94 (this export) | prior_export=51.94 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.45 (this export) | prior_export=1.45 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRSR  ·  score **+16**  ·  Computer Hardware
price=11.84000015258789  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.98 on 2026-08-26; prev RSI=53.98 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.98@2026-08-25 → 53.98@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.98@2026-08-25 → 53.98@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.98@2026-08-25 → 53.98@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0500 R=0.0000); 2026-08-25:GREEN:O=11.1000,C=11.8400,body=+0.7400,vol=1667300.0; 2026-08-26:GREEN:O=11.5300,C=11.8400,body=+0.3100,vol=1205931.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=2873231 Rvol=0); 2026-08-25:GREEN:O=11.1000,C=11.8400,body=+0.7400,vol=1667300.0; 2026-08-26:GREEN:O=11.5300,C=11.8400,body=+0.3100,vol=1205931.0 | **GOOD** |
| `A07_rvol` | RVOL=0.525 on 2026-08-26: today_vol=1205931 / avg20=2297785 (avg window 2026-07-29→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.014 on 2026-08-26 (price=11.8400, mid=11.8710, upper=14.1655, lower=9.5765; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=11.8400 vs SMA50=10.3935 dist=+13.92% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=11.8710 SMA50=10.3935 SMA80=9.6746 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-28→2026-08-26 (63 bars); S1[2026-05-28→2026-06-26] low=2026-06-09@8.0500; S2[2026-06-29→2026-07-28] low=2026-07-07@8.3000; S3[2026-07-29→2026-08-26] low=2026-07-29@10.0600 | lows=[8.050000190734863, 8.300000190734863, 10.0600004196167] span=24.97% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.8483716404356092 wick_frac=0.15162835956439083 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-0.3300:wick=0.2800; 2026-08-21:RED:body=-0.1400:wick=0.1900; 2026-08-24:GREEN:body=+0.1600:wick=0.1040; 2026-08-25:GREEN:body=+0.7400:wick=0.1000; 2026-08-26:GREEN:body=+0.3100:wick=0.0700 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=227.64 (current export asof; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.24 (current export; earnings_date=8/6/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1451.46 | **NEUTRAL** |
| `B04_income` | 33.3 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.22 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.22 vs prior_export=13.22 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.44 | **GOOD** |
| `B10_insider_transactions` | -0.01 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.01 vs prior=-0.01 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.73 | **GOOD** |
| `B13_short_float` | 23.38 | **GOOD** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=227.64 (this export) | prior_export=227.64 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.24 (this export) | prior_export=1.24 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### SOBO  ·  score **+16**  ·  Oil & Gas Midstream
price=37.849998474121094  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.43 on 2026-08-26; prev RSI=49.59 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.59@2026-08-25 → 57.43@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.59@2026-08-25 → 57.43@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.59@2026-08-25 → 57.43@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=10.364 (G=1.1400 R=0.1100); 2026-08-25:RED:O=36.9300,C=36.8200,body=-0.1100,vol=605400.0; 2026-08-26:GREEN:O=36.7100,C=37.8500,body=+1.1400,vol=631240.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.043 (Gvol=631240 Rvol=605400); 2026-08-25:RED:O=36.9300,C=36.8200,body=-0.1100,vol=605400.0; 2026-08-26:GREEN:O=36.7100,C=37.8500,body=+1.1400,vol=631240.0 | **GOOD** |
| `A07_rvol` | RVOL=1.223 on 2026-08-26: today_vol=631240 / avg20=516065 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.638 on 2026-08-26 (price=37.8500, mid=36.7750, upper=38.4605, lower=35.0895; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=37.8500 vs SMA50=36.5917 dist=+3.44% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=36.7750 SMA50=36.5917 SMA80=36.2974 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-18@34.6503; S2[2026-06-24→2026-07-27] low=2026-07-02@34.0700; S3[2026-07-28→2026-08-26] low=2026-08-06@34.8200 | lows=[34.65031328915051, 34.06999969482422, 34.81999969482422] span=2.20% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.8351656950584921 wick_frac=0.1648343049415079 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.2037044886195058 wick_frac=0.7962955113804943 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=10.363573311138854 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:GREEN:body=+0.4600:wick=0.5000; 2026-08-21:RED:body=-0.8500:wick=0.5380; 2026-08-24:DOJI:body=+0.0000:wick=1.1200; 2026-08-25:RED:body=-0.1100:wick=0.4300; 2026-08-26:GREEN:body=+1.1400:wick=0.2250 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=12.84 (current export asof; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=9.89 (current export; earnings_date=8/5/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1529.5 | **NEUTRAL** |
| `B04_income` | 434.53 | **GOOD** |
| `B05_profit_margin` | 28.41 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.00999999999999801 (now=36.26 vs prior_export=36.25 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 3.05 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | -1.67 | **BAD** |
| `B13_short_float` | 5.42 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=12.84 (this export) | prior_export=12.84 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=9.89 (this export) | prior_export=9.89 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### ABM  ·  score **+16**  ·  Specialty Business Services
price=47.77000045776367  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.97 on 2026-08-26; prev RSI=44.25 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 44.25@2026-08-25 → 54.97@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 44.25@2026-08-25 → 54.97@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 44.25@2026-08-25 → 54.97@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=3.000 (G=0.9600 R=0.3200); 2026-08-25:RED:O=46.9100,C=46.5900,body=-0.3200,vol=295200.0; 2026-08-26:GREEN:O=46.8100,C=47.7700,body=+0.9600,vol=451016.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.528 (Gvol=451016 Rvol=295200); 2026-08-25:RED:O=46.9100,C=46.5900,body=-0.3200,vol=295200.0; 2026-08-26:GREEN:O=46.8100,C=47.7700,body=+0.9600,vol=451016.0 | **GOOD** |
| `A07_rvol` | RVOL=1.210 on 2026-08-26: today_vol=451016 / avg20=372865 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.108 on 2026-08-26 (price=47.7700, mid=47.9475, upper=49.5838, lower=46.3112; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=47.7700 vs SMA50=46.3784 dist=+3.00% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=47.9475 SMA50=46.3784 SMA80=44.0115 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-01@38.1500; S2[2026-06-24→2026-07-27] low=2026-07-02@43.6100; S3[2026-07-28→2026-08-26] low=2026-08-25@46.2400 | lows=[38.14998152536855, 43.61000061035156, 46.2400016784668] span=21.21% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.7406825305726017 wick_frac=0.25931746942739836 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.4776127900885923 wick_frac=0.5223872099114076 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=3.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-20:GREEN:body=+0.1700:wick=0.9700; 2026-08-21:RED:body=-0.6300:wick=0.0400; 2026-08-24:RED:body=-0.3200:wick=0.4600; 2026-08-25:RED:body=-0.3200:wick=0.3500; 2026-08-26:GREEN:body=+0.9600:wick=0.3361 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=2.3 (current export asof; earnings_date=9/8/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=3.4 (current export; earnings_date=9/8/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 9052.8 | **NEUTRAL** |
| `B04_income` | 158.5 | **GOOD** |
| `B05_profit_margin` | 1.75 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 52.43 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=52.43 vs prior_export=52.43 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.25 | **GOOD** |
| `B10_insider_transactions` | -5.63 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-5.63 vs prior=-5.63 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.94 | **GOOD** |
| `B13_short_float` | 3.27 | **NEUTRAL** |
| `B14_earnings_date` | 9/8/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=2.3 (this export) | prior_export=2.3 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=3.4 (this export) | prior_export=3.4 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CFFN  ·  score **+16**  ·  Banks - Regional
price=8.65999984741211  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=52.30 on 2026-08-26; prev RSI=45.69 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 45.69@2026-08-25 → 52.30@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 45.69@2026-08-25 → 52.30@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 45.69@2026-08-25 → 52.30@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=2.667 (G=0.0800 R=0.0300); 2026-08-25:RED:O=8.5700,C=8.5400,body=-0.0300,vol=741600.0; 2026-08-26:GREEN:O=8.5800,C=8.6600,body=+0.0800,vol=771198.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.040 (Gvol=771198 Rvol=741600); 2026-08-25:RED:O=8.5700,C=8.5400,body=-0.0300,vol=741600.0; 2026-08-26:GREEN:O=8.5800,C=8.6600,body=+0.0800,vol=771198.0 | **GOOD** |
| `A07_rvol` | RVOL=0.888 on 2026-08-26: today_vol=771198 / avg20=868855 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.244 on 2026-08-26 (price=8.6600, mid=8.7490, upper=9.1143, lower=8.3837; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=8.6600 vs SMA50=8.5092 dist=+1.77% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=8.7490 SMA50=8.5092 SMA80=8.1960 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-01@7.5182; S2[2026-06-24→2026-07-27] low=2026-07-08@8.1126; S3[2026-07-28→2026-08-26] low=2026-07-29@8.1621 | lows=[7.51823704049521, 8.112563409381613, 8.162090576156126] span=8.56% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.4571444141689373 wick_frac=0.5428555858310626 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.1764689382804699 wick_frac=0.8235310617195302 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.666687859617891 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:GREEN:body=+0.0300:wick=0.0800; 2026-08-21:RED:body=-0.0300:wick=0.1700; 2026-08-24:RED:body=-0.0600:wick=0.0200; 2026-08-25:RED:body=-0.0300:wick=0.1400; 2026-08-26:GREEN:body=+0.0800:wick=0.0950 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=11.76 (current export asof; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=2.57 (current export; earnings_date=7/29/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 443.11 | **NEUTRAL** |
| `B04_income` | 82.73 | **GOOD** |
| `B05_profit_margin` | 18.67 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=9.5 vs prior_export=9.5 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.3 | **GOOD** |
| `B13_short_float` | 5.45 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=11.76 (this export) | prior_export=11.76 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=2.57 (this export) | prior_export=2.57 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### PAA  ·  score **+16**  ·  Oil & Gas Midstream
price=25.6299991607666  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.52 on 2026-08-26; prev RSI=62.15 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.15@2026-08-25 → 69.52@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.15@2026-08-25 → 69.52@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.15@2026-08-25 → 69.52@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=9.300 (G=0.9300 R=0.1000); 2026-08-25:RED:O=24.8000,C=24.7000,body=-0.1000,vol=2629200.0; 2026-08-26:GREEN:O=24.7000,C=25.6300,body=+0.9300,vol=2756817.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.049 (Gvol=2756817 Rvol=2629200); 2026-08-25:RED:O=24.8000,C=24.7000,body=-0.1000,vol=2629200.0; 2026-08-26:GREEN:O=24.7000,C=25.6300,body=+0.9300,vol=2756817.0 | **GOOD** |
| `A07_rvol` | RVOL=1.026 on 2026-08-26: today_vol=2756817 / avg20=2685975 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.139 on 2026-08-26 (price=25.6300, mid=23.9771, upper=25.4280, lower=22.5263; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-26: price=25.6300 vs SMA50=23.0623 dist=+11.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=23.9771 SMA50=23.0623 SMA80=22.7939 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-26 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-18@20.6070; S2[2026-06-25→2026-07-27] low=2026-06-25@20.9806; S3[2026-07-28→2026-08-26] low=2026-08-07@22.7300 | lows=[20.607006609339088, 20.980608573035216, 22.729999542236328] span=10.30% rising_lows=True flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.9253705566415517 wick_frac=0.07462944335844832 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.2499952316466078 wick_frac=0.7500047683533922 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=9.300125886930648 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:GREEN:body=+0.3300:wick=0.1600; 2026-08-21:RED:body=-0.0500:wick=0.3400; 2026-08-24:GREEN:body=+0.4100:wick=0.2400; 2026-08-25:RED:body=-0.1000:wick=0.3000; 2026-08-26:GREEN:body=+0.9300:wick=0.0750 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=7.44 (current export asof; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=39.05 (current export; earnings_date=8/7/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 52179.0 | **NEUTRAL** |
| `B04_income` | 2548.0 | **GOOD** |
| `B05_profit_margin` | 4.88 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.0 vs prior_export=25.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 2.3 | **GOOD** |
| `B10_insider_transactions` | -0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.0 vs prior=-0.0 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.12 | **GOOD** |
| `B13_short_float` | 3.0 | **NEUTRAL** |
| `B14_earnings_date` | 8/7/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=7.44 (this export) | prior_export=7.44 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=39.05 (this export) | prior_export=39.05 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### HLNE  ·  score **+16**  ·  Asset Management
price=105.2699966430664  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.95 on 2026-08-26; prev RSI=66.59 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.59@2026-08-25 → 62.95@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.59@2026-08-25 → 62.95@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.59@2026-08-25 → 62.95@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=1.740 (G=1.8100 R=1.0400); 2026-08-25:GREEN:O=105.0200,C=106.8300,body=+1.8100,vol=477600.0; 2026-08-26:RED:O=106.3100,C=105.2700,body=-1.0400,vol=459341.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.040 (Gvol=477600 Rvol=459341); 2026-08-25:GREEN:O=105.0200,C=106.8300,body=+1.8100,vol=477600.0; 2026-08-26:RED:O=106.3100,C=105.2700,body=-1.0400,vol=459341.0 | **GOOD** |
| `A07_rvol` | RVOL=0.643 on 2026-08-26: today_vol=459341 / avg20=713985 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.371 on 2026-08-26 (price=105.2700, mid=101.3935, upper=111.8564, lower=90.9306; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=105.2700 vs SMA50=89.2516 dist=+17.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=101.3935 SMA50=89.2516 SMA80=88.0811 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-22→2026-08-26 (63 bars); S1[2026-05-22→2026-06-23] low=2026-06-23@74.9600; S2[2026-06-24→2026-07-27] low=2026-06-24@71.8800; S3[2026-07-28→2026-08-26] low=2026-07-30@85.7800 | lows=[74.95999908447266, 71.87999725341797, 85.77999877929688] span=19.34% rising_lows=False flatish(≤12%)=False | **NEUTRAL** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.5142053951891524 wick_frac=0.4857946048108476 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.3322689546745317 wick_frac=0.6677310453254683 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.7403880717455893 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-0.7400:wick=2.2800; 2026-08-21:RED:body=-0.2000:wick=1.9900; 2026-08-24:RED:body=-0.0100:wick=2.9600; 2026-08-25:GREEN:body=+1.8100:wick=1.7100; 2026-08-26:RED:body=-1.0400:wick=2.0900 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=26.41 (current export asof; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=24.05 (current export; earnings_date=8/4/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 858.37 | **NEUTRAL** |
| `B04_income` | 275.89 | **GOOD** |
| `B05_profit_margin` | 32.14 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 135.14 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=135.14 vs prior_export=135.14 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.38 | **GOOD** |
| `B10_insider_transactions` | 1.13 | **GOOD** |
| `B11_insider_tx_delta` | delta=-0.07000000000000006 (now=1.13 vs prior=1.2 on finviz_2026-08-25) | **BAD** |
| `B12_institutional_transactions` | 6.61 | **GOOD** |
| `B13_short_float` | 11.3 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=26.41 (this export) | prior_export=26.41 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=24.05 (this export) | prior_export=24.05 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### IOT  ·  score **+15**  ·  Software - Infrastructure
price=39.84000015258789  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.44 on 2026-08-26; prev RSI=53.11 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.11@2026-08-25 → 55.44@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.11@2026-08-25 → 55.44@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.11@2026-08-25 → 55.44@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=2.603 (G=1.7700 R=0.6800); 2026-08-25:RED:O=39.8200,C=39.1400,body=-0.6800,vol=4054200.0; 2026-08-26:GREEN:O=38.0700,C=39.8400,body=+1.7700,vol=3708900.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=0.915 (Gvol=3708900 Rvol=4054200); 2026-08-25:RED:O=39.8200,C=39.1400,body=-0.6800,vol=4054200.0; 2026-08-26:GREEN:O=38.0700,C=39.8400,body=+1.7700,vol=3708900.0 | **BAD** |
| `A07_rvol` | RVOL=0.739 on 2026-08-26: today_vol=3708900 / avg20=5016205 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.249 on 2026-08-26 (price=39.8400, mid=39.2285, upper=41.6860, lower=36.7710; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=39.8400 vs SMA50=36.3562 dist=+9.58% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=39.2285 SMA50=36.3562 SMA80=34.6173 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-27→2026-08-26 (63 bars); S1[2026-05-27→2026-06-25] low=2026-06-25@28.7700; S2[2026-06-26→2026-07-27] low=2026-06-26@29.1100; S3[2026-07-28→2026-08-26] low=2026-07-28@35.5600 | lows=[28.770000457763672, 29.110000610351562, 35.560001373291016] span=23.60% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.7424810977317278 wick_frac=0.2575189022682722 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.30088531424858006 wick_frac=0.69911468575142 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=2.602940681484141 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:RED:body=-0.2700:wick=0.9900; 2026-08-21:GREEN:body=+0.9400:wick=1.0590; 2026-08-24:GREEN:body=+0.9300:wick=0.9100; 2026-08-25:RED:body=-0.6800:wick=1.5800; 2026-08-26:GREEN:body=+1.7700:wick=0.6139 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=30.57 (current export asof; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=5.19 (current export; earnings_date=9/3/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 1730.6 | **NEUTRAL** |
| `B04_income` | 57.51 | **GOOD** |
| `B05_profit_margin` | 3.32 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.66 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.19999999999999574 (now=45.66 vs prior_export=45.46 on finviz_2026-08-25) | **GOOD** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | -3.11 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-3.11 vs prior=-3.11 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.99 | **GOOD** |
| `B13_short_float` | 7.99 | **NEUTRAL** |
| `B14_earnings_date` | 9/3/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=30.57 (this export) | prior_export=30.57 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=5.19 (this export) | prior_export=5.19 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### VYX  ·  score **+15**  ·  Information Technology Services
price=8.880000114440918  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.83 on 2026-08-26; prev RSI=51.36 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 51.36@2026-08-25 → 60.83@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 51.36@2026-08-25 → 60.83@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 51.36@2026-08-25 → 60.83@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=99.000 (G=1.0200 R=0.0000); 2026-08-25:GREEN:O=7.9600,C=8.1300,body=+0.1700,vol=1667500.0; 2026-08-26:GREEN:O=8.0300,C=8.8800,body=+0.8500,vol=3183173.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=99.000 (Gvol=4850673 Rvol=0); 2026-08-25:GREEN:O=7.9600,C=8.1300,body=+0.1700,vol=1667500.0; 2026-08-26:GREEN:O=8.0300,C=8.8800,body=+0.8500,vol=3183173.0 | **GOOD** |
| `A07_rvol` | RVOL=1.461 on 2026-08-26: today_vol=3183173 / avg20=2178465 (avg window 2026-07-28→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.650 on 2026-08-26 (price=8.8800, mid=8.2355, upper=9.2266, lower=7.2444; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=8.8800 vs SMA50=8.0546 dist=+10.25% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-26: SMA20=8.2355 SMA50=8.0546 SMA80=7.6764 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-26 (63 bars); S1[2026-05-26→2026-06-24] low=2026-05-28@6.4600; S2[2026-06-25→2026-07-27] low=2026-07-23@7.3700; S3[2026-07-28→2026-08-26] low=2026-08-20@7.5100 | lows=[6.460000038146973, 7.369999885559082, 7.510000228881836] span=16.25% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.7083334911407748 wick_frac=0.2916665088592251 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=nan wick_frac=nan | **NEUTRAL** |
| `A15_tape_recovery_setup` | body_rg_2d=99.0 need>1.4; red_wick_gt_green=False 5d trail=2026-08-20:GREEN:body=+0.0200:wick=0.2000; 2026-08-21:GREEN:body=+0.3000:wick=0.2100; 2026-08-24:GREEN:body=+0.0100:wick=0.3100; 2026-08-25:GREEN:body=+0.1700:wick=0.1500; 2026-08-26:GREEN:body=+0.8500:wick=0.1100 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=13.33 (current export asof; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=1.37 (current export; earnings_date=8/5/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 2533.0 | **NEUTRAL** |
| `B04_income` | 58.0 | **GOOD** |
| `B05_profit_margin` | 2.29 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 12.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=12.96 vs prior_export=12.96 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | -0.83 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.83 vs prior=-0.83 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.47 | **GOOD** |
| `B13_short_float` | 20.64 | **GOOD** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=13.33 (this export) | prior_export=13.33 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.37 (this export) | prior_export=1.37 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

### CRM  ·  score **+15**  ·  Software - Application
price=205.6199951171875  pair=`2026-08-25→2026-08-26`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.33 on 2026-08-26; prev RSI=63.41 on 2026-08-25 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 63.41@2026-08-25 → 63.33@2026-08-26 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 63.41@2026-08-25 → 63.33@2026-08-26 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 63.41@2026-08-25 → 63.33@2026-08-26 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_body_sum/RED_body_sum=35.499 (G=5.6800 R=0.1600); 2026-08-25:RED:O=205.8500,C=205.6900,body=-0.1600,vol=10081800.0; 2026-08-26:GREEN:O=199.9400,C=205.6200,body=+5.6800,vol=14051371.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-25 + 2026-08-26; ratio=GREEN_vol/RED_vol=1.394 (Gvol=14051371 Rvol=10081800); 2026-08-25:RED:O=205.8500,C=205.6900,body=-0.1600,vol=10081800.0; 2026-08-26:GREEN:O=199.9400,C=205.6200,body=+5.6800,vol=14051371.0 | **GOOD** |
| `A07_rvol` | RVOL=1.137 on 2026-08-26: today_vol=14051371 / avg20=12355910 (avg window 2026-07-27→2026-08-25, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.543 on 2026-08-26 (price=205.6200, mid=195.5490, upper=214.0978, lower=177.0002; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-26: price=205.6200 vs SMA50=175.9312 dist=+16.88% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=195.55_50=175.93_80=177.64 on 2026-08-26: SMA20=195.5490 SMA50=175.9312 SMA80=177.6361 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-26→2026-08-26 (63 bars); S1[2026-05-26→2026-06-24] low=2026-06-22@146.3200; S2[2026-06-25→2026-07-24] low=2026-06-25@148.7800; S3[2026-07-27→2026-08-26] low=2026-07-27@167.1400 | lows=[146.32000732421875, 148.77999877929688, 167.13999938964844] span=14.23% rising_lows=True flatish(≤12%)=False | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: GREEN body_frac=0.7583429252442118 wick_frac=0.24165707475578826 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-25+2026-08-26: RED body_frac=0.0288294728160011 wick_frac=0.9711705271839989 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=35.49914171275987 need>1.4; red_wick_gt_green=True 5d trail=2026-08-20:GREEN:body=+0.2100:wick=4.6700; 2026-08-21:GREEN:body=+3.5900:wick=2.7500; 2026-08-24:GREEN:body=+0.6400:wick=5.1400; 2026-08-25:RED:body=-0.1600:wick=5.3900; 2026-08-26:GREEN:body=+5.6800:wick=1.8100 | **GOOD** |
| `B01_eps_surprise` | EPS surprise=80.49 (current export asof; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.13 (current export; earnings_date=8/26/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 42829.0 | **NEUTRAL** |
| `B04_income` | 8023.0 | **GOOD** |
| `B05_profit_margin` | 18.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 239.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=239.86 vs prior_export=239.86 on finviz_2026-08-25) | **NEUTRAL** |
| `B09_analyst_recom` | 1.7 | **GOOD** |
| `B10_insider_transactions` | 0.02 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.02 vs prior=0.02 on finviz_2026-08-25) | **NEUTRAL** |
| `B12_institutional_transactions` | -6.15 | **BAD** |
| `B13_short_float` | 3.34 | **NEUTRAL** |
| `B14_earnings_date` | 8/26/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=80.49 (this export) | prior_export=24.05 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.13 (this export) | prior_export=0.72 (finviz_2026-08-25) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-27_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.