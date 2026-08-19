# A+B1 Feature Checklist — 2026-08-18

- Gate: Market Cap > $80M · ADV > 500,000 shares → **2,698** names
- Export: `finviz_2026-08-18.csv` · prior export for Δ: `2026-08-17`
- score = sum of flags over **27** features

## How dates work

- **A05/A06/A12/A13** use the last **10 trading sessions** ending on the latest OHLC bar on/before `2026-08-18`.
- Each session is listed as `YYYY-MM-DD:COLOR:O=…,C=…,body=±…` (body) or with volume/wicks.
- **GREEN dates** / **RED dates** are the exact session dates that entered the ratio numerator/denominator.
- **A07 RVOL** = volume on as-of session / mean volume of the prior **20** sessions (excludes today).
- **A11 max DD** scans the last **42** sessions and reports peak date+price and trough date+price.
- **B08/B11 deltas** compare current Finviz export vs the previous dated export file.

## Ranked (top 20)

| Rank | Ticker | score | good | bad | Industry |
|-----:|--------|------:|-----:|----:|----------|
| 1 | FLS | +16 | 16 | 0 | Specialty Industrial Machinery |
| 2 | BNL | +15 | 15 | 0 | REIT - Diversified |
| 3 | CLX | +14 | 14 | 0 | Household & Personal Products |
| 4 | HRI | +14 | 15 | 1 | Rental & Leasing Services |
| 5 | LIVN | +13 | 14 | 1 | Medical Devices |
| 6 | AMGN | +13 | 15 | 2 | Drug Manufacturers - General |
| 7 | EVTC | +13 | 14 | 1 | Software - Infrastructure |
| 8 | GAU | +13 | 14 | 1 | Gold |
| 9 | DCTH | +13 | 14 | 1 | Medical Devices |
| 10 | MH | +13 | 14 | 1 | Education & Training Services |
| 11 | EPD | +13 | 14 | 1 | Oil & Gas Midstream |
| 12 | SONO | +13 | 14 | 1 | Consumer Electronics |
| 13 | RPM | +13 | 13 | 0 | Specialty Chemicals |
| 14 | DSGX | +12 | 12 | 0 | Software - Application |
| 15 | SN | +12 | 14 | 2 | Furnishings, Fixtures & Appliances |
| 16 | BLK | +12 | 13 | 1 | Asset Management |
| 17 | KBR | +12 | 13 | 1 | Engineering & Construction |
| 18 | PHR | +12 | 13 | 1 | Health Information Services |
| 19 | WTTR | +12 | 14 | 2 | Oil & Gas Equipment & Services |
| 20 | BKR | +12 | 13 | 1 | Oil & Gas Equipment & Services |

## Full checklist with dates — top 20

### FLS  ·  score **+16**  ·  Specialty Industrial Machinery
price=80.87000274658203  mcap=$10.07B  ADV=1,963,300
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-07,2026-08-11,2026-08-13]  RED=[2026-08-05,2026-08-06,2026-08-10,2026-08-12,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.43 on 2026-08-14; prev RSI=64.40 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.40@2026-08-13 → 63.43@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.40@2026-08-13 → 63.43@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.40@2026-08-13 → 63.43@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.657 (= sum GREEN bodies / sum RED bodies); GREEN_sum=5.5500 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-11,2026-08-13]; RED_sum=3.3500 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-12,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=74.9800,C=76.5400,body=+1.5600 | 2026-08-04:GREEN:O=77.7500,C=79.5500,body=+1.8000 | 2026-08-05:RED:O=79.3500,C=78.6700,body=-0.6800 | 2026-08-06:RED:O=79.5700,C=78.6400,body=-0.9300 | 2026-08-07:GREEN:O=79.2000,C=79.9500,body=+0.7500 | 2026-08-10:RED:O=79.9700,C=79.4900,body=-0.4800 | 2026-08-11:GREEN:O=79.8100,C=80.8600,body=+1.0500 | 2026-08-12:RED:O=81.7000,C=80.9500,body=-0.7500 | 2026-08-13:GREEN:O=80.7600,C=81.1500,body=+0.3900 | 2026-08-14:RED:O=81.3800,C=80.8700,body=-0.5100 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.115 if finite else n/a; GREEN_vol_sum=7816500 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-11,2026-08-13]; RED_vol_sum=7011500 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-12,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=2123900 | 2026-08-04:GREEN:vol=2270400 | 2026-08-05:RED:vol=1615100 | 2026-08-06:RED:vol=1605600 | 2026-08-07:GREEN:vol=1121200 | 2026-08-10:RED:vol=1020100 | 2026-08-11:GREEN:vol=999700 | 2026-08-12:RED:vol=1425800 | 2026-08-13:GREEN:vol=1301300 | 2026-08-14:RED:vol=1344900 | **GOOD** |
| `A07_rvol` | RVOL=0.739 on 2026-08-14: today_vol=1344900 / avg20=1820275 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.564 on 2026-08-14 (price=80.8700, mid=75.2755, upper=85.1912, lower=65.3598; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=80.8700 vs SMA50=74.9115 dist=+7.95% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=75.2755 SMA50=74.9115 SMA80=74.6133 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-18.07% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-06-18 @ 81.4603 → trough 2026-07-20 @ 66.7400 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-11,2026-08-13] body_frac=0.515 wick_frac=0.485; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=1.5600,upperW=0.4800,lowerW=0.0000,range=2.0400 | 2026-08-04:GREEN:body=1.8000,upperW=0.4700,lowerW=1.0900,range=3.3600 | 2026-08-05:RED:body=0.6800,upperW=0.6500,lowerW=0.0500,range=1.3800 | 2026-08-06:RED:body=0.9300,upperW=2.7700,lowerW=0.3800,range=4.0800 | 2026-08-07:GREEN:body=0.7500,upperW=0.3000,lowerW=0.7200,range=1.7700 | 2026-08-10:RED:body=0.4800,upperW=0.0200,lowerW=0.3200,range=0.8200 | 2026-08-11:GREEN:body=1.0500,upperW=0.5600,lowerW=0.0500,range=1.6600 | 2026-08-12:RED:body=0.7500,upperW=0.0500,lowerW=1.5100,range=2.3100 | 2026-08-13:GREEN:body=0.3900,upperW=0.2500,lowerW=1.3100,range=1.9500 | 2026-08-14:RED:body=0.5100,upperW=0.7200,lowerW=0.9800,range=2.2100 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-12,2026-08-14] body_frac=0.310 wick_frac=0.690; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=1.5600,upperW=0.4800,lowerW=0.0000,range=2.0400 | 2026-08-04:GREEN:body=1.8000,upperW=0.4700,lowerW=1.0900,range=3.3600 | 2026-08-05:RED:body=0.6800,upperW=0.6500,lowerW=0.0500,range=1.3800 | 2026-08-06:RED:body=0.9300,upperW=2.7700,lowerW=0.3800,range=4.0800 | 2026-08-07:GREEN:body=0.7500,upperW=0.3000,lowerW=0.7200,range=1.7700 | 2026-08-10:RED:body=0.4800,upperW=0.0200,lowerW=0.3200,range=0.8200 | 2026-08-11:GREEN:body=1.0500,upperW=0.5600,lowerW=0.0500,range=1.6600 | 2026-08-12:RED:body=0.7500,upperW=0.0500,lowerW=1.5100,range=2.3100 | 2026-08-13:GREEN:body=0.3900,upperW=0.2500,lowerW=1.3100,range=1.9500 | 2026-08-14:RED:body=0.5100,upperW=0.7200,lowerW=0.9800,range=2.2100 | **GOOD** |
| `B01_eps_surprise` | 10.45 | **GOOD** |
| `B02_revenue_surprise` | 0.91 | **GOOD** |
| `B03_sales` | 4634.07 | **NEUTRAL** |
| `B04_income` | 371.27 | **GOOD** |
| `B05_profit_margin` | 8.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 89.1 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=89.1 vs prior_export=89.1 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.86 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=1.34 (now=0.43 vs prior=-0.91 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | 4.34 | **GOOD** |
| `B13_short_float` | 6.39 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |

### BNL  ·  score **+15**  ·  REIT - Diversified
price=21.389999389648438  mcap=$4.03B  ADV=2,015,940
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-07,2026-08-11,2026-08-12,2026-08-13,2026-08-14]  RED=[2026-08-03,2026-08-04,2026-08-05,2026-08-06,2026-08-10]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=50.85 on 2026-08-14; prev RSI=48.30 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs level 50 | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.30@2026-08-13 → 50.85@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=0.989 (= sum GREEN bodies / sum RED bodies); GREEN_sum=0.8800 dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_sum=0.8900 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-06,2026-08-10]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:RED:O=21.3900,C=21.2600,body=-0.1300 | 2026-08-04:RED:O=21.2300,C=21.1600,body=-0.0700 | 2026-08-05:RED:O=21.2200,C=21.2000,body=-0.0200 | 2026-08-06:RED:O=21.4600,C=21.1600,body=-0.3000 | 2026-08-07:GREEN:O=20.8800,C=20.9300,body=+0.0500 | 2026-08-10:RED:O=20.8300,C=20.4600,body=-0.3700 | 2026-08-11:GREEN:O=20.5400,C=20.6500,body=+0.1100 | 2026-08-12:GREEN:O=20.6800,C=21.0700,body=+0.3900 | 2026-08-13:GREEN:O=21.0600,C=21.2400,body=+0.1800 | 2026-08-14:GREEN:O=21.2400,C=21.3900,body=+0.1500 | **NEUTRAL** |
| `A06_volume_red_green_ratio` | ratio=1.304 if finite else n/a; GREEN_vol_sum=15594000 dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_vol_sum=11960700 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-06,2026-08-10]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:vol=1783700 | 2026-08-04:RED:vol=2063100 | 2026-08-05:RED:vol=2299900 | 2026-08-06:RED:vol=1440100 | 2026-08-07:GREEN:vol=6807200 | 2026-08-10:RED:vol=4373900 | 2026-08-11:GREEN:vol=2285400 | 2026-08-12:GREEN:vol=3443800 | 2026-08-13:GREEN:vol=1317600 | 2026-08-14:GREEN:vol=1740000 | **GOOD** |
| `A07_rvol` | RVOL=0.762 on 2026-08-14: today_vol=1740000 / avg20=2284320 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.202 on 2026-08-14 (price=21.3900, mid=21.6905, upper=23.1770, lower=20.2040; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=21.3900 vs SMA50=21.1398 dist=+1.18% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=21.6905 SMA50=21.1398 SMA80=20.6293 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-10.26% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-07-17 @ 22.8000 → trough 2026-08-10 @ 20.4600 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-13,2026-08-14] body_frac=0.511 wick_frac=0.489; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=0.1300,upperW=0.2300,lowerW=0.1500,range=0.5100 | 2026-08-04:RED:body=0.0700,upperW=0.0550,lowerW=0.2200,range=0.3450 | 2026-08-05:RED:body=0.0200,upperW=0.1550,lowerW=0.1600,range=0.3350 | 2026-08-06:RED:body=0.3000,upperW=0.0700,lowerW=0.0850,range=0.4550 | 2026-08-07:GREEN:body=0.0500,upperW=0.0100,lowerW=0.3300,range=0.3900 | 2026-08-10:RED:body=0.3700,upperW=0.0550,lowerW=0.0100,range=0.4350 | 2026-08-11:GREEN:body=0.1100,upperW=0.1300,lowerW=0.1800,range=0.4200 | 2026-08-12:GREEN:body=0.3900,upperW=0.0000,lowerW=0.0000,range=0.3900 | 2026-08-13:GREEN:body=0.1800,upperW=0.1050,lowerW=0.0000,range=0.2850 | 2026-08-14:GREEN:body=0.1500,upperW=0.0700,lowerW=0.0180,range=0.2380 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-06,2026-08-10] body_frac=0.428 wick_frac=0.572; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=0.1300,upperW=0.2300,lowerW=0.1500,range=0.5100 | 2026-08-04:RED:body=0.0700,upperW=0.0550,lowerW=0.2200,range=0.3450 | 2026-08-05:RED:body=0.0200,upperW=0.1550,lowerW=0.1600,range=0.3350 | 2026-08-06:RED:body=0.3000,upperW=0.0700,lowerW=0.0850,range=0.4550 | 2026-08-07:GREEN:body=0.0500,upperW=0.0100,lowerW=0.3300,range=0.3900 | 2026-08-10:RED:body=0.3700,upperW=0.0550,lowerW=0.0100,range=0.4350 | 2026-08-11:GREEN:body=0.1100,upperW=0.1300,lowerW=0.1800,range=0.4200 | 2026-08-12:GREEN:body=0.3900,upperW=0.0000,lowerW=0.0000,range=0.3900 | 2026-08-13:GREEN:body=0.1800,upperW=0.1050,lowerW=0.0000,range=0.2850 | 2026-08-14:GREEN:body=0.1500,upperW=0.0700,lowerW=0.0180,range=0.2380 | **GOOD** |
| `B01_eps_surprise` | 9.66 | **GOOD** |
| `B02_revenue_surprise` | 0.56 | **GOOD** |
| `B03_sales` | 477.57 | **NEUTRAL** |
| `B04_income` | 140.69 | **GOOD** |
| `B05_profit_margin` | 29.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.7 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.7 vs prior_export=23.7 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.73 | **GOOD** |
| `B10_insider_transactions` | 0.04 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.04 vs prior=0.04 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.57 | **GOOD** |
| `B13_short_float` | 6.1 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |

### CLX  ·  score **+14**  ·  Household & Personal Products
price=105.69999694824219  mcap=$12.92B  ADV=2,574,980
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-13]  RED=[2026-08-06,2026-08-07,2026-08-12,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.16 on 2026-08-14; prev RSI=65.64 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 65.64@2026-08-13 → 63.16@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 65.64@2026-08-13 → 63.16@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 65.64@2026-08-13 → 63.16@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=5.829 (= sum GREEN bodies / sum RED bodies); GREEN_sum=12.7854 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-13]; RED_sum=2.1934 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=96.1962,C=97.1254,body=+0.9291 | 2026-08-04:GREEN:O=96.8881,C=103.4613,body=+6.5732 | 2026-08-05:GREEN:O=104.1038,C=104.6178,body=+0.5140 | 2026-08-06:RED:O=105.0330,C=104.3806,body=-0.6524 | 2026-08-07:RED:O=104.7068,C=103.9358,body=-0.7710 | 2026-08-10:GREEN:O=103.6294,C=106.4564,body=+2.8270 | 2026-08-11:GREEN:O=105.4679,C=107.0000,body=+1.5321 | 2026-08-12:RED:O=106.0000,C=105.7600,body=-0.2400 | 2026-08-13:GREEN:O=106.1300,C=106.5400,body=+0.4100 | 2026-08-14:RED:O=106.2300,C=105.7000,body=-0.5300 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=2.811 if finite else n/a; GREEN_vol_sum=22511800 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-13]; RED_vol_sum=8008300 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=3982600 | 2026-08-04:GREEN:vol=5292700 | 2026-08-05:GREEN:vol=3281900 | 2026-08-06:RED:vol=2014900 | 2026-08-07:RED:vol=2044100 | 2026-08-10:GREEN:vol=3553600 | 2026-08-11:GREEN:vol=2554300 | 2026-08-12:RED:vol=2013100 | 2026-08-13:GREEN:vol=3846700 | 2026-08-14:RED:vol=1936200 | **GOOD** |
| `A07_rvol` | RVOL=0.700 on 2026-08-14: today_vol=1936200 / avg20=2766115 (avg window 2026-07-15→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.548 on 2026-08-14 (price=105.7000, mid=100.2032, upper=110.2312, lower=90.1751; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=105.7000 vs SMA50=96.5587 dist=+9.47% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=100.2032 SMA50=96.5587 SMA80=94.9590 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-1.21% inside window 2026-06-12→2026-08-14 (42 sessions): peak 2026-08-11 @ 107.0000 → trough 2026-08-14 @ 105.7000 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-13] body_frac=0.576 wick_frac=0.424; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.9291,upperW=1.5222,lowerW=0.7315,range=3.1828 | 2026-08-04:GREEN:body=6.5732,upperW=0.1680,lowerW=0.0000,range=6.7412 | 2026-08-05:GREEN:body=0.5140,upperW=0.2570,lowerW=2.5008,range=3.2718 | 2026-08-06:RED:body=0.6524,upperW=1.4135,lowerW=0.4151,range=2.4810 | 2026-08-07:RED:body=0.7710,upperW=1.1466,lowerW=0.3756,range=2.2932 | 2026-08-10:GREEN:body=2.8270,upperW=0.3558,lowerW=0.6820,range=3.8648 | 2026-08-11:GREEN:body=1.5321,upperW=0.3460,lowerW=0.2570,range=2.1351 | 2026-08-12:RED:body=0.2400,upperW=1.0200,lowerW=0.4200,range=1.6800 | 2026-08-13:GREEN:body=0.4100,upperW=0.9100,lowerW=1.6800,range=3.0000 | 2026-08-14:RED:body=0.5300,upperW=0.3000,lowerW=1.0300,range=1.8600 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-14] body_frac=0.264 wick_frac=0.736; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.9291,upperW=1.5222,lowerW=0.7315,range=3.1828 | 2026-08-04:GREEN:body=6.5732,upperW=0.1680,lowerW=0.0000,range=6.7412 | 2026-08-05:GREEN:body=0.5140,upperW=0.2570,lowerW=2.5008,range=3.2718 | 2026-08-06:RED:body=0.6524,upperW=1.4135,lowerW=0.4151,range=2.4810 | 2026-08-07:RED:body=0.7710,upperW=1.1466,lowerW=0.3756,range=2.2932 | 2026-08-10:GREEN:body=2.8270,upperW=0.3558,lowerW=0.6820,range=3.8648 | 2026-08-11:GREEN:body=1.5321,upperW=0.3460,lowerW=0.2570,range=2.1351 | 2026-08-12:RED:body=0.2400,upperW=1.0200,lowerW=0.4200,range=1.6800 | 2026-08-13:GREEN:body=0.4100,upperW=0.9100,lowerW=1.6800,range=3.0000 | 2026-08-14:RED:body=0.5300,upperW=0.3000,lowerW=1.0300,range=1.8600 | **GOOD** |
| `B01_eps_surprise` | 1.18 | **GOOD** |
| `B02_revenue_surprise` | 1.73 | **GOOD** |
| `B03_sales` | 6720.0 | **NEUTRAL** |
| `B04_income` | 587.0 | **GOOD** |
| `B05_profit_margin` | 8.74 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 97.13 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=97.13 vs prior_export=97.13 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 3.19 | **NEUTRAL** |
| `B10_insider_transactions` | 0.68 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.13 (now=0.68 vs prior=0.55 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | 0.92 | **GOOD** |
| `B13_short_float` | 11.25 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |

### HRI  ·  score **+14**  ·  Rental & Leasing Services
price=173.50999450683594  mcap=$5.58B  ADV=541,870
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-14]  RED=[2026-08-05,2026-08-06,2026-08-07,2026-08-12,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=64.53 on 2026-08-14; prev RSI=61.04 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.04@2026-08-13 → 64.53@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.04@2026-08-13 → 64.53@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.04@2026-08-13 → 64.53@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=3.389 (= sum GREEN bodies / sum RED bodies); GREEN_sum=30.2300 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-14]; RED_sum=8.9200 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-12,2026-08-13]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=152.3900,C=163.9300,body=+11.5400 | 2026-08-04:GREEN:O=165.9500,C=170.8000,body=+4.8500 | 2026-08-05:RED:O=171.4900,C=168.8300,body=-2.6600 | 2026-08-06:RED:O=168.1900,C=168.1400,body=-0.0500 | 2026-08-07:RED:O=169.2900,C=167.9000,body=-1.3900 | 2026-08-10:GREEN:O=167.1600,C=169.7600,body=+2.6000 | 2026-08-11:GREEN:O=165.0200,C=171.0300,body=+6.0100 | 2026-08-12:RED:O=172.5900,C=168.5400,body=-4.0500 | 2026-08-13:RED:O=169.4800,C=168.7100,body=-0.7700 | 2026-08-14:GREEN:O=168.2800,C=173.5100,body=+5.2300 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.221 if finite else n/a; GREEN_vol_sum=2138600 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-14]; RED_vol_sum=1751400 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-12,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=655300 | 2026-08-04:GREEN:vol=654700 | 2026-08-05:RED:vol=580200 | 2026-08-06:RED:vol=306500 | 2026-08-07:RED:vol=311200 | 2026-08-10:GREEN:vol=335900 | 2026-08-11:GREEN:vol=285700 | 2026-08-12:RED:vol=289200 | 2026-08-13:RED:vol=264300 | 2026-08-14:GREEN:vol=207000 | **GOOD** |
| `A07_rvol` | RVOL=0.385 on 2026-08-14: today_vol=207000 / avg20=538100 (avg window 2026-07-15→2026-08-13, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.635 on 2026-08-14 (price=173.5100, mid=160.7590, upper=180.8407, lower=140.6773; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=173.5100 vs SMA50=150.6740 dist=+15.16% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=160.7590 SMA50=150.6740 SMA80=142.6217 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-12→2026-08-14 (42 sessions): peak 2026-08-14 @ 173.5100 → trough 2026-08-14 @ 173.5100 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-14] body_frac=0.630 wick_frac=0.370; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=11.5400,upperW=0.1500,lowerW=5.3300,range=17.0200 | 2026-08-04:GREEN:body=4.8500,upperW=1.7000,lowerW=3.0200,range=9.5700 | 2026-08-05:RED:body=2.6600,upperW=1.4800,lowerW=0.8100,range=4.9500 | 2026-08-06:RED:body=0.0500,upperW=1.5500,lowerW=2.2800,range=3.8800 | 2026-08-07:RED:body=1.3900,upperW=2.9200,lowerW=3.6800,range=7.9900 | 2026-08-10:GREEN:body=2.6000,upperW=1.4500,lowerW=0.6700,range=4.7200 | 2026-08-11:GREEN:body=6.0100,upperW=4.9600,lowerW=0.0000,range=10.9700 | 2026-08-12:RED:body=4.0500,upperW=0.0000,lowerW=1.5000,range=5.5500 | 2026-08-13:RED:body=0.7700,upperW=1.5200,lowerW=3.1500,range=5.4400 | 2026-08-14:GREEN:body=5.2300,upperW=0.5100,lowerW=0.0000,range=5.7400 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-12,2026-08-13] body_frac=0.321 wick_frac=0.679; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=11.5400,upperW=0.1500,lowerW=5.3300,range=17.0200 | 2026-08-04:GREEN:body=4.8500,upperW=1.7000,lowerW=3.0200,range=9.5700 | 2026-08-05:RED:body=2.6600,upperW=1.4800,lowerW=0.8100,range=4.9500 | 2026-08-06:RED:body=0.0500,upperW=1.5500,lowerW=2.2800,range=3.8800 | 2026-08-07:RED:body=1.3900,upperW=2.9200,lowerW=3.6800,range=7.9900 | 2026-08-10:GREEN:body=2.6000,upperW=1.4500,lowerW=0.6700,range=4.7200 | 2026-08-11:GREEN:body=6.0100,upperW=4.9600,lowerW=0.0000,range=10.9700 | 2026-08-12:RED:body=4.0500,upperW=0.0000,lowerW=1.5000,range=5.5500 | 2026-08-13:RED:body=0.7700,upperW=1.5200,lowerW=3.1500,range=5.4400 | 2026-08-14:GREEN:body=5.2300,upperW=0.5100,lowerW=0.0000,range=5.7400 | **GOOD** |
| `B01_eps_surprise` | 87.15 | **GOOD** |
| `B02_revenue_surprise` | 4.15 | **GOOD** |
| `B03_sales` | 4856.0 | **NEUTRAL** |
| `B04_income` | 49.0 | **GOOD** |
| `B05_profit_margin` | 1.01 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 196.3 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=196.3 vs prior_export=196.3 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.43 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.010000000000000009 (now=0.43 vs prior=0.42 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | 8.21 | **GOOD** |
| `B13_short_float` | 6.17 | **NEUTRAL** |
| `B14_earnings_date` | 7/28/2026 8:30:00 AM | **NEUTRAL** |

### LIVN  ·  score **+13**  ·  Medical Devices
price=82.22000122070312  mcap=$4.40B  ADV=718,320
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]  RED=[2026-08-05,2026-08-06]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.89 on 2026-08-14; prev RSI=55.84 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.84@2026-08-13 → 55.89@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.84@2026-08-13 → 55.89@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.84@2026-08-13 → 55.89@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=3.609 (= sum GREEN bodies / sum RED bodies); GREEN_sum=12.6500 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_sum=3.5050 dates=[2026-08-05,2026-08-06]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=80.4350,C=82.4600,body=+2.0250 | 2026-08-04:GREEN:O=82.9500,C=85.3250,body=+2.3750 | 2026-08-05:RED:O=79.8800,C=78.1450,body=-1.7350 | 2026-08-06:RED:O=77.5500,C=75.7800,body=-1.7700 | 2026-08-07:GREEN:O=75.4300,C=76.2200,body=+0.7900 | 2026-08-10:GREEN:O=75.0400,C=76.4800,body=+1.4400 | 2026-08-11:GREEN:O=76.5500,C=78.5800,body=+2.0300 | 2026-08-12:GREEN:O=77.9500,C=80.7800,body=+2.8300 | 2026-08-13:GREEN:O=81.6700,C=82.2000,body=+0.5300 | 2026-08-14:GREEN:O=81.5900,C=82.2200,body=+0.6300 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=2.166 if finite else n/a; GREEN_vol_sum=6053100 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_vol_sum=2795000 dates=[2026-08-05,2026-08-06]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=660600 | 2026-08-04:GREEN:vol=1284900 | 2026-08-05:RED:vol=1862500 | 2026-08-06:RED:vol=932500 | 2026-08-07:GREEN:vol=745000 | 2026-08-10:GREEN:vol=473200 | 2026-08-11:GREEN:vol=673300 | 2026-08-12:GREEN:vol=979100 | 2026-08-13:GREEN:vol=677600 | 2026-08-14:GREEN:vol=559400 | **GOOD** |
| `A07_rvol` | RVOL=0.755 on 2026-08-14: today_vol=559400 / avg20=740495 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.444 on 2026-08-14 (price=82.2200, mid=80.0625, upper=84.9265, lower=75.1985; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=82.2200 vs SMA50=79.6434 dist=+3.24% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=80.0625 SMA50=79.6434 SMA80=75.2878 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-11.19% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-04 @ 85.3250 → trough 2026-08-06 @ 75.7800 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14] body_frac=0.570 wick_frac=0.430; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=2.0250,upperW=0.0800,lowerW=0.0000,range=2.1050 | 2026-08-04:GREEN:body=2.3750,upperW=0.4350,lowerW=0.8700,range=3.6800 | 2026-08-05:RED:body=1.7350,upperW=1.9750,lowerW=3.0750,range=6.7850 | 2026-08-06:RED:body=1.7700,upperW=0.6300,lowerW=0.5000,range=2.9000 | 2026-08-07:GREEN:body=0.7900,upperW=0.1200,lowerW=0.7600,range=1.6700 | 2026-08-10:GREEN:body=1.4400,upperW=0.5150,lowerW=0.0000,range=1.9550 | 2026-08-11:GREEN:body=2.0300,upperW=0.5600,lowerW=0.8650,range=3.4550 | 2026-08-12:GREEN:body=2.8300,upperW=0.7750,lowerW=1.2300,range=4.8350 | 2026-08-13:GREEN:body=0.5300,upperW=0.5000,lowerW=1.0900,range=2.1200 | 2026-08-14:GREEN:body=0.6300,upperW=0.1200,lowerW=1.6100,range=2.3600 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06] body_frac=0.362 wick_frac=0.638; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=2.0250,upperW=0.0800,lowerW=0.0000,range=2.1050 | 2026-08-04:GREEN:body=2.3750,upperW=0.4350,lowerW=0.8700,range=3.6800 | 2026-08-05:RED:body=1.7350,upperW=1.9750,lowerW=3.0750,range=6.7850 | 2026-08-06:RED:body=1.7700,upperW=0.6300,lowerW=0.5000,range=2.9000 | 2026-08-07:GREEN:body=0.7900,upperW=0.1200,lowerW=0.7600,range=1.6700 | 2026-08-10:GREEN:body=1.4400,upperW=0.5150,lowerW=0.0000,range=1.9550 | 2026-08-11:GREEN:body=2.0300,upperW=0.5600,lowerW=0.8650,range=3.4550 | 2026-08-12:GREEN:body=2.8300,upperW=0.7750,lowerW=1.2300,range=4.8350 | 2026-08-13:GREEN:body=0.5300,upperW=0.5000,lowerW=1.0900,range=2.1200 | 2026-08-14:GREEN:body=0.6300,upperW=0.1200,lowerW=1.6100,range=2.3600 | **GOOD** |
| `B01_eps_surprise` | 15.12 | **GOOD** |
| `B02_revenue_surprise` | 2.65 | **GOOD** |
| `B03_sales` | 1471.5 | **NEUTRAL** |
| `B04_income` | 188.54 | **GOOD** |
| `B05_profit_margin` | 12.81 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 91.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=91.0 vs prior_export=91.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -2.39 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.39 vs prior=-2.39 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.41 | **GOOD** |
| `B13_short_float` | 7.55 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |

### AMGN  ·  score **+13**  ·  Drug Manufacturers - General
price=415.2099914550781  mcap=$229.53B  ADV=2,703,750
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14]  RED=[2026-08-03,2026-08-06,2026-08-11,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.86 on 2026-08-14; prev RSI=72.63 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 72.63@2026-08-13 → 69.86@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 72.63@2026-08-13 → 69.86@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 72.63@2026-08-13 → 69.86@2026-08-14 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.002 (= sum GREEN bodies / sum RED bodies); GREEN_sum=43.6500 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14]; RED_sum=21.8000 dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-13]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:RED:O=388.5000,C=378.8700,body=-9.6300 | 2026-08-04:GREEN:O=380.7600,C=390.0200,body=+9.2600 | 2026-08-05:GREEN:O=393.3600,C=407.8300,body=+14.4700 | 2026-08-06:RED:O=413.7200,C=404.8500,body=-8.8700 | 2026-08-07:GREEN:O=404.0900,C=410.9500,body=+6.8600 | 2026-08-10:GREEN:O=408.4600,C=417.2000,body=+8.7400 | 2026-08-11:RED:O=415.5200,C=414.3000,body=-1.2200 | 2026-08-12:GREEN:O=413.3100,C=416.1800,body=+2.8700 | 2026-08-13:RED:O=419.9200,C=417.8400,body=-2.0800 | 2026-08-14:GREEN:O=413.7600,C=415.2100,body=+1.4500 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.589 if finite else n/a; GREEN_vol_sum=17256300 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14]; RED_vol_sum=10859700 dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:vol=3443100 | 2026-08-04:GREEN:vol=3622700 | 2026-08-05:GREEN:vol=5750700 | 2026-08-06:RED:vol=2609000 | 2026-08-07:GREEN:vol=1821500 | 2026-08-10:GREEN:vol=1959700 | 2026-08-11:RED:vol=2029700 | 2026-08-12:GREEN:vol=2065500 | 2026-08-13:RED:vol=2777900 | 2026-08-14:GREEN:vol=2036200 | **GOOD** |
| `A07_rvol` | RVOL=0.748 on 2026-08-14: today_vol=2036200 / avg20=2723570 (avg window 2026-07-16→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.596 on 2026-08-14 (price=415.2100, mid=392.3550, upper=430.7153, lower=353.9947; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=415.2100 vs SMA50=369.6604 dist=+12.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=392.3550 SMA50=369.6604 SMA80=356.1460 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-0.63% inside window 2026-06-15→2026-08-14 (42 sessions): peak 2026-08-13 @ 417.8400 → trough 2026-08-14 @ 415.2100 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14] body_frac=0.572 wick_frac=0.428; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=9.6300,upperW=2.5300,lowerW=2.9100,range=15.0700 | 2026-08-04:GREEN:body=9.2600,upperW=0.9000,lowerW=4.7400,range=14.9000 | 2026-08-05:GREEN:body=14.4700,upperW=8.4100,lowerW=2.3600,range=25.2400 | 2026-08-06:RED:body=8.8700,upperW=4.6800,lowerW=3.6500,range=17.2000 | 2026-08-07:GREEN:body=6.8600,upperW=0.6500,lowerW=3.0200,range=10.5300 | 2026-08-10:GREEN:body=8.7400,upperW=0.1600,lowerW=2.0400,range=10.9400 | 2026-08-11:RED:body=1.2200,upperW=6.2700,lowerW=2.1000,range=9.5900 | 2026-08-12:GREEN:body=2.8700,upperW=2.0100,lowerW=4.3100,range=9.1900 | 2026-08-13:RED:body=2.0800,upperW=0.3400,lowerW=2.0600,range=4.4800 | 2026-08-14:GREEN:body=1.4500,upperW=0.8200,lowerW=3.2600,range=5.5300 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-13] body_frac=0.470 wick_frac=0.530; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=9.6300,upperW=2.5300,lowerW=2.9100,range=15.0700 | 2026-08-04:GREEN:body=9.2600,upperW=0.9000,lowerW=4.7400,range=14.9000 | 2026-08-05:GREEN:body=14.4700,upperW=8.4100,lowerW=2.3600,range=25.2400 | 2026-08-06:RED:body=8.8700,upperW=4.6800,lowerW=3.6500,range=17.2000 | 2026-08-07:GREEN:body=6.8600,upperW=0.6500,lowerW=3.0200,range=10.5300 | 2026-08-10:GREEN:body=8.7400,upperW=0.1600,lowerW=2.0400,range=10.9400 | 2026-08-11:RED:body=1.2200,upperW=6.2700,lowerW=2.1000,range=9.5900 | 2026-08-12:GREEN:body=2.8700,upperW=2.0100,lowerW=4.3100,range=9.1900 | 2026-08-13:RED:body=2.0800,upperW=0.3400,lowerW=2.0600,range=4.4800 | 2026-08-14:GREEN:body=1.4500,upperW=0.8200,lowerW=3.2600,range=5.5300 | **GOOD** |
| `B01_eps_surprise` | 11.97 | **GOOD** |
| `B02_revenue_surprise` | 6.66 | **GOOD** |
| `B03_sales` | 38202.0 | **NEUTRAL** |
| `B04_income` | 8743.0 | **GOOD** |
| `B05_profit_margin` | 22.89 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 389.74 | **NEUTRAL** |
| `B08_target_price_delta` | delta=3.670000000000016 (now=389.74 vs prior_export=386.07 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 2.46 | **GOOD** |
| `B10_insider_transactions` | -4.92 | **BAD** |
| `B11_insider_tx_delta` | delta=0.20000000000000018 (now=-4.92 vs prior=-5.12 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | -2.99 | **BAD** |
| `B13_short_float` | 2.46 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |

### EVTC  ·  score **+13**  ·  Software - Infrastructure
price=31.56999969482422  mcap=$1.82B  ADV=578,130
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13]  RED=[2026-08-05,2026-08-06,2026-08-07,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.17 on 2026-08-14; prev RSI=60.64 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.64@2026-08-13 → 58.17@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.259 (= sum GREEN bodies / sum RED bodies); GREEN_sum=5.1600 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_sum=4.1000 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=30.5800,C=31.1100,body=+0.5300 | 2026-08-04:GREEN:O=30.6100,C=32.5500,body=+1.9400 | 2026-08-05:RED:O=33.9900,C=31.3100,body=-2.6800 | 2026-08-06:RED:O=31.1700,C=30.9400,body=-0.2300 | 2026-08-07:RED:O=30.8800,C=29.8500,body=-1.0300 | 2026-08-10:GREEN:O=29.4900,C=29.6400,body=+0.1500 | 2026-08-11:GREEN:O=29.5000,C=29.5100,body=+0.0100 | 2026-08-12:GREEN:O=29.4500,C=30.4100,body=+0.9600 | 2026-08-13:GREEN:O=30.4100,C=31.9800,body=+1.5700 | 2026-08-14:RED:O=31.7300,C=31.5700,body=-0.1600 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.470 if finite else n/a; GREEN_vol_sum=2628900 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_vol_sum=1788600 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=387000 | 2026-08-04:GREEN:vol=616000 | 2026-08-05:RED:vol=707400 | 2026-08-06:RED:vol=271300 | 2026-08-07:RED:vol=334900 | 2026-08-10:GREEN:vol=322900 | 2026-08-11:GREEN:vol=252600 | 2026-08-12:GREEN:vol=588600 | 2026-08-13:GREEN:vol=461800 | 2026-08-14:RED:vol=475000 | **GOOD** |
| `A07_rvol` | RVOL=1.164 on 2026-08-14: today_vol=475000 / avg20=407905 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.435 on 2026-08-14 (price=31.5700, mid=30.7550, upper=32.6286, lower=28.8814; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=31.5700 vs SMA50=28.1554 dist=+12.13% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=30.7550 SMA50=28.1554 SMA80=27.5883 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-9.34% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-04 @ 32.5500 → trough 2026-08-11 @ 29.5100 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13] body_frac=0.654 wick_frac=0.346; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.5300,upperW=0.5600,lowerW=0.0000,range=1.0900 | 2026-08-04:GREEN:body=1.9400,upperW=0.1700,lowerW=0.0000,range=2.1100 | 2026-08-05:RED:body=2.6800,upperW=0.4200,lowerW=0.5300,range=3.6300 | 2026-08-06:RED:body=0.2300,upperW=0.0800,lowerW=0.2600,range=0.5700 | 2026-08-07:RED:body=1.0300,upperW=0.1900,lowerW=0.1400,range=1.3600 | 2026-08-10:GREEN:body=0.1500,upperW=0.4700,lowerW=0.3100,range=0.9300 | 2026-08-11:GREEN:body=0.0100,upperW=0.4000,lowerW=0.2400,range=0.6500 | 2026-08-12:GREEN:body=0.9600,upperW=0.1700,lowerW=0.3600,range=1.4900 | 2026-08-13:GREEN:body=1.5700,upperW=0.0500,lowerW=0.0000,range=1.6200 | 2026-08-14:RED:body=0.1600,upperW=0.2500,lowerW=0.1000,range=0.5100 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-14] body_frac=0.675 wick_frac=0.325; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.5300,upperW=0.5600,lowerW=0.0000,range=1.0900 | 2026-08-04:GREEN:body=1.9400,upperW=0.1700,lowerW=0.0000,range=2.1100 | 2026-08-05:RED:body=2.6800,upperW=0.4200,lowerW=0.5300,range=3.6300 | 2026-08-06:RED:body=0.2300,upperW=0.0800,lowerW=0.2600,range=0.5700 | 2026-08-07:RED:body=1.0300,upperW=0.1900,lowerW=0.1400,range=1.3600 | 2026-08-10:GREEN:body=0.1500,upperW=0.4700,lowerW=0.3100,range=0.9300 | 2026-08-11:GREEN:body=0.0100,upperW=0.4000,lowerW=0.2400,range=0.6500 | 2026-08-12:GREEN:body=0.9600,upperW=0.1700,lowerW=0.3600,range=1.4900 | 2026-08-13:GREEN:body=1.5700,upperW=0.0500,lowerW=0.0000,range=1.6200 | 2026-08-14:RED:body=0.1600,upperW=0.2500,lowerW=0.1000,range=0.5100 | **BAD** |
| `B01_eps_surprise` | 10.53 | **GOOD** |
| `B02_revenue_surprise` | 4.8 | **GOOD** |
| `B03_sales` | 996.16 | **NEUTRAL** |
| `B04_income` | 97.58 | **GOOD** |
| `B05_profit_margin` | 9.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 35.6 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=35.6 vs prior_export=35.6 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.17 | **GOOD** |
| `B10_insider_transactions` | 3.44 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.44 vs prior=3.44 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.34 | **GOOD** |
| `B13_short_float` | 4.11 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |

### GAU  ·  score **+13**  ·  Gold
price=2.0899999141693115  mcap=$0.53B  ADV=3,223,980
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-05,2026-08-06,2026-08-07,2026-08-11]  RED=[2026-08-10,2026-08-12,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=61.73 on 2026-08-14; prev RSI=61.73 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 61.73@2026-08-13 → 61.73@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 61.73@2026-08-13 → 61.73@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 61.73@2026-08-13 → 61.73@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.467 (= sum GREEN bodies / sum RED bodies); GREEN_sum=0.2200 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-11]; RED_sum=0.1500 dates=[2026-08-10,2026-08-12,2026-08-14]; DOJI=[2026-08-03,2026-08-04,2026-08-13]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:DOJI:O=1.7700,C=1.7700,body=+0.0000 | 2026-08-04:DOJI:O=1.8000,C=1.8000,body=+0.0000 | 2026-08-05:GREEN:O=1.8800,C=1.9300,body=+0.0500 | 2026-08-06:GREEN:O=1.9200,C=1.9500,body=+0.0300 | 2026-08-07:GREEN:O=2.0200,C=2.0600,body=+0.0400 | 2026-08-10:RED:O=2.0600,C=2.0000,body=-0.0600 | 2026-08-11:GREEN:O=2.0000,C=2.1000,body=+0.1000 | 2026-08-12:RED:O=2.1500,C=2.0900,body=-0.0600 | 2026-08-13:DOJI:O=2.0900,C=2.0900,body=+0.0000 | 2026-08-14:RED:O=2.1200,C=2.0900,body=-0.0300 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.026 if finite else n/a; GREEN_vol_sum=15059250 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-11]; RED_vol_sum=14679650 dates=[2026-08-10,2026-08-12,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:DOJI:vol=1142900 | 2026-08-04:DOJI:vol=2453900 | 2026-08-05:GREEN:vol=3132800 | 2026-08-06:GREEN:vol=2371300 | 2026-08-07:GREEN:vol=2934400 | 2026-08-10:RED:vol=3276600 | 2026-08-11:GREEN:vol=3316000 | 2026-08-12:RED:vol=5237300 | 2026-08-13:DOJI:vol=3012700 | 2026-08-14:RED:vol=2861000 | **GOOD** |
| `A07_rvol` | RVOL=0.914 on 2026-08-14: today_vol=2861000 / avg20=3130225 (avg window 2026-07-17→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.728 on 2026-08-14 (price=2.0900, mid=1.8975, upper=2.1619, lower=1.6331; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=2.0900 vs SMA50=1.9372 dist=+7.89% | **GOOD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-08-14: SMA20=1.8975 SMA50=1.9372 SMA80=2.0956 | **BAD** |
| `A11_max_downside_2m` | maxDD=-24.23% inside window 2026-06-16→2026-08-14 (42 sessions): peak 2026-06-16 @ 2.2700 → trough 2026-07-17 @ 1.7200 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-11] body_frac=0.537 wick_frac=0.463; window=2026-08-03→2026-08-14; sessions: 2026-08-03:DOJI:body=0.0000,upperW=0.0100,lowerW=0.0300,range=0.0400 | 2026-08-04:DOJI:body=0.0000,upperW=0.0400,lowerW=0.0100,range=0.0500 | 2026-08-05:GREEN:body=0.0500,upperW=0.0200,lowerW=0.0100,range=0.0800 | 2026-08-06:GREEN:body=0.0300,upperW=0.0200,lowerW=0.0200,range=0.0700 | 2026-08-07:GREEN:body=0.0400,upperW=0.0600,lowerW=0.0300,range=0.1300 | 2026-08-10:RED:body=0.0600,upperW=0.0000,lowerW=0.0600,range=0.1200 | 2026-08-11:GREEN:body=0.1000,upperW=0.0100,lowerW=0.0200,range=0.1300 | 2026-08-12:RED:body=0.0600,upperW=0.0100,lowerW=0.0300,range=0.1000 | 2026-08-13:DOJI:body=0.0000,upperW=0.0500,lowerW=0.0400,range=0.0900 | 2026-08-14:RED:body=0.0300,upperW=0.0800,lowerW=0.0000,range=0.1100 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-10,2026-08-12,2026-08-14] body_frac=0.455 wick_frac=0.545; window=2026-08-03→2026-08-14; sessions: 2026-08-03:DOJI:body=0.0000,upperW=0.0100,lowerW=0.0300,range=0.0400 | 2026-08-04:DOJI:body=0.0000,upperW=0.0400,lowerW=0.0100,range=0.0500 | 2026-08-05:GREEN:body=0.0500,upperW=0.0200,lowerW=0.0100,range=0.0800 | 2026-08-06:GREEN:body=0.0300,upperW=0.0200,lowerW=0.0200,range=0.0700 | 2026-08-07:GREEN:body=0.0400,upperW=0.0600,lowerW=0.0300,range=0.1300 | 2026-08-10:RED:body=0.0600,upperW=0.0000,lowerW=0.0600,range=0.1200 | 2026-08-11:GREEN:body=0.1000,upperW=0.0100,lowerW=0.0200,range=0.1300 | 2026-08-12:RED:body=0.0600,upperW=0.0100,lowerW=0.0300,range=0.1000 | 2026-08-13:DOJI:body=0.0000,upperW=0.0500,lowerW=0.0400,range=0.0900 | 2026-08-14:RED:body=0.0300,upperW=0.0800,lowerW=0.0000,range=0.1100 | **GOOD** |
| `B01_eps_surprise` | 135.9 | **GOOD** |
| `B02_revenue_surprise` | 21.01 | **GOOD** |
| `B03_sales` | 596.97 | **NEUTRAL** |
| `B04_income` | 72.76 | **GOOD** |
| `B05_profit_margin` | 12.19 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 4.11 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.010000000000000675 (now=4.11 vs prior_export=4.1 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 1.33 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.83 | **GOOD** |
| `B13_short_float` | 1.36 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 4:30:00 PM | **NEUTRAL** |

### DCTH  ·  score **+13**  ·  Medical Devices
price=16.729999542236328  mcap=$0.58B  ADV=564,320
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13]  RED=[2026-08-04,2026-08-05,2026-08-07,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.61 on 2026-08-14; prev RSI=76.35 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 76.35@2026-08-13 → 69.61@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.35@2026-08-13 → 69.61@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 76.35@2026-08-13 → 69.61@2026-08-14 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.558 (= sum GREEN bodies / sum RED bodies); GREEN_sum=4.4000 dates=[2026-08-03,2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_sum=1.7200 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=12.3500,C=13.0400,body=+0.6900 | 2026-08-04:RED:O=13.1500,C=13.1400,body=-0.0100 | 2026-08-05:RED:O=13.2100,C=12.6300,body=-0.5800 | 2026-08-06:GREEN:O=14.0300,C=15.4700,body=+1.4400 | 2026-08-07:RED:O=15.6700,C=15.1100,body=-0.5600 | 2026-08-10:GREEN:O=15.0000,C=16.6700,body=+1.6700 | 2026-08-11:GREEN:O=16.8800,C=17.2200,body=+0.3400 | 2026-08-12:GREEN:O=17.1000,C=17.1100,body=+0.0100 | 2026-08-13:GREEN:O=17.1100,C=17.3600,body=+0.2500 | 2026-08-14:RED:O=17.3000,C=16.7300,body=-0.5700 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=3.007 if finite else n/a; GREEN_vol_sum=7868100 dates=[2026-08-03,2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_vol_sum=2616200 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=579900 | 2026-08-04:RED:vol=441300 | 2026-08-05:RED:vol=432600 | 2026-08-06:GREEN:vol=2986700 | 2026-08-07:RED:vol=1212800 | 2026-08-10:GREEN:vol=1483200 | 2026-08-11:GREEN:vol=1228800 | 2026-08-12:GREEN:vol=774700 | 2026-08-13:GREEN:vol=814800 | 2026-08-14:RED:vol=529500 | **GOOD** |
| `A07_rvol` | RVOL=0.760 on 2026-08-14: today_vol=529500 / avg20=696520 (avg window 2026-07-16→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.681 on 2026-08-14 (price=16.7300, mid=13.8335, upper=18.0849, lower=9.5821; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=16.7300 vs SMA50=12.8851 dist=+29.84% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=13.8335 SMA50=12.8851 SMA80=12.1709 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-3.63% inside window 2026-06-15→2026-08-14 (42 sessions): peak 2026-08-13 @ 17.3600 → trough 2026-08-14 @ 16.7300 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13] body_frac=0.714 wick_frac=0.286; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.6900,upperW=0.0400,lowerW=0.0000,range=0.7300 | 2026-08-04:RED:body=0.0100,upperW=0.1800,lowerW=0.1000,range=0.2900 | 2026-08-05:RED:body=0.5800,upperW=0.0390,lowerW=0.2000,range=0.8190 | 2026-08-06:GREEN:body=1.4400,upperW=0.1500,lowerW=0.0300,range=1.6200 | 2026-08-07:RED:body=0.5600,upperW=0.7050,lowerW=0.1450,range=1.4100 | 2026-08-10:GREEN:body=1.6700,upperW=0.0200,lowerW=0.0000,range=1.6900 | 2026-08-11:GREEN:body=0.3400,upperW=0.2000,lowerW=0.3950,range=0.9350 | 2026-08-12:GREEN:body=0.0100,upperW=0.1700,lowerW=0.4700,range=0.6500 | 2026-08-13:GREEN:body=0.2500,upperW=0.0400,lowerW=0.2500,range=0.5400 | 2026-08-14:RED:body=0.5700,upperW=0.1000,lowerW=0.0550,range=0.7250 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-14] body_frac=0.530 wick_frac=0.470; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.6900,upperW=0.0400,lowerW=0.0000,range=0.7300 | 2026-08-04:RED:body=0.0100,upperW=0.1800,lowerW=0.1000,range=0.2900 | 2026-08-05:RED:body=0.5800,upperW=0.0390,lowerW=0.2000,range=0.8190 | 2026-08-06:GREEN:body=1.4400,upperW=0.1500,lowerW=0.0300,range=1.6200 | 2026-08-07:RED:body=0.5600,upperW=0.7050,lowerW=0.1450,range=1.4100 | 2026-08-10:GREEN:body=1.6700,upperW=0.0200,lowerW=0.0000,range=1.6900 | 2026-08-11:GREEN:body=0.3400,upperW=0.2000,lowerW=0.3950,range=0.9350 | 2026-08-12:GREEN:body=0.0100,upperW=0.1700,lowerW=0.4700,range=0.6500 | 2026-08-13:GREEN:body=0.2500,upperW=0.0400,lowerW=0.2500,range=0.5400 | 2026-08-14:RED:body=0.5700,upperW=0.1000,lowerW=0.0550,range=0.7250 | **BAD** |
| `B01_eps_surprise` | 178.56 | **GOOD** |
| `B02_revenue_surprise` | 10.97 | **GOOD** |
| `B03_sales` | 95.42 | **NEUTRAL** |
| `B04_income` | 0.53 | **GOOD** |
| `B05_profit_margin` | 0.56 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.5 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=24.5 vs prior_export=24.5 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | 0.21 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.21 vs prior=0.21 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.08 | **GOOD** |
| `B13_short_float` | 7.9 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |

### MH  ·  score **+13**  ·  Education & Training Services
price=13.100000381469727  mcap=$2.52B  ADV=783,520
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-13]  RED=[2026-08-06,2026-08-11,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=68.99 on 2026-08-14; prev RSI=75.74 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 75.74@2026-08-13 → 68.99@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 75.74@2026-08-13 → 68.99@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 75.74@2026-08-13 → 68.99@2026-08-14 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=3.952 (= sum GREEN bodies / sum RED bodies); GREEN_sum=2.4500 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-13]; RED_sum=0.6200 dates=[2026-08-06,2026-08-11,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=11.1400,C=11.4900,body=+0.3500 | 2026-08-04:GREEN:O=11.3400,C=11.5600,body=+0.2200 | 2026-08-05:GREEN:O=11.6000,C=11.6700,body=+0.0700 | 2026-08-06:RED:O=11.6500,C=11.5500,body=-0.1000 | 2026-08-07:GREEN:O=11.3800,C=11.6000,body=+0.2200 | 2026-08-10:GREEN:O=11.4500,C=11.6200,body=+0.1700 | 2026-08-11:RED:O=11.5700,C=11.5000,body=-0.0700 | 2026-08-12:GREEN:O=11.3000,C=11.6000,body=+0.3000 | 2026-08-13:GREEN:O=12.4500,C=13.5700,body=+1.1200 | 2026-08-14:RED:O=13.5500,C=13.1000,body=-0.4500 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.916 if finite else n/a; GREEN_vol_sum=4767800 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-13]; RED_vol_sum=2488000 dates=[2026-08-06,2026-08-11,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=511200 | 2026-08-04:GREEN:vol=514000 | 2026-08-05:GREEN:vol=297800 | 2026-08-06:RED:vol=321500 | 2026-08-07:GREEN:vol=363300 | 2026-08-10:GREEN:vol=509300 | 2026-08-11:RED:vol=683700 | 2026-08-12:GREEN:vol=719800 | 2026-08-13:GREEN:vol=1852400 | 2026-08-14:RED:vol=1482800 | **GOOD** |
| `A07_rvol` | RVOL=2.340 on 2026-08-14: today_vol=1482800 / avg20=633755 (avg window 2026-07-14→2026-08-13, excludes today) | **GOOD** |
| `A08_bollinger_position` | pos=0.906 on 2026-08-14 (price=13.1000, mid=11.0340, upper=13.3132, lower=8.7548; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-14: price=13.1000 vs SMA50=10.8260 dist=+21.00% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=11.03_50=10.83_80=11.37 on 2026-08-14: SMA20=11.0340 SMA50=10.8260 SMA80=11.3730 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=-3.46% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-13 @ 13.5700 → trough 2026-08-14 @ 13.1000 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-13] body_frac=0.564 wick_frac=0.436; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.3500,upperW=0.0500,lowerW=0.1800,range=0.5800 | 2026-08-04:GREEN:body=0.2200,upperW=0.0200,lowerW=0.2350,range=0.4750 | 2026-08-05:GREEN:body=0.0700,upperW=0.0600,lowerW=0.3000,range=0.4300 | 2026-08-06:RED:body=0.1000,upperW=0.0900,lowerW=0.1650,range=0.3550 | 2026-08-07:GREEN:body=0.2200,upperW=0.3200,lowerW=0.0000,range=0.5400 | 2026-08-10:GREEN:body=0.1700,upperW=0.2290,lowerW=0.0300,range=0.4290 | 2026-08-11:RED:body=0.0700,upperW=0.1950,lowerW=0.2900,range=0.5550 | 2026-08-12:GREEN:body=0.3000,upperW=0.1600,lowerW=0.0900,range=0.5500 | 2026-08-13:GREEN:body=1.1200,upperW=0.0000,lowerW=0.2220,range=1.3420 | 2026-08-14:RED:body=0.4500,upperW=0.1200,lowerW=0.9100,range=1.4800 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-11,2026-08-14] body_frac=0.259 wick_frac=0.741; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.3500,upperW=0.0500,lowerW=0.1800,range=0.5800 | 2026-08-04:GREEN:body=0.2200,upperW=0.0200,lowerW=0.2350,range=0.4750 | 2026-08-05:GREEN:body=0.0700,upperW=0.0600,lowerW=0.3000,range=0.4300 | 2026-08-06:RED:body=0.1000,upperW=0.0900,lowerW=0.1650,range=0.3550 | 2026-08-07:GREEN:body=0.2200,upperW=0.3200,lowerW=0.0000,range=0.5400 | 2026-08-10:GREEN:body=0.1700,upperW=0.2290,lowerW=0.0300,range=0.4290 | 2026-08-11:RED:body=0.0700,upperW=0.1950,lowerW=0.2900,range=0.5550 | 2026-08-12:GREEN:body=0.3000,upperW=0.1600,lowerW=0.0900,range=0.5500 | 2026-08-13:GREEN:body=1.1200,upperW=0.0000,lowerW=0.2220,range=1.3420 | 2026-08-14:RED:body=0.4500,upperW=0.1200,lowerW=0.9100,range=1.4800 | **GOOD** |
| `B01_eps_surprise` | 23.59 | **GOOD** |
| `B02_revenue_surprise` | 3.36 | **GOOD** |
| `B03_sales` | 2116.97 | **NEUTRAL** |
| `B04_income` | 92.68 | **GOOD** |
| `B05_profit_margin` | 4.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 17.92 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=17.92 vs prior_export=17.92 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.23 | **GOOD** |
| `B10_insider_transactions` | 0.05 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.05 vs prior=0.05 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.0 | **NEUTRAL** |
| `B13_short_float` | 8.52 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |

### EPD  ·  score **+13**  ·  Oil & Gas Midstream
price=38.900001525878906  mcap=$83.69B  ADV=3,203,860
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]  RED=[2026-08-05,2026-08-06,2026-08-07]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=62.96 on 2026-08-14; prev RSI=54.41 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 54.41@2026-08-13 → 62.96@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 54.41@2026-08-13 → 62.96@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 54.41@2026-08-13 → 62.96@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=5.744 (= sum GREEN bodies / sum RED bodies); GREEN_sum=2.2400 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_sum=0.3900 dates=[2026-08-05,2026-08-06,2026-08-07]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=37.4700,C=37.8700,body=+0.4000 | 2026-08-04:GREEN:O=37.7000,C=38.1200,body=+0.4200 | 2026-08-05:RED:O=38.0000,C=37.7200,body=-0.2800 | 2026-08-06:RED:O=38.1000,C=38.0500,body=-0.0500 | 2026-08-07:RED:O=37.8100,C=37.7500,body=-0.0600 | 2026-08-10:GREEN:O=37.7600,C=37.9900,body=+0.2300 | 2026-08-11:GREEN:O=37.7700,C=37.8600,body=+0.0900 | 2026-08-12:GREEN:O=37.7500,C=37.9200,body=+0.1700 | 2026-08-13:GREEN:O=37.7600,C=38.0200,body=+0.2600 | 2026-08-14:GREEN:O=38.2300,C=38.9000,body=+0.6700 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=3.209 if finite else n/a; GREEN_vol_sum=25217400 dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14]; RED_vol_sum=7857400 dates=[2026-08-05,2026-08-06,2026-08-07]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=2840400 | 2026-08-04:GREEN:vol=4108300 | 2026-08-05:RED:vol=3043300 | 2026-08-06:RED:vol=2321200 | 2026-08-07:RED:vol=2492900 | 2026-08-10:GREEN:vol=3302000 | 2026-08-11:GREEN:vol=2812400 | 2026-08-12:GREEN:vol=2417500 | 2026-08-13:GREEN:vol=2358000 | 2026-08-14:GREEN:vol=7378800 | **GOOD** |
| `A07_rvol` | RVOL=2.239 on 2026-08-14: today_vol=7378800 / avg20=3294990 (avg window 2026-07-17→2026-08-13, excludes today) | **GOOD** |
| `A08_bollinger_position` | pos=1.498 on 2026-08-14 (price=38.9000, mid=38.0224, upper=38.6082, lower=37.4366; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-14: price=38.9000 vs SMA50=37.1873 dist=+4.61% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=38.02_50=37.19_80=37.36 on 2026-08-14: SMA20=38.0224 SMA50=37.1873 SMA80=37.3608 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-16→2026-08-14 (42 sessions): peak 2026-08-14 @ 38.9000 → trough 2026-08-14 @ 38.9000 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14] body_frac=0.557 wick_frac=0.443; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.4000,upperW=0.2300,lowerW=0.0800,range=0.7100 | 2026-08-04:GREEN:body=0.4200,upperW=0.1400,lowerW=0.2600,range=0.8200 | 2026-08-05:RED:body=0.2800,upperW=0.0700,lowerW=0.3000,range=0.6500 | 2026-08-06:RED:body=0.0500,upperW=0.0500,lowerW=0.3800,range=0.4800 | 2026-08-07:RED:body=0.0600,upperW=0.3800,lowerW=0.0000,range=0.4400 | 2026-08-10:GREEN:body=0.2300,upperW=0.2000,lowerW=0.0000,range=0.4300 | 2026-08-11:GREEN:body=0.0900,upperW=0.3000,lowerW=0.0900,range=0.4800 | 2026-08-12:GREEN:body=0.1700,upperW=0.2000,lowerW=0.0000,range=0.3700 | 2026-08-13:GREEN:body=0.2600,upperW=0.1600,lowerW=0.0000,range=0.4200 | 2026-08-14:GREEN:body=0.6700,upperW=0.0000,lowerW=0.1200,range=0.7900 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-07] body_frac=0.248 wick_frac=0.752; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.4000,upperW=0.2300,lowerW=0.0800,range=0.7100 | 2026-08-04:GREEN:body=0.4200,upperW=0.1400,lowerW=0.2600,range=0.8200 | 2026-08-05:RED:body=0.2800,upperW=0.0700,lowerW=0.3000,range=0.6500 | 2026-08-06:RED:body=0.0500,upperW=0.0500,lowerW=0.3800,range=0.4800 | 2026-08-07:RED:body=0.0600,upperW=0.3800,lowerW=0.0000,range=0.4400 | 2026-08-10:GREEN:body=0.2300,upperW=0.2000,lowerW=0.0000,range=0.4300 | 2026-08-11:GREEN:body=0.0900,upperW=0.3000,lowerW=0.0900,range=0.4800 | 2026-08-12:GREEN:body=0.1700,upperW=0.2000,lowerW=0.0000,range=0.3700 | 2026-08-13:GREEN:body=0.2600,upperW=0.1600,lowerW=0.0000,range=0.4200 | 2026-08-14:GREEN:body=0.6700,upperW=0.0000,lowerW=0.1200,range=0.7900 | **GOOD** |
| `B01_eps_surprise` | 12.24 | **GOOD** |
| `B02_revenue_surprise` | 33.47 | **GOOD** |
| `B03_sales` | 58217.0 | **NEUTRAL** |
| `B04_income` | 6244.0 | **GOOD** |
| `B05_profit_margin` | 10.73 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 41.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.04999999999999716 (now=41.61 vs prior_export=41.56 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 2.22 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.42 | **GOOD** |
| `B13_short_float` | 1.2 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |

### SONO  ·  score **+13**  ·  Consumer Electronics
price=16.309999465942383  mcap=$1.83B  ADV=1,978,070
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-07,2026-08-12,2026-08-13]  RED=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.85 on 2026-08-14; prev RSI=57.80 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.80@2026-08-13 → 55.85@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.80@2026-08-13 → 55.85@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.80@2026-08-13 → 55.85@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.565 (= sum GREEN bodies / sum RED bodies); GREEN_sum=3.0200 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-12,2026-08-13]; RED_sum=1.9300 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=15.0700,C=15.7700,body=+0.7000 | 2026-08-04:GREEN:O=16.0000,C=16.4200,body=+0.4200 | 2026-08-05:RED:O=16.4300,C=15.9200,body=-0.5100 | 2026-08-06:RED:O=15.7600,C=15.4500,body=-0.3100 | 2026-08-07:GREEN:O=15.3600,C=15.6400,body=+0.2800 | 2026-08-10:RED:O=15.6600,C=15.4400,body=-0.2200 | 2026-08-11:RED:O=15.4400,C=15.0700,body=-0.3700 | 2026-08-12:GREEN:O=14.9000,C=15.7500,body=+0.8500 | 2026-08-13:GREEN:O=15.8200,C=16.5900,body=+0.7700 | 2026-08-14:RED:O=16.8300,C=16.3100,body=-0.5200 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.264 if finite else n/a; GREEN_vol_sum=8699300 dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-12,2026-08-13]; RED_vol_sum=6879900 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=2007800 | 2026-08-04:GREEN:vol=1795500 | 2026-08-05:RED:vol=1665800 | 2026-08-06:RED:vol=1544500 | 2026-08-07:GREEN:vol=1189800 | 2026-08-10:RED:vol=1203900 | 2026-08-11:RED:vol=984000 | 2026-08-12:GREEN:vol=2055800 | 2026-08-13:GREEN:vol=1650400 | 2026-08-14:RED:vol=1481700 | **GOOD** |
| `A07_rvol` | RVOL=0.681 on 2026-08-14: today_vol=1481700 / avg20=2174835 (avg window 2026-07-15→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.434 on 2026-08-14 (price=16.3100, mid=15.5615, upper=17.2866, lower=13.8364; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=16.3100 vs SMA50=14.9488 dist=+9.11% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=15.56_50=14.95_80=14.96 on 2026-08-14: SMA20=15.5615 SMA50=14.9488 SMA80=14.9592 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=-17.64% inside window 2026-06-12→2026-08-14 (42 sessions): peak 2026-07-29 @ 17.5200 → trough 2026-07-30 @ 14.4300 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-07,2026-08-12,2026-08-13] body_frac=0.744 wick_frac=0.256; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.7000,upperW=0.0500,lowerW=0.0000,range=0.7500 | 2026-08-04:GREEN:body=0.4200,upperW=0.0500,lowerW=0.3800,range=0.8500 | 2026-08-05:RED:body=0.5100,upperW=0.0000,lowerW=0.0500,range=0.5600 | 2026-08-06:RED:body=0.3100,upperW=0.0700,lowerW=0.2600,range=0.6400 | 2026-08-07:GREEN:body=0.2800,upperW=0.1400,lowerW=0.0000,range=0.4200 | 2026-08-10:RED:body=0.2200,upperW=0.2100,lowerW=0.0300,range=0.4600 | 2026-08-11:RED:body=0.3700,upperW=0.1300,lowerW=0.0300,range=0.5300 | 2026-08-12:GREEN:body=0.8500,upperW=0.2200,lowerW=0.0000,range=1.0700 | 2026-08-13:GREEN:body=0.7700,upperW=0.0300,lowerW=0.1700,range=0.9700 | 2026-08-14:RED:body=0.5200,upperW=0.1800,lowerW=0.0200,range=0.7200 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14] body_frac=0.663 wick_frac=0.337; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.7000,upperW=0.0500,lowerW=0.0000,range=0.7500 | 2026-08-04:GREEN:body=0.4200,upperW=0.0500,lowerW=0.3800,range=0.8500 | 2026-08-05:RED:body=0.5100,upperW=0.0000,lowerW=0.0500,range=0.5600 | 2026-08-06:RED:body=0.3100,upperW=0.0700,lowerW=0.2600,range=0.6400 | 2026-08-07:GREEN:body=0.2800,upperW=0.1400,lowerW=0.0000,range=0.4200 | 2026-08-10:RED:body=0.2200,upperW=0.2100,lowerW=0.0300,range=0.4600 | 2026-08-11:RED:body=0.3700,upperW=0.1300,lowerW=0.0300,range=0.5300 | 2026-08-12:GREEN:body=0.8500,upperW=0.2200,lowerW=0.0000,range=1.0700 | 2026-08-13:GREEN:body=0.7700,upperW=0.0300,lowerW=0.1700,range=0.9700 | 2026-08-14:RED:body=0.5200,upperW=0.1800,lowerW=0.0200,range=0.7200 | **BAD** |
| `B01_eps_surprise` | 525.0 | **GOOD** |
| `B02_revenue_surprise` | 2.63 | **GOOD** |
| `B03_sales` | 1490.35 | **NEUTRAL** |
| `B04_income` | 56.91 | **GOOD** |
| `B05_profit_margin` | 3.82 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 19.67 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=19.67 vs prior_export=19.67 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.6 | **GOOD** |
| `B10_insider_transactions` | 409.09 | **GOOD** |
| `B11_insider_tx_delta` | delta=297.27 (now=409.09 vs prior=111.82 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | 3.32 | **GOOD** |
| `B13_short_float` | 8.35 | **NEUTRAL** |
| `B14_earnings_date` | 7/29/2026 4:30:00 PM | **NEUTRAL** |

### RPM  ·  score **+13**  ·  Specialty Chemicals
price=113.81999969482422  mcap=$14.23B  ADV=913,360
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-11,2026-08-14]  RED=[2026-08-06,2026-08-10,2026-08-12,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.28 on 2026-08-14; prev RSI=55.40 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.40@2026-08-13 → 56.28@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.40@2026-08-13 → 56.28@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.40@2026-08-13 → 56.28@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.528 (= sum GREEN bodies / sum RED bodies); GREEN_sum=9.2600 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-11,2026-08-14]; RED_sum=6.0600 dates=[2026-08-06,2026-08-10,2026-08-12,2026-08-13]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=110.5000,C=111.4900,body=+0.9900 | 2026-08-04:GREEN:O=112.6500,C=115.0900,body=+2.4400 | 2026-08-05:GREEN:O=115.2900,C=116.7200,body=+1.4300 | 2026-08-06:RED:O=116.9900,C=115.6500,body=-1.3400 | 2026-08-07:GREEN:O=114.8900,C=117.3600,body=+2.4700 | 2026-08-10:RED:O=117.0500,C=115.9300,body=-1.1200 | 2026-08-11:GREEN:O=115.7000,C=116.6600,body=+0.9600 | 2026-08-12:RED:O=115.9300,C=115.0700,body=-0.8600 | 2026-08-13:RED:O=116.0700,C=113.3300,body=-2.7400 | 2026-08-14:GREEN:O=112.8500,C=113.8200,body=+0.9700 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.219 if finite else n/a; GREEN_vol_sum=4946800 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-11,2026-08-14]; RED_vol_sum=4059700 dates=[2026-08-06,2026-08-10,2026-08-12,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=868900 | 2026-08-04:GREEN:vol=1059000 | 2026-08-05:GREEN:vol=802600 | 2026-08-06:RED:vol=1123500 | 2026-08-07:GREEN:vol=812000 | 2026-08-10:RED:vol=947500 | 2026-08-11:GREEN:vol=604400 | 2026-08-12:RED:vol=735700 | 2026-08-13:RED:vol=1253000 | 2026-08-14:GREEN:vol=799900 | **GOOD** |
| `A07_rvol` | RVOL=0.792 on 2026-08-14: today_vol=799900 / avg20=1010080 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.305 on 2026-08-14 (price=113.8200, mid=110.7345, upper=120.8667, lower=100.6023; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=113.8200 vs SMA50=108.2452 dist=+5.15% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=110.7345 SMA50=108.2452 SMA80=105.7044 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-3.43% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-07 @ 117.3600 → trough 2026-08-13 @ 113.3300 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-07,2026-08-11,2026-08-14] body_frac=0.514 wick_frac=0.486; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.9900,upperW=0.0300,lowerW=2.7500,range=3.7700 | 2026-08-04:GREEN:body=2.4400,upperW=0.1700,lowerW=1.0500,range=3.6600 | 2026-08-05:GREEN:body=1.4300,upperW=0.0300,lowerW=0.5400,range=2.0000 | 2026-08-06:RED:body=1.3400,upperW=0.7000,lowerW=0.5600,range=2.6000 | 2026-08-07:GREEN:body=2.4700,upperW=0.3900,lowerW=0.8800,range=3.7400 | 2026-08-10:RED:body=1.1200,upperW=0.7200,lowerW=1.4700,range=3.3100 | 2026-08-11:GREEN:body=0.9600,upperW=1.2700,lowerW=0.0000,range=2.2300 | 2026-08-12:RED:body=0.8600,upperW=1.5600,lowerW=0.8400,range=3.2600 | 2026-08-13:RED:body=2.7400,upperW=0.7400,lowerW=0.6000,range=4.0800 | 2026-08-14:GREEN:body=0.9700,upperW=0.2100,lowerW=1.4400,range=2.6200 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-10,2026-08-12,2026-08-13] body_frac=0.457 wick_frac=0.543; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.9900,upperW=0.0300,lowerW=2.7500,range=3.7700 | 2026-08-04:GREEN:body=2.4400,upperW=0.1700,lowerW=1.0500,range=3.6600 | 2026-08-05:GREEN:body=1.4300,upperW=0.0300,lowerW=0.5400,range=2.0000 | 2026-08-06:RED:body=1.3400,upperW=0.7000,lowerW=0.5600,range=2.6000 | 2026-08-07:GREEN:body=2.4700,upperW=0.3900,lowerW=0.8800,range=3.7400 | 2026-08-10:RED:body=1.1200,upperW=0.7200,lowerW=1.4700,range=3.3100 | 2026-08-11:GREEN:body=0.9600,upperW=1.2700,lowerW=0.0000,range=2.2300 | 2026-08-12:RED:body=0.8600,upperW=1.5600,lowerW=0.8400,range=3.2600 | 2026-08-13:RED:body=2.7400,upperW=0.7400,lowerW=0.6000,range=4.0800 | 2026-08-14:GREEN:body=0.9700,upperW=0.2100,lowerW=1.4400,range=2.6200 | **GOOD** |
| `B01_eps_surprise` | 3.3 | **GOOD** |
| `B02_revenue_surprise` | 2.3 | **GOOD** |
| `B03_sales` | 7863.42 | **NEUTRAL** |
| `B04_income` | 658.68 | **GOOD** |
| `B05_profit_margin` | 8.38 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 130.07 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=130.07 vs prior_export=130.07 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.58 | **GOOD** |
| `B13_short_float` | 3.07 | **NEUTRAL** |
| `B14_earnings_date` | 7/22/2026 8:30:00 AM | **NEUTRAL** |

### DSGX  ·  score **+12**  ·  Software - Application
price=77.23999786376953  mcap=$6.49B  ADV=539,980
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-04,2026-08-06,2026-08-07,2026-08-10]  RED=[2026-08-03,2026-08-05,2026-08-11,2026-08-12,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.02 on 2026-08-14; prev RSI=55.02 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 55.02@2026-08-13 → 55.02@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 55.02@2026-08-13 → 55.02@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 55.02@2026-08-13 → 55.02@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.418 (= sum GREEN bodies / sum RED bodies); GREEN_sum=6.3800 dates=[2026-08-04,2026-08-06,2026-08-07,2026-08-10]; RED_sum=4.5000 dates=[2026-08-03,2026-08-05,2026-08-11,2026-08-12,2026-08-13]; DOJI=[2026-08-14]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:RED:O=76.9800,C=76.5900,body=-0.3900 | 2026-08-04:GREEN:O=75.3100,C=77.4200,body=+2.1100 | 2026-08-05:RED:O=78.6500,C=76.6200,body=-2.0300 | 2026-08-06:GREEN:O=75.5200,C=77.7800,body=+2.2600 | 2026-08-07:GREEN:O=78.1800,C=79.5700,body=+1.3900 | 2026-08-10:GREEN:O=79.3800,C=80.0000,body=+0.6200 | 2026-08-11:RED:O=80.0000,C=79.1800,body=-0.8200 | 2026-08-12:RED:O=78.2400,C=77.6200,body=-0.6200 | 2026-08-13:RED:O=77.8800,C=77.2400,body=-0.6400 | 2026-08-14:DOJI:O=77.2400,C=77.2400,body=+0.0000 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=0.895 if finite else n/a; GREEN_vol_sum=1952450 dates=[2026-08-04,2026-08-06,2026-08-07,2026-08-10]; RED_vol_sum=2181750 dates=[2026-08-03,2026-08-05,2026-08-11,2026-08-12,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:vol=236300 | 2026-08-04:GREEN:vol=567100 | 2026-08-05:RED:vol=451300 | 2026-08-06:GREEN:vol=306700 | 2026-08-07:GREEN:vol=363700 | 2026-08-10:GREEN:vol=498800 | 2026-08-11:RED:vol=250700 | 2026-08-12:RED:vol=426500 | 2026-08-13:RED:vol=600800 | 2026-08-14:DOJI:vol=432300 | **NEUTRAL** |
| `A07_rvol` | RVOL=0.893 on 2026-08-14: today_vol=432300 / avg20=483925 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.247 on 2026-08-14 (price=77.2400, mid=75.3735, upper=82.9172, lower=67.8298; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=77.2400 vs SMA50=73.4176 dist=+5.21% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=75.3735 SMA50=73.4176 SMA80=72.7700 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-3.45% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-10 @ 80.0000 → trough 2026-08-13 @ 77.2400 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-04,2026-08-06,2026-08-07,2026-08-10] body_frac=0.696 wick_frac=0.304; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=0.3900,upperW=2.1300,lowerW=1.0900,range=3.6100 | 2026-08-04:GREEN:body=2.1100,upperW=0.2200,lowerW=0.0000,range=2.3300 | 2026-08-05:RED:body=2.0300,upperW=0.5200,lowerW=0.4800,range=3.0300 | 2026-08-06:GREEN:body=2.2600,upperW=0.0200,lowerW=0.7900,range=3.0700 | 2026-08-07:GREEN:body=1.3900,upperW=0.1500,lowerW=0.6500,range=2.1900 | 2026-08-10:GREEN:body=0.6200,upperW=0.5700,lowerW=0.3900,range=1.5800 | 2026-08-11:RED:body=0.8200,upperW=0.3700,lowerW=0.2900,range=1.4800 | 2026-08-12:RED:body=0.6200,upperW=0.2800,lowerW=1.3800,range=2.2800 | 2026-08-13:RED:body=0.6400,upperW=0.0000,lowerW=3.5000,range=4.1400 | 2026-08-14:DOJI:body=0.0000,upperW=0.1900,lowerW=2.1300,range=2.3200 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-03,2026-08-05,2026-08-11,2026-08-12,2026-08-13] body_frac=0.309 wick_frac=0.691; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=0.3900,upperW=2.1300,lowerW=1.0900,range=3.6100 | 2026-08-04:GREEN:body=2.1100,upperW=0.2200,lowerW=0.0000,range=2.3300 | 2026-08-05:RED:body=2.0300,upperW=0.5200,lowerW=0.4800,range=3.0300 | 2026-08-06:GREEN:body=2.2600,upperW=0.0200,lowerW=0.7900,range=3.0700 | 2026-08-07:GREEN:body=1.3900,upperW=0.1500,lowerW=0.6500,range=2.1900 | 2026-08-10:GREEN:body=0.6200,upperW=0.5700,lowerW=0.3900,range=1.5800 | 2026-08-11:RED:body=0.8200,upperW=0.3700,lowerW=0.2900,range=1.4800 | 2026-08-12:RED:body=0.6200,upperW=0.2800,lowerW=1.3800,range=2.2800 | 2026-08-13:RED:body=0.6400,upperW=0.0000,lowerW=3.5000,range=4.1400 | 2026-08-14:DOJI:body=0.0000,upperW=0.1900,lowerW=2.1300,range=2.3200 | **GOOD** |
| `B01_eps_surprise` | 5.99 | **GOOD** |
| `B02_revenue_surprise` | 1.04 | **GOOD** |
| `B03_sales` | 753.87 | **NEUTRAL** |
| `B04_income` | 176.0 | **GOOD** |
| `B05_profit_margin` | 23.35 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 100.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=100.23 vs prior_export=100.23 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.41 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 4.93 | **GOOD** |
| `B13_short_float` | 3.21 | **NEUTRAL** |
| `B14_earnings_date` | 9/10/2026 4:30:00 PM | **NEUTRAL** |

### SN  ·  score **+12**  ·  Furnishings, Fixtures & Appliances
price=185.10000610351562  mcap=$25.72B  ADV=1,867,440
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-13]  RED=[2026-08-03,2026-08-06,2026-08-11,2026-08-12,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=71.21 on 2026-08-14; prev RSI=76.76 on 2026-08-13 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 76.76@2026-08-13 → 71.21@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 76.76@2026-08-13 → 71.21@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | above | RSI 76.76@2026-08-13 → 71.21@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.891 (= sum GREEN bodies / sum RED bodies); GREEN_sum=24.8500 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-13]; RED_sum=13.1400 dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-12,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:RED:O=163.9200,C=160.5700,body=-3.3500 | 2026-08-04:GREEN:O=162.5000,C=168.1900,body=+5.6900 | 2026-08-05:GREEN:O=169.3000,C=182.1100,body=+12.8100 | 2026-08-06:RED:O=184.1400,C=179.9300,body=-4.2100 | 2026-08-07:GREEN:O=181.7300,C=185.5200,body=+3.7900 | 2026-08-10:GREEN:O=185.6000,C=186.1700,body=+0.5700 | 2026-08-11:RED:O=188.0000,C=187.6800,body=-0.3200 | 2026-08-12:RED:O=188.9700,C=186.1300,body=-2.8400 | 2026-08-13:GREEN:O=186.6000,C=188.5900,body=+1.9900 | 2026-08-14:RED:O=187.5200,C=185.1000,body=-2.4200 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.206 if finite else n/a; GREEN_vol_sum=12421500 dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-13]; RED_vol_sum=10301400 dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-12,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:vol=2156900 | 2026-08-04:GREEN:vol=3445200 | 2026-08-05:GREEN:vol=3710400 | 2026-08-06:RED:vol=2659000 | 2026-08-07:GREEN:vol=1476000 | 2026-08-10:GREEN:vol=2327400 | 2026-08-11:RED:vol=1828100 | 2026-08-12:RED:vol=2268000 | 2026-08-13:GREEN:vol=1462500 | 2026-08-14:RED:vol=1389400 | **GOOD** |
| `A07_rvol` | RVOL=0.768 on 2026-08-14: today_vol=1389400 / avg20=1809410 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.561 on 2026-08-14 (price=185.1000, mid=167.5810, upper=198.8329, lower=136.3291; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=185.1000 vs SMA50=150.0060 dist=+23.40% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=167.5810 SMA50=150.0060 SMA80=136.2513 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-1.85% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-13 @ 188.5900 → trough 2026-08-14 @ 185.1000 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-04,2026-08-05,2026-08-07,2026-08-10,2026-08-13] body_frac=0.577 wick_frac=0.423; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=3.3500,upperW=0.7800,lowerW=1.9000,range=6.0300 | 2026-08-04:GREEN:body=5.6900,upperW=1.5000,lowerW=0.8400,range=8.0300 | 2026-08-05:GREEN:body=12.8100,upperW=0.7400,lowerW=2.6400,range=16.1900 | 2026-08-06:RED:body=4.2100,upperW=2.9000,lowerW=1.8700,range=8.9800 | 2026-08-07:GREEN:body=3.7900,upperW=2.1100,lowerW=2.5500,range=8.4500 | 2026-08-10:GREEN:body=0.5700,upperW=1.3360,lowerW=2.6300,range=4.5360 | 2026-08-11:RED:body=0.3200,upperW=3.2200,lowerW=4.1990,range=7.7390 | 2026-08-12:RED:body=2.8400,upperW=1.0300,lowerW=3.2100,range=7.0800 | 2026-08-13:GREEN:body=1.9900,upperW=1.6200,lowerW=2.2600,range=5.8700 | 2026-08-14:RED:body=2.4200,upperW=2.7600,lowerW=2.3200,range=7.5000 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-03,2026-08-06,2026-08-11,2026-08-12,2026-08-14] body_frac=0.352 wick_frac=0.648; window=2026-08-03→2026-08-14; sessions: 2026-08-03:RED:body=3.3500,upperW=0.7800,lowerW=1.9000,range=6.0300 | 2026-08-04:GREEN:body=5.6900,upperW=1.5000,lowerW=0.8400,range=8.0300 | 2026-08-05:GREEN:body=12.8100,upperW=0.7400,lowerW=2.6400,range=16.1900 | 2026-08-06:RED:body=4.2100,upperW=2.9000,lowerW=1.8700,range=8.9800 | 2026-08-07:GREEN:body=3.7900,upperW=2.1100,lowerW=2.5500,range=8.4500 | 2026-08-10:GREEN:body=0.5700,upperW=1.3360,lowerW=2.6300,range=4.5360 | 2026-08-11:RED:body=0.3200,upperW=3.2200,lowerW=4.1990,range=7.7390 | 2026-08-12:RED:body=2.8400,upperW=1.0300,lowerW=3.2100,range=7.0800 | 2026-08-13:GREEN:body=1.9900,upperW=1.6200,lowerW=2.2600,range=5.8700 | 2026-08-14:RED:body=2.4200,upperW=2.7600,lowerW=2.3200,range=7.5000 | **GOOD** |
| `B01_eps_surprise` | 13.45 | **GOOD** |
| `B02_revenue_surprise` | 6.82 | **GOOD** |
| `B03_sales` | 6909.96 | **NEUTRAL** |
| `B04_income` | 695.22 | **GOOD** |
| `B05_profit_margin` | 10.06 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 209.56 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=209.56 vs prior_export=209.56 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.0 | **GOOD** |
| `B10_insider_transactions` | -6.42 | **BAD** |
| `B11_insider_tx_delta` | delta=0.16000000000000014 (now=-6.42 vs prior=-6.58 on finviz_2026-08-17) | **GOOD** |
| `B12_institutional_transactions` | 8.76 | **GOOD** |
| `B13_short_float` | 9.31 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 8:30:00 AM | **NEUTRAL** |

### BLK  ·  score **+12**  ·  Asset Management
price=1173.72998046875  mcap=$187.69B  ADV=769,020
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13]  RED=[2026-08-04,2026-08-05,2026-08-06,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=69.77 on 2026-08-14; prev RSI=73.08 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 73.08@2026-08-13 → 69.77@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 73.08@2026-08-13 → 69.77@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 73.08@2026-08-13 → 69.77@2026-08-14 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.987 (= sum GREEN bodies / sum RED bodies); GREEN_sum=71.1700 dates=[2026-08-03,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_sum=23.8300 dates=[2026-08-04,2026-08-05,2026-08-06,2026-08-14]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=1100.0699,C=1126.6300,body=+26.5601 | 2026-08-04:RED:O=1138.5000,C=1131.1300,body=-7.3700 | 2026-08-05:RED:O=1138.5000,C=1133.5800,body=-4.9200 | 2026-08-06:RED:O=1135.5699,C=1129.3000,body=-6.2699 | 2026-08-07:GREEN:O=1132.5300,C=1136.3900,body=+3.8600 | 2026-08-10:GREEN:O=1129.8199,C=1131.4000,body=+1.5801 | 2026-08-11:GREEN:O=1130.6500,C=1148.8400,body=+18.1899 | 2026-08-12:GREEN:O=1157.0000,C=1160.9100,body=+3.9100 | 2026-08-13:GREEN:O=1165.7700,C=1182.8400,body=+17.0699 | 2026-08-14:RED:O=1179.0000,C=1173.7300,body=-5.2700 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.224 if finite else n/a; GREEN_vol_sum=3186600 dates=[2026-08-03,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13]; RED_vol_sum=2603300 dates=[2026-08-04,2026-08-05,2026-08-06,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=745900 | 2026-08-04:RED:vol=625000 | 2026-08-05:RED:vol=761600 | 2026-08-06:RED:vol=701800 | 2026-08-07:GREEN:vol=588800 | 2026-08-10:GREEN:vol=656200 | 2026-08-11:GREEN:vol=438100 | 2026-08-12:GREEN:vol=378400 | 2026-08-13:GREEN:vol=379200 | 2026-08-14:RED:vol=514900 | **GOOD** |
| `A07_rvol` | RVOL=0.694 on 2026-08-14: today_vol=514900 / avg20=742330 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.765 on 2026-08-14 (price=1173.7300, mid=1109.5795, upper=1193.4494, lower=1025.7096; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=1173.7300 vs SMA50=1049.6814 dist=+11.82% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=1109.58_50=1049.68_80=1052.70 on 2026-08-14: SMA20=1109.5795 SMA50=1049.6814 SMA80=1052.6952 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=-0.77% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-13 @ 1182.8400 → trough 2026-08-14 @ 1173.7300 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13] body_frac=0.564 wick_frac=0.436; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=26.5601,upperW=2.6100,lowerW=3.8800,range=33.0500 | 2026-08-04:RED:body=7.3700,upperW=11.8000,lowerW=0.2800,range=19.4501 | 2026-08-05:RED:body=4.9200,upperW=4.3900,lowerW=12.4099,range=21.7200 | 2026-08-06:RED:body=6.2699,upperW=4.0601,lowerW=9.0200,range=19.3500 | 2026-08-07:GREEN:body=3.8600,upperW=7.1300,lowerW=3.8800,range=14.8700 | 2026-08-10:GREEN:body=1.5801,upperW=8.5800,lowerW=3.8199,range=13.9800 | 2026-08-11:GREEN:body=18.1899,upperW=3.9601,lowerW=2.2001,range=24.3501 | 2026-08-12:GREEN:body=3.9100,upperW=1.7300,lowerW=11.9200,range=17.5601 | 2026-08-13:GREEN:body=17.0699,upperW=3.9000,lowerW=1.4600,range=22.4299 | 2026-08-14:RED:body=5.2700,upperW=1.0000,lowerW=5.2300,range=11.5000 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-04,2026-08-05,2026-08-06,2026-08-14] body_frac=0.331 wick_frac=0.669; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=26.5601,upperW=2.6100,lowerW=3.8800,range=33.0500 | 2026-08-04:RED:body=7.3700,upperW=11.8000,lowerW=0.2800,range=19.4501 | 2026-08-05:RED:body=4.9200,upperW=4.3900,lowerW=12.4099,range=21.7200 | 2026-08-06:RED:body=6.2699,upperW=4.0601,lowerW=9.0200,range=19.3500 | 2026-08-07:GREEN:body=3.8600,upperW=7.1300,lowerW=3.8800,range=14.8700 | 2026-08-10:GREEN:body=1.5801,upperW=8.5800,lowerW=3.8199,range=13.9800 | 2026-08-11:GREEN:body=18.1899,upperW=3.9601,lowerW=2.2001,range=24.3501 | 2026-08-12:GREEN:body=3.9100,upperW=1.7300,lowerW=11.9200,range=17.5601 | 2026-08-13:GREEN:body=17.0699,upperW=3.9000,lowerW=1.4600,range=22.4299 | 2026-08-14:RED:body=5.2700,upperW=1.0000,lowerW=5.2300,range=11.5000 | **GOOD** |
| `B01_eps_surprise` | 9.6 | **GOOD** |
| `B02_revenue_surprise` | 5.3 | **GOOD** |
| `B03_sales` | 27627.0 | **NEUTRAL** |
| `B04_income` | 6576.0 | **GOOD** |
| `B05_profit_margin` | 23.8 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 1324.69 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=1324.69 vs prior_export=1324.69 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.35 | **GOOD** |
| `B10_insider_transactions` | -0.35 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.35 vs prior=-0.35 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.37 | **GOOD** |
| `B13_short_float` | 1.51 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |

### KBR  ·  score **+12**  ·  Engineering & Construction
price=38.369998931884766  mcap=$4.78B  ADV=1,801,170
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-06,2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-14]  RED=[2026-08-05,2026-08-11]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.62 on 2026-08-14; prev RSI=57.77 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.77@2026-08-13 → 58.62@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.77@2026-08-13 → 58.62@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.77@2026-08-13 → 58.62@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=2.029 (= sum GREEN bodies / sum RED bodies); GREEN_sum=4.8500 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-14]; RED_sum=2.3900 dates=[2026-08-05,2026-08-11]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=36.6700,C=37.0900,body=+0.4200 | 2026-08-04:GREEN:O=36.9500,C=37.8300,body=+0.8800 | 2026-08-05:RED:O=38.0000,C=36.3300,body=-1.6700 | 2026-08-06:GREEN:O=36.5900,C=37.5700,body=+0.9800 | 2026-08-07:GREEN:O=37.5000,C=38.0400,body=+0.5400 | 2026-08-10:GREEN:O=37.7500,C=38.2900,body=+0.5400 | 2026-08-11:RED:O=38.2800,C=37.5600,body=-0.7200 | 2026-08-12:GREEN:O=36.8300,C=38.1000,body=+1.2700 | 2026-08-13:GREEN:O=38.0500,C=38.1600,body=+0.1100 | 2026-08-14:GREEN:O=38.2600,C=38.3700,body=+0.1100 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=3.860 if finite else n/a; GREEN_vol_sum=10134500 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-14]; RED_vol_sum=2625600 dates=[2026-08-05,2026-08-11]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=1645200 | 2026-08-04:GREEN:vol=1171700 | 2026-08-05:RED:vol=1364000 | 2026-08-06:GREEN:vol=1768900 | 2026-08-07:GREEN:vol=1096200 | 2026-08-10:GREEN:vol=1103800 | 2026-08-11:RED:vol=1261600 | 2026-08-12:GREEN:vol=1157800 | 2026-08-13:GREEN:vol=880100 | 2026-08-14:GREEN:vol=1310800 | **GOOD** |
| `A07_rvol` | RVOL=0.820 on 2026-08-14: today_vol=1310800 / avg20=1598935 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.622 on 2026-08-14 (price=38.3700, mid=36.8470, upper=39.2973, lower=34.3967; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=38.3700 vs SMA50=35.6599 dist=+7.60% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=36.8470 SMA50=35.6599 SMA80=35.0819 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-14 @ 38.3700 → trough 2026-08-14 @ 38.3700 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-14] body_frac=0.563 wick_frac=0.437; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.4200,upperW=0.2900,lowerW=0.1100,range=0.8200 | 2026-08-04:GREEN:body=0.8800,upperW=0.2100,lowerW=0.4300,range=1.5200 | 2026-08-05:RED:body=1.6700,upperW=0.1400,lowerW=0.1100,range=1.9200 | 2026-08-06:GREEN:body=0.9800,upperW=0.3400,lowerW=0.0400,range=1.3600 | 2026-08-07:GREEN:body=0.5400,upperW=0.0500,lowerW=0.2300,range=0.8200 | 2026-08-10:GREEN:body=0.5400,upperW=0.2800,lowerW=0.0000,range=0.8200 | 2026-08-11:RED:body=0.7200,upperW=0.0000,lowerW=0.1800,range=0.9000 | 2026-08-12:GREEN:body=1.2700,upperW=0.0400,lowerW=0.1300,range=1.4400 | 2026-08-13:GREEN:body=0.1100,upperW=0.3400,lowerW=0.7300,range=1.1800 | 2026-08-14:GREEN:body=0.1100,upperW=0.1400,lowerW=0.4100,range=0.6600 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-11] body_frac=0.848 wick_frac=0.152; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.4200,upperW=0.2900,lowerW=0.1100,range=0.8200 | 2026-08-04:GREEN:body=0.8800,upperW=0.2100,lowerW=0.4300,range=1.5200 | 2026-08-05:RED:body=1.6700,upperW=0.1400,lowerW=0.1100,range=1.9200 | 2026-08-06:GREEN:body=0.9800,upperW=0.3400,lowerW=0.0400,range=1.3600 | 2026-08-07:GREEN:body=0.5400,upperW=0.0500,lowerW=0.2300,range=0.8200 | 2026-08-10:GREEN:body=0.5400,upperW=0.2800,lowerW=0.0000,range=0.8200 | 2026-08-11:RED:body=0.7200,upperW=0.0000,lowerW=0.1800,range=0.9000 | 2026-08-12:GREEN:body=1.2700,upperW=0.0400,lowerW=0.1300,range=1.4400 | 2026-08-13:GREEN:body=0.1100,upperW=0.3400,lowerW=0.7300,range=1.1800 | 2026-08-14:GREEN:body=0.1100,upperW=0.1400,lowerW=0.4100,range=0.6600 | **BAD** |
| `B01_eps_surprise` | 10.44 | **GOOD** |
| `B02_revenue_surprise` | 5.97 | **GOOD** |
| `B03_sales` | 7723.0 | **NEUTRAL** |
| `B04_income` | 422.0 | **GOOD** |
| `B05_profit_margin` | 5.46 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 45.83 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=45.83 vs prior_export=45.83 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | 1.55 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.55 vs prior=1.55 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.14 | **GOOD** |
| `B13_short_float` | 7.22 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |

### PHR  ·  score **+12**  ·  Health Information Services
price=12.279999732971191  mcap=$0.72B  ADV=1,151,880
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-12,2026-08-13]  RED=[2026-08-07,2026-08-11,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.01 on 2026-08-14; prev RSI=68.71 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 68.71@2026-08-13 → 63.01@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.71@2026-08-13 → 63.01@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 68.71@2026-08-13 → 63.01@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=2.481 (= sum GREEN bodies / sum RED bodies); GREEN_sum=1.2900 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-12,2026-08-13]; RED_sum=0.5200 dates=[2026-08-07,2026-08-11,2026-08-14]; DOJI=[2026-08-05]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=10.9900,C=11.3000,body=+0.3100 | 2026-08-04:GREEN:O=11.0800,C=11.4400,body=+0.3600 | 2026-08-05:DOJI:O=11.4900,C=11.4900,body=+0.0000 | 2026-08-06:GREEN:O=11.3200,C=11.4200,body=+0.1000 | 2026-08-07:RED:O=12.6000,C=12.5100,body=-0.0900 | 2026-08-10:GREEN:O=12.3500,C=12.4400,body=+0.0900 | 2026-08-11:RED:O=12.2900,C=12.2700,body=-0.0200 | 2026-08-12:GREEN:O=12.1100,C=12.2600,body=+0.1500 | 2026-08-13:GREEN:O=12.3400,C=12.6200,body=+0.2800 | 2026-08-14:RED:O=12.6900,C=12.2800,body=-0.4100 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.680 if finite else n/a; GREEN_vol_sum=4407500 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-12,2026-08-13]; RED_vol_sum=2624200 dates=[2026-08-07,2026-08-11,2026-08-14]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=1035400 | 2026-08-04:GREEN:vol=784200 | 2026-08-05:DOJI:vol=627200 | 2026-08-06:GREEN:vol=788700 | 2026-08-07:RED:vol=1126100 | 2026-08-10:GREEN:vol=543500 | 2026-08-11:RED:vol=747800 | 2026-08-12:GREEN:vol=450600 | 2026-08-13:GREEN:vol=491500 | 2026-08-14:RED:vol=436700 | **GOOD** |
| `A07_rvol` | RVOL=0.606 on 2026-08-14: today_vol=436700 / avg20=720680 (avg window 2026-07-14→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.579 on 2026-08-14 (price=12.2800, mid=11.3165, upper=12.9801, lower=9.6529; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=12.2800 vs SMA50=10.4910 dist=+17.05% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=11.3165 SMA50=10.4910 SMA80=10.0557 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-2.69% inside window 2026-06-11→2026-08-14 (42 sessions): peak 2026-08-13 @ 12.6200 → trough 2026-08-14 @ 12.2800 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-12,2026-08-13] body_frac=0.576 wick_frac=0.424; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.3100,upperW=0.0900,lowerW=0.0450,range=0.4450 | 2026-08-04:GREEN:body=0.3600,upperW=0.0550,lowerW=0.0000,range=0.4150 | 2026-08-05:DOJI:body=0.0000,upperW=0.0300,lowerW=0.2800,range=0.3100 | 2026-08-06:GREEN:body=0.1000,upperW=0.0750,lowerW=0.1060,range=0.2810 | 2026-08-07:RED:body=0.0900,upperW=0.4100,lowerW=0.1050,range=0.6050 | 2026-08-10:GREEN:body=0.0900,upperW=0.0800,lowerW=0.1200,range=0.2900 | 2026-08-11:RED:body=0.0200,upperW=0.3550,lowerW=0.0800,range=0.4550 | 2026-08-12:GREEN:body=0.1500,upperW=0.0100,lowerW=0.2000,range=0.3600 | 2026-08-13:GREEN:body=0.2800,upperW=0.0500,lowerW=0.1200,range=0.4500 | 2026-08-14:RED:body=0.4100,upperW=0.0000,lowerW=0.0200,range=0.4300 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-07,2026-08-11,2026-08-14] body_frac=0.349 wick_frac=0.651; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.3100,upperW=0.0900,lowerW=0.0450,range=0.4450 | 2026-08-04:GREEN:body=0.3600,upperW=0.0550,lowerW=0.0000,range=0.4150 | 2026-08-05:DOJI:body=0.0000,upperW=0.0300,lowerW=0.2800,range=0.3100 | 2026-08-06:GREEN:body=0.1000,upperW=0.0750,lowerW=0.1060,range=0.2810 | 2026-08-07:RED:body=0.0900,upperW=0.4100,lowerW=0.1050,range=0.6050 | 2026-08-10:GREEN:body=0.0900,upperW=0.0800,lowerW=0.1200,range=0.2900 | 2026-08-11:RED:body=0.0200,upperW=0.3550,lowerW=0.0800,range=0.4550 | 2026-08-12:GREEN:body=0.1500,upperW=0.0100,lowerW=0.2000,range=0.3600 | 2026-08-13:GREEN:body=0.2800,upperW=0.0500,lowerW=0.1200,range=0.4500 | 2026-08-14:RED:body=0.4100,upperW=0.0000,lowerW=0.0200,range=0.4300 | **GOOD** |
| `B01_eps_surprise` | 338.6 | **GOOD** |
| `B02_revenue_surprise` | 0.66 | **GOOD** |
| `B03_sales` | 495.59 | **NEUTRAL** |
| `B04_income` | 9.18 | **GOOD** |
| `B05_profit_margin` | 1.85 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.94 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.94 vs prior_export=13.94 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.89 | **GOOD** |
| `B10_insider_transactions` | 29.27 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=29.27 vs prior=29.27 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -12.26 | **BAD** |
| `B13_short_float` | 5.89 | **NEUTRAL** |
| `B14_earnings_date` | 9/2/2026 4:30:00 PM | **NEUTRAL** |

### WTTR  ·  score **+12**  ·  Oil & Gas Equipment & Services
price=20.860000610351562  mcap=$2.74B  ADV=1,830,100
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-14]  RED=[2026-08-06,2026-08-07,2026-08-12,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=56.09 on 2026-08-14; prev RSI=53.53 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 53.53@2026-08-13 → 56.09@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 53.53@2026-08-13 → 56.09@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 53.53@2026-08-13 → 56.09@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.805 (= sum GREEN bodies / sum RED bodies); GREEN_sum=3.5503 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-14]; RED_sum=1.9668 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=18.1385,C=18.3178,body=+0.1794 | 2026-08-04:GREEN:O=18.0189,C=18.4374,body=+0.4186 | 2026-08-05:GREEN:O=19.9324,C=22.1847,body=+2.2524 | 2026-08-06:RED:O=21.5868,C=20.6300,body=-0.9568 | 2026-08-07:RED:O=20.5200,C=20.2300,body=-0.2900 | 2026-08-10:GREEN:O=20.6100,C=20.8900,body=+0.2800 | 2026-08-11:GREEN:O=21.0300,C=21.1100,body=+0.0800 | 2026-08-12:RED:O=21.2300,C=21.2100,body=-0.0200 | 2026-08-13:RED:O=21.0700,C=20.3700,body=-0.7000 | 2026-08-14:GREEN:O=20.5200,C=20.8600,body=+0.3400 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=2.118 if finite else n/a; GREEN_vol_sum=15030000 dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-14]; RED_vol_sum=7096000 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=947400 | 2026-08-04:GREEN:vol=3115800 | 2026-08-05:GREEN:vol=6975500 | 2026-08-06:RED:vol=2875500 | 2026-08-07:RED:vol=1759400 | 2026-08-10:GREEN:vol=1714300 | 2026-08-11:GREEN:vol=1582100 | 2026-08-12:RED:vol=1187200 | 2026-08-13:RED:vol=1273900 | 2026-08-14:GREEN:vol=694900 | **GOOD** |
| `A07_rvol` | RVOL=0.350 on 2026-08-14: today_vol=694900 / avg20=1984050 (avg window 2026-07-15→2026-08-13, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.346 on 2026-08-14 (price=20.8600, mid=19.9674, upper=22.5460, lower=17.3887; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=20.8600 vs SMA50=19.2509 dist=+8.36% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=19.9674 SMA50=19.2509 SMA80=18.6398 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-8.81% inside window 2026-06-12→2026-08-14 (42 sessions): peak 2026-08-05 @ 22.1847 → trough 2026-08-07 @ 20.2300 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-05,2026-08-10,2026-08-11,2026-08-14] body_frac=0.503 wick_frac=0.497; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.1794,upperW=0.3488,lowerW=0.3488,range=0.8770 | 2026-08-04:GREEN:body=0.4186,upperW=0.1794,lowerW=0.0000,range=0.5980 | 2026-08-05:GREEN:body=2.2524,upperW=0.2890,lowerW=0.7973,range=3.3387 | 2026-08-06:RED:body=0.9568,upperW=0.0000,lowerW=0.2392,range=1.1959 | 2026-08-07:RED:body=0.2900,upperW=1.0840,lowerW=0.0850,range=1.4590 | 2026-08-10:GREEN:body=0.2800,upperW=0.3250,lowerW=0.0000,range=0.6050 | 2026-08-11:GREEN:body=0.0800,upperW=0.7180,lowerW=0.1450,range=0.9430 | 2026-08-12:RED:body=0.0200,upperW=0.2200,lowerW=0.1700,range=0.4100 | 2026-08-13:RED:body=0.7000,upperW=0.2200,lowerW=0.0300,range=0.9500 | 2026-08-14:GREEN:body=0.3400,upperW=0.2400,lowerW=0.1100,range=0.6900 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13] body_frac=0.490 wick_frac=0.510; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.1794,upperW=0.3488,lowerW=0.3488,range=0.8770 | 2026-08-04:GREEN:body=0.4186,upperW=0.1794,lowerW=0.0000,range=0.5980 | 2026-08-05:GREEN:body=2.2524,upperW=0.2890,lowerW=0.7973,range=3.3387 | 2026-08-06:RED:body=0.9568,upperW=0.0000,lowerW=0.2392,range=1.1959 | 2026-08-07:RED:body=0.2900,upperW=1.0840,lowerW=0.0850,range=1.4590 | 2026-08-10:GREEN:body=0.2800,upperW=0.3250,lowerW=0.0000,range=0.6050 | 2026-08-11:GREEN:body=0.0800,upperW=0.7180,lowerW=0.1450,range=0.9430 | 2026-08-12:RED:body=0.0200,upperW=0.2200,lowerW=0.1700,range=0.4100 | 2026-08-13:RED:body=0.7000,upperW=0.2200,lowerW=0.0300,range=0.9500 | 2026-08-14:GREEN:body=0.3400,upperW=0.2400,lowerW=0.1100,range=0.6900 | **GOOD** |
| `B01_eps_surprise` | 78.15 | **GOOD** |
| `B02_revenue_surprise` | 6.02 | **GOOD** |
| `B03_sales` | 1430.51 | **NEUTRAL** |
| `B04_income` | 31.98 | **GOOD** |
| `B05_profit_margin` | 2.24 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 23.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=23.86 vs prior_export=23.86 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.17 | **GOOD** |
| `B10_insider_transactions` | -36.74 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-36.74 vs prior=-36.74 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 13.86 | **GOOD** |
| `B13_short_float` | 4.63 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 4:30:00 PM | **NEUTRAL** |

### BKR  ·  score **+12**  ·  Oil & Gas Equipment & Services
price=64.81999969482422  mcap=$63.96B  ADV=8,641,470
body window: `2026-08-03→2026-08-14`  GREEN=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-11,2026-08-14]  RED=[2026-08-05,2026-08-07,2026-08-12,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=65.90 on 2026-08-14; prev RSI=62.15 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 62.15@2026-08-13 → 65.90@2026-08-14 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 62.15@2026-08-13 → 65.90@2026-08-14 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 62.15@2026-08-13 → 65.90@2026-08-14 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=2.589 (= sum GREEN bodies / sum RED bodies); GREEN_sum=6.3609 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-11,2026-08-14]; RED_sum=2.4568 dates=[2026-08-05,2026-08-07,2026-08-12,2026-08-13]; DOJI=[none]; window=2026-08-03→2026-08-14 (10 sessions); sessions: 2026-08-03:GREEN:O=59.8000,C=60.5871,body=+0.7871 | 2026-08-04:GREEN:O=60.2185,C=61.5037,body=+1.2853 | 2026-08-05:RED:O=62.3108,C=61.4440,body=-0.8668 | 2026-08-06:GREEN:O=62.1115,C=62.5200,body=+0.4085 | 2026-08-07:RED:O=62.0900,C=61.5500,body=-0.5400 | 2026-08-10:GREEN:O=62.0500,C=64.0700,body=+2.0200 | 2026-08-11:GREEN:O=64.2300,C=64.8100,body=+0.5800 | 2026-08-12:RED:O=64.8100,C=64.2800,body=-0.5300 | 2026-08-13:RED:O=63.9600,C=63.4400,body=-0.5200 | 2026-08-14:GREEN:O=63.5400,C=64.8200,body=+1.2800 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.283 if finite else n/a; GREEN_vol_sum=36538900 dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-11,2026-08-14]; RED_vol_sum=28483700 dates=[2026-08-05,2026-08-07,2026-08-12,2026-08-13]; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:vol=6714000 | 2026-08-04:GREEN:vol=7363400 | 2026-08-05:RED:vol=5791700 | 2026-08-06:GREEN:vol=4996400 | 2026-08-07:RED:vol=7322500 | 2026-08-10:GREEN:vol=5273500 | 2026-08-11:GREEN:vol=5499100 | 2026-08-12:RED:vol=7498700 | 2026-08-13:RED:vol=7870800 | 2026-08-14:GREEN:vol=6692500 | **GOOD** |
| `A07_rvol` | RVOL=0.745 on 2026-08-14: today_vol=6692500 / avg20=8977230 (avg window 2026-07-17→2026-08-13, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.713 on 2026-08-14 (price=64.8200, mid=60.3265, upper=66.6327, lower=54.0203; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-14: price=64.8200 vs SMA50=59.2125 dist=+9.47% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=60.33_50=59.21_80=61.58 on 2026-08-14: SMA20=60.3265 SMA50=59.2125 SMA80=61.5810 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-16→2026-08-14 (42 sessions): peak 2026-08-14 @ 64.8200 → trough 2026-08-14 @ 64.8200 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-03,2026-08-04,2026-08-06,2026-08-10,2026-08-11,2026-08-14] body_frac=0.668 wick_frac=0.332; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.7871,upperW=0.1694,lowerW=0.9366,range=1.8930 | 2026-08-04:GREEN:body=1.2853,upperW=0.1196,lowerW=0.2590,range=1.6639 | 2026-08-05:RED:body=0.8668,upperW=0.0000,lowerW=0.5480,range=1.4148 | 2026-08-06:GREEN:body=0.4085,upperW=0.5579,lowerW=0.2491,range=1.2155 | 2026-08-07:RED:body=0.5400,upperW=0.7500,lowerW=0.0100,range=1.3000 | 2026-08-10:GREEN:body=2.0200,upperW=0.2000,lowerW=0.0500,range=2.2700 | 2026-08-11:GREEN:body=0.5800,upperW=0.0400,lowerW=0.1600,range=0.7800 | 2026-08-12:RED:body=0.5300,upperW=0.2900,lowerW=0.0400,range=0.8600 | 2026-08-13:RED:body=0.5200,upperW=0.0700,lowerW=0.9400,range=1.5300 | 2026-08-14:GREEN:body=1.2800,upperW=0.3500,lowerW=0.0700,range=1.7000 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-07,2026-08-12,2026-08-13] body_frac=0.481 wick_frac=0.519; window=2026-08-03→2026-08-14; sessions: 2026-08-03:GREEN:body=0.7871,upperW=0.1694,lowerW=0.9366,range=1.8930 | 2026-08-04:GREEN:body=1.2853,upperW=0.1196,lowerW=0.2590,range=1.6639 | 2026-08-05:RED:body=0.8668,upperW=0.0000,lowerW=0.5480,range=1.4148 | 2026-08-06:GREEN:body=0.4085,upperW=0.5579,lowerW=0.2491,range=1.2155 | 2026-08-07:RED:body=0.5400,upperW=0.7500,lowerW=0.0100,range=1.3000 | 2026-08-10:GREEN:body=2.0200,upperW=0.2000,lowerW=0.0500,range=2.2700 | 2026-08-11:GREEN:body=0.5800,upperW=0.0400,lowerW=0.1600,range=0.7800 | 2026-08-12:RED:body=0.5300,upperW=0.2900,lowerW=0.0400,range=0.8600 | 2026-08-13:RED:body=0.5200,upperW=0.0700,lowerW=0.9400,range=1.5300 | 2026-08-14:GREEN:body=1.2800,upperW=0.3500,lowerW=0.0700,range=1.7000 | **GOOD** |
| `B01_eps_surprise` | 27.57 | **GOOD** |
| `B02_revenue_surprise` | 3.35 | **GOOD** |
| `B03_sales` | 27725.0 | **NEUTRAL** |
| `B04_income` | 3096.0 | **GOOD** |
| `B05_profit_margin` | 11.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 71.86 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.3100000000000023 (now=71.86 vs prior_export=71.55 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 1.64 | **GOOD** |
| `B10_insider_transactions` | -31.32 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-31.32 vs prior=-31.32 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.87 | **GOOD** |
| `B13_short_float` | 2.48 | **NEUTRAL** |
| `B14_earnings_date` | 7/26/2026 4:30:00 PM | **NEUTRAL** |

CSV: `data/ab_checklist/2026-08-18_ab_checklist.csv`
Columns: `val_*` (full dated string), `flag_*` (+1/0/-1), `status_*`, plus `green_dates` / `red_dates` / `body_window`.