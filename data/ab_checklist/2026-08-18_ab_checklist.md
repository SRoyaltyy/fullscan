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

## Ranked (top 15)

| Rank | Ticker | score | good | bad | Industry |
|-----:|--------|------:|-----:|----:|----------|
| 1 | ERO | +13 | 14 | 1 | Copper |
| 2 | ET | +13 | 15 | 2 | Oil & Gas Midstream |
| 3 | OKE | +13 | 15 | 2 | Oil & Gas Midstream |
| 4 | DCTH | +12 | 14 | 2 | Medical Devices |
| 5 | FTI | +12 | 14 | 2 | Oil & Gas Equipment & Services |
| 6 | DHT | +12 | 14 | 2 | Oil & Gas Midstream |
| 7 | ELV | +12 | 14 | 2 | Healthcare Plans |
| 8 | BBDC | +12 | 13 | 1 | Asset Management |
| 9 | WTTR | +12 | 14 | 2 | Oil & Gas Equipment & Services |
| 10 | RHI | +12 | 13 | 1 | Staffing & Employment Services |
| 11 | FA | +12 | 14 | 2 | Specialty Business Services |
| 12 | NOMD | +12 | 14 | 2 | Packaged Foods |
| 13 | EBC | +12 | 13 | 1 | Banks - Regional |
| 14 | DUOL | +12 | 14 | 2 | Software - Application |
| 15 | HAE | +12 | 15 | 3 | Medical Devices |

## Full checklist with dates — top 15

### ERO  ·  score **+13**  ·  Copper
price=32.93000030517578  mcap=$3.47B  ADV=1,195,000
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-05,2026-08-06,2026-08-07,2026-08-10,2026-08-17]  RED=[2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-18]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=58.40 on 2026-08-18; prev RSI=66.97 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.97@2026-08-17 → 58.40@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.97@2026-08-17 → 58.40@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.97@2026-08-17 → 58.40@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.812 (= sum GREEN bodies / sum RED bodies); GREEN_sum=6.8500 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-10,2026-08-17]; RED_sum=3.7800 dates=[2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-18]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:GREEN:O=29.9300,C=30.4300,body=+0.5000 | 2026-08-06:GREEN:O=29.9100,C=31.1000,body=+1.1900 | 2026-08-07:GREEN:O=32.0000,C=34.3100,body=+2.3100 | 2026-08-10:GREEN:O=34.3100,C=36.5400,body=+2.2300 | 2026-08-11:RED:O=36.5000,C=36.3000,body=-0.2000 | 2026-08-12:RED:O=36.6900,C=36.0700,body=-0.6200 | 2026-08-13:RED:O=35.3700,C=34.3700,body=-1.0000 | 2026-08-14:RED:O=34.7100,C=33.8000,body=-0.9100 | 2026-08-17:GREEN:O=34.2900,C=34.9100,body=+0.6200 | 2026-08-18:RED:O=33.9800,C=32.9300,body=-1.0500 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.541 if finite else n/a; GREEN_vol_sum=9966100 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-10,2026-08-17]; RED_vol_sum=6466775 dates=[2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-18]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:vol=1271500 | 2026-08-06:GREEN:vol=1389400 | 2026-08-07:GREEN:vol=2753000 | 2026-08-10:GREEN:vol=3557800 | 2026-08-11:RED:vol=1769000 | 2026-08-12:RED:vol=1315700 | 2026-08-13:RED:vol=1528400 | 2026-08-14:RED:vol=992600 | 2026-08-17:GREEN:vol=994400 | 2026-08-18:RED:vol=861075 | **GOOD** |
| `A07_rvol` | RVOL=0.691 on 2026-08-18: today_vol=861075 / avg20=1246460 (avg window 2026-07-17→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.313 on 2026-08-18 (price=32.9300, mid=30.2790, upper=38.7369, lower=21.8211; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=32.9300 vs SMA50=28.2334 dist=+16.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=30.2790 SMA50=28.2334 SMA80=28.1559 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-9.88% inside window 2026-06-16→2026-08-18 (42 sessions): peak 2026-08-10 @ 36.5400 → trough 2026-08-18 @ 32.9300 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-10,2026-08-17] body_frac=0.723 wick_frac=0.277; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=0.5000,upperW=0.2700,lowerW=0.3200,range=1.0900 | 2026-08-06:GREEN:body=1.1900,upperW=0.3850,lowerW=0.5500,range=2.1250 | 2026-08-07:GREEN:body=2.3100,upperW=0.0350,lowerW=0.3600,range=2.7050 | 2026-08-10:GREEN:body=2.2300,upperW=0.0800,lowerW=0.0000,range=2.3100 | 2026-08-11:RED:body=0.2000,upperW=0.0700,lowerW=0.8800,range=1.1500 | 2026-08-12:RED:body=0.6200,upperW=0.1650,lowerW=0.2200,range=1.0050 | 2026-08-13:RED:body=1.0000,upperW=0.1600,lowerW=0.3000,range=1.4600 | 2026-08-14:RED:body=0.9100,upperW=0.3100,lowerW=0.2000,range=1.4200 | 2026-08-17:GREEN:body=0.6200,upperW=0.2900,lowerW=0.3400,range=1.2500 | 2026-08-18:RED:body=1.0500,upperW=0.4416,lowerW=0.0594,range=1.5510 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-18] body_frac=0.574 wick_frac=0.426; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=0.5000,upperW=0.2700,lowerW=0.3200,range=1.0900 | 2026-08-06:GREEN:body=1.1900,upperW=0.3850,lowerW=0.5500,range=2.1250 | 2026-08-07:GREEN:body=2.3100,upperW=0.0350,lowerW=0.3600,range=2.7050 | 2026-08-10:GREEN:body=2.2300,upperW=0.0800,lowerW=0.0000,range=2.3100 | 2026-08-11:RED:body=0.2000,upperW=0.0700,lowerW=0.8800,range=1.1500 | 2026-08-12:RED:body=0.6200,upperW=0.1650,lowerW=0.2200,range=1.0050 | 2026-08-13:RED:body=1.0000,upperW=0.1600,lowerW=0.3000,range=1.4600 | 2026-08-14:RED:body=0.9100,upperW=0.3100,lowerW=0.2000,range=1.4200 | 2026-08-17:GREEN:body=0.6200,upperW=0.2900,lowerW=0.3400,range=1.2500 | 2026-08-18:RED:body=1.0500,upperW=0.4416,lowerW=0.0594,range=1.5510 | **BAD** |
| `B01_eps_surprise` | 12.7 | **GOOD** |
| `B02_revenue_surprise` | 2.4 | **GOOD** |
| `B03_sales` | 1044.73 | **NEUTRAL** |
| `B04_income` | 311.26 | **GOOD** |
| `B05_profit_margin` | 29.79 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 36.31 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.020000000000003126 (now=36.31 vs prior_export=36.29 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 1.72 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.21 | **GOOD** |
| `B13_short_float` | 5.07 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |

### ET  ·  score **+13**  ·  Oil & Gas Midstream
price=21.350099563598633  mcap=$73.33B  ADV=9,092,490
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-14,2026-08-18]  RED=[2026-08-05,2026-08-07,2026-08-13,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=72.21 on 2026-08-18; prev RSI=67.22 on 2026-08-17 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 67.22@2026-08-17 → 72.21@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 67.22@2026-08-17 → 72.21@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 67.22@2026-08-17 → 72.21@2026-08-18 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.944 (= sum GREEN bodies / sum RED bodies); GREEN_sum=1.3483 dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-14,2026-08-18]; RED_sum=0.4580 dates=[2026-08-05,2026-08-07,2026-08-13,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=20.1235,C=20.0054,body=-0.1180 | 2026-08-06:GREEN:O=20.2218,C=20.3300,body=+0.1082 | 2026-08-07:RED:O=20.2000,C=20.1300,body=-0.0700 | 2026-08-10:GREEN:O=20.2400,C=20.5900,body=+0.3500 | 2026-08-11:GREEN:O=20.5800,C=20.7800,body=+0.2000 | 2026-08-12:GREEN:O=20.7800,C=20.9500,body=+0.1700 | 2026-08-13:RED:O=20.9300,C=20.7600,body=-0.1700 | 2026-08-14:GREEN:O=20.7800,C=21.0500,body=+0.2700 | 2026-08-17:RED:O=21.0400,C=20.9400,body=-0.1000 | 2026-08-18:GREEN:O=21.1000,C=21.3501,body=+0.2501 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.451 if finite else n/a; GREEN_vol_sum=56105184 dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-14,2026-08-18]; RED_vol_sum=38663900 dates=[2026-08-05,2026-08-07,2026-08-13,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=14107400 | 2026-08-06:GREEN:vol=12519300 | 2026-08-07:RED:vol=11093000 | 2026-08-10:GREEN:vol=11743800 | 2026-08-11:GREEN:vol=8357200 | 2026-08-12:GREEN:vol=9004700 | 2026-08-13:RED:vol=8122500 | 2026-08-14:GREEN:vol=6945200 | 2026-08-17:RED:vol=5341000 | 2026-08-18:GREEN:vol=7534984 | **GOOD** |
| `A07_rvol` | RVOL=0.778 on 2026-08-18: today_vol=7534984 / avg20=9686995 (avg window 2026-07-21→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.048 on 2026-08-18 (price=21.3501, mid=20.3111, upper=21.3022, lower=19.3200; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-18: price=21.3501 vs SMA50=19.5755 dist=+9.07% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=20.3111 SMA50=19.5755 SMA80=19.4835 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-18→2026-08-18 (42 sessions): peak 2026-08-18 @ 21.3501 → trough 2026-08-18 @ 21.3501 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-14,2026-08-18] body_frac=0.715 wick_frac=0.285; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.1180,upperW=0.1377,lowerW=0.1475,range=0.4033 | 2026-08-06:GREEN:body=0.1082,upperW=0.1377,lowerW=0.0688,range=0.3147 | 2026-08-07:RED:body=0.0700,upperW=0.2400,lowerW=0.0200,range=0.3300 | 2026-08-10:GREEN:body=0.3500,upperW=0.0400,lowerW=0.0400,range=0.4300 | 2026-08-11:GREEN:body=0.2000,upperW=0.1000,lowerW=0.0000,range=0.3000 | 2026-08-12:GREEN:body=0.1700,upperW=0.0100,lowerW=0.0600,range=0.2400 | 2026-08-13:RED:body=0.1700,upperW=0.1900,lowerW=0.0100,range=0.3700 | 2026-08-14:GREEN:body=0.2700,upperW=0.0500,lowerW=0.0000,range=0.3200 | 2026-08-17:RED:body=0.1000,upperW=0.0300,lowerW=0.0800,range=0.2100 | 2026-08-18:GREEN:body=0.2501,upperW=0.0099,lowerW=0.0200,range=0.2800 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-07,2026-08-13,2026-08-17] body_frac=0.349 wick_frac=0.651; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.1180,upperW=0.1377,lowerW=0.1475,range=0.4033 | 2026-08-06:GREEN:body=0.1082,upperW=0.1377,lowerW=0.0688,range=0.3147 | 2026-08-07:RED:body=0.0700,upperW=0.2400,lowerW=0.0200,range=0.3300 | 2026-08-10:GREEN:body=0.3500,upperW=0.0400,lowerW=0.0400,range=0.4300 | 2026-08-11:GREEN:body=0.2000,upperW=0.1000,lowerW=0.0000,range=0.3000 | 2026-08-12:GREEN:body=0.1700,upperW=0.0100,lowerW=0.0600,range=0.2400 | 2026-08-13:RED:body=0.1700,upperW=0.1900,lowerW=0.0100,range=0.3700 | 2026-08-14:GREEN:body=0.2700,upperW=0.0500,lowerW=0.0000,range=0.3200 | 2026-08-17:RED:body=0.1000,upperW=0.0300,lowerW=0.0800,range=0.2100 | 2026-08-18:GREEN:body=0.2501,upperW=0.0099,lowerW=0.0200,range=0.2800 | **GOOD** |
| `B01_eps_surprise` | 55.43 | **GOOD** |
| `B02_revenue_surprise` | 23.9 | **GOOD** |
| `B03_sales` | 107379.0 | **NEUTRAL** |
| `B04_income` | 5048.0 | **GOOD** |
| `B05_profit_margin` | 4.7 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 24.51 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.120000000000001 (now=24.51 vs prior_export=24.39 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 1.45 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.64 | **GOOD** |
| `B13_short_float` | 0.86 | **NEUTRAL** |
| `B14_earnings_date` | 8/4/2026 8:30:00 AM | **NEUTRAL** |

### OKE  ·  score **+13**  ·  Oil & Gas Midstream
price=97.625  mcap=$61.27B  ADV=3,757,950
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]  RED=[2026-08-05,2026-08-06,2026-08-07]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=70.40 on 2026-08-18; prev RSI=66.10 on 2026-08-17 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 66.10@2026-08-17 → 70.40@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.10@2026-08-17 → 70.40@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 66.10@2026-08-17 → 70.40@2026-08-18 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=4.720 (= sum GREEN bodies / sum RED bodies); GREEN_sum=9.2050 dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]; RED_sum=1.9500 dates=[2026-08-05,2026-08-06,2026-08-07]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=87.8500,C=87.2400,body=-0.6100 | 2026-08-06:RED:O=88.8200,C=87.9300,body=-0.8900 | 2026-08-07:RED:O=86.8700,C=86.4200,body=-0.4500 | 2026-08-10:GREEN:O=87.3200,C=90.3400,body=+3.0200 | 2026-08-11:GREEN:O=90.2800,C=91.5200,body=+1.2400 | 2026-08-12:GREEN:O=90.9300,C=92.4700,body=+1.5400 | 2026-08-13:GREEN:O=91.9600,C=92.6400,body=+0.6800 | 2026-08-14:GREEN:O=93.5000,C=94.9900,body=+1.4900 | 2026-08-17:GREEN:O=95.1700,C=95.3200,body=+0.1500 | 2026-08-18:GREEN:O=96.5400,C=97.6250,body=+1.0850 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.866 if finite else n/a; GREEN_vol_sum=22267007 dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]; RED_vol_sum=11933700 dates=[2026-08-05,2026-08-06,2026-08-07]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=3984400 | 2026-08-06:RED:vol=5208400 | 2026-08-07:RED:vol=2740900 | 2026-08-10:GREEN:vol=3091800 | 2026-08-11:GREEN:vol=3344400 | 2026-08-12:GREEN:vol=3418500 | 2026-08-13:GREEN:vol=3476400 | 2026-08-14:GREEN:vol=2479100 | 2026-08-17:GREEN:vol=3815700 | 2026-08-18:GREEN:vol=2641107 | **GOOD** |
| `A07_rvol` | RVOL=0.688 on 2026-08-18: today_vol=2641107 / avg20=3836240 (avg window 2026-07-17→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.140 on 2026-08-18 (price=97.6250, mid=90.5948, upper=96.7615, lower=84.4281; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-18: price=97.6250 vs SMA50=88.9656 dist=+9.73% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=90.5948 SMA50=88.9656 SMA80=88.5051 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-16→2026-08-18 (42 sessions): peak 2026-08-18 @ 97.6250 → trough 2026-08-18 @ 97.6250 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18] body_frac=0.692 wick_frac=0.308; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.6100,upperW=0.1100,lowerW=1.9600,range=2.6800 | 2026-08-06:RED:body=0.8900,upperW=0.1900,lowerW=1.1900,range=2.2700 | 2026-08-07:RED:body=0.4500,upperW=1.3600,lowerW=0.2200,range=2.0300 | 2026-08-10:GREEN:body=3.0200,upperW=0.0300,lowerW=0.0000,range=3.0500 | 2026-08-11:GREEN:body=1.2400,upperW=0.3400,lowerW=0.3500,range=1.9300 | 2026-08-12:GREEN:body=1.5400,upperW=0.0200,lowerW=0.0900,range=1.6500 | 2026-08-13:GREEN:body=0.6800,upperW=0.8100,lowerW=0.3000,range=1.7900 | 2026-08-14:GREEN:body=1.4900,upperW=0.0600,lowerW=0.6800,range=2.2300 | 2026-08-17:GREEN:body=0.1500,upperW=0.1600,lowerW=0.8500,range=1.1600 | 2026-08-18:GREEN:body=1.0850,upperW=0.2750,lowerW=0.1400,range=1.5000 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-07] body_frac=0.279 wick_frac=0.721; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.6100,upperW=0.1100,lowerW=1.9600,range=2.6800 | 2026-08-06:RED:body=0.8900,upperW=0.1900,lowerW=1.1900,range=2.2700 | 2026-08-07:RED:body=0.4500,upperW=1.3600,lowerW=0.2200,range=2.0300 | 2026-08-10:GREEN:body=3.0200,upperW=0.0300,lowerW=0.0000,range=3.0500 | 2026-08-11:GREEN:body=1.2400,upperW=0.3400,lowerW=0.3500,range=1.9300 | 2026-08-12:GREEN:body=1.5400,upperW=0.0200,lowerW=0.0900,range=1.6500 | 2026-08-13:GREEN:body=0.6800,upperW=0.8100,lowerW=0.3000,range=1.7900 | 2026-08-14:GREEN:body=1.4900,upperW=0.0600,lowerW=0.6800,range=2.2300 | 2026-08-17:GREEN:body=0.1500,upperW=0.1600,lowerW=0.8500,range=1.1600 | 2026-08-18:GREEN:body=1.0850,upperW=0.2750,lowerW=0.1400,range=1.5000 | **GOOD** |
| `B01_eps_surprise` | 5.1 | **GOOD** |
| `B02_revenue_surprise` | 34.63 | **GOOD** |
| `B03_sales` | 39646.0 | **NEUTRAL** |
| `B04_income` | 3656.0 | **GOOD** |
| `B05_profit_margin` | 9.22 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 96.26 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.10000000000000853 (now=96.26 vs prior_export=96.16 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 2.29 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 2.08 | **GOOD** |
| `B13_short_float` | 4.72 | **NEUTRAL** |
| `B14_earnings_date` | 8/3/2026 4:30:00 PM | **NEUTRAL** |

### DCTH  ·  score **+12**  ·  Medical Devices
price=16.68000030517578  mcap=$0.58B  ADV=564,320
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-17]  RED=[2026-08-05,2026-08-07,2026-08-14,2026-08-18]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.16 on 2026-08-18; prev RSI=71.01 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 71.01@2026-08-17 → 67.16@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 71.01@2026-08-17 → 67.16@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 71.01@2026-08-17 → 67.16@2026-08-18 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.055 (= sum GREEN bodies / sum RED bodies); GREEN_sum=4.0800 dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-17]; RED_sum=1.9850 dates=[2026-08-05,2026-08-07,2026-08-14,2026-08-18]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=13.2100,C=12.6300,body=-0.5800 | 2026-08-06:GREEN:O=14.0300,C=15.4700,body=+1.4400 | 2026-08-07:RED:O=15.6700,C=15.1100,body=-0.5600 | 2026-08-10:GREEN:O=15.0000,C=16.6700,body=+1.6700 | 2026-08-11:GREEN:O=16.8800,C=17.2200,body=+0.3400 | 2026-08-12:GREEN:O=17.1000,C=17.1100,body=+0.0100 | 2026-08-13:GREEN:O=17.1100,C=17.3600,body=+0.2500 | 2026-08-14:RED:O=17.3000,C=16.7300,body=-0.5700 | 2026-08-17:GREEN:O=16.6800,C=17.0500,body=+0.3700 | 2026-08-18:RED:O=16.9550,C=16.6800,body=-0.2750 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=3.188 if finite else n/a; GREEN_vol_sum=7820800 dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-17]; RED_vol_sum=2453184 dates=[2026-08-05,2026-08-07,2026-08-14,2026-08-18]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=432600 | 2026-08-06:GREEN:vol=2986700 | 2026-08-07:RED:vol=1212800 | 2026-08-10:GREEN:vol=1483200 | 2026-08-11:GREEN:vol=1228800 | 2026-08-12:GREEN:vol=774700 | 2026-08-13:GREEN:vol=814800 | 2026-08-14:RED:vol=529500 | 2026-08-17:GREEN:vol=532600 | 2026-08-18:RED:vol=278284 | **GOOD** |
| `A07_rvol` | RVOL=0.391 on 2026-08-18: today_vol=278284 / avg20=712240 (avg window 2026-07-20→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.534 on 2026-08-18 (price=16.6800, mid=14.2500, upper=18.7978, lower=9.7022; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=16.6800 vs SMA50=13.1373 dist=+26.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=14.2500 SMA50=13.1373 SMA80=12.3194 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-3.92% inside window 2026-06-17→2026-08-18 (42 sessions): peak 2026-08-13 @ 17.3600 → trough 2026-08-18 @ 16.6800 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-06,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-17] body_frac=0.682 wick_frac=0.318; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.5800,upperW=0.0390,lowerW=0.2000,range=0.8190 | 2026-08-06:GREEN:body=1.4400,upperW=0.1500,lowerW=0.0300,range=1.6200 | 2026-08-07:RED:body=0.5600,upperW=0.7050,lowerW=0.1450,range=1.4100 | 2026-08-10:GREEN:body=1.6700,upperW=0.0200,lowerW=0.0000,range=1.6900 | 2026-08-11:GREEN:body=0.3400,upperW=0.2000,lowerW=0.3950,range=0.9350 | 2026-08-12:GREEN:body=0.0100,upperW=0.1700,lowerW=0.4700,range=0.6500 | 2026-08-13:GREEN:body=0.2500,upperW=0.0400,lowerW=0.2500,range=0.5400 | 2026-08-14:RED:body=0.5700,upperW=0.1000,lowerW=0.0550,range=0.7250 | 2026-08-17:GREEN:body=0.3700,upperW=0.0100,lowerW=0.1700,range=0.5500 | 2026-08-18:RED:body=0.2750,upperW=0.2150,lowerW=0.0600,range=0.5500 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-07,2026-08-14,2026-08-18] body_frac=0.566 wick_frac=0.434; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.5800,upperW=0.0390,lowerW=0.2000,range=0.8190 | 2026-08-06:GREEN:body=1.4400,upperW=0.1500,lowerW=0.0300,range=1.6200 | 2026-08-07:RED:body=0.5600,upperW=0.7050,lowerW=0.1450,range=1.4100 | 2026-08-10:GREEN:body=1.6700,upperW=0.0200,lowerW=0.0000,range=1.6900 | 2026-08-11:GREEN:body=0.3400,upperW=0.2000,lowerW=0.3950,range=0.9350 | 2026-08-12:GREEN:body=0.0100,upperW=0.1700,lowerW=0.4700,range=0.6500 | 2026-08-13:GREEN:body=0.2500,upperW=0.0400,lowerW=0.2500,range=0.5400 | 2026-08-14:RED:body=0.5700,upperW=0.1000,lowerW=0.0550,range=0.7250 | 2026-08-17:GREEN:body=0.3700,upperW=0.0100,lowerW=0.1700,range=0.5500 | 2026-08-18:RED:body=0.2750,upperW=0.2150,lowerW=0.0600,range=0.5500 | **BAD** |
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

### FTI  ·  score **+12**  ·  Oil & Gas Equipment & Services
price=79.12999725341797  mcap=$31.02B  ADV=4,714,260
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-07,2026-08-10,2026-08-11,2026-08-13,2026-08-14,2026-08-17]  RED=[2026-08-05,2026-08-06,2026-08-12,2026-08-18]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.66 on 2026-08-18; prev RSI=70.35 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 70.35@2026-08-17 → 67.66@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 70.35@2026-08-17 → 67.66@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_down | RSI 70.35@2026-08-17 → 67.66@2026-08-18 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=3.956 (= sum GREEN bodies / sum RED bodies); GREEN_sum=10.8400 dates=[2026-08-07,2026-08-10,2026-08-11,2026-08-13,2026-08-14,2026-08-17]; RED_sum=2.7400 dates=[2026-08-05,2026-08-06,2026-08-12,2026-08-18]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=70.6000,C=69.2700,body=-1.3300 | 2026-08-06:RED:O=70.3400,C=69.6200,body=-0.7200 | 2026-08-07:GREEN:O=68.5700,C=69.6200,body=+1.0500 | 2026-08-10:GREEN:O=70.4300,C=74.1300,body=+3.7000 | 2026-08-11:GREEN:O=74.0000,C=75.3200,body=+1.3200 | 2026-08-12:RED:O=75.4700,C=75.2700,body=-0.2000 | 2026-08-13:GREEN:O=74.6500,C=76.6700,body=+2.0200 | 2026-08-14:GREEN:O=76.9500,C=78.6100,body=+1.6600 | 2026-08-17:GREEN:O=78.7200,C=79.8100,body=+1.0900 | 2026-08-18:RED:O=79.6200,C=79.1300,body=-0.4900 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=2.200 if finite else n/a; GREEN_vol_sum=21715400 dates=[2026-08-07,2026-08-10,2026-08-11,2026-08-13,2026-08-14,2026-08-17]; RED_vol_sum=9869738 dates=[2026-08-05,2026-08-06,2026-08-12,2026-08-18]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=3364100 | 2026-08-06:RED:vol=2988500 | 2026-08-07:GREEN:vol=3377400 | 2026-08-10:GREEN:vol=4986300 | 2026-08-11:GREEN:vol=3011400 | 2026-08-12:RED:vol=2454900 | 2026-08-13:GREEN:vol=2690900 | 2026-08-14:GREEN:vol=3748900 | 2026-08-17:GREEN:vol=3900500 | 2026-08-18:RED:vol=1062238 | **GOOD** |
| `A07_rvol` | RVOL=0.312 on 2026-08-18: today_vol=1062238 / avg20=3400680 (avg window 2026-07-16→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.778 on 2026-08-18 (price=79.1300, mid=73.5815, upper=80.7170, lower=66.4460; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=79.1300 vs SMA50=70.4856 dist=+12.26% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=73.58_50=70.49_80=71.19 on 2026-08-18: SMA20=73.5815 SMA50=70.4856 SMA80=71.1859 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=-0.85% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-17 @ 79.8100 → trough 2026-08-18 @ 79.1300 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-10,2026-08-11,2026-08-13,2026-08-14,2026-08-17] body_frac=0.655 wick_frac=0.345; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=1.3300,upperW=0.4800,lowerW=0.2400,range=2.0500 | 2026-08-06:RED:body=0.7200,upperW=0.4600,lowerW=0.0500,range=1.2300 | 2026-08-07:GREEN:body=1.0500,upperW=0.4900,lowerW=0.8100,range=2.3500 | 2026-08-10:GREEN:body=3.7000,upperW=0.3400,lowerW=0.8100,range=4.8500 | 2026-08-11:GREEN:body=1.3200,upperW=0.1500,lowerW=0.4000,range=1.8700 | 2026-08-12:RED:body=0.2000,upperW=1.0700,lowerW=0.3300,range=1.6000 | 2026-08-13:GREEN:body=2.0200,upperW=0.1400,lowerW=0.6600,range=2.8200 | 2026-08-14:GREEN:body=1.6600,upperW=0.6200,lowerW=0.3500,range=2.6300 | 2026-08-17:GREEN:body=1.0900,upperW=0.6100,lowerW=0.3400,range=2.0400 | 2026-08-18:RED:body=0.4900,upperW=0.6900,lowerW=0.5450,range=1.7250 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-12,2026-08-18] body_frac=0.415 wick_frac=0.585; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=1.3300,upperW=0.4800,lowerW=0.2400,range=2.0500 | 2026-08-06:RED:body=0.7200,upperW=0.4600,lowerW=0.0500,range=1.2300 | 2026-08-07:GREEN:body=1.0500,upperW=0.4900,lowerW=0.8100,range=2.3500 | 2026-08-10:GREEN:body=3.7000,upperW=0.3400,lowerW=0.8100,range=4.8500 | 2026-08-11:GREEN:body=1.3200,upperW=0.1500,lowerW=0.4000,range=1.8700 | 2026-08-12:RED:body=0.2000,upperW=1.0700,lowerW=0.3300,range=1.6000 | 2026-08-13:GREEN:body=2.0200,upperW=0.1400,lowerW=0.6600,range=2.8200 | 2026-08-14:GREEN:body=1.6600,upperW=0.6200,lowerW=0.3500,range=2.6300 | 2026-08-17:GREEN:body=1.0900,upperW=0.6100,lowerW=0.3400,range=2.0400 | 2026-08-18:RED:body=0.4900,upperW=0.6900,lowerW=0.5450,range=1.7250 | **GOOD** |
| `B01_eps_surprise` | 13.68 | **GOOD** |
| `B02_revenue_surprise` | 3.49 | **GOOD** |
| `B03_sales` | 10343.7 | **NEUTRAL** |
| `B04_income` | 1175.6 | **GOOD** |
| `B05_profit_margin` | 11.37 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 76.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.15000000000000568 (now=76.25 vs prior_export=76.1 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 2.0 | **GOOD** |
| `B10_insider_transactions` | -16.8 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-16.8 vs prior=-16.8 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.55 | **GOOD** |
| `B13_short_float` | 3.46 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 8:30:00 AM | **NEUTRAL** |

### DHT  ·  score **+12**  ·  Oil & Gas Midstream
price=19.139999389648438  mcap=$3.07B  ADV=3,236,820
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-07,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]  RED=[2026-08-05,2026-08-06,2026-08-10,2026-08-11]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=59.95 on 2026-08-18; prev RSI=57.89 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 57.89@2026-08-17 → 59.95@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 57.89@2026-08-17 → 59.95@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 57.89@2026-08-17 → 59.95@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.964 (= sum GREEN bodies / sum RED bodies); GREEN_sum=2.0819 dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]; RED_sum=1.0600 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=18.2500,C=17.8700,body=-0.3800 | 2026-08-06:RED:O=18.4800,C=18.3500,body=-0.1300 | 2026-08-07:GREEN:O=18.3800,C=18.7600,body=+0.3800 | 2026-08-10:RED:O=18.7600,C=18.3300,body=-0.4300 | 2026-08-11:RED:O=18.3200,C=18.2000,body=-0.1200 | 2026-08-12:GREEN:O=17.0156,C=17.5500,body=+0.5344 | 2026-08-13:GREEN:O=17.6344,C=18.0000,body=+0.3656 | 2026-08-14:GREEN:O=18.1781,C=18.3000,body=+0.1219 | 2026-08-17:GREEN:O=18.4200,C=18.9000,body=+0.4800 | 2026-08-18:GREEN:O=18.9400,C=19.1400,body=+0.2000 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.355 if finite else n/a; GREEN_vol_sum=18459546 dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18]; RED_vol_sum=13621300 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=4307900 | 2026-08-06:RED:vol=3689600 | 2026-08-07:GREEN:vol=3122000 | 2026-08-10:RED:vol=2443900 | 2026-08-11:RED:vol=3179900 | 2026-08-12:GREEN:vol=4073700 | 2026-08-13:GREEN:vol=2981100 | 2026-08-14:GREEN:vol=3314000 | 2026-08-17:GREEN:vol=3506600 | 2026-08-18:GREEN:vol=1462146 | **GOOD** |
| `A07_rvol` | RVOL=0.511 on 2026-08-18: today_vol=1462146 / avg20=2863185 (avg window 2026-07-21→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=1.064 on 2026-08-18 (price=19.1400, mid=18.3805, upper=19.0946, lower=17.6664; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-18: price=19.1400 vs SMA50=17.9922 dist=+6.38% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=18.3805 SMA50=17.9922 SMA80=17.7681 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-17.18% inside window 2026-06-18→2026-08-18 (42 sessions): peak 2026-06-23 @ 19.9600 → trough 2026-06-30 @ 16.5300 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-14,2026-08-17,2026-08-18] body_frac=0.601 wick_frac=0.399; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.3800,upperW=0.2400,lowerW=0.1000,range=0.7200 | 2026-08-06:RED:body=0.1300,upperW=0.3200,lowerW=0.2400,range=0.6900 | 2026-08-07:GREEN:body=0.3800,upperW=0.0700,lowerW=0.2500,range=0.7000 | 2026-08-10:RED:body=0.4300,upperW=0.1000,lowerW=0.0400,range=0.5700 | 2026-08-11:RED:body=0.1200,upperW=0.0800,lowerW=0.4400,range=0.6400 | 2026-08-12:GREEN:body=0.5344,upperW=0.1406,lowerW=0.0000,range=0.6750 | 2026-08-13:GREEN:body=0.3656,upperW=0.2062,lowerW=0.0000,range=0.5719 | 2026-08-14:GREEN:body=0.1219,upperW=0.1781,lowerW=0.1500,range=0.4500 | 2026-08-17:GREEN:body=0.4800,upperW=0.0200,lowerW=0.1500,range=0.6500 | 2026-08-18:GREEN:body=0.2000,upperW=0.0800,lowerW=0.1400,range=0.4200 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11] body_frac=0.405 wick_frac=0.595; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.3800,upperW=0.2400,lowerW=0.1000,range=0.7200 | 2026-08-06:RED:body=0.1300,upperW=0.3200,lowerW=0.2400,range=0.6900 | 2026-08-07:GREEN:body=0.3800,upperW=0.0700,lowerW=0.2500,range=0.7000 | 2026-08-10:RED:body=0.4300,upperW=0.1000,lowerW=0.0400,range=0.5700 | 2026-08-11:RED:body=0.1200,upperW=0.0800,lowerW=0.4400,range=0.6400 | 2026-08-12:GREEN:body=0.5344,upperW=0.1406,lowerW=0.0000,range=0.6750 | 2026-08-13:GREEN:body=0.3656,upperW=0.2062,lowerW=0.0000,range=0.5719 | 2026-08-14:GREEN:body=0.1219,upperW=0.1781,lowerW=0.1500,range=0.4500 | 2026-08-17:GREEN:body=0.4800,upperW=0.0200,lowerW=0.1500,range=0.6500 | 2026-08-18:GREEN:body=0.2000,upperW=0.0800,lowerW=0.1400,range=0.4200 | **GOOD** |
| `B01_eps_surprise` | 4.02 | **GOOD** |
| `B02_revenue_surprise` | 3.15 | **GOOD** |
| `B03_sales` | 722.19 | **NEUTRAL** |
| `B04_income` | 473.73 | **GOOD** |
| `B05_profit_margin` | 65.6 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 21.34 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=21.34 vs prior_export=21.34 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.67 | **GOOD** |
| `B10_insider_transactions` | -0.16 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.16 vs prior=-0.16 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 14.32 | **GOOD** |
| `B13_short_float` | 7.37 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |

### ELV  ·  score **+12**  ·  Healthcare Plans
price=399.9200134277344  mcap=$86.06B  ADV=1,527,970
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-18]  RED=[2026-08-06,2026-08-11,2026-08-13,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=54.28 on 2026-08-18; prev RSI=48.70 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.70@2026-08-17 → 54.28@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.70@2026-08-17 → 54.28@2026-08-18 vs level 50 | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.70@2026-08-17 → 54.28@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=2.399 (= sum GREEN bodies / sum RED bodies); GREEN_sum=41.8300 dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-18]; RED_sum=17.4400 dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:GREEN:O=375.2000,C=391.2400,body=+16.0400 | 2026-08-06:RED:O=391.8300,C=391.4000,body=-0.4300 | 2026-08-07:GREEN:O=390.3900,C=394.2000,body=+3.8100 | 2026-08-10:GREEN:O=395.3100,C=397.7000,body=+2.3900 | 2026-08-11:RED:O=398.2200,C=390.3700,body=-7.8500 | 2026-08-12:GREEN:O=386.9400,C=399.1400,body=+12.2000 | 2026-08-13:RED:O=400.8000,C=398.2200,body=-2.5800 | 2026-08-14:GREEN:O=398.3900,C=400.3200,body=+1.9300 | 2026-08-17:RED:O=396.8200,C=390.2400,body=-6.5800 | 2026-08-18:GREEN:O=394.4600,C=399.9200,body=+5.4600 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.449 if finite else n/a; GREEN_vol_sum=4885588 dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-18]; RED_vol_sum=3371100 dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:vol=1165300 | 2026-08-06:RED:vol=910900 | 2026-08-07:GREEN:vol=941900 | 2026-08-10:GREEN:vol=854600 | 2026-08-11:RED:vol=907300 | 2026-08-12:GREEN:vol=786700 | 2026-08-13:RED:vol=652300 | 2026-08-14:GREEN:vol=730800 | 2026-08-17:RED:vol=900600 | 2026-08-18:GREEN:vol=406288 | **GOOD** |
| `A07_rvol` | RVOL=0.340 on 2026-08-18: today_vol=406288 / avg20=1195105 (avg window 2026-07-16→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.695 on 2026-08-18 (price=399.9200, mid=387.1160, upper=405.5466, lower=368.6854; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=399.9200 vs SMA50=396.8037 dist=+0.79% | **GOOD** |
| `A10_sma20_50_80_stack` | mixed_20=387.12_50=396.80_80=389.39 on 2026-08-18: SMA20=387.1160 SMA50=396.8037 SMA80=389.3860 | **NEUTRAL** |
| `A11_max_downside_2m` | maxDD=-12.64% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-07-14 @ 426.7900 → trough 2026-07-16 @ 372.8500 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-18] body_frac=0.650 wick_frac=0.350; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=16.0400,upperW=1.1900,lowerW=4.7300,range=21.9600 | 2026-08-06:RED:body=0.4300,upperW=0.9100,lowerW=4.7100,range=6.0500 | 2026-08-07:GREEN:body=3.8100,upperW=1.5000,lowerW=3.0800,range=8.3900 | 2026-08-10:GREEN:body=2.3900,upperW=3.2300,lowerW=0.2900,range=5.9100 | 2026-08-11:RED:body=7.8500,upperW=5.1100,lowerW=2.2100,range=15.1700 | 2026-08-12:GREEN:body=12.2000,upperW=0.0200,lowerW=1.1000,range=13.3200 | 2026-08-13:RED:body=2.5800,upperW=3.9200,lowerW=0.5900,range=7.0900 | 2026-08-14:GREEN:body=1.9300,upperW=1.9800,lowerW=1.1600,range=5.0700 | 2026-08-17:RED:body=6.5800,upperW=3.3500,lowerW=2.1200,range=12.0500 | 2026-08-18:GREEN:body=5.4600,upperW=1.5950,lowerW=2.6887,range=9.7437 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-17] body_frac=0.432 wick_frac=0.568; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=16.0400,upperW=1.1900,lowerW=4.7300,range=21.9600 | 2026-08-06:RED:body=0.4300,upperW=0.9100,lowerW=4.7100,range=6.0500 | 2026-08-07:GREEN:body=3.8100,upperW=1.5000,lowerW=3.0800,range=8.3900 | 2026-08-10:GREEN:body=2.3900,upperW=3.2300,lowerW=0.2900,range=5.9100 | 2026-08-11:RED:body=7.8500,upperW=5.1100,lowerW=2.2100,range=15.1700 | 2026-08-12:GREEN:body=12.2000,upperW=0.0200,lowerW=1.1000,range=13.3200 | 2026-08-13:RED:body=2.5800,upperW=3.9200,lowerW=0.5900,range=7.0900 | 2026-08-14:GREEN:body=1.9300,upperW=1.9800,lowerW=1.1600,range=5.0700 | 2026-08-17:RED:body=6.5800,upperW=3.3500,lowerW=2.1200,range=12.0500 | 2026-08-18:GREEN:body=5.4600,upperW=1.5950,lowerW=2.6887,range=9.7437 | **GOOD** |
| `B01_eps_surprise` | 19.94 | **GOOD** |
| `B02_revenue_surprise` | 1.94 | **GOOD** |
| `B03_sales` | 201113.0 | **NEUTRAL** |
| `B04_income` | 4963.0 | **GOOD** |
| `B05_profit_margin` | 2.47 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 445.57 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=445.57 vs prior_export=445.57 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.88 | **GOOD** |
| `B10_insider_transactions` | 0.23 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.23 vs prior=0.23 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.63 | **BAD** |
| `B13_short_float` | 2.58 | **NEUTRAL** |
| `B14_earnings_date` | 7/15/2026 8:30:00 AM | **NEUTRAL** |

### BBDC  ·  score **+12**  ·  Asset Management
price=9.449999809265137  mcap=$0.99B  ADV=738,370
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-06,2026-08-07,2026-08-10,2026-08-13,2026-08-17,2026-08-18]  RED=[2026-08-05,2026-08-11,2026-08-12,2026-08-14]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=67.60 on 2026-08-18; prev RSI=66.49 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 66.49@2026-08-17 → 67.60@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 66.49@2026-08-17 → 67.60@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 66.49@2026-08-17 → 67.60@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=3.515 (= sum GREEN bodies / sum RED bodies); GREEN_sum=1.1600 dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-13,2026-08-17,2026-08-18]; RED_sum=0.3300 dates=[2026-08-05,2026-08-11,2026-08-12,2026-08-14]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=8.5800,C=8.4200,body=-0.1600 | 2026-08-06:GREEN:O=8.5100,C=9.2000,body=+0.6900 | 2026-08-07:GREEN:O=9.2000,C=9.2200,body=+0.0200 | 2026-08-10:GREEN:O=9.2400,C=9.3600,body=+0.1200 | 2026-08-11:RED:O=9.4000,C=9.3100,body=-0.0900 | 2026-08-12:RED:O=9.3000,C=9.2300,body=-0.0700 | 2026-08-13:GREEN:O=9.2900,C=9.4300,body=+0.1400 | 2026-08-14:RED:O=9.3200,C=9.3100,body=-0.0100 | 2026-08-17:GREEN:O=9.3500,C=9.3900,body=+0.0400 | 2026-08-18:GREEN:O=9.3000,C=9.4500,body=+0.1500 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=3.821 if finite else n/a; GREEN_vol_sum=7840154 dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-13,2026-08-17,2026-08-18]; RED_vol_sum=2052000 dates=[2026-08-05,2026-08-11,2026-08-12,2026-08-14]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=646800 | 2026-08-06:GREEN:vol=1803500 | 2026-08-07:GREEN:vol=1471800 | 2026-08-10:GREEN:vol=1411400 | 2026-08-11:RED:vol=438100 | 2026-08-12:RED:vol=373500 | 2026-08-13:GREEN:vol=1041900 | 2026-08-14:RED:vol=593600 | 2026-08-17:GREEN:vol=988600 | 2026-08-18:GREEN:vol=1122954 | **GOOD** |
| `A07_rvol` | RVOL=1.551 on 2026-08-18: today_vol=1122954 / avg20=724090 (avg window 2026-07-16→2026-08-17, excludes today) | **GOOD** |
| `A08_bollinger_position` | pos=0.654 on 2026-08-18 (price=9.4500, mid=8.7955, upper=9.7956, lower=7.7954; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=9.4500 vs SMA50=8.5516 dist=+10.51% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=8.7955 SMA50=8.5516 SMA80=8.5406 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-18 @ 9.4500 → trough 2026-08-18 @ 9.4500 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-13,2026-08-17,2026-08-18] body_frac=0.705 wick_frac=0.295; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.1600,upperW=0.0000,lowerW=0.0300,range=0.1900 | 2026-08-06:GREEN:body=0.6900,upperW=0.0100,lowerW=0.0000,range=0.7000 | 2026-08-07:GREEN:body=0.0200,upperW=0.0800,lowerW=0.0700,range=0.1700 | 2026-08-10:GREEN:body=0.1200,upperW=0.0400,lowerW=0.1100,range=0.2700 | 2026-08-11:RED:body=0.0900,upperW=0.0000,lowerW=0.0300,range=0.1200 | 2026-08-12:RED:body=0.0700,upperW=0.0100,lowerW=0.0600,range=0.1400 | 2026-08-13:GREEN:body=0.1400,upperW=0.0200,lowerW=0.0100,range=0.1700 | 2026-08-14:RED:body=0.0100,upperW=0.1000,lowerW=0.0500,range=0.1600 | 2026-08-17:GREEN:body=0.0400,upperW=0.0600,lowerW=0.0900,range=0.1900 | 2026-08-18:GREEN:body=0.1500,upperW=0.0650,lowerW=-0.0700,range=0.1450 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-11,2026-08-12,2026-08-14] body_frac=0.541 wick_frac=0.459; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.1600,upperW=0.0000,lowerW=0.0300,range=0.1900 | 2026-08-06:GREEN:body=0.6900,upperW=0.0100,lowerW=0.0000,range=0.7000 | 2026-08-07:GREEN:body=0.0200,upperW=0.0800,lowerW=0.0700,range=0.1700 | 2026-08-10:GREEN:body=0.1200,upperW=0.0400,lowerW=0.1100,range=0.2700 | 2026-08-11:RED:body=0.0900,upperW=0.0000,lowerW=0.0300,range=0.1200 | 2026-08-12:RED:body=0.0700,upperW=0.0100,lowerW=0.0600,range=0.1400 | 2026-08-13:GREEN:body=0.1400,upperW=0.0200,lowerW=0.0100,range=0.1700 | 2026-08-14:RED:body=0.0100,upperW=0.1000,lowerW=0.0500,range=0.1600 | 2026-08-17:GREEN:body=0.0400,upperW=0.0600,lowerW=0.0900,range=0.1900 | 2026-08-18:GREEN:body=0.1500,upperW=0.0650,lowerW=-0.0700,range=0.1450 | **BAD** |
| `B01_eps_surprise` | 12.68 | **GOOD** |
| `B02_revenue_surprise` | 8.49 | **GOOD** |
| `B03_sales` | 264.4 | **NEUTRAL** |
| `B04_income` | 87.11 | **GOOD** |
| `B05_profit_margin` | 32.95 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 9.96 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=9.96 vs prior_export=9.96 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 1.56 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=1.56 vs prior=1.56 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | nan | **NEUTRAL** |
| `B13_short_float` | 2.87 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |

### WTTR  ·  score **+12**  ·  Oil & Gas Equipment & Services
price=21.174999237060547  mcap=$2.74B  ADV=1,830,100
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-05,2026-08-10,2026-08-11,2026-08-14,2026-08-17]  RED=[2026-08-06,2026-08-07,2026-08-12,2026-08-13,2026-08-18]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=57.38 on 2026-08-18; prev RSI=58.41 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 58.41@2026-08-17 → 57.38@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 58.41@2026-08-17 → 57.38@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 58.41@2026-08-17 → 57.38@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.433 (= sum GREEN bodies / sum RED bodies); GREEN_sum=3.2124 dates=[2026-08-05,2026-08-10,2026-08-11,2026-08-14,2026-08-17]; RED_sum=2.2418 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13,2026-08-18]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:GREEN:O=19.9324,C=22.1847,body=+2.2524 | 2026-08-06:RED:O=21.5868,C=20.6300,body=-0.9568 | 2026-08-07:RED:O=20.5200,C=20.2300,body=-0.2900 | 2026-08-10:GREEN:O=20.6100,C=20.8900,body=+0.2800 | 2026-08-11:GREEN:O=21.0300,C=21.1100,body=+0.0800 | 2026-08-12:RED:O=21.2300,C=21.2100,body=-0.0200 | 2026-08-13:RED:O=21.0700,C=20.3700,body=-0.7000 | 2026-08-14:GREEN:O=20.5200,C=20.8600,body=+0.3400 | 2026-08-17:GREEN:O=21.0600,C=21.3200,body=+0.2600 | 2026-08-18:RED:O=21.4500,C=21.1750,body=-0.2750 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.575 if finite else n/a; GREEN_vol_sum=12008400 dates=[2026-08-05,2026-08-10,2026-08-11,2026-08-14,2026-08-17]; RED_vol_sum=7624045 dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13,2026-08-18]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:vol=6975500 | 2026-08-06:RED:vol=2875500 | 2026-08-07:RED:vol=1759400 | 2026-08-10:GREEN:vol=1714300 | 2026-08-11:GREEN:vol=1582100 | 2026-08-12:RED:vol=1187200 | 2026-08-13:RED:vol=1273900 | 2026-08-14:GREEN:vol=694900 | 2026-08-17:GREEN:vol=1041600 | 2026-08-18:RED:vol=528045 | **GOOD** |
| `A07_rvol` | RVOL=0.273 on 2026-08-18: today_vol=528045 / avg20=1934985 (avg window 2026-07-17→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.403 on 2026-08-18 (price=21.1750, mid=20.0904, upper=22.7833, lower=17.3975; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=21.1750 vs SMA50=19.3466 dist=+9.45% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=20.0904 SMA50=19.3466 SMA80=18.7927 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-8.81% inside window 2026-06-16→2026-08-18 (42 sessions): peak 2026-08-05 @ 22.1847 → trough 2026-08-07 @ 20.2300 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-05,2026-08-10,2026-08-11,2026-08-14,2026-08-17] body_frac=0.523 wick_frac=0.477; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=2.2524,upperW=0.2890,lowerW=0.7973,range=3.3387 | 2026-08-06:RED:body=0.9568,upperW=0.0000,lowerW=0.2392,range=1.1959 | 2026-08-07:RED:body=0.2900,upperW=1.0840,lowerW=0.0850,range=1.4590 | 2026-08-10:GREEN:body=0.2800,upperW=0.3250,lowerW=0.0000,range=0.6050 | 2026-08-11:GREEN:body=0.0800,upperW=0.7180,lowerW=0.1450,range=0.9430 | 2026-08-12:RED:body=0.0200,upperW=0.2200,lowerW=0.1700,range=0.4100 | 2026-08-13:RED:body=0.7000,upperW=0.2200,lowerW=0.0300,range=0.9500 | 2026-08-14:GREEN:body=0.3400,upperW=0.2400,lowerW=0.1100,range=0.6900 | 2026-08-17:GREEN:body=0.2600,upperW=0.0500,lowerW=0.2500,range=0.5600 | 2026-08-18:RED:body=0.2750,upperW=0.1600,lowerW=0.1426,range=0.5776 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-06,2026-08-07,2026-08-12,2026-08-13,2026-08-18] body_frac=0.488 wick_frac=0.512; window=2026-08-05→2026-08-18; sessions: 2026-08-05:GREEN:body=2.2524,upperW=0.2890,lowerW=0.7973,range=3.3387 | 2026-08-06:RED:body=0.9568,upperW=0.0000,lowerW=0.2392,range=1.1959 | 2026-08-07:RED:body=0.2900,upperW=1.0840,lowerW=0.0850,range=1.4590 | 2026-08-10:GREEN:body=0.2800,upperW=0.3250,lowerW=0.0000,range=0.6050 | 2026-08-11:GREEN:body=0.0800,upperW=0.7180,lowerW=0.1450,range=0.9430 | 2026-08-12:RED:body=0.0200,upperW=0.2200,lowerW=0.1700,range=0.4100 | 2026-08-13:RED:body=0.7000,upperW=0.2200,lowerW=0.0300,range=0.9500 | 2026-08-14:GREEN:body=0.3400,upperW=0.2400,lowerW=0.1100,range=0.6900 | 2026-08-17:GREEN:body=0.2600,upperW=0.0500,lowerW=0.2500,range=0.5600 | 2026-08-18:RED:body=0.2750,upperW=0.1600,lowerW=0.1426,range=0.5776 | **GOOD** |
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

### RHI  ·  score **+12**  ·  Staffing & Employment Services
price=43.845001220703125  mcap=$4.45B  ADV=2,288,780
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-06,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-18]  RED=[2026-08-05,2026-08-14,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=63.32 on 2026-08-18; prev RSI=60.18 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 60.18@2026-08-17 → 63.32@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 60.18@2026-08-17 → 63.32@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 60.18@2026-08-17 → 63.32@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=3.266 (= sum GREEN bodies / sum RED bodies); GREEN_sum=6.0750 dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-18]; RED_sum=1.8600 dates=[2026-08-05,2026-08-14,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=41.0600,C=40.5500,body=-0.5100 | 2026-08-06:GREEN:O=40.4000,C=40.6800,body=+0.2800 | 2026-08-07:GREEN:O=40.9200,C=42.7400,body=+1.8200 | 2026-08-10:GREEN:O=41.8900,C=42.6800,body=+0.7900 | 2026-08-11:GREEN:O=42.0200,C=42.1400,body=+0.1200 | 2026-08-12:GREEN:O=41.3900,C=41.5500,body=+0.1600 | 2026-08-13:GREEN:O=41.8400,C=43.7800,body=+1.9400 | 2026-08-14:RED:O=44.3500,C=43.2200,body=-1.1300 | 2026-08-17:RED:O=42.6900,C=42.4700,body=-0.2200 | 2026-08-18:GREEN:O=42.8800,C=43.8450,body=+0.9650 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.887 if finite else n/a; GREEN_vol_sum=9596107 dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-18]; RED_vol_sum=5085300 dates=[2026-08-05,2026-08-14,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=1249100 | 2026-08-06:GREEN:vol=1784200 | 2026-08-07:GREEN:vol=1971500 | 2026-08-10:GREEN:vol=1119900 | 2026-08-11:GREEN:vol=1137600 | 2026-08-12:GREEN:vol=939400 | 2026-08-13:GREEN:vol=1923700 | 2026-08-14:RED:vol=1817100 | 2026-08-17:RED:vol=2019100 | 2026-08-18:GREEN:vol=719807 | **GOOD** |
| `A07_rvol` | RVOL=0.301 on 2026-08-18: today_vol=719807 / avg20=2388680 (avg window 2026-07-16→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.662 on 2026-08-18 (price=43.8450, mid=40.8373, upper=45.3774, lower=36.2971; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=43.8450 vs SMA50=35.8123 dist=+22.43% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=40.8373 SMA50=35.8123 SMA80=32.3707 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-18 @ 43.8450 → trough 2026-08-18 @ 43.8450 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-06,2026-08-07,2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-18] body_frac=0.512 wick_frac=0.488; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.5100,upperW=0.1100,lowerW=0.5800,range=1.2000 | 2026-08-06:GREEN:body=0.2800,upperW=0.6800,lowerW=0.4000,range=1.3600 | 2026-08-07:GREEN:body=1.8200,upperW=1.4400,lowerW=0.3500,range=3.6100 | 2026-08-10:GREEN:body=0.7900,upperW=0.0100,lowerW=0.1700,range=0.9700 | 2026-08-11:GREEN:body=0.1200,upperW=0.7800,lowerW=0.4000,range=1.3000 | 2026-08-12:GREEN:body=0.1600,upperW=0.3300,lowerW=0.5700,range=1.0600 | 2026-08-13:GREEN:body=1.9400,upperW=0.0800,lowerW=0.2700,range=2.2900 | 2026-08-14:RED:body=1.1300,upperW=0.1200,lowerW=0.5800,range=1.8300 | 2026-08-17:RED:body=0.2200,upperW=0.3400,lowerW=0.2900,range=0.8500 | 2026-08-18:GREEN:body=0.9650,upperW=0.1150,lowerW=0.1900,range=1.2700 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-14,2026-08-17] body_frac=0.479 wick_frac=0.521; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.5100,upperW=0.1100,lowerW=0.5800,range=1.2000 | 2026-08-06:GREEN:body=0.2800,upperW=0.6800,lowerW=0.4000,range=1.3600 | 2026-08-07:GREEN:body=1.8200,upperW=1.4400,lowerW=0.3500,range=3.6100 | 2026-08-10:GREEN:body=0.7900,upperW=0.0100,lowerW=0.1700,range=0.9700 | 2026-08-11:GREEN:body=0.1200,upperW=0.7800,lowerW=0.4000,range=1.3000 | 2026-08-12:GREEN:body=0.1600,upperW=0.3300,lowerW=0.5700,range=1.0600 | 2026-08-13:GREEN:body=1.9400,upperW=0.0800,lowerW=0.2700,range=2.2900 | 2026-08-14:RED:body=1.1300,upperW=0.1200,lowerW=0.5800,range=1.8300 | 2026-08-17:RED:body=0.2200,upperW=0.3400,lowerW=0.2900,range=0.8500 | 2026-08-18:GREEN:body=0.9650,upperW=0.1150,lowerW=0.1900,range=1.2700 | **GOOD** |
| `B01_eps_surprise` | 0.93 | **GOOD** |
| `B02_revenue_surprise` | 0.91 | **GOOD** |
| `B03_sales` | 5293.4 | **NEUTRAL** |
| `B04_income` | 114.78 | **GOOD** |
| `B05_profit_margin` | 2.17 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 34.78 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=34.78 vs prior_export=34.78 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 3.25 | **NEUTRAL** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 10.65 | **GOOD** |
| `B13_short_float` | 23.76 | **GOOD** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |

### FA  ·  score **+12**  ·  Specialty Business Services
price=20.760000228881836  mcap=$3.58B  ADV=2,052,310
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-06,2026-08-11,2026-08-13,2026-08-18]  RED=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=49.95 on 2026-08-18; prev RSI=47.86 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 47.86@2026-08-17 → 49.95@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 47.86@2026-08-17 → 49.95@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 47.86@2026-08-17 → 49.95@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.417 (= sum GREEN bodies / sum RED bodies); GREEN_sum=3.3450 dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-18]; RED_sum=2.3600 dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=20.6500,C=20.5600,body=-0.0900 | 2026-08-06:GREEN:O=22.1550,C=24.1200,body=+1.9650 | 2026-08-07:RED:O=24.4500,C=24.0100,body=-0.4400 | 2026-08-10:RED:O=23.7700,C=23.5900,body=-0.1800 | 2026-08-11:GREEN:O=21.1400,C=21.1700,body=+0.0300 | 2026-08-12:RED:O=21.0200,C=20.9900,body=-0.0300 | 2026-08-13:GREEN:O=21.2200,C=22.2000,body=+0.9800 | 2026-08-14:RED:O=22.1500,C=21.5800,body=-0.5700 | 2026-08-17:RED:O=21.3500,C=20.3000,body=-1.0500 | 2026-08-18:GREEN:O=20.3900,C=20.7600,body=+0.3700 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.398 if finite else n/a; GREEN_vol_sum=21233081 dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-18]; RED_vol_sum=15183900 dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=1599300 | 2026-08-06:GREEN:vol=4725700 | 2026-08-07:RED:vol=2577900 | 2026-08-10:RED:vol=2955000 | 2026-08-11:GREEN:vol=9086700 | 2026-08-12:RED:vol=1987700 | 2026-08-13:GREEN:vol=6795100 | 2026-08-14:RED:vol=3465200 | 2026-08-17:RED:vol=2598800 | 2026-08-18:GREEN:vol=625581 | **GOOD** |
| `A07_rvol` | RVOL=0.226 on 2026-08-18: today_vol=625581 / avg20=2762960 (avg window 2026-07-16→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=-0.196 on 2026-08-18 (price=20.7600, mid=21.3115, upper=24.1191, lower=18.5039; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=20.7600 vs SMA50=19.2533 dist=+7.83% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=21.3115 SMA50=19.2533 SMA80=17.4616 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-15.84% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-06 @ 24.1200 → trough 2026-08-17 @ 20.3000 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-06,2026-08-11,2026-08-13,2026-08-18] body_frac=0.526 wick_frac=0.474; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.0900,upperW=0.2100,lowerW=0.4590,range=0.7590 | 2026-08-06:GREEN:body=1.9650,upperW=0.4600,lowerW=0.6950,range=3.1200 | 2026-08-07:RED:body=0.4400,upperW=0.7000,lowerW=0.0200,range=1.1600 | 2026-08-10:RED:body=0.1800,upperW=0.1800,lowerW=0.5900,range=0.9500 | 2026-08-11:GREEN:body=0.0300,upperW=0.3450,lowerW=1.0800,range=1.4550 | 2026-08-12:RED:body=0.0300,upperW=0.3500,lowerW=0.2350,range=0.6150 | 2026-08-13:GREEN:body=0.9800,upperW=0.0600,lowerW=0.0800,range=1.1200 | 2026-08-14:RED:body=0.5700,upperW=0.1900,lowerW=0.1600,range=0.9200 | 2026-08-17:RED:body=1.0500,upperW=0.3200,lowerW=0.2000,range=1.5700 | 2026-08-18:GREEN:body=0.3700,upperW=0.2800,lowerW=0.0088,range=0.6588 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-07,2026-08-10,2026-08-12,2026-08-14,2026-08-17] body_frac=0.395 wick_frac=0.605; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.0900,upperW=0.2100,lowerW=0.4590,range=0.7590 | 2026-08-06:GREEN:body=1.9650,upperW=0.4600,lowerW=0.6950,range=3.1200 | 2026-08-07:RED:body=0.4400,upperW=0.7000,lowerW=0.0200,range=1.1600 | 2026-08-10:RED:body=0.1800,upperW=0.1800,lowerW=0.5900,range=0.9500 | 2026-08-11:GREEN:body=0.0300,upperW=0.3450,lowerW=1.0800,range=1.4550 | 2026-08-12:RED:body=0.0300,upperW=0.3500,lowerW=0.2350,range=0.6150 | 2026-08-13:GREEN:body=0.9800,upperW=0.0600,lowerW=0.0800,range=1.1200 | 2026-08-14:RED:body=0.5700,upperW=0.1900,lowerW=0.1600,range=0.9200 | 2026-08-17:RED:body=1.0500,upperW=0.3200,lowerW=0.2000,range=1.5700 | 2026-08-18:GREEN:body=0.3700,upperW=0.2800,lowerW=0.0088,range=0.6588 | **GOOD** |
| `B01_eps_surprise` | 20.07 | **GOOD** |
| `B02_revenue_surprise` | 8.13 | **GOOD** |
| `B03_sales` | 1663.13 | **NEUTRAL** |
| `B04_income` | 25.14 | **GOOD** |
| `B05_profit_margin` | 1.51 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 26.25 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=26.25 vs prior_export=26.25 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.9 | **GOOD** |
| `B10_insider_transactions` | -11.77 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-11.77 vs prior=-11.77 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 8.68 | **GOOD** |
| `B13_short_float` | 15.14 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |

### NOMD  ·  score **+12**  ·  Packaged Foods
price=11.574999809265137  mcap=$1.61B  ADV=1,437,030
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-07,2026-08-12,2026-08-13,2026-08-18]  RED=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=51.58 on 2026-08-18; prev RSI=48.37 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 48.37@2026-08-17 → 51.58@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 48.37@2026-08-17 → 51.58@2026-08-18 vs level 50 | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 48.37@2026-08-17 → 51.58@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.232 (= sum GREEN bodies / sum RED bodies); GREEN_sum=1.6265 dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-18]; RED_sum=1.3198 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=11.9037,C=11.6377,body=-0.2661 | 2026-08-06:RED:O=11.7362,C=11.6081,body=-0.1281 | 2026-08-07:GREEN:O=11.5785,C=11.8150,body=+0.2365 | 2026-08-10:RED:O=11.7756,C=11.4800,body=-0.2956 | 2026-08-11:RED:O=11.5000,C=11.3700,body=-0.1300 | 2026-08-12:GREEN:O=11.2200,C=11.4000,body=+0.1800 | 2026-08-13:GREEN:O=10.9300,C=12.0400,body=+1.1100 | 2026-08-14:RED:O=12.0100,C=11.7300,body=-0.2800 | 2026-08-17:RED:O=11.6000,C=11.3800,body=-0.2200 | 2026-08-18:GREEN:O=11.4750,C=11.5750,body=+0.1000 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.176 if finite else n/a; GREEN_vol_sum=9782893 dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-18]; RED_vol_sum=8316000 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=771600 | 2026-08-06:RED:vol=689000 | 2026-08-07:GREEN:vol=745600 | 2026-08-10:RED:vol=1114000 | 2026-08-11:RED:vol=1654900 | 2026-08-12:GREEN:vol=3927700 | 2026-08-13:GREEN:vol=3866400 | 2026-08-14:RED:vol=1405800 | 2026-08-17:RED:vol=2680700 | 2026-08-18:GREEN:vol=1243193 | **GOOD** |
| `A07_rvol` | RVOL=0.781 on 2026-08-18: today_vol=1243193 / avg20=1592605 (avg window 2026-07-16→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.220 on 2026-08-18 (price=11.5750, mid=11.7093, upper=12.3187, lower=11.0999; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=11.5750 vs SMA50=11.0270 dist=+4.97% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=11.7093 SMA50=11.0270 SMA80=10.4885 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-9.08% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-07-29 @ 12.5048 → trough 2026-08-11 @ 11.3700 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-12,2026-08-13,2026-08-18] body_frac=0.719 wick_frac=0.281; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.2661,upperW=0.0069,lowerW=0.0591,range=0.3321 | 2026-08-06:RED:body=0.1281,upperW=0.0049,lowerW=0.1360,range=0.2690 | 2026-08-07:GREEN:body=0.2365,upperW=0.0099,lowerW=0.0493,range=0.2956 | 2026-08-10:RED:body=0.2956,upperW=0.0000,lowerW=0.0296,range=0.3252 | 2026-08-11:RED:body=0.1300,upperW=0.1550,lowerW=0.0200,range=0.3050 | 2026-08-12:GREEN:body=0.1800,upperW=0.2600,lowerW=0.0700,range=0.5100 | 2026-08-13:GREEN:body=1.1100,upperW=0.0800,lowerW=0.0000,range=1.1900 | 2026-08-14:RED:body=0.2800,upperW=0.0000,lowerW=0.2300,range=0.5100 | 2026-08-17:RED:body=0.2200,upperW=0.0550,lowerW=0.0250,range=0.3000 | 2026-08-18:GREEN:body=0.1000,upperW=0.0450,lowerW=0.1200,range=0.2650 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-11,2026-08-14,2026-08-17] body_frac=0.647 wick_frac=0.353; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.2661,upperW=0.0069,lowerW=0.0591,range=0.3321 | 2026-08-06:RED:body=0.1281,upperW=0.0049,lowerW=0.1360,range=0.2690 | 2026-08-07:GREEN:body=0.2365,upperW=0.0099,lowerW=0.0493,range=0.2956 | 2026-08-10:RED:body=0.2956,upperW=0.0000,lowerW=0.0296,range=0.3252 | 2026-08-11:RED:body=0.1300,upperW=0.1550,lowerW=0.0200,range=0.3050 | 2026-08-12:GREEN:body=0.1800,upperW=0.2600,lowerW=0.0700,range=0.5100 | 2026-08-13:GREEN:body=1.1100,upperW=0.0800,lowerW=0.0000,range=1.1900 | 2026-08-14:RED:body=0.2800,upperW=0.0000,lowerW=0.2300,range=0.5100 | 2026-08-17:RED:body=0.2200,upperW=0.0550,lowerW=0.0250,range=0.3000 | 2026-08-18:GREEN:body=0.1000,upperW=0.0450,lowerW=0.1200,range=0.2650 | **BAD** |
| `B01_eps_surprise` | 2.0 | **GOOD** |
| `B02_revenue_surprise` | 0.04 | **GOOD** |
| `B03_sales` | 3457.3 | **NEUTRAL** |
| `B04_income` | 145.06 | **GOOD** |
| `B05_profit_margin` | 4.2 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 13.58 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=13.58 vs prior_export=13.58 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 3.5 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=3.5 vs prior=3.5 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -6.23 | **BAD** |
| `B13_short_float` | 2.68 | **NEUTRAL** |
| `B14_earnings_date` | 8/13/2026 8:30:00 AM | **NEUTRAL** |

### EBC  ·  score **+12**  ·  Banks - Regional
price=23.459999084472656  mcap=$5.35B  ADV=3,232,110
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17]  RED=[2026-08-05,2026-08-06,2026-08-07,2026-08-18]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=60.07 on 2026-08-18; prev RSI=64.13 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 64.13@2026-08-17 → 60.07@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 64.13@2026-08-17 → 60.07@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 64.13@2026-08-17 → 60.07@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=0.956 (= sum GREEN bodies / sum RED bodies); GREEN_sum=0.7550 dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17]; RED_sum=0.7900 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-18]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=23.4700,C=23.2000,body=-0.2700 | 2026-08-06:RED:O=23.3100,C=23.0100,body=-0.3000 | 2026-08-07:RED:O=22.9000,C=22.8400,body=-0.0600 | 2026-08-10:GREEN:O=22.7300,C=22.9400,body=+0.2100 | 2026-08-11:GREEN:O=22.9400,C=23.1300,body=+0.1900 | 2026-08-12:GREEN:O=23.2000,C=23.3500,body=+0.1500 | 2026-08-13:GREEN:O=23.5000,C=23.5200,body=+0.0200 | 2026-08-14:GREEN:O=23.5100,C=23.6250,body=+0.1150 | 2026-08-17:GREEN:O=23.5800,C=23.6500,body=+0.0700 | 2026-08-18:RED:O=23.6200,C=23.4600,body=-0.1600 | **NEUTRAL** |
| `A06_volume_red_green_ratio` | ratio=1.544 if finite else n/a; GREEN_vol_sum=9782300 dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17]; RED_vol_sum=6337376 dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-18]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=3210000 | 2026-08-06:RED:vol=1388800 | 2026-08-07:RED:vol=983700 | 2026-08-10:GREEN:vol=1952300 | 2026-08-11:GREEN:vol=2116400 | 2026-08-12:GREEN:vol=1178700 | 2026-08-13:GREEN:vol=1055900 | 2026-08-14:GREEN:vol=1299200 | 2026-08-17:GREEN:vol=2179800 | 2026-08-18:RED:vol=754876 | **GOOD** |
| `A07_rvol` | RVOL=0.283 on 2026-08-18: today_vol=754876 / avg20=2665965 (avg window 2026-07-16→2026-08-17, excludes today) | **BAD** |
| `A08_bollinger_position` | pos=0.494 on 2026-08-18 (price=23.4600, mid=23.1598, upper=23.7673, lower=22.5522; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=23.4600 vs SMA50=22.1092 dist=+6.11% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=23.1598 SMA50=22.1092 SMA80=21.2141 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-0.80% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-17 @ 23.6500 → trough 2026-08-18 @ 23.4600 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-10,2026-08-11,2026-08-12,2026-08-13,2026-08-14,2026-08-17] body_frac=0.501 wick_frac=0.499; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.2700,upperW=0.0550,lowerW=0.0500,range=0.3750 | 2026-08-06:RED:body=0.3000,upperW=0.0550,lowerW=0.0390,range=0.3940 | 2026-08-07:RED:body=0.0600,upperW=0.1400,lowerW=0.0250,range=0.2250 | 2026-08-10:GREEN:body=0.2100,upperW=0.0180,lowerW=0.0900,range=0.3180 | 2026-08-11:GREEN:body=0.1900,upperW=0.0100,lowerW=0.0300,range=0.2300 | 2026-08-12:GREEN:body=0.1500,upperW=0.0350,lowerW=0.0700,range=0.2550 | 2026-08-13:GREEN:body=0.0200,upperW=0.0800,lowerW=0.0900,range=0.1900 | 2026-08-14:GREEN:body=0.1150,upperW=0.1050,lowerW=0.0900,range=0.3100 | 2026-08-17:GREEN:body=0.0700,upperW=0.1250,lowerW=0.0100,range=0.2050 | 2026-08-18:RED:body=0.1600,upperW=0.1200,lowerW=0.9250,range=1.2050 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-07,2026-08-18] body_frac=0.359 wick_frac=0.641; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=0.2700,upperW=0.0550,lowerW=0.0500,range=0.3750 | 2026-08-06:RED:body=0.3000,upperW=0.0550,lowerW=0.0390,range=0.3940 | 2026-08-07:RED:body=0.0600,upperW=0.1400,lowerW=0.0250,range=0.2250 | 2026-08-10:GREEN:body=0.2100,upperW=0.0180,lowerW=0.0900,range=0.3180 | 2026-08-11:GREEN:body=0.1900,upperW=0.0100,lowerW=0.0300,range=0.2300 | 2026-08-12:GREEN:body=0.1500,upperW=0.0350,lowerW=0.0700,range=0.2550 | 2026-08-13:GREEN:body=0.0200,upperW=0.0800,lowerW=0.0900,range=0.1900 | 2026-08-14:GREEN:body=0.1150,upperW=0.1050,lowerW=0.0900,range=0.3100 | 2026-08-17:GREEN:body=0.0700,upperW=0.1250,lowerW=0.0100,range=0.2050 | 2026-08-18:RED:body=0.1600,upperW=0.1200,lowerW=0.9250,range=1.2050 | **GOOD** |
| `B01_eps_surprise` | 6.01 | **GOOD** |
| `B02_revenue_surprise` | 2.53 | **GOOD** |
| `B03_sales` | 1491.93 | **NEUTRAL** |
| `B04_income` | 376.12 | **GOOD** |
| `B05_profit_margin` | 25.21 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 25.13 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=25.13 vs prior_export=25.13 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 1.5 | **GOOD** |
| `B10_insider_transactions` | 0.04 | **GOOD** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.04 vs prior=0.04 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 5.3 | **GOOD** |
| `B13_short_float` | 7.88 | **NEUTRAL** |
| `B14_earnings_date` | 7/23/2026 4:30:00 PM | **NEUTRAL** |

### DUOL  ·  score **+12**  ·  Software - Application
price=140.1999969482422  mcap=$6.53B  ADV=1,349,470
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-18]  RED=[2026-08-05,2026-08-06,2026-08-11,2026-08-14,2026-08-17]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=55.84 on 2026-08-18; prev RSI=49.08 on 2026-08-17 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.08@2026-08-17 → 55.84@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | cross_up | RSI 49.08@2026-08-17 → 55.84@2026-08-18 vs level 50 | **GOOD** |
| `A04_rsi_cross_70` | below | RSI 49.08@2026-08-17 → 55.84@2026-08-18 vs level 70 | **NEUTRAL** |
| `A05_body_red_green_ratio` | ratio=1.280 (= sum GREEN bodies / sum RED bodies); GREEN_sum=29.3350 dates=[2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-18]; RED_sum=22.9150 dates=[2026-08-05,2026-08-06,2026-08-11,2026-08-14,2026-08-17]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=138.2100,C=135.3200,body=-2.8900 | 2026-08-06:RED:O=130.1100,C=122.5800,body=-7.5300 | 2026-08-07:GREEN:O=122.9800,C=130.9000,body=+7.9200 | 2026-08-10:GREEN:O=129.8000,C=137.1900,body=+7.3900 | 2026-08-11:RED:O=136.0250,C=135.4400,body=-0.5850 | 2026-08-12:GREEN:O=132.8400,C=134.6300,body=+1.7900 | 2026-08-13:GREEN:O=136.5000,C=144.1000,body=+7.6000 | 2026-08-14:RED:O=143.4000,C=132.8300,body=-10.5700 | 2026-08-17:RED:O=131.5000,C=130.1600,body=-1.3400 | 2026-08-18:GREEN:O=135.5650,C=140.2000,body=+4.6350 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=0.705 if finite else n/a; GREEN_vol_sum=6421674 dates=[2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-18]; RED_vol_sum=9105300 dates=[2026-08-05,2026-08-06,2026-08-11,2026-08-14,2026-08-17]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=1871000 | 2026-08-06:RED:vol=3824400 | 2026-08-07:GREEN:vol=1499600 | 2026-08-10:GREEN:vol=1447000 | 2026-08-11:RED:vol=860000 | 2026-08-12:GREEN:vol=966100 | 2026-08-13:GREEN:vol=1643600 | 2026-08-14:RED:vol=1065300 | 2026-08-17:RED:vol=1484600 | 2026-08-18:GREEN:vol=865374 | **BAD** |
| `A07_rvol` | RVOL=0.644 on 2026-08-18: today_vol=865374 / avg20=1343650 (avg window 2026-07-20→2026-08-17, excludes today) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.530 on 2026-08-18 (price=140.2000, mid=132.5675, upper=146.9635, lower=118.1715; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=True on 2026-08-18: price=140.2000 vs SMA50=128.2450 dist=+9.32% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=132.5675 SMA50=128.2450 SMA80=120.7143 | **GOOD** |
| `A11_max_downside_2m` | maxDD=-9.67% inside window 2026-06-17→2026-08-18 (42 sessions): peak 2026-08-13 @ 144.1000 → trough 2026-08-17 @ 130.1600 | **GOOD** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-10,2026-08-12,2026-08-13,2026-08-18] body_frac=0.678 wick_frac=0.322; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=2.8900,upperW=0.7900,lowerW=1.8700,range=5.5500 | 2026-08-06:RED:body=7.5300,upperW=5.0300,lowerW=10.5590,range=23.1190 | 2026-08-07:GREEN:body=7.9200,upperW=0.2050,lowerW=0.0000,range=8.1250 | 2026-08-10:GREEN:body=7.3900,upperW=0.2100,lowerW=1.3000,range=8.9000 | 2026-08-11:RED:body=0.5850,upperW=2.4650,lowerW=1.4400,range=4.4900 | 2026-08-12:GREEN:body=1.7900,upperW=1.6000,lowerW=4.6600,range=8.0500 | 2026-08-13:GREEN:body=7.6000,upperW=2.7500,lowerW=1.5000,range=11.8500 | 2026-08-14:RED:body=10.5700,upperW=0.5300,lowerW=0.2600,range=11.3600 | 2026-08-17:RED:body=1.3400,upperW=1.4900,lowerW=1.6950,range=4.5250 | 2026-08-18:GREEN:body=4.6350,upperW=0.2100,lowerW=1.4958,range=6.3408 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-11,2026-08-14,2026-08-17] body_frac=0.467 wick_frac=0.533; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=2.8900,upperW=0.7900,lowerW=1.8700,range=5.5500 | 2026-08-06:RED:body=7.5300,upperW=5.0300,lowerW=10.5590,range=23.1190 | 2026-08-07:GREEN:body=7.9200,upperW=0.2050,lowerW=0.0000,range=8.1250 | 2026-08-10:GREEN:body=7.3900,upperW=0.2100,lowerW=1.3000,range=8.9000 | 2026-08-11:RED:body=0.5850,upperW=2.4650,lowerW=1.4400,range=4.4900 | 2026-08-12:GREEN:body=1.7900,upperW=1.6000,lowerW=4.6600,range=8.0500 | 2026-08-13:GREEN:body=7.6000,upperW=2.7500,lowerW=1.5000,range=11.8500 | 2026-08-14:RED:body=10.5700,upperW=0.5300,lowerW=0.2600,range=11.3600 | 2026-08-17:RED:body=1.3400,upperW=1.4900,lowerW=1.6950,range=4.5250 | 2026-08-18:GREEN:body=4.6350,upperW=0.2100,lowerW=1.4958,range=6.3408 | **GOOD** |
| `B01_eps_surprise` | 9.31 | **GOOD** |
| `B02_revenue_surprise` | 0.97 | **GOOD** |
| `B03_sales` | 1145.0 | **NEUTRAL** |
| `B04_income` | 410.77 | **GOOD** |
| `B05_profit_margin` | 35.87 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 126.46 | **NEUTRAL** |
| `B08_target_price_delta` | delta=1.6699999999999875 (now=126.46 vs prior_export=124.79 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 2.78 | **NEUTRAL** |
| `B10_insider_transactions` | -0.27 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-0.27 vs prior=-0.27 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.49 | **GOOD** |
| `B13_short_float` | 18.31 | **NEUTRAL** |
| `B14_earnings_date` | 8/5/2026 4:30:00 PM | **NEUTRAL** |

### HAE  ·  score **+12**  ·  Medical Devices
price=105.19000244140625  mcap=$4.79B  ADV=851,130
body window: `2026-08-05→2026-08-18`  GREEN=[2026-08-07,2026-08-11,2026-08-12,2026-08-14,2026-08-17,2026-08-18]  RED=[2026-08-05,2026-08-06,2026-08-10,2026-08-13]

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=82.65 on 2026-08-18; prev RSI=68.71 on 2026-08-17 | **BAD** |
| `A02_rsi_cross_30` | above | RSI 68.71@2026-08-17 → 82.65@2026-08-18 vs level 30 | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 68.71@2026-08-17 → 82.65@2026-08-18 vs level 50 | **NEUTRAL** |
| `A04_rsi_cross_70` | cross_up | RSI 68.71@2026-08-17 → 82.65@2026-08-18 vs level 70 | **GOOD** |
| `A05_body_red_green_ratio` | ratio=2.065 (= sum GREEN bodies / sum RED bodies); GREEN_sum=10.7400 dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-14,2026-08-17,2026-08-18]; RED_sum=5.2000 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-13]; DOJI=[none]; window=2026-08-05→2026-08-18 (10 sessions); sessions: 2026-08-05:RED:O=86.0200,C=83.6000,body=-2.4200 | 2026-08-06:RED:O=85.7700,C=85.0200,body=-0.7500 | 2026-08-07:GREEN:O=85.5500,C=87.8400,body=+2.2900 | 2026-08-10:RED:O=87.3900,C=86.6100,body=-0.7800 | 2026-08-11:GREEN:O=86.7900,C=87.2200,body=+0.4300 | 2026-08-12:GREEN:O=86.3800,C=91.3400,body=+4.9600 | 2026-08-13:RED:O=91.6300,C=90.3800,body=-1.2500 | 2026-08-14:GREEN:O=90.6100,C=90.8000,body=+0.1900 | 2026-08-17:GREEN:O=90.0900,C=90.2700,body=+0.1800 | 2026-08-18:GREEN:O=102.5000,C=105.1900,body=+2.6900 | **GOOD** |
| `A06_volume_red_green_ratio` | ratio=1.176 if finite else n/a; GREEN_vol_sum=4468567 dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-14,2026-08-17,2026-08-18]; RED_vol_sum=3798500 dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-13]; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:vol=1140300 | 2026-08-06:RED:vol=1275200 | 2026-08-07:GREEN:vol=657100 | 2026-08-10:RED:vol=571100 | 2026-08-11:GREEN:vol=745400 | 2026-08-12:GREEN:vol=823000 | 2026-08-13:RED:vol=811900 | 2026-08-14:GREEN:vol=533300 | 2026-08-17:GREEN:vol=467900 | 2026-08-18:GREEN:vol=1241867 | **GOOD** |
| `A07_rvol` | RVOL=1.603 on 2026-08-18: today_vol=1241867 / avg20=774680 (avg window 2026-07-16→2026-08-17, excludes today) | **GOOD** |
| `A08_bollinger_position` | pos=1.522 on 2026-08-18 (price=105.1900, mid=85.7670, upper=98.5285, lower=73.0055; 20d BB) | **BAD** |
| `A09_above_sma50` | above=True on 2026-08-18: price=105.1900 vs SMA50=79.3108 dist=+32.63% | **GOOD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-18: SMA20=85.7670 SMA50=79.3108 SMA80=71.9805 | **GOOD** |
| `A11_max_downside_2m` | maxDD=+0.00% inside window 2026-06-15→2026-08-18 (42 sessions): peak 2026-08-18 @ 105.1900 → trough 2026-08-18 @ 105.1900 | **NEUTRAL** |
| `A12_green_body_vs_wick` | GREEN dates=[2026-08-07,2026-08-11,2026-08-12,2026-08-14,2026-08-17,2026-08-18] body_frac=0.546 wick_frac=0.454; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=2.4200,upperW=0.9200,lowerW=0.2800,range=3.6200 | 2026-08-06:RED:body=0.7500,upperW=2.2300,lowerW=4.1900,range=7.1700 | 2026-08-07:GREEN:body=2.2900,upperW=0.0300,lowerW=0.6700,range=2.9900 | 2026-08-10:RED:body=0.7800,upperW=0.5800,lowerW=0.6400,range=2.0000 | 2026-08-11:GREEN:body=0.4300,upperW=1.6600,lowerW=0.6900,range=2.7800 | 2026-08-12:GREEN:body=4.9600,upperW=0.0500,lowerW=0.2400,range=5.2500 | 2026-08-13:RED:body=1.2500,upperW=0.6200,lowerW=0.4700,range=2.3400 | 2026-08-14:GREEN:body=0.1900,upperW=0.6900,lowerW=0.7900,range=1.6700 | 2026-08-17:GREEN:body=0.1800,upperW=1.0900,lowerW=1.0300,range=2.3000 | 2026-08-18:GREEN:body=2.6900,upperW=1.4800,lowerW=0.4999,range=4.6699 | **GOOD** |
| `A13_red_body_vs_wick` | RED dates=[2026-08-05,2026-08-06,2026-08-10,2026-08-13] body_frac=0.344 wick_frac=0.656; window=2026-08-05→2026-08-18; sessions: 2026-08-05:RED:body=2.4200,upperW=0.9200,lowerW=0.2800,range=3.6200 | 2026-08-06:RED:body=0.7500,upperW=2.2300,lowerW=4.1900,range=7.1700 | 2026-08-07:GREEN:body=2.2900,upperW=0.0300,lowerW=0.6700,range=2.9900 | 2026-08-10:RED:body=0.7800,upperW=0.5800,lowerW=0.6400,range=2.0000 | 2026-08-11:GREEN:body=0.4300,upperW=1.6600,lowerW=0.6900,range=2.7800 | 2026-08-12:GREEN:body=4.9600,upperW=0.0500,lowerW=0.2400,range=5.2500 | 2026-08-13:RED:body=1.2500,upperW=0.6200,lowerW=0.4700,range=2.3400 | 2026-08-14:GREEN:body=0.1900,upperW=0.6900,lowerW=0.7900,range=1.6700 | 2026-08-17:GREEN:body=0.1800,upperW=1.0900,lowerW=1.0300,range=2.3000 | 2026-08-18:GREEN:body=2.6900,upperW=1.4800,lowerW=0.4999,range=4.6699 | **GOOD** |
| `B01_eps_surprise` | 5.87 | **GOOD** |
| `B02_revenue_surprise` | 3.51 | **GOOD** |
| `B03_sales` | 1352.01 | **NEUTRAL** |
| `B04_income` | 96.29 | **GOOD** |
| `B05_profit_margin` | 7.12 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 98.9 | **NEUTRAL** |
| `B08_target_price_delta` | delta=2.0 (now=98.9 vs prior_export=96.9 on finviz_2026-08-17) | **GOOD** |
| `B09_analyst_recom` | 1.83 | **GOOD** |
| `B10_insider_transactions` | 0.0 | **NEUTRAL** |
| `B11_insider_tx_delta` | delta=0.0 (now=0.0 vs prior=0.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -5.02 | **BAD** |
| `B13_short_float` | 6.55 | **NEUTRAL** |
| `B14_earnings_date` | 8/6/2026 8:30:00 AM | **NEUTRAL** |

CSV: `data/ab_checklist/2026-08-18_ab_checklist.csv`
Columns: `val_*` (full dated string), `flag_*` (+1/0/-1), `status_*`, plus `green_dates` / `red_dates` / `body_window`.