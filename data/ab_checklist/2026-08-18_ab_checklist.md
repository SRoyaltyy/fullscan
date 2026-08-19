# A+B1 Feature Checklist — 2026-08-18

- Gate: Market Cap > $80M · ADV > 500,000 shares → **1** names
- Export: `finviz_2026-08-18.csv` · prior export for Δ: `2026-08-17`
- score = sum of flags over **29** features

## Framing (per asof trading day)

- **A05/A06/A12/A13** use **exactly two connected sessions**: `pair_day_a` (prev) + `pair_day_b` (asof).
  No multi-day green/red sums.
- **RSI crosses**: cross **up** through 30 or 50 → GOOD; cross **down** through 50 or 70 → BAD.
- **A11 downside structure**: last ~63 sessions split into 3 equal sections; lowest **low** in each;
  GOOD if rising lows or span(highest low − lowest low)/lowest ≤ 12%.
- **B17/B18**: current export EPS/Rev surprise vs **prior export** snapshot (proxy for last 2 prints).
- Analyst last-2 rating actions (upgrade/downgrade) come from quote scrape → merge step (B19).

## Ranked (top 5)

| Rank | Ticker | score | good | bad | pair | Industry |
|-----:|--------|------:|-----:|----:|------|----------|
| 1 | BBAI | -7 | 5 | 12 | 2026-08-13→2026-08-14 | Information Technology Services |

## Full checklist — top 5

### BBAI  ·  score **-7**  ·  Information Technology Services
price=3.2699999809265137  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=53.67 on 2026-08-14; prev RSI=56.38 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 56.38@2026-08-13 → 53.67@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | above | RSI 56.38@2026-08-13 → 53.67@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 56.38@2026-08-13 → 53.67@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=0.818 (G=0.0900 R=0.1100); 2026-08-13:GREEN:O=3.2500,C=3.3400,body=+0.0900,vol=18615700.0; 2026-08-14:RED:O=3.3800,C=3.2700,body=-0.1100,vol=14393100.0 | **BAD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.293 (Gvol=18615700 Rvol=14393100); 2026-08-13:GREEN:O=3.2500,C=3.3400,body=+0.0900,vol=18615700.0; 2026-08-14:RED:O=3.3800,C=3.2700,body=-0.1100,vol=14393100.0 | **GOOD** |
| `A07_rvol` | RVOL=0.587 on 2026-08-14: today_vol=14393100 / avg20=24511890 (avg window 2026-07-17→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.613 on 2026-08-14 (price=3.2700, mid=2.9900, upper=3.4465, lower=2.5335; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-08-14: price=3.2700 vs SMA50=3.3902 dist=-3.55% | **BAD** |
| `A10_sma20_50_80_stack` | bear_aligned_20<50<80 on 2026-08-14: SMA20=2.9900 SMA50=3.3902 SMA80=3.7120 | **BAD** |
| `A11_three_section_lows` | window=2026-05-15→2026-08-14 (63 bars); S1[2026-05-15→2026-06-15] low=2026-05-19@3.7500; S2[2026-06-16→2026-07-16] low=2026-07-16@2.9100; S3[2026-07-17→2026-08-14] low=2026-07-29@2.5900 | lows=[3.75, 2.9100000858306885, 2.5899999141693115] span=44.79% rising_lows=False flatish(≤12%)=False | **BAD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.6 wick_frac=0.4 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.6470593185158591 wick_frac=0.352940681484141 | **BAD** |
| `B01_eps_surprise` | EPS surprise=-11.11 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **BAD** |
| `B02_revenue_surprise` | Revenue surprise=1.04 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 131.63 | **NEUTRAL** |
| `B04_income` | -85.82 | **BAD** |
| `B05_profit_margin` | -65.2 | **BAD** |
| `B06_profitable` | False | **BAD** |
| `B07_target_price` | 4.0 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=4.0 vs prior_export=4.0 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 3.0 | **NEUTRAL** |
| `B10_insider_transactions` | -2.08 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.08 vs prior=-2.08 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | -0.99 | **BAD** |
| `B13_short_float` | 31.12 | **GOOD** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=-11.11 (this export) | prior_export=-11.11 (finviz_2026-08-17) | GOOD if latest beat (and better if both beat) | **BAD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=1.04 (this export) | prior_export=1.04 (finviz_2026-08-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-18_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.