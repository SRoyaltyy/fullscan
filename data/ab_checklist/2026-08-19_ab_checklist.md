# A+B1 Feature Checklist — 2026-08-19

- Gate: Market Cap > $80M · ADV > 500,000 shares → **1** names
- Export: `finviz_2026-08-19.csv` · prior export for Δ: `2026-08-18`
- score = sum of flags over **30** features

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
| 1 | AAPL | +10 | 13 | 3 | 2026-08-13→2026-08-14 | Consumer Electronics |

## Full checklist — top 5

### AAPL  ·  score **+10**  ·  Consumer Electronics
price=305.92999267578125  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=43.82 on 2026-08-14; prev RSI=43.18 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 43.18@2026-08-13 → 43.82@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 43.18@2026-08-13 → 43.82@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 43.18@2026-08-13 → 43.82@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=14.999 (G=1.0500 R=0.0700); 2026-08-13:GREEN:O=304.2100,C=305.2600,body=+1.0500,vol=40349300.0; 2026-08-14:RED:O=306.0000,C=305.9300,body=-0.0700,vol=28186700.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.432 (Gvol=40349300 Rvol=28186700); 2026-08-13:GREEN:O=304.2100,C=305.2600,body=+1.0500,vol=40349300.0; 2026-08-14:RED:O=306.0000,C=305.9300,body=-0.0700,vol=28186700.0 | **GOOD** |
| `A07_rvol` | RVOL=0.518 on 2026-08-14: today_vol=28186700 / avg20=54376315 (avg window 2026-07-17→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=-0.473 on 2026-08-14 (price=305.9300, mid=318.2199, upper=344.2205, lower=292.2193; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-08-14: price=305.9300 vs SMA50=308.9507 dist=-0.98% | **BAD** |
| `A10_sma20_50_80_stack` | bull_aligned_20>50>80 on 2026-08-14: SMA20=318.2199 SMA50=308.9507 SMA80=302.7094 | **GOOD** |
| `A11_three_section_lows` | window=2026-05-15→2026-08-14 (63 bars); S1[2026-05-15→2026-06-15] low=2026-06-10@287.1324; S2[2026-06-16→2026-07-16] low=2026-06-25@273.5141; S3[2026-07-17→2026-08-14] low=2026-07-31@299.7415 | lows=[287.1323727394638, 273.5141152813601, 299.7415030462749] span=9.59% rising_lows=False flatish(≤12%)=True | **GOOD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.2658265988843735 wick_frac=0.7341734011156265 | **BAD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.021945852865206162 wick_frac=0.9780541471347939 | **GOOD** |
| `A15_tape_recovery_setup` | body_rg_2d=14.998692240627724 need>1.4; red_wick_gt_green=True 5d trail=2026-08-10:GREEN:body=+1.4300:wick=2.2200; 2026-08-11:RED:body=-2.8400:wick=4.3400; 2026-08-12:RED:body=-2.8500:wick=2.2400; 2026-08-13:GREEN:body=+1.0500:wick=2.9000; 2026-08-14:RED:body=-0.0700:wick=3.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=6.77 (current export asof; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=0.35 (current export; earnings_date=7/30/2026 4:30:00 PM) | **GOOD** |
| `B03_sales` | 466823.0 | **NEUTRAL** |
| `B04_income` | 128930.0 | **GOOD** |
| `B05_profit_margin` | 27.62 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 334.23 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=334.23 vs prior_export=334.23 on finviz_2026-08-18) | **NEUTRAL** |
| `B09_analyst_recom` | 2.06 | **GOOD** |
| `B10_insider_transactions` | -2.23 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-2.23 vs prior=-2.23 on finviz_2026-08-18) | **NEUTRAL** |
| `B12_institutional_transactions` | 0.0 | **NEUTRAL** |
| `B13_short_float` | 0.97 | **NEUTRAL** |
| `B14_earnings_date` | 7/30/2026 4:30:00 PM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=6.77 (this export) | prior_export=6.77 (finviz_2026-08-18) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=0.35 (this export) | prior_export=0.35 (finviz_2026-08-18) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-19_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.