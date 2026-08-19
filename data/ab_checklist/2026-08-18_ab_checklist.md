# A+B1 Feature Checklist — 2026-08-18

- Gate: Market Cap > $80M · ADV > 500,000 shares → **1** names
- Export: `finviz_2026-08-18.csv` · prior export for Δ: `2026-08-17`
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
| 1 | BB | +7 | 11 | 4 | 2026-08-13→2026-08-14 | Software - Infrastructure |

## Full checklist — top 5

### BB  ·  score **+7**  ·  Software - Infrastructure
price=8.899999618530273  pair=`2026-08-13→2026-08-14`

| Feature | Value (with dates) | Status |
|---------|--------------------|:------:|
| `A01_rsi_value` | RSI=47.81 on 2026-08-14; prev RSI=49.11 on 2026-08-13 | **NEUTRAL** |
| `A02_rsi_cross_30` | above | RSI 49.11@2026-08-13 → 47.81@2026-08-14 vs 30 | rule: cross_up=GOOD | **NEUTRAL** |
| `A03_rsi_cross_50` | below | RSI 49.11@2026-08-13 → 47.81@2026-08-14 vs 50 | rule: cross_up=GOOD cross_down=BAD | **NEUTRAL** |
| `A04_rsi_cross_70` | below | RSI 49.11@2026-08-13 → 47.81@2026-08-14 vs 70 | rule: cross_down=BAD | **NEUTRAL** |
| `A05_body_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_body_sum/RED_body_sum=1.286 (G=0.1800 R=0.1400); 2026-08-13:GREEN:O=8.8100,C=8.9900,body=+0.1800,vol=11421200.0; 2026-08-14:RED:O=9.0400,C=8.9000,body=-0.1400,vol=7606600.0 | **GOOD** |
| `A06_volume_red_green_2day` | STRICT 2-day pair only: 2026-08-13 + 2026-08-14; ratio=GREEN_vol/RED_vol=1.501 (Gvol=11421200 Rvol=7606600); 2026-08-13:GREEN:O=8.8100,C=8.9900,body=+0.1800,vol=11421200.0; 2026-08-14:RED:O=9.0400,C=8.9000,body=-0.1400,vol=7606600.0 | **GOOD** |
| `A07_rvol` | RVOL=0.531 on 2026-08-14: today_vol=7606600 / avg20=14319155 (avg window 2026-07-17→2026-08-13, excludes asof) | **NEUTRAL** |
| `A08_bollinger_position` | pos=0.351 on 2026-08-14 (price=8.9000, mid=8.6775, upper=9.3108, lower=8.0442; 20d BB) | **NEUTRAL** |
| `A09_above_sma50` | above=False on 2026-08-14: price=8.9000 vs SMA50=9.5656 dist=-6.96% | **BAD** |
| `A10_sma20_50_80_stack` | mixed_20=8.68_50=9.57_80=8.50 on 2026-08-14: SMA20=8.6775 SMA50=9.5656 SMA80=8.4965 | **NEUTRAL** |
| `A11_three_section_lows` | window=2026-05-15→2026-08-14 (63 bars); S1[2026-05-15→2026-06-15] low=2026-05-15@5.9600; S2[2026-06-16→2026-07-16] low=2026-06-18@8.2100; S3[2026-07-17→2026-08-14] low=2026-07-28@7.7100 | lows=[5.960000038146973, 8.210000038146973, 7.710000038146973] span=37.75% rising_lows=False flatish(≤12%)=False | **BAD** |
| `A12_green_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: GREEN body_frac=0.5454527064127388 wick_frac=0.4545472935872612 | **GOOD** |
| `A13_red_body_vs_wick_2day` | pair 2026-08-13+2026-08-14: RED body_frac=0.5384623849172872 wick_frac=0.46153761508271285 | **BAD** |
| `A15_tape_recovery_setup` | body_rg_2d=1.2857065006369166 need>1.4; red_wick_gt_green=False 5d trail=2026-08-10:RED:body=-0.1100:wick=0.1900; 2026-08-11:GREEN:body=+0.0800:wick=0.3300; 2026-08-12:RED:body=-0.4300:wick=0.0100; 2026-08-13:GREEN:body=+0.1800:wick=0.1500; 2026-08-14:RED:body=-0.1400:wick=0.1200 | **NEUTRAL** |
| `B01_eps_surprise` | EPS surprise=42.86 (current export asof; earnings_date=6/25/2026 8:30:00 AM) | **GOOD** |
| `B02_revenue_surprise` | Revenue surprise=11.43 (current export; earnings_date=6/25/2026 8:30:00 AM) | **GOOD** |
| `B03_sales` | 580.3 | **NEUTRAL** |
| `B04_income` | 59.8 | **GOOD** |
| `B05_profit_margin` | 10.31 | **GOOD** |
| `B06_profitable` | True | **GOOD** |
| `B07_target_price` | 10.61 | **NEUTRAL** |
| `B08_target_price_delta` | delta=0.0 (now=10.61 vs prior_export=10.61 on finviz_2026-08-17) | **NEUTRAL** |
| `B09_analyst_recom` | 2.8 | **NEUTRAL** |
| `B10_insider_transactions` | -21.86 | **BAD** |
| `B11_insider_tx_delta` | delta=0.0 (now=-21.86 vs prior=-21.86 on finviz_2026-08-17) | **NEUTRAL** |
| `B12_institutional_transactions` | 1.26 | **GOOD** |
| `B13_short_float` | 6.71 | **NEUTRAL** |
| `B14_earnings_date` | 6/25/2026 8:30:00 AM | **NEUTRAL** |
| `B17_eps_surprise_pair` | last2 EPS surprises: current=42.86 (this export) | prior_export=42.86 (finviz_2026-08-17) | GOOD if latest beat (and better if both beat) | **GOOD** |
| `B18_rev_surprise_pair` | last2 Revenue surprises: current=11.43 (this export) | prior_export=11.43 (finviz_2026-08-17) | GOOD if latest beat (and better if both beat) | **GOOD** |

CSV: `data/ab_checklist/2026-08-18_ab_checklist.csv`
Columns: `val_*`, `flag_*`, `status_*`, `pair_day_a`, `pair_day_b`.