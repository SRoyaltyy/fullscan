# Paper trading — Futubull-fee simulation

As of **2026-11-20** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-20** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,067.91 | -19.32% | $337.08 | 8 | 656 | $1,405.97 | $-1,878.78 | $-53.31 | 34.0% | 0.0% |
| 1d_size | $8,648.37 | -13.52% | $305.47 | 6 | 588 | $1,391.34 | $-1,284.11 | $-67.52 | 37.1% | 0.0% |
| 3d_top | $8,549.86 | -14.50% | $120.97 | 18 | 658 | $1,242.98 | $-1,328.22 | $-121.92 | 31.9% | 5.6% |
| 3d_size | $8,818.78 | -11.81% | $159.20 | 17 | 609 | $1,187.90 | $-1,151.43 | $-29.79 | 32.4% | 11.8% |
| 1w_top | $8,934.40 | -10.66% | $192.32 | 21 | 537 | $991.43 | $-821.55 | $-244.05 | 27.5% | 4.8% |
| 1w_size | $8,551.82 | -14.48% | $88.20 | 21 | 529 | $953.34 | $-1,413.61 | $-34.57 | 29.1% | 19.0% |
| 2w_top | $9,526.36 | -4.74% | $168.97 | 24 | 368 | $552.67 | $-252.85 | $-220.79 | 39.0% | 4.2% |
| 2w_size | $9,765.60 | -2.34% | $189.95 | 20 | 376 | $568.46 | $-84.32 | $-150.08 | 37.1% | 10.0% |
| 1m_top | $8,852.11 | -11.48% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-314.72 | 38.8% | 17.4% |
| 1m_size | $9,701.22 | -2.99% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $-235.10 | 39.4% | 23.5% |

Equity curves + positions: `dashboard/index.html`
