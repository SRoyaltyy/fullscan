# Paper trading — Futubull-fee simulation

As of **2026-09-04** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-04** ranker: **green pile** (117 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,902.43 | -0.98% | $4,846.74 | 8 | 190 | $416.31 | $-81.17 | $-16.40 | 40.7% | 0.0% |
| 1d_size | $10,517.92 | +5.18% | $4,987.07 | 6 | 180 | $482.84 | $+530.46 | $-12.55 | 42.5% | 0.0% |
| 3d_top | $10,021.24 | +0.21% | $2,650.42 | 25 | 151 | $323.60 | $+78.89 | $-57.65 | 42.9% | 0.0% |
| 3d_size | $10,888.11 | +8.88% | $2,949.76 | 21 | 139 | $299.11 | $+933.21 | $-45.10 | 40.7% | 0.0% |
| 1w_top | $10,582.44 | +5.82% | $1,602.83 | 25 | 107 | $222.86 | $+688.56 | $-106.12 | 39.0% | 16.0% |
| 1w_size | $10,765.08 | +7.65% | $1,193.27 | 24 | 114 | $228.87 | $+859.55 | $-94.47 | 44.4% | 16.7% |
| 2w_top | $10,717.65 | +7.18% | $75.73 | 14 | 46 | $108.83 | $+818.07 | $-100.43 | 62.5% | 42.9% |
| 2w_size | $11,284.55 | +12.85% | $72.00 | 22 | 62 | $95.26 | $+943.81 | $+340.74 | 65.0% | 40.9% |
| 1m_top | $10,014.13 | +0.14% | $54.91 | 24 | 24 | $36.14 | $+0.00 | $+14.13 | — | 50.0% |
| 1m_size | $10,937.22 | +9.37% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+937.22 | — | 63.6% |

Equity curves + positions: `dashboard/index.html`
