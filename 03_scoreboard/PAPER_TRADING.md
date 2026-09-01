# Paper trading — Futubull-fee simulation

As of **2026-08-31** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-08-31** ranker: **green pile** (134 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,120.71 | +1.21% | $10,120.71 | 0 | 114 | $259.77 | $+120.71 | $+0.00 | 43.9% | — |
| 1d_size | $10,712.84 | +7.13% | $10,712.84 | 0 | 140 | $396.02 | $+712.84 | $+0.00 | 47.1% | — |
| 3d_top | $9,963.18 | -0.37% | $537.11 | 13 | 97 | $223.01 | $+114.95 | $-151.77 | 45.2% | 46.2% |
| 3d_size | $10,907.94 | +9.08% | $532.74 | 13 | 87 | $202.59 | $+1,114.86 | $-206.92 | 48.6% | 53.8% |
| 1w_top | $10,745.99 | +7.46% | $556.05 | 11 | 71 | $167.52 | $+857.92 | $-111.93 | 36.7% | 45.5% |
| 1w_size | $10,758.51 | +7.59% | $763.00 | 13 | 77 | $164.20 | $+926.47 | $-167.96 | 50.0% | 53.8% |
| 2w_top | $10,813.08 | +8.13% | $7,592.08 | 15 | 41 | $105.66 | $+854.95 | $-41.87 | 76.9% | 40.0% |
| 2w_size | $10,716.97 | +7.17% | $253.93 | 17 | 49 | $92.76 | $+945.06 | $-228.09 | 68.8% | 35.3% |
| 1m_top | $10,130.52 | +1.31% | $118.94 | 20 | 20 | $35.38 | $+0.00 | $+130.52 | — | 50.0% |
| 1m_size | $10,517.82 | +5.18% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+517.82 | — | 45.5% |

Equity curves + positions: `dashboard/index.html`
