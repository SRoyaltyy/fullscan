# Paper trading — Futubull-fee simulation

As of **2026-09-16** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-16** ranker: **green pile** (100 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,620.38 | -3.80% | $2,482.20 | 6 | 262 | $573.99 | $-361.40 | $-18.21 | 34.4% | 0.0% |
| 1d_size | $10,028.97 | +0.29% | $1,844.83 | 6 | 244 | $629.60 | $+47.77 | $-18.80 | 36.1% | 0.0% |
| 3d_top | $9,572.96 | -4.27% | $1,421.48 | 25 | 259 | $545.23 | $-378.41 | $-48.62 | 31.6% | 0.0% |
| 3d_size | $10,300.85 | +3.01% | $534.64 | 27 | 259 | $552.63 | $+353.28 | $-52.43 | 28.4% | 0.0% |
| 1w_top | $10,143.64 | +1.44% | $994.23 | 39 | 207 | $411.93 | $+217.71 | $-74.07 | 25.0% | 0.0% |
| 1w_size | $10,319.55 | +3.20% | $488.40 | 43 | 213 | $416.56 | $+399.23 | $-79.68 | 27.1% | 2.3% |
| 2w_top | $10,232.14 | +2.32% | $458.99 | 40 | 92 | $175.47 | $+282.70 | $-50.56 | 50.0% | 0.0% |
| 2w_size | $10,977.79 | +9.78% | $320.01 | 43 | 117 | $186.67 | $+1,044.26 | $-66.48 | 48.6% | 2.3% |
| 1m_top | $9,659.72 | -3.40% | $412.49 | 47 | 63 | $98.21 | $-289.99 | $-50.29 | 37.5% | 0.0% |
| 1m_size | $10,687.39 | +6.87% | $195.60 | 39 | 69 | $121.99 | $+744.36 | $-56.97 | 53.3% | 2.6% |

Equity curves + positions: `dashboard/index.html`
