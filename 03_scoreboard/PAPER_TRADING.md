# Paper trading — Futubull-fee simulation

As of **2026-09-15** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-15** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,620.38 | -3.80% | $2,482.20 | 6 | 262 | $573.99 | $-361.40 | $-18.21 | 34.4% | 0.0% |
| 1d_size | $10,030.02 | +0.30% | $1,948.20 | 5 | 243 | $628.55 | $+47.77 | $-17.75 | 36.1% | 0.0% |
| 3d_top | $9,572.96 | -4.27% | $1,421.48 | 25 | 259 | $545.23 | $-378.41 | $-48.62 | 31.6% | 0.0% |
| 3d_size | $10,303.02 | +3.03% | $230.73 | 27 | 257 | $550.47 | $+353.01 | $-49.99 | 27.8% | 3.7% |
| 1w_top | $10,143.64 | +1.44% | $994.23 | 39 | 207 | $411.93 | $+217.71 | $-74.07 | 25.0% | 0.0% |
| 1w_size | $10,319.95 | +3.20% | $527.17 | 42 | 212 | $416.16 | $+399.23 | $-79.29 | 27.1% | 2.4% |
| 2w_top | $10,232.14 | +2.32% | $458.99 | 40 | 92 | $175.47 | $+282.70 | $-50.56 | 50.0% | 0.0% |
| 2w_size | $10,977.92 | +9.78% | $332.93 | 42 | 116 | $186.54 | $+1,044.26 | $-66.35 | 48.6% | 2.4% |
| 1m_top | $9,659.72 | -3.40% | $412.49 | 47 | 63 | $98.21 | $-289.99 | $-50.29 | 37.5% | 0.0% |
| 1m_size | $10,687.52 | +6.88% | $208.52 | 38 | 68 | $121.86 | $+744.36 | $-56.84 | 53.3% | 2.6% |

Equity curves + positions: `dashboard/index.html`
