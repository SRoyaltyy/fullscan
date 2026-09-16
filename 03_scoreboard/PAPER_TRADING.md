# Paper trading — Futubull-fee simulation

As of **2026-09-16** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-16** ranker: **green pile** (55 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,618.87 | -3.81% | $2,330.16 | 7 | 263 | $575.51 | $-361.40 | $-19.73 | 34.4% | 0.0% |
| 1d_size | $10,028.50 | +0.29% | $1,796.16 | 6 | 244 | $630.07 | $+47.77 | $-19.27 | 36.1% | 0.0% |
| 3d_top | $9,550.54 | -4.49% | $1,652.64 | 25 | 265 | $554.47 | $-403.57 | $-45.89 | 30.8% | 0.0% |
| 3d_size | $10,222.17 | +2.22% | $1,647.91 | 23 | 263 | $561.75 | $+266.83 | $-44.66 | 28.3% | 0.0% |
| 1w_top | $10,118.05 | +1.18% | $1,499.89 | 37 | 211 | $418.07 | $+186.59 | $-68.54 | 24.1% | 0.0% |
| 1w_size | $10,248.11 | +2.48% | $1,495.62 | 37 | 219 | $427.61 | $+320.89 | $-72.78 | 25.3% | 0.0% |
| 2w_top | $10,212.16 | +2.12% | $420.98 | 41 | 93 | $175.85 | $+282.70 | $-70.54 | 50.0% | 0.0% |
| 2w_size | $10,949.38 | +9.49% | $332.93 | 42 | 116 | $186.54 | $+1,044.26 | $-94.89 | 48.6% | 0.0% |
| 1m_top | $9,625.30 | -3.75% | $428.71 | 46 | 64 | $98.40 | $-291.21 | $-83.48 | 33.3% | 0.0% |
| 1m_size | $10,637.35 | +6.37% | $208.52 | 38 | 68 | $121.86 | $+744.36 | $-107.01 | 53.3% | 0.0% |

Equity curves + positions: `dashboard/index.html`
