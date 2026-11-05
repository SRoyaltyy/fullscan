# Paper trading — Futubull-fee simulation

As of **2026-11-05** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-05** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,529.81 | -4.70% | $605.04 | 8 | 520 | $1,101.17 | $-453.79 | $-16.40 | 33.2% | 0.0% |
| 1d_size | $10,213.60 | +2.14% | $272.01 | 6 | 474 | $1,103.95 | $+226.21 | $-12.61 | 38.0% | 0.0% |
| 3d_top | $9,495.00 | -5.05% | $238.59 | 10 | 542 | $1,098.27 | $-483.77 | $-21.24 | 33.5% | 0.0% |
| 3d_size | $10,170.09 | +1.70% | $179.31 | 9 | 511 | $1,062.79 | $+189.21 | $-19.12 | 34.3% | 0.0% |
| 1w_top | $9,749.69 | -2.50% | $115.23 | 10 | 476 | $904.83 | $-228.93 | $-21.38 | 26.2% | 0.0% |
| 1w_size | $9,511.33 | -4.89% | $279.35 | 9 | 463 | $873.01 | $-469.63 | $-19.04 | 28.6% | 0.0% |
| 2w_top | $9,941.41 | -0.59% | $199.67 | 10 | 326 | $495.76 | $-37.00 | $-21.59 | 39.9% | 0.0% |
| 2w_size | $10,216.20 | +2.16% | $240.42 | 9 | 339 | $528.11 | $+235.31 | $-19.10 | 38.2% | 0.0% |
| 1m_top | $9,145.38 | -8.55% | $202.55 | 10 | 252 | $275.65 | $-833.16 | $-21.46 | 38.8% | 0.0% |
| 1m_size | $9,917.23 | -0.83% | $92.19 | 9 | 217 | $321.87 | $-63.68 | $-19.09 | 39.4% | 0.0% |

Equity curves + positions: `dashboard/index.html`
