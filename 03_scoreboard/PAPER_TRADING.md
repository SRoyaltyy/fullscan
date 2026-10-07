# Paper trading — Futubull-fee simulation

As of **2026-10-07** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-10-07** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,825.81 | -11.74% | $4,458.43 | 2 | 510 | $1,080.39 | $-1,169.87 | $-4.32 | 32.7% | 0.0% |
| 1d_size | $9,454.32 | -5.46% | $4,783.48 | 2 | 466 | $1,086.93 | $-541.34 | $-4.34 | 37.5% | 0.0% |
| 3d_top | $8,844.85 | -11.55% | $2,367.91 | 19 | 513 | $1,039.08 | $-1,069.02 | $-86.13 | 30.8% | 10.5% |
| 3d_size | $9,866.50 | -1.33% | $2,704.91 | 18 | 484 | $1,007.26 | $-84.40 | $-49.09 | 33.0% | 22.2% |
| 1w_top | $9,475.59 | -5.24% | $2,291.42 | 20 | 446 | $845.20 | $-505.32 | $-19.09 | 23.9% | 25.0% |
| 1w_size | $9,451.57 | -5.48% | $2,295.03 | 25 | 429 | $810.51 | $-512.40 | $-36.04 | 26.7% | 32.0% |
| 2w_top | $9,551.93 | -4.48% | $1,460.46 | 42 | 274 | $414.84 | $-425.74 | $-22.33 | 33.6% | 42.9% |
| 2w_size | $10,365.72 | +3.66% | $1,167.83 | 43 | 287 | $441.88 | $+330.00 | $+35.72 | 34.4% | 55.8% |
| 1m_top | $9,394.68 | -6.05% | $1,386.94 | 67 | 175 | $195.62 | $-510.31 | $-95.00 | 27.8% | 32.8% |
| 1m_size | $10,160.73 | +1.61% | $1,156.53 | 57 | 151 | $235.51 | $+272.74 | $-112.01 | 38.3% | 35.1% |

Equity curves + positions: `dashboard/index.html`
