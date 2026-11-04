# Paper trading — Futubull-fee simulation

As of **2026-11-03** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-03** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,551.45 | -4.49% | $4,458.43 | 2 | 510 | $1,080.39 | $-1,169.87 | $+721.32 | 32.7% | 100.0% |
| 1d_size | $10,231.08 | +2.31% | $4,783.48 | 2 | 466 | $1,086.93 | $-541.34 | $+772.42 | 37.5% | 100.0% |
| 3d_top | $9,772.70 | -2.27% | $2,367.91 | 19 | 513 | $1,039.08 | $-1,069.02 | $+841.72 | 30.8% | 73.7% |
| 3d_size | $10,459.63 | +4.60% | $2,704.91 | 18 | 484 | $1,007.26 | $-84.40 | $+544.04 | 33.0% | 55.6% |
| 1w_top | $10,046.11 | +0.46% | $2,291.42 | 20 | 446 | $845.20 | $-505.32 | $+551.43 | 23.9% | 55.0% |
| 1w_size | $9,885.52 | -1.14% | $2,295.03 | 25 | 429 | $810.51 | $-512.40 | $+397.92 | 26.7% | 60.0% |
| 2w_top | $10,297.17 | +2.97% | $1,460.46 | 42 | 274 | $414.84 | $-425.74 | $+722.92 | 33.6% | 64.3% |
| 2w_size | $10,675.27 | +6.75% | $1,167.83 | 43 | 287 | $441.88 | $+330.00 | $+345.27 | 34.4% | 60.5% |
| 1m_top | $9,571.29 | -4.29% | $1,386.94 | 67 | 175 | $195.62 | $-510.31 | $+81.61 | 27.8% | 53.7% |
| 1m_size | $10,402.31 | +4.02% | $1,156.53 | 57 | 151 | $235.51 | $+272.74 | $+129.57 | 38.3% | 47.4% |

Equity curves + positions: `dashboard/index.html`
