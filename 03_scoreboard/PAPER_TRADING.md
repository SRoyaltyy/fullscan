# Paper trading — Futubull-fee simulation

As of **2026-11-03** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-03** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,551.45 | -4.49% | $4,458.43 | 2 | 510 | $1,080.39 | $-1,169.87 | $+721.32 | 32.7% | 100.0% |
| 1d_size | $10,231.08 | +2.31% | $4,783.48 | 2 | 466 | $1,086.93 | $-541.34 | $+772.42 | 37.5% | 100.0% |
| 3d_top | $9,751.86 | -2.48% | $2,367.91 | 19 | 513 | $1,039.08 | $-1,069.02 | $+820.88 | 30.8% | 78.9% |
| 3d_size | $10,387.84 | +3.88% | $2,704.91 | 18 | 484 | $1,007.26 | $-84.40 | $+472.25 | 33.0% | 55.6% |
| 1w_top | $10,020.50 | +0.21% | $2,291.42 | 20 | 446 | $845.20 | $-505.32 | $+525.82 | 23.9% | 60.0% |
| 1w_size | $9,817.12 | -1.83% | $2,295.03 | 25 | 429 | $810.51 | $-512.40 | $+329.52 | 26.7% | 60.0% |
| 2w_top | $10,276.67 | +2.77% | $1,460.46 | 42 | 274 | $414.84 | $-425.74 | $+702.41 | 33.6% | 66.7% |
| 2w_size | $10,624.04 | +6.24% | $1,167.83 | 43 | 287 | $441.88 | $+330.00 | $+294.04 | 34.4% | 62.8% |
| 1m_top | $9,534.17 | -4.66% | $1,386.94 | 67 | 175 | $195.62 | $-510.31 | $+44.48 | 27.8% | 55.2% |
| 1m_size | $10,363.47 | +3.63% | $1,156.53 | 57 | 151 | $235.51 | $+272.74 | $+90.73 | 38.3% | 49.1% |

Equity curves + positions: `dashboard/index.html`
