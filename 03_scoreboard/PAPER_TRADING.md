# Paper trading — Futubull-fee simulation

As of **2026-11-23** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-23** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,340.02 | -16.60% | $197.72 | 8 | 670 | $1,435.10 | $-1,676.61 | $+16.63 | 34.7% | 12.5% |
| 1d_size | $8,900.03 | -11.00% | $121.29 | 7 | 599 | $1,414.25 | $-1,133.15 | $+33.18 | 37.5% | 14.3% |
| 3d_top | $8,730.02 | -12.70% | $84.15 | 19 | 665 | $1,244.98 | $-1,330.10 | $+60.12 | 31.6% | 52.6% |
| 3d_size | $9,067.78 | -9.32% | $115.27 | 17 | 615 | $1,189.17 | $-1,152.95 | $+220.73 | 32.1% | 70.6% |
| 1w_top | $9,144.02 | -8.56% | $100.51 | 21 | 543 | $993.52 | $-828.83 | $-27.15 | 27.2% | 42.9% |
| 1w_size | $8,777.68 | -12.22% | $33.00 | 18 | 536 | $957.71 | $-1,436.10 | $+213.79 | 29.0% | 61.1% |
| 2w_top | $9,813.08 | -1.87% | $119.98 | 22 | 376 | $554.12 | $-261.92 | $+75.00 | 37.9% | 59.1% |
| 2w_size | $10,116.14 | +1.16% | $41.32 | 22 | 384 | $570.67 | $-93.23 | $+209.37 | 36.5% | 45.5% |
| 1m_top | $9,024.89 | -9.75% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-141.94 | 38.8% | 17.4% |
| 1m_size | $9,748.21 | -2.52% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $-188.11 | 39.4% | 23.5% |

Equity curves + positions: `dashboard/index.html`
