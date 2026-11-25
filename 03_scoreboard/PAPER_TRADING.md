# Paper trading — Futubull-fee simulation

As of **2026-11-23** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-23** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,479.02 | -15.21% | $197.72 | 8 | 670 | $1,435.10 | $-1,676.61 | $+155.63 | 34.7% | 75.0% |
| 1d_size | $8,988.89 | -10.11% | $121.29 | 7 | 599 | $1,414.25 | $-1,133.15 | $+122.04 | 37.5% | 71.4% |
| 3d_top | $8,744.70 | -12.55% | $84.15 | 19 | 665 | $1,244.98 | $-1,330.10 | $+74.80 | 31.6% | 68.4% |
| 3d_size | $9,118.18 | -8.82% | $115.27 | 17 | 615 | $1,189.17 | $-1,152.95 | $+271.13 | 32.1% | 70.6% |
| 1w_top | $9,103.78 | -8.96% | $100.51 | 21 | 543 | $993.52 | $-828.83 | $-67.39 | 27.2% | 47.6% |
| 1w_size | $8,836.60 | -11.63% | $33.00 | 18 | 536 | $957.71 | $-1,436.10 | $+272.71 | 29.0% | 61.1% |
| 2w_top | $9,845.08 | -1.55% | $119.98 | 22 | 376 | $554.12 | $-261.92 | $+107.00 | 37.9% | 59.1% |
| 2w_size | $10,187.32 | +1.87% | $41.32 | 22 | 384 | $570.67 | $-93.23 | $+280.55 | 36.5% | 50.0% |
| 1m_top | $8,938.80 | -10.61% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-228.03 | 38.8% | 13.0% |
| 1m_size | $9,836.00 | -1.64% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $-100.32 | 39.4% | 35.3% |

Equity curves + positions: `dashboard/index.html`
