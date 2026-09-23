# Paper trading — Futubull-fee simulation

As of **2026-09-22** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-22** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,622.31 | -3.78% | $4,882.98 | 4 | 390 | $863.65 | $-369.13 | $-8.56 | 32.1% | 0.0% |
| 1d_size | $10,128.30 | +1.28% | $5,118.88 | 4 | 350 | $869.39 | $+136.89 | $-8.59 | 35.3% | 0.0% |
| 3d_top | $8,935.43 | -10.65% | $1,731.99 | 22 | 402 | $818.96 | $-1,125.76 | $+61.19 | 28.4% | 22.7% |
| 3d_size | $9,496.93 | -5.03% | $2,096.13 | 21 | 375 | $785.23 | $-601.45 | $+98.38 | 29.9% | 28.6% |
| 1w_top | $9,729.61 | -2.70% | $1,386.05 | 35 | 343 | $664.46 | $-409.48 | $+139.09 | 20.1% | 37.1% |
| 1w_size | $9,842.57 | -1.57% | $1,261.26 | 34 | 330 | $643.26 | $-330.68 | $+173.25 | 22.3% | 41.2% |
| 2w_top | $9,688.00 | -3.12% | $2,685.67 | 40 | 168 | $275.22 | $-204.16 | $-107.84 | 32.8% | 27.5% |
| 2w_size | $10,846.24 | +8.46% | $1,050.23 | 45 | 191 | $292.08 | $+610.29 | $+235.95 | 34.2% | 40.0% |
| 1m_top | $9,298.38 | -7.02% | $135.59 | 56 | 96 | $125.96 | $-470.38 | $-231.24 | 20.0% | 30.4% |
| 1m_size | $10,238.84 | +2.39% | $120.40 | 45 | 89 | $133.64 | $+743.58 | $-504.74 | 40.9% | 33.3% |

Equity curves + positions: `dashboard/index.html`
