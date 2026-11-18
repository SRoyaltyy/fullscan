# Paper trading — Futubull-fee simulation

As of **2026-11-18** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-18** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,499.01 | -15.01% | $127.93 | 7 | 627 | $1,342.68 | $-1,459.65 | $-41.34 | 34.8% | 0.0% |
| 1d_size | $9,170.07 | -8.30% | $180.85 | 6 | 564 | $1,336.68 | $-786.10 | $-43.83 | 38.0% | 0.0% |
| 3d_top | $8,999.61 | -10.00% | $138.67 | 17 | 629 | $1,189.89 | $-895.17 | $-105.21 | 32.4% | 47.1% |
| 3d_size | $9,103.60 | -8.96% | $88.36 | 15 | 585 | $1,145.00 | $-664.40 | $-231.99 | 33.0% | 33.3% |
| 1w_top | $9,321.10 | -6.79% | $118.88 | 17 | 515 | $949.98 | $-299.92 | $-378.98 | 27.7% | 23.5% |
| 1w_size | $8,785.80 | -12.14% | $96.69 | 24 | 514 | $921.69 | $-561.05 | $-653.15 | 29.8% | 33.3% |
| 2w_top | $10,128.34 | +1.28% | $52.75 | 22 | 338 | $497.25 | $-37.00 | $+165.33 | 39.9% | 31.8% |
| 2w_size | $10,185.25 | +1.85% | $50.35 | 22 | 352 | $530.06 | $+235.31 | $-50.06 | 38.2% | 36.4% |
| 1m_top | $9,313.32 | -6.87% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $+146.49 | 38.8% | 30.4% |
| 1m_size | $9,981.35 | -0.19% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $+45.03 | 39.4% | 41.2% |

Equity curves + positions: `dashboard/index.html`
