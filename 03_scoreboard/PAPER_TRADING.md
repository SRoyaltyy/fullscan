# Paper trading — Futubull-fee simulation

As of **2026-11-17** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-17** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,604.79 | -13.95% | $249.59 | 5 | 617 | $1,319.02 | $-1,384.96 | $-10.25 | 34.6% | 0.0% |
| 1d_size | $9,275.61 | -7.24% | $471.08 | 5 | 555 | $1,313.51 | $-714.12 | $-10.27 | 37.8% | 0.0% |
| 3d_top | $8,852.13 | -11.48% | $217.74 | 14 | 626 | $1,189.09 | $-895.17 | $-252.69 | 32.4% | 7.1% |
| 3d_size | $9,085.63 | -9.14% | $137.41 | 11 | 581 | $1,144.50 | $-664.40 | $-249.97 | 33.0% | 9.1% |
| 1w_top | $9,224.99 | -7.75% | $150.98 | 15 | 511 | $949.54 | $-299.47 | $-475.54 | 27.8% | 20.0% |
| 1w_size | $8,627.59 | -13.72% | $121.17 | 23 | 507 | $920.87 | $-559.38 | $-813.03 | 29.8% | 8.7% |
| 2w_top | $9,999.55 | -0.00% | $52.75 | 22 | 338 | $497.25 | $-37.00 | $+36.54 | 39.9% | 27.3% |
| 2w_size | $10,042.37 | +0.42% | $50.35 | 22 | 352 | $530.06 | $+235.31 | $-192.94 | 38.2% | 27.3% |
| 1m_top | $9,194.55 | -8.05% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $+27.72 | 38.8% | 26.1% |
| 1m_size | $9,881.73 | -1.18% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $-54.59 | 39.4% | 35.3% |

Equity curves + positions: `dashboard/index.html`
