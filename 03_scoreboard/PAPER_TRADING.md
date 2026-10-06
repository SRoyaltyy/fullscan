# Paper trading — Futubull-fee simulation

As of **2026-10-01** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-10-01** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,031.81 | -9.68% | $4,639.03 | 5 | 511 | $1,118.15 | $-957.96 | $-10.23 | 29.6% | 0.0% |
| 1d_size | $9,583.77 | -4.16% | $5,019.38 | 5 | 465 | $1,112.58 | $-405.98 | $-10.25 | 32.6% | 0.0% |
| 3d_top | $8,575.82 | -14.24% | $2,180.69 | 19 | 517 | $1,023.66 | $-1,399.58 | $-24.60 | 26.9% | 42.1% |
| 3d_size | $9,137.24 | -8.63% | $2,414.30 | 19 | 487 | $1,001.63 | $-854.61 | $-8.15 | 29.9% | 36.8% |
| 1w_top | $9,381.96 | -6.18% | $1,801.63 | 29 | 453 | $851.64 | $-567.52 | $-50.52 | 23.1% | 34.5% |
| 1w_size | $9,557.75 | -4.42% | $2,081.86 | 30 | 436 | $822.31 | $-401.43 | $-40.82 | 25.6% | 33.3% |
| 2w_top | $9,509.19 | -4.91% | $536.91 | 52 | 228 | $346.45 | $-340.67 | $-150.15 | 30.7% | 32.7% |
| 2w_size | $10,540.32 | +5.40% | $3,216.16 | 49 | 259 | $367.55 | $+627.38 | $-87.06 | 34.3% | 30.6% |
| 1m_top | $9,259.72 | -7.40% | $149.86 | 53 | 107 | $127.07 | $-475.53 | $-264.74 | 18.5% | 28.3% |
| 1m_size | $10,070.24 | +0.70% | $97.74 | 46 | 92 | $133.91 | $+743.41 | $-673.17 | 39.1% | 30.4% |

Equity curves + positions: `dashboard/index.html`
