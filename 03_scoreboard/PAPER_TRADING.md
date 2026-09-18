# Paper trading — Futubull-fee simulation

As of **2026-09-08** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-08** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,789.25 | -2.11% | $4,916.29 | 1 | 233 | $505.22 | $-208.28 | $-2.48 | 35.3% | 0.0% |
| 1d_size | $10,185.50 | +1.86% | $5,101.98 | 1 | 217 | $560.70 | $+188.00 | $-2.50 | 37.0% | 0.0% |
| 3d_top | $9,682.53 | -3.17% | $1,677.97 | 24 | 206 | $438.16 | $-272.15 | $-45.32 | 34.1% | 0.0% |
| 3d_size | $10,512.44 | +5.12% | $1,687.15 | 21 | 195 | $423.79 | $+552.62 | $-40.18 | 32.2% | 0.0% |
| 1w_top | $10,491.08 | +4.91% | $2,039.11 | 40 | 154 | $310.21 | $+534.44 | $-43.36 | 29.8% | 2.5% |
| 1w_size | $10,642.79 | +6.43% | $2,180.83 | 34 | 158 | $308.55 | $+681.07 | $-38.27 | 33.9% | 2.9% |
| 2w_top | $10,768.28 | +7.68% | $725.75 | 18 | 64 | $127.49 | $+762.07 | $+6.20 | 52.2% | 5.6% |
| 2w_size | $11,037.69 | +10.38% | $5,430.26 | 24 | 82 | $141.85 | $+1,064.16 | $-26.46 | 62.1% | 0.0% |
| 1m_top | $9,962.86 | -0.37% | $60.69 | 27 | 27 | $35.99 | $+0.00 | $-37.14 | — | 0.0% |
| 1m_size | $9,965.95 | -0.34% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $-34.05 | — | 0.0% |

Equity curves + positions: `dashboard/index.html`
