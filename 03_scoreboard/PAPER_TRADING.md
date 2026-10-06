# Paper trading — Futubull-fee simulation

As of **2026-09-08** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-08** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,602.95 | -3.97% | $4,916.29 | 1 | 233 | $505.22 | $-208.28 | $-188.78 | 35.3% | 0.0% |
| 1d_size | $9,991.15 | -0.09% | $5,101.98 | 1 | 217 | $560.70 | $+188.00 | $-196.85 | 37.0% | 0.0% |
| 3d_top | $9,417.65 | -5.82% | $1,677.97 | 24 | 206 | $438.16 | $-272.15 | $-310.20 | 34.1% | 25.0% |
| 3d_size | $10,096.32 | +0.96% | $1,687.15 | 21 | 195 | $423.79 | $+552.62 | $-456.30 | 32.2% | 23.8% |
| 1w_top | $10,293.88 | +2.94% | $2,039.11 | 40 | 154 | $310.21 | $+534.44 | $-240.56 | 29.8% | 30.0% |
| 1w_size | $10,545.51 | +5.46% | $2,180.83 | 34 | 158 | $308.55 | $+681.07 | $-135.55 | 33.9% | 35.3% |
| 2w_top | $9,412.80 | -5.87% | $725.75 | 18 | 64 | $127.49 | $+762.07 | $-1,349.28 | 52.2% | 27.8% |
| 2w_size | $10,805.33 | +8.05% | $5,430.26 | 24 | 82 | $141.85 | $+1,064.16 | $-258.82 | 62.1% | 20.8% |
| 1m_top | $9,065.87 | -9.34% | $60.69 | 27 | 27 | $35.99 | $+0.00 | $-934.13 | — | 22.2% |
| 1m_size | $10,458.46 | +4.58% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $+458.46 | — | 30.4% |

Equity curves + positions: `dashboard/index.html`
