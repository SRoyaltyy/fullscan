# Paper trading — Futubull-fee simulation

As of **2026-11-12** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-12** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,396.29 | -6.04% | $54.14 | 4 | 600 | $1,280.48 | $-593.30 | $-10.41 | 35.6% | 0.0% |
| 1d_size | $10,119.86 | +1.20% | $104.20 | 4 | 538 | $1,274.29 | $+130.77 | $-10.90 | 39.0% | 0.0% |
| 3d_top | $9,184.10 | -8.16% | $121.75 | 21 | 591 | $1,144.79 | $-599.71 | $-216.19 | 33.0% | 23.8% |
| 3d_size | $9,662.66 | -3.37% | $96.09 | 19 | 551 | $1,104.65 | $+163.02 | $-500.36 | 34.2% | 10.5% |
| 1w_top | $9,679.32 | -3.21% | $195.81 | 15 | 501 | $948.08 | $-298.36 | $-22.32 | 28.0% | 6.7% |
| 1w_size | $9,421.40 | -5.79% | $273.15 | 18 | 490 | $915.11 | $-554.40 | $-24.20 | 29.7% | 5.6% |
| 2w_top | $10,067.44 | +0.67% | $58.81 | 21 | 337 | $497.19 | $-37.00 | $+104.43 | 39.9% | 38.1% |
| 2w_size | $10,138.85 | +1.39% | $56.40 | 21 | 351 | $529.99 | $+235.31 | $-96.46 | 38.2% | 28.6% |
| 1m_top | $9,257.69 | -7.42% | $48.18 | 22 | 264 | $277.23 | $-833.17 | $+90.85 | 38.8% | 36.4% |
| 1m_size | $9,874.81 | -1.25% | $45.22 | 16 | 224 | $322.36 | $-63.68 | $-61.51 | 39.4% | 31.2% |

Equity curves + positions: `dashboard/index.html`
