# Paper trading — Futubull-fee simulation

As of **2026-11-12** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-12** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,388.79 | -6.11% | $54.14 | 4 | 600 | $1,280.48 | $-593.30 | $-17.90 | 35.6% | 0.0% |
| 1d_size | $10,111.79 | +1.12% | $104.20 | 4 | 538 | $1,274.29 | $+130.77 | $-18.98 | 39.0% | 0.0% |
| 3d_top | $9,183.45 | -8.17% | $121.75 | 21 | 591 | $1,144.79 | $-599.71 | $-216.85 | 33.0% | 23.8% |
| 3d_size | $9,662.74 | -3.37% | $96.09 | 19 | 551 | $1,104.65 | $+163.02 | $-500.28 | 34.2% | 10.5% |
| 1w_top | $9,679.60 | -3.20% | $195.81 | 15 | 501 | $948.08 | $-298.36 | $-22.04 | 28.0% | 13.3% |
| 1w_size | $9,420.90 | -5.79% | $273.15 | 18 | 490 | $915.11 | $-554.40 | $-24.70 | 29.7% | 5.6% |
| 2w_top | $10,066.33 | +0.66% | $58.81 | 21 | 337 | $497.19 | $-37.00 | $+103.32 | 39.9% | 38.1% |
| 2w_size | $10,138.11 | +1.38% | $56.40 | 21 | 351 | $529.99 | $+235.31 | $-97.20 | 38.2% | 28.6% |
| 1m_top | $9,256.67 | -7.43% | $48.18 | 22 | 264 | $277.23 | $-833.17 | $+89.83 | 38.8% | 36.4% |
| 1m_size | $9,873.84 | -1.26% | $45.22 | 16 | 224 | $322.36 | $-63.68 | $-62.48 | 39.4% | 31.2% |

Equity curves + positions: `dashboard/index.html`
