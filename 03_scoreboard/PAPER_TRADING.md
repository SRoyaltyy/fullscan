# Paper trading — Futubull-fee simulation

As of **2026-11-12** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-12** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,134.19 | -8.66% | $54.14 | 4 | 600 | $1,280.48 | $-593.30 | $-272.50 | 35.6% | 25.0% |
| 1d_size | $9,842.47 | -1.58% | $104.20 | 4 | 538 | $1,274.29 | $+130.77 | $-288.30 | 39.0% | 25.0% |
| 3d_top | $9,226.45 | -7.74% | $121.75 | 21 | 591 | $1,144.79 | $-599.71 | $-173.84 | 33.0% | 28.6% |
| 3d_size | $9,511.43 | -4.89% | $96.09 | 19 | 551 | $1,104.65 | $+163.02 | $-651.59 | 34.2% | 15.8% |
| 1w_top | $9,714.87 | -2.85% | $195.81 | 15 | 501 | $948.08 | $-298.36 | $+13.23 | 28.0% | 40.0% |
| 1w_size | $9,224.81 | -7.75% | $273.15 | 18 | 490 | $915.11 | $-554.40 | $-220.79 | 29.7% | 33.3% |
| 2w_top | $10,258.46 | +2.58% | $58.81 | 21 | 337 | $497.19 | $-37.00 | $+295.46 | 39.9% | 38.1% |
| 2w_size | $10,282.02 | +2.82% | $56.40 | 21 | 351 | $529.99 | $+235.31 | $+46.71 | 38.2% | 42.9% |
| 1m_top | $9,431.05 | -5.69% | $48.18 | 22 | 264 | $277.23 | $-833.17 | $+264.22 | 38.8% | 36.4% |
| 1m_size | $10,024.78 | +0.25% | $45.22 | 16 | 224 | $322.36 | $-63.68 | $+88.46 | 39.4% | 43.8% |

Equity curves + positions: `dashboard/index.html`
