# Paper trading — Futubull-fee simulation

As of **2026-10-01** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-10-01** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,027.24 | -9.73% | $4,649.30 | 4 | 510 | $1,116.24 | $-964.44 | $-8.32 | 29.2% | 0.0% |
| 1d_size | $9,578.73 | -4.21% | $5,004.52 | 4 | 464 | $1,110.68 | $-412.93 | $-8.34 | 32.2% | 0.0% |
| 3d_top | $8,567.19 | -14.33% | $2,079.00 | 17 | 515 | $1,019.82 | $-1,402.73 | $-30.08 | 26.9% | 5.9% |
| 3d_size | $9,098.36 | -9.02% | $2,389.58 | 19 | 487 | $1,001.47 | $-870.47 | $-31.17 | 29.9% | 5.3% |
| 1w_top | $9,306.84 | -6.93% | $3,548.88 | 27 | 451 | $847.58 | $-590.25 | $-102.91 | 23.1% | 14.8% |
| 1w_size | $9,432.65 | -5.67% | $2,040.75 | 30 | 436 | $822.26 | $-415.68 | $-151.67 | 26.1% | 10.0% |
| 2w_top | $9,440.44 | -5.60% | $494.01 | 50 | 226 | $346.62 | $-343.58 | $-215.98 | 30.7% | 26.0% |
| 2w_size | $10,493.09 | +4.93% | $3,172.54 | 49 | 259 | $367.56 | $+577.56 | $-84.46 | 34.3% | 18.4% |
| 1m_top | $9,165.00 | -8.35% | $114.50 | 55 | 109 | $127.43 | $-475.65 | $-359.35 | 18.5% | 27.3% |
| 1m_size | $9,930.66 | -0.69% | $97.68 | 46 | 92 | $133.91 | $+743.35 | $-812.69 | 39.1% | 21.7% |

Equity curves + positions: `dashboard/index.html`
