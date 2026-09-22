# Paper trading — Futubull-fee simulation

As of **2026-09-22** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-22** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,752.72 | -2.47% | $3,591.47 | 6 | 280 | $613.54 | $-233.92 | $-13.37 | 35.0% | 0.0% |
| 1d_size | $10,217.78 | +2.18% | $3,508.75 | 5 | 267 | $679.87 | $+228.01 | $-10.23 | 35.9% | 0.0% |
| 3d_top | $9,535.48 | -4.65% | $1,793.22 | 26 | 278 | $568.44 | $-417.57 | $-46.95 | 30.2% | 0.0% |
| 3d_size | $10,219.46 | +2.19% | $1,132.53 | 27 | 277 | $579.23 | $+270.41 | $-50.95 | 28.8% | 0.0% |
| 1w_top | $10,108.73 | +1.09% | $1,338.44 | 41 | 221 | $428.20 | $+178.93 | $-70.20 | 23.3% | 2.4% |
| 1w_size | $10,249.14 | +2.49% | $884.36 | 43 | 231 | $441.51 | $+328.17 | $-79.03 | 25.5% | 2.3% |
| 2w_top | $9,987.10 | -0.13% | $284.48 | 43 | 97 | $181.35 | $+49.31 | $-62.20 | 48.1% | 2.3% |
| 2w_size | $10,978.65 | +9.79% | $215.76 | 48 | 122 | $187.74 | $+1,044.26 | $-65.61 | 48.6% | 4.2% |
| 1m_top | $9,646.93 | -3.53% | $333.91 | 50 | 68 | $99.36 | $-291.21 | $-61.86 | 33.3% | 2.0% |
| 1m_size | $10,690.21 | +6.90% | $151.68 | 43 | 73 | $122.45 | $+744.36 | $-54.14 | 53.3% | 4.7% |

Equity curves + positions: `dashboard/index.html`
