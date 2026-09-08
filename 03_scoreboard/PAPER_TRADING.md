# Paper trading — Futubull-fee simulation

As of **2026-09-08** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-08** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,820.89 | -1.79% | $6,640.34 | 2 | 230 | $499.42 | $-174.92 | $-4.18 | 36.0% | 0.0% |
| 1d_size | $10,372.83 | +3.73% | $5,304.63 | 2 | 214 | $555.06 | $+377.14 | $-4.30 | 36.8% | 0.0% |
| 3d_top | $9,876.07 | -1.24% | $1,500.26 | 23 | 193 | $399.24 | $-101.00 | $-22.93 | 36.5% | 8.7% |
| 3d_size | $10,702.75 | +7.03% | $1,535.21 | 21 | 177 | $366.83 | $+763.44 | $-60.69 | 35.9% | 4.8% |
| 1w_top | $10,524.87 | +5.25% | $2,496.57 | 33 | 137 | $266.82 | $+586.84 | $-61.97 | 34.6% | 9.1% |
| 1w_size | $10,697.85 | +6.98% | $1,993.36 | 28 | 140 | $266.48 | $+757.70 | $-59.85 | 39.3% | 3.6% |
| 2w_top | $10,588.15 | +5.88% | $845.32 | 15 | 59 | $122.29 | $+774.27 | $-186.12 | 50.0% | 13.3% |
| 2w_size | $10,757.08 | +7.57% | $3,330.45 | 25 | 73 | $119.03 | $+780.66 | $-23.59 | 58.3% | 16.0% |
| 1m_top | $10,083.56 | +0.84% | $35.78 | 28 | 28 | $36.35 | $+0.00 | $+83.56 | — | 42.9% |
| 1m_size | $10,370.85 | +3.71% | $22.96 | 23 | 23 | $34.05 | $+0.00 | $+370.85 | — | 30.4% |

Equity curves + positions: `dashboard/index.html`
