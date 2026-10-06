# Paper trading — Futubull-fee simulation

As of **2026-10-06** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-10-06** ranker: **green pile** (137 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,912.89 | -10.87% | $4,924.78 | 10 | 498 | $1,055.65 | $-1,066.88 | $-20.22 | 33.6% | 0.0% |
| 1d_size | $9,572.31 | -4.28% | $5,174.60 | 9 | 455 | $1,064.06 | $-409.35 | $-18.34 | 39.0% | 0.0% |
| 3d_top | $8,895.54 | -11.04% | $2,769.85 | 20 | 508 | $1,028.87 | $-1,098.34 | $-6.12 | 30.3% | 30.0% |
| 3d_size | $9,937.44 | -0.63% | $3,000.44 | 23 | 475 | $988.84 | $-58.92 | $-3.64 | 32.7% | 34.8% |
| 1w_top | $9,542.57 | -4.57% | $2,353.59 | 28 | 434 | $821.52 | $-499.54 | $+42.11 | 23.2% | 42.9% |
| 1w_size | $9,530.86 | -4.69% | $2,308.14 | 31 | 419 | $790.14 | $-532.05 | $+62.91 | 25.3% | 51.6% |
| 2w_top | $9,600.43 | -4.00% | $2,276.01 | 45 | 267 | $405.23 | $-409.94 | $+10.38 | 34.2% | 53.3% |
| 2w_size | $10,431.53 | +4.32% | $1,932.69 | 46 | 280 | $433.60 | $+332.71 | $+98.82 | 34.2% | 58.7% |
| 1m_top | $9,444.16 | -5.56% | $2,703.87 | 65 | 173 | $191.54 | $-510.31 | $-45.53 | 27.8% | 36.9% |
| 1m_size | $10,214.93 | +2.15% | $2,222.71 | 55 | 149 | $231.44 | $+272.74 | $-57.82 | 38.3% | 38.2% |

Equity curves + positions: `dashboard/index.html`
