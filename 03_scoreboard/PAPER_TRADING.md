# Paper trading — Futubull-fee simulation

As of **2026-11-19** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-19** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,212.75 | -17.87% | $425.80 | 8 | 642 | $1,376.78 | $-1,770.69 | $-16.55 | 34.1% | 0.0% |
| 1d_size | $8,828.71 | -11.71% | $376.97 | 7 | 577 | $1,368.32 | $-1,156.69 | $-14.60 | 37.2% | 0.0% |
| 3d_top | $8,739.86 | -12.60% | $171.59 | 17 | 643 | $1,219.25 | $-1,336.29 | $+76.15 | 31.6% | 17.6% |
| 3d_size | $8,913.86 | -10.86% | $132.52 | 13 | 599 | $1,174.77 | $-1,189.71 | $+103.58 | 32.4% | 7.7% |
| 1w_top | $9,151.44 | -8.49% | $345.85 | 17 | 533 | $989.89 | $-821.55 | $-27.01 | 27.5% | 0.0% |
| 1w_size | $8,558.52 | -14.41% | $104.86 | 20 | 528 | $953.17 | $-1,413.61 | $-27.87 | 29.1% | 15.0% |
| 2w_top | $9,750.69 | -2.49% | $223.20 | 22 | 354 | $531.47 | $-382.70 | $+133.39 | 39.2% | 9.1% |
| 2w_size | $9,891.27 | -1.09% | $206.97 | 21 | 369 | $566.46 | $-70.16 | $-38.57 | 37.9% | 0.0% |
| 1m_top | $8,997.40 | -10.03% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-169.43 | 38.8% | 17.4% |
| 1m_size | $9,734.06 | -2.66% | $39.16 | 17 | 225 | $322.43 | $-63.68 | $-202.26 | 39.4% | 23.5% |

Equity curves + positions: `dashboard/index.html`
