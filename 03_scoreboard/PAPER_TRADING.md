# Paper trading — Futubull-fee simulation

As of **2026-09-23** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-23** ranker: **green pile** (168 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,597.25 | -4.03% | $4,366.28 | 10 | 402 | $888.71 | $-381.77 | $-20.98 | 31.6% | 0.0% |
| 1d_size | $10,100.95 | +1.01% | $5,169.05 | 9 | 363 | $896.74 | $+119.62 | $-18.67 | 34.5% | 0.0% |
| 3d_top | $8,900.19 | -11.00% | $3,832.62 | 23 | 419 | $854.20 | $-1,065.03 | $-34.78 | 28.8% | 8.7% |
| 3d_size | $9,463.95 | -5.36% | $3,983.04 | 21 | 391 | $818.21 | $-512.20 | $-23.84 | 30.8% | 9.5% |
| 1w_top | $9,705.47 | -2.95% | $1,523.52 | 37 | 357 | $688.60 | $-413.85 | $+119.32 | 21.2% | 27.0% |
| 1w_size | $9,816.65 | -1.83% | $1,569.34 | 34 | 344 | $669.18 | $-293.70 | $+110.35 | 23.9% | 26.5% |
| 2w_top | $9,655.37 | -3.45% | $2,422.83 | 40 | 184 | $307.85 | $-301.94 | $-42.69 | 31.9% | 22.5% |
| 2w_size | $10,834.06 | +8.34% | $947.56 | 46 | 202 | $304.26 | $+603.62 | $+230.44 | 34.6% | 34.8% |
| 1m_top | $9,298.30 | -7.02% | $141.23 | 55 | 97 | $126.04 | $-471.34 | $-230.36 | 19.0% | 30.9% |
| 1m_size | $10,238.84 | +2.39% | $120.40 | 45 | 89 | $133.64 | $+743.58 | $-504.74 | 40.9% | 33.3% |

Equity curves + positions: `dashboard/index.html`
