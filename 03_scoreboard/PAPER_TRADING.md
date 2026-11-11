# Paper trading — Futubull-fee simulation

As of **2026-11-10** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-10** ranker: **green pile** (474 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,610.96 | -3.89% | $298.52 | 10 | 566 | $1,206.50 | $-368.44 | $-20.61 | 35.3% | 0.0% |
| 1d_size | $10,485.30 | +4.85% | $324.38 | 9 | 507 | $1,197.54 | $+504.19 | $-18.89 | 39.4% | 0.0% |
| 3d_top | $9,385.00 | -6.15% | $313.88 | 18 | 570 | $1,139.68 | $-594.11 | $-20.89 | 33.7% | 16.7% |
| 3d_size | $10,147.05 | +1.47% | $322.09 | 14 | 534 | $1,100.02 | $+164.93 | $-17.88 | 34.6% | 28.6% |
| 1w_top | $9,743.13 | -2.57% | $70.74 | 15 | 481 | $905.28 | $-228.93 | $-27.94 | 26.2% | 40.0% |
| 1w_size | $9,518.35 | -4.82% | $73.44 | 15 | 469 | $875.09 | $-469.63 | $-12.02 | 28.6% | 53.3% |
| 2w_top | $10,119.80 | +1.20% | $67.88 | 19 | 335 | $497.10 | $-37.00 | $+156.80 | 39.9% | 42.1% |
| 2w_size | $10,209.71 | +2.10% | $86.56 | 17 | 347 | $529.67 | $+235.31 | $-25.60 | 38.2% | 47.1% |
| 1m_top | $9,304.99 | -6.95% | $57.25 | 20 | 262 | $277.13 | $-833.17 | $+138.16 | 38.8% | 40.0% |
| 1m_size | $9,935.94 | -0.64% | $70.22 | 12 | 220 | $322.10 | $-63.68 | $-0.38 | 39.4% | 50.0% |

Equity curves + positions: `dashboard/index.html`
