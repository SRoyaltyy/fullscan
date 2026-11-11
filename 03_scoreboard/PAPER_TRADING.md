# Paper trading — Futubull-fee simulation

As of **2026-11-09** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-09** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,651.23 | -3.49% | $69.54 | 7 | 549 | $1,166.23 | $-539.76 | $+190.99 | 34.3% | 71.4% |
| 1d_size | $10,524.70 | +5.25% | $29.95 | 4 | 494 | $1,158.14 | $+105.00 | $+419.69 | 38.8% | 75.0% |
| 3d_top | $9,424.88 | -5.75% | $88.87 | 19 | 551 | $1,099.80 | $-483.77 | $-91.35 | 33.5% | 36.8% |
| 3d_size | $10,183.21 | +1.83% | $73.06 | 15 | 517 | $1,063.86 | $+189.21 | $-6.00 | 34.3% | 53.3% |
| 1w_top | $9,743.13 | -2.57% | $70.74 | 15 | 481 | $905.28 | $-228.93 | $-27.94 | 26.2% | 40.0% |
| 1w_size | $9,518.35 | -4.82% | $73.44 | 15 | 469 | $875.09 | $-469.63 | $-12.02 | 28.6% | 53.3% |
| 2w_top | $10,119.80 | +1.20% | $67.88 | 19 | 335 | $497.10 | $-37.00 | $+156.80 | 39.9% | 42.1% |
| 2w_size | $10,209.85 | +2.10% | $99.94 | 16 | 346 | $529.54 | $+235.31 | $-25.46 | 38.2% | 50.0% |
| 1m_top | $9,304.99 | -6.95% | $57.25 | 20 | 262 | $277.13 | $-833.17 | $+138.16 | 38.8% | 40.0% |
| 1m_size | $9,935.94 | -0.64% | $70.22 | 12 | 220 | $322.10 | $-63.68 | $-0.38 | 39.4% | 50.0% |

Equity curves + positions: `dashboard/index.html`
