# Paper trading — Futubull-fee simulation

As of **2026-11-09** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-09** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,490.71 | -5.09% | $69.54 | 7 | 549 | $1,166.23 | $-539.76 | $+30.47 | 34.3% | 14.3% |
| 1d_size | $10,157.26 | +1.57% | $29.95 | 4 | 494 | $1,158.14 | $+105.00 | $+52.25 | 38.8% | 25.0% |
| 3d_top | $9,470.70 | -5.29% | $88.87 | 19 | 551 | $1,099.80 | $-483.77 | $-45.53 | 33.5% | 31.6% |
| 3d_size | $10,149.47 | +1.49% | $73.06 | 15 | 517 | $1,063.86 | $+189.21 | $-39.74 | 34.3% | 40.0% |
| 1w_top | $9,744.66 | -2.55% | $70.74 | 15 | 481 | $905.28 | $-228.93 | $-26.41 | 26.2% | 40.0% |
| 1w_size | $9,487.47 | -5.13% | $73.44 | 15 | 469 | $875.09 | $-469.63 | $-42.90 | 28.6% | 40.0% |
| 2w_top | $10,109.58 | +1.10% | $67.88 | 19 | 335 | $497.10 | $-37.00 | $+146.58 | 39.9% | 42.1% |
| 2w_size | $10,101.80 | +1.02% | $99.94 | 16 | 346 | $529.54 | $+235.31 | $-133.51 | 38.2% | 31.2% |
| 1m_top | $9,295.69 | -7.04% | $57.25 | 20 | 262 | $277.13 | $-833.17 | $+128.86 | 38.8% | 40.0% |
| 1m_size | $9,901.91 | -0.98% | $70.22 | 12 | 220 | $322.10 | $-63.68 | $-34.41 | 39.4% | 33.3% |

Equity curves + positions: `dashboard/index.html`
