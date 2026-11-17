# Paper trading — Futubull-fee simulation

As of **2026-11-16** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-16** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,894.70 | -11.05% | $78.18 | 5 | 607 | $1,297.14 | $-1,173.76 | $+68.46 | 35.2% | 20.0% |
| 1d_size | $9,588.75 | -4.11% | $14.33 | 5 | 545 | $1,291.32 | $-485.59 | $+74.34 | 38.5% | 20.0% |
| 3d_top | $9,087.97 | -9.12% | $193.83 | 16 | 616 | $1,186.32 | $-881.91 | $-30.12 | 32.7% | 12.5% |
| 3d_size | $9,317.26 | -6.83% | $123.91 | 13 | 575 | $1,143.57 | $-656.32 | $-26.42 | 33.5% | 0.0% |
| 1w_top | $9,495.29 | -5.05% | $177.77 | 13 | 509 | $949.26 | $-299.47 | $-205.25 | 27.8% | 30.8% |
| 1w_size | $8,897.81 | -11.02% | $232.88 | 18 | 502 | $919.74 | $-559.38 | $-542.81 | 29.8% | 16.7% |
| 2w_top | $10,259.73 | +2.60% | $58.81 | 21 | 337 | $497.19 | $-37.00 | $+296.73 | 39.9% | 42.9% |
| 2w_size | $10,294.60 | +2.95% | $56.40 | 21 | 351 | $529.99 | $+235.31 | $+59.29 | 38.2% | 38.1% |
| 1m_top | $9,434.74 | -5.65% | $48.18 | 22 | 264 | $277.23 | $-833.17 | $+267.91 | 38.8% | 40.9% |
| 1m_size | $10,115.97 | +1.16% | $45.22 | 16 | 224 | $322.36 | $-63.68 | $+179.65 | 39.4% | 50.0% |

Equity curves + positions: `dashboard/index.html`
