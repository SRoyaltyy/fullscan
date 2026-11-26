# Paper trading — Futubull-fee simulation

As of **2026-11-26** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-26** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,409.49 | -15.91% | $84.20 | 8 | 700 | $1,505.64 | $-1,569.63 | $-20.89 | 35.0% | 0.0% |
| 1d_size | $8,886.62 | -11.13% | $137.44 | 8 | 626 | $1,480.97 | $-1,092.14 | $-21.24 | 37.5% | 0.0% |
| 3d_top | $8,792.78 | -12.07% | $38.82 | 16 | 700 | $1,308.15 | $-1,176.24 | $-30.98 | 33.9% | 0.0% |
| 3d_size | $9,185.51 | -8.14% | $100.10 | 12 | 644 | $1,236.53 | $-792.32 | $-22.17 | 34.2% | 0.0% |
| 1w_top | $9,178.60 | -8.21% | $106.21 | 17 | 567 | $1,032.41 | $-809.09 | $-12.31 | 28.4% | 41.2% |
| 1w_size | $8,959.92 | -10.40% | $137.96 | 13 | 561 | $991.41 | $-1,025.47 | $-14.60 | 31.0% | 15.4% |
| 2w_top | $9,918.79 | -0.81% | $108.30 | 22 | 380 | $554.45 | $-263.19 | $+181.99 | 37.4% | 72.7% |
| 2w_size | $10,248.09 | +2.48% | $59.52 | 20 | 392 | $571.39 | $-98.59 | $+346.68 | 36.0% | 65.0% |
| 1m_top | $9,032.65 | -9.67% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-134.18 | 38.8% | 21.7% |
| 1m_size | $10,037.06 | +0.37% | $33.18 | 19 | 227 | $322.49 | $-63.68 | $+100.74 | 39.4% | 36.8% |

Equity curves + positions: `dashboard/index.html`
