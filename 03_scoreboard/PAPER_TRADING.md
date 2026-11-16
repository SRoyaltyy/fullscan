# Paper trading — Futubull-fee simulation

As of **2026-11-16** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-16** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,893.43 | -11.07% | $78.18 | 5 | 607 | $1,297.14 | $-1,173.76 | $+67.19 | 35.2% | 20.0% |
| 1d_size | $9,587.40 | -4.13% | $14.33 | 5 | 545 | $1,291.32 | $-485.59 | $+72.99 | 38.5% | 20.0% |
| 3d_top | $9,091.02 | -9.09% | $193.83 | 16 | 616 | $1,186.32 | $-881.91 | $-27.07 | 32.7% | 12.5% |
| 3d_size | $9,319.04 | -6.81% | $123.91 | 13 | 575 | $1,143.57 | $-656.32 | $-24.64 | 33.5% | 0.0% |
| 1w_top | $9,497.67 | -5.02% | $177.77 | 13 | 509 | $949.26 | $-299.47 | $-202.87 | 27.8% | 30.8% |
| 1w_size | $8,899.71 | -11.00% | $232.88 | 18 | 502 | $919.74 | $-559.38 | $-540.91 | 29.8% | 16.7% |
| 2w_top | $10,258.36 | +2.58% | $58.81 | 21 | 337 | $497.19 | $-37.00 | $+295.36 | 39.9% | 42.9% |
| 2w_size | $10,290.44 | +2.90% | $56.40 | 21 | 351 | $529.99 | $+235.31 | $+55.13 | 38.2% | 38.1% |
| 1m_top | $9,433.50 | -5.67% | $48.18 | 22 | 264 | $277.23 | $-833.17 | $+266.66 | 38.8% | 40.9% |
| 1m_size | $10,112.14 | +1.12% | $45.22 | 16 | 224 | $322.36 | $-63.68 | $+175.82 | 39.4% | 50.0% |

Equity curves + positions: `dashboard/index.html`
