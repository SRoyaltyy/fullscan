# Paper trading — Futubull-fee simulation

As of **2026-09-24** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-24** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,505.55 | -4.94% | $4,974.03 | 7 | 419 | $925.48 | $-478.88 | $-15.56 | 30.1% | 0.0% |
| 1d_size | $10,008.10 | +0.08% | $5,082.58 | 7 | 379 | $931.34 | $+23.86 | $-15.76 | 32.8% | 0.0% |
| 3d_top | $8,896.86 | -11.03% | $2,325.30 | 26 | 432 | $873.06 | $-1,040.35 | $-62.79 | 28.1% | 3.8% |
| 3d_size | $9,386.69 | -6.13% | $2,516.82 | 26 | 404 | $840.08 | $-545.18 | $-68.13 | 30.2% | 0.0% |
| 1w_top | $9,622.04 | -3.78% | $1,966.48 | 37 | 373 | $719.63 | $-386.64 | $+8.68 | 22.0% | 10.8% |
| 1w_size | $9,721.15 | -2.79% | $1,893.75 | 36 | 360 | $698.56 | $-275.24 | $-3.61 | 24.7% | 11.1% |
| 2w_top | $9,565.08 | -4.35% | $1,356.67 | 45 | 195 | $321.81 | $-324.06 | $-110.86 | 30.7% | 8.9% |
| 2w_size | $10,785.79 | +7.86% | $842.58 | 46 | 214 | $311.78 | $+590.04 | $+195.76 | 34.5% | 23.9% |
| 1m_top | $9,252.95 | -7.47% | $142.62 | 55 | 99 | $126.24 | $-472.27 | $-274.78 | 18.2% | 23.6% |
| 1m_size | $10,108.12 | +1.08% | $120.40 | 45 | 89 | $133.64 | $+743.58 | $-635.46 | 40.9% | 28.9% |

Equity curves + positions: `dashboard/index.html`
