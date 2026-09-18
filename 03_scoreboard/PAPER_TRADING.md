# Paper trading — Futubull-fee simulation

As of **2026-09-17** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-17** ranker: **green pile** (188 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,609.87 | -3.90% | $1,990.58 | 10 | 272 | $585.61 | $-367.07 | $-23.06 | 33.6% | 0.0% |
| 1d_size | $10,018.68 | +0.19% | $1,396.78 | 10 | 254 | $639.89 | $+41.88 | $-23.20 | 35.2% | 0.0% |
| 3d_top | $9,555.23 | -4.45% | $1,773.19 | 28 | 272 | $562.95 | $-397.10 | $-47.68 | 31.1% | 0.0% |
| 3d_size | $10,228.17 | +2.28% | $1,422.48 | 27 | 269 | $569.31 | $+276.73 | $-48.56 | 28.9% | 0.0% |
| 1w_top | $10,120.83 | +1.21% | $1,452.93 | 41 | 217 | $424.09 | $+191.38 | $-70.55 | 25.0% | 0.0% |
| 1w_size | $10,259.25 | +2.59% | $1,097.65 | 42 | 224 | $431.66 | $+323.35 | $-64.10 | 25.3% | 2.4% |
| 2w_top | $9,981.56 | -0.18% | $342.23 | 40 | 94 | $180.76 | $+49.31 | $-67.75 | 48.1% | 2.5% |
| 2w_size | $10,956.08 | +9.56% | $318.47 | 43 | 117 | $186.69 | $+1,044.26 | $-88.19 | 48.6% | 2.3% |
| 1m_top | $9,636.11 | -3.64% | $401.77 | 47 | 65 | $98.67 | $-291.21 | $-72.68 | 33.3% | 2.1% |
| 1m_size | $10,649.14 | +6.49% | $194.06 | 39 | 69 | $122.01 | $+744.36 | $-95.22 | 53.3% | 2.6% |

Equity curves + positions: `dashboard/index.html`
