# Paper trading — Futubull-fee simulation

As of **2026-09-14** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-14** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,601.94 | -3.98% | $9,601.94 | 0 | 268 | $592.43 | $-398.06 | $+0.00 | 32.8% | — |
| 1d_size | $10,012.04 | +0.12% | $10,012.04 | 0 | 248 | $646.53 | $+12.04 | $+0.00 | 34.7% | — |
| 3d_top | $9,560.68 | -4.39% | $1,748.21 | 24 | 266 | $557.52 | $-394.14 | $-45.18 | 30.6% | 0.0% |
| 3d_size | $10,286.96 | +2.87% | $1,112.49 | 25 | 263 | $562.20 | $+336.09 | $-49.12 | 26.9% | 0.0% |
| 1w_top | $10,147.80 | +1.48% | $1,387.17 | 37 | 215 | $425.20 | $+194.52 | $-46.72 | 24.7% | 10.8% |
| 1w_size | $10,302.61 | +3.03% | $1,311.16 | 37 | 221 | $433.61 | $+363.97 | $-61.36 | 25.0% | 8.1% |
| 2w_top | $10,247.15 | +2.47% | $461.67 | 39 | 93 | $175.52 | $+281.87 | $-34.71 | 48.1% | 25.6% |
| 2w_size | $10,969.92 | +9.70% | $338.31 | 41 | 117 | $186.62 | $+1,042.61 | $-72.69 | 47.4% | 19.5% |
| 1m_top | $9,658.76 | -3.41% | $449.15 | 45 | 65 | $98.63 | $-293.19 | $-48.05 | 30.0% | 17.8% |
| 1m_size | $10,680.38 | +6.80% | $211.20 | 37 | 69 | $121.91 | $+743.81 | $-63.43 | 50.0% | 16.2% |

Equity curves + positions: `dashboard/index.html`
