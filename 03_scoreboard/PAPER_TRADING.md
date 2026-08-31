# Paper trading — Futubull-fee simulation

As of **2026-08-31** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-08-31** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,090.48 | +0.90% | $103.59 | 10 | 116 | $264.09 | $+111.48 | $-21.00 | 37.7% | 0.0% |
| 1d_size | $10,758.67 | +7.59% | $172.19 | 9 | 139 | $393.49 | $+777.96 | $-19.29 | 43.1% | 0.0% |
| 3d_top | $10,072.90 | +0.73% | $95.16 | 19 | 103 | $227.34 | $+103.40 | $-30.50 | 45.2% | 0.0% |
| 3d_size | $11,080.21 | +10.80% | $226.62 | 16 | 90 | $205.68 | $+1,114.86 | $-34.65 | 48.6% | 0.0% |
| 1w_top | $10,828.53 | +8.29% | $117.24 | 17 | 75 | $168.98 | $+859.24 | $-30.71 | 37.9% | 0.0% |
| 1w_size | $10,889.35 | +8.89% | $183.54 | 17 | 81 | $170.01 | $+926.47 | $-37.12 | 50.0% | 0.0% |
| 2w_top | $11,011.19 | +10.11% | $123.08 | 23 | 49 | $122.67 | $+1,052.36 | $-41.16 | 69.2% | 4.3% |
| 2w_size | $10,925.40 | +9.25% | $189.16 | 19 | 51 | $93.52 | $+950.45 | $-25.05 | 68.8% | 5.3% |
| 1m_top | $10,444.09 | +4.44% | $110.08 | 21 | 21 | $35.47 | $+0.00 | $+444.09 | — | 47.6% |
| 1m_size | $11,075.13 | +10.75% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+1,075.13 | — | 68.2% |

Equity curves + positions: `dashboard/index.html`
