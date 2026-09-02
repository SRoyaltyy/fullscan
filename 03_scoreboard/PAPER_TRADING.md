# Paper trading — Futubull-fee simulation

As of **2026-09-02** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-02** ranker: **green pile** (138 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,079.80 | +0.80% | $773.32 | 10 | 146 | $323.30 | $+101.34 | $-21.54 | 45.6% | 0.0% |
| 1d_size | $10,622.36 | +6.22% | $43.36 | 4 | 146 | $405.83 | $+630.89 | $-8.54 | 50.7% | 0.0% |
| 3d_top | $10,120.62 | +1.21% | $101.55 | 10 | 108 | $241.28 | $-7.04 | $+127.66 | 42.9% | 70.0% |
| 3d_size | $10,930.65 | +9.31% | $125.87 | 14 | 104 | $229.75 | $+792.07 | $+138.57 | 44.4% | 57.1% |
| 1w_top | $10,733.10 | +7.33% | $210.47 | 17 | 91 | $205.66 | $+725.90 | $+7.20 | 37.8% | 35.3% |
| 1w_size | $10,898.38 | +8.98% | $187.00 | 18 | 96 | $203.09 | $+892.59 | $+5.80 | 48.7% | 38.9% |
| 2w_top | $10,948.21 | +9.48% | $108.27 | 17 | 47 | $110.14 | $+853.50 | $+94.71 | 66.7% | 64.7% |
| 2w_size | $11,009.63 | +10.10% | $102.95 | 19 | 57 | $94.84 | $+944.46 | $+65.17 | 68.4% | 42.1% |
| 1m_top | $9,841.47 | -1.59% | $59.81 | 23 | 23 | $36.09 | $+0.00 | $-158.53 | — | 39.1% |
| 1m_size | $10,681.28 | +6.81% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+681.28 | — | 40.9% |

Equity curves + positions: `dashboard/index.html`
