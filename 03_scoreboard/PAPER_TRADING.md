# Paper trading — Futubull-fee simulation

As of **2026-11-25** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-25** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,444.84 | -15.55% | $711.46 | 8 | 686 | $1,470.28 | $-1,536.76 | $-18.40 | 35.7% | 0.0% |
| 1d_size | $8,921.19 | -10.79% | $752.68 | 7 | 613 | $1,446.39 | $-1,061.52 | $-17.29 | 38.3% | 0.0% |
| 3d_top | $8,808.13 | -11.92% | $289.56 | 14 | 688 | $1,292.79 | $-1,211.04 | $+19.18 | 33.5% | 28.6% |
| 3d_size | $9,187.61 | -8.12% | $138.61 | 12 | 638 | $1,234.42 | $-789.83 | $-22.56 | 34.5% | 8.3% |
| 1w_top | $9,216.56 | -7.83% | $145.46 | 17 | 549 | $994.44 | $-830.24 | $+46.81 | 27.4% | 76.5% |
| 1w_size | $8,990.95 | -10.09% | $188.86 | 10 | 548 | $960.38 | $-1,435.26 | $+426.21 | 29.7% | 70.0% |
| 2w_top | $9,918.93 | -0.81% | $112.90 | 22 | 378 | $554.31 | $-262.68 | $+181.61 | 37.6% | 72.7% |
| 2w_size | $10,248.30 | +2.48% | $69.65 | 19 | 389 | $571.19 | $-98.07 | $+346.36 | 36.2% | 68.4% |
| 1m_top | $9,032.65 | -9.67% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $-134.18 | 38.8% | 21.7% |
| 1m_size | $10,037.09 | +0.37% | $35.94 | 18 | 226 | $322.46 | $-63.68 | $+100.77 | 39.4% | 38.9% |

Equity curves + positions: `dashboard/index.html`
