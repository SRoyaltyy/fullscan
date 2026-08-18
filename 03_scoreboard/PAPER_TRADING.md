# Paper trading — Futubull-fee simulation

As of **2026-08-18** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $10,274.69 | +2.75% | $277.88 | 10 | 22 | $68.44 | $+45.50 | 16.7% |
| 1d_size | $10,135.01 | +1.35% | $160.11 | 9 | 59 | $181.67 | $+169.90 | 48.0% |
| 3d_top | $10,112.29 | +1.12% | $329.09 | 10 | 30 | $92.59 | $+77.57 | 30.0% |
| 3d_size | $9,956.02 | -0.44% | $175.65 | 9 | 63 | $225.18 | $-11.82 | 44.4% |
| 1w_top | $9,956.19 | -0.44% | $1,710.83 | 10 | 38 | $112.16 | $-23.69 | 35.7% |
| 1w_size | $10,165.45 | +1.65% | $492.04 | 9 | 63 | $183.51 | $+189.44 | 44.4% |
| 2w_top | $9,955.97 | -0.44% | $1,668.59 | 10 | 36 | $107.45 | $-23.94 | 30.8% |
| 2w_size | $10,102.58 | +1.03% | $462.61 | 9 | 63 | $198.67 | $+126.46 | 48.1% |
| 1m_top | $9,753.55 | -2.46% | $1,929.96 | 10 | 36 | $95.34 | $-226.38 | 30.8% |
| 1m_size | $9,774.07 | -2.26% | $387.50 | 9 | 61 | $226.35 | $-202.30 | 38.5% |

Equity curves + positions: `dashboard/index.html`
