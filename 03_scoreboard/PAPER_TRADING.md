# Paper trading — Futubull-fee simulation

As of **2026-08-17** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $10,150.16 | +1.50% | $90.15 | 10 | 18 | $60.40 | $+111.59 | 25.0% |
| 1d_size | $10,217.97 | +2.18% | $242.21 | 9 | 41 | $121.42 | $+173.07 | 43.8% |
| 3d_top | $10,113.30 | +1.13% | $464.05 | 10 | 18 | $64.29 | $+137.43 | 25.0% |
| 3d_size | $10,021.16 | +0.21% | $221.30 | 9 | 45 | $167.73 | $+46.03 | 33.3% |
| 1w_top | $10,123.07 | +1.23% | $119.44 | 10 | 16 | $61.87 | $+148.81 | 33.3% |
| 1w_size | $10,282.28 | +2.82% | $217.79 | 9 | 45 | $134.01 | $+307.57 | 50.0% |
| 2w_top | $10,085.72 | +0.86% | $190.07 | 10 | 14 | $57.69 | $+110.86 | 50.0% |
| 2w_size | $10,208.44 | +2.08% | $238.21 | 9 | 45 | $149.27 | $+233.61 | 50.0% |
| 1m_top | $9,939.78 | -0.60% | $245.85 | 10 | 14 | $45.76 | $-35.21 | 0.0% |
| 1m_size | $9,928.98 | -0.71% | $187.74 | 9 | 43 | $178.18 | $-46.92 | 41.2% |

Equity curves + positions: `dashboard/index.html`
