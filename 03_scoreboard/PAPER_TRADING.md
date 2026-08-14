# Paper trading — Futubull-fee simulation

As of **2026-08-14** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $9,929.06 | -0.71% | $245.92 | 10 | 14 | $70.94 | $-31.12 | 0.0% |
| 1d_size | $9,889.14 | -1.11% | $130.96 | 9 | 27 | $110.86 | $-63.45 | 0.0% |
| 3d_top | $9,869.14 | -1.31% | $1.98 | 10 | 14 | $130.86 | $-31.12 | 0.0% |
| 3d_size | $9,881.93 | -1.18% | $0.68 | 9 | 27 | $118.07 | $-63.68 | 0.0% |
| 1w_top | $9,906.06 | -0.94% | $2.98 | 10 | 14 | $93.94 | $-31.12 | 0.0% |
| 1w_size | $9,902.03 | -0.98% | $4.81 | 9 | 27 | $97.97 | $-64.14 | 0.0% |
| 2w_top | $9,931.28 | -0.69% | $1.93 | 10 | 12 | $68.72 | $-26.86 | 0.0% |
| 2w_size | $9,885.62 | -1.14% | $0.60 | 9 | 27 | $114.38 | $-63.99 | 0.0% |
| 1m_top | $9,902.19 | -0.98% | $1.76 | 10 | 12 | $97.81 | $-4.26 | 0.0% |
| 1m_size | $9,897.86 | -1.02% | $6.48 | 9 | 27 | $102.14 | $-63.49 | 0.0% |

Equity curves + positions: `dashboard/index.html`
