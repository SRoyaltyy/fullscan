# Paper trading — Futubull-fee simulation

As of **2026-11-11** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-11** ranker: **green pile** (317 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,618.00 | -3.82% | $276.88 | 10 | 586 | $1,248.58 | $-360.73 | $-21.27 | 36.5% | 0.0% |
| 1d_size | $10,452.80 | +4.53% | $186.82 | 9 | 525 | $1,239.86 | $+476.04 | $-23.24 | 39.9% | 0.0% |
| 3d_top | $9,465.39 | -5.35% | $215.46 | 20 | 580 | $1,142.49 | $-595.83 | $+61.21 | 33.6% | 45.0% |
| 3d_size | $10,126.97 | +1.27% | $134.29 | 17 | 545 | $1,103.85 | $+163.52 | $-36.55 | 34.5% | 35.3% |
| 1w_top | $9,823.24 | -1.77% | $65.59 | 16 | 482 | $905.33 | $-228.93 | $+52.17 | 26.2% | 62.5% |
| 1w_size | $9,589.73 | -4.10% | $47.20 | 18 | 472 | $875.37 | $-469.63 | $+59.36 | 28.6% | 55.6% |
| 2w_top | $10,183.30 | +1.83% | $62.73 | 20 | 336 | $497.15 | $-37.00 | $+220.29 | 39.9% | 55.0% |
| 2w_size | $10,316.31 | +3.16% | $60.32 | 20 | 350 | $529.95 | $+235.31 | $+81.00 | 38.2% | 50.0% |
| 1m_top | $9,363.75 | -6.36% | $52.10 | 21 | 263 | $277.19 | $-833.17 | $+196.92 | 38.8% | 52.4% |
| 1m_size | $10,009.59 | +0.10% | $49.14 | 15 | 223 | $322.32 | $-63.68 | $+73.27 | 39.4% | 53.3% |

Equity curves + positions: `dashboard/index.html`
