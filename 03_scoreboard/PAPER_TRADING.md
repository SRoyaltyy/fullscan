# Paper trading — Futubull-fee simulation

As of **2026-09-25** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-25** ranker: **green pile** (125 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,484.02 | -5.16% | $5,507.22 | 9 | 435 | $959.69 | $-497.56 | $-18.42 | 31.0% | 0.0% |
| 1d_size | $10,047.84 | +0.48% | $5,877.22 | 8 | 394 | $963.83 | $+64.27 | $-16.43 | 34.2% | 0.0% |
| 3d_top | $8,858.18 | -11.42% | $2,175.24 | 26 | 450 | $899.51 | $-1,080.30 | $-61.52 | 27.8% | 11.5% |
| 3d_size | $9,439.20 | -5.61% | $2,373.84 | 23 | 419 | $867.04 | $-575.30 | $+14.50 | 29.3% | 21.7% |
| 1w_top | $9,554.70 | -4.45% | $3,173.88 | 37 | 389 | $752.43 | $-362.31 | $-82.99 | 22.7% | 13.5% |
| 1w_size | $9,683.61 | -3.16% | $3,337.43 | 35 | 373 | $725.10 | $-283.36 | $-33.02 | 25.4% | 17.1% |
| 2w_top | $9,541.23 | -4.59% | $936.98 | 51 | 201 | $326.04 | $-323.66 | $-135.11 | 30.7% | 19.6% |
| 2w_size | $10,716.71 | +7.17% | $676.47 | 49 | 217 | $313.30 | $+582.22 | $+134.49 | 34.5% | 30.6% |
| 1m_top | $9,211.60 | -7.88% | $135.69 | 56 | 100 | $126.32 | $-472.25 | $-316.14 | 18.2% | 26.8% |
| 1m_size | $10,091.43 | +0.91% | $120.40 | 45 | 89 | $133.64 | $+743.58 | $-652.15 | 40.9% | 31.1% |

Equity curves + positions: `dashboard/index.html`
