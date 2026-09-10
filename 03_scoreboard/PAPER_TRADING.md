# Paper trading — Futubull-fee simulation

As of **2026-09-10** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-10** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,807.28 | -1.93% | $4,904.24 | 1 | 237 | $514.92 | $-190.24 | $-2.48 | 35.6% | 0.0% |
| 1d_size | $10,241.52 | +2.42% | $5,127.92 | 1 | 221 | $571.17 | $+244.02 | $-2.50 | 36.4% | 0.0% |
| 3d_top | $9,813.48 | -1.87% | $1,530.83 | 28 | 212 | $435.63 | $-148.21 | $-38.31 | 34.8% | 3.6% |
| 3d_size | $10,636.56 | +6.37% | $2,764.93 | 24 | 194 | $401.20 | $+668.01 | $-31.46 | 34.1% | 4.2% |
| 1w_top | $10,418.08 | +4.18% | $1,957.51 | 35 | 157 | $307.12 | $+457.71 | $-39.63 | 31.1% | 5.7% |
| 1w_size | $10,563.93 | +5.64% | $2,193.31 | 30 | 160 | $310.43 | $+594.34 | $-30.40 | 35.4% | 3.3% |
| 2w_top | $10,743.95 | +7.44% | $396.51 | 25 | 71 | $130.88 | $+762.07 | $-18.12 | 52.2% | 8.0% |
| 2w_size | $11,064.39 | +10.64% | $2,976.67 | 31 | 89 | $157.33 | $+1,064.16 | $+0.24 | 62.1% | 3.2% |
| 1m_top | $9,963.62 | -0.36% | $39.76 | 27 | 27 | $36.19 | $+0.00 | $-36.38 | — | 7.4% |
| 1m_size | $9,965.76 | -0.34% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $-34.24 | — | 4.3% |

Equity curves + positions: `dashboard/index.html`
