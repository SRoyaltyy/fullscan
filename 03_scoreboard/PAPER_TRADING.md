# Paper trading — Futubull-fee simulation

As of **2026-09-10** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-10** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,807.28 | -1.93% | $4,904.24 | 1 | 237 | $514.92 | $-190.24 | $-2.48 | 35.6% | 0.0% |
| 1d_size | $10,241.52 | +2.42% | $5,127.92 | 1 | 221 | $571.17 | $+244.02 | $-2.50 | 36.4% | 0.0% |
| 3d_top | $9,823.10 | -1.77% | $1,963.52 | 25 | 207 | $426.02 | $-159.61 | $-17.29 | 34.1% | 8.0% |
| 3d_size | $10,643.42 | +6.43% | $2,748.80 | 23 | 191 | $394.34 | $+650.72 | $-7.30 | 33.3% | 8.7% |
| 1w_top | $10,426.41 | +4.26% | $3,589.57 | 31 | 153 | $298.79 | $+457.71 | $-31.30 | 31.1% | 6.5% |
| 1w_size | $10,568.24 | +5.68% | $3,048.76 | 28 | 158 | $306.12 | $+594.34 | $-26.10 | 35.4% | 3.6% |
| 2w_top | $10,746.41 | +7.46% | $638.25 | 21 | 67 | $128.42 | $+762.07 | $-15.67 | 52.2% | 9.5% |
| 2w_size | $11,068.75 | +10.69% | $3,960.74 | 29 | 87 | $152.97 | $+1,064.16 | $+4.59 | 62.1% | 3.4% |
| 1m_top | $9,963.62 | -0.36% | $39.76 | 27 | 27 | $36.19 | $+0.00 | $-36.38 | — | 7.4% |
| 1m_size | $9,965.76 | -0.34% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $-34.24 | — | 4.3% |

Equity curves + positions: `dashboard/index.html`
