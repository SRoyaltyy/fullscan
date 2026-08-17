# Sector Outcome — Real Estate — 2026-08-17

Actuals: {'etf': 'XLRE', 'pct': -0.9719430577861288, 'spy_pct': -0.47273666413644166, 'rel': -0.4992063936496871, 'open': 45.099998474121094, 'close': 44.83000183105469}

I now have a comprehensive picture. Let me synthesize the post-session review.

The key finding: On August 17, 2026, there was a **bond selloff** — the 30-year Treasury yield rose to 5.311% (highest since June 2007), and the 10-year rose to 4.73% (+0.03). This is the OPPOSITE of what the morning prediction assumed (that yields were easing). The morning prediction was built on the prior day's data (DFII10 2.39, -0.04 1w) but the actual day saw yields RISE. This is the classic rate-sensitive REIT vulnerability — rising long-end yields crush REIT duration.

The morning prediction said "up/mild" (total score 7.5, notable band in the pipeline). Actual was **down -0.97%** vs SPY -0.47%, relative -0.50%. Direction MISS.

Let me write the review.

---

# Sector Post-Session Review — Real Estate (XLRE)

## Step 0: FACTS
- **XLRE:** -0.97% (open 45.10 → close 44.83)
- **SPY:** -0.47%
- **Relative:** -0.50% (XLRE underperformed SPY)
- **Path:** Opened near 45.10, closed lower at 44.83 — a steady decline through the session, consistent with a rate-driven grind lower.

## Step 1: What drove the sector today

The dominant driver was a **bond selloff / rising long-end Treasury yields** — the exact inverse of the morning's core assumption.

- **30-year Treasury yield rose to 5.311%** — highest since June 2007 (19-year high). Driven by surging government spending, a flood of long-dated bond supply, and sticky inflation.
- **10-year Treasury yield rose to 4.73%** (+0.03 on the day).
- **Oil rose above $81.50** amid US-Iran diplomatic deadlock, adding to inflation/stagflation concerns and pressuring the long end.
- **FOMC minutes due Wednesday** — market pricing showed a **64% chance of a rate HIKE by year-end** (per Schwab), not cuts. This is a hawkish repricing that directly hits REIT duration.

For a long-duration, rate-sensitive sector like REITs, a 19-year high in the 30-year yield is a severe negative. Real estate is the most rate-sensitive sector in the S&P 500, and the long-end move crushed it. XLRE underperformed SPY by -0.50%.

**Taxonomy-aligned factors:**
- **Rates rising / REIT duration headwind** — NEGATIVE HIT (dominant). 30Y at 19-year high, 10Y up.
- **Long-end supply / fiscal concerns** — NEGATIVE (Treasury supply flood, government spending angst).
- **Oil up / geopolitical (US-Iran deadlock)** — NEGATIVE (inflation pressure, stagflation fears).
- **Hawkish Fed repricing** (64% chance of hike by year-end) — NEGATIVE.

## Step 2: Audit morning S0–S4 reads against reality

| Component | Morning read | Reality | Verdict |
|---|---|---|---|
| **S0_SHARED_MACRO** | +1 (real yields easing, CPI cool, duration relief) | **WRONG** — yields ROSE, 30Y at 19-yr high, hawkish repricing | **MISS** |
| **S1_SECTOR_FACTORS** | +1 (duration relief, data-center, earnings upside) | Duration relief did NOT materialize — rates rose. Data-center/earnings positives swamped by rate shock | **MISS** |
| **S2_BREADTH** | +1 (1d/3d/1w rel positive, inflection continuing) | Tape reversed — XLRE underperformed SPY by -0.50% | **MISS** |
| **S3_FLOWS_POSITIONING** | 0 (neutral, +$130M 1m inflows) | Neutral — no evidence of flow reversal | **OK** |
| **S4_ETF_TAPE** | +1 (positive confirmation) | **WRONG** — tape confirmed the DOWN move, not up | **MISS** |

**Direction: MISS** (predicted up, actual down -0.97%).
**Magnitude: MISS** (predicted mild/notable, actual -0.97% is notable on the downside).

## Step 3: Interactions / double-count / knowable-at-open test

**Primary failure: the morning prediction anchored on the prior day's easing-yield data (DFII10 2.39, -0.04 1w) and extrapolated that the duration-relief inflection would continue.** But the actual session saw a sharp bond selloff — 30Y at a 19-year high, 10Y up to 4.73%. The morning read treated the rate backdrop as "turning positive" when in fact the long end was breaking out to multi-decade highs.

**Knowable at open?** **Partially.** The bond selloff and 30Y at 19-year high were developing through the session. However, the pre-market data available at the open (futures flat, oil up, US-Iran deadlock headlines, hawkish Fed repricing at 64% hike probability) contained strong warning signals that the morning analysis underweighted. The morning noted "EPU spiked to 395.85 (+220 1d)" and "oil up slightly" but dismissed these as non-triggering for the 08-11 geopolitical lesson. In hindsight, the oil-up + US-Iran deadlock + hawkish repricing combination was a clear rate-risk signal that should have flipped S0 negative.

**Double-count check:** The morning double-counted the "duration relief" theme across S0, S1, and S4 — all three components leaned on the same easing-yield assumption. When that assumption failed, three components failed simultaneously. This is a concentration risk in the scoring.

**Key interaction:** Rising oil (US-Iran deadlock) + rising long-end yields (Treasury supply, fiscal angst) + hawkish Fed repricing = a stagflation-style rate shock that is maximally toxic for REIT duration. The morning treated oil and yields as separate, benign factors; in reality they compounded.

## Step 4: Outliers inside the sector

- **Data-center REITs** (e.g., Equinix, Digital Realty) likely held up relatively better given hyperscaler capex demand — but even these were dragged down by the rate shock.
- **Long-duration, low-yield REITs** (cell towers, data centers, some net-lease) would be the most rate-sensitive and likely the worst performers.
- **Senior housing / Welltower** had been a leader on the tape; likely gave back some gains but may have outperformed the sector on fundamentals.
- No single-name blowup identified; the move was broad-based rate-driven selling.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.97
SPY_PCT: -0.47
REL_PCT: -0.50
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Bond selloff — 30Y Treasury hit 19-year high (5.31%), 10Y rose to 4.73%, crushing REIT duration; compounded by oil up on US-Iran deadlock and hawkish Fed repricing (64% hike probability by year-end).
KEY_INTERACTION: Rising oil (US-Iran deadlock) + rising long-end yields (Treasury supply/fiscal angst) + hawkish Fed repricing compounded into a stagflation-style rate shock maximally toxic for REIT duration.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: MISS — morning anchored on prior-day easing yields and extrapolated duration relief; actual session saw a sharp long-end bond selloff (30Y at 19-yr high) that flipped the rate backdrop decisively negative. S0/S1/S2/S4 all failed on the same easing-yield assumption (double-counted).
OUTCOME_END