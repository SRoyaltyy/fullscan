# Sector Outcome — Real Estate — 2026-08-18

Actuals: {'etf': 'XLRE', 'pct': -0.44613150740696605, 'spy_pct': -0.6755757074274671, 'rel': 0.22944420002050103, 'open': 45.18000030517578, 'close': 44.630001068115234}

I have enough data. Let me compile the review.

**FACTS:**
- XLRE: -0.446% (open 45.18 → close 44.63)
- SPY: -0.676%
- REL: +0.229% (XLRE outperformed SPY)
- Direction: down (mild)
- Morning predicted: down/mild

Let me verify the actuals against the morning prediction.

**Morning prediction:** down/mild, total_score -4.5, confidence 0.6
**Actual:** down, -0.446% (mild magnitude)

Direction HIT, magnitude HIT.

Now let me write the full review.

---

# Sector Post-Session Review — Real Estate (XLRE) — 2026-08-18

## Step 0: FACTS
- **XLRE:** -0.446% (open 45.18 → close 44.63)
- **SPY:** -0.676%
- **REL:** +0.229% (XLRE outperformed SPY by ~23bps)
- **Path:** Opened 45.18, closed 44.63 — drifted lower through the session, mild decline.

## Step 1: What drove the sector today

The dominant driver was the **rising long-end rate tape** — the same spine identified at open. The 30-year Treasury yield hit **5.30-5.33%**, its highest since 2007 (19-year high), with the 10-year near 4.63-4.68%. This is the classic negative for long-duration REITs.

Evidence:
- CLAIM: 30Y Treasury yield reached 5.30-5.33%, highest since 2007 / 19 years
  URL: seekingalpha.com/news/4634066; thesis.so; businesstimes.com.sg
  PUBLISHED: Aug 17-18, 2026
  QUOTE: "The U.S. 30-year Treasury yield (US30Y) climbed 4 basis points on Monday to 5.30%, its highest level since 2007."
  SUMMARY: Sustained fiscal/inflation worries pushed long-end yields to multi-decade highs, pressuring long-duration REITs.

- CLAIM: XLRE declined on the day, consistent with rate pressure
  URL: finance.yahoo.com/markets/stocks/articles/sector-financial-stocks-decline-monday
  PUBLISHED: Aug 18, 2026
  QUOTE: "The Philadelphia Housing Index was shedding 0.8%, and the State Street Real Estate Select Sector SPDR ETF (XLRE) was down 0.8%."
  SUMMARY: Real estate sector declined in line with the rate-driven risk-off tape.

- CLAIM: REITs pressured by elevated 10Y near 4.7%
  URL: perplexity.ai/finance/NNN
  PUBLISHED: ~Aug 14-18, 2026
  QUOTE: "...pressured by elevated 10-year Treasury yields near 4.7%."
  SUMMARY: The rate spine was the binding constraint on REITs all week.

**Taxonomy-aligned factors:**
- **Real yields rising** — HIT (DFII10 2.41%, +0.02 1d) — negative spine
- **Rates rising / REIT selloff** — HIT (30Y at 19-year high) — negative spine
- **Data-center REIT demand** — positive dispersion (AI-driven), but not enough to carry the sector on a rate-shock day
- **Office vacancy / refinancing wall** — structural negatives, background

## Step 2: Audit morning S0–S4 reads against reality

| Component | Morning read | Reality | Verdict |
|---|---|---|---|
| **S0_SHARED_MACRO** | -1 (risk-off, rising rates) | Correct. Risk-off tape, 30Y at 19-year high, futures negative. | **HIT** |
| **S1_SECTOR_FACTORS** | -1 (rates/real yields neg, data-center pos) | Correct. Rate spine dominated; data-center strength insufficient to flip. | **HIT** |
| **S2_BREADTH** | 0 (defensive bid, 1d/3d rel positive) | Correct. XLRE did outperform SPY (+0.23% rel) — defensive bid confirmed. | **HIT** |
| **S3_FLOWS_POSITIONING** | 0 (mixed flows) | Neutral, no same-day flow spike. | **HIT** |
| **S4_ETF_TAPE** | 0 (mixed confirmation) | Correct — relative strength confirmed but absolute direction down. | **HIT** |

**Direction:** down → actual down. **HIT**
**Magnitude:** mild → actual -0.446% (mild). **HIT**

## Step 3: Interactions / double-count / knowable-at-open test

**Interactions:** The key interaction was the **rate spine vs. defensive bid**. Rising long-end rates (30Y at 19-year high) argued for a sharper down move, but the defensive bid into REITs on a risk-off day (relative outperformance vs SPY) capped the magnitude at mild. This interaction was correctly identified at open — the morning explicitly noted "the defensive relative strength may cap the magnitude at mild."

**Double-count check:** S0 (-1) and S1 (-1) both captured the rate/real-yield negative. This is legitimate double-counting of the same spine (shared macro + sector-specific manifestation), not an error — the rubric intends both. No inflation of the score.

**Knowable-at-open:** **Yes.** The 30Y at 19-year high, rising real yields, and sharply negative futures were all live at open. The 08-17 lesson (live rate tape at open dominates) was correctly applied. The defensive-bid offset was also visible in the 1d/3d relative tape. Everything needed was available pre-market.

## Step 4: Outliers inside the sector

- **Data-center REITs** remained the standout positive dispersion (AI-driven demand), consistent with the morning's positive HIT. They likely outperformed the sector average.
- **Senior housing REITs** showed continued bullishness (Law360: "REITs Bullish On Senior Housing But Wary Of Labor Costs") — a positive dispersion theme.
- **Office REITs** remained under structural pressure (elevated vacancy, refinancing cliff) — the negative tail.

These dispersion themes are consistent with the morning's S1 read (data-center positive offsetting office/refinancing negatives) and explain why the sector's decline was mild rather than severe despite the rate shock.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.446
SPY_PCT: -0.676
REL_PCT: +0.229
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Rising long-end rates (30Y at 19-year high, 5.30-5.33%) pressuring long-duration REITs on a risk-off day
KEY_INTERACTION: Rate spine (negative) vs. defensive bid into REITs (positive relative) — defensive bid capped magnitude at mild
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Full HIT — direction down and magnitude mild both correct; all five S-components scored accurately; the defensive-bid cap on magnitude was correctly anticipated
OUTCOME_END

**Rolling accuracy note:** This is a clean **dir HIT + mag HIT** for Real Estate. The 08-17 live-rate lesson was correctly applied (rising long-end rates at open → down), and the defensive-bid offset was correctly weighted to keep magnitude at mild. The pipeline's -4.5 total score (down/mild) matched reality exactly.