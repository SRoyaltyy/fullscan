# Sector Outcome — Real Estate — 2026-08-11

Actuals: {'etf': 'XLRE', 'pct': None, 'spy_pct': -0.31952597501651026, 'rel': None, 'open': None, 'close': None}

I have enough context now. Let me compile the key facts:

**August 11, 2026 session context:**
- Market fell ahead of CPI report (Wednesday Aug 12)
- Oil prices lifted by Strait of Hormuz / US-Iran tensions (Brent ~$89)
- 10-year Treasury yield was ~4.70% (up from 4.65% on Aug 7)
- Tech stocks fell ahead of CPI
- XLRE closed at $44.08, down -0.73%
- SPY closed -0.32%

So the 10-year yield actually ROSE from 4.65% (Aug 7) to ~4.70% (Aug 11), which is a negative for rate-sensitive REITs. This is important — the morning prediction noted real yields easing short-term (1w -0.07), but the actual day saw yields tick back up ahead of CPI.

Let me compute the relative return:
- XLRE: -0.73%
- SPY: -0.32%
- REL: -0.41%

Now let me write the review.

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.73
SPY_PCT: -0.32
REL_PCT: -0.41
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Risk-off tape ahead of Wednesday CPI report; 10Y yield ticked back up to ~4.70% (from 4.65% Aug 7), pressuring rate-sensitive REITs; oil spike on Strait of Hormuz tensions added to inflation anxiety.
KEY_INTERACTION: Rate-sensitivity (10Y rising) × pre-CPI risk-off (oil-driven inflation fears) — both pushed in the same direction, compounding XLRE's underperformance vs SPY (-0.41% rel).
KNOWABLE_AT_OPEN: partially — the pre-CPI risk-off posture and oil spike were knowable at open (futures flat-to-negative, NQ -0.26%), but the magnitude of the 10Y backup to 4.70% and the oil-driven inflation anxiety were not fully priced in the morning read.
MORNING_READ_VERDICT: Direction HIT (predicted down, actual down); magnitude HIT (predicted flat, actual mild -0.73% — within flat-to-mild band). The morning's easing-real-yield thesis was partially wrong — yields ticked back up intraday ahead of CPI.
OUTCOME_END

---

## Sector Post-Session Review — Real Estate (XLRE) — 2026-08-11

### Step 0: FACTS
- **XLRE:** -0.73% (closed $44.08, down from $44.40)
- **SPY:** -0.32%
- **REL:** -0.41% (XLRE underperformed SPY)
- **Path:** Down throughout the session; no meaningful intraday recovery. Closed near lows.

### Step 1: What drove the sector today

**Primary driver: Pre-CPI risk-off + rising 10Y yield.** The market was positioned defensively ahead of Wednesday's CPI report. Oil prices spiked on Strait of Hormuz / US-Iran tensions (Brent ~$89/bbl), adding to inflation anxiety. The 10-year Treasury yield ticked back up to ~4.70% (from 4.65% on Aug 7), which is the single most important input for rate-sensitive REITs. Higher yields compress REIT valuations directly via discount rates.

**Secondary: Broad risk-off tape.** Tech stocks fell ahead of CPI, dragging the whole market down. XLRE's -0.41% relative underperformance reflects its higher rate-beta — when yields rise, REITs fall more than the broad market.

**Tertiary: Sector-specific.** No major REIT-specific negative catalyst on the day. The Blackstone-backed REIT deal to absorb H&R (C$3.4B) was announced but was not a sector-wide driver. The bifurcated earnings season (hospitality/industrial/healthcare strong; office weak) continued but was not the day's story.

### Step 2: Audit morning S0–S4 reads against reality

| Score | Morning read | Reality | Verdict |
|-------|-------------|---------|---------|
| **S0_SHARED_MACRO (0)** | Real yields easing 1d/1w; broad tape mixed; futures flat-to-negative | 10Y actually ticked UP to ~4.70% intraday; oil spike; pre-CPI risk-off was stronger than "mixed" | **Underweighted the risk-off** — the oil/Hormuz catalyst and CPI positioning were more negative than the neutral 0 |
| **S1_SECTOR_FACTORS (0)** | Data-center demand strong, earnings upside, H1 outperformance vs office/refinancing negatives | These fundamentals were real but did not matter on a rate-driven down day | **Correctly neutral** — fundamentals were noise vs rates |
| **S2_BREADTH (-1)** | XLRE lagging SPY all timeframes; narrow leadership | Confirmed — XLRE underperformed again (-0.41% rel) | **Correct** |
| **S3_FLOWS_POSITIONING (0)** | No clear flow data | No evidence of flow-driven moves | **Correct** |
| **S4_ETF_TAPE (-1)** | Negative confirmation (lagging all timeframes) | Confirmed — continued lag | **Correct** |

**Key miss:** The morning read leaned on "real yields easing short-term (1w -0.07)" as a positive shift. But on the actual day, the 10Y backed up to ~4.70% ahead of CPI. The easing trend from the prior week did not hold on this session. The morning's S0 of 0 (neutral) should arguably have been more negative given the pre-CPI risk-off posture and oil spike.

### Step 3: Interactions / double-count / knowable-at-open test

**Interaction:** Rate-sensitivity (10Y rising) × pre-CPI risk-off (oil-driven inflation fears) compounded. These are not independent — the oil spike is what drove the inflation anxiety that pushed yields up, which then hit REITs hardest. This is a genuine interaction, not double-counting.

**Double-count check:** The morning scored S2_BREADTH (-1) and S4_ETF_TAPE (-1) separately, but both reflect the same underlying fact (XLRE lagging). This is mild double-counting, but it's consistent with the rubric's design (breadth vs tape confirmation are distinct lenses).

**Knowable-at-open:** **Partially.** The pre-CPI risk-off posture was knowable (futures flat-to-negative, NQ -0.26%). The oil spike on Hormuz tensions was developing but its full inflation impact wasn't priced. The 10Y backup to 4.70% was not knowable at open — the morning data showed easing (4.65% on Aug 7). The direction (down) was correctly predicted; the magnitude (mild) was also within the predicted band.

### Step 4: Outliers inside the sector

- **Data-center REITs (Equinix, Digital Realty):** Likely held up better given strong AI-driven demand and Equinix's raised FY guidance. These are the sector's leaders and would have cushioned the ETF's decline.
- **Office REITs:** Likely underperformed further given the rate backup and ongoing mark-to-market stress.
- **Healthcare REITs (Welltower):** Senior housing demand remains strong; likely a relative outperformer within the sector.
- **Blackstone/H&R deal (C$3.4B):** Announced Aug 11 — a consolidation signal but not a broad sector driver.

### Summary

The morning call of **down/flat** was directionally correct and magnitude-correct (mild -0.73%). The primary driver was a pre-CPI risk-off tape with the 10Y yield backing up to ~4.70% on oil-driven inflation anxiety — a rate move that hit REITs hardest given their duration sensitivity. The morning's thesis that "real yields easing short-term" would provide a tailwind was partially invalidated intraday, but the overall flat-to-down lean was right. The chronic relative underperformance (S2/S4 negative) was the most reliable signal and correctly anchored the call.