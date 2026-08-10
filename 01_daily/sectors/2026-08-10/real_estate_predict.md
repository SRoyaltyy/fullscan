# Sector Prediction — Real Estate — 2026-08-10

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.75** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-10):
  1d: XLRE -1.45% | SPY +0.07% | rel -1.52%
  3d: XLRE -1.92% | SPY +0.52% | rel -2.45%
  1w: XLRE -1.88% | SPY +2.13% | rel -4.01%
  1m: XLRE -0.27% | SPY +2.50% | rel -2.77%
```

The web searches are returning empty results across all queries. I'll note this and proceed with the analysis based on the pre-fetched Channel 1 data and the prior run context, which is substantial.

Let me analyze the data I have:

**Channel 1 key data:**
- VIX 15.16 (low, calm), VIX/VIX3M 0.804 (contango, no stress)
- DGS10 4.69 (up +0.06 1d, +0.13 1m) — rates RISING
- DFII10 (real yield) 2.43 (up +0.02 1d, +0.12 1m) — real yields RISING (spine negative)
- DGS30 5.22 (up)
- HY spread 2.71 (tight, risk-on)
- DXY 1d +0.21%, 1m -1.15% (USD mixed)
- ES premarket -0.02%, NQ -0.17% (flat)
- Asia composite +0.87% (risk-on), Europe -0.09% (flat)
- Fear & Greed 63.7 (Greed)
- 5-day corr 10Y vs SPX: -0.842 (strong negative — rates up = stocks down)
- XLRE vs SPY: 1d rel -1.52%, 3d rel -2.45%, 1w rel -4.01%, 1m rel -2.77% — XLRE sharply lagging

**Key drivers for Real Estate:**
- Real yields RISING (2.43, +0.12 1m) — spine NEGATIVE for REIT duration
- 10Y rising (4.69, +0.13 1m) — rates rising / REIT selloff spine NEGATIVE
- 5-day corr 10Y vs SPX strongly negative (-0.842) — rising rates are dragging equities, especially duration-sensitive REITs
- XLRE lagging SPY across ALL timeframes, especially 1w -4.01% — clear relative underperformance
- Broad tape risk-on (Asia +0.87%, HY tight, VIX low, Fear&Greed Greed) but REITs lag in risk-on with rising real yields

**Sector factors:**
- Real yields rising: NEGATIVE (spine)
- Rates rising / REIT selloff: NEGATIVE (spine)
- Data-center REIT demand: POSITIVE (dispersion) — from prior context, still relevant
- Refinancing wall stress: mild negative
- Office vacancy: negative sub-type

**Breadth:** XLRE lagging SPY across all timeframes, chronic underperformance. Narrow leadership (data centers, senior housing).

**Flows:** No clear REIT-specific flow data.

**Tape confirmation:** XLRE sharply lagging — negative confirmation.

Let me score:

**S0_SHARED_MACRO (-2..+2): -1**
Real yields rising (2.43, +0.12 1m), 10Y rising (4.69, +0.13 1m). 5-day corr 10Y vs SPX strongly negative (-0.842) — rising rates dragging equities, especially duration-sensitive REITs. Broad tape risk-on but that's a headwind for REITs. USD mixed. Net negative for this sector.

**S1_SECTOR_FACTORS (-3..+3): -1**
- Real yields rising: NEGATIVE (spine)
- Rates rising / REIT selloff: NEGATIVE (spine)
- Data-center REIT demand: POSITIVE (dispersion)
- Refinancing wall stress: mild negative
- Office vacancy: mild negative
Net negative, offset by data-center strength.

**S2_BREADTH (-2..+2): -1**
XLRE lagging SPY across all timeframes (1w -4.01%), chronic underperformance. Narrow leadership (data centers, senior housing) carrying the sector. Breadth failure pattern.

**S3_FLOWS_POSITIONING (-2..+2): 0**
No clear REIT-specific flow data. Neutral.

**S4_ETF_TAPE (-1..+1): -1**
Confirmation only. XLRE lagging across all timeframes, especially 1w -4.01% and 1d -1.52%. Negative confirmation.

**Divergence:** The forward easing bias (from prior context, Dec cut odds high) and data-center strength are positives not fully priced, but real yields still rising caps upside. Not strong enough to override.

**Multiplier:** 0.9 (risk-on broad tape but REITs lag in risk-on with rising real yields).

**Confidence:** 0.6 (data clear, but web search unavailable so relying on pre-fetched data and prior context).

**Direction:** down, magnitude mild.

Let me write the final output.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate — last run 2026-08-08 predicted down/mild (ungraded). Rolling accuracy n=0. Standing lessons: mega-cap-earnings-over-macro-drag (general scope, not directly applicable to REIT sector call); ops-missing-predict-file (ops scope). No REIT-specific lesson yet.

---

## Real Estate (XLRE) — Sector Environment Analysis — 2026-08-10

### Channel 2 Research Summary

**Note:** Live web_search returned empty results across all queries this session. Analysis relies on pre-fetched Channel 1 data and prior-run context. Flagged as data-thin on flows/positioning.

**1. Shared macro regime:** Broad tape is **risk-on** — Asia composite +0.87%, HY spread tight at 2.71, VIX low at 15.16, Fear & Greed at 63.7 (Greed). However, this is a **headwind for REITs**, which are rate-sensitive/duration assets that lag in risk-on when real yields are elevated. The macro tape for this sector is dominated by rates, not equity beta.

**2. Rates / real yields (dominant driver):**
- **10Y real yield (TIPS): 2.43%, up +0.02 1d, +0.12 1m** — RISING trend (spine NEGATIVE for REIT duration).
- **10Y nominal: 4.69%, up +0.06 1d, +0.13 1m** — rates RISING (spine NEGATIVE).
- **30Y: 5.22%, up +0.05 1d, +0.16 1m** — long-end rising, directly pressuring duration assets.
- **5-day corr 10Y vs SPX: -0.842** — strongly negative; rising rates are actively dragging equities, and REITs (long-duration) are most exposed.
- **USD:** DXY +0.21% 1d but -1.15% 1m — mixed, not a dominant factor for domestic REITs.

**3. Sector-specific factors (taxonomy):**
- **Real yields rising** — HIT (negative, spine). 10Y real yield 2.43%, rising.
- **Rates rising / REIT selloff** — HIT (negative, spine). 10Y at 4.69%, 30Y at 5.22%, both rising.
- **Data-center REIT demand / rent upside** — HIT (positive, dispersion). From prior context: NA data-center vacancy <3%, data-center REIT FFO +29.4% YoY.
- **Refinancing wall stress** — mild negative. 2026 CRE maturity wall ~$875B (smaller than 2025's $957B).
- **Office vacancy / mark-to-market stress** — mild negative sub-type.
- **Rates falling / REIT duration relief** — NOT materialized; real yields still rising.

**4. Breadth / leadership:** XLRE lagging SPY across ALL timeframes (1d -1.52%, 3d -2.45%, 1w -4.01%, 1m -2.77%). Chronic underperformance. Leadership narrow — data centers and senior housing (Welltower) carrying the sector; broader REITs lag. Breadth-failure pattern.

**5. Flows / positioning:** No REIT-specific flow data available (web search empty). Neutral-to-unclear.

**6. Earnings / policy catalysts:** Fed path is the key catalyst. Prior context noted December cut odds rising (~80%), a forward easing bias not yet in the tape. But real yields still rising caps near-term upside.

### Channel 1 Confirmation
XLRE vs SPY: 1d rel **-1.52%**, 3d rel -2.45%, **1w rel -4.01%**, 1m rel -2.77%. XLRE sharply lagging across all timeframes, with a fresh 1-day underperformance of -1.52% on a day SPY was +0.07%. Clear negative confirmation.

### Section A: Regime
**A1.** mixed (risk-on broad tape, but rate-sensitive REITs face rising real yields — negative regime for this sector specifically).
**A2.** Multiplier **0.9** — risk-on broad market is a mild offset but rising real yields dominate for REITs.

### Section B: Component Scores
**S0_SHARED_MACRO (-2..+2): -1**
Real yields rising (2.43, +0.12 1m), 10Y rising (4.69, +0.13 1m), 30Y rising (5.22). 5-day corr 10Y vs SPX strongly negative (-0.842) — rising rates actively dragging duration assets. Broad risk-on is a headwind for REITs. Net negative.

**S1_SECTOR_FACTORS (-3..+3): -1**
- Real yields rising: NEGATIVE (spine)
- Rates rising / REIT selloff: NEGATIVE (spine)
- Data-center REIT demand: POSITIVE (dispersion)
- Refinancing wall stress: mild negative
- Office vacancy: mild negative
Net negative, offset by data-center strength.

**S2_BREADTH (-2..+2): -1**
XLRE lagging SPY across all timeframes (1w -4.01%, 1d -1.52%). Narrow leadership (data centers, senior housing) carrying the sector. Breadth-failure pattern.

**S3_FLOWS_POSITIONING (-2..+2): 0**
No REIT-specific flow data available. Neutral.

**S4_ETF_TAPE (-1..+1): -1**
Confirmation only. XLRE sharply lagging across all timeframes, especially 1w -4.01% and fresh 1d -1.52%. Negative confirmation.

**Divergence:** Forward easing bias (Dec cut odds high per prior context) and data-center strength are positives not fully priced, but real yields still rising caps upside. Not strong enough to override the negative tape.

**Multiplier:** 0.9 (risk-on broad tape but REITs lag in risk-on with rising real yields).
**Confidence:** 0.6 (data reasonably clear; flows data thin due to web search unavailability).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.6
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Real yields rising|hit|0.8|2026-08-10|Channel1 DFII10 2.43 +0.12 1m
Rates rising / REIT selloff|hit|0.8|2026-08-10|Channel1 DGS10 4.69 +0.13 1m, DGS30 5.22
Data-center REIT demand / rent upside|hit|0.6|2026-08-10|Prior context NA DC vacancy<3%, FFO +29.4%
Refinancing wall stress|hit|0.4|2026-08-10|2026 CRE wall ~$875B
Office vacancy / mark-to-market stress|hit|0.3|2026-08-10|Sub-type negative
Sector breadth failure (ETF up, names flat)|hit|0.5|2026-08-10|XLRE lagging all timeframes, narrow leadership
Sector ETF outflow / volume dry-up|miss|0.3|2026-08-10|No flow data
HIT_GRID_END

**Final call:** XLRE **down / mild** — real yields and rates rising (spine negatives) dominate, XLRE sharply lagging SPY across all timeframes, breadth narrow. Data-center strength and forward easing bias are partial offsets but not enough to flip the call.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.75, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.6, 'regime': 'mixed'}
```
