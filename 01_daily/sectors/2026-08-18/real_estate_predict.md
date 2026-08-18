# Sector Prediction — Real Estate — 2026-08-18

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.5** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-18):
  1d: XLRE -0.78% | SPY -0.95% | rel +0.17%
  3d: XLRE +0.96% | SPY -0.46% | rel +1.41%
  1w: XLRE -0.14% | SPY -0.56% | rel +0.41%
  1m: XLRE -1.20% | SPY +2.43% | rel -3.63%
```

I have enough data. Let me synthesize the analysis.

Key data for today (2026-08-18):
- **Real yields RISING**: DFII10 2.41, +0.02 1d, +0.01 1w, +0.06 1m — spine NEGATIVE
- **10Y RISING**: 4.68, +0.05 1d, +0.03 1w, +0.11 1m
- **30Y RISING**: 5.25, +0.04 1d, +0.06 1w, +0.16 1m — 30Y at 19-year high (per CNBC Aug 18)
- **Futures sharply NEGATIVE**: ES -0.61%, NQ -1.7% — risk-off
- **Asia mixed-to-negative**: Nikkei -1.82%, Kospi -1.55%, ASX -0.8%, but Hang Seng +1.41%, Shanghai +1.61%
- **Europe negative**: DAX -0.92%, CAC -1.23%, EuroStoxx -0.79%
- **Oil flat-to-down**: CL -0.46%, BZ +0.04%
- **Gold up +0.73%** — defensive bid
- **USD weakening** (DXY -0.1% 1d, -1.17% 1m)
- **Fear & Greed 55.2 (Greed)** — cooling from prior 65
- **5-day corr 10Y vs SPX: -0.58** — rising rates dragging equities
- **XLRE tape**: 1d rel +0.17%, 3d rel +1.41%, 1w rel +0.41%, 1m rel -3.63%

The 08-17 reflect lesson is directly applicable: **live rate tape at open > prior-day CPI/easing extrapolation**. Today 10Y/30Y/real yields are ALL RISING at open, 30Y at 19-year high, futures sharply negative (ES -0.61%, NQ -1.7%). This is a negative long-end rate shock day for REITs.

The 08-12 lesson (don't default S0 negative after cool CPI) does NOT apply because the live rate tape is now negative — yields are rising, not easing.

The 08-11 lesson (active geopolitical/oil risk-off) — oil is flat-to-down, no Hormuz crisis. Not firing.

Let me score.

**S0_SHARED_MACRO (-2..+2): -1**
Real yields RISING (+0.02 1d), 10Y rising (+0.05 1d), 30Y at 19-year high (5.25, +0.04 1d). Futures sharply negative (ES -0.61%, NQ -1.7%). Europe negative. 5-day corr 10Y vs SPX -0.58 (rising rates dragging equities). This is a risk-off day with rising long-end rates — the worst backdrop for REITs. USD weakening is a mild offset. Net negative.

**S1_SECTOR_FACTORS (-3..+3): -1**
- Real yields rising: NEGATIVE HIT (spine, DFII10 +0.02 1d)
- Rates rising / REIT selloff: NEGATIVE HIT (spine, 30Y at 19-year high)
- Data-center REIT demand / rent upside: POSITIVE HIT (strong, AI-driven)
- Office vacancy / mark-to-market stress: NEGATIVE (elevated vacancy, $1.1T refinancing cliff)
- Refinancing wall stress: NEGATIVE (~$1T maturing 2026)
Net: negatives (real yields, rates, office, refinancing) offset by data-center strength. Slightly negative.

**S2_BREADTH (-2..+2): 0**
XLRE 1d rel +0.17% (positive today despite SPY -0.95%), 3d rel +1.41% (strong). But 1m still lagging -3.63%. The 1d/3d positive relative returns suggest some defensive bid into REITs on this risk-off day. Mixed — improving short-term but chronic laggard on 1m. Neutral.

**S3_FLOWS_POSITIONING (-2..+2): 0**
XLRE 5-day net AUM change -$222M (outflow), but 1-month +$146M (inflow). Mixed. No confirmed same-day flow spike. Neutral.

**S4_ETF_TAPE (-1..+1): 0**
Confirmation only. XLRE 1d rel +0.17% (positive today, defensive bid on risk-off), 3d rel +1.41% (strong). But 1m still lagging -3.63%. The 1d/3d positive relative returns suggest REITs are holding up relatively on this risk-off day. Mixed confirmation — positive short-term relative but chronic laggard. Neutral.

**Divergence:** Leading factors (rising real yields, 30Y at 19-year high, sharply negative futures) are negative for REITs, but the 1d/3d tape is showing relative strength (+0.17%/+1.41% rel). This is a divergence — the negative rate shock argues down, but the tape shows REITs holding up relatively on a risk-off day. The 08-17 lesson says live rate tape at open should dominate. However, the tape is showing defensive bid into REITs.

**Multiplier:** 0.9 (risk-off broad tape with sharply negative futures; REITs showing defensive relative strength but facing rising long-end rates).

**Confidence:** 0.6.

**Direction:** The dominant driver for REITs is the rate spine. Today real yields are RISING, 30Y at 19-year high, and futures are sharply negative. The 08-17 lesson is directly applicable — live rate tape at open is negative, so the positive default does NOT apply. However, the 1d/3d relative tape is showing REITs holding up relatively on this risk-off day (defensive bid). 

Given the rising long-end rates (30Y at 19-year high) and sharply negative futures, I lean **down** for absolute direction, but the defensive relative strength suggests the move may be mild. The 08-17 lesson says when yields are rising to multi-decade highs at open, direction should be down/negative for the rate-sensitive sector.

I'll go **down/mild** — the rising long-end rate shock and sharply negative futures dominate, but the defensive relative bid (REITs holding up vs SPY on risk-off) caps the magnitude at mild.

Let me write the final output.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate — 2026-08-08 down/mild (ungraded), 2026-08-10 down/mild actual -1.29% (dir HIT), 2026-08-11 down/flat actual -0.72% (dir HIT, mag MISS), 2026-08-12 down/mild actual +0.93% (dir MISS), 2026-08-13 up/mild actual +1.42% (dir HIT), 2026-08-14 up/notable actual +0.33% (dir HIT, mag MISS — pipeline mismatch), 2026-08-17 up/notable actual -0.97% (dir MISS). Rolling accuracy dir=0.667, mag=0.167 (n=6). Active REIT lessons applied: (1) 08-17 live-rate reversal — today 10Y/30Y/real yields ALL RISING at open, 30Y at 19-year high, futures sharply negative (ES -0.61%, NQ -1.7%), so the positive default does NOT apply; (2) 08-12 duration-relief lesson — superseded by live-rate tape which is now negative; (3) 08-11 geopolitical risk-off — oil flat-to-down, not firing.

---

## Real Estate (XLRE) — Sector Environment Analysis — 2026-08-18

### Channel 2 Research Summary

**1. Shared macro regime:** Broad tape is **risk-off** — ES premarket -0.61%, NQ -1.7% (sharply negative futures), Europe negative (DAX -0.92%, CAC -1.23%, EuroStoxx -0.79%), Asia mixed (Nikkei -1.82%, Kospi -1.55%, but Hang Seng +1.41%, Shanghai +1.61%). VIX up to 15.73 (+1.48 1d). Fear & Greed cooling to 55.2 (Greed, down from 65). Gold up +0.73% (defensive bid). This is a risk-off day with rising long-end rates — the worst backdrop for REITs.

**2. Rates / real yields (dominant driver):**
- **10Y real yield (TIPS): 2.41%, +0.02 1d, +0.01 1w, +0.06 1m** — real yields RISING (spine NEGATIVE for REIT duration).
- **10Y nominal: 4.68%, +0.05 1d, +0.03 1w, +0.11 1m** — rates RISING.
- **30Y: 5.25%, +0.04 1d, +0.06 1w, +0.16 1m** — **30Y at 19-year high** (CNBC Aug 18: "30-year Treasury yield has surged to its highest level in 19 years, and some strategists see scope for the move to go further"). This is the key negative for long-duration REITs.
- **5-day corr 10Y vs SPX: -0.58** — rising rates actively dragging equities, and REITs (long-duration) are most exposed.
- **Wells Fargo (Aug 18):** "Treasury yields are mostly higher before the opening bell Tuesday ahead of today's economic releases, including industrial production, import price, and housing market data for July."

**3. Sector-specific factors (taxonomy):**
- **Real yields rising** — HIT (negative, spine). DFII10 2.41%, +0.02 1d.
- **Rates rising / REIT selloff** — HIT (negative, spine). 30Y at 19-year high, 10Y rising.
- **Data-center REIT demand / rent upside** — HIT (positive, dispersion). AI-driven demand surging; data-center REITs the standout performer (The Real Deal Aug 7).
- **Office vacancy / mark-to-market stress** — HIT (negative). Elevated vacancy; $1.1T refinancing cliff (AlphabetaStock).
- **Refinancing wall stress** — HIT (negative). ~$1T CRE loans maturing 2026.
- **Cap-rate expansion** — negative in rising-rate environment (pomegra.io).

**4. Breadth / leadership:** XLRE 1d rel +0.17% (positive today despite SPY -0.95%), 3d rel +1.41% (strong). But 1m still lagging -3.63%. The 1d/3d positive relative returns suggest a defensive bid into REITs on this risk-off day — REITs holding up relatively vs SPY. However, this is narrow leadership (data centers, senior housing) carrying the sector.

**5. Flows / positioning:** XLRE 5-day net AUM change -$222M (outflow), but 1-month +$146M (inflow). Mixed. No confirmed same-day flow spike. REITs as defensive/safe-haven bid in risk-off context.

**6. Earnings / policy catalysts:** FOMC minutes Wednesday (July 28-29 meeting). Today's economic releases: industrial production, import price, housing market data for July (Wells Fargo). No REIT-specific earnings catalyst today.

### Channel 1 Confirmation
XLRE vs SPY: 1d rel +0.17%, 3d rel +1.41%, 1w rel +0.41%, 1m rel -3.63%. XLRE positive on 1d/3d/1w relative (defensive bid on risk-off), but still lagging on 1m.

### Section A: Regime
**A1.** risk_off — sharply negative futures (ES -0.61%, NQ -1.7%), Europe negative, VIX up, rising long-end rates (30Y at 19-year high). This is a risk-off day with rising rates — the worst backdrop for REITs.
**A2.** Multiplier **0.9** — risk-off broad tape with sharply negative futures; REITs showing defensive relative strength but facing rising long-end rates.

### Section B: Component Scores

**S0_SHARED_MACRO (-2..+2): -1**
Real yields RISING (+0.02 1d), 10Y rising (+0.05 1d), 30Y at 19-year high (5.25, +0.04 1d). Futures sharply negative (ES -0.61%, NQ -1.7%). Europe negative. 5-day corr 10Y vs SPX -0.58 (rising rates dragging equities). This is a risk-off day with rising long-end rates — the worst backdrop for REITs. USD weakening is a mild offset. Net negative.

**S1_SECTOR_FACTORS (-3..+3): -1**
- Real yields rising: NEGATIVE HIT (spine, DFII10 +0.02 1d)
- Rates rising / REIT selloff: NEGATIVE HIT (spine, 30Y at 19-year high)
- Data-center REIT demand / rent upside: POSITIVE HIT (strong, AI-driven)
- Office vacancy / mark-to-market stress: NEGATIVE (elevated vacancy, $1.1T refinancing cliff)
- Refinancing wall stress: NEGATIVE (~$1T maturing 2026)
Net: negatives (real yields, rates, office, refinancing) offset by data-center strength. Slightly negative.

**S2_BREADTH (-2..+2): 0**
XLRE 1d rel +0.17% (positive today despite SPY -0.95%), 3d rel +1.41% (strong). But 1m still lagging -3.63%. The 1d/3d positive relative returns suggest a defensive bid into REITs on this risk-off day. Mixed — improving short-term but chronic laggard on 1m. Neutral.

**S3_FLOWS_POSITIONING (-2..+2): 0**
XLRE 5-day net AUM change -$222M (outflow), but 1-month +$146M (inflow). Mixed. No confirmed same-day flow spike. Neutral.

**S4_ETF_TAPE (-1..+1): 0**
Confirmation only. XLRE 1d rel +0.17% (positive today, defensive bid on risk-off), 3d rel +1.41% (strong). But 1m still lagging -3.63%. The 1d/3d positive relative returns suggest REITs holding up relatively on this risk-off day. Mixed confirmation — positive short-term relative but chronic laggard. Neutral.

**Divergence:** Leading factors (rising real yields, 30Y at 19-year high, sharply negative futures) are negative for REITs, but the 1d/3d tape is showing relative strength (+0.17%/+1.41% rel). This is a divergence — the negative rate shock argues down, but the tape shows REITs holding up relatively on a risk-off day. The 08-17 lesson says live rate tape at open should dominate. However, the tape is showing defensive bid into REITs.

**Multiplier:** 0.9 (risk-off broad tape with sharply negative futures; REITs showing defensive relative strength but facing rising long-end rates).

**Confidence:** 0.6.

**Direction:** The dominant driver for REITs is the rate spine. Today real yields are RISING, 30Y at 19-year high, and futures are sharply negative. The 08-17 lesson is directly applicable — live rate tape at open is negative, so the positive default does NOT apply. However, the 1d/3d relative tape is showing REITs holding up relatively on this risk-off day (defensive bid). Given the rising long-end rates (30Y at 19-year high) and sharply negative futures, I lean **down** for absolute direction, but the defensive relative strength suggests the move may be mild. I'll go **down/mild**.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.6
REGIME: risk_off
TOTAL_SCORE: -1.8
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
DIVERGENCE_FLAGGED: True
HORIZON_3D: down:mild:0.55
HORIZON_1W: flat:mild:0.5
HORIZON_2W: flat:mild:0.5
HORIZON_1M: flat:mild:0.45
SECTOR_SCORES_END

HIT_GRID_BEGIN
Real yields rising|HIT|0.7|2026-08-18|DFII10 2.41 +0.02 1d
Rates rising / REIT selloff|HIT|0.8|2026-08-18|30Y at 19-year high 5.25, 10Y 4.68 rising
Data-center REIT demand / rent upside|HIT|0.6|2026-08-18|AI-driven demand surging
Office vacancy / mark-to-market stress|HIT|0.5|2026-08-18|Elevated vacancy, $1.1T refinancing cliff
Refinancing wall stress|HIT|0.5|2026-08-18|~$1T CRE loans maturing 2026
Risk-off tape / flight to safety|HIT|0.6|2026-08-18|ES -0.61%, NQ -1.7%, Europe negative
Sector rotation into REITs|PARTIAL|0.4|2026-08-18|1d/3d rel positive (defensive bid) but 1m lagging
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.6, 'regime': 'risk_off'}
```
