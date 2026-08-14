# Sector Prediction — Real Estate — 2026-08-14

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **up**
- predicted_magnitude_band: **notable**
- total_score: **7.5** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-13):
  1d: XLRE +1.42% | SPY +0.70% | rel +0.72%
  3d: XLRE +1.62% | SPY +0.63% | rel +0.99%
  1w: XLRE +0.69% | SPY +1.21% | rel -0.52%
  1m: XLRE +0.94% | SPY +3.83% | rel -2.89%
```

I have enough data to construct the analysis. Let me synthesize the key inputs.

**Key Channel 1 data for today (2026-08-14):**
- **DFII10 (real yield): 2.42%, 1d -0.01, 1w +0.01, 1m +0.09** — real yields flat-to-slightly-easing today, elevated over 1m
- **DGS10: 4.68%, 1d -0.02, 1w +0.05, 1m +0.10** — 10Y easing slightly today, elevated 1w/1m
- **DGS30: 5.24%, 1d +0.0, 1w +0.07, 1m +0.16** — long-end elevated
- **VIX 14.56** (low, calm), Fear & Greed 66.7 (Greed)
- **ES premarket +0.06%, NQ +0.15%** — futures flat-to-slightly-positive
- **Asia composite +0.22%** (Kospi +2.42%, Nikkei +0.59%, but Hang Seng -1.1%, ASX -0.8%), Europe +0.22% — mixed-to-mildly-positive
- **Oil UP** (CL +0.8%, BZ +0.32%) — mild geopolitical premium
- **Gold UP +1.0%** — defensive bid
- **USD weakening** (DXY -0.23% 1d, -1.53% 1m)
- **5-day corr 10Y vs SPX: -0.493** (moderate negative)
- **XLRE vs SPY: 1d rel +0.72%, 3d rel +0.99%, 1w rel -0.52%, 1m rel -2.89%** — XLRE positive on 1d/3d, still lagging on 1w/1m

**Key context:**
- CPI printed in-line/cool on Aug 12 — second/third session of yield relief
- Real yields flat today, not intensifying
- XLRE 1d rel +0.72% and 3d rel +0.99% — positive tape confirmation, inflection continuing
- Data-center REIT demand strong (hyperscaler capex $625-690B)
- Office vacancy 20.5%, refinancing wall ~$936B maturing 2026 — negatives
- Oil up slightly, gold up — mild defensive/risk-off undertone but futures flat-positive

Let me apply the active lessons:
- **08-12 lesson (a-long-duration-rate-sensitive-sector-reits-has-been-lagging):** CPI is now PAST (printed in-line/cool Aug 12). The duration relief is materializing. This lesson now points POSITIVE — don't default S0 negative.
- **08-11 lesson (active-geopolitical-oil-supply-risk-off):** Oil is up slightly (+0.8%) but not a major spike. No Hormuz-style crisis headline in the news. This lesson's negative S0 trigger is NOT strongly firing today.
- **08-12 utilities lesson (a-rate-sensitive-defensive-sector-utilities-has-a-confirmed):** Warns about capping magnitude when broad tape is risk-on with tech leading. Today futures are flat-to-positive, not a strong tech-led rally. But the principle of not over-correcting magnitude on a defensive bid applies.

Let me score.

**S0_SHARED_MACRO (-2..+2): +1**
Real yields flat today (2.42%, 1d -0.01), 10Y easing slightly (-0.02 1d). CPI printed in-line/cool Aug 12, easing Fed rate pressure — the duration headwind is relieving. Oil up slightly (+0.8%) but no major geopolitical shock. USD weakening (mild positive for domestic REITs). Futures flat-to-positive (ES +0.06%). Broad tape mixed-to-mildly-positive. For REITs, the macro backdrop is turning positive — the cool CPI relieved the dominant duration headwind. Net positive.

**S1_SECTOR_FACTORS (-3..+3): +1**
- Real yields flat/easing short-term: POSITIVE shift (spine relief)
- Rates falling / REIT duration relief: POSITIVE HIT (CPI relief, yields steady)
- Data-center REIT demand / rent upside: POSITIVE HIT (strong, hyperscaler capex $625-690B, vacancy low)
- REIT earnings season broad upside: POSITIVE
- Office vacancy / mark-to-market stress: NEGATIVE (20.5% vacancy, record CMBS delinquency)
- Refinancing wall stress: NEGATIVE (~$936B maturing 2026)
Net: positives (duration relief, data-center, earnings) offset by office/refinancing negatives. Slightly positive.

**S2_BREADTH (-2..+2): +1**
XLRE 1d rel +0.72%, 3d rel +0.99% — positive tape confirmation, inflection continuing. But still lagging on 1w -0.52%, 1m -2.89%. The 1d/3d positive relative returns suggest improving breadth/leadership. Data centers and senior housing leading. Improving short-term. Slightly positive.

**S3_FLOWS_POSITIONING (-2..+2): 0**
No clear REIT-specific flow data. XLRE 1d/3d positive relative returns suggest some rotation in. REITs as safe-haven/defensive bid in geopolitical risk context. Neutral-to-slightly-positive but no confirmed flow data.

**S4_ETF_TAPE (-1..+1): +1**
Confirmation only. XLRE 1d rel +0.72%, 3d rel +0.99% — positive tape confirmation, the inflection continuing from the cool CPI. This confirms the improving rate backdrop. Positive confirmation.

**Divergence:** Leading factors (duration relief from cool CPI, strong data-center demand, broad REIT earnings upside) are positive and the 1d/3d tape is now confirming (+0.72%/+0.99% rel). The 1w/1m lag is the residual of the prior rate-driven selloff. This is a positive inflection — the driver that caused the selloff (rising real yields) is now easing.

**Multiplier:** 1.0 (broad tape mixed-to-mildly-positive; REITs inflecting positive but still chronic laggards on 1w/1m).

**Confidence:** 0.6.

**Direction:** The cool CPI relieved the duration headwind, real yields are flat, and the 1d/3d tape is confirming positive. The active 08-12 lesson says don't default S0 negative on CPI-imminent days when easing expectations are visible — and now the CPI has printed cool, so the relief is materializing. I'll go **up/mild**.

Let me write the final output.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate — 2026-08-08 down/mild (ungraded), 2026-08-10 down/mild actual -1.29% (dir HIT), 2026-08-11 down/flat actual -0.72% (dir HIT, mag MISS), 2026-08-12 down/mild actual +0.93% (dir MISS), 2026-08-13 up/mild actual +1.42% (dir HIT). Rolling accuracy dir=0.75, mag=0.25 (n=4). Active lessons: (1) 08-11 active-geopolitical-oil-supply-risk-off — today oil up slightly but no Hormuz crisis, trigger not strongly firing; (2) 08-12 a-long-duration-rate-sensitive-sector-reits-has-been-lagging — CPI printed cool Aug 12, duration relief materializing, this now points POSITIVE; (3) 08-12 utilities lesson on capping magnitude when tech leads — futures flat-to-positive, not a strong tech-led rally. Applying the 08-12 REIT lesson: CPI is past and cool, so don't default S0 negative; the relief is materializing and the 1d/3d tape confirms.

---

## Real Estate (XLRE) — Sector Environment Analysis — 2026-08-14

### Channel 2 Research Summary

**1. Shared macro regime:** Broad tape is **mixed-to-mildly-positive** — ES premarket +0.06%, NQ +0.15% (flat-to-slightly-positive futures), Asia composite +0.22% (Kospi +2.42% strong but Hang Seng -1.1%, ASX -0.8% mixed), Europe +0.22%, VIX low at 14.56, Fear & Greed at 66.7 (Greed). Oil up slightly (+0.8% CL), gold up +1.0% (mild defensive bid). For REITs, the macro tape is dominated by rates, and the rate backdrop is now turning positive.

**2. Rates / real yields (dominant driver):**
- **10Y real yield (TIPS): 2.42%, 1d -0.01, 1w +0.01, 1m +0.09** — real yields FLAT-to-slightly-easing today, elevated over 1m. The duration headwind is not intensifying.
- **10Y nominal: 4.68%, 1d -0.02, 1w +0.05, 1m +0.10** — 10Y easing slightly today.
- **30Y: 5.24%, 1d +0.0, 1w +0.07, 1m +0.16** — long-end elevated.
- **CPI printed in-line/cool on Aug 12** — this is the second/third session of yield relief. The cool CPI eased Fed rate pressure, pushing yields down and rallying rate-sensitive REITs. This is the key positive driver.
- **5-day corr 10Y vs SPX: -0.493** — moderate negative; rates still a mild drag on equities but less than prior.
- **USD weakening** (DXY -0.23% 1d, -1.53% 1m) — mild positive for domestic REITs.

**3. Sector-specific factors (taxonomy):**
- **Rates falling / REIT duration relief** — HIT (positive, spine). CPI relief, yields steady, real yields flat.
- **Real yields flat/easing short-term** — positive shift (spine relief).
- **Data-center REIT demand / rent upside** — HIT (positive, dispersion). Hyperscaler capex $625-690B in 2026, data-center vacancy low, rents doubled since 2021. Data-center REITs outperforming traditional office REITs.
- **REIT earnings season broad upside** — positive (Hoya "meaningful earnings-season outperformance").
- **Office vacancy / mark-to-market stress** — HIT (negative). National office vacancy 20.5%, office CMBS delinquency record 12.34%.
- **Refinancing wall stress** — HIT (negative). ~$936B CRE loans mature in 2026, ~$2.6T through 2029.

**4. Breadth / leadership:** XLRE 1d rel +0.72%, 3d rel +0.99% — positive tape confirmation, the inflection continuing. But still lagging on 1w -0.52%, 1m -2.89%. Leadership is narrow (data centers, senior housing/Welltower) carrying the sector, but the short-term relative returns are improving.

**5. Flows / positioning:** No clear REIT-specific ETF flow data. XLRE 1d/3d positive relative returns suggest some rotation in. REITs as safe-haven/defensive bid in geopolitical risk context (Colliers: "REITs as a Safe Haven: Why Geopolitical Risk Could Drive 2026 Outperformance"). Neutral-to-slightly-positive.

**6. Earnings / policy catalysts:** Fed path is the key catalyst. CPI printed cool Aug 12, easing rate-hike fears. Data-center capex cycle strong. REIT earnings season broad upside.

### Channel 1 Confirmation
XLRE vs SPY: 1d rel +0.72%, 3d rel +0.99%, 1w rel -0.52%, 1m rel -2.89%. XLRE positive on 1d/3d (inflection), still lagging on 1w/1m. The 1d/3d positive relative returns confirm the improving rate backdrop.

### Section A: Regime
**A1.** mixed (broad tape mixed-to-mildly-positive; rate backdrop turning positive for REITs after cool CPI).
**A2.** Multiplier **1.0** — broad tape mixed-to-mildly-positive; REITs inflecting positive but still chronic laggards on 1w/1m.

### Section B: Component Scores

**S0_SHARED_MACRO (+1):** Real yields flat today, 10Y easing slightly, CPI printed cool Aug 12 relieving the duration headwind. Oil up slightly but no major geopolitical shock. USD weakening. Futures flat-to-positive. For REITs, the macro backdrop is turning positive.

**S1_SECTOR_FACTORS (+1):** Duration relief (positive spine), data-center demand strong (positive dispersion), REIT earnings upside (positive), offset by office vacancy and refinancing wall (negatives). Net slightly positive.

**S2_BREADTH (+1):** XLRE 1d/3d positive relative returns — inflection continuing. Still lagging on 1w/1m but improving short-term.

**S3_FLOWS_POSITIONING (0):** No confirmed REIT-specific flow data. Some rotation in suggested by 1d/3d relative strength. Neutral.

**S4_ETF_TAPE (+1):** Confirmation only. XLRE 1d rel +0.72%, 3d rel +0.99% — positive tape confirmation of the improving rate backdrop.

**Divergence:** Leading factors (duration relief, data-center demand, earnings upside) are positive and the 1d/3d tape is confirming. The 1w/1m lag is residual of the prior selloff. Positive inflection confirmed.

**Multiplier:** 1.0. **Confidence:** 0.6.

**Direction:** up, magnitude mild.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 1
MULTIPLIER: 1.0
CONFIDENCE: 0.6
REGIME: mixed
HORIZON_3D: up:mild:0.6
HORIZON_1W: up:mild:0.55
HORIZON_2W: flat:mild:0.5
HORIZON_1M: flat:mild:0.5
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates falling / REIT duration relief|HIT|0.7|2026-08-14|CPI relief, yields steady
Data-center REIT demand / rent upside|HIT|0.7|2026-08-14|Hyperscaler capex $625-690B
Office vacancy / mark-to-market stress|HIT|0.6|2026-08-14|20.5% vacancy, record CMBS delinquency
Refinancing wall stress|HIT|0.6|2026-08-14|~$936B maturing 2026
Real yields rising|MISS|0.5|2026-08-14|Flat today, elevated 1m
Sector rotation into REITs|PARTIAL|0.5|2026-08-14|1d/3d rel positive
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 1.0, 'leading_sum': 7.0, 'divergence_flagged': False, 'total_score': 7.5, 'predicted_direction': 'up', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.6, 'regime': 'mixed'}
```
