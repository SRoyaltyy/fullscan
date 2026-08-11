# Sector Prediction — Real Estate — 2026-08-11

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **flat**
- total_score: **-2.25** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-10):
  1d: XLRE -1.29% | SPY -0.03% | rel -1.26%
  3d: XLRE -1.77% | SPY +0.42% | rel -2.19%
  1w: XLRE -1.73% | SPY +2.03% | rel -3.75%
  1m: XLRE -0.11% | SPY +2.39% | rel -2.51%
```

I have enough data. Let me analyze the key inputs.

**Key Channel 1 data:**
- **DFII10 (real yield): 2.40%, 1d -0.03, 1w -0.07, 1m +0.09** — real yields EASING over 1d/1w but still elevated over 1m. This is a shift from prior runs where real yields were RISING.
- **DGS10: 4.65%, 1d -0.04, 1w -0.10, 1m +0.11** — 10Y easing over 1d/1w.
- **DGS30: 5.19%, 1d -0.03, 1w -0.08, 1m +0.14** — easing short-term.
- **VIX 15.54** (low, calm), Fear & Greed 66.3 (Greed)
- **ES premarket +0.16%, NQ +0.36%** — risk-on futures
- **Asia composite +0.22%, Europe +0.22%** — mildly risk-on
- **5-day corr 10Y vs SPX: -0.843** — rising rates drag equities
- **XLRE vs SPY: 1d rel -1.26%, 3d rel -2.19%, 1w rel -3.75%, 1m rel -2.51%** — XLRE sharply lagging

**Key change vs prior runs:** Real yields and nominal rates are now EASING over 1d/1w (DFII10 -0.07 1w, DGS10 -0.10 1w). This is a shift toward duration relief — the spine negative is weakening. However, 1m still shows +0.09/+0.11 (elevated).

**Sector factors:**
- Real yields easing short-term (positive shift) but elevated 1m (still negative backdrop)
- Data-center REIT demand strong (positive dispersion)
- REIT earnings season broad upside (58 of 98 raised guidance, only 4 lowered) — positive
- Office vacancy multi-decade highs, refinancing wall stress — negative
- XLRE lagging SPY across all timeframes — negative confirmation

Let me finalize scoring.

**S0_SHARED_MACRO (-2..+2): 0**
Real yields easing 1d/1w (DFII10 -0.07 1w) — a positive shift toward duration relief. But 1m still +0.09 (elevated). 10Y easing 1w. Broad tape risk-on (ES +0.16%, NQ +0.36%, Asia/Europe positive, VIX low, Fear&Greed Greed). The easing real yields are a mild positive for REITs, but the 1m trend is still elevated. Net neutral-to-slightly-positive.

**S1_SECTOR_FACTORS (-3..+3): 0**
- Real yields easing short-term (positive shift, but 1m still elevated) — mixed
- Data-center REIT demand / rent upside: POSITIVE HIT (strong, vacancy 1.3%, rents doubled)
- REIT earnings season broad upside: POSITIVE (58/98 raised guidance)
- Office vacancy / mark-to-market stress: NEGATIVE (multi-decade highs)
- Refinancing wall stress: mild negative
Net: positives (data-center, earnings) offset by office/refinancing negatives and still-elevated real yields. Neutral.

**S2_BREADTH (-2..+2): -1**
XLRE lagging SPY across all timeframes (1w -3.75%, 1m -2.51%). Narrow leadership (data centers, senior housing) carrying the sector. Breadth failure pattern.

**S3_FLOWS_POSITIONING (-2..+2): 0**
No clear REIT-specific flow data. XLRE trades at elevated 36.1 forward P/E (expensive). Neutral.

**S4_ETF_TAPE (-1..+1): -1**
Confirmation only. XLRE lagging across all timeframes, especially 1w -3.75% and 1d -1.26%. Negative confirmation.

**Divergence:** Leading factors (real yields easing short-term, strong data-center demand, broad REIT earnings upside) are positive and not fully reflected in the tape, which is still lagging. This is a mild positive divergence — the tape may be oversold relative to improving fundamentals. But the 1m real yield trend is still elevated, capping the upside.

**Multiplier:** 0.9 (risk-on broad tape, but REITs still laggards; easing real yields are a mild tailwind).

**Confidence:** 0.6.

**Direction:** The easing real yields and strong earnings are positives, but the tape is still sharply lagging and 1m real yields remain elevated. Given the divergence between improving fundamentals and still-negative tape, I lean toward flat-to-slightly-down. The prior two runs predicted down/mild (one hit). The easing real yields short-term argue for a less negative call. I'll go with **flat** — the improving real-yield trend and strong earnings offset the lagging tape.

Actually, let me reconsider. The real yields easing 1d/1w is a meaningful shift. Combined with strong earnings and data-center demand, this argues for a neutral-to-slightly-positive setup. But the tape is still sharply lagging (1w -3.75%) and 1m real yields elevated. The divergence is real but not strong enough to flip to up. I'll call **flat** with mild magnitude.

Let me set scores: S0=0, S1=0, S2=-1, S3=0, S4=-1. Total = -2. With multiplier 0.9 → -1.8. That's a mild negative, closer to flat. Direction flat/mild.

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate — 2026-08-08 predicted down/mild (ungraded), 2026-08-10 predicted down/mild, actual -1.29% (dir HIT). Rolling accuracy dir=1.0, mag=0.0 (n=1). Standing lessons: mega-cap-earnings-over-macro-drag (general scope, not directly applicable to REIT sector call); ops-missing-predict-file (ops scope). No REIT-specific lesson yet.

---

## Real Estate (XLRE) — Sector Environment Analysis — 2026-08-11

### Channel 2 Research Summary

**1. Shared macro regime:** Broad tape is **risk-on** — ES premarket +0.16%, NQ +0.36%, Asia composite +0.22%, Europe +0.22%, VIX low at 15.54, Fear & Greed at 66.3 (Greed). HY spread tight at 2.70. However, this is a **mild headwind for REITs**, which lag in risk-on when real yields are elevated. The macro tape for this sector is dominated by rates.

**2. Rates / real yields (dominant driver):**
- **10Y real yield (TIPS): 2.40%, 1d -0.03, 1w -0.07, 1m +0.09** — real yields EASING over 1d/1w (a shift from prior runs where they were rising), but still elevated over 1m. This is a **positive shift toward duration relief** for REITs.
- **10Y nominal: 4.65%, 1d -0.04, 1w -0.10, 1m +0.11** — easing short-term, elevated 1m.
- **30Y: 5.19%, 1d -0.03, 1w -0.08, 1m +0.14** — easing short-term.
- **5-day corr 10Y vs SPX: -0.843** — strongly negative; rising rates drag equities, especially duration-sensitive REITs. But rates are easing short-term.
- **Fed:** held at 3.50–3.75% for a 5th straight meeting in July; markets had priced ~1/3 chance of a HIKE. Soft July labor data reduced hike odds and lifted December rate-cut probability (from prior context, ~80%). Forward easing bias building.

**3. Sector-specific factors (taxonomy):**
- **Real yields easing short-term** — POSITIVE shift (spine relief), but 1m still elevated.
- **Data-center REIT demand / rent upside** — HIT (positive, dispersion). US data-center vacancy declined from 10.2% (2019) to just 1.3% (2025); market rents doubled since 2021. Data-center REITs returned ~13.75% annually over the past decade. BTIG initiated Buy ratings on Digital Realty and Equinix citing AI investment.
- **REIT earnings season broad upside** — POSITIVE. 58 of 98 equity REITs providing full-year FFO guidance RAISED outlooks; only 4 lowered (Hoya Capital). "One of the strongest earnings seasons in recent years."
- **Office vacancy / mark-to-market stress** — NEGATIVE. Office vacancy at multi-decade highs (~18-20% in major cities); CRE refinancing wall being "extend-and-pretended."
- **Refinancing wall stress** — mild negative. 2026 CRE maturity wall ~$875B (smaller than 2025's $957B).

**4. Breadth / leadership:** XLRE is the **7th-strongest US sector**, trailing SPY by a wide margin on 12-month score. Chronic underperformance since 2022 on "higher-for-longer" rates. Leadership is **narrow** — data centers and senior housing (Welltower) carrying the sector; broader REITs lag. Breadth-failure pattern.

**5. Flows / positioning:** No clear REIT-specific flow data. XLRE trades at an **elevated 36.1 forward P/E** (expensive) while facing a macro headwind from elevated rates. Morningstar data shows cautious inflows in 2024-2025 as yield-hungry investors returned. Positioning neutral-to-unclear.

**6. Earnings / policy catalysts:** Strong REIT earnings season underway (~200 US REITs reporting). Fed path is the key catalyst — December cut odds rising, which would be duration relief for REITs.

### Channel 1 Confirmation
XLRE vs SPY: 1d rel -1.26%, 3d rel -2.19%, **1w rel -3.75%**, 1m rel -2.51%. XLRE lagging across all timeframes, with a sharp 1-week underperformance. Negative confirmation.

### Section A: Regime
**A1.** mixed (risk-on broad tape, but REITs still face elevated 1m real yields; short-term easing is a positive shift).
**A2.** Multiplier **0.9** — risk-on broad market is a mild headwind for REITs, but easing real yields are a partial offset.

### Section B: Divergence
**Leading divergence:** Real yields easing short-term (DFII10 -0.07 1w), strong data-center demand, and broad REIT earnings upside are positives the lagging tape hasn't fully priced. This is a mild positive divergence — the tape may be oversold relative to improving fundamentals. However, the 1m real yield trend is still elevated (+0.09), capping the upside. Not strong enough to flip to up, but enough to pull the call from down to flat.

### Final Scoring
- **S0_SHARED_MACRO: 0** — Real yields easing 1d/1w (positive shift) but 1m still elevated; broad risk-on is a mild headwind for REITs. Net neutral.
- **S1_SECTOR_FACTORS: 0** — Data-center demand (strong) and broad REIT earnings upside offset office vacancy/refinancing stress and still-elevated 1m real yields. Net neutral.
- **S2_BREADTH: -1** — XLRE lagging SPY across all timeframes (1w -3.75%), narrow leadership (data centers, senior housing). Breadth failure.
- **S3_FLOWS_POSITIONING: 0** — No clear REIT-specific flow data; elevated P/E noted. Neutral.
- **S4_ETF_TAPE: -1** — Confirmation only. XLRE lagging across all timeframes. Negative confirmation.

**Total: -2** (mult 0.9 → -1.8). Direction **flat**, magnitude **mild**.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.6
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Real yields rising|miss|0.6|2026-08-11|DFII10 2.40%, easing 1d/1w but +0.09 1m
Data-center REIT demand / rent upside|hit|0.8|2026-08-11|vacancy 1.3%, rents doubled since 2021
Rates rising / REIT selloff|miss|0.6|2026-08-11|10Y easing 1d/1w, elevated 1m
Office vacancy / mark-to-market stress|hit|0.7|2026-08-11|multi-decade highs ~18-20%
Refinancing wall stress|hit|0.6|2026-08-11|2026 maturity wall ~$875B
Sector breadth failure (ETF up, names flat)|hit|0.6|2026-08-11|XLRE lagging SPY 1w -3.75%
Sector rotation out of real estate|hit|0.5|2026-08-11|XLRE chronic underperformer
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -2.25, 'predicted_direction': 'down', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.6, 'regime': 'mixed'}
```
