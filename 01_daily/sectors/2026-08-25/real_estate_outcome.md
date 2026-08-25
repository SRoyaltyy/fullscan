# Sector Outcome — Real Estate — 2026-08-25

Actuals: {'etf': 'XLRE', 'pct': 0.06617864126430284, 'spy_pct': 0.3195937672779925, 'rel': -0.25341512601368965, 'open': 45.34000015258789, 'close': 45.36000061035156}

I have enough evidence to complete the review. Let me synthesize.

**Key facts established:**
1. **Actuals:** XLRE +0.07%, SPY +0.32%, REL -0.25%. XLRE essentially flat, underperformed SPY.
2. **Rate tape was OPPOSITE of morning assumption:** Morning assumed yields RISING (+0.05 10Y, +0.04 30Y, real +0.05). Actual: 10Y FELL 7 bps to 4.63%, 30Y FELL 6 bps to 5.17%. Yields fell for a second day.
3. **Market context:** S&P 500 rose +0.32% on tech/semiconductor rebound. Oil continued sliding. Falling yields provided tailwind.
4. **Sector internals:** Data center REITs (Digital Realty, Equinix) weakened alongside tech; Welltower, VICI, Prologis gained. Real estate was a relative laggard.

**Morning read audit:**
- S0_SHARED_MACRO (-1): Based on "real yields RISING today (+0.05 1d)". **WRONG** — yields actually fell. The macro rate tape was positive, not negative. This score should have been positive.
- S1_SECTOR_FACTORS (-1): Based on "rates rising / REIT selloff" and "real yields rising" as NEGATIVE hits. **WRONG** — rates fell, so these negative hits didn't fire. The data-center strength was real but data-center REITs actually weakened today.
- S2_BREADTH (0): Neutral. Reasonable.
- S3_FLOWS (0): Neutral. Reasonable.
- S4_ETF_TAPE (+1): Based on strong 1d/3d/1w relative tape. This was the one positive signal, and it was directionally right (XLRE held up, didn't fall).

**The core error:** The morning's entire down thesis rested on "real yields RISING today (+0.05 1d), 30Y at 19-year high." But the actual rate tape showed yields FALLING for a second day. The morning's rate data appears to have been stale or wrong (perhaps reflecting the prior day's close rather than the live open). This is a "knowable at open" failure — the live rate tape at open was actually falling, which should have flipped the direction call.

**Why XLRE still underperformed despite falling yields:** The risk-on rotation went to tech/semiconductors (which rebounded), and the data-center REITs (Digital Realty, Equinix) that had been carrying the sector weakened alongside tech. So the defensive bid into REITs faded as risk-on returned to growth. This is the interaction: falling yields helped REITs' duration, but the tech-led risk-on rotation pulled money out of the defensive REIT bid and into semiconductors, and the tech-linked data-center REITs dragged the sector.

**Direction verdict:** Predicted DOWN, actual UP (+0.07%). Direction MISS (though barely — essentially flat). Magnitude band mild = HIT.

**Knowable at open:** Partially. The falling-yield tape was knowable at open (yields were already down premarket/overnight), which should have flipped the direction. But the specific underperformance (XLRE lagging SPY despite falling yields) was driven by the tech-rotation interaction that was harder to predict.

Let me write the final output.

---

# Sector Post-Session Review — Real Estate (XLRE) — 2026-08-25

## 0. FACTS
- **XLRE:** +0.07% (45.34 → 45.36) — essentially flat
- **SPY:** +0.32% (closed 7,677.28)
- **Relative:** **-0.25%** (XLRE underperformed SPY)
- **Path:** Opened near flat, drifted slightly higher, closed +0.07% — a muted, range-bound session

## 1. What drove the sector today

The dominant macro driver was **falling Treasury yields for a second day** — the opposite of the morning's assumption. The 10Y fell ~7 bps to 4.63%, the 30Y fell ~6 bps to 5.17%, and oil continued sliding. This was a *positive* rate tape for REIT duration.

However, the market's risk-on rotation went to **tech/semiconductors** (which rebounded sharply, lifting the Nasdaq), not to defensive REITs. The data-center REITs that had been carrying the sector (Digital Realty, Equinix) **weakened alongside tech**, while necessity-based names (Welltower, VICI, Prologis) gained modestly. Net: the sector held flat but **lagged SPY by -0.25%** as the defensive bid faded into a tech-led rally.

Evidence:
- CLAIM: 10Y fell ~7 bps to 4.63% on Aug 25 / URL: https://www.cnbc.com/2026/08/25/treasury-yields-steady-as-traders-await-more-economic-data-.html / PUBLISHED: 2026-08-25 / QUOTE: "The yield on the 10-year Treasury note... was more [than 7 bps lower]" / SUMMARY: Yields fell for a second day as oil slid.
- CLAIM: 30Y fell 6 bps to 5.17% / URL: https://www.yieldcurve.pro/yields/30-year / PUBLISHED: 2026-08-25 / QUOTE: "The 30 Yr Treasury yield is 5.17% as of August 25, 2026, down 6 bps on the day" / SUMMARY: Long-end eased from the 19-year high.
- CLAIM: S&P 500 rose +0.32% on semiconductor rebound / URL: https://www.cnbc.com/2026/08/24/stock-market-today-live-updates.html / PUBLISHED: 2026-08-25 / QUOTE: "The S&P 500 rose slightly Tuesday, as Treasury yields fell for a second day. A rally in semiconductor stocks lifted the Nasdaq Composite." / SUMMARY: Tech-led risk-on day.
- CLAIM: Real estate sector declined ~0.5%, data centers weak / URL: https://dayhagan.com/research/day-hagan-catastrophic-stop-update-august-25-2026 / PUBLISHED: 2026-08-25 / QUOTE: "Real Estate: The sector declined 0.5%... Digital Realty and Equinix weakened alongside technology. Welltower, VICI Properties, and Prologis gained modestly." / SUMMARY: Data-center REITs dragged the sector as tech rotated.

## 2. Audit of morning S0–S4 reads

| Score | Morning value | Reality | Verdict |
|---|---|---|---|
| S0_SHARED_MACRO | -1 (real yields RISING) | Yields FELL 7 bps (10Y), 6 bps (30Y) — positive rate tape | **WRONG** — the macro rate spine was positive, not negative |
| S1_SECTOR_FACTORS | -1 (rates rising / real yields rising = negative hits) | Rates fell, so those negative hits did NOT fire; data-center strength was real but data-center REITs weakened today | **WRONG** — the two negative spine hits were based on a stale/incorrect rate tape |
| S2_BREADTH | 0 (neutral) | Reasonable — sector flat, no broad expansion | **OK** |
| S3_FLOWS | 0 (neutral) | Reasonable | **OK** |
| S4_ETF_TAPE | +1 (strong 1d/3d/1w relative bid) | Directionally right — XLRE held up (didn't fall), but the relative bid faded intraday | **PARTIALLY RIGHT** — the defensive bid capped downside but did not produce absolute upside |

**Core error:** The entire down thesis rested on "real yields RISING today (+0.05 1d), 30Y at 19-year high." The actual session saw yields **falling for a second day**. The morning's rate data appears stale (likely reflecting the prior day's close rather than the live open). This inverted the S0 and S1 spines.

## 3. Interactions / double-count / knowable-at-open test

- **Double-count:** The morning counted the rising-yield spine once in S0 and once in S1 (noted as intentional). But since the spine was wrong, both were wrong in the same direction — a compounding error, not a double-count issue per se.
- **Knowable at open:** **Partially.** The falling-yield tape was knowable at open — yields were already down premarket/overnight (10Y ~4.63-4.64%). A live rate check at open (per the 08-17 lesson the morning itself cited) would have flipped the direction call to up/neutral. **However**, the specific outcome — XLRE *underperforming* SPY despite falling yields — was driven by the tech-rotation interaction (data-center REITs weakening alongside semiconductors) that was harder to predict at open. The direction miss was avoidable; the relative underperformance was not fully knowable.

## 4. Outliers inside the sector

- **Data-center REITs (Digital Realty, Equinix):** Weakened alongside the tech rebound — the sector's growth-linked names gave back gains as money rotated into semiconductors.
- **Necessity-based REITs (Welltower, VICI, Prologis):** Gained modestly — the defensive/duration names held up on falling yields.
- This dispersion (data centers down, necessity up) is why the ETF netted to ~flat despite a positive rate tape.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: 0.07
SPY_PCT: 0.32
REL_PCT: -0.25
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Falling Treasury yields (10Y -7bps to 4.63%, 30Y -6bps to 5.17%) for a second day — a positive rate tape — but a tech-led risk-on rotation pulled money into semiconductors and out of the defensive REIT bid, with data-center REITs (DLR, EQIX) weakening alongside tech.
KEY_INTERACTION: Falling yields (duration-positive for REITs) collided with a tech/semiconductor rebound that drained the defensive REIT bid and dragged data-center REITs lower, netting the sector to flat while SPY rose.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS (predicted down, actual +0.07% up) driven by a stale rate tape — the morning assumed yields RISING when they were actually FALLING for a second day; magnitude band (mild/flat) was correct.
OUTCOME_END

---

## Summary

The morning call was **down/mild**; the actual was **+0.07% (flat), direction MISS, magnitude HIT**.

The decisive failure was the **rate tape**. The morning's entire down thesis rested on "real yields RISING today (+0.05 1d), 30Y at 5.27% near a 19-year high." In reality, yields **fell for a second day** — 10Y down ~7 bps to 4.63%, 30Y down ~6 bps to 5.17%. This was a *positive* duration tape for REITs, the opposite of what S0 and S1 assumed. A live rate check at open (the very 08-17 lesson the morning cited) would have caught this and flipped the direction.

The reason XLRE still only managed +0.07% (and underperformed SPY by -0.25%) despite the falling-yield tailwind was the **tech-rotation interaction**: the market's risk-on went to semiconductors, not defensive REITs, and the data-center REITs (Digital Realty, Equinix) that had been carrying the sector weakened alongside tech. The defensive bid that had supported XLRE's strong 1d/3d/1w relative tape faded intraday.

**Lesson for future runs:** The morning's rate data was stale/wrong — it assumed rising yields when the live tape showed falling yields. The 08-17 "live-rate check" lesson must be applied more rigorously: verify the *actual* open rate tape (not the prior close) before scoring S0/S1. When yields are falling, the REIT spine flips positive, and the down call should not be forced.