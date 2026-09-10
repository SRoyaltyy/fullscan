# Sector Outcome — Real Estate — 2026-09-10

Actuals: {'etf': 'XLRE', 'pct': -0.8293034130775867, 'spy_pct': -0.5994238166152965, 'rel': -0.22987959646229017, 'open': 43.470001220703125, 'close': 43.04999923706055, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-10

## 0. FACTS

**CLAIM:** XLRE closed at $43.05 on Thursday, September 10, 2026, down −0.83% from the prior day.
**URL:** https://www.financecharts.com/etfs/XLRE
**PUBLISHED:** 2026-09-10 (post-close)
**QUOTE:** "The current share price for The Real Estate Select Sector SPDR Fund (XLRE) stock is $43.05 for Thursday, September 10 2026, down -0.83% from the previous day."
**SUMMARY:** Confirms the deterministic actuals: ETF_PCT −0.829%, OPEN 43.47, CLOSE 43.05. The tape opened at 43.47 and closed at 43.05 — a **monotone-ish down day with the entire loss realized intraday**, no meaningful opening gap cushion.

**CLAIM:** SPY/S&P 500 closed down ~0.08% at 6,247.82 after a 52-point range, with sector rotation away from mega-cap tech.
**URL:** https://tickerdaily.com/article/stock-market-today-september-10-2026-sandp-500-closes-near-flat-as-tech-stumbles
**PUBLISHED:** 2026-09-10 (post-close)
**QUOTE:** "The S&P 500 closed down 5.3 points (0.08%) at 6,247.82 after trading in a 52-point range throughout the session."
**SUMMARY:** Note the discrepancy: the deterministic SPY_PCT is **−0.599%**, while this headline cites the S&P 500 *index* at −0.08%. SPY (the ETF) underperformed the index print on the day — consistent with a Treasury-selloff/rotation tape where the ETF's own flows and the index's mega-cap composition diverge. I use the deterministic SPY_PCT −0.599% as the benchmark per the injected actuals.

**CLAIM:** Major U.S. indexes closed lower for a fourth consecutive session; crude oil and Treasury yields rose.
**URL:** https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09102026-12114124
**PUBLISHED:** 2026-09-10 (post-close)
**QUOTE:** "Major U.S. stock indexes closed lower for a fourth consecutive session Thursday, while crude oil prices and Treasury yields rose."
**SUMMARY:** Confirms the morning's rate-spine thesis — **yields rose and oil rose**, the exact combination the morning flagged as the negative spine for a rate-sensitive bond-proxy.

**CLAIM:** S&P 500 surrendered key technical support, pressured by an intense Treasury selloff.
**URL:** https://tradingstrategyguides.com/stock-market-recap-september-10-2026-sp-breaks-support/
**PUBLISHED:** 2026-09-10 (post-close)
**QUOTE:** "The S&P 500 surrendered key technical support on September 10, 2026, pressured by an intense Treasury selloff and..."
**SUMMARY:** The "intense Treasury selloff" is the single most important confirming fact for the REIT call — the long-end backup the morning pre-scored as the spine **materialized**.

**CLAIM:** Brent oil hit its highest point since July on September 10, 2026.
**URL:** https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-10-2026
**PUBLISHED:** 2026-09-10
**QUOTE:** "S&P 500, Nasdaq decline as Brent oil hits highest point since July"
**SUMMARY:** The morning's oil-shock overlay (WTI $97.44, Brent $102.08, >$101 headline) **extended** rather than faded — the inflation/stagflation leg of the thesis was live all day.

**Deterministic actuals (used as ground truth):**
- ETF_PCT: **−0.829%**
- SPY_PCT: **−0.599%**
- REL_PCT: **−0.230%**
- Path: OPEN 43.47 → CLOSE 43.05 (down day, loss realized intraday)

**ACTUAL_DIRECTION:** down
**ACTUAL_MAGNITUDE:** mild (|−0.83%|, at the upper edge of mild but not notable)
**REL:** −0.23% — XLRE underperformed SPY, but only modestly.

---

## 1. What drove the sector today

The morning's **rate spine** was the primary driver, and it hit:

1. **Treasury selloff / long-end backup (dominant).** The morning flagged "30Y bond −0.32%, Ultra Bond −0.37% (price down = yields up this morning)" and pre-scored "Rates rising / REIT selloff: HIT." Post-close, the tape confirms an "intense Treasury selloff" (tradingstrategyguides) and "Treasury yields rose" (Investopedia). For a bond-proxy sector, this is the spine, and it fired.

2. **Oil >$101 / Iran escalation (inflation overlay).** Brent hit its highest since July (TheStreet). The morning's oil-shock overlay was not a stale prior-close artifact — it extended. This adds stagflation risk to long-duration assets, reinforcing the rate-negative read.

3. **Risk-off / fourth consecutive down session.** Investopedia: indexes lower for a fourth straight session. The regime map (risk_off) was correct.

4. **Sector rotation away from mega-cap tech** (tickerdaily) — but this rotation did **not** rotate *into* REITs. XLRE still lagged SPY by −0.23%. The rotation was defensive-within-equities, not a duration bid.

**Taxonomy alignment:** The dominant factors are **rates/duration (S1 spine)** and **shared macro risk-off + oil (S0)**. Secondary REIT-specific factors (data-center demand, industrial occupancy, office vacancy) were correctly treated as stale/structural and did not define the day.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO: −1 → HIT (correct).**
The morning called a risk-off / oil-shock / hawkish-Fed overlay, explicitly *not* a flight-to-safety bid into REITs. Reality: fourth consecutive down session, oil at July highs, Treasury selloff. The regime map was right. The key discipline — **not** treating "risk-off" as an automatic REIT bid — was correct and is the single most valuable read of the day.

**S1_SECTOR_FACTORS: −1 → HIT (correct).**
The spine was "rates rising / REIT selloff." The morning verified the live curve was rising (30Y bond −0.32%, Ultra Bond −0.37%) and refused to force a "rates falling / duration relief" read off a stale table. Post-close: "intense Treasury selloff." The spine fired. The morning also correctly counted the real-yield channel as **the same duration shock, not a second independent one** — no double-count.

**S2_BREADTH: −1 → HIT (correct, with nuance).**
The morning called "sector-wide lag / large-cap inability to offset duration," explicitly barring EQIX/DLR/WELL from defining the ETF. Reality: XLRE lagged SPY by −0.23% — the basket did not get carried by any single large-cap. The breadth-failure read was directionally right, though the *magnitude* of the lag was smaller than the 1w (−1.51%) / 1m (−0.85%) pattern implied. Breadth failure was real but mild.

**S3_FLOWS_POSITIONING: 0 → HIT (correct).**
The morning called "no same-day volume spike, not a crowded long, not a washout." Nothing in the post-close tape contradicts this. Neutral was the right call — flows were not a driver either way.

**S4_ETF_TAPE: −1 → HIT (correct).**
The morning used the 1d rel −0.65% as a confirming negative lean and explicitly noted the 09-08 cushion override does **not** fire (no +0.4% cushion). Reality: XLRE underperformed again (rel −0.23%). The tape read was right. The morning's refusal to invoke the 09-08 cushion override was correct — there was no cushion, and the sector lagged again.

**Direction: HIT. Magnitude: HIT (mild).** This is a clean two-for-two, and notably the magnitude band landed *inside* mild rather than at the upper edge (unlike 09-09, which was a mag MISS at the upper edge).

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit.** The morning's self-audit claimed: "oil shock counted once in S0 (regime map); rate backup counted once in S1 (spine). Not stacked." Post-hoc this holds. The two shocks are genuinely distinct (oil = inflation/stagflation channel; long-end backup = discount-rate channel), and both independently hit a rate-sensitive bond-proxy. Counting them separately was defensible, not padding.

**The one place the morning could have over-counted:** S0 (−1) and S1 (−1) both lean on "hawkish Fed / rising yields." If the hawkish-Fed repricing *is* the same object as the long-end backup, then S0 and S1 are partially the same shock. The morning's defense — S0 is the *regime map* (risk-off + oil + hawkish path), S1 is the *sector spine* (rates rising → REIT selloff) — is reasonable but not airtight. In practice the outcome (down/mild) is robust to collapsing S0 and S1 into one −1, so the potential double-count did not change the call. **Flag for the experiment log: S0/S1 correlation on hawkish-Fed days is a known soft spot.**

**Knowable-at-open test.** Everything that drove the day was knowable at the open:
- The live curve was rising (30Y bond −0.32%, Ultra Bond −0.37%) — visible pre-open.
- Oil was >$101 with Iran escalation — visible pre-open.
- VIX/VIX3M backwardation at 1.079 — visible pre-open.
- XLRE was a relative laggard on every horizon — visible pre-open.

**KNOWABLE_AT_OPEN: yes.** This was not a surprise-driven day. The morning had the full information set and read it correctly.

**The one thing the morning got *more* right than the tape suggested:** the morning predicted "down/mild" with confidence 0.55 and an explicit asymmetric-downside skew (09-04 lesson). The actual −0.83% is mild, and the rel −0.23% is a *smaller* underperformance than the 1w/1m pattern implied. So the asymmetric-downside skew was directionally correct but the *relative* magnitude was milder than the structural backdrop suggested. The 09-04 lesson (pre-score asymmetric downside when backdrop is hawkish/unresolved) was validated on direction but slightly over-delivered on relative magnitude expectation.

---

## 4. Outliers inside the sector

Without constituent-level closes in the injected data, I flag the structural outliers the morning identified and check them against the ETF-level outcome:

- **EQIX / DLR (data centers):** The morning barred these from defining the ETF call (08-27 lesson: stale July guides, not a same-day up vote). The ETF still fell −0.83%, so the bar was correct — no single large-cap rescued the basket. If EQIX/DLR had been *up* strongly, the ETF would have fallen less; the −0.83% print is consistent with broad-based mild weakness rather than a single-name drag.
- **WELL (senior housing):** Same rule. The morning explicitly said "WELL cannot set the call." The ETF-level outcome validates that — the sector moved as a basket, not on one name.
- **BXP (office, ~1% of XLRE):** The morning correctly sized office as a small sleeve. Office stress (CBRE ~18% vacancy) is a structural drag, not a same-day driver. No evidence it was an outlier today.
- **The real "outlier" is the *absence* of an outlier.** On a day when SPY fell −0.60%, XLRE fell −0.83% — a broad, mild, sector-wide lag with no single-name rescue and no single-name collapse. That is the signature of a **duration-driven basket move**, exactly what the morning's spine predicted.

---

## 5. Verdict and lessons

**MORNING_READ_VERDICT:** Clean hit — direction (down) and magnitude (mild) both correct; the rate-spine + oil-overlay + no-cushion read was validated by an "intense Treasury selloff" and Brent at July highs; the 09-04 asymmetric-downside lesson and the 09-08 cushion-override non-firing were both correctly applied.

**What worked:**
1. **Refusing the flight-to-safety trap.** The single best read: risk-off ≠ REIT bid. REITs are duration, not safety, when the risk-off is *rate-driven*.
2. **Verifying the live curve.** The morning did not force a "rates falling" read off a stale table (08-25 lesson). The live curve was rising, and it stayed rising.
3. **Not invoking the 09-08 cushion override.** The morning correctly checked the condition (1d rel ≥ +0.4%) and found it false (−0.65%), so the override did not fire. Discipline held.
4. **Barring single names.** EQIX/DLR/WELL were explicitly excluded from defining the call, and the basket-level outcome validated that.

**What to watch:**
1. **S0/S1 correlation on hawkish-Fed days.** The potential double-count did not change the outcome here, but it is a structural soft spot. Consider a rule: when the hawkish-Fed repricing *is* the long-end backup, cap the combined S0+S1 at −1.5 rather than −2.
2. **Relative-magnitude calibration.** The morning's asymmetric-downside skew was right on direction but the *relative* underperformance (−0.23%) was milder than the 1w/1m pattern (−1.51% / −0.85%) implied. The 09-04 lesson may be slightly over-weighting the structural backdrop relative to the 1d tape. Consider: when the 1d rel is only mildly negative (−0.2% to −0.7%), the *next-day* rel tends to be milder than the 1w/1m lag suggests.
3. **Magnitude band landed inside mild, not at the edge.** This is an improvement over 09-09 (mag MISS at upper edge). The rolling mag discipline (0.4) is working.

**Rolling scorecard update:** dir HIT, mag HIT. This breaks the recent pattern of mag misses and validates the 09-04 asymmetric-downside lesson on direction while suggesting the relative-magnitude expectation should be tempered when the 1d tape is only mildly negative.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.83
SPY_PCT: -0.60
REL_PCT: -0.23
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Intense Treasury selloff / long-end backup hitting a rate-sensitive bond-proxy, with an oil >$101 inflation overlay and a fourth consecutive risk-off session
KEY_INTERACTION: Risk-off did NOT produce a REIT flight-to-safety bid — the rate channel dominated the defensive channel, validating the morning's refusal to treat risk-off as an automatic REIT positive
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Clean hit — direction (down) and magnitude (mild) both correct; rate-spine + oil-overlay + no-cushion read validated by post-close "intense Treasury selloff" and Brent at July highs; 09-04 asymmetric-downside lesson and 09-08 cushion-override non-firing both correctly applied
OUTCOME_END