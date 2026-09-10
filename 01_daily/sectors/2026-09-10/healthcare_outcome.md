# Sector Outcome — Healthcare — 2026-09-10

Actuals: {'etf': 'XLV', 'pct': -0.5522860840633026, 'spy_pct': -0.5994238166152965, 'rel': 0.04713773255199394, 'open': 166.5, 'close': 165.66000366210938, 'source': 'yf_download'}

# Sector Post-Session Review — Healthcare / XLV — 2026-09-10

## 0. FACTS

**Tape (deterministic actuals):**
- XLV: **−0.552%** (open 166.50 → close 165.66)
- SPY: **−0.599%**
- Relative: **+0.047%** (XLV marginally outperformed)
- Path: opened at 166.50 (vs prior close 166.58), traded down through the session to close near the low at 165.66. MarketWatch real-time quote at 10:04 a.m. EDT showed $166.26 (−0.19%), so the bulk of the decline came in the afternoon — a slow bleed, not a gap-and-flush.

**Morning prediction:** down / mild, total_score −6.525, confidence 0.55, regime risk_off, divergence_flagged False.

**Verdict on the call:** **Direction HIT, magnitude HIT.** XLV closed down −0.55%, squarely inside the "mild" band (roughly −0.3% to −1.0%). The relative print (+0.05%) is essentially flat — XLV did not lead the tape down, it tracked SPY almost tick-for-tick.

---

## 1. What actually drove the sector

The dominant fact of the session is that **XLV was not a healthcare story — it was a beta story.** XLV −0.55% vs SPY −0.60% is a 5bp relative move. That is noise. The sector did not have an idiosyncratic driver in either direction; it was carried by the broad risk-off tape that the morning note correctly identified.

The morning framework's core mechanism — **oil above $100 Brent → inflation/duration unwind → crowded-long, duration-sensitive sector sells off** — is directionally consistent with what happened, but the *magnitude* of the sector-specific effect was much smaller than the framework implied. The framework scored S0 = −1.0, S1 = −1.0, S2 = −1.0, i.e. a strongly negative sector-specific setup. Reality delivered a sector that moved *with* the market, not *against* it. The correct read in hindsight is that the oil/duration channel was a **market-wide** drag, not a healthcare-specific one.

Evidence on the macro backdrop:

CLAIM: XLV opened at 166.50 on 2026-09-10 vs prior close 166.58, and closed at 165.66.
URL: https://finance.yahoo.com/quote/XLV/
PUBLISHED: 2026-09-10
QUOTE: "Previous Close 166.58; Open 166.50"
SUMMARY: Confirms a small gap-down open and a full-session grind lower — no single-session catalyst gap.

CLAIM: XLV was trading at $166.26 (−0.19%) at 10:04 a.m. EDT on 2026-09-10.
URL: https://www.marketwatch.com/investing/fund/xlv
PUBLISHED: 2026-09-10
QUOTE: "Last Updated: Sep 10, 2026 10:04 a.m. EDT Real time quote. $ 166.26. -0.32 -0.19%."
SUMMARY: The decline was back-loaded into the afternoon — consistent with a macro/beta drift rather than an opening-bell sector shock.

CLAIM: XLV traded between $166.26 and $167.87 on 2026-09-09.
URL: https://robinhood.com/us/en/stocks/XLV/
PUBLISHED: 2026-09-09
QUOTE: "On 2026-09-09, State Street Health Care Select Sector SPDR ETF(XLV) stock traded between a low of $166.26 and a high of $167.87."
SUMMARY: The 09-10 close of 165.66 broke below the prior session's low — a continuation of the multi-day downtrend the morning note flagged.

CLAIM: A sharp sector-wide healthcare selloff was noted in the tape around this period, with XLV-tracked sector falling.
URL: https://www.perplexity.ai/finance/KRYS
PUBLISHED: 2026-09-10 (approx.)
QUOTE: "Krystal Biotech shares closed down 1.95% at $351.26, broadly in line with a sharp sector-wide healthcare selloff (XLV-tracked sector fell ...)"
SUMMARY: Corroborates a sector-wide (not single-name) down day, though the magnitude described as "sharp" overstates the −0.55% XLV print — individual biotech names fell harder than the cap-weighted ETF.

**Taxonomy-aligned driver:** Shared macro / risk-off beta (S0), with a secondary duration drag on the biotech sleeve (S1). No fresh sector-fundamental catalyst fired.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = −1.0 — PARTIALLY CORRECT, OVERWEIGHTED.**
The direction was right: risk-off + oil-driven duration pressure did push XLV down. But the morning note framed this as a *sector-specific unwind trigger* ("crowded-long unwind candidate," "inflation scare hitting the crowded-long, duration-sensitive sector"). Reality: XLV's relative return was +0.05% — the sector did not unwind relative to the market. The oil/duration shock hit SPY (−0.60%) at least as hard as XLV. The correct S0 read was "shared macro drag, sector-neutral," which should have scored closer to −0.5 with the sector-specific component stripped out. The morning note explicitly claimed "no oil double-count into rotation" — but it *did* effectively double-count oil by scoring it as both a shared macro negative (S0) and a sector-specific unwind accelerant (S1/S2).

**S1_SECTOR_FACTORS = −1.0 — OVERWEIGHTED.**
The morning note's own reasoning was internally inconsistent. It conceded: (a) no fresh XBI leadership, (b) no same-morning mega-cap Rx headline, (c) ABBV/AMGN cluster is T+4/paid, (d) ABT FDA is single-ticker, (e) MA rates stale. That is a list of *absences*. Scoring −1.0 on a list of "nothing fresh happened" is scoring the *absence of a positive* as a *presence of a negative*. The duration drag on biotech was real but was already captured in S0. This is the clearest double-count in the morning stack.

**S2_BREADTH = −1.0 — OVERWEIGHTED.**
The morning note cited the metals co-move (Gold −0.56%, Silver −2.43%, Copper −2.89%) as evidence of "risk-asset liquidation, not a defensive pocket." But metals weakness is a *market-wide* signal, not a healthcare breadth signal. The note also admitted "no fresh XLV mega-cap breakdown confirmed premarket." Scoring −1.0 on breadth with no confirmed breadth failure is a forward-looking bet, not an observation. Reality: XLV's relative flatness suggests breadth was *not* failing — the sector held together.

**S3_FLOWS_POSITIONING = 0.0 — CORRECT.**
The note correctly identified that the crowded-long extension had unwound (1m rel +0.27% ≈ flat) and that there was no fresh inflow bid or outflow lid. Neutral was the right call. No adjustment needed.

**S4_ETF_TAPE = −0.5 — CORRECT AND WELL-CALIBRATED.**
The note's treatment of the 1d rel +0.14% stabilization as a *magnitude cap (mild), not a direction override* was exactly right. This was the single best-reasoned component of the morning stack, and it is what kept the call in the "mild" band rather than overshooting to "notable." The 09-09 reflect lesson it cited ("1d stabilization caps magnitude at mild") is validated again today.

**Net audit:** Direction correct, magnitude correct, but the *reasoning* was over-determined. Three components (S0, S1, S2) all scored −1.0 on what was substantially the same underlying shock (oil/risk-off), producing a leading_sum of −7.0 that implied a much more sector-specific negative than reality delivered. The call was right for partly the wrong reasons.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count identified:** Oil/risk-off was scored three times:
1. S0 as shared macro (−1.0)
2. S1 as "duration/risk-off drag on biotech sleeve" (−1.0)
3. S2 as "risk-asset liquidation" via the metals co-move (−1.0)

The morning note's self-audit claimed "oil scored once in S0 (unwind trigger), not re-scored in S1 as rotation." That claim is not supported by the text. S1's stated rationale — "Duration sleeve hit by oil-driven inflation/rates" — is explicitly oil-driven. S2's rationale — "risk-asset liquidation" — is the same shock. The self-audit was a *stated* discipline that the scoring did not actually honor.

**Knowable-at-open test:** The direction was knowable at open — risk-off regime, oil elevated, VIX backwardation, negative 10Y–SPX correlation. All of that was in hand premarket. What was *not* knowable at open was the **relative flatness**: nothing in the premarket data predicted XLV would track SPY within 5bp. The morning note's 3d/1w rel figures (−2.46%, −3.05%) suggested ongoing sector-specific underperformance; the 1d rel (+0.14%) suggested stabilization. The note chose to weight the multi-day lag as "live momentum" and score it negative. Reality: the 1d stabilization was the better signal, and the multi-day lag did not extend. **The knowable-at-open answer is "partially" — direction yes, relative magnitude no.**

**Interaction the morning note missed:** When a sector's 1m relative performance has fully mean-reverted to flat (+0.27%) *and* its 1d relative is positive, the base rate for a large sector-specific underperformance day is low. The note acknowledged the flat 1m but did not draw the inference that this *removes* the crowded-long accelerant entirely — it instead treated the flat 1m as neutral (S3 = 0) while still scoring S1/S2 as if the unwind were live. That is the internal inconsistency.

---

## 4. Outliers inside the sector

The cap-weighted XLV print (−0.55%) masks dispersion beneath the surface. The Krystal Biotech reference (KRYS −1.95%, "broadly in line with a sharp sector-wide healthcare selloff") indicates that **small/mid-cap biotech fell roughly 3–4x harder than the ETF**. This is the classic pattern: the duration-sensitive, unprofitable biotech tail takes the oil/rates hit, while large-cap pharma (which dominates XLV weights) absorbs it. The morning note's S1 rationale ("duration sleeve hit") was *correct for XBI*, but XLV is not XBI — the ETF's cap-weighting diluted the biotech drag to near-zero at the index level.

This is the single most important post-session lesson: **the morning note conflated the XBI duration story with the XLV tape.** The biotech sleeve did get hit; XLV did not, because biotech is a small weight. If the object had been XBI, the −1.0 S1 score would have been justified. For XLV, it was not.

---

## 5. Lessons for the framework

1. **Sector-specific vs shared-macro attribution.** When a shock (oil, rates, risk-off) is market-wide, it should be scored once in S0 and *not* re-scored in S1/S2 unless there is a confirmed sector-specific transmission channel (e.g., a healthcare-specific cost or revenue link to oil). "Duration-sensitive sector" is not healthcare-specific — it applies to tech, REITs, utilities, and consumer discretionary equally.

2. **Absence of a positive is not a negative.** S1 = −1.0 was justified by a list of stale/paid/absent catalysts. That should score 0, not −1.0. Reserve negative S1 for *fresh, sector-specific negatives*.

3. **The 1d-rel-stabilization-as-magnitude-cap rule worked again.** This is now validated across 09-09 and 09-10. Keep it.

4. **Cap-weighted ETF ≠ sector sleeve.** When the bearish thesis rests on a sub-sector (biotech) that is a minority weight in the ETF, discount the score by the weight. XLV's biotech exposure is roughly 15–20%; a −1.0 biotech drag should translate to roughly −0.2 at the ETF level.

5. **Flat 1m relative performance removes the crowded-long accelerant.** The morning note said this in S3 but did not propagate it to S1/S2. Propagate it.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -0.552
SPY_PCT: -0.599
REL_PCT: +0.047
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Broad risk-off / oil-driven duration pressure — a market-wide beta drag, not a healthcare-specific catalyst
KEY_INTERACTION: Oil/risk-off was triple-counted (S0 shared macro + S1 biotech duration + S2 metals co-move), over-determining a leading_sum of −7.0 when the actual sector-specific effect was near zero (rel +0.05%)
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction and magnitude HIT (down/mild, −0.55%), but for partly wrong reasons — the sector tracked SPY within 5bp, so the strongly negative S1/S2 sector-specific scores were not validated; S4's magnitude-cap logic was the best-calibrated component and saved the band
OUTCOME_END