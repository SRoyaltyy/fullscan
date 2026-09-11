# Sector Outcome — Energy — 2026-09-11

Actuals: {'etf': 'XLE', 'pct': 0.32342381562551203, 'spy_pct': 0.8524287494320992, 'rel': -0.5290049338065872, 'open': 64.88999938964844, 'close': 65.13999938964844, 'source': 'yf_download'}

# Sector Post-Session Review — Energy (XLE) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLE: **+0.32%** (open 64.89 → close 65.14)
- SPY: **+0.85%**
- Relative: **−0.53%** (XLE underperformed SPY)
- Actual direction: **up** (absolute), **down** (relative)
- Actual magnitude: **flat/mild** — a +0.32% absolute move is a sub-half-percent drift, not a directional session

**Path:** XLE opened at 64.89 and closed at 65.14 — a narrow, low-amplitude grind higher. No gap-and-fade, no trend day. The ETF spent the session essentially flat-to-slightly-up while SPY ran +0.85%.

**The critical fact:** XLE was **up in absolute terms but down on relative**. The morning call was **down/mild**. On the absolute axis the call is a **direction MISS**. On the relative axis (XLE vs SPY) the call is a **direction HIT** — XLE did underperform by −0.53%.

This is the central tension of the review, and it must be adjudicated honestly rather than resolved in the prediction's favor.

**Oil (the sector's own object):**
- CLAIM: WTI traded around $100.36 and Brent around $104.42 on Sep 11, 2026.
- URL: https://convextrade.com/metrics/wti ; https://convextrade.com/metrics/brent
- PUBLISHED: 2026-09-11
- QUOTE: "WTI Crude Oil at $100.36 as of Sep 11, 2026." / "Brent Crude Oil at $104.42 as of Sep 11, 2026."
- SUMMARY: The morning snapshot had WTI $99.91 / Brent $104.62. By the close, WTI was ~$100.36 (slightly higher) and Brent ~$104.42 (roughly flat). **The barrel did not collapse.** The morning's "decisively offered" crude print was a modest dip that did not extend — and by some measures reversed.

- CLAIM: WTI futures opened at $104.02/bbl and Brent at $108.92/bbl on Sep 11, 2026.
- URL: https://www.forbes.com/advisor/investing/oil-prices-today/
- PUBLISHED: 2026-09-11
- QUOTE: "West Texas Intermediate (WTI) futures opened at $104.02 per barrel (bbl) on September 11, 2026. Brent crude opened at $108.92 per barrel."
- SUMMARY: This source conflicts materially with the Convex print and with the morning snapshot. The discrepancy is large enough that I treat the Forbes figure as unreliable for this review (likely a different contract month, a stale page, or a data error). The Convex prints are internally consistent with the morning snapshot's $99.91/$104.62 and are the better anchor. **Flagging the conflict rather than silently choosing.**

**Net factual picture:** XLE drifted up +0.32%, lagged a +0.85% SPY, and the barrel was roughly flat-to-marginally-higher, not collapsing.

---

## 1. What actually drove the sector

The morning thesis was "CPI-day risk-on setup with the barrel offered." The reality split that thesis in half:

**(a) The risk-on half was correct and dominant.** SPY +0.85% on CPI day is a clean risk-on tape. The morning read of ES +0.63% / NQ +0.65% / Russell +0.63% was directionally right, and the equity beta expansion carried into the close. This is the **Risk-on tape / equity beta expansion** factor — which the morning HIT_GRID marked **MISS**. That grid call was wrong: risk-on was the single largest driver of the session, and it lifted XLE's absolute print.

**(b) The "barrel offered" half did not transmit.** The morning's core S1 premise was a decisive oil-offered print (WTI −2.54%, CL=F −2.78%). By the close, WTI was ~$100.36 — **higher than the morning snapshot**. The offered barrel did not extend, and the sector's own object stopped pushing down. The **Crude price collapse** factor, marked HIT at 0.80 in the morning grid, did **not** materialize as a collapse. It was a dip that held.

**(c) The relative underperformance is real but small.** XLE lagged SPY by −0.53%. That is consistent with a commodity sector failing to fully participate in a risk-on equity day when its own underlying (crude) is not confirming. This is the **crowded-long / exhaustion** mechanism partially working — but at a fraction of the magnitude the morning implied.

**Primary driver:** Risk-on equity beta (SPY +0.85%) lifted XLE absolutely, while a non-confirming barrel capped XLE's participation, producing a small relative lag.

---

## 2. Audit of morning S0–S4 reads

**S0_SHARED_MACRO = 0.** Morning reasoning: risk-on futures are "a mild headwind for a commodity sector when the barrel is offered," so keep S0 muted. **Verdict: under-scored.** On a CPI risk-on day, equity beta was a *tailwind* for XLE's absolute print, not a headwind. The morning correctly identified the risk-on tape but assigned it the wrong sign for a sector that trades with the broad market on beta days. S0 should arguably have been **+0.5** (mild positive) given the tape, or at minimum the sign should not have been treated as a headwind. This is a **sign error on a knowable input** — futures were green at the snapshot.

**S1_SECTOR_FACTORS = −1.** Morning reasoning: live oil-offered print, scored once, geo not double-counted, EIA draw as floor. **Verdict: directionally wrong on the session.** The offered barrel did not extend; WTI finished ~$100.36 vs the $99.91 morning print. The −1 was a bet that the offered print would persist and transmit. It didn't. The discipline of scoring oil once was correct; the **sign** was wrong because the print reversed. Note the morning itself flagged this as a "leading-vs-tape divergence" — it knew the tape wasn't confirming.

**S2_BREADTH = 0.** Morning reasoning: 1d rel stalled to +0.02%, don't re-vote 3d/1w/1m. **Verdict: correct and well-disciplined.** The 1d stall was the right read; the session produced a small relative lag, consistent with stalled breadth. This was the best-scored component.

**S3_FLOWS_POSITIONING = −0.5.** Morning reasoning: record-close sequence + 1m rel +8.22% = crowded-long unwind risk. **Verdict: partially validated.** XLE did lag SPY, consistent with crowded-long fatigue — but there was no unwind, just a −0.53% relative drift. The mechanism fired weakly. −0.5 was a reasonable magnitude for what actually happened.

**S4_ETF_TAPE = 0.** Morning reasoning: 1d rel +0.02% is flat, not confirming up. **Verdict: correct.** The tape was flat and stayed flat-ish. No double-count with S2.

**Score arithmetic check:** The prose says total = (0 + (−1) + 0 + (−0.5) + 0) × 0.9 = −1.35. The pipeline JSON says total_score = −3.375 with leading_sum = −3.0. **These disagree.** The prose's own components sum to −1.5, not −3.0. The pipeline's −3.375 implies a leading_sum of −3.75 before the 0.9 multiplier — which matches none of the stated components. This is an internal inconsistency in the morning artifact that I flag but cannot resolve from the inputs. The **stated** prediction (down/mild) is what I grade against.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count check:** The morning correctly avoided scoring oil + Hormuz twice, and correctly refused to re-vote 3d/1w/1m into S2. That discipline held. No material double-count in the final score.

**The real interaction error:** S0 and S1 were scored as if they pointed the same way (both neutral-to-down for a commodity sector). In reality they **offset**: risk-on equity beta pushed XLE up while the offered barrel pushed it down. The morning treated the risk-on tape as a headwind; it was a tailwind. Had S0 been scored +0.5 and S1 kept at −1, the net would have been closer to flat — which is what happened.

**Knowable-at-open test:** The single most important input — **green futures on a CPI risk-on setup** — was fully knowable at the open and was in the morning snapshot. The morning saw it and assigned it the wrong sign. The barrel's morning dip was also knowable, but its *persistence* was not. So:
- The **absolute up-move** was **knowable at open** (green futures → risk-on → XLE up).
- The **relative lag** was **partially knowable** (crowded-long + non-confirming barrel → underperform).
- The **magnitude** (flat/mild) was **knowable** and was correctly called.

**Verdict: partially knowable.** The morning had the pieces but mis-signed the dominant one.

---

## 4. Outliers inside the sector

The morning flagged **BKR −6.5%** on Chart acquisition margin drags as a single-name event not to set S1. With XLE closing +0.32%, a −6.5% single-name drag inside the ETF is notable — it means the *rest* of the sector was stronger than the ETF print suggests, and it contributed to the relative lag versus SPY. This is consistent with the **Sector breadth failure** factor (marked HIT at 0.55): the ETF underperformed partly because a large constituent was idiosyncratically weak. Worth noting that the morning correctly quarantined BKR from the sector score — that was right — but it also means the ETF's relative weakness is partly a single-name artifact, not a pure sector signal.

No other outliers are verifiable from the inputs.

---

## 5. Verdict on the morning read

The morning got the **magnitude right** (flat/mild), the **relative direction right** (XLE underperformed), and the **discipline right** (no double-counts, magnitude cap honored). It got the **absolute direction wrong** (called down, XLE closed up) and **mis-signed the dominant driver** (treated risk-on equity beta as a headwind when it was the session's main tailwind).

The honest grade: **direction MISS on absolute, direction HIT on relative, magnitude HIT.** The morning's own divergence flag ("leading-vs-tape divergence — factors lean down, tape is flat/risk-on") was the correct warning, and the pipeline's decision to cut conviction to 0.52 and cap at mild was the right response to that warning. The failure was not in the risk management; it was in the **sign of S0** and the **persistence assumption in S1**.

**Lesson for the next Energy session:** when futures are green on a risk-on macro day, score equity beta as a **tailwind** for XLE's absolute print, and let the barrel set only the **relative** sign. A non-confirming barrel caps participation; it does not force an absolute down-close when the broad tape is bid.

---

OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: 0.32
SPY_PCT: 0.85
REL_PCT: -0.53
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Risk-on equity beta (SPY +0.85%) lifted XLE absolutely; a non-confirming barrel (WTI ~$100.36, above the morning $99.91) capped participation, producing a small relative lag.
KEY_INTERACTION: S0 (risk-on tape) and S1 (offered barrel) offset rather than aligned — the morning mis-signed S0 as a headwind when it was the session's dominant tailwind.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Magnitude and relative direction correct; absolute direction wrong (called down, closed +0.32%) due to a sign error on S0 and an unwarranted persistence assumption on the offered barrel.
OUTCOME_END