# Sector Outcome — Utilities — 2026-09-11

Actuals: {'etf': 'XLU', 'pct': -0.30574098474991374, 'spy_pct': 0.8524287494320992, 'rel': -1.158169734182013, 'open': 42.88999938964844, 'close': 42.38999938964844, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-11

## 0. FACTS

**CLAIM:** XLU closed at $42.39, down 0.31% on the session, from an open of $42.89.
**URL:** (injected Channel 1 actuals)
**PUBLISHED:** 2026-09-11
**QUOTE:** `OPEN: 42.88999938964844 CLOSE: 42.38999938964844`; `ETF_PCT: -0.30574098474991374`
**SUMMARY:** XLU printed a mild absolute decline. The open-to-close path was a monotone fade: it opened at the session high area ($42.89) and closed at $42.39, i.e. the entire day was a drift lower with no recovery. That is a "sell-the-open, hold-the-weakness" shape, not a whipsaw.

**CLAIM:** SPY rose 0.85% on the session.
**URL:** (injected Channel 1 actuals)
**PUBLISHED:** 2026-09-11
**QUOTE:** `SPY_PCT: 0.8524287494320992`
**SUMMARY:** The broad tape was firmly green. This is the single most important fact of the day for the review, because it converts a small absolute loss into a large relative loss.

**CLAIM:** XLU underperformed SPY by 1.16 percentage points.
**URL:** (injected Channel 1 actuals)
**PUBLISHED:** 2026-09-11
**QUOTE:** `REL_PCT: -1.158169734182013`
**SUMMARY:** Relative return −1.16% is roughly **3× the size of the absolute move**. The morning call was directionally right on XLU but the *magnitude of the relative damage* was the real story, and it was under-modeled.

**Path:** open $42.89 → close $42.39, no intraday recovery. Direction: **down**. Magnitude: **mild** in absolute terms (−0.31%), **notable** in relative terms (−1.16% vs a +0.85% SPY).

---

## 1. What drove the sector today

The dominant driver was the **CPI binary resolving hawkish**, which hit the long end and therefore the bond-proxy complex, while the broad market took the same print as a growth-tolerant "not-hot-enough-to-break-the-expansion" outcome and rallied.

**CLAIM:** August CPI rose 0.4% m/m and 3.4% y/y, released 2026-09-11.
**URL:** https://www.bls.gov/cpi/
**PUBLISHED:** 2026-09-11
**QUOTE:** "In August, the Consumer Price Index for All Urban Consumers rose 0.4 percent, seasonally adjusted (SA), and rose 3.4 percent over the last 12 months, not seasonally adjusted (NSA)."
**SUMMARY:** Inflation ran hot relative to target and showed "little improvement."

**CLAIM:** The market read the print as raising the probability of a Fed hike at the meeting the following week.
**URL:** https://www.nytimes.com/live/2026/09/11/business/inflation-cpi-report
**PUBLISHED:** 2026-09-11
**QUOTE:** "U.S. inflation showed little improvement in August, running at a 3.4 percent annual rate. Investors believe the Federal Reserve is very likely to raise rates at its meeting next week."
**SUMMARY:** This is the transmission channel. A hike-odds-up print is a duration-negative print, and XLU is the index's purest duration proxy.

**CLAIM:** Gasoline was a named contributor to the August CPI increase.
**URL:** https://www.bls.gov/cpi/
**PUBLISHED:** 2026-09-11
**QUOTE:** "CPI for all items increases 0.4% in August; gasoline rises"
**SUMMARY:** Important nuance — the morning note leaned on oil *offering hard* (WTI −2.78%, Brent −3.37%) as an inflation-relief offset. That offset was real but **backward-looking-irrelevant** to a CPI print that measures August, when oil was still >$100. The morning read treated a same-day oil slide as if it softened a same-day CPI. It did not.

**Taxonomy alignment:**
- **Rates rising (bond-proxy selloff): HIT.** The morning grid called this HIT at 0.60 and it was correct — but the morning *scoring* had it as "PARTIAL / carried" in S1 and explicitly declined to double-count it. That was the central scoring error: the factor was live, not carried, because CPI was the trigger.
- **Real yields rising: HIT.** Confirmed by the hike-odds repricing.
- **Risk-on rotation away from utilities: HIT (not PARTIAL).** SPY +0.85% with XLU −0.31% is a textbook rotation-away, and the morning scored it PARTIAL at 0.50 on the reasoning that "NQ is only marginally leading ES." That reasoning was about *pre-market futures*, which is a weak proxy for the actual rotation that a CPI-day risk-on impulse produces.
- **Risk-off tape / flight to safety: MISS.** Correctly called. There was no FTS bid, and the 09-10 gate (VIX <20, backwardated → rising long end is a relative-*lag* signal) was the single best piece of analysis in the morning note. It fired exactly as written.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO = 0 — **WRONG SIGN, should have been negative**

The morning wrote: *"S0 = 0 (do not score +1 from green futures/oil-slide; do not score −2 from rates — the CPI binary is unresolved and the oil slide is a genuine offset)."*

The "do not score +1" half was right. The "do not score −2" half was the error. The reasoning was that a two-sided binary cannot be pre-scored. But that is a rule about **not pre-committing to a direction**, not a rule that forces the score to zero. The correct treatment of a live high-impact binary with a **known asymmetric exposure** is:

- XLU's beta to a hot CPI is strongly negative (duration).
- XLU's beta to a soft CPI is modestly positive (duration relief, but capped by the 09-10 gate).
- Therefore the *expected value* of the binary, even at 50/50 odds, is negative for XLU.

The morning note actually identified this asymmetry and then discarded it. It wrote that the macro was "mixed-to-mildly-negative for a bond proxy" and then scored it **0**. That is an internal contradiction: the prose says mildly negative, the score says neutral. **S0 should have been −1.**

This is the cleanest lesson of the day: *"do not pre-score either branch"* (09-03/09-04) was over-applied. The rule means don't write "CPI will be hot so XLU goes down." It does not mean "score the binary at zero." A binary with asymmetric sector beta has a non-zero expected value and should be scored as such.

### S1_SECTOR_FACTORS = −1 — **RIGHT SIGN, UNDERWEIGHTED**

The morning identified "rates rising (carried) + mild risk-on rotation away" as the dominant fresh factors and scored −1. Both were correct. But:

- It labeled rates-rising as **"PARTIAL / carried"** and explicitly said *"do not HIT and do not double-count with S0."* In reality rates-rising was a **fresh HIT** — CPI was the catalyst that made it live today. The "carried" label was wrong; the 1m DGS10 +11bp was the setup, and CPI was the trigger.
- It labeled rotation-away as **PARTIAL** on the basis of pre-market futures spreads. The realized rotation was a full HIT.

Had both been scored at full weight, S1 would have been −2. The morning's own HIT_GRID, written at the same time, scored "Rates rising (bond-proxy selloff)" as **HIT at 0.60** and "Risk-on rotation away" as **PARTIAL at 0.50** — the grid and the S1 score disagree with each other. The grid was closer to right.

### S2_BREADTH = 0 — **DEFENSIBLE**

1d rel −0.38%, 3d +0.30%, 1w +0.61%, 1m −0.89%. Genuinely mixed. No live premarket breakdown was visible. Zero is a fair read. The realized −1.16% rel is a *consequence* of the macro/factor layer, not evidence that breadth was already signaling. **No change.**

### S3_FLOWS_POSITIONING = 0 — **DEFENSIBLE, UNTESTABLE**

No same-day flow data was available at the open and none is available now. Zero is honest. **No change.**

### S4_ETF_TAPE = −0.5 — **RIGHT SIGN, TOO SMALL**

The morning scored −0.5 citing 1d rel −0.38% and 1m rel −0.89%. The realized 1d rel was −1.16%, i.e. **3× the prior-day relative fade**. The tape was telling you XLU was already lagging into a catalyst that was asymmetric against it. A −0.5 weight on a tape that had just printed −0.38% rel *the day before a duration-sensitive binary* understated the momentum. **S4 should have been −1.**

### Multiplier 0.9 / Confidence 0.55 — **TOO LOW**

The multiplier was cut to 0.9 and confidence to 0.55 because "the CPI binary is unresolved and futures are green." But the *direction* was never really in doubt — the morning's own 09-10 gate said a rising long end with VIX <20 is a relative-lag signal, and the morning's own S1/S4 were both negative. The uncertainty was about **magnitude**, not direction. Cutting the multiplier for magnitude-uncertainty while the direction was well-supported cost the model real score. The band should have been **down/mild** (as the prose said) rather than the pipeline's **down/flat**.

Note the internal inconsistency: the prose says *"Band = down/mild (the CPI binary forbids flat per 09-03/09-04)"* but the pipeline-computed decision emitted `predicted_magnitude_band: flat`. The prose and the deterministic output disagree. The prose was right; the pipeline flattened it.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning was worried about double-counting rates-rising between S0 and S1. In fact the opposite happened — it *under*-counted by labeling the factor "carried" in S1 and zeroing it in S0, so a live factor got scored roughly once at half weight instead of once at full weight. The double-count fear produced a **double-discount**.

**Interaction that mattered:** CPI → hike odds → long end → XLU duration. This is a single chain, and the morning treated its links as separate, individually-uncertain items (S0 uncertain, S1 "carried," S4 small) rather than as one coherent, directional chain. When a single macro chain drives a sector, the components should be scored as a chain, not averaged as independent uncertainties.

**Second interaction the morning got backwards:** oil. The morning used the same-day oil slide (WTI −2.78%) as an *offset* to the CPI risk. But August CPI measures August, when oil was >$100 — and indeed gasoline was a named contributor to the print. The same-day oil slide was irrelevant to the release and, if anything, was part of the *risk-on* impulse that lifted SPY and hurt XLU relatively. The morning treated an equity-positive, duration-neutral-to-negative input as a sector cushion. It was not a cushion.

**Knowable-at-open test:** Was the outcome knowable at the open?
- Direction: **yes.** The 09-10 gate, the sticky-high long end, the backwardated VIX, and the prior-day relative fade all pointed down. The morning got the direction right.
- The *relative* magnitude: **partially.** You could not know CPI would print 3.4% / 0.4%, but you could know that (a) XLU's beta to a hot print was much larger than its beta to a soft print, and (b) SPY's beta to a hot-but-not-catastrophic print was positive. That asymmetry was knowable and was not scored.
- The absolute magnitude: **no.** −0.31% absolute is a mild move and the morning's "flat" band was arguably fine on absolute terms. The miss was on relative.

So: **KNOWABLE_AT_OPEN = partially** — direction yes, relative magnitude partially, absolute magnitude no.

---

## 4. Outliers inside the sector

**CLAIM:** PG&E dropped 5.2% on 2026-09-03 after a $2 billion spend delay raised growth questions.
**URL:** https://news.google.com/rss/articles/CBMilAFBVV95cUxNY09WOHNmX3ZNLU42QUduWk8zVUdxc0VOclhmR0dQY3FRbkU0T05uV2wydkdDRmp1Yk5ZTU9pblVNUklfMEVFdlBsMzBMMG1QYnlwbUJCZ2VqeXJiWmFnVkNaaVJFbUZNNGxnRkphVVBCT0U5NG85SXBnUmhjQ1lMLVVSTFRJQ3dhZEhoVUJLWTN6cHlW?oc=5
**PUBLISHED:** 2026-09-03
**SUMMARY:** A single-name regulatory/load-growth item, already a week old by 09-11. Correctly not promoted into S1 per the 08-28 rule. No fresh single-name outlier appears to have driven today's move — the −0.31% absolute is consistent with a broad, low-dispersion duration repricing rather than an idiosyncratic name event.

**CLAIM:** Sector commentary on 09-11 was dominated by valuation/dividend-sustainability and "AI-power exposure" framing (GuruFocus GF Value, 24/7 Wall St. "Forget XLU").
**URL:** https://news.google.com/rss/articles/CBMitAFBVV95cUxOSTlreVdRS09uN2xTTG5JZUpLVHc3SDhyRFRpNVV1alRla1hvOVdHaHlqdTNDQl9JLW04NFdFOTNKSEdublgxZWJDZ2JQZDBNVXNiVlMwYXpmRVE1OFRET2dXajdHSzhnWE1LdjlFUlNSWXRrcVh0cEpkaDJPS0lfOERzeG1uVTREa0pIYkU1TmVidVBhVlJuenN0ZVZCMXVnVFNQaTVEbjBjeVJJTFZubzROaEo?oc=5
**PUBLISHED:** 2026-09-11
**SUMMARY:** This is the "structural positives are stale" bucket the morning correctly identified. AI-power and dividend-sustainability stories are multi-year narratives; they did not and could not offset a one-day duration repricing. The morning's 08-12 rule ("AI-power is a 1d dampener, not a band engine") held up.

**No outlier required explanation.** The sector moved as a bloc on rates. That is itself informative: it means the correct model for today was a single-factor duration model, and the morning's multi-factor averaging diluted the one factor that mattered.

---

## 5. Verdict and lessons

**The morning got the direction right and the magnitude wrong — specifically, it got the *relative* magnitude wrong, and it got it wrong for a structural reason: it treated a live, asymmetric macro binary as a neutral.**

Three concrete, reusable corrections:

1. **A live high-impact binary with asymmetric sector beta is not a zero.** "Do not pre-score either branch" means don't commit to an outcome; it does not mean score the expected value at zero. If XLU's downside beta to a hot CPI is 3× its upside beta to a soft CPI, S0 is negative even at 50/50 odds. Score the asymmetry.

2. **"Carried" is not a synonym for "not live."** The morning labeled rates-rising as carried because the 1m DGS10 move was already in the price. But CPI was the trigger that made it live *today*. A factor with a same-day catalyst is live, regardless of how much of it was pre-positioned. The morning's own HIT_GRID scored it HIT; the S1 score said PARTIAL. Trust the grid.

3. **Same-day commodity moves do not soften same-day inflation prints.** August CPI measures August. The oil slide was an equity-positive input, not a bond-proxy cushion. Do not let a same-day risk-on commodity move be scored as an offset to a backward-looking inflation release.

**What the morning got right, and should keep:** the 09-10 gate. "VIX <20 and backwardated → a rising long end is a relative-lag signal for XLU, not a relative cushion" was the correct frame, it was applied correctly, and it predicted the exact failure mode that occurred (relative lag, not absolute collapse). That gate should be promoted from a lesson to a standing rule.

**What the pipeline got wrong:** the deterministic output emitted `predicted_magnitude_band: flat` while the prose said `down/mild`. The prose was right. The pipeline flattened a band that the reasoning had already widened. That is a pipeline bug worth fixing — the band should be inherited from the prose, not recomputed to a narrower value.

**Scorecard:** Direction HIT. Absolute magnitude HIT (mild, −0.31%). Relative magnitude MISS (−1.16% vs a band that implied roughly −0.3 to −0.5%). The miss is attributable to S0 (0 instead of −1), S1 (−1 instead of −2), and S4 (−0.5 instead of −1), partially offset by the too-low multiplier. Had those been scored correctly, total_score would have been roughly −4.5 × 0.9 ≈ −4.0, which would have produced a down/notable band — still short of the realized relative damage, but far closer.

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.31
SPY_PCT: 0.85
REL_PCT: -1.16
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild (absolute) / notable (relative)
PRIMARY_DRIVER: Hot August CPI (0.4% m/m, 3.4% y/y) lifted Fed hike odds, repriced the long end, and hit the bond-proxy complex while SPY rallied on a growth-tolerant read of the same print.
KEY_INTERACTION: CPI → hike odds → long end → XLU duration, with SPY taking the same print as risk-on; the morning scored this single chain as three separate, individually-uncertain items and thereby double-discounted it.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction correct and the 09-10 VIX/backwardation gate was excellent, but S0 was wrongly zeroed on an asymmetric binary, S1 under-weighted a live (not carried) rates factor, S4 under-weighted the prior-day relative fade, and the pipeline flattened the band the prose had already widened — netting a relative-magnitude miss.
OUTCOME_END