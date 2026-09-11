# Sector Outcome — Real Estate — 2026-09-11

Actuals: {'etf': 'XLRE', 'pct': 0.8594632716421691, 'spy_pct': 0.8524287494320992, 'rel': 0.007034522210069838, 'open': 43.400001525878906, 'close': 43.41999816894531, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLRE: **+0.859%** (open 43.40 → close 43.42)
- SPY: **+0.852%**
- Relative: **+0.007%** (essentially dead flat vs SPY)
- Actual direction: **UP**
- Actual magnitude: **mild** (sub-1% absolute move; relative move ~0 bp)

**Path:** Open 43.400 → Close 43.420. A ~2-cent range on a ~43.4 handle. This is a **gap-and-hold, dead-flat intraday tape** — the entire move was in the open, and the session did nothing after. XLRE opened at essentially its closing level and stayed there. That is the signature of a **macro-driven, single-print repricing** (CPI at 8:30 ET), not a sector-specific flow story.

**Morning prediction:** down / flat, total_score −1.125, confidence 0.5, regime mixed, divergence_flagged False.

**Verdict up front:** Direction **MISS** (predicted down, got up). Magnitude band **flat** vs actual mild — a near-miss on the band, but the band was the wrong sign of the wrong call. The relative call (mild negative lean) was **flatly wrong**: XLRE matched SPY to within 1 bp.

---

## 1. What actually drove the sector

The dominant object was the **August CPI print at 8:30 ET**, and it was **hotter than expected on the headline**.

**CLAIM:** August 2026 CPI rose 0.4% MoM and 3.4% YoY; the MoM came in hotter than estimates, YoY unchanged from July.
**URL:** https://www.stephens.com/perspectives/consumer-price-index-update-september-11-2026
**PUBLISHED:** 2026-09-11
**QUOTE:** "The report showed prices increased 0.4% from July to August and increased 3.4% year over year. The month over month data came in hotter than estimates and the yearly change remained the same as July."
**SUMMARY:** A hot headline CPI. This is the single most important fact of the session and it was **not knowable at the open** — it printed at 8:30 ET, 30 minutes before the bell.

**CLAIM:** Core CPI came in at 0.3% MoM / 2.4% YoY, and the market repriced toward a hike (80.5¢ on a hike per the settlement table).
**URL:** https://polymarkettrader.com/events/us-cpi-2026/
**PUBLISHED:** 2026-09-11
**QUOTE:** "August 2026 results: the official print and how the markets settled... the Fed repricing to 80.5¢ on a hike."
**SUMMARY:** The hot headline did **not** translate into a hawkish rate shock that hurt REITs. Core was contained, and the tape treated the print as absorbable.

**CLAIM:** The CPI was the final inflation reading before the FOMC meeting, arriving against surging oil, a strong August jobs report, and Treasury stress.
**URL:** https://wallstreettimes.com/august-cpi-report-due-september-11-as-rising-oil-prices-and-strong-jobs-data-complicate-the-feds-next-move/
**PUBLISHED:** 2026-09-11 (pre-release)
**QUOTE:** "The report is the final inflation reading Federal Reserve Chair Kevin Warsh and the FOMC will see before their next policy meeting, and it arrives against a backdrop of surging oil prices, a stronger-than-expected August jobs report, and Treasury..."
**SUMMARY:** Confirms the setup the morning note described — a two-sided, high-impact binary. Note the pre-release framing said "surging oil prices," which is **inconsistent with the morning note's oil-down read** (CL −2.78%, BZ −3.37%). That discrepancy matters and I flag it below.

**CLAIM:** Dow and S&P futures gained ahead of the CPI; Oracle surged; diesel hit record highs.
**URL:** https://tradingstrategyguides.com/stock-market-preview-september-11-2026-cpi-and-oracle-in-focus/
**PUBLISHED:** 2026-09-11 (pre-open)
**QUOTE:** "Dow and S&P futures gain ahead of August CPI while Oracle surges and diesel hits record highs."
**SUMMARY:** Confirms the morning note's green-futures read (ES +0.63%, NQ +0.65%). The risk-on open was real and it **held**.

**CLAIM:** A cooler CPI would support equities; a hotter report could weigh on rate-sensitive sectors.
**URL:** https://features.financialjuice.com/2026/09/07/us-cpi-prep-11th-september-2/
**PUBLISHED:** 2026-09-07
**QUOTE:** "A hotter report could weigh on rate-sensitive sectors by reviving concerns that inflation is proving more persistent than expected."
**SUMMARY:** The consensus framing was that a hot print = bad for REITs. The hot headline printed and REITs went **up**. The consensus framing was wrong, and so was the morning note's implicit alignment with it.

**Taxonomy-aligned driver:** The primary driver was a **shared-macro CPI event that resolved risk-on**, with the rate-sensitive duration channel **not** firing as a headwind. XLRE's +0.86% is essentially pure SPY beta (+0.85%) — the sector had **no idiosyncratic move at all**. This is the cleanest possible read: the sector was carried by the tape, not by anything REIT-specific.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO = 0 — **PARTIALLY RIGHT, WRONG REASON**

The morning note scored S0 = 0 as "mixed — hawkish backdrop offset by flat-to-easing live curve, falling oil, and green futures; CPI is two-sided and unprinted." The **score of 0 was correct in outcome** (the macro did not produce a directional sector move), but the reasoning was internally contradictory and the note then **failed to honor its own S0 = 0**.

The note wrote: *"This is a CPI-day risk-on setup, not a risk-off overlay. Futures are clearly green."* It correctly identified the setup as risk-on. Then it scored S2 and S4 negative anyway, producing a net −1.0 and a down call. **If S0 is genuinely 0 and the setup is risk-on, the burden of proof for a down call has to come from S1/S2/S3/S4 — and it didn't.**

The deeper S0 error: the note treated the **hawkish Warsh backdrop as a live headwind** while simultaneously acknowledging it was "already printed (T+2), not a fresh same-morning shock." That is a **double-count of a stale object**. If it's already in the term premium, it cannot also be a fresh negative for today. The note flagged this and then scored as if it hadn't.

### S1_SECTOR_FACTORS = 0 — **RIGHT SCORE, RIGHT REASON**

The spine audit was honest: rates-falling MISS, rates-rising "not a clean HIT at the open," real-yields-rising correctly identified as "backdrop, same duration channel, not a second independent shock." The note **correctly refused to double-count the rate object**. S1 = 0 was the right call and it held. The secondary factors (data-center, industrial, office, refinancing wall) were all correctly tagged stale and correctly excluded from the same-day vote. **This was the best-executed part of the morning note.**

### S2_BREADTH = −0.5 — **WRONG**

The note scored breadth −0.5 on the basis of "XLRE 1d rel −0.23% — a mild lag" and "multi-horizon lag is sector-wide duration." But a −0.23% 1d relative lag is **noise**, not a breadth failure. The note itself wrote "a mild lag, not a cushion and not a smash" — and then scored it as a half-point negative. **A factor you describe as "not a smash" should not carry a −0.5 weight.** The multi-horizon lag (1w −0.60, 1m −0.68) is a **slow-moving structural** object; using it to justify a same-day breadth score is exactly the kind of stale-object double-count the note's own lesson (7) warns against.

### S3_FLOWS_POSITIONING = 0 — **RIGHT**

No same-day volume spike, not a crowded long, near-term outflow. 0 is defensible. No complaint.

### S4_ETF_TAPE = −0.5 — **WRONG, AND THE MOST CONSEQUENTIAL ERROR**

The note scored the tape −0.5 on the 1d rel −0.23% and the multi-horizon lag. But the tape **at the open** was: green futures, oil falling, live curve flat-to-slightly-down. The note's own Channel 1 read said the live curve was easing. **The tape was not negative at the open.** The note took a backward-looking relative-laggard statistic and scored it as if it were a forward-looking tape signal. That is the definition of forcing a down call off a stale prior-close table — precisely what lesson (2) from 08-25 forbids.

### The reconciliation failure

Σ = 0 + 0 − 0.5 + 0 − 0.5 = −1.0; × 0.9 = −0.9 → "flat/mild." The note then **overrode its own band** via lesson (9) (09-03 macro-surprise: widen to at least mild when a high-impact two-sided catalyst is identified) and called **down/flat**. This is where the pipeline and the narrative diverged: the deterministic pipeline output `predicted_direction: down, predicted_magnitude_band: flat`, but the narrative said "flat/mild with a slight down lean" and the HORIZON_3D said flat/mild. **The note applied lesson (9) to widen the band but then let the widened band inherit the down direction from a −0.9 score that was itself built on two mis-scored factors.**

Lesson (9) says widen the band. It does **not** say convert a flat/mild lean into a down call. The note conflated "widen the band" with "strengthen the direction." That is a **lesson-application error**, and it is the proximate cause of the direction miss.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count inventory:**
1. **Rate object counted twice.** The note explicitly says "count the rate object once in S1; S0 is the regime map, not a second copy." It then put the hawkish backdrop in S0 (as the offset to green futures) *and* let the multi-horizon rate-driven lag drive S2 and S4. The rate channel effectively scored three times: S0 (as a wash), S2 (as breadth failure), S4 (as tape weakness). **This is the core double-count.**
2. **Stale Warsh hawkishness counted as live.** The note admits it's T+2 and already in term premium, then scores it as a headwind anyway.
3. **Multi-horizon lag counted as same-day tape.** 1w/1m relative lag is a structural descriptor; using it in S4 is a stale-object error.

**Knowable-at-open test:**
- Green futures: **knowable** (ES +0.63%, NQ +0.65%). ✓
- Oil down: **knowable** (CL −2.78%). ✓
- Live curve flat-to-slightly-down: **knowable** (10Y note +0.03%, 30Y bond +0.03%, Ultra +0.09%). ✓
- CPI print: **NOT knowable** (8:30 ET). ✗
- CPI outcome (hot headline, contained core, risk-on resolution): **NOT knowable**. ✗

**The critical asymmetry:** Everything knowable at the open pointed **flat-to-up** (green futures, falling oil, easing curve). The only thing pointing down was a **stale multi-horizon relative lag** and a **stale hawkish backdrop**. The note weighted the stale objects more heavily than the live ones. That is backwards.

**The oil discrepancy — flag it.** The morning note read oil as **down** (CL −2.78%, BZ −3.37%, WTI $99.91). The pre-release CPI preview (wallstreettimes) framed the backdrop as "**surging oil prices**." These cannot both be right for the same morning. Either the morning note's oil feed was wrong, or the preview was stale. Given the note cited specific live levels (WTI $99.91, Brent $104.62) with 1d changes, I lean toward the note's oil read being correct and the preview being a stale framing. **But this is an unresolved data conflict and it should be logged.** If oil was actually *rising*, the note's "oil slide eases the inflation channel" argument collapses, and S0 should have been scored negative — which would have made the down call *more* defensible, not less. Either way, the note's S0 reasoning rests on a fact that is now in dispute.

---

## 4. Outliers inside the sector

With XLRE at +0.859% vs SPY +0.852% (rel +0.007%), there is **no sector-level outlier**. The ETF tracked the index to within a basis point. This is the tell: **there was no REIT-specific story today.** No data-center divergence, no office idiosyncratic move, no WELL-only carry. The sector was pure beta.

This is itself the most important outlier finding: **the morning note's entire S1 secondary-factor apparatus (data-center, industrial, office, refinancing wall) was correctly identified as stale and correctly excluded — and the outcome confirms that exclusion was right.** The sector had no idiosyncratic driver because none of those factors fired. The note got the *exclusion* right and then let the *stale macro* drive the call anyway.

---

## 5. Verdict and lessons

**Direction: MISS.** Predicted down, actual up.
**Magnitude: near-miss.** Predicted flat, actual mild — but the band was attached to the wrong direction, so it doesn't count as a hit.
**Relative: MISS.** Predicted mild negative lean, actual dead flat (+0.007%).

**What went right:** S1 = 0 (refused to double-count the rate object), S3 = 0, correct exclusion of stale secondary factors, correct identification of the CPI as the dominant two-sided binary.

**What went wrong:**
1. **Scored S2 and S4 negative on stale multi-horizon lag** while the live tape was green. Backwards weighting of stale vs live.
2. **Double-counted the rate channel** across S0/S2/S4 despite explicitly warning against it.
3. **Mis-applied lesson (9):** widened the band but let it inherit the down direction from a score built on mis-scored factors.
4. **Treated stale Warsh hawkishness as a live headwind** after admitting it was T+2 and already priced.
5. **Unresolved oil data conflict** between the note's live read and the pre-release framing.

**The one-line lesson:** When S0 is genuinely 0 and the live tape (futures, oil, curve) is green, a down call requires a *live* negative — not a stale multi-horizon relative lag. The 09-08 cushion rule correctly did not fire (rel was −0.23%, not +0.4%), but the *absence* of a cushion is not the *presence* of a headwind. **No cushion ≠ negative signal.** The note treated "no positive override" as "negative input," which is a logical error that flipped the sign of the call.

**Pipeline note:** The deterministic pipeline output (down/flat, −1.125) and the narrative (flat/mild, slight down lean) **disagreed**, and the narrative's own HORIZON_3D said flat/mild. When the pipeline, the narrative, and the 3D horizon all point different directions, that is a **divergence that should have been flagged** — but `divergence_flagged: False`. The divergence check compared factors to tape and called it "agree on a mild-negative relative lean," which was itself wrong: the tape was green, not mildly negative. **The divergence detector was fed a mis-scored tape and therefore missed a real divergence.**

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: 0.859
SPY_PCT: 0.852
REL_PCT: 0.007
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hot August CPI headline (0.4% MoM / 3.4% YoY) resolved risk-on with contained core; XLRE tracked SPY beta to within 1 bp with zero idiosyncratic move.
KEY_INTERACTION: Rate channel double-counted across S0/S2/S4 while the live tape (green futures, falling oil, flat-to-easing curve) was positive; stale multi-horizon relative lag was weighted as a live headwind.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS — S1/S3 correctly scored 0 and stale secondary factors correctly excluded, but S2/S4 were mis-scored negative on stale lag, and lesson (9) was mis-applied to convert a flat/mild lean into a down call.
OUTCOME_END