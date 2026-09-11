# Sector Outcome — Real Estate — 2026-09-11

Actuals: {'etf': 'XLRE', 'pct': 0.8594632716421691, 'spy_pct': 0.8524287494320992, 'rel': 0.007034522210069838, 'open': 43.400001525878906, 'close': 43.41999816894531, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLRE: **+0.859%** (open 43.40 → close 43.42)
- SPY: **+0.852%**
- Relative: **+0.007%** (essentially dead flat — a rounding-error outperformance)
- Path: open 43.400 → close 43.420. The entire day's move was a gap; intraday range was ~2 cents. XLRE opened at its high and closed at its high, with no meaningful intraday travel.

**Direction:** UP. **Magnitude:** notable in absolute terms (+0.86% is a real up day), but the *relative* outcome is flat — XLRE matched SPY to within one basis point.

**The single most important fact:** the morning call was **down/flat**, and the actual was **up/notable**. This is a **direction MISS** on the absolute call, and a **magnitude MISS** as well (flat band vs. +0.86%). The relative call (mild-negative lean) was also wrong in sign, though the magnitude of the relative error is trivial (+0.007% vs. a predicted mild lag).

**What actually happened — the CPI print:**

CLAIM: August 2026 CPI rose 0.4% m/m and 3.4% y/y, released 2026-09-11.
URL: https://www.bls.gov/cpi/
PUBLISHED: 2026-09-11
QUOTE: "In August, the Consumer Price Index for All Urban Consumers rose 0.4 percent, seasonally adjusted (SA), and rose 3.4 percent over the last 12 months, not seasonally adjusted (NSA)."
SUMMARY: A hot CPI print — 3.4% y/y is well above target and showed "little improvement."

CLAIM: Markets read the hot CPI as near-certain Fed hike next week, yet equities rallied.
URL: https://www.nytimes.com/live/2026/09/11/business/inflation-cpi-report
PUBLISHED: 2026-09-11
QUOTE: "U.S. inflation showed little improvement in August, running at a 3.4 percent annual rate. Investors believe the Federal Reserve is very likely to raise rates at its meeting next week."
SUMMARY: The macro binary resolved hawkish, and the tape went up anyway — a "sell the rumor, buy the news" / relief-rally dynamic after a 4-day slide.

---

## 1. WHAT DROVE THE SECTOR TODAY

The dominant driver was **not sector-specific at all**. It was a **market-wide risk-on relief rally** on CPI day:

1. **CPI-day relief rally (primary).** Futures were already green pre-print (ES +0.63%, NQ +0.65%). The CPI came in hot (3.4% y/y) but *not worse than feared*, and after a 4-day slide the market rallied broadly. SPY +0.85% and XLRE +0.86% moved together — this was **beta, not sector alpha**.

2. **Rates did NOT spike on the hot print.** This is the key tell. A 3.4% CPI with a near-certain hike should have sent the long end up hard. Instead XLRE — the most rate-sensitive sector — *matched* SPY rather than lagging. That implies the long end was stable-to-lower intraday, i.e., the hawkish print was already in the term premium (the morning note's "T+2 Warsh repricing" thesis was directionally right about *why* the curve didn't react, even though it drew the wrong conclusion about the equity path).

3. **Oil slide continued** (WTI −2.54%, Brent −2.86% pre-open) — easing the inflation channel and supporting the "hot CPI is backward-looking" read.

4. **No REIT-specific catalyst.** No fresh earnings, no cap-rate news. The sector was a passenger.

**Taxonomy alignment:** This was a **shared-macro / risk-on beta** day. The sector's own factors (data-center demand, industrial occupancy, office vacancy, refinancing wall) were all irrelevant to the print — they were correctly scored as stale/structural, and correctly did not move the ETF.

---

## 2. AUDIT OF MORNING S0–S4 READS

### S0_SHARED_MACRO = 0 — **VERDICT: WRONG SIGN, RIGHT INSTINCT ON MAGNITUDE**

The morning note explicitly identified the setup: "This is a **CPI-day risk-on setup**, not a risk-off overlay. Futures are **clearly green**." It then scored S0 = 0 because "CPI is two-sided and unprinted."

This is the core error. The note **saw the green futures, saw the falling oil, saw the flat-to-easing live curve** — and then refused to let those observable, pre-open facts carry positive weight, because it had pre-committed to treating CPI as a symmetric binary. But the *tape at the open* was already telling you the market's posture: green futures after a 4-day slide, oil falling, curve easing. That is a **risk-on open**, and the honest S0 was **positive**, not zero.

The 08-12 lesson ("two-sided policy events stay two-sided; do not one-way score S0") was applied too literally. A two-sided event does not mean S0 = 0; it means **do not score it one-way in the direction of your prior**. The note's prior was down, so "two-sided" collapsed into "no positive credit" — a de facto down bias. That is the exact failure mode 08-12 was meant to prevent, inverted.

### S1_SECTOR_FACTORS = 0 — **VERDICT: CORRECT**

No fresh REIT print; all sector factors stale or structural. S1 = 0 was right. The note correctly refused to pad S1 with always-on DC/industrial (08-27 lesson applied well).

### S2_BREADTH = −0.5 — **VERDICT: WRONG, AND DOUBLE-COUNTED**

The note scored −0.5 for "sector rotation out of real estate" based on 1w/1m relative lag. But it *also* flagged this in the divergence check as a tape factor, and S4 = −0.5 was scored on the same multi-horizon lag. **The 1w/1m relative lag was counted twice** — once in S2 (rotation) and once in S4 (tape). The note even wrote "Do not also dump it into S1" — but the double-count happened between S2 and S4 instead.

On the day, breadth was fine: XLRE matched SPY. The −0.5 was a stale-lag penalty that had no same-day information content.

### S3_FLOWS_POSITIONING = 0 — **VERDICT: CORRECT**

No same-day flow signal. Correctly neutral.

### S4_ETF_TAPE = −0.5 — **VERDICT: WRONG**

The 1d rel was −0.23% — a *mild* lag. Scoring −0.5 (half the max) for a −0.23% relative move is too heavy, and it compounded the S2 double-count. More importantly, the tape *at the open* (green futures, easing curve) was the more relevant tape, and it was positive.

### Reconciliation audit

Σ = 0 + 0 − 0.5 + 0 − 0.5 = −1.0; × 0.9 = −0.9 → down/flat. The arithmetic was clean; the **inputs were biased**. Two of the five components (S2, S4) were the same stale-lag signal counted twice, and S0 was a mis-signed zero. Strip the double-count and the honest score was roughly **S0 +0.5 / S1 0 / S2 0 / S3 0 / S4 −0.25 → net positive**, i.e., **flat-to-up**, which is what happened.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN TEST

**Double-count:** Confirmed — S2 and S4 both scored the 1w/1m relative lag. This is the single largest mechanical error in the run.

**Knowable-at-open test:** The winning information was **fully knowable at the open**:
- Green futures (ES +0.63%, NQ +0.65%) — printed pre-open.
- Oil down (WTI −2.54%, Brent −2.86%) — printed pre-open.
- Live curve flat-to-slightly-down (10Y note +0.03%, 30Y bond +0.03%, Ultra Bond +0.09%) — printed pre-open.
- VIX backwardation (stress) — printed pre-open, but this was a *contrarian* tell: backwardation into a known binary after a 4-day slide often marks a local bottom.

The note had all four. It chose to net them to zero because of the CPI binary. **The correct read was: risk-on open + easing curve + falling oil = positive S0, with the CPI binary as a variance-widener (band ≥ mild), not a sign-flipper.**

**The 09-03 lesson was misapplied.** "Widen the band to at least mild when a high-impact two-sided catalyst is identified" — the note used this to *forbid flat* and force the band to mild, but kept the *direction* at down. The lesson says widen the band, not commit to a direction. Widening should have produced **flat/mild with the direction genuinely undetermined**, and the pre-open tape should have broken the tie **upward**.

**The 08-27 "no-force-down" rule was violated in spirit.** The rule says: default flat unless the live curve is independently verified still falling; do not force down. The note *did* force down — it took a −1.0 score to a down call when the live curve was easing and futures were green. The rule was cited but not obeyed.

---

## 4. OUTLIERS INSIDE THE SECTOR

With XLRE matching SPY to one basis point, there was **no dispersion signal** — no single name drove the ETF, and no name diverged enough to matter. The morning note's caution against letting WELL, EQIX/DLR, or BXP set the call was correct and moot: none of them did. The sector traded as pure duration/beta, and on a day when the long end didn't move, duration/beta meant "match the index."

The one mild outlier worth noting: XLRE's **intraday range was ~2 cents** (43.40 → 43.42). The entire move was the opening gap. That is the signature of a **macro-driven, single-impulse day** — the sector repriced once at the open on the CPI/futures read and then did nothing. This is consistent with the "shared macro, not sector alpha" diagnosis and inconsistent with any sector-specific story.

---

## 5. LESSONS FOR THE NEXT RUN

1. **A two-sided binary does not license S0 = 0 when the pre-open tape is one-sided.** If futures are green, oil is falling, and the live curve is easing, S0 should carry *positive* weight even with CPI pending. "Two-sided" widens the band; it does not zero the score.

2. **Never score the same multi-horizon relative lag in both S2 and S4.** Pick one home for the stale-lag penalty. This run's −1.0 was really ~−0.5 after de-duplication.

3. **The 08-27 no-force-down rule needs a hard gate.** If the live curve is flat-to-down and futures are green, a down call requires an *independent* same-morning negative print. There was none. The down call should have been blocked.

4. **Backwardation into a known binary after a multi-day slide is a contrarian tell, not a confirmation of stress.** VIX/VIX3M 1.111 was read as "stress ⇒ down." It was actually "everyone hedged ⇒ relief rally."

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: 0.859
SPY_PCT: 0.852
REL_PCT: 0.007
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Market-wide CPI-day relief rally (hot 3.4% print already priced; long end stable) — pure beta, no sector alpha
KEY_INTERACTION: Stale 1w/1m relative lag double-counted in S2 and S4, while the positive pre-open tape (green futures, falling oil, easing curve) was netted to zero by an over-literal "two-sided CPI" read
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction MISS — down/flat called vs. +0.86% actual; S0 mis-signed to zero and S2/S4 double-counted the same stale lag, producing a spurious −1.0
OUTCOME_END