# Sector Outcome — Energy — 2026-09-21

Actuals: {'etf': 'XLE', 'pct': -2.907788839800507, 'spy_pct': 1.5518133737258744, 'rel': -4.459602213526381, 'open': 63.16999816894531, 'close': 62.439998626708984, 'source': 'yf_download'}

# Sector Post-Session Review — Energy / XLE — 2026-09-21

## 0. FACTS

**CLAIM:** XLE closed at $62.44, down 2.91% on the day, from a $63.17 open (Friday close $64.31).
**URL:** Channel 1 injected panel (deterministic actuals)
**PUBLISHED:** 2026-09-21
**QUOTE:** `OPEN: 63.16999816894531 CLOSE: 62.439998626708984`; `ETF_PCT: -2.907788839800507`
**SUMMARY:** XLE gapped down ~1.8% at the open (consistent with the −1.29% premarket print plus the $0.3803 ex-div), then sold off further through the session to close near the lows. Full-day loss ~2.91%.

**CLAIM:** SPY closed +1.55% on the day.
**URL:** Channel 1 injected panel
**PUBLISHED:** 2026-09-21
**QUOTE:** `SPY_PCT: 1.5518133737258744`
**SUMMARY:** A strong risk-on tape. Energy was the sole large loser against a broad green index.

**CLAIM:** XLE relative return vs SPY was **−4.46%**.
**URL:** Channel 1 injected panel
**PUBLISHED:** 2026-09-21
**QUOTE:** `REL_PCT: -4.459602213526381`
**SUMMARY:** This is a severe single-day relative dislocation — roughly 4.5 points of underperformance in one session. It is the largest relative gap in the recent memory window and far outside the "mild" band the morning call implied.

**CLAIM:** WTI settled around $92.5, down ~3.7% on the day; Brent around $96.4, down ~$7.8.
**URL:** https://worldoilmonitor.com/ ; https://convextrade.com/today/oil-price
**PUBLISHED:** 2026-09-21 (close)
**QUOTE:** "As of the 2026-09-21 close, WTI crude traded at $92.48 per barrel (down $7.82 on the day) and Brent crude at $96.39." / "WTI Crude Oil Price Today: $92.5 -3.73% (September 21, 2026)"
**SUMMARY:** The barrel did not fall ~2% as the morning read assumed — it fell ~3.7% and broke below $93 WTI / $97 Brent. The morning's "offered ~2%, not a collapse" framing understated the move by roughly a factor of two and mis-set the level by ~$6.

**Path:** Open $63.17 → close $62.44. Gap-down open, no meaningful intraday bounce, close in the lower part of the range. Directionally monotone-down day.

**ACTUAL_DIRECTION:** down
**ACTUAL_MAGNITUDE:** notable (bordering severe on a relative basis; −4.46% rel is severe)

---

## 1. What drove the sector

The primary driver was a **crude oil price decline that was materially larger than the morning read assumed, compounded by a mechanical ex-dividend drag and a rotation out of energy into a strongly risk-on tech-led tape.**

**CLAIM:** Oil fell for a fourth consecutive session and broke below $93 WTI.
**URL:** https://convextrade.com/today/oil-price ; https://worldoilmonitor.com/
**PUBLISHED:** 2026-09-21
**QUOTE:** "WTI Crude Oil Price Today: $92.5 -3.73%"
**SUMMARY:** The barrel was the spine. XLE is oil-weighted; a ~3.7% crude decline with no offsetting product squeeze transmits almost one-for-one into the integrated/E&P complex.

**CLAIM:** The geopolitical supply premium continued to fade — Hormuz shipments at a six-month high, diplomacy track open.
**URL:** https://www.bloomberg.com/news/articles/2026-09-19/hormuz-oil-shipments-hit-six-month-high-us-commander-says
**PUBLISHED:** 2026-09-19
**QUOTE:** Adm. Brad Cooper / CENTCOM: crude/LNG shipments through Hormuz over the prior two weeks at a six-month high.
**SUMMARY:** The premium-fade cluster the morning note identified was correct in *direction* but under-weighted in *magnitude*. The market re-priced the geo premium harder than the morning read allowed.

**CLAIM:** XLE went ex-dividend $0.3803 on 2026-09-21.
**URL:** https://www.dividendinvestor.com/dividend-news/20260918/state-street-energy-select-sector-spdr-etf-select-sector-spdr-trust-nyse-xle-declared-a-dividend-of-$0.3803-per-share/
**PUBLISHED:** ex-date 2026-09-21
**QUOTE:** XLE $0.3803 dividend, record 9/21, payable 9/23.
**SUMMARY:** ~59 bp of mechanical drag on the open. This is real but small relative to the −2.91% print; it explains part of the gap, not the day.

**CLAIM:** The tape was strongly risk-on and tech-led.
**URL:** https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls
**PUBLISHED:** 2026-09-21 04:58 ET
**QUOTE:** DJIA/S&P futures +0.6%, Nasdaq futures +0.8%; Asia/Europe green; Brent >2% to ~$101.79, WTI ~$98.
**SUMMARY:** The morning's own source had oil ">2%" down and futures green. By the close, SPY was +1.55% and oil was −3.7%. The rotation out of energy into tech was the second leg.

**Taxonomy alignment:** The dominant HITs are **Crude price collapse** (not just "decline"), **Sector rotation out of energy**, and **Geopolitical supply risk premium** (fading). The morning grid scored "Crude price collapse" as MISS on the grounds that live WTI was ~−2%, "not >5%." That threshold was too strict — a −3.7% barrel day with a −4.46% relative ETF move is functionally a collapse for the sector even if it doesn't clear a 5% crude threshold.

---

## 2. Audit of morning S0–S4 reads

### S0_SHARED_MACRO = 0 — **VERDICT: WRONG (should have been negative)**

The morning note argued: "risk-on futures + contango vol + overnight yield ease are a cyclical overlay that is **not transmitting** to energy; 09-11 forbids scoring that overlay negative, 09-17 forbids scoring it as an energy bid."

This was the single largest analytical error. The reasoning treated the risk-on tape as *neutral* to energy. In reality, a +1.55% SPY day with energy at −2.91% is the textbook definition of **rotation out of energy** — the risk-on tape is not neutral, it is *actively negative* for a sector the market is funding by selling. The morning note even had the evidence in hand: "XLE −1.29% vs XLK +0.98%" in premarket. That is not a non-transmitting overlay; that is the rotation signal itself.

The 09-11 and 09-17 lessons were applied too literally. 09-11 says "don't score S0 negative on green futures" — but that lesson is about not *manufacturing* a negative from a benign tape. Here the tape was green *and* energy was the clear laggard, which is a genuine negative, not a manufactured one. The lesson was over-applied.

**Correct S0: −1.**

### S1_SECTOR_FACTORS = −1 — **VERDICT: RIGHT DIRECTION, UNDER-SIZED (should have been −2)**

The morning note correctly identified the cluster: live offered barrel + geo-premium fade, counted once. It correctly rejected the stale Channel 1 CL=F −5.94% column. It correctly refused to score a fresh geopolitical HIT.

But it explicitly capped S1 at −1 with the reasoning: "Not −2: not a collapse, 1w/1m not extended, increment ~2% not a smash, residual geo/cracks still a floor."

The "increment ~2% not a smash" premise was **factually wrong by the close** — the barrel fell ~3.7%, not ~2%. And the "residual geo/cracks still a floor" premise did not hold: cracks are a refiner-sleeve story, not an XLE floor, and the geo premium was *fading*, not flooring. The morning note itself said the geo premium was "fading, with a weekend increment" — a fading premium is a *headwind*, not a floor.

The −2 cap was justified by a magnitude discipline that was appropriate for a ~2% barrel move but not for a ~3.7% move. The morning read had the right cluster and the right sign but under-weighted the increment.

**Correct S1: −2.**

### S2_BREADTH = −1 — **VERDICT: RIGHT (arguably should have been −2)**

The morning note correctly read live breadth: "PM:XLE −1.29% is the sector board's worst print; XOM/CVX red with the ETF; XLK/XLC green. That is constituent-confirmed participation in the oil-down."

This was the best-reasoned component of the morning call. It correctly refused to copy the stale 3d rel −3.27% into S2 and instead used live PM breadth. The direction and the reasoning were sound.

The only quibble: given that breadth was *already* the worst on the board premarket and the tape was risk-on, a −2 was defensible. But −1 is not a miss — it's a conservative read of a correct signal. I'll leave this as **RIGHT**.

### S3_FLOWS_POSITIONING = 0 — **VERDICT: RIGHT**

The morning note correctly identified outflow prints (~$157M, ~$422M) as PARTIAL and correctly refused to score crowded-long (1m rel +1.86% ≪ +8%). It treated flows as "the flow expression of S1, not a second spine." That is correct double-count discipline. Flows were not the driver today; the barrel was.

**RIGHT.**

### S4_ETF_TAPE = 0 — **VERDICT: RIGHT**

The morning note correctly treated prior-close tape as a neutral starting point (1d rel −0.39%, sub-threshold) and refused to copy the stale 3d rel −3.27%. It used live PM:XLE −1.29% as the live tape signal, which was correct. S4 = 0 as a *scoring* component is defensible because the live tape signal was already captured in S2.

**RIGHT.**

### Multiplier 0.85 and confidence 0.48 — **VERDICT: CONFIDENCE TOO LOW, MULTIPLIER FINE**

The 0.85 multiplier is a standard regime adjustment. The 0.48 confidence is too low given that the morning note had *three* independent live signals all pointing down (PM:XLE −1.29%, XOM/CVX red, oil offered). A 0.48 confidence on a call with that much confirming evidence understates the setup. The low confidence appears to be driven by the magnitude-discipline lessons (mag hit-rate 0.40/0.35), which correctly cap *magnitude* but should not cap *direction* confidence when the direction signals are this aligned.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit:** The morning note counted the oil-down + geo-fade cluster once (correct). It did not triple-count oil + Hormuz + inventory (correct). It nested the refiner sleeve and did not let VLO/MPC set the ETF (correct). It treated flows as the expression of S1, not a second spine (correct). **No double-count error.**

**The real interaction error was the opposite: under-counting.** The morning note treated S0 (risk-on tape) as *neutral* to energy. But S0 and S1 were *interacting*: the risk-on tape was the mechanism by which capital rotated *out of* energy and *into* tech. That is not two independent factors — it is one rotation with two faces. The morning note's "do not flip to up on a tech-led risk-on tape energy is not in" was correct as a *direction* statement but wrong as a *scoring* statement: the tech-led tape was not neutral, it was the funding source for the energy sell.

**Knowable-at-open test:** Every element of the actual outcome was knowable at the open:
- Oil was already offered ~2% premarket and the trend was a fourth down session.
- XLE was already the worst sector print at −1.29%.
- XOM/CVX were already red.
- The ex-div was already known.
- The risk-on tape was already green.

The morning note *had all of this* and still emitted a "mild" band with 0.48 confidence. The knowable-at-open test **fails on magnitude, not on direction**. The direction was knowable and was called correctly. The magnitude was knowable (a −1.29% premarket ETF with a −2% barrel and a risk-on tape that funds rotation is not a "mild" setup) and was called too small.

**The specific magnitude error:** The morning note's own band-refinement logic said "09-18 band refinement FIRES toward mild (oil extends ~2% and PM ≤ −1%; not the |PM|<1% flat preference)." But the note also said "09-18's flat preference does not bind: oil extends ~2% and PM:XLE −1.29% ≤ −1%." The note correctly identified that the flat preference was off, but then *still* chose mild. The logic should have been: flat preference off + PM ≤ −1% + oil extending + risk-on tape funding rotation → **notable**, not mild. The note had the right inputs and the wrong output.

---

## 4. Outliers inside the sector

**CLAIM:** XLE closed at $62.44, near the session low, with no intraday recovery.
**URL:** Channel 1 injected panel
**PUBLISHED:** 2026-09-21
**QUOTE:** `OPEN: 63.16999816894531 CLOSE: 62.439998626708984`
**SUMMARY:** The ETF opened at $63.17 and closed at $62.44 — a monotone-down day with no bounce. This is consistent with a sector being *sold* rather than *hedged*: no dip-buyers stepped in.

**Outlier candidates:**
- **Refiners (VLO/MPC/PSX):** The morning note flagged the refiner sleeve as a cushion (cracks ~$69–72, diesel extreme). If refiners held up better than the integrated/E&P complex, that would confirm the morning's "nested refiner bid does not rescue the parent" read. If refiners also sold off hard, the crack-spread cushion failed entirely. Either way, the parent ETF was not rescued — the −2.91% print confirms the nested bid was insufficient.
- **Integrated majors (XOM/CVX):** Premarket XOM ~−0.8%, CVX ~−0.5%. If these closed down ~2–3%, the decline broadened through the session — consistent with the monotone path.
- **E&P / OFS:** The morning note had OFS/Drilling/Coal/Uranium OVERRIDE down. If those closed worse than the parent, the high-beta tail amplified the move.

The key outlier observation: **there was no positive outlier large enough to matter.** In a −2.91% ETF day, any single-name strength was noise. The sector sold as a unit.

---

## 5. Verdict and lessons

**The morning call got the direction right and the magnitude wrong.** Predicted down/mild; actual down/notable (severe on a relative basis). Direction HIT, magnitude MISS — the same pattern as 09-18 (dir HIT, mag MISS, actual flat), but in the opposite direction: this time the miss was *too small*, not too large.

**The three specific errors:**

1. **S0 was scored 0 when it should have been −1.** The risk-on tape was not neutral to energy; it was the funding mechanism for the rotation out of energy. The 09-11 and 09-17 lessons were over-applied — they are about not *manufacturing* a negative from a benign tape, not about ignoring a genuine rotation signal when energy is the clear laggard on a green day.

2. **S1 was capped at −1 when it should have been −2.** The "increment ~2% not a smash" premise was factually wrong by the close (barrel fell ~3.7%). The "residual geo/cracks still a floor" premise was wrong: a *fading* geo premium is a headwind, not a floor, and cracks are a refiner-sleeve story, not an XLE floor.

3. **The magnitude band was set to mild when the inputs pointed to notable.** The note correctly identified that the 09-18 flat preference was off, then still chose mild. The correct logic: flat preference off + PM ≤ −1% + oil extending + risk-on tape funding rotation → notable.

**The one thing the morning call got right that mattered:** It refused to flip to up on the tech-led risk-on tape. That was the correct call and it saved the direction. The temptation to read a +1.35% ES / +2.12% NQ tape as an energy bid was real, and the note correctly resisted it.

**Lesson for the sector_energy experiment:** The magnitude discipline (mag hit-rate 0.40/0.35, size_gate=True) is correctly capping *notable → mild* in the average case, but it is *under-capping* when the live signals are this aligned. The discipline should be conditional: when PM:XLE ≤ −1% **and** the barrel is offered **and** the tape is risk-on (rotation funding), the mild cap should lift to notable. The 09-18 band refinement was designed for this but was applied too conservatively.

**Lesson for S0:** A green tape with energy as the clear laggard is a *negative* S0 for energy, not a neutral one. The 09-11/09-17 lessons should be amended: "don't score S0 negative on green futures *when energy is participating in the green tape*; do score S0 negative when energy is the clear laggard on a green tape."

---

OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: -2.907788839800507
SPY_PCT: 1.5518133737258744
REL_PCT: -4.459602213526381
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Crude oil fell ~3.7% (WTI ~$92.5, Brent ~$96.4) on a fourth down session as the geopolitical supply premium faded (Hormuz six-month high, diplomacy track open), compounded by a ~59 bp ex-dividend drag and a rotation out of energy into a strongly risk-on tech-led tape (SPY +1.55%).
KEY_INTERACTION: The risk-on tape was not neutral to energy — it was the funding mechanism for the rotation out of energy; the morning note scored S0=0 when the green tape with XLE as the clear laggard was itself a negative signal.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction correct (down), magnitude under-called (mild vs actual notable/severe-relative); S0 mis-scored as neutral when it was negative, S1 under-capped at −1 when the barrel move and fading geo premium warranted −2.
OUTCOME_END