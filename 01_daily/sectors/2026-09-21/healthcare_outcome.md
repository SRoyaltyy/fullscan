# Sector Outcome — Healthcare — 2026-09-21

Actuals: {'etf': 'XLV', 'pct': 0.35037492726952557, 'spy_pct': 1.5518133737258744, 'rel': -1.2014384464563488, 'open': 167.31500244140625, 'close': 168.97999572753906, 'source': 'yf_download'}

# Sector Post-Session Review — Healthcare / XLV — 2026-09-21

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLV open | 167.315 |
| XLV close | 168.980 |
| XLV % change | **+0.35%** |
| SPY % change | **+1.55%** |
| Relative (XLV − SPY) | **−1.20%** |
| Actual direction | **up** |
| Actual magnitude | **mild** (absolute), **notable** (relative) |

**Path:** XLV opened at 167.315 — *below* Friday's 168.39 close (the ex-div adjustment, ~$0.6422, mechanically accounts for roughly 0.38% of that gap). It then recovered through the session to close at 168.980, i.e. **+0.35% on the day** and roughly **+1.0% off the open**. So the intraday path was: gap down on the mechanical ex-div, then a steady grind higher that still badly lagged a +1.55% SPY tape.

**The headline fact of the day:** XLV was **absolutely green** but **relatively crushed** — a 120 bp relative underperformance on a strongly risk-on, tech-led tape. This is the single most important number in this review, because the morning call was **up/mild**, and the morning *reasoning* was explicitly built to **reject** an up/mild call.

**Morning prediction:** direction **up**, magnitude **mild**, total_score 3.016, mult 0.8, regime risk_on, **divergence_flagged = True**, tape_anchor 0.198 (ES +1.35%, PM:XLV −0.30%), index_carry 3.218, llm_overlay −0.4, confidence 0.40.

**The central tension:** the LLM overlay and the entire Channel 2 narrative argued for **flat-to-down / funding-source**, while the deterministic engine minted **up/mild** off index_carry and a positive tape_anchor. The actual outcome — **up/mild absolute, badly lagging relative** — is a split verdict that needs careful adjudication, not a simple "engine right / LLM wrong."

---

## 1. WHAT DROVE THE SECTOR TODAY

### 1a. The dominant driver: sector rotation OUT of healthcare into tech/AI

The tape was a textbook **tech-led risk-on session**. The morning Channel 1 panel already showed the tell: **NQ ≥ ES** (Finviz NQ +0.41% vs ES +0.20%; separate panel NQ=F +2.12% vs ES=F +1.35%), **oil offered hard** (CL=F −5.94%, BZ=F −5.76%), **VIX 14.98 in contango**, Asia +1.04% / Europe +0.95%. That is a **cyclical / high-beta impulse**, and a low-beta defensive like healthcare is the **funding source** for it.

The morning memo said this explicitly and correctly:

> "Oil-offered + green NQ is a **cyclical/high-beta impulse**; a low-beta defensive is the **funding source**."

The session confirmed it. SPY +1.55% is a big up day; XLV captured only +0.35% of it. The **−1.20% relative** is the rotation-out signature. The HIT_GRID entries that fired — **"Risk-on tape / equity beta expansion" (HIT 0.78)** and **"Sector rotation out of healthcare" (HIT 0.70)** — are the correct taxonomy tags for what happened.

### 1b. The ex-dividend mechanical drag

XLV went ex-div **2026-09-21**, ~$0.6422/share (~0.38%). This is a **mechanical** NAV/price drop vs Friday's close, not a fundamental signal. It explains the gap-down open (167.315 vs Friday 168.39) and it means the **+0.35% close is flattered** relative to the raw price path — the fund actually had to climb ~1.0% off the open just to print +0.35%. The morning memo correctly flagged this as "mechanical, not an S1 spine." **Verdict: correctly handled, correctly excluded from scoring.**

### 1c. What did NOT drive it (correctly rejected in the morning)

- **CMS/MA 2027 +2.48%** — April finalization, stale. No re-rate. ✅ correctly rejected.
- **IRA cycle-3 final offers (Sep 30)** — not today. ✅
- **MFN 50-state Medicaid** — Friday 09-18 residual. ✅
- **NVO Capital Markets Day** — NVO is **not an XLV holding**; LLY ~15% and MAP-tagged mixed. The single-name rule held. ✅
- **FDA/CRL cluster, BSX Citi, CI Jefferies, AMGN IMDELLTRA, ARGX Forte** — all paid/T+n/single-name. ✅
- **XBI leadership** — XBI closed Friday −0.97%; no biotech risk-on bid. ✅

The morning's **single-name discipline was excellent**. None of the nested/single-name noise was allowed to drive the ETF call, and none of it did.

---

## 2. AUDIT OF MORNING S0–S4 READS AGAINST REALITY

I am auditing the **morning numbers as written**, not post-close rewrites.

### S0_SHARED_MACRO = −0.5 → **VERDICT: DIRECTIONALLY WRONG, but the reasoning was right**

The morning scored S0 = **−0.5**, arguing that tech-led risk-on makes healthcare the **funding source**, so XLV should be *offered* on a relative basis. The **relative** call was **dead right** (−1.20%). But S0 is a *shared macro* score that feeds the **absolute** direction, and the absolute outcome was **+0.35% (up)**. So S0 = −0.5 **over-weighted the funding-source effect into the absolute call**.

This is the crux of the whole review. The morning memo conflated two distinct things:
1. **Relative** underperformance (healthcare lags a risk-on tape) — **correct**.
2. **Absolute** decline (healthcare falls) — **wrong**.

On a +1.55% SPY day, a low-beta defensive can lag badly *and still close green*. The morning's S0 = −0.5 essentially bet on absolute weakness, and that was the error. The correct S0 for a "funding source on a strong up tape" is closer to **−0.2 to −0.3** (mild relative drag, not absolute drag), or the funding-source effect should have been routed to a **relative** channel rather than the absolute S0.

### S1_SECTOR_FACTORS = 0 → **VERDICT: CORRECT**

No fresh sector spine existed. CMS stale, IRA Sep 30, MFN Friday residual, no same-morning mega-cap Rx headline, no FDA breadth event. **S1 = 0 was right.** The ex-div was correctly excluded. No missed catalyst.

### S2_BREADTH = 0 → **VERDICT: CORRECT, and the reasoning was sharp**

The morning refused to copy 1w RS into S2 and refused to average the nested Facilities OVERRIDE into XLV. It noted "live PM XLV −0.30% vs XLK +0.98% is large-cap/low-beta lag, not high-beta expansion." That read was **exactly right** — XLV lagged, breadth did not expand. **S2 = 0 correct.**

### S3_FLOWS_POSITIONING = 0 → **VERDICT: CORRECT**

5d ~−$143M, 1m ~−$431M, 3m +$461M; 1m rel −3.44% = not crowded-long. Trailing flows are not a 1-day vote. **S3 = 0 correct.** No flow-driven surprise.

### S4_ETF_TAPE = 0 → **VERDICT: CORRECT**

1d rel −0.38% / 3d rel −0.38% confirmed lag; 1w rel +1.93% correctly treated as leftover, not copied into S4. **S4 = 0 correct.**

### The scoring architecture problem

**S0–S4 net = −0.5, S4 = 0, leading_sum = −1.0.** The *factor* stack said **flat-to-down**. The engine nonetheless printed **up/mild** because **index_carry = 3.218** and **tape_anchor = 0.198** (ES +1.35% weighted 0.6, PM:XLV −0.30% weighted 0.7) dominated. The **divergence was flagged** — the system *knew* the factors and the tape disagreed.

**So the honest audit is:**
- The **factor reads (S0–S4) were directionally correct on the RELATIVE call** and correctly identified the funding-source dynamic.
- The **factor reads were wrong on the ABSOLUTE call** (they implied down; reality was up).
- The **engine's up/mild was right on absolute direction and magnitude band**, but for the "wrong" reason (it rode index_carry/ES beta, not a healthcare-specific bid).
- The **LLM overlay (−0.4) was wrong on absolute direction** — it reinforced the funding-source-implies-down error.

**Net: the engine beat the overlay on this one, but neither side had the *right* model.** The right model was: *"strong up tape → XLV up mild absolute, down notable relative."* The engine got the first half by accident (beta carry); the overlay got the second half by design (funding source). Neither captured both.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN TEST

### Double-count check

The morning memo was **disciplined** here:
- Oil-offered + NQ-green + 1d real-yield dip scored **once** in S0. ✅
- 1w RS explicitly **not** copied into S2/S3/S4. ✅
- Nested Facilities OVERRIDE **not** averaged into XLV. ✅
- No oil double-count into rotation. ✅
- No AVGO/XLK map into S0=+1. ✅

**No material double-count found.** This was a clean scoring sheet.

### The one interaction the morning under-weighted

The morning treated "funding source" as a **single negative** for XLV. But on a +1.55% SPY day, the interaction is **non-linear**: a low-beta defensive in a strong up tape typically **rises less**, not **falls**. The morning's S0 = −0.5 implicitly assumed the funding-source pressure could flip XLV negative. It couldn't, because the **market beta** of XLV (~0.6–0.7) times a +1.55% tape is roughly +0.9% to +1.1% of *gross* lift, against which the relative-rotation drag of ~−1.2% nets to roughly **flat-to-slightly-positive**. That is almost exactly what printed (+0.35%).

**This is the knowable-at-open insight the morning missed:** with SPY futures strongly green and XLV PM only −0.30%, the *arithmetic* of beta × tape minus rotation drag pointed to **mild up absolute**, not down. The morning's own tape_anchor (ES +1.35%) contained this information; the memo chose to distrust it entirely rather than decompose it.

### Knowable-at-open test

| Element | Knowable at open? |
|---|---|
| Tech-led risk-on, NQ ≥ ES | **Yes** (Channel 1) |
| Oil offered hard | **Yes** |
| PM XLV −0.30% vs XLK +0.98% | **Yes** |
| Ex-div ~0.38% mechanical | **Yes** |
| XLV would close **green** | **Partially** — beta arithmetic implied it |
| XLV would lag by **−1.20%** | **Yes** — the rotation tell was fully visible |
| Exact magnitude | **No** |

**KNOWABLE_AT_OPEN: partially.** The *relative* underperformance was fully knowable and was correctly called. The *absolute* green close was inferable from beta arithmetic but was not inferred.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **LLY (~15% of XLV):** MAP-tagged **mixed**; NVO CMD (non-constituent) reported NVO shares −6% to −9% on GLP-1 rivalry headlines. LLY's weight means it was a **swing factor** for XLV, but the single-name rule correctly prevented it from driving the ETF call. Worth noting: if LLY was soft on NVO CMD spillover, that would *amplify* the relative lag — consistent with the −1.20%.
- **XBI / biotech sleeve:** Friday −0.97%, no Monday risk-on bid. The duration sleeve did **not** lead, consistent with the morning's "not XBI leadership" read.
- **Insurers (UNH/CVS/CI):** structural utilization, no same-morning smash. No outlier.
- **Devices:** BSX Citi Neutral was T+n/paid. No outlier.
- **The real "outlier" is the ETF itself:** XLV was the **relative outlier** — a green sector on a very green day that still lost 120 bp of relative. That is the story.

---

## 5. VERDICT AND LESSONS

### On the prediction

- **Direction: CORRECT (up).** ✅
- **Magnitude band: CORRECT (mild).** ✅
- **Relative call: WRONG in the engine, RIGHT in the overlay.** The engine's up/mild was right on absolute but the overlay's funding-source logic was right on relative. The **divergence flag was the most valuable output of the morning** — it told the desk "factors and tape disagree; trust factors over tape." Here, **trusting factors over tape would have produced a flat/down absolute call that was wrong**, while trusting tape produced the right absolute call for a non-healthcare reason.

### The core lesson

**On a strong risk-on tape, "funding source" predicts RELATIVE underperformance, not ABSOLUTE decline.** The morning memo collapsed these two into a single S0 = −0.5, which over-weighted the funding-source effect into the absolute direction. The correct decomposition is:

- **Absolute:** beta × tape ≈ mild up (engine got this).
- **Relative:** rotation-out ≈ notable lag (overlay got this).

The engine and the overlay were each **half right**, and the divergence flag correctly surfaced the split — but the system had no mechanism to *reconcile* them into "up mild absolute / down notable relative." That reconciliation is the improvement to bank.

### Secondary lessons

1. **The 09-18 complement rule ("paid FOMC + PM ≤ 0 → do not accept ES tape_anchor up/mild") fired and was WRONG today.** FOMC was paid, PM:XLV was −0.30%, and the rule said reject up/mild — but up/mild is exactly what printed. This rule needs a **tape-strength qualifier**: it should only suppress the tape_anchor when the tape is *modestly* green, not when SPY is on track for +1.5%. A +1.55% SPY day lifts almost everything, including funding sources.
2. **The 08-13 reversal-tell ban on up/notable was correct** — magnitude was mild, not notable. ✅
3. **The ex-div handling was textbook.** ✅
4. **Single-name discipline was excellent.** ✅
5. **The 1w RS "leftover, do not copy" rule was correct** — XLV did not extend its 1w outperformance. ✅

### What would have improved the call

A **beta-arithmetic sanity check** at the S0 stage: *"SPY futures +1.35–1.55%; XLV beta ~0.65; gross lift ~+0.9–1.0%; rotation drag ~−1.2%; net ≈ flat-to-mild-up absolute, notable-down relative."* That single calculation would have converted the morning's flat/down lean into the correct up/mild absolute call **while preserving** the correct relative-lag call — and would have removed the need for the divergence flag to arbitrate between two half-right models.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: 0.35
SPY_PCT: 1.55
REL_PCT: -1.20
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Tech-led risk-on rotation out of low-beta defensives; XLV rose on market beta but lagged badly as the funding source for the AI/tech bid, with a ~0.38% ex-div mechanical drag on the open.
KEY_INTERACTION: "Funding source" predicts RELATIVE underperformance, not ABSOLUTE decline — on a +1.55% SPY tape, XLV's beta lift (~+0.9–1.0%) roughly offset the rotation drag (~−1.2%), netting mild up absolute / notable down relative. The morning collapsed both into a single S0 = −0.5.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction (up) and magnitude (mild) correct; S0–S4 factor stack was right on the relative call but wrong on the absolute call, and the engine's up/mild was right on absolute for a non-healthcare reason (index_carry/ES beta) — the divergence flag correctly surfaced the split but the system could not reconcile the two half-right models.
OUTCOME_END