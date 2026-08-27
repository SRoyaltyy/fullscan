---
trigger_pattern: "A Consumer Cyclical/XLY call maps green ES/NQ into S0-positive because the 08-21 reversal checklist is on (futures ≥ +0.3%, real yields easing, oil off highs), even though the NQ bid is a known semiconductor/AI mega-cap earnings follow-through rather than AMZN/TSLA cyclical participation, XLY’s own 1d/3d/1w relative tape is already negative, and the consumer spine (confidence, real goods spending, retail) is still soft."
current_behavior: "Treats NQ leadership as XLY-positive beta, scores S0 = +1, leaves S4 at 0 despite a persistent relative lag, overweights multi-month inflows, and relieves any PCE downside tilt on core-in-line. Narrative goes up/mild; pipeline stays flat; neither follows the sector tape."
corrected_behavior: "Do not import ES/NQ as XLY direction when NQ >> ES on a known AI/semis follow-through and XLY is already lagging 1d/3d/1w vs SPY. Cap S0 at 0 (non-transmission). Let S1 (soft consumer spine) and S4 (relative lag) bind; stale 1m/3m inflows are not a same-session bid. 08-21 only sets direction to the tape after a risk-off down-bias when the sector’s own tape is mixed/neutral and the bid is not a narrow AI rotation. Score headline PCE / real goods demand for XLY, not core-in-line as an all-clear. Prefer down/mild (or down/flat), not up, and do not force notable solely from concentration."
evidence_cited: "2026-08-27 predicted flat/flat (narrative up/mild); XLY −1.09%, SPY +0.66%, rel −1.75%, open 116.54 → close 115.88. NVDA ~+9% led XLK; AMZN ~−1.3–1.5% and HD/MCD down while TSLA ~+2.5% failed to save the ETF. Headline PCE 3.7% vs ~3.6% with goods PCE −$49.9B; Conference Board 89.4 / Expectations 68.2 already out."
error_category: "B"
falsifier: "If NQ >> ES on a known AI/semis follow-through, XLY already lagging 1d/3d/1w, and the consumer spine still soft, yet XLY closes up and outperforms SPY by more than ~0.5% with no offsetting sector-specific positive, demote the non-transmission rule."
sector: "Consumer Cyclical"
date: "2026-08-27"
status: "candidate"
---

# Sector Reflection — Consumer Cyclical — 2026-08-27

Triage: **reasoning failure (B)**, not a tool/data failure. Futures, the lagging XLY tape, and the soft consumer spine were all in the morning packet. The miss was mapping an NVDA/NQ bid into XLY beta via an over-applied 08-21 reversal checklist. Calendar noise (PCE/NVDA treated as still-pending on 8/27) is secondary; even with those labels, S0/S4 were misweighted.

**CHECK 1 — LESSON MATCH:** Closest match is the 08-27 Communication Services / Basic Materials candidate pattern: green NQ/ES on a known mega-cap AI follow-through does not transmit to a sector whose own 1d relative tape is already negative. Same family as 08-25 Industrials (futures bounce does not rescue a laggard). The 08-25 XLY pending-print lesson is only a partial match (wrong PCE slice / “relieved” tilt), not the driver. **08-21 was applied and hurt** — over-applied beyond its trigger (stale down-bias after a risk-off day, no fresh catalyst). This is not a retrieval miss of a Consumer Cyclical rule that already said “don’t import NQ as XLY”; that gate needs to be written.

**CHECK 2 — BACKWARD TEST:** Helps 08-27. Does not break 08-21 if narrowed: 08-21 was a post-risk-off recovery with stale negatives and a live bounce; 08-27 is XLY already lagging 1d/3d/1w on a green SPY while NQ >> ES on an AI earnings tape. Would not have fired on 08-25 (XLY 1d rel was positive). 08-12/08-17/08-18 are different spines (idiosyncratic name, hard-data+event week, tech-led selloff composition). No similar recent day where blocking NQ→XLY would have flipped a correct up call.

**CHECK 3 — CONFLICT:** Apparent conflict with active 08-21. Resolution: 08-21 still governs stale-down vs a genuine cyclical recovery tape after risk-off. It does **not** fire when (a) XLY 1d/3d/1w relative tape is already negative vs SPY and (b) the futures bid is a known AI/semis earnings follow-through rather than discretionary beta. No conflict with 08-11 (oil, correctly not fired), 08-18 (severe cap; not in play), 08-12 (don’t retrofit idiosyncratic notable), or 08-17 (flat-futures notable; futures were not flat).

**CHECK 4 — APPLIED-LESSON REVIEW:** 08-21 applied → **hurt**. 08-25 candidate applied as “core in-line, tilt relieved” → **hurt** (headline + real goods PCE were the XLY lines; core-in-line is not an all-clear). 08-11 correctly not fired. 08-18/08-12 not applicable. 08-17 correctly did not force notable (futures were green). Pipeline **flat/flat** was less wrong than narrative **up/mild**; both missed **down**.

**CHECK 5 — FALSIFIER:** If this setup recurs — NQ >> ES on a known AI/semis follow-through, XLY already lagging 1d/3d/1w, soft consumer spine still live — and XLY still closes up and beats SPY by >~0.5% (or >+0.5% absolute) with no offsetting sector-specific positive, the non-transmission rule is too strong and should be revised.

**Divergence:** Narrative flagged it; pipeline did not. Leading (S1 soft spine + lagging XLY tape) was right for this ETF. Futures were right for SPY/XLK, not for XLY. **leading_right.** Knowable-at-open: **partially** (lag, concentration, confidence/retail, and if this is Thursday then NVDA+PCE were already public). How hard AMZN refused the AI bid was not fully knowable.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A Consumer Cyclical/XLY call maps green ES/NQ into S0-positive because the 08-21 reversal checklist is on (futures ≥ +0.3%, real yields easing, oil off highs), even though the NQ bid is a known semiconductor/AI mega-cap earnings follow-through rather than AMZN/TSLA cyclical participation, XLY’s own 1d/3d/1w relative tape is already negative, and the consumer spine (confidence, real goods spending, retail) is still soft.
CURRENT_BEHAVIOR: Treats NQ leadership as XLY-positive beta, scores S0 = +1, leaves S4 at 0 despite a persistent relative lag, overweights multi-month inflows, and relieves any PCE downside tilt on core-in-line. Narrative goes up/mild; pipeline stays flat; neither follows the sector tape.
CORRECTED_BEHAVIOR: Do not import ES/NQ as XLY direction when NQ >> ES on a known AI/semis follow-through and XLY is already lagging 1d/3d/1w vs SPY. Cap S0 at 0 (non-transmission). Let S1 (soft consumer spine) and S4 (relative lag) bind; stale 1m/3m inflows are not a same-session bid. 08-21 only sets direction to the tape after a risk-off down-bias when the sector’s own tape is mixed/neutral and the bid is not a narrow AI rotation. Score headline PCE / real goods demand for XLY, not core-in-line as an all-clear. Prefer down/mild (or down/flat), not up, and do not force notable solely from concentration.
EVIDENCE: 2026-08-27 predicted flat/flat (narrative up/mild); XLY −1.09%, SPY +0.66%, rel −1.75%, open 116.54 → close 115.88. NVDA ~+9% led XLK; AMZN ~−1.3–1.5% and HD/MCD down while TSLA ~+2.5% failed to save the ETF. Headline PCE 3.7% vs ~3.6% with goods PCE −$49.9B; Conference Board 89.4 / Expectations 68.2 already out.
LESSON_MATCH_CHECK: Matches 08-27 Communication Services and Basic Materials candidates (tech-led NQ bid does not transmit to a lagging non-AI book) and 08-25 Industrials (futures bounce does not rescue a laggard). Distinct from 08-25 XLY pending-print lesson. 08-21 was applied, not missing — over-applied past its trigger.
BACKWARD_CHECK: Helps 08-27. Does not overturn 08-21 if gated on post-risk-off recovery vs AI-narrow tape plus XLY already lagging. Would not have fired on 08-25 (XLY 1d rel positive). No hurt on 08-12/08-17/08-18.
CONFLICT_CHECK: Narrows 08-21 rather than replacing it: 08-21 still governs stale-down vs genuine cyclical recovery; this lesson governs NQ-led AI follow-through while XLY’s own tape is already negative. No conflict with 08-11/08-12/08-17/08-18.
FALSIFIER: If NQ >> ES on a known AI/semis follow-through, XLY already lagging 1d/3d/1w, and the consumer spine still soft, yet XLY closes up and outperforms SPY by more than ~0.5% with no offsetting sector-specific positive, demote the non-transmission rule.
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-21 applied and hurt (imported NVDA/NQ as XLY beta). 08-25 PCE-tilt candidate applied as “relieved on core-in-line” and hurt. 08-11 correctly not fired. 08-18/08-12 not applicable. 08-17 correctly did not force notable.
SECTOR: Consumer Cyclical
LESSON_END
