# Sector Reflect — Consumer Cyclical — 2026-08-25

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: On a scheduled release day, a sector-specific high-impact hard-data print (e.g., Conference Board Consumer Confidence for Consumer Cyclical) is pending at open, consensus is below the prior reading, and the forward-looking component is the relevant spending lead. The model names it “the sector’s own print” and a “two-sided binary,” but then neutralizes it in S1 by treating unprinted data as event risk rather than live directional risk. When the print misses, the discretionary ETF underperforms even on a rising SPY because the sector’s own expectations component overrides the broad risk-on macro bid.

CURRENT_BEHAVIOR: Scheduled same-day sector-own prints are scored as zero before release (“it has not printed”), while S0 gives full credit to broad futures/equity risk-on. This can produce an up/flat call when the binding variable for the sector is an unresolved 10:00 ET confidence/expectations print.

CORRECTED_BEHAVIOR: When the pending release is the sector’s own scheduled high-impact print, do not fully neutralize it. Assign S1 a live two-sided weight with explicit downside asymmetry when consensus is below prior and the forward-looking sub-index (e.g., expectations) is the spending lead. At minimum, reduce S0 risk-on credit for that sector and cap the total score so a same-day sector-own miss cannot be swamped by a broad market bid. The goal is not to forecast the exact number, but to avoid emitting a confident up/flat call while the sector’s own binary is unresolved and skewed toward downside.

EVIDENCE: On 2026-08-25, the morning called XLY up/flat with S0=+1 (ES +0.44%, NQ +0.92%), S1=0 (Conference Board “unprinted”, “two-sided”), S4=+1 (carried relative tape). At 10:00 ET, Conference Board printed 89.4 vs 90.1 consensus, with the Expectations Index collapsing to 68.2 — a seven-month low. SPY still rose +0.32%, but XLY closed -0.296% and underperformed by -0.615% relative. The sector’s own print, not the broad risk-on tape, drove the sector. The morning explicitly identified the Conference Board as the sector’s own binary but then scored it as zero.

LESSON_MATCH_CHECK: No existing lesson directly covers weighting a same-day scheduled sector-own macro print. The 08-21 industrials lesson is about avoiding stale macro overweight against a positive futures bounce; this case is the inverse — underweighting a fresh, scheduled, sector-defining print. The 08-25 communication services lesson covers scorecard `None/None` corruption, which is present in the scoreboard but is not the cause of the direction miss. The 08-18 single-ticker/DKS discipline was applied correctly and is not implicated.

BACKWARD_CHECK: Applying this rule to the provided history does not create a new miss. On 08-14, the same-day retail sales miss was already scored as a downside factor and the down/mild call hit. On 08-21, there was no same-day scheduled sector-own XLY print in the facts, so the rule would not fire and would not have blocked the reversal-lesson correction. For 08-10, 08-12, 08-13, 08-17, and 08-18, no equivalent same-day sector-own consumer-confidence binary is identified in the supplied context, so the rule would not change those outcomes. Backward check passes with available data.

CONFLICT_CHECK: This does not conflict with the 08-17 stale-hard-data lesson or the 08-21 industrials lesson. Those lessons address data that is one or more sessions old and already absorbed in the tape; this rule targets same-session scheduled releases that are still unresolved at prediction time. It also does not conflict with single-ticker severity rules: DKS remained correctly non-driving; the driver was the aggregate confidence print.

FALSIFIER: If a Conference Board/consumer-confidence miss prints and XLY still closes up or outperforms SPY, the claimed downside asymmetry is refuted. Similarly, if a confidence beat prints and XLY falls anyway, the sector-own-print linkage is too mechanical. More generally, if treating scheduled sector-own prints as live directional inputs does not improve rolling direction accuracy over the next 20 sector-days, this lesson should be revised or dropped.

DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: Relevant active lessons were checked. The 08-17 stale-data lesson held (stale July retail/UMich were not allowed to drive the call). The 08-18 composition/single-ticker cap held (DKS is not an XLY weight and correctly did not drive the ETF call). The 08-21 reversal checklist was applied but was partial and insufficient against the scheduled sector-own print. The 08-25 communication services scoreboard-`None` issue appears in the scoreboard but is a separate pipeline/data-integrity problem; even restoring `up/flat` in the scoreboard, the direction call would still be a miss. The missing active lesson is the scheduled same-day sector-own print weighting rule above.

SECTOR: Consumer Cyclical
LESSON_END
