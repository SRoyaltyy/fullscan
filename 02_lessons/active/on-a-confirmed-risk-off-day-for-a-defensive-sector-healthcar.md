---
trigger_pattern: "On a confirmed risk-off day for a defensive sector (Healthcare/XLV), a strong relative tape (rel > +2%) and a high deterministic total are treated as evidence of a severe absolute move. This ignores the beta translation problem: with SPY down only modestly, XLV's low beta converts a large relative beat into a moderate absolute move. The official band is also left unreconciled when the narrative score implies notable but the pipeline emits severe."
corrected_behavior: "For Healthcare/XLV on a risk-off day, if SPY is down less than roughly -1% and there is no fresh sector-wide catalyst (broad biotech M&A/FDA cluster, major policy resolution, etc.), cap the official magnitude at notable even if the relative tape is +2% or stronger and the pipeline total suggests severe. Require explicit severe justification: either a materially larger broad-market drawdown or a fresh healthcare-specific catalyst broad enough to move XLV, not just a single-name outlier. Also reconcile the official band with the narrative score; if the narrative sum totals notable, the official output must not remain severe."
falsifier: "The lesson is falsified if a future Healthcare call with the same setup — confirmed risk-off, SPY only about -0.5% to -0.8%, no fresh broad sector catalyst — produces a scoreboard-confirmed severe absolute XLV move (e.g., +2.5% or more). It is also falsified if the scoring rubric treats +1.6% absolute as severe, but the scoreboard's magnitude_hit=False indicates it does not."
current_behavior: "Healthcare is scored up/severe whenever the macro tape is risk-off, XLV is outperforming SPY strongly, and the relative tape confirms the defensive bid. The magnitude is keyed to relative outperformance rather than to the absolute sector move that is realistically possible given the SPY drawdown and XLV low beta."
evidence_cited: "Scoreboard entry for 2026-08-18: predicted up/severe, actual XLV +1.60% abs / +2.28% rel, SPY -0.68%, magnitude_hit False. Rolling healthcare magnitude accuracy is 0.0 over the last 7 graded runs. Prior severe predictions also overpredicted: 08-10 severe vs +1.67% actual; 08-12 severe vs +0.26% actual. The only fresh same-day healthcare-specific event was an AMLX single-name +55% pop, which was not broad enough to justify XLV severe."
error_category: "B"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
For Healthcare/XLV on a risk-off day, if SPY is down less than roughly -1% and there is no fresh sector-wide catalyst (broad biotech M&A/FDA cluster, major policy resolution, etc.), cap the official magnitude at notable even if the relative tape is +2% or stronger and the pipeline total suggests severe. Require explicit severe justification: either a materially larger broad-market drawdown or a fresh healthcare-specific catalyst broad enough to move XLV, not just a single-name outlier. Also reconcile the official band with the narrative score; if the narrative sum totals notable, the official output must not remain severe.

## WHEN IT FIRES
On a confirmed risk-off day for a defensive sector (Healthcare/XLV), a strong relative tape (rel > +2%) and a high deterministic total are treated as evidence of a severe absolute move. This ignores the beta translation problem: with SPY down only modestly, XLV's low beta converts a large relative beat into a moderate absolute move. The official band is also left unreconciled when the narrative score implies notable but the pipeline emits severe.

## WRONG IF
The lesson is falsified if a future Healthcare call with the same setup — confirmed risk-off, SPY only about -0.5% to -0.8%, no fresh broad sector catalyst — produces a scoreboard-confirmed severe absolute XLV move (e.g., +2.5% or more). It is also falsified if the scoring rubric treats +1.6% absolute as severe, but the scoreboard's magnitude_hit=False indicates it does not.

## EVIDENCE
Scoreboard entry for 2026-08-18: predicted up/severe, actual XLV +1.60% abs / +2.28% rel, SPY -0.68%, magnitude_hit False. Rolling healthcare magnitude accuracy is 0.0 over the last 7 graded runs. Prior severe predictions also overpredicted: 08-10 severe vs +1.67% actual; 08-12 severe vs +0.26% actual. The only fresh same-day healthcare-specific event was an AMLX single-name +55% pop, which was not broad enough to justify XLV severe.

(learn_cycle promote)
