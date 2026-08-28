---
trigger_pattern: "A cyclical sector ETF that remains a 1w/1m relative laggard has just posted a strong one-day relative bounce (>~1% vs SPY); the next session a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES, and S1 is being filled with stale surveys plus already-traded awards (IDIQ ceilings, closed M&A)."
corrected_behavior: "Score that 1d bounce once — not in S2, S4, and HIT_GRID. When the live catalyst is a non-holdings mega-cap AHR already public and NQ leads ES, do not default follow-through up for a 1w/1m laggard cyclical; prefer flat or down:mild. Cap S1 at 0/+1 unless a fresh same-morning sector print confirms; verify award economics (ceiling vs obligated) and whether the name already traded the news. Confirm the economic calendar so a prior-session PCE/CPI/NFP is not scored as today’s binary. Treat 08-21 green futures as necessary to avoid a stale-macro down call, not sufficient for up when the impulse is XLK-idiosyncratic. Reconcile narrative band with the pipeline before emit."
falsifier: "If this setup recurs (prior-day XLI rel >~+1%, 1w/1m still lagging, mega-cap AI AHR already public, NQ leading ES, S1 only stale survey/already-traded awards) and XLI still closes up without lagging SPY, the flat/down:mild preference is too strong and must relax to flat/mild."
current_behavior: "Treats the prior-session 1d relative bounce as live S2 and S4 confirmation (and again in HIT_GRID), holds S1 at +2 on 3-week-old ISM plus a multi-day-old defense ceiling and prior-session M&A, misdates a prior-day PCE print as today’s two-sided binary, applies the 08-21 green-futures checklist as an up license, and lets the pipeline emit up/notable while the writeup says up/mild."
evidence_cited: "2026-08-27 XLI predicted up/notable (pipeline 7.65; narrative up/mild +3.6); actual −0.85% vs SPY +0.66%, rel −1.51% (dir MISS, mag MISS). Path: open 180.47 → close 178.80. XLK +3.16%, 1 of 11 sectors up after NVDA 8/26 AHR ($96.2B / Q3 $108B). PCE printed 8/26 (headline +0.2% / +3.7% y/y), not 8/27. BA ~−1.0% on an 8/24 F-15 IDIQ ceiling; AME Indicor closed 8/26. Aug 26 XLI rel +1.07% fully reversed."
error_category: "B"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Score that 1d bounce once — not in S2, S4, and HIT_GRID. When the live catalyst is a non-holdings mega-cap AHR already public and NQ leads ES, do not default follow-through up for a 1w/1m laggard cyclical; prefer flat or down:mild. Cap S1 at 0/+1 unless a fresh same-morning sector print confirms; verify award economics (ceiling vs obligated) and whether the name already traded the news. Confirm the economic calendar so a prior-session PCE/CPI/NFP is not scored as today’s binary. Treat 08-21 green futures as necessary to avoid a stale-macro down call, not sufficient for up when the impulse is XLK-idiosyncratic. Reconcile narrative band with the pipeline before emit.

## WHEN IT FIRES
A cyclical sector ETF that remains a 1w/1m relative laggard has just posted a strong one-day relative bounce (>~1% vs SPY); the next session a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES, and S1 is being filled with stale surveys plus already-traded awards (IDIQ ceilings, closed M&A).

## WRONG IF
If this setup recurs (prior-day XLI rel >~+1%, 1w/1m still lagging, mega-cap AI AHR already public, NQ leading ES, S1 only stale survey/already-traded awards) and XLI still closes up without lagging SPY, the flat/down:mild preference is too strong and must relax to flat/mild.

## EVIDENCE
2026-08-27 XLI predicted up/notable (pipeline 7.65; narrative up/mild +3.6); actual −0.85% vs SPY +0.66%, rel −1.51% (dir MISS, mag MISS). Path: open 180.47 → close 178.80. XLK +3.16%, 1 of 11 sectors up after NVDA 8/26 AHR ($96.2B / Q3 $108B). PCE printed 8/26 (headline +0.2% / +3.7% y/y), not 8/27. BA ~−1.0% on an 8/24 F-15 IDIQ ceiling; AME Indicor closed 8/26. Aug 26 XLI rel +1.07% fully reversed.

(learn_cycle promote)
