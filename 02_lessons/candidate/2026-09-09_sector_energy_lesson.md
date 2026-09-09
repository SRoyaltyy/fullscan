---
trigger_pattern: "When a fresh severe kinetic escalation (US-Iran direct tanker strikes, Brent >$100) is present at the open with a deeply risk-off broad tape (futures down), and the sector's magnitude hit-rate is low (<0.4), the model correctly caps magnitude at mild despite the severity of the catalyst, treating the risk-off tape as a ceiling on energy upside."
current_behavior: "Model correctly identifies fresh severe kinetic escalation as directionally bullish for energy, scores S1=+2 (oil/Hormuz cluster counted once, not double-scored), S2=+1 (breadth confirmation), S4=+1 (tape confirmation), and applies the validated 09-08 magnitude-discipline lesson to cap at mild when mag hit-rate <0.4 and tape is risk-off."
corrected_behavior: "No correction needed — this is a validated pattern. Continue applying magnitude discipline when: (a) sector mag hit-rate <0.4, (b) broad tape is risk-off, (c) catalyst is severe but lacks confirmation of >5% oil move or >2% XLE futures. The mild cap was validated for a second consecutive session (09-08: +1.11%, 09-09: +0.83%)."
evidence_cited: "Predicted up/mild with total score 8.5; actual XLE +0.83% (dir HIT, mag HIT). The fresh US-Iran escalation (US strikes on 5 Iranian tankers, Iran attacks on 10 ships + Jordan base) pushed Brent above $100, but the risk-off tape (SPY −0.46%) capped XLE's gains — XLE opened at 65.56 and faded to 65.31 close. The magnitude-discipline lesson fired and was validated for the second consecutive session."
error_category: "NONE"
falsifier: "This lesson would be falsified if a fresh severe kinetic escalation with Brent >$100 produced XLE gains >2% (notable+) despite a risk-off tape — i.e., if the risk-off tape did NOT cap energy upside. It would also be falsified if the model capped at mild and XLE delivered notable gains (>2%) on two consecutive similar setups. The 09-08 and 09-09 actuals (+1.11%, +0.83%) both confirm the mild cap was correct."
sector: "Energy"
date: "2026-09-09"
status: "candidate"
---

# Sector Reflection — Energy — 2026-09-09

LESSON_BEGIN
ERROR_CATEGORY: NONE
TRIGGER_PATTERN: When a fresh severe kinetic escalation (US-Iran direct tanker strikes, Brent >$100) is present at the open with a deeply risk-off broad tape (futures down), and the sector's magnitude hit-rate is low (<0.4), the model correctly caps magnitude at mild despite the severity of the catalyst, treating the risk-off tape as a ceiling on energy upside.
CURRENT_BEHAVIOR: Model correctly identifies fresh severe kinetic escalation as directionally bullish for energy, scores S1=+2 (oil/Hormuz cluster counted once, not double-scored), S2=+1 (breadth confirmation), S4=+1 (tape confirmation), and applies the validated 09-08 magnitude-discipline lesson to cap at mild when mag hit-rate <0.4 and tape is risk-off.
CORRECTED_BEHAVIOR: No correction needed — this is a validated pattern. Continue applying magnitude discipline when: (a) sector mag hit-rate <0.4, (b) broad tape is risk-off, (c) catalyst is severe but lacks confirmation of >5% oil move or >2% XLE futures. The mild cap was validated for a second consecutive session (09-08: +1.11%, 09-09: +0.83%).
EVIDENCE: Predicted up/mild with total score 8.5; actual XLE +0.83% (dir HIT, mag HIT). The fresh US-Iran escalation (US strikes on 5 Iranian tankers, Iran attacks on 10 ships + Jordan base) pushed Brent above $100, but the risk-off tape (SPY −0.46%) capped XLE's gains — XLE opened at 65.56 and faded to 65.31 close. The magnitude-discipline lesson fired and was validated for the second consecutive session.
LESSON_MATCH_CHECK: Matches 2026-09-08_sector_energy_lesson.md — the magnitude-discipline lesson (cap at mild when mag hit-rate <0.4 + risk-off tape) fired again and was validated. Also matches 2026-09-09_lesson.md (fresh overnight kinetic/oil-supply escalation pattern) — correctly identified as a step-change beyond day-2 continuation.
BACKWARD_CHECK: Backward test passes — if this lesson had been applied to 09-08 (predicted up/mild, actual +1.11%), it would have been correct. The 09-08 lesson was itself validated by 09-08's actual, and now 09-09 validates it again. No contradiction with prior correct calls.
CONFLICT_CHECK: No conflict with active lessons. The 08-14 green-oil + live Hormuz lesson (S1=+2 not +1) fired correctly. The 08-12 stale-severe cap and 08-21 overbought decoupling correctly did NOT fire (1w rel +0.57%, not >+4% or >+5%). The 09-03/09-04 exhaustion lesson correctly did NOT fire (oil up sharply, not offered). All lessons consistent.
FALSIFIER: This lesson would be falsified if a fresh severe kinetic escalation with Brent >$100 produced XLE gains >2% (notable+) despite a risk-off tape — i.e., if the risk-off tape did NOT cap energy upside. It would also be falsified if the model capped at mild and XLE delivered notable gains (>2%) on two consecutive similar setups. The 09-08 and 09-09 actuals (+1.11%, +0.83%) both confirm the mild cap was correct.
DIVERGENCE_VERDICT: none_flagged — factors and tape agreed up; no leading-vs-tape divergence. The model correctly trusted factors over the red ES print.
ACTIVE_LESSON_REVIEW: The 09-08 magnitude-discipline lesson is now validated twice (09-08: +1.11%, 09-09: +0.83%). This lesson should be considered firmly established. The 09-09 general lesson (fresh overnight kinetic/oil-supply escalation → B1=−3 when severe and fresh) fired correctly in S1 scoring. No new lesson needed — this run confirms existing lessons rather than revealing a new pattern.
SECTOR: Energy
LESSON_END
