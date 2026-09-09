---
trigger_pattern: "When a fresh severe kinetic escalation (US-Iran direct tanker strikes, Brent >$100) is present at the open with a deeply risk-off broad tape (futures down), and the sector's magnitude hit-rate is low (<0.4), the model correctly caps magnitude at mild despite the severity of the catalyst, treating the risk-off tape as a ceiling on energy upside."
corrected_behavior: "No correction needed — this is a validated pattern. Continue applying magnitude discipline when: (a) sector mag hit-rate <0.4, (b) broad tape is risk-off, (c) catalyst is severe but lacks confirmation of >5% oil move or >2% XLE futures. The mild cap was validated for a second consecutive session (09-08: +1.11%, 09-09: +0.83%)."
falsifier: "This lesson would be falsified if a fresh severe kinetic escalation with Brent >$100 produced XLE gains >2% (notable+) despite a risk-off tape — i.e., if the risk-off tape did NOT cap energy upside. It would also be falsified if the model capped at mild and XLE delivered notable gains (>2%) on two consecutive similar setups. The 09-08 and 09-09 actuals (+1.11%, +0.83%) both confirm the mild cap was correct."
current_behavior: "Model correctly identifies fresh severe kinetic escalation as directionally bullish for energy, scores S1=+2 (oil/Hormuz cluster counted once, not double-scored), S2=+1 (breadth confirmation), S4=+1 (tape confirmation), and applies the validated 09-08 magnitude-discipline lesson to cap at mild when mag hit-rate <0.4 and tape is risk-off."
evidence_cited: "Predicted up/mild with total score 8.5; actual XLE +0.83% (dir HIT, mag HIT). The fresh US-Iran escalation (US strikes on 5 Iranian tankers, Iran attacks on 10 ships + Jordan base) pushed Brent above $100, but the risk-off tape (SPY −0.46%) capped XLE's gains — XLE opened at 65.56 and faded to 65.31 close. The magnitude-discipline lesson fired and was validated for the second consecutive session."
error_category: "NONE"
scope: "general"
date: "2026-09-09"
status: "active"
occurrences: "1"
promoted_on: "2026-09-09"
sources: "['2026-09-09_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
No correction needed — this is a validated pattern. Continue applying magnitude discipline when: (a) sector mag hit-rate <0.4, (b) broad tape is risk-off, (c) catalyst is severe but lacks confirmation of >5% oil move or >2% XLE futures. The mild cap was validated for a second consecutive session (09-08: +1.11%, 09-09: +0.83%).

## WHEN IT FIRES
When a fresh severe kinetic escalation (US-Iran direct tanker strikes, Brent >$100) is present at the open with a deeply risk-off broad tape (futures down), and the sector's magnitude hit-rate is low (<0.4), the model correctly caps magnitude at mild despite the severity of the catalyst, treating the risk-off tape as a ceiling on energy upside.

## WRONG IF
This lesson would be falsified if a fresh severe kinetic escalation with Brent >$100 produced XLE gains >2% (notable+) despite a risk-off tape — i.e., if the risk-off tape did NOT cap energy upside. It would also be falsified if the model capped at mild and XLE delivered notable gains (>2%) on two consecutive similar setups. The 09-08 and 09-09 actuals (+1.11%, +0.83%) both confirm the mild cap was correct.

## EVIDENCE
Predicted up/mild with total score 8.5; actual XLE +0.83% (dir HIT, mag HIT). The fresh US-Iran escalation (US strikes on 5 Iranian tankers, Iran attacks on 10 ships + Jordan base) pushed Brent above $100, but the risk-off tape (SPY −0.46%) capped XLE's gains — XLE opened at 65.56 and faded to 65.31 close. The magnitude-discipline lesson fired and was validated for the second consecutive session.

(learn_cycle promote)
