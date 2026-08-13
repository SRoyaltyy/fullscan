---
trigger_pattern: "A Communication Services/XLC call faces a live geopolitical/oil supply-shock risk (e.g., Hormuz/oil near $90), rising real yields, unresolved Alphabet/Meta AI-capex/FCF negatives, and XLC persistently lagging SPY on 1d/1w/1m, while premarket futures are positive and a scheduled CPI print is moderate. The correct output is down/mild; positive futures and a benign CPI are offsets, not reasons to flip the sector call to up."
corrected_behavior: "No correction required. Continue to cap at flat/down caution when the 2026-08-11 geopolitical/oil risk-off lesson is active, even with positive futures. Optional refinement: when both Alphabet and Meta are under fresh negative AI-capex/FCF catalysts at the open, S1 could be scored negative rather than neutral, since XLC’s two-stock concentration can turn a mild absolute decline into notable relative underperformance."
falsifier: "The pattern would be falsified if XLC rose or matched SPY in a future setup where Alphabet and Meta are both under fresh AI-capex/FCF scrutiny, Hormuz/oil risk is active, and real yields are rising. It would also need re-examination if the scoreboard classified the same setup as a magnitude miss rather than a mild hit."
current_behavior: "The model correctly scores S0/S2/S3/S4 negative, keeps S1 neutral, applies multiplier 1.0, and emits down/mild. It treats the live geopolitical/oil risk-off as a cap on the call and does not convert prior-week flows into same-day support."
evidence_cited: "XLC -0.90% vs SPY +0.25%, relative -1.15%; scoreboard direction_hit True, magnitude_hit True. Prediction was down/mild with total -6.0 and components S0 -1, S1 0, S2 -1, S3 -1, S4 -1. Primary driver was Alphabet AI-capex scrutiny; all key inputs were knowable at the open."
error_category: "NONE"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
No correction required. Continue to cap at flat/down caution when the 2026-08-11 geopolitical/oil risk-off lesson is active, even with positive futures. Optional refinement: when both Alphabet and Meta are under fresh negative AI-capex/FCF catalysts at the open, S1 could be scored negative rather than neutral, since XLC’s two-stock concentration can turn a mild absolute decline into notable relative underperformance.

## WHEN IT FIRES
A Communication Services/XLC call faces a live geopolitical/oil supply-shock risk (e.g., Hormuz/oil near $90), rising real yields, unresolved Alphabet/Meta AI-capex/FCF negatives, and XLC persistently lagging SPY on 1d/1w/1m, while premarket futures are positive and a scheduled CPI print is moderate. The correct output is down/mild; positive futures and a benign CPI are offsets, not reasons to flip the sector call to up.

## WRONG IF
The pattern would be falsified if XLC rose or matched SPY in a future setup where Alphabet and Meta are both under fresh AI-capex/FCF scrutiny, Hormuz/oil risk is active, and real yields are rising. It would also need re-examination if the scoreboard classified the same setup as a magnitude miss rather than a mild hit.

## EVIDENCE
XLC -0.90% vs SPY +0.25%, relative -1.15%; scoreboard direction_hit True, magnitude_hit True. Prediction was down/mild with total -6.0 and components S0 -1, S1 0, S2 -1, S3 -1, S4 -1. Primary driver was Alphabet AI-capex scrutiny; all key inputs were knowable at the open.

(learn_cycle promote)
