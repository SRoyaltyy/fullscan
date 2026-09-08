---
trigger_pattern: "When a sector has a fresh overnight geopolitical catalyst (oil supply shock) that is NOT in the morning's Channel 1 data, AND the broad equity tape is deeply risk-off (ES −0.44%, SPY −0.55%), the model over-extrapolates the fresh catalyst into a notable magnitude call despite (a) the sector's low magnitude hit-rate (~0.3), (b) the risk-off tape capping extension, and (c) the catalyst being partially priced from the prior session's run. The model's own magnitude discipline note ('Energy mag hit-rate is 0.3', 'Do not emit severe') flags the risk but does not cascade into a mild call when the fresh headline is strong."
corrected_behavior: "When a fresh overnight catalyst is present but the broad tape is deeply risk-off AND the sector's magnitude hit-rate is ≤0.3, the model should default to mild unless there is evidence the catalyst can overcome the risk-off cap (e.g., pre-market XLE futures up >2%, oil up >5% with no fade). The magnitude discipline note should be operationalized: if mag hit-rate <0.4 and SPY/ES are red >0.3%, cap at mild regardless of catalyst strength. The fresh catalyst justifies direction (up) but not magnitude escalation when the tape is risk-off — the risk-off tape historically caps energy extension near +1% (actual +1.11%)."
falsifier: "A session where oil is up >3%, SPY is red >0.3%, and XLE still delivers >2% (notable) would falsify this lesson. Also falsified if Energy's mag hit-rate improves above 0.5 over the next 10 graded runs while the rule remains in place."
current_behavior: "Model scores S1 = +2 on a live oil/Hormuz cluster, sees a fresh overnight catalyst (Houthi-Saudi attack) that was not in Channel 1, and upgrades magnitude to 'notable' (total 8.5) because the catalyst is strong and 08-14 allows notable when oil is green and Hormuz is live. The model treats the fresh catalyst as a magnitude accelerant without weighting the offsetting factors: deeply red equity tape (ES −0.44%), the sector's 0.3 mag hit-rate, and the fact that some of the oil move was already priced from the prior session."
evidence_cited: "Predicted up/notable (total 8.5) vs actual up/mild (+1.11%). Direction HIT, magnitude MISS. The morning's own note flagged 'Energy mag hit-rate is 0.3' and 'Do not emit severe' but still called notable. Actual +1.11% is squarely mild. The fresh Houthi headline was strong (Brent to $98–99) but the red tape (SPY −0.55%) capped extension. Prior session's run had already priced part of the move."
error_category: "B"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
When a fresh overnight catalyst is present but the broad tape is deeply risk-off AND the sector's magnitude hit-rate is ≤0.3, the model should default to mild unless there is evidence the catalyst can overcome the risk-off cap (e.g., pre-market XLE futures up >2%, oil up >5% with no fade). The magnitude discipline note should be operationalized: if mag hit-rate <0.4 and SPY/ES are red >0.3%, cap at mild regardless of catalyst strength. The fresh catalyst justifies direction (up) but not magnitude escalation when the tape is risk-off — the risk-off tape historically caps energy extension near +1% (actual +1.11%).

## WHEN IT FIRES
When a sector has a fresh overnight geopolitical catalyst (oil supply shock) that is NOT in the morning's Channel 1 data, AND the broad equity tape is deeply risk-off (ES −0.44%, SPY −0.55%), the model over-extrapolates the fresh catalyst into a notable magnitude call despite (a) the sector's low magnitude hit-rate (~0.3), (b) the risk-off tape capping extension, and (c) the catalyst being partially priced from the prior session's run. The model's own magnitude discipline note ("Energy mag hit-rate is 0.3", "Do not emit severe") flags the risk but does not cascade into a mild call when the fresh headline is strong.

## WRONG IF
A session where oil is up >3%, SPY is red >0.3%, and XLE still delivers >2% (notable) would falsify this lesson. Also falsified if Energy's mag hit-rate improves above 0.5 over the next 10 graded runs while the rule remains in place.

## EVIDENCE
Predicted up/notable (total 8.5) vs actual up/mild (+1.11%). Direction HIT, magnitude MISS. The morning's own note flagged "Energy mag hit-rate is 0.3" and "Do not emit severe" but still called notable. Actual +1.11% is squarely mild. The fresh Houthi headline was strong (Brent to $98–99) but the red tape (SPY −0.55%) capped extension. Prior session's run had already priced part of the move.

(learn_cycle promote)
