---
trigger_pattern: "An Energy/XLE call is scored up/severe from a geopolitical supply-shock catalyst whose oil-price sign is correct, but the catalyst has already driven a large relative run in XLE (3d/1w rel > +4%) and is therefore largely priced in; a same-day official report (IEA/OPEC/EIA) contains demand-destruction or two-sided supply/demand signals, and the ETF's current-day relative tape is not confirming fresh leadership at the open."
corrected_behavior: "Before assigning severe to Energy, decompose S1 into (a) catalyst sign/freshness and (b) expected transmission to XLE. If XLE has already rallied 1w rel > +4% on the same geopolitical narrative, treat the headline as a continuation, not a new shock. If a same-day official report contains demand destruction or a two-sided signal, apply it as a direct S1 negative offset. Cap S1 at +1.0 and multiplier at 1.0 unless the current-day 1d tape shows fresh XLE relative leadership (not just prior-day momentum). With a continuing-but-stale catalyst, emit at most up/notable. Preserve severe only for fresh escalations with clear same-day ETF transmission."
falsifier: "On a future Energy session, if XLE enters with 1w rel > +4%, oil rises >2% on a continuing-but-not-new Hormuz headline, a same-day IEA/EIA demand-cut report is published, yet XLE closes up >2% (severe), then this lesson's magnitude cap would be falsified. It would also be falsified if applying it systematically made Energy calls too conservative and caused repeated missed severe upside in fresh oil shocks."
current_behavior: "The model treats a correct live oil-price sign and an active geopolitical premium as sufficient to score S1_SECTOR_FACTORS = +2.0, apply multiplier 1.2, and emit up/severe — without checking whether the catalyst is incrementally fresh vs. already embedded in XLE's recent relative run, and without fully offsetting a knowable same-day demand-destruction report (e.g., IEA demand-cut)."
evidence_cited: "2026-08-12 morning: XLE predicted up/severe (S1=+2.0, S4=+0.5, mult=1.2, total=12.0). Outcome: XLE +0.16%, SPY +0.25%, rel -0.09% — direction hit, magnitude badly missed. Oil was indeed up (~2% overnight, Brent near $90), so the active 08-11 energy lesson (verify live oil sign) was applied and passed. But the geopolitical premium was already priced after XLE's prior 3d rel +4.76% and 1w rel +4.12%; the IEA Aug 12 report simultaneously cut H2 demand, capping transmission. Oil +2% did not translate into XLE upside — a stale-catalyst/transmission failure."
error_category: "B"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
Before assigning severe to Energy, decompose S1 into (a) catalyst sign/freshness and (b) expected transmission to XLE. If XLE has already rallied 1w rel > +4% on the same geopolitical narrative, treat the headline as a continuation, not a new shock. If a same-day official report contains demand destruction or a two-sided signal, apply it as a direct S1 negative offset. Cap S1 at +1.0 and multiplier at 1.0 unless the current-day 1d tape shows fresh XLE relative leadership (not just prior-day momentum). With a continuing-but-stale catalyst, emit at most up/notable. Preserve severe only for fresh escalations with clear same-day ETF transmission.

## WHEN IT FIRES
An Energy/XLE call is scored up/severe from a geopolitical supply-shock catalyst whose oil-price sign is correct, but the catalyst has already driven a large relative run in XLE (3d/1w rel > +4%) and is therefore largely priced in; a same-day official report (IEA/OPEC/EIA) contains demand-destruction or two-sided supply/demand signals, and the ETF's current-day relative tape is not confirming fresh leadership at the open.

## WRONG IF
On a future Energy session, if XLE enters with 1w rel > +4%, oil rises >2% on a continuing-but-not-new Hormuz headline, a same-day IEA/EIA demand-cut report is published, yet XLE closes up >2% (severe), then this lesson's magnitude cap would be falsified. It would also be falsified if applying it systematically made Energy calls too conservative and caused repeated missed severe upside in fresh oil shocks.

## EVIDENCE
2026-08-12 morning: XLE predicted up/severe (S1=+2.0, S4=+0.5, mult=1.2, total=12.0). Outcome: XLE +0.16%, SPY +0.25%, rel -0.09% — direction hit, magnitude badly missed. Oil was indeed up (~2% overnight, Brent near $90), so the active 08-11 energy lesson (verify live oil sign) was applied and passed. But the geopolitical premium was already priced after XLE's prior 3d rel +4.76% and 1w rel +4.12%; the IEA Aug 12 report simultaneously cut H2 demand, capping transmission. Oil +2% did not translate into XLE upside — a stale-catalyst/transmission failure.

(learn_cycle promote)
