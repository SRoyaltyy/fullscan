---
trigger_pattern: "Consumer Cyclical (XLY) is called down/mild from macro caution, but actual outcome is down/notable because a same-day company-specific shock hits one of XLY's top 2-3 mega-cap holdings (e.g., CEO leadership news, single-name earnings/valuation shock) while the broad tape (SPY) is flat/up and the scheduled macro catalyst is benign. The specific shock is absent from all pre-open channels and is therefore not knowable at the open."
corrected_behavior: "Do not retrofit a magnitude correction. Keep the pre-open output when it is supported by the available data; do not systematically change down/mild to down/notable merely because XLY has high single-name concentration. In concentrated sectors, note explicitly that magnitude bands are less reliable and set confidence lower, but do not manufacture a catalyst that is not in the evidence."
falsifier: "A future Consumer Cyclical call with the same concentration and no pre-open idiosyncratic catalyst that closes flat/mild would falsify any rule that automatically scores down/notable on concentration alone. The correct implication is that today's miss is idiosyncratic and non-systematic."
current_behavior: "The pipeline converts the pre-open evidence into a defensible down/mild call and does not upgrade the magnitude band for un-knowable idiosyncratic tail risk. After the fact, the sector appears to be a magnitude miss even though no pre-open process error occurred."
evidence_cited: "2026-08-12 predicted down/mild / -4.0; actual XLY -1.13%, SPY +0.25%, rel -1.38%. Direction HIT, magnitude MISS. CPI was in-line; SPY rose; XLY fell on Home Depot CEO medical leave announced during the day and Tesla -1.59%. Neither shock was knowable from the pre-open data bundle."
error_category: "NONE"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Do not retrofit a magnitude correction. Keep the pre-open output when it is supported by the available data; do not systematically change down/mild to down/notable merely because XLY has high single-name concentration. In concentrated sectors, note explicitly that magnitude bands are less reliable and set confidence lower, but do not manufacture a catalyst that is not in the evidence.

## WHEN IT FIRES
Consumer Cyclical (XLY) is called down/mild from macro caution, but actual outcome is down/notable because a same-day company-specific shock hits one of XLY's top 2-3 mega-cap holdings (e.g., CEO leadership news, single-name earnings/valuation shock) while the broad tape (SPY) is flat/up and the scheduled macro catalyst is benign. The specific shock is absent from all pre-open channels and is therefore not knowable at the open.

## WRONG IF
A future Consumer Cyclical call with the same concentration and no pre-open idiosyncratic catalyst that closes flat/mild would falsify any rule that automatically scores down/notable on concentration alone. The correct implication is that today's miss is idiosyncratic and non-systematic.

## EVIDENCE
2026-08-12 predicted down/mild / -4.0; actual XLY -1.13%, SPY +0.25%, rel -1.38%. Direction HIT, magnitude MISS. CPI was in-line; SPY rose; XLY fell on Home Depot CEO medical leave announced during the day and Tesla -1.59%. Neither shock was knowable from the pre-open data bundle.

(learn_cycle promote)
