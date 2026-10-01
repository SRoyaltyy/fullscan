---
trigger_pattern: "Mega-cap-concentrated cyclical ETF (XLY-like; AMZN+TSLA ~40%+ AI/index-beta) has a net-negative S0–S4 sum while ES/NQ are green, VIX is in contango, and the official SECTOR_SCORES block already splits to flat-absolute / down-relative — but the v2 engine still emits down/mild via skill-multiplied overlay/index_carry and the grader scores the engine print, not the official block."
corrected_behavior: "When official OFFICIAL_DIRECTION/BAND and engine predicted_direction/band diverge, grade the official block. If 09-25 conditions hold (net-negative factor sum + green ES/NQ + VIX contango + AI/index-beta top-2), the engine emit must also be flat/flat with relative lean down. Do not let skill_multipliers or index_carry promote a modest negative leading sum into an absolute down/mild the official block already rejected. Do not write a new S0/S1 weighting lesson from this scoreboard line."
falsifier: "If 09-25 conditions recur and XLY still closes ≤ −0.3% absolute (down/mild or worse) while SPY is green, flattening the graded/engine emit is wrong and must be revised rather than defended."
current_behavior: "LLM official call is flat/flat + relative down (09-25 applied, confidence 0.42). Engine maps the same components through skill_multipliers (S0/S2/S4 ×1.25) to overlay −6.0 / total −6.764 and emits down/mild (confidence 0.771). Scoreboard grades the engine, recording dir and mag False against a −0.028% close."
evidence_cited: "2026-10-01 XLY −0.028% / SPY +0.178% / rel −0.206% (open 109.21 → close 108.81). Official flat/flat HIT; pipeline down/mild MISS. S0 −1 / S1 −1 / S2 −0.5 signs HIT. 10Y tagged ~5.34% then ~5.25%; XLK led the bounce; XLY did not. Nike AMC and oil reversal were not cash-open drivers."
error_category: "D"
scope: "ops"
date: "2026-10-01"
status: "active"
occurrences: "1"
promoted_on: "2026-10-01"
sources: "['2026-10-01_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When official OFFICIAL_DIRECTION/BAND and engine predicted_direction/band diverge, grade the official block. If 09-25 conditions hold (net-negative factor sum + green ES/NQ + VIX contango + AI/index-beta top-2), the engine emit must also be flat/flat with relative lean down. Do not let skill_multipliers or index_carry promote a modest negative leading sum into an absolute down/mild the official block already rejected. Do not write a new S0/S1 weighting lesson from this scoreboard line.

## WHEN IT FIRES
Mega-cap-concentrated cyclical ETF (XLY-like; AMZN+TSLA ~40%+ AI/index-beta) has a net-negative S0–S4 sum while ES/NQ are green, VIX is in contango, and the official SECTOR_SCORES block already splits to flat-absolute / down-relative — but the v2 engine still emits down/mild via skill-multiplied overlay/index_carry and the grader scores the engine print, not the official block.

## WRONG IF
If 09-25 conditions recur and XLY still closes ≤ −0.3% absolute (down/mild or worse) while SPY is green, flattening the graded/engine emit is wrong and must be revised rather than defended.

## EVIDENCE
2026-10-01 XLY −0.028% / SPY +0.178% / rel −0.206% (open 109.21 → close 108.81). Official flat/flat HIT; pipeline down/mild MISS. S0 −1 / S1 −1 / S2 −0.5 signs HIT. 10Y tagged ~5.34% then ~5.25%; XLK led the bounce; XLY did not. Nike AMC and oil reversal were not cash-open drivers.

(learn_cycle promote)
