---
trigger_pattern: "A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (funding-source S0 ≤ 0, no HC spine, S4=0 because leftover 3d/1w/1m RS is banned and live PM lags the growth ETF) into an unprinted same-session FOMC+SEP+Chair-presser path-binary, while overnight ES/NQ vs prior cash are green enough for v2 tape_anchor + index_carry to write official direction=up even though the live operator panel is only modestly green."
corrected_behavior: "If XLV S0–S4 is net ≤ 0, S4 is not a live breakout, leftover multi-horizon RS is banned, and FOMC+SEP+presser is unprinted, official direction must follow the factor card (flat), not tape_anchor/index_carry from overnight ES vs cash. Do not treat yfinance ES ≥ +1% vs prior close as an XLV participation certificate when Finviz/live PM is only a modest lag. Size-gate (no notable) is not enough — do not leave up/mild standing. Keep 09-11 S0 as funding-source on the pre-binary tape; do not flip S0 positive for an unknowable hawkish-haven branch."
falsifier: "If this trigger recurs, official direction is forced flat instead of tape_anchor up, and XLV still closes ≥ +0.3% absolute, revise the follow-the-factor-card rule rather than defend it."
current_behavior: "LLM scored S0=-0.5 / S1–S4=0, fired 08-13 as a ban on up/notable, overlay -0.4, prefer flat/mild. Engine still emitted official up/mild (total 2.995) from tape_anchor 2.071 (yfinance ES +1.14%, PM:XLV +0.20%) + index_carry 1.324, with calendar_size_gate only blocking notable."
evidence_cited: "2026-09-16 predicted up/mild vs XLV +0.066% / SPY −0.441% / rel +0.507% (flat/flat). LLM leading_sum −0.5, overlay −0.4, divergence_flagged false; engine divergence_flagged true. Path: tech-led green into 14:00, hawkish 25bp + Warsh/dots fade; healthcare/utilities flat. Finviz ES +0.20% vs tape_anchor ES +1.14%."
error_category: "D"
scope: "ops"
date: "2026-09-16"
status: "active"
occurrences: "1"
promoted_on: "2026-09-16"
sources: "['2026-09-16_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
If XLV S0–S4 is net ≤ 0, S4 is not a live breakout, leftover multi-horizon RS is banned, and FOMC+SEP+presser is unprinted, official direction must follow the factor card (flat), not tape_anchor/index_carry from overnight ES vs cash. Do not treat yfinance ES ≥ +1% vs prior close as an XLV participation certificate when Finviz/live PM is only a modest lag. Size-gate (no notable) is not enough — do not leave up/mild standing. Keep 09-11 S0 as funding-source on the pre-binary tape; do not flip S0 positive for an unknowable hawkish-haven branch.

## WHEN IT FIRES
A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (funding-source S0 ≤ 0, no HC spine, S4=0 because leftover 3d/1w/1m RS is banned and live PM lags the growth ETF) into an unprinted same-session FOMC+SEP+Chair-presser path-binary, while overnight ES/NQ vs prior cash are green enough for v2 tape_anchor + index_carry to write official direction=up even though the live operator panel is only modestly green.

## WRONG IF
If this trigger recurs, official direction is forced flat instead of tape_anchor up, and XLV still closes ≥ +0.3% absolute, revise the follow-the-factor-card rule rather than defend it.

## EVIDENCE
2026-09-16 predicted up/mild vs XLV +0.066% / SPY −0.441% / rel +0.507% (flat/flat). LLM leading_sum −0.5, overlay −0.4, divergence_flagged false; engine divergence_flagged true. Path: tech-led green into 14:00, hawkish 25bp + Warsh/dots fade; healthcare/utilities flat. Finviz ES +0.20% vs tape_anchor ES +1.14%.

(learn_cycle promote)
