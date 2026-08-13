---
trigger_pattern: "When an Industrials/XLI severe-up call is built on strong structural sector factors (ISM expansion, AI-power/grid backlog, defense budgets) plus positive tape/flow confirmations, while a live two-sided geopolitical/oil supply-shock headline (e.g., US-Iran/Hormuz) is active, do not treat the pre-fetched oil direction as authoritative and do not score S0_SHARED_MACRO +1 merely by taking the constructive side (“peace-deal hopes, oil down”). A stale/misread oil print can flip the regime read from risk-on to risk-off at the open. On such days SPY may fall while XLI still rises modestly through defense/AI-power composition: direction can be right, but severe is not justified."
corrected_behavior: "When an Iran/Hormuz-style two-sided geopolitical headline is active, verify the oil sign from current overnight/news evidence before scoring S0; cap S0_SHARED_MACRO at 0, or negative if oil is confirmed up/risk-off. Keep the sector’s relative-strength signal (tape, flows, defense/AI-power factors) separate from the absolute macro overlay, and reduce the emitted band to at most up/notable, not up/severe."
falsifier: "The lesson is false if, with the same active Hormuz impasse and a stale/oil-down S0 read, XLI actually delivers a severe up day (e.g., >2%) in a broad risk-off tape. A confirmed severe XLI gain despite a risk-off macro day would show defense/AI-power concentration can justify severe even when the macro overlay is negative."
current_behavior: "The morning read the pre-fetched Channel 1 oil print as “oil down -0.66% on peace-deal hopes,” scored S0 +1, left divergence_flagged False, and emitted up/severe with total 14.4. The active two-sided geopolitical risk was treated as leaning constructive rather than as a reason to cap absolute magnitude."
evidence_cited: "2026-08-11 predicted XLI up/severe; actual XLI +0.596%, SPY -0.320%, rel +0.915% (direction HIT, magnitude MISS). Oil rose ~5% to ~$90 Brent on Hormuz stalemate; stocks fell risk-off ahead of inflation data; gold hit a two-month high. The morning’s S0 claim that oil was down on peace-deal hopes was stale/misread. Defense/aerospace and AI-power strength still let XLI outperform SPY by +0.92%, but the gain was mild, not severe."
error_category: "A"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
When an Iran/Hormuz-style two-sided geopolitical headline is active, verify the oil sign from current overnight/news evidence before scoring S0; cap S0_SHARED_MACRO at 0, or negative if oil is confirmed up/risk-off. Keep the sector’s relative-strength signal (tape, flows, defense/AI-power factors) separate from the absolute macro overlay, and reduce the emitted band to at most up/notable, not up/severe.

## WHEN IT FIRES
When an Industrials/XLI severe-up call is built on strong structural sector factors (ISM expansion, AI-power/grid backlog, defense budgets) plus positive tape/flow confirmations, while a live two-sided geopolitical/oil supply-shock headline (e.g., US-Iran/Hormuz) is active, do not treat the pre-fetched oil direction as authoritative and do not score S0_SHARED_MACRO +1 merely by taking the constructive side (“peace-deal hopes, oil down”). A stale/misread oil print can flip the regime read from risk-on to risk-off at the open. On such days SPY may fall while XLI still rises modestly through defense/AI-power composition: direction can be right, but severe is not justified.

## WRONG IF
The lesson is false if, with the same active Hormuz impasse and a stale/oil-down S0 read, XLI actually delivers a severe up day (e.g., >2%) in a broad risk-off tape. A confirmed severe XLI gain despite a risk-off macro day would show defense/AI-power concentration can justify severe even when the macro overlay is negative.

## EVIDENCE
2026-08-11 predicted XLI up/severe; actual XLI +0.596%, SPY -0.320%, rel +0.915% (direction HIT, magnitude MISS). Oil rose ~5% to ~$90 Brent on Hormuz stalemate; stocks fell risk-off ahead of inflation data; gold hit a two-month high. The morning’s S0 claim that oil was down on peace-deal hopes was stale/misread. Defense/aerospace and AI-power strength still let XLI outperform SPY by +0.92%, but the gain was mild, not severe.

(learn_cycle promote)
