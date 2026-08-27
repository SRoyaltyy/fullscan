---
trigger_pattern: "Basic Materials / XLB is predicted up from (a) a broad risk-on tape that is actually Nasdaq/tech-led and (b) firm copper/gold prices, while XLB's own 1d relative tape is <0.5%, premarket breadth is mixed, and the ETF is chemicals-heavy rather than a pure metals basket. The model passes full S0/S1 macro/metal credit through to XLB without a composition/transmission discount, so a flat ETF can coexist with the metal/macro story."
corrected_behavior: "Before converting the leading sum to direction for XLB, apply a composition/transmission discount. If NQ is much stronger than ES, cap S0's sector push because the risk-on is tech-led, not materials-led. If S1 is two-sided (gold/miners HIT vs China/property copper-demand MISS) and XLB's 1d relative tape is <0.5% with mixed breadth, emit flat (or flat/mild), not up/mild. Do not derive direction from 1w/3d relative strength that is already in the price. Fresh-squeeze days with a strong same-day tape remain eligible for up/severe."
falsifier: "Over the next 10+ XLB sessions with NQ >> ES, firm copper/gold, XLB 1d rel <0.5%, mixed breadth, and S1 net-zero, if XLB closes up >1% with a directional hit rate above 60%, the discount is too strong and should be relaxed. A single opposite example is not sufficient to falsify this lesson."
current_behavior: "On such days, the model keeps the magnitude band mild but still lets the leading sum set direction to up. It treats S1's gold offset plus China/property drag as a net positive, and lets already-traded 1w/3d relative strength count as fresh directional support."
evidence_cited: "2026-08-25: predicted up/mild; XLB closed 0.0%, SPY -0.3%, rel +0.3%. NQ +0.92% vs ES +0.44%; XLB 1d rel +0.37%; copper firm but LME stocks rebuilding; gold +1.08%; premarket LIN/FCX/NEM mixed. The morning itself called it a 'post-run follow-through' and 'not a new rotation day,' yet still printed up. Candidate lesson 2026-08-25_sector_basic_materials_lesson.md matches exactly."
error_category: "B"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
Before converting the leading sum to direction for XLB, apply a composition/transmission discount. If NQ is much stronger than ES, cap S0's sector push because the risk-on is tech-led, not materials-led. If S1 is two-sided (gold/miners HIT vs China/property copper-demand MISS) and XLB's 1d relative tape is <0.5% with mixed breadth, emit flat (or flat/mild), not up/mild. Do not derive direction from 1w/3d relative strength that is already in the price. Fresh-squeeze days with a strong same-day tape remain eligible for up/severe.

## WHEN IT FIRES
Basic Materials / XLB is predicted up from (a) a broad risk-on tape that is actually Nasdaq/tech-led and (b) firm copper/gold prices, while XLB's own 1d relative tape is <0.5%, premarket breadth is mixed, and the ETF is chemicals-heavy rather than a pure metals basket. The model passes full S0/S1 macro/metal credit through to XLB without a composition/transmission discount, so a flat ETF can coexist with the metal/macro story.

## WRONG IF
Over the next 10+ XLB sessions with NQ >> ES, firm copper/gold, XLB 1d rel <0.5%, mixed breadth, and S1 net-zero, if XLB closes up >1% with a directional hit rate above 60%, the discount is too strong and should be relaxed. A single opposite example is not sufficient to falsify this lesson.

## EVIDENCE
2026-08-25: predicted up/mild; XLB closed 0.0%, SPY -0.3%, rel +0.3%. NQ +0.92% vs ES +0.44%; XLB 1d rel +0.37%; copper firm but LME stocks rebuilding; gold +1.08%; premarket LIN/FCX/NEM mixed. The morning itself called it a "post-run follow-through" and "not a new rotation day," yet still printed up. Candidate lesson 2026-08-25_sector_basic_materials_lesson.md matches exactly.

(learn_cycle promote)
