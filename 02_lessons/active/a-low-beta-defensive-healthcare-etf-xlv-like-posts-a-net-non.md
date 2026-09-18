---
trigger_pattern: "A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (09-11 funding-source S0 ≤ 0, no HC spine, leftover 3d/1w/1m RS banned, live PM lags the growth ETF) on the session AFTER an already-printed FOMC+SEP+presser, into a tech-led risk-on rebound (NQ≥ES, oil offered), while v2 tape_anchor (overnight ES vs cash + already-mild green PM) + index_carry still write official up/mild."
corrected_behavior: "No direction/band correction. Keep 09-11 S0 as the relative funding-source read; keep 08-13 as a ban on leftover-RS up/notable. Do not extend 09-16’s force-flat past an unprinted path-binary. When FOMC is paid and PM:XLV is already in the mild band (~+0.4%) on a confirmed NQ-led risk-on tape, official absolute may follow tape_anchor up/mild; relative is the S0 object. Do not rewrite S0 as a duration bid from same-session yield relief, and do not promote XBI/high-beta sleeve or single-name FDA/BD into S1 for the XLV object."
falsifier: "If this T+1 setup recurs (paid FOMC, tech-led risk-on, oil offered, XLV S0≤0 / leftover RS banned, PM already ~+0.4% mild, official up/mild from tape_anchor) and XLV still closes |pct|<0.3% or prints positive rel vs a green SPY, revise the “tape_anchor owns absolute / S0 owns relative” split rather than defend it."
current_behavior: "LLM scored S0=-0.5 / S1–S4=0, fired 08-13 as a ban on up/notable, overlay -0.4, prefer flat/mild, divergence flagged, trust factors over tape. Engine emitted official up/mild (total 4.847) from tape_anchor 3.401 (ES +1.71%, PM:XLV +0.37%) + index_carry 1.846. 09-16 force-flat correctly not applied because FOMC is paid."
evidence_cited: "2026-09-17 predicted up/mild vs XLV +0.620% / SPY +1.134% / rel −0.514% (up/mild). LLM leading_sum −0.5, overlay −0.4, divergence_flagged true. Path: post-FOMC tech-led rebound (Nasdaq +1.7%, XLK ~+2.2–2.4%); XLV gapped with PM then ground +0.22% from the open; volume dry. XBI +2.64% was S0 high-beta, not an XLV spine."
error_category: "NONE"
scope: "general"
date: "2026-09-17"
status: "active"
occurrences: "2"
promoted_on: "2026-09-18"
sources: "['2026-09-17_sector_healthcare_lesson.md', '2026-09-18_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
No direction/band correction. Keep 09-11 S0 as the relative funding-source read; keep 08-13 as a ban on leftover-RS up/notable. Do not extend 09-16’s force-flat past an unprinted path-binary. When FOMC is paid and PM:XLV is already in the mild band (~+0.4%) on a confirmed NQ-led risk-on tape, official absolute may follow tape_anchor up/mild; relative is the S0 object. Do not rewrite S0 as a duration bid from same-session yield relief, and do not promote XBI/high-beta sleeve or single-name FDA/BD into S1 for the XLV object.

## WHEN IT FIRES
A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (09-11 funding-source S0 ≤ 0, no HC spine, leftover 3d/1w/1m RS banned, live PM lags the growth ETF) on the session AFTER an already-printed FOMC+SEP+presser, into a tech-led risk-on rebound (NQ≥ES, oil offered), while v2 tape_anchor (overnight ES vs cash + already-mild green PM) + index_carry still write official up/mild.

## WRONG IF
If this T+1 setup recurs (paid FOMC, tech-led risk-on, oil offered, XLV S0≤0 / leftover RS banned, PM already ~+0.4% mild, official up/mild from tape_anchor) and XLV still closes |pct|<0.3% or prints positive rel vs a green SPY, revise the “tape_anchor owns absolute / S0 owns relative” split rather than defend it.

## EVIDENCE
2026-09-17 predicted up/mild vs XLV +0.620% / SPY +1.134% / rel −0.514% (up/mild). LLM leading_sum −0.5, overlay −0.4, divergence_flagged true. Path: post-FOMC tech-led rebound (Nasdaq +1.7%, XLK ~+2.2–2.4%); XLV gapped with PM then ground +0.22% from the open; volume dry. XBI +2.64% was S0 high-beta, not an XLV spine.

(learn_cycle promote)
