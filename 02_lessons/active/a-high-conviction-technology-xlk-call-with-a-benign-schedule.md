---
trigger_pattern: "A high-conviction Technology/XLK call with a benign scheduled macro print, fresh mega-cap/AI-infrastructure earnings beats, strongly positive Nasdaq futures, and no leading-vs-tape divergence. The narrative and deterministic pipeline agree, and the outcome fully confirms the call."
corrected_behavior: "No change required. Keep the freshness gate: fresh index-relevant AI/mega-cap catalysts plus non-negative/risk-on futures allow the model to override crowding and real-yield dampeners. Do not generalize this to stale catalysts or negative futures. Also avoid over-weighting S3 crowding as a magnitude cap when fresh catalysts have already drawn flows."
falsifier: "This no-error pattern is falsified if XLK declines despite an in-line CPI, fresh index-relevant AI-infra earnings beats, and strongly positive NQ futures at the open. It is also falsified if XLK rallies on stale catalysts or weak futures. Future calls must require all three conditions — fresh catalysts, benign/encouraging macro, and positive futures confirmation — before emitting up/notable."
current_behavior: "Predicted up/notable with multiplier 1.3, regime risk_on, and no divergence flag. Narrative explicitly applied the active mega-cap-earnings-over-macro-drag lesson, gated on freshness: CoreWeave Q2 beat, Super Micro outlook, NQ futures +0.86%, benign CPI. Component scores S0=+1, S1=+2, S2=+1, S3=-1, S4=+1."
evidence_cited: "XLK +1.49%, SPY +0.25%, rel +1.24%. Direction HIT and magnitude HIT. Actual drivers matched the morning analysis: benign CPI cleared the rate-hike overhang, while CoreWeave/Super Micro/Nebius earnings beats lifted the AI-infrastructure complex that dominates XLK. All key inputs were knowable at the open."
error_category: "NONE"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
No change required. Keep the freshness gate: fresh index-relevant AI/mega-cap catalysts plus non-negative/risk-on futures allow the model to override crowding and real-yield dampeners. Do not generalize this to stale catalysts or negative futures. Also avoid over-weighting S3 crowding as a magnitude cap when fresh catalysts have already drawn flows.

## WHEN IT FIRES
A high-conviction Technology/XLK call with a benign scheduled macro print, fresh mega-cap/AI-infrastructure earnings beats, strongly positive Nasdaq futures, and no leading-vs-tape divergence. The narrative and deterministic pipeline agree, and the outcome fully confirms the call.

## WRONG IF
This no-error pattern is falsified if XLK declines despite an in-line CPI, fresh index-relevant AI-infra earnings beats, and strongly positive NQ futures at the open. It is also falsified if XLK rallies on stale catalysts or weak futures. Future calls must require all three conditions — fresh catalysts, benign/encouraging macro, and positive futures confirmation — before emitting up/notable.

## EVIDENCE
XLK +1.49%, SPY +0.25%, rel +1.24%. Direction HIT and magnitude HIT. Actual drivers matched the morning analysis: benign CPI cleared the rate-hike overhang, while CoreWeave/Super Micro/Nebius earnings beats lifted the AI-infrastructure complex that dominates XLK. All key inputs were knowable at the open.

(learn_cycle promote)
