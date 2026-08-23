---
trigger_pattern: "A high-beta/cyclical sector (XLY) is predicted down after a sharp risk-off day, but the following morning has a clear reversal checklist: US index futures ≥ +0.3% (ES/NQ positive), real yields easing, oil not spiking, and the leading negative fundamental factors (retail sales miss, consumer sentiment collapse, Fed minutes, bellwether earnings) are stale — released 1–7 days earlier — with no fresh same-day negative catalyst. The model flags a leading-vs-tape divergence but keeps the stale down bias and only shrinks confidence."
corrected_behavior: "When the same-morning reversal checklist is positive and no fresh negative catalyst hits the sector that day, set the day-horizon direction to the tape for high-beta/cyclical sectors — up if futures are clearly positive, real yields are easing, and oil is steady/off highs. Stale macro/fundamental negatives should not override a live recovery tape. If a dominant mega-cap has pending catalysts or binary news risk, flag single-stock asymmetric risk and cap magnitude at mild/notable rather than assuming the fundamental direction will dominate."
falsifier: "If a high-beta cyclical sector has this exact stale-negative vs fresh-positive pattern and still closes down more than -0.5% with no fresh catalyst, the corrected behavior would be wrong. The lesson also does not apply if a fresh same-day hard-data miss or geopolitical/oil shock appears."
current_behavior: "Stale S0/S1 negatives are assigned full negative weight even after the risk-off shock has passed; the positive same-morning tape is treated only as a confidence reducer, not a direction signal, producing a down call that misses the recovery."
evidence_cited: "Predicted XLY down/notable (-10.0), but actual XLY closed +1.15% vs SPY +0.41%, relative +0.74%. At prediction time, futures were bouncing (ES +0.35%, NQ +0.49%), real yields were easing (DFII10 down on 1d/1w), and oil was down. The negative spine was stale: retail sales -0.6% (08-14), UMich collapse (08-14), Walmart caution (08-20), Fed minutes (08-19). The intraday Tesla catalyst (Europe Semi, Nevada robotaxi permits) was not knowable at open and amplified the sector move, but the recovery tape alone already contradicted a down call."
error_category: "D"
scope: "ops"
date: "2026-08-21"
status: "active"
occurrences: "1"
promoted_on: "2026-08-23"
sources: "['2026-08-21_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When the same-morning reversal checklist is positive and no fresh negative catalyst hits the sector that day, set the day-horizon direction to the tape for high-beta/cyclical sectors — up if futures are clearly positive, real yields are easing, and oil is steady/off highs. Stale macro/fundamental negatives should not override a live recovery tape. If a dominant mega-cap has pending catalysts or binary news risk, flag single-stock asymmetric risk and cap magnitude at mild/notable rather than assuming the fundamental direction will dominate.

## WHEN IT FIRES
A high-beta/cyclical sector (XLY) is predicted down after a sharp risk-off day, but the following morning has a clear reversal checklist: US index futures ≥ +0.3% (ES/NQ positive), real yields easing, oil not spiking, and the leading negative fundamental factors (retail sales miss, consumer sentiment collapse, Fed minutes, bellwether earnings) are stale — released 1–7 days earlier — with no fresh same-day negative catalyst. The model flags a leading-vs-tape divergence but keeps the stale down bias and only shrinks confidence.

## WRONG IF
If a high-beta cyclical sector has this exact stale-negative vs fresh-positive pattern and still closes down more than -0.5% with no fresh catalyst, the corrected behavior would be wrong. The lesson also does not apply if a fresh same-day hard-data miss or geopolitical/oil shock appears.

## EVIDENCE
Predicted XLY down/notable (-10.0), but actual XLY closed +1.15% vs SPY +0.41%, relative +0.74%. At prediction time, futures were bouncing (ES +0.35%, NQ +0.49%), real yields were easing (DFII10 down on 1d/1w), and oil was down. The negative spine was stale: retail sales -0.6% (08-14), UMich collapse (08-14), Walmart caution (08-20), Fed minutes (08-19). The intraday Tesla catalyst (Europe Semi, Nevada robotaxi permits) was not knowable at open and amplified the sector move, but the recovery tape alone already contradicted a down call.


