---
trigger_pattern: "A fresh same-morning negative mega-cap/bellwether earnings miss (Walmart-type) or hard-data macro miss coincides with an active geopolitical/oil supply shock (Hormuz-type, Brent up sharply), while US index futures are flat-to-mildly-negative (ES/NQ within ±0.5% and not confirming up) and the main positives are carried (prior-day Treasury-buyback yield relief, Asia/Kospi chip rebound, low-confidence dovish Fed repricing)."
corrected_behavior: "When a fresh same-morning negative mega-cap/bellwether earnings miss or hard-data miss is present together with an active geopolitical/oil supply shock, and ES/NQ do not independently confirm up (net ≥ +0.5%), score B1 at -2 (or at least -1.5) for the negative cluster, B7 at -1 when oil is spiking, cap carried positives (B3 at +0.5 when >1 session old or LOW-confidence; B2 neutral unless yields durably confirm a move lower), and emit direction DOWN with magnitude capped at MILD. Do not flip to up on Treasury-buyback yield relief or an Asia chip rebound unless futures independently confirm ≥ +0.5% up."
falsifier: "If this trigger recurs (fresh negative mega-cap/bellwether miss + active oil/geopolitical shock + flat-to-mildly-negative futures, no fresh positive mega-cap catalyst) and SPX closes ≥ +0.5% up on 2 of the next 3 such days, the down-bias correction is wrong and must be narrowed or discarded."
current_behavior: "Scored B1=-0.5 for the combined Walmart-miss + Hormuz/oil cluster and B7=-0.5 for the oil spike, while B2/B3/B5 stayed +0.5 each and B0=+0.5, producing total 1.125 → up/flat. The model applied the 08-19 Treasury-support reversal lesson and the 08-13 flat-futures magnitude cap, treating the carried yield relief as dominant over the fresh negative cluster, even though the 08-17/08-18 fresh-hard-data-miss principle says a fresh negative cluster beats carried positives without futures confirmation."
evidence_cited: "2026-08-20 predicted up/flat (total 1.125, B1=-0.5, B7=-0.5); actual SPX -0.87% (down/mild), both direction and magnitude missed. Walmart Q2 miss (comps +2.6% vs 3.8%, shares -6% premarket), Hormuz closure, Brent +2.91%, VIX 15.31 rising, and EPU +82 1d were all cited premarket; the fresh negative cluster was knowable at the open and was underweighted relative to carried positives."
error_category: "B"
scope: "general"
date: "2026-08-20"
status: "active"
occurrences: "1"
promoted_on: "2026-08-20"
sources: "['2026-08-20_lesson.md']"
schema_ok: "true"
---

## RULE
When a fresh same-morning negative mega-cap/bellwether earnings miss or hard-data miss is present together with an active geopolitical/oil supply shock, and ES/NQ do not independently confirm up (net ≥ +0.5%), score B1 at -2 (or at least -1.5) for the negative cluster, B7 at -1 when oil is spiking, cap carried positives (B3 at +0.5 when >1 session old or LOW-confidence; B2 neutral unless yields durably confirm a move lower), and emit direction DOWN with magnitude capped at MILD. Do not flip to up on Treasury-buyback yield relief or an Asia chip rebound unless futures independently confirm ≥ +0.5% up.

## WHEN IT FIRES
A fresh same-morning negative mega-cap/bellwether earnings miss (Walmart-type) or hard-data macro miss coincides with an active geopolitical/oil supply shock (Hormuz-type, Brent up sharply), while US index futures are flat-to-mildly-negative (ES/NQ within ±0.5% and not confirming up) and the main positives are carried (prior-day Treasury-buyback yield relief, Asia/Kospi chip rebound, low-confidence dovish Fed repricing).

## WRONG IF
If this trigger recurs (fresh negative mega-cap/bellwether miss + active oil/geopolitical shock + flat-to-mildly-negative futures, no fresh positive mega-cap catalyst) and SPX closes ≥ +0.5% up on 2 of the next 3 such days, the down-bias correction is wrong and must be narrowed or discarded.

## EVIDENCE
2026-08-20 predicted up/flat (total 1.125, B1=-0.5, B7=-0.5); actual SPX -0.87% (down/mild), both direction and magnitude missed. Walmart Q2 miss (comps +2.6% vs 3.8%, shares -6% premarket), Hormuz closure, Brent +2.91%, VIX 15.31 rising, and EPU +82 1d were all cited premarket; the fresh negative cluster was knowable at the open and was underweighted relative to carried positives.

(learn_cycle promote)
