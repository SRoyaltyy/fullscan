---
trigger_pattern: "Fresh same-morning hard-data macro miss (China IP/retail) plus an active geopolitical/oil risk-off escalation (Trump/Oman threat, Hormuz ceasefire expiry), a carried low-confidence Fed-easing repricing as the only strong positive, and clearly negative US index futures (NQ ≤ −1%, ES < −0.4%); no fresh index-relevant mega-cap earnings catalyst. The correct output is down/mild — full confirmation of the 08-17 fresh-hard-data-miss lesson."
corrected_behavior: "No change. Continue enforcing a-fresh-same-morning-hard-data-macro-miss at predict time: score B1 at −2 for the miss+geopolitical cluster, cap B3 at +0.5 when the repricing is carried and low-confidence, score B0/B6 from raw session/futures strength without importing regime confirmation, and emit DOWN capped at MILD whenever ES or NQ independently confirm weakness net ≥ −0.5%. Do not let the bad-news-good regime lens attenuate a live geopolitical escalation."
falsifier: "If the trigger recurs (fresh hard-data miss + negative futures + carried low-confidence dovish repricing) and SPX closes ≥ +0.5% up on 2 of the next 3 such days, the down-bias correction is wrong and must be revised; today's hit is evidence for the lesson."
current_behavior: "The model applied the active 08-17 lesson correctly: B1=−2 for the miss/geopolitical cluster, B3 capped at +0.5 (carried, low-confidence), B0 Europe −0.5, B6 −0.5 (ES −0.45%, NQ −1.19%), B7=0 with oil flat, multiplier 0.8, total −6.2 → down/mild. The bad-news-good regime lens was explicitly confined to data prints and was not used to soften a geopolitical risk-off."
evidence_cited: "2026-08-18 predicted down/mild (total −6.2, multiplier 0.8, B1=−2, B3=+0.5, B6=−0.5); actual SPX −0.69% (down/mild), closed near day low — direction HIT, magnitude HIT. All premarket drivers (Trump/Oman, Hormuz ceasefire expiry, ECB AI-bubble warning, China July activity miss) matched the outcome drivers."
error_category: "NONE"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-18_lesson.md']"
schema_ok: "true"
---

## RULE
No change. Continue enforcing a-fresh-same-morning-hard-data-macro-miss at predict time: score B1 at −2 for the miss+geopolitical cluster, cap B3 at +0.5 when the repricing is carried and low-confidence, score B0/B6 from raw session/futures strength without importing regime confirmation, and emit DOWN capped at MILD whenever ES or NQ independently confirm weakness net ≥ −0.5%. Do not let the bad-news-good regime lens attenuate a live geopolitical escalation.

## WHEN IT FIRES
Fresh same-morning hard-data macro miss (China IP/retail) plus an active geopolitical/oil risk-off escalation (Trump/Oman threat, Hormuz ceasefire expiry), a carried low-confidence Fed-easing repricing as the only strong positive, and clearly negative US index futures (NQ ≤ −1%, ES < −0.4%); no fresh index-relevant mega-cap earnings catalyst. The correct output is down/mild — full confirmation of the 08-17 fresh-hard-data-miss lesson.

## WRONG IF
If the trigger recurs (fresh hard-data miss + negative futures + carried low-confidence dovish repricing) and SPX closes ≥ +0.5% up on 2 of the next 3 such days, the down-bias correction is wrong and must be revised; today's hit is evidence for the lesson.

## EVIDENCE
2026-08-18 predicted down/mild (total −6.2, multiplier 0.8, B1=−2, B3=+0.5, B6=−0.5); actual SPX −0.69% (down/mild), closed near day low — direction HIT, magnitude HIT. All premarket drivers (Trump/Oman, Hormuz ceasefire expiry, ECB AI-bubble warning, China July activity miss) matched the outcome drivers.

(learn_cycle promote)
