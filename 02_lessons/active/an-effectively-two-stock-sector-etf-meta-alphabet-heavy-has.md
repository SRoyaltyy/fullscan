---
trigger_pattern: "An effectively two-stock sector ETF (Meta + Alphabet heavy) has a bullish case built from structural positives that are really one underlying thesis, prior-week flows are treated as same-day support, the largest holdings have unresolved capex/FCF vulnerability, and a fresh geopolitical/oil supply-shock risk is active at the open while futures are flat. The model emits up/notable from structural positives instead of flat/down caution."
corrected_behavior: "Deduplicate sector positives: ad-spend recovery + AI monetization = one ad/AI thesis; rotation + sector inflows = one flow observation. Before emitting an up call on XLC, check for knowable-at-open geopolitical/oil/high-impact-print suppressors. If a live geopolitical risk-off signal is present, score S0 negatively and cap the call at flat/down caution. Flat futures should be treated as non-confirmation, not as bullish confirmation. Do not extend “mega-cap-earnings-over-macro-drag” to a live geopolitical supply shock."
falsifier: "The rule would be falsified if an identical setup — live Hormuz-style geopolitical/oil risk, flat futures, positive ad/AI fundamentals, prior-week inflows — still produced an XLC up/notable close. It would also be weakened if prior-week flows were shown to reliably protect a two-stock sector on same-day risk-off opens."
current_behavior: "S0_SHARED_MACRO is scored 0 because premarket futures are flat and real yields eased on 1d/1w; S1 counts digital-ad recovery and AI monetization as separate positives; S3 treats last week’s $3.8B inflow/rotation as same-day support; the active lesson “mega-cap-earnings-over-macro-drag” is used to override macro concerns. Total score 7.5 → up/notable."
evidence_cited: "Predicted up/notable but actual XLC fell roughly -0.4% to -0.5% while SPY fell -0.32%; relative return was negative. Direction MISS and magnitude MISS. The Strait of Hormuz standoff and oil near $83 were knowable at the open, plus CPI caution was looming. Positive ad/AI and prior-week inflow facts did not protect a concentrated growth/duration sector in a risk-off tape. The morning’s S1/S3 scoring also double-counted the same ad/AI/flow story, inflating the score."
error_category: "B"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
Deduplicate sector positives: ad-spend recovery + AI monetization = one ad/AI thesis; rotation + sector inflows = one flow observation. Before emitting an up call on XLC, check for knowable-at-open geopolitical/oil/high-impact-print suppressors. If a live geopolitical risk-off signal is present, score S0 negatively and cap the call at flat/down caution. Flat futures should be treated as non-confirmation, not as bullish confirmation. Do not extend “mega-cap-earnings-over-macro-drag” to a live geopolitical supply shock.

## WHEN IT FIRES
An effectively two-stock sector ETF (Meta + Alphabet heavy) has a bullish case built from structural positives that are really one underlying thesis, prior-week flows are treated as same-day support, the largest holdings have unresolved capex/FCF vulnerability, and a fresh geopolitical/oil supply-shock risk is active at the open while futures are flat. The model emits up/notable from structural positives instead of flat/down caution.

## WRONG IF
The rule would be falsified if an identical setup — live Hormuz-style geopolitical/oil risk, flat futures, positive ad/AI fundamentals, prior-week inflows — still produced an XLC up/notable close. It would also be weakened if prior-week flows were shown to reliably protect a two-stock sector on same-day risk-off opens.

## EVIDENCE
Predicted up/notable but actual XLC fell roughly -0.4% to -0.5% while SPY fell -0.32%; relative return was negative. Direction MISS and magnitude MISS. The Strait of Hormuz standoff and oil near $83 were knowable at the open, plus CPI caution was looming. Positive ad/AI and prior-week inflow facts did not protect a concentrated growth/duration sector in a risk-off tape. The morning’s S1/S3 scoring also double-counted the same ad/AI/flow story, inflating the score.

(learn_cycle promote)
