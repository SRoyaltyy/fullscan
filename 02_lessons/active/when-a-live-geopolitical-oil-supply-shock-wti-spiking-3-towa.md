---
trigger_pattern: "When a live geopolitical/oil supply shock (WTI spiking +3% toward $94-100, Brent +2% toward $99) is present at the open with confirming negative futures (ES −0.44%, NQ −0.16%) and the sector is Consumer Cyclical (XLY with AMZN ~24% + TSLA ~17%), the model correctly scores S0 = −2 (dominant macro driver, more negative for discretionary) and S1 = −1 (gasoline transmission channel) without double-counting, and correctly caps magnitude at mild when no severe-cap-breaking premarket action is present in mega-cap holdings."
corrected_behavior: "No correction needed — the model's handling of the live oil shock (S0 = −2 dominant, S1 = −1 transmission, no double-count, mild cap) was validated by the outcome. Continue this pattern for future live oil/geopolitical shocks affecting Consumer Cyclical."
falsifier: "This pattern would be falsified if a live oil shock (WTI +3% toward $94+) with negative futures produced an XLY gain or flat day (direction MISS), or if XLY fell >1.5% (severe) when mega-caps were not breaking premarket. The 2026-09-08 outcome (XLY −0.80%, mild) confirms the pattern."
current_behavior: "Model correctly identified the live oil shock as the dominant driver, scored S0 = −2 and S1 = −1 without double-counting the same event, capped magnitude at mild per the 08-18 severe-cap lesson, and produced a down/mild call that was directionally and magnitude-correct (XLY −0.80%, mild band)."
evidence_cited: "XLY −0.80% vs predicted down/mild — direction HIT, magnitude HIT. Schwab open note confirmed 'Consumer discretionary and real estate are least favored.' The S0 = −2 weighting on the live oil shock was the right call — it was the dominant driver and the sector underperformed as predicted. Outcome audit: all five S0-S4 reads were HITs."
error_category: "NONE"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
No correction needed — the model's handling of the live oil shock (S0 = −2 dominant, S1 = −1 transmission, no double-count, mild cap) was validated by the outcome. Continue this pattern for future live oil/geopolitical shocks affecting Consumer Cyclical.

## WHEN IT FIRES
When a live geopolitical/oil supply shock (WTI spiking +3% toward $94-100, Brent +2% toward $99) is present at the open with confirming negative futures (ES −0.44%, NQ −0.16%) and the sector is Consumer Cyclical (XLY with AMZN ~24% + TSLA ~17%), the model correctly scores S0 = −2 (dominant macro driver, more negative for discretionary) and S1 = −1 (gasoline transmission channel) without double-counting, and correctly caps magnitude at mild when no severe-cap-breaking premarket action is present in mega-cap holdings.

## WRONG IF
This pattern would be falsified if a live oil shock (WTI +3% toward $94+) with negative futures produced an XLY gain or flat day (direction MISS), or if XLY fell >1.5% (severe) when mega-caps were not breaking premarket. The 2026-09-08 outcome (XLY −0.80%, mild) confirms the pattern.

## EVIDENCE
XLY −0.80% vs predicted down/mild — direction HIT, magnitude HIT. Schwab open note confirmed "Consumer discretionary and real estate are least favored." The S0 = −2 weighting on the live oil shock was the right call — it was the dominant driver and the sector underperformed as predicted. Outcome audit: all five S0-S4 reads were HITs.

(learn_cycle promote)
