---
trigger_pattern: "When a sector-specific negative cluster (packaged-food margin/dividend stress) is fresh and has already demonstrated it can override a defensive bid (prior session showed XLP underperforming despite macro crosscurrents), the model still scores S0 positive for a geopolitical/oil risk-off FTS bid, treating the macro tailwind and sector drag as roughly offsetting. The model fails to weight the demonstrated precedent — if the sector-specific drag dominated the FTS bid on the prior session with the same cluster active, it should dominate again today, making S0 closer to 0 or negative rather than +0.5."
corrected_behavior: "When a sector-specific negative cluster is fresh AND has already demonstrated dominance over the FTS bid in a prior session (XLP rel −1.36% on 09-03 with the same CPB/GIS/KHC stress active), the model must treat that demonstrated dominance as the base case. Score S0 = 0 or negative for the FTS bid when the sector has shown it cannot hold a defensive bid under the active sector-specific drag. The precedent of the sector failing to receive the defensive bid is stronger evidence than the theoretical FTS channel. Only score S0 positive if the sector has shown it CAN hold a defensive bid under the current sector-specific conditions."
falsifier: "A session where a fresh sector-specific negative cluster (e.g., packaged-food dividend cuts) is active, a geopolitical/oil risk-off tape is present, and XLP nonetheless outperforms SPY (rel > 0) would falsify this lesson. Also falsified if XLP underperforms but the underperformance is traceable to a different driver (e.g., rates shock) rather than the sector-specific cluster."
current_behavior: "Scores S0 = +0.5 for a fresh Hormuz oil spike / risk-off tape as a relative bid for defensives, while separately scoring S1 = −0.5 for the food-crash cluster, allowing the two to net to approximately zero. This treats the macro FTS bid as having equal force to the sector-specific drag, despite the prior session (09-03) showing XLP underperforming SPY by −1.36% on the same food-crash cluster with macro crosscurrents present."
evidence_cited: "2026-09-08: Predicted flat/flat (total −0.45) vs actual XLP −0.66% / SPY −0.55% / rel −0.11% (down/mild, dir MISS, mag MISS). S0 = +0.5 for the Hormuz FTS bid was overweighted — XLP did not outperform despite the risk-off tape. The 09-03 precedent (XLP rel −1.36% on the same food-crash cluster) was available at open and should have been weighted more heavily. S1 = −0.5 correctly identified the food-crash drag as dominant, but S0's +0.5 offset it, producing a flat call when the demonstrated precedent pointed to down."
error_category: "B"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When a sector-specific negative cluster is fresh AND has already demonstrated dominance over the FTS bid in a prior session (XLP rel −1.36% on 09-03 with the same CPB/GIS/KHC stress active), the model must treat that demonstrated dominance as the base case. Score S0 = 0 or negative for the FTS bid when the sector has shown it cannot hold a defensive bid under the active sector-specific drag. The precedent of the sector failing to receive the defensive bid is stronger evidence than the theoretical FTS channel. Only score S0 positive if the sector has shown it CAN hold a defensive bid under the current sector-specific conditions.

## WHEN IT FIRES
When a sector-specific negative cluster (packaged-food margin/dividend stress) is fresh and has already demonstrated it can override a defensive bid (prior session showed XLP underperforming despite macro crosscurrents), the model still scores S0 positive for a geopolitical/oil risk-off FTS bid, treating the macro tailwind and sector drag as roughly offsetting. The model fails to weight the demonstrated precedent — if the sector-specific drag dominated the FTS bid on the prior session with the same cluster active, it should dominate again today, making S0 closer to 0 or negative rather than +0.5.

## WRONG IF
A session where a fresh sector-specific negative cluster (e.g., packaged-food dividend cuts) is active, a geopolitical/oil risk-off tape is present, and XLP nonetheless outperforms SPY (rel > 0) would falsify this lesson. Also falsified if XLP underperforms but the underperformance is traceable to a different driver (e.g., rates shock) rather than the sector-specific cluster.

## EVIDENCE
2026-09-08: Predicted flat/flat (total −0.45) vs actual XLP −0.66% / SPY −0.55% / rel −0.11% (down/mild, dir MISS, mag MISS). S0 = +0.5 for the Hormuz FTS bid was overweighted — XLP did not outperform despite the risk-off tape. The 09-03 precedent (XLP rel −1.36% on the same food-crash cluster) was available at open and should have been weighted more heavily. S1 = −0.5 correctly identified the food-crash drag as dominant, but S0's +0.5 offset it, producing a flat call when the demonstrated precedent pointed to down.

(learn_cycle promote)
