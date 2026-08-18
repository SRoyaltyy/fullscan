---
trigger_pattern: "A sector call has a decisively negative fundamental spine from a prior-session hard-data macro miss (e.g., retail sales, consumer sentiment) plus an imminent sector-specific event risk (retailer earnings week), and the sector ETF already underperforms SPY across 3d/1w/1m. Premarket futures are flat (ES within ±0.5%) and there is no fresh same-day macro print. The model treats flat futures as a universal magnitude cap and downgrades the call from notable/severe to mild, producing a magnitude miss."
corrected_behavior: "The flat-futures cap should apply only when the call’s primary confirmation source is the premarket tape/broad-market follow-through. When the call is driven by a pre-existing hard-data spine and sector-specific event risk, flat futures should reduce confidence — not collapse magnitude. The final magnitude should stay at least notable when S1 is strongly negative (multiple hits), S2/S4 are negative, and there is no divergence. The pipeline-vs-narrative mismatch should be reconciled explicitly instead of letting the narrative silently downgrade the official band."
falsifier: "A future sector call with the same trigger conditions — hard-data macro miss, negative sector tape, scheduled sector-specific event, flat futures — that closes in the mild range (<0.5%) would falsify the “at least notable” part of the corrected rule. Also, if the relative tape is positive or S1 has only one weak hit, the corrected rule should not force notable."
current_behavior: "Flat futures are used to cap magnitude at mild even when the direction call is supported by multiple independent fundamental spine hits, confirming relative tape, and an upcoming sector catalyst. In this case, the final narrative became down/mild and overrode the deterministic pipeline’s down/severe, with confidence cut despite leading factors and tape converging in the same direction."
evidence_cited: "2026-08-17 XLY closed -1.23% vs SPY -0.47% (rel -0.75), down through the session and near lows. The morning call was down/mild, but the pipeline printed down/severe; actual magnitude was notable. All six hit-grid items HIT, including retail sales miss (-0.6% vs +0.1%), UMich sentiment collapse (-8% to 51.0), rising credit delinquencies, ETF outflows, breadth failure, and rising real yields. Retailer earnings week (HD/LOW/TGT/WMT) was knowable at open. Flat futures did not prevent the sector’s decisive negative move."
error_category: "D"
scope: "ops"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
The flat-futures cap should apply only when the call’s primary confirmation source is the premarket tape/broad-market follow-through. When the call is driven by a pre-existing hard-data spine and sector-specific event risk, flat futures should reduce confidence — not collapse magnitude. The final magnitude should stay at least notable when S1 is strongly negative (multiple hits), S2/S4 are negative, and there is no divergence. The pipeline-vs-narrative mismatch should be reconciled explicitly instead of letting the narrative silently downgrade the official band.

## WHEN IT FIRES
A sector call has a decisively negative fundamental spine from a prior-session hard-data macro miss (e.g., retail sales, consumer sentiment) plus an imminent sector-specific event risk (retailer earnings week), and the sector ETF already underperforms SPY across 3d/1w/1m. Premarket futures are flat (ES within ±0.5%) and there is no fresh same-day macro print. The model treats flat futures as a universal magnitude cap and downgrades the call from notable/severe to mild, producing a magnitude miss.

## WRONG IF
A future sector call with the same trigger conditions — hard-data macro miss, negative sector tape, scheduled sector-specific event, flat futures — that closes in the mild range (<0.5%) would falsify the “at least notable” part of the corrected rule. Also, if the relative tape is positive or S1 has only one weak hit, the corrected rule should not force notable.

## EVIDENCE
2026-08-17 XLY closed -1.23% vs SPY -0.47% (rel -0.75), down through the session and near lows. The morning call was down/mild, but the pipeline printed down/severe; actual magnitude was notable. All six hit-grid items HIT, including retail sales miss (-0.6% vs +0.1%), UMich sentiment collapse (-8% to 51.0), rising credit delinquencies, ETF outflows, breadth failure, and rising real yields. Retailer earnings week (HD/LOW/TGT/WMT) was knowable at open. Flat futures did not prevent the sector’s decisive negative move.

(learn_cycle promote)
