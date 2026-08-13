---
trigger_pattern: "Premarket tape shows no directional confirmation: global sessions flat (±0.5%), US index futures flat (±0.5%), and overnight catalysts are moderate (|B1| around 1) with headline risk (stalled geopolitical deal, oil spike, looming CPI) but no panic selloff — S&P pausing near records after a prior rally; no index-relevant earnings catalyst active."
corrected_behavior: "When B6=0 (±0.5%), B0=0 (±0.5%), and no index-relevant earnings catalyst is active: cap |B1| at 0.5 and |B7| at 0.5 (a headline that does not move futures is not worth full weight, especially in a bad-news-good regime), and force the predicted magnitude band to FLAT — do not let a moderate-catalyst raw sum of ~-4.0 produce a mild call absent futures confirmation. Only allow a mild/severe band if a non-flat futures move or a dominant |B1|≥2 event independently confirms a ≥0.5% move. If an earnings catalyst is present, apply mega-cap-earnings-over-macro-drag instead."
falsifier: "If this exact trigger recurs (flat futures, flat globals, no earnings catalyst, |B1|≤1) and SPX still closes ≥0.5% in either direction, the flat-cap rule is wrong and must be revised — not defended."
current_behavior: "On 2026-08-10, scored B1=-1 (Hormuz stall), B2=-0.5, B7=-0.5 → raw sum -4.0, total -3.6 → predicted down/mild, even though B6 futures=+0.1% and B0 globals ≈0 gave zero confirmation and the write-up itself said the tape was 'pausing,' not reversing."
evidence_cited: "2026-08-10 predicted down/mild (total -3.6); actual SPX -0.06% (flat) → direction MISS, magnitude MISS. Premarket: ES +0.1%, Asia +0.15%, Europe -0.0%, oil +3.8%. Contrast 08-05 and 08-06: down/flat calls matched flat actuals (-0.17%, -0.18%) and were HITs — the flat band was the correct calibration both then and now."
error_category: "B"
scope: "general"
date: "2026-08-10"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-10_lesson.md']"
schema_ok: "true"
---

## RULE
When B6=0 (±0.5%), B0=0 (±0.5%), and no index-relevant earnings catalyst is active: cap |B1| at 0.5 and |B7| at 0.5 (a headline that does not move futures is not worth full weight, especially in a bad-news-good regime), and force the predicted magnitude band to FLAT — do not let a moderate-catalyst raw sum of ~-4.0 produce a mild call absent futures confirmation. Only allow a mild/severe band if a non-flat futures move or a dominant |B1|≥2 event independently confirms a ≥0.5% move. If an earnings catalyst is present, apply mega-cap-earnings-over-macro-drag instead.

## WHEN IT FIRES
Premarket tape shows no directional confirmation: global sessions flat (±0.5%), US index futures flat (±0.5%), and overnight catalysts are moderate (|B1| around 1) with headline risk (stalled geopolitical deal, oil spike, looming CPI) but no panic selloff — S&P pausing near records after a prior rally; no index-relevant earnings catalyst active.

## WRONG IF
If this exact trigger recurs (flat futures, flat globals, no earnings catalyst, |B1|≤1) and SPX still closes ≥0.5% in either direction, the flat-cap rule is wrong and must be revised — not defended.

## EVIDENCE
2026-08-10 predicted down/mild (total -3.6); actual SPX -0.06% (flat) → direction MISS, magnitude MISS. Premarket: ES +0.1%, Asia +0.15%, Europe -0.0%, oil +3.8%. Contrast 08-05 and 08-06: down/flat calls matched flat actuals (-0.17%, -0.18%) and were HITs — the flat band was the correct calibration both then and now.

(learn_cycle promote)
