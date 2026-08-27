---
trigger_pattern: "A sector PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, but the scoreboard entry later records `predicted None/None` and marks both `direction_hit` and `magnitude_hit` False, even when the actual close confirms both. This is a grader/extraction/pipeline failure, not a forecasting failure."
corrected_behavior: "The scoreboard must extract predicted direction and magnitude from the PREDICT block fields before grading. If those fields are non-null and equal the actual direction/band, record `direction_hit: True` and `magnitude_hit: True`. For 2026-08-25 Energy, the row should be corrected to a HIT/HIT on the down/notable call. Rolling stats should be recomputed from corrected rows rather than propagating extraction `None` as a market miss."
falsifier: "If a PREDICT block actually omits `predicted_direction` or `predicted_magnitude_band`, then `None/None` and a false miss are correct. Also, if the actual close did not match the stated direction or magnitude band, a HIT would not be appropriate. This lesson applies only when explicit non-null predicted fields exist and the actual close confirms them."
current_behavior: "The Energy 2026-08-25 scoreboard row shows `predicted None/None` with `direction_hit: False | magnitude_hit: False`, despite the PREDICT block explicitly stating `predicted_direction: down` and `predicted_magnitude_band: notable`, and despite XLE closing at −1.66% (down/notable) with the review verdict “Full HIT.” This false miss corrupts rolling accuracy: Energy dir is recorded as 0.556 and mag as 0.333 instead of the corrected dir ~0.667 and mag ~0.444 (n=9, with 08-24 still pending)."
evidence_cited: "Energy 2026-08-25 PREDICT block: `predicted_direction: down`, `predicted_magnitude_band: notable`, `total_score: -10.0`. Actual outcome: XLE −1.66%, SPY +0.32%, rel −1.98%; actual direction down and magnitude notable. The outcome review explicitly states: “Full HIT — direction (down) and magnitude (notable) both confirmed.” The scoreboard entry nevertheless records `predicted None/None` and two false misses. This matches recent candidate `2026-08-25_sector_energy_lesson.md` and the same-pattern candidate lessons for communication services, consumer defensive, financial, and technology."
error_category: "D"
scope: "ops"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
The scoreboard must extract predicted direction and magnitude from the PREDICT block fields before grading. If those fields are non-null and equal the actual direction/band, record `direction_hit: True` and `magnitude_hit: True`. For 2026-08-25 Energy, the row should be corrected to a HIT/HIT on the down/notable call. Rolling stats should be recomputed from corrected rows rather than propagating extraction `None` as a market miss.

## WHEN IT FIRES
A sector PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, but the scoreboard entry later records `predicted None/None` and marks both `direction_hit` and `magnitude_hit` False, even when the actual close confirms both. This is a grader/extraction/pipeline failure, not a forecasting failure.

## WRONG IF
If a PREDICT block actually omits `predicted_direction` or `predicted_magnitude_band`, then `None/None` and a false miss are correct. Also, if the actual close did not match the stated direction or magnitude band, a HIT would not be appropriate. This lesson applies only when explicit non-null predicted fields exist and the actual close confirms them.

## EVIDENCE
Energy 2026-08-25 PREDICT block: `predicted_direction: down`, `predicted_magnitude_band: notable`, `total_score: -10.0`. Actual outcome: XLE −1.66%, SPY +0.32%, rel −1.98%; actual direction down and magnitude notable. The outcome review explicitly states: “Full HIT — direction (down) and magnitude (notable) both confirmed.” The scoreboard entry nevertheless records `predicted None/None` and two false misses. This matches recent candidate `2026-08-25_sector_energy_lesson.md` and the same-pattern candidate lessons for communication services, consumer defensive, financial, and technology.

(learn_cycle promote)
