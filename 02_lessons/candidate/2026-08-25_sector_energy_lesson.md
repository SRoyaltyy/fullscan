---
trigger_pattern: "A sector PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, and the actual close confirms those values, but the scoreboard entry is recorded as `predicted None/None` with `direction_hit: False | magnitude_hit: False`. The false miss corrupts rolling accuracy and is a grading/pipeline failure, not a failed market call."
current_behavior: "The grader falls back to `None/None` for a valid prediction, writes a direction/magnitude miss, and the sector’s rolling accuracy is punished for a call that actually hit. In Energy this also obscures a rare clean double-hit."
corrected_behavior: "When a scoreboard entry shows `predicted None/None`, first check for a contemporaneous sector PREDICT block. If it contains explicit `predicted_direction` and `predicted_magnitude_band`, use those as the baseline and repair the entry. For Energy 2026-08-25, the corrected entry should be `direction_hit: True | magnitude_hit: True | predicted down/notable vs actual -1.6637604609503365%`. Add a grader guardrail: never record a miss against `None` while an explicit prediction exists."
evidence_cited: "Energy 2026-08-25 predict had `predicted_direction: down`, `predicted_magnitude_band: notable`, `total_score: -10.0`. Actual XLE was -1.66%, SPY +0.32%, relative -1.98%. Actual direction and magnitude both confirmed down/notable. The outcome narrative itself says “Direction and magnitude both HIT,” yet the scoreboard entry says `predicted None/None` and marks both False."
error_category: "D"
falsifier: "This lesson would be falsified if a `predicted None/None` scoreboard entry were accompanied by a genuinely absent or empty predict file at grading time. In that case, there is no baseline and the miss is legitimate. It is also falsified if an explicit predict exists but the actual outcome does not match its direction or magnitude; then the correct grading is a real miss."
sector: "Energy"
date: "2026-08-25"
status: "candidate"
---

# Sector Reflection — Energy — 2026-08-25

LESSON_BEGIN
ERROR_CATEGORY: D

TRIGGER_PATTERN: A sector PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, and the actual close confirms those values, but the scoreboard entry is recorded as `predicted None/None` with `direction_hit: False | magnitude_hit: False`. The false miss corrupts rolling accuracy and is a grading/pipeline failure, not a failed market call.

CURRENT_BEHAVIOR: The grader falls back to `None/None` for a valid prediction, writes a direction/magnitude miss, and the sector’s rolling accuracy is punished for a call that actually hit. In Energy this also obscures a rare clean double-hit.

CORRECTED_BEHAVIOR: When a scoreboard entry shows `predicted None/None`, first check for a contemporaneous sector PREDICT block. If it contains explicit `predicted_direction` and `predicted_magnitude_band`, use those as the baseline and repair the entry. For Energy 2026-08-25, the corrected entry should be `direction_hit: True | magnitude_hit: True | predicted down/notable vs actual -1.6637604609503365%`. Add a grader guardrail: never record a miss against `None` while an explicit prediction exists.

EVIDENCE: Energy 2026-08-25 predict had `predicted_direction: down`, `predicted_magnitude_band: notable`, `total_score: -10.0`. Actual XLE was -1.66%, SPY +0.32%, relative -1.98%. Actual direction and magnitude both confirmed down/notable. The outcome narrative itself says “Direction and magnitude both HIT,” yet the scoreboard entry says `predicted None/None` and marks both False.

LESSON_MATCH_CHECK: Matches the already-flagged candidate lessons for Communication Services and Consumer Defensive on 2026-08-25: a valid predict block exists but the scoreboard records `None/None`. It is also adjacent to the absent-predict-file lessons, but this case is not an absent file; the prediction is fully present. It is not a reasoning error.

BACKWARD_CHECK: Correcting this entry changes Energy’s rolling stats from a false miss to a true hit: direction would move from 0.556 to 0.667 and magnitude from 0.333 to 0.444 over the graded n=9. It does not alter any prior reasoned call. It only repairs a corrupted grading artifact.

CONFLICT_CHECK: No conflict with active Energy lessons. The applied reasoning lessons — 08-11 live-oil verify, 08-14 green-oil escalation, 08-12 stale-run cap, 08-21 decoupling — remain valid and unaffected. This lesson also does not conflict with the absent-file lessons because those are genuinely no-baseline cases.

FALSIFIER: This lesson would be falsified if a `predicted None/None` scoreboard entry were accompanied by a genuinely absent or empty predict file at grading time. In that case, there is no baseline and the miss is legitimate. It is also falsified if an explicit predict exists but the actual outcome does not match its direction or magnitude; then the correct grading is a real miss.

DIVERGENCE_VERDICT: none_flagged

ACTIVE_LESSON_REVIEW: Applied-lesson check: the morning correctly applied 08-11 live-oil verify, correctly declined to fire 08-14 green-oil escalation, and correctly inverted the 08-21 decoupling logic. No active lesson covered the scoreboard `None/None` corruption; this lesson fills that gap.

SECTOR: Energy
LESSON_END
