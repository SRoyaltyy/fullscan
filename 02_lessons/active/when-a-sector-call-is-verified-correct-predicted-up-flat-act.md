---
trigger_pattern: "When a sector call is verified correct — predicted up/flat, actual XLY +0.475% classified as flat, post-session review says both direction and magnitude HIT — but the individual scoreboard line records magnitude_hit False while the same scoreboard's rolling mag=0.5 (n=4) arithmetically requires the current run to be a hit, the False flag is a scoreboard/accounting data error, not a sector reasoning miss."
corrected_behavior: "Reconcile the scoreboard flag against the band classification and rolling accuracy before writing a lesson. If predicted flat and actual +0.475% is classified as flat by the outcome, and the post-session verdict says magnitude HIT, score it as a magnitude HIT and flag the individual False line as a data-entry/accounting error. Do not convert a phantom scoreboard miss into a sector-behavior lesson."
falsifier: "If the official Consumer Cyclical rubric defines the flat band as |m| <= 0.40%, then 0.475% is actually mild, the scoreboard False flag would be legitimate, and this data-error lesson should be discarded in favor of a mild-magnitude-miss lesson. Absent such rubric evidence, the scoreboard line is a data error."
current_behavior: "The individual magnitude_hit False line is taken at face value. The reflection would hunt for a 'flat magnitude call was wrong' lesson despite the actual move being flat, contaminating rolling accuracy and potentially overriding the correct DO-INSTEAD lesson."
evidence_cited: "Predicted up/flat. Actual XLY +0.475%, SPY +0.698%, rel -0.223%. Outcome states: 'Direction: up, Magnitude: flat (sub-1%)' and 'DIRECTION HIT, MAGNITUDE HIT.' Post-session review says the morning call was 'fully correct.' Scoreboard line says magnitude_hit False, but the same scoreboard history shows mag=0.5 (n=4), which is consistent only with 08-13 being a hit if the prior four graded runs are 08-10 hit, 08-11 miss, 08-12 miss, 08-13 hit."
error_category: "B"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Reconcile the scoreboard flag against the band classification and rolling accuracy before writing a lesson. If predicted flat and actual +0.475% is classified as flat by the outcome, and the post-session verdict says magnitude HIT, score it as a magnitude HIT and flag the individual False line as a data-entry/accounting error. Do not convert a phantom scoreboard miss into a sector-behavior lesson.

## WHEN IT FIRES
When a sector call is verified correct — predicted up/flat, actual XLY +0.475% classified as flat, post-session review says both direction and magnitude HIT — but the individual scoreboard line records magnitude_hit False while the same scoreboard's rolling mag=0.5 (n=4) arithmetically requires the current run to be a hit, the False flag is a scoreboard/accounting data error, not a sector reasoning miss.

## WRONG IF
If the official Consumer Cyclical rubric defines the flat band as |m| <= 0.40%, then 0.475% is actually mild, the scoreboard False flag would be legitimate, and this data-error lesson should be discarded in favor of a mild-magnitude-miss lesson. Absent such rubric evidence, the scoreboard line is a data error.

## EVIDENCE
Predicted up/flat. Actual XLY +0.475%, SPY +0.698%, rel -0.223%. Outcome states: "Direction: up, Magnitude: flat (sub-1%)" and "DIRECTION HIT, MAGNITUDE HIT." Post-session review says the morning call was "fully correct." Scoreboard line says magnitude_hit False, but the same scoreboard history shows mag=0.5 (n=4), which is consistent only with 08-13 being a hit if the prior four graded runs are 08-10 hit, 08-11 miss, 08-12 miss, 08-13 hit.

(learn_cycle promote)
