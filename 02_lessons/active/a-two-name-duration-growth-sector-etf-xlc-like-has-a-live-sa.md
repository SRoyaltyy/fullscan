---
trigger_pattern: "A two-name duration/growth sector ETF (XLC-like) has a live same-session down confirmation (worst or bottom-quartile premarket sector print and/or same-day 1d rel already negative) into a knowable oil/duration risk-off tape, the narrative emits down/mild, but the deterministic sector_rs_veto flattens official direction to flat because leftover Finviz 1d AND 1w relative strength are both still positive."
corrected_behavior: "Enforce the leftover-ban inside sector_rs_veto: do not flatten a directional call on prior-close 1d/1w RS when live PM:ETF or same-session 1d rel confirms the call. Live tape outranks leftover RS. If narrative and pipeline disagree under a live risk-off overlay, do not let the veto silently win; grade the reconciled live-tape call. Do not score a stale prior predict (up/mild) when the contemporaneous block is down/mild or flat/flat."
falsifier: "Same setup (leftover d1 and w1 RS positive, live PM/1d rel confirming down, oil/duration shock live, veto suppressed) but XLC closes flat-to-up because leftover RS correctly signaled the two anchors being bought — then restore the veto and treat leftover RS as the better signal."
current_behavior: "Post-LLM decision_gate flattened predicted_direction/magnitude to flat/flat (sector_rs_veto_applied True, d1 +2.68 / w1 +3.47) despite narrative down/mild, live PM:XLC −0.63%, and same-day 1d rel −0.46%. Scoreboard then graded a leftover up/mild, producing direction_hit False / magnitude_hit True against XLC −0.90%."
evidence_cited: "2026-09-15 XLC −0.90% vs SPY −0.46% (rel −0.45%), actual down/mild, knowable-at-open. Narrative S0 −1 / S1 −0.5 / S2 0 / S3 0 / S4 −1, down/mild, 5/5 components HIT. Pipeline total_score −4.64 but veto → flat/flat. Scoreboard predicted up/mild (matches neither). Veto inputs d1 +2.68 / w1 +3.47 are leftover vs live 1d rel −0.46%."
error_category: "D"
scope: "ops"
date: "2026-09-15"
status: "active"
occurrences: "1"
promoted_on: "2026-09-15"
sources: "['2026-09-15_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
Enforce the leftover-ban inside sector_rs_veto: do not flatten a directional call on prior-close 1d/1w RS when live PM:ETF or same-session 1d rel confirms the call. Live tape outranks leftover RS. If narrative and pipeline disagree under a live risk-off overlay, do not let the veto silently win; grade the reconciled live-tape call. Do not score a stale prior predict (up/mild) when the contemporaneous block is down/mild or flat/flat.

## WHEN IT FIRES
A two-name duration/growth sector ETF (XLC-like) has a live same-session down confirmation (worst or bottom-quartile premarket sector print and/or same-day 1d rel already negative) into a knowable oil/duration risk-off tape, the narrative emits down/mild, but the deterministic sector_rs_veto flattens official direction to flat because leftover Finviz 1d AND 1w relative strength are both still positive.

## WRONG IF
Same setup (leftover d1 and w1 RS positive, live PM/1d rel confirming down, oil/duration shock live, veto suppressed) but XLC closes flat-to-up because leftover RS correctly signaled the two anchors being bought — then restore the veto and treat leftover RS as the better signal.

## EVIDENCE
2026-09-15 XLC −0.90% vs SPY −0.46% (rel −0.45%), actual down/mild, knowable-at-open. Narrative S0 −1 / S1 −0.5 / S2 0 / S3 0 / S4 −1, down/mild, 5/5 components HIT. Pipeline total_score −4.64 but veto → flat/flat. Scoreboard predicted up/mild (matches neither). Veto inputs d1 +2.68 / w1 +3.47 are leftover vs live 1d rel −0.46%.

(learn_cycle promote)
