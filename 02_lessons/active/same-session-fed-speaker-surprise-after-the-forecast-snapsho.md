---
trigger_pattern: "Same-session Fed-speaker surprise after the forecast snapshot reverses a carried hawkish hike-odds narrative. The pre-open tape and yield levels contain no signal of the speaker’s dovish pivot; the sector’s leading scores are all zero, so a flat/flat call is the modal output and the actual absolute move is a direction miss despite a correct relative-lag read."
corrected_behavior: "No Utilities-specific rule change is warranted. A general same-calendar-day Fed-speaker rule — if promoted — should be used to flag an unresolved binary and lower confidence, not to mint a directional up or down call, because the side of the speaker surprise is not knowable at the snapshot. For XLU on a broad risk-on day, flat-lag remains the correct relative characterization."
falsifier: "If it is later shown that Waller’s 9/3 remarks were present in a pre-snapshot wire feed, calendar entry, or reliable pre-open yield/futures footprint and the model ignored them, then this NONE verdict would be wrong and should become a data/reasoning-error category."
current_behavior: "Utilities sector scoring treated the 9/2 hawkish Fed object (Sep hike odds ~70%, DGS10 4.79%, DGS30 5.27%) as already carried into price, correctly declined to manufacture downside from 1w/1m XLU lag, and set S0–S4 all to zero → flat/flat."
evidence_cited: "XLU +0.84% vs SPY +1.05% (rel −0.20%); Waller’s same-session rate-hold signal drove yields down and a risk-on tech-led rally; the prediction had no pre-open access to that signal. Outcome labeling: knowable_at_open = no, mirror of 08-28 hawkish-Warsh in the opposite direction."
error_category: "NONE"
scope: "general"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
No Utilities-specific rule change is warranted. A general same-calendar-day Fed-speaker rule — if promoted — should be used to flag an unresolved binary and lower confidence, not to mint a directional up or down call, because the side of the speaker surprise is not knowable at the snapshot. For XLU on a broad risk-on day, flat-lag remains the correct relative characterization.

## WHEN IT FIRES
Same-session Fed-speaker surprise after the forecast snapshot reverses a carried hawkish hike-odds narrative. The pre-open tape and yield levels contain no signal of the speaker’s dovish pivot; the sector’s leading scores are all zero, so a flat/flat call is the modal output and the actual absolute move is a direction miss despite a correct relative-lag read.

## WRONG IF
If it is later shown that Waller’s 9/3 remarks were present in a pre-snapshot wire feed, calendar entry, or reliable pre-open yield/futures footprint and the model ignored them, then this NONE verdict would be wrong and should become a data/reasoning-error category.

## EVIDENCE
XLU +0.84% vs SPY +1.05% (rel −0.20%); Waller’s same-session rate-hold signal drove yields down and a risk-on tech-led rally; the prediction had no pre-open access to that signal. Outcome labeling: knowable_at_open = no, mirror of 08-28 hawkish-Warsh in the opposite direction.

(learn_cycle promote)
