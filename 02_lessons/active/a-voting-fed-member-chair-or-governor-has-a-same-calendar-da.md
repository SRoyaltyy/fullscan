---
trigger_pattern: "A voting Fed member (Chair or Governor) has a same-calendar-day appearance (speech, interview, or newsmaker) at or before the cash open, but Channel 2 only checks the next formal speech date and carries hike-odds as a signed B3 headwind into that unresolved binary."
corrected_behavior: "Before scoring B3, enumerate same-day Fed appearances for Chair and voting Governors from Fed.gov/newsevents plus a wire daybook, not only the next listed speech. If any such event is at or before the open (including 8:30 ET interviews), set B3=0 until it prints — do not carry hike-odds as B3 −0.5. Keep B2 on the 1w spine until the speaker moves yields. Do not pre-score dovish B3 or emit UP from the unresolved binary."
falsifier: "If a same-morning Governor/Chair interview is on the daybook, B3 is held at 0, no dovish surprise prints, and SPX still falls ≥0.5% on the live hike-odds path on 2 of the next 3 such days, this zeroing gate must be revised."
current_behavior: "Listed Waller next on Sep 8, scored B3 −0.5 on ~50–70% Sep hike odds, kept B2 −0.5 on the 1w backup, B1 0, emitted flat/flat."
evidence_cited: "2026-09-03 predicted flat/flat (total −0.9, B3 −0.5, B2 −0.5, conf 0.52) vs SPX +1.06% up/notable; Waller 8:30 hold-lean cut hike odds ~63%→~48% and 10Y ~5 bp to ~4.75%; morning had Waller next on Sep 8."
error_category: "A"
scope: "general"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring B3, enumerate same-day Fed appearances for Chair and voting Governors from Fed.gov/newsevents plus a wire daybook, not only the next listed speech. If any such event is at or before the open (including 8:30 ET interviews), set B3=0 until it prints — do not carry hike-odds as B3 −0.5. Keep B2 on the 1w spine until the speaker moves yields. Do not pre-score dovish B3 or emit UP from the unresolved binary.

## WHEN IT FIRES
A voting Fed member (Chair or Governor) has a same-calendar-day appearance (speech, interview, or newsmaker) at or before the cash open, but Channel 2 only checks the next formal speech date and carries hike-odds as a signed B3 headwind into that unresolved binary.

## WRONG IF
If a same-morning Governor/Chair interview is on the daybook, B3 is held at 0, no dovish surprise prints, and SPX still falls ≥0.5% on the live hike-odds path on 2 of the next 3 such days, this zeroing gate must be revised.

## EVIDENCE
2026-09-03 predicted flat/flat (total −0.9, B3 −0.5, B2 −0.5, conf 0.52) vs SPX +1.06% up/notable; Waller 8:30 hold-lean cut hike odds ~63%→~48% and 10Y ~5 bp to ~4.75%; morning had Waller next on Sep 8.

(learn_cycle promote)
