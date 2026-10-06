---
trigger_pattern: "Before the open, Channel 1 ES/NQ is locked from an early snapshot inside ±0.5% while a later same-window Channel 1 print already shows ES or NQ independently ≥ +0.5%, on a leftover-catalyst session (prior-day macro already paid, no fresh B1 AHR, no unsigned CPI/NFP/FOMC binary)."
corrected_behavior: "Ops step — re-fetch Channel 1 ES/NQ in the final pre-open window (~09:15–09:28 ET) and overwrite B6 if the later snapshot independently exceeds ±0.5%; after that refresh, if |B6| ≥ 0.5% and no unsigned CPI/NFP/FOMC binary is pending, do not emit predicted_direction=flat against that confirming B6. Do not restack paid NFP/hike-odds into B1; do not refresh-and-follow a later red B0 Asia as the US direction when B6 is the confirming tape."
falsifier: "If B6 is refreshed to ES or NQ independently ≥ +0.5% with no CPI/NFP/FOMC pending and SPX still closes < +0.3% on 2 of the next 3 such days, this gate must be revised, not defended."
current_behavior: "Emit used the early Channel 1 B6=0 (ES +0.02% / NQ −0.04%), refused a greener non-Channel-1 quote, left B6 confirmation-only at 0, and emitted flat/flat; tape_anchor stayed negative/flat against a premarket that had already greened."
evidence_cited: "2026-10-05 predicted flat/flat (total 0.086, B6=0, ES +0.02%/NQ −0.04%) vs SPX +0.66% up/mild (NDX +1.05%); later Channel 1 ES +0.67% to +0.80% / NQ +0.88% to +0.89%; KNOWABLE_AT_9AM partial; ISM 54.9/Prices 74 mid-morning, SPX/NDX never red."
error_category: "D"
scope: "ops"
date: "2026-10-05"
status: "active"
occurrences: "1"
promoted_on: "2026-10-05"
sources: "['2026-10-05_lesson.md']"
schema_ok: "true"
---

## RULE
Ops step — re-fetch Channel 1 ES/NQ in the final pre-open window (~09:15–09:28 ET) and overwrite B6 if the later snapshot independently exceeds ±0.5%; after that refresh, if |B6| ≥ 0.5% and no unsigned CPI/NFP/FOMC binary is pending, do not emit predicted_direction=flat against that confirming B6. Do not restack paid NFP/hike-odds into B1; do not refresh-and-follow a later red B0 Asia as the US direction when B6 is the confirming tape.

## WHEN IT FIRES
Before the open, Channel 1 ES/NQ is locked from an early snapshot inside ±0.5% while a later same-window Channel 1 print already shows ES or NQ independently ≥ +0.5%, on a leftover-catalyst session (prior-day macro already paid, no fresh B1 AHR, no unsigned CPI/NFP/FOMC binary).

## WRONG IF
If B6 is refreshed to ES or NQ independently ≥ +0.5% with no CPI/NFP/FOMC pending and SPX still closes < +0.3% on 2 of the next 3 such days, this gate must be revised, not defended.

## EVIDENCE
2026-10-05 predicted flat/flat (total 0.086, B6=0, ES +0.02%/NQ −0.04%) vs SPX +0.66% up/mild (NDX +1.05%); later Channel 1 ES +0.67% to +0.80% / NQ +0.88% to +0.89%; KNOWABLE_AT_9AM partial; ISM 54.9/Prices 74 mid-morning, SPX/NDX never red.

(learn_cycle promote)
