---
trigger_pattern: "A sector ETF's trailing 1d relative print (from the prior close) is used to justify TWO separate positive component scores (e.g., S2 breadth/leadership AND S4 ETF tape) on the same session, when no fresh same-day sector-internal breadth data (constituent-level tape, flow print) is available to independently confirm the leadership claim. The single stale rel print is treated as both a breadth signal and a tape signal."
corrected_behavior: "When no fresh same-day constituent/breadth data exists, a single trailing 1d rel print may anchor AT MOST ONE component score. If it is used for S4 (tape), S2 must be scored 0 absent independent breadth evidence (constituent-level moves, flow print, or a multi-day rel pattern that is itself the breadth signal). Do not let one stale rel print justify two positive scores. Additionally, a single day's rel print in a concentrated two-name book is weak evidence of structural leadership — it should inform S4 lightly, not anchor both S2 and S4."
falsifier: "If on a future session a single trailing 1d rel print is used for both S2 and S4, and the sector subsequently delivers a rel print of the same sign and comparable magnitude (i.e., the leadership persists), then the double-count was not harmful and this lesson should be downgraded. Specifically: if XLC's next-session rel is ≥ +0.5% (persisting the prior +1.20%), the 'weak evidence' claim is falsified for that instance."
current_behavior: "On 2026-09-11 the morning note scored S2=+1 ('large-cap leadership inside sector') and S4=+1 ('freshest tape print') — both anchored on the same 09-10 1d rel +1.20% print. The note explicitly acknowledged the print was prior-close history and argued around the 08-28 leftover ban rather than cleanly satisfying it. The realized rel collapsed to +13bp, showing the +1.20% was a one-day risk-off rotation artifact, not persistent two-name leadership. Direction was right (up) but the breadth rationale was over-extrapolated and the two scores were a latent double-count of one data point."
evidence_cited: "Morning: S2=+1 and S4=+1, both citing 09-10 1d rel +1.20%. Actual: XLC +0.99% vs SPY +0.85% = rel +0.13% — the +1.20% did not persist. Outcome review §3: 'S4=+1 and S2=+1 were both leaning on the same 09-10 rel +1.20% print. That is a latent double-count.' §2 S2 verdict: 'Direction right, magnitude of conviction overstated.' §5: 'a single day's relative print in a two-name book is weak evidence of structural leadership."
error_category: "C"
scope: "general"
date: "2026-09-11"
status: "active"
occurrences: "1"
promoted_on: "2026-09-11"
sources: "['2026-09-11_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
When no fresh same-day constituent/breadth data exists, a single trailing 1d rel print may anchor AT MOST ONE component score. If it is used for S4 (tape), S2 must be scored 0 absent independent breadth evidence (constituent-level moves, flow print, or a multi-day rel pattern that is itself the breadth signal). Do not let one stale rel print justify two positive scores. Additionally, a single day's rel print in a concentrated two-name book is weak evidence of structural leadership — it should inform S4 lightly, not anchor both S2 and S4.

## WHEN IT FIRES
A sector ETF's trailing 1d relative print (from the prior close) is used to justify TWO separate positive component scores (e.g., S2 breadth/leadership AND S4 ETF tape) on the same session, when no fresh same-day sector-internal breadth data (constituent-level tape, flow print) is available to independently confirm the leadership claim. The single stale rel print is treated as both a breadth signal and a tape signal.

## WRONG IF
If on a future session a single trailing 1d rel print is used for both S2 and S4, and the sector subsequently delivers a rel print of the same sign and comparable magnitude (i.e., the leadership persists), then the double-count was not harmful and this lesson should be downgraded. Specifically: if XLC's next-session rel is ≥ +0.5% (persisting the prior +1.20%), the "weak evidence" claim is falsified for that instance.

## EVIDENCE
Morning: S2=+1 and S4=+1, both citing 09-10 1d rel +1.20%. Actual: XLC +0.99% vs SPY +0.85% = rel +0.13% — the +1.20% did not persist. Outcome review §3: "S4=+1 and S2=+1 were both leaning on the same 09-10 rel +1.20% print. That is a latent double-count." §2 S2 verdict: "Direction right, magnitude of conviction overstated." §5: "a single day's relative print in a two-name book is weak evidence of structural leadership.

(learn_cycle promote)
