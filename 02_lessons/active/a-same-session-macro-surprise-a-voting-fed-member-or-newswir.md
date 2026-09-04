---
trigger_pattern: "A same-session macro surprise — a voting Fed member or newswire appearance that resolves a contested rate-hike narrative in the opposite direction from what had been priced — moves equities broadly after the sector forecast snapshot, turning a flat/flat call into a broad risk-on day. The sector layer has no event-risk marker for “Fed speaker live later today” and therefore treats the contested policy path as fully paid."
corrected_behavior: "Before emitting S0, scan the calendar for voting Fed-member appearances — speeches, interviews, newsmaker events — not just formal meetings / next speech dates. If a Fed speaker is scheduled while rate-hike odds are contested, do not encode that binary as fully paid. Keep S0 at 0 from a direction standpoint, but lower confidence further and explicitly mark the session as an unresolved-policy event day rather than treating it as a settled “pause” tape. If the data layer can support conditional scenarios, note the dovish-surprise asymmetry explicitly; otherwise leave direction flat but prevent confidence from implying that the event risk was already resolved."
falsifier: "If a scheduled Fed-speaker day with contested hike odds produces a no-surprise or hawkish speech and XLI closes down, any rule that turned S0 positive preemptively would be falsified. The corrected behavior survives that test only because it does not pre-score the speaker’s content. It would also be falsifiable if, over a sample of Fed-speaker event days, the event-risk flag never improves calibration and merely lowers confidence without changing outcomes; then it should be removed."
current_behavior: "The sector layer sees flat premarket futures, treats Warsh/hike odds as already priced, treats a scheduled two-sided macro item as binary-but-unscorable, and emits S0=0 with a deterministic flat/flat score. It does not separately flag a same-calendar-day Fed appearance that can resolve the contested rate path before the cash session is over."
evidence_cited: "XLI closed +1.03% vs SPY +1.05% (relative −0.02%), after Governor Waller’s same-session dovish speech cooled September rate-hike bets and ISM Services beat at 55.4. The morning forecast was flat/flat with S0=S1=S2=S3=S4=0. The sector-specific reads S1–S4 were correct; the miss was entirely that S0 had no mechanism to carry a scheduled Fed-speaker event risk into the session."
error_category: "A"
scope: "general"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Before emitting S0, scan the calendar for voting Fed-member appearances — speeches, interviews, newsmaker events — not just formal meetings / next speech dates. If a Fed speaker is scheduled while rate-hike odds are contested, do not encode that binary as fully paid. Keep S0 at 0 from a direction standpoint, but lower confidence further and explicitly mark the session as an unresolved-policy event day rather than treating it as a settled “pause” tape. If the data layer can support conditional scenarios, note the dovish-surprise asymmetry explicitly; otherwise leave direction flat but prevent confidence from implying that the event risk was already resolved.

## WHEN IT FIRES
A same-session macro surprise — a voting Fed member or newswire appearance that resolves a contested rate-hike narrative in the opposite direction from what had been priced — moves equities broadly after the sector forecast snapshot, turning a flat/flat call into a broad risk-on day. The sector layer has no event-risk marker for “Fed speaker live later today” and therefore treats the contested policy path as fully paid.

## WRONG IF
If a scheduled Fed-speaker day with contested hike odds produces a no-surprise or hawkish speech and XLI closes down, any rule that turned S0 positive preemptively would be falsified. The corrected behavior survives that test only because it does not pre-score the speaker’s content. It would also be falsifiable if, over a sample of Fed-speaker event days, the event-risk flag never improves calibration and merely lowers confidence without changing outcomes; then it should be removed.

## EVIDENCE
XLI closed +1.03% vs SPY +1.05% (relative −0.02%), after Governor Waller’s same-session dovish speech cooled September rate-hike bets and ISM Services beat at 55.4. The morning forecast was flat/flat with S0=S1=S2=S3=S4=0. The sector-specific reads S1–S4 were correct; the miss was entirely that S0 had no mechanism to carry a scheduled Fed-speaker event risk into the session.

(learn_cycle promote)
