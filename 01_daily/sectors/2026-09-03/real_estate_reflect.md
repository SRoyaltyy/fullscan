# Sector Reflect — Real Estate — 2026-09-03

LESSON_BEGIN
ERROR_CATEGORY: NONE
TRIGGER_PATTERN: A rate-path-sensitive sector forecast is built from S0–S3 all near zero, a contested but already-priced Fed hike path, a modest pre-open long-end dip capped as not-duration-relief because the 30Y remains in a stress zone, and a prior-session relative-lag tape used only as a cap. Later in the same session, an unscheduled voting-Fed-member statement resolves the contested policy path in the dovish direction and delivers the duration-relief move that the open factor set had no data to pre-score.
CURRENT_BEHAVIOR: The model treats the FOMC path as fully priced when no formal decision or known same-day Fed object is in the calendar, maps S0–S3 = 0 to flat, and uses the prior red relative tape only as S4 = −1. It does not create a separate event-risk reserve for “unscheduled Fed speaker later today,” so the resulting flat/flat point estimate carries no visible openness to a policy-reversal gap.
CORRECTED_BEHAVIOR: Before finalizing a flat call in a rate-path-sensitive sector, run a true same-calendar-day Fed-appearance scan — formal speeches, interviews, newsmaker windows, and Fed publication times — and treat any listed voting member as unresolved event risk rather than fully priced. If
