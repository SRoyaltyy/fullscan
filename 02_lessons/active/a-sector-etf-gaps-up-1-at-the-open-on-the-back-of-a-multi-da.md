---
trigger_pattern: "A sector ETF gaps up ≥1% at the open on the back of a multi-day shock that is now on day-3+ with no fresh step-change headline, in a risk-off broad tape, after a large 1m relative run (≥+8%); the model scores prior-session relative tape (Channel 1 through the prior close) as live forward confirmation in both the breadth channel (S2) and the ETF-tape channel (S4), and scores the sector-factor cluster (S1) at +2 despite its own prose calling it a 'modest continuation' with 'no fresh step-change."
corrected_behavior: "(1) On a gap-up open, prior-session relative tape is already in the price — score S4=0, not +1, and do not re-score the same fact in S2. (2) When the sector's own shock is day-3+ with no fresh increment and the incremental commodity print is sub-1.5%, S1 caps at +1 (not +2) — the prose and the score must agree. (3) The magnitude-discipline rule must be enforced at the scoring layer: if the narrative says 'cap at mild,' the pipeline must not emit notable; a written cap the arithmetic can override is not a cap. (4) Run the knowable-at-open test explicitly: if every bullish item is already-printed and the only forward item is two-sided (EIA WPSR), the base case is a gap fade, not a continuation."
falsifier: "This lesson is falsified if, on a future day matching the trigger (gap-up ≥1% open, day-3+ shock with no fresh headline, risk-off tape, ≥+8% 1m rel), XLE closes at or above its open and holds the gap — i.e., the stale-tape-as-confirmation scoring produces a correct up/notable call. Concretely: if a gap-up energy open on a day-3 shock with no fresh increment closes green and near the high, the 'gap fade / stale tape is not confirmation' rule is wrong for that regime."
current_behavior: "Channel 1 tape 'through 2026-09-09' (1d rel +1.30%, 3d rel +2.46%) was read as 'the oil bid is transmitting' and scored S2=+1 AND S4=+1 — the same stale observation counted twice. S1 was netted at +2 while the narrative simultaneously wrote 'this is a modest continuation, not 09-08's +3.18% surge' and 'no fresh step-change headline.' The pipeline then emitted total_score 8.5 / notable, silently overriding the narrative's own written magnitude-discipline cap at mild. XLE gapped +1.3% and round-tripped to −0.58% (rel +0.02%), closing near the low."
evidence_cited: "Predicted up/notable (pipeline 8.5) vs actual XLE −0.58% / SPY −0.60% / rel +0.02% — dir MISS, mag MISS. XLE open 66.14 → close 64.93 (full round-trip of the +1.3% gap, close near low). WTI prior-session reference $97.26 (FRED DCOILWTICO 2026-09-09) — barrel was not materially higher intraday; intraday crude prints mixed/conflicting across sources. Scoreboard: last-10 dir=0.3 mag=0.2; 09-10 dir MISS."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
(1) On a gap-up open, prior-session relative tape is already in the price — score S4=0, not +1, and do not re-score the same fact in S2. (2) When the sector's own shock is day-3+ with no fresh increment and the incremental commodity print is sub-1.5%, S1 caps at +1 (not +2) — the prose and the score must agree. (3) The magnitude-discipline rule must be enforced at the scoring layer: if the narrative says "cap at mild," the pipeline must not emit notable; a written cap the arithmetic can override is not a cap. (4) Run the knowable-at-open test explicitly: if every bullish item is already-printed and the only forward item is two-sided (EIA WPSR), the base case is a gap fade, not a continuation.

## WHEN IT FIRES
A sector ETF gaps up ≥1% at the open on the back of a multi-day shock that is now on day-3+ with no fresh step-change headline, in a risk-off broad tape, after a large 1m relative run (≥+8%); the model scores prior-session relative tape (Channel 1 through the prior close) as live forward confirmation in both the breadth channel (S2) and the ETF-tape channel (S4), and scores the sector-factor cluster (S1) at +2 despite its own prose calling it a "modest continuation" with "no fresh step-change.

## WRONG IF
This lesson is falsified if, on a future day matching the trigger (gap-up ≥1% open, day-3+ shock with no fresh headline, risk-off tape, ≥+8% 1m rel), XLE closes at or above its open and holds the gap — i.e., the stale-tape-as-confirmation scoring produces a correct up/notable call. Concretely: if a gap-up energy open on a day-3 shock with no fresh increment closes green and near the high, the "gap fade / stale tape is not confirmation" rule is wrong for that regime.

## EVIDENCE
Predicted up/notable (pipeline 8.5) vs actual XLE −0.58% / SPY −0.60% / rel +0.02% — dir MISS, mag MISS. XLE open 66.14 → close 64.93 (full round-trip of the +1.3% gap, close near low). WTI prior-session reference $97.26 (FRED DCOILWTICO 2026-09-09) — barrel was not materially higher intraday; intraday crude prints mixed/conflicting across sources. Scoreboard: last-10 dir=0.3 mag=0.2; 09-10 dir MISS.

(learn_cycle promote)
