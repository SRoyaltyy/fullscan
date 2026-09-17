---
trigger_pattern: "Energy/XLE on the session after a large down day, when leftover 1d relative ≤ −1.5% but live oil is still only offered and sub-1.5%, PM:XLE is ~flat (not extending), and post-event index repair is live (ES/NQ green vs prior cash). Overlay then fires S1+S4 aligned-negative off prior-close relative and mutes S0 because risk-on looks tech-not-energy."
current_behavior: "Force absolute down from leftover S4 plus an offered barrel; treat oil-down and equity beta as independent non-transmission; keep yesterday’s direction after a mag miss."
corrected_behavior: "Prior-close 1d rel confirms the previous session, not today, unless PM or live oil is extending the smash. Score offered oil as relative drag and as a possible SPX-positive inflation/yield tailwind that can leak beta into XLE. Allow mixed/up absolute when PM is flat and index repair is the live impulse; keep magnitude mild. 09-04 still binds only when S1 and live S4 (PM extension or same-session rel) are both negative."
evidence_cited: "2026-09-17 issued engine up/mild HIT (XLE +0.70%, SPY +1.13%, rel −0.43%) after a gap-down open 63.50 then afternoon repair; WTI still ~−1.3% (not a collapse); overlay down/mild from S0=0, S1=−1, S4=−1. Path: Benzinga ~9:10am ET XLE −0.75% only red sector, close green. Writeup already said do not rerun 09-16’s −2.88% hindsight, then 09-04 did exactly that. Tape_anchor CL −1.59% was a stale 09-16 sleeve; engine still won on ES +1.71% and PM:XLE +0.08%."
error_category: "A"
falsifier: "Same setup (leftover 1d rel smash, PM ~flat, oil offered sub-1.5%, ES/NQ green, no fresh kinetic) but XLE still closes down on the day — then leftover S4 still had directional information and 09-04 should not be gated."
sector: "Energy"
date: "2026-09-17"
status: "candidate"
---

# Sector Reflection — Energy — 2026-09-17

Memory index is paused (different embedding provider); this uses the injected predict, outcome, scoreboard, and candidate/active lessons only.

LESSON_BEGIN
ERROR_CATEGORY: A
TRIGGER_PATTERN: Energy/XLE on the session after a large down day, when leftover 1d relative ≤ −1.5% but live oil is still only offered and sub-1.5%, PM:XLE is ~flat (not extending), and post-event index repair is live (ES/NQ green vs prior cash). Overlay then fires S1+S4 aligned-negative off prior-close relative and mutes S0 because risk-on looks tech-not-energy.
CURRENT_BEHAVIOR: Force absolute down from leftover S4 plus an offered barrel; treat oil-down and equity beta as independent non-transmission; keep yesterday’s direction after a mag miss.
CORRECTED_BEHAVIOR: Prior-close 1d rel confirms the previous session, not today, unless PM or live oil is extending the smash. Score offered oil as relative drag and as a possible SPX-positive inflation/yield tailwind that can leak beta into XLE. Allow mixed/up absolute when PM is flat and index repair is the live impulse; keep magnitude mild. 09-04 still binds only when S1 and live S4 (PM extension or same-session rel) are both negative.
EVIDENCE: 2026-09-17 issued engine up/mild HIT (XLE +0.70%, SPY +1.13%, rel −0.43%) after a gap-down open 63.50 then afternoon repair; WTI still ~−1.3% (not a collapse); overlay down/mild from S0=0, S1=−1, S4=−1. Path: Benzinga ~9:10am ET XLE −0.75% only red sector, close green. Writeup already said do not rerun 09-16’s −2.88% hindsight, then 09-04 did exactly that. Tape_anchor CL −1.59% was a stale 09-16 sleeve; engine still won on ES +1.71% and PM:XLE +0.08%.
LESSON_MATCH_CHECK: No Energy-specific candidate covers leftover 09-04 S4 reuse. General 09-17 post-FOMC lesson (printed FOMC, ES/NQ ≥ +0.5%, oil offered, no fresh kinetic) matches the macro tape only. 09-17 industrials is the closest cyclical cousin but was all-zero/flat overlay and a fade-to-flat print, not 09-04-forced down. 09-17 defensive lessons are the inverse (engine up vs true lag). 09-16 FOMC-unprinted lessons do not apply — FOMC already printed 09-16. Partial overlap, not a duplicate.
BACKWARD_CHECK: Would not flip 09-16 down/mild HIT (that day was the live smash, not leftover S4). Would have stopped 09-17 overlay down without forcing up on every post-smash morning — gate is PM/oil not extending plus live index repair. Does not license refiners (VLO/MPC) to set XLE. Mag-discipline stays.
CONFLICT_CHECK: Refines 09-04 rather than repealing it. Conflicts with a blind sector_energy “keep direction, shrink confidence after mag misses” when the kept direction is leftover tape. No conflict with 08-11 live-oil verify (oil sign stayed down and explained the lag). No conflict with 08-14/09-15/09-10/09-14 non-fires.
FALSIFIER: Same setup (leftover 1d rel smash, PM ~flat, oil offered sub-1.5%, ES/NQ green, no fresh kinetic) but XLE still closes down on the day — then leftover S4 still had directional information and 09-04 should not be gated.
DIVERGENCE_VERDICT: futures_right
ACTIVE_LESSON_REVIEW: 09-04 aligned-negative fired and was the overlay miss (S4 reused 09-16 1d rel −2.44%; PM +0.08% already said not extending). Mag-discipline / size_gate correctly capped mild. 08-11 live-oil verify correctly kept oil DOWN vs stale Finviz. 08-14 green-oil, 09-15 physical-increment, 09-14 backwardation, 09-11 pending-binary, 09-10 crowded-long, 09-03/09-04 exhaustion all correctly did not fire. sector_energy keep-direction-after-09-16-mag-miss reinforced repeating down and should yield to live PM/oil increment. S0=0 was too muted: oil-down was XLE-negative and SPX-positive; beta leaked enough for absolute up / relative lag.
SECTOR: Energy
LESSON_END
