# Sector Reflect — Consumer Defensive — 2026-09-03

Diagnostic verdict: **Primary error category C — reasoning/macro-gating failure**, not a tool/data failure. The ISM calendar item and the two-sided interpretation were both present in the morning text, so the data was available; the failure was allowing a weak S1 positive sleeve to produce an **up/flat** call when the load-bearing scheduled macro event should have suppressed a signed direction.

- XLP actual: **−0.32%**
- SPY actual: **+1.05%**
- Relative: **−1.36%**, direction miss
- Scoreboard: direction **MISS**, magnitude **MISS**
- Driver: ISM Services **55.4** vs ~54.2/54.3 forecast and 54.1 prior → strong risk-on rotation out of defensives.

LESSON_BEGIN
ERROR_CATEGORY: C
TRIGGER_PATTERN: A same-session high-impact macro release (ISM Services, CPI, payrolls, etc.) is explicitly named as the load-bearing, two-sided catalyst. Premarket macro tape is flat/mild and there is no anti-FTS or risk-on tape license; S0/S2/S4 are all near zero. Only a modest sector-factor sleeve (e.g., S1 input-cost relief) is positive. The model still emits a signed direction (up) because no divergence flag is tripped.
CURRENT_BEHAVIOR: The two-sided macro event is described in prose as event risk, but the score does not use it as a direction gate. An S1 sector-factor positive such as ag input-cost relief is allowed to generate an up/flat call. When the macro print surprises to the risk-on side, the defensive sector lags SPY by >1.3% and both direction and magnitude miss.
CORRECTED_BEHAVIOR: When a high-impact scheduled macro release is the stated load-bearing catalyst and S0/S2/S4 are neutral, do not let S1 alone create an up/down forecast. Before the data, either emit no-sign/flat or issue an explicitly conditional call (e.g., “if ISM services beats, XLP should lag in a risk-on rotation; if it misses, XLP can hold flat/up”). If a sector-factor sleeve is retained, it must be capped at sub-directional size or confirmed by S0/S2; the default pre-release output should be flat/no-sign.
EVIDENCE: 2026-09-03 XLP −0.32% vs SPY +1.05%, relative −1.36%. Morning components were S0=0, S1=+0.5, S2=0, S3=0, S4=0 with predicted up/flat. ISM Services printed 55.4 vs ~54.2 forecast, triggering a broad risk-on rotation into cyclicals/small caps/materials and out of staples; ISM Employment at 47.8 was the two-sided tension the morning noted, but the headline beat dominated.
LESSON_MATCH_CHECK: No existing Consumer Defensive lesson exactly matches. Closest recent candidates are 2026-09-03_sector_communication_services_lesson and 2026-09-03_sector_consumer_cyclical_lesson, which both point to the same failure family: flat premarket futures do not mean macro-neutral, and a scheduled macro binary cannot be treated as fully neutralized in the score. This lesson adds the defensive-rotation variant.
BACKWARD_CHECK: Correcting this on prior XLP runs would reduce unlicensed up calls. On 08-26, predicted up/flat and actual −0.29% would have been better served by flat/no-sign. On 08-28, predicted down/mild and actual +0.43% was caused by a different failure, not this macro-gate issue. The corrected rule does not contradict 08-27’s down/mild hit, because that call had a tape-based anti-FTS license rather than an S1 sleeve call.
CONFLICT_CHECK: No conflict with the active 08-12 analog (“do not one-way score two-sided macro”). This correction suppresses an unlicensed up rather than requiring a one-way down call. It also reinforces the 08-28 DO-INSTEAD “prefer flat/mild when sign fights tape” lesson rather than contradicting it.
FALSIFIER: The lesson is falsified if future cases with flat premarket tape, a scheduled two-sided macro print, and S1-only positive factors repeatedly resolve according to S1 (e.g., XLP rises on input-cost relief) while macro is not the dominant driver. A single macro surprise does not falsify it; a systematic failure of the flat/no-sign default versus the S1-based directional call would.
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: The morning correctly applied the 08-28 and 08-12 disciplines in prose—it did not restack the 3d FTS, did not fire the 08-27 anti-FTS gate, and did not treat AVGO or COST as fresh XLP drivers. The missing active-lesson reinforcement was that the identified macro binary should have capped the entire directional score when no confirmed premarket component supported up. That is the new lesson to carry forward.
SECTOR: Consumer Defensive
LESSON_END
