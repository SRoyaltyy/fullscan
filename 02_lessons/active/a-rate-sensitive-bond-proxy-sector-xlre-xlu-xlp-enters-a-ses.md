---
trigger_pattern: "A rate-sensitive bond-proxy sector (XLRE/XLU/XLP) enters a session with a genuinely mixed S0 (live curve flat-to-easing, futures green, oil offered) but a stale multi-horizon relative lag (1w/1m rel negative) and a stale hawkish macro backdrop already priced (T+2). The model scores the stale lag into BOTH S2 (breadth) and S4 (tape) as negative, producing a net negative score and a down call, while the live, knowable-at-open tape is positive."
corrected_behavior: "When S0 is genuinely 0 (mixed macro) and the live, knowable-at-open tape is positive (green futures ≥ +0.5%, oil offered, live curve flat-to-easing), a down call requires a LIVE negative input. A stale multi-horizon relative lag (1w/1m) is a structural descriptor, not a same-day tape signal — it must not be scored into S2 or S4. The absence of a positive cushion (09-08 override not firing) is NOT the presence of a negative signal; 'no cushion ≠ headwind.' If the only negatives are stale objects, the correct call is flat (or flat/mild with the sign set by the live tape), not down. Additionally: a macro object explicitly identified as already-priced (T+2) must be scored 0, not as a live headwind — flagging a double-count and then committing it is not a mitigation."
falsifier: "If a future session has S0 = 0 (mixed macro), a live positive tape (green futures ≥ +0.5%, oil offered, live curve flat-to-easing), and a stale multi-horizon relative lag, and the sector ETF nonetheless closes DOWN with a negative relative print (rel ≤ −0.3%), then the corrected rule is wrong and the stale lag does carry same-day predictive weight. Conversely, if the sector closes flat-to-up with rel within ±0.2% (as today), the corrected rule is confirmed."
current_behavior: "The model wrote 'a mild lag, not a cushion and not a smash' for the 1d rel −0.23%, then scored S2 = −0.5 and S4 = −0.5 anyway. It also admitted the Warsh hawkish backdrop was 'already printed (T+2), not a fresh same-morning shock' and then scored it as a live headwind in S0. Net −1.0 × 0.9 = −0.9 → down/flat. Actual: XLRE +0.859%, SPY +0.852%, rel +0.007% — pure beta, no idiosyncratic move."
evidence_cited: "Morning note: S2 = −0.5 on '1d rel −0.23% — a mild lag, not a cushion and not a smash'; S4 = −0.5 on the same stale lag; S0 = 0 with the hawkish backdrop admitted as T+2. Outcome: XLRE +0.859% vs SPY +0.852%, rel +0.007% — the sector tracked the index to within 1 bp, confirming zero idiosyncratic driver and a pure-beta session. The note's own Channel 1 read said the live curve was easing and futures were green; the note's own HORIZON_3D said flat/mild; the deterministic pipeline said down/flat. Three-way disagreement with divergence_flagged: False."
error_category: "A"
scope: "general"
date: "2026-09-11"
status: "active"
occurrences: "1"
promoted_on: "2026-09-11"
sources: "['2026-09-11_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
When S0 is genuinely 0 (mixed macro) and the live, knowable-at-open tape is positive (green futures ≥ +0.5%, oil offered, live curve flat-to-easing), a down call requires a LIVE negative input. A stale multi-horizon relative lag (1w/1m) is a structural descriptor, not a same-day tape signal — it must not be scored into S2 or S4. The absence of a positive cushion (09-08 override not firing) is NOT the presence of a negative signal; "no cushion ≠ headwind." If the only negatives are stale objects, the correct call is flat (or flat/mild with the sign set by the live tape), not down. Additionally: a macro object explicitly identified as already-priced (T+2) must be scored 0, not as a live headwind — flagging a double-count and then committing it is not a mitigation.

## WHEN IT FIRES
A rate-sensitive bond-proxy sector (XLRE/XLU/XLP) enters a session with a genuinely mixed S0 (live curve flat-to-easing, futures green, oil offered) but a stale multi-horizon relative lag (1w/1m rel negative) and a stale hawkish macro backdrop already priced (T+2). The model scores the stale lag into BOTH S2 (breadth) and S4 (tape) as negative, producing a net negative score and a down call, while the live, knowable-at-open tape is positive.

## WRONG IF
If a future session has S0 = 0 (mixed macro), a live positive tape (green futures ≥ +0.5%, oil offered, live curve flat-to-easing), and a stale multi-horizon relative lag, and the sector ETF nonetheless closes DOWN with a negative relative print (rel ≤ −0.3%), then the corrected rule is wrong and the stale lag does carry same-day predictive weight. Conversely, if the sector closes flat-to-up with rel within ±0.2% (as today), the corrected rule is confirmed.

## EVIDENCE
Morning note: S2 = −0.5 on "1d rel −0.23% — a mild lag, not a cushion and not a smash"; S4 = −0.5 on the same stale lag; S0 = 0 with the hawkish backdrop admitted as T+2. Outcome: XLRE +0.859% vs SPY +0.852%, rel +0.007% — the sector tracked the index to within 1 bp, confirming zero idiosyncratic driver and a pure-beta session. The note's own Channel 1 read said the live curve was easing and futures were green; the note's own HORIZON_3D said flat/mild; the deterministic pipeline said down/flat. Three-way disagreement with divergence_flagged: False.

(learn_cycle promote)
