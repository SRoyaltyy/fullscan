# Sector Reflect — Real Estate — 2026-08-25

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A rate-sensitive sector call uses a yield-change table (TIPS 10Y, nominal 10Y/30Y) labeled through the prior close, and treats those 1d changes as the live open tape. The existing live-rate-check lesson is cited, but no verification against the actual open/premarket curve is performed. If the prior close was up and 30Y is at a long-term high, the model emits down even when the live open is falling for a second day. The resulting S0/S1 spine is inverted, and the wrong direction is forced despite a positive relative tape.

CURRENT_BEHAVIOR: (1) Reads “DFII10 2.40, +0.05 1d; 10Y 4.74, +0.05 1d; 30Y 5.27, +0.04 1d” as today’s rising rate tape. (2) Scores S0 -1 and S1 -1 citing “real yields rising” and “30Y at 19-year high” as negative spine hits. (3) Cites active lesson 08-17 as if it were satisfied (“yields rising at open, so do not force up”). (4) Overrides the positive 1d/3d/1w XLRE relative bid, treating it only as a magnitude cap, and emits down/mild.

CORRECTED_BEHAVIOR: Before scoring S0/S1 for any rate-sensitive sector, verify the actual open/premarket yield snapshot and today’s 1d changes (current 10Y/30Y/TIPS levels; overnight/official open quotes). If the source is dated through the prior close, do not use it as “today.” On 2026-08-25, the live open had 10Y -7 bps to 4.63% and 30Y -6 bps to 5.17%, so the rate spine should have been positive/neutral, not negative. Do not force a down call from 30Y at a high when the live change is falling; the high level caps the size of relief but does not reverse a falling live tape. If a live check is not possible, reduce confidence or widen to flat/neutral rather than emitting a confident negative spine.

EVIDENCE: Prediction was down/mild with S0=-1/S1=-1 based on “real yields RISING today (+0.05 1d), 10Y RISING, 30Y at 5.27%.” Actual: 10Y fell ~7 bps to 4.63%, 30Y fell ~6 bps to 5.17%; XLRE +0.07% (flat), SPY +0.32%, REL -0.25%. The post-session review states the morning rate data was stale/wrong, that the falling-yield tape was knowable at open, and that a live rate check would have flipped the direction call. S4 (+1) was the only score on the right side.

LESSON_MATCH_CHECK: Directly matches active lesson 08-17 “live-rate check” — this is the exact failure mode that lesson was written to prevent. Also matches the 08-21 candidate pattern: a stale rate/macro read is given full weight while a live, knowable signal points the other way. It is not primarily a scoreboard None/None pipeline issue, although the scoreboard line contains formatting/extraction noise.

BACKWARD_CHECK: Applied to 2026-08-17 (predicted up/notable, actual -0.97): a real live-rate check would have caught rising yields and prevented the up call. Applied to 2026-08-25: it flips down/mild to flat/up/neutral, matching the actual flat/up close. Applied to 2026-08-18 (down/mild, actual -0.446): no harm, because live rates were actually negative that day. No previously correct call is invalidated.

CONFLICT_CHECK: No conflict with 08-18 (relative bid is a magnitude cap) — if the live rate tape is positive, the relative bid can support flat/up but still keep magnitude mild. No conflict with 08-21 level-vs-change — that lesson says a yield drop is not relief while 30Y is at a high, but it still requires an actual yield drop; it does not authorize treating a stale prior-close up-move as the live drop. No conflict with 08-12 two-sided PCE, because a scheduled two-sided event does not override a known live falling-yield tape.

FALSIFIER: If a future run verifies live falling yields and a rate-sensitive sector still falls outright due to a dominant idiosyncratic rotation (e.g., data-center REITs selling off with tech) while SPY rises, then “live rates control absolute direction” would need a rotation overlay. 2026-08-25 is not that falsifier: the sector was flat, not down, and the rotation only capped upside.

DIVERGENCE_VERDICT: futures_right

ACTIVE_LESSON_REVIEW: 08-17 must be strengthened with an actual data-freshness gate: citing the lesson is insufficient; check live yield levels before S0/S1. 08-21 was directionally relevant but applied to the wrong/stale live change. 08-18 held up: the relative bid capped magnitude at mild/flat. 08-12 PCE/two-sided was correctly noted but did not cause the error. 08-11 oil/geopolitical correctly did not fire. 08-14 band-vs-multiplier calculation was deterministic and not the error. The recent candidate lessons about predicted None/None scoreboard entries are a separate pipeline/extraction issue; the scoreboard line for 2026-08-25 showing None/True while the PREDICT block and outcome review conflict should be cleaned separately and should not be read as validating the down call.

SECTOR: Real Estate
LESSON_END
