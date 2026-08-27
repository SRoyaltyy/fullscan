---
trigger_pattern: ""
current_behavior: ""
corrected_behavior: ""
evidence_cited: ""
error_category: "NONE"
falsifier: ""
sector: "Utilities"
date: "2026-08-21"
status: "promoted"
---

# Sector Reflection — Utilities — 2026-08-21

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A bond-proxy/defensive sector (Utilities/XLU, REITs, Staples) has an apparent “yields easing” positive driver in S0/S1, but the yield quote is one or more sessions old while a hawkish Fed/rate-hike overhang is live and the sector ETF is already underperforming SPY on the 1d/3d relative tape. The model emits an absolute up/flat call from stale leading factors, and the negative live tape is treated as non-confirmation rather than as an absolute cap.
CURRENT_BEHAVIOR: The model anchors on the pre-fetched 1d/1w easing in the 10Y/30Y/real-yield prints, scores S0/S1 positive on “rates falling,” and allows the pipeline to resolve toward up/flat even when S2 is negative, S4 is non-confirming, and a recently released hawkish Fed Minutes catalyst remains live. It does not verify whether the yield quote is current relative to the trade date, so stale easing data can override a negative market tape.
CORRECTED_BEHAVIOR: Before scoring yield relief for a bond-proxy sector, check the recency of the yield data. If the easing print is stale by one or two sessions, do not treat it as a live leading signal. If the current/close-of-prior day yield is actually elevated or rising, or if the live sector tape is negative (S2 -1, S4 0/negative), score S0/S1 no higher than 0 on yield relief and keep the call flat-to-down rather than up/flat. Weight a fresh hawkish-Fed/rate-hike overhang as a live headwind for long-duration sectors until the current yield/tape confirms relief.
EVIDENCE: Morning inputs showed 10Y 4.65% with 1d -0.06, real yields -0.06, 30Y 5.19% with 1d -0.09, which looked like bond-proxy relief. Actual close: 10Y rose to 4.74% (+5bps), 30Y rose to 5.27%, XLU fell -2.28% while SPY rose +0.41% (rel -2.69%). S2 had already flagged the correct warning: XLU 1d rel -0.58%, 3d rel -0.51%. AEP -2.77% with a Morgan Stanley PT cut compounded the rate-driven weakness. The failure was trusting a stale easing-yield read over the live negative tape and the active hawkish-Fed overhang.
LESSON_MATCH_CHECK: Partially matches active lesson 08-18 — bond-proxy + rising 10Y/long-end yields → default to flat-to-negative absolute, do not upgrade to absolute up. It also matches 08-17 — defensive bid without fresh confirmation is a relative, not absolute, signal. The miss occurred because the model did not see the 08-18 condition as triggered: the stale easing print masked what was actually a rising-yield day. Also aligns with the 2026-08-21 B-category candidate lesson: necessary inputs to avoid an absolute-up call were present but weighted incorrectly.
BACKWARD_CHECK: Applying this corrected behavior to prior XLU scoreboard misses would improve 08-17 (predicted up/notable, actual -0.29%), 08-18
