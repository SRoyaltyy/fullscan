# Sector Reflect — Energy — 2026-09-24

**TRIAGE:** Neither a reasoning miss nor a tool/data miss decided the graded axes. Predicted **up/mild**; actual XLE **+0.37%** vs SPY **−0.08%** (rel **+0.45%**) → direction **HIT**, magnitude **HIT** (mild = 0.3–1.0%). Overlay used live oil (CL/BZ green, WTI ~$92.71 then settle ~$94.61 / +2.7%) and emitted the signed up call. The engine `tape_anchor` still carried stale Finviz oil (CL **−1.59%**, QA **−1.02%**); that is an 08-11-class input bug in the **anchor**, not in the LLM card. Overlay rescued direction. Gap-and-fade (open 63.07 → close 62.60, ~−0.75% open-to-close) and the EIA calendar error did not flip the EOD band.

**CHECK 1 — LESSON MATCH:** No new miss pattern. Closest existing rules all **match and were applied**, not retrieval failures: **08-11** live-oil verify (LLM yes / engine no); **08-14** green-oil + live supply-risk → don’t cap S1; **08-10** sector-shock / mixed on a red tape; **09-15** notable license withheld (rebound, not confirmed ≥2% global-supply outage); **09-18** mild band (|PM| ~1.1%, oil sub-2.5%, 1m not crowded); **09-17** leftover-S4 not the thesis; **09-23** rotation-bid (today inverted: energy the destination). The weak oil→XLE translation looks like **08-12**, but 08-12’s trigger (1w rel **> +4%** priced-in) **does not fire** (1w rel **−4.09%**). Do not mint a duplicate.

**CHECK 2 — BACKWARD TEST:** No score-changing correction. Keeping **up** + **mild** on a ~2% live barrel, board-leading PM, risk-off/yields cap, products not confirming, no physical outage would have **helped** 09-16/18/21/22 (fade/mild hits) and **today**. It would have **hurt** 09-15 (up/mild vs **+2.17%** notable) — that day is why 09-15’s outage license exists and why it stayed **PARTIAL** here. A new “cap S1 at +1 on any rebound” rule would be one-day overfitting against 08-14.

**CHECK 3 — CONFLICT SCAN:** None if we do **not** write “S1=+1 whenever oil is only ~2%.” That would collide with **08-14** (oil green >1.8% + live supply-risk → spine dominates). 08-12 vs 08-14 stays resolved by **freshness/positioning**: 1w rel was a bleed, not a crowded run, so 08-14 controlled S1 sign; 09-15/09-18 controlled **band**. Hygiene (don’t triple-count the same PM rotation in S0/S2/S4; don’t treat an unheld gap as a passed extension test) does not change those triggers.

**CHECK 4 — APPLIED-LESSON REVIEW:**
- **08-11 live-oil:** applied in overlay → **helped** (wrong-sign S1 avoided). Engine oil legs **failed** the same rule.
- **08-14:** applied → **helped** direction; did **not** force notable.
- **09-15 / 09-18 / mag hit-rate <0.4:** applied → **helped** (close **not** >2%; falsifier 2 dead).
- **08-10 sector_shock / S0 +0.5 not +1:** applied → **helped** (10Y 5.11%→5.20% capped absolute XLE).
- **09-23 / 09-21 rotation:** applied with sign flipped to destination → **helped** relative +0.45%.
- **09-17 leftover-S4:** applied as “PM is the live test.” Open PM **+1.11%** was green; it **did not hold**. Leftover 1d rel still confirmed. **Mixed**, call intact.
- **09-03 emit-signed:** applied → **helped**.
- **09-10 / 09-14 / 09-11 / 09-04:** correctly **did not fire**.
- Open Energy DO-INSTEAD (keep direction, shrink confidence): applied (llm conf **0.52**, mult **0.9**) → **helped**. Pipeline conf 0.771 was higher than the card; the modest close vindicates the **card’s** shrink, not the engine’s lift.

**CHECK 5 — FALSIFIER:** If this same open (live oil ~+2%, two-sided Iran/Hormuz, XLE board-leading PM ~+1%, no confirmed outage, products mixed, risk-off/yields up) prints XLE **down** on the close, the signed-up call is wrong. If it prints **>2%**, the 09-15 mild cap is too tight. Neither happened. EIA-as-Thursday-10:30 was a **mis-specified** falsifier (WPSR already printed **09-23**, crude **+2.969 Mb**); it is not today’s driver.

**KNOWABLE_AT_OPEN:** partially — sign and mild cap yes; sub-0.5% after a +1.11% gap and midday oil/yield air-pocket no. Intraday diplomacy/Houthi path is not an A/B miss.

**DIVERGENCE:** morning `divergence_flagged: False`. Live S1 and PM agreed up; close agreed up. Stale engine oil vs PM was **not** the overlay’s divergence flag.

**Verdict:** Full hit. **ERROR_CATEGORY NONE.** No new Energy lesson. Process notes only: engine oil legs still stale; S0/S2/S4 were one rotation tape; PM failed as **held** extension; WPSR is Wednesday.

LESSON_BEGIN
ERROR_CATEGORY: NONE
TRIGGER_PATTERN: Energy/XLE with a live-verified green barrel (~2%, not a confirmed ≥2%-of-global-supply outage), two-sided Iran/Hormuz headlines, products not confirming, and the sector as the premarket rotation destination on a risk-off/yield-up tape — emit up and cap magnitude at mild unless oil >5%, sector PM >2%, or a confirmed physical outage is live.
CURRENT_BEHAVIOR: Overlay rejected stale Finviz oil, signed S1 from live CL/BZ/WTI up, fused crude+geo once, scored S0 +0.5 (rotation into energy, yields as cap), S2 +0.5 without constituent confirmation, S4 +1 as confirmation, withheld notable, confidence 0.52 / mult 0.9, regime mixed. Engine tape_anchor still used CL −1.59% / QA −1.02%.
CORRECTED_BEHAVIOR: No score change. Keep live-oil verify in the overlay; keep the mild cap on rebound-plus-premium inside risk-off. Do not treat S0 rotation + S2 board-leader + S4 PM as three independent confirms of one print. Do not treat a green PM gap as a passed extension test until it holds. Do not schedule EIA WPSR as a Thursday 10:30 binary — the weekly print is Wednesday. Fix engine oil legs so the anchor uses the same live sign as the overlay.
EVIDENCE: 2026-09-24 predicted up/mild vs XLE +0.3688% (SPY −0.0821%, rel +0.4508%); direction_hit True, magnitude_hit True. WTI Nov ~$94.61 +2.66%; 10Y 5.11%→5.20%; open 63.07→close 62.60 gap-and-fade; EIA crude +2.969 Mb dated 09-23 not 09-24.
LESSON_MATCH_CHECK: no new match — confirms 08-11 (overlay applied, engine did not), 08-14, 08-10, 09-15, 09-18, 09-23 inverted rotation; 08-12 trigger not met (1w rel −4.09%, not >+4%)
BACKWARD_CHECK: confirming mild-up on this setup helped 09-16/18/21/22 and today; would have hurt 09-15’s notable undercall, which is why the physical-outage license stays the escalator rather than a blanket S1 cut
CONFLICT_CHECK: none — a new “cap S1 at +1 on ~2% rebounds” rule would conflict with 08-14; 08-12 vs 08-14 remains split by priced-in 1w rel vs live unpriced spine
FALSIFIER: Same open (live oil ~+2%, two-sided geo, board-leading PM ~+1%, no confirmed outage, mixed products, yields up) with XLE closing down would kill the signed-up call; closing >2% would kill the mild cap / 09-15 withhold
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 08-11 helped in overlay, failed in tape_anchor; 08-14 helped direction; 09-15/09-18 helped band; 08-10 helped S0 mute; 09-23/09-21 rotation helped with sign flipped to destination; 09-17 mixed (PM faded); 09-10/09-14/09-11/09-04 correctly idle; Energy keep-direction/shrink-confidence helped. No active Energy lesson to retire.
SECTOR: Energy
LESSON_END
