---
trigger_pattern: "A cyclical industrials ETF (XLI-like) posts an all-zero S0–S4 card on the session AFTER a paid FOMC/SEP, with 1w/1m relative lag forbidding up, oil offered (no live squeeze), operator-tape futures only modestly green / not unanimous ≥+0.5% across ES/NQ/RTY/DJIA, tech-led PM (growth ETF >> sector PM), and a large overnight ES-vs-cash sleeve in tape_anchor. The ETF gaps with the index then fades; close-to-close finishes a tiny positive inside the flat magnitude band, which the grader can still mark a direction miss vs predicted flat."
corrected_behavior: "No signed-call change. Keep flat/flat on an unsigned post-event industrials card. Do not promote the overnight ES gap into up, and do not promote the post-close relative lag into down — both were path, not pre-open factors. A ~0.2% faded-gap print that misses the flat/up direction threshold is banding noise, not a mandate to lift direction."
falsifier: "Keep-flat is wrong if, on this same unsigned post-paid-FOMC XLI card (08-27 lag on, oil offered, tech-led PM, no four-index confirm), XLI *holds* a ≥mild close-to-close gain with industrials-led breadth or four-index confirmation rather than fading a leftover gap. Today’s +0.178% fade does not falsify the flatten."
current_behavior: "Emit official close-to-close flat/flat. Do not re-derive S0 from the overnight ES sleeve vs the live operator panel. Apply 08-27 forbid-up and RS veto. Do not map oil-down onto trucking as S1 relief. Leave 8:30 spine prints unscored. Shrink confidence on a modest unsigned |score|. Treat still-negative 3d/1w/1m tape as a flatten, not a down call."
evidence_cited: "Predicted flat/flat (S0–S4 all 0; leading_sum 0; RS veto + calendar size-gate on; tape_anchor 4.416 from ES +1.71% / PM:XLI +0.43% unused as a re-derive). Actual XLI +0.178% / SPY +1.134% / rel −0.956%; open 171.51 → close 169.01 (gap-and-fade). Scoreboard dir MISS / mag HIT. Outcome autopsy: S1–S4 HIT at the open; S0=0 right for XLI (tech-led beta XLI did not keep); Philly 37.8 beat-but-slowed; starts 1.275M construction drag carried; GEV/CAT did not pin the ETF. Memory index unavailable this run; used injected card + outcome + scoreboard only."
error_category: "D"
scope: "ops"
date: "2026-09-17"
status: "active"
occurrences: "2"
promoted_on: "2026-09-17"
sources: "['2026-09-17_sector_industrials_lesson.md', '2026-09-17_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
No signed-call change. Keep flat/flat on an unsigned post-event industrials card. Do not promote the overnight ES gap into up, and do not promote the post-close relative lag into down — both were path, not pre-open factors. A ~0.2% faded-gap print that misses the flat/up direction threshold is banding noise, not a mandate to lift direction.

## WHEN IT FIRES
A cyclical industrials ETF (XLI-like) posts an all-zero S0–S4 card on the session AFTER a paid FOMC/SEP, with 1w/1m relative lag forbidding up, oil offered (no live squeeze), operator-tape futures only modestly green / not unanimous ≥+0.5% across ES/NQ/RTY/DJIA, tech-led PM (growth ETF >> sector PM), and a large overnight ES-vs-cash sleeve in tape_anchor. The ETF gaps with the index then fades; close-to-close finishes a tiny positive inside the flat magnitude band, which the grader can still mark a direction miss vs predicted flat.

## WRONG IF
Keep-flat is wrong if, on this same unsigned post-paid-FOMC XLI card (08-27 lag on, oil offered, tech-led PM, no four-index confirm), XLI *holds* a ≥mild close-to-close gain with industrials-led breadth or four-index confirmation rather than fading a leftover gap. Today’s +0.178% fade does not falsify the flatten.

## EVIDENCE
Predicted flat/flat (S0–S4 all 0; leading_sum 0; RS veto + calendar size-gate on; tape_anchor 4.416 from ES +1.71% / PM:XLI +0.43% unused as a re-derive). Actual XLI +0.178% / SPY +1.134% / rel −0.956%; open 171.51 → close 169.01 (gap-and-fade). Scoreboard dir MISS / mag HIT. Outcome autopsy: S1–S4 HIT at the open; S0=0 right for XLI (tech-led beta XLI did not keep); Philly 37.8 beat-but-slowed; starts 1.275M construction drag carried; GEV/CAT did not pin the ETF. Memory index unavailable this run; used injected card + outcome + scoreboard only.

(learn_cycle promote)
