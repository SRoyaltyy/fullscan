---
trigger_pattern: "A Financials call scores an extended multi-week win streak and strong 3d/1w relative tape as rotation/IB support in S1, S2, and S3 at once, while S4 1d relative tape is already flat/neutral, and a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES. The book still emits absolute up from that streak, treats a prior-session PCE print as both still-pending and in-line relief, and does not treat AI re-acceleration as rotation-reversal risk to the crowded financials bid."
corrected_behavior: "When a non-holdings mega-cap AI/tech print is already public overnight and NQ leads ES, that is the live flow regime for Financials — the inverse of the tech-risk-off rotation-into-banks license. Do not triple-count one 3d/1w streak across S1/S2/S3. If S4 is flat/neutral, emit absolute flat (mild), not up, even if divergence_flagged is False. Calendar-check macro prints: never score the same release as still-pending and already-relief; if it printed prior session, it is stale unless the next session’s tape confirms transmission. Keep the 08-21 green-futures rule as a ban on a down call, not a license for up."
falsifier: "If this trigger recurs (overnight NVDA-class AI beat already public, NQ leading ES, XLF 1d rel flat, extended multi-week streak) and XLF still closes up absolutely on 2 of the next 3 such sessions, the flat-not-up correction is wrong and must be revised. Relative lag alone does not falsify; a forced absolute down call is also not the claim."
current_behavior: "S4 is scored 0.0 and the standing “don’t convert structure into an absolute up call” lesson is cited only to cap magnitude at mild. Direction stays up. The same 3d/1w RS is counted in S1, S2, and S3. “AI trade unravels / rotation into financials” remains an S1 high HIT. PCE is both tomorrow’s 8:30 binary and already-benign S0 +0.5. Green futures and easing long-end from the prior close keep S0 constructive."
evidence_cited: "2026-08-27 XLF −0.65% vs SPY +0.66% (rel −1.31%); predicted up/mild; direction miss, magnitude hit. NVDA ~+8.7%, CRM ~+22.6%; only technology advanced of 11 S&P sectors. Morning S0 0.5 / S1 1.0 / S2 0.5 / S3 0.5 / S4 0.0 × 0.9 → 2.25 up/mild (pipeline 5.175, same band). PCE was Wednesday (headline 0.1 ppt hot, core in-line); Thursday was Jackson Hole + NVDA, not a financials NIM day. 1d rel at open −0.11% (flat); 3d/1w rel +1.31%/+1.74% was the streak that reversed."
error_category: "B"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
When a non-holdings mega-cap AI/tech print is already public overnight and NQ leads ES, that is the live flow regime for Financials — the inverse of the tech-risk-off rotation-into-banks license. Do not triple-count one 3d/1w streak across S1/S2/S3. If S4 is flat/neutral, emit absolute flat (mild), not up, even if divergence_flagged is False. Calendar-check macro prints: never score the same release as still-pending and already-relief; if it printed prior session, it is stale unless the next session’s tape confirms transmission. Keep the 08-21 green-futures rule as a ban on a down call, not a license for up.

## WHEN IT FIRES
A Financials call scores an extended multi-week win streak and strong 3d/1w relative tape as rotation/IB support in S1, S2, and S3 at once, while S4 1d relative tape is already flat/neutral, and a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES. The book still emits absolute up from that streak, treats a prior-session PCE print as both still-pending and in-line relief, and does not treat AI re-acceleration as rotation-reversal risk to the crowded financials bid.

## WRONG IF
If this trigger recurs (overnight NVDA-class AI beat already public, NQ leading ES, XLF 1d rel flat, extended multi-week streak) and XLF still closes up absolutely on 2 of the next 3 such sessions, the flat-not-up correction is wrong and must be revised. Relative lag alone does not falsify; a forced absolute down call is also not the claim.

## EVIDENCE
2026-08-27 XLF −0.65% vs SPY +0.66% (rel −1.31%); predicted up/mild; direction miss, magnitude hit. NVDA ~+8.7%, CRM ~+22.6%; only technology advanced of 11 S&P sectors. Morning S0 0.5 / S1 1.0 / S2 0.5 / S3 0.5 / S4 0.0 × 0.9 → 2.25 up/mild (pipeline 5.175, same band). PCE was Wednesday (headline 0.1 ppt hot, core in-line); Thursday was Jackson Hole + NVDA, not a financials NIM day. 1d rel at open −0.11% (flat); 3d/1w rel +1.31%/+1.74% was the streak that reversed.

(learn_cycle promote)
