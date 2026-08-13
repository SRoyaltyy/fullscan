---
trigger_pattern: "A bond-proxy/rate-sensitive sector has been sold off for multiple sessions by a specific macro driver (rising yields). On the next call, pre-fetched data shows that driver is now easing (1d/1w yields tick down) and the sector ETF already shows 1d relative outperformance vs SPY. The analysis text identifies this as the key new input and may even call it a “leading positive” divergence, but component scores remain at or below neutral and the deterministic output continues to emit down/flat. The miss is amplified when a geopolitical/oil supply-shock risk-off headline is knowable at open, because it adds a defensive bid into the same sector."
corrected_behavior: "When the named prior driver is easing and the 1d tape is already showing relative outperformance, do not continue the prior down call mechanically. Re-score the sector components to reflect the inflection: S0 should not remain negative if the macro driver is easing or if geopolitical risk-off is active; S1 and S4 should be positive when structural factors are intact and the tape is inflecting; S2 should recognize oversold breadth turning; if defensive rotation is underway, S3 should reflect flow reversal. For a defensive bond-proxy ETF, active geopolitical/oil supply-shock risk-off is a positive — not neutral — input."
falsifier: "If on a later occurrence yields tick down and XLU shows 1d relative strength, but the next session has no risk-off/geopolitical driver, equities rally strongly, and XLU still falls or underperforms SPY, then the automatic positive re-score would be wrong. The lesson should therefore require either continued rate relief that persists through the session or an actual defensive/risk-off bid, not just the morning yield tick plus short-term relative tape strength."
current_behavior: "Keeps S0 negative, holds S1/S2/S3/S4 at 0, applies a sub-1.0 multiplier, and emits down/flat on the strength of the prior rate-driven selloff, even though the prior selloff driver is now easing and the tape is already inflecting positive. It fails to translate the analysis text’s “driver easing / leading positive” read into component scores. It also fails to treat an active geopolitical risk-off/oil shock as a positive for defensive bond-proxy utilities."
evidence_cited: "2026-08-10 down/notable was validated by rising-yield pressure. On 2026-08-11, yields were ticking down (10Y 1d -0.04, real yield 1d -0.03), XLU was already +0.43% relative vs SPY on the 1d tape, and a Hormuz/oil risk-off headline was knowable at the open. Actual outcome: XLU +1.16%, SPY -0.32%, relative +1.48%. Morning components S0=-1, S1/S2/S3/S4=0, multiplier=0.9, predicted down/flat — a direction and magnitude miss despite the analysis text correctly naming the inflection."
error_category: "C"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
When the named prior driver is easing and the 1d tape is already showing relative outperformance, do not continue the prior down call mechanically. Re-score the sector components to reflect the inflection: S0 should not remain negative if the macro driver is easing or if geopolitical risk-off is active; S1 and S4 should be positive when structural factors are intact and the tape is inflecting; S2 should recognize oversold breadth turning; if defensive rotation is underway, S3 should reflect flow reversal. For a defensive bond-proxy ETF, active geopolitical/oil supply-shock risk-off is a positive — not neutral — input.

## WHEN IT FIRES
A bond-proxy/rate-sensitive sector has been sold off for multiple sessions by a specific macro driver (rising yields). On the next call, pre-fetched data shows that driver is now easing (1d/1w yields tick down) and the sector ETF already shows 1d relative outperformance vs SPY. The analysis text identifies this as the key new input and may even call it a “leading positive” divergence, but component scores remain at or below neutral and the deterministic output continues to emit down/flat. The miss is amplified when a geopolitical/oil supply-shock risk-off headline is knowable at open, because it adds a defensive bid into the same sector.

## WRONG IF
If on a later occurrence yields tick down and XLU shows 1d relative strength, but the next session has no risk-off/geopolitical driver, equities rally strongly, and XLU still falls or underperforms SPY, then the automatic positive re-score would be wrong. The lesson should therefore require either continued rate relief that persists through the session or an actual defensive/risk-off bid, not just the morning yield tick plus short-term relative tape strength.

## EVIDENCE
2026-08-10 down/notable was validated by rising-yield pressure. On 2026-08-11, yields were ticking down (10Y 1d -0.04, real yield 1d -0.03), XLU was already +0.43% relative vs SPY on the 1d tape, and a Hormuz/oil risk-off headline was knowable at the open. Actual outcome: XLU +1.16%, SPY -0.32%, relative +1.48%. Morning components S0=-1, S1/S2/S3/S4=0, multiplier=0.9, predicted down/flat — a direction and magnitude miss despite the analysis text correctly naming the inflection.

(learn_cycle promote)
