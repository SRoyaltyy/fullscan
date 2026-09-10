---
trigger_pattern: "A rate-sensitive bond-proxy sector (XLU/XLRE) has a positive medium-term relative tape (3d/1w/1m rel all positive) and the model cites the 08-18 'risk-off + rising long end → relative beat' frame to allow relative resilience, while VIX is shallow (<20, backwardated) and a live long-end Treasury auction sits on the calendar — the sector then lags SPY on the day."
corrected_behavior: "Gate the 08-18 relative-beat frame on a DEEP risk-off bid: require VIX ≥ ~20 or an explicit flight-to-quality impulse before allowing a relative-beat claim for a bond proxy against a rising long end. With VIX <20 and backwardated, treat a rising long end as a relative-lag signal for XLU, not a relative-cushion signal. Additionally, when the 10Y/30Y are in multi-decade stress zones and a long-end auction is on the calendar, score the auction as a live S1 input rather than classifying it as 'a supply event, not a scored binary.' Do not let a positive medium-term relative tape (3d/1w/1m) soften a negative 1d tape into a relative-beat claim."
falsifier: "If on a future session with VIX <20 and backwardated, a rising long end, and a live long-end auction, XLU (or XLRE) beats SPY on the day, this gate is wrong and the 08-18 frame does not require a deep-risk-off precondition. Conversely, if XLU lags SPY on such a session even with VIX ≥20, the gate is insufficient and the binding constraint is the auction/long-end level alone, not the VIX depth."
current_behavior: "The morning note wrote 'XLU can outperform SPY relatively on a defensive bid while falling in absolute terms,' importing the 08-18 frame without its precondition. The scores themselves were correct (S0 −1, S1 −1, S2 0, S3 0, S4 −0.5 → down/mild), but the prose overlay added a relative-beat claim the scores did not support (S4 was already negative on the tape). The 08-13 rule ('S2/S4 confirmation only') was cited and then partially violated."
evidence_cited: "Actual XLU −0.978% vs SPY −0.599%, rel −0.379% — XLU lagged, not beat. 30Y auction Sep 10 2026 priced at high yield 5.308%, bid-to-cover 2.61 (sofrrate.com). VIX 16.51, backwardated (VIX/VIX3M 1.079). Morning note's own S4 = −0.5 was already negative on the tape, contradicting the prose relative-resilience claim."
error_category: "C"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
Gate the 08-18 relative-beat frame on a DEEP risk-off bid: require VIX ≥ ~20 or an explicit flight-to-quality impulse before allowing a relative-beat claim for a bond proxy against a rising long end. With VIX <20 and backwardated, treat a rising long end as a relative-lag signal for XLU, not a relative-cushion signal. Additionally, when the 10Y/30Y are in multi-decade stress zones and a long-end auction is on the calendar, score the auction as a live S1 input rather than classifying it as "a supply event, not a scored binary." Do not let a positive medium-term relative tape (3d/1w/1m) soften a negative 1d tape into a relative-beat claim.

## WHEN IT FIRES
A rate-sensitive bond-proxy sector (XLU/XLRE) has a positive medium-term relative tape (3d/1w/1m rel all positive) and the model cites the 08-18 "risk-off + rising long end → relative beat" frame to allow relative resilience, while VIX is shallow (<20, backwardated) and a live long-end Treasury auction sits on the calendar — the sector then lags SPY on the day.

## WRONG IF
If on a future session with VIX <20 and backwardated, a rising long end, and a live long-end auction, XLU (or XLRE) beats SPY on the day, this gate is wrong and the 08-18 frame does not require a deep-risk-off precondition. Conversely, if XLU lags SPY on such a session even with VIX ≥20, the gate is insufficient and the binding constraint is the auction/long-end level alone, not the VIX depth.

## EVIDENCE
Actual XLU −0.978% vs SPY −0.599%, rel −0.379% — XLU lagged, not beat. 30Y auction Sep 10 2026 priced at high yield 5.308%, bid-to-cover 2.61 (sofrrate.com). VIX 16.51, backwardated (VIX/VIX3M 1.079). Morning note's own S4 = −0.5 was already negative on the tape, contradicting the prose relative-resilience claim.

(learn_cycle promote)
