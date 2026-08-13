---
trigger_pattern: "A long-duration/rate-sensitive sector (REITs) has been lagging, real/nominal yields tick up into a scheduled CPI print, and premarket equity futures are positive. The model applies an active “CPI imminent => S0 negative for REITs” lesson without treating the CPI surprise as binary. It also sees a lower 2Y pre-CPI but treats it as a minor offset rather than an easing-expectation tell that a cool print could flip the rate spine and rally rate-sensitive assets."
corrected_behavior: "When CPI is imminent for a long-duration/rate-sensitive sector, treat the catalyst as two-sided. Before scoring S0, check the pre-CPI yield-curve positioning: if the 2Y is drifting lower / easing expectations are visible, do not default S0 to -1; score at least 0 and consider positive S0, because a cool/in-line CPI would relieve the duration headwind and can make the sector rally. Reserve negative S0 for pre-CPI curves pricing hot/higher-for-longer outcomes (2Y rising, no easing tell) or for cases where the real-yield trend is strongly rising and no binary macro event is pending. Also avoid double-counting the same “rates rising” spine in both S0 and S1."
falsifier: "This corrected behavior would be wrong if the pre-CPI 2Y easing tell is present but CPI still prints hot, yields rise, and the rate-sensitive sector falls. It would also be falsified if CPI prints cool but REITs fall anyway due to an independent sector-specific shock (e.g., data-center capex cut, refinancing crisis). Therefore the lesson should be scoped to setups where the rate spine is the dominant driver."
current_behavior: "On CPI-imminent days for long-duration/rate-sensitive sectors, S0 is scored negative simply because the print is imminent and the 1m real-yield trend is elevated. S1/S2/S4 then compound the same negative rate spine, producing down/mild. The CPI is treated as one-sided downside risk rather than a two-sided binary catalyst."
evidence_cited: "2026-08-12 predicted XLRE down/mild (S0=-1, S1=-1, S2=-1, S4=-1, total -6.75). Actual: XLRE +0.93%, SPY +0.25%, rel +0.68%. Driver: July CPI printed cooler than expected, easing Fed rate pressure, pushing yields down and rallying rate-sensitive REITs. The morning analysis explicitly noted the 2Y was lower pre-CPI “suggesting some easing expectations,” but still scored S0 negative on the active 08-11 CPI-imminent lesson."
error_category: "B"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
When CPI is imminent for a long-duration/rate-sensitive sector, treat the catalyst as two-sided. Before scoring S0, check the pre-CPI yield-curve positioning: if the 2Y is drifting lower / easing expectations are visible, do not default S0 to -1; score at least 0 and consider positive S0, because a cool/in-line CPI would relieve the duration headwind and can make the sector rally. Reserve negative S0 for pre-CPI curves pricing hot/higher-for-longer outcomes (2Y rising, no easing tell) or for cases where the real-yield trend is strongly rising and no binary macro event is pending. Also avoid double-counting the same “rates rising” spine in both S0 and S1.

## WHEN IT FIRES
A long-duration/rate-sensitive sector (REITs) has been lagging, real/nominal yields tick up into a scheduled CPI print, and premarket equity futures are positive. The model applies an active “CPI imminent => S0 negative for REITs” lesson without treating the CPI surprise as binary. It also sees a lower 2Y pre-CPI but treats it as a minor offset rather than an easing-expectation tell that a cool print could flip the rate spine and rally rate-sensitive assets.

## WRONG IF
This corrected behavior would be wrong if the pre-CPI 2Y easing tell is present but CPI still prints hot, yields rise, and the rate-sensitive sector falls. It would also be falsified if CPI prints cool but REITs fall anyway due to an independent sector-specific shock (e.g., data-center capex cut, refinancing crisis). Therefore the lesson should be scoped to setups where the rate spine is the dominant driver.

## EVIDENCE
2026-08-12 predicted XLRE down/mild (S0=-1, S1=-1, S2=-1, S4=-1, total -6.75). Actual: XLRE +0.93%, SPY +0.25%, rel +0.68%. Driver: July CPI printed cooler than expected, easing Fed rate pressure, pushing yields down and rallying rate-sensitive REITs. The morning analysis explicitly noted the 2Y was lower pre-CPI “suggesting some easing expectations,” but still scored S0 negative on the active 08-11 CPI-imminent lesson.

(learn_cycle promote)
