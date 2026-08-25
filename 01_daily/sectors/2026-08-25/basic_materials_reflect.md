# Sector Reflect — Basic Materials — 2026-08-25

LESSON_BEGIN
ERROR_CATEGORY: B  
TRIGGER_PATTERN: Basic Materials / XLB is called up from (a) a broad risk-on macro read that is actually Nasdaq/tech-led (NQ much stronger than ES) and (b) strong copper/gold prices, while XLB’s own 1d relative tape is modest (<0.5%) and the ETF is chemicals-heavy, not a pure metals basket. The model gives full S0/S1 credit to the metals/macro story without applying a composition/transmission discount, so metal price records can coexist with a flat materials equity ETF.

CURRENT_BEHAVIOR: S0 scored +1 for “risk-on” even though premarket leadership was NQ/XLK/tech, a weaker cyclical/materials signal. S1 scored +1 for copper firm/elevated plus gold strength, without haircutting for the fact that XLB’s dominant weights are chemicals (LIN, SHW, APD, ECL) and the metals bid is concentrated in minority miner weights or in a speculative COMEX/LME price premium. Result: predicted up/mild; actual XLB 0.00%, SPY +0.32%, relative −0.32%.

CORRECTED_BEHAVIOR: When the premarket tape is NQ-led/tech-led, do not award full S0 +1 to Basic Materials unless there is evidence of broad cyclical participation or XLB 1d relative strength ≥ roughly +0.5%. When copper/gold are strong, score S1 as at most PARTIAL unless there is transmission to XLB’s actual major weights; explicitly ask whether the metal price move changes the cash-flow/earnings outlook for LIN, SHW, APD, ECL. If transmission is absent and the 1d tape is <0.5%, default toward flat/neutral rather than up/mild.

EVIDENCE: Actual outcome: XLB 0.00%, SPY +0.32%, rel −0.32%. Copper hit record highs on the tariff threat (LME 3m settled ~$14,273/t, traded as high as ~$14,343/t; COMEX ~$6.70, +1.54%) and gold held ~$4,650, yet XLB was flat. XLK +1.06% led the tape; materials lagged. Morning premarket showed mixed/weak major XLB weights (LIN ~−0.3%, NEM ~−0.8%, SHW +1%). The S4 = 0/non-confirming tape was the better signal than the S0/S1 leading positives.

LESSON_MATCH_CHECK: No existing Basic Materials lesson directly covers this exact pattern. Closest adjacent lesson is the 2026-08-21 energy lesson: a commodity price catalyst can fail to lift the sector ETF (oil up / XLE flat). That lesson does not cover the chemicals-heavy composition / tech-led tape transmission gap for XLB, so a new sector lesson is warranted.

BACKWARD_CHECK: The corrected behavior would likely have improved 2026-08-12 (up/severe, actual −1.24%) and 2026-08-17 (up/severe, actual −0.57%) by preventing over-credit from metals/macro alone. It would not necessarily have hurt 2026-08-21 (up/severe, actual +2.14%) because that day had confirmed broad sector transmission and a strong ETF tape; the guard is conditional on missing transmission, not a blanket ban on XLB up calls.

CONFLICT_CHECK: No conflict with the active 8/14 gold-offset lesson: the corrected behavior still counts gold as a partial S1 positive, but does not let it become a full +1 in the absence of ETF transmission. It also does not conflict with the 8/18 metals-co-move floor ban or the 8/21 keep-pipeline-severe rule, since those concern fresh squeeze/severe days rather than a post-run follow-through with a modest 1d tape.

FALSIFIER: This lesson would be falsified if a future Basic Materials day has the same setup — NQ-led risk-on, copper/gold strong, XLB 1d rel <0.5%, chemicals-heavy weights — and XLB nonetheless closes up >0.5% or beats SPY on broad sector participation. In that case the transmission discount is too aggressive and needs to be relaxed.

DIVERGENCE_VERDICT: futures_right — formally `divergence_flagged` was False, but the effective split was leading positives (+2) versus the S4 tape read (0). The tape/tech-led side proved correct; this should have been flagged as a transmission/magnitude caution even though direction did not flip to down.

ACTIVE_LESSON_REVIEW: The morning predict correctly checked the active XLB rules: temper-severe (off), China-miss severe ban (off), metals-co-move floor ban (off), gold-offset (applied), keep-pipeline-severe (off). No active Basic Materials lesson was missed; the gap is a new lesson about tech-led risk-on and the metals-price-to-chemicals-heavy-ETF transmission failure. Scoreboard note: the entry’s `predicted None/None` conflicts with the supplied predict; I used the emitted predict for this diagnostic. If the None/None is literal, that is a separate pipeline/annotation issue, but the substantive call error is a reasoning/weighting error.

SECTOR: Basic Materials
LESSON_END
