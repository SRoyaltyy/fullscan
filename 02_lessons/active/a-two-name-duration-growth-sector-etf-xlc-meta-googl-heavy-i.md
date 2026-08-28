---
trigger_pattern: "A two-name duration/growth sector ETF (XLC: META+GOOGL heavy) is scored S0=+1 because ES/NQ are green and real yields are easing, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK spillover) and the sector’s own 1d relative tape is already negative with mixed-to-flat premarket leaders. The 08-21 green-futures rule is used as a license to call up rather than as a ban on keeping S0 negative. Pipeline emits up/flat; the sector lags a narrow tech rally."
corrected_behavior: "Green ES/NQ does not equal XLC participation when the futures impulse is a print in names XLC does not hold. Keep S0 at 0 in that case (08-21 still forbids flipping S0 to −1 on stale macro alone). If 1d XLC vs SPY is already negative and META/GOOGL are mixed-to-flat, do not let S0=+1 set direction to up — emit flat or down/mild. Do not double-count the same NQ print as both shared risk-on and sector leadership. Refresh legal state to the latest knowable resolution (settlement vs ongoing trial) even if it is not a same-morning severe shock."
falsifier: "If NVDA or another non-XLC mega prints after the close, NQ is green, XLC 1d rel is negative, META/GOOGL are mixed-to-flat premarket, S0 is held at 0, and XLC still closes up with or ahead of SPY on that session, the “don’t map foreign NQ beta into XLC S0=+1” rule is too strong and should be relaxed to a magnitude cap only. Also weakened if two such mornings produce XLC up/mild or better after the corrected flat/down-mild call."
current_behavior: "Treat green NQ as proof that “mega-cap growth participates,” score S0=+1, leave S1–S4 at 0, and emit up/flat. The 1d rel lag is used only as a magnitude cap. Legal state is left on a stale “trial week two” frame even when a prior-session settlement was knowable. The note’s own “XLK spillover only, not a comms spine” line is discarded."
evidence_cited: "2026-08-27 predicted up/flat (S0=+1, S1–S4=0, total 1.8, conf 0.5); actual XLC −1.07% vs SPY +0.66%, rel −1.72% (down/notable). Direction MISS, magnitude MISS. NVDA ~+9% after the 08-26 after-close print lifted Nasdaq ~+1.6% while META faded −0.87% after the 08-26 settlement bounce. Morning had ES +0.31%/NQ +0.55%, 1d XLC rel −0.53%, and an explicit XLK-only isolation, then contradicted it. Settlement ($16.7B / $10B Q3 accrual / YouTube contingent) was knowable at the open and absent from the note."
error_category: "A"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
Green ES/NQ does not equal XLC participation when the futures impulse is a print in names XLC does not hold. Keep S0 at 0 in that case (08-21 still forbids flipping S0 to −1 on stale macro alone). If 1d XLC vs SPY is already negative and META/GOOGL are mixed-to-flat, do not let S0=+1 set direction to up — emit flat or down/mild. Do not double-count the same NQ print as both shared risk-on and sector leadership. Refresh legal state to the latest knowable resolution (settlement vs ongoing trial) even if it is not a same-morning severe shock.

## WHEN IT FIRES
A two-name duration/growth sector ETF (XLC: META+GOOGL heavy) is scored S0=+1 because ES/NQ are green and real yields are easing, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK spillover) and the sector’s own 1d relative tape is already negative with mixed-to-flat premarket leaders. The 08-21 green-futures rule is used as a license to call up rather than as a ban on keeping S0 negative. Pipeline emits up/flat; the sector lags a narrow tech rally.

## WRONG IF
If NVDA or another non-XLC mega prints after the close, NQ is green, XLC 1d rel is negative, META/GOOGL are mixed-to-flat premarket, S0 is held at 0, and XLC still closes up with or ahead of SPY on that session, the “don’t map foreign NQ beta into XLC S0=+1” rule is too strong and should be relaxed to a magnitude cap only. Also weakened if two such mornings produce XLC up/mild or better after the corrected flat/down-mild call.

## EVIDENCE
2026-08-27 predicted up/flat (S0=+1, S1–S4=0, total 1.8, conf 0.5); actual XLC −1.07% vs SPY +0.66%, rel −1.72% (down/notable). Direction MISS, magnitude MISS. NVDA ~+9% after the 08-26 after-close print lifted Nasdaq ~+1.6% while META faded −0.87% after the 08-26 settlement bounce. Morning had ES +0.31%/NQ +0.55%, 1d XLC rel −0.53%, and an explicit XLK-only isolation, then contradicted it. Settlement ($16.7B / $10B Q3 accrual / YouTube contingent) was knowable at the open and absent from the note.

(learn_cycle promote)
