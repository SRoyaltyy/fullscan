---
trigger_pattern: "When a sector's own ETF tape is decisively negative across all timeframes (1d/3d/1w/1m relative all negative) AND there is a fresh knowable-at-open single-name negative (e.g., strike authorization on a top-weight name), a positive broad-market futures bounce (ES/NQ/Asia/Europe all up) does NOT automatically rescue the sector. The 08-21 reversal checklist (positive futures → avoid down call) must be weighed against the sector's own tape; when S4=-1 is decisive and S1 contains a fresh negative, the relative call should be down:mild, not flat."
corrected_behavior: "When S4=-1 (decisive negative tape across all timeframes) AND S1 contains a fresh knowable-at-open negative (not just carried/structural positives), the positive-futures bounce should be treated as a SPY-level phenomenon that may not transmit to the laggard sector. The reversal checklist should only cap the call at flat when the sector's own tape is neutral or mixed; when the sector tape is decisively negative and there is a fresh negative catalyst, predict down:mild (or at minimum down:flat) rather than flat. The futures bounce lifts SPY, not necessarily the laggard sector."
falsifier: "A future case where S4=-1 (all timeframes negative relative), a fresh single-name negative exists, but the sector ETF closes positive on a broad risk-on day (SPY up >0.5%, XLI up >0.3%) would falsify this lesson. Also falsified if a down:mild call based on this lesson misses when the sector closes flat or up."
current_behavior: "The model applies the 08-21 reversal checklist (positive futures bounce argues against a down call) as a cap that overrides a decisively negative sector tape (S4=-1) and a fresh single-name negative (Boeing SPEEA strike authorization). The call is set to flat with a 'mild negative bias' even when the tape, breadth, and a fresh negative all point to down:mild."
evidence_cited: "2026-08-25 Industrials: Morning predicted flat/flat with S4=-1 (tape 1d rel -0.40%, 3d -0.89%, 1w -2.74%, 1m -5.32%) and S1=+1 (capped, with Boeing SPEEA strike authorization noted as fresh negative). Actual: XLI -0.335%, SPY +0.320%, rel -0.655%. The futures bounce lifted SPY but XLI failed to participate — exactly the laggard pattern the morning flagged but did not act on. The 08-21 reversal checklist (positive futures) capped the call at flat, overriding the tape."
error_category: "B"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
When S4=-1 (decisive negative tape across all timeframes) AND S1 contains a fresh knowable-at-open negative (not just carried/structural positives), the positive-futures bounce should be treated as a SPY-level phenomenon that may not transmit to the laggard sector. The reversal checklist should only cap the call at flat when the sector's own tape is neutral or mixed; when the sector tape is decisively negative and there is a fresh negative catalyst, predict down:mild (or at minimum down:flat) rather than flat. The futures bounce lifts SPY, not necessarily the laggard sector.

## WHEN IT FIRES
When a sector's own ETF tape is decisively negative across all timeframes (1d/3d/1w/1m relative all negative) AND there is a fresh knowable-at-open single-name negative (e.g., strike authorization on a top-weight name), a positive broad-market futures bounce (ES/NQ/Asia/Europe all up) does NOT automatically rescue the sector. The 08-21 reversal checklist (positive futures → avoid down call) must be weighed against the sector's own tape; when S4=-1 is decisive and S1 contains a fresh negative, the relative call should be down:mild, not flat.

## WRONG IF
A future case where S4=-1 (all timeframes negative relative), a fresh single-name negative exists, but the sector ETF closes positive on a broad risk-on day (SPY up >0.5%, XLI up >0.3%) would falsify this lesson. Also falsified if a down:mild call based on this lesson misses when the sector closes flat or up.

## EVIDENCE
2026-08-25 Industrials: Morning predicted flat/flat with S4=-1 (tape 1d rel -0.40%, 3d -0.89%, 1w -2.74%, 1m -5.32%) and S1=+1 (capped, with Boeing SPEEA strike authorization noted as fresh negative). Actual: XLI -0.335%, SPY +0.320%, rel -0.655%. The futures bounce lifted SPY but XLI failed to participate — exactly the laggard pattern the morning flagged but did not act on. The 08-21 reversal checklist (positive futures) capped the call at flat, overriding the tape.

(learn_cycle promote)
