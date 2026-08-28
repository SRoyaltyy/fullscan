---
trigger_pattern: "A high-beta consumer/cyclical ETF (XLY) is scored S0 positive because ES/NQ are green and the 08-21 reversal checklist is ticked, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK) and the sector’s own 1d/3d/1w relative tape is already negative."
corrected_behavior: "Do not score S0=+1 or invoke 08-21-up when the futures impulse is explicitly a non-XLY holdings earnings print and XLY 1d/3d/1w rel is already negative. Map S0 from consumer-beta participation (AMZN/TSLA/HD), not index futures. Let S4 go negative on that lag; do not keep S4=0 on the same green-futures story. Verify the sector-owned print calendar; a T-1 sticky PCE/confidence print stays in the consumer tape and does not relieve S1. Default to down/mild or down/flat, not up. Do not force notable from unknowable intraday oil/breadth."
falsifier: "If ES/NQ are green solely on a non-holdings mega-cap earnings print, XLY 1d/3d/1w relative is already negative, and XLY still closes up and outperforms SPY by more than ~0.5% with no offsetting consumer-specific positive, this lesson is wrong and must be revised."
current_behavior: "Maps green ES/NQ onto XLY beta (S0=+1), uses 08-21 as a license to call up, holds S4 at 0 because “XLY may bounce on green futures” (same shock counted twice), credits 1m/3m inflows as a 1-day bid, and treats a prior-session PCE core-in-line print as same-day relief of a downside tilt."
evidence_cited: "2026-08-27 pipeline flat/flat vs XLY -1.09% / SPY +0.66% / rel -1.75%. Only S&P tech advanced; XLY worst sector; NVDA +8.7%; AMZN ~-1.5%, HD ~-1.9%, MCD ~-2.4%, TSLA +2.6%. July PCE released Aug 26 (headline +3.7% YoY, real goods -$49.9B), not Aug 27 8:30. Narrative up/mild would have been a worse miss than pipeline flat."
error_category: "B"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Do not score S0=+1 or invoke 08-21-up when the futures impulse is explicitly a non-XLY holdings earnings print and XLY 1d/3d/1w rel is already negative. Map S0 from consumer-beta participation (AMZN/TSLA/HD), not index futures. Let S4 go negative on that lag; do not keep S4=0 on the same green-futures story. Verify the sector-owned print calendar; a T-1 sticky PCE/confidence print stays in the consumer tape and does not relieve S1. Default to down/mild or down/flat, not up. Do not force notable from unknowable intraday oil/breadth.

## WHEN IT FIRES
A high-beta consumer/cyclical ETF (XLY) is scored S0 positive because ES/NQ are green and the 08-21 reversal checklist is ticked, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK) and the sector’s own 1d/3d/1w relative tape is already negative.

## WRONG IF
If ES/NQ are green solely on a non-holdings mega-cap earnings print, XLY 1d/3d/1w relative is already negative, and XLY still closes up and outperforms SPY by more than ~0.5% with no offsetting consumer-specific positive, this lesson is wrong and must be revised.

## EVIDENCE
2026-08-27 pipeline flat/flat vs XLY -1.09% / SPY +0.66% / rel -1.75%. Only S&P tech advanced; XLY worst sector; NVDA +8.7%; AMZN ~-1.5%, HD ~-1.9%, MCD ~-2.4%, TSLA +2.6%. July PCE released Aug 26 (headline +3.7% YoY, real goods -$49.9B), not Aug 27 8:30. Narrative up/mild would have been a worse miss than pipeline flat.

(learn_cycle promote)
