---
trigger_pattern: "A live macro risk-off shock (oil/geopolitical escalation, Brent >$100, elevated/rising yields, negative yield–equity correlation, VIX backwardation) is present at the open, and the sector's dominant complex is a crowded long (extreme relative-performance/valuation crowding) that shows premarket strength or a fresh single-name positive catalyst. The model scores that premarket strength / fresh catalyst as a positive sector factor (S1 > 0) and treats it as a counterweight that nets the macro shock to flat."
corrected_behavior: "When a live macro risk-off shock coincides with a crowded long-duration complex and a red NQ, premarket strength in that complex must be scored as distribution (negative), not accumulation (positive). S0 and S1 are additive, not offsetting: the premarket rally is the liquidity that lets the crowd exit into the gap-down open. The correct output is down/mild at minimum (arguably down/notable), and the divergence flag must actually propagate to the deterministic output so the conviction cut is real, not cosmetic."
falsifier: "If on a future day with a live oil/yield risk-off shock, a crowded long-duration complex, and a red NQ, the sector ETF opens flat-to-up and closes up or flat (no gap-down, no relative underperformance), then premarket strength in the crowded complex was accumulation, not distribution, and this lesson is falsified."
current_behavior: "S1 was scored +1 on the NVDA AI deal + Dell server backlog semis rally (AMAT +5%, LITE +10%, ALAB +12%) plus the Apple event, explicitly framed as 'the counterweight to the macro risk-off.' S0 (−1) and S1 (+1) were assembled as offsetting forces netting to flat, producing flat/flat at full conviction (divergence_flagged: False in the deterministic pipeline despite the essay text claiming True)."
evidence_cited: "XLK gapped down 1.4% at the open (185.24 vs 187.87 prior close) and closed 185.22 — the entire loss was the opening gap, i.e., a repricing-at-the-open day. XLK −1.41% vs SPY −0.60% (rel −0.81%); Nasdaq Composite −1.16%; CNBC 'Chip selloff deepens.' The morning's own Channel 1 had NQ −0.17% (red, 'no green confirmation'), 10Y–SPX corr −0.969, VIX/VIX3M 1.079 backwardation, and JPMorgan semis crowding ~99%. The premarket leaders (AMAT +5%) became session losers; APH −6.5% extended. Every ingredient was knowable at the open; the gap-down alone falsified 'flat' within the first minute."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When a live macro risk-off shock coincides with a crowded long-duration complex and a red NQ, premarket strength in that complex must be scored as distribution (negative), not accumulation (positive). S0 and S1 are additive, not offsetting: the premarket rally is the liquidity that lets the crowd exit into the gap-down open. The correct output is down/mild at minimum (arguably down/notable), and the divergence flag must actually propagate to the deterministic output so the conviction cut is real, not cosmetic.

## WHEN IT FIRES
A live macro risk-off shock (oil/geopolitical escalation, Brent >$100, elevated/rising yields, negative yield–equity correlation, VIX backwardation) is present at the open, and the sector's dominant complex is a crowded long (extreme relative-performance/valuation crowding) that shows premarket strength or a fresh single-name positive catalyst. The model scores that premarket strength / fresh catalyst as a positive sector factor (S1 > 0) and treats it as a counterweight that nets the macro shock to flat.

## WRONG IF
If on a future day with a live oil/yield risk-off shock, a crowded long-duration complex, and a red NQ, the sector ETF opens flat-to-up and closes up or flat (no gap-down, no relative underperformance), then premarket strength in the crowded complex was accumulation, not distribution, and this lesson is falsified.

## EVIDENCE
XLK gapped down 1.4% at the open (185.24 vs 187.87 prior close) and closed 185.22 — the entire loss was the opening gap, i.e., a repricing-at-the-open day. XLK −1.41% vs SPY −0.60% (rel −0.81%); Nasdaq Composite −1.16%; CNBC "Chip selloff deepens." The morning's own Channel 1 had NQ −0.17% (red, "no green confirmation"), 10Y–SPX corr −0.969, VIX/VIX3M 1.079 backwardation, and JPMorgan semis crowding ~99%. The premarket leaders (AMAT +5%) became session losers; APH −6.5% extended. Every ingredient was knowable at the open; the gap-down alone falsified "flat" within the first minute.

(learn_cycle promote)
