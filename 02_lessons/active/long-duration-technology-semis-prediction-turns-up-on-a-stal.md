---
trigger_pattern: "Long-duration technology/semis prediction turns up on a stale positive mega-cap catalyst carried from prior context, while a fresh knowable-at-open inflation/geopolitical shock is present, real yields are rising, and the yield-equity correlation is strongly negative — with crowded long positioning making the sector asymmetrically vulnerable to risk-off."
corrected_behavior: "Before using any catalyst from prior context, verify it is fresh for the current session; archived catalysts cannot override a live macro shock. When real yields are elevated/rising and the 5-day 10Y-SPX correlation is strongly negative, treat the macro read as net negative for long-duration tech even if VIX is calm and credit spreads are tightening. When the most-crowded trade faces a fresh inflation/geopolitical shock, forbid/avoid an up call or sharply reduce magnitude and confidence; if direction is uncertain, prefer flat/down rather than up/notable."
falsifier: "The rule would be falsified if a future session contains a fresh, knowable oil/geopolitical inflation shock, rising real yields, strongly negative 10Y-SPX correlation, extreme crowding in tech, and only stale positive catalysts from prior context — yet XLK still closes up with notable relative strength. It would also be weakened if the supposedly stale TSMC catalyst is later shown to have been genuinely market-moving on the session itself."
current_behavior: "When prior-run context contains a strong positive tech catalyst (e.g., TSMC revenue surge, hyperscaler capex raise), the model treats it as current and lets it anchor an up/notable call. Simultaneously, live macro headwinds (rising real yields, negative 10Y-SPX correlation, crowding) are listed but netted to mildly positive rather than allowed to gate direction. A fresh oil-driven inflation shock can therefore be missed entirely."
evidence_cited: "On 2026-08-10, the prediction was XLK up/notable, citing TSMC 45% revenue surge and risk-on regime. Actual XLK closed -0.88% vs SPY -0.03%, relative -0.85%. The dominant driver was a knowable-at-open oil spike (Brent +5% to ~$88) on the Strait of Hormuz standoff, pushing bond yields higher and hitting long-duration tech ahead of CPI/Treasury auctions. The morning analysis itself flagged DFII10 at 2.43% (+0.12 1m) and 5-day 10Y-SPX correlation at -0.842, but still netted macro +1 and applied the active mega-cap-earnings-over-macro-drag lesson to a stale TSMC catalyst."
error_category: "A"
scope: "general"
date: "2026-08-10"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-10_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
Before using any catalyst from prior context, verify it is fresh for the current session; archived catalysts cannot override a live macro shock. When real yields are elevated/rising and the 5-day 10Y-SPX correlation is strongly negative, treat the macro read as net negative for long-duration tech even if VIX is calm and credit spreads are tightening. When the most-crowded trade faces a fresh inflation/geopolitical shock, forbid/avoid an up call or sharply reduce magnitude and confidence; if direction is uncertain, prefer flat/down rather than up/notable.

## WHEN IT FIRES
Long-duration technology/semis prediction turns up on a stale positive mega-cap catalyst carried from prior context, while a fresh knowable-at-open inflation/geopolitical shock is present, real yields are rising, and the yield-equity correlation is strongly negative — with crowded long positioning making the sector asymmetrically vulnerable to risk-off.

## WRONG IF
The rule would be falsified if a future session contains a fresh, knowable oil/geopolitical inflation shock, rising real yields, strongly negative 10Y-SPX correlation, extreme crowding in tech, and only stale positive catalysts from prior context — yet XLK still closes up with notable relative strength. It would also be weakened if the supposedly stale TSMC catalyst is later shown to have been genuinely market-moving on the session itself.

## EVIDENCE
On 2026-08-10, the prediction was XLK up/notable, citing TSMC 45% revenue surge and risk-on regime. Actual XLK closed -0.88% vs SPY -0.03%, relative -0.85%. The dominant driver was a knowable-at-open oil spike (Brent +5% to ~$88) on the Strait of Hormuz standoff, pushing bond yields higher and hitting long-duration tech ahead of CPI/Treasury auctions. The morning analysis itself flagged DFII10 at 2.43% (+0.12 1m) and 5-day 10Y-SPX correlation at -0.842, but still netted macro +1 and applied the active mega-cap-earnings-over-macro-drag lesson to a stale TSMC catalyst.

(learn_cycle promote)
