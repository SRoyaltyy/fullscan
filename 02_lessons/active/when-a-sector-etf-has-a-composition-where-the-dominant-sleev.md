---
trigger_pattern: "When a sector ETF has a composition where the dominant sleeve (chemicals/processors ~40-50% of XLB) faces a direct cost headwind (oil spike) while a minority sleeve (copper miners ~10-15%) benefits from a commodity surge, the model scores S1 based on the headline commodity move (copper at records = +1) rather than the composition-weighted net. The model must weight sector-factor scores by the ETF's actual holdings breakdown — a minority-sleeve positive cannot offset a majority-sleeve negative when the cost channel is direct and knowable."
corrected_behavior: "Before scoring S1, decompose the sector ETF by holdings weight. If a majority sleeve faces a direct, knowable cost/margin headwind (oil feedstock for chemicals) while a minority sleeve benefits from a commodity surge, score S1 negative or neutral — the composition math dictates the ETF outcome. Additionally, when S4 confirmation is derived from the same commodity move already scored in S1, do not double-count: if the 1d relative strength is attributable to the same factor (copper bid) already in S1, either reduce S1 or zero S4, not both."
falsifier: "A session where copper surges to records AND oil spikes simultaneously, but XLB closes positive or flat despite the chemicals drag — if chemicals sleeve does not actually drag (e.g., chemicals companies with pricing power pass through costs), the composition-weighting assumption fails."
current_behavior: "Scores S1 = +1 on copper at records + FCX strength, treating the oil cost headwind on chemicals as a minor offset within a net positive. Ignores that XLB is chemicals-heavy (LIN ~13%, SHW, ECL — roughly 40-50% combined) while copper miners are only ~10-15%. Also double-counts the copper bid in S4 (+0.5 via 1d rel +0.56%) when that relative strength was itself copper-driven and already captured in S1."
evidence_cited: "2026-09-08 XLB: predicted flat/flat (total 0.45), actual −0.95% (rel −0.40% vs SPY). Copper hit records, FCX +5.35%, but chemicals sleeve (LIN/SHW/ECL) dragged the ETF down ~1.5-2% on oil cost squeeze. The composition asymmetry was knowable at open from XLB's holdings breakdown. The morning's own 8/18 rule (don't use copper as floor on oil-shock days) was identified but then violated by scoring S1 = +1."
error_category: "B"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring S1, decompose the sector ETF by holdings weight. If a majority sleeve faces a direct, knowable cost/margin headwind (oil feedstock for chemicals) while a minority sleeve benefits from a commodity surge, score S1 negative or neutral — the composition math dictates the ETF outcome. Additionally, when S4 confirmation is derived from the same commodity move already scored in S1, do not double-count: if the 1d relative strength is attributable to the same factor (copper bid) already in S1, either reduce S1 or zero S4, not both.

## WHEN IT FIRES
When a sector ETF has a composition where the dominant sleeve (chemicals/processors ~40-50% of XLB) faces a direct cost headwind (oil spike) while a minority sleeve (copper miners ~10-15%) benefits from a commodity surge, the model scores S1 based on the headline commodity move (copper at records = +1) rather than the composition-weighted net. The model must weight sector-factor scores by the ETF's actual holdings breakdown — a minority-sleeve positive cannot offset a majority-sleeve negative when the cost channel is direct and knowable.

## WRONG IF
A session where copper surges to records AND oil spikes simultaneously, but XLB closes positive or flat despite the chemicals drag — if chemicals sleeve does not actually drag (e.g., chemicals companies with pricing power pass through costs), the composition-weighting assumption fails.

## EVIDENCE
2026-09-08 XLB: predicted flat/flat (total 0.45), actual −0.95% (rel −0.40% vs SPY). Copper hit records, FCX +5.35%, but chemicals sleeve (LIN/SHW/ECL) dragged the ETF down ~1.5-2% on oil cost squeeze. The composition asymmetry was knowable at open from XLB's holdings breakdown. The morning's own 8/18 rule (don't use copper as floor on oil-shock days) was identified but then violated by scoring S1 = +1.

(learn_cycle promote)
