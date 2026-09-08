---
trigger_pattern: "When a sector ETF has a large pre-existing 1d relative cushion (+0.5% or greater vs SPY) going into a risk-off session, and the model has identified that cushion as a 'defensive bid,' the model treats the cushion as a magnitude cap (allowing down/mild) rather than as a direction override signal (pointing to flat). The model applies an 'asymmetric downside' lesson (09-04) too aggressively when the defensive cushion is already large and positive, failing to recognize that the cushion's size relative to the expected macro drag determines whether it caps magnitude or flips direction."
corrected_behavior: "When the 1d relative cushion is ≥ +0.5% (or the sector has outperformed SPY on the prior session by a similar margin) AND the macro drag is a moderate risk-off day (ES −0.4% to −0.6%, not a −1%+ crash), the cushion should be scored as a direction signal toward flat, not merely a magnitude cap. The decision rule: if the cushion is large enough that the sector would need to underperform SPY by the full cushion amount just to reach −0.3%, and the macro drag is mild-to-moderate, pre-score flat rather than down. The 09-04 asymmetric-downside lesson applies only when the cushion is small/negative or the macro shock is severe (ES −1%+, 30Y smashing through stress zone)."
falsifier: "A future session where the 1d relative cushion is ≥ +0.5%, the macro drag is moderate (ES −0.4% to −0.6%), and the sector still finishes down ≥ −0.3% would falsify this lesson. Conversely, a session where the cushion is ≥ +0.5% and the sector finishes flat or positive would confirm it."
current_behavior: "Scores S0/S1 negative on the rate spine (30Y stress zone, oil spike, risk-off futures) and treats the 1d relative cushion (+0.52%) as a magnitude cap only — emitting down/mild when the cushion is large enough that flat is the more probable outcome. The model's own data showed XLRE +0.24% 1d vs SPY −0.28% (prior session outperformance) and the outcome confirmed +0.48% relative — the cushion was the single most predictive input but was neutralized in S4 and only used to cap magnitude."
evidence_cited: "2026-09-08: predicted down/mild, actual −0.07% (flat). Morning data showed XLRE 1d rel +0.52% vs SPY, and the outcome was +0.48% relative — the cushion was nearly perfectly predictive. The model's own S4 read noted '1d/3d are defensive relative cushions' but scored S4 = 0 and used the cushion only to cap magnitude at mild. The outcome review confirmed: 'the cushion overwhelmed the spine, producing flat rather than down.' The 09-04 lesson (asymmetric downside) was applied but should have been tempered by the cushion's size."
error_category: "B"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
When the 1d relative cushion is ≥ +0.5% (or the sector has outperformed SPY on the prior session by a similar margin) AND the macro drag is a moderate risk-off day (ES −0.4% to −0.6%, not a −1%+ crash), the cushion should be scored as a direction signal toward flat, not merely a magnitude cap. The decision rule: if the cushion is large enough that the sector would need to underperform SPY by the full cushion amount just to reach −0.3%, and the macro drag is mild-to-moderate, pre-score flat rather than down. The 09-04 asymmetric-downside lesson applies only when the cushion is small/negative or the macro shock is severe (ES −1%+, 30Y smashing through stress zone).

## WHEN IT FIRES
When a sector ETF has a large pre-existing 1d relative cushion (+0.5% or greater vs SPY) going into a risk-off session, and the model has identified that cushion as a "defensive bid," the model treats the cushion as a magnitude cap (allowing down/mild) rather than as a direction override signal (pointing to flat). The model applies an "asymmetric downside" lesson (09-04) too aggressively when the defensive cushion is already large and positive, failing to recognize that the cushion's size relative to the expected macro drag determines whether it caps magnitude or flips direction.

## WRONG IF
A future session where the 1d relative cushion is ≥ +0.5%, the macro drag is moderate (ES −0.4% to −0.6%), and the sector still finishes down ≥ −0.3% would falsify this lesson. Conversely, a session where the cushion is ≥ +0.5% and the sector finishes flat or positive would confirm it.

## EVIDENCE
2026-09-08: predicted down/mild, actual −0.07% (flat). Morning data showed XLRE 1d rel +0.52% vs SPY, and the outcome was +0.48% relative — the cushion was nearly perfectly predictive. The model's own S4 read noted "1d/3d are defensive relative cushions" but scored S4 = 0 and used the cushion only to cap magnitude at mild. The outcome review confirmed: "the cushion overwhelmed the spine, producing flat rather than down." The 09-04 lesson (asymmetric downside) was applied but should have been tempered by the cushion's size.

(learn_cycle promote)
