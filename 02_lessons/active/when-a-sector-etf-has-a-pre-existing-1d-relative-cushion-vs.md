---
trigger_pattern: "When a sector ETF has a pre-existing 1d relative cushion vs SPY (≥ +0.4%) that is confirmed by the prior session's actuals (XLRE +0.24% vs SPY −0.28% on 09-05), and the morning identifies a negative rate spine (30Y stress zone, oil spike, risk-off futures), the model treats the cushion as a magnitude cap only (down/mild) rather than considering it as a potential direction override toward flat. The model applies the 09-04 'asymmetric downside' lesson too aggressively without checking whether the defensive cushion is already large and established enough to neutralize the same-day downside translation."
corrected_behavior: "When the 1d relative cushion is ≥ +0.4% AND the prior session confirmed XLRE outperforming SPY on a risk-off day, the cushion must be scored as a positive S4 input (not neutral) and weighed as a potential direction override. Specifically: if the cushion is large and the negative spine is structural (not a fresh same-day shock to REITs specifically), downgrade S0/S1 contribution to the direction call — either reduce S1 to 0 or cap the combined S0+S1 at −1 total (not −2) when the cushion exceeds the expected same-day downside transmission. The 09-04 asymmetric-downside lesson applies when the open is flat-to-easing (no cushion); it does NOT apply when the defensive bid is already large and established."
falsifier: "If XLRE has a 1d relative cushion ≥ +0.4% going into a risk-off session with 30Y in stress zone, and XLRE still falls ≥ −0.5% (direction down confirmed despite cushion), this lesson would be falsified. Also falsified if the model downgrades to flat on a large cushion and XLRE falls > −0.5% on three consecutive similar setups."
current_behavior: "Scores S0=−1 and S1=−1 on the negative rate spine (30Y ~5.25%, oil +3.18%, ES −0.44%) while scoring S4=0 despite the 1d relative cushion (+0.52%) being the single most predictive input. The model resolves the tension between the negative spine and the defensive cushion by capping magnitude at mild (down/mild) rather than downgrading direction to flat when the cushion is large and pre-established."
evidence_cited: "Predicted down/mild (−1.8 total) vs actual flat (−0.07% XLRE, +0.48% relative). The morning's own data showed XLRE 1d +0.24% vs SPY −0.28% (rel +0.52%) — the cushion was already in place and the prior session confirmed the pattern. XLRE finished −0.07% (essentially flat) while SPY fell −0.55%, producing +0.48% relative — almost exactly matching the morning's 1d relative read. The outcome review states: 'The 1d relative cushion (+0.52% vs SPY through 09-08 in the morning data) was the single most predictive input for today's outcome... This should have been scored more positively as a same-day signal, not neutralized."
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
When the 1d relative cushion is ≥ +0.4% AND the prior session confirmed XLRE outperforming SPY on a risk-off day, the cushion must be scored as a positive S4 input (not neutral) and weighed as a potential direction override. Specifically: if the cushion is large and the negative spine is structural (not a fresh same-day shock to REITs specifically), downgrade S0/S1 contribution to the direction call — either reduce S1 to 0 or cap the combined S0+S1 at −1 total (not −2) when the cushion exceeds the expected same-day downside transmission. The 09-04 asymmetric-downside lesson applies when the open is flat-to-easing (no cushion); it does NOT apply when the defensive bid is already large and established.

## WHEN IT FIRES
When a sector ETF has a pre-existing 1d relative cushion vs SPY (≥ +0.4%) that is confirmed by the prior session's actuals (XLRE +0.24% vs SPY −0.28% on 09-05), and the morning identifies a negative rate spine (30Y stress zone, oil spike, risk-off futures), the model treats the cushion as a magnitude cap only (down/mild) rather than considering it as a potential direction override toward flat. The model applies the 09-04 "asymmetric downside" lesson too aggressively without checking whether the defensive cushion is already large and established enough to neutralize the same-day downside translation.

## WRONG IF
If XLRE has a 1d relative cushion ≥ +0.4% going into a risk-off session with 30Y in stress zone, and XLRE still falls ≥ −0.5% (direction down confirmed despite cushion), this lesson would be falsified. Also falsified if the model downgrades to flat on a large cushion and XLRE falls > −0.5% on three consecutive similar setups.

## EVIDENCE
Predicted down/mild (−1.8 total) vs actual flat (−0.07% XLRE, +0.48% relative). The morning's own data showed XLRE 1d +0.24% vs SPY −0.28% (rel +0.52%) — the cushion was already in place and the prior session confirmed the pattern. XLRE finished −0.07% (essentially flat) while SPY fell −0.55%, producing +0.48% relative — almost exactly matching the morning's 1d relative read. The outcome review states: "The 1d relative cushion (+0.52% vs SPY through 09-08 in the morning data) was the single most predictive input for today's outcome... This should have been scored more positively as a same-day signal, not neutralized.

(learn_cycle promote)
