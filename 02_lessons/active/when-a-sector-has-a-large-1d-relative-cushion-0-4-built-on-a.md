---
trigger_pattern: "When a sector has a large 1d relative cushion (≥ +0.4%) built on a single-session defensive spike driven by a kinetic/oil shock, and the shock is static (not escalating) at the open of the next session, the model treats the cushion as a direction override toward flat. The correct treatment is to recognize the cushion as mean-reversion fuel — the market already expressed the defensive trade, and a non-escalating shock leaves the rates headwind (created by the shock) as the sole dominant driver, pointing down."
corrected_behavior: "Before applying the cushion-as-flat-override lesson, check whether the catalyst that created the cushion is escalating, static, or fading. If the shock is static (oil holding at the same level, no new kinetic escalation) or fading, the cushion is a SELL signal — the defensive bid has been priced and mean-reversion is the higher-probability outcome. In that case, score S1 negative (rates headwind is the sole dominant factor, no offsetting defensive bid), do NOT apply the flat override from the cushion, and predict down/mild. The cushion-as-flat-override applies ONLY when the shock is fresh/escalating (new strikes, oil moving higher) such that a new defensive bid can plausibly form."
falsifier: "A future session where XLU has a ≥ +0.4% 1d relative cushion from a defensive spike, the shock is static (oil holding, no new escalation), yet XLU closes flat or up (cushion holds, no mean-reversion) would falsify this lesson. Also falsified if a static shock with a large cushion leads to down but the magnitude exceeds mild (≥ 2%) — the lesson predicts mild, not notable."
current_behavior: "Model applies the 09-08 lesson (1d relative cushion ≥ +0.4% + prior-session outperformance → direction override toward flat) without checking whether the shock that created the cushion is escalating or static. On 2026-09-09, XLU had a +1.41% 1d relative cushion from the 09-08 oil-shock defensive bid. The model scored S0=0, S1=0 (rates-rising offset by defensive bid), S2=0 (cushion → flat override), S4=+1, predicting flat/flat. Actual: XLU −1.17%, SPY −0.46%, rel −0.71% — the cushion reverted and rates dominated."
evidence_cited: "2026-09-09 Utilities: predicted flat/flat (total_score 0.45), actual XLU −1.17% / SPY −0.46% / rel −0.71% (dir MISS, mag MISS). The 09-08 cushion (+1.41% rel) was built on an oil shock (WTI crossed $100, US struck Iranian tankers). On 09-09, oil held ~$100 but did NOT escalate (no new strikes). The 10-year auction at 4.834% (20-month high) reinforced the duration headwind. The defensive bid faded; mean-reversion dominated. The outcome review explicitly states: 'A one-day defensive spike on a static shock is mean-reversion fuel, not a support level."
error_category: "B"
scope: "general"
date: "2026-09-09"
status: "active"
occurrences: "1"
promoted_on: "2026-09-09"
sources: "['2026-09-09_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
Before applying the cushion-as-flat-override lesson, check whether the catalyst that created the cushion is escalating, static, or fading. If the shock is static (oil holding at the same level, no new kinetic escalation) or fading, the cushion is a SELL signal — the defensive bid has been priced and mean-reversion is the higher-probability outcome. In that case, score S1 negative (rates headwind is the sole dominant factor, no offsetting defensive bid), do NOT apply the flat override from the cushion, and predict down/mild. The cushion-as-flat-override applies ONLY when the shock is fresh/escalating (new strikes, oil moving higher) such that a new defensive bid can plausibly form.

## WHEN IT FIRES
When a sector has a large 1d relative cushion (≥ +0.4%) built on a single-session defensive spike driven by a kinetic/oil shock, and the shock is static (not escalating) at the open of the next session, the model treats the cushion as a direction override toward flat. The correct treatment is to recognize the cushion as mean-reversion fuel — the market already expressed the defensive trade, and a non-escalating shock leaves the rates headwind (created by the shock) as the sole dominant driver, pointing down.

## WRONG IF
A future session where XLU has a ≥ +0.4% 1d relative cushion from a defensive spike, the shock is static (oil holding, no new escalation), yet XLU closes flat or up (cushion holds, no mean-reversion) would falsify this lesson. Also falsified if a static shock with a large cushion leads to down but the magnitude exceeds mild (≥ 2%) — the lesson predicts mild, not notable.

## EVIDENCE
2026-09-09 Utilities: predicted flat/flat (total_score 0.45), actual XLU −1.17% / SPY −0.46% / rel −0.71% (dir MISS, mag MISS). The 09-08 cushion (+1.41% rel) was built on an oil shock (WTI crossed $100, US struck Iranian tankers). On 09-09, oil held ~$100 but did NOT escalate (no new strikes). The 10-year auction at 4.834% (20-month high) reinforced the duration headwind. The defensive bid faded; mean-reversion dominated. The outcome review explicitly states: "A one-day defensive spike on a static shock is mean-reversion fuel, not a support level.

(learn_cycle promote)
