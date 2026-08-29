---
trigger_pattern: "Mega-cap cyclical ETF (XLY: AMZN/TSLA/HD) with S0=0 (mixed/flat ES/NQ, leftover impulse in a non-holding, two-sided policy event, no same-morning consumer print) and S1 only a stale/confirming consumer spine; the only negatives are yesterday’s completed relative fade copied into S2 (prior-session composition), S3 (trailing 5d outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation for a down call."
corrected_behavior: "Do not triple-count a completed prior-session lag. With S0=0 and stale S1, set S2=0 unless a live premarket AMZN/TSLA/HD breakdown is confirmed; do not treat 5d outflows as a 1-day lid; do not re-vote yesterday’s 1d rel as a full S4 down. Prefer flat/mild. A ban on mapping XLK/NVDA into S0=+1 is not a license to extrapolate the sector lag. A hawkish two-sided speech that hits semis can be a relative bid for AMZN-weight XLY — do not score it as consumer-beta down. Do not flip to up from an unknowable same-day top-weight note."
falsifier: "If this S0=0 / stale-S1 / inherited-S2-S4 setup recurs, the call is flat/mild, and XLY still closes ≤ −0.5% or lags SPY by ≥ 0.5% with AMZN/HD also red, revise this lesson. Also falsified if a confirmed premarket AMZN/TSLA/HD breakdown is present and the rule still forces flat."
current_behavior: "After correctly banning S0=+1 from a non-holdings XLK/NVDA leftover, the model still emits down/mild by triple-counting the completed lag in S2/S3/S4, calling that agreement non-divergence, and letting a stale confidence/retail spine set direction."
evidence_cited: "2026-08-28 predicted down/mild (S0=0, S1=S2=S3=S4=−1, total −6.3); XLY +1.15% vs SPY −0.23% (rel +1.37%), open already 116.71 vs prior 115.88. AMZN +3.97% carried the ETF; TSLA red did not set it; NVDA −4.61% / SOX −3.5%; UMich final 51.7 vs 51.0 and Chicago PMI 47.1 did not price. Counterfactual S2=0, S4=0 → ~−1.8, flat/mild."
error_category: "B"
scope: "general"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Do not triple-count a completed prior-session lag. With S0=0 and stale S1, set S2=0 unless a live premarket AMZN/TSLA/HD breakdown is confirmed; do not treat 5d outflows as a 1-day lid; do not re-vote yesterday’s 1d rel as a full S4 down. Prefer flat/mild. A ban on mapping XLK/NVDA into S0=+1 is not a license to extrapolate the sector lag. A hawkish two-sided speech that hits semis can be a relative bid for AMZN-weight XLY — do not score it as consumer-beta down. Do not flip to up from an unknowable same-day top-weight note.

## WHEN IT FIRES
Mega-cap cyclical ETF (XLY: AMZN/TSLA/HD) with S0=0 (mixed/flat ES/NQ, leftover impulse in a non-holding, two-sided policy event, no same-morning consumer print) and S1 only a stale/confirming consumer spine; the only negatives are yesterday’s completed relative fade copied into S2 (prior-session composition), S3 (trailing 5d outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation for a down call.

## WRONG IF
If this S0=0 / stale-S1 / inherited-S2-S4 setup recurs, the call is flat/mild, and XLY still closes ≤ −0.5% or lags SPY by ≥ 0.5% with AMZN/HD also red, revise this lesson. Also falsified if a confirmed premarket AMZN/TSLA/HD breakdown is present and the rule still forces flat.

## EVIDENCE
2026-08-28 predicted down/mild (S0=0, S1=S2=S3=S4=−1, total −6.3); XLY +1.15% vs SPY −0.23% (rel +1.37%), open already 116.71 vs prior 115.88. AMZN +3.97% carried the ETF; TSLA red did not set it; NVDA −4.61% / SOX −3.5%; UMich final 51.7 vs 51.0 and Chicago PMI 47.1 did not price. Counterfactual S2=0, S4=0 → ~−1.8, flat/mild.

(learn_cycle promote)
