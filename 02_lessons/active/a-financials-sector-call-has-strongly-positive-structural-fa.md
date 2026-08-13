---
trigger_pattern: "A Financials sector call has strongly positive structural factor scores (curve steepening, credit tightening, capital-markets/trading strength) and a positive shared-macro score taken from pre-market risk-on indicators, but the sector ETF tape at open is flat/neutral (S4_ETF_TAPE ≈ 0.0) and divergence_flagged is True. A live geopolitical/oil supply-shock headline (e.g., Iran/Hormuz) and/or a high-impact CPI print is knowable at open. The analysis text explicitly says “damp/mild,” but the deterministic score still emits up/severe because the multiplier and component scores are left unchanged. Financials may still outperform SPY relatively, but that does not make the absolute move up or severe."
corrected_behavior: "When S4 is flat/neutral and divergence_flagged=True, cap the emitted magnitude band at mild/flat and use multiplier ≤1.0 unless the absolute tape confirms a same-day move. If a geopolitical/oil supply-shock or high-impact CPI risk is active at open, classify the day as risk-off/neutral rather than risk_on, and score S0 accordingly. Do not convert relative-strength structural support into an absolute up call the ETF tape does not confirm. Enforce consistency between the narrative “damp” conclusion and the deterministic score: either adjust components/multiplier or override the severe band."
falsifier: "The lesson would be falsified if, in repeated episodes with S4≈0, divergence_flagged=True, and an active geopolitical/oil risk-off headline at open, XLF still closes up by more than ~0.5% absolute more often than not. A single counterexample is insufficient; relative outperformance alone does not falsify the lesson."
current_behavior: "The pipeline emits up/severe based on structural sector strength (S1=+2.0, S0=+1.5) with multiplier 1.2 and total 15.0, despite S4_ETF_TAPE=0.0 and divergence_flagged=True. The analysis narrative cites the over-calling lesson and says magnitude should be damped, but the emitted band remains severe. A known geopolitical risk-off suppressor is not incorporated into S0/regime."
evidence_cited: "2026-08-11 XLF closed -0.017% (flat) while SPY fell -0.32%; XLF outperformed by +0.30% relative. The morning call was up/severe. The analysis text explicitly said “this argues for a modest magnitude band (mild), not severe,” but the deterministic output kept multiplier 1.2, total 15.0, and band severe. The actual day was driven by US-Iran/Strait of Hormuz escalation, oil rising, and pre-CPI caution — a partially knowable-at-open risk-off tape."
error_category: "B"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
When S4 is flat/neutral and divergence_flagged=True, cap the emitted magnitude band at mild/flat and use multiplier ≤1.0 unless the absolute tape confirms a same-day move. If a geopolitical/oil supply-shock or high-impact CPI risk is active at open, classify the day as risk-off/neutral rather than risk_on, and score S0 accordingly. Do not convert relative-strength structural support into an absolute up call the ETF tape does not confirm. Enforce consistency between the narrative “damp” conclusion and the deterministic score: either adjust components/multiplier or override the severe band.

## WHEN IT FIRES
A Financials sector call has strongly positive structural factor scores (curve steepening, credit tightening, capital-markets/trading strength) and a positive shared-macro score taken from pre-market risk-on indicators, but the sector ETF tape at open is flat/neutral (S4_ETF_TAPE ≈ 0.0) and divergence_flagged is True. A live geopolitical/oil supply-shock headline (e.g., Iran/Hormuz) and/or a high-impact CPI print is knowable at open. The analysis text explicitly says “damp/mild,” but the deterministic score still emits up/severe because the multiplier and component scores are left unchanged. Financials may still outperform SPY relatively, but that does not make the absolute move up or severe.

## WRONG IF
The lesson would be falsified if, in repeated episodes with S4≈0, divergence_flagged=True, and an active geopolitical/oil risk-off headline at open, XLF still closes up by more than ~0.5% absolute more often than not. A single counterexample is insufficient; relative outperformance alone does not falsify the lesson.

## EVIDENCE
2026-08-11 XLF closed -0.017% (flat) while SPY fell -0.32%; XLF outperformed by +0.30% relative. The morning call was up/severe. The analysis text explicitly said “this argues for a modest magnitude band (mild), not severe,” but the deterministic output kept multiplier 1.2, total 15.0, and band severe. The actual day was driven by US-Iran/Strait of Hormuz escalation, oil rising, and pre-CPI caution — a partially knowable-at-open risk-off tape.

(learn_cycle promote)
