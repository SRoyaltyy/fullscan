---
trigger_pattern: "A Healthcare/XLV call with S0=0 (flat/mixed ES/NQ, leftover tech impulse already in the prior close, two-sided scheduled policy event, oil not spiking) scores S1 negative from residual drug-pricing overhang plus “rotation out,” then copies the same completed prior-session relative lag into S2 (breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel), treats that as independent confirmation, and emits down/mild. A rates/Fed binary is parked as “not an HC spine” even though duration hits XBI while XLV mega-caps typically buffer the ETF to SPY beta."
corrected_behavior: "Do not triple-count a completed prior-session XLV relative lag. With S0=0 and flat futures, set S2=0 unless a live premarket XLV/mega-cap breakdown is confirmed; trailing unit outflows are not a 1-day lid; S4 may describe the prior close, it does not forecast the next session after a large lag already printed. Residual IRA/MFN/TrumpRx after comments-closed is not a same-morning S1=−1 — 08-14 requires a same-day policy headline, not a multi-day overhang. If S0=0 and S1 has no fresh spine, do not let S2+S3+S4 carry down/mild; prefer down/flat or flat/mild (keep the 08-13 ban on up/notable). Do not treat no-divergence as confirmation when leading_sum is S1-rotation + S2 + S4 echoing the same lag. Do not pre-score a two-sided Fed speech as XLV-down/mild: hawkish duration can crush XBI without moving XLV off SPY beta. Do not invent up from an unknowable hawkish resolution or a single-ticker FDA bounce."
falsifier: "If this S0=0 / inherited-S1-rotation-S2-S4 setup recurs, the call is down/flat or flat/mild, and XLV still closes ≤ −0.3% with continued relative lag vs SPY on repeated such sessions, leftover follow-through is real and this lesson is wrong. Also falsified if a same-morning 08-14-type Rx headline is printing and the rule still forces flat, or if the rule is used to emit up after every red XLV day."
current_behavior: "Applied 08-13 (forbid up/notable) and 08-14 (S1 ≤ −0.5, mild not flat). Kept S0=0, then stacked S1=S2=S3=S4=−1 off Thursday’s already-paid −1.78% rel, called that non-divergence, and let leftover tape plus residual IRA/MFN set mild-down. Mag=0.0 experiment kept direction and still printed mild."
evidence_cited: "2026-08-28 predicted down/mild (S0 0 / S1–S4 −1, total −6.3, mult 0.9, conf 0.58) vs XLV −0.245% / SPY −0.227% / rel −0.018% (down/flat). Direction HIT, magnitude MISS. Warsh hawkish; 2y +~8 bp to ~4.31%; XBI −3.47%; JNJ +0.84% on 08-27 17:00 Imaavy FDA. 08-14 falsifier hit (closed within ±0.3%). Rolling HC mag=0.0 n=9. Memory index unavailable this run."
error_category: "B"
scope: "general"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
Do not triple-count a completed prior-session XLV relative lag. With S0=0 and flat futures, set S2=0 unless a live premarket XLV/mega-cap breakdown is confirmed; trailing unit outflows are not a 1-day lid; S4 may describe the prior close, it does not forecast the next session after a large lag already printed. Residual IRA/MFN/TrumpRx after comments-closed is not a same-morning S1=−1 — 08-14 requires a same-day policy headline, not a multi-day overhang. If S0=0 and S1 has no fresh spine, do not let S2+S3+S4 carry down/mild; prefer down/flat or flat/mild (keep the 08-13 ban on up/notable). Do not treat no-divergence as confirmation when leading_sum is S1-rotation + S2 + S4 echoing the same lag. Do not pre-score a two-sided Fed speech as XLV-down/mild: hawkish duration can crush XBI without moving XLV off SPY beta. Do not invent up from an unknowable hawkish resolution or a single-ticker FDA bounce.

## WHEN IT FIRES
A Healthcare/XLV call with S0=0 (flat/mixed ES/NQ, leftover tech impulse already in the prior close, two-sided scheduled policy event, oil not spiking) scores S1 negative from residual drug-pricing overhang plus “rotation out,” then copies the same completed prior-session relative lag into S2 (breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel), treats that as independent confirmation, and emits down/mild. A rates/Fed binary is parked as “not an HC spine” even though duration hits XBI while XLV mega-caps typically buffer the ETF to SPY beta.

## WRONG IF
If this S0=0 / inherited-S1-rotation-S2-S4 setup recurs, the call is down/flat or flat/mild, and XLV still closes ≤ −0.3% with continued relative lag vs SPY on repeated such sessions, leftover follow-through is real and this lesson is wrong. Also falsified if a same-morning 08-14-type Rx headline is printing and the rule still forces flat, or if the rule is used to emit up after every red XLV day.

## EVIDENCE
2026-08-28 predicted down/mild (S0 0 / S1–S4 −1, total −6.3, mult 0.9, conf 0.58) vs XLV −0.245% / SPY −0.227% / rel −0.018% (down/flat). Direction HIT, magnitude MISS. Warsh hawkish; 2y +~8 bp to ~4.31%; XBI −3.47%; JNJ +0.84% on 08-27 17:00 Imaavy FDA. 08-14 falsifier hit (closed within ±0.3%). Rolling HC mag=0.0 n=9. Memory index unavailable this run.

(learn_cycle promote)
