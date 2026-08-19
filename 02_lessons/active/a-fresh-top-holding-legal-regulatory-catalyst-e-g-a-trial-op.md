---
trigger_pattern: "A fresh top-holding legal/regulatory catalyst (e.g., a trial opening with large headline damages) is correctly identified and the sector is correctly scored down, but the model escalates the magnitude to severe based on the severity of the single-company headline alone. It fails to scan for knowable offsetting positives in the other mega-cap leaders of the same concentrated sector ETF. On a risk-off day, a concentrated sector can outperform SPY when the negative catalyst hits only one mega-cap leader while others carry positive flows/news. The severe band should require the legal shock to threaten both mega-cap leaders, or to hit one leader with no offsetting positives elsewhere in the sector."
corrected_behavior: "Whenever a top-holding legal catalyst is present, after scoring the negative catalyst, run a mandatory offset scan for the other largest holdings in the ETF: check major institutional/activist flow news (13-Fs, Ackman-style returns), positive idiosyncratic catalysts in high-weight components, and whether the legal shock actually hits one or both mega-cap leaders. Do not assign the severe magnitude band unless the negative catalyst threatens both Meta and Alphabet simultaneously and/or no material offsetting positives are knowable at open. Reconcile the official pipeline band with the narrative score so a single legal catalyst is not double-counted across S1, S2, and S3."
falsifier: "Find a future case where a fresh legal/regulatory catalyst hits only one of the two mega-cap leaders, no offsetting positive catalysts are knowable in the other top holdings, and XLC still drops only mildly or outperforms SPY despite the broad market being risk-off. That would weaken the rule that severe requires both leaders hit or no offsets. Conversely, if a one-leader legal shock ever produces an XLC relative lag of -3% or worse with no offsetting positives, the severe cap should be loosened."
current_behavior: "On 2026-08-18, the model correctly identified the Meta trial opening, the Ninth Circuit Section 230 overhang, and broad risk-off, and predicted XLC down. But it over-weighted the Meta/$200B legal shock and emitted down/severe (-14 official total) while missing two knowable offsets in the same sector: Berkshire's $17B Q2 Alphabet stake addition (reported Aug 15) and Bill Ackman's Pershing Square return to Netflix (reported Aug 18 premarket). It also treated XLC concentration as a uniform downside risk rather than recognizing that Meta was the trial casualty while Alphabet and Netflix had positive supports."
evidence_cited: "XLC fell only -0.31% on 2026-08-18 while SPY fell -0.68%, so XLC outperformed by +0.37% relative. Meta fell ~4% on the trial, but Netflix rose +4% on Ackman's return and Alphabet was supported by Berkshire's $17B stake addition. The prediction was down/severe, implying a large relative lag; actual was down/mild with relative outperformance. Scoreboard: direction_hit=True, magnitude_hit=False."
error_category: "B"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
Whenever a top-holding legal catalyst is present, after scoring the negative catalyst, run a mandatory offset scan for the other largest holdings in the ETF: check major institutional/activist flow news (13-Fs, Ackman-style returns), positive idiosyncratic catalysts in high-weight components, and whether the legal shock actually hits one or both mega-cap leaders. Do not assign the severe magnitude band unless the negative catalyst threatens both Meta and Alphabet simultaneously and/or no material offsetting positives are knowable at open. Reconcile the official pipeline band with the narrative score so a single legal catalyst is not double-counted across S1, S2, and S3.

## WHEN IT FIRES
A fresh top-holding legal/regulatory catalyst (e.g., a trial opening with large headline damages) is correctly identified and the sector is correctly scored down, but the model escalates the magnitude to severe based on the severity of the single-company headline alone. It fails to scan for knowable offsetting positives in the other mega-cap leaders of the same concentrated sector ETF. On a risk-off day, a concentrated sector can outperform SPY when the negative catalyst hits only one mega-cap leader while others carry positive flows/news. The severe band should require the legal shock to threaten both mega-cap leaders, or to hit one leader with no offsetting positives elsewhere in the sector.

## WRONG IF
Find a future case where a fresh legal/regulatory catalyst hits only one of the two mega-cap leaders, no offsetting positive catalysts are knowable in the other top holdings, and XLC still drops only mildly or outperforms SPY despite the broad market being risk-off. That would weaken the rule that severe requires both leaders hit or no offsets. Conversely, if a one-leader legal shock ever produces an XLC relative lag of -3% or worse with no offsetting positives, the severe cap should be loosened.

## EVIDENCE
XLC fell only -0.31% on 2026-08-18 while SPY fell -0.68%, so XLC outperformed by +0.37% relative. Meta fell ~4% on the trial, but Netflix rose +4% on Ackman's return and Alphabet was supported by Berkshire's $17B stake addition. The prediction was down/severe, implying a large relative lag; actual was down/mild with relative outperformance. Scoreboard: direction_hit=True, magnitude_hit=False.

(learn_cycle promote)
