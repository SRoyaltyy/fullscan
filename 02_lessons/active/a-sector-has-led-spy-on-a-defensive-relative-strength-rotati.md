---
trigger_pattern: "A sector has led SPY on a defensive/relative-strength rotation (3d/1w/1m all positive relative), but the current 1d relative tape is flat or barely positive; there is no fresh same-day sector catalyst; and the broad tape is setting up as a tech/growth-led risk-on day (Big Tech strong, Nasdaq futures ≥ S&P futures, SPY near record highs). In that setup, the carried rotation is at risk of reversing because the prior healthcare/defensive leadership was partly the inverse of tech momentum. The flat 1d tape is a reversal tell, not merely a magnitude cap."
corrected_behavior: "When a defensive/relative-strength sector has flat 1d relative tape after a strong 3d/1w/1m run, and the incoming tape is tech/growth-led with no fresh sector catalyst, do not convert the carried rotation into an up call. Treat the flat 1d tape as a possible reversal warning. Output should be neutral-to-down / flat-to-mild, or an explicit low-confidence “no-call.” The official band must not be up/notable. Also reconcile the narrative band with the deterministic pipeline band before submission; the scoreboard grades the pipeline output."
falsifier: "If XLV shows the exact trigger setup — 3d/1w/1m leadership, flat 1d rel, no fresh healthcare catalyst, tech-led premarket — and still closes with strong positive relative performance (e.g., XLV rel > +0.3% on a tech-led SPY day), then the reversal rule is wrong for that case and continuation remains possible."
current_behavior: "On 2026-08-13, XLV led SPY by +1.77% 3d, +2.26% 1w, +3.66% 1m while the 1d rel was flat (+0.01%). The model viewed the medium-term relative strength as confirmed rotation, treated the macro backdrop as benign risk-on, applied the 08-12 freshness lesson to cap magnitude, but still committed to direction “up.” The pipeline then emitted up/notable (total 10.25) even though the narrative arithmetic capped at up/mild (total 5.0). Actual: SPY +0.70% on a Big-Tech record day (MSFT +1.4%, NVDA +0.6%, AAPL +0.5%, Nasdaq +0.9%); XLV -0.04%, rel -0.73%. The prior-week healthcare bid was partly a hedge against tech turbulence; when tech ripped, the hedge unwound."
evidence_cited: "2026-08-13 XLV -0.04% vs SPY +0.70%, rel -0.73%. Reuters (2026-08-05) theme: “Wall Street warms to healthcare stocks as tech trade faces turbulence” — showing healthcare’s recent leadership was partly the reciprocal of tech turbulence. Morning’s own tape read “1d rel +0.01% (flat)” and “no fresh same-day catalyst,” which were the honest leading tells. Additional known issue: narrative said up/mild while pipeline emitted up/notable from the same components."
error_category: "A"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When a defensive/relative-strength sector has flat 1d relative tape after a strong 3d/1w/1m run, and the incoming tape is tech/growth-led with no fresh sector catalyst, do not convert the carried rotation into an up call. Treat the flat 1d tape as a possible reversal warning. Output should be neutral-to-down / flat-to-mild, or an explicit low-confidence “no-call.” The official band must not be up/notable. Also reconcile the narrative band with the deterministic pipeline band before submission; the scoreboard grades the pipeline output.

## WHEN IT FIRES
A sector has led SPY on a defensive/relative-strength rotation (3d/1w/1m all positive relative), but the current 1d relative tape is flat or barely positive; there is no fresh same-day sector catalyst; and the broad tape is setting up as a tech/growth-led risk-on day (Big Tech strong, Nasdaq futures ≥ S&P futures, SPY near record highs). In that setup, the carried rotation is at risk of reversing because the prior healthcare/defensive leadership was partly the inverse of tech momentum. The flat 1d tape is a reversal tell, not merely a magnitude cap.

## WRONG IF
If XLV shows the exact trigger setup — 3d/1w/1m leadership, flat 1d rel, no fresh healthcare catalyst, tech-led premarket — and still closes with strong positive relative performance (e.g., XLV rel > +0.3% on a tech-led SPY day), then the reversal rule is wrong for that case and continuation remains possible.

## EVIDENCE
2026-08-13 XLV -0.04% vs SPY +0.70%, rel -0.73%. Reuters (2026-08-05) theme: “Wall Street warms to healthcare stocks as tech trade faces turbulence” — showing healthcare’s recent leadership was partly the reciprocal of tech turbulence. Morning’s own tape read “1d rel +0.01% (flat)” and “no fresh same-day catalyst,” which were the honest leading tells. Additional known issue: narrative said up/mild while pipeline emitted up/notable from the same components.

(learn_cycle promote)
