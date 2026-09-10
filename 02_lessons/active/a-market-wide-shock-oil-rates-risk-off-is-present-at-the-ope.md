---
trigger_pattern: "A market-wide shock (oil/rates/risk-off) is present at the open, and the model scores the SAME shock as an independent negative in S0 (shared macro), S1 (sector transmission channel), and S2 (breadth via a cross-asset co-move like metals), producing a leading_sum far more negative than the sector-specific effect justifies — while the sector's own 1m relative has already mean-reverted to flat and its 1d relative is non-negative."
corrected_behavior: "When a shock is market-wide, score it ONCE in S0. Do not re-score it in S1/S2 unless there is a confirmed healthcare-specific transmission channel (a healthcare cost/revenue link to oil, a fresh sector-fundamental catalyst, or a confirmed breadth failure in XLV's own constituents). 'Duration-sensitive sector' is not healthcare-specific — it applies equally to tech, REITs, utilities, consumer discretionary. Additionally: (a) an S1 built on a list of stale/paid/absent catalysts is an absence of a positive, which scores 0, not −1.0; (b) when the bearish thesis rests on a sub-sector (biotech/XBI) that is a minority weight in the cap-weighted ETF, discount the score by that weight (XLV biotech ≈15–20%, so a −1.0 biotech drag ≈ −0.2 at the ETF level); (c) when 1m relative has fully mean-reverted to flat, the crowded-long accelerant is REMOVED — propagate that to S1/S2, not just S3."
falsifier: "If on a future market-wide oil/risk-off day with a flat 1m relative and non-negative 1d relative, XLV's relative return closes ≤ −0.5% (i.e. the sector genuinely underperforms the market on a shared shock), then a healthcare-specific transmission channel exists and the S1/S2 negative scores are justified — this lesson would be falsified for healthcare. Conversely, if XLV rel closes within ±0.2% of SPY on such a day, the lesson is confirmed."
current_behavior: "On 2026-09-10 the morning stack scored S0=−1.0 (oil/risk-off), S1=−1.0 ('duration sleeve hit by oil-driven inflation/rates'), S2=−1.0 ('risk-asset liquidation' via Gold/Silver/Copper co-move), yielding leading_sum=−7.0 and total −6.525. The self-audit *claimed* 'oil scored once in S0, not re-scored in S1 as rotation,' but the S1 and S2 rationales were explicitly the same oil/risk-off shock. XLV closed −0.552% vs SPY −0.599% (rel +0.047%) — a pure beta day with essentially zero sector-specific transmission. Direction and magnitude hit, but the sector-specific components were not validated."
evidence_cited: "XLV −0.552% / SPY −0.599% / rel +0.047% (yf_download). Morning note's own S1 text: 'Duration sleeve hit by oil-driven inflation/rates'; S2 text: 'risk-asset liquidation' via metals co-move — both the same shock as S0. Morning note conceded no fresh XBI leadership, no same-morning mega-cap Rx headline, ABBV/AMGN T+4/paid, ABT single-ticker, MA rates stale — a list of absences scored as −1.0. KRYS −1.95% ('broadly in line with a sharp sector-wide healthcare selloff') confirms the biotech tail fell 3–4x harder than the ETF, i.e. the duration story was real for XBI and diluted to ~zero for XLV."
error_category: "C"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When a shock is market-wide, score it ONCE in S0. Do not re-score it in S1/S2 unless there is a confirmed healthcare-specific transmission channel (a healthcare cost/revenue link to oil, a fresh sector-fundamental catalyst, or a confirmed breadth failure in XLV's own constituents). "Duration-sensitive sector" is not healthcare-specific — it applies equally to tech, REITs, utilities, consumer discretionary. Additionally: (a) an S1 built on a list of stale/paid/absent catalysts is an absence of a positive, which scores 0, not −1.0; (b) when the bearish thesis rests on a sub-sector (biotech/XBI) that is a minority weight in the cap-weighted ETF, discount the score by that weight (XLV biotech ≈15–20%, so a −1.0 biotech drag ≈ −0.2 at the ETF level); (c) when 1m relative has fully mean-reverted to flat, the crowded-long accelerant is REMOVED — propagate that to S1/S2, not just S3.

## WHEN IT FIRES
A market-wide shock (oil/rates/risk-off) is present at the open, and the model scores the SAME shock as an independent negative in S0 (shared macro), S1 (sector transmission channel), and S2 (breadth via a cross-asset co-move like metals), producing a leading_sum far more negative than the sector-specific effect justifies — while the sector's own 1m relative has already mean-reverted to flat and its 1d relative is non-negative.

## WRONG IF
If on a future market-wide oil/risk-off day with a flat 1m relative and non-negative 1d relative, XLV's relative return closes ≤ −0.5% (i.e. the sector genuinely underperforms the market on a shared shock), then a healthcare-specific transmission channel exists and the S1/S2 negative scores are justified — this lesson would be falsified for healthcare. Conversely, if XLV rel closes within ±0.2% of SPY on such a day, the lesson is confirmed.

## EVIDENCE
XLV −0.552% / SPY −0.599% / rel +0.047% (yf_download). Morning note's own S1 text: "Duration sleeve hit by oil-driven inflation/rates"; S2 text: "risk-asset liquidation" via metals co-move — both the same shock as S0. Morning note conceded no fresh XBI leadership, no same-morning mega-cap Rx headline, ABBV/AMGN T+4/paid, ABT single-ticker, MA rates stale — a list of absences scored as −1.0. KRYS −1.95% ("broadly in line with a sharp sector-wide healthcare selloff") confirms the biotech tail fell 3–4x harder than the ETF, i.e. the duration story was real for XBI and diluted to ~zero for XLV.

(learn_cycle promote)
