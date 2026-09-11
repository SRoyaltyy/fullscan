---
trigger_pattern: "A defensive sector (healthcare/staples/utilities) is a multi-day relative laggard (3d/1w rel negative) heading into a scheduled risk-on macro catalyst (benign CPI/NFP, green futures), and the model scores the shared-macro component POSITIVE (S0 > 0) on the reasoning that 'risk-on + duration relief = sector tailwind' — treating the sector as a beneficiary of the risk-on bid rather than as the funding source that capital rotates OUT of."
corrected_behavior: "When a defensive sector carries a multi-day relative lag (3d/1w rel negative) into a risk-on macro catalyst, score S0 at 0 to −0.3, not positive. Risk-on is a ROTATION signal: capital flows into high-beta/cyclical and OUT of defensives, so a defensive sector is a funding source, not a destination. Additionally: (a) do not score S2/S3/S4 at 0.0 when the rotation-out flow is intact — a lower-high tape (two small positive rel prints then a fade) is distribution, not a base; (b) require a positive 1d relative print on above-average volume before calling exhaustion; (c) flat 1m rel ≠ demand — absence of crowding is not presence of buyers, so S3 should be negative when a sector has given back outperformance with no fresh inflow; (d) run an explicit contrarian check: 'what would make this sector UNDERPERFORM a green tape?' — if the answer is 'rotation out,' score it negative."
falsifier: "If a defensive sector with a multi-day relative lag (3d/1w rel negative) gaps up on a benign risk-on macro print and CLOSES at or above the market (rel ≥ 0) on above-average volume, then 'risk-on = rotation out of defensives' is falsified for that regime and S0 should not be scored negative. Concretely: XLV rel ≥ 0 on a SPY +0.5%+ day would break this lesson."
current_behavior: "On 2026-09-11 the model scored S0 = +0.5 for XLV ('CPI-day risk-on setup… oil falling is a duration tailwind… S0 = +0.5 mild risk-on/duration relief'), and scored S2/S3/S4 = 0.0, reading the two prior small positive relative prints (+0.14%, +0.05%) as exhaustion/stabilization and the flat 1m rel (+0.25%) as 'crowded-long fully unwound.' Leading sum +1.0 → total +0.9 → flat. Actual: XLV −0.18% vs SPY +0.85%, rel −1.03% — a gap-up-then-fade, worst single-day relative print in the sequence."
evidence_cited: "Morning note S0 = +0.5 with the explicit rationale 'oil falling + futures green + non-extended sector = S0 = +0.5 (mild risk-on/duration relief).' Outcome: SPY +0.85%, XLV −0.18%, rel −1.03%; XLV opened +0.70% and faded monotonically all day. The morning note itself flagged '1d/3d/1w out of healthcare is decaying' and scored rotation-out as MISS in the HIT_GRID — the evidence for the correct sign was present and mis-signed. The duration-tailwind thesis (falling oil → easing inflation → XBI relief) was maximally supported by the actual benign 3.4% CPI and a bond rally, yet the XBI sleeve did not lead (HIT_GRID 'Biotech risk-on / XBI leadership: MISS'), falsifying the transmission assumption."
error_category: "A"
scope: "general"
date: "2026-09-11"
status: "active"
occurrences: "1"
promoted_on: "2026-09-11"
sources: "['2026-09-11_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When a defensive sector carries a multi-day relative lag (3d/1w rel negative) into a risk-on macro catalyst, score S0 at 0 to −0.3, not positive. Risk-on is a ROTATION signal: capital flows into high-beta/cyclical and OUT of defensives, so a defensive sector is a funding source, not a destination. Additionally: (a) do not score S2/S3/S4 at 0.0 when the rotation-out flow is intact — a lower-high tape (two small positive rel prints then a fade) is distribution, not a base; (b) require a positive 1d relative print on above-average volume before calling exhaustion; (c) flat 1m rel ≠ demand — absence of crowding is not presence of buyers, so S3 should be negative when a sector has given back outperformance with no fresh inflow; (d) run an explicit contrarian check: "what would make this sector UNDERPERFORM a green tape?" — if the answer is "rotation out," score it negative.

## WHEN IT FIRES
A defensive sector (healthcare/staples/utilities) is a multi-day relative laggard (3d/1w rel negative) heading into a scheduled risk-on macro catalyst (benign CPI/NFP, green futures), and the model scores the shared-macro component POSITIVE (S0 > 0) on the reasoning that "risk-on + duration relief = sector tailwind" — treating the sector as a beneficiary of the risk-on bid rather than as the funding source that capital rotates OUT of.

## WRONG IF
If a defensive sector with a multi-day relative lag (3d/1w rel negative) gaps up on a benign risk-on macro print and CLOSES at or above the market (rel ≥ 0) on above-average volume, then "risk-on = rotation out of defensives" is falsified for that regime and S0 should not be scored negative. Concretely: XLV rel ≥ 0 on a SPY +0.5%+ day would break this lesson.

## EVIDENCE
Morning note S0 = +0.5 with the explicit rationale "oil falling + futures green + non-extended sector = S0 = +0.5 (mild risk-on/duration relief)." Outcome: SPY +0.85%, XLV −0.18%, rel −1.03%; XLV opened +0.70% and faded monotonically all day. The morning note itself flagged "1d/3d/1w out of healthcare is decaying" and scored rotation-out as MISS in the HIT_GRID — the evidence for the correct sign was present and mis-signed. The duration-tailwind thesis (falling oil → easing inflation → XBI relief) was maximally supported by the actual benign 3.4% CPI and a bond rally, yet the XBI sleeve did not lead (HIT_GRID "Biotech risk-on / XBI leadership: MISS"), falsifying the transmission assumption.

(learn_cycle promote)
