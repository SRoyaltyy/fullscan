---
trigger_pattern: "A defensive/staples sector prediction is set to flat solely because premarket US futures are flat and global sessions are mixed, while an active geopolitical/oil supply-shock headline (e.g., Strait-of-Hormuz style impasse) and/or a scheduled high-impact CPI print is present in the news cycle. The model treats the day as neutral/risk-on, scores S0_SHARED_MACRO at 0 or positive, and emits flat/flat — but a defensive ETF can still fall modestly in absolute terms on broad risk-off while outperforming SPY relatively, so flat/flat is a graded miss."
corrected_behavior: "When an active geopolitical/oil/CPI risk-off catalyst is present, score S0_SHARED_MACRO negative/risk-off rather than neutral, keep magnitude capped at mild (not notable), and emit down/mild when the defensive ETF is already underperforming on the multi-day tape. Do not let a flat premarket futures print alone justify an absolute flat call. A defensive sector can fall about -0.3% absolutely while matching or slightly outperforming SPY."
falsifier: "If a later morning has the same active geopolitical/oil/CPI-risk signature and XLP closes positive, or rises more than ~0.2% absolutely, or outperforms SPY by more than ~0.5%, this lesson is overcorrecting; the defensive-bid signal would then be strong enough to justify flat/up, and the rule should be revised."
current_behavior: "On 2026-08-11, morning saw flat futures (ES -0.02%, NQ -0.26%) and mixed global sessions, framed the regime as risk_on, scored S0_SHARED_MACRO 0, S4_ETF_TAPE -1, and emitted flat/flat (total -0.9). It did not parse the live Hormuz/oil supply risk or the looming CPI print into S0/direction; the prose noted a divergence, but the deterministic output had divergence_flagged False."
evidence_cited: "Actual XLP -0.306%, SPY -0.320%, rel +0.013. Oil spiked >3% on Strait of Hormuz uncertainty ahead of July CPI, driving broad mild risk-off. Morning flat/flat missed both direction and magnitude under strict scoring. S3 flows/positioning (BofA defensive rotation + $551M XLP inflows) was validated by XLP's relative outperformance, but S0 macro was underweighted."
error_category: "B"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When an active geopolitical/oil/CPI risk-off catalyst is present, score S0_SHARED_MACRO negative/risk-off rather than neutral, keep magnitude capped at mild (not notable), and emit down/mild when the defensive ETF is already underperforming on the multi-day tape. Do not let a flat premarket futures print alone justify an absolute flat call. A defensive sector can fall about -0.3% absolutely while matching or slightly outperforming SPY.

## WHEN IT FIRES
A defensive/staples sector prediction is set to flat solely because premarket US futures are flat and global sessions are mixed, while an active geopolitical/oil supply-shock headline (e.g., Strait-of-Hormuz style impasse) and/or a scheduled high-impact CPI print is present in the news cycle. The model treats the day as neutral/risk-on, scores S0_SHARED_MACRO at 0 or positive, and emits flat/flat — but a defensive ETF can still fall modestly in absolute terms on broad risk-off while outperforming SPY relatively, so flat/flat is a graded miss.

## WRONG IF
If a later morning has the same active geopolitical/oil/CPI-risk signature and XLP closes positive, or rises more than ~0.2% absolutely, or outperforms SPY by more than ~0.5%, this lesson is overcorrecting; the defensive-bid signal would then be strong enough to justify flat/up, and the rule should be revised.

## EVIDENCE
Actual XLP -0.306%, SPY -0.320%, rel +0.013. Oil spiked >3% on Strait of Hormuz uncertainty ahead of July CPI, driving broad mild risk-off. Morning flat/flat missed both direction and magnitude under strict scoring. S3 flows/positioning (BofA defensive rotation + $551M XLP inflows) was validated by XLP's relative outperformance, but S0 macro was underweighted.

(learn_cycle promote)
