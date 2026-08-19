---
trigger_pattern: "A defensive bond-proxy sector (Utilities) faces a risk-off tape while long-end/10Y yields are rising. The model sees a fresh geopolitical defensive bid, positive relative tape, and confirmed inflows, scores all components positive, and treats the “fresh” risk-off catalyst as an exception to the standing relative-vs-absolute lesson. The result is an absolute up call when the correct default is relative outperformance with a flat-to-negative absolute close."
corrected_behavior: "When a bond-proxy sector has both a risk-off defensive bid and rising 10Y/long-end yields, default to relative outperformance / flat-to-negative absolute. Do not upgrade to absolute up just because the risk-off catalyst is fresh. Weight the rising-yield headwind at least as heavily as the defensive bid. Reconcile narrative and pipeline to the same band; if yields dominate, score down/mild or down/flat unless a strong absolute offset is knowable at the open (e.g., flight-to-quality yield decline, large sector inflow, or sector-specific earnings catalyst)."
falsifier: "If on a day with 10Y yields clearly rising, XLU closes > +0.5% absolute while SPY is flat/down, the blanket “rising yields => flat-to-negative absolute” rule would be too rigid. It should then be narrowed to cases without an exceptional offset — e.g., a >1% AUM inflow, a sector-specific earnings catalyst, or a VIX spike >20 forcing outright flight-to-quality into bond-proxies."
current_behavior: "Scores S0–S4 all +1, leaves the deterministic pipeline at up/notable (total 9.0) while the narrative text says up/mild (total 5.0), and does not reconcile the two. It underweights the rising-yield bond-proxy headwind and uses the positive relative tape plus fresh risk-off to justify absolute upside."
evidence_cited: "2026-08-18 XLU opened 44.53, faded to 44.02, closing -0.36% (SPY -0.68%, rel +0.31%). Morning predicted up/notable (pipeline) / up/mild (narrative). 10Y rose from ~4.68% to 4.72–4.73%. The defensive bid was real but only relative; the bond-proxy yield drag won the absolute close. This repeats the 08-17 pattern: XLU -0.29% with positive relative tape."
error_category: "D"
scope: "ops"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
When a bond-proxy sector has both a risk-off defensive bid and rising 10Y/long-end yields, default to relative outperformance / flat-to-negative absolute. Do not upgrade to absolute up just because the risk-off catalyst is fresh. Weight the rising-yield headwind at least as heavily as the defensive bid. Reconcile narrative and pipeline to the same band; if yields dominate, score down/mild or down/flat unless a strong absolute offset is knowable at the open (e.g., flight-to-quality yield decline, large sector inflow, or sector-specific earnings catalyst).

## WHEN IT FIRES
A defensive bond-proxy sector (Utilities) faces a risk-off tape while long-end/10Y yields are rising. The model sees a fresh geopolitical defensive bid, positive relative tape, and confirmed inflows, scores all components positive, and treats the “fresh” risk-off catalyst as an exception to the standing relative-vs-absolute lesson. The result is an absolute up call when the correct default is relative outperformance with a flat-to-negative absolute close.

## WRONG IF
If on a day with 10Y yields clearly rising, XLU closes > +0.5% absolute while SPY is flat/down, the blanket “rising yields => flat-to-negative absolute” rule would be too rigid. It should then be narrowed to cases without an exceptional offset — e.g., a >1% AUM inflow, a sector-specific earnings catalyst, or a VIX spike >20 forcing outright flight-to-quality into bond-proxies.

## EVIDENCE
2026-08-18 XLU opened 44.53, faded to 44.02, closing -0.36% (SPY -0.68%, rel +0.31%). Morning predicted up/notable (pipeline) / up/mild (narrative). 10Y rose from ~4.68% to 4.72–4.73%. The defensive bid was real but only relative; the bond-proxy yield drag won the absolute close. This repeats the 08-17 pattern: XLU -0.29% with positive relative tape.

(learn_cycle promote)
