---
trigger_pattern: "On a scheduled release day, a sector’s own high-impact hard-data print is pending; consensus is below the prior reading and the forward-looking component is a spending lead. The model names the print “the sector’s own print” but then treats it as neutral event risk in S1, while giving full positive credit to a broad risk-on tape in S0. When the print resolves below consensus, the discretionary ETF underperforms even on a rising SPY."
corrected_behavior: "When a same-day sector-owned print is scheduled with consensus below prior and the relevant component is forward-looking (e.g., Conference Board Expectations), do not neutralize it as pure event risk. Score a conditional downside tilt — either S1 slightly negative or S0 capped/discounted — rather than emitting a full positive call. Do not use “do not score a collapse that has not printed” to mean “no scheduled-downside risk”; distinguish not-yet-realized from not-risky."
falsifier: "If a same-day sector-owned print with consensus below prior resolves below consensus, but the sector ETF still closes up and outperforms SPY by more than ~0.5% on a risk-on tape with no offsetting sector-specific positive, then the “sector’s own print overrides broad tape” claim is falsified and this lesson should be demoted."
current_behavior: "Pending same-day sector-specific data is treated as unprinted/two-sided and therefore neutral in S1. Broad futures risk-on drives S0 = +1, and the output is up/flat with no downside tilt or confidence discount for the scheduled sector-own print."
evidence_cited: "2026-08-25 Conference Board printed 89.4 vs ~90.3 consensus and 90.2 prior; the Expectations Index fell to 68.2, a seven-month low. XLY closed -0.30% while SPY rose +0.32%; XLY relative performance was -0.62%. Consumer staples/financials led while discretionary lagged, and AMZN/TSLA reversed lower. The print was knowable as scheduled, consensus was below prior, and the morning explicitly identified it as the sector’s own catalyst."
error_category: "A"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When a same-day sector-owned print is scheduled with consensus below prior and the relevant component is forward-looking (e.g., Conference Board Expectations), do not neutralize it as pure event risk. Score a conditional downside tilt — either S1 slightly negative or S0 capped/discounted — rather than emitting a full positive call. Do not use “do not score a collapse that has not printed” to mean “no scheduled-downside risk”; distinguish not-yet-realized from not-risky.

## WHEN IT FIRES
On a scheduled release day, a sector’s own high-impact hard-data print is pending; consensus is below the prior reading and the forward-looking component is a spending lead. The model names the print “the sector’s own print” but then treats it as neutral event risk in S1, while giving full positive credit to a broad risk-on tape in S0. When the print resolves below consensus, the discretionary ETF underperforms even on a rising SPY.

## WRONG IF
If a same-day sector-owned print with consensus below prior resolves below consensus, but the sector ETF still closes up and outperforms SPY by more than ~0.5% on a risk-on tape with no offsetting sector-specific positive, then the “sector’s own print overrides broad tape” claim is falsified and this lesson should be demoted.

## EVIDENCE
2026-08-25 Conference Board printed 89.4 vs ~90.3 consensus and 90.2 prior; the Expectations Index fell to 68.2, a seven-month low. XLY closed -0.30% while SPY rose +0.32%; XLY relative performance was -0.62%. Consumer staples/financials led while discretionary lagged, and AMZN/TSLA reversed lower. The print was knowable as scheduled, consensus was below prior, and the morning explicitly identified it as the sector’s own catalyst.

(learn_cycle promote)
