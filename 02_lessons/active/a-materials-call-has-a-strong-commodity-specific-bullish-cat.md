---
trigger_pattern: "A materials call has a strong commodity-specific bullish catalyst (fresh copper squeeze / record backwardation) while the same-morning macro tape is risk-off: US futures flat-to-mildly-negative, prior session SPY dragged lower by oil/stagflation, a China hard-data miss is released, and oil/geopolitical headlines are live. The model focuses on the sector-specific positive and misreads flat futures/Asia strength as bad-news-good confirmation, producing an up/severe call that is reversed by the macro drag."
corrected_behavior: "Before weighting sector-specific positives, check the macro risk-off overlay. If US futures are flat-to-negative, the prior session saw SPY lower on oil/stagflation concerns, and a same-morning China hard-data miss is live, do not build an up/severe materials call on copper strength alone. Cap S1/S4 so the call is at most flat/defensive, require explicit acknowledgment of the macro override, and verify that the narrative SECTOR_SCORES total matches the deterministic pipeline total before grading."
falsifier: "If this exact setup recurs — flat/weak US futures, prior SPY lower on oil/stagflation, same-day China hard-data miss, copper in record backwardation — and XLB still closes up with positive relative performance, then the lesson is too strong and should be narrowed to magnitude-only or discarded."
current_behavior: "When copper is at record highs with a fresh supply squeeze and XLB 1d relative tape is positive, the model scores S1=2 and S4=1, applies a positive multiplier, and predicts up/severe even when broader equity conditions are fragile. It explicitly dismisses oil/geopolitical risk as “no headline” and treats Asia strength as confirmation, allowing the commodity story to override the macro tape."
evidence_cited: "2026-08-17 XLB predicted up/severe; actual XLB -0.571%, SPY -0.473%, relative -0.098%. The model cited record copper backwardation and a fresh squeeze, but the market reversed on Middle East/oil risk-off and a China macro-miss backdrop (see candidate lesson 2026-08-17). The narrative SECTOR_SCORES block said up/notable 6.6 while the pipeline printed up/severe 13.2; the scoreboard graded the pipeline output, making the magnitude miss worse than the reasoning miss alone."
error_category: "A"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
Before weighting sector-specific positives, check the macro risk-off overlay. If US futures are flat-to-negative, the prior session saw SPY lower on oil/stagflation concerns, and a same-morning China hard-data miss is live, do not build an up/severe materials call on copper strength alone. Cap S1/S4 so the call is at most flat/defensive, require explicit acknowledgment of the macro override, and verify that the narrative SECTOR_SCORES total matches the deterministic pipeline total before grading.

## WHEN IT FIRES
A materials call has a strong commodity-specific bullish catalyst (fresh copper squeeze / record backwardation) while the same-morning macro tape is risk-off: US futures flat-to-mildly-negative, prior session SPY dragged lower by oil/stagflation, a China hard-data miss is released, and oil/geopolitical headlines are live. The model focuses on the sector-specific positive and misreads flat futures/Asia strength as bad-news-good confirmation, producing an up/severe call that is reversed by the macro drag.

## WRONG IF
If this exact setup recurs — flat/weak US futures, prior SPY lower on oil/stagflation, same-day China hard-data miss, copper in record backwardation — and XLB still closes up with positive relative performance, then the lesson is too strong and should be narrowed to magnitude-only or discarded.

## EVIDENCE
2026-08-17 XLB predicted up/severe; actual XLB -0.571%, SPY -0.473%, relative -0.098%. The model cited record copper backwardation and a fresh squeeze, but the market reversed on Middle East/oil risk-off and a China macro-miss backdrop (see candidate lesson 2026-08-17). The narrative SECTOR_SCORES block said up/notable 6.6 while the pipeline printed up/severe 13.2; the scoreboard graded the pipeline output, making the magnitude miss worse than the reasoning miss alone.

(learn_cycle promote)
