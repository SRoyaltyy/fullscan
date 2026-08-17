---
trigger_pattern: "A catalyst is labeled “fresh positive” based on deal size and participant names without checking how the market already traded it after the announcement. The catalyst is actually stale-negative (supplier financing its own customers = circular-financing alarm) and the relevant stock already fell days earlier on the same news. This stale-positive read is counted in S1, the multiplier, and the fresh-catalyst list, while a scheduled high-impact macro release is pending and the tech tape is crowded/extended."
corrected_behavior: "Before scoring any catalyst, verify how the market already reacted to it. If the relevant stock/ETF fell on the news, classify it as stale-negative or neutral, not fresh-positive, and do not use it as a positive S1 driver, multiplier input, or fresh-catalyst support. Enumerate scheduled high-impact data at open; if one is due and tech is crowded/extended, treat futures as provisional, cap magnitude at MILD or lower, and explicitly include a flat/down scenario. Reconcile the narrative magnitude cap with the deterministic pipeline score so the pipeline cannot override a lesson-based cap."
falsifier: "The lesson would be weakened if the same known-negative/stale catalyst is followed by a broad up/notable tech day with no new positive catalyst, because requiring market confirmation would have caused an undercall. Also, if scheduled consumer-data prints routinely fail to hurt crowded tech, the scheduled-data downside component should be de-weighted."
current_behavior: "Treats large announced deals as unambiguously positive; does not inspect post-announcement price action; triple-counts a single catalyst; treats mild pre-market futures as confirmation; ignores scheduled-data risk; lets the deterministic pipeline emit NOTABLE even when the narrative cap is MILD."
evidence_cited: "XLK closed -0.40% vs SPY -0.20%; Broadcom -6% and AI/semis led declines; the Nvidia $500B financing deal was read as circular financing rather than a positive catalyst, and Nvidia had already fallen ~2.5% on Aug 10 on that exact news; July retail sales fell -0.6% and consumer sentiment dropped to 51."
error_category: "A"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring any catalyst, verify how the market already reacted to it. If the relevant stock/ETF fell on the news, classify it as stale-negative or neutral, not fresh-positive, and do not use it as a positive S1 driver, multiplier input, or fresh-catalyst support. Enumerate scheduled high-impact data at open; if one is due and tech is crowded/extended, treat futures as provisional, cap magnitude at MILD or lower, and explicitly include a flat/down scenario. Reconcile the narrative magnitude cap with the deterministic pipeline score so the pipeline cannot override a lesson-based cap.

## WHEN IT FIRES
A catalyst is labeled “fresh positive” based on deal size and participant names without checking how the market already traded it after the announcement. The catalyst is actually stale-negative (supplier financing its own customers = circular-financing alarm) and the relevant stock already fell days earlier on the same news. This stale-positive read is counted in S1, the multiplier, and the fresh-catalyst list, while a scheduled high-impact macro release is pending and the tech tape is crowded/extended.

## WRONG IF
The lesson would be weakened if the same known-negative/stale catalyst is followed by a broad up/notable tech day with no new positive catalyst, because requiring market confirmation would have caused an undercall. Also, if scheduled consumer-data prints routinely fail to hurt crowded tech, the scheduled-data downside component should be de-weighted.

## EVIDENCE
XLK closed -0.40% vs SPY -0.20%; Broadcom -6% and AI/semis led declines; the Nvidia $500B financing deal was read as circular financing rather than a positive catalyst, and Nvidia had already fallen ~2.5% on Aug 10 on that exact news; July retail sales fell -0.6% and consumer sentiment dropped to 51.

(learn_cycle promote)
