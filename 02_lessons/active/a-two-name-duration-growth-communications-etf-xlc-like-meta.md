---
trigger_pattern: "A two-name duration/growth communications ETF (XLC-like: META+GOOGL dominate) posts an all-zero / S4=0 factor card because live PM is absent or flat and the two anchors are mixed, while overnight NQ/ES are green vs prior close (and/or an unprinted same-session FOMC+SEP+presser suppressor is still open). The v2 engine then writes official direction=up from tape_anchor + index_carry anyway."
corrected_behavior: "If XLC S4=0 — not on the PM sector board, live print ~flat, META/GOOGL not participating together — official direction must follow the factor card (flat), not tape_anchor/index_carry. Green NQ/ES/XLK is not an XLC participation certificate. An unprinted FOMC+SEP+presser independently forbids up. Do not convert a correct all-zero card into an up call via futures overlay."
falsifier: "Live XLC PM is actually green and on the sector board, and META and GOOGL are both bid together — then NQ/ES tape_anchor agreeing with S4>0 is real participation, not a mapping error. Also falsified if FOMC+SEP+presser is already printed and XLC is catching a known risk-on, or if the two-name book is printing a same-morning ad/AI HIT that S1 should pay."
current_behavior: "LLM S0–S4 are scored 0 with an explicit ban on mapping NQ/ES onto XLC and an 08-13 FOMC suppressor against an up call, but official predicted_direction still inherits the NQ/ES overnight rebound (here tape_anchor 7.92 on NQ +1.50% / ES +1.14% plus index_carry) and emits up/mild."
evidence_cited: "2026-09-16 engine up/mild (total 9.244) vs XLC −0.90% / SPY −0.44% / rel −0.46% (dir MISS, mild HIT). LLM call was flat/flat, leading_sum 0, divergence_flagged false. Path: gap-up ~+0.17% then post-14:00 fade to 113.00. Driver was hawkish SEP/dots + Warsh after a priced 25 bp hike, not ads/AI. META +0.46% vs GOOGL −0.73%; T/VZ/CMCSA ~−3% (~half the ETF print). Same class as 08-27 / 09-10."
error_category: "D"
scope: "ops"
date: "2026-09-16"
status: "active"
occurrences: "1"
promoted_on: "2026-09-16"
sources: "['2026-09-16_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
If XLC S4=0 — not on the PM sector board, live print ~flat, META/GOOGL not participating together — official direction must follow the factor card (flat), not tape_anchor/index_carry. Green NQ/ES/XLK is not an XLC participation certificate. An unprinted FOMC+SEP+presser independently forbids up. Do not convert a correct all-zero card into an up call via futures overlay.

## WHEN IT FIRES
A two-name duration/growth communications ETF (XLC-like: META+GOOGL dominate) posts an all-zero / S4=0 factor card because live PM is absent or flat and the two anchors are mixed, while overnight NQ/ES are green vs prior close (and/or an unprinted same-session FOMC+SEP+presser suppressor is still open). The v2 engine then writes official direction=up from tape_anchor + index_carry anyway.

## WRONG IF
Live XLC PM is actually green and on the sector board, and META and GOOGL are both bid together — then NQ/ES tape_anchor agreeing with S4>0 is real participation, not a mapping error. Also falsified if FOMC+SEP+presser is already printed and XLC is catching a known risk-on, or if the two-name book is printing a same-morning ad/AI HIT that S1 should pay.

## EVIDENCE
2026-09-16 engine up/mild (total 9.244) vs XLC −0.90% / SPY −0.44% / rel −0.46% (dir MISS, mild HIT). LLM call was flat/flat, leading_sum 0, divergence_flagged false. Path: gap-up ~+0.17% then post-14:00 fade to 113.00. Driver was hawkish SEP/dots + Warsh after a priced 25 bp hike, not ads/AI. META +0.46% vs GOOGL −0.73%; T/VZ/CMCSA ~−3% (~half the ETF print). Same class as 08-27 / 09-10.

(learn_cycle promote)
