---
trigger_pattern: "A long-duration, rate-sensitive sector (REITs/utilities) is called up/positive because prior-session real yields were easing and a CPI print was cool, but the live morning tape contains a long-end Treasury selloff/term-premium repricing (30Y near multi-decade high), oil/geopolitical supply-risk headlines, and hawkish Fed repricing. The model double-counts the same “duration relief” theme across S0/S1/S4, underweights the live rate shock, and leaves the official band unreconciled with the deterministic pipeline."
corrected_behavior: "Before scoring a REIT/long-duration up call, run an explicit live-rate check: are 10Y/30Y yields rising at open, is the 30Y at/near a multi-decade high, is there a Treasury supply/term-premium story, is oil/geopolitical inflation risk active, is Fed repricing hawkish? If yes, S0 and S1 must reflect the negative long-end rate shock, and direction should be down/negative for the rate-sensitive sector. Do not count the same easing-rate theme separately in S0, S1, and S4; reconcile the final official band to the deterministic Σ×mult using the 08-14 pipeline-mismatch lesson."
falsifier: "If XLRE still rises/outperforms SPY on a day when 10Y/30Y are rising to multi-decade highs, oil/geopolitical risk is live, and hawkish Fed repricing is firm, the rule would be falsified. A clear dovish reversal (e.g., Fed cut odds increasing) would legitimately suspend the rule. If XLRE repeatedly de-correlates from long-end rates, the lesson should be retired."
current_behavior: "Uses prior-day DFII10/10Y easing and the 08-12 CPI-relief lesson as the spine positive; treats oil-up + US-Iran deadlock + 64% hike probability as non-triggering because there is no named Hormuz-style headline; carries the easing-yield theme into S0, S1, and S4 as if they were independent positives; and leaves the official scoreboard/pipeline band (up/notable) unreconciled with the prose band (up/mild)."
evidence_cited: "XLRE fell -0.97% vs SPY -0.47% on 2026-08-17 (rel -0.50%) after the morning predicted up/notable; 30Y hit 5.311% (19-year high), 10Y rose to 4.73%; oil rose above $81.50 on US-Iran deadlock; market priced 64% probability of a hike by year-end. The morning noted EPU spiked and oil up but dismissed these as non-triggering; S0/S1/S2/S4 all failed on the same easing-yield assumption."
error_category: "C"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring a REIT/long-duration up call, run an explicit live-rate check: are 10Y/30Y yields rising at open, is the 30Y at/near a multi-decade high, is there a Treasury supply/term-premium story, is oil/geopolitical inflation risk active, is Fed repricing hawkish? If yes, S0 and S1 must reflect the negative long-end rate shock, and direction should be down/negative for the rate-sensitive sector. Do not count the same easing-rate theme separately in S0, S1, and S4; reconcile the final official band to the deterministic Σ×mult using the 08-14 pipeline-mismatch lesson.

## WHEN IT FIRES
A long-duration, rate-sensitive sector (REITs/utilities) is called up/positive because prior-session real yields were easing and a CPI print was cool, but the live morning tape contains a long-end Treasury selloff/term-premium repricing (30Y near multi-decade high), oil/geopolitical supply-risk headlines, and hawkish Fed repricing. The model double-counts the same “duration relief” theme across S0/S1/S4, underweights the live rate shock, and leaves the official band unreconciled with the deterministic pipeline.

## WRONG IF
If XLRE still rises/outperforms SPY on a day when 10Y/30Y are rising to multi-decade highs, oil/geopolitical risk is live, and hawkish Fed repricing is firm, the rule would be falsified. A clear dovish reversal (e.g., Fed cut odds increasing) would legitimately suspend the rule. If XLRE repeatedly de-correlates from long-end rates, the lesson should be retired.

## EVIDENCE
XLRE fell -0.97% vs SPY -0.47% on 2026-08-17 (rel -0.50%) after the morning predicted up/notable; 30Y hit 5.311% (19-year high), 10Y rose to 4.73%; oil rose above $81.50 on US-Iran deadlock; market priced 64% probability of a hike by year-end. The morning noted EPU spiked and oil up but dismissed these as non-triggering; S0/S1/S2/S4 all failed on the same easing-yield assumption.

(learn_cycle promote)
