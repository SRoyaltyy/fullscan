# Sector Reflect — Real Estate — 2026-08-17

LESSON_BEGIN
ERROR_CATEGORY: C
TRIGGER_PATTERN: A long-duration, rate-sensitive sector (REITs/utilities) is called up/positive because prior-session real yields were easing and a CPI print was cool, but the live morning tape contains a long-end Treasury selloff/term-premium repricing (30Y near multi-decade high), oil/geopolitical supply-risk headlines, and hawkish Fed repricing. The model double-counts the same “duration relief” theme across S0/S1/S4, underweights the live rate shock, and leaves the official band unreconciled with the deterministic pipeline.

CURRENT_BEHAVIOR: Uses prior-day DFII10/10Y easing and the 08-12 CPI-relief lesson as the spine positive; treats oil-up + US-Iran deadlock + 64% hike probability as non-triggering because there is no named Hormuz-style headline; carries the easing-yield theme into S0, S1, and S4 as if they were independent positives; and leaves the official scoreboard/pipeline band (up/notable) unreconciled with the prose band (up/mild).

CORRECTED_BEHAVIOR: Before scoring a REIT/long-duration up call, run an explicit live-rate check: are 10Y/30Y yields rising at open, is the 30Y at/near a multi-decade high, is there a Treasury supply/term-premium story, is oil/geopolitical inflation risk active, is Fed repricing hawkish? If yes, S0 and S1 must reflect the negative long-end rate shock, and direction should be down/negative for the rate-sensitive sector. Do not count the same easing-rate theme separately in S0, S1, and S4; reconcile the final official band to the deterministic Σ×mult using the 08-14 pipeline-mismatch lesson.

EVIDENCE: XLRE fell -0.97% vs SPY -0.47% on 2026-08-17 (rel -0.50%) after the morning predicted up/notable; 30Y hit 5.311% (19-year high), 10Y rose to 4.73%; oil rose above $81.50 on US-Iran deadlock; market priced 64% probability of a hike by year-end. The morning noted EPU spiked and oil up but dismissed these as non-triggering; S0/S1/S2/S4 all failed on the same easing-yield assumption.

LESSON_MATCH_CHECK: Strongly matches active 08-11 geopolitical-oil-supply-risk-off lesson (US-Iran deadlock + oil up should have flipped risk sentiment) and the 08-12 REIT duration lesson (but only if duration relief is actually occurring; it was not). Also matches the 08-17 sector_financial candidate lesson about a bearish long-end Treasury selloff hurting rate-sensitive sectors. No real-estate-specific lesson yet covers this exact live-rate reversal, so this lesson should be added for Real Estate.

BACKWARD_CHECK: Corrected behavior would have changed 08-17 from up to down; it would not have reversed 08-13/08-14 up calls that followed actual easing/CPI relief, and it is consistent with 08-10/08-11 down predictions. It only forces down when live long-end rates/geopolitical/hawkish repricing are negative, not on every REIT day.

CONFLICT_CHECK: The 08-12 lesson (“don’t default REIT S0 negative after cool CPI”) can conflict with this new live-rate rule. Resolve by temporal priority: live rate tape at open > prior-day CPI/easing extrapolation. If yields are rising to multi-decade highs at open, the 08-12 positive default does not apply. Also, flat futures should cap magnitude but must not be used to keep an up direction in the face of a negative rate shock.

FALSIFIER: If XLRE still rises/outperforms SPY on a day when 10Y/30Y are rising to multi-decade highs, oil/geopolitical risk is live, and hawkish Fed repricing is firm, the rule would be falsified. A clear dovish reversal (e.g., Fed cut odds increasing) would legitimately suspend the rule. If XLRE repeatedly de-correlates from long-end rates, the lesson should be retired.

DIVERGENCE_VERDICT: none_flagged

ACTIVE_LESSON_REVIEW: 08-11 was applied but under-applied (should have treated oil/US-Iran as live risk-off); 08-12 was applied but over-applied (used to keep S0 positive despite no live duration relief); 08-14 was partially applied in prose but the official pipeline/scoreboard band remained up/notable. Candidate 08-17 financial lesson should be considered for adoption to sector Real Estate.

SECTOR: Real Estate
LESSON_END
