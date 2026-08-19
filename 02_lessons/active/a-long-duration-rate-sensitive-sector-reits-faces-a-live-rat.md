---
trigger_pattern: "A long-duration, rate-sensitive sector (REITs) faces a live rate spine of rising long-end yields — 30Y at a multi-decade high, real yields higher, 10Y rising — on a risk-off tape, while the sector’s 1d/3d relative tape is positive/defensive. The correct output is down/mild: the leading rate shock drives absolute direction down, and the defensive relative bid caps magnitude at mild rather than allowing notable/severe."
corrected_behavior: "No behavioral correction is needed. Continue to treat a live long-end rate shock at a multi-decade high as directionally dominant for REITs, while treating short-term positive relative tape as a magnitude cap, not an absolute up signal. Optionally reconcile the internal total-score/divergence-flag inconsistency for cleanliness, but it did not affect the emitted call."
falsifier: "A future identical setup — 30Y at a 19-year high, rising real yields, sharply negative futures, XLRE 1d/3d relative strength — that closes up, or with a notable/severe decline, would weaken the down/mild rule. A strong up close would suggest the defensive bid was a leading signal rather than only a magnitude cap."
current_behavior: "Predicted down/mild by correctly weighting the live rate tape over any prior easing/positive default. Scored S0=-1 and S1=-1 on rising real yields and the 30Y multi-decade high, kept S2/S4 neutral because relative defensiveness is not an absolute up signal, and held the magnitude at mild despite the negative rate tape."
evidence_cited: "2026-08-18 XLRE closed -0.446% vs SPY -0.676%, rel +0.229%. Predicted down/mild; actual down/mild. The 30Y Treasury reached ~5.30-5.33%, a 19-year high; real yields and 10Y were rising; futures were sharply negative. The defensive relative tape was confirmed by XLRE outperforming SPY, exactly capping the loss at mild."
error_category: "NONE"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
No behavioral correction is needed. Continue to treat a live long-end rate shock at a multi-decade high as directionally dominant for REITs, while treating short-term positive relative tape as a magnitude cap, not an absolute up signal. Optionally reconcile the internal total-score/divergence-flag inconsistency for cleanliness, but it did not affect the emitted call.

## WHEN IT FIRES
A long-duration, rate-sensitive sector (REITs) faces a live rate spine of rising long-end yields — 30Y at a multi-decade high, real yields higher, 10Y rising — on a risk-off tape, while the sector’s 1d/3d relative tape is positive/defensive. The correct output is down/mild: the leading rate shock drives absolute direction down, and the defensive relative bid caps magnitude at mild rather than allowing notable/severe.

## WRONG IF
A future identical setup — 30Y at a 19-year high, rising real yields, sharply negative futures, XLRE 1d/3d relative strength — that closes up, or with a notable/severe decline, would weaken the down/mild rule. A strong up close would suggest the defensive bid was a leading signal rather than only a magnitude cap.

## EVIDENCE
2026-08-18 XLRE closed -0.446% vs SPY -0.676%, rel +0.229%. Predicted down/mild; actual down/mild. The 30Y Treasury reached ~5.30-5.33%, a 19-year high; real yields and 10Y were rising; futures were sharply negative. The defensive relative tape was confirmed by XLRE outperforming SPY, exactly capping the loss at mild.

(learn_cycle promote)
