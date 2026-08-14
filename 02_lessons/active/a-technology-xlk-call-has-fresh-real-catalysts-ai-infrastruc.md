---
trigger_pattern: "A Technology/XLK call has fresh, real catalysts (AI-infrastructure earnings: SMCI/CRWV/NBIS) but those catalysts were reported after the prior close and are already embedded in the strong 1d/1w relative tape. The macro driver (benign CPI) already produced the prior day’s rally; US equity futures, especially NQ, are flat/non-confirming; a scheduled 8:30 ET data release (PPI) is pending. The model converts fresh earnings catalysts plus strong S4 tape into NOTABLE without recognizing that S4 is partly double-counting S1 and that flat futures should cap the day at MILD."
corrected_behavior: "When the dominant catalyst is already reflected in the prior tape and NQ futures are flat/non-positive, cap the day at up/mild (multiplier ≤1.0). Treat flat NQ as failing the “positive futures confirmation” requirement from the 08-12 no-error lesson. Do not use S4 tape as an independent magnitude booster if S4 is the market’s repricing of the same S1 catalysts. In a broad SPY record-rally regime, expect relative outperformance to compress unless NQ futures are clearly leading and XLK breadth confirms."
falsifier: "The rule is falsified if a later Technology session has the same setup — fresh carried AI-infra catalyst, NQ futures flat at 0.0%, PPI pending, strong prior-day tape — and XLK still delivers relative return ≥0.8% or an absolute gain >1.5% against a flat/negative SPY. That would show flat NQ futures do not cap XLK magnitude in the AI-infra regime. Also, if the formal scoreboard band defines +1.01% absolute as notable, then the disagreement between scoreboard and the MILD label must be resolved first; otherwise the lesson is unfalsifiable."
current_behavior: "On a follow-through day with fresh catalysts but flat NQ futures (0.0%), the model still emits up/notable, multiplier 1.2, with S4 tape used as an independent magnitude additive. It treats the 08-12 no-error lesson as satisfied even though the futures-confirmation leg is missing, and it does not account for the fact that a strong broad-market SPY rally dilutes XLK’s relative edge even when AI-infra leaders pop."
evidence_cited: "Prediction 2026-08-13 was up/notable, total 10.8, multiplier 1.2, S1=+2, S4=+1, with text explicitly noting “NQ futures FLAT (0.0%)” and “PPI pending.” Actual: XLK +1.01%, SPY +0.70%, rel +0.31%; outcome classifies actual magnitude as MILD and the post-session review says “magnitude overcalled — predicted notable, delivered mild.” The review also notes the +1.24% tape rel over-predicted actual +0.31% rel. SMCI +13%, CRWV +19%, NBIS +23% were real, but NVDA faded to ~flat, so XLK’s relative edge was diluted. Scoreboard shows magnitude_hit True, which conflicts with the outcome’s MILD label — that scoreboard band should be re-audited."
error_category: "B"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When the dominant catalyst is already reflected in the prior tape and NQ futures are flat/non-positive, cap the day at up/mild (multiplier ≤1.0). Treat flat NQ as failing the “positive futures confirmation” requirement from the 08-12 no-error lesson. Do not use S4 tape as an independent magnitude booster if S4 is the market’s repricing of the same S1 catalysts. In a broad SPY record-rally regime, expect relative outperformance to compress unless NQ futures are clearly leading and XLK breadth confirms.

## WHEN IT FIRES
A Technology/XLK call has fresh, real catalysts (AI-infrastructure earnings: SMCI/CRWV/NBIS) but those catalysts were reported after the prior close and are already embedded in the strong 1d/1w relative tape. The macro driver (benign CPI) already produced the prior day’s rally; US equity futures, especially NQ, are flat/non-confirming; a scheduled 8:30 ET data release (PPI) is pending. The model converts fresh earnings catalysts plus strong S4 tape into NOTABLE without recognizing that S4 is partly double-counting S1 and that flat futures should cap the day at MILD.

## WRONG IF
The rule is falsified if a later Technology session has the same setup — fresh carried AI-infra catalyst, NQ futures flat at 0.0%, PPI pending, strong prior-day tape — and XLK still delivers relative return ≥0.8% or an absolute gain >1.5% against a flat/negative SPY. That would show flat NQ futures do not cap XLK magnitude in the AI-infra regime. Also, if the formal scoreboard band defines +1.01% absolute as notable, then the disagreement between scoreboard and the MILD label must be resolved first; otherwise the lesson is unfalsifiable.

## EVIDENCE
Prediction 2026-08-13 was up/notable, total 10.8, multiplier 1.2, S1=+2, S4=+1, with text explicitly noting “NQ futures FLAT (0.0%)” and “PPI pending.” Actual: XLK +1.01%, SPY +0.70%, rel +0.31%; outcome classifies actual magnitude as MILD and the post-session review says “magnitude overcalled — predicted notable, delivered mild.” The review also notes the +1.24% tape rel over-predicted actual +0.31% rel. SMCI +13%, CRWV +19%, NBIS +23% were real, but NVDA faded to ~flat, so XLK’s relative edge was diluted. Scoreboard shows magnitude_hit True, which conflicts with the outcome’s MILD label — that scoreboard band should be re-audited.

(learn_cycle promote)
