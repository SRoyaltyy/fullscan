---
trigger_pattern: "A concentrated sector ETF (two mega-cap names ≥35% combined weight) is predicted down on a risk-off/oil-shock macro day, where the only non-zero score components are (a) shared macro and (b) a breadth term derived from a broad-tech index (NQ/ES) that does not represent the ETF's actual holdings composition."
corrected_behavior: "(1) Never map NQ/ES divergence onto a two-name book — NQ composition (semis/software/AI-infra) is not XLC composition; the morning itself flagged AMAT/LITE/ALAB as premarket leaders *outside* XLC, which is the tell that the NQ weakness was exogenous to the sector. (2) When the only non-zero components are correlated expressions of the same regime (oil shock → risk-off → broad tech lag), collapse them to a single negative rather than stacking S0 and S2 as independent; the effective negative was ~−1.5, not −2. (3) Do not score S1=0 by default for a concentrated mega-cap FCF book on a risk-off day — absence of a fresh catalyst is not absence of relative strength; large-cap cash-generative platforms are natural rotation destinations in oil-shock tapes. (4) A 'no divergence' verdict built on two correlated negatives is not confirmation — it is double-counting, and should trigger a confidence cut, not a confidence hold."
falsifier: "If on a future risk-off/oil-shock day XLC's two anchors are demonstrably not being bought (e.g., META and GOOGL both red in premarket with fresh negative single-name news) and XLC still underperforms, then the 'mega-cap FCF rotation destination' prior is wrong and S1=0-by-default would be vindicated. Conversely, if NQ/ES divergence is ever shown to lead XLC relative returns with positive hit-rate over ≥5 observations, the proxy-mismatch claim is weakened."
current_behavior: "Model scored S0=−1 (oil/hawkish macro) and S2=−1 (NQ lags ES → 'large-cap failure inside the sector'), treated the two as independent confirmation ('no divergence, factors and tape agree in sign'), left S1=0 by default because no fresh single-name catalyst existed, and concluded down/mild. The NQ/ES gap was imported as a direct proxy for META/GOOGL participation."
evidence_cited: "XLC +0.60% vs SPY −0.60% → rel +1.20% (notable), full-session grind higher (open 110.62, low 110.53, close 111.50) against SPY −0.60% / Nasdaq −0.70%. A +1.20% relative move on a −0.60% tape is arithmetically near-impossible without both META and Alphabet strongly green. Morning HIT_GRID 'Large-cap leadership inside sector | MISS | 0.60' is the single most costly grid entry. S0 macro call was directionally correct (SPY/Nasdaq both down) but was swamped; S2 sign-flipped."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
(1) Never map NQ/ES divergence onto a two-name book — NQ composition (semis/software/AI-infra) is not XLC composition; the morning itself flagged AMAT/LITE/ALAB as premarket leaders *outside* XLC, which is the tell that the NQ weakness was exogenous to the sector. (2) When the only non-zero components are correlated expressions of the same regime (oil shock → risk-off → broad tech lag), collapse them to a single negative rather than stacking S0 and S2 as independent; the effective negative was ~−1.5, not −2. (3) Do not score S1=0 by default for a concentrated mega-cap FCF book on a risk-off day — absence of a fresh catalyst is not absence of relative strength; large-cap cash-generative platforms are natural rotation destinations in oil-shock tapes. (4) A "no divergence" verdict built on two correlated negatives is not confirmation — it is double-counting, and should trigger a confidence cut, not a confidence hold.

## WHEN IT FIRES
A concentrated sector ETF (two mega-cap names ≥35% combined weight) is predicted down on a risk-off/oil-shock macro day, where the only non-zero score components are (a) shared macro and (b) a breadth term derived from a broad-tech index (NQ/ES) that does not represent the ETF's actual holdings composition.

## WRONG IF
If on a future risk-off/oil-shock day XLC's two anchors are demonstrably not being bought (e.g., META and GOOGL both red in premarket with fresh negative single-name news) and XLC still underperforms, then the "mega-cap FCF rotation destination" prior is wrong and S1=0-by-default would be vindicated. Conversely, if NQ/ES divergence is ever shown to lead XLC relative returns with positive hit-rate over ≥5 observations, the proxy-mismatch claim is weakened.

## EVIDENCE
XLC +0.60% vs SPY −0.60% → rel +1.20% (notable), full-session grind higher (open 110.62, low 110.53, close 111.50) against SPY −0.60% / Nasdaq −0.70%. A +1.20% relative move on a −0.60% tape is arithmetically near-impossible without both META and Alphabet strongly green. Morning HIT_GRID "Large-cap leadership inside sector | MISS | 0.60" is the single most costly grid entry. S0 macro call was directionally correct (SPY/Nasdaq both down) but was swamped; S2 sign-flipped.

(learn_cycle promote)
