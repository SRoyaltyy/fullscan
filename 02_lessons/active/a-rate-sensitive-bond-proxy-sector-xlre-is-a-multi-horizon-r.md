---
trigger_pattern: "A rate-sensitive bond-proxy sector (XLRE) is a multi-horizon relative laggard with a negative 1d relative tape, on a live escalating macro shock day (oil >$100 / long-end Treasury backup / hawkish-Fed repricing), and the model pre-scores asymmetric downside (S0/S1/S2/S4 all −1) with the 09-04 lesson applied and the 09-08 cushion override correctly NOT fired — the call resolves as direction HIT and magnitude HIT, with realized relative underperformance (−0.23%) milder than the 1w/1m lag pattern (−1.51% / −0.85%) implied."
corrected_behavior: "Keep the current treatment. When (a) the sector is a rate-sensitive bond-proxy, (b) the live long-end curve is verified rising at the open (not a stale prior-close table), (c) a live escalating oil/inflation overlay is present, and (d) the 1d relative tape is negative (no ≥ +0.4% cushion, so the 09-08 override does not fire), pre-score asymmetric downside at down/mild and cap magnitude at mild per rolling mag discipline. Do NOT treat risk-off as an automatic REIT bid. Do NOT invoke the 09-08 cushion override when the 1d rel is negative. Do NOT let single large-caps (EQIX/DLR/WELL) define the ETF call. Count the oil shock once (S0 regime map) and the rate backup once (S1 spine) — but flag the S0/S1 correlation soft spot on hawkish-Fed days (see CONFLICT_CHECK)."
falsifier: "This lesson would be falsified if, on a future day with the same setup (rate-sensitive bond-proxy, live rising long-end curve, live escalating oil overlay, negative 1d relative tape, no ≥ +0.4% cushion), the sector instead (a) rallies on a flight-to-safety bid despite the rate backup, or (b) the realized magnitude lands at notable (|move| ≥ ~1.5%) rather than mild, or (c) the 09-08 cushion override fires on a negative 1d rel and is correct. Specifically: if XLRE closes up on a day when the long-end curve is verified rising and oil is >$100, the 'risk-off ≠ REIT bid' rule is falsified."
current_behavior: "On 2026-09-10 the model predicted XLRE down/mild (total_score −6.75, confidence 0.55), explicitly refused the flight-to-safety trap (risk-off ≠ REIT bid), verified the live curve was rising rather than forcing a 'rates falling' read off a stale table, checked and correctly declined the 09-08 cushion override (1d rel −0.65%, no +0.4% cushion), barred EQIX/DLR/WELL from defining the ETF call, and counted the oil shock once in S0 and the rate backup once in S1. Actual: XLRE −0.83% vs SPY −0.60%, rel −0.23% — direction HIT, magnitude HIT (inside mild, not at the upper edge)."
evidence_cited: "Deterministic actuals: ETF_PCT −0.829%, SPY_PCT −0.599%, REL_PCT −0.230%, OPEN 43.47 → CLOSE 43.05. Post-close confirmations: 'intense Treasury selloff' (tradingstrategyguides), 'Treasury yields rose' (Investopedia), 'Brent oil hits highest point since July' (TheStreet), 'fourth consecutive session' lower (Investopedia). Morning read: S0 −1, S1 −1, S2 −1, S3 0, S4 −1, mult 0.9 → −3.6 (pipeline −6.75), predicted down/mild. Scoreboard: direction_hit True, magnitude_hit True."
error_category: "NONE"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
Keep the current treatment. When (a) the sector is a rate-sensitive bond-proxy, (b) the live long-end curve is verified rising at the open (not a stale prior-close table), (c) a live escalating oil/inflation overlay is present, and (d) the 1d relative tape is negative (no ≥ +0.4% cushion, so the 09-08 override does not fire), pre-score asymmetric downside at down/mild and cap magnitude at mild per rolling mag discipline. Do NOT treat risk-off as an automatic REIT bid. Do NOT invoke the 09-08 cushion override when the 1d rel is negative. Do NOT let single large-caps (EQIX/DLR/WELL) define the ETF call. Count the oil shock once (S0 regime map) and the rate backup once (S1 spine) — but flag the S0/S1 correlation soft spot on hawkish-Fed days (see CONFLICT_CHECK).

## WHEN IT FIRES
A rate-sensitive bond-proxy sector (XLRE) is a multi-horizon relative laggard with a negative 1d relative tape, on a live escalating macro shock day (oil >$100 / long-end Treasury backup / hawkish-Fed repricing), and the model pre-scores asymmetric downside (S0/S1/S2/S4 all −1) with the 09-04 lesson applied and the 09-08 cushion override correctly NOT fired — the call resolves as direction HIT and magnitude HIT, with realized relative underperformance (−0.23%) milder than the 1w/1m lag pattern (−1.51% / −0.85%) implied.

## WRONG IF
This lesson would be falsified if, on a future day with the same setup (rate-sensitive bond-proxy, live rising long-end curve, live escalating oil overlay, negative 1d relative tape, no ≥ +0.4% cushion), the sector instead (a) rallies on a flight-to-safety bid despite the rate backup, or (b) the realized magnitude lands at notable (|move| ≥ ~1.5%) rather than mild, or (c) the 09-08 cushion override fires on a negative 1d rel and is correct. Specifically: if XLRE closes up on a day when the long-end curve is verified rising and oil is >$100, the "risk-off ≠ REIT bid" rule is falsified.

## EVIDENCE
Deterministic actuals: ETF_PCT −0.829%, SPY_PCT −0.599%, REL_PCT −0.230%, OPEN 43.47 → CLOSE 43.05. Post-close confirmations: "intense Treasury selloff" (tradingstrategyguides), "Treasury yields rose" (Investopedia), "Brent oil hits highest point since July" (TheStreet), "fourth consecutive session" lower (Investopedia). Morning read: S0 −1, S1 −1, S2 −1, S3 0, S4 −1, mult 0.9 → −3.6 (pipeline −6.75), predicted down/mild. Scoreboard: direction_hit True, magnitude_hit True.

(learn_cycle promote)
