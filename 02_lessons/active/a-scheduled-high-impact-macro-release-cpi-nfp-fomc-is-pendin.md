---
trigger_pattern: "A scheduled high-impact macro release (CPI/NFP/FOMC) is pending at the open, US index futures are independently confirming a directional move of ≥+0.5% across all four indices, and the model collapses the entire leading sum to zero ('don't pre-score the binary') — emitting flat/flat on a session that resolves as a directional (up) day. The binary is treated as unknowable, but the pre-binary futures tape is knowable and is discarded along with it."
corrected_behavior: "Separate the *binary* (unknowable at the snapshot — do not pre-score) from the *pre-binary tape* (knowable — must be scored). When all four index futures confirm ≥ +0.5% in the same direction AND the sector's own cost driver is easing (oil offered for a chemicals-heavy book), score a modest S0 lean in the futures direction (e.g. +0.5 to +1) rather than 0, while keeping the magnitude band capped and confidence reduced for the pending binary. The 08-21 checklist is a ban on a *stale opposite* call; it is not a ban on a *modest same-direction* lean. Do not let 'don't pre-score the binary' collapse into 'score everything zero."
falsifier: "The correction is falsified if, on a future session with a pending high-impact macro release AND all four index futures confirming ≥ +0.5% in one direction AND the sector's cost driver easing, the sector ETF closes in the *opposite* direction or flat (|pct| < ~0.15%) — i.e., the pre-binary futures tape fails to predict the absolute direction. It is also falsified if applying the modest S0 lean systematically degrades direction accuracy on pending-binary sessions (tracked over ≥5 such sessions)."
current_behavior: "On 2026-09-11 the morning note correctly refused to pre-score the CPI print, but then scored S0=0 despite ES +0.63% / NQ +0.65% / Russell +0.63% / DJIA +0.53% (all ≥ +0.5%, 08-21 reversal checklist ON), oil offered (WTI −2.54%, Brent −2.86%), USD flat, and no fresh kinetic increment. The note explicitly framed the 08-21 checklist as 'a ban on a stale down call, not a license for up' — which symmetrically blocked a modest up lean. Total 0.0 → flat/flat. Actual: XLB +0.374% absolute (up/mild), SPY +0.852%, rel −0.478%. Direction MISS, magnitude MISS."
evidence_cited: "Morning: S0=0, total 0.0, flat/flat, confidence 0.5. Outcome: XLB +0.374%, SPY +0.852%, rel −0.478%; CPI came in close to expectations (0.4% m/m, 3.4% y/y; core 0.3% m/m, 2.4% y/y) — a benign resolution, not a shock; AP: 'S&P 500 climbed 0.9% and broke a four-day losing streak... after oil prices eased.' The futures tape at the open was green across all four indices and pointed to a green absolute day. Scoreboard: direction_hit False, magnitude_hit False."
error_category: "A"
scope: "general"
date: "2026-09-11"
status: "active"
occurrences: "1"
promoted_on: "2026-09-11"
sources: "['2026-09-11_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
Separate the *binary* (unknowable at the snapshot — do not pre-score) from the *pre-binary tape* (knowable — must be scored). When all four index futures confirm ≥ +0.5% in the same direction AND the sector's own cost driver is easing (oil offered for a chemicals-heavy book), score a modest S0 lean in the futures direction (e.g. +0.5 to +1) rather than 0, while keeping the magnitude band capped and confidence reduced for the pending binary. The 08-21 checklist is a ban on a *stale opposite* call; it is not a ban on a *modest same-direction* lean. Do not let "don't pre-score the binary" collapse into "score everything zero.

## WHEN IT FIRES
A scheduled high-impact macro release (CPI/NFP/FOMC) is pending at the open, US index futures are independently confirming a directional move of ≥+0.5% across all four indices, and the model collapses the entire leading sum to zero ("don't pre-score the binary") — emitting flat/flat on a session that resolves as a directional (up) day. The binary is treated as unknowable, but the pre-binary futures tape is knowable and is discarded along with it.

## WRONG IF
The correction is falsified if, on a future session with a pending high-impact macro release AND all four index futures confirming ≥ +0.5% in one direction AND the sector's cost driver easing, the sector ETF closes in the *opposite* direction or flat (|pct| < ~0.15%) — i.e., the pre-binary futures tape fails to predict the absolute direction. It is also falsified if applying the modest S0 lean systematically degrades direction accuracy on pending-binary sessions (tracked over ≥5 such sessions).

## EVIDENCE
Morning: S0=0, total 0.0, flat/flat, confidence 0.5. Outcome: XLB +0.374%, SPY +0.852%, rel −0.478%; CPI came in close to expectations (0.4% m/m, 3.4% y/y; core 0.3% m/m, 2.4% y/y) — a benign resolution, not a shock; AP: "S&P 500 climbed 0.9% and broke a four-day losing streak... after oil prices eased." The futures tape at the open was green across all four indices and pointed to a green absolute day. Scoreboard: direction_hit False, magnitude_hit False.

(learn_cycle promote)
