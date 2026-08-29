---
trigger_pattern: "A Financials / XLF call with S0=0 and S1=0 — mixed/flat ES/NQ, leftover non-holdings AI impulse already public, two-sided scheduled policy event, no 8:30, long-end steepener correctly refused as NIM+ — emits absolute down because yesterday’s completed rotation-out is copied into S2 (prior-session breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation. A ban on calling up off a broken streak is used as a license to call down."
corrected_behavior: "Do not triple-count a completed prior-session rotation-out. With S0=0 and S1=0, set S2=0 unless a live premarket BKX/XLF breakdown is confirmed; trailing unit outflows are not a 1-day lid; S4 may describe the prior close, it does not forecast the next session after a large lag. Prefer flat/mild. Keep a two-sided Fed speech two-sided for direction, not only as a mag-cap. Do not pre-score hawkish short-end flattening as NIM+ (unknowable at open) and do not flip to up from an 08-18 rotation trigger that is not live at open (need 1d rel ≥ +0.4% now). 08-27 remains: don’t emit up on the live AI-impulse session; that ban is not a next-day down mandate."
falsifier: "If this S0=S1=0 / inherited-S2-S3-S4 setup recurs, the call is flat/mild, and XLF still closes ≤ −0.3% with continued relative lag on repeated such sessions, leftover follow-through is real and this lesson is wrong. Also falsified if the rule is used to emit up after every red XLF day, or if 08-18’s live ≥ +0.4% 1d rel is present at open and the rule still forces flat."
current_behavior: "Keeps S0=0 and S1=0 (honest pre-speech), then stacks the same 08-27 lag in S2=−1 / S3=−0.5 / S4=−1, calls that agreement non-divergence, and lets leftover tape sign direction while the policy binary is only a magnitude cap. Pipeline emits down/flat."
evidence_cited: "2026-08-28 predicted down/flat (S0 0 / S1 0 / S2 −1 / S3 −0.5 / S4 −1, total −2.925, mult 0.9) vs XLF +0.38% / SPY −0.23% / rel +0.61% (up/mild). Warsh hawkish: Sep hike ~35%→~57%, 2Y +~12 bp to 4.356%, 10Y +~5 bp, 30Y +~2 bp; HY OAS 2.63% still tight; BKX ~+0.3%, JPM ~+0.8–0.9%, WFC ~+2%, GS lag, KRX ~−0.26%. Open already ~57.98 vs prior 57.89; not a squeeze. 08-18 ≥+0.4% rel fired only at the close, not at the open (1d rel −1.31%). Memory index unavailable this run."
error_category: "B"
scope: "general"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
Do not triple-count a completed prior-session rotation-out. With S0=0 and S1=0, set S2=0 unless a live premarket BKX/XLF breakdown is confirmed; trailing unit outflows are not a 1-day lid; S4 may describe the prior close, it does not forecast the next session after a large lag. Prefer flat/mild. Keep a two-sided Fed speech two-sided for direction, not only as a mag-cap. Do not pre-score hawkish short-end flattening as NIM+ (unknowable at open) and do not flip to up from an 08-18 rotation trigger that is not live at open (need 1d rel ≥ +0.4% now). 08-27 remains: don’t emit up on the live AI-impulse session; that ban is not a next-day down mandate.

## WHEN IT FIRES
A Financials / XLF call with S0=0 and S1=0 — mixed/flat ES/NQ, leftover non-holdings AI impulse already public, two-sided scheduled policy event, no 8:30, long-end steepener correctly refused as NIM+ — emits absolute down because yesterday’s completed rotation-out is copied into S2 (prior-session breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation. A ban on calling up off a broken streak is used as a license to call down.

## WRONG IF
If this S0=S1=0 / inherited-S2-S3-S4 setup recurs, the call is flat/mild, and XLF still closes ≤ −0.3% with continued relative lag on repeated such sessions, leftover follow-through is real and this lesson is wrong. Also falsified if the rule is used to emit up after every red XLF day, or if 08-18’s live ≥ +0.4% 1d rel is present at open and the rule still forces flat.

## EVIDENCE
2026-08-28 predicted down/flat (S0 0 / S1 0 / S2 −1 / S3 −0.5 / S4 −1, total −2.925, mult 0.9) vs XLF +0.38% / SPY −0.23% / rel +0.61% (up/mild). Warsh hawkish: Sep hike ~35%→~57%, 2Y +~12 bp to 4.356%, 10Y +~5 bp, 30Y +~2 bp; HY OAS 2.63% still tight; BKX ~+0.3%, JPM ~+0.8–0.9%, WFC ~+2%, GS lag, KRX ~−0.26%. Open already ~57.98 vs prior 57.89; not a squeeze. 08-18 ≥+0.4% rel fired only at the close, not at the open (1d rel −1.31%). Memory index unavailable this run.

(learn_cycle promote)
