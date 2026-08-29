---
trigger_pattern: "After a multi-day oil-down / XLE-relative-fade stack that already missed magnitude (last session flat or non-transmitting), a still-red but sub-1% WTI/CL open is treated as a live S1 down spine and then copied into S2 (prior-day internals), S3 (trailing outflows), and S4 (yesterday’s 1d/3d/1w rel). Extreme cracks are scored HIT then damped out of S1. S0 is mixed/flat. The book still emits down."
corrected_behavior: "After that stack has already failed to deliver, default **flat/mild** unless **WTI/CL** (not Brent alone) is still ≥~1% down **and** XOM/COP are confirming in the **premarket**, not yesterday’s close. Count the oil shock once. Do not copy yesterday’s relative tape into S4 or restack S2/S3 as independent confirmation. If cracks/RBOB are the live mover, let them neutralize or flip S1 rather than damping a HIT to a footnote. Warsh-type policy binaries stay S0 two-sided: hawkish Fed + soft crude can bid XLE vs SPY. End the Energy experiment “keep direction, shrink confidence after mag misses” for this setup — confidence 0.52 does not fix a signed −6.3."
falsifier: "After a multi-day oil-down stack that already missed magnitude, WTI/CL still only sub-1% red at the open, cracks elevated, yesterday’s tape not reused as S4, model emits flat/mild — yet XLE still closes down ≥~0.5% with negative relative performance **driven by crude** (XOM/COP confirming the fade). That would mean the leftover oil-down still transmits and flattening was the error. The “unless transmitting” clause is separately falsified if WTI/CL ≥~1% down **and** XOM/COP are red premarket, we still flatten, and XLE then sells with oil."
current_behavior: "Apply the 08-27 cap (S1 not −2, mag mild, shrink confidence) but keep direction down and still sum S1–S4 as four independent −1s. Treat elevated cracks as a refiner footnote that must not carry XLE. Use prior-session relative tape as today’s S4. Treat a two-sided policy speech as “not a bid” for Energy."
evidence_cited: "2026-08-28 predicted down/mild (total −6.3, S0=0, S1=S2=S3=S4=−1, conf 0.52) vs XLE +0.626% / SPY −0.227% / rel +0.853% (dir MISS, mag HIT). Premarket CL −0.8% / BZ −1.91% / WTI −0.63% faded to WTI −0.16% / Brent −0.43%; RBOB +3.12%; VLO +1.66%, PSX +1.75%, CVX +1.04%; FANG −1.43%, EOG −0.80%. 08-27 mag cap held; keep-direction failed. Knowable at open: partially (mild yes; down required a leftover print to keep transmitting)."
error_category: "A"
scope: "general"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
After that stack has already failed to deliver, default **flat/mild** unless **WTI/CL** (not Brent alone) is still ≥~1% down **and** XOM/COP are confirming in the **premarket**, not yesterday’s close. Count the oil shock once. Do not copy yesterday’s relative tape into S4 or restack S2/S3 as independent confirmation. If cracks/RBOB are the live mover, let them neutralize or flip S1 rather than damping a HIT to a footnote. Warsh-type policy binaries stay S0 two-sided: hawkish Fed + soft crude can bid XLE vs SPY. End the Energy experiment “keep direction, shrink confidence after mag misses” for this setup — confidence 0.52 does not fix a signed −6.3.

## WHEN IT FIRES
After a multi-day oil-down / XLE-relative-fade stack that already missed magnitude (last session flat or non-transmitting), a still-red but sub-1% WTI/CL open is treated as a live S1 down spine and then copied into S2 (prior-day internals), S3 (trailing outflows), and S4 (yesterday’s 1d/3d/1w rel). Extreme cracks are scored HIT then damped out of S1. S0 is mixed/flat. The book still emits down.

## WRONG IF
After a multi-day oil-down stack that already missed magnitude, WTI/CL still only sub-1% red at the open, cracks elevated, yesterday’s tape not reused as S4, model emits flat/mild — yet XLE still closes down ≥~0.5% with negative relative performance **driven by crude** (XOM/COP confirming the fade). That would mean the leftover oil-down still transmits and flattening was the error. The “unless transmitting” clause is separately falsified if WTI/CL ≥~1% down **and** XOM/COP are red premarket, we still flatten, and XLE then sells with oil.

## EVIDENCE
2026-08-28 predicted down/mild (total −6.3, S0=0, S1=S2=S3=S4=−1, conf 0.52) vs XLE +0.626% / SPY −0.227% / rel +0.853% (dir MISS, mag HIT). Premarket CL −0.8% / BZ −1.91% / WTI −0.63% faded to WTI −0.16% / Brent −0.43%; RBOB +3.12%; VLO +1.66%, PSX +1.75%, CVX +1.04%; FANG −1.43%, EOG −0.80%. 08-27 mag cap held; keep-direction failed. Knowable at open: partially (mild yes; down required a leftover print to keep transmitting).

(learn_cycle promote)
