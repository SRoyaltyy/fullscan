---
trigger_pattern: "A defensive/staples sector is entering a heavy retail-earnings week (WMT/TGT/HD) while the same morning's macro data is weak (retail sales, consumer sentiment). The model scores that weak consumer data as a one-way flight-to-safety tailwind in both S0 and S1, treats positive 1d/3d/1w relative tape as confirmation in S2/S4, and underweights the event risk that XLP's largest holdings are the exact retailers about to report on that weak data. Result: all-components-positive up/notable call in a session that de-risks the sector."
corrected_behavior: "Before scoring Consumer Defensive in a retail-earnings week, classify weak consumer spending data as two-sided: it may support staples as bond-proxies, but it also undermines the revenue base of the mega-cap retailers reporting within 1–5 sessions (WMT, TGT, HD, COST, STZ). Do not count the same weak data positively in both S0 and S1. If the data is weak AND the sector's largest weights are pre-earnings, score S0/S1 neutral-to-negative or explicitly split the channel: positive for bond-proxy/yield relief, negative for the retail-earnings event risk. Cap or flip the call when the event risk is knowable at open, and flag single-name concentration in the confidence note rather than letting S3/S4 confirm a notable up call."
falsifier: "The lesson is falsified if a comparable setup — weak July retail sales + weak consumer sentiment + WMT/TGT/HD reporting within 1–5 sessions — is followed by XLP positive relative performance because investors rotate into staples as flight-to-safety, while WMT and STZ also hold or rally. A later earnings beat that reverses the pre-earnings drop would not falsify the lesson; it would only mean the de-risking was correctly anticipated."
current_behavior: "Weak consumer data is automatically double-counted as positive for Consumer Defensive — S0 'defensive bid from weak data' plus S1 'flight-to-safety' — especially when S3/S4 show inflows and positive relative tape. Retail-earnings-week event risk is mentioned as a catalyst but not converted into a two-sided or negative component score. This yields 9.0/up/notable while XLP falls -1.64% with -1.17% relative underperformance."
evidence_cited: "2026-08-17 XLP: July retail sales -0.6% and consumer sentiment 51.0 were known; WMT fell -0.73% ahead of Aug 20 earnings; STZ dropped ~4.8%, worst in S&P 500; XLP -1.64% vs SPY -0.47%, relative -1.17%. The morning prediction scored all five components +1 (total 9.0, up/notable) and explicitly read weak consumer data as a defensive-bid catalyst."
error_category: "C"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring Consumer Defensive in a retail-earnings week, classify weak consumer spending data as two-sided: it may support staples as bond-proxies, but it also undermines the revenue base of the mega-cap retailers reporting within 1–5 sessions (WMT, TGT, HD, COST, STZ). Do not count the same weak data positively in both S0 and S1. If the data is weak AND the sector's largest weights are pre-earnings, score S0/S1 neutral-to-negative or explicitly split the channel: positive for bond-proxy/yield relief, negative for the retail-earnings event risk. Cap or flip the call when the event risk is knowable at open, and flag single-name concentration in the confidence note rather than letting S3/S4 confirm a notable up call.

## WHEN IT FIRES
A defensive/staples sector is entering a heavy retail-earnings week (WMT/TGT/HD) while the same morning's macro data is weak (retail sales, consumer sentiment). The model scores that weak consumer data as a one-way flight-to-safety tailwind in both S0 and S1, treats positive 1d/3d/1w relative tape as confirmation in S2/S4, and underweights the event risk that XLP's largest holdings are the exact retailers about to report on that weak data. Result: all-components-positive up/notable call in a session that de-risks the sector.

## WRONG IF
The lesson is falsified if a comparable setup — weak July retail sales + weak consumer sentiment + WMT/TGT/HD reporting within 1–5 sessions — is followed by XLP positive relative performance because investors rotate into staples as flight-to-safety, while WMT and STZ also hold or rally. A later earnings beat that reverses the pre-earnings drop would not falsify the lesson; it would only mean the de-risking was correctly anticipated.

## EVIDENCE
2026-08-17 XLP: July retail sales -0.6% and consumer sentiment 51.0 were known; WMT fell -0.73% ahead of Aug 20 earnings; STZ dropped ~4.8%, worst in S&P 500; XLP -1.64% vs SPY -0.47%, relative -1.17%. The morning prediction scored all five components +1 (total 9.0, up/notable) and explicitly read weak consumer data as a defensive-bid catalyst.

(learn_cycle promote)
