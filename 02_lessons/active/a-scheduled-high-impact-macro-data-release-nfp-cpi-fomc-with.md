---
trigger_pattern: "A scheduled high-impact macro data release (NFP/CPI/FOMC) with a soft/expected-easing narrative is the flagged dominant event-risk of the day, while a separate geopolitical de-escalation story is generating positive overnight momentum; the market's actual driver becomes the macro print's repricing of the Fed path, and the geopolitical/oil catalyst fades or flips as attention shifts."
corrected_behavior: "When a scheduled high-impact macro release is flagged as the day's dominant event risk, set the macro-linked components (Fed path B3, bonds B2) from the expected-print conditional under the regime lens — a soft-print-expected day under bad-news-good cannot carry a negative B3. Independently cap/discount geopolitical-oil components (B1/B7) to at most ±0.5 unless US futures independently confirm them. Add a final narrative-vs-scores consistency check: any narrative sentence claiming a macro print is bullish requires the Fed-path/bond component to carry non-negative weight."
falsifier: "If this trigger recurs and the market's move is instead dominated by the geopolitical/oil catalyst while the Fed-path repricing is a secondary move, the lesson is wrong and must be revised; additionally, if applying this correction produces direction misses on 2 of the next 3 scheduled-macro-print days, narrow or discard it."
current_behavior: "Scored the Fed path (B3) -0.5 from the standing hawkish backdrop and treated the Hormuz/oil complex (B1 +1, B7 +1) as the primary positive driver on a jobs-report day; the narrative acknowledged 'soft jobs = positive under bad-news-good' in the good-news list but the component scores contradicted that narrative, producing two offsetting sign errors that happened to yield the correct total."
evidence_cited: "2026-08-07: SPX +0.62% (up/mild — both axes hit, but for the wrong reasons). B3 scored -0.5 while the July NFP (-23k vs +80k expected, -103k revisions) collapsed September hike odds — the day's biggest positive. B7 scored +1 (oil down) but Brent reversed +1.2–1.3% intraday on Hormuz doubts — a headwind. B1 +1 credited the Hormuz deal, but the jobs report was the actual dominant driver; B2 +0.5 understated a sharp yield fall (10Y to 4.64%)."
error_category: "B"
scope: "general"
date: "2026-08-07"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-07_lesson.md']"
schema_ok: "true"
---

## RULE
When a scheduled high-impact macro release is flagged as the day's dominant event risk, set the macro-linked components (Fed path B3, bonds B2) from the expected-print conditional under the regime lens — a soft-print-expected day under bad-news-good cannot carry a negative B3. Independently cap/discount geopolitical-oil components (B1/B7) to at most ±0.5 unless US futures independently confirm them. Add a final narrative-vs-scores consistency check: any narrative sentence claiming a macro print is bullish requires the Fed-path/bond component to carry non-negative weight.

## WHEN IT FIRES
A scheduled high-impact macro data release (NFP/CPI/FOMC) with a soft/expected-easing narrative is the flagged dominant event-risk of the day, while a separate geopolitical de-escalation story is generating positive overnight momentum; the market's actual driver becomes the macro print's repricing of the Fed path, and the geopolitical/oil catalyst fades or flips as attention shifts.

## WRONG IF
If this trigger recurs and the market's move is instead dominated by the geopolitical/oil catalyst while the Fed-path repricing is a secondary move, the lesson is wrong and must be revised; additionally, if applying this correction produces direction misses on 2 of the next 3 scheduled-macro-print days, narrow or discard it.

## EVIDENCE
2026-08-07: SPX +0.62% (up/mild — both axes hit, but for the wrong reasons). B3 scored -0.5 while the July NFP (-23k vs +80k expected, -103k revisions) collapsed September hike odds — the day's biggest positive. B7 scored +1 (oil down) but Brent reversed +1.2–1.3% intraday on Hormuz doubts — a headwind. B1 +1 credited the Hormuz deal, but the jobs report was the actual dominant driver; B2 +0.5 understated a sharp yield fall (10Y to 4.64%).

(learn_cycle promote)
