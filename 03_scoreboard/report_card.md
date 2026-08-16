# 📊 Engine Report Card — 2026-08-16 18:36 EDT

Graded runs on record: **69**. This report is pure arithmetic over the scoreboard — no LLM, no spin. If a number looks bad here, it is bad.

## Q1. Is accuracy actually improving?

- last 10 (n=10): direction **80%**, magnitude 20%
- last 30 (n=30): direction **67%**, magnitude 23%
- all time (n=69): direction **59%**, magnitude 29%
- first half 53% vs second half 66% → **📈 improving**

## Q2. Is it beating 'just guess up' / 'guess yesterday'?

- engine direction accuracy: **59%** (n=69)
- 'always guess UP' would score: **57%**
- 'guess same as yesterday' would score: **51%**
- ⚠️ engine barely beats the best naive baseline (+3 pts) — within noise

## Q3. Calibration — when it says X% confident, is it right X% of the time?

| stated confidence | n | actual hit rate | honest? |
|---|---|---|---|
| 0.5–0.6 | 21 | 62% | ✅ |
| 0.6–0.7 | 36 | 53% | ✅ |
| 0.7–0.8 | 9 | 100% | underconfident |

## Q4. Are the same mistakes repeating?

- **?** (?): 56× (?, ?, ?, ?, ?)  ⚠️ **repeating after being logged — lessons are being written but not changing behavior**
- **NONE** (no error): 9× (2026-08-12, 2026-08-12, 2026-08-13, 2026-08-14, 2026-08-14)

## Q5. Lesson pipeline health

- candidates: 65 | active: 47 | archived: 0
- ⚠️ active-lesson pile is large — risk of narrow, contradictory standing rules (overfitting); consider a cull

## Q6. Sniff test — the 3 most recent lessons, verbatim triggers

Read these cold. Specific and falsifiable = good; vague and unfalsifiable = warning sign.

**2026-08-14_sector_utilities_lesson.md** [(not recorded)]
- when: A Utilities/XLU call is built after a stretch of risk-on, growth/tech-led tape (low VIX, Greed, strong Asia tech), but the same session has a scheduled 8:30 ET high-impact consumer/macro release (retail sales, sentiment) that can miss consensus. The model anchors S0 to the prior session’s risk-on rotation and treats that rotation as the permanent cap on the defensive/bond-proxy bid, without stress-testing the scheduled macro calendar for a regime-flip catalyst.
- do instead: (not recorded)
- wrong if: ⚠️ NO FALSIFIER RECORDED

**2026-08-15_lesson.md** [(not recorded)]
- when: Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade.
- do instead: Deploy a hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block; if missing, retry generation once, alert loudly on the ops channel, and if still missing at grading mark 'no baseline — ungraded' with ops_fail=True and direction_hit/
- wrong if: If the watchdog is deployed and a scheduled trading day still opens with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; if the guard rejects legitimately late baselines, add an override.

**2026-08-16_lesson.md** [(not recorded)]
- when: Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade.
- do instead: No new lesson — this is a recurrence record only. Per the standing 08-15 instruction, consolidate the three existing ops lessons into one and increment occurrences to 5; deploy the hard pre-open gate before 09:30 ET that verifies predict.md exists, is non-empty, and contains the SCORES_BEGIN block, 
- wrong if: If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; a new lesson is only justified if a consolidated, enforced gate still fails to prevent recurrence.


## Q7. Hit rate on foreseeable vs shock days

- foreseeable at 9am: 33% direction accuracy (n=3)
- partially foreseeable: 100% direction accuracy (n=2)
- genuine shock: 100% direction accuracy (n=1)
- misses on genuine-shock days are NOT reasoning failures; only the first two rows measure the engine's real skill
