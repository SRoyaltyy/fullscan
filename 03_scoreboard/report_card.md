# 📊 Engine Report Card — 2026-08-09 18:49 EDT

Graded runs on record: **9**. This report is pure arithmetic over the scoreboard — no LLM, no spin. If a number looks bad here, it is bad.

## Q1. Is accuracy actually improving?

- last 10 (n=9): direction **56%**, magnitude 44%
- last 30 (n=9): direction **56%**, magnitude 44%
- all time (n=9): direction **56%**, magnitude 44%
- not enough graded runs yet for a trend split (need 12+)

## Q2. Is it beating 'just guess up' / 'guess yesterday'?

- engine direction accuracy: **56%** (n=9)
- 'always guess UP' would score: **78%**
- 'guess same as yesterday' would score: **75%**
- ❌ engine is BEHIND the best naive baseline by 22 pts — the AI is currently adding negative value vs a coin flip

## Q3. Calibration — when it says X% confident, is it right X% of the time?

| stated confidence | n | actual hit rate | honest? |
|---|---|---|---|
| 0.5–0.6 | 3 | 67% | ✅ |
| 0.6–0.7 | 2 | 100% | underconfident |
| 0.7–0.8 | 1 | 100% | underconfident |

## Q4. Are the same mistakes repeating?

- **?** (?): 7× (?, ?, ?, ?, ?)  ⚠️ **repeating after being logged — lessons are being written but not changing behavior**
- **NONE** (no error): 2× (2026-08-04, ?)

## Q5. Lesson pipeline health

- candidates: 9 | active: 0 | archived: 0
- ⚠️ nothing has ever been promoted — either the promotion gate is too strict or lessons aren't generalizing

## Q6. Sniff test — the 3 most recent lessons, verbatim triggers

Read these cold. Specific and falsifiable = good; vague and unfalsifiable = warning sign.

**2026-08-07_lesson.md** [(not recorded)]
- when: A scheduled high-impact macro data release (NFP/CPI/FOMC) with a soft/expected-easing narrative is the flagged dominant event-risk of the day, while a separate geopolitical de-escalation story is generating positive overnight momentum; the market's actual driver becomes the macro print's repricing of the Fed path, and the geopolitical/oil catalyst fades or flips as attention shifts.
- do instead: (not recorded)
- wrong if: ⚠️ NO FALSIFIER RECORDED

**2026-08-08_lesson.md** [D]
- when: Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at grading time — or, better, at market open — so no scored baseline exists and the run is graded as a miss by default.
- do instead: Add a premarket pipeline guard before 09:30 ET: verify YYYY-MM-DD_predict.md exists and is non-empty with complete B0-B7/scores. If missing, fail loudly, retry generation, and alert. If still missing at grading time, mark the run as 
- wrong if: ⚠️ NO FALSIFIER RECORDED

**2026-08-09_lesson.md** [(not recorded)]
- when: Scheduled trading day opens with the premarket prediction file (YYYY-MM-DD_predict.md) missing or empty at open/grading time — a pipeline failure upstream of reasoning, not a market condition.
- do instead: Deploy a pre-open watchdog: verify YYYY-MM-DD_predict.md exists and is non-empty before market open; if missing, retry generation, alert loudly, and mark the run 
- wrong if: ⚠️ NO FALSIFIER RECORDED


## Q7. Hit rate on foreseeable vs shock days

- foreseeable at 9am: 50% direction accuracy (n=2)
- partially foreseeable: 100% direction accuracy (n=1)
- genuine shock: 100% direction accuracy (n=1)
- misses on genuine-shock days are NOT reasoning failures; only the first two rows measure the engine's real skill
