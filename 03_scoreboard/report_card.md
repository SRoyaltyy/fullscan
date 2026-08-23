# 📊 Engine Report Card — 2026-08-23 18:38 EDT

Graded runs on record: **108**. This report is pure arithmetic over the scoreboard — no LLM, no spin. If a number looks bad here, it is bad.

## Q1. Is accuracy actually improving?

- last 10 (n=10): direction **20%**, magnitude 20%
- last 30 (n=30): direction **47%**, magnitude 33%
- all time (n=108): direction **54%**, magnitude 30%
- first half 56% vs second half 52% → **➖ flat / wobbling**

## Q2. Is it beating 'just guess up' / 'guess yesterday'?

- engine direction accuracy: **54%** (n=108)
- 'always guess UP' would score: **52%**
- 'guess same as yesterday' would score: **54%**
- ❌ engine is BEHIND the best naive baseline by 1 pts — the AI is currently adding negative value vs a coin flip

## Q3. Calibration — when it says X% confident, is it right X% of the time?

| stated confidence | n | actual hit rate | honest? |
|---|---|---|---|
| 0.5–0.6 | 40 | 40% | overconfident ⚠️ |
| 0.6–0.7 | 54 | 57% | ✅ |
| 0.7–0.8 | 11 | 100% | underconfident |

## Q4. Are the same mistakes repeating?

- **?** (?): 91× (?, ?, ?, ?, ?)  ⚠️ **repeating after being logged — lessons are being written but not changing behavior**
- **B — REASONING failure (not tool/data); direction correct, absolute magnitude overpredicted. All necessary inputs were knowable at open; the issue was weighting, not data availability.** (B — REASONING failure (not tool/data); direction correct, absolute magnitude overpredicted. All necessary inputs were knowable at open; the issue was weighting, not data availability.): 1× (2026-08-17)
- **B — reasoning/weighting error, not tool/data failure. All necessary inputs (positive futures, easing real yields, stale macro headline, negative medium-term relative tape) were available at open.** (B — reasoning/weighting error, not tool/data failure. All necessary inputs (positive futures, easing real yields, stale macro headline, negative medium-term relative tape) were available at open.): 1× (2026-08-21)
- **NONE** (no error): 12× (2026-08-14, 2026-08-14, 2026-08-17, 2026-08-19, 2026-08-21)

## Q5. Lesson pipeline health

- candidates: 105 | active: 79 | archived: 0
- ⚠️ active-lesson pile is large — risk of narrow, contradictory standing rules (overfitting); consider a cull

## Q6. Sniff test — the 3 most recent lessons, verbatim triggers

Read these cold. Specific and falsifiable = good; vague and unfalsifiable = warning sign.

**2026-08-21_sector_utilities_lesson.md** [NONE]
- when: 
- do instead: 
- wrong if: 

**2026-08-22_lesson.md** [(not recorded)]
- when: Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade.
- do instead: No new lesson — recurrence record only. Per the standing 08-15/08-16 instruction, consolidate the existing ops lessons into one D-category rule and increment occurrences to 6. Deploy the hard pre-open gate before 09:30 ET: verify predict.md exists, is non-empty, and contains the SCORES_BEGIN block; 
- wrong if: If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken and must be fixed at the tooling level, not by another lesson.

**2026-08-23_lesson.md** [(not recorded)]
- when: A predict file is dated on a US cash-closed day (weekend/holiday) and the note already treats the prior Friday close as fact while forecasting the next cash session, but the grader injects that already-printed Friday OHLC and records a direction/magnitude miss with ops_fail false.
- do instead: OPS gate before scoring: if predict.md’s calendar date is not a US cash session, or the note already cites the injected SPX close as prior-session fact and forecasts the next cash day, set ops_fail=true and leave direction_hit/magnitude_hit null; pair the file to the next cash session or leave ungra
- wrong if: If a weekend-dated file is written as a same-session Friday forecast (does not treat Friday as already closed) and that Friday call is wrong on reasoning, auto-ungrading would hide a real miss and this pairing rule must be narrowed.


## Q7. Hit rate on foreseeable vs shock days

- foreseeable at 9am: 33% direction accuracy (n=3)
- partially foreseeable: 67% direction accuracy (n=3)
- genuine shock: 100% direction accuracy (n=1)
- misses on genuine-shock days are NOT reasoning failures; only the first two rows measure the engine's real skill
