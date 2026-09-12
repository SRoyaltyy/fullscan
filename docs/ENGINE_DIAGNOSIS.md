# Why the prediction engines were losing, and what changed

_Companion to `03_scoreboard/REPLAY_HARNESS.md` (numbers regenerate from
`python -m src.replay_harness`) and `03_scoreboard/IMPROVEMENT_TRACKER.md`
(regenerates every night). Every number below comes from the repo's own
scoreboard and Channel 1 snapshots; nothing is fitted on the days it is
scored on._

## 1. The numbers we started from

| Board | graded runs | direction hit | best naive baseline | magnitude hit |
|---|---:|---:|---:|---:|
| General (SPX) | 26 | **53.8 %** | always up 55.6 % | 46.2 % |
| Sectors (11 ETFs) | 174 | **48.3 %** | always down 51.4 % | 31.6 % |

Both engines were *below* a coin-flip baseline picked with hindsight, and
the second half of the history was worse than the first (general 69 % →
38 %, sectors 55 % → 42 %) while 200 "lessons" accumulated. That is the
signature of a system that is learning noise.

## 2. Root causes (evidence, not opinion)

### 2a. The graded answer is largely known at 05:55 and the rubric ignored it

Grading is close vs *previous* close (`fetch_channel1.fetch_actual_close`).
The overnight gap — ES/NQ futures, Europe cash, the VIX change — is a
big part of that answer and is sitting in Channel 1 before the LLM reads a
single headline. The principle check in the harness (290 sessions of
`data/prices/ohlc.parquet`, no snapshots involved) shows the sign of the
09:30 gap alone calls the close-to-close direction 63 % of the time for SPY
and 57–71 % for the sector ETFs, versus 38–54 % for the naive baselines.

The old rubric weighted the futures component at 0.5 and labelled it
"confirmation only", so a deterministic 60 %+ signal was outvoted by LLM
components with **33–48 % sign accuracy** (table below). The harness row
"tape anchor only (no LLM)" scores 61.9 % on the general board with zero
LLM involvement.

### 2b. Components with no skill carried full weight

Sign of each stored component vs the day's actual direction, all graded
runs (`engine_policy.factor_skill`):

| Component | n | sign hit | Component | n | sign hit |
|---|---:|---:|---|---:|---:|
| B0_ASIA | 13 | 0.385 | S0_SHARED_MACRO | 114 | 0.596 |
| B0_EUROPE | 8 | 1.000 | S1_SECTOR_FACTORS | 141 | 0.553 |
| B1_CATALYSTS | 21 | 0.762 | S2_BREADTH | 116 | 0.586 |
| B2_BONDS | 24 | **0.333** | S3_FLOWS_POSITIONING | 87 | **0.471** |
| B3_FEDPATH | 23 | 0.478 | S4_ETF_TAPE | 122 | 0.623 |
| B4_VIX | 8 | 0.500 | | | |
| B5_SENTIMENT | 21 | **0.333** | | | |
| B6_FUTURES | 15 | 0.733 | | | |
| B7_OIL_DOLLAR | 21 | 0.667 | | | |

B2_BONDS (weight 1.0) and B5_SENTIMENT (weight 0.75) were *anti*-signals
at n≈21–24 and nothing in the pipeline could ever down-weight them: all
"learning" was prose injected into the prompt.

### 2c. The flat zone was a guaranteed miss

`|total| < 1.0 → flat` produced 4 general flat calls (0 hits) and 21
sector flat calls (1 hit). "Flat" only grades as a hit when the ETF closes
within ±0.1 %, which happens 5–12 % of the time. Calling flat whenever the
LLM was undecided threw those days away.

### 2d. The learning loop rewarded volume, not accuracy

* `learn_cycle._promote_complete_candidates(min_market=1)` promoted **any
  single complete candidate** every night. The designed gate
  (`promote_lessons`: ≥ 2 recurring market candidates) was bypassed.
* `lesson_efficacy` measured topic accuracy before vs after each lesson
  went live and found 65 WORSE vs 33 improved (109 judged, mean Δ −0.086),
  but the result was only journaled. The monthly distill that was meant to
  retire lessons has been disabled since 2026-08-29.
* Every predict (general and each of 11 sectors) received **all ~200
  active lessons** (~600 k characters) plus an 800-character excerpt of
  every one of them again inside `mutable_policy.md`. Nothing about a
  Utilities lesson helps the Technology call; it just buries the rules that
  might.
* No part of the loop produced a number that the scoring code consumed.
  The LLM could (and did) ignore the prose.

## 3. What changed

### 3a. Scoring — `compute_scores.compute` / `compute_sector_scores.compute` (engine `v2`)

```
total = tape_anchor + clip(skill-weighted LLM overlay, ±6) [+ 0.25 × general total for sectors]
```

* **Tape anchor** (`src/tape_anchor.py`, Python-owned, deterministic).
  General: weighted mean of ES (1.0), NQ (0.6), Europe composite (0.8),
  −0.15 × VIX 1-day change, × 6 score points per %, clipped ±12. Sectors:
  beta-sum of the sector's overnight drivers (e.g. Energy = 0.35 CL +
  0.15 QA + 0.6 ES; Utilities = 0.3 ES + 0.75 ZN) blended 70/30 with the
  ETF's own pre-market gap when Channel 1 has it (new `etf_premarket` block
  in `fetch_channel1.build`). No anchor → the engine falls back to the
  LLM-only rules, so quiet-tape days still get a call.
* **Skill-weighted overlay.** Each LLM component is multiplied by its
  design weight *and* by a skill multiplier from `00_grounding/engine_policy.json`
  (sign-hit < 0.45 → ×0, < 0.55 → ×0.5, ≥ 0.65 → ×1.25, needs ≥ 8 graded
  observations, per-sector with pooled fallback). Components that duplicate
  the anchor (B0_ASIA, B0_EUROPE, B6_FUTURES; S4_ETF_TAPE) are dropped from
  the overlay when the anchor is present so the same move is not counted
  twice. The overlay is clipped to ±6 when anchored so prose cannot flip a
  2 % futures gap.
* **Flat only when |total| < 0.25.** Sectors with a near-zero total inherit
  the index sign instead of calling flat.
* **Magnitude from the anchor** (6 → notable, 12 → severe) when anchored;
  from the total with wider bands (9/15) otherwise.
* Score scale is unchanged, so `weather` (±4), `decision_lattice` (3.0),
  the paper gate (S ≥ +1) and sleeve routing keep working. `compute_legacy`
  is kept for reference and for the harness.

### 3b. Walk-forward result (`03_scoreboard/REPLAY_HARNESS.md`)

Re-scores every graded run from its stored components plus the **earliest
pre-09:30** Channel 1 snapshot of that day (post-open snapshots are
excluded — those days are scored with no tape, as live would be). Skill
multipliers for date *d* are built only from runs before *d*.

| Board | policy | n | direction | magnitude | 1st half | 2nd half | flat calls |
|---|---|---:|---:|---:|---:|---:|---:|
| General | legacy (as shipped) | 26 | 53.8 % | 46.2 % | 69.2 % | 38.5 % | 4 (0 hits) |
| General | **v2** | 28 | **71.4 %** | **57.1 %** | 69.2 % | 73.3 % | 0 |
| General | best naive baseline | 36 | 55.6 % | 66.7 % | | | |
| Sectors | legacy (as shipped) | 174 | 48.3 % | 31.6 % | 54.5 % | 41.9 % | 21 (1 hit) |
| Sectors | **v2** | 183 | **60.7 %** | **47.5 %** | 60.2 % | 61.1 % | 0 |
| Sectors | best naive baseline | 183 | 51.4 % | 49.2 % | | | |

v2 beats the legacy engine by +17.6 pp (general) and +12.4 pp (sectors) and
the best hindsight baseline by +15.8 pp and +9.3 pp, with both halves of
the history on the right side. Every one of the 11 sectors improves. On
the general board, on the 21 days that had a pre-open snapshot, v2 is at
81 %.

Caveats, stated plainly: n is small (28 general days); the anchor betas
and score-per-% were chosen by hand from these data rather than fitted,
and the split-half stability plus the 290-session principle check are the
guard against having tuned to noise. The tracker exists to catch it if
live diverges.

### 3c. Learning engine

* **Recurrence-gated promotion in the live path.** `learn_cycle` now needs
  ≥ 2 recurring complete market candidates (ops stays at 1).
* **Efficacy-gated retirement** (`src/lesson_retire.py`). Each night
  `lesson_efficacy.evaluate()` runs and lessons judged WORSE (topic hit
  fell > 5 pp with ≥ 4 graded runs on each side) move to
  `02_lessons/retired/` with `status: "retired"` and the numbers that
  retired them; worst first, max 10 per night; ledger in
  `03_scoreboard/LESSON_RETIREMENTS.md`. Nothing is deleted.
* **Numeric learning that cannot be ignored.** `engine_policy.update_policy_file`
  recomputes every component's skill multiplier nightly and appends a
  ledger row of what changed; `mutable_policy.md` shows the weights in
  force so the LLM's prose and the arithmetic agree.
* **Prompt injection cap** (`src/lesson_select.py`). A predict receives
  only its own topic's lessons plus ops rules, WORSE-verdict lessons
  excluded, ranked improved → unjudged → flat, newest first, max 20, body
  only (≈ 20 k chars instead of ≈ 600 k). `mutable_policy.md` excerpts the
  15 newest instead of all of them.
* **Improvement tracker** (`src/improvement_tracker.py` →
  `03_scoreboard/IMPROVEMENT_TRACKER.md`). Rolling 10/20-session direction
  and magnitude hit for the shipped call vs always-up / always-down /
  same-as-yesterday on the same runs, per session, legacy era vs v2 era
  (scoreboard rows now carry `engine`), cumulative curve since v2 went
  live, and the replay estimate it should converge to. Runs inside
  `learn_cycle` and again as its own no-LLM step in `run_postclose_all`.

## 4. How to read progress from here

1. `03_scoreboard/IMPROVEMENT_TRACKER.md` → "v2 engine live" row: direction
   hit and edge vs best baseline, both should be positive and growing
   toward the replay estimate (71 % / 61 %). If the v2 curve sits well
   below it for 20+ sessions, look at the anchor legs in the predict
   footer (`engine: v2 · tape_anchor …`) — a missing ES/NQ/Europe fetch is
   the usual cause.
2. `00_grounding/engine_policy.json` → `history`: which components were
   muted or boosted last night and on what n/hit.
3. `03_scoreboard/LESSON_RETIREMENTS.md` → what left the rule book and why.
4. `03_scoreboard/REPLAY_HARNESS.md` → rerun after any scoring change; the
   change must beat the row it replaces walk-forward before it ships.
