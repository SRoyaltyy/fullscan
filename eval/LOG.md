# Experiment Log — article↔stock correlation

This file is the project's memory. **Every round of the build-measure-judge
loop gets an entry here before the next round starts.** A round without an
entry does not count as having happened. Negative results (things tried that
failed and were reverted) get the same weight as successes — they are the
expensive knowledge this project produces.

## Entry format

```
## R<NNN> — <YYYY-MM-DD> — <one-line: the change or question>
- **Change:** files edited / commits, one line each
- **Hypothesis:** expected effect on which failure class or behavior
- **Evidence:** eval_diff summary, draw size, n correct, failure classes seen
- **Inference:** what we actually learned, including negative results
- **Corpus:** cases added / versioned (id + kind + one line)
- **Open:** what this round leaves unresolved
```

Rules:
- Append only — never rewrite or delete old entries. Correct yourself in a
  new entry that references the old one.
- One entry per round, even for rounds that changed nothing (record that the
  round ran and confirmed the status quo).
- An edit counts as one logical change. If three things changed, say so and
  say which was responsible for which observed effect, or say you can't
  attribute and why.
- When you re-judge or version-bump a corpus case, note it in the entry —
  self-graded labels must stay visible as `human_checked: false`.

---

## Reconstructed history (seeded 2026-10-19 from PRs, round files, and
outside review; entries before R004 are reconstructed, treat detail level
accordingly)

## R000 — 2026-10-18 — baseline harness run
- **Change:** eval harness introduced (branch `kimi/eval-harness`, later
  folded into this PR); frozen corpus seeded from `src/test_correlation.py`
  cases.
- **Hypothesis:** the existing scorer has systematic failure classes worth
  naming before fixing.
- **Evidence:** baseline regression pass rate 0.429.
- **Inference:** failures are not random; they cluster into alias traps,
  relation leaks, gate misses, and threshold issues. Naming them is a
  prerequisite for fixing them.
- **Corpus:** 13 cases, kimi-seed.
- **Open:** everything.

## R001–R003 — 2026-10-18/19 — taxonomy + corpus rounds
- **Change:** prospective problem taxonomy (rows A–G) written into
  `eval/rubric.md`; corpus rounds on frozen cases; harness diff tooling
  (`eval_diff`) added so rounds are comparable.
- **Hypothesis:** a fixed taxonomy makes each round's failures attributable.
- **Evidence:** regression pass rate driven to 1.0 across rounds 001–003.
- **Inference:** the taxonomy holds up — every observed failure maps to a
  row, and rows without a live failure stay empty rather than being padded.
- **Corpus:** cases locked as regression/target with versioning rules.
- **Open:** frozen corpus is self-selected; nothing yet says anything about
  unseen articles.

## R004 — 2026-10-19 — first random draw exposes live alias failures
- **Change:** none yet — this was a measurement round. Drew 20 articles,
  seed 20261019, from `data/news_intake/2026-10-17/documents.json`
  (pool 11,213). Scored with the then-current program.
- **Hypothesis:** frozen-corpus pass rate 1.0 should transfer to random
  articles.
- **Evidence:** 5 of 20 wrong before fixes. Alias guard failed on the live
  draw: "AP News" → ticker AP; dateline "SAN JUAN," → SAN; agencies
  IRS/CI/HHS/DEA → name hits; "MD Peter Woods" → MD.
- **Inference:** the corpus cases `ap-news-not-a-ticker`,
  `san-juan-dateline-agencies`, `md-is-a-title` were necessary but the guard
  generalization was incomplete. Random draws catch what frozen cases
  pre-imagine; both are needed.
- **Corpus:** 5 new cases from this draw (defence spelling, AP alias,
  Moderna possessive, MD title, SAN/agencies), author `cursor-round`,
  `human_checked: false`.
- **Open:** biotech bleed and single-word `valve` pass not yet isolated.

## R004b — 2026-10-19 — spelling, synonyms, alias guard, industry-word cap
(commit `8e397be`, plus `8b11053` partner gating landed here too)
- **Change:** `src/spelling.py` (UK→US fold with deny-lists: `hour`,
  `promise`, `exercise` untouched; only `cheque`→`check` moves);
  `src/synonyms.py` (true synonyms fold to one token; near pairs at 0.45
  credit and never cast an industry vote; homonyms deliberately excluded);
  alias guard for publisher suffixes, datelines, agency abbreviations,
  `MD`-before-name, possessives preserved; dependency rows now `order: 2`
  and surface only when the article's industry vote matches the partner's
  industry; a term used by ≥45% of a large industry stops scoring every
  member.
- **Hypothesis:** each maps to taxonomy rows A1/A3/A4, B3, E7, F7
  respectively.
- **Evidence:** round-005 re-score of the same 20: 16/20 correct, regression
  pass rate stays 1.0. All five new corpus cases pass on live output.
- **Inference (negative result, important):** a within-industry z-score was
  tried against the biotech bleed. It cleared r4-11 but failed the space
  memo, the `launches-inflection` target, and the defence-spelling match —
  **reverted**. Lesson: intra-industry relative scoring punishes genuinely
  distinctive articles; the fix for generic-word bleed is not stock-relative
  ranking.
- **Corpus:** 20 cases total. Two new locked targets from the draw:
  `bhvn-not-every-biotech` (8 biotechs passing on `drug`/`readout`),
  `heart-valve-not-cytokinetics` (CYTK on `valve`).
- **Open:** F7 (biotech subset) remains; A4/A3 beyond hand-built lists; A5
  filing acronyms; B2 metonymy; E2 subsidiaries; F1 cutoff calibration;
  stale `dashboard/news-dir/correlation/2026-10-17.json` still pre-fix.

## R005 — 2026-10-19 — outside review + spec; measurement gap identified
- **Change:** none to scorer. Outside review verified round-005 output
  against the sample JSON; harness comparator fixed (short texts expecting
  `unknown` now score; commit `99eb3df` on `kimi/eval-harness`); spec
  comment posted on PR #643 (alias registry with corroboration rule;
  industry centroids; geography-as-scope; 98% loop protocol).
- **Hypothesis:** the remaining failure mass concentrates in three
  structural gaps rather than more alias trivia.
- **Evidence:** 16/20 on the draw; both misses are F7-class; median body
  length in the 2026-10-18 intake measured at 88 chars — the scorer is a
  headline scorer for ~99% of input.
- **Inference:** (1) alias hits must trigger inspection, never passes —
  the DKNG partner case generalized; (2) cross-cutting terms (`fuel oil`)
  can never pass the purity≥0.55 industry vote, so industry-level signal
  needs centroids, not votes; (3) places are currently stripped signal —
  they should scope candidates, never pass them.
- **Corpus:** none this round. `thin-markets-moved` now scorable.
- **Open:** the three spec mechanisms unbuilt; headline-dominated input
  structurally caps tier-2 recall — needs its own taxonomy row; all
  cursor-round labels still `human_checked: false`.

---

## Status board (update every round)

- **Frozen corpus:** 20 cases, regression pass rate 1.0 (round-005).
- **Random-draw record:** 16/20 (round 004/005 draw).
- **Locked targets outstanding:** `bhvn-not-every-biotech`,
  `heart-valve-not-cytokinetics` (both F7).
- **98% bar:** ≥98/100 on a body>title random draw + regression 1.0,
  two consecutive rounds. Not yet attempted at n=100.
- **Mechanisms outstanding:** alias registry (E5/A5/product aliases),
  industry centroids, geography-as-scope.
