# JEV news filtering: findings and candidate replacement

The current hop-0 filter is a separate experiment/trainer, rather than a call in
the Lane market-analysis pipeline. Its outputs are `*_jev_keep*` and
`*_jev_junk*`. Updating it does not itself wire a new filter into Lane.

## Why the prompt iterations stalled

`src/__init__.py` imports `jev_hop0_overlay`, which replaces the questions and
formula in `jev_bits` at import time. The active rule is `(done OR print OR
spoke) AND NOT tip`; `tape` and `soft` only supply drop reasons. Several tests,
gold answer files and recorded scores still describe retired policies.

The human training rubric accepts announced deals, analyst revisions, new
products and market-relevant structural facts. The `done` question explicitly
rejects talks and analyst price targets. This is a definition mismatch, not
something a clever example can reliably repair. A publisher blacklist adds a
second, unrelated source of false drops.

The supposedly unseen 100-title stop set has been reused across many prompt
iterations. It is development data. Its recorded report does not fingerprint
the actual prompt, and the checked-in answer keys still include retired
`listed` instead of active `spoke`. Reported performance is therefore not
evidence of the current policy's out-of-sample accuracy.

Previously, HTTP failures and empty answers were converted into six zeros.
Useful news could silently become a confident drop, while the trainer's
`jev_error` retry path could not run. Partial and malformed responses were
also treated as valid negatives. This change retains those rows for review,
validates complete probabilities, and makes evaluations fail on unresolved API
errors after retries. It also removes automatic credential forwarding to the
unverified alternate API domain and distinguishes seconds from milliseconds
in retry headers. Official overload status 529 is retryable.

## Candidate policy

Set `JEV_GATE_POLICY=triage` when running the existing live gate or trainer.
The default remains `sixbit` pending validation and deployment review.

Two independent Choice questions identify:

1. Information: reported fact, specific factual context, noise, or unclear.
2. Market connection: direct US exposure, concrete global transmission, none,
   or unclear.

Announcements and proposals need not be completed acts. A concrete underlying
development survives a stock-price or investment-advice wrapper. Available
summary/description/snippet/content is included; human grades and prior model
classifications are excluded. JEV must not invent missing article facts.

Automatic decisions require high probability and confidence. These initial
cutoffs are conservative starting values, not calibrated probabilities of
correctness. Independent question probabilities are not multiplied. All
other rows have `routing=review`, `review_required=true`, and stay in the
keep output so existing consumers cannot silently discard them. An explicit
`reviewer` callback can resolve only uncertain rows, without receiving JEV's
prediction. Teacher failures leave review rows retained. No paid teacher call
is implicitly enabled. Existing downstream classification can consume retained
rows; near-frontier performance is a property to measure for the combined
system, not a promised property of JEV alone.

## Evaluation

`python -m src.jev_triage_eval --output validation/jev_triage_report.json`
requires a real JEV key. The feature-branch workflow runs the paired comparison
on `jev-1.13.0`, saves the report on that branch and uploads an artifact. It does
not write to main or invoke market/trading workflows.

- Development: original human K/D grades from five September 29 sessions,
  deduplicated by exact title. Conflicting grades would become review, not
  be relabeled to improve the score.
- Validation: 100 October 1 headlines independently labeled by the frontier
  assistant before the candidate's first live run. This draw already had older
  six-bit predictions, so it is not a prospective unseen holdout. Ambiguous
  title-only cases have review labels and are reported separately.
- Headline classification is scored independently of deduplication.
- Both policies see the same original news evidence, no gold labels.
- Review never counts as an automatically correct prediction. Reports include
  automatic accuracy, automatic keep precision/recall, retained recall, review
  count, error count, actual model names and prompt/dataset fingerprints.
- Retained recall includes review; it is not final classifier recall or proof
  of high precision. Teacher decisions need a separate end-to-end test.

Once this validation has been inspected, it too is development material for
future changes. Freeze the policy and use a later, independently labeled draw
before claiming frontier-equivalent accuracy. Include article excerpts for
ambiguous headlines and report costs and review coverage alongside accuracy.
Do not retune labels or cutoffs on validation misses and call the next run
unseen.

## Regression checks

Run `python -m src.test_jev_triage`, `python -m src.test_jev_bits`,
`python -m src.test_jev_hop0_overlay`, and `python -m src.test_jev_sixbit_stop`.
The older `src.test_jev_gate` suite has 26 failures on the original main
snapshot because it expects retired geo/material policies; the same 26 occur
with these changes. The trainer replay fixture now supplies all required live
bits. Its hard-miss bank test also fails on existing data (0 of 20 matches),
independently of these changes. These existing suites are not evidence that
the active model meets an accuracy target.

## First paired live result

Run: https://github.com/SRoyaltyy/fullscan/actions/runs/36813585656
Report: `validation/jev_triage_report.json`. Both policies returned without API
errors on pinned `jev-1.13.0`. Thresholds and prompts have not been retuned
after examining validation results.

| Dataset / policy | Labeled automatic accuracy | Automatic keep precision | Useful items retained | Review rows |
|---|---:|---:|---:|---:|
| Human development (497), six-bit | 66.4% | 59.1% | 14.9% | 0/497 |
| Human development (497), triage | 93.8% on 112 automatic rows | 83.3% | 99.4% | 385/497 |
| Validation (100), six-bit | 77.8% on 81 labeled rows | 75.0% | 27.3% | 0/100 |
| Validation (100), triage | 100% on 25 automatic rows | 100% | 100% | 75/100 |

Validation contains 19 title-only ambiguous cases, excluded from labeled
accuracy and reported in the raw results. These are small samples and provide
no frontier-equivalence guarantee. The candidate automatically drops 17 of the
100 validation rows and keeps 8; its remaining 75 need downstream or human
review. Development has 76 automatic drops, 36 automatic keeps, 385 reviews.
Thus the candidate repairs recall but only modestly reduces the frontier
workload at these conservative thresholds. Lower thresholds on development
reduce review while adding errors; no free accuracy improvement was found.

Six candidate automatic keeps disagree with historical D grades, including
reported insider sales, a dividend cut, actual results and a broker target
change. Those grades are retained unchanged. The single automatic false drop
is the credit-default-swap explainer that was human-marked K. These cases
expose a remaining rubric ambiguity: which factual financial developments
are useful, and when an explainer merits keeping. Do not hide it by changing
labels to match predictions. Confirm the rubric, add article evidence, then
collect an independent prospective test for the complete JEV-plus-reviewer
system. The candidate remains opt-in; error-handling fixes apply to the
existing default immediately when this PR is merged.
