# Lane FACA continuation checkpoint — 2026-10-14

Goal: at least 90% action agreement, action precision, action recall, Lane agreement and exact coarse sectors in every one of ten consecutive disjoint 100-title batches. Freeze runtime/configuration across the streak.

Acceptance streak: **0/10**. Recovered reference labels lack original freeze metadata and remain diagnostic only. The batch_recall_fresh_02 fixture is now a development replay, not fresh acceptance data.

## Completed V6 diagnostic run

GitHub Actions run: 38507824186; runtime commit e4d4c7e75b1535efb7d75ed52f1538ef5264421b.
100 requested; 99 analyzed. Action agreement 95%; precision 100% (11 TP, 0 FP); recall 84.615% (11/13); Lane 88%; exact sectors 85%.
The same-run first-pass sectors scored 82%; sector-only audit improved 9 rows and worsened 6. It preserved actions and Lane.
Unavailable row 22 had nonliteral sector evidence in first-pass responses and a missing row in final subset recovery, not a documented rate-limit failure.

## V7 development candidate

Keep ten headlines per prompt, bounded failed-subset retries and an independent small sector prompt. No runtime benchmark imports or label access.
Clarify unnamed criminal law-enforcement instruments and statutory securities/accounting enforcement fines without inventing institution names; retain private-fine, citizen-arrest and future/proposal exclusions.
Sector prompt honors matched catalog whole-issuer sector for financial/insider events, ignores watchlist footers and personal/campus politics, permits explicit operating-industry funding/trade topics, and excludes central-bank policy alone as Financial.
Clarify commercial contract changes under demand and commercial surcharge decreases under input_cost.
41 semantic contract tests passed locally. Provider performance not yet measured for V7.

CI workflow self-check at 657aba passed all 23 workflow tests but failed a second stale GLM allowlist expectation in src/test_lane_news_scan.py; V7 aligns that test with the exact approved Flash allowlist.

## V8 reliability change prepared during V7 measurement

Failed-subset retries now receive per-row schema/evidence validation errors, never semantic labels or benchmark answers. Successful rows remain excluded from retries. Both extraction and independent sector stages use the same bounded recovery pattern. All 41 semantic tests pass, including failed-row isolation and error-feedback assertions. V7 live run 38510032205 retains its original df5be72bf4f2804afc8248b6cd5238d1f4f5ccf5 checkout. V8 is saved with provider validation skipped to avoid overlapping duplicate calls; performance still unmeasured.

## Completed V7 development replay

Run 38510032205, runtime df5be72bf4f2804afc8248b6cd5238d1f4f5ccf5; artifact 11869124751, ZIP SHA256 dbf5a8c8f61bfb9c41442dc527cbaddab72e44be8a34c5f9a70802a07dcdea49.
Availability 100%; action agreement 94%; precision 92.3077% (12 TP, 1 FP); recall 92.3077% (12/13); Lane 85%; exact sectors 91%. Does not pass overall. Diagnostic only, acceptance streak 0/10.
The recalled criminal/enforcement instruments improved; municipal supply agreement was omitted as action_kind other; a policy speech was falsely treated as official engagement. Lane errors include commentary incorrectly null, non-economic stories classified as business events, hypothetical listings, commercial promotions and corporate/public statement boundaries.

## V9 candidate

Preserves V8 bounded validation feedback. Clarifies public supply agreements, speech/keyword-fragment exclusions, private procedural processes, and Lane commentary/personal/forecast boundaries. 43 semantic tests passed. New independent replay started on the same diagnostic input; not acceptance.
Unrun reserve commit d36730231711746676559187895f5bc9a1a654b9 contains 1000 source-traced input titles in ten batches, compared to 300 recoverable previous runtime inputs. References have not been judged/frozen; no exhaustive historical exclusion claim.

## Completed V9 diagnostic replay; first holdout launched

Run 38511496266, runtime commit 2f135857222aad772209d851e28f08978545ff57, artifact 11870447275, ZIP SHA256 7d912b66b584e3447253f306e16492571eaad61f04792446010ac80a86be2ee8.
Availability 100%; action agreement 96%; precision 100% (12 TP, 0 FP); recall 92.3077% (12/13); Lane 97%; exact sectors 91%. All five development metrics passed. Reconstructed diagnostic references still make this ineligible for acceptance.
First independent holdout reference was frozen 2026-10-14T04:38:27.099Z at commit e396aaa813ca36d9c160e299d8276fc9c5df2fd1, before first classifier execution. Input SHA256 92c2c76f24dcc9d06693bda8e809591ca06ef2679fcfc8a6104dd3495746f632. The runtime prompt, source files, catalog and configuration are retained from V9. Only the workflow input path changes. References are outside sparse runtime checkout and never supplied to program prompts.
Acceptance streak remains 0/10 pending the fresh score. Nine reserve batches still require independently frozen references.

## V9 first independent holdout failed

Program run 38512622813; scorer run 38513753936. Program artifact 11870409712 (ZIP SHA256 11e4efa52235170f8dd73209ef65e4cc8ce661e2179c02278c32991a2ecad946); score artifact 11870609116 (SHA256 371388ebc42bcf80841bdeb32267f314164243c85cb0590bf4b9b1717ad8e711).
Source/configuration/catalog fingerprints matched V9. Reference freeze preceded execution and input SHA256 matched. Availability 100%; action agreement 93%; precision 93.3333% (14 TP,1 FP); recall 82.3529% (14/17); Lane 87%; exact sectors 86%. Acceptance streak **0/10**. This input is now development data and must never count as fresh again.
Second independent 100-title reference was frozen at 2026-10-14T05:03:42.063Z, before any program execution, commit b150c012fa4056820d6aae7f362643842e652853; input SHA256 cf60e9b315856ede67b7de0be6932228a906cf2cdd3ed1af01ecf6ea8fba9e5f. It remains unrun.

## V10 development candidate

Clarifies military/security operations, strategic-reserve releases and statutory public-service restoration; distinguishes court venues/private requests from actual judicial acts and preserves unresolved authority. Adds primary-event stage consistency validation without rewriting predictions. Clarifies criminal legal events, public institutional personnel, future mechanisms, multi-clause business participants, market-data sector exclusions and physical security/education activities. No company/sample names added to runtime, no benchmark labels imported. Two new integrity tests; remote contract gate will run before the program. Same failed first holdout is replayed strictly as development, with isolated automatic offline grading and no grading LLM calls. Native local connection stalled, so source/checkpoints and grading remain in GitHub.
