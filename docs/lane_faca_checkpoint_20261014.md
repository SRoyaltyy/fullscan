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

## V10 measured development replay and V11 continuation

V10 run 38515060184 analyzed 99/100. Action agreement 94%, precision 93.75% (15 TP, 1 FP), recall 88.2353% (15/17), Lane agreement 92%, exact sectors 87%. It failed the target and remains development-only. Program artifact 11872017332; score artifact 11872051466. The grader wrote the score before its raw-action reporter crashed on an unavailable model_output; V11 fixes that reporter.

V11 uses --action-review with --no-review. First stage extracts only Lane events; second independently extracts action atoms and sectors, without Lane definitions or the first draft. Both stages batch ten titles, preserving the normal two calls per batch. Required missing facts remain unavailable. The runner no longer treats an empty successful first-stage attempts list plus a failed second stage as missing credentials.

All 50 semantic contract tests passed locally on 2026-10-14. V11 live performance is not yet measured. The workflow replays frozen batch_01 as development; no acceptance credit. Frozen batch_02 remains unrun. Acceptance streak remains 0/10. Source/configuration/catalog must stay fixed across future genuine holdouts; all five metrics must reach 90% on every pass.

## V11 development measurement and V12 boundary correction

V11 run 38605686197, commit d2e3947a4d9beba6544e23b21f6057505e2f36e8: availability 100%, action agreement 89%, action precision 70.8333% (17 TP, 7 FP), recall 100% (17/17), Lane 92%, exact sectors 89%. The fixed grader succeeded. This fails the goal and is development-only; acceptance remains 0/10.

V12 retains the two independent stages and batches of ten. It requires a newly reported predicate before assigning public action, separates existing/background instruments from fresh operations, distinguishes campaign appearances from official engagements, and avoids inventing public executors from event venues, ordinary business stoppages or national adjectives. Claimed substantive agreements retain their own uncertain atom rather than only the communication. Sector instructions prioritize the explicitly targeted operating activity and recognize clinical workforce activity while excluding personal trading narratives. No reference labels changed. Fresh frozen batch_02 remains unrun.

## V12 qualified development candidate; first frozen V12 holdout

V12 run 38607393547, runtime commit 90d53684595e655f6ee0f4c96756cb5ff657b479: availability 100%, action agreement 94%, precision 94.4444% (17 TP, 1 FP), recall 100% (17/17), Lane 96%, exact sectors 90%. All five development thresholds pass. This replay is development-only, with zero acceptance credit.

Freeze this runtime/configuration/catalog for genuine acceptance. The new lane_semantic_holdout.yml runs untouched reserve batch_02, whose reference was saved before any execution at b150c012fa4056820d6aae7f362643842e652853 (2026-10-14T05:03:42.063Z; input SHA256 cf60e9b315856ede67b7de0be6932228a906cf2cdd3ed1af01ecf6ea8fba9e5f). The offline grader compares all candidate fingerprints against V12 run 38607393547 and verifies input identity and reference freeze time. No source/prompt/configuration/catalog changes accompany this holdout. Acceptance remains 0/10 until its measured result.

## Completed V12 holdout 02; next reference frozen before execution

Run 38608978245, execution commit 369584cfa712a6147c5615f320948ffc05df8004: all input/reference/runtime/configuration/catalog guards passed. Availability 100%; action agreement 90%, precision 94.7368% (18 TP, 1 FP), recall 90% (18/20), Lane 94%, exact sectors 87%. FAIL on exact sectors. Acceptance streak remains 0/10. Batch_02 now becomes development data permanently; do not count a tuned replay as fresh.

Batch_03's 100 reference labels were independently prepared and frozen at 2026-10-14T17:53:14.580Z, before any execution of that batch. Input SHA256 13e7174383594e2f7af4bb081aa677a9e35a29e20cdf5a34e4de33252a97c83c. Official identity sources and title/ontology ambiguity notes are included. Batch_03 remains unrun.

Next development comparison uses the already approved gemini-2.5-flash first, with the same two-stage batched design and bounded recovery, replaying failed batch_02. This changes provider configuration and therefore is a new acceptance candidate if it qualifies. No labels are changed; batch_03 remains withheld. Historical acceptance score artifact references remain pinned to GLM V12.

## 2026-10-15 continuation: completed Gemini comparison; V13 sector roles

Gemini-first development run 38610678006 completed before the conversation timeout. It analyzed all 100 but failed: action agreement 90%, precision 80.9524% (17 TP, 4 FP), recall 85% (17/20), Lane 90%, exact sectors 86%. First-stage providers: {"gemini":66,"zhipu":34}; this was not a pure Gemini result. Batch_02 remains development-only. No fresh acceptance runs followed. Streak remains 0/10.

The workflow checker is repaired to check sparse-checkout cleanup within individual ECS jobs, independently of later Ubuntu grading jobs. Only the two ECS installer jobs actually enable sparse checkout; the Lane ECS jobs use full checkout and remain covered by the clean:false guards. Dedicated workflow contract run 38611394819 passed all 23 tests at commit 4ec9e334fde0c1789634441f4f99994da97926f2.

V13 adds opt-in --sector-roles within the existing independent fact/sector batch call. The model provides a semantic role and literal role evidence for each candidate. Python keeps issuer/operator, target-business and policy-target-industry roles for final tags; incidental investor/lender, adviser/analyst, publisher/venue, macro/background and personal/campus roles remain in raw review output. Invalid/missing roles are unavailable, never a fabricated empty sector result. No runtime reference labels, entity exception lists, title regex classifiers or additional normal model calls are introduced. Batches remain ten titles with two normal prompts and bounded failed-subset retries; output caps unchanged.

A reported meeting with a public official can include that official as a participant even when visitors are the grammatical subject. Explicitly fresh/latest executive/judicial instruments remain extractable separately from private filings and old background rulings. These clarify generic missed-action boundaries.

All 55 semantic contracts passed locally. V13 live performance remains unmeasured. The workflow replays failed batch_02 with GLM first; batch_03 remains frozen and unrun. Source/configuration changes restart any future acceptance streak. The ten-pass goal is not met.

## V13 measured fallback failure; V14 rate-limit recovery

V13 run 38671138216 at dd9873989a67d199e3d765924929b409057a253c completed all 100, but failed: action agreement 84%, precision 61.5385% (16 TP, 10 FP), recall 80%, Lane 87%, exact sectors 76%. Raw artifact 11944237668 has ZIP SHA256 b6b5b637f7ed11815bf023001fb10481c2b0d65be34d364e722f33386c54784e. Score artifact 11943524442 has ZIP SHA256 0549d4d4d125dfbf5decf8a17658fbda9496948c866869a88a4350f740aec3ae.

Both stages used GLM 5.3 for rows 1-40, Gemini 2.5 Flash for rows 41-50, then Qwen Flash for rows 51-100. HTTP 429 diagnostics on both Zhipu and Gemini explain the fallback. The 40 GLM rows had 37/40 exact sector matches (92.5%), a diagnostic subset, not an acceptance pass. Do not attribute the full regression solely to the role schema or report a pure GLM/Gemini score. All 100 mixed-provider predictions remain failed development data.

V14 retains semantic role extraction but adds resumable execution with immutable runtime/configuration/catalog/input/limit guards, title/id checks, original first-execution timestamp preservation, atomic checkpoint replacement, and reuse of validated first-stage drafts after second-stage failure/deadline. Pure HTTP 429 failures do not fan out into pointless subset retries. A limited run stops with rate_limited while preserving completed analysis, rather than silently substituting an unqualified fallback.

The development workflow tries only the already approved Zhipu glm-5.3-flash and glm-4.7-flash models. It pauses 30 seconds between batches and allows at most three resumed attempts with 60-second waits within one total time budget. Neither model's availability/accuracy is presumed. All 65 semantic tests pass locally, including rate-limit fan-out prevention, unchanged-candidate resume, first-stage reuse, row-identity rejection and interrupted-write preservation.

Reserve batch_03 remains frozen and unrun. Batch_04 was also frozen before execution in commit 2400913e02d69b4f6acb294e06f8a48ab1c102b1; input SHA256 2551734d05971860b948c0ac1c27249445c9ad420edd3d4d1b1b3a9b450574de, reference freeze 2026-10-15T02:33:51.514Z. V14 is a new candidate with no measured live score. Acceptance remains 0/10, target unmet.
