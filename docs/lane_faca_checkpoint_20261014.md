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
