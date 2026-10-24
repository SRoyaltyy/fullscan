# On-demand parser comparison continuation

User target: resume the lowercase **Assess parser accuracy** chat and make Enter fetch one stock's saved full source, run old/new on identical bytes and show both keyword/relationship/evidence outputs. No catalog rebuild should be needed.

## Code

- PR: https://github.com/SRoyaltyy/fullscan/pull/717
- Branch: codex/on-demand-parser-comparison
- Parser candidate remains independently unaccepted in draft PR #712. This PR changes execution and comparison infrastructure, not parser rules or accuracy certification.
- Old parser: 7adb09dd5091fb51ac68859645a26e2292951e46
- Candidate parser: 4d221aa78f21d1968fe4878998a2ac9455d3b6aa
- Candidate parsing code is identical to the earlier 6a3fd0cedeedd61404e4d6a823f82780cc9a540e rollout; subsequent changes to #712 were UI/instructions/evaluation receipts.
- Public destination: https://sroyaltyy.github.io/fullscan/dashboard/scanner-backtest/compare/

## Verified locally

Four Python integrity tests and six Node test entries pass. Node coverage includes real request-client dispatch/discovery/download logic with controlled HTTP responses; this is not a real authenticated browser dispatch. Workflow YAML and module syntax parse. Local browser exercised page load, invalid ticker, and unauthenticated Enter errors.

## Pending, not successful

Real BLZE archived-source execution is queued in GitHub Actions. At this checkpoint the parser jobs for runs 40096396531 and 40096578419 have not started. Broad unrelated PR workflows are occupying the available runners; do not call this a parser failure or a deployment success.

The PR is not merged, and the served page still has the previous saved-output implementation. Default-branch registration is required for workflow_dispatch. Verify the real parse and publication, merge the reviewed infrastructure, then verify the served request-config/UI revision and an authenticated Enter request. Do not report this as usable before that.

The current connector exposes workflow reads and reruns but no cancellation or dispatch. Browser control could not retain its tab session for accessing GitHub; no queued runs were cancelled. Do not claim they were.

## Execution contract

- Parse job has read-only repository/artifact access; parser checkout credentials are not persisted.
- Selective archive reader prepares one source file; separate old/new processes read identical bytes.
- Assembly rejects stock/revision/input/source-receipt mismatches.
- A separate write-enabled job retains checksum-verified gzip output and receipt under immutable request paths on scanner-request-results.
- UI uses a fine-grained Actions read/write token kept in tab memory and sent only to api.github.com. No token was requested from or entered by the user in this continuation.
- Filing-backed versus description-only coverage stays explicit. Undated descriptions may produce no old dated keyword records.
- Missing source text and failed workflows are explicit failures. Counts/differences are not accuracy scores.
- Saved comparisons are archived before replacing UI. Installer supports advancing the preceding candidate to the old revision on subsequent releases.

## Next steps

1. Inspect latest #717 Actions runs; older listed IDs may be superseded by the final commit.
2. If the real parser run fails, inspect the failed job logs and repair the concrete error.
3. Run result retention/publication and default-branch workflow registration to completion.
4. Verify browser dispatch, matching hash receipts, stock switching, evidence, filtering and download on the actual served page.
5. Watch for the older catalog-rollout workflow overwriting comparison UI; reapply/guard the new publisher if needed.
