# Deterministic parser testing page

For parser work, every user-facing progress update must distinguish implemented,
tested, merged and actually deployed code. The latest reviewed parser must be
published to https://sroyaltyy.github.io/fullscan/dashboard/scanner-backtest/.
Merge/CI success alone is not deployment: verify the served snapshot source
revision and exercise both an article and a stock. Keep source-coverage limits
visible. Never describe description-only stocks as SEC-parsed, unresolved
context fingerprints as resolved senses, or browser-only custom matching as a
spaCy run. Report deployment failures and stale snapshots explicitly.

# Cumulative scanner methodology validation

For every change to collection, extraction, identity, classification, Lane,
impact, filtering, source routing or publication methodology:

1. Load `eval/scanner-failures/register.json`. Visit EVERY stable failure ID,
   including every nested incident/check. Add newly encountered mechanisms;
   preserve the original example and expected behavior. Missing, inapplicable,
   unavailable, disputed and unreviewed are OPEN, never successful checks.
2. Keep original reproductions as permanent regressions. Also reserve a fresh
   random sample of real filing-backed issuers with
   `python -m scripts.check_scanner_methodology --reserve-catalog CATALOG.json
   --out DRAW.json`. Commit the draw/issuer ledger before examining source text.
   Resolve ticker aliases to CIKs; exclude every previously consumed issuer,
   including failed and abandoned runs. Do not rename a ticker in a fixture and
   call that an unseen-stock test. Exhaustion requires reporting, not reuse.
3. Freeze candidate code/resources and source hashes before holdout review.
   A changed candidate needs a new draw. Never tune on an inspected holdout.
   For each failure mechanism require the original reproduction, a same-cause
   real case on another random reserved issuer, a useful positive vulnerable
   to over-filtering, and review of all changed decisions. If a mode does not
   occur in the sample, obtain more unused real issuers; do not mark it passed.
4. Build reference terms from COMPLETE selected stored sources, independently
   of candidate rows. Report document inclusion boundaries and omissions. Score
   extraction precision AND recall at >=98%, separately from classification on
   verified terms and on actual extractor outputs. Include direction, business
   capacity, negation, modality, time, multi-role truths and abstentions. No
   classification aggregate threshold has been specified by the user.
5. Run `python -m scripts.check_scanner_methodology --out VALIDATION.json
   --require-pass` and retain the entire per-mode report. A unit/browser pass,
   evidence checksum, synthetic control or aggregate improvement cannot replace
   this gate. Any regression reopens the existing mode and requires a new
   candidate/draw, recursively repeating the cumulative review.
6. Do not merge or publish an unvalidated methodology as a complete fix.
   Record implemented/tested/merged/deployed/live-verified states separately.
   Use stored sources; article-side evaluation remains headlines only.

See `eval/scanner-failures/README.md` for the machine-readable contract. The
register includes historical reports that still need raw reproductions; never
silently manufacture evidence or claim those incidents are fixed.

Current user scope is STOCK-SIDE ONLY: extraction/precision/recall and the
term's relationship to its stock across stored company sources. Article-side
matching, Lane and financial impact remain recorded but are OUT OF SCOPE for
this milestone and must not block it. Out-of-scope is not a passing result.

# Standing old/new comparison page requirement — user instruction, 24 October 2026

Every future scanner methodology update must publish its stock-side outputs at
https://sroyaltyy.github.io/fullscan/dashboard/scanner-backtest/compare/ with the
previous published version and the new version shown side by side for the SAME
stock. Preserve previous output assets immutably; show source revisions, source
coverage, dates, term additions/removals, changed relationship statements and
the supporting source evidence. Use the preceding published release as the old
version for each future update, rather than keeping an arbitrarily old baseline
forever. Preserve an earlier comparison when advancing to the next release.

Roll updates out across the entire available catalog. Fresh randomly selected
stocks are for regression/generalization testing and must not become a limit on
deployment coverage. Keep filing-backed and description-only inputs distinct.
Show pending, failed and unavailable rebuilds explicitly; never relabel an old
result as a new one or call a partial catalog rollout complete.

The user authorizes publishing experimental versions for testing before accuracy
acceptance. Keep experimental/deployment readiness separate from independently
reviewed extraction precision, recall and classification accuracy. A term count
increase or an output difference is not an accuracy improvement. Verify both
versions on the actual served page before calling the comparison usable.
