# Cumulative scanner failures

The 87 original modes are open until reproduced and revalidated. This directory
does not certify that fixes exist. The 23 October candidate failed its holdout;
those issuers are excluded from future unseen-stock claims.

Every methodology change has two tests: unchanged historical regressions AND
new randomly selected real issuers. The ledger is cumulative across successes,
failures and abandoned work. Selection is by CIK, not ticker, so share classes
and aliases cannot re-enter as supposedly different stocks. A mode not observed
in a fresh sample remains open; draw further unused issuers for that mechanism.
No finite random sample establishes correctness for every stock.

## Procedure

1. Make a catalog JSON array of `{ticker,cik,filing_backed}` from the immutable
   stored-source index. Freeze the index hash, source revision and parser state.
2. Run `python -m scripts.check_scanner_methodology --reserve-catalog catalog.json
   --out draw.json`. Commit `issuer-ledger.json` and the draw BEFORE reviewing
   source text. The default seed is system-random and is retained for replay.
3. For development and then a frozen candidate, preserve full source documents,
   their dates and SHA-256s, source-first term labels and actual predictions.
   Do not inspect reserved examples while tuning. Any later change consumes
   that holdout and requires another random draw.
4. Record `current-validation.json` using schema `scanner-validation-1`:
   `run_id`, `methodology_sha256`, `reserved_issuers`, candidate/draw freeze
   declarations, `source_scores`, and `mode_results` for EVERY register ID.
5. `source_scores` contains `{issuer_id,reference,predictions}`. Reference has
   `reference_basis: full_stored_sources`, reviewer, complete source inventory
   (`id,text,date,sha256,document_complete,review_complete`) and complete term
   occurrences (`source_id,start,end,text,relationships`). Predictions have
   actual `terms` and separate `classification_on_verified_terms`. Relationships
   record `subject,predicate,object,capacity,assertion,time`. Source exact spans
   prevent clipped titles earning full-name credit. Multi-label sets must match.
6. Each `mode_results[ID]` records this candidate hash and fresh run ID, plus
   `original_reproducer`, `fresh_real_issuer`, `useful_positive_control`, and
   `changed_decisions`. Each is a recursive tree of `checks`; reviewed leaves
   have immutable `evidence_sha256`, `expected` and `actual`. Fresh cases have
   `provenance: stored_source` and `issuer_id` from the holdout. Missing descendants
   or unequal expectations keep the mode open. Future incidents append children;
   never replace a former failure with an easier example.
7. Run `python -m scripts.check_scanner_methodology --out result.json --require-pass`.
   The command visits every mode and exits nonzero for open modes, stale code,
   unreviewed/incomplete sources, or extraction below 98% precision OR recall.
   The publication workflow calls the same gate. Infrastructure controls and
   parser unit tests are separate and cannot override this result.

Scores use exact complete source occurrences. They explicitly do not certify
canonical-identity deduplication, corpus coverage, article identity, Lane or
financial impact. These have their own failure IDs and require their own
checks. Source review must cover original stored text, not only extracted rows.
Classification is measured separately on verified terms and end-to-end; the
user has not specified an aggregate classification threshold. Report sample
size, review provenance and uncertainty rather than a population guarantee.

## Current state

No accepted source-based generalization report exists. `--require-pass` MUST
fail. Do not remove the flag, mark absent tests passed, lower extraction targets,
or merge this semantic candidate as validated merely to make CI green.
