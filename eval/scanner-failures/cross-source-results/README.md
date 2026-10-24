# Stock-only cross-source evidence experiment — 24 October 2026

Status: FAILED acceptance. Candidate remains a draft, unmerged and undeployed.

The implementation separates candidate terms from relationship statements, links
explicit document-scoped definitions across nonadjacent sections, joins complete
names across documents, preserves conflicting definitions and retains dated,
negated and prospective statements. It is literal evidence assembly, not a
complete semantic interpreter of corporate language.

## Matched diagnostic comparisons

Both versions receive identical source paragraphs. Reference terms and expected
relationships were assistant-reviewed from source text and committed before
baseline/candidate predictions. This is not independent human validation.
Explicit source aliases are consolidated per issuer. Predicates and assertion
states must match the complete expected set; unknown classifications are failures.
A separate pass supplies verified term spans without their types or expected roles.

| Second diagnostic: 4 new issuers, 8 witness paragraphs, 19 reference terms | Baseline | Candidate |
|---|---:|---:|
| Correct extracted identities / all extracted identities | 2/2 (100%) | 6/15 (40%) |
| Recovered reference identities | 2/19 (10.5%) | 6/19 (31.6%) |
| Classification on supplied verified terms | 2/19 (10.5%) | 2/19 (10.5%) |
| End-to-end correct term and classification | 1/19 (5.3%) | 2/19 (10.5%) |

Reserved half alone: IREN and OLLI; five reference terms. Extraction precision
1/1 -> 2/5; recall 1/5 -> 2/5; isolated classification 2/5 -> 2/5.
These denominators are far too small to certify a population threshold.
The baseline's 100% precision comes from emitting very few terms; it is not a
claim that the live scanner is 100% precise.

The first diagnostic used five different issuers and 17 reference terms. Its
candidate also increased recall while hurting precision, with unchanged isolated
classification. Different samples must not be used to claim that the second
revision's percentage is an improvement over the first revision's percentage.

The term/reference contract excludes forms, generic document titles, issuer
self-names and unnamed generic participant classes. It includes concrete named
entities and specific operating/financial concepts. Granularity and relation
labels remain reviewable judgments; changing them requires versioned adjudication,
not silently editing the results. No claim is made that these judgments exhaust
all reasonable definitions of business relevance.

## Scope and failures

These are definition-enriched selected WITNESS paragraphs, not full documents.
They therefore do not establish the requested full-source extraction precision,
recall, generalization or classification scores. The >=98% extraction target is
not demonstrated. The consumed issuers cannot be reused as unseen holdouts.

Observed remaining problems include lower-case product/service names, complete
legal and corporate-name boundaries, financial instrument/investment capacity,
accounting-standard classification and semantic distinctions beyond explicit
sentence patterns. Linking two mentions does not itself understand their role.

The cumulative register is retained. Article matching, Lane and financial impact
are marked out of scope for this stock-only milestone, not counted as passes.
Missing or unreviewed stock-side checks remain open.

`second-diagnostic.json` is the authoritative second result. The raw output is
retained separately; its original generic limitations footer incorrectly mentioned
ARM/NU from the first run. The corrected file changes that footer only, preserving
all counts, examples and predictions.

## Full-source access

PR #712 adds a read-only GitHub Actions range-reader workflow because the connected
artifact download tool cannot retrieve source ZIPs larger than 512 MiB. That job
successfully materialized complete source text objects for the four latest issuers:
34 filings, 1,228 documents, 57,786,657 stored characters including 9,590,874 prose
characters. The selected full-text artifact is 6,277,513 bytes:
https://github.com/SRoyaltyy/fullscan/actions/runs/40067419585
Artifact ID: 12549582539; SHA-256:
7601d1710113e1d6bfe79d333ce24ee6b03efed532dcc1bbae3f9cb3270e413c

The original full archive remains unchanged. Full-text parsing and source inventory
receipts are separate from accuracy validation. Exhaustive reviewed reference
inventories for those complete documents have not yet been created.

## Reproduction

Frozen issuer draws are in `../draws/`, labels in the parent directory, and original
source samples and both scoring scripts are retained here. The scoring scripts
use the repository root and the `run_scratch` filenames recorded in their source;
copy the corresponding source sample there before reproducing. Baseline code is
loaded from commit b05eb1b13d01f81f33a30c772c6f092541cf8fd0. Install the pinned grammar
resources and bootstrap the pinned semantic resources first. Do not substitute
article matching or a combined retain/reject score for these stock-side results.
