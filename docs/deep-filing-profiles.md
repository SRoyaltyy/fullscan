# Deep stock-side SEC profiles

The stock-side compiler reads all available, verified whole-filing text objects
from the archive release in `data/company_filings_full_release.json`. The
initial release covers 2,026 issuers, 2,032 ticker associations and 23,635 dated
filings. Its scope is the latest annual and subsequent reports in the archived
universe, not every historical SEC filing since incorporation. Catalog tickers
without archive coverage stay explicitly description-only.

## Extraction

`src/filing_relationships.py` uses pinned local spaCy dependency parsing,
generic issuer/argument rules, and a general agency dictionary. Supplier and
competitor roles are determined by their clauses, not by a hardcoded role for a
company name or an embedding cluster. Every prose character receives dependency
analysis, including passages with no familiar relation cues; bounded windows
control grammar-model memory. No keyword gate excludes prose from parsing.
Identical parsing windows can share work, and identical claims can share
storage, while retaining all dated source references. There is no filing,
document, text-length, or retained-fact cap. Machine-readable attachments remain
in the verified source inventory and are not passed off as narrative prose.

Version 1.1 also retains and classifies every paragraph, including headings,
tables, boilerplate, machine attachments and unresolved topics. The archived
text does not retain original HTML DOM paragraphs: nonempty text lines define
review paragraphs, with blank separators retained. These exact spans reconstruct
every section without gaps or overlap. Original visual paragraph boundaries
cannot be claimed from this archive representation.

Each paragraph has full text, a hash, source offsets, multiple topic labels,
exact rule witnesses, reported currency/percentage/scale mentions, a review
priority with reasons, an extraction outcome and links to every overlapping
relationship. Quantities are mentions, not normalized financial metrics. Topic
keywords describe discussion, not an asserted issuer relationship. High priority
flags materiality/disruption language or selected explicit relationships; normal
priority is not a judgment that a passage is economically unimportant. Unmatched
topics, tables/machine data, and business language without a resolved relationship
are explicitly flagged for review. This is exhaustive accounting of text, not a
claim that deterministic rules understand every possible financial fact.

The graph records suppliers and purchased inputs, operational dependencies,
offerings, customers, competitors, partners, licenses, agencies/laws/approvals,
policy risks, geography, acquisitions/divestitures and issuer differentiation
claims. These are explicit filing claims, not independently verified economic
facts or guarantees of a competitive moat.

Each relationship carries its subject, predicate, object, object type,
qualifiers, assertion status, rule, and evidence. Evidence identifies an exact
quote and character span, source hash, filing date, reporting date, acceptance
timestamp, accession, document, section and SEC URL. Missing dates remain
missing; reporting dates are not replaced with filing dates.

Negated, historical and potential claims remain visible. Latest negative claims
and same-date contradictions are not indexed as current affirmative
dependencies. Missing relationships mean unknown, not absent. Unresolved
pronouns, unusual grammar, tables, quoted third-party speech and relationships
not covered by explicit rules remain extraction limitations. Source coverage
and relation accuracy are different measurements.

## Build and inspect

After materializing all complete text objects from a source shard:

```sh
python -m scripts.build_filing_profiles --objects full-text-source/objects \
  --as-of 2026-10-20 --descriptions excel_bot/data/finviz_with_descriptions.csv \
  --out compiled-profiles
python -m scripts.audit_filing_profiles --profiles compiled-profiles \
  --objects full-text-source/objects
```

The builder checks every source manifest/text hash, processes one issuer at a
time, and checkpoints per issuer. Resuming requires matching source content,
dates, labels, extractor/reader/builder source, and pinned grammar requirements.
The `Deep SEC filing profiles` workflow resolves every shard from the verified
release and refuses to combine incomplete or mixed-version shards. It verifies
the release issuer and filing totals before producing the complete artifact.

## Scoring and the test page

`--filing-profiles DIRECTORY --as-of DATE` loads precompiled graphs into the
same stock evidence board used by the classifier. That read path uses only the
standard library and existing local lexical resources: it imports no spaCy
runtime and performs no network or LLM inference. Existing matching channels
consume supported relationship types; adding a fact type does not create an
unvalidated automatic stock-pass rule.

The page exports the exact scoring profiles as individually compressed,
hash-verified assets and also exposes every original graph claim and source.
UI pagination changes only how many records are rendered at once. It does not
discard facts, shorten stored quotes or restrict the full downloadable profile.
Every paragraph can be searched and filtered by topic, outcome, priority and
review status. The complete paragraph text and its source remain available.

The publication workflow waits for a complete compatible main-branch graph
artifact, scores the full fixed headline intake, retains and publishes all page
assets, then checks the public snapshot. It preserves the existing public page
until those checks pass. Relevant parser changes rebuild the profiles and then
refresh the existing GitHub Pages test page automatically.

## Validation

Controls cover subject ownership, supplier/customer direction, passive and
appositional supplier phrases, common-company names without global role
assignment, agency/law types, negation/modality, exact source offsets,
repeatability, complete text windows, hashes, date cutoffs, contradictory claims,
and uncapped profile export. The real-file audit reconstructs every supporting
quote from the verified archived source using its content hash; repeated SEC
headings cannot select a different section by accident.
Build, shard merge and publication readers also reject paragraph gaps, altered
text or witnesses, incomplete grammar coverage, and missing relationship links.
The real-source audit reconstructs every paragraph against the verified archive.

These controls are not a precision/recall certification. A real-file audit that
quotes match their sources does not prove that every extracted relationship is
semantically right or that every relevant relationship was extracted.
