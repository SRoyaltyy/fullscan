# Deterministic article-to-stock relevance

The candidate engine is `evidence-v2`, selected with `--engine evidence`.
The production default stays `legacy` until broader validation passes.
The candidate returns direct events (tier 1),
documented business/competitive/dependency exposure (tier 2), industry context
(tier 3), and inspection/mentions (tier 4). Only tiers 1 and 2 have `pass:true`.
Every pass includes the article passage, decision route, and stock-source
passage where that route needs business or ownership evidence.

## Stock evidence

`src/filing_evidence.py` reads every available full text view in a PR642 object
root, checks the text-object digests, retains filing dates and accessions,
and routes 10-K, 20-F, 40-F, 10-Q, 8-K, 6-K and exhibit text. It imposes no
document/character cap. Machine-readable data remain source records rather
than prose keywords. Business, competition, customers, suppliers, properties,
officers, financial exposures and subsidiaries are distinct buckets.

`src/stock_evidence.py` builds the profiles and indexes. Every unigram stays
available; dictionary phrases and sourced product/subsidiary identities stay
as complete terms. Arbitrary unrepeated 3-grams are not stored as millions of
false concepts. Rare identifiers use stock rarity rather than being penalized
for low industry coverage. Industry vocabulary is a separate index.

The old SQLite corpus is supported only through an explicit section-cache
option. Its coverage report says `complete:false`; it is never represented as
the whole PR642 archive. `--require-complete` rejects that fallback.

## Article evidence

Raw identities are checked separately from normalized lexical terms. The
spaCy lookup resource normalizes word forms. Open English WordNet supplies
65,034 senses from 29 noun/verb topic files. A sense needs at least two context
matches and an untied best score before its synonyms can expand retrieval.
Synonyms receive 0.45 diagnostic credit, retain their source word, and cannot
turn one original term into multiple independent anchors. There are no LLM,
embedding-service, or network calls during scanning.

Company-specific events need company identity plus a local event; product
events need sourced ownership plus the local event. Unnamed business exposure
needs distinct business anchors supported together in a stock-source passage.
Short business paragraphs can supply that witness across sentences without
joining sections, dates or counterparty relations. Industry vocabulary does
not veto a supported niche business. An unnamed local incident does not pass
every company selling the object involved; broad policy or business activity
must be present for the unnamed business route.
People/acronyms are inspection signals until business context corroborates
them. Generic industry words such as drug/readout cannot pass an arbitrary
subset of an industry. Places alone do not pass companies. A dependency can
be material when a sole/critical counterparty suffers a disruptive event.

## Build and scan

Prepare the public dictionary once (PyYAML is build-time only):

```sh
python -m pip install PyYAML==6.0.3
python -m scripts.bootstrap_semantics
```

The installer fetches a pinned public Open English WordNet commit and checks
each source SHA256 and each compiled output against the committed manifest.
For an offline build pass `--source /path/to/wordnet/src/yaml`. The resulting
sense files are local build outputs. Scanning makes no network requests.

From a complete archive and Finviz descriptions:

```sh
PYTHONPATH=. python scripts/build_profile_cache.py \
  --objects /path/to/objects \
  --descriptions excel_bot/data/finviz_with_descriptions.csv \
  --as-of 2026-10-19 --workers 4 --out profiles.jsonl.gz
python -m src.link_correlation --engine evidence --filings /path/to/objects \
  --require-complete --universe /path/to/pr642-dates.csv \
  --as-of 2026-10-19 --news /path/to/documents.json \
  --out /path/to/links.json
```

For a paired legacy baseline, pass `--engine legacy`. It preserves the former
scorer for comparison and remains the production default. The v2 pass decision is independent
of batch order, duplicate articles, and batch-relative z scores.

For primary-headline evaluation, pass `--headline-only` to the scan CLI.
Scanner text labeled as a body may instead concatenate related headlines.
The sample exporter calls longer text an unverified extended-text stratum,
exports primary headlines separately, and can exclude development headlines.
Length is never proof of a genuine publisher body.

To verify evaluation with site packages, DNS and socket connections disabled:

```sh
python -I -S scripts/offline_evidence_checks.py --engine evidence \
  --cases eval/cases.jsonl --descriptions /path/to/finviz.csv \
  --out /path/to/results.json
```

Rebuild profile caches after changing stock extraction. A cache preserves the
facts extracted at build time; a newer matcher cannot recover a missing brand
or paragraph context from a cache that omitted it.

Compiler `stock-evidence-2.1` rejects caches without its matching version.
It retains founding dates, former names and headquarters as history rather
than operating vocabulary. Short business context cannot bridge those facts.
Calendar words cannot qualify as independent business anchors, and an issuer
launching a product does not by itself establish an industry-wide activity.

Short issuer names require corporate grammar or discriminating business
evidence. Identical names, industries and source digests allow the same issuer's
share classes to resolve together. Maximal issuer spans prevent a shorter
issuer from matching words contained inside another issuer's full name.
Exchange qualifiers and quote-directory headings do not establish events at
the exchange operator or the quoted issuer.

Explicit trading/DBA names must occur in the issuer's own legal introduction.
Source-derived initials remain candidates requiring case-sensitive article
identity and independent business/financial corroboration. A shared brand in
two distinct, explicitly owned operating segments also requires article-side
business corroboration. None of these mechanisms adds a ticker-specific alias.
Missing brands and stale company descriptions remain source-coverage gaps.

The isolated runner also accepts `--unittest` followed by test modules.
The sample exporter accepts repeated `--exclude-sample` arguments so later
holdouts can exclude all previously inspected primary headlines.

## Measurement and release bar

`src/link_metrics.py` computes exact-set article accuracy, link precision,
link recall, positive/negative case accuracy, abstention rate, per-category
counts and Wilson confidence intervals. Extra links and missing links both
fail exact-set accuracy. A missing output fails even a negative case. A
positive abstention is a false negative. Partial bug assertions do not enter
accuracy denominators. Labels retain their author and human-review status.

The target is at least 90% accuracy, precision and recall on a frozen, complete
label set with meaningful positives, followed by fresh unseen evaluation.
Unit-test success and a mostly-negative sample do not certify that target.
The metric explicitly leaves `production_accuracy_certified:false` because
no finite self-graded benchmark establishes accuracy on every internet article.

See the [coverage matrix](deterministic-coverage-matrix.md) for evidence,
positive/negative examples, missing-evidence behavior and coverage limitations.
The matrix explicitly tracks the incomplete global geographic inventory,
historical ownership state, and local rather than general English grammar.

## Resource provenance

- English lemma lookup: Explosion `spacy-lookups-data`, MIT; adjacent license
  retained. Original Git blob: `c0e6a9cdc6a532d3722ed972ccf29370e5743e29`.
- Sense data: Global WordNet Association, Open English WordNet, CC-BY-4.0;
  adjacent license retained. Per-topic source and output SHA256 digests are in
  `src/data/senses/manifest.json`. The build script needs PyYAML; scanning does not.
- The full original source corpus remains outside git. Profile caches are
  reproducible derived artifacts; do not commit corpus/archive blobs.
