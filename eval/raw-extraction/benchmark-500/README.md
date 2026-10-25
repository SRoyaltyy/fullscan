# Frozen extraction benchmark: 851 references across 12 stocks

This evaluates extraction before relationship decisions. The parser remained at `6dad95df8f7c2bb3b49df4de62c9ab80813c20db` throughout the test. No parser changes or reference relabelling followed the results.

## Results

| Measure | Existing raw inventory | Extraction-first mode |
| --- | ---: | ---: |
| Exact term or predeclared source alias | 570/851 (67.0%) | 754/851 (88.6%) |
| Complete span inside any candidate, including longer phrases | 775/851 (91.1%) | 785/851 (92.2%) |
| Single-word labels, including written compounds/identifiers | 319/330 (96.7%) | 328/330 (99.4%) |
| Multiword labels | 251/521 (48.2%) | 426/521 (81.8%) |

Without source-alias credit, the new mode recovers 746/851 canonical reference strings exactly. The exact and containment measures are intentionally separate. Relationship correctness is not scored.

## Output noise review

A seeded, issuer-stratified sample of 500 distinct normalized output groups was reviewed against the same source passages. These are groups, not repeated occurrences. Generic business nouns and process nouns are accepted even without a known company relationship.

- Clearly valid standalone nominal/named/technical terms: 306/500 (61.2%).
- Invalid as standalone terms: 182/500 (36.4%).
- Ambiguous scope or boundaries: 12/500 (2.4%).
- Validity ranges from 61.2% to 63.6% depending on the 12 unresolved judgments. This is an annotation range, not a confidence interval.

The main unwanted categories are 71 generic verbs, 58 isolated modifiers, 29 discourse/function/quantifier items and 24 malformed or incomplete names, phrases and identifiers. Examples include "paid", "solely", "financial", "Rule 457(a" and "Wilcox A". The liberal content-word fallback improves capture while retaining these extras.

## By stock

| Stock | Reference terms | Exact term/alias | Complete span | Valid output groups / reviewed |
| --- | ---: | ---: | ---: | ---: |
| LYNX | 27 | 23/27 (85.2%) | 23/27 (85.2%) | 20/42 |
| BVN | 71 | 53/71 (74.6%) | 58/71 (81.7%) | 23/42 |
| SHEN | 88 | 79/88 (89.8%) | 83/88 (94.3%) | 29/42 |
| NKE | 56 | 54/56 (96.4%) | 54/56 (96.4%) | 28/42 |
| KLC | 81 | 66/81 (81.5%) | 70/81 (86.4%) | 22/42 |
| BKR | 117 | 104/117 (88.9%) | 110/117 (94.0%) | 29/42 |
| BRO | 48 | 40/48 (83.3%) | 40/48 (83.3%) | 32/42 |
| INCY | 64 | 59/64 (92.2%) | 61/64 (95.3%) | 29/42 |
| BW | 64 | 58/64 (90.6%) | 61/64 (95.3%) | 21/41 |
| CBRE | 90 | 84/90 (93.3%) | 88/90 (97.8%) | 26/41 |
| CRUS | 60 | 56/60 (93.3%) | 56/60 (93.3%) | 24/41 |
| VST | 85 | 78/85 (91.8%) | 81/85 (95.3%) | 23/41 |

## Actual remaining failures

- NKE: the competitor names "On" and "Under Armour" were not recovered as complete terms.
- LYNX/BRO/VST: statutory citations such as "Rule 457(a)", "15(d)" and "Section 401(a)" are split or clipped.
- BVN: multiword legal titles and citations, including "Article 177 of the Peruvian Companies Law", remain incomplete.
- Other losses involve prepositions and coordination, including "risk of liability", "results of operations", "selling, general, and administrative expenses" and "worker health and safety".
- Some recovered terms have surplus possessives/modifiers, e.g. "MEM’s technical branch" instead of the minimal "technical branch". Those count in containment, not exact recovery.

## Sampling and provenance

The sample contains 36 retained filing passages (35 distinct after whitespace normalization) from 12 issuers across 11 named sectors plus one unclassified issuer. It covers 36,873 source characters. There are 851 distinct company–term reference pairs and 770 distinct reference strings across companies. Repeated mentions and the duplicated LYNX passage do not add reference units. Single and multiword units can overlap; their counts are disclosed above.

The source profiles retain selected filing prose. They do not reconstruct every original filing. One BW passage ends mid-sentence; labels cover only the text actually present. This is passage-level extraction evidence, not whole-filing recall. The 12 issuers were excluded from the recoverable cumulative prior-issuer ledger; unrecovered historical overlap cannot be ruled out.

Source-first references and the 500-output judgments were made by the assistant, not an independent human annotator. The sources, labels, producer code hashes and all per-term evidence are retained. These clustered samples do not establish a market-wide 98% precision/recall guarantee.

Freeze order:
- Draw/protocol: `3ce7585495eab4363c13d1b76d689709df077eac`.
- Exact sources before annotation: `fa4e6e56b44363dc3fe2cbe7c9e13c9a5e516388`.
- 851 reference pairs and scorer before predictions: `9ae093f2eff590a1cf784c57c6887ba154988117`.
- First predictions and seeded review sample before output judgments: `c5944ef827b8f56e0677bd09042a736f15906982`.

## Reproduction

The independent integrity audit verified 7,427 exact source occurrence spans, recomputed all term-level results from the saved predictions, reproduced the seeded 500-group sample and checked JSON/CSV/checksum agreement. All 12 archived ticker identities matched the reserved issuers.

Install requirements-filing-parser.txt and run from the repository root:

    python scripts/score_extraction_500.py --label-freeze 9ae093f2eff590a1cf784c57c6887ba154988117
    python scripts/finalize_extraction_500.py
    python scripts/verify_extraction_500.py

The scorer checks the frozen parser files and source/label hashes, then writes the complete predictions, per-term JSON/CSV and the same seeded output sample. The finalizer reuses all 500 recorded source-review judgments; it does not automatically judge relevance or use POS tags as gold labels.

Files: `summary.json`, `term-results.csv`, `term-results.json`, `reference-labels.json`, `precision-sample.json`, `precision-judgments.json`, `predictions.json.gz`, `sources/` and the freeze manifests.
