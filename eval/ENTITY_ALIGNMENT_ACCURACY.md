# Entity alignment baseline: 21 October 2026

The shared identity gate is **not accurate enough to trust unattended** on this
challenge set. Passing the earlier regression suite established specific
behaviour, not semantic accuracy.

Tested production source: `c03df1059238e90cb75d64dfdcc7fb8fa9362b60`.
The resolver, keyword engine, older matcher and page source match that revision.
The actual parser is spaCy 3.8.11 with en_core_web_sm 3.8.0.

## Method

The fixed 80-case set contains 30 valid identity links, 30 explicit different
entities/senses and 20 ambiguous mentions. Expected outcomes were authored by
Codex before the first run. They have not been independently validated by a
human or a separate frontier judge. These are deliberately difficult synthetic
headlines, not randomly sampled real news, a FACA pass, or a population accuracy
estimate. Production rules were not tuned to improve this result.

The alignment stage starts with supplied typed source assertions. A separate
12-probe test runs dated SEC-style source paragraphs through the grammar and
legacy profile compilers before inspecting relationship, name, type and
assertion recovery. Those probes are synthetic, not quotations from SEC filings.

Every positive abstention counts as a missed valid link. Precision counts false
links on both explicit negatives and ambiguous cases. Browser evaluation runs
the actual page JavaScript functions in Node, not a rewritten approximation.

## Results

| Measure | Deterministic / browser | With pinned spaCy |
|---|---:|---:|
| Correct link-or-no-link decisions | 56/80 (70.0%) | 51/80 (63.75%) |
| Correct links / all accepted links: precision | 20/34 (58.8%) | 19/37 (51.4%) |
| Valid links recovered: recall | 20/30 (66.7%) | 19/30 (63.3%) |
| False links | 14 | 18 |
| Missed valid links | 10 | 11 |
| Ambiguous cases linked unsafely | 0/20 | 0/20 |
| Unresolved or unretrieved cases | 40/80 | 27/80 |
| Strict match / rejection / unresolved agreement | 44/80 (55.0%) | 38/80 (47.5%) |

Here link-or-no-link accuracy treats abstaining on a negative as safe, even if
the resolver did not establish that it is a different identity. The stricter
three-way measure requires explicit rejection on explicit negatives. Both are
reported so cautious abstention cannot hide poor recall or incomplete decisions.
Python and browser candidate decisions agree on all 80 cases.

The source probes fully recover the target relationship/name/type/assertion in
**6/12 cases**. This is targeted recovery, not source-extraction precision: the
target list does not exhaustively label all possible extra extracted claims.

## Concrete failures

| Case | Expected | Observed |
|---|---|---|
| Apple Bank raises savings rates | Different company from Apple Inc. | spaCy-assisted path accepts the Apple Inc. link. |
| Green Apple Restaurants reports earnings | Different full company name | Both paths accept the Apple Inc. link. |
| Actor John McDonald, who has never worked for Acme Industries | Different person | Both paths accept the Acme officer link. |
| A cloud of litigation hangs over a software company | Figurative cloud | Both paths accept the computing sense. |
| Wood chips shipped to a semiconductor plant as fuel | Wood, not semiconductors | Both paths accept the semiconductor sense. |
| Acme Industries CEO McDonald retires next month | Source officer | spaCy labels McDonald ORG; type veto rejects the valid person link. |

Source probes miss officer assertions outside a people-labelled section,
the direct 'we use semiconductor chips' relation and the supplier-as-subject
supplier construction. The McDonald's customer role is extracted with a
malformed `McDonald 's` name, failing identity-name recovery.

The main gaps are full mention boundaries and identities inside longer names,
negated affiliations and same-name people, grammatical attachment for concept
sense, source relationship coverage, and treating NER predictions as fallible
evidence. Merely adding PERSON/ORG tags does not solve identity.

## Reproduce and inspect

```bash
python -m pip install --only-binary=:all: --index-url https://pypi.org/simple -r requirements-filing-parser.txt PyYAML==6.0.3
python -m scripts.bootstrap_semantics
python -m scripts.evaluate_entity_alignment --spacy \
  --source-revision YOUR_CHECKED_OUT_SHA \
  --out entity-alignment-results.json
```

Node 22 or later runs the page comparison functions. The evaluation workflow
uploads the complete per-case results, NER spans, decision reasons, source
outputs, case hashes and production source hashes. A successful workflow means
the measurement executed; it is not a semantic quality pass.

Files: `entity-alignment-stress-v1.json` (fixed cases),
`entity-alignment-stress-v1-results.json` (recorded baseline).
