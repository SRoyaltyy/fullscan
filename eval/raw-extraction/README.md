# Extraction-first lexical candidates

This mode collects plausible lexical terms before business relevance, entity
resolution or relationship classification. It has no company-specific name list,
relationship requirement, background-word filter or minimum business-importance
threshold. Every occurrence keeps exact source offsets and its original text.

The candidate pool includes named entities, base noun phrases, individual nouns
and proper nouns, contiguous noun sequences, literal hyphen compounds and
non-stop content words. The last path intentionally retains extra verbs and
adjectives so a statistical POS error does not silently remove an unfamiliar
product name. Filler words such as standalone "and" and "otherwise" are omitted.
Internal words in names and compounds remain intact.

Common plural nouns get singular lemma search keys: batteries -> battery,
turbines -> turbine, facilities -> facility. Their original plural text remains
available. Proper nouns and words inside recognized proper names are protected:
General Motors, United States, Bank of America and Johnson & Johnson retain
their source spelling. These are lexical search keys, not canonical entity IDs.

## Run

Install the repository's requirements-filing-parser.txt, then:

    python scripts/extract_filing_terms.py --input filing.txt --out terms.json

The input is UTF-8 filing text. The command scans its entire contents in bounded
windows. It reports window coverage/checksums, exact term occurrences and shared
sentence contexts. All relationships and identities remain unresolved. This is
an explicit extraction-only command; it does not replace the deployed comparison
page or silently alter the selected-keyword parser.

## Recorded checks

    python -m unittest scripts.test_filing_term_candidates
    python scripts/audit_raw_extraction.py

Eight controls passed: filler words; common-noun plurals; proper names; generic
terms without relationship decisions; negated sentences; hyphenated compounds;
unfamiliar content words; and complete long-input coverage with valid offsets.

The replay reuses the earlier source-first labels: four issuers, eight selected
witness paragraphs, 19 reference terms. It is consumed diagnostic material,
not a fresh holdout or a complete-filing inventory. No issuers are claimed new.

| Same source paragraphs and reference identities | Existing raw inventory | Extraction-first mode |
| --- | ---: | ---: |
| Exact accepted term/alias recovered | 14/19 | 19/19 |
| Complete reference span captured, possibly within a larger phrase | 17/19 | 19/19 |

The output contains 595 candidate occurrences. They have NOT all been reviewed
for plausible relevance, so precision is unmeasured. These counts do not
establish population recall or acceptable downstream classification. The former
6/19 selected-keyword result measured a later, filtered output and is not this
raw-inventory comparison.

The replay records input/label/code hashes, spaCy 3.8.11 and model 3.8.0, and
per-term matches in diagnostic-replay.json. Running it also writes the complete
predictions to diagnostic-predictions.json.gz; their uncompressed canonical JSON
checksum is recorded in the report. Source paragraphs and unchanged reference
labels are copied byte-for-byte from the diagnostic at commit
4d221aa78f21d1968fe4878998a2ac9455d3b6aa under eval/scanner-failures/.
Their local copies, source-sample.json and reference-labels.json, are retained
alongside the report so this replay is self-contained.

## Remaining limits

Grammar and NER remain statistical. Phrases involving prepositions, coordination,
tables or unusual legal names can still split or include extra words. A broad
pool increases review noise by design. Whole-filing recall and precision under
the broader annotation contract still need a frozen, source-first review on
further unused issuers. Existing semantic accuracy gates are not certified by
these lexical controls or by this small replay.
