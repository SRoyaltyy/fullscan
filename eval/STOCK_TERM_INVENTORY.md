# Stock-first filing term inventory

The stock side defines the reference knowledge: every paragraph's terms and
their local meaning/evidence, then the company's relationships to those terms.
The article side must resolve mentions into that reference and report their
article role. It must not equate a company with every industry its partners
operate in.

## Implemented in this iteration

Every prose window now retains all spaCy named entities, noun chunks, and
NOUN/PROPN token candidates, even when no relationship rule recognizes them.
The model and extraction are pinned and deterministic; this is exhaustive
retention of parser candidates, not a guarantee of perfect semantic recall.
Every original source block remains retained. XML/JSON attachments remain
machine inventory rather than being misrepresented as natural-language prose.

`graph.term_inventory.terms[]` contains a separate occurrence for each source
span. It carries the source/paragraph/window IDs, exact phrase and normalized
form, extraction kinds, NER types, head span, noun phrase, context fingerprint,
and grammatical capacity (dependency path, governor, subjects, objects,
negation and modals). Shared sentence text/features are stored once in
`graph.term_inventory.windows[].contexts[]`; an occurrence's
`context.sentence_id` refers to that record. Window syntax retains every token
and dependency head. All indices use exact source character offsets.

`office space` and `space missions` therefore retain different occurrences,
noun phrases, grammatical attachments, and context fingerprints for `space`.
They are **not yet assigned resolved real-estate/aerospace sense universes**.
A fingerprint is an observation identifier, not a semantic identity.

Known extracted relationships now retain exact argument and trigger spans.
Occurrence-to-relationship links require membership in that actual argument;
an unrelated name elsewhere in the paragraph cannot inherit the relationship.
The link retains the full relationship object and says
`attachment: within_extracted_argument`; a contained noun is not silently
promoted into a separate supplier/partner. Unsupported relationships remain
unresolved while the raw grammar stays available.

The keyword projection retains dated occurrence references, excludes later
filing occurrences at historical cutoffs, and leaves unmatched inventory
candidates out of stock passes. Article annotations use the same term parser;
the scanner keeps those references rather than retaining every issuer's full
term graph in memory. The complete inventory remains in the profile assets.
Existing keyword candidates now carry the article's actual grammatical role.
External-entity candidates expose an indirect path (`order: 2`), with
`core_industry_inferred: false`, context alignment unassessed, and impact not
inferred. The browser renders source occurrence context/relations and cached
article capacities; custom browser text has no spaCy parser.

## Playboy / DraftKings contract

The regression fixture is a **synthetic illustration of the requested path**,
not a newly verified PLBY SEC quotation:

| Layer | Recorded observation |
|---|---|
| Stock fixture | `PLBY → partner → DraftKings` |
| Article A | `DraftKings → faces → new gambling regulations` |
| Article B | `DraftKings → sponsors → a charity concert` |
| Connection | Same DraftKings catalog identity; an indirect partner path to PLBY |
| Not established by that match | PLBY's core gambling exposure, article-context relevance, financial impact |

Both articles can identify DraftKings while reporting different capacities.
A generic article mentioning gambling firms without DraftKings does not gain
this partner link from the fixture. A later relevance/impact stage must assess
whether the article action bears on the specific filing-defined relationship.

## Validation

The complete real Backblaze archive replay (all ten archived accessions) retains
463 source sections and 31,069 blocks, analyzing all 3,303,752 prose characters.
It nominates and retains 302,422 distinct term occurrences and preserves all
455 existing relationship claims. 887 occurrences link to explicit extracted
relationship arguments; the other candidates remain available with their raw
grammar and unresolved relationship status. These are coverage counts, not
semantic precision or recall. See `stock-term-inventory-real-coverage.json`
for the exact input/compiler/output hashes and model pins.

271 offline integration controls and 35 pinned grammar/identity/inventory
controls passed locally, as did page integrity and matching parity checks.
Controls cover cue-free paragraphs, every nominated candidate, context
separation, argument boundaries, cache relocation, duplicate sources, dates,
missing inventory, forged links, exact article annotations, deterministic
serialization, and access to all paginated occurrences.

The existing synthetic 80-case identity benchmark remains unchanged at 67.5%
with spaCy and 70% without annotations. Targeted source probes recover 7/12
(previously 6/12); preserving original possessive punctuation fixes the
McDonald's source-name probe. These known development cases are not a fresh
holdout or FACA pass. Full sense clustering/resolution and exhaustive semantic
relationship recovery remain unfinished.

## Build and inspect

The normal `scripts.build_filing_profiles` pipeline automatically includes this
inventory for every issuer and paragraph; no opt-in term/paragraph limit is
introduced. The compiler fingerprint includes the inventory implementation,
so existing compiled profiles require rebuilding before this code can consume
them. The refresh workflow refuses old compiler artifacts and can publish when
the new complete compilation succeeds. This PR does not claim that all issuers
have already been rebuilt or that the live scanner has been updated.

To replay an existing full source graph offline:

```bash
python -m scripts.replay_filing_terms --graph /path/to/issuer.json.gz \
  --all --out /tmp/issuer-term-replay
```

This reconstructs and verifies the complete archived source text before
parsing. `graph.json.gz` contains all occurrences, contexts and dependency
arcs; `report.json` records coverage/hashes. Selecting `--accession` instead
replays every source of that one accession, never an excerpt.
