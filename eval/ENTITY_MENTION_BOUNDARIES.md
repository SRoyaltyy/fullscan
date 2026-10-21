# Complete organization mentions: development iteration, 21 October 2026

The article annotator now retains noun phrases and token-level dependency arcs,
with exact text offsets and the existing pinned spaCy/model identifiers. This
includes non-NER concepts such as `Wood chips`; those observations are available
for future attachment-aware sense selection. The concept resolver does not yet
consume them, and no context-universe clustering is claimed.

The shared Python gate now rejects an organization alias when its enclosing
parsed organization mention has a different complete name. `Apple` inside
`Apple Bank`, `Apple Records`, and `Green Apple Restaurants` no longer resolves
to Apple Inc. Legal suffixes and possessives remain allowed. This is a generic
whole-name comparison, with no Apple-specific list of names.

## Measurement

Re-ran the fixed synthetic 80-case development benchmark from PR #673, commit
`57050a1ac13d2caadfff83b9a43b1ae124bf4834`. Production base for this iteration:
`1e53faa0d793e5c792eff4f386592be6897ae2f3`. For the before run the base resolver
was restored; the candidate annotator's extra noun/dependency fields do not
affect base decisions. Both runs use the same cases and semantic resources.

| Pinned spaCy path | Before | Candidate |
|---|---:|---:|
| Correct link-or-no-link decisions | 51/80 (63.75%) | 54/80 (67.5%) |
| Link precision | 19/37 (51.35%) | 19/34 (55.88%) |
| Valid links recovered | 19/30 (63.33%) | 19/30 (63.33%) |
| False links | 18 | 15 |
| Unsafe links on ambiguous cases | 0/20 | 0/20 |
| Source-probe recovery | 6/12 | 6/12 |

Only O15, O16 and O17 changed, from false acceptance to rejection. No previously
correct development decision was lost. The unannotated Python/browser path is
unchanged at 56/80 (70%); all 80 browser/Python candidate comparisons agree.
The browser cannot run spaCy and receives no new grammatical capability here.

These are known synthetic development cases with Codex-authored labels, not a
new holdout, independently validated benchmark, real-news population estimate,
or FACA pass. The candidate remains inadequate for unattended identity linking.
Wrong NER boundaries can still cause missed links; missing NER boundaries cannot
activate this guard. Same-name people, negated affiliation, literal versus
figurative concepts, fallible type vetoes, and source recovery remain open.

## Validation and reproduction

271 existing offline integration/regression controls (including the new
whole-name control) passed. All 12 focused identity/pinned-parser controls
passed. Page integrity and Python/browser matching parity passed.

The full fixed benchmark and evaluator are in PR #673. On this candidate:

```bash
git fetch origin 57050a1ac13d2caadfff83b9a43b1ae124bf4834
git restore --source=57050a1ac13d2caadfff83b9a43b1ae124bf4834 -- \
  eval/entity-alignment-stress-v1.json \
  scripts/evaluate_entity_alignment.py \
  scripts/evaluate_entity_alignment_browser.mjs
python -m pip install -r requirements-filing-parser.txt PyYAML==6.0.3
python -m scripts.bootstrap_semantics
python -m scripts.evaluate_entity_alignment --spacy \
  --source-revision "$(git rev-parse HEAD)" --out /tmp/alignment-candidate.json
```

The adjacent JSON records input/source hashes, metrics and changed cases.
