# Lane headline fixes: development replay, 2026-10-13

The evaluated path was `news_intake.first_pass`, using only headlines and zero
model calls. Its failures therefore were in deterministic routing and missing
output wiring; this replay cannot diagnose a frontier model's reasoning or
prompt quality. The previous sector output was explicitly hard-coded empty.

## Causes and changes

| Cause | Change |
|---|---|
| Reaction-title rejection happened before extracting an embedded event | Recognize bounded event relations before applying the price-only filter; fix the macro-only path too |
| First-match keywords lost context and primary-event precedence | Match verb/object relations, prioritize merger review and acquisitions, distinguish sports walkouts and customer buybacks |
| Coverage omitted credit changes, placements, index flows, bankruptcy, factory openings, resource revisions and endpoint results | Add typed candidates mapped to existing enums where justified |
| Proposals, future trial results and retrospective approvals lost their stage | Emit explicit stages, preserve negation and interim decisions; retain mixed-guidance hygiene |
| Institution vocabulary and actor binding were incomplete | Add public administrative/personnel acts, passive rulings/relief and sovereign subject binding; preserve unknown jurisdiction |
| Sector fields were empty and gated behind institutional actions | Independently map activity nouns and strict company/catalog evidence, preserving provenance and unknowns |
| Coverage hints could still route a semantically rejected keyword to the wrong analyst | Keep those hints in review without overriding the supported routing class |

## Frozen 100-title replay

The original seed, titles, raw baseline and frontier reference were preserved.
This is a development replay after inspecting these failures, not a fresh
holdout accuracy estimate. The frontier reference was produced by the assistant
separately, not by an external API or independently adjudicated panel; its
original limitations (including partial visibility of the initial outputs)
continue to apply.

| Measure | Before | After |
|---|---:|---:|
| Concrete institutional-action agreement | 79/95 | 95/95 |
| Action recall | 4/20 | 20/20 |
| Action false positives | 0 | 0 |
| Lane class agreement | 41/94 | 94/94 |
| Sector answer coverage | 0/69 | 69/69 |
| Expected coarse sectors covered | Not measured | 69/69 |
| Exact coarse sector sets | Not measured | 68/69 |

Five ambiguous action judgments and six unsupported Lane enum mappings retain
their original exclusions. No new enum was added to make them pass. The original
sector reference used free-form industry/activity phrases; a published,
evaluation-only normalization maps these to coarse sectors. This measures
coarse sector agreement, not industry accuracy or verified stock exposure.

The exact-set disagreement is sample 94: “Gujarat offers MSMEs interest aid,
power subsidies and support for AI adoption”. The frozen reference lists SME
financing and industrial electricity use; the program additionally emits
Technology with the literal evidence “AI adoption”. This additional output is
retained and disclosed, rather than suppressing a supported activity to improve
the score. Consequently not every exact-set flag is green.

The local catalog used 4,465 company baseline rows (ticker, company, sector,
industry); private business-model descriptions were not provided to the runtime
or published. Production uses its existing latest repository Finviz export.
Catalog changes can change candidate mappings, and this replay does not prove
identical sector output for every possible catalog version. Supplemental public
business facts include their primary-source URLs in the output.

## Other checks and limits

All 56 available automated tests pass, including 17 new semantic test methods
covering price-only controls, negated injunctions, future results, stage binding,
ordinary-word/company collisions, footer tickers, explicit ticker matching,
multistory sector isolation and existing mixed-guidance hygiene. The earlier
18-case diagnostic passes 18/18 action checks, all 9 scored Lane checks and
11/11 sector-population checks. Sector population is not semantic accuracy.

These changes improve headline classification. Body/source verification,
legal implementation, issuer materiality, winners/losers and directional stock
analysis are separate deliverables and have not been validated by this test.
An unseen, independently labeled sample is still needed for a general accuracy
claim. Program execution reads no reference answers and receives no manual
corrections; development necessarily used observed failures to design fixes.

Reproduce using the frozen inputs distributed with the comparison artifact:

```bash
PYTHONPATH=. python scripts/evaluate_lane_headlines.py replay \
  --selection selection.json --output replay.json
PYTHONPATH=. python scripts/evaluate_lane_headlines.py compare \
  --baseline Lane_100_Raw_Program_Outputs.json --program replay.json \
  --reference Frontier_Title_Reference.json --report comparison.json
```
