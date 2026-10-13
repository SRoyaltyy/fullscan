# Headline event and institutional-action annotations

News intake, saved Lane queues, compact dashboard records and one-shot results
preserve independent `headline_event`, `institutional_actions` and
`sector_classification` fields. Recognition runs on the title; a trading
abstention does not erase these fields. None establishes stock direction.

`headline_event` records a typed reported event, Lane enum when justified, stage,
sign and literal evidence span. Embedded events are recognized before reaction
packaging is discarded. Specific relations take precedence over incidental
keywords. Customer buybacks, supply agreements and product launches retain a
typed `enum_gap` rather than being forced into an unrelated locked Lane enum.
Future results, negation, retrospective approvals and proposed actions preserve
their stage. Existing mixed-guidance hygiene remains in force.

`institutional_actions` records authority, type, jurisdiction, authority level,
category, action, subject, stage and literal spans. Explicit US federal, state
and territorial authority patterns remain supported. Bare courts, omitted
issuers and a governor's surname without geographical context can remain
unknown. Country references to counterparties do not replace the acting
sovereign. Administrative acts and public communications are concrete reported
actions, but are not necessarily implemented economic policies. A chamber's
passage is not enactment, and an interim injunction is not a final judgment.
An FDA-approved adjective is not a fresh approval announcement.

`sector_classification` is independent of institutional recognition and Lane's
trade gate. It combines bounded economic activity nouns, strict company names,
explicit tickers and the repository's latest Finviz export, plus source-linked
supplemental business facts. Each match states its basis, evidence and, where
applicable, company identity and source. Catalog availability is explicit.
Publisher/footer tickers and ambiguous partial company aliases do not count.
The output separates canonical sectors from detailed activities; it describes
relevance, not verified material exposure. Missing support remains `unknown`.

For compatibility, per-action `lane_classification` and `market_sectors` are
populated from the action's own semicolon-delimited headline segment, with
`sector_scope` stating that this is segment relevance. Other stories' sectors
are not copied onto the action. Unknown Lane mappings remain null.

The routing class no longer substitutes unconfirmed lexical coverage hints for
a semantic rejection. Such hints remain available for contextual review.
Newly recognized dated events can enter the candidate queue; uncertain stages
and enum gaps stay in review. All titles remain in the saved Lane queue.

These are bounded deterministic rules. They are not a general legal entity
resolver, primary-source fact verification or an exhaustive reasoning engine.
See [the development evaluation](evaluation/lane-headline-fixes-2026-10-13.md)
for measured results, exclusions and replay limitations.

Validation: `PYTHONPATH=. python -m unittest discover -s scripts -p 'test_*.py'`.
The separate `scripts/evaluate_lane_headlines.py replay` invocation reads only
selected titles; `compare` reads the frozen reference without executing runtime
analysis. Reference labels are never imported into production modules.
