# Primary-origin action scanner

This collector parses literal headlines and publisher metadata from official sources. It makes zero LLM or paid inference calls. It does not label proposals as enacted policy, or claim that every official announcement is a concrete action.

The maintained source catalog records verified channels, failed endpoints and exclusions. Institution records overlap across directories and curated source owners and include closed/historical bodies. They are not a count of unique active institutions or working websites. A shared portal can cover many issuing organisations cheaply.

The workflow is intended to run twice daily at 01:17 and 13:17 UTC (09:17 and 21:17 Hong Kong), with a rolling three-day window, weekly catalog refresh and persistent SQLite deduplication restored between runners. Scheduled operation only becomes active after the workflow reaches the default branch. Full worldwide completeness and semantic classification accuracy are not claimed.

See the source catalog, taxonomy, health reports and workflow for implementation details. The experimental Lane/LLM benchmark remains separate.

## Persistent operation and outputs

The collection job has read-only repository permissions, disabled checkout credentials and no user secrets. Its HTTP client permits only official hosts, public GETs and conditional-cache headers; it blocks external redirects and never executes page JavaScript. A separate publication job commits only the public catalog and headline outputs.

Each successful run uploads a 90-day SQLite state artifact. The next runner restores it, including canonical URL aliases, source-scoped native IDs and previous source response state. Promotion to main reuses the verified PR state instead of resetting IDs. If no state artifact can be recovered, the job explicitly reports a bootstrap; deduplication beyond the artifact retention period is not guaranteed. Full discovery runs weekly or after source/adapter changes. If a refresh fails, an existing catalog may continue in degraded mode; refresh outcome and catalog age remain visible. Total collection failure fails the job.

| Latest output | Meaning |
|---|---|
| headlines.md | Current three-day dated view, grouped by publisher stage |
| report.json | Counts, source health, capacity/freshness warnings and zero-inference counters |
| current_documents.json | Current view; existing documents may remain visible here |
| new_items.json | Newly retained articles this run, including undated items |
| pending_headlines.json | Newly retained dated headlines only; repeats never re-enter this incremental queue |
| action_documents.json | Source-typed published decisions/rules/orders/laws, not a semantic action verdict |
| proposals_and_procedure.json | Proposals and procedural material kept separate from final stages |
| undated_review.json | New articles without usable publication dates |
| updated_documents_review.json | Existing documents revised by their original publisher, with stable IDs |
| integrity.json | SQLite integrity, unique count and audited official GETs |
| run_status.json | Catalog refresh outcome and restored state run |

The new-item queue is incremental, not an acknowledged delivery queue. Downstream Lane/Grok classification must keep its own persistent inference/delivery ledger, consume stable article IDs and recover unconsumed batches from retained run artifacts. This collector does not automatically invoke the Ultra subscription or assert that 100–200-article batches meet the separate accuracy target.

## Coverage limits

- A publication date is not necessarily the decision date or effective date. Old cases may be newly uploaded.
- Proposed rules, introduced/enrolled bills, hearings, notices and press announcements are not automatically enacted policy. Semantic action verdicts remain null.
- GOV.UK indexing does not cover every organisation, every language, or bodies publishing elsewhere. Closed and exempt bodies remain visible in the registry.
- GovInfo covers selected participating courts, not all PACER or all courts. Paid case documents are not fetched.
- A finite RSS feed may overflow between polls. The collector warns when a full feed remains entirely inside the requested window. Twice-daily snapshots do not prove complete recall.
- Federal Register result-ceiling warnings require a narrower date window/backfill if triggered.
- Directory identities, overlapping channel owners and verified working channels are counted separately. Many international bodies still have disabled/unavailable channels; worldwide completeness is not claimed.
- URL/native-ID deduplication does not prove two different publications describe the same real-world event. Event clustering remains downstream.
- No semantic accuracy score or measured daily usable-action percentage is inferred from collection counts.

## Run and extend

```sh
PYTHONPATH=.:scripts python -m unittest discover -s scripts -p test_primary_actions.py
PYTHONPATH=. python scripts/build_primary_action_catalog.py
PYTHONPATH=. python -m src.primary_actions --days 3 --workers 8 --state data/primary_actions_state/ledger.sqlite3 --output data/primary_actions/latest
PYTHONPATH=. python scripts/primary_action_ops.py audit
```

Add official discovery pages to config/primary_action_seeds.json. Prefer documented APIs and advertised RSS/Atom feeds. An HTML adapter requires an explicit publication-link selector and useful dates. Typed document labels are appropriate only for consistently typed channels. Never enable a source merely because it returns HTTP 200.
