# Subscription bulk screen: 100–200 headlines in one prompt

The active requirement is to use the existing ECS/OpenClaw route to the user's Grok Ultra subscription, classify 100–200 parsed news headlines in the same prompt, return one compact decision per input, and avoid spending allowance on repeat articles. Direct paid xAI API access is not used. The subscription plan identity is supplied by the user; the gateway does not provide a verified weekly allowance counter.

## Execution

`PYTHONPATH=. python scripts/run_lane_bulk_screen.py --input parsed_news.json --output results/screen.json --ledger /retained-volume/lane_bulk.sqlite3 --batch-size 200 --max-new 200 --model xai/grok-4.3 --timeout 1200`

Input is an array of unique integer `n` plus `title`, optionally `url` or `source_url`. Classification scope remains title-only, matching the preceding evaluations. Each batch uses one gateway HTTP request with the existing agent and requested backend header. There is no JSON-repair call, subdivision, provider hopping, direct paid API fallback, or automatic resend of failed articles. The output allowance is 240 tokens per item, capped at 48,000; the gateway may impose a smaller limit, which the capacity test will reveal. A 200 response with missing/invalid items remains incomplete rather than becoming negative decisions.

The prompt combines the locked Lane ontology, the latest action-stage/authority rules and sector rules. Compact action atoms retain actor type, kind, stage and literal evidence; Python derives the existing concrete public-action decision. The full two-stage V15/V16 scores do not transfer to this compact single-pass implementation. Its 200-article development accuracy must be measured separately.

JSON keeps validated decisions and request metadata. The adjacent Markdown file renders point-form ID, YES/NO/UNCERTAIN/UNAVAILABLE, Lane and short reason. Private corporate acts retain the existing distinction from public institutional actions; this revision does not silently broaden that definition. The response includes exactly one validated result per supplied ID or marks it unavailable. Duplicate/unknown response IDs fail closed. Evidence must come from that item's own headline.

## Deduplication and recovery

The SQLite ledger claims an article transactionally before a network request. Identity uses a conservative normalized headline core (Unicode/HTML normalization, case/whitespace and final publisher suffix) plus a stable URL alias without tracking parameters/fragments. Numbers and event wording are preserved; similar events are not merged just because their numeric details differ. Cached duplicate decisions are rebound to the current presentation ID, with raw decisions retained under the original ledger article ID.

Completed, failed and in-flight claims persist across runs and process restarts. A failure or uncertain timeout is not automatically retried: an attempted request might already have consumed allowance. Claims for later unsubmitted batches are not made when the run stops. Unvisited input rows are explicitly marked not_submitted. The ledger refuses a changed method/model/catalog fingerprint rather than silently reprocessing the pool. Use a separately named ledger only for an intentional new experiment.

The ledger must live on a retained volume and be shared by every process that needs cross-run deduplication. The workflow uses `/home/gha/fullscan_state/lane_subscription_bulk_v1.sqlite3` on the existing runner and backs it up as an artifact; a replacement runner must restore that ledger before processing the same pool. This code cannot promise exactly-once model execution after an ambiguous network failure; it enforces at-most-one automatic submission per article in the ledger, leaving ambiguous failures unresolved.

## Capacity experiment

Workflow `lane_bulk_capacity.yml` replaces the superseded queued two-pass Grok development run through the same concurrency group. It performs the complete Linux contract suite first, then checks production idle and probes only selected Grok 4.3. It does not restart services. At most one health probe and two article-analysis requests are planned:

- One request containing 100 unique previously exposed development articles.
- If complete, one request containing 200 disjoint articles from the existing development sets 01–02, with independent reference labels available for offline grading.

The samples have zero identity overlap across the 300 articles. Unopened future holdouts 03–12 are not consumed. The persistent ledger prevents resending these articles on a workflow rerun. A cached rerun is explicitly distinguishable from a new 100/200-item capacity observation. The offline grade marks every unavailable item wrong, reports all five existing metrics and provides no fresh acceptance credit.

Metrics include actual gateway request count, articles submitted/validated/reused, elapsed time, prompt/response characters, output truncation finish reason, HTTP throttling, numeric provider-reported token usage and selected numeric quota headers if the gateway exposes them. Neither provider token counts nor number of prompts establish how the subscription debits its weekly allowance. The report leaves weekly allowance remaining null unless a future supported usage source makes it observable. No API-dollar estimate is used as a proxy for Ultra usage. This controlled test measures per-prompt capacity and throughput; it does not deliberately exhaust the user's weekly allocation.

Local verification: 10 new contract tests, 77 runnable semantic tests and 5 provider tests pass on Windows. Two existing POSIX runner tests are deferred to Linux CI, which runs 79 semantic + 10 bulk + 5 provider tests. The offline grader was also checked against synthetic perfect decisions and an unavailable item without any model calls. Live bulk accuracy and weekly capacity remain unmeasured until ECS execution and observable allowance data are available.
