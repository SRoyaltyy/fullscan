# Free news intake: operation and coverage

The service collects all configured free sources into a durable day ledger,
then parses every saved document. No API keys, paid feeds, model inference or
new hosting are used by this service. Existing paid or authenticated model
workflows are not enabled by it. Standard GitHub-hosted runners are used in
this public repository; no larger runners or external compute are provisioned.

## Run and inspect

```bash
python -m unittest src.test_news_intake
python -m src.news_intake --date 2026-10-02 --force
python -m src.news_intake --date 2026-10-02 --parse-only
```

The date is the Hong Kong calendar date; timestamps and rolling collection
windows are UTC. Default lookback is 48 hours, so a day report deliberately
includes relevant prior-session material. `today_hk_count` reports publication
on the requested HK date separately. `--parse-only` makes no remote requests.

The `Free news intake` GitHub workflow runs at minutes 7, 22, 37 and 52 of
every hour, seven days a week. Sources have independent 15/30-minute due times.
It publishes a quick harvest first, before slower overflow and filing scans.
The minute-52 run then performs deeper backfill. Manual dispatch has a `deep`
switch. A quick capped result stays explicitly partial; previously saved
evidence remains in the ledger. This keeps a long SEC sweep from delaying the
first published headlines.
GitHub scheduling may be delayed; this is not a guaranteed 15-minute delivery
SLA. The workflow also supports manual dispatch and saves its ledger in git
before publishing. It does not allocate billed Actions artifact storage.
Successful job execution is not proof of complete coverage;
read `coverage_status` and the source health table.

## What is collected

- Existing fullscan publisher and Google RSS sources, except defunct Reuters
  URLs. Reuters/AP/Bloomberg publisher searches supplement live publishers.
- One search definition for each non-discard Lane class, plus business,
  technology, products, FDA, Fed, government, legislation, court, central-bank,
  geopolitics and corporate-announcement searches. Interpretation-heavy classes
  are discovery vocabulary, not a guarantee that a search determines the class.
- Direct Fed, BLS, BEA, ECB, Bank of England, FTC, DOJ and tech-company feeds;
  FDA, Treasury, Commerce and Supreme Court listing monitors; the Federal
  Register via GovInfo; institution-specific fallback searches.
- SEC current-filing feeds for 8-K, 6-K, Form 4, both old/new 13D names, S-1,
  S-3 and tender offers. Up to 20 pages per form; unfinished windows are marked
  saturated. Filing indexes can be followed to primary documents or release
  exhibits during extraction. This is not a comprehensive docket/filing archive.
- Same-day existing Finviz exports, parsed news, action/event records and
  available Grok dump files. Legacy generated events are explicitly reviewed
  rather than being treated as verified original announcements.

The implementation consolidates reusable pathways in fullscan; it does not
restart redundant collectors in every repository. NewsAPI/GNews paid endpoints
and fresh Grok mail acquisition are excluded. Available local dumps are read.

## Add sources

Edit `config/news_intake_sources.json`. Supported kinds are `feed`, `search`,
`page` and `sec`. Each needs a unique `id` and `interval_minutes`. Feeds need
`url`; searches need `query`; page monitors need `url` and `link_pattern`.
Company IR monitoring currently comprises broad IR discovery, filing intake,
several direct company blogs and any explicit registry additions. It is not
direct monitoring of every listed issuer. Add issuer-specific endpoints here.

## Storage and handoff

`data/news_intake/YYYY-MM-DD/` contains:

| File | Purpose |
|---|---|
| `documents.json` | Full retained feed/page evidence, URLs, clocks and discovery provenance |
| `parsed.json` | Every document's themes, candidate classes, existing Lane rule classification and source health |
| `review_queue.json` | Unknown dates, rumours, unfamiliar headlines and legacy generated events |
| `lane_queue.json` | Evidence references and family routing for subsequent contextual analysis |
| `headline_groups.json` | Conservative normalized-headline groups; not semantic event clusters |
| `runs/*.json` | Per-run summary and source-health history |

Human report: `01_daily/news/YYYY-MM-DD_intake.md`.
Dashboard: `dashboard/news-intake/index.html`; downloaded JSON includes all
rows even when the browser displays a limited number.

The old headline loader unions the ledger with available DB/RSS/local evidence.
The Lane corpus loader reads full documents from the ledger. The news judge's
single-name field mismatch is repaired. The intake preserves unknown/unmatched
items instead of permanently dropping them. No stock-book trading rules change.

The parser is deterministic. Candidate phrases do not prove an event happened,
that it is fresh, or that it matters to the book. Existing Lane rule routing is
recorded independently of search hints. No JEV or LLM calls are made. The queue
references complete evidence in `documents.json`, avoiding a second full-text
copy. Disagreement with a concrete coverage hint is kept visible and can route
that hint for confirmation; it does not silently discard a board appointment
or supply agreement merely because the old rule router has no matching rule.
The queue
is ready for contextual analysis but this workflow does not execute stronger
model analysis. `final_lane_class` remains null rather than claiming certainty.

## Failure handling and limits

Each source has its own allowance; no busy topic can stop the rest from being
polled. Google search overflows trigger date and phrase follow-up queries.
Remaining capped leaves are flagged. Failed direct feeds can use explicit
Google fallback queries; the original error and index dependency stay visible.
Empty results are unverified, not declared a complete quiet day.

Documents land before extraction. Failed extraction retries have a cooldown
and a three-attempt dead-letter state. Page text is unverified extraction, not
guaranteed article-body isolation. PDFs and some formats need further adapters.
Google News links are retained but currently not reliably resolved to full
publisher articles. A substantial part of the first harvest is headline/snippet
evidence. There is no paywall bypass or paid content retrieval.

No permanent drops, inferred publication times, claimed calibrated confidence
scores or claimed verified event recall are produced. The saved record is
durable in git; scheduled writes can increase repository storage over time.
Keep an eye on growth before expanding source volume or lookback substantially.

## Compare independently selected events

Freeze a CSV before looking at pipeline output:

```csv
event_id,title,keywords,published_at,importance
event-1,Company X announcement,Company X;acquisition,2026-10-02T08:00:00Z,major
```

```bash
python -m src.news_intake_compare --date 2026-10-02 --expected checklist.csv
```

`comparison.json` shows not-found events and possible evidence matches with
discovery latency where clocks exist. A human must confirm event identity and
the relevant update. Keyword overlap is not automatically counted as verified
recall. Today has a real harvested corpus for this comparison; it has not yet
passed an independent completeness benchmark.
