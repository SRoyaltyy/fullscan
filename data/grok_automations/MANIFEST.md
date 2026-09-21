# Grok Automations Dump Manifest

**Source:** Gmail (`from:noreply@x.ai`) — NOT `automation_get_results` / Automations MCP (unavailable).

**Generated:** 2026-09-21 (Asia/Hong_Kong / HKT)

## Summary

| Metric | Value |
|--------|-------|
| Dump dir | `/workspace/grok_automations_dump/` |
| Dated JSON files | 157 |
| Total items | 157 |
| Date range | 2026-08-13 → 2026-09-21 |
| Messages harvested | 157 |
| Full Gmail plaintext previews (`body_complete=true`) | 20 |
| Snippet-enriched excerpts (`body_complete=false`) | 137 |

## Per-task counts

| task_id | slug | kind | files | items |
|---------|------|------|------:|------:|
| `23a24524-4eaa-4b9e-bd39-5b30e2ec74ae` | hype-factor | ticker_news | 40 | 40 |
| `5b4f01c3-fe5b-463a-a270-8bc9def8e26f` | google-news-prompt | ticker_news | 39 | 39 |
| `7f250154-6401-4836-b5ff-5862211c5468` | 13-questions | macro | 39 | 39 |
| `unknown_news-parsing` | news-parsing | ticker_news | 39 | 39 |

## Notes

- Gmail query: `from:noreply@x.ai ("Hype factor" OR "Google News prompt" OR "13 Questions" OR "News parsing") after:2026/08/12` (4 pages, 161 threads; 4 dated 2026-08-12 excluded).
- Filename pattern: `{YYYY-MM-DD}_{slug}.json` — one file per (date, slug); same-day multi-runs would merge into `items[]`.
- Gmail Automation emails only contain **truncated previews** ending in Continue reading → `grok.com/chat/…`. Full automation run text is not stored in Gmail.
- Unsubscribe JWT links stripped / never written to dated dumps.
- Sector lean / Overall Market Condition automations were not in scope of this query.
- Provenance helpers retained: `_index_all.json`, `_bodies/`.
