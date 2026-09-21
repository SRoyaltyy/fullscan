# Grok Automations harvest

Research-only first-class source for `news_impact`. **Not a live trading path.**

Dumps live at `data/grok_automations/{date}_{slug}.json`. The loader treats a
missing or empty directory as zero items and does not crash. Coordinator may
drop a full dump tree in a follow-up; this path stays ready.

## Schema

```json
{
  "task_id": "23a24524-4eaa-4b9e-bd39-5b30e2ec74ae",
  "slug": "hype-factor",
  "kind": "hype-factor",
  "retrieved": "2026-09-21T16:00:00Z",
  "items": [
    {
      "headline": "…",
      "source": "sec.gov",
      "published": null,
      "retrieved": "2026-09-21T16:00:00Z",
      "task_id": "23a24524-4eaa-4b9e-bd39-5b30e2ec74ae",
      "raw_excerpt": "…"
    }
  ]
}
```

`source` on the harvested article is always `grok_automations`. When the same
fact also appears as a Finviz wrap, the automations headline ranks above it
(dedupe + harvest rank + mix).

## Known tasks

| task_id | slug | ingest |
| --- | --- | --- |
| `23a24524-4eaa-4b9e-bd39-5b30e2ec74ae` | hype-factor | normal router |
| `5b4f01c3-fe5b-463a-a270-8bc9def8e26f` | google-news-prompt | normal router |
| `7f250154-6401-4836-b5ff-5862211c5468` | 13-questions | **factor_impulse / regime only** (macro, never tickers) |

## Daily refresh (why GH Actions cannot do this)

GitHub Actions tokens **cannot** call the Grok Automations API. Do not add a
workflow that pretends they can.

Refresh is bot / Cursor:

1. Pull Automation mails from Gmail (`noreply@x.ai`), **or**
2. Call `automation_get_results` when an X/Grok connector exists in the session.
3. Convert with the ingest stub:
   `python3 scripts/ingest_grok_automations.py --date YYYY-MM-DD --file mail.txt`
   (or `--connector results.json`).
4. Commit the dated JSON. The next unique-title / corpus restamp reads it.

Helper: `src/news_impact/grok_automations.py` (parse + load). Stub CLI:
`scripts/ingest_grok_automations.py`.

## Hygiene that still applies

Reaction-title kill, guidance reaffirm/mixed, `entry_clock=retrieved_only` when
Published is missing, FOMC/macro collapse, and class-horizon 0-1d skip — same
rules as #306 / #307. Missing tape is listed as **no tape**, never invented.
