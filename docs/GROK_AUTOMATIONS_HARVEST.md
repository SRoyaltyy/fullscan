# Grok Automations harvest

Research-only first-class source for `news_impact`. **Not a live trading path.**

Dumps live at `data/grok_automations/{date}_{slug}.json`. The loader treats a
missing or empty directory as zero items and does not crash.

**This tree is a Gmail harvest, not live Automations API.** Source:
`from:noreply@x.ai`. Automations MCP / `automation_get_results` was
unavailable. Gmail stores **truncated previews** (Continue reading →
grok.com/chat/…). On the 2026-08-13 → 2026-09-21 dump: **157 files / 157
items**, `body_complete=true` on **20/157** only. Do not treat snippet
excerpts as full automation runs.

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
| `23a24524-4eaa-4b9e-bd39-5b30e2ec74ae` | hype-factor | normal router (40 files in the 08-13..09-21 dump) |
| `5b4f01c3-fe5b-463a-a270-8bc9def8e26f` | google-news-prompt | normal router (39) |
| `7f250154-6401-4836-b5ff-5862211c5468` | 13-questions | **factor_impulse / regime only** (macro, never tickers) (39) |
| `unknown_news-parsing` | news-parsing | normal router (39; no Automations task id in the Gmail subject) |

## Daily refresh (why GH Actions cannot do this)

GitHub Actions tokens **cannot** call the Grok Automations API. Do not add a
workflow that pretends they can.

Refresh is **bot / Cursor Gmail harvest → commit dumps**. Actions must not
call live Automations.

1. Pull Automation mails from Gmail (`noreply@x.ai`). Expect truncated
   previews (`body_complete=false` on most). Full run text is not in Gmail.
2. If an X/Grok connector exists later, prefer `automation_get_results`
   (complete bodies). It was unavailable for this dump.
3. Convert with the ingest stub:
   `python3 scripts/ingest_grok_automations.py --date YYYY-MM-DD --file mail.txt`
   (or `--connector results.json`).
4. Commit dated JSON only (`{date}_{slug}.json` + `MANIFEST.md`). Skip
   `_index_all.json` and `_bodies/` — those are harvest provenance, not
   the router input.

Helper: `src/news_impact/grok_automations.py` (parse + load). Stub CLI:
`scripts/ingest_grok_automations.py`.

## Hygiene that still applies

Reaction-title kill, guidance reaffirm/mixed, `entry_clock=retrieved_only` when
Published is missing, FOMC/macro collapse, and class-horizon 0-1d skip — same
rules as #306 / #307. Missing tape is listed as **no tape**, never invented.
