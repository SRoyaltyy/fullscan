# Frozen Grok automation dumps (Stage 1)

Schema: `grok_automation_harvest_v1`

Each file is `{YYYY-MM-DD}_{task}.json` with UTC `createTime`,
`conversationId`, and raw text/JSON. `_coverage.json` is the harvest
report.

## How to harvest (NOT a GitHub Action)

GitHub Actions cannot see automation run logs. Run this from **Cursor /
Grok Bot** with an Automations connector:

```bash
python -m src.grok_automation_harvest --since 2026-08-13
```

If you already pulled results locally:

```bash
python -m src.grok_automation_harvest --ingest /path/to/raw_dumps
```

Task slugs: `news_parsing`, `google_news`, `hype_factor`,
`macro_intelligence`, `sector_fime`, `sector_defensive`, `sector_tech`,
`human_sources`, `overall_market`.

Skip 13 Questions and Webull STANDTEST.

## Stage 2

Replay reads these frozen files plus dated repo news only. It never
calls a live Automations API.

```bash
python -m src.grok_news_ticker_bt
```
