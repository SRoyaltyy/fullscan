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

Replay reads these frozen files plus the dated Grok pipeline
(`01_daily/news/{D}_{parsed,actions,judge}.json`, events). It never
calls a live Automations API. Raw Finviz CSV is not the article list.

It maps each article to the listed names it **names**, then overlays
those signals on `union_hot_n4_h1` and `flatten_h5`.

```bash
python -m src.grok_news_ticker_bt
```
