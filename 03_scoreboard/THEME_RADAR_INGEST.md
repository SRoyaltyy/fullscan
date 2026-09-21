# Theme-radar headline ingest (no repo merge)

Cyrus 2026-09-22: theme-radar is the Elite camera. fullscan reads four columns.

## Acceptance
`data/snapshots/2026-09-18.csv` in SRoyaltyy/theme-radar:

```
AMRX | Amneal Announces FDA Approval and Launch of Lanreotide Injection | 2026-09-18 16:01:00
```

That row never entered `01_daily/news/2026-09-18_parsed.md` (04:20 ET, before the PR) or Grok Automations (EPA / Trump-Xi).

## How
`src/news_impact/theme_radar.py`
- local: `THEME_RADAR_ROOT`, `vendor/theme-radar/data/snapshots`, `data/theme_radar_snapshots`
- else: raw.githubusercontent.com for a **named date** (not the whole history over HTTP in CI)
- skips `.raw.csv` and blank News Title

`python -m src.news_impact_backtest --date 2026-09-18` after this branch loads those titles into the combined book (`harvest_source=theme_radar_elite`).

Actions: checkout `SRoyaltyy/theme-radar` → `vendor/theme-radar` (sparse `data/snapshots/*.csv` only).

Do not copy 11MB raw files into fullscan.
