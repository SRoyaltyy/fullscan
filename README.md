# fullscan

Live **US-market prediction + news + event + Finviz + paper-trade** system.
The daily bot (`Market-Bot-Automaton`) commits outputs to `main`. This repo is
both the engine **and** a git data lake (`01_daily/`, `data/`, scoreboards).
Structured tables also live in Supabase/Postgres via `DATABASE_URL`.

Dashboard: https://SRoyaltyy.github.io/fullscan/dashboard/

## vs [theme-radar](https://github.com/SRoyaltyy/theme-radar)

| | **fullscan** (this repo) | **theme-radar** |
|---|---|---|
| Job | Directional predict / outcome / reflect for the general market and 11 sectors, plus news, events, Finviz digest, AB checklist, paper trade | Closed-loop **theme / sector / stock** radar on daily Finviz Elite snapshots |
| Bot | Market-Bot-Automaton | Theme-Radar-Bot (still running daily) |
| Cadence | Predict ~9am ET, outcome 5pm, reflect 5:05pm; Sunday promote; monthly memory | Snapshot ~20:30 UTC, score/features ~21:00 UTC |
| Do not merge the two | Keep them separate unless a specific helper is verified missing here | Cleaner, smaller scoring loop |

Design and rubrics live in `00_grounding/`. Do not retune weights there unless
you intend a strategy change. Daily cycle detail: [`PREDICTOR_README.md`](PREDICTOR_README.md).

**Timezone:** market-day logic is `America/New_York`. HKT is display-only.

## Local run

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Smoke (no API keys, no writes to bot outputs)
python -m unittest tests.test_entrypoints -v

# Predict / outcome / reflect (needs secrets)
export DEEPSEEK_API_KEY=... FRED_API_KEY=... DATABASE_URL=...
python -m src.run_predict --date 2026-08-19
python -m src.run_outcome --date 2026-08-19
python -m src.run_reflect --date 2026-08-19

# Pre-market inputs
python -m src.finviz_digest
python -m src.news_parse
python -m src.run_news_judge
python -m src.run_events

# Liquid-universe AB checklist (needs Finviz export + price store)
python -m src.ab_checklist --date 2026-08-19 --top 20
python -m src.price_store status
```

Copy secrets from GitHub Actions — **never commit `.env` or keys**.

## Live GitHub Actions

Critical weekday chain (orchestrator re-dispatches misses):

| UTC cron | Workflow | Role |
|---|---|---|
| `5 10,12,14 * * 1-5` | `finviz_digest.yml` | Finviz AI digest |
| `43 10 * * *` | `events_daily.yml` | Event scan + catcher |
| `15 10,12,14 * * 1-5` | `news_parse.yml` | Mechanical news parse |
| `25 10,12,14 * * 1-5` | `news_judge.yml` | LLM news judge → B1 inject |
| `34 11 * * 0-6` / `3 21 * * 0-6` | `daily_pipeline.yml` | General predict / outcome / reflect |
| `40 12 * * 1-5` / `10 22 * * 1-5` | `sector_daily.yml` | 11-sector mirror |
| `50 10,11,12 * * 1-5` | `daily_orchestrator.yml` | Re-dispatch missed predict-side jobs |

Evening (staggered so they do not all push `main` at 22:30 UTC):

| UTC cron | Workflow |
|---|---|
| `30 21 * * 1-5` | `ab_checklist.yml` |
| `30 22 * * 1-5` | `ab_full_market.yml` (checklist + peers + PIT backfill) |
| `0 23 * * 1-5` | `hit_board.yml` |
| `0 5 * * 2-6` | `ab_full_scan.yml` (colors + Form4 + merge; after full-market window) |

Also live: collectors (`collect-*`, RSS/Reddit/NewsAPI/FRED/yfinance/SEC),
`news_actions` / `news_grade`, `learn_cycle`, `label_weather`, stock-book jobs,
`weekly_consolidation`, `monthly_distillation`, `report_card`.

Manual-only (kept): `ab_one`, `ab_one_button`, `ab_backfill`, `ab_enrich`,
`catalyst_*`, `industry_predict`, `price_checklist`, `run_migration`.

**Removed** (they could overwrite source on `main`): `restore_*`,
`patch_ab_backfill`, `auto_fix_ab_backfill`, `apply_finviz_wiring`,
`gemini-catcher`, `grok-test-harvest`.

New: `smoke.yml` runs entrypoint tests on pull requests only.

## Layout

```
00_grounding/     rubrics, prompts, segment registry  (strategy — leave alone)
01_daily/         bot predict/outcome/reflect artifacts
02_lessons/       candidate / active / archive
03_scoreboard/    HIT board + scoreboard.json
src/              prediction + AB + news + sector engines
collectors/       news / macro / SEC / Finviz / catalyst
data/             Finviz exports, prices, AB outputs, paper book
.github/workflows live Actions
```
