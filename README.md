# fullscan

Live **US-market prediction + news + event + Finviz + paper-trade** system.
The daily bot (`Market-Bot-Automaton`) commits outputs to `main`. This repo is
both the engine **and** a git data lake (`01_daily/`, `data/`, scoreboards).
Structured tables also live in Supabase/Postgres via `DATABASE_URL`.
`src/` **reads** `news` and `macro_indicators` only — it never writes the DB.
`DATABASE_KEY` is unused.

Dashboard: https://SRoyaltyy.github.io/fullscan/dashboard/

## Two machines (do not collapse)

**A — Daily LLM loop** (general market + 11-sector twin). Channel 1 fetch →
DeepSeek predict → outcome → reflect → distill / promote / `learn_cycle`.
Scoreboard: `03_scoreboard/scoreboard.json`. The LLM does **not** score 6,000
names.

**B — Stock book** (mechanical). `segments` × `weather` × `join` → news edges →
`stock_book` → paper dashboard via `run_stock_book_all`. Design thesis:
**label → regime → join**. Rubric weights stay in `00_grounding/` — do not
retune them here.

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
python -m src.run_sector_predict --date 2026-08-19 --sectors Technology
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

# Stock book (machine B) — label → regime → join
python -m src.segments
python -m src.weather
python -m src.join
python -m src.run_stock_book_all --date 2026-08-19 --skip-llm

# Paper P&L (after Futubull fees) — rebuild scoreboard from saved curve
python -m src.paper_trade --from-store
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
| `40 12 * * 1-5` / `10 22 * * 1-5` | `sector_daily.yml` | 11-sector mirror (same news_judge + finviz_digest inject as general predict) |
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

Manual leftover workflows **remain in the tree** (not deleted): `restore_*`,
`patch_ab_backfill`, `auto_fix_ab_backfill`, `apply_finviz_wiring`,
`gemini-catcher`, `grok-test-harvest`. Do not dispatch them casually —
`restore_*` / `auto_fix_ab_backfill` / `apply_finviz_wiring` can overwrite
source on `main`. `restore_catalyst_analysis.yml` also runs if that YAML
itself is pushed. `gemini_catcher.py` stays for `collect-catalyst`
(`GEMINI_BROWSER_STATE`); it is not on the daily LLM or stock-book chain.

Obvious junk removed only: `src/_ab_backfill.b64.p0`–`p3`, `src/_ab_src.p0`
(PLACEHOLDER* split dumps).

New: `smoke.yml` runs entrypoint tests on pull requests only.

## Layout

```
00_grounding/     rubrics, prompts, segment registry  (strategy — leave alone)
01_daily/         bot predict/outcome/reflect markdown (not _transcripts/)
02_lessons/       candidate / active / archive
03_scoreboard/    HIT board + scoreboard.json
src/              prediction + AB + news + sector engines
collectors/       news / macro / SEC / Finviz / catalyst
data/             live bot CSVs the Actions still publish; see .gitignore
.github/workflows live Actions
```

**Future growth (gitignore only — current tracked files were not purged):**

| Path | Why | Workflows that used to add them |
|---|---|---|
| `01_daily/_transcripts/` | raw LLM JSON | `daily_pipeline` (`git add -A`), `sector_*`, `news_judge`, `events_daily` |
| `data/prices/ohlc.parquet` | ~75MB OHLC store | `ab_full_market`, `ab_one_button`, `ab_backfill`, `price_checklist` |
| `data/exports/finviz_YYYY-MM-DD.csv` | ~11MB/day Elite dumps | `label_weather`, `stock_book_all` |

Rolling Finviz snapshot remains `data/finviz/latest.csv`. `git add -A` jobs run `scripts/unstage_growth_blobs.sh`. Dated join/universe/insider/checklist CSVs are gitignored for accidental adds; the stock-book / AB / insider jobs still `git add -f` the files they publish.
