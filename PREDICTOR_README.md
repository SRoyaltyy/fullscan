# Autonomous Market Prediction Engine

Self-improving US-market direction predictor running on GitHub Actions.
(Companion to the data collectors in this repo — the prediction pipeline
reads the `news` and `macro_indicators` tables those collectors maintain.)

## Daily cycle (Mon–Fri, America/New_York)

| Time | Stage | What happens |
|---|---|---|
| 9:00 AM | `predict` | Channel 1 fetch (FRED/yfinance/Supabase, exact numbers, archived to `01_daily/_channel1/`) → DeepSeek with live web search → component scores → **Python computes** weighted total, divergence rule, direction + magnitude → `01_daily/general/<date>_predict.md` + scoreboard |
| 5:00 PM | `outcome` | Actual close fetched → DeepSeek reviews the day with verified citations → prediction graded (direction hit, magnitude hit) → `<date>_outcome.md` |
| 5:05 PM | `reflect` | Diagnostic engine classifies any miss (missing evidence / misweighted / miscalibrated) → candidate lesson in `02_lessons/candidate/` |

Sunday 11 AM: repeated candidate lessons (≥2 similar triggers) are promoted to
standing rules in `02_lessons/active/`. 1st of month: `04_consolidated_memory.md`
rewritten (<1,500 words) from the whole scoreboard; stale candidates archived.

## The self-improvement loop

Every prediction is forced to read, before predicting: the master rubric,
consolidated memory, all active lessons, the full scoreboard (rolling
accuracy), and the last 10 trading days of its own predictions + reflections.
Its first output line must confirm what it learned — otherwise it aborts.

## The stock-book learning loop (closed)

The daily stock book has its own feedback cycle, keyed to the one metric
that matters — the paper dashboard:

| Step | Module | Output |
|---|---|---|
| Preflight | `src/input_health.py` | `data/stock_book/{date}_input_health.json` — stale/degraded/missing inputs; the ranker renormalizes weights over the families actually present |
| Rank | `src/stock_book.py` | reads learned weights from `00_grounding/book_policy.json` (bounded to ±0.12 of code defaults); sell book ranks on the core score, without buy-side add-ons |
| Learn | `src/book_learn.py` | walk-forward weight tuner on realized forward returns (local price store, no lookahead). Guardrails: ≥5 dates, ≥5bps mean improvement, wins ≥60% of dates, half-step adoption. Ledger: `03_scoreboard/BOOK_LEARN.md` |
| Reflect | `src/book_reflect.py` | gap scan of movers the book missed, classed blind / outweighed / gated-out (`03_scoreboard/BOOK_GAPS.md`), then the reasoner (`grok-4.6` if `XAI_API_KEY` is set, else `deepseek-reasoner`) writes book-scoped lessons into `02_lessons/candidate/` and maintains `02_lessons/hypotheses/book_missing_inputs.md` — the book's own list of what it cannot currently see |

## Secrets used

`DEEPSEEK_API_KEY`, `XAI_API_KEY` (optional — wires Grok into the
reasoner slots), `FRED_API_KEY`, `DATABASE_URL`, `DATABASE_KEY`,
`SEARXNG_URL` (optional — falls back to DuckDuckGo search).

**SuperGrok on grok.com and Cursor is not an API.** The nightly GitHub
Actions bot cannot use a website or Cursor subscription. To actually run
Grok in this repo, create a key at https://console.x.ai and add it as
the repository secret `XAI_API_KEY` (Settings → Secrets and variables →
Actions). Until that secret exists, every LLM stage stays on DeepSeek
exactly as before.

## Models

| Slot | Default without `XAI_API_KEY` | Default with `XAI_API_KEY` | Why |
|---|---|---|---|
| `MODEL_PREDICT` / `MODEL_OUTCOME` / `MODEL_JUDGE` | `deepseek-chat` | `deepseek-chat` | High volume, tool-calling, cheap |
| `MODEL_REFLECT` / `MODEL_DISTILL` / `MODEL_DEEPTHINK` | `deepseek-reasoner` | `grok-4.6` | Few calls, hard reasoning |

Override any slot via env (`MODEL_REFLECT=grok-4.6`, `MODEL_PREDICT=grok-4.6`,
…). If Grok is requested but the xAI call fails, the client falls back
to DeepSeek when `DEEPSEEK_API_KEY` is set.

Print the live routing table (no network): `python -m src.deepseek_client`

## Manual run

Actions → "Autonomous Daily Market Pipeline" → Run workflow → pick a stage
(and optionally a date, for backfills).
