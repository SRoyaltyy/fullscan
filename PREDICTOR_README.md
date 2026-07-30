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

## Secrets used

`DEEPSEEK_API_KEY`, `FRED_API_KEY`, `DATABASE_URL`, `DATABASE_KEY`,
`SEARXNG_URL` (optional — falls back to DuckDuckGo search).

## Models

Predict/outcome stages: `deepseek-chat` (tool-calling). Reflect/distill:
`deepseek-reasoner`. Override via env vars `MODEL_PREDICT` etc. in the
workflow files.

## Manual run

Actions → "Autonomous Daily Market Pipeline" → Run workflow → pick a stage
(and optionally a date, for backfills).
