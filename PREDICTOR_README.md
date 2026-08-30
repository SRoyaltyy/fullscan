# Autonomous Market Prediction Engine

Self-improving US-market direction predictor running on GitHub Actions.
(Companion to the data collectors in this repo — the prediction pipeline
reads the `news` and `macro_indicators` tables those collectors maintain.)

## Daily cycle (Mon–Fri, America/New_York)

| Time | Stage | What happens |
|---|---|---|
| **Post-close 10:00 PM ET** | **`Map Heat Captain Research`** | ECS systemd (DST-correct; GitHub backup) researches top-2 liquid SPX/RUT captains for every industry, batched by the 11 sectors. This slot is after book/outcome/learn/sector-outcome, and leaves almost eight hours before pre-open. Stores tomorrow’s cited baseline plus completed economic actual-vs-consensus and sector reactions. |
| **5:55 AM ET** | **`Pre-Open ALL` (ECS systemd)** | **The clock is the Alibaba box, not GitHub.** `fullscan-preopen.timer` fires `scripts/ecs_preopen.sh`: git pull → parse/digest → live Finviz groups/theme-ETF/futures/calendar/ticker-news tables → **one Grok overnight captain delta refresh** → events/judge/actions → general + 11 sector predicts → regex/Grok QC → git push. The morning never repeats the 11 post-close research batches. |
| 8:00+ AM | (fallback only) | Individual predictive workflows have **no schedule crons**. Orchestrator fires them after 08:00 ET only if Pre-Open ALL has **finished** today and is not quality-ok — never while ALL is still on the ECS runner, and never after 09:25 ET. |
| 5:00 PM | `outcome` | Actual close fetched → Grok (DeepSeek fallback) reviews the day with verified citations → prediction graded → `<date>_outcome.md` |
| 5:05 PM | `reflect` | Diagnostic engine classifies any miss (missing evidence / misweighted / miscalibrated) → candidate lesson in `02_lessons/candidate/` |
| ~5:30 PM | `learn_cycle` | Mine wins/losses → hypotheses → LEARNINGS.md + mutable_policy. Orchestrator re-dispatches if it missed. |
| ~1:00 PM UTC | `stock_book_all` | Weather/AB/book/backtest/paper **dashboard**. Not predictive; runs after the open. |

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
| Reflect | `src/book_reflect.py` | gap scan of movers the book missed, classed blind / outweighed / gated-out (`03_scoreboard/BOOK_GAPS.md`), then `deepseek-reasoner` writes book-scoped lessons into `02_lessons/candidate/` and maintains `02_lessons/hypotheses/book_missing_inputs.md` — the book's own list of what it cannot currently see |

## LLM: Grok 4.6 primary (OpenClaw), DeepSeek fallback

Every LLM stage (predict, outcome, sector ×11, news judge, events +
catcher, reflect, book-reflect, deepthink, promote merge, distill,
catalyst) routes through **one client** (`src/deepseek_client.chat`),
which tries providers in order:

1. **OpenClaw gateway** — Grok 4.6 (`x-openclaw-model: xai/grok-4.6`)
   running on the always-on box (Alibaba ECS, Singapore) under the
   SuperGrok OAuth login. On research stages Grok uses its **own native
   web/X search** inside the agent turn — no SearXNG — and appends a
   RESEARCH APPENDIX (queries, sources, facts) to keep output auditable.
2. **DeepSeek API** — the original client, unchanged, including the
   SearXNG → ddgs → DDG HTML → Google News RSS tool loop. Fires only
   when the gateway is unreachable, errors, or answers empty.

### Wiring the gateway (one-time, on the ECS)

```bash
openclaw models auth login --provider xai --method oauth   # SuperGrok login
openclaw config set tools.web.search.provider grok         # native Grok search
# enable the OpenAI-compatible surface:
#   gateway.http.endpoints.chatCompletions.enabled = true
```

Then give GitHub Actions a way to reach it — either:

- **Expose the gateway** (simplest): open the gateway port on the ECS
  security group, keep a strong shared-secret bearer token, and set the
  repo secrets `OPENCLAW_GATEWAY_URL=http://<ecs-public-ip>:18789` and
  `OPENCLAW_TOKEN=<token>` (Settings → Secrets and variables → Actions).
  Put it behind TLS (Caddy/nginx) if possible. GitHub-hosted runners
  have no fixed IPs, so the token is the perimeter.
- **Self-hosted runner**: install a GitHub Actions runner on the same
  ECS, set `OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789`, and switch
  `runs-on` in the workflows to the self-hosted label. Nothing is
  exposed to the internet, but the runner must stay up.

With neither secret set, everything runs on DeepSeek exactly as before.

### Env knobs

| Var | Default | Meaning |
|---|---|---|
| `OPENCLAW_GATEWAY_URL` | — | gateway base URL; setting it makes Grok primary everywhere |
| `OPENCLAW_TOKEN` | — | gateway shared-secret bearer token |
| `OPENCLAW_AGENT` | `openclaw/default` | agent target sent as the OpenAI `model` field |
| `OPENCLAW_BACKEND_MODEL` | `xai/grok-4.6` | backend model header (`x-openclaw-model`) |
| `OPENCLAW_TIMEOUT` | `10800` | per-call timeout (seconds, **3 hours**); research turns are slow. Job-level timeouts on ECS LLM workflows are ≥ this so GitHub does not kill the runner first. |
| `DEEPSEEK_API_KEY` | — | fallback provider (keep it set) |
| `MODEL_PREDICT` … `MODEL_DISTILL` | deepseek-chat / deepseek-reasoner | models used on the fallback path only |

Check routing (no network): `python -m src.deepseek_client`
Live round-trip test: `python -m src.deepseek_client --probe`

## Other secrets

`FRED_API_KEY`, `DATABASE_URL`, `DATABASE_KEY`, `SEARXNG_URL`
(optional — the DeepSeek fallback search chain degrades to DuckDuckGo).

## Manual run

Actions → **"Pre-Open ALL (predictive one-shot)"** → Run workflow
(optional date; `force=true` only as an emergency after 09:25 ET).

This is the one-button for every predictive part **as a backup**. The weekday clock is the ECS systemd timer (`scripts/install_ecs_preopen.sh`). Dispatch this workflow only if the box missed 05:55 ET.

## Ticker lookback — any stock

GitHub Actions → **Ticker Lookback (any stock)** → **Run workflow**.
Enter one or more comma-separated symbols (`AAPL,TEM,BRK-B`) and optional
start/end dates, or check **Random** for 50 names with market cap > $100M
and average volume > 500K. This is ticker-first: any name, not just the
book. Each dated row is the **09:30 ET information set** — what the
pipeline actually knew before the cash open — not the same-day close.
The Action opens with the featured setups that paid market-wide
(mine window, n, 1d/3d/1w edge) and every date those setups printed
on the names in this run.

| Box | Vintage on date D |
|---|---|
| join | D's `{D}_ranked.csv` when D's weather was built from the morning predict (labels × pre-open weather). Else last prior join. |
| vol, AB, peer, Finviz factor cells | Last completed tape dated before D (walk back if that session's file is missing). Never same-day Finviz. |
| buy | Last stock book dated before D (~13:00 ET). Never same-day book. |
| sector, gen | D's morning predict, else the last prior predict still knowable at 09:30 |
| news, digest, judge, catal | D's pre-open files. Ticker row first; else that day's sector tilt / sector digest. |
| heat | D morning captains, else the map-heat industry/sector board knowable at 09:30 (D `morning_overlay` or last night's board) |
| Price, +1d, +3d, +1w | Outcomes from D's close (not factors) |

Same-day `data/stock_book/{D}_stock_book.csv` and `data/exports/finviz_{D}.csv`
never color D's factor boxes. Black means nothing knowable printed for that
name — not that we refused a last-known file from an earlier session.

The Action publishes:

- a **setups board** at the top of the phone page, markdown, Action job summary, and the first XLSX sheet — market-wide n / 1d edge / 3d xs / 1w xs plus every date this run printed one;
- downloadable XLSX with red/yellow/green cells (one sheet per ticker, plus a Setups column);
- +1d / +3d / +1w **forward** price changes from that session (next 1 / 3 / 5 trading days), colored the same way (green up, yellow flat, red down);
- a blue date when factor colors improved vs the prior session (no cell worse and at least one better) **or** factor points jumped by ≥3 (red=1, yellow=2, green=3);
- 🚨 when the day is purely worse vs the prior session (no cell better, at least one worse);
- a white mark on any day with zero red factor cells;
- a **Cond** column with the G/Y/R tally; green or red when that color is the majority;
- a **Reg** column with green vs red cell mass (yellows ignored). A simple
  count of 🔵 / 🚨 / ⚪ is not predictive; the same tag changes meaning on
  the color stretch around it. Market-wide (2659 liquid names / ~29k
  printed days) the clean fade is 🚨 on a still-green row
  (`first_crack`). 🔵 on a mixed 3-day stretch is the useful blue.
  🔵 on a red row (`turn`) did not clear the bar. Factor configs that
  printed: `vol=good` with `ab=good` or `join=bad`, and `judge=neutral`;
- responsive phone HTML under `/dashboard/ticker-lookback/<tickers>.html`;
- compact Markdown color table;
- machine-readable JSON;
- as-of close plus forward +1d/+3d/+1w returns (calendar weekends and holiday dumps are omitted);
- the same forward returns in JSON for backtest verification.

A separate Action, **Ticker Lookback Mine**, scans the whole liquid
universe (same mcap/vol gates) and grades 🔵 / 🚨 / ⚪ × region/stretch
plus factor tones and pairs against +1d/+3d/+1w excess vs that day's
universe median. Write-up: `03_scoreboard/TICKER_LOOKBACK_MINE.md`.

Black means nothing knowable printed for that name; it never means
neutral. Last-known tape / predict / map-heat from an earlier session
still color when those files were knowable at 09:30. The Action does
not need an Elite login because it backtests the historical files
already committed to the repository.

The job self-reads every
output (trash / timeout stub / carry-forward → fail) and Grok reads the
files as text. It backs off if `/tmp/fullscan-preopen.lock` is held.

Post-close (outcome, learn, deepthink, weekly, dashboard) stay on their
own crons. The daily orchestrator re-dispatches any that missed.

Legacy: Actions → "Autonomous Daily Market Pipeline" → pick a stage
(and optionally a date, for backfills).
