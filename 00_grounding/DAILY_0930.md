# Daily 09:30 — what must run, in order

Operator map for the one thing that has to land every NYSE session
**before 09:30 ET**: today's stock BUY/SELL tickets.

There is no single GitHub Action that owns the whole day. The spine is
four workflows plus two ECS systemd timers. Everything else is a heal,
a sidecar, a dashboard publish, or research that must not sit in front
of the bell.

Clocks below are **America/New_York**. File dates are the session date
`YYYY-MM-DD`. Skip-if-good is `python3 -m src.skip_if_good --job JOB
--date DATE` (exit 0 = already good, do not rewrite).

`PREDICTOR_README.md` still describes the old 17:00 / 22:00 individual
crons. Those schedules are commented out. Use this file.

---

## 1. Two 09:30 outputs (they are not the same list)

| What | Writer | Files you actually trade from | Live? |
|---|---|---|---|
| **Stock book** (multi-horizon BUY/SELL) | `src.stock_book` via `src.run_stock_book_all` | `data/stock_book/{D}_stock_book.json`, `01_daily/{D}_stock_book.md`, `data/stock_book/{D}_green.json` | Suggestions book. Horizons `1d` / `3d` / `1w` / `2w` / `1m`. |
| **Flatten live card** | `src.sleeve_merge --card --write-card` (`LIVE_POLICY = flatten_robust`) | `01_daily/{D}_flatten_card.md`, `data/sleeve_merge/today.json`, `data/sleeve_merge/positions.json`, `dashboard/sleeve-merge/` | Production sleeve. 3d size-book picks + leftover cash + open lots. Does **not** run the 25-policy sweep. |

A full-quality morning also writes the LLM packet (general + 11 sector
predicts, news judge, events). The book can rank without those essays
(`s_general` / `s_sector` stay empty). Skip-if-good will **not** accept
that book as done: `check_stock_book_all` requires `same_day_general`
plus ≥8 same-day sector essays, so the 09:15 heal re-ranks once the
packet lands.

Pages (`https://sroyaltyy.github.io/fullscan/`) is display, not a gate.

---

## 2. Order of operations (night → bell)

```
NIGHT (closed session D, next session D+1)
  16:10  postclose_all.yml          ubuntu / DeepSeek
  16:15  postclose_last_closed.yml  ubuntu sidecar (last_closed only)
  17:15  daily_orchestrator.yml     heals postclose if still red
  22:00  ECS systemd                scripts/ecs_map_postclose.sh  (Grok)
  23:30  postclose_all.yml          ubuntu backup

MORNING (session D+1 — must finish before 09:30)
  05:40  finviz_preopen_scrape.yml  ubuntu Elite  (GH cron)
  05:55  ECS systemd                scripts/ecs_preopen.sh
                                    → python -m src.run_preopen_all
                                    (preopen_all.yml has NO GH schedule)
  06:10  stock_book_all.yml         ubuntu skip_llm + skip_extras
  07:50  daily_orchestrator.yml     redispatch scrape / preopen if red
  08:50  stock_book_diag.yml        read-only readiness board
  09:15  stock_book_all.yml         last-chance ubuntu rank
  09:20  daily_orchestrator.yml     ubuntu book heal if still red
  09:25  sleeve_merge_live.yml      flatten_robust card
         + run_preopen_all cuts LLM essays (weather/join/AB/book still run)
```

Unattended start of the morning packet is **ECS 05:55**, not GitHub.
If that timer is down, scrape + the two book crons + the orchestrator
are the only automatic path.

---

## 3. Stage I/O

### 3a. Post-Close ALL — last night

| | |
|---|---|
| **Action** | `Post-Close ALL (grade + learn + next captains)` · `.github/workflows/postclose_all.yml` |
| **Python** | `python3 -m src.run_postclose_all [--date D]` |
| **Also** | `postclose_last_closed.yml` (same Python, **always** `last_closed_session()`, DeepSeek) |
| **ECS** | `fullscan-map-postclose.timer` → `scripts/ecs_map_postclose.sh` at 22:00 |
| **Inputs** | Closed-session prices (yfinance), the morning packet that was predicted for D, last night's map-heat JSON to clone |
| **Outputs (date = closed D)** | `01_daily/general/{D}_outcome.md`, `{D}_reflect.md`, `01_daily/sectors/{D}/*_outcome.md` + `*_reflect.md` (≥8 real essays, 0 DeepSeek tool-dumps), `01_daily/{D}_learnings.md`, HIT / news-grade boards |
| **Outputs (date = next session D+1)** | `01_daily/map_heat/{D+1}_research_baseline.json` + `.md` (captains). Also clones `{D}_map_heat.json` → `{D+1}_map_heat.json` if missing |
| **Skip-if-good** | `--job postclose_all` (outcome + reflect + sector outcomes/reflects + next baseline + learnings). `postclose_all.yml` **yields** if the sidecar is already writing |
| **Does not** | Scrape Finviz. Rewrite the morning packet. Rewrite the stock book |

Default no-date run walks `night_pack_dates()`: last-closed, plus the
prior weekday if last-closed is still red. The sidecar never does that
walk — it grades yesterday only.

### 3b. Finviz pre-open scrape — 05:40

| | |
|---|---|
| **Action** | `Finviz pre-open scrape (GH-hosted Elite)` · `finviz_preopen_scrape.yml` |
| **Also** | First job of `preopen_all.yml` (same commands) when someone clicks Pre-Open ALL |
| **Why ubuntu** | Aliyun ECS 403s `finviz.com` |
| **Inputs** | Secrets `FINVIZ_EMAIL` / `FINVIZ_PASSWORD` / `FINVIZ_EXPORT` (Elite) |
| **Outputs** | `01_daily/news/{D}_finviz_digest.json` + `.md`, `01_daily/map_heat/{D}_map_heat.json` (phase=overlay, non-empty futures tape, `overlay_at` starts with D), `data/exports/finviz_{D}.csv` (≥50 KB) |
| **Skip-if-good** | `--job finviz_preopen_scrape` |
| **Hard stop** | Past 09:25 ET the standalone scrape cron refuses (no rewrite). ECS pre-open then ranks on whatever export is already on main |

### 3c. Pre-Open ALL — 05:55 ECS (the quality path)

| | |
|---|---|
| **Action** | `Pre-Open ALL (predictive one-shot)` · `preopen_all.yml` |
| **Start** | `fullscan-preopen.timer` → `scripts/ecs_preopen.sh`. **No GitHub `schedule:`** |
| **Python** | `python3 -m src.run_preopen_all [--date D]` |
| **Jobs when clicked** | `scrape` (ubuntu) → `land_book` (ubuntu, `run_stock_book_all --skip-llm --skip-extras`) **and** `preopen` (ECS, full packet). Book does not wait on Grok |

**Order inside `run_preopen_all`:**

1. Restore ECS persist; wait GH scrape (~10 min, 45 s if already past 09:25); wait night captain baseline.
2. **Deterministic, always** (even after 09:25): segments → weather (+ `--offline` retry) → join → AB checklist + enrich.
3. **LLM packet** (skipped after 09:25): news_parse → events → events_catcher → news_judge → map_heat_refresh (one delta, not 11 night batches) → news_actions → general_predict → 11 sector_predict → sector_board.
4. Weather + join refresh, then `run_stock_book_all(..., skip_llm=True, skip_extras=True, refresh_ranker=True)`.
5. Push book + green + weather + join + AB + membership immediately.
6. If still before 09:25: paper_trade → sleeve_combine_bt → `sleeve_merge --card`.
7. Catalyst dossiers **after** the book. Grok text review last.

| Packet outputs | Path |
|---|---|
| General predict | `01_daily/general/{D}_predict.md` |
| 11 sector predicts | `01_daily/sectors/{D}/*_predict.md` (≥8 quality for skip-if-good) |
| Events | `01_daily/events/{D}_events.json` (must be same-day, not carried) |
| News parse / judge / actions | `01_daily/news/{D}_parsed.json`, `{D}_judge.md`, `{D}_actions.json` |
| Morning heat delta | `01_daily/map_heat/{D}_*` research refresh on top of last night's baseline |
| Weather | `01_daily/weather/{D}_weather.json` (≥5 sector stances) |
| Join | `data/join/{D}_ranked.csv` (≥5 KB) |
| AB | `data/ab_checklist/{D}_ab_checklist_enriched.csv` |
| Membership | `data/universe/{D}_membership.csv` (≥50 KB) |
| Book + green | see §1 |
| QC | `01_daily/{D}_preopen_qc.json`, `{D}_preopen_status.md` |

**Skip-if-good:** `--job preopen_full` = packet (`check_preopen_all`) **and** `check_stock_book_all`.

**Hard gates before rank:** membership, weather with ≥5 sectors, join ≥5 KB.
Missing AB = warn; book ranks without `s_ab`.

### 3d. Stock Book ALL — 06:10 / 09:15 heal

| | |
|---|---|
| **Action** | `Stock Book ALL (one-shot)` · `stock_book_all.yml` |
| **Python** | `python3 -m src.run_stock_book_all` |
| **Scheduled / push / workflow_run** | Forced **ubuntu** + `--skip-llm --skip-extras`. Must not take the ECS box |
| **Also fires on** | `workflow_run` of Pre-Open ALL completed; `push` to `src/stock_book.py` / `green_pile.py` / `weather.py` / `run_stock_book_all.py` / `skip_if_good.py` / this yml |
| **Manual default** | ECS + full LLM + extras (paper, catalyst, sleeve `--write` sweep dashboard) — do **not** use this as the 09:30 heal |

**Hard stops in `run()`:** no membership → leave; no weather sectors → leave; join < 5 KB → leave.

**`check_stock_book_all` (the 09:30 gate):**

- `data/stock_book/{D}_stock_book.json` or `01_daily/{D}_stock_book.md`
- `data/stock_book/{D}_green.json`
- not a degraded / empty-books stub
- `meta.same_day_general` and `same_day_sectors ≥ 8` (so a 06:10 essay-less book does not skip the 09:15 heal)
- 1d BUY is all-green and has no printed dead RelVol `(0, 0.7)`
- weather complete, AB present, join ≥ 5 KB

`--skip-extras` still writes the flatten card, then exits (no paper / catalyst / backtest).

### 3e. Sleeve merge live card — ~09:25

| | |
|---|---|
| **Action** | `Sleeve merge live card` · `sleeve_merge_live.yml` |
| **Python** | `python3 -m src.sleeve_merge --card --date D --write-card` |
| **Also written by** | `run_preopen_all` (if before 09:25) and `run_stock_book_all` (always, including skip_extras) |
| **Inputs** | Today's stock book (3d size-book picks), `data/sleeve_merge/positions.json` / paper lots, leftover cash, flatten-switch gate |
| **Outputs** | files in §1 |
| **Must not** | Sit in front of the 09:30 book. Run the 25-policy sweep |

### 3f. Daily Orchestrator — missed-job guard

`.github/workflows/daily_orchestrator.yml`. Does **not** own the two ALL
clocks. Redispatches only when skip-if-good is red **and** nothing is
already running.

| ET | What it may click |
|---|---|
| before 09:00 | `finviz_preopen_scrape.yml`, `preopen_all.yml` |
| 09:00–16:00 | `stock_book_all.yml` ubuntu skip_llm + skip_extras |
| after 16:00 | `postclose_all.yml` ubuntu/DeepSeek, plus `postclose_last_closed.yml` if last-closed is still red |

---

## 4. What each strategy needs at 09:30

| Strategy / surface | On the 09:30 clock? | Consumes | Notes |
|---|---|---|---|
| Stock book BUY/SELL | **Yes — this is the list** | weather, join, AB, membership, Elite CSV, same-day essays if they landed | `src/stock_book.py` |
| `flatten_robust` live card | **Yes — after the book** | book 3d picks + open lots + leftover cash | `LIVE_POLICY` in `src/sleeve_merge.py`. Do not change unless asked |
| Paper `.io` / sleeve-combine dashboard | Nice if before 09:25 | book + `src.paper_trade` + `src.sleeve_combine_bt` | Scheduled book heals set `skip_extras` — paper is **not** required for tickets |
| Excel bot cluster signals | No | own Yahoo cache on `excel-state` | `excel_bot.yml` Tue–Sat 06:30 ET ubuntu. Separate suggestions.csv |
| Factor-mine / flatten lookback / gainer / mover / camera combo | No | historical books + OHLC parquet | Research. Do not dispatch on the morning ECS box |
| Rank residual / down-day / CANSLIM-enrich mines | No | join + Elite cards | Research only. Do not wire into `join_rules.json` from this doc |

Retired / dispatch-only (do **not** click for 09:30): `learn_cycle.yml`,
`daily_pipeline.yml`, `news_parse.yml`, `news_judge.yml`,
`news_actions.yml`, `events_daily.yml`, `label_weather.yml`,
`finviz_digest.yml`, `sector_daily.yml` predict cron, `catalyst_daily.yml`
schedule, `map_heat_postclose.yml` schedule, `pipeline_health.yml`
schedule, `ab_checklist.yml` cron. Pre-Open ALL / Post-Close ALL already
run those modules.

---

## 5. Why jobs miss or double-run

**Misses**

- `preopen_all.yml` has no GH cron. If `fullscan-preopen.timer` is
  disabled or the ECS runner is offline, the LLM packet never starts
  unless the orchestrator (07:50) or a human clicks it.
- ECS 403s Finviz. No Elite export → thin join / no membership → ranker
  hard-stops.
- Post-close hung on Grok (old 10800 s HTTP cap) ate the morning box.
  Night pack now DeepSeek on ubuntu; ECS 22:00 is the Grok pass.
- `check_stock_book_all` stays red on an early book that ranked before
  essays. That is intentional (09:15 re-rank). It looks like a "failed"
  06:10 even when BUY/SELL already exist.
- Weekend / NYSE holiday: `_session_date` skips. Labor Day 2026-09-07
  is a holiday — captains from Friday 09-04 target Tuesday 09-08, not
  Monday.

**Duplicates** (skip-if-good + split concurrency are the brake;
`cancel-in-progress: false` so they do not kill each other)

- Book writers: Pre-Open `land_book`, `run_preopen_all` internals,
  `stock_book_all` 06:10, 09:15, `workflow_run` after Pre-Open, push of
  ranker files, orchestrator 09:20.
- Flatten card: pre-open (if before 09:25), every `run_stock_book_all`,
  `sleeve_merge_live.yml` cron + after Stock Book ALL.
- Post-close: 16:10 + sidecar 16:15 + orch 16:20/17:15 + ECS 22:00 +
  23:30 + push of healer code. Sidecar yield exists; two writers can
  still persist if the yield check misses.
- Scrape: 05:40 cron **and** Pre-Open ALL's `scrape` job.

If skip-if-good says SKIP, a second fire is a no-op (no LLM). A
`--force` click is what burns tokens on a good pack.

---

## 6. If everything is on fire (manual)

Do **not** click the old single-module ymls. Do **not** click Stock Book
ALL on ECS with default inputs (6 h Grok). Do **not** `--force` a pack
that `skip_if_good` already accepts.

**Tickets only (minutes, ubuntu):**

1. `finviz_preopen_scrape.yml` (if digest / overlay / Elite CSV missing).
2. `stock_book_all.yml` — runner **ubuntu**, `skip_llm=true`,
   `skip_extras=true`, today's date.
3. `sleeve_merge_live.yml` after the book is on main.

**Full-quality morning (essays + book):**

1. Confirm last night: `python3 -m src.skip_if_good --job postclose_all`
   (no `--date` = night pack). If red, `postclose_all.yml` ubuntu /
   `llm_backend=deepseek` **or** `postclose_last_closed.yml`.
2. `finviz_preopen_scrape.yml`.
3. `preopen_all.yml` — runner **ecs**, `with_book=true`. Ubuntu cannot
   reach Grok; it will DeepSeek or skip essays.

**Readiness (does not write):** `stock_book_diag.yml` or

```bash
python3 -m src.skip_if_good --job finviz_preopen_scrape --date "$D"
python3 -m src.skip_if_good --job preopen_full --date "$D"
python3 -m src.skip_if_good --job stock_book_all --date "$D"
python3 -m src.output_qc --date "$D" --preopen
```

---

## 7. Leak-free rule (do not break this)

At 09:30 on D the pickers must not use D's printed OHLC, volume,
Change%, Gap, RelVol, or printed book. Morning Elite scrape is
pre-open tape / calendar / fundamentals, not the regular-session print.
Live flatten / `LIVE_POLICY` / sleeve-merge SWEEP stay as they are
unless a human asks to change them.
