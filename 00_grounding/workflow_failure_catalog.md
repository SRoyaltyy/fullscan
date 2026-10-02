# Workflow failure catalog (mined 2026-09-04)

Mined from GitHub Actions: last ~40–50 runs per workflow, plus **130 failed-run logs** (`gh run view --log-failed`). This is the “why the morning/night jobs do not research everything / take every input / write every output” list.

Counts are **not** lifetime. They are the recent window. A green last-run does not mean the job is healthy if most of the window is cancel or incomplete.

---

## 1. What lives where (correction)

Learning is **not** in Pre-Open. Stock Book ALL / Label+Weather / AB / Catalyst / Daily Pipeline / Learn / Map Heat are **not** all merged into Pre-Open.

| Clock | Workflow | Owns |
|---|---|---|
| **Pre-Open ALL** (`preopen_all.yml`, ECS ~05:55 ET + GH scrape child) | Morning packet | Finviz scrape (ubuntu child — ECS 403s), news parse, events+catcher, news judge, map-heat **morning delta**, news actions, general + 11 sector predicts, sector board, weather, catalyst, **stock book + paper `.io`** (`--with-book` default). Label/weather/AB are **healed inside the book chain**, not separate morning clicks. |
| **Post-Close ALL** (`postclose_all.yml`) | Closed session | General outcome + horizon grade + reflect, sector outcomes + sector reflect + board, news-actions grader, HIT board (no LLM), **Learn cycle**, **map-heat captain research for the NEXT session**. |
| Standalone layer jobs | Dispatch-only after the two-job merge | Stock Book ALL, Label+Weather, AB, Catalyst, Daily Pipeline, Learn, Map Heat post-close. Crons retired so they cannot double-spend Grok/DeepSeek. |
| **Not in either ALL** | Separate | Finviz pre-open scrape (must stay ubuntu), Pages deploy, lookbacks, collectors, readiness boards, xAI reauth, OpenClaw probe. |

`postclose_all.yml` is **not on `main` yet** (404 on the default-branch workflow API as of this mine). Until that merge lands, night work is still the old layer jobs + ECS `ecs_map_postclose.sh`.

---

## 2. Cross-cutting failure modes

These show up in many jobs. Fix them once; most “this job is flaky” rows below collapse.

### A. One ECS runner + `cancel-in-progress: true`

There is one Aliyun ECS runner. Long Grok jobs (pre-open, map-heat, daily pipeline, catalyst) queue. Re-dispatch **kills** the job that was already researching.

| Workflow | Recent window | Cancels |
|---|---:|---:|
| `map_heat_postclose.yml` | 45 | **22** |
| `preopen_all.yml` | 31 | **12** |
| `catalyst_daily.yml` | 12 | **8** (0 hard fails) |
| `ab_full_market.yml` | 14 | 7 |
| `stock_book_all.yml` | 44 | 7 |
| `sector_daily.yml` | 46 | 6 |

**Fix:** `cancel-in-progress: false` on every Grok-hour job. Skip-if-good on the second fire (already in the two-job branch). Orchestrator must not re-dispatch if the same job is `in_progress` or `queued`. Phone “Run again” should queue, not murder.

### B. OpenClaw / Grok is the single point of failure

Seen in Daily Pipeline (5 of 6 mined fails), Map Heat, Pre-Open, Stock Book ALL:

| Symptom in logs | What it is |
|---|---|
| `HTTP 500: {"error":{"message":"internal error"}}` | Gateway up, Grok/xAI blew up. Marked DOWN for the **whole run**. |
| `401 Unauthorized` / `404 Not Found` on `/v1/chat/completions` | Token or path wrong that hour. |
| `GROK_ONLY: OpenClaw failed — no DeepSeek/SearXNG fallback` | Job was pinned to Grok. Empty predicts, then QC fail. |
| `chat completion failed: FallbackSummaryError: All models failed (1): xai/g…` | xAI OAuth dead or model refused. |
| `gateway port 18789 still down` / `Connection refused` | systemd unit down or config clobbered (`gateway.mode` missing). |
| `json_len=48` vs workflows that inject a **64-char** secret | Token-length mismatch (partially aligned; still logged). |
| xAI OAuth “dies ~6h” | `xai_reauth.yml` 27 fail / 34 in window. Device login cannot finish unattended. |

**Fix:**

1. Default `LLM_BACKEND=auto` (Grok then DeepSeek). `GROK_ONLY=1` only as an explicit dispatch. (Two-job PR already adds this toggle.)
2. Do **not** mark the gateway DOWN for the whole run on a single 500. Retry that call; keep Grok for the next sector.
3. On OpenClaw 401/404: one heal (`enable_openclaw_chat.sh`) then retry, then DeepSeek.
4. Keep xAI reauth **human + detached**. Do not cron it onto the same runner as research.
5. Never print `OPENCLAW_TOKEN` in logs (Map Heat env dumps have leaked it).

### C. Finviz 403 on ECS (Aliyun)

`[finviz] all Elite URLs failed (403)` on every Map Heat / Finviz-ALL / apply-no403 run that scraped from ECS. Ubuntu scrape is **39 success / 0 fail / 1 cancel** in 40.

**Fix (already the rule, still violated):** Elite HTML scrape **only** on `ubuntu-latest`. ECS clones today’s heat / digest from persist or `origin/main`. Any ECS step that still calls Elite URLs is a bug. `apply_finviz_no403.yml` cannot “fix” Cloudflare from China.

### D. Incomplete packet that still “ran”

Pre-Open fails that finished Python and then died in QC or git:

- Missing 1–4 of 11 **sector predicts** (healthcare / communication / consumer cyclical / tech / utilities / basic materials / industrials).
- Missing `map_heat_research.md` (morning refresh) while tables exist.
- `empty_futures_tape` — overlay JSON present, futures scrape empty, QC treats as fail.
- Night `research_baseline` missing → morning warns / health FAILs.
- 09:25 refuse: late sectors never written.

**Fix:** Per-step skip-if-good (do not redo finished sectors). Fail the **job** if required files are missing (two-job branch already fails if book missing after `--with-book`). Do not treat empty overlay tape as ready. Persist `/home/gha/fullscan-persist` so checkout `--clean` does not lie. Keep night baseline as a **warn** for morning, not a hard block, if morning heat tables exist.

### E. Git commit/push is a fake “research failed”

Many red X’s are **after** the LLM work:

| Log line | Jobs |
|---|---|
| `fatal: not in a git directory` + `$HOME not set` | Stock Book ALL (all 6 mined), Learn Cycle (4/6) |
| `cannot pull with rebase: You have unstaged changes` | Label+Weather, AB, Price checklist |
| `error: could not apply … auto: pre-open ALL` / `learn_cycle` / `sector daily` | Concurrent auto-commits on `main` |
| `failed to push some refs` | Same race |

Research often succeeded. The bot then `cd`’d out of the repo, or two jobs committed the same tree.

**Fix:** One `scripts/safe_git_push.sh` everywhere: `unset GIT_DIR GIT_WORK_TREE`, `HOME=/home/gha`, `cd $GITHUB_WORKSPACE`, stash unstaged, rebase-or-retry 5×, never `git pull --rebase` on a dirty tree. Learn/Book commit steps must not `cd` into persist. Two-job merge removes most of this race.

### F. Supabase (Seoul pooler) timeouts

`psycopg2` `timeout expired` / `QueryCanceled` / `ECHECKOUTTIMEOUT` on Label+Weather (all 6 mined fails), Collect ALL and almost every `collect-*` child, plus catalyst snapshots inside Pre-Open.

**Fix:** Retry with backoff (3×). On timeout, write weather/labels from Finviz CSV **without** DB (degraded OK), do not fail the morning. Collectors: skip the DB write, still commit the files. Move pooler closer or raise statement timeout. Do not hard-require `DATABASE_URL` for Label+Weather.

### G. Health/diag jobs fail because they are honest scoreboards

`pipeline_health.yml`: **0 success / 6 fail / 2 cancel** in 8. It heals, then exits 1 if any required file is still missing (night baseline, OAuth, empty futures). It also holds the **only** ECS runner for up to 180 minutes (`HEALTH_HEAL_ROUNDS=16`), which **causes** the missing files.

`stock_book_diag.yml`: 8 recent fails. The script works. It exits 1 when Label+Weather / Catalyst / book are FAIL on that date — including `No module named 'pandas'` on the gainer-asof walk (fixed in workflow; still in older runs).

**Fix:** Health: audit-only by default; never occupy ECS during 05:55–09:25 or 22:00 research. Diag: exit 0 on a written board; use a check conclusion or annotation for FAIL rows. Require pandas (already added). Stop afternoon cron that still expects the retired 13:00 Stock Book ALL clock.

---

## 3. Per-job catalog

Window = last 40–50 runs unless noted. “Last fail” is the newest failure in that window.

### 3.1 The two ALL jobs

#### `preopen_all.yml` — Pre-Open ALL

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 31 | 13 | 6 | 12 | 2026-09-02 `33618162369` |

**Failure modes (from 6 logs):**

1. **Cancel storm** (12/31) — `concurrency: preopen-all` + `cancel-in-progress: true`. Re-click / orchestrator / late dispatch kills a 2–4h Grok run.
2. **QC incomplete packet** (`32958398826`, `33370577740`, `33399857749`, `33618162369`) — job ran; missing sector predicts and/or `map_heat_research.md`. Example 2026-09-02: Tech + Utilities missing, `all_ok=False` while `qc_all_ok=True` / `grok_ok=True`.
3. **OpenClaw 500 → gateway DOWN** — later sectors never written (feeds #2).
4. **Git rebase race** on the auto-commit (`could not apply … auto: pre-open ALL`).
5. **Old script bugs (fixed since):** `ensure_ecs_clock.sh` syntax error (`32957814889`); `fatal: $HOME not set` (`32875051663`).
6. **empty_futures_tape** (`32958398826`) — heat JSON failed QC.
7. **Supabase** on catalyst snapshot (warn, then continue).

**Proposed fix:** `cancel-in-progress: false`. Per-sector skip-if-good. Do not trip whole-run OpenClaw breaker on one 500. Persist + fail job if book/packet still missing (two-job branch). Keep scrape on ubuntu child.

#### `postclose_all.yml` — Post-Close ALL

No runs on `main` yet (workflow 404). Night failures today still belong to Daily Pipeline + Learn + Map Heat below. After merge: same OpenClaw / cancel / git-push modes; skip-if-good should make the 16:10 + 22:00 double-fire cheap.

---

### 3.2 Layer jobs (now emergency dispatch)

#### `stock_book_all.yml` — Stock Book ALL

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 44 | 27 | 10 | 7 | 2026-08-26 `33008376228` |

**Modes:** All 6 mined fails die in the **commit** step (`fatal: not in a git directory` / `$HOME not set`) after the book ran. Also: OpenClaw 404 → DOWN; `GROK_ONLY` no fallback (2); Finviz login 403 when ECS scraped; yfinance rate-limit on junk tickers (`KCA-U`); Supabase timeout. Cancels from queue.

**Fix:** Isolate git (`HOME=/home/gha`, never leave workspace). Book chain must not scrape Elite on ECS. Default LLM auto. After two-job merge this job is dispatch-only — do not leave a 13:00 cron (diag still looks for it).

#### `label_weather.yml` — Label + Weather

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 28 | 19 | 8 | 1 | 2026-09-03 `33760788230` |

**Modes:** **100% of mined fails are Supabase** (`QueryCanceled` statement timeout, or pooler connect timeout). Then dirty-tree push (`cannot pull with rebase`). Finviz `Recom` column missing is a warn, not the fail.

**Fix:** Weather/labels from Finviz CSV with DB optional. Retry pooler. `safe_git_push`. Healed inside Pre-Open book chain — do not schedule a second morning copy.

#### `ab_checklist.yml` — A+B1 Checklist

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 33 | 29 | 3 | 1 | 2026-08-20 |

**Modes:** yfinance `YFRateLimitError`; `KeyError: 'score'` (`32160476631`); dirty-tree push. Recent window is healthy.

**Fix:** Treat yfinance 429 as skip-row, not abort. Guard missing `score`. `safe_git_push`. Heal inside book chain.

#### `catalyst_daily.yml` — Catalyst

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 12 | 4 | 0 | **8** | — |

**Modes:** Not a code crash. Cancels / queue behind Pre-Open / Map Heat on the one ECS box. `cancel-in-progress: false` already — these are runner-preempt / manual / workflow-update cancels.

**Fix:** Run catalyst **inside** Pre-Open after predicts (already designed). Do not dispatch the standalone job on the same morning.

#### `daily_pipeline.yml` — Autonomous Daily Market Pipeline (outcome)

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 50 | 38 | 10 | 2 | 2026-09-01 `33458567281` |

**Modes (cluster, 2026-08-31 → 09-01):** OpenClaw HTTP 500 → `GROK_ONLY` no fallback → exit 1. One older: OpenClaw 401 + **DeepSeek 402 Payment Required**. `concurrency` `cancel-in-progress: true`.

**Fix:** Outcome grader should DeepSeek-fallback (or skip LLM reflect and still write the numeric grade). Do not fail the whole night if reflect is empty. Move under Post-Close ALL. `GROK_ONLY` off unless asked.

#### `learn_cycle.yml` — Learn Cycle

| n | success | fail | cancel | last fail |
|---:|---:|---:|---:|---|
| 29 | 19 | 9 | 1 | 2026-08-27 `33121883832` |

**Modes:** Learn **finished** (lessons written), then `fatal: not in a git directory` (4) or `could not apply … auto: learn_cycle` (2). Same git-home / concurrent-push bug as Stock Book.

**Fix:** Same `safe_git_push`. Under Post-Close ALL so it is not racing Book/Map Heat commits.

#### `map_heat_postclose.yml` — Map Heat captains

| n | success | fail | cancel | in_progress | last fail |
|---:|---:|---:|---:|---:|---|
| 45 | 8 | 14 | **22** | 1 | 2026-08-31 `33401684769` |

**Modes:**

1. Cancel storm (`cancel-in-progress: true` + 6h timeout + re-dispatch).
2. OpenClaw 500 / xAI `FallbackSummaryError` / 401.
3. `GROK_ONLY` no fallback → empty captains → exit 1.
4. **ECS Finviz 403** (`33159529821` and others) — still scraping Elite on Aliyun.
5. `SyntaxError: f-string expression part cannot include a backslash` (2026-08-28, two runs) — shipped-broken Python; fixed later.
6. SearXNG 0 results + empty futures tape.
7. Multi-hour connection/timeout spam (`33070086548` ~3.5h then fail).

**Fix:** `cancel-in-progress: false`. Never scrape Elite on ECS — clone GH heat. LLM auto. Per-captain skip-if-good so a retry does not re-research finished sectors. Under Post-Close ALL.

#### `sector_daily.yml` / `sector_pipeline.yml` / `sector_predict.yml`

| Job | n | success | fail | cancel | last fail |
|---|---:|---:|---:|---:|---|
| sector_daily | 46 | 37 | 3 | 6 | 2026-08-25 |
| sector_pipeline | 2 | 1 | 1 | 0 | 2026-08-08 |
| sector_predict | 0 | | | | never run |

**Modes:** Concurrent auto-commit rebase (`could not apply … sector daily predict`); runner shutdown mid-job (`32799613760`). Incomplete 11-for-11 now shows up **inside Pre-Open**, not here.

**Fix:** Sectors only via Pre-Open. Keep standalone dispatch for one-sector repair. `cancel-in-progress: false`.

#### `news_parse.yml` / `news_judge.yml` / `news_actions.yml` / `news_grade.yml` / `hit_board.yml`

All green in the window (parse 35/35, judge 11/11, actions 28/28, grade 17/17, hit 16/16). Graders do not call Grok.

**Known residual:** they can write a file from a thin upstream (events parse-fail used to hard-fail the **events** job, not these). Keep them inside Pre-Open / Post-Close; no cron needed.

#### `events_daily.yml` — Event Scanner

| n | success | fail | last fail |
|---:|---:|---:|---|
| 24 | 17 | 7 | 2026-08-21 |

**Modes:** `[events] 0 events (PARSE FAILED)` after SearXNG 0 results / DDG block. Catcher skips. Job exits 1. Later Pre-Open has a repair pass (`prefer_openclaw=True`) that sometimes still yields empty.

**Fix:** If search is empty, write a dated stub with `PARSE_FAILED` and **exit 0** (morning can continue). Retry OpenClaw JSON repair once. Do not require SearXNG if Grok native search is up. Fail only if the file is missing.

#### `finviz_preopen_scrape.yml`

| n | success | fail | cancel |
|---:|---:|---:|---:|
| 49 | 48 | 0 | 1 |

Healthy. **Keep on ubuntu-latest.** Do not move to ECS.

#### `finviz_all.yml` / `finviz_digest.yml`

Finviz ALL: 8 ok / 2 fail (2026-08-30). Logs: Elite 403 **and** a workflow-script bug — `command substitution: syntax error near unexpected token 'phase'` / `'overlay_at'` while printing QC. Digest: 9 ok / 1 fail (log expired, push-era).

**Fix:** Scrape on ubuntu only. Quote JSON in bash (`jq`, not unquoted `$(...)`). Empty futures tape = WARN + retry once, not “3 tests failed” abort if digest is good.

---

### 3.3 Orchestrator, health, doors

#### `daily_orchestrator.yml`

| n | success | fail | last fail |
|---:|---:|---:|---|
| 50 | 37 | 13 | 2026-08-26 (push CI; logs expired) |

Scheduled orchestrator is mostly green. The 3 mined “fails” are **push** runs whose logs expired (`log not found`). Older push breaks were SearXNG/Grok wiring (titles: “do not require SEARXNG_URL”, “install Grok native-search overlay”).

**Risk after two-job merge:** orchestrator still allowed to poke layer jobs. If it re-dispatches Pre-Open while ECS is already running it, cancel-in-progress will kill the packet.

**Fix:** Orchestrator may dispatch **only** `finviz_preopen_scrape`, `preopen_all`, `postclose_all`, and only if the file is missing **and** nothing with that concurrency group is queued/in_progress.

#### `pipeline_health.yml`

| n | success | fail | cancel |
|---:|---:|---:|---:|
| 8 | **0** | 6 | 2 |

**Modes:** Required FAILs it cannot heal: xAI OAuth (human-only), night `research_baseline` missing, `empty_futures_tape`, “DID NOT RUN — file missing” for predicts written on a **different checkout** (persist vs workspace). Heal loop then starts more ECS work and sits on the runner. `GROK_ONLY=1` inside health.

**Fix:** Default `--no-fix` / audit. Never GH-dispatch another ECS job from health (comment already says this — enforce). Exit 0 after writing the board; page on required FAIL. Clock disabled 2026-08-29 — leave it off.

#### `stock_book_diag.yml` — Stock Book readiness

| n | success | fail | last fail |
|---:|---:|---:|---|
| 24 | 16 | 8 | 2026-09-03 `33799089078` |

**Modes:** Board is correct and red: Label+Weather FAIL (Supabase), Catalyst “no run today”, Stock Book blocked. Older: `_walk failed: No module named 'pandas'` (gainer-asof). Job **exits 1** because the board has FAIL rows. Afternoon cron `20 17 * * 1-5` still assumes a 13:00 Stock Book ALL.

**Fix:** `pip install pandas` (done). Exit 0 after `--write`. Drop or retarget the 13:20 ET cron to “Pre-Open ALL wrote a book”. Treat catalyst-inside-preopen as the catalyst run.

#### `runtime_door.yml` / `openclaw_probe.yml`

14/14 and 3/3 success. Good canaries. Keep them short; do not let probe restart the gateway during a live Grok turn.

#### `xai_reauth.yml`

| n | success | fail | cancel |
|---:|---:|---:|---:|
| 34 | 4 | **27** | 3 |

**Modes:** `systemd-run` fails (`$DBUS_SESSION_BUS_ADDRESS` / `$XDG_RUNTIME_DIR` unset under Actions). Device-code waiter cannot finish. Runtime snapshot then FAILs “Grok turn timeout / OAuth dies ~6h” and leftover HTTP 403 mentions from old health JSON. Job is designed to detach — it still exits 1.

**Fix:** Do not `systemctl --user` from Actions. Write the device URL to `01_daily/_xai_reauth.json` and exit 0. Phone does the login. Skip if a research process is live (already). Cron must stay off (comment is correct).

#### `enable_openclaw_chat.yml`

2/2 fail. `Connection refused` on `:18789`; then restart blocked: `existing config is missing gateway.mode` (clobbered OpenClaw config).

**Fix:** Never overwrite `openclaw.json` without a backup + required keys. If port down, start the **unit** (`fullscan-openclaw-gateway.service`), do not `openclaw onboard`.

---

### 3.4 Collectors (Supabase farm)

These are not in the two ALL jobs. They fail the same way.

| Workflow | n | success | fail | Dominant log |
|---|---:|---:|---:|---|
| `collect-all.yml` | 40 | 17 | **23** | Seoul pooler timeout / `ECHECKOUTTIMEOUT` |
| `collect-catalyst.yml` | 40 | 26 | 13 | same + cancel 1 |
| `collect-finviz.yml` | 32 | 21 | 8 | `QueryCanceled` statement timeout |
| `collect-macro-sentiment.yml` | 40 | 31 | 9 | pooler timeout |
| `collect-macro-fred.yml` | 40 | 33 | 7 | pooler timeout |
| `collect-sec-filings.yml` | 40 | 31 | 9 | `ECHECKOUTTIMEOUT` |
| `collect-tickers.yml` | 20 | 15 | 5 | pooler timeout |
| `collect_company_profiles.yml` | 23 | 15 | 8 | `ECHECKOUTTIMEOUT` |
| `collect_sec_fundamentals.yml` | 21 | 19 | 2 | pooler timeout |
| `collect-news-newsapi.yml` | 40 | 38 | 2 | pooler timeout |
| `collect-market-yfinance.yml` | 40 | 39 | 1 | pooler timeout |
| `collect-news-reddit.yml` | 40 | **40** | 0 | — |
| `collect-news-rss.yml` | 40 | 34 | 0 | 6 **cancels** only |

**Fix:** One shared DB helper: retry 3×, on failure **commit files anyway** and exit 0 (or WARN). Do not fail Collect ALL because one child could not check out a pooler connection. Consider writing parquet/JSON first, DB second.

---

### 3.5 Lookbacks, mines, one-buttons

| Workflow | Window | Modes / fix |
|---|---|---|
| `mover_lookback_action.yml` | 5/5 ok | Healthy. |
| `gainer_lookback_action.yml` | 1/1 ok | Healthy. |
| `book_lookback.yml` | 10/10 ok | Healthy. |
| `ticker_lookback.yml` | 13 ok / 1 fail | `33328940495`: empty inputs (`TICKERS=` and `RANDOM_PICK=false`) → `Provide tickers or check Random.` **Fix:** default `--random` on dispatch, or fail in the UI before the job starts. |
| `gainer_asof.yml` | 8 ok / 1 fail | `33532093167`: `unrecognized arguments: --sells --losers` — workflow ahead of the script. **Fix:** ship flags + workflow in the same commit; or `|| true` unknown flags. |
| `excel_bot.yml` | 9/9 ok | Healthy. |
| `excel_backtest.yml` | 3 ok / 1 fail | `couldn't find remote ref excel-deep-grids` + `empty ident name`. **Fix:** create the branch or stop fetching it; set `user.name`/`email` before commit. |
| `stock_book_backtest.yml` | 12/12 ok | Healthy. |
| `boring_winners.yml` | 15 ok / 1 fail | `33654990066`: `AssertionError` in a test/script. Pin the assert; do not fail the daily file write. |
| `full_feature_mine.yml` | 2 ok / 1 fail | `ImportError: attempted relative import with no known parent package`. Run as `python -m src.…`. |
| `camera_combo_mine.yml` | 1/1 ok | — |
| `industry_predict.yml` | 3 ok / 1 fail | `DateParseError: 2026+06-01` — typo’d date input. Validate `YYYY-MM-DD` in the workflow. |
| `ab_one.yml` | 1/1 ok | — |
| `ab_one_button.yml` | 6 ok / 1 fail | `ab_one.py: error: unrecognized arguments: --ticker`. Align CLI. |
| `ab_full_scan.yml` | 8 ok / 2 fail | One log expired. |
| `ab_full_market.yml` | 5 ok / 2 fail / 7 cancel | `ERROR: checklist CSV not written — ab_checklist main() did not run`. Cancel storm. **Fix:** do not start full-market if liquid CSV missing; do not cancel-in-progress. |
| `ab_backfill.yml` / `auto_fix_ab_backfill.yml` | all ok | — |
| `apply_finviz_no403.yml` | 1 ok / 2 fail | Still 403 on ECS + push fail. **Do not use.** Scrape stays on ubuntu. |
| `apply_finviz_wiring.yml` | 1 fail | One-shot wiring. |
| `price_checklist.yml` | 4 ok / 5 fail | Latest `33760787053` (2026-09-03): yfinance “Failed download” spam → `Price store empty` → push fail. Older: `openpyxl` missing; git HTTP 408 on huge parquet. **Fix:** bootstrap parquet must already be on `main`; incremental update only; `openpyxl` in requirements; do not fail the whole store on a handful of dead tickers. |
| `deploy-dashboard.yml` | 43 ok / 0–1 fail / 6 skipped | Healthy. Skips when no dashboard change. |
| `gemini-catcher.yml` | 7 ok / 4 fail | Logs expired (May 2026). Legacy. |
| `deepthink.yml` / `quote_colors.yml` / `report_card.yml` / `weekly_consolidation.yml` / `monthly_distillation.yml` / `insider_fetch.yml` / `catalyst_one_button.yml` | all ok in window | Leave alone. |
| `stock_book.yml` | 6/6 ok | Thin wrapper. |

---

## 4. Ranked fix list

Do these in order. Later items get cheaper after the first four.

1. **Stop killing running research**  
   `cancel-in-progress: false` on `preopen_all`, `map_heat_postclose`, `daily_pipeline`, `learn_cycle`, `stock_book_all`, `xai_reauth`. Orchestrator: no re-dispatch if in_progress/queued.

2. **LLM default = auto (Grok → DeepSeek)**  
   A 500 from xAI must not blank the rest of the packet. Whole-run “gateway DOWN” breaker is too hot.

3. **Finviz Elite only on ubuntu**  
   Delete or no-op every ECS Elite fetch. Map Heat post-close clones today’s GH heat. This is the actual 403 bug.

4. **One `safe_git_push.sh`**  
   Fixes Stock Book / Learn / Label / AB / Pre-Open “failed” after the work was done. Set `HOME`. Never `git pull --rebase` dirty.

5. **Supabase is optional for morning**  
   Label+Weather + catalyst snapshot + collectors: retry, then degrade. Morning book must not die because Seoul pooler timed out.

6. **QC = required files, not “process started”**  
   Per-step skip-if-good. Fail the ALL job if any required output is missing (sectors 11/11 or documented minimum, book JSON, heat overlay with non-empty tape). Empty futures tape is not ready.

7. **Health/diag must not steal the runner**  
   Audit-only. Exit 0 after writing the board. Drop the 13:20 ET Stock Book ALL assumption.

8. **xAI OAuth stays human**  
   Reauth writes a URL and exits 0. Do not systemd --user from Actions. Do not hold ECS 16 minutes.

9. **OpenClaw config is sacred**  
   Backup before heal. Require `gateway.mode`. Stop leaking tokens in logs.

10. **Retire layer crons for real**  
    Diag, orchestrator, and ECS timers must only know the two ALL jobs + ubuntu scrape.

---

## 5. Example run IDs (latest of each mode)

| Mode | Example |
|---|---|
| Pre-Open incomplete sectors + missing research.md | https://github.com/SRoyaltyy/fullscan/actions/runs/33618162369 |
| Pre-Open cancel / clock script syntax (older) | https://github.com/SRoyaltyy/fullscan/actions/runs/32957814889 |
| Stock Book git-not-a-directory after book ran | https://github.com/SRoyaltyy/fullscan/actions/runs/33008376228 |
| Daily Pipeline OpenClaw 500 + GROK_ONLY | https://github.com/SRoyaltyy/fullscan/actions/runs/33458567281 |
| Daily Pipeline DeepSeek 402 | https://github.com/SRoyaltyy/fullscan/actions/runs/33046732176 |
| Map Heat xAI FallbackSummaryError | https://github.com/SRoyaltyy/fullscan/actions/runs/33401684769 |
| Map Heat ECS Finviz 403 + f-string SyntaxError | https://github.com/SRoyaltyy/fullscan/actions/runs/33159529821 |
| Label+Weather Supabase timeout | https://github.com/SRoyaltyy/fullscan/actions/runs/33760788230 |
| Learn Cycle commit race | https://github.com/SRoyaltyy/fullscan/actions/runs/33121883832 |
| Pipeline health never-green | https://github.com/SRoyaltyy/fullscan/actions/runs/33211880611 |
| Stock Book diag (honest FAIL board) | https://github.com/SRoyaltyy/fullscan/actions/runs/33799089078 |
| Events parse-failed | https://github.com/SRoyaltyy/fullscan/actions/runs/32475398785 |
| xAI reauth dbus / OAuth | https://github.com/SRoyaltyy/fullscan/actions/runs/33597448920 |
| Price store empty | https://github.com/SRoyaltyy/fullscan/actions/runs/33760787053 |
| Gainer as-of unknown flags | https://github.com/SRoyaltyy/fullscan/actions/runs/33532093167 |

---

## 6. What this catalog is not

- Not a claim that last night’s Pre-Open was red (last Pre-Open in the window **succeeded** 2026-09-03 `33732885143`). Success can still mean thin sectors or a skipped book — check the QC block in that run, not just the green check.
- Not an implementation. Fixes above are ordered so they can be separate PRs.
- Logs older than ~90 days (Gemini catcher, some orchestrator pushes) are gone from GitHub.
