# Modular daily workflows

Inventory of every scheduled (or clock-owned) daily workflow, then the stage split this branch builds. The runners, scripts, and time budgets below are the ones already on main plus the open Pre-Open renew / essay-slice branch (`cursor/preopen-oauth-in-job-b960`, PR #604). This work does not add Grok calls, paid models, or minutes to those budgets.

Engine files pinned by `research/hot_n4_clean_v4/protocol.py` `ENGINE_SHA256` (`src/factor_mine.py`, `src/factor_mine_book.py`, `src/paper_trade.py`, and the rest of that map) are not edited. Buy/sell ledgers stay append-only. A stage that finishes pushes its own files before the next stage starts, which `src/land_file.py` already does inside Pre-Open; the progress file rides along and a failed progress push cannot fail the job.

## How to read a row

- **Trigger** is the unattended clock. `workflow_dispatch` exists on almost every file and is not repeated.
- **Runner** is `ubuntu-latest` or the self-hosted label pair `self-hosted` + `ecs` (`ecs-openclaw` in the task language).
- **Entry** is the shell or Python module the job actually starts.
- Paths are repo-relative. `01_daily/<D>` means the America/New_York session date.

## Inventory

| Workflow | File | Trigger | Runner | Entry | Reads | Writes |
| --- | --- | --- | --- | --- | --- | --- |
| Pre-Open ALL | `.github/workflows/preopen_all.yml` | cron `55 9 * * 1-5` (~05:55 ET while EDT). Push of this file on `main` is a poke, not a clock. | Scrape + book jobs: ubuntu. Packet job: ubuntu on push, schedule, and any dispatch that is not `runner=ecs`. ECS dispatch: `self-hosted` + `ecs`. systemd `scripts/ecs_preopen.sh` is the on-box twin at 05:55 ET. | Workflow calls `python -m src.run_preopen_all`. ECS timer calls `scripts/ecs_preopen.sh`, which renews xAI (`scripts/openclaw_oauth_guard.py renew`, `OPENCLAW_RUN_COVER_MIN=75`) and then the same module. | `01_daily/news/<D>_finviz_digest.*`, `01_daily/news/<D>_finviz_market_digest.*`, `01_daily/map_heat/<D>_map_heat.json`, `01_daily/map_heat/<D>_research_baseline.*`, `data/exports/finviz_<D>.csv`, `data/universe/<D>_membership.csv`, `data/join/<D>_ranked.csv`, `data/ab_checklist/<D>_ab_checklist*.csv`, `01_daily/weather/<D>_weather.json`, Channel 1 under `01_daily/_channel1/`, `01_daily/_transcripts/`. | Packet: `01_daily/news/<D>_parsed.*`, `01_daily/news/<D>_actions.*`, `01_daily/news/<D>_judge.*`, `01_daily/events/<D>_events.*`, `01_daily/general/<D>_predict.md` (+ trace), `01_daily/sectors/<D>/*_predict.md`, `01_daily/sectors/<D>/_board.json`, `01_daily/map_heat/<D>_research.*`, `01_daily/catalyst/<D>_dossiers.*`, `01_daily/<D>_preopen_qc.json`, `01_daily/<D>_preopen_status.json`, `01_daily/<D>_preopen_status.md`, `01_daily/<D>_grok_review.*`. Book: `data/stock_book/<D>_*.json`, `01_daily/<D>_stock_book.md`, `data/day_board/`, `dashboard/` (live strip, factor-mine today, paper index), `03_scoreboard/PAPER_TRADING.md` via `src.paper_trade_append` (append only). |
| Finviz pre-open scrape | `.github/workflows/finviz_preopen_scrape.yml` | cron `40 9 * * 1-5` (~05:40 ET). Also the first job inside Pre-Open ALL. | ubuntu (ECS 403s finviz.com) | `collectors.finviz_financials`, `src.finviz_digest`, `src.finviz_market_digest`, `src.map_heat --overlay` | Elite secrets, prior `01_daily/news/<D>_finviz_digest.*`, prior overlay. | `data/exports/finviz_<D>.csv`, `01_daily/news/<D>_finviz_digest.{md,json}`, `01_daily/news/<D>_finviz_market_digest.{md,json}`, `01_daily/map_heat/<D>_map_heat.{md,json}`, `data/day_board/`. Exits 0 after 09:25 ET and does not write. |
| Homepage digest | `.github/workflows/finviz_market_digest.yml` | cron `0 12 * * 1-5` (~08:00 ET last chance) and cron `5 16 * * 1-5` with `timezone: America/New_York` (close answer-key). Morning capture also runs inside the scrape. | ubuntu | `src.finviz_market_digest` | Homepage `$MARKET` prose. Not quote-page blurbs. | Morning: `01_daily/news/<D>_finviz_market_digest.{md,json}`. Close: `01_daily/news/<D>_finviz_market_digest_close.{md,json}` (`clock_use=next_open`). Close never clobbers morning. |
| Finviz digest fallback | `.github/workflows/finviz_digest.yml` | dispatch only. Orchestrator last chance after 08:00 ET. No cron. | ubuntu | `src.finviz_digest` | Same export the scrape owns. | Same digest paths. |
| Stock Book ALL | `.github/workflows/stock_book_all.yml` | cron `10 10 * * 1-5` (~06:10 ET) and `15 13 * * 1-5` (~09:15 ET). Also `workflow_run` after scrape. | ubuntu | `src.run_stock_book_all` | Scrape artifacts, universe, join, AB, weather. `--skip-llm` on the Pre-Open book job. | `data/stock_book/`, `data/join/`, `data/universe/`, `data/ab_checklist/`, `data/peers/`, `01_daily/weather/`, `01_daily/<D>_stock_book.md`, `data/day_board/`. |
| Post-Close ALL | `.github/workflows/postclose_all.yml` | cron `10 20 * * 1-5` (~16:10 ET) and `30 3 * * 2-6` (~23:30 ET backup). Push of the listed Python modules starts ubuntu last-closed. | Schedule and push: ubuntu (`LLM_BACKEND=deepseek`). Dispatch default: `self-hosted` + `ecs` (Grok). systemd `scripts/ecs_map_postclose.sh` at 22:00 ET is the on-box twin. ECS yields 04:00–10:30 ET. | `src.run_postclose_all` → `src.run_outcome`, `src.horizon_grade`, `src.run_reflect`, `src.run_sector_outcome`, `src.run_sector_reflect`, `src.sector_board`, `src.news_grade`, `src.hit_board`, `src.learn_cycle`, `src.improvement_tracker`, `src.map_heat_postclose` | That session's predicts, sectors, map heat, news. | `01_daily/general/<D>_outcome.md`, `01_daily/general/<D>_reflect.md`, `01_daily/sectors/<D>/*_outcome.md`, `*_reflect.md`, `01_daily/<D>_learnings.md`, `01_daily/map_heat/<next>_research_baseline.*`, `02_lessons/`, `03_scoreboard/`, `00_grounding/mutable_policy.md`, `book_policy.json`, `weather_rules_proposals.json`. Each block already calls `safe_git_push.sh` before the next block. |
| Post-Close last-closed | `.github/workflows/postclose_last_closed.yml` | cron `25 15 * * 1-5` (~11:25 ET) and `15 20 * * 1-5` (~16:15 ET). | ubuntu | `src.run_postclose_all` with `last_closed_session()` only (not the two-date night pack). | Same as Post-Close, one closed session. | Same paths for that one date. |
| Map-heat captain research | `.github/workflows/map_heat_postclose.yml` | dispatch. Cron is commented. The live clock is the Post-Close step plus ECS 22:00. | `self-hosted` + `ecs` when dispatched | `src.map_heat_postclose` | Closed session map heat and research. | Next session `01_daily/map_heat/<target>_research_baseline.*`. |
| Daily orchestrator | `.github/workflows/daily_orchestrator.yml` | cron `50 11`, `20 13`, `35 13`, `20 20`, `15 21` weekdays, plus Sunday `30 16`. | ubuntu | Shell. Dispatches the owning workflow. Does not scrape or call a model itself. | `src.skip_if_good` for preopen / postclose / book. | No packet files. |
| Runtime door | `.github/workflows/runtime_door.yml` | dispatch (phone Claw tab). | `self-hosted` + `ecs` | `scripts/runtime_status.py --heal` or `--write` | OpenClaw gateway, timers, `scripts/enable_openclaw_chat.sh`. Does not PONG while a Grok job is in `pgrep`. | `01_daily/_runtime.json`. |
| Pipeline health | `.github/workflows/pipeline_health.yml` | dispatch. Cron disabled 2026-08-29. | `self-hosted` + `ecs` | `src.pipeline_health` | Door, timers, today's packet / book / outcomes. | `01_daily/<D>_pipeline_health.{md,json}`, `01_daily/_xai_reauth.json`, `data/health/`. Default `--no-fix`. |
| Deploy / Pages | `.github/workflows/deploy-dashboard.yml` | `workflow_run` after Stock Book ALL, Pre-Open ALL, Publish strategy tickets, Open 09:30, Factor strategy mine, Factor Mine pre-open sleeves. Push of `dashboard/**` or `data/stock_book/**`. | ubuntu | `src.pages_publish_gate`, then `scripts/publish_dashboard.sh` | `dashboard/`, day-board required roles, factor-mine `today.json`, tickets. | `gh-pages` (orphan publish). A failed gate fails the job. Push-triggered packet freeze guards stay in `src.freeze_guard` / `scripts/safe_git_push.sh`. |
| Factor strategy mine (append / land-closed) | `.github/workflows/factor_mine.yml` | cron `25 20 * * 1-5` (~16:25 ET) and Saturday `0 12 * * 6`. `workflow_run` after book, Pre-Open, Post-Close. | ubuntu | `src.factor_mine` land-closed path. `src/factor_mine_book.py` is byte-pinned and is not edited; the workflow is the wrapper. | Published recipe set, cash book, past-day lock. | Append-only scoreboard / state under `data/factor_mine/`, `03_scoreboard/factor_mine/`, `dashboard/factor-mine/`. No backfill. Morning triggers are a no-op once yesterday is on the board. |
| Factor Mine pre-open sleeves | `.github/workflows/fm_preopen.yml` | cron `2 13` and `15 13` weekdays (seal, 09:00–09:25 ET only) and `40 21` (book, ~17:40 ET). | ubuntu | `src.factor_mine_preopen` | Pre-open tickets (`look.source=look`), morning weather S gate, prior sleeve state. Not the predict markdown. | Seal once: `data/factor_mine/preopen/seals/<D>.json`. Book: `data/factor_mine/state/<sleeve>/<D>.json` once, append the day under the past-day lock. A seal at or after 09:30 ET is a sit. |
| Webull sim | `.github/workflows/webull_sim.yml` | cron `45 13`, `15 14`, `15 20` weekdays (~09:45, ~10:15, ~16:15 ET). First run is after the 03:00–09:40 ET freeze. | ubuntu | `src.webull_sim` | Theme Radar plans (public clone), price store, `data/past_day_lock/manifest.jsonl`. | `data/webull_sim/`, `03_scoreboard/WEBULL_SIM.md`, `dashboard/webull-sim/`, append-only lock rows. 16:15 is the only run that may lock a name with no open. |
| Webull paper | `.github/workflows/webull_paper.yml` | cron `7 12,13 * * 1-5` (not a sender). Real send is `workflow_run` after `h1 append-only forward`, and only before 09:30 ET with `PAPER_OPEN_SENDER=seal`. | ubuntu | `src.paper_open` (the pinned `src/paper_trade.py` is not the entry and is not edited). | Sealed h1 new-buy set, sandbox credentials. | Paper-open journal. Cron and manual dispatch exit before any write. |
| OAuth renew + gateway guard | `.github/workflows/openclaw_oauth_guard.yml` | cron `10 1 * * 1-5` (~21:10 ET) and `40 8 * * 1-5` (~04:40 ET). | `self-hosted` + `ecs` | `scripts/install_openclaw_oauth_timer.sh`, `scripts/openclaw_oauth_guard.py` (`renew`, `guard`, or `all`) | OpenClaw profile expiry, gateway. Does not treat `ecs_preopen.sh` as a competing Grok job when Pre-Open calls renew itself (#604). | `01_daily/_openclaw_oauth_status.json` (tokens stripped). Local log `/home/gha/fullscan-logs/openclaw_guard_status.json`. |
| SuperGrok canary | `.github/workflows/grok_auth_canary.yml` | cron `30 8 * * 1-5` (~04:30 ET). | `self-hosted` + `ecs` | `scripts/grok_auth_canary.py` | Gateway on 127.0.0.1:18789. Read-only completion. No renew, no restart, no `XAI_API_KEY`. | Step summary only. No packet files. |
| SuperGrok ping | `.github/workflows/openclaw_grok_ping.yml` | Push of the ping script / model map. No cron. | `self-hosted` + `ecs` | `scripts/enable_openclaw_chat.sh` (skipped if a Grok job is running), `scripts/openclaw_grok_ping.py` | Gateway. | `01_daily/_openclaw_grok_ping.json`, `01_daily/_openclaw_grok_ping.md`. Device login via `scripts/xai_device_reauth.py` only when the ping got no reply and no Grok job is running. The `user_code` is not published for Cyrus's phone. |
| Input freeze | `.github/workflows/input_freeze.yml` | cron `15 13` and `15 14` weekdays (09:15 ET) plus `0 11` early backstop that sleeps to 09:20 ET. | ubuntu | freeze writer | The morning packet files it lists. | `research/input_freeze/<D>/manifest.json` with `status=frozen`. `src.freeze_guard` then refuses re-lands of those dated paths. |
| Decision freeze check | `.github/workflows/decision_freeze_check.yml` | cron `43 13` and `43 14` weekdays. | ubuntu | replay check | The frozen manifest. | Check output only. Does not rewrite the frozen day. |
| Pre-open plan check | `.github/workflows/preopen_plan_check.yml` | cron 08:45 ET and 09:27 ET (both DST offsets). | ubuntu | `src.preopen_plan_check` | The packet. | A named fail. Does not rewrite essays. |
| Sleeve merge live | `.github/workflows/sleeve_merge_live.yml` | cron `25 13 * * 1-5` (~09:25 ET). | ubuntu | `src.sleeve_merge` | Today's book. | Flatten card. Skips a hollow card when the book is missing. |
| Publish strategy status | `.github/workflows/publish_strategy_status.yml` | cron `*/10 10-13` and `*/30 14-21` weekdays. | ubuntu | status publisher | Day board / tickets. | `data/strategy_status/`. |
| News intake | `.github/workflows/news_intake.yml` | cron `7,22,37` and `52` every hour. | ubuntu | free intake collectors | Public news. | `01_daily/news/` intake files. No paid model. |
| Lane public articles | `.github/workflows/lane_public_articles.yml` | cron `5,20,35,50` every hour. | ubuntu | lane article script | Public pages. | Lane outputs. |
| Live hold marks | `.github/workflows/live_px.yml` | cron `45 13` plus `0,20,40 14-19` and `0 20` weekdays. | ubuntu | hold mark writer | Open prices. | `data/day_board/hold_live_px.json` and the factor-mine copy. Marks, not a new buy/sell day. |
| Updown shadow | `.github/workflows/updown_shadow.yml` | several after-close and pre-open crons (see the file). | ubuntu | shadow freeze | Paper books. | Append-only shadow files. |
| Forward shadow v1 | `.github/workflows/forward_shadow_v1.yml` | cron `20 14`, `45 21`, `15 23` weekdays. | ubuntu | shadow script | Prior signals. | Append-only shadow. |
| Excel bot | `.github/workflows/excel_bot.yml` | cron `30 10 * * 2-6` and `17 21 * * 1-5`. | ubuntu | excel bot | Sheets / cluster signals. | Suggestion files. Undated rolling files are not freeze-blocked. |
| JEV hop-0 | `.github/workflows/jev_hop0.yml` | cron `25 11 * * 1-6`. | ubuntu | `src` JEV hop-0 | News directory. | JEV nets. |
| News impact backtest | `.github/workflows/news_impact_backtest.yml` | cron `30 21 * * 1-6` (~17:30 ET). | ubuntu | backtest module | Closed news. | Research output. Not a live order. |
| Finviz ALL | `.github/workflows/finviz_all.yml` | cron `10 1 * * 2-6`. | ubuntu | Elite export | Finviz. | `data/exports/`. Delayed schedule before 05:35 ET is a no-op. |
| h1 open fill / holdup open fill | `.github/workflows/h1_open_fill.yml`, `holdup_open_fill.yml` | cron `35 13 * * 1-5` (~09:35 ET). | ubuntu | open-fill modules | Sealed h1 / holdup sets. | Append-only fills. Missed deadline does not buy. |
| h1 forward / holdup forward / h1 fill / holdup fill | matching `h1_*.yml` / `holdup_*.yml` | morning forward cron `5 13`; after-close fill cron `30 21`. | ubuntu | those modules | Prior state. | Append-only. |
| Excel deep corr mine | `.github/workflows/excel_deep_corr_mine.yml` | Saturday `20 11` and `20 5,11,17,23` every day. | ubuntu | research miner | Excel panel. | Research only. |

Commented-out crons (not in the live clock): pipeline health, daily pipeline, sector daily predict, label/weather, news grade, learn cycle, map-heat post-close workflow, insider fetch, collect-* historical crons. They stay dispatch-only.

## Clocks that must stay

- Pre-Open job timeout stays 75 minutes. The Python step stays 50 minutes. `FULLSCAN_STEP_DEADLINE` stays 40 minutes (`2400` seconds) so a cancel does not revoke `GITHUB_TOKEN` mid-push.
- `PREOPEN_LLM_TIMEOUT` stays `420`. Five essays (events, catcher, news actions, judge, general predict) split that deadline via `_essay_timeout` (floor 90s, 120s reserved). A child process cannot move `FULLSCAN_STEP_DEADLINE` later than the parent (`src/step_deadline.py`).
- Sector predict stays on `_step_budget(600)` and, inside the loop, `MIN_SECTOR_S=180` / `MAX_SECTOR_S=600`. A sector that no longer fits is not started.
- Map-heat refresh stays on `_step_budget(180)`. If it does not fit, the night baseline is passed through and the model is not called.
- Catalyst wall stays `CATALYST_STEP_S` = 8 × 360 + 45 + 75 seconds. It runs after the book is on main.
- Tail steps (live boards, sleeve, flatten, paper) still shrink with `_step_budget` and skip when the deadline is inside the floor.
- Post-Close ECS still yields 04:00–10:30 ET. Ubuntu post-close stays on `deepseek` (free path the workflow already selected; this branch does not add a provider).
- Push of `preopen_all.yml` must not start a packet before 03:55 ET, must not start one on a weekend, and must not start one during 03:00–09:40 ET. `force=true` remains the only bypass, matching the old gate. Schedule at 05:55 ET is not a push.

## Stage split

Each stage has a wall-clock budget that already exists. A stage does not get a new model call. Outputs are pushed by `src.land_file` (Pre-Open) or `safe_git_push.sh` (Post-Close and the progress writer) before the next stage starts. A later failure leaves the earlier stage on `main`.

Progress for one run is `01_daily/_progress/<date>_<workflow>.json`. The index is `01_daily/_progress/manifest.json`. A seal for a finished stage is `01_daily/_progress/seals/<date>_<workflow>_<stage>.json`. A solo `workflow_dispatch` with `stage=<name>` and `run_date` refuses to overwrite a seal, a frozen input-freeze path, or a past-day lock row. It does not backfill a date before the last sealed day.

### Pre-Open (`workflow=preopen`)

| Stage | What runs (existing modules) | Inputs | Outputs | Budget |
| --- | --- | --- | --- | --- |
| `prep` | `src.segments`, `src.weather`, `src.join`, `src.ab_checklist`, `src.ab_enrich` | export, universe, FRED/DB if present | `data/universe/<D>_membership.csv`, `01_daily/weather/<D>_weather.*`, `data/join/<D>_ranked.csv`, `data/ab_checklist/<D>_ab_checklist*.csv` | 180 + 50 + 180 + 1500 + 180 seconds, same caps as `run_preopen_all`. No LLM. |
| `news` | `src.news_parse`, `src.run_events`, `src.run_events_catcher`, `src.news_actions` | digest, homepage digest, overlay, Channel 1 | `01_daily/news/<D>_parsed.*`, `01_daily/news/<D>_actions.*`, `01_daily/events/<D>_events.*` | Parse 120s (one retry, then limit-80). Each essay is one slice of the 40-minute step, ceiling `PREOPEN_LLM_TIMEOUT+60` (480s), floor 90s. Five slices are shared with judge and general predict. |
| `judge` | `src.run_news_judge` | parsed news, actions | `01_daily/news/<D>_judge.{md,json}` | One slice of the same 40-minute step (the second-to-last essay). |
| `general_predict` | `src.run_predict`, then the existing heal in `run_preopen_all._heal_general_predict` (only when that heal already qualifies, before 09:00 ET, max two retries) | judge, events, weather, map-heat baseline | `01_daily/general/<D>_predict.md` and its trace. Landed before sectors start. | Last essay slice, then heal capped at `min(llm_sub_t, 420)` and shrunk to the step deadline (floor 90s). Skipped when the deadline cannot hold it. |
| `sectors` | `src.run_sector_predict` for all 11, or `stage=sectors:<Sector>` for one. Sectors: Basic Materials, Communication Services, Consumer Cyclical, Consumer Defensive, Energy, Financial, Healthcare, Industrials, Real Estate, Technology, Utilities. | general predict, Channel 1, ETF tape | `01_daily/sectors/<D>/<slug>_predict.md` (+ trace, transcript). Done/total is N/11. Each fresh QC-ok sector already mid-commits. | Parent wall `_step_budget(600)`. Per sector 180–600s via `step_deadline.share`. Stop when the next sector does not fit. No stub essays. |
| `map_heat` | `src.map_heat_refresh`. Passthrough of the night baseline when the slice does not fit or the call times out. | `01_daily/map_heat/<D>_research_baseline.*`, morning overlay | `01_daily/map_heat/<D>_research.{md,json}` | 180s shrunk to the deadline (floor 60s). Passthrough is local and 60s. |
| `book_sleeve` | `src.run_stock_book_all` (`--skip-llm --skip-extras`), `src.morning_look live_boards`, `src.sleeve_combine_bt`, `src.sleeve_merge`, `src.paper_trade_append` | ranker inputs, predicts | `data/stock_book/<D>_*.json`, `01_daily/<D>_stock_book.md`, `data/day_board/`, flatten card, append-only `03_scoreboard/PAPER_TRADING.md` | Book is the ranker (no extra LLM). Live boards 120s, sleeve 180s, flatten 120s, paper 180s, each shrunk or skipped at the deadline. Paper append does not rewrite a sealed day. |
| `catalyst` | `src.catalyst_daily` | book names | `01_daily/catalyst/<D>_dossiers.{md,json}` | `CATALYST_STEP_S` (3000s). Runs after the book push. |
| `qc` | `src.output_qc`, `src.grok_review` | files already on disk | `01_daily/<D>_preopen_qc.json`, `01_daily/<D>_preopen_status.*`, `01_daily/<D>_grok_review.*` | Local. The review is the existing stamp, not a new essay. |
| `deploy` | `scripts/publish_dashboard.sh` and the Pages workflow | `dashboard/` | GitHub Pages | Workflow step, outside the 50-minute Python budget. A progress-push failure here does not fail the publish. |

Solo rerun input: `stage` equals one of those names (or `sectors:<Sector>`), `run_date` is `YYYY-MM-DD`. Empty `stage` is the full packet.

### Post-Close (`workflow=postclose`)

| Stage | What runs | Inputs | Outputs | Budget |
| --- | --- | --- | --- | --- |
| `outcome` | `src.run_outcome` | that session's general predict | `01_daily/general/<D>_outcome.md` | LLM HTTP cap (default 900s), shrunk to the 04:00 ET yield on ECS. Pushed before reflect. |
| `horizon` | `src.horizon_grade` | outcomes | horizon grade files | 60s. No LLM. |
| `reflect` | `src.run_reflect` | outcome | `01_daily/general/<D>_reflect.md` | same 900s cap. Pushed before the sector loop. |
| `sector_outcomes` | `src.run_sector_outcome` | 11 sector predicts | `01_daily/sectors/<D>/*_outcome.md` | `max(5400, 11 * (llm_cap + 90))`, still cut by the morning yield. |
| `sector_reflect` | `src.run_sector_reflect` | sector outcomes | `*_reflect.md` | same sector wall. |
| `learn` | `src.news_grade`, `src.hit_board`, `src.learn_cycle`, `src.improvement_tracker` | grades | `01_daily/<D>_learnings.md`, `02_lessons/`, `03_scoreboard/` | 300 + 60 + 180 + 60 seconds. Pushed before captains. |
| `captains` | `src.map_heat_postclose` | closed map heat | next session baseline | subprocess wall 7200s, LLM cap `POSTCLOSE_CAPTAIN_TIMEOUT` (default 1200s), cut by the morning yield. |

### The rest

| Workflow id | Stages | Budget | Notes |
| --- | --- | --- | --- |
| `finviz_scrape` | `elite`, `digest`, `homepage`, `overlay` | 20 minute job. Each module is the one the scrape already runs. | Exit 0 after 09:25 ET, no write. |
| `homepage_digest` | `morning` or `close` | 20 minute job | Close file is a different path from morning. |
| `runtime_door` | `snapshot` | 8 minute job | No PONG under a live Grok job. |
| `pipeline_health` | `audit` | 180 minute job, heal rounds stay 1 | `--no-fix` unless the dispatch says otherwise. |
| `deploy` | `gate`, `publish` | 20 minute job | Gate failure still fails the job. |
| `factor_mine` | `land_closed` | mine step 120 minutes inside a 150 minute job | Wrapper only. Pinned book module is not edited. No backfill. |
| `fm_preopen` | `seal` or `book` | 30 minute job. Seal only 09:00–09:25 ET. | Write-once seal. |
| `webull_sim` | `rows` | 30 minute job | Past-day lock stays in `src.webull_sim`. |
| `webull_paper` | `preflight` | cron exits before a write. Seal send is unchanged. | Pinned `paper_trade.py` is not edited. |
| `oauth_guard` | `renew`, `guard` | 20 minute job | In-job Pre-Open renew stays in the Pre-Open step and in `ecs_preopen.sh`. |
| `supergrok_canary` | `ping` | 8 minute job | Read-only. |
| `supergrok_ping` | `ping` | 20 minute job | Skips enable/restart while a Grok job is running. |

## Live page

`dashboard/progress/index.html` reads `01_daily/_progress/manifest.json` and each run file from `main` (raw GitHub, so a heartbeat does not republish Pages). It shows each stage's state, an elapsed timer from `started_at`, and a stale warning when a `running` stage's `heartbeat_at` is more than 3 minutes old.

## Verify on the next morning

1. After 05:40 ET, `01_daily/_progress/<today>_finviz_scrape.json` should show `digest` / `homepage` / `overlay` moving to `ok`, and those files should already be on `main` before Pre-Open's packet job starts.
2. After the 05:55 ET Pre-Open starts, open the Pages path `dashboard/progress/` (or the HTML on `main`). `01_daily/_progress/<today>_preopen.json` should list `prep` through `deploy`. `general_predict` should flip to `ok` and `01_daily/general/<today>_predict.md` should be on `main` before `sectors` finishes. `sectors.done` / `sectors.total` should climb toward 11. A heartbeat older than 3 minutes while the state is `running` is the stale warning.
3. A push of `.github/workflows/preopen_all.yml` between 03:00 and 09:40 ET should end at the clock gate with `go=no` and no packet job.
4. Solo rerun: dispatch Pre-Open ALL with `stage=general_predict` and `run_date=<today>` after that file is quality-ok or sealed. The job must exit refused and the predict file's bytes must be unchanged.
