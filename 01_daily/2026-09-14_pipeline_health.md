# Pipeline health — preopen

pre-open date=2026-09-14  post-close source=2026-09-11  post-close target=2026-09-14  book=2026-09-14
generated 2026-09-14T06:19:50.521914-04:00  round=1
**result=FAIL**  required_fails=10  warns=5

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| OK | HOME is /home/gha on ECS | runtime | no | HOME='/home/gha' | `` |
| OK | OpenClaw token (48 json vs 64 secret) | runtime | yes | live_len=48 tail=864c | `` |
| OK | OpenClaw port 18789 listening | runtime | yes | http://127.0.0.1:18789 | `` |
| FAIL | OpenClaw PONG | runtime | yes | http=200 Hey. I just came online. Who am I? Who are you?

I | `` |
| OK | Classroom model is Grok not DeepSeek | runtime | yes | model=openclaw/default | `` |
| WARN | xAI auth (OAuth or API key) | runtime | no | xAI token expiring but still usable / Config        : ~/.openclaw/openclaw.json Agent dir     : ~/.openclaw/agents/main/agent Default       : xai/grok-4.6 Fal | `` |
| WARN | XAI_API_KEY on this box (permanent) | runtime | no | missing — OAuth dies ~6h. Put a console.x.ai key in ~/.openclaw/.env | `` |
| WARN | GROK_ONLY (this health process) | runtime | no | off here — heal exports GROK_ONLY=1 | `` |
| OK | systemd pre-open timer enabled | clock | yes | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | no | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 306 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-09-14 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/34821315406` |
| WARN | Map heat captain research (post-close) ran on 2026-09-11 | clock | no | n=0 on 2026-09-11 — file checks below decide | `` |
| OK | Pre-Open ALL ran on 2026-09-14 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/34821320339` |
| OK | Finviz Elite digest JSON | scrape | yes | 89013 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_finviz_digest.json` |
| OK | Finviz Elite digest MD | scrape | no | 11985 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_finviz_digest.md` |
| FAIL | Map heat JSON (groups + morning overlay) | scrape | yes | PASS-OVER from date=2026-09-09 (want 2026-09-14) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-09-14T04:15:16.809073-04:00 tape_n=59 | `` |
| FAIL | 2026-09-14_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-09-09 (want 2026-09-14) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_map_heat.json` |
| OK | 2026-09-14_map_heat.md | postclose | no | 17571 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_map_heat.md` |
| FAIL | 2026-09-14_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-11 (want 2026-09-14) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_research_baseline.json` |
| OK | 2026-09-14_research_baseline.md | postclose | yes | 43435 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| FAIL | Research used Grok (not DeepSeek fallback) | postclose | yes | DeepSeek fallback text in baseline | `` |
| OK | Post-close LLM transcripts | postclose | no | n=40 | `` |
| OK | INPUT: Finviz digest from 05:40 scrape | preopen | yes | 89013 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_finviz_digest.json` |
| FAIL | INPUT: last-night captain baseline | preopen | yes | PASS-OVER from source_heat_date=2026-09-11 (want 2026-09-14) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 80514 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 28795 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-09-14_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| OK | News judge MD | preopen | yes | 13697 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 384097 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 43638 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-14_research.md` |
| OK | News actions JSON | preopen | no | 33045 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-14_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 6361 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-09-14_dossiers.json` |
| OK | General market predict | preopen | yes | 12096 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-14_predict.md` |
| OK | Sector predict — Basic Materials | preopen | yes | 14968 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/basic_materials_predict.md` |
| FAIL | Sector predict — Communication Services | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | yes | 13706 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | yes | 18605 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | yes | 11377 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/energy_predict.md` |
| OK | Sector predict — Financial | preopen | yes | 12561 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | yes | 13197 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | yes | 14974 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | yes | 17602 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | yes | 11537 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | yes | 12965 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | yes | 10/11 | `` |
| OK | Sector board JSON | preopen | no | 6967 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-14/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 6638 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-14_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 6936 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-14_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=False | `` |
| OK | Grok text review JSON | preopen | yes | 1469 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-14_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Core is otherwise strong: general predict takes a clear DOWN/mild direction with full SCORES markers, events JSON is a real same-day scan (scan_date 2026-09-14, | `` |

## Fix actions

_none this run_

