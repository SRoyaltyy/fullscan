# Pipeline health — preopen

pre-open date=2026-09-22  post-close source=2026-09-21  post-close target=2026-09-22  book=2026-09-22
generated 2026-09-22T08:37:12.699057-04:00  round=1
**result=FAIL**  required_fails=5  warns=7

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| OK | HOME is /home/gha on ECS | runtime | no | HOME='/home/gha' | `` |
| OK | OpenClaw token (48 json vs 64 secret) | runtime | yes | live_len=48 tail=864c | `` |
| OK | OpenClaw port 18789 listening | runtime | yes | http://127.0.0.1:18789 | `` |
| OK | OpenClaw PONG | runtime | yes | model=openclaw/default | `` |
| OK | Classroom model is Grok not DeepSeek | runtime | yes | model=openclaw/default | `` |
| WARN | xAI auth (OAuth or API key) | runtime | no | xAI token expiring but still usable / Config        : ~/.openclaw/openclaw.json Agent dir     : ~/.openclaw/agents/main/agent Default       : xai/grok-4.6 Fal | `` |
| WARN | XAI_API_KEY on this box (permanent) | runtime | no | missing — OAuth dies ~6h. Put a console.x.ai key in ~/.openclaw/.env | `` |
| WARN | GROK_ONLY (this health process) | runtime | no | off here — heal exports GROK_ONLY=1 | `` |
| OK | systemd pre-open timer enabled | clock | yes | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | no | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 556 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-09-22 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35703317424` |
| WARN | Map heat captain research (post-close) ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| OK | Pre-Open ALL ran on 2026-09-22 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35703325293` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 87496 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 11487 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 6032 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 2583 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 269772 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-09-22T04:13:32.637758-04:00 tape_n=59 | `` |
| OK | 2026-09-22_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 269772 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_map_heat.json` |
| OK | 2026-09-22_map_heat.md | postclose | no | 17556 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_map_heat.md` |
| FAIL | 2026-09-22_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-21 (want 2026-09-22) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research_baseline.json` |
| OK | 2026-09-22_research_baseline.md | postclose | yes | 44517 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=30 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 87496 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 6032 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_finviz_market_digest.json` |
| FAIL | INPUT: last-night captain baseline | preopen | yes | PASS-OVER from source_heat_date=2026-09-21 (want 2026-09-22) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 62803 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 37678 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-09-22_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| OK | News judge MD | preopen | yes | 11088 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 407286 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 44719 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research.md` |
| OK | News actions JSON | preopen | no | 32179 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-22_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 2853 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-09-22_dossiers.json` |
| OK | General market predict | preopen | yes | 15225 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-22_predict.md` |
| OK | Sector predict — Basic Materials | preopen | yes | 19367 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | yes | 18642 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | yes | 17896 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | yes | 21159 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | yes | 19350 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/energy_predict.md` |
| OK | Sector predict — Financial | preopen | yes | 19258 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | yes | 17480 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | yes | 22941 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | yes | 20368 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | yes | 18424 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | yes | 17835 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | yes | 11/11 | `` |
| OK | Sector board JSON | preopen | no | 5243 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-22/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 7264 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-22_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 8096 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-22_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=False | `` |
| OK | Grok text review JSON | preopen | yes | 1744 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-22_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Core general/events/news-judge/parse/finviz-digest/map-heat are same-day and usable (DOWN mild; scan_date 2026-09-22; live futures tape populated), but sector c | `` |

## Fix actions

_none this run_

