# Pipeline health — preopen

pre-open date=2026-10-07  post-close source=2026-10-06  post-close target=2026-10-07  book=2026-10-07
generated 2026-10-07T08:18:15.060353-04:00  round=1
**result=FAIL**  required_fails=4  warns=8

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
| OK | ECS clock file written | clock | no | 306 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-10-07 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/37592753888` |
| WARN | Map heat captain research (post-close) ran on 2026-10-06 | clock | no | n=0 on 2026-10-06 — file checks below decide | `` |
| OK | Pre-Open ALL ran on 2026-10-07 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/37604436216` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 98242 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 11722 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 4568 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 2227 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 273475 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-10-07T04:22:15.456945-04:00 tape_n=59 | `` |
| FAIL | 2026-10-07_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-10-07 (want 2026-10-06) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_map_heat.json` |
| OK | 2026-10-07_map_heat.md | postclose | no | 18251 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_map_heat.md` |
| OK | 2026-10-07_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 354425 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_research_baseline.json` |
| OK | 2026-10-07_research_baseline.md | postclose | yes | 43162 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=33 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 98242 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 4568 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_finviz_market_digest.json` |
| OK | INPUT: last-night captain baseline | preopen | yes | phase=postclose_baseline 354425 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 8261123 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 35440 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-10-07_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| FAIL | events/latest.json points at today | preopen | yes | PASS-OVER latest scan_date=2026-10-05 | `` |
| OK | News judge MD | preopen | yes | 13415 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 379434 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 43364 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-07_research.md` |
| OK | News actions JSON | preopen | no | 96327 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-07_actions.json` |
| WARN | Catalyst dossiers JSON | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-10-07_dossiers.json` |
| OK | General market predict | preopen | yes | 15902 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-10-07_predict.md` |
| OK | Sector predict — Basic Materials | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/energy_predict.md` |
| OK | Sector predict — Financial | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | no | optional — not run | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | no | 0/11 optional | `` |
| OK | Sector board JSON | preopen | no | 6363 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-07/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 8353 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-07_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 6264 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-07_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=True | `` |
| OK | Grok text review JSON | preopen | yes | 1008 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-07_grok_review.json` |
| OK | Grok text review ok=true | preopen | yes | All required core artifacts are present, same-day (2026-10-07), and complete: general predict takes a clear DOWN/mild direction with MEMORY_CONFIRM and full fac | `` |

## Fix actions

_none this run_

