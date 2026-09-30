# Pipeline health — preopen

pre-open date=2026-09-30  post-close source=2026-09-29  post-close target=2026-09-30  book=2026-09-30
generated 2026-09-30T08:11:27.883713-04:00  round=1
**result=FAIL**  required_fails=18  warns=8

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
| OK | ECS clock file written | clock | no | 556 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-09-30 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/36697310484` |
| WARN | Map heat captain research (post-close) ran on 2026-09-29 | clock | no | n=0 on 2026-09-29 — file checks below decide | `` |
| WARN | Pre-Open ALL ran on 2026-09-30 | clock | no | n=1 latest=failure event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/36688284790` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 97081 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 11917 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 4532 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 2170 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 278563 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-09-30T04:22:14.006127-04:00 tape_n=59 | `` |
| FAIL | 2026-09-30_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-09-30 (want 2026-09-29) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_map_heat.json` |
| OK | 2026-09-30_map_heat.md | postclose | no | 18263 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_map_heat.md` |
| OK | 2026-09-30_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 372862 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_research_baseline.json` |
| OK | 2026-09-30_research_baseline.md | postclose | yes | 44399 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=32 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 97081 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 4532 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_finviz_market_digest.json` |
| OK | INPUT: last-night captain baseline | preopen | yes | phase=postclose_baseline 372862 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 392687 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 34034 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-09-30_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| FAIL | events/latest.json points at today | preopen | yes | PASS-OVER latest scan_date=2026-09-29 | `` |
| OK | News judge MD | preopen | yes | 11318 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 396427 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 44601 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-30_research.md` |
| OK | News actions JSON | preopen | no | 33685 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-30_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 2869 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-09-30_dossiers.json` |
| OK | General market predict | preopen | yes | 15785 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-30_predict.md` |
| FAIL | Sector predict — Basic Materials | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/basic_materials_predict.md` |
| FAIL | Sector predict — Communication Services | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/communication_services_predict.md` |
| FAIL | Sector predict — Consumer Cyclical | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/consumer_cyclical_predict.md` |
| FAIL | Sector predict — Consumer Defensive | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/consumer_defensive_predict.md` |
| FAIL | Sector predict — Energy | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/energy_predict.md` |
| FAIL | Sector predict — Financial | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/financial_predict.md` |
| FAIL | Sector predict — Healthcare | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/healthcare_predict.md` |
| FAIL | Sector predict — Industrials | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/industrials_predict.md` |
| FAIL | Sector predict — Real Estate | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/real_estate_predict.md` |
| FAIL | Sector predict — Technology | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/technology_predict.md` |
| FAIL | Sector predict — Utilities | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/utilities_predict.md` |
| FAIL | ≥8/11 quality sector predicts | preopen | yes | 0/11 | `` |
| OK | Sector board JSON | preopen | no | 6363 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-30/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 8324 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-30_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 3029 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-30_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=False | `` |
| OK | Grok text review JSON | preopen | yes | 309 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-30_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Grok returned an empty review | `` |

## Fix actions

_none this run_

