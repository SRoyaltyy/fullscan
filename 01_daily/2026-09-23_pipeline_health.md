# Pipeline health — preopen

pre-open date=2026-09-23  post-close source=2026-09-22  post-close target=2026-09-23  book=2026-09-23
generated 2026-09-23T09:01:15.783091-04:00  round=1
**result=FAIL**  required_fails=6  warns=7

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
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-09-23 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35835870043` |
| WARN | Map heat captain research (post-close) ran on 2026-09-22 | clock | no | n=1 latest=cancelled event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35794564945` |
| OK | Pre-Open ALL ran on 2026-09-23 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35835879204` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 88045 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 11599 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 5067 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 2350 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 270679 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-09-23T04:16:39.961966-04:00 tape_n=59 | `` |
| OK | 2026-09-23_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 270679 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_map_heat.json` |
| OK | 2026-09-23_map_heat.md | postclose | no | 17495 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_map_heat.md` |
| FAIL | 2026-09-23_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-22 (want 2026-09-23) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_research_baseline.json` |
| OK | 2026-09-23_research_baseline.md | postclose | yes | 44842 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=32 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 88045 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 5067 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_finviz_market_digest.json` |
| FAIL | INPUT: last-night captain baseline | preopen | yes | PASS-OVER from source_heat_date=2026-09-22 (want 2026-09-23) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 61434 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 37907 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-09-23_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| OK | News judge MD | preopen | yes | 10823 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 421588 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 45044 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-23_research.md` |
| OK | News actions JSON | preopen | no | 31057 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-09-23_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 2853 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-09-23_dossiers.json` |
| OK | General market predict | preopen | yes | 15383 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-23_predict.md` |
| OK | Sector predict — Basic Materials | preopen | yes | 19952 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | yes | 18735 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | yes | 19530 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | yes | 19872 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | yes | 18978 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/energy_predict.md` |
| OK | Sector predict — Financial | preopen | yes | 18371 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | yes | 20446 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | yes | 21969 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | yes | 21866 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | yes | 18813 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | yes | 19141 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | yes | 11/11 | `` |
| OK | Sector board JSON | preopen | no | 5243 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-09-23/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 7264 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-23_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 8096 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-23_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=False | `` |
| OK | Grok text review JSON | preopen | yes | 1834 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-23_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Core general/events/judge/parse/finviz/map-heat are same-day and usable (UP mild; events scan_date 2026-09-23 with 14 new; 59-row futures tape). All 11 sector p | `` |

## Fix actions

_none this run_

