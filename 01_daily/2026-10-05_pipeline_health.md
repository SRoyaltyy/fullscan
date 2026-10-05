# Pipeline health — preopen

pre-open date=2026-10-05  post-close source=2026-10-02  post-close target=2026-10-05  book=2026-10-05
generated 2026-10-05T07:17:49.943547-04:00  round=1
**result=FAIL**  required_fails=16  warns=8

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
| OK | ECS clock file written | clock | no | 562 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-10-05 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/37282863252` |
| WARN | Map heat captain research (post-close) ran on 2026-10-02 | clock | no | n=0 on 2026-10-02 — file checks below decide | `` |
| WARN | Pre-Open ALL ran on 2026-10-05 | clock | no | n=2 latest=queued event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/37301175138` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 97128 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 11964 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 3225 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 1688 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 274169 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-10-05T07:14:39.387392-04:00 tape_n=59 | `` |
| FAIL | 2026-10-05_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-10-05 (want 2026-10-02) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_map_heat.json` |
| OK | 2026-10-05_map_heat.md | postclose | no | 18714 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_map_heat.md` |
| OK | 2026-10-05_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 359475 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_research_baseline.json` |
| OK | 2026-10-05_research_baseline.md | postclose | yes | 44951 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=38 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 97128 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 3225 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_finviz_market_digest.json` |
| OK | INPUT: last-night captain baseline | preopen | yes | phase=postclose_baseline 359475 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 4043466 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 33476 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-10-05_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| OK | News judge MD | preopen | yes | 10178 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_judge.md` |
| OK | Map-heat morning refresh JSON | preopen | yes | phase=morning_refresh 384187 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_research.json` |
| OK | Map-heat morning refresh MD | preopen | yes | 45153 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-10-05_research.md` |
| OK | News actions JSON | preopen | no | 66279 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-10-05_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 2756 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-10-05_dossiers.json` |
| OK | General market predict | preopen | yes | 16689 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-10-05_predict.md` |
| FAIL | Sector predict — Basic Materials | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/basic_materials_predict.md` |
| FAIL | Sector predict — Communication Services | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/communication_services_predict.md` |
| FAIL | Sector predict — Consumer Cyclical | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/consumer_cyclical_predict.md` |
| FAIL | Sector predict — Consumer Defensive | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/consumer_defensive_predict.md` |
| FAIL | Sector predict — Energy | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/energy_predict.md` |
| FAIL | Sector predict — Financial | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/financial_predict.md` |
| FAIL | Sector predict — Healthcare | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/healthcare_predict.md` |
| FAIL | Sector predict — Industrials | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/industrials_predict.md` |
| FAIL | Sector predict — Real Estate | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/real_estate_predict.md` |
| FAIL | Sector predict — Technology | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/technology_predict.md` |
| FAIL | Sector predict — Utilities | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/utilities_predict.md` |
| FAIL | ≥8/11 quality sector predicts | preopen | yes | 0/11 | `` |
| OK | Sector board JSON | preopen | no | 6363 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-10-05/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 8325 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-05_preopen_qc.json` |
| OK | Pre-open status JSON | preopen | yes | 9089 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-05_preopen_status.json` |
| FAIL | preopen_status.all_ok true | preopen | yes | all_ok=False grok_ok=False | `` |
| OK | Grok text review JSON | preopen | yes | 2544 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-10-05_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Core same-day packet is otherwise human-usable: general predict is UP/mild with MEMORY_CONFIRM and SCORES_BEGIN; events scan_date=2026-10-05 is a real 34-event  | `` |

## Fix actions

_none this run_

