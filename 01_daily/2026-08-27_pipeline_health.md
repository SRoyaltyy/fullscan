# Pipeline health (audit only) — all

pre-open date=2026-08-27  post-close source=2026-08-26  post-close target=2026-08-27
generated 2026-08-27T12:54:00.772391-04:00
**result=FAIL**  required_fails=18  warns=10

This job **does not run** research, scrape, or OpenClaw. It only checks whether each step already ran and whether the file is empty / garbled / a timeout stub / a carry-forward / the wrong date.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| OK | HOME is /home/gha on ECS | runtime | no | HOME='/home/gha' | `` |
| OK | OpenClaw token (48 json vs 64 secret) | runtime | yes | live_len=48 tail=864c | `` |
| OK | OpenClaw port 18789 listening | runtime | yes | http://127.0.0.1:18789 | `` |
| OK | OpenClaw PONG | runtime | yes | model=openclaw/default | `` |
| OK | Classroom model is Grok not DeepSeek | runtime | yes | model=openclaw/default | `` |
| FAIL | xAI OAuth not expired | runtime | yes | xAI token expiring/expired | `` |
| OK | GROK_ONLY (this health process) | runtime | no | on | `` |
| OK | systemd pre-open timer enabled | clock | no | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | no | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 558 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| FAIL | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-08-27 | clock | yes | DID NOT RUN on 2026-08-27 (n=0) | `` |
| FAIL | Pre-Open ALL ran on 2026-08-27 | clock | yes | DID NOT RUN on 2026-08-27 (n=0) | `` |
| FAIL | Map heat captain research (post-close) ran on 2026-08-26 | clock | yes | n=2 latest=failure event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33037180154` |
| OK | Finviz Elite digest JSON | scrape | yes | 86230 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_finviz_digest.json` |
| OK | Finviz Elite digest MD | scrape | no | 11260 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_finviz_digest.md` |
| FAIL | Map heat JSON (groups + morning overlay) | scrape | yes | empty_futures_tape | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_map_heat.json` |
| FAIL | Futures tape non-empty | scrape | yes | EMPTY TAPE (403 overlay / scrape skipped) | `` |
| WARN | Econ calendar rows | scrape | no | n=0 | `` |
| WARN | Earnings calendar rows | scrape | no | n=0 | `` |
| WARN | Ticker-tagged Finviz news | scrape | no | n=0 | `` |
| FAIL | 2026-08-27_map_heat.json (industry groups + captains) | postclose | yes | empty_futures_tape | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_map_heat.json` |
| OK | 2026-08-27_map_heat.md | postclose | no | 6971 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_map_heat.md` |
| FAIL | 2026-08-27_research_baseline.json (captain cards) | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research_baseline.json` |
| FAIL | 2026-08-27_research_baseline.md | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research_baseline.md` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Finviz digest from 05:40 scrape | preopen | yes | 86230 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_finviz_digest.json` |
| FAIL | INPUT: last-night captain baseline | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 294954 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_parsed.json` |
| OK | News parse MD | preopen | no | 4691 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_parsed.md` |
| FAIL | Event scanner (primary, NOT carry) | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-08-27_events.json` |
| WARN | Events MD | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/events/2026-08-27_events.md` |
| FAIL | News judge MD | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_judge.md` |
| OK | News judge JSON (parsed tilts) | preopen | no | 1064 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_judge.json` |
| FAIL | Map-heat morning refresh JSON | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research.json` |
| FAIL | Map-heat morning refresh MD | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research.md` |
| OK | News actions JSON | preopen | no | 43953 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/news/2026-08-27_actions.json` |
| WARN | Catalyst dossiers JSON | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-08-27_dossiers.json` |
| WARN | Catalyst dossiers MD | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/catalyst/2026-08-27_dossiers.md` |
| FAIL | General market predict | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-08-27_predict.md` |
| OK | Sector predict — Basic Materials | preopen | yes | 10260 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | yes | 7960 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | yes | 13438 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | yes | 9518 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | yes | 8312 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/energy_predict.md` |
| OK | Sector predict — Financial | preopen | yes | 13698 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/financial_predict.md` |
| FAIL | Sector predict — Healthcare | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | yes | 9515 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | yes | 13866 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | yes | 10932 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | yes | 9453 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | yes | 10/11 | `` |
| OK | Sector board JSON | preopen | no | 6909 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/sectors/2026-08-27/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 6661 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_preopen_qc.json` |
| FAIL | Pre-open status JSON | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_preopen_status.json` |
| WARN | Pre-open status MD | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_preopen_status.md` |
| FAIL | Grok text review JSON | preopen | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_grok_review.json` |
| WARN | Grok text review MD | preopen | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_grok_review.md` |

## What each job is supposed to produce

Post-close night of SOURCE writes **TARGET-dated** files (`_map_heat.json`, `_research_baseline.json`).
GH Finviz scrape ~05:40 ET writes **today’s** digest and overlays tape onto that map heat.
Pre-open 05:55 ET consumes those, then writes parse / events / catcher / judge / morning refresh / actions / catalyst / general predict / 11 sector predicts / board / QC / Grok review.

