# Pipeline health — postclose

pre-open date=2026-08-28  post-close source=2026-08-27  post-close target=2026-08-28  book=2026-08-28
generated 2026-08-28T15:40:10.746955-04:00  round=16
**result=FAIL**  required_fails=7  warns=7

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
| OK | GROK_ONLY (this health process) | runtime | no | on | `` |
| OK | systemd pre-open timer enabled | clock | no | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | yes | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 554 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| WARN | Map heat captain research (post-close) ran on 2026-08-27 | clock | no | n=3 latest=cancelled event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33135103109` |
| OK | Label + Weather ran on 2026-08-28 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33145981263` |
| OK | A+B1 Checklist ran on 2026-08-28 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33145047801` |
| OK | Daily pipeline outcome+reflect ran on 2026-08-28 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33144343676` |
| OK | Sector Daily outcome+reflect ran on 2026-08-28 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33146755596` |
| OK | Learn Cycle ran on 2026-08-28 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33145039651` |
| WARN | Stock Book ALL ran on 2026-08-28 | clock | no | n=0 on 2026-08-28 — file checks below decide | `` |
| OK | 2026-08-28_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 246453 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_map_heat.json` |
| OK | 2026-08-28_map_heat.md | postclose | no | 10969 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_map_heat.md` |
| FAIL | 2026-08-28_research_baseline.json (captain cards) | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_research_baseline.json` |
| FAIL | 2026-08-28_research_baseline.md | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_research_baseline.md` |
| OK | Post-close LLM transcripts | postclose | no | n=19 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11205350 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-08-28.csv` |
| OK | Universe labels (segments) | book | yes | 1132859 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-08-28_membership.csv` |
| OK | Weather / regime JSON | book | yes | 9260 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-08-28_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2159855 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-08-28_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7730312 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-08-28_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8552699 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-08-28_ab_checklist_enriched.csv` |
| FAIL | Peer relative strength — s_peer | book | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-08-28_peer_rs.csv` |
| FAIL | Stock book JSON (5 horizons) | book | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-08-28_stock_book.json` |
| FAIL | Stock book MD | book | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-28_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 3240 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1718 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 2217095 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 6811 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| FAIL | General outcome (graded call) | outcome | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-08-28_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-08-28_reflect.md` |
| WARN | Candidate lesson filed today | outcome | no | n=0 | `` |
| FAIL | Sector outcomes graded (>=8/11) | outcome | yes | 0/11 | `` |
| OK | LEARNINGS.md digest | learn | yes | 45485 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 101027 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-08-27 (want 2026-08-28) | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- enable_openclaw_chat.sh exit=0 ons.enabled. Restart the gateway to apply. | port 18789 already listening — ping before restart | chat_http=200 hdr=bearer token=len=48 tail=864c | chat_body_prefix: {"id":"chatcmp
- enable_openclaw_chat.sh exit=0 ons.enabled. Restart the gateway to apply. | port 18789 already listening — ping before restart | chat_http=200 hdr=bearer token=len=48 tail=864c | chat_body_prefix: {"id":"chatcmp
- enable_openclaw_chat.sh exit=0 ons.enabled. Restart the gateway to apply. | port 18789 already listening — ping before restart | chat_http=200 hdr=bearer token=len=48 tail=864c | chat_body_prefix: {"id":"chatcmp

