# Pipeline health — postclose

pre-open date=2026-08-27  post-close source=2026-08-26  post-close target=2026-08-27  book=2026-08-27
generated 2026-08-27T22:25:29.205984-04:00  round=16
**result=FAIL**  required_fails=6  warns=4

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth expiry needs a human (`openclaw models auth login --provider xai`).

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| OK | HOME is /home/gha on ECS | runtime | no | HOME='/home/gha' | `` |
| OK | OpenClaw token (48 json vs 64 secret) | runtime | yes | live_len=48 tail=864c | `` |
| OK | OpenClaw port 18789 listening | runtime | yes | http://127.0.0.1:18789 | `` |
| OK | OpenClaw PONG | runtime | no | skipped — Grok job running (will not interrupt) | `` |
| OK | Classroom model is Grok not DeepSeek | runtime | no | probe skipped | `` |
| FAIL | xAI OAuth not expired | runtime | yes | HUMAN: openclaw models auth login --provider xai  (cannot auto-heal) | `` |
| OK | GROK_ONLY (this health process) | runtime | no | on | `` |
| OK | systemd pre-open timer enabled | clock | no | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | yes | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 546 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| FAIL | Map heat captain research (post-close) ran on 2026-08-26 | clock | yes | n=1 latest=failure event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33037180154` |
| OK | Label + Weather ran on 2026-08-27 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33132685053` |
| OK | A+B1 Checklist ran on 2026-08-27 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33128783279` |
| OK | Daily pipeline outcome+reflect ran on 2026-08-27 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33115084414` |
| OK | Sector Daily outcome+reflect ran on 2026-08-27 | clock | no | n=4 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33121876025` |
| FAIL | Learn Cycle ran on 2026-08-27 | clock | yes | n=3 latest=failure event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/33121883832` |
| WARN | Stock Book ALL ran on 2026-08-27 | clock | no | n=2 latest=queued event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/33132699701` |
| FAIL | 2026-08-27_map_heat.json (industry groups + captains) | postclose | yes | empty_futures_tape | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_map_heat.json` |
| OK | 2026-08-27_map_heat.md | postclose | no | 6971 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_map_heat.md` |
| FAIL | 2026-08-27_research_baseline.json (captain cards) | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research_baseline.json` |
| FAIL | 2026-08-27_research_baseline.md | postclose | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-27_research_baseline.md` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11225673 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-08-27.csv` |
| OK | Universe labels (segments) | book | yes | 1132877 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-08-27_membership.csv` |
| OK | Weather / regime JSON | book | yes | 9367 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-08-27_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2217844 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-08-27_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7734573 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-08-27_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8741529 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-08-27_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 817363 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-08-27_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 192042 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-08-27_stock_book.json` |
| OK | Stock book MD | book | yes | 102994 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-08-27_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 3240 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1718 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 2217095 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 6678 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 24159 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-08-27_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-08-27_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=11 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | yes | 10/11 | `` |
| OK | LEARNINGS.md digest | learn | yes | 45805 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 101065 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| OK | book_policy.json (learned ranker weights) | learn | no | 2520 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- dispatched label_weather.yml force (for clock.label_weather.yml)
- spawned python3 -m src.learn_cycle log=/home/gha/fullscan-logs/heal-learn_cycle.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- enable_openclaw_chat.sh exit=0 ons.enabled. Restart the gateway to apply. | port 18789 already listening — ping before restart | chat_http=200 hdr=bearer token=len=48 tail=864c | chat_body_prefix: {"id":"chatcmp
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- enable_openclaw_chat.sh exit=0 ons.enabled. Restart the gateway to apply. | port 18789 already listening — ping before restart | chat_http=200 hdr=bearer token=len=48 tail=864c | chat_body_prefix: {"id":"chatcmp
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log
- spawned bash /home/gha/actions-runner/_work/fullscan/fullscan/scripts/ecs_map_postclose.sh log=/home/gha/fullscan-logs/heal-map_heat_postclose.log

## Human-only (cannot auto-heal)

- `runtime.oauth`: HUMAN: openclaw models auth login --provider xai  (cannot auto-heal)

