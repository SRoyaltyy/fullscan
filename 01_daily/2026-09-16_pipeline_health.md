# Pipeline health — postclose

pre-open date=2026-09-16  post-close source=2026-09-15  post-close target=2026-09-16  book=2026-09-16
generated 2026-09-16T20:59:14.824676-04:00  round=1
**result=FAIL**  required_fails=3  warns=11

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
| OK | systemd pre-open timer enabled | clock | no | enabled | `` |
| WARN | systemd pre-open service (now) | clock | no | failed | `` |
| OK | systemd post-close timer enabled | clock | yes | enabled | `` |
| OK | OpenClaw gateway unit | clock | no | active | `` |
| OK | ECS clock file written | clock | no | 306 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_ecs_clock.md` |
| WARN | Map heat captain research (post-close) ran on 2026-09-15 | clock | no | n=10 latest=cancelled event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35000147305` |
| OK | Post-Close ALL ran on 2026-09-15 | clock | no | n=7 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35042183397` |
| WARN | Label + Weather ran on 2026-09-16 | clock | no | n=0 on 2026-09-16 — file checks below decide | `` |
| WARN | A+B1 Checklist ran on 2026-09-16 | clock | no | n=0 on 2026-09-16 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-09-16 | clock | no | n=0 on 2026-09-16 — file checks below decide | `` |
| OK | Sector Daily outcome+reflect ran on 2026-09-16 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35088688829` |
| WARN | Learn Cycle ran on 2026-09-16 | clock | no | n=0 on 2026-09-16 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-09-16 | clock | no | n=8 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/35129414035` |
| OK | 2026-09-16_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 272533 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-16_map_heat.json` |
| OK | 2026-09-16_map_heat.md | postclose | no | 17369 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-16_map_heat.md` |
| FAIL | 2026-09-16_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-15 (want 2026-09-16) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-16_research_baseline.json` |
| OK | 2026-09-16_research_baseline.md | postclose | yes | 37373 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-16_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=34 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11002161 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-09-16.csv` |
| OK | Universe labels (segments) | book | yes | 1118410 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-09-16_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10085 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-09-16_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2239250 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-09-16_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7638352 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-16_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8473940 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-16_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 817685 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-09-16_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1413565 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-16_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 10038 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-16_green.json` |
| OK | Stock book MD | book | yes | 67246 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-16_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 8911 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1851 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 6089640 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 8711 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| FAIL | General outcome (graded call) | outcome | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-16_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-16_reflect.md` |
| WARN | Candidate lesson filed today | outcome | no | n=0 | `` |
| FAIL | Sector outcomes graded (>=8/11) | outcome | yes | 0/11 | `` |
| OK | 2026-09-16_learnings.md (session copy) | learn | yes | 47447 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-16_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47447 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20139 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| OK | book_policy.json (learned ranker weights) | learn | no | 9573 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

