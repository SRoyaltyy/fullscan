# Pipeline health — postclose

pre-open date=2026-09-18  post-close source=2026-09-18  post-close target=2026-09-18  book=2026-09-18
generated 2026-09-18T19:33:48.631315-04:00  round=1
**result=FAIL**  required_fails=4  warns=13

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
| WARN | Map heat captain research (post-close) ran on 2026-09-18 | clock | no | n=0 on 2026-09-18 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-09-18 | clock | no | n=3 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/35401954967` |
| WARN | Label + Weather ran on 2026-09-18 | clock | no | n=0 on 2026-09-18 — file checks below decide | `` |
| WARN | A+B1 Checklist ran on 2026-09-18 | clock | no | n=0 on 2026-09-18 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-09-18 | clock | no | n=0 on 2026-09-18 — file checks below decide | `` |
| OK | Sector Daily outcome+reflect ran on 2026-09-18 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35328193719` |
| WARN | Learn Cycle ran on 2026-09-18 | clock | no | n=0 on 2026-09-18 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-09-18 | clock | no | n=6 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35396213827` |
| OK | 2026-09-18_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 268645 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-18_map_heat.json` |
| OK | 2026-09-18_map_heat.md | postclose | no | 17362 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-18_map_heat.md` |
| FAIL | 2026-09-18_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-17 (want 2026-09-18) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-18_research_baseline.json` |
| OK | 2026-09-18_research_baseline.md | postclose | yes | 45122 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-18_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=52 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11255053 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-09-18.csv` |
| OK | Universe labels (segments) | book | yes | 1119394 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-09-18_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10132 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-09-18_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2271385 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-09-18_ranked.csv` |
| WARN | AB checklist (raw) | book | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-18_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 3272514 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-18_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 817497 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-09-18_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1660837 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-18_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 4863 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-18_green.json` |
| OK | Stock book MD | book | yes | 107797 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-18_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 9159 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1799 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 6301011 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 8906 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| FAIL | General outcome (graded call) | outcome | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-18_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-18_reflect.md` |
| WARN | Candidate lesson filed today | outcome | no | n=0 | `` |
| FAIL | Sector outcomes graded (>=8/11) | outcome | yes | 0/11 | `` |
| FAIL | 2026-09-18_learnings.md (session copy) | learn | yes | DID NOT RUN — file missing | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-18_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47492 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| WARN | LEARNINGS.md refreshed today | learn | no | no '2026-09-18' inside | `` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20171 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-09-17 (want 2026-09-18) | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

