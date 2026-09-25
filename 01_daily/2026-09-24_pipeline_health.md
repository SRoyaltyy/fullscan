# Pipeline health — postclose

pre-open date=2026-09-24  post-close source=2026-09-24  post-close target=2026-09-25  book=2026-09-24
generated 2026-09-24T20:38:24.124597-04:00  round=1
**result=FAIL**  required_fails=2  warns=8

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
| WARN | Map heat captain research (post-close) ran on 2026-09-24 | clock | no | n=0 on 2026-09-24 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-09-24 | clock | no | n=4 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/36071383153` |
| OK | Label + Weather ran on 2026-09-24 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35983925474` |
| OK | A+B1 Checklist ran on 2026-09-24 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35983925842` |
| WARN | Daily pipeline outcome+reflect ran on 2026-09-24 | clock | no | n=0 on 2026-09-24 — file checks below decide | `` |
| WARN | Sector Daily outcome+reflect ran on 2026-09-24 | clock | no | n=0 on 2026-09-24 — file checks below decide | `` |
| WARN | Learn Cycle ran on 2026-09-24 | clock | no | n=0 on 2026-09-24 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-09-24 | clock | no | n=5 latest=success event=push | `https://github.com/SRoyaltyy/fullscan/actions/runs/36077796466` |
| FAIL | 2026-09-25_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-09-24 (want 2026-09-25) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-25_map_heat.json` |
| OK | 2026-09-25_map_heat.md | postclose | no | 17075 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-25_map_heat.md` |
| FAIL | 2026-09-25_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-24 (want 2026-09-25) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-25_research_baseline.json` |
| OK | 2026-09-25_research_baseline.md | postclose | yes | 44468 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-25_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=32 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 10963783 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-09-24.csv` |
| OK | Universe labels (segments) | book | yes | 1121131 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-09-24_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10537 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-09-24_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2264238 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-09-24_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7628740 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-24_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 3297063 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-24_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 814069 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-09-24_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1243271 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-24_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 585 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-24_green.json` |
| OK | Stock book MD | book | yes | 53299 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-24_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 10360 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1819 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 8633452 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 9468 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 24320 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-24_outcome.md` |
| OK | General reflect MD | outcome | no | 488 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-24_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=11 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | yes | 10/11 | `` |
| OK | 2026-09-24_learnings.md (session copy) | learn | yes | 47575 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-24_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47575 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20137 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-09-17 (want 2026-09-24) | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

