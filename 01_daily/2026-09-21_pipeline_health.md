# Pipeline health — postclose

pre-open date=2026-09-21  post-close source=2026-09-21  post-close target=2026-09-22  book=2026-09-21
generated 2026-09-21T16:43:49.086104-04:00  round=1
**result=FAIL**  required_fails=2  warns=10

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
| WARN | Map heat captain research (post-close) ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| WARN | Post-Close ALL ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| WARN | Label + Weather ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| WARN | A+B1 Checklist ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| OK | Sector Daily outcome+reflect ran on 2026-09-21 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35582256348` |
| WARN | Learn Cycle ran on 2026-09-21 | clock | no | n=0 on 2026-09-21 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-09-21 | clock | no | n=6 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/35640358710` |
| FAIL | 2026-09-22_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-09-21 (want 2026-09-22) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_map_heat.json` |
| OK | 2026-09-22_map_heat.md | postclose | no | 17324 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_map_heat.md` |
| FAIL | 2026-09-22_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-21 (want 2026-09-22) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research_baseline.json` |
| OK | 2026-09-22_research_baseline.md | postclose | yes | 44517 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-22_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=30 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 10981709 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-09-21.csv` |
| OK | Universe labels (segments) | book | yes | 1116675 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-09-21_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10212 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-09-21_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2285038 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-09-21_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7652058 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-21_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8489311 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-21_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 816990 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-09-21_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1403296 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-21_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 1598 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-21_green.json` |
| OK | Stock book MD | book | yes | 69142 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-21_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 9159 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1799 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 6301103 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 9112 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 29395 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-21_outcome.md` |
| OK | General reflect MD | outcome | no | 12282 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-21_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=12 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | yes | 11/11 | `` |
| OK | 2026-09-21_learnings.md (session copy) | learn | yes | 47864 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-21_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47864 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20389 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-09-17 (want 2026-09-21) | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

