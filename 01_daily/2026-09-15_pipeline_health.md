# Pipeline health — postclose

pre-open date=2026-09-15  post-close source=2026-09-15  post-close target=2026-09-15  book=2026-09-15
generated 2026-09-15T20:53:46.868808-04:00  round=1
**result=FAIL**  required_fails=1  warns=9

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
| OK | Post-Close ALL ran on 2026-09-15 | clock | no | n=6 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/35040838506` |
| WARN | Label + Weather ran on 2026-09-15 | clock | no | n=0 on 2026-09-15 — file checks below decide | `` |
| WARN | A+B1 Checklist ran on 2026-09-15 | clock | no | n=0 on 2026-09-15 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-09-15 | clock | no | n=0 on 2026-09-15 — file checks below decide | `` |
| OK | Sector Daily outcome+reflect ran on 2026-09-15 | clock | no | n=3 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/34974913032` |
| WARN | Learn Cycle ran on 2026-09-15 | clock | no | n=0 on 2026-09-15 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-09-15 | clock | no | n=10 latest=success event=workflow_run | `https://github.com/SRoyaltyy/fullscan/actions/runs/35020965904` |
| OK | 2026-09-15_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 269893 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-15_map_heat.json` |
| OK | 2026-09-15_map_heat.md | postclose | no | 17316 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-15_map_heat.md` |
| FAIL | 2026-09-15_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-09-14 (want 2026-09-15) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-15_research_baseline.json` |
| OK | 2026-09-15_research_baseline.md | postclose | yes | 43443 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-09-15_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| OK | Post-close LLM transcripts | postclose | no | n=39 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11089030 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/exports/finviz_2026-09-15.csv` |
| OK | Universe labels (segments) | book | yes | 1119114 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/universe/2026-09-15_membership.csv` |
| OK | Weather / regime JSON | book | yes | 9755 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/weather/2026-09-15_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2282919 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/join/2026-09-15_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7626799 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-15_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8478333 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/ab_checklist/2026-09-15_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 827894 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/peers/2026-09-15_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1600177 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-15_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 585 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/data/stock_book/2026-09-15_green.json` |
| OK | Stock book MD | book | yes | 84033 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-15_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 8315 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1805 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 5955084 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 8512 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 24177 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-15_outcome.md` |
| OK | General reflect MD | outcome | no | 10633 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/general/2026-09-15_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=10 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | yes | 11/11 | `` |
| OK | 2026-09-15_learnings.md (session copy) | learn | yes | 47276 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/2026-09-15_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47276 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20139 bytes | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-09-11 (want 2026-09-15) | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

- systemctl start fullscan-map-postclose.service → failed Job for fullscan-map-postclose.service failed because the control process exited with error code.
See "systemctl status fullscan-map-postclose.service" and "journalctl -xeu fullscan-map-postclose.service" for details.

