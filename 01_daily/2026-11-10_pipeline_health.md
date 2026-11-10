# Pipeline health — postclose

pre-open date=2026-11-10  post-close source=2026-11-10  post-close target=2026-11-10  book=2026-11-10
generated 2026-11-10T17:43:24.274285-05:00  round=1
**result=FAIL**  required_fails=3  warns=8

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| WARN | Map heat captain research (post-close) ran on 2026-11-10 | clock | no | n=0 on 2026-11-10 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-11-10 | clock | no | n=1 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/42809121619` |
| OK | Label + Weather ran on 2026-11-10 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42797238676` |
| WARN | A+B1 Checklist ran on 2026-11-10 | clock | no | n=0 on 2026-11-10 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-11-10 | clock | no | n=0 on 2026-11-10 — file checks below decide | `` |
| WARN | Sector Daily outcome+reflect ran on 2026-11-10 | clock | no | n=0 on 2026-11-10 — file checks below decide | `` |
| WARN | Learn Cycle ran on 2026-11-10 | clock | no | n=0 on 2026-11-10 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-11-10 | clock | no | n=10 latest=success event=workflow_run | `https://github.com/SRoyaltyy/fullscan/actions/runs/42899680034` |
| OK | 2026-11-10_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 275967 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_map_heat.json` |
| OK | 2026-11-10_map_heat.md | postclose | no | 18484 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_map_heat.md` |
| FAIL | 2026-11-10_research_baseline.json (captain cards) | postclose | yes | PASS-OVER from source_heat_date=2026-11-09 (want 2026-11-10) | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_research_baseline.json` |
| OK | 2026-11-10_research_baseline.md | postclose | yes | 25855 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 12367040 bytes | `/home/runner/work/fullscan/fullscan/data/exports/finviz_2026-11-10.csv` |
| OK | Universe labels (segments) | book | yes | 1124403 bytes | `/home/runner/work/fullscan/fullscan/data/universe/2026-11-10_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10058 bytes | `/home/runner/work/fullscan/fullscan/01_daily/weather/2026-11-10_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2314913 bytes | `/home/runner/work/fullscan/fullscan/data/join/2026-11-10_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7533042 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-11-10_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8310264 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-11-10_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 808484 bytes | `/home/runner/work/fullscan/fullscan/data/peers/2026-11-10_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1642248 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-11-10_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 6313 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-11-10_green.json` |
| OK | Stock book MD | book | yes | 107577 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-10_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 10508 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1808 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 16224610 bytes | `/home/runner/work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 12394 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| FAIL | General outcome (graded call) | outcome | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-10_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-10_reflect.md` |
| WARN | Candidate lesson filed today | outcome | no | n=0 | `` |
| FAIL | Sector outcomes graded (>=8/11) | outcome | yes | 0/11 | `` |
| OK | 2026-11-10_learnings.md (session copy) | learn | yes | 47555 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-10_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47555 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20165 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-11-03 (want 2026-11-10) | `/home/runner/work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

