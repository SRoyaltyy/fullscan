# Pipeline health — postclose

pre-open date=2026-10-28  post-close source=2026-10-27  post-close target=2026-10-28  book=2026-10-27
generated 2026-10-28T04:45:17.480597-04:00  round=1
**result=FAIL**  required_fails=1  warns=4

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| WARN | Map heat captain research (post-close) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-10-27 | clock | no | n=3 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/40604064770` |
| OK | Label + Weather ran on 2026-10-27 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40493981711` |
| OK | A+B1 Checklist ran on 2026-10-27 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40482239012` |
| OK | Daily pipeline outcome+reflect ran on 2026-10-27 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40457022861` |
| OK | Sector Daily outcome+reflect ran on 2026-10-27 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40457026053` |
| OK | Learn Cycle ran on 2026-10-27 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40467980636` |
| OK | Stock Book ALL ran on 2026-10-27 | clock | no | n=8 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/40562501151` |
| OK | 2026-10-28_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 276739 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-28_map_heat.json` |
| OK | 2026-10-28_map_heat.md | postclose | no | 18512 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-28_map_heat.md` |
| OK | 2026-10-28_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 187519 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-28_research_baseline.json` |
| OK | 2026-10-28_research_baseline.md | postclose | yes | 25855 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-28_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 11030866 bytes | `/home/runner/work/fullscan/fullscan/data/exports/finviz_2026-10-27.csv` |
| OK | Universe labels (segments) | book | yes | 1122695 bytes | `/home/runner/work/fullscan/fullscan/data/universe/2026-10-27_membership.csv` |
| OK | Weather / regime JSON | book | yes | 9869 bytes | `/home/runner/work/fullscan/fullscan/01_daily/weather/2026-10-27_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2287474 bytes | `/home/runner/work/fullscan/fullscan/data/join/2026-10-27_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7561499 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-10-27_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8352418 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-10-27_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 821534 bytes | `/home/runner/work/fullscan/fullscan/data/peers/2026-10-27_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1418050 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-10-27_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 2424 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-10-27_green.json` |
| OK | Stock book MD | book | yes | 71599 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-27_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 10524 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1837 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 10510817 bytes | `/home/runner/work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 11407 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| FAIL | General outcome (graded call) | outcome | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-10-27_outcome.md` |
| WARN | General reflect MD | outcome | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-10-27_reflect.md` |
| WARN | Candidate lesson filed today | outcome | no | n=0 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | no | SKIPPED — no sector predict files that day (sectors optional) | `` |
| OK | 2026-10-27_learnings.md (session copy) | learn | yes | 47585 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-27_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47585 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20190 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-10-26 (want 2026-10-27) | `/home/runner/work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

