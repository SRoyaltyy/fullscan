# Pipeline health — postclose

pre-open date=2026-11-09  post-close source=2026-11-06  post-close target=2026-11-09  book=2026-11-09
generated 2026-11-09T18:13:49.479471-05:00  round=1
**result=FAIL**  required_fails=1  warns=5

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| WARN | Map heat captain research (post-close) ran on 2026-11-06 | clock | no | n=0 on 2026-11-06 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-11-06 | clock | no | n=2 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/42320867395` |
| OK | Label + Weather ran on 2026-11-09 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42623666956` |
| OK | A+B1 Checklist ran on 2026-11-09 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42624808461` |
| WARN | Daily pipeline outcome+reflect ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Sector Daily outcome+reflect ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Learn Cycle ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-11-09 | clock | no | n=10 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/42706198833` |
| FAIL | 2026-11-09_map_heat.json (industry groups + captains) | postclose | yes | PASS-OVER from date=2026-11-09 (want 2026-11-06) | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_map_heat.json` |
| OK | 2026-11-09_map_heat.md | postclose | no | 18426 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_map_heat.md` |
| OK | 2026-11-09_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 187523 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_research_baseline.json` |
| OK | 2026-11-09_research_baseline.md | postclose | yes | 25855 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Finviz Elite export | book | yes | 12407137 bytes | `/home/runner/work/fullscan/fullscan/data/exports/finviz_2026-11-09.csv` |
| OK | Universe labels (segments) | book | yes | 1127482 bytes | `/home/runner/work/fullscan/fullscan/data/universe/2026-11-09_membership.csv` |
| OK | Weather / regime JSON | book | yes | 10284 bytes | `/home/runner/work/fullscan/fullscan/01_daily/weather/2026-11-09_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2292796 bytes | `/home/runner/work/fullscan/fullscan/data/join/2026-11-09_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7552428 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-11-09_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8347885 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-11-09_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 810923 bytes | `/home/runner/work/fullscan/fullscan/data/peers/2026-11-09_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1499826 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-11-09_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 585 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-11-09_green.json` |
| OK | Stock book MD | book | yes | 83531 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 10508 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1808 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 12568491 bytes | `/home/runner/work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 12342 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 22719 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-09_outcome.md` |
| OK | General reflect MD | outcome | no | 5109 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-09_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=1 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | no | SKIPPED — no sector predict files that day (sectors optional) | `` |
| OK | 2026-11-09_learnings.md (session copy) | learn | yes | 47599 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47599 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20209 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-11-03 (want 2026-11-09) | `/home/runner/work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

