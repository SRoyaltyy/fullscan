# Pipeline health — all

pre-open date=2026-10-27  post-close source=2026-10-26  post-close target=2026-10-27  book=2026-10-26
generated 2026-10-27T03:18:51.817230-04:00  round=1
**result=FAIL**  required_fails=13  warns=29

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| WARN | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Map heat captain research (post-close) ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-10-26 | clock | no | n=3 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/40422222144` |
| OK | Pre-Open ALL ran on 2026-10-27 | clock | no | n=1 latest=success event=push | `https://github.com/SRoyaltyy/fullscan/actions/runs/40451041448` |
| WARN | Collect: RSS News ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Collect: NewsAPI ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Collect: Reddit ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Collect: yfinance Market Data ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Collect: FRED Macro ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Collect: Catalyst ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Insider Fetch ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Event Scanner (fallback yml) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | News judge (fallback yml) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| OK | Label + Weather ran on 2026-10-26 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40329126612` |
| OK | A+B1 Checklist ran on 2026-10-26 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/40301039813` |
| WARN | Catalyst daily (midday extra) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Daily pipeline outcome+reflect ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| WARN | Sector Daily outcome+reflect ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| WARN | Learn Cycle ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| WARN | News Actions Grader ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| WARN | HIT Board ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-10-26 | clock | no | n=4 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/40386013194` |
| WARN | Excel Bot (cluster signals) ran on 2026-10-27 | clock | no | n=0 on 2026-10-27 — file checks below decide | `` |
| WARN | Deploy dashboard to Pages ran on 2026-10-26 | clock | no | n=0 on 2026-10-26 — file checks below decide | `` |
| FAIL | Quote-page digest JSON (*_finviz_digest) | scrape | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_digest.json` |
| WARN | Quote-page digest MD (*_finviz_digest) | scrape | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_digest.md` |
| WARN | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_market_digest.json` |
| WARN | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_market_digest.md` |
| WARN | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_market_digest_close.json` |
| WARN | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_market_digest_close.md` |
| FAIL | Map heat JSON (groups + morning overlay) | scrape | yes | PASS-OVER from date=2026-10-26 (want 2026-10-27) | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_map_heat.json` |
| FAIL | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-10-26T04:33:19.374060-04:00 (pass-over / not overlaid today) | `` |
| OK | 2026-10-27_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 275729 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_map_heat.json` |
| OK | 2026-10-27_map_heat.md | postclose | no | 18736 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_map_heat.md` |
| OK | 2026-10-27_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 187513 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_research_baseline.json` |
| OK | 2026-10-27_research_baseline.md | postclose | yes | 25855 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| FAIL | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_digest.json` |
| WARN | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_finviz_market_digest.json` |
| OK | INPUT: last-night captain baseline | preopen | yes | phase=postclose_baseline 187513 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_research_baseline.json` |
| FAIL | News parse JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_parsed.json` |
| FAIL | Event scanner (primary, NOT carry) | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/events/2026-10-27_events.json` |
| FAIL | News judge MD | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_judge.md` |
| FAIL | Map-heat morning refresh JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_research.json` |
| FAIL | Map-heat morning refresh MD | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-10-27_research.md` |
| WARN | News actions JSON | preopen | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-10-27_actions.json` |
| WARN | Catalyst dossiers JSON | preopen | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/catalyst/2026-10-27_dossiers.json` |
| FAIL | General market predict | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-10-27_predict.md` |
| OK | Sector predict — Basic Materials | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/energy_predict.md` |
| OK | Sector predict — Financial | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | no | 0/11 optional | `` |
| WARN | Sector board JSON | preopen | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-10-27/_board.json` |
| FAIL | Pre-open QC JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-27_preopen_qc.json` |
| FAIL | Pre-open status JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-27_preopen_status.json` |
| FAIL | Grok text review JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-27_grok_review.json` |
| OK | INPUT: Finviz Elite export | book | yes | 11030866 bytes | `/home/runner/work/fullscan/fullscan/data/exports/finviz_2026-10-26.csv` |
| OK | Universe labels (segments) | book | yes | 1122695 bytes | `/home/runner/work/fullscan/fullscan/data/universe/2026-10-26_membership.csv` |
| OK | Weather / regime JSON | book | yes | 9387 bytes | `/home/runner/work/fullscan/fullscan/01_daily/weather/2026-10-26_weather.json` |
| OK | Weather signals.sectors ≥ 5 | book | yes | n=11 | `` |
| OK | Join ranked CSV | book | yes | 2218150 bytes | `/home/runner/work/fullscan/fullscan/data/join/2026-10-26_ranked.csv` |
| OK | AB checklist (raw) | book | no | 7564827 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-10-26_ab_checklist.csv` |
| OK | AB checklist (enriched) — s_ab | book | yes | 8374046 bytes | `/home/runner/work/fullscan/fullscan/data/ab_checklist/2026-10-26_ab_checklist_enriched.csv` |
| OK | Peer relative strength — s_peer | book | yes | 821534 bytes | `/home/runner/work/fullscan/fullscan/data/peers/2026-10-26_peer_rs.csv` |
| OK | Stock book JSON (5 horizons) | book | yes | 1450602 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-10-26_stock_book.json` |
| OK | Green pile grades (all-green BUY) | book | yes | 585 bytes | `/home/runner/work/fullscan/fullscan/data/stock_book/2026-10-26_green.json` |
| OK | Stock book MD | book | yes | 83223 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-26_stock_book.md` |
| OK | Stock book backtest (repo-level) | book | yes | 10524 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/STOCK_BOOK_BACKTEST.md` |
| OK | Paper trading summary | book | yes | 1837 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/PAPER_TRADING.md` |
| OK | Dashboard HTML (Pages source) | book | yes | 10510817 bytes | `/home/runner/work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 11354 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 28202 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-10-26_outcome.md` |
| OK | General reflect MD | outcome | no | 6001 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-10-26_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=1 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | no | SKIPPED — no sector predict files that day (sectors optional) | `` |
| OK | 2026-10-26_learnings.md (session copy) | learn | yes | 47568 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-10-26_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47568 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20169 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| OK | book_policy.json (learned ranker weights) | learn | no | 11495 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

