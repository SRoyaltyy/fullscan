# Pipeline health — all

pre-open date=2026-11-09  post-close source=2026-11-09  post-close target=2026-11-10  book=2026-11-09
generated 2026-11-09T19:39:56.053681-05:00  round=1
**result=FAIL**  required_fails=5  warns=19

Heal loop: audit → fix OpenClaw door / timers on this box → start systemd or spawn the owning ECS job (ubuntu workflows are GH-dispatched with force=true) → wait for files → re-audit. Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in `~/.openclaw/.env`. An expiring token does not block Grok jobs.

| status | step | group | required | detail | path |
| --- | --- | --- | --- | --- | --- |
| OK | Finviz pre-open scrape (GH-hosted Elite) ran on 2026-11-09 | clock | no | n=2 latest=success event=schedule | `https://github.com/SRoyaltyy/fullscan/actions/runs/42698712780` |
| WARN | Map heat captain research (post-close) ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| OK | Post-Close ALL ran on 2026-11-09 | clock | no | n=2 latest=success event=push | `https://github.com/SRoyaltyy/fullscan/actions/runs/42726410794` |
| OK | Pre-Open ALL ran on 2026-11-09 | clock | no | n=6 latest=success event=push | `https://github.com/SRoyaltyy/fullscan/actions/runs/42726410901` |
| WARN | Collect: RSS News ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Collect: NewsAPI ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Collect: Reddit ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Collect: yfinance Market Data ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Collect: FRED Macro ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Collect: Catalyst ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Insider Fetch ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Event Scanner (fallback yml) ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | News judge (fallback yml) ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| OK | Label + Weather ran on 2026-11-09 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42623666956` |
| OK | A+B1 Checklist ran on 2026-11-09 | clock | no | n=2 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42624808461` |
| WARN | Catalyst daily (midday extra) ran on 2026-11-09 | clock | no | n=1 latest=failure event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42594867949` |
| WARN | Daily pipeline outcome+reflect ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Sector Daily outcome+reflect ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | Learn Cycle ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | News Actions Grader ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| WARN | HIT Board ran on 2026-11-09 | clock | no | n=0 on 2026-11-09 — file checks below decide | `` |
| OK | Stock Book ALL ran on 2026-11-09 | clock | no | n=10 latest=success event=workflow_run | `https://github.com/SRoyaltyy/fullscan/actions/runs/42726605578` |
| OK | Excel Bot (cluster signals) ran on 2026-11-09 | clock | no | n=1 latest=success event=workflow_dispatch | `https://github.com/SRoyaltyy/fullscan/actions/runs/42701965712` |
| WARN | Deploy dashboard to Pages ran on 2026-11-09 | clock | no | n=10 latest=pending event=workflow_run | `https://github.com/SRoyaltyy/fullscan/actions/runs/42729612346` |
| OK | Quote-page digest JSON (*_finviz_digest) | scrape | yes | 102881 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_digest.json` |
| OK | Quote-page digest MD (*_finviz_digest) | scrape | no | 12091 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_digest.md` |
| OK | Homepage warm-up JSON (*_finviz_market_digest) | scrape | no | 7947 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_market_digest.json` |
| OK | Homepage warm-up MD (*_finviz_market_digest) | scrape | no | 3523 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_market_digest.md` |
| OK | Close answer-key JSON (*_finviz_market_digest_close) | scrape | no | 7889 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_market_digest_close.json` |
| OK | Close answer-key MD (*_finviz_market_digest_close) | scrape | no | 3768 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_market_digest_close.md` |
| OK | Map heat JSON (groups + morning overlay) | scrape | yes | phase=morning_overlay 273892 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_map_heat.json` |
| OK | Futures tape + overlay_at today | scrape | yes | overlay_at=2026-11-09T04:57:03.852164-05:00 tape_n=59 | `` |
| OK | 2026-11-10_map_heat.json (industry groups + captains) | postclose | yes | phase=morning_overlay 273892 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_map_heat.json` |
| OK | 2026-11-10_map_heat.md | postclose | no | 18426 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_map_heat.md` |
| OK | 2026-11-10_research_baseline.json (captain cards) | postclose | yes | phase=postclose_baseline 187512 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_research_baseline.json` |
| OK | 2026-11-10_research_baseline.md | postclose | yes | 25855 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-10_research_baseline.md` |
| OK | Captain coverage ≥ 90% | postclose | yes | cards=142 coverage=1.0 | `` |
| WARN | Post-close LLM transcripts | postclose | no | n=0 | `` |
| OK | INPUT: Quote-page digest (*_finviz_digest) | preopen | yes | 102881 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_digest.json` |
| OK | INPUT: Homepage warm-up (*_finviz_market_digest) | preopen | no | 7947 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_finviz_market_digest.json` |
| OK | INPUT: last-night captain baseline | preopen | yes | phase=postclose_baseline 187523 bytes | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_research_baseline.json` |
| OK | News parse JSON | preopen | yes | 3792743 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_parsed.json` |
| OK | Event scanner (primary, NOT carry) | preopen | yes | 28845 bytes | `/home/runner/work/fullscan/fullscan/01_daily/events/2026-11-09_events.json` |
| FAIL | Event catcher second pass ran | preopen | yes | catcher key missing — second pass DID NOT RUN | `` |
| OK | News judge MD | preopen | yes | 10060 bytes | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_judge.md` |
| FAIL | Map-heat morning refresh JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_research.json` |
| FAIL | Map-heat morning refresh MD | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/map_heat/2026-11-09_research.md` |
| WARN | News actions JSON | preopen | no | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/news/2026-11-09_actions.json` |
| OK | Catalyst dossiers JSON | preopen | no | 1861 bytes | `/home/runner/work/fullscan/fullscan/01_daily/catalyst/2026-11-09_dossiers.json` |
| OK | General market predict | preopen | yes | 13285 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-09_predict.md` |
| OK | Sector predict — Basic Materials | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/basic_materials_predict.md` |
| OK | Sector predict — Communication Services | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/communication_services_predict.md` |
| OK | Sector predict — Consumer Cyclical | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/consumer_cyclical_predict.md` |
| OK | Sector predict — Consumer Defensive | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/consumer_defensive_predict.md` |
| OK | Sector predict — Energy | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/energy_predict.md` |
| OK | Sector predict — Financial | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/financial_predict.md` |
| OK | Sector predict — Healthcare | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/healthcare_predict.md` |
| OK | Sector predict — Industrials | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/industrials_predict.md` |
| OK | Sector predict — Real Estate | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/real_estate_predict.md` |
| OK | Sector predict — Technology | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/technology_predict.md` |
| OK | Sector predict — Utilities | preopen | no | optional — not run | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/utilities_predict.md` |
| OK | ≥8/11 quality sector predicts | preopen | no | 0/11 optional | `` |
| OK | Sector board JSON | preopen | no | 6085 bytes | `/home/runner/work/fullscan/fullscan/01_daily/sectors/2026-11-09/_board.json` |
| OK | Pre-open QC JSON | preopen | yes | 7576 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_preopen_qc.json` |
| FAIL | Pre-open status JSON | preopen | yes | DID NOT RUN — file missing | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_preopen_status.json` |
| OK | Grok text review JSON | preopen | yes | 308 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_grok_review.json` |
| FAIL | Grok text review ok=true | preopen | yes | Grok returned an empty review | `` |
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
| OK | Dashboard HTML (Pages source) | book | yes | 16224610 bytes | `/home/runner/work/fullscan/fullscan/dashboard/index.html` |
| OK | HIT_BOARD | book | no | 12342 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/HIT_BOARD.md` |
| OK | General outcome (graded call) | outcome | yes | 22719 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-09_outcome.md` |
| OK | General reflect MD | outcome | no | 5109 bytes | `/home/runner/work/fullscan/fullscan/01_daily/general/2026-11-09_reflect.md` |
| OK | Candidate lesson filed today | outcome | no | n=1 | `` |
| OK | Sector outcomes graded (>=8/11) | outcome | no | SKIPPED — no sector predict files that day (sectors optional) | `` |
| OK | 2026-11-09_learnings.md (session copy) | learn | yes | 47555 bytes | `/home/runner/work/fullscan/fullscan/01_daily/2026-11-09_learnings.md` |
| OK | LEARNINGS.md digest | learn | yes | 47555 bytes | `/home/runner/work/fullscan/fullscan/03_scoreboard/LEARNINGS.md` |
| OK | mutable_policy.md (machine injection) | learn | yes | 20209 bytes | `/home/runner/work/fullscan/fullscan/00_grounding/mutable_policy.md` |
| FAIL | book_policy.json (learned ranker weights) | learn | no | PASS-OVER from asof=2026-11-03 (want 2026-11-09) | `/home/runner/work/fullscan/fullscan/00_grounding/book_policy.json` |
| OK | Live dashboard reachable + data injected | pages | yes | HTTP 200, data_injected=True | `https://sroyaltyy.github.io/fullscan/dashboard/` |

## Fix actions

_none this run_

