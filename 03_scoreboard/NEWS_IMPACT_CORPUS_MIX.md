# News-impact corpus + mix + horizons

Research-only B+C+D on top of the #306 hygiene merge. Not a new taxonomy. Deterministic router. No Lane on 5657. theme-radar is read-only — no PR there.

## Window

- earliest on disk: 2026-04-26 (Finviz export). earliest parse: 2026-08-08. latest: 2026-09-21.
- June 2026 parse present: False. No June 2026 *_parsed.json. Earliest parse is 2026-08-08. Earliest Finviz export is 2026-04-26 (one file). Window used = earliest on-disk news through latest.

## Honest funnel

- raw headlines (all used sources, before title dedupe): **333132**
- unique after title dedupe: **29307**
- non-reaction / non-weather: **8749**
- impulse + up/down + listed expression: **1531**
- has tape (graded 0-1d row): **94**

has_tape_graded counts unique articles with at least one graded 0-1d tape row. If this is only hundreds, that is the truth — the five-digit target needs unused/empty sources (theme-radar headline export, RSS/Supabase dumps, June parses) to fill.

## Graded rates (same #305 + #306 cut, horizon-aware)

- 0-1d: 31/47 = 66.0%
- 1-4w: 26/51 = 51.0%
- guidance slice 0-1d (after reaffirm + 1-4w natural window): n/a
- guidance slice 1-4w: n/a
- macro headline-level basket 0-1d (not legs): 6/8 = 75.0% (stories=37, reprints_collapsed=15)

## Convergence vs singleton 0-1d

- converge groups: 42  0-1d 13/18 = 72.2%
- singleton groups: 7219  0-1d 56/102 = 54.9%
- conflict groups (ungraded): 36
- high_mass groups (binary flag, no multiplier): 185

## Sources scanned

- **parsed_json** `01_daily/news/*_parsed.json` — files=31 2026-08-08..2026-09-21 status=used
- **finviz_export** `data/exports/finviz_YYYY-MM-DD.csv (News Title + Daily Digest)` — files=35 2026-04-26..2026-09-21 status=used
- **finviz_digest** `01_daily/news/*finviz*digest*.json` — files=62 2026-07-30..2026-09-21 status=used
- **events_json** `01_daily/events/*_events.json` — files=29 2026-08-10..2026-09-21 status=used
- **actions_keep** `01_daily/news/*_actions.json (KEEP / conditional evidence)` — files=33 2026-08-09..2026-09-21 status=used
- **grok_automations** `data/grok_automations/*.json` — files=0 .. status=empty
- **rss_dumps** `collectors/rss_news.py (workflow exists; no dump dir on disk)` — files=0 .. status=empty
- **supabase_dumps** `src/db.py news pooler (no local dump on disk)` — files=0 .. status=empty
- **theme_radar_snapshots** `https://github.com/SRoyaltyy/theme-radar data/snapshots/*.csv` — files=30 2026-08-06..2026-09-18 status=unused_readonly
  - Theme Radar: please export a headline pack (date, ticker, News Title, News Time, Daily Digest, News URL) from data/snapshots/*.csv into a JSON/CSV we can ingest in fullscan. Do not merge the repos.

## Theme Radar ask (do not edit that repo)

Theme Radar: please export a headline pack (date, ticker, News Title, News Time, Daily Digest, News URL) from data/snapshots/*.csv into a JSON/CSV we can ingest in fullscan. Do not merge the repos.

## Horizon window grid (graded directional only)

| class | natural | 0-1d hits | 0-1d n | 0-1d rate | 1-4w hits | 1-4w n | 1-4w rate | 1-6m hits | 1-6m n | 1-6m rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| access_control | ? | 0 | 1 | 0.0 | 0 | 0 | — | 0 | 0 | — |
| blast_cyber | 1-4w | 0 | 0 | — | 0 | 0 | — | 0 | 0 | — |
| blast_legal | ? | 2 | 2 | 1.0 | 1 | 2 | 0.5 | 0 | 0 | — |
| capacity | 1-6m | 0 | 0 | — | 0 | 4 | 0.0 | 0 | 0 | — |
| capital_return | ? | 5 | 13 | 0.3846 | 1 | 3 | 0.3333 | 0 | 0 | — |
| demand | ? | 0 | 1 | 0.0 | 0 | 0 | — | 0 | 0 | — |
| dilution | ? | 0 | 0 | — | 0 | 0 | — | 0 | 0 | — |
| gate | 1-6m | 0 | 0 | — | 1 | 2 | 0.5 | 0 | 0 | — |
| guidance | 1-4w | 0 | 0 | — | 0 | 0 | — | 0 | 0 | — |
| input_cost | ? | 6 | 6 | 1.0 | 6 | 6 | 1.0 | 0 | 0 | — |
| inventory_print | 0-1d | 1 | 2 | 0.5 | 0 | 0 | — | 0 | 0 | — |
| labor_stop | ? | 0 | 0 | — | 0 | 0 | — | 0 | 0 | — |
| listing_flow | ? | 2 | 3 | 0.6667 | 2 | 2 | 1.0 | 0 | 0 | — |
| market_structure | ? | 13 | 16 | 0.8125 | 7 | 8 | 0.875 | 0 | 0 | — |
| peer_spill | ? | 0 | 0 | — | 0 | 0 | — | 0 | 0 | — |
| print_vs_priced | 1-4w | 0 | 0 | — | 8 | 23 | 0.3478 | 0 | 0 | — |
| product_harm | ? | 2 | 3 | 0.6667 | 0 | 1 | 0.0 | 0 | 0 | — |

