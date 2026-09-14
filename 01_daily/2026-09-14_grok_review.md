# Grok text review — 2026-09-14

ok=False

Core is otherwise strong: general predict takes a clear DOWN/mild direction with full SCORES markers, events JSON is a real same-day scan (scan_date 2026-09-14, new items dated 09-14/09-15), news judge is complete with NEWS_PARSE_BEGIN/END, finviz digest is a full 400-ticker same-day export, and 10 of 11 sector essays are quality-ok with SECTOR_SCORES/HIT_GRID markers and real directions (only communication_services is missing, which is within the 8-of-11 tolerance). Two hard fails: the required news_parse JSON is absent, and the map_heat markdown is a stale 2026-09-09 carry-forward (the JSON map_heat table is same-day, but the .md is not). Baseline/research are present and phase is not morning_bootstrap (research.md shows a failed morning delta refresh falling back to post-close cards), s

## Fails
- `01_daily/news/2026-09-14_parsed.json`: missing — required core news_parse artifact absent (regex FAIL, no exception applies)
- `01_daily/map_heat/2026-09-14_map_heat.md`: wrong date — header reads 'MAP HEAT — 2026-09-09' with export finviz_2026-09-08.csv and generated 2026-09-09T01:59, i.e. a carry-forward of the 09-09 session, not today's 2026-09-14 tape
