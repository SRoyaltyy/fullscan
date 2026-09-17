# Grok text review — 2026-09-17

ok=False

Core dies on 0/11 sector predicts (need ≥8) plus a same-day news judge that is still the 09-16 FOMC-eve tape. General predict, events scan_date, news parse, finviz digest, and map-heat futures tables look like usable 2026-09-17 artifacts; research reused last-night captain cards after a failed morning QC (enhancement, not a core fail); close market digest is optional at preopen.

## Fails
- `01_daily/news/2026-09-17_judge.md`: Wrong-session / stale FOMC-eve packet labeled 2026-09-17: treats FOMC as unprinted, ranks warm retail and Jackson Hole hike-odds as live, and keeps B1/B3 unsigned; general predict itself flags this as 09-16 copy.
- `01_daily/sectors/2026-09-17/basic_materials_predict.md`: missing
- `01_daily/sectors/2026-09-17/communication_services_predict.md`: missing
- `01_daily/sectors/2026-09-17/consumer_cyclical_predict.md`: missing
- `01_daily/sectors/2026-09-17/consumer_defensive_predict.md`: missing
- `01_daily/sectors/2026-09-17/energy_predict.md`: missing
- `01_daily/sectors/2026-09-17/financial_predict.md`: missing
- `01_daily/sectors/2026-09-17/healthcare_predict.md`: missing
- `01_daily/sectors/2026-09-17/industrials_predict.md`: missing
- `01_daily/sectors/2026-09-17/real_estate_predict.md`: missing
- `01_daily/sectors/2026-09-17/technology_predict.md`: missing
- `01_daily/sectors/2026-09-17/utilities_predict.md`: missing
