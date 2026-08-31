# Pre-open ALL status — 2026-08-31

all_ok=False  qc_all_ok=True  grok_ok=False  missing=['map_heat_research']

Predictive modules (must land before 09:30 ET). Lessons / outcome /
deepthink / weekly / dashboard run later on their own crons.

- News parse: exit 0
- Event scanner (primary): exit 0
- Event catcher (gap hunt, no carry): exit 0
- News judge: exit 0
- Map heat morning delta refresh: exit 1
- Map heat morning delta refresh (retry): exit 1
- News actions: exit 0
- Catalyst dossiers (identified names): exit 0
- General market predict: exit 0
- Per-sector predict (all 11): exit 0
- Sector board: exit 0
