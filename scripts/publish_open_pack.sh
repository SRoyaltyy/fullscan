#!/usr/bin/env bash
# Session-open tickets + land + Pages restamp.
# Called from Pre-Open / Stock Book skip-if-good paths so a quality-ok
# book does not leave .io / factor-mine / sleeve boards on last night's
# strip. Does not remine factor-mine. Does not change flatten_robust.
set -u
DATE="${1:-$(TZ=America/New_York date +%F)}"
export FULLSCAN_LAND="${FULLSCAN_LAND:-1}"
echo "[open-pack] date=$DATE"
python3 -m src.strategy_tickets --date "$DATE" --write
python3 -m src.publish_live_boards --date "$DATE" --write --no-extras || true
python3 -m src.land_file --date "$DATE" --key live_boards || true
chmod +x scripts/publish_dashboard.sh || true
bash scripts/publish_dashboard.sh || true
echo "[open-pack] done $DATE"
