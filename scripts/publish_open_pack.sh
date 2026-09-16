#!/usr/bin/env bash
# Session-open tickets + land + Pages restamp.
# Called from Pre-Open / Stock Book skip-if-good paths so a quality-ok
# book does not leave .io / factor-mine / sleeve boards on last night's
# strip. Does not remine factor-mine. Does not change flatten_robust.
set -euo pipefail
DATE="${1:-$(TZ=America/New_York date +%F)}"
export FULLSCAN_LAND="${FULLSCAN_LAND:-1}"
# Job-level FINVIZ_SKIP_LIVE=1 is for scrape/ranker 403s, not ticket quotes.
# After the bell, drop it so GH-hosted ubuntu can pull Elite Overview Price.
ET_HM=$((10#$(TZ=America/New_York date +%H%M)))
if [ "$ET_HM" -ge 930 ]; then
  unset FINVIZ_SKIP_LIVE
  echo "[open-pack] after 09:30 — Elite Overview live px allowed (not Theme Radar)"
fi
# Pre-Open / Stock Book skip-if-good used to pip install pandas/openpyxl
# only. finviz_session imports requests — without it the 11:27 ET restamp
# wrote after_open=true and src=session_export+No module named 'requests'.
python3 -c "import requests" 2>/dev/null || pip install requests
echo "[open-pack] date=$DATE"
python3 -m src.strategy_tickets --date "$DATE" --write
python3 -m src.hold_live_px --date "$DATE" --write || true
python3 - "$DATE" "$ET_HM" <<'PY'
import json
import sys
from pathlib import Path
date = sys.argv[1]
et_hm = int(sys.argv[2])
payload = json.loads(Path("data/day_board/today_strategies.json").read_text())
legal = str(payload.get("clock_legal_for") or "")
use = str(payload.get("clock_use") or "")
if legal != date or use != "session_open":
    raise SystemExit(
        f"open-pack clock miss legal={legal!r} use={use!r} "
        f"want {date} session_open"
    )
quote = payload.get("quote") or {}
src = str(quote.get("src") or "")
if et_hm >= 930 and (
    not quote.get("after_open") or not src.startswith("elite_live")
):
    raise SystemExit(
        f"open-pack live px miss src={src!r} after_open={quote.get('after_open')!r}"
    )
print(f"[open-pack] clock_legal_for={legal} clock_use={use} src={src}")
PY
python3 -m src.publish_live_boards --date "$DATE" --write --no-extras || true
python3 -m src.land_file --date "$DATE" --key live_boards || true
chmod +x scripts/publish_dashboard.sh || true
bash scripts/publish_dashboard.sh || true
echo "[open-pack] done $DATE"
