#!/usr/bin/env bash
# DST-correct 22:00 ET post-close map/captain research, run by systemd.
set -euo pipefail

ROOT="${FULLSCAN_ROOT:-/home/gha/fullscan}"
ENVF="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
LOCK="${MAP_POSTCLOSE_LOCK:-/tmp/fullscan-map-postclose.lock}"
exec 9>"$LOCK"
flock -n 9 || { echo "[map-postclose] lock held — skip"; exit 0; }

if [ -f "$ENVF" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENVF"
  set +a
fi
export OPENCLAW_GATEWAY_URL="${OPENCLAW_GATEWAY_URL:-http://127.0.0.1:18789}"
export OPENCLAW_TIMEOUT="${OPENCLAW_TIMEOUT:-10800}"
export HOME="${FULLSCAN_HOME:-/home/gha}"
export PYTHONUNBUFFERED=1

cd "$ROOT"
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true
git config --global --add safe.directory "$ROOT" || true
git config --global --add safe.directory '*' || true
git config user.name "Market-Bot-Automaton"
git config user.email "bot@users.noreply.github.com"
if [ -n "${GITHUB_TOKEN:-}" ]; then
  git config --local http.https://github.com/.extraheader \
    "AUTHORIZATION: bearer ${GITHUB_TOKEN}"
fi
git fetch origin main
git checkout main
git reset --hard origin/main
chmod +x scripts/*.sh || true
bash scripts/ensure_openclaw_timeouts.sh || true

PY="${FULLSCAN_PYTHON:-python3}"
[ -x "$ROOT/.venv/bin/python" ] && PY="$ROOT/.venv/bin/python"
SOURCE=$(TZ=America/New_York date +%F)
TARGET=$("$PY" -c "from src.map_heat_postclose import next_weekday; print(next_weekday('$SOURCE'))")
echo "[map-postclose] source=$SOURCE target=$TARGET"

"$PY" -m src.map_heat --date "$TARGET" --force
"$PY" -m src.map_heat_postclose \
  --source-date "$SOURCE" --target-date "$TARGET"

bash scripts/safe_git_push.sh \
  "auto: post-close captain research [$SOURCE→$TARGET]" \
  01_daily/map_heat/ 01_daily/_transcripts/
echo "[map-postclose] complete $(TZ=America/New_York date '+%F %H:%M %Z')"
