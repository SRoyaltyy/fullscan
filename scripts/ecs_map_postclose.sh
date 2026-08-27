#!/usr/bin/env bash
# DST-correct 22:00 ET post-close map/captain research, run by systemd.
# Preopen lock is the hard gate. Clock window is advisory so a missed
# 22:00 can still be caught up before 05:55.
set -euo pipefail

ROOT="${FULLSCAN_ROOT:-/home/gha/fullscan}"
ENVF="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
LOCK="${MAP_POSTCLOSE_LOCK:-/tmp/fullscan-map-postclose.lock}"
PREOPEN_LOCK="${PREOPEN_LOCK:-/tmp/fullscan-preopen.lock}"
PERSIST="${FULLSCAN_PERSIST:-/home/gha/fullscan-persist}"
FALLBACK="$PERSIST/locks/map-postclose.lock"
mkdir -p "$PERSIST/locks" 2>/dev/null || true

if [ -e "$LOCK" ] && [ ! -w "$LOCK" ]; then
  chmod 0666 "$LOCK" 2>/dev/null || LOCK="$FALLBACK"
fi
exec 9>"$LOCK"
chmod 0666 "$LOCK" 2>/dev/null || true
flock -n 9 || { echo "[map-postclose] lock held — skip"; exit 0; }

ET_HM=$((10#$(TZ=America/New_York date +%H%M)))
# Preferred window: 22:00–04:29 ET. Do not start if preopen owns the box.
# Clock abort only when preopen lock is held; otherwise catch-up is allowed.
if [ "$ET_HM" -ge 430 ] && [ "$ET_HM" -lt 2200 ]; then
  echo "[map-postclose] outside 22:00–04:29 ET (et_hm=$ET_HM) — catch-up if preopen free"
fi

if ! flock -n "$PREOPEN_LOCK" -c true; then
  echo "[map-postclose] preopen lock held — will not git reset or run"
  exit 1
fi

if [ -f "$ENVF" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENVF"
  set +a
fi
# 30 minutes per Grok batch. 11 sectors + retries fit in the 6h unit cap.
export OPENCLAW_GATEWAY_URL="${OPENCLAW_GATEWAY_URL:-http://127.0.0.1:18789}"
export OPENCLAW_TIMEOUT="${OPENCLAW_TIMEOUT:-1800}"
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

if ! flock -n "$PREOPEN_LOCK" -c true; then
  echo "[map-postclose] preopen grabbed the lock before reset — abort"
  exit 1
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
echo "[map-postclose] source=$SOURCE target=$TARGET OPENCLAW_TIMEOUT=$OPENCLAW_TIMEOUT"

if ! flock -n "$PREOPEN_LOCK" -c true; then
  echo "[map-postclose] preopen grabbed the lock before scrape — abort"
  exit 1
fi

"$PY" -m src.map_heat --date "$TARGET" --force
"$PY" -m src.map_heat_postclose \
  --source-date "$SOURCE" --target-date "$TARGET"

mkdir -p "$PERSIST/01_daily/map_heat" "$PERSIST/01_daily/_transcripts"
cp -a "$ROOT/01_daily/map_heat/." "$PERSIST/01_daily/map_heat/" 2>/dev/null || true
cp -a "$ROOT/01_daily/_transcripts/." "$PERSIST/01_daily/_transcripts/" 2>/dev/null || true
echo "[map-postclose] persist snapshot → $PERSIST/01_daily/map_heat"

bash scripts/safe_git_push.sh \
  "auto: post-close captain research [$SOURCE→$TARGET]" \
  01_daily/map_heat/ 01_daily/_transcripts/
echo "[map-postclose] complete $(TZ=America/New_York date '+%F %H:%M %Z')"
