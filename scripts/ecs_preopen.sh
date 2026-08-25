#!/usr/bin/env bash
# Pre-Open ALL, started by the ECS clock (systemd timer). GitHub is not
# in the start path. Git is only the store we push artifacts to.
#
# Called by: systemd fullscan-preopen.service
# Manual:    sudo -u gha -H /home/gha/fullscan/scripts/ecs_preopen.sh
set -euo pipefail

ROOT="${FULLSCAN_ROOT:-/home/gha/fullscan}"
ENVF="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
LOCK="${PREOPEN_LOCK:-/tmp/fullscan-preopen.lock}"
LOG_DIR="${FULLSCAN_LOG:-/home/gha/fullscan-logs}"
mkdir -p "$LOG_DIR"

exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[ecs-preopen] lock held at $LOCK — another pre-open owns the box"
  exit 0
fi

ET_NOW=$(TZ=America/New_York date '+%F %H:%M %Z')
ET_DOW=$(TZ=America/New_York date +%u)
ET_HM=$((10#$(TZ=America/New_York date +%H%M)))
DAY=$(TZ=America/New_York date +%F)
echo "[ecs-preopen] start $ET_NOW  root=$ROOT"

if [ "$ET_DOW" -ge 6 ]; then
  echo "[ecs-preopen] weekend — skip"
  exit 0
fi

if [ -f "$ENVF" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENVF"
  set +a
else
  echo "[ecs-preopen] WARN: $ENVF missing — relying on process env"
fi

export OPENCLAW_GATEWAY_URL="${OPENCLAW_GATEWAY_URL:-http://127.0.0.1:18789}"
export OPENCLAW_TIMEOUT="${OPENCLAW_TIMEOUT:-10800}"
export FULLSCAN_PERSIST="${FULLSCAN_PERSIST:-/home/gha/fullscan-persist}"
export PYTHONUNBUFFERED=1

cd "$ROOT"
git config --global --add safe.directory "$ROOT" || true
git config --global --add safe.directory '*' || true
git config user.name "Market-Bot-Automaton"
git config user.email "bot@users.noreply.github.com"
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true

if [ -n "${GITHUB_TOKEN:-}" ]; then
  git config --local http.https://github.com/.extraheader \
    "AUTHORIZATION: bearer ${GITHUB_TOKEN}"
fi

git fetch origin main
git checkout main
git reset --hard origin/main

# Latest scripts are now on disk. Raise the gateway cap BEFORE any Grok call,
# and make sure the 05:55 timer is actually enabled.
bash "$ROOT/scripts/ensure_openclaw_timeouts.sh" || true

PY="${FULLSCAN_PYTHON:-python3}"
if [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
fi

ARGS=()
[ -n "${1:-}" ] && ARGS+=("$@")

set +e
"$PY" -m src.run_preopen_all "${ARGS[@]}"
code=$?
set -e

bash scripts/safe_git_push.sh \
  "auto: pre-open ALL (ECS) [$DAY $(TZ=America/New_York date +%H%M)]" \
  01_daily/general/ 01_daily/sectors/ 01_daily/events \
  01_daily/news/ 01_daily/_transcripts/ 01_daily/_channel1/ \
  01_daily/*_preopen_qc.json 01_daily/*_preopen_status.json \
  01_daily/*_preopen_status.md \
  01_daily/*_grok_review.json 01_daily/*_grok_review.md \
  02_lessons/ 03_scoreboard/

echo "[ecs-preopen] python exit=$code  done $(TZ=America/New_York date '+%F %H:%M %Z')"
exit "$code"
