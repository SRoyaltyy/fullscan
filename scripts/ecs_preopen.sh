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
DAY="${RUN_DATE:-$(TZ=America/New_York date +%F)}"
echo "[ecs-preopen] start $ET_NOW  root=$ROOT day=$DAY force=${FORCE:-false}"

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
export FULLSCAN_HOME="${FULLSCAN_HOME:-/home/gha}"
export PYTHONUNBUFFERED=1

write_clock() {
  mkdir -p "$ROOT/01_daily"
  {
    echo "# ECS clock status"
    echo
    echo "- generated: $(date -u +%Y-%m-%dT%H:%M:%SZ) UTC / $(TZ=America/New_York date '+%F %H:%M %Z')"
    echo "- source: ecs_preopen.sh"
    echo "- timer: $(systemctl is-enabled fullscan-preopen.timer 2>/dev/null || echo NOT_ENABLED)"
    echo
    echo '```'
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || echo "(no timer)"
    echo '```'
  } > "$ROOT/01_daily/_ecs_clock.md"
  echo "[ecs-preopen] wrote $ROOT/01_daily/_ecs_clock.md"
}

git_prep() {
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
}

# Persistent=true catch-up after a late enable must NOT rewrite today.
# Timer is enabled for the next 05:55. Prove git push with the clock file.
# Health healer may set FORCE=true to ignore the 09:25 cutoff.
if [ "$ET_HM" -ge 925 ] && [ "${FORCE:-false}" != "true" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "[ecs-preopen] past 09:25 ET — not running python, not resetting the tree"
  write_clock
  git_prep
  bash "$ROOT/scripts/safe_git_push.sh" \
    "chore: ecs clock status (cutoff skip) [$DAY $(TZ=America/New_York date +%H%M)]" \
    01_daily/_ecs_clock.md || true
  exit 0
fi

git_prep
git fetch origin main
git checkout main
git reset --hard origin/main
# reset --hard restores git's 100644 mode. systemd ExecStart uses bash
# now, but keep +x so a hand-run still works.
chmod +x "$ROOT/scripts/"*.sh || true

# Latest scripts are now on disk. Raise the gateway cap BEFORE any Grok call.
bash "$ROOT/scripts/ensure_openclaw_timeouts.sh" || true

# Live Finviz HTML is GH-hosted (finviz_preopen_scrape.yml, ~05:40 ET).
# Do NOT scrape from this Aliyun box. Wait/pull digest + overlay, then Grok.
wait_finviz_scrape() {
  local wait_s="${FINVIZ_SCRAPE_WAIT:-720}"
  local deadline=$((SECONDS + wait_s))
  echo "[ecs-preopen] waiting up to ${wait_s}s for GH-hosted Finviz scrape"
  while [ "$SECONDS" -lt "$deadline" ]; do
    git fetch origin main >/dev/null 2>&1 || true
    git checkout origin/main -- \
      "01_daily/news/${DAY}_finviz_digest.json" \
      "01_daily/news/${DAY}_finviz_digest.md" \
      "01_daily/news/latest_finviz_digest.md" \
      "01_daily/map_heat/${DAY}_map_heat.json" \
      "01_daily/map_heat/${DAY}_map_heat.md" 2>/dev/null || true
    if [ -s "01_daily/news/${DAY}_finviz_digest.json" ] \
       && [ -s "01_daily/map_heat/${DAY}_map_heat.json" ]; then
      if grep -q "morning_overlay" "01_daily/map_heat/${DAY}_map_heat.json" 2>/dev/null; then
        echo "[ecs-preopen] GH Finviz scrape on disk"
        return 0
      fi
    fi
    echo "[ecs-preopen] scrape not ready — sleep 20s"
    sleep 20
  done
  echo "[ecs-preopen] WARN: GH Finviz scrape not on disk after ${wait_s}s — python QC will fail if missing"
  return 0
}
wait_finviz_scrape

PY="${FULLSCAN_PYTHON:-python3}"
if [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
fi

ARGS=()
[ -n "${RUN_DATE:-}" ] && ARGS+=(--date "$RUN_DATE")
if [ "${FORCE:-}" = "true" ] || [ "${FORCE:-}" = "1" ]; then
  ARGS+=(--force)
fi
[ -n "${1:-}" ] && ARGS+=("$@")

set +e
"$PY" -m src.run_preopen_all "${ARGS[@]}"
code=$?
set -e

write_clock || true
bash scripts/safe_git_push.sh \
  "auto: pre-open ALL (ECS) [$DAY $(TZ=America/New_York date +%H%M)]" \
  01_daily/general/ 01_daily/sectors/ 01_daily/events \
  01_daily/news/ 01_daily/map_heat/ \
  01_daily/_transcripts/ 01_daily/_channel1/ \
  01_daily/*_preopen_qc.json 01_daily/*_preopen_status.json \
  01_daily/*_preopen_status.md \
  01_daily/*_grok_review.json 01_daily/*_grok_review.md \
  01_daily/_ecs_clock.md \
  02_lessons/ 03_scoreboard/

echo "[ecs-preopen] python exit=$code  done $(TZ=America/New_York date '+%F %H:%M %Z')"
exit "$code"
