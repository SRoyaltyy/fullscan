#!/usr/bin/env bash
# DST-correct 22:00 ET post-close map/captain research, run by systemd.
# Shared lock lives under persist (gha-owned), not /tmp (root leftover).
set -euo pipefail

ROOT="${FULLSCAN_ROOT:-/home/gha/fullscan}"
ENVF="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
PERSIST="${FULLSCAN_PERSIST:-/home/gha/fullscan-persist}"
LOCK="${MAP_POSTCLOSE_LOCK:-$PERSIST/locks/map-postclose.lock}"
PREOPEN_LOCK="${PREOPEN_LOCK:-$PERSIST/locks/preopen.lock}"
mkdir -p "$PERSIST/locks" 2>/dev/null || true

exec 9>"$LOCK"
chmod 0666 "$LOCK" 2>/dev/null || true
flock -n 9 || { echo "[map-postclose] lock held — skip"; exit 0; }

ET_HM=$((10#$(TZ=America/New_York date +%H%M)))
echo "[map-postclose] et_hm=$ET_HM uid=$(id -u)"

if [ -f "$ENVF" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENVF"
  set +a
fi
# 10800s ate the 2026-09-04 morning packet. Do not inherit that default
# from .fullscan.env — night pack must fail over at 900s.
export OPENCLAW_GATEWAY_URL="${OPENCLAW_GATEWAY_URL:-http://127.0.0.1:18789}"
if [ -z "${POSTCLOSE_LLM_TIMEOUT:-}" ]; then
  if [ -z "${OPENCLAW_TIMEOUT:-}" ] || [ "${OPENCLAW_TIMEOUT}" = "10800" ]; then
    export OPENCLAW_TIMEOUT=900
  fi
else
  export OPENCLAW_TIMEOUT="$POSTCLOSE_LLM_TIMEOUT"
fi
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
# Do not pass --date last_closed. After 16:00 ET that is today, and
# run_postclose_all --date skips night_pack_dates() — 2026-09-03
# _learnings.md would stay missing. Explicit SOURCE_DATE is the override.
FORCE_FLAG=()
if [ "${FORCE:-}" = "true" ] || [ "${FORCE:-}" = "1" ]; then
  FORCE_FLAG=(--force)
fi
if [ -n "${SOURCE_DATE:-}" ]; then
  SOURCE="$SOURCE_DATE"
  SKIP_ARGS=(--job postclose_all --date "$SOURCE")
  PC_ARGS=(--date "$SOURCE" --llm-backend "${LLM_BACKEND:-auto}")
else
  SOURCE=$("$PY" -c "from src.skip_if_good import last_closed_session; print(last_closed_session())")
  SKIP_ARGS=(--job postclose_all)
  PC_ARGS=(--llm-backend "${LLM_BACKEND:-auto}")
fi
if [ -n "${TARGET_DATE:-}" ]; then
  TARGET="$TARGET_DATE"
else
  TARGET=$("$PY" -c "from src.map_heat_postclose import next_weekday; print(next_weekday('$SOURCE'))")
fi
echo "[map-postclose] POST-CLOSE ALL last_closed=$SOURCE OPENCLAW_TIMEOUT=$OPENCLAW_TIMEOUT force=${FORCE:-false} dated=${SOURCE_DATE:-night_pack_dates}"

if [ "${FORCE:-}" != "true" ] && [ "${FORCE:-}" != "1" ]; then
  if "$PY" -m src.skip_if_good "${SKIP_ARGS[@]}"; then
    echo "[map-postclose] night pack already on disk — skip"
    exit 0
  fi
fi
# GH Post-Close ALL first existed 2026-09-04. A leftover morning lock
# used to skip 22:00 and dated _learnings.md never landed. If the night
# pack is still missing, run anyway and warn.
if [ -e "$PREOPEN_LOCK" ] && ! flock -n "$PREOPEN_LOCK" -c true 2>/dev/null; then
  echo "[map-postclose] WARN: preopen lock held — night pack still missing, running anyway"
fi
if command -v systemctl >/dev/null 2>&1 \
   && systemctl is-active --quiet fullscan-preopen.service; then
  echo "[map-postclose] WARN: preopen service active — night pack still missing, running anyway"
fi

# Aliyun Cloudflare-blocks Elite HTML. Never scrape here.
export FINVIZ_SKIP_LIVE=1
export LLM_BACKEND="${LLM_BACKEND:-auto}"
PC_ARGS+=("${FORCE_FLAG[@]}")
set +e
"$PY" -m src.run_postclose_all "${PC_ARGS[@]}"
code=$?
set -e

mkdir -p "$PERSIST/01_daily/map_heat" "$PERSIST/01_daily/_transcripts" \
  "$PERSIST/01_daily/general" "$PERSIST/01_daily/sectors"
cp -a "$ROOT/01_daily/map_heat/." "$PERSIST/01_daily/map_heat/" 2>/dev/null || true
cp -a "$ROOT/01_daily/_transcripts/." "$PERSIST/01_daily/_transcripts/" 2>/dev/null || true
echo "[map-postclose] persist snapshot → $PERSIST (python exit=$code)"

bash scripts/safe_git_push.sh \
  "auto: post-close ALL [$SOURCE→$TARGET]" \
  01_daily/general/ 01_daily/sectors/ 01_daily/map_heat/ \
  01_daily/news/ 01_daily/_transcripts/ 01_daily/_channel1/ \
  01_daily/*_learnings.md \
  02_lessons/ 03_scoreboard/ \
  00_grounding/mutable_policy.md \
  00_grounding/book_policy.json \
  00_grounding/weather_rules_proposals.json
echo "[map-postclose] complete $(TZ=America/New_York date '+%F %H:%M %Z')"
exit 0
