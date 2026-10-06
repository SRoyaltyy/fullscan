#!/usr/bin/env bash
# Weekday 09:12 America/New_York backstop for the sealed-h1 Webull paper batch.
#
# Hosted cron is not a punctual clock. This runs on the ECS box
# (ecs-openclaw, /home/gha), started by fullscan-paper-backstop.timer.
# It does not touch the chat gateway.
#
# 1. Record the actual start time.
# 2. If today's h1 plan is sealed on origin/main and no sandbox batch
#    has been attempted (submit journal / attempt status), run
#    `python -m src.paper_open --submit --owner actions`.
#    The dormant 08:15 unit passes owner ecs and still no-ops, because
#    00_grounding/paper_open_owner.json is actions. This backstop must
#    pass owner actions or the gate skips the send.
# 3. Always commit data/paper_open/<date>_backstop.json (start, action, result).
#    A start after 09:25 ET still writes that file with late=true.
#
# Paper/sandbox only. paper_open refuses a non-sandbox host, keeps the
# 09:30 deadline on this path (bell wait, not a standing order), and will not
# resend once the journal exists. Before it places anything it queries the
# sandbox open and filled book. A matching client_order_id (or the same
# symbol and side) is not sent again. A failed query places nothing.
set -euo pipefail

ROOT="${FULLSCAN_ROOT:-/home/gha/fullscan}"
ENV_PAPER="${FULLSCAN_PAPER_ENV:-/home/gha/.fullscan-paper.env}"
ENV_GH="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
LOG_REL="research/hot_n4_clean_v4/forward_h1/h1_log.jsonl"
PY="${PAPER_BACKSTOP_PYTHON:-}"

export HOME="${HOME:-/home/gha}"
export FULLSCAN_HOME="${FULLSCAN_HOME:-/home/gha}"
export TZ="${TZ:-America/New_York}"
export PYTHONUNBUFFERED=1
export GIT_TERMINAL_PROMPT=0

FINALIZED=0
ACTION="pending"
RESULT="started"
LATE="false"
SEALED="false"
SUBMITTED="false"
NOTE=""
FOUND_JSON="[]"
DAY=""
START_ISO=""
RC=0

if [ -z "$PY" ]; then
  if [ -x /home/gha/fullscan-paper-open/.venv/bin/python ]; then
    PY=/home/gha/fullscan-paper-open/.venv/bin/python
  elif [ -x "$ROOT/.venv/bin/python" ]; then
    PY="$ROOT/.venv/bin/python"
  else
    PY=python3
  fi
fi

cd "$ROOT"

git config --global --add safe.directory "$ROOT" || true
git config --global --add safe.directory /home/gha/fullscan || true
git config --local --add safe.directory "$ROOT" || true
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true

# Secrets stay in the environment. Never echo them.
load_env() {
  local file="$1"
  if [ -f "$file" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$file"
    set +a
  fi
}
load_env "$ENV_GH"
load_env "$ENV_PAPER"

if [ -n "${GITHUB_TOKEN:-}" ]; then
  git config --local http.https://github.com/.extraheader \
    "AUTHORIZATION: bearer ${GITHUB_TOKEN}" || true
fi

START_ISO="$("$PY" - <<'PY'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("America/New_York")).isoformat())
PY
)"
DAY="${START_ISO%%T*}"
echo "[paper-backstop] start $START_ISO root=$ROOT py=$PY"

finalize() {
  local status="${1:-$?}"
  if [ "$FINALIZED" = 1 ]; then
    return 0
  fi
  FINALIZED=1
  local out="$ROOT/data/paper_open/${DAY}_backstop.json"
  mkdir -p "$ROOT/data/paper_open"
  "$PY" -m src.paper_backstop record \
    --out "$out" \
    --started-at "$START_ISO" \
    --action "$ACTION" \
    --result "$RESULT" \
    --late "$LATE" \
    --sealed "$SEALED" \
    --submitted "$SUBMITTED" \
    --note "$NOTE" \
    --found "$FOUND_JSON" || echo "[paper-backstop] WARN: could not write $out"
  echo "[paper-backstop] wrote $out action=$ACTION result=$RESULT late=$LATE"
  if [ -x "$ROOT/scripts/safe_git_push.sh" ] || [ -f "$ROOT/scripts/safe_git_push.sh" ]; then
    bash "$ROOT/scripts/safe_git_push.sh" \
      "auto: paper backstop ${DAY}" \
      "data/paper_open/${DAY}_backstop.json" \
      "data/paper_open/${DAY}_submit.json" \
      "data/paper_open/${DAY}_status.json" \
      "data/sleeve_merge/webull_last.json" \
      || echo "[paper-backstop] WARN: push failed"
  else
    echo "[paper-backstop] WARN: safe_git_push.sh missing — file is local only"
  fi
  exit "$status"
}
TMP=""
cleanup() {
  if [ -n "${TMP:-}" ] && [ -d "$TMP" ]; then
    rm -rf "$TMP"
  fi
}
trap 'status=$?; cleanup || true; finalize "$status"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

DOW="$(TZ=America/New_York date +%u)"
if [ "$DOW" -ge 6 ]; then
  ACTION="skipped_weekend"
  RESULT="skipped_weekend"
  exit 0
fi

TMP="$(mktemp -d)"

if ! git fetch origin main; then
  ACTION="refused"
  RESULT="fetch_failed"
  NOTE="git fetch origin main failed"
  exit 1
fi

git show "origin/main:${LOG_REL}" > "$TMP/h1_log.jsonl" || {
  ACTION="refused"
  RESULT="log_missing"
  NOTE="sealed h1 log missing on origin/main"
  exit 1
}
JOURNAL_ARG=()
STATUS_ARG=()
if git cat-file -e "origin/main:data/paper_open/${DAY}_submit.json" 2>/dev/null; then
  git show "origin/main:data/paper_open/${DAY}_submit.json" > "$TMP/submit.json"
  JOURNAL_ARG=(--journal "$TMP/submit.json")
fi
if git cat-file -e "origin/main:data/paper_open/${DAY}_status.json" 2>/dev/null; then
  git show "origin/main:data/paper_open/${DAY}_status.json" > "$TMP/status.json"
  STATUS_ARG=(--status "$TMP/status.json")
fi

DECISION="$("$PY" -m src.paper_backstop decide \
  --started-at "$START_ISO" \
  --log "$TMP/h1_log.jsonl" \
  "${JOURNAL_ARG[@]}" \
  "${STATUS_ARG[@]}")" || {
  ACTION="refused"
  RESULT="decide_failed"
  exit 1
}
echo "[paper-backstop] decision $DECISION"

eval "$("$PY" -c '
import json, shlex, sys
rec = json.loads(sys.argv[1])

def flag(key):
    return "true" if rec.get(key) else "false"

pairs = {
    "ACTION": rec.get("action") or "",
    "RESULT": rec.get("result") or "",
    "NOTE": rec.get("note") or "",
    "LATE": flag("late"),
    "SEALED": flag("sealed"),
    "SUBMITTED": flag("submitted_before"),
}
for key, value in pairs.items():
    print("%s=%s" % (key, shlex.quote(str(value))))
' "$DECISION")"

if [ "$ACTION" != "send" ]; then
  exit 0
fi

# Bring the sealed log into the tree paper_open reads. Do not reset the repo.
git checkout origin/main -- "$LOG_REL" || {
  ACTION="refused"
  RESULT="checkout_log_failed"
  NOTE="could not checkout sealed h1 log"
  exit 1
}

if [ ! -s "$ENV_PAPER" ] && [ -z "${WEBULL_APP_KEY:-}" ]; then
  ACTION="refused"
  RESULT="missing_paper_env"
  NOTE="no sandbox credentials"
  exit 1
fi

if [ -f "$ROOT/scripts/check_paper_clock.sh" ]; then
  if ! bash "$ROOT/scripts/check_paper_clock.sh"; then
    ACTION="refused"
    RESULT="clock_unsynced"
    NOTE="paper clock is not synchronized"
    exit 1
  fi
fi

# Bell-wait path. Do not add the standing flag: after 09:30 a standing
# MARKET order would fill at the live print. paper_open records
# missed_deadline instead. Owner actions is the configured sender.
# PAPER_OPEN_SENDER=backstop is the only non-seal process allowed to
# pass --submit. A start at or after 09:30 places nothing.
# Unset anything that would point PaperAPI at the live host.
unset WEBULL_LIVE || true
export PAPER_OPEN_SENDER=backstop
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
set +e
"$PY" -m src.paper_open --submit --owner actions
RC=$?
set -e
RESULT="exit_${RC}"
STATUS_FILE="$ROOT/data/paper_open/${DAY}_status.json"
if [ -f "$STATUS_FILE" ]; then
  GOT="$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("status",""))' "$STATUS_FILE" 2>/dev/null || true)"
  if [ -n "$GOT" ]; then
    RESULT="$GOT"
  fi
  FOUND_JSON="$("$PY" -c 'import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps(d.get("found") or []))' "$STATUS_FILE" 2>/dev/null || printf '%s' '[]')"
fi
echo "[paper-backstop] paper_open rc=$RC result=$RESULT"
if [ "$RC" -ne 0 ]; then
  exit "$RC"
fi
exit 0
