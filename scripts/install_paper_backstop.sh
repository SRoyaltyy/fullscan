#!/usr/bin/env bash
# Install the 09:12 ET sealed-h1 paper backstop on the ECS box.
# Idempotent. Does not upgrade the chat gateway and does not restart it.
# Does not start the oneshot service (that would send). Enables the timer only.
#
# Same shape as scripts/ensure_ecs_clock.sh: if this host has no systemd,
# exit 0 so an ubuntu job can call it. PAPER_BACKSTOP_STRICT=1 (the ECS
# install workflow) fails when the timer cannot be enabled.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STRICT="${PAPER_BACKSTOP_STRICT:-0}"
UNIT_TIMER="fullscan-paper-backstop.timer"
UNIT_SERVICE="fullscan-paper-backstop.service"
DEST_REPO="${FULLSCAN_ROOT:-/home/gha/fullscan}"

echo "[paper-backstop-install] repo=$ROOT uid=$(id -u) user=$(id -un) dest=$DEST_REPO"

fail() {
  echo "[paper-backstop-install] $*"
  if [ "$STRICT" = 1 ]; then
    exit 1
  fi
  exit 0
}

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[paper-backstop-install] no systemctl — skip (not the ECS box)"
  exit 0
fi

run_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
    return $?
  fi
  if sudo -n true 2>/dev/null; then
    sudo -n "$@"
    return $?
  fi
  return 1
}

if ! run_root bash -c "install -m 0644 '$ROOT/scripts/systemd/$UNIT_SERVICE' /etc/systemd/system/ && install -m 0644 '$ROOT/scripts/systemd/$UNIT_TIMER' /etc/systemd/system/"; then
  fail "no root/sudo — cannot install $UNIT_TIMER"
fi

# The timer's ExecStart path is /home/gha/fullscan. A GitHub checkout
# lives elsewhere; copy the runner script into that tree when needed.
# Do not git-reset the tree and do not start OpenClaw.
if [ -d "$DEST_REPO" ] && [ "$DEST_REPO" != "$ROOT" ]; then
  mkdir -p "$DEST_REPO/scripts" "$DEST_REPO/src" || true
  if run_root install -o gha -g gha -m 0755 \
      "$ROOT/scripts/ecs_paper_backstop.sh" \
      "$DEST_REPO/scripts/ecs_paper_backstop.sh" \
    && run_root install -o gha -g gha -m 0644 \
      "$ROOT/src/paper_backstop.py" \
      "$DEST_REPO/src/paper_backstop.py"; then
    echo "[paper-backstop-install] copied runner into $DEST_REPO"
  else
    # Runner may already own the tree (no root needed).
    cp "$ROOT/scripts/ecs_paper_backstop.sh" "$DEST_REPO/scripts/ecs_paper_backstop.sh" \
      && cp "$ROOT/src/paper_backstop.py" "$DEST_REPO/src/paper_backstop.py" \
      && echo "[paper-backstop-install] copied runner into $DEST_REPO (user)" \
      || fail "could not copy runner into $DEST_REPO"
  fi
else
  chmod +x "$ROOT/scripts/ecs_paper_backstop.sh" || true
  echo "[paper-backstop-install] runner already at $ROOT"
fi

if ! run_root systemctl daemon-reload; then
  fail "daemon-reload failed"
fi
# Enable and start the timer unit only. Never `systemctl start` the service.
if ! run_root systemctl enable "$UNIT_TIMER"; then
  fail "enable $UNIT_TIMER failed"
fi
if ! run_root systemctl start "$UNIT_TIMER"; then
  fail "start $UNIT_TIMER failed"
fi

echo "[paper-backstop-install] enabled=$(systemctl is-enabled "$UNIT_TIMER" 2>/dev/null || echo NOT_ENABLED)"
echo "[paper-backstop-install] service=$(systemctl is-active "$UNIT_SERVICE" 2>/dev/null || echo inactive)"
echo "[paper-backstop-install] systemctl list-timers:"
systemctl list-timers --all "$UNIT_TIMER" --no-pager || true
echo "[paper-backstop-install] timer properties:"
systemctl show "$UNIT_TIMER" \
  -p Id -p Unit -p TimersCalendar -p NextElapseUSecRealtime -p Persistent -p ActiveState \
  --no-pager || true

if ! systemctl is-enabled "$UNIT_TIMER" >/dev/null 2>&1; then
  fail "timer still not enabled"
fi
# The oneshot must not be running just because we installed the clock.
if systemctl is-active --quiet "$UNIT_SERVICE"; then
  echo "[paper-backstop-install] WARN: $UNIT_SERVICE is active (timer elapse, not this installer)"
fi
exit 0
