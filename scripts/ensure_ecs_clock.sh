#!/usr/bin/env bash
# Self-heal the ECS 05:55 timer. If this job is running because GitHub is
# still the clock, install the timer so tomorrow systemd owns the start.
# Never fails the caller.
set +e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "[ecs-clock] repo=$ROOT uid=$(id -u) user=$(id -un)"

bash "$ROOT/scripts/ensure_openclaw_timeouts.sh"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ecs-clock] no systemctl on this box"
  exit 0
fi

if systemctl is-enabled fullscan-preopen.timer >/dev/null 2>&1; then
  echo "[ecs-clock] fullscan-preopen.timer already enabled"
  systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
  exit 0
fi

echo "[ecs-clock] timer NOT enabled — installing so 05:55 ET is the clock"

run_install() {
  bash "$ROOT/scripts/install_ecs_preopen.sh"
}

if [ "$(id -u)" -eq 0 ]; then
  run_install
elif sudo -n true 2>/dev/null; then
  sudo bash "$ROOT/scripts/install_ecs_preopen.sh"
else
  echo "[ecs-clock] no root/sudo — cannot enable the timer from this job"
  echo "[ecs-clock] run once on the box: sudo bash $ROOT/scripts/install_ecs_preopen.sh"
  exit 0
fi

systemctl is-enabled fullscan-preopen.timer 2>/dev/null \
  && echo "[ecs-clock] timer enabled" \
  || echo "[ecs-clock] WARN: install ran but timer still not enabled"
systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
exit 0
