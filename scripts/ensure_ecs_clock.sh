#!/usr/bin/env bash
# Self-heal the ECS 05:55 timer. If this job is running because GitHub is
# still the clock, install the timer so tomorrow systemd owns the start.
# Never fails the caller.
set +e

export HOME="${FULLSCAN_HOME:-/home/gha}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "[ecs-clock] repo=$ROOT uid=$(id -u) user=$(id -un) home=$HOME"

bash "$ROOT/scripts/ensure_openclaw_timeouts.sh"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ecs-clock] no systemctl on this box"
else
  if systemctl is-enabled fullscan-preopen.timer >/dev/null 2>&1; then
    echo "[ecs-clock] fullscan-preopen.timer already enabled"
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
  else
    echo "[ecs-clock] timer NOT enabled — installing so 05:55 ET is the clock"

    run_install() {
      bash "$ROOT/scripts/install_ecs_preopen.sh"
    }

    if [ "$(id -u)" -eq 0 ]; then
      run_install
    elif sudo -n true 2>/dev/null; then
      sudo -E HOME="$HOME" bash "$ROOT/scripts/install_ecs_preopen.sh"
    else
      echo "[ecs-clock] no root/sudo — cannot enable the timer from this job"
      echo "[ecs-clock] GitHub cron 09:55 UTC is the backup clock until someone runs:"
      echo "[ecs-clock]   sudo bash $ROOT/scripts/install_ecs_preopen.sh"
    fi

    if systemctl is-enabled fullscan-preopen.timer >/dev/null 2>&1; then
      echo "[ecs-clock] timer enabled"
    else
      echo "[ecs-clock] WARN: install ran but timer still not enabled"
    fi
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
  fi
fi

# Land a small file the GH commit step can push even past 09:25, so the
# GIT_DIR / dubious-ownership path is proven without rewriting the day.
OUT="${GITHUB_WORKSPACE:-$ROOT}/01_daily/_ecs_clock.md"
mkdir -p "$(dirname "$OUT")"
{
  echo "# ECS clock status"
  echo
  echo "- generated: $(date -u +%Y-%m-%dT%H:%M:%SZ) UTC / $(TZ=America/New_York date '+%F %H:%M %Z')"
  echo "- uid=$(id -u) user=$(id -un) home=$HOME"
  echo "- repo=$ROOT"
  if command -v systemctl >/dev/null 2>&1; then
    echo "- timer: $(systemctl is-enabled fullscan-preopen.timer 2>/dev/null || echo NOT_ENABLED)"
    echo "- service: $(systemctl is-active fullscan-preopen.service 2>/dev/null || echo n/a)"
  else
    echo "- timer: no-systemctl"
  fi
  if command -v openclaw >/dev/null 2>&1; then
    echo "- xai.timeoutSeconds: $(openclaw config get models.providers.xai.timeoutSeconds 2>/dev/null | tr '\n' ' ')"
    echo "- defaults.timeoutSeconds: $(openclaw config get agents.defaults.timeoutSeconds 2>/dev/null | tr '\n' ' ')"
  fi
  echo
  echo '```'
  if command -v systemctl >/dev/null 2>&1; then
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || echo "(no timer listed)"
  else
    echo "(no systemctl)"
  fi
  echo '```'
} > "$OUT"
echo "[ecs-clock] wrote $OUT"
exit 0
