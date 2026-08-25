#!/usr/bin/env bash
# Install the ECS-native Pre-Open ALL timer. Run ONCE on the box as root:
#
#   sudo bash /home/gha/fullscan/scripts/install_ecs_preopen.sh
#
# After this, GitHub cron is not required to start the predictive job.
# GitHub remains the git remote we push artifacts to.
#
# Timer enable MUST happen before venv/pip. Run #5 died on
# `python3 -m venv` (ensurepip missing) under `set -e` and never reached
# `systemctl enable --now`. A missing venv is recoverable (python3
# fallback). A disabled timer is a missed 05:55.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root: sudo bash $0"
  exit 1
fi

GHA_USER="${FULLSCAN_USER:-gha}"
GHA_HOME="$(getent passwd "$GHA_USER" | cut -d: -f6)"
ROOT="${FULLSCAN_ROOT:-$GHA_HOME/fullscan}"
ENVF="${FULLSCAN_ENV:-$GHA_HOME/.fullscan.env}"
LOG_DIR="$GHA_HOME/fullscan-logs"
REPO_URL="https://github.com/SRoyaltyy/fullscan.git"

echo "[install] user=$GHA_USER home=$GHA_HOME root=$ROOT"

if ! id "$GHA_USER" >/dev/null 2>&1; then
  echo "user $GHA_USER does not exist"
  exit 1
fi

mkdir -p "$LOG_DIR"
chown "$GHA_USER:$GHA_USER" "$LOG_DIR"

if [ ! -d "$ROOT/.git" ]; then
  echo "[install] cloning $REPO_URL → $ROOT"
  sudo -u "$GHA_USER" git clone "$REPO_URL" "$ROOT"
else
  echo "[install] updating $ROOT"
  sudo -u "$GHA_USER" git -C "$ROOT" fetch origin main
  sudo -u "$GHA_USER" git -C "$ROOT" checkout main
  sudo -u "$GHA_USER" git -C "$ROOT" pull --ff-only origin main \
    || sudo -u "$GHA_USER" git -C "$ROOT" reset --hard origin/main
fi

chmod +x "$ROOT/scripts/ecs_preopen.sh" "$ROOT/scripts/install_ecs_preopen.sh" \
  "$ROOT/scripts/ensure_openclaw_timeouts.sh" "$ROOT/scripts/ensure_ecs_clock.sh" \
  "$ROOT/scripts/safe_git_push.sh"

if [ ! -f "$ENVF" ]; then
  cp "$ROOT/scripts/fullscan.env.example" "$ENVF"
  chown "$GHA_USER:$GHA_USER" "$ENVF"
  chmod 600 "$ENVF"
  echo ""
  echo ">>> Created $ENVF from the example."
  echo ">>> Edit it (nano $ENVF) — at minimum:"
  echo "      OPENCLAW_TOKEN     (if the gateway requires it)"
  echo "      DEEPSEEK_API_KEY   (fallback)"
  echo "      GITHUB_TOKEN       (PAT, contents:write — for git push)"
  echo "      DATABASE_URL / DATABASE_KEY / FRED_API_KEY / Finviz"
  echo ">>> Then:  sudo systemctl start fullscan-preopen.service"
  echo ""
else
  chown "$GHA_USER:$GHA_USER" "$ENVF"
  chmod 600 "$ENVF"
  echo "[install] keeping existing $ENVF"
fi

install -m 0644 "$ROOT/scripts/systemd/fullscan-preopen.service" \
  /etc/systemd/system/fullscan-preopen.service
install -m 0644 "$ROOT/scripts/systemd/fullscan-preopen.timer" \
  /etc/systemd/system/fullscan-preopen.timer

# Gateway timeouts — Python waiting 3h is useless if OpenClaw dies at ~9 min.
if [ -x "$ROOT/scripts/ensure_openclaw_timeouts.sh" ]; then
  sudo -u "$GHA_USER" -H bash "$ROOT/scripts/ensure_openclaw_timeouts.sh" || true
else
  echo "[install] ensure_openclaw_timeouts.sh missing"
fi

# Enable the 05:55 clock BEFORE venv. Persistent=true will catch up if
# 05:55 already passed; ecs_preopen.sh refuses python after 09:25 ET.
systemctl daemon-reload
systemctl enable --now fullscan-preopen.timer
echo "[install] systemctl enable --now fullscan-preopen.timer done"
systemctl is-enabled fullscan-preopen.timer
systemctl list-timers --all fullscan-preopen.timer || true

# venv is nice-to-have. Run #5: ensurepip missing, set -e aborted, timer
# never enabled. Never let that happen again.
set +e
if [ ! -x "$ROOT/.venv/bin/python" ]; then
  echo "[install] creating venv"
  rm -rf "$ROOT/.venv"
  if ! sudo -u "$GHA_USER" python3 -m venv "$ROOT/.venv"; then
    echo "[install] venv failed (ensurepip?). installing python3-venv"
    apt-get update -qq
    apt-get install -y python3-venv python3.10-venv
    rm -rf "$ROOT/.venv"
    sudo -u "$GHA_USER" python3 -m venv "$ROOT/.venv" \
      || echo "[install] WARN: venv still failed; ecs_preopen.sh will use python3"
  fi
fi
if [ -x "$ROOT/.venv/bin/pip" ]; then
  echo "[install] pip (this takes a minute)"
  sudo -u "$GHA_USER" "$ROOT/.venv/bin/pip" install -q --upgrade pip
  sudo -u "$GHA_USER" "$ROOT/.venv/bin/pip" install -q -r "$ROOT/requirements.txt"
  sudo -u "$GHA_USER" "$ROOT/.venv/bin/pip" install -q \
    psycopg2-binary yfinance pandas requests openai ddgs
else
  echo "[install] no venv pip — skipping; GH job and python3 still work"
fi
set -e

echo ""
echo "[install] timer installed. Next fire:"
systemctl is-enabled fullscan-preopen.timer \
  && echo "[install] fullscan-preopen.timer ENABLED" \
  || echo "[install] WARN: timer NOT enabled"
systemctl list-timers --all fullscan-preopen.timer || true
echo ""
echo "Logs:    journalctl -u fullscan-preopen.service -f"
echo "         tail -f $LOG_DIR/preopen.log"
echo "Run now: sudo systemctl start fullscan-preopen.service"
echo ""
echo "Persistent=true — if 05:55 ET already passed today, systemd will"
echo "catch up and start the service as soon as the timer is enabled"
echo "(ecs_preopen.sh and run_preopen_all still refuse after 09:25 ET)."
