#!/usr/bin/env bash
# Install the xAI OAuth renew + gateway guard timer. Run as root on ECS:
#   sudo bash /home/gha/fullscan/scripts/install_openclaw_oauth_timer.sh
# Touches only the two fullscan-openclaw-oauth units. No books, no engine.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "run as root"; exit 1; }
SRC="$(cd "$(dirname "$0")" && pwd)"
mkdir -p /home/gha/fullscan-logs && chown gha:gha /home/gha/fullscan-logs
install -m 0644 "$SRC/systemd/fullscan-openclaw-oauth.service" /etc/systemd/system/
install -m 0644 "$SRC/systemd/fullscan-openclaw-oauth.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now fullscan-openclaw-oauth.timer
systemctl is-enabled fullscan-openclaw-oauth.timer
systemctl list-timers --all fullscan-openclaw-oauth.timer
