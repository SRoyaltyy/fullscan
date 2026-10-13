#!/usr/bin/env bash
# Install the xAI OAuth renew + gateway guard timer. Run as root on ECS:
#   sudo bash /home/gha/fullscan/scripts/install_openclaw_oauth_timer.sh
# Deploys the guard/helper and the two OAuth units. No books, no engine.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "run as root"; exit 1; }
SRC="$(cd "$(dirname "$0")" && pwd)"
# Actions checks out under actions-runner/_work; the old unit executed a
# different /home/gha/fullscan checkout. Deploy the exact checked-in scripts
# to a stable root-owned bundle so timer runs use the reviewed version.
RUNTIME=/opt/fullscan/openclaw-oauth
test -f "$SRC/openclaw_oauth_guard.py"
test -f "$SRC/enable_openclaw_chat.sh"
install -d -m 0755 "$RUNTIME/scripts"
install -m 0644 "$SRC/openclaw_oauth_guard.py" "$RUNTIME/scripts/"
install -m 0755 "$SRC/enable_openclaw_chat.sh" "$RUNTIME/scripts/"
mkdir -p /home/gha/fullscan-logs && chown gha:gha /home/gha/fullscan-logs
install -m 0644 "$SRC/systemd/fullscan-openclaw-oauth.service" /etc/systemd/system/
install -m 0644 "$SRC/systemd/fullscan-openclaw-oauth.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now fullscan-openclaw-oauth.timer
systemctl is-enabled fullscan-openclaw-oauth.timer
systemctl list-timers --all fullscan-openclaw-oauth.timer
sha256sum "$RUNTIME/scripts/openclaw_oauth_guard.py"
