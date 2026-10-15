#!/usr/bin/env bash
# Explicit installation only. Isolated checkout; never reset the running preopen repository.
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then echo 'Run this installer as root'; exit 1; fi
SOURCE="$(cd "$(dirname "$0")/.." && pwd)"
TARGET=/home/gha/fullscan-paper-open
ENVFILE=/home/gha/.fullscan-paper.env
if [ ! -s "$ENVFILE" ]; then
  echo "Missing $ENVFILE (WEBULL_APP_KEY, WEBULL_APP_SECRET, optional WEBULL_ACCOUNT_ID)."
  exit 1
fi
if [ ! -d "$TARGET/.git" ]; then
  sudo -u gha git clone --depth=1 --filter=blob:none --sparse https://github.com/SRoyaltyy/fullscan.git "$TARGET"
fi
sudo -u gha git -C "$TARGET" sparse-checkout set src 00_grounding scripts
# Update only this dedicated service checkout while the service is stopped.
if systemctl is-active --quiet fullscan-paper-open.service; then
  echo 'Service is active; defer upgrade until after the opening batch.'; exit 1
fi
sudo -u gha git -C "$TARGET" fetch --depth=1 origin main
# Shallow (--depth=1) checkout: each fetch replaces the graft, so the old tip
# looks unrelated to the new one and `merge --ff-only` fails ("refusing to
# merge unrelated histories"). This checkout is code-only (sparse: src,
# 00_grounding, scripts); the only expected local edits are the three files
# this script installs below. Refuse if anything else changed, else move to
# the fetched tip. Untracked files are left alone.
DIRTY=$(sudo -u gha git -C "$TARGET" status --porcelain --untracked-files=no \
  | awk '{print $NF}' \
  | grep -vxE 'scripts/check_paper_clock.sh|src/paper_open.py|src/webull_exec.py' || true)
if [ -n "$DIRTY" ]; then
  echo "Unexpected local changes in $TARGET; not updating:"; echo "$DIRTY"; exit 1
fi
# Back up untracked/ignored files (gateway config, Grok/xAI login files,
# systemd units, env files) before moving HEAD. No git clean, no re-clone.
BACKUP=/home/gha/fullscan-paper-open-backup-$(date -u +%Y%m%dT%H%M%SZ).tgz
( cd "$TARGET" && sudo -u gha git ls-files -z --others ) \
  | ( cd "$TARGET" && tar --null -czf "$BACKUP" --files-from=- ) || true
mkdir -p "${BACKUP%.tgz}-units"
cp -a /etc/systemd/system/fullscan-paper-open.* "${BACKUP%.tgz}-units/" 2>/dev/null || true
cp -a "$ENVFILE" "${BACKUP%.tgz}.env" && chmod 600 "${BACKUP%.tgz}.env"
chmod 600 "$BACKUP" 2>/dev/null || true
echo "Backed up untracked files to $BACKUP"
sudo -u gha git -C "$TARGET" checkout -B main FETCH_HEAD
sudo -u gha git -C "$TARGET" reset --hard FETCH_HEAD
bash "$SOURCE/scripts/check_paper_clock.sh"
sudo -u gha python3 -m venv "$TARGET/.venv"
sudo -u gha "$TARGET/.venv/bin/pip" install webull-openapi-python-sdk pandas numpy pyarrow yfinance requests
# Install the exact reviewed sources from the deployment checkout.
install -o gha -g gha -m 0644 "$SOURCE/scripts/check_paper_clock.sh" "$TARGET/scripts/check_paper_clock.sh"
install -o gha -g gha -m 0644 "$SOURCE/src/paper_open.py" "$TARGET/src/paper_open.py"
install -o gha -g gha -m 0644 "$SOURCE/src/webull_exec.py" "$TARGET/src/webull_exec.py"
install -m 0644 "$SOURCE/scripts/systemd/fullscan-paper-open.service" /etc/systemd/system/
install -m 0644 "$SOURCE/scripts/systemd/fullscan-paper-open.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now fullscan-paper-open.timer
systemctl is-enabled fullscan-paper-open.timer
systemctl list-timers fullscan-paper-open.timer --no-pager
