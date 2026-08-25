#!/usr/bin/env bash
# Raise OpenClaw PROVIDER timeouts to 3h. Python waiting 10800s is useless
# if the gateway still kills the turn at ~9 minutes.
#
# Live CLI on this box (run #5 / #6 / #7):
#   models.providers.xai.timeoutSeconds     the real 9-min stub knob = 10800
#   agents.defaults.timeoutSeconds          10800
#   agents.defaults.llm                     LEGACY — strip, never write
#
# Run #6 bounce killed 18789. `openclaw gateway start` as gha prints
# "Gateway service disabled" and GH kills nohup orphans. Use systemd-run
# so the gateway outlives the Actions job.
set +e

export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"
TARGET="${OPENCLAW_TIMEOUT:-10800}"
GHA_USER="${FULLSCAN_USER:-gha}"

echo "[openclaw-timeouts] want timeoutSeconds=${TARGET} home=$HOME uid=$(id -u)"

as_gha() {
  if [ "$(id -u)" -eq 0 ]; then
    sudo -u "$GHA_USER" -H env HOME="$HOME" "$@"
  else
    env HOME="$HOME" "$@"
  fi
}

COUNT_FILE="$(mktemp)"
python3 - "$TARGET" "$COUNT_FILE" <<'PY'
import json, os, sys
target = int(sys.argv[1])
count_path = sys.argv[2]
home = os.environ.get("HOME") or "/home/gha"
cands = [
    os.path.join(home, ".openclaw", "openclaw.json"),
    os.path.join(home, ".openclaw", "config.json"),
    os.path.join(home, ".config", "openclaw", "openclaw.json"),
    "/home/gha/.openclaw/openclaw.json",
    "/home/gha/.openclaw/config.json",
]
seen = set()
patched = 0
timeout_changed = 0
for path in cands:
    if not path or path in seen or not os.path.isfile(path):
        continue
    seen.add(path)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as e:
        print(f"[openclaw-timeouts] skip {path}: {e}")
        continue
    agents = data.setdefault("agents", {})
    defaults = agents.setdefault("defaults", {})
    sub = defaults.setdefault("subagents", {})
    models = data.setdefault("models", {})
    providers = models.setdefault("providers", {})
    before_to = (
        defaults.get("timeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
    )
    defaults["timeoutSeconds"] = target
    sub["runTimeoutSeconds"] = target
    if "llm" in defaults:
        defaults.pop("llm", None)
        print(f"[openclaw-timeouts] stripped agents.defaults.llm from {path}")
    for name, prov in list(providers.items()):
        if not isinstance(prov, dict):
            providers[name] = {"timeoutSeconds": target}
            continue
        prov["timeoutSeconds"] = target
    after_to = (
        defaults.get("timeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
    )
    if before_to != after_to:
        timeout_changed += 1
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.write("\n")
    except OSError as e:
        print(f"[openclaw-timeouts] write failed {path}: {e}")
        continue
    print(f"[openclaw-timeouts] patched {path}")
    print(f"[openclaw-timeouts]   agents.defaults.timeoutSeconds {before_to[0]} -> {after_to[0]}")
    print(f"[openclaw-timeouts]   providers {before_to[2]} -> {after_to[2]}")
    patched += 1
if not patched:
    print("[openclaw-timeouts] no openclaw JSON found to patch")
try:
    with open(count_path, "w", encoding="utf-8") as fh:
        fh.write(str(timeout_changed))
except OSError:
    pass
PY
CHANGED="$(cat "$COUNT_FILE" 2>/dev/null || echo 0)"
rm -f "$COUNT_FILE"

if [ "$(id -u)" -eq 0 ] && [ -d /home/gha/.openclaw ]; then
  chown -R "$GHA_USER:$GHA_USER" /home/gha/.openclaw 2>/dev/null || true
fi

if command -v openclaw >/dev/null 2>&1; then
  as_gha openclaw config set agents.defaults.timeoutSeconds "$TARGET"
  as_gha openclaw config set agents.defaults.subagents.runTimeoutSeconds "$TARGET"
  for id in xai openai anthropic; do
    as_gha openclaw config set "models.providers.${id}.timeoutSeconds" "$TARGET"
  done
  echo "[openclaw-timeouts] CLI set attempted (as ${GHA_USER})"
  echo "[openclaw-timeouts] get models.providers.xai.timeoutSeconds:"
  as_gha openclaw config get models.providers.xai.timeoutSeconds
  echo "[openclaw-timeouts] get agents.defaults.timeoutSeconds:"
  as_gha openclaw config get agents.defaults.timeoutSeconds
else
  echo "[openclaw-timeouts] openclaw CLI not on PATH"
fi

port_up() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -q ':18789'
    return $?
  fi
  if command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | grep -q ':18789'
    return $?
  fi
  return 1
}

start_gateway() {
  echo "[openclaw-timeouts] starting gateway via linger + systemd-run"
  if [ "$(id -u)" -eq 0 ]; then
    loginctl enable-linger "$GHA_USER" 2>/dev/null \
      && echo "[openclaw-timeouts] linger enabled for $GHA_USER"
  fi
  if command -v systemctl >/dev/null 2>&1; then
    echo "[openclaw-timeouts] openclaw-related units:"
    systemctl list-units --all --no-pager --full '*openclaw*' '*claw*' 2>/dev/null | head -40
    systemctl list-unit-files --no-pager '*openclaw*' 2>/dev/null | head -20
  fi
  # Do NOT `openclaw gateway install` — run #8 auto-generated a token.
  if [ "$(id -u)" -eq 0 ] && command -v systemd-run >/dev/null 2>&1 && command -v openclaw >/dev/null 2>&1; then
    OC="$(command -v openclaw)"
    systemctl reset-failed fullscan-openclaw-gateway 2>/dev/null || true
    if systemctl is-active --quiet fullscan-openclaw-gateway 2>/dev/null; then
      echo "[openclaw-timeouts] fullscan-openclaw-gateway already active"
    else
      systemctl stop fullscan-openclaw-gateway 2>/dev/null || true
      systemd-run --uid="$GHA_USER" --gid="$GHA_USER" \
        --working-directory=/home/gha \
        --unit=fullscan-openclaw-gateway \
        --property=Restart=always \
        --property=RestartSec=5 \
        -E HOME=/home/gha -E USER=gha \
        "$OC" gateway \
        && echo "[openclaw-timeouts] systemd-run fullscan-openclaw-gateway" \
        || systemctl start fullscan-openclaw-gateway
    fi
    sleep 12
    echo "[openclaw-timeouts] unit status:"
    systemctl status fullscan-openclaw-gateway --no-pager -l 2>/dev/null | head -40
    echo "[openclaw-timeouts] unit journal:"
    journalctl -u fullscan-openclaw-gateway -n 50 --no-pager 2>/dev/null
  fi
}

if [ "${CHANGED:-0}" -gt 0 ] 2>/dev/null; then
  echo "[openclaw-timeouts] timeout values changed (${CHANGED}) — reloading gateway"
  as_gha openclaw gateway restart || as_gha openclaw gateway reload || true
  sleep 2
else
  echo "[openclaw-timeouts] timeout values already ${TARGET}s — not bouncing gateway"
fi

if port_up; then
  echo "[openclaw-timeouts] gateway port 18789 is up"
else
  echo "[openclaw-timeouts] gateway port 18789 is DOWN — starting"
  start_gateway
  if port_up; then
    echo "[openclaw-timeouts] gateway port 18789 is up after start"
  else
    echo "[openclaw-timeouts] WARN: gateway port 18789 still down"
  fi
fi

if command -v systemctl >/dev/null 2>&1; then
  echo "[openclaw-timeouts] systemd timer:"
  systemctl is-enabled fullscan-preopen.timer 2>/dev/null \
    || echo "  fullscan-preopen.timer NOT enabled (clock is still GitHub)"
  systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
fi

exit 0
