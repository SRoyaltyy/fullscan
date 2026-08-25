#!/usr/bin/env bash
# Raise OpenClaw idle / run / PROVIDER timeouts to 3h. Python waiting 10800s
# is useless if the gateway still kills the turn at ~9 minutes.
#
# Live CLI on this box (run #5):
#   agents.defaults.timeoutSeconds          works
#   models.providers.xai.timeoutSeconds     the real 9-min stub knob
#   models.providers.openai/anthropic       same
#   agents.defaults.llm                     UNRECOGNIZED — do not write
#   models.providers.grok                   not a provider id (needs baseUrl)
#
# Called from install_ecs_preopen.sh AND every ecs_preopen.sh / GH ALL
# start so a missed one-shot install cannot leave Monday's cap in place.
# Never fails the caller.
set +e

# ExecStartPre=+ runs as root. `openclaw config set` would then write
# /root/.openclaw, which the gha-owned gateway never reads. Pin HOME.
export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"

TARGET="${OPENCLAW_TIMEOUT:-10800}"
echo "[openclaw-timeouts] want timeoutSeconds=${TARGET} home=$HOME uid=$(id -u)"

if command -v openclaw >/dev/null 2>&1; then
  openclaw config set agents.defaults.timeoutSeconds "$TARGET"
  openclaw config set agents.defaults.subagents.runTimeoutSeconds "$TARGET"
  # xai is the live provider id. grok is not. llm is rejected by this build.
  for id in xai openai anthropic; do
    openclaw config set "models.providers.${id}.timeoutSeconds" "$TARGET"
  done
  echo "[openclaw-timeouts] CLI set attempted"
  echo "[openclaw-timeouts] get agents.defaults.timeoutSeconds:"
  openclaw config get agents.defaults.timeoutSeconds 2>/dev/null
  echo "[openclaw-timeouts] get models.providers.xai.timeoutSeconds:"
  openclaw config get models.providers.xai.timeoutSeconds 2>/dev/null
else
  echo "[openclaw-timeouts] openclaw CLI not on PATH — patching JSON if present"
fi

COUNT_FILE="$(mktemp)"
python3 - "$TARGET" "$COUNT_FILE" <<'PY'
import json, os, sys
target = int(sys.argv[1])
count_path = sys.argv[2]
home = os.environ.get("HOME") or "/home/gha"
# Never patch /root/.openclaw — the gha gateway does not read it.
cands = [
    os.path.join(home, ".openclaw", "openclaw.json"),
    os.path.join(home, ".openclaw", "config.json"),
    os.path.join(home, ".config", "openclaw", "openclaw.json"),
    "/home/gha/.openclaw/openclaw.json",
    "/home/gha/.openclaw/config.json",
]
seen = set()
patched = 0
changed = 0
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
    llm_was = "llm" in defaults
    before = (
        defaults.get("timeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
        llm_was,
    )
    defaults["timeoutSeconds"] = target
    sub["runTimeoutSeconds"] = target
    # Strip the rejected agents.defaults.llm key if a prior patch wrote it.
    if "llm" in defaults:
        defaults.pop("llm", None)
        print(f"[openclaw-timeouts] stripped agents.defaults.llm from {path}")
    for name, prov in list(providers.items()):
        if not isinstance(prov, dict):
            providers[name] = {"timeoutSeconds": target}
            continue
        prov["timeoutSeconds"] = target
    after = (
        defaults.get("timeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
        "llm" in defaults,
    )
    if before != after:
        changed += 1
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.write("\n")
    except OSError as e:
        print(f"[openclaw-timeouts] write failed {path}: {e}")
        continue
    print(f"[openclaw-timeouts] patched {path}")
    print(f"[openclaw-timeouts]   agents.defaults.timeoutSeconds {before[0]} -> {after[0]}")
    print(f"[openclaw-timeouts]   providers {before[2]} -> {after[2]}")
    patched += 1
if not patched:
    print("[openclaw-timeouts] no openclaw JSON found to patch")
try:
    with open(count_path, "w", encoding="utf-8") as fh:
        fh.write(str(changed))
except OSError:
    pass
PY
CHANGED="$(cat "$COUNT_FILE" 2>/dev/null || echo 0)"
rm -f "$COUNT_FILE"

# Root wrote gha's config. Hand it back so the gateway can read it.
if [ "$(id -u)" -eq 0 ]; then
  chown gha:gha /home/gha/.openclaw/openclaw.json /home/gha/.openclaw/config.json \
    2>/dev/null || true
fi

# Reload the gateway only when a value actually moved. A 05:55 restart is
# cheap. An 08:20 GH last-chance restart would kill an in-flight Grok turn,
# so we only bounce if we just raised the cap.
if [ "${CHANGED:-0}" -gt 0 ] 2>/dev/null; then
  echo "[openclaw-timeouts] config changed (${CHANGED} file(s)) — reloading gateway"
  if command -v openclaw >/dev/null 2>&1; then
    openclaw gateway restart 2>/dev/null \
      || openclaw gateway reload 2>/dev/null \
      || true
  fi
  if command -v systemctl >/dev/null 2>&1; then
    systemctl reload openclaw 2>/dev/null \
      || systemctl restart openclaw 2>/dev/null \
      || systemctl restart openclaw-gateway 2>/dev/null \
      || true
  fi
else
  echo "[openclaw-timeouts] config already at ${TARGET}s — not bouncing gateway"
fi

if command -v systemctl >/dev/null 2>&1; then
  echo "[openclaw-timeouts] systemd timer:"
  systemctl is-enabled fullscan-preopen.timer 2>/dev/null \
    || echo "  fullscan-preopen.timer NOT enabled (clock is still GitHub)"
  systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
fi

exit 0
