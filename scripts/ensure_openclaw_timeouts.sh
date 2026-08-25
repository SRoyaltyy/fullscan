#!/usr/bin/env bash
# Raise OpenClaw idle / run / PROVIDER timeouts to 3h. Python waiting 10800s
# is useless if the gateway still kills the turn at ~9 minutes.
#
# Monday/Tuesday Grok stubs named the real knob:
#   models.providers.<id>.timeoutSeconds
# agents.defaults.timeoutSeconds alone does not extend a provider idle cap.
#
# Called from install_ecs_preopen.sh AND every ecs_preopen.sh / GH ALL
# start so a missed one-shot install cannot leave Monday's cap in place.
# Never fails the caller.
set +e

TARGET="${OPENCLAW_TIMEOUT:-10800}"
echo "[openclaw-timeouts] want timeoutSeconds/idleTimeoutSeconds=${TARGET}"

if command -v openclaw >/dev/null 2>&1; then
  openclaw config set agents.defaults.timeoutSeconds "$TARGET"
  openclaw config set agents.defaults.llm.idleTimeoutSeconds "$TARGET"
  openclaw config set agents.defaults.subagents.runTimeoutSeconds "$TARGET"
  # Provider ids we have actually seen. JSON patch below covers the rest.
  for id in xai grok openai anthropic; do
    openclaw config set "models.providers.${id}.timeoutSeconds" "$TARGET"
  done
  echo "[openclaw-timeouts] CLI set attempted"
  openclaw config get agents.defaults.timeoutSeconds 2>/dev/null
  openclaw config get agents.defaults.llm.idleTimeoutSeconds 2>/dev/null
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
cands = [
    os.path.join(home, ".openclaw", "openclaw.json"),
    os.path.join(home, ".openclaw", "config.json"),
    os.path.join(home, ".config", "openclaw", "openclaw.json"),
    "/home/gha/.openclaw/openclaw.json",
    "/home/gha/.openclaw/config.json",
    "/root/.openclaw/openclaw.json",
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
    llm = defaults.setdefault("llm", {})
    sub = defaults.setdefault("subagents", {})
    models = data.setdefault("models", {})
    providers = models.setdefault("providers", {})
    before = (
        defaults.get("timeoutSeconds"),
        llm.get("idleTimeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
    )
    defaults["timeoutSeconds"] = target
    llm["idleTimeoutSeconds"] = target
    sub["runTimeoutSeconds"] = target
    if not providers:
        providers["xai"] = {}
    for name, prov in list(providers.items()):
        if not isinstance(prov, dict):
            providers[name] = {"timeoutSeconds": target}
            continue
        prov["timeoutSeconds"] = target
    after = (
        defaults.get("timeoutSeconds"),
        llm.get("idleTimeoutSeconds"),
        sub.get("runTimeoutSeconds"),
        {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
         for k, v in providers.items()},
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
    print(f"[openclaw-timeouts]   llm.idleTimeoutSeconds {before[1]} -> {after[1]}")
    print(f"[openclaw-timeouts]   providers {before[3]} -> {after[3]}")
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
