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

# ExecStartPre=+ runs as root. `openclaw config set` would then write
# /root/.openclaw, which the gha-owned gateway never reads. Pin HOME.
export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"

TARGET="${OPENCLAW_TIMEOUT:-10800}"
echo "[openclaw-timeouts] want timeoutSeconds/idleTimeoutSeconds=${TARGET} home=$HOME uid=$(id -u)"

# ONLY keys OpenClaw's config validator accepts (2026-08-25 run 32871268606
# proved the others are rejected):
#   agents.defaults: Unrecognized key: "llm"          → no llm.* here
#   models.providers.grok.baseUrl: custom model providers must declare
#     baseUrl                                          → only bundled `xai`
# Writing rejected keys is worse than a short timeout: if they land in
# openclaw.json, the gateway can refuse the whole config on restart and
# every stage silently runs on the DeepSeek spare tire.
if command -v openclaw >/dev/null 2>&1; then
  openclaw config set agents.defaults.timeoutSeconds "$TARGET"
  openclaw config set models.providers.xai.timeoutSeconds "$TARGET"
  echo "[openclaw-timeouts] CLI set attempted (agents.defaults + providers.xai)"
  openclaw config get agents.defaults.timeoutSeconds 2>/dev/null
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
    models = data.setdefault("models", {})
    providers = models.setdefault("providers", {})

    def snap():
        return (
            defaults.get("timeoutSeconds"),
            {k: (v.get("timeoutSeconds") if isinstance(v, dict) else None)
             for k, v in providers.items()},
            "llm" in defaults, "subagents" in defaults,
        )

    before = snap()

    # --- set the two VALID keys ---
    defaults["timeoutSeconds"] = target
    xai = providers.get("xai")
    if isinstance(xai, dict):
        xai["timeoutSeconds"] = target
    # Do NOT invent provider entries: a providers.<id> block without
    # baseUrl fails validation for non-bundled ids. If xai is absent the
    # gateway is using bundled defaults; the CLI path above handles it.

    # --- self-heal: REMOVE the invalid keys an earlier version of this
    # script wrote (they can make the gateway reject the whole file) ---
    llm = defaults.get("llm")
    if isinstance(llm, dict):
        llm.pop("idleTimeoutSeconds", None)
        if not llm:
            defaults.pop("llm", None)
            print(f"[openclaw-timeouts] removed invalid agents.defaults.llm from {path}")
    sub = defaults.get("subagents")
    if isinstance(sub, dict):
        sub.pop("runTimeoutSeconds", None)
        if not sub:
            defaults.pop("subagents", None)
            print(f"[openclaw-timeouts] removed invalid agents.defaults.subagents from {path}")
    for name in list(providers.keys()):
        prov = providers[name]
        # Drop provider blocks WE invented: exactly {timeoutSeconds} and no
        # baseUrl. Never touch a block with real user config.
        if (name != "xai" and isinstance(prov, dict)
                and set(prov.keys()) <= {"timeoutSeconds"}):
            providers.pop(name)
            print(f"[openclaw-timeouts] removed invented providers.{name} from {path}")
    if not providers:
        models.pop("providers", None)

    after = snap()
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
    print(f"[openclaw-timeouts]   providers {before[1]} -> {after[1]}")
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
