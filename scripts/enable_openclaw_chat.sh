#!/usr/bin/env bash
# Turn on the OpenClaw /v1/chat/completions classroom and prove it answers.
# Runs ON the ECS box via the self-hosted runner. Does not print tokens.
set -u
export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"
CFG="${HOME}/.openclaw/openclaw.json"
GW="http://127.0.0.1:18789"

if [ -f /home/gha/.fullscan.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /home/gha/.fullscan.env
  set +a
fi
TOKEN="${OPENCLAW_TOKEN:-}"
AGENT="${OPENCLAW_AGENT:-openclaw/default}"

echo "uid=$(id -u) user=$(id -un) cfg=$CFG"
echo "token_set=$([ -n "$TOKEN" ] && echo yes || echo no)"

if [ ! -f "$CFG" ]; then
  echo "FAIL: $CFG missing"
  exit 1
fi
cp -a "$CFG" "${CFG}.bak.$(date -u +%Y%m%dT%H%M%SZ)"

python3 - "$CFG" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
gw = data.setdefault("gateway", {})
http = gw.setdefault("http", {})
ends = http.setdefault("endpoints", {})
chat = ends.setdefault("chatCompletions", {})
before = bool(chat.get("enabled"))
chat["enabled"] = True
# Keep existing auth token if present; do not invent one.
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
print(f"chatCompletions.enabled: {before} -> True")
PY

if command -v openclaw >/dev/null 2>&1; then
  openclaw config set gateway.http.endpoints.chatCompletions.enabled true 2>&1 | tail -20 || true
fi

restart_ok=0
for unit in fullscan-openclaw-gateway openclaw-gateway; do
  if command -v systemctl >/dev/null 2>&1 && systemctl list-units --all --no-legend 2>/dev/null | grep -q "$unit"; then
    echo "restart $unit"
    if sudo -n systemctl restart "$unit" 2>/dev/null || systemctl restart "$unit" 2>/dev/null; then
      restart_ok=1
      break
    fi
  fi
done
if [ "$restart_ok" -eq 0 ]; then
  echo "WARN: could not restart a gateway unit — sending HUP to listener if we own it"
  pid=$(ss -ltnp 2>/dev/null | awk '/18789/ {print}' | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1)
  echo "listener_pid=${pid:-none}"
  if [ -n "${pid:-}" ] && [ "$(id -u)" -eq 0 ]; then
    kill -HUP "$pid" 2>/dev/null || true
  fi
fi

sleep 3
echo "GET $GW/health"
curl -sS -m 8 -w " HTTP %{http_code}\n" "$GW/health" | tail -1

echo "POST $GW/v1/chat/completions (body redacted)"
code=$(curl -sS -m 45 -o /tmp/oc_chat.json -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "{\"model\":\"${AGENT}\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly the word PONG\"}],\"max_tokens\":16,\"temperature\":0}" \
  "$GW/v1/chat/completions" || echo 000)
echo "chat_http=$code"
python3 - <<'PY'
import json
try:
    d = json.load(open("/tmp/oc_chat.json", encoding="utf-8"))
except Exception as e:
    print("chat_body_unreadable", e)
    raise SystemExit
msg = ((d.get("choices") or [{}])[0].get("message") or {}).get("content")
print("content_prefix:", (msg or "")[:80].replace("\n", " "))
print("has_choices:", bool(d.get("choices")))
PY

if [ "$code" = "200" ]; then
  echo "CLASSROOM_OPEN=yes"
  exit 0
fi
echo "CLASSROOM_OPEN=no"
exit 1
