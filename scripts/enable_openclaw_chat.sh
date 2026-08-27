#!/usr/bin/env bash
# Turn on OpenClaw POST /v1/chat/completions, start the gateway if dead,
# wait until it answers. Never print tokens. Safe to re-run.
set +e
set -u
export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"
GHA_USER="${FULLSCAN_USER:-gha}"
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

port_up() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -q ':18789'
    return $?
  fi
  curl -sS -m 2 -o /dev/null "$GW/health" 2>/dev/null
}

wait_port() {
  local i=0
  while [ "$i" -lt 30 ]; do
    if port_up; then
      echo "port 18789 up after ${i}s"
      return 0
    fi
    sleep 2
    i=$((i + 2))
  done
  echo "port 18789 still down after ${i}s"
  return 1
}

chat_ping() {
  rm -f /tmp/oc_chat.json
  local code
  code=$(curl -sS -m 60 -o /tmp/oc_chat.json -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -d "{\"model\":\"${AGENT}\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly the word PONG\"}],\"max_tokens\":16,\"temperature\":0}" \
    "$GW/v1/chat/completions" 2>/tmp/oc_chat.err || echo 000)
  echo "chat_http=$code"
  if [ -s /tmp/oc_chat.err ]; then
    head -c 200 /tmp/oc_chat.err; echo
  fi
  python3 - <<'PY' || true
import json, os
p = "/tmp/oc_chat.json"
if not os.path.isfile(p) or os.path.getsize(p) == 0:
    print("chat_body=empty")
    raise SystemExit
try:
    d = json.load(open(p, encoding="utf-8"))
except Exception as e:
    print("chat_body_unreadable", e)
    raise SystemExit
msg = ((d.get("choices") or [{}])[0].get("message") or {}).get("content")
print("content_prefix:", (msg or "")[:80].replace("\n", " "))
print("has_choices:", bool(d.get("choices")))
PY
  CHAT_CODE="$code"
  [ "$code" = "200" ]
}

start_gateway() {
  echo "starting fullscan-openclaw-gateway via systemd-run"
  if [ "$(id -u)" -eq 0 ]; then
    loginctl enable-linger "$GHA_USER" 2>/dev/null || true
  fi
  if ! command -v openclaw >/dev/null 2>&1; then
    echo "FAIL: openclaw not on PATH"
    return 1
  fi
  OC="$(command -v openclaw)"
  systemctl reset-failed fullscan-openclaw-gateway 2>/dev/null || true
  systemctl stop fullscan-openclaw-gateway 2>/dev/null || true
  systemd-run --uid="$GHA_USER" --gid="$GHA_USER" \
    --working-directory=/home/gha \
    --unit=fullscan-openclaw-gateway \
    --property=Restart=always \
    --property=RestartSec=5 \
    -E HOME=/home/gha -E USER=gha \
    "$OC" gateway
  echo "systemd-run exit=$?"
  systemctl status fullscan-openclaw-gateway --no-pager -l 2>/dev/null | head -25
}

# --- 1. enable classroom in JSON ---
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
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
print(f"chatCompletions.enabled: {before} -> True")
print("NEED_RESTART" if not before else "ALREADY_ON")
PY
NEED=$(python3 - "$CFG" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
en = (((d.get("gateway") or {}).get("http") or {}).get("endpoints") or {}).get("chatCompletions") or {}
print("1" if en.get("enabled") else "0")
PY
)

if [ "$(id -u)" -eq 0 ]; then
  chown -R "$GHA_USER:$GHA_USER" /home/gha/.openclaw 2>/dev/null || true
fi

if command -v openclaw >/dev/null 2>&1; then
  if [ "$(id -u)" -eq 0 ]; then
    sudo -u "$GHA_USER" -H env HOME="$HOME" openclaw config set \
      gateway.http.endpoints.chatCompletions.enabled true 2>&1 | tail -8
  else
    openclaw config set gateway.http.endpoints.chatCompletions.enabled true 2>&1 | tail -8
  fi
fi

# --- 2. if already answering, leave the running process alone ---
if port_up; then
  echo "port 18789 already listening — ping before restart"
  if chat_ping; then
    echo "CLASSROOM_OPEN=yes (no restart needed)"
    exit 0
  fi
  echo "port up but chat not 200 — restart required to load chatCompletions"
fi

# --- 3. bounce or start ---
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet fullscan-openclaw-gateway 2>/dev/null; then
  echo "restart fullscan-openclaw-gateway"
  systemctl restart fullscan-openclaw-gateway
elif command -v systemctl >/dev/null 2>&1 && systemctl list-units --all --no-legend 2>/dev/null | grep -q fullscan-openclaw-gateway; then
  echo "start existing fullscan-openclaw-gateway unit"
  systemctl start fullscan-openclaw-gateway
else
  start_gateway
fi

wait_port || start_gateway
wait_port || true

echo "GET $GW/health"
curl -sS -m 8 -w " HTTP %{http_code}\n" "$GW/health" | tail -3

# --- 4. ping with retries (gateway can take a minute after restart) ---
for try in 1 2 3 4 5; do
  echo "chat ping try $try"
  if chat_ping; then
    echo "CLASSROOM_OPEN=yes"
    exit 0
  fi
  sleep 8
done

echo "CLASSROOM_OPEN=no"
journalctl -u fullscan-openclaw-gateway -n 40 --no-pager 2>/dev/null | tail -40
exit 1
