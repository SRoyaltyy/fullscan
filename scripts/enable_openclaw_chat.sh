#!/usr/bin/env bash
# Turn on OpenClaw POST /v1/chat/completions, start the gateway if dead,
# wait until it answers. Never print tokens. Safe to re-run.
#
# 401 = the GitHub secret / .fullscan.env token is NOT the gateway token.
# Always prefer the live token from ~/.openclaw/openclaw.json.
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
ENV_TOKEN="${OPENCLAW_TOKEN:-}"
GW_ENV_TOKEN="${OPENCLAW_GATEWAY_TOKEN:-}"
AGENT="${OPENCLAW_AGENT:-openclaw/default}"

echo "uid=$(id -u) user=$(id -un) cfg=$CFG"

if [ ! -f "$CFG" ]; then
  echo "FAIL: $CFG missing"
  exit 1
fi

# Live token from the running gateway config (this is the source of truth).
read_cfg() {
  python3 - "$CFG" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p, encoding="utf-8"))
gw = d.get("gateway") or {}
auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
token = (auth.get("token") or gw.get("token") or auth.get("password") or "")
mode = auth.get("mode") or gw.get("auth") or ""
if isinstance(mode, dict):
    mode = mode.get("mode") or ""
print(f"auth_mode={mode or '?'}")
print(f"cfg_token_len={len(str(token))}")
print(f"cfg_token_tail={(str(token)[-4:] if len(str(token)) >= 4 else 'short')}")
open("/tmp/oc_live_token", "w", encoding="utf-8").write(str(token))
PY
}
read_cfg
CFG_TOKEN=""
if [ -f /tmp/oc_live_token ]; then
  CFG_TOKEN=$(cat /tmp/oc_live_token)
  rm -f /tmp/oc_live_token
fi

fingerprint() {
  local t="${1:-}"
  if [ -z "$t" ]; then echo "empty"; return; fi
  echo "len=${#t} tail=${t: -4}"
}
echo "env_OPENCLAW_TOKEN=$(fingerprint "$ENV_TOKEN")"
echo "env_OPENCLAW_GATEWAY_TOKEN=$(fingerprint "$GW_ENV_TOKEN")"
echo "json_token=$(fingerprint "$CFG_TOKEN")"

# Prefer the json token. Env secrets are often stale after a gateway restart.
TOKEN="$CFG_TOKEN"
if [ -z "$TOKEN" ]; then TOKEN="$GW_ENV_TOKEN"; fi
if [ -z "$TOKEN" ]; then TOKEN="$ENV_TOKEN"; fi
echo "using_token=$(fingerprint "$TOKEN")"
echo "token_set=$([ -n "$TOKEN" ] && echo yes || echo no)"

# Push the LIVE token to later GH steps so Python does not use the stale secret.
if [ -n "${GITHUB_ENV:-}" ] && [ -n "$TOKEN" ]; then
  {
    echo "OPENCLAW_TOKEN<<EOF"
    echo "$TOKEN"
    echo "EOF"
    echo "OPENCLAW_GATEWAY_TOKEN<<EOF"
    echo "$TOKEN"
    echo "EOF"
  } >> "$GITHUB_ENV"
  echo "exported live token to GITHUB_ENV"
fi
export OPENCLAW_TOKEN="$TOKEN"
export OPENCLAW_GATEWAY_TOKEN="$TOKEN"

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
  # Try Bearer, then x-api-key, then the other candidate tokens.
  local tokens=("$TOKEN")
  [ -n "$CFG_TOKEN" ] && [ "$CFG_TOKEN" != "$TOKEN" ] && tokens+=("$CFG_TOKEN")
  [ -n "$GW_ENV_TOKEN" ] && [ "$GW_ENV_TOKEN" != "$TOKEN" ] && tokens+=("$GW_ENV_TOKEN")
  [ -n "$ENV_TOKEN" ] && [ "$ENV_TOKEN" != "$TOKEN" ] && tokens+=("$ENV_TOKEN")
  local t hdr code
  for t in "${tokens[@]}"; do
    [ -n "$t" ] || continue
    for hdr in bearer xapikey; do
      rm -f /tmp/oc_chat.json /tmp/oc_chat.err
      if [ "$hdr" = "bearer" ]; then
        code=$(curl -sS -m 60 -o /tmp/oc_chat.json -w "%{http_code}" \
          -H "Authorization: Bearer ${t}" \
          -H "x-api-key: ${t}" \
          -H "Content-Type: application/json" \
          -H "Accept: application/json" \
          -d "{\"model\":\"${AGENT}\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly the word PONG\"}],\"max_tokens\":16,\"temperature\":0}" \
          "$GW/v1/chat/completions" 2>/tmp/oc_chat.err || echo 000)
      else
        code=$(curl -sS -m 60 -o /tmp/oc_chat.json -w "%{http_code}" \
          -H "x-api-key: ${t}" \
          -H "Content-Type: application/json" \
          -d "{\"model\":\"${AGENT}\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly the word PONG\"}],\"max_tokens\":16,\"temperature\":0}" \
          "$GW/v1/chat/completions" 2>/tmp/oc_chat.err || echo 000)
      fi
      echo "chat_http=$code hdr=$hdr token=$(fingerprint "$t")"
      if [ -s /tmp/oc_chat.err ]; then
        head -c 200 /tmp/oc_chat.err; echo
      fi
      python3 - <<'PY' || true
import json, os
p = "/tmp/oc_chat.json"
if not os.path.isfile(p) or os.path.getsize(p) == 0:
    print("chat_body=empty")
    raise SystemExit
raw = open(p, encoding="utf-8", errors="replace").read()[:240]
try:
    d = json.loads(raw if raw.startswith("{") else open(p, encoding="utf-8").read())
except Exception:
    print("chat_body_prefix:", raw.replace("\n", " ")[:200])
    raise SystemExit
msg = ((d.get("choices") or [{}])[0].get("message") or {}).get("content")
if msg:
    print("content_prefix:", str(msg)[:80].replace("\n", " "))
else:
    err = d.get("error") or d.get("message") or d
    print("chat_error:", str(err)[:200].replace("\n", " "))
print("has_choices:", bool(d.get("choices")))
PY
      if [ "$code" = "200" ]; then
        TOKEN="$t"
        export OPENCLAW_TOKEN="$t"
        if [ -n "${GITHUB_ENV:-}" ]; then
          {
            echo "OPENCLAW_TOKEN<<EOF"
            echo "$t"
            echo "EOF"
          } >> "$GITHUB_ENV"
        fi
        CHAT_CODE=200
        return 0
      fi
    done
  done
  CHAT_CODE="${code:-000}"
  return 1
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
