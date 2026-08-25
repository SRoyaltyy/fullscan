#!/usr/bin/env bash
# Read-only live probe of OpenClaw timeout + clocks on the ECS box.
# Does NOT bounce the gateway, rewrite a day, or enable/disable units.
set +e
export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"
export GIT_TERMINAL_PROMPT=0

GHA_USER="${FULLSCAN_USER:-gha}"
OUT="${GITHUB_WORKSPACE:-.}/01_daily/_openclaw_probe.md"
# On-box probe always hits loopback. The gateway binds 127.0.0.1.
GW="http://127.0.0.1:18789"
export OPENCLAW_GATEWAY_URL="$GW"
if [ -f /home/gha/.fullscan.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /home/gha/.fullscan.env
  set +a
  export OPENCLAW_GATEWAY_URL="$GW"
fi
TOKEN="${OPENCLAW_TOKEN:-}"

as_gha() {
  if [ "$(id -u)" -eq 0 ] && id "$GHA_USER" >/dev/null 2>&1; then
    sudo -u "$GHA_USER" -H env HOME="$HOME" USER="$GHA_USER" PATH="$PATH" "$@"
  else
    "$@"
  fi
}

# Gateway RPCs can hang at 30s. GNU timeout cannot run a shell function.
# Do NOT export OPENCLAW_GATEWAY_URL here: this CLI treats a URL override
# as "explicit credentials required" and then refuses config token, so
# `cron list` / `health` die even though the gateway is up on loopback.
oc() {
  if [ "$(id -u)" -eq 0 ] && id "$GHA_USER" >/dev/null 2>&1; then
    timeout 45 sudo -u "$GHA_USER" -H \
      env HOME="$HOME" USER="$GHA_USER" PATH="$PATH" \
          OPENCLAW_GATEWAY_TOKEN="${TOKEN}" \
      openclaw "$@" 2>&1
  else
    timeout 45 env OPENCLAW_GATEWAY_TOKEN="${TOKEN}" openclaw "$@" 2>&1
  fi
  echo "[exit $?]"
}

section() { echo ""; echo "## $1"; echo; }
pre() {
  echo '```'
  cat
  echo '```'
}

mkdir -p "$(dirname "$OUT")"
exec > >(tee "$OUT")
echo "# OpenClaw live probe"
echo
echo "- generated: $(date -u +%Y-%m-%dT%H:%M:%SZ) UTC / $(TZ=America/New_York date '+%F %H:%M %Z') / $(TZ=Asia/Shanghai date '+%F %H:%M %Z')"
echo "- uid=$(id -u) user=$(id -un) home=$HOME"
echo "- gateway_url=${GW}"
echo "- token_set=$([ -n "$TOKEN" ] && echo yes || echo no)"

# ---------------------------------------------------------------------------
section "1. Port + HTTP health (running process, not disk)"
if command -v ss >/dev/null 2>&1; then
  echo "### ss :18789"
  ss -ltnp 2>/dev/null | grep 18789 | pre
else
  echo "(no ss)"
fi

for path in /health /healthz /ready /readyz /startup; do
  echo "### GET ${GW}${path}"
  curl -sS -m 8 -w "\nHTTP %{http_code} time=%{time_total}s\n" "${GW}${path}" 2>&1 | head -20 | pre
done

echo "### GET ${GW}/v1/models (Accept: application/json)"
curl -sS -m 15 -w "\nHTTP %{http_code} time=%{time_total}s\n" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: application/json" \
  "${GW}/v1/models" 2>&1 | head -40 | pre

# ---------------------------------------------------------------------------
section "2. Live timeoutSeconds (CLI talks to the running gateway)"
echo "Disk JSON is not enough. CLI get after a live gateway is the loaded value."
echo
echo "### agents.defaults.timeoutSeconds"
oc config get agents.defaults.timeoutSeconds | pre
echo "### models.providers.xai.timeoutSeconds"
oc config get models.providers.xai.timeoutSeconds | pre
echo "### models.providers.openai.timeoutSeconds"
oc config get models.providers.openai.timeoutSeconds | pre
echo "### models.providers.anthropic.timeoutSeconds"
oc config get models.providers.anthropic.timeoutSeconds | pre
echo "### agents.defaults.subagents.runTimeoutSeconds"
oc config get agents.defaults.subagents.runTimeoutSeconds | pre
echo "### agents.defaults.llm (must be ABSENT)"
oc config get agents.defaults.llm | pre

echo "### disk ~/.openclaw/openclaw.json timeout fields"
python3 - <<'PY' 2>&1 | pre
import json, os
p = os.path.expanduser("/home/gha/.openclaw/openclaw.json")
try:
    d = json.load(open(p, encoding="utf-8"))
except Exception as e:
    print(f"unreadable {p}: {e}")
    raise SystemExit
defaults = (d.get("agents") or {}).get("defaults") or {}
providers = ((d.get("models") or {}).get("providers") or {})
print("agents.defaults.timeoutSeconds =", defaults.get("timeoutSeconds"))
print("agents.defaults.llm present =", "llm" in defaults)
print("subagents.runTimeoutSeconds =", (defaults.get("subagents") or {}).get("runTimeoutSeconds"))
print("gateway.mode =", (d.get("gateway") or {}).get("mode"))
for name, prov in providers.items():
    if isinstance(prov, dict):
        print(f"models.providers.{name}.timeoutSeconds =", prov.get("timeoutSeconds"))
    else:
        print(f"models.providers.{name} =", type(prov).__name__)
PY

# ---------------------------------------------------------------------------
section "3. OpenClaw gateway / health / doctor (live)"
echo "### openclaw health --json"
oc health --json | head -80 | pre
echo "### openclaw health --verbose"
oc health --verbose | head -80 | pre
echo "### openclaw status --deep"
oc status --deep | head -120 | pre
echo "### openclaw gateway status --deep"
oc gateway status --deep | head -120 | pre
echo "### openclaw gateway probe"
oc gateway probe | head -80 | pre

# ---------------------------------------------------------------------------
section "4. OpenClaw cron / automations scheduler"
echo "This is OpenClaw's own job timer, distinct from systemd fullscan-preopen.timer."
echo
echo "### openclaw automations status"
oc automations status | head -80 | pre
echo "### openclaw automations list --all"
oc automations list --all | head -120 | pre
echo "### openclaw automations list --json"
oc automations list --all --json | head -200 | pre
echo "### openclaw cron list --all (alias)"
oc cron list --all | head -80 | pre
echo "### openclaw cron status"
oc cron status | head -80 | pre
echo "### openclaw cron status --json"
oc cron status --json | head -200 | pre
echo "### cron store on disk"
ls -la /home/gha/.openclaw/cron 2>&1 | pre
if [ -f /home/gha/.openclaw/cron/jobs.json ]; then
  echo "### ~/.openclaw/cron/jobs.json (legacy)"
  python3 - <<'PY' 2>&1 | pre
import json
p="/home/gha/.openclaw/cron/jobs.json"
try:
    d=json.load(open(p, encoding="utf-8"))
except Exception as e:
    print("unreadable:", e)
    raise SystemExit
if isinstance(d, list):
    print(f"jobs: {len(d)}")
    for j in d[:20]:
        if isinstance(j, dict):
            print("-", j.get("name") or j.get("id"), "enabled=", j.get("enabled"),
                  "sched=", j.get("schedule") or j.get("cron") or j.get("every"),
                  "next=", j.get("state",{}).get("nextRunAtMs") if isinstance(j.get("state"), dict) else None)
elif isinstance(d, dict):
    jobs = d.get("jobs") or d.get("items") or d
    print(type(d).__name__, "keys=", list(d)[:20] if isinstance(d, dict) else "")
    if isinstance(jobs, list):
        print(f"jobs: {len(jobs)}")
        for j in jobs[:20]:
            print("-", (j or {}).get("name"), (j or {}).get("enabled"), (j or {}).get("schedule"))
    else:
        print(json.dumps(d, indent=2)[:2000])
else:
    print(type(d))
PY
fi

# ---------------------------------------------------------------------------
section "5. systemd clocks (ECS 05:55 Pre-Open ALL)"
echo "### fullscan-preopen.timer"
systemctl is-enabled fullscan-preopen.timer 2>&1
systemctl is-active fullscan-preopen.timer 2>&1
systemctl list-timers --all fullscan-preopen.timer 2>&1
echo
echo "### fullscan-preopen.timer show"
systemctl show fullscan-preopen.timer \
  -p Unit -p NextElapseUSecRealtime -p LastTriggerUSec -p Persistent \
  -p Triggers -p ActiveState -p SubState -p UnitFileState 2>&1 | pre
echo "### timer unit file OnCalendar"
systemctl cat fullscan-preopen.timer 2>&1 | pre
echo "### fullscan-openclaw-gateway (the process we systemd-run)"
systemctl is-active fullscan-openclaw-gateway 2>&1
systemctl status fullscan-openclaw-gateway --no-pager -l 2>&1 | head -30 | pre

echo "### expected next 05:55 America/New_York vs systemd Next"
python3 - <<'PY' 2>&1 | pre
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
et = ZoneInfo("America/New_York")
now = datetime.now(et)
d = now.date()
# next weekday 05:55 ET (Mon-Fri)
for i in range(0, 8):
    cand = datetime(d.year, d.month, d.day, 5, 55, tzinfo=et) + timedelta(days=i)
    if cand.weekday() >= 5:
        continue
    if cand > now:
        print("now ET:", now.isoformat())
        print("next weekday 05:55 ET:", cand.isoformat())
        print("next as CST:", cand.astimezone(ZoneInfo("Asia/Shanghai")).isoformat())
        print("hours until:", round((cand - now).total_seconds()/3600, 2))
        break
else:
    print("could not compute next 05:55")
PY

# ---------------------------------------------------------------------------
section "6. Live chat ping (gateway actually answers)"
echo "Short completion against /v1/chat/completions. 90s cap. Proves the"
echo "running process will take a Grok turn. Does NOT soak 9 minutes."
echo
python3 - <<'PY' 2>&1 | pre
import json, os, urllib.request, urllib.error, time
base = os.environ.get("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789").rstrip("/")
token = os.environ.get("OPENCLAW_TOKEN") or ""
body = json.dumps({
    "model": os.environ.get("OPENCLAW_AGENT", "openclaw/default"),
    "messages": [{"role": "user", "content": "Reply with exactly the word PONG and nothing else."}],
    "max_tokens": 16,
    "temperature": 0,
}).encode()
paths = [
    "/v1/chat/completions",
    "/openai/v1/chat/completions",
    "/api/v1/chat/completions",
    "/chat/completions",
]
for path in paths:
    url = base + path
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    req.add_header("x-openclaw-model", os.environ.get("OPENCLAW_BACKEND_MODEL", "xai/grok-4.6"))
    req.add_header("x-openclaw-session-key", "fullscan-openclaw-probe")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read()[:2500]
            dt = time.time() - t0
            print(f"{path} HTTP {resp.status} in {dt:.1f}s")
            try:
                data = json.loads(raw)
            except Exception:
                print((raw[:400]).decode("utf-8", "replace"))
                continue
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            print("content:", (choice or "")[:400])
            print("model:", data.get("model"))
            low = (choice or "").lower()
            if "pong" in low:
                print("PING_RESULT=PONG_OK")
            elif any(n in (choice or "") for n in ("timed out", "idle timeout", "LLM request timed out")):
                print("PING_RESULT=TIMEOUT_STUB")
            else:
                print("PING_RESULT=ANSWERED")
            break
    except urllib.error.HTTPError as e:
        dt = time.time() - t0
        snippet = ""
        try:
            snippet = e.read()[:200].decode("utf-8", "replace")
        except Exception:
            pass
        print(f"{path} HTTP {e.code} in {dt:.1f}s {snippet!r}")
    except Exception as e:
        dt = time.time() - t0
        print(f"{path} ERROR after {dt:.1f}s: {type(e).__name__}: {e}")
else:
    print("PING_RESULT=NO_CHAT_ENDPOINT")
PY

# ---------------------------------------------------------------------------
section "7. Verdict (live, this run)"
python3 - <<'PY'
import os, re, subprocess, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

home = "/home/gha"
fail = []
warn = []
ok = []

def sh(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=20)
        return (r.stdout or "") + (r.stderr or "")
    except Exception as e:
        return str(e)

ss = sh("ss -ltn 2>/dev/null | grep 18789 || netstat -ltn 2>/dev/null | grep 18789")
if ":18789" in ss:
    ok.append("gateway port 18789 is LISTENING")
else:
    fail.append("gateway port 18789 is NOT listening")

# disk JSON
p = os.path.join(home, ".openclaw", "openclaw.json")
try:
    d = json.load(open(p, encoding="utf-8"))
    defaults = (d.get("agents") or {}).get("defaults") or {}
    xai = ((d.get("models") or {}).get("providers") or {}).get("xai") or {}
    to = defaults.get("timeoutSeconds")
    xto = xai.get("timeoutSeconds") if isinstance(xai, dict) else None
    if to == 10800:
        ok.append(f"disk agents.defaults.timeoutSeconds={to}")
    else:
        fail.append(f"disk agents.defaults.timeoutSeconds={to} (want 10800)")
    if xto == 10800:
        ok.append(f"disk models.providers.xai.timeoutSeconds={xto}")
    else:
        fail.append(f"disk models.providers.xai.timeoutSeconds={xto} (want 10800)")
    if "llm" in defaults:
        fail.append("disk still has rejected agents.defaults.llm")
    else:
        ok.append("disk agents.defaults.llm ABSENT")
except Exception as e:
    fail.append(f"disk openclaw.json unreadable: {e}")

en = sh("systemctl is-enabled fullscan-preopen.timer 2>/dev/null").strip().splitlines()
en = en[-1] if en else ""
act = sh("systemctl is-active fullscan-preopen.timer 2>/dev/null").strip().splitlines()
act = act[-1] if act else ""
if en == "enabled":
    ok.append("fullscan-preopen.timer is-enabled=enabled")
else:
    fail.append(f"fullscan-preopen.timer is-enabled={en!r} (want enabled)")
if act == "active":
    ok.append("fullscan-preopen.timer is-active=active")
else:
    warn.append(f"fullscan-preopen.timer is-active={act!r} (want active)")

nxt = sh("systemctl show fullscan-preopen.timer -p NextElapseUSecRealtime --value")
print("systemd NextElapseUSecRealtime:", nxt.strip())
cal = sh("systemctl show fullscan-preopen.timer -p TimersCalendar --value")
print("systemd TimersCalendar:", cal.strip())
pers = sh("systemctl show fullscan-preopen.timer -p Persistent --value")
print("systemd Persistent:", pers.strip())
if "true" in pers.lower() or pers.strip() == "yes":
    ok.append("timer Persistent=true")
else:
    warn.append(f"timer Persistent={pers.strip()!r}")

et = ZoneInfo("America/New_York")
now = datetime.now(et)
d = now.date()
expect = None
for i in range(0, 8):
    cand = datetime(d.year, d.month, d.day, 5, 55, tzinfo=et) + timedelta(days=i)
    if cand.weekday() < 5 and cand > now:
        expect = cand
        break
print("expect next 05:55 ET:", expect.isoformat() if expect else "?")
print("now ET:", now.isoformat())

gw_unit = sh("systemctl is-active fullscan-openclaw-gateway 2>/dev/null").strip().splitlines()
gw_unit = gw_unit[-1] if gw_unit else ""
print("fullscan-openclaw-gateway:", gw_unit)
if gw_unit == "active":
    ok.append("fullscan-openclaw-gateway unit active")
else:
    warn.append(f"fullscan-openclaw-gateway unit={gw_unit!r} (port may still be up)")

print()
print("OK:")
for x in ok:
    print("  +", x)
print("WARN:")
for x in warn:
    print("  !", x)
if not warn:
    print("  (none)")
print("FAIL:")
for x in fail:
    print("  -", x)
if not fail:
    print("  (none)")
print()
if fail:
    print("VERDICT=NOT_OPERATIONAL")
elif warn:
    print("VERDICT=OPERATIONAL_WITH_WARN")
else:
    print("VERDICT=OPERATIONAL")
PY

echo
echo "[probe] wrote $OUT"
exit 0
