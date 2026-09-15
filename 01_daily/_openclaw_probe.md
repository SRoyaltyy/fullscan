# OpenClaw live probe

- generated: 2026-09-15T10:00:42Z UTC / 2026-09-15 06:00 EDT / 2026-09-15 18:00 CST
- uid=0 user=root home=/home/gha
- gateway_url=http://127.0.0.1:18789
- token_set=yes

## 1. Port + HTTP health (running process, not disk)

### ss :18789
```
LISTEN 0      511        127.0.0.1:18789      0.0.0.0:*    users:(("openclaw-gatewa",pid=417604,fd=33))
LISTEN 0      511            [::1]:18789         [::]:*    users:(("openclaw-gatewa",pid=417604,fd=34))
```
### GET http://127.0.0.1:18789/health
```
{"ok":true,"status":"live"}
HTTP 200 time=0.086623s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.001372s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":1268373679,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":7126,"delayP99Ms":28,"delayMaxMs":75.8,"utilization":0.223,"cpuCoreRatio":0.167}}
HTTP 200 time=0.009906s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":1268373700,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":7126,"delayP99Ms":28,"delayMaxMs":75.8,"utilization":0.223,"cpuCoreRatio":0.167}}
HTTP 200 time=0.002028s
```
### GET http://127.0.0.1:18789/startup
```
<!doctype html>
<html data-openclaw-terminal-enabled="false" lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, viewport-fit=cover, interactive-widget=resizes-content"
    />
    <title>OpenClaw Control</title>
    <meta name="color-scheme" content="dark light" />
    <link rel="icon" type="image/svg+xml" href="./favicon.svg" />
    <link rel="icon" type="image/png" sizes="32x32" href="./favicon-32.png" />
    <link rel="apple-touch-icon" sizes="180x180" href="./apple-touch-icon.png" />
    <link rel="manifest" href="./manifest.webmanifest" />
    <script>
      (function () {
        var THEMES = { claw: 1, knot: 1, dash: 1 };
        var MODES = { system: 1, light: 1, dark: 1 };
        var LEGACY = {
          dark: "claw:dark",
```
### GET http://127.0.0.1:18789/v1/models (Accept: application/json)
```
{"error":{"message":"Unauthorized","type":"unauthorized"}}
HTTP 401 time=0.024774s
```

## 2. Live timeoutSeconds (CLI talks to the running gateway)

Disk JSON is not enough. CLI get after a live gateway is the loaded value.

### agents.defaults.timeoutSeconds
```
10800
[exit 0]
```
### models.providers.xai.timeoutSeconds
```
10800
[exit 0]
```
### models.providers.openai.timeoutSeconds
```
10800
[exit 0]
```
### models.providers.anthropic.timeoutSeconds
```
10800
[exit 0]
```
### agents.defaults.subagents.runTimeoutSeconds
```
10800
[exit 0]
```
### agents.defaults.llm (must be ABSENT)
```
Config path not found: agents.defaults.llm. Run openclaw config validate to inspect config shape.
[exit 1]
```
### disk ~/.openclaw/openclaw.json timeout fields
```
agents.defaults.timeoutSeconds = 10800
agents.defaults.llm present = False
subagents.runTimeoutSeconds = 10800
gateway.mode = local
models.providers.xai.timeoutSeconds = 10800
models.providers.openai.timeoutSeconds = 10800
models.providers.anthropic.timeoutSeconds = 10800
```

## 3. OpenClaw gateway / health / doctor (live)

### openclaw health --json
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (provide gateway auth token)
{
  "ok": false,
  "error": {
    "type": "gateway_transport_error",
    "kind": "closed",
    "message": "gateway closed (1008): unauthorized: gateway token mismatch (provide gateway auth token)",
    "code": 1008,
    "reason": "unauthorized: gateway token mismatch (provide gateway auth token)"
  },
  "gateway": {
    "url": "ws://127.0.0.1:18789",
    "urlSource": "local loopback",
    "bindDetail": "Bind: loopback"
  }
}
[exit 1]
```
### openclaw health --verbose
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (provide gateway auth token)
[openclaw] Could not start the CLI.
[openclaw] Reason: gateway closed (1008): unauthorized: gateway token mismatch (provide gateway auth token)
Gateway target: ws://127.0.0.1:18789
Source: local loopback
Config: /home/gha/.openclaw/openclaw.json
Bind: loopback
[openclaw] Stack:
[openclaw] GatewayTransportError: gateway closed (1008): unauthorized: gateway token mismatch (provide gateway auth token)
[openclaw] Gateway target: ws://127.0.0.1:18789
[openclaw] Source: local loopback
[openclaw] Config: /home/gha/.openclaw/openclaw.json
[openclaw] Bind: loopback
[openclaw]     at createGatewayCloseTransportError (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:459:9)
[openclaw]     at Object.onClose (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:596:10)
[openclaw]     at GatewayClient.notifyClose (file:///usr/lib/node_modules/openclaw/dist/src-DZzKBMa7.js:728:23)
[openclaw]     at WebSocket.<anonymous> (file:///usr/lib/node_modules/openclaw/dist/src-DZzKBMa7.js:414:10)
[openclaw]     at WebSocket.emit (node:events:514:20)
[openclaw]     at WebSocket.emitClose (/usr/lib/node_modules/openclaw/node_modules/ws/lib/websocket.js:279:10)
[openclaw]     at Socket.socketOnClose (/usr/lib/node_modules/openclaw/node_modules/ws/lib/websocket.js:1360:15)
[openclaw]     at Socket.emit (node:events:514:20)
[openclaw]     at TCP.<anonymous> (node:net:362:12)
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw status --deep
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (provide gateway auth token)
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (provide gateway auth token)
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (provide gateway auth token)
[openclaw] Could not start the CLI.
[openclaw] Reason: gateway closed (1008): unauthorized: gateway token mismatch (provide gateway auth token)
Gateway target: ws://127.0.0.1:18789
Source: local loopback
Config: /home/gha/.openclaw/openclaw.json
Bind: loopback
[openclaw] Debug: set OPENCLAW_DEBUG=1 to include the stack trace.
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw gateway status --deep
```
Service: systemd user (disabled)
File logs: /tmp/openclaw-1000/openclaw-2026-09-15.log

Config (cli): ~/.openclaw/openclaw.json
Config (service): ~/.openclaw/openclaw.json

Gateway: bind=loopback (127.0.0.1), port=18789 (env/config)
Probe target: ws://127.0.0.1:18789
Dashboard: http://127.0.0.1:18789/
Probe note: Loopback-only gateway; only local clients can connect.

Runtime: stopped (state inactive, sub dead, last exit 0, reason 0)
Connectivity probe: failed
Probe target: ws://127.0.0.1:18789
  unauthorized: gateway token mismatch (provide gateway auth token)
Capability: unknown

Port 18789 is already in use.
- pid 417604 gha: openclaw-gateway (127.0.0.1:18789)
- pid 417604 gha: openclaw-gateway ([::1]:18789)
Listening: 127.0.0.1:18789, [::1]:18789
Troubles: run openclaw status
Troubleshooting: https://docs.openclaw.ai/troubleshooting
[exit 0]
```
### openclaw gateway probe
```
Gateway Status
Reachable: yes
Capability: connected-no-operator-scope
Probe budget: 3000ms

Warning:
- Read-probe diagnostics are limited by gateway scopes (missing operator.read). Connection succeeded, but read-only status calls are incomplete. Hint: pair device identity or use credentials with operator.read.

Discovery (this machine)
Found 0 gateways via Bonjour (local.)
Tip: if the gateway is remote, mDNS won’t cross networks; use Wide-Area Bonjour (split DNS) or SSH tunnels.

Targets
Local loopback ws://127.0.0.1:18789
  Connect: ok (53ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

[exit 0]
```

## 4. OpenClaw cron / automations scheduler

This is OpenClaw's own job timer, distinct from systemd fullscan-preopen.timer.

### openclaw automations status
```
[openclaw] Could not start the CLI.
[openclaw] Reason: Unknown command: openclaw automations. No built-in command or plugin CLI metadata owns "automations".
[openclaw] Debug: set OPENCLAW_DEBUG=1 to include the stack trace.
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw automations list --all
```
[openclaw] Could not start the CLI.
[openclaw] Reason: Unknown command: openclaw automations. No built-in command or plugin CLI metadata owns "automations".
[openclaw] Debug: set OPENCLAW_DEBUG=1 to include the stack trace.
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw automations list --json
```
[openclaw] Could not start the CLI.
[openclaw] Reason: Unknown command: openclaw automations. No built-in command or plugin CLI metadata owns "automations".
[openclaw] Debug: set OPENCLAW_DEBUG=1 to include the stack trace.
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw cron list --all (alias)
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
GatewayTransportError: gateway closed (1008): unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
Gateway target: ws://127.0.0.1:18789
Source: local loopback
Config: /home/gha/.openclaw/openclaw.json
Bind: loopback
[exit 1]
```
### openclaw cron status
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
GatewayTransportError: gateway closed (1008): unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
Gateway target: ws://127.0.0.1:18789
Source: local loopback
Config: /home/gha/.openclaw/openclaw.json
Bind: loopback
[exit 1]
```
### openclaw cron status --json
```
gateway connect failed: GatewayClientRequestError: unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
GatewayTransportError: gateway closed (1008): unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token)
Gateway target: ws://127.0.0.1:18789
Source: local loopback
Config: /home/gha/.openclaw/openclaw.json
Bind: loopback
[exit 1]
```
### cron store on disk
```
ls: cannot access '/home/gha/.openclaw/cron': No such file or directory
```

## 5. systemd clocks (ECS 05:55 Pre-Open ALL)

### fullscan-preopen.timer
enabled
active
NEXT                        LEFT     LAST                        PASSED   UNIT                   ACTIVATES
Wed 2026-09-16 17:55:00 CST 23h left Tue 2026-09-15 17:55:04 CST 7min ago fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Wed 2026-09-16 17:55:00 CST
LastTriggerUSec=Tue 2026-09-15 17:55:04 CST
Persistent=yes
Triggers=fullscan-preopen.service
ActiveState=active
SubState=waiting
UnitFileState=enabled
```
### timer unit file OnCalendar
```
# /etc/systemd/system/fullscan-preopen.timer
[Unit]
Description=fullscan Pre-Open ALL weekdays 05:55 America/New_York
Documentation=file:///home/gha/fullscan/PREDICTOR_README.md

[Timer]
# ECS clock, not GitHub cron. 05:55 ET so Grok has ~3.5h before 09:25.
OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York
# If the box was down at 05:55, fire on boot (run_preopen_all still
# refuses after 09:25 ET and skip-if-good no-ops a finished day).
Persistent=true
AccuracySec=30s
Unit=fullscan-preopen.service

[Install]
WantedBy=timers.target
```
### fullscan-openclaw-gateway (the process we systemd-run)
active
```
● fullscan-openclaw-gateway.service - /usr/bin/openclaw gateway
     Loaded: loaded (/run/systemd/transient/fullscan-openclaw-gateway.service; transient)
  Transient: yes
     Active: active (running) since Tue 2026-09-01 01:40:55 CST; 2 weeks 0 days ago
   Main PID: 417604 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 398.5M
        CPU: 11h 58min 25.938s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─417604 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Sep 15 18:01:53 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:01:53.283+08:00 [ws] ⇄ res ✗ status 173ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=27d59cbe…b45e id=e1d0dcc1…99db
Sep 15 18:01:53 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:01:53.288+08:00 [ws] ⇄ res ✗ system-presence 193ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=27d59cbe…b45e id=c28444fb…18d7
Sep 15 18:01:53 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:01:53.319+08:00 [ws] ⇄ res ✗ config.get 198ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=27d59cbe…b45e id=9022355a…007a
Sep 15 18:01:53 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:01:53.346+08:00 [ws] ⇄ res ✓ health 251ms cached=true conn=27d59cbe…b45e id=7c882ee7…0f7d
Sep 15 18:02:11 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:11.681+08:00 [ws] unauthorized conn=de138a09-4a84-421e-b879-a413771f94ce peer=127.0.0.1:38592->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=02b4fff3-523c-4380-acc9-0500f64bb49d host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 15 18:02:11 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:11.767+08:00 [ws] closed before connect conn=de138a09-4a84-421e-b879-a413771f94ce peer=127.0.0.1:38592->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Sep 15 18:02:15 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:15.972+08:00 [ws] unauthorized conn=b1b02bce-5dcc-45a0-b86c-663e1d80a03f peer=127.0.0.1:38594->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=44f6142b-5ed8-4073-8b59-35cf7f0e9554 host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 15 18:02:16 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:16.058+08:00 [ws] closed before connect conn=b1b02bce-5dcc-45a0-b86c-663e1d80a03f peer=127.0.0.1:38594->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Sep 15 18:02:20 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:20.255+08:00 [ws] unauthorized conn=52993900-f4b3-467a-9e58-19b870dc0874 peer=127.0.0.1:38608->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=e63964e6-6de0-43ca-bf0b-323dd77ee1f3 host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 15 18:02:20 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-15T18:02:20.343+08:00 [ws] closed before connect conn=52993900-f4b3-467a-9e58-19b870dc0874 peer=127.0.0.1:38608->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-09-15T06:02:20.889031-04:00
next weekday 05:55 ET: 2026-09-16T05:55:00-04:00
next as CST: 2026-09-16T17:55:00+08:00
hours until: 23.88
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
/v1/chat/completions HTTP 401 in 0.0s '{"error":{"message":"Unauthorized","type":"unauthorized"}}'
/openai/v1/chat/completions HTTP 404 in 0.0s 'Not Found'
/api/v1/chat/completions HTTP 404 in 0.0s 'Not Found'
/chat/completions HTTP 404 in 0.0s 'Not Found'
PING_RESULT=NO_CHAT_ENDPOINT
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Wed 2026-09-16 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Wed 2026-09-16 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-09-16T05:55:00-04:00
now ET: 2026-09-15T06:02:21.171395-04:00
fullscan-openclaw-gateway: active

OK:
  + gateway port 18789 is LISTENING
  + disk agents.defaults.timeoutSeconds=10800
  + disk models.providers.xai.timeoutSeconds=10800
  + disk agents.defaults.llm ABSENT
  + fullscan-preopen.timer is-enabled=enabled
  + fullscan-preopen.timer is-active=active
  + timer Persistent=true
  + fullscan-openclaw-gateway unit active
WARN:
  (none)
FAIL:
  (none)

VERDICT=OPERATIONAL

[probe] wrote /home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_openclaw_probe.md
