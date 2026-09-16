# OpenClaw live probe

- generated: 2026-09-16T11:10:43Z UTC / 2026-09-16 07:10 EDT / 2026-09-16 19:10 CST
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
HTTP 200 time=0.158369s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.002992s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":1358974738,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":20170,"delayP99Ms":36.1,"delayMaxMs":577.2,"utilization":0.134,"cpuCoreRatio":0.067}}
HTTP 200 time=0.008045s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":1358974762,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":20170,"delayP99Ms":36.1,"delayMaxMs":577.2,"utilization":0.134,"cpuCoreRatio":0.067}}
HTTP 200 time=0.002146s
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
HTTP 401 time=0.004140s
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
File logs: /tmp/openclaw-1000/openclaw-2026-09-16.log

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
  Connect: ok (48ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

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
NEXT                        LEFT     LAST                        PASSED       UNIT                   ACTIVATES
Thu 2026-09-17 17:55:00 CST 22h left Wed 2026-09-16 17:55:04 CST 1h 17min ago fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Thu 2026-09-17 17:55:00 CST
LastTriggerUSec=Wed 2026-09-16 17:55:04 CST
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
     Active: active (running) since Tue 2026-09-01 01:40:55 CST; 2 weeks 1 day ago
   Main PID: 417604 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 460.7M
        CPU: 13h 10min 876ms
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─417604 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Sep 16 19:11:43 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:11:43.132+08:00 [ws] closed before connect conn=a674d788-119a-4e67-8522-ae67bceef06f peer=127.0.0.1:52742->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (provide gateway auth token) phase=auth_credentials_received
Sep 16 19:11:54 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:11:54.986+08:00 [ws] ⇄ res ✗ status 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=20eeb7be…223a id=d8cef965…5334
Sep 16 19:11:55 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:11:55.000+08:00 [ws] ⇄ res ✗ system-presence 20ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=20eeb7be…223a id=9cc98972…1434
Sep 16 19:11:55 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:11:55.008+08:00 [ws] ⇄ res ✗ config.get 29ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=20eeb7be…223a id=3128b690…e9b1
Sep 16 19:12:13 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:13.400+08:00 [ws] unauthorized conn=c856424c-48de-4259-8616-fa6cc34d6c80 peer=127.0.0.1:34424->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=d74b9723-6006-4097-89ae-855695a4fcdd host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 16 19:12:13 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:13.484+08:00 [ws] closed before connect conn=c856424c-48de-4259-8616-fa6cc34d6c80 peer=127.0.0.1:34424->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Sep 16 19:12:17 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:17.772+08:00 [ws] unauthorized conn=95bea352-5d1e-4ead-aab8-36527b566e33 peer=127.0.0.1:34432->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=a22f7067-81e3-4d31-a0e4-0aedebe16cfc host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 16 19:12:17 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:17.857+08:00 [ws] closed before connect conn=95bea352-5d1e-4ead-aab8-36527b566e33 peer=127.0.0.1:34432->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Sep 16 19:12:22 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:22.277+08:00 [ws] unauthorized conn=a23534b4-d4e0-46a4-8d9c-83907bc242fc peer=127.0.0.1:41412->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=a93dacf6-2ad9-491d-bd79-0fb99471fb62 host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Sep 16 19:12:22 iZt4nagf215582ts0wf5jcZ openclaw[417604]: 2026-09-16T19:12:22.368+08:00 [ws] closed before connect conn=a23534b4-d4e0-46a4-8d9c-83907bc242fc peer=127.0.0.1:41412->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-09-16T07:12:22.567083-04:00
next weekday 05:55 ET: 2026-09-17T05:55:00-04:00
next as CST: 2026-09-17T17:55:00+08:00
hours until: 22.71
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

systemd NextElapseUSecRealtime: Thu 2026-09-17 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Thu 2026-09-17 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-09-17T05:55:00-04:00
now ET: 2026-09-16T07:12:22.855624-04:00
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
