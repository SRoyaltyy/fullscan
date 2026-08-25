# OpenClaw live probe

- generated: 2026-08-25T17:30:28Z UTC / 2026-08-25 13:30 EDT / 2026-08-26 01:30 CST
- uid=0 user=root home=/home/gha
- gateway_url=http://127.0.0.1:18789
- token_set=yes

## 1. Port + HTTP health (running process, not disk)

### ss :18789
```
LISTEN 0      511        127.0.0.1:18789      0.0.0.0:*    users:(("openclaw-gatewa",pid=86268,fd=32))
LISTEN 0      511            [::1]:18789         [::]:*    users:(("openclaw-gatewa",pid=86268,fd=33))
```
### GET http://127.0.0.1:18789/health
```
{"ok":true,"status":"live"}
HTTP 200 time=0.002622s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.001347s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":994699,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":29897,"delayP99Ms":20.3,"delayMaxMs":21.1,"utilization":0.002,"cpuCoreRatio":0.004}}
HTTP 200 time=0.002560s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":994715,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":29897,"delayP99Ms":20.3,"delayMaxMs":21.1,"utilization":0.002,"cpuCoreRatio":0.004}}
HTTP 200 time=0.001961s
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
          light: "claw:light",
          openknot: "knot:dark",
          fieldmanual: "dash:dark",
          clawdash: "dash:light",
          system: "claw:system",
        };
        try {
          var keys = Object.keys(localStorage);
          var raw;
          for (var i = 0; i < keys.length; i++) {
            if (keys[i].indexOf("openclaw.control.settings.v1") === 0) {
              raw = localStorage.getItem(keys[i]);
              if (raw) break;
            }
          }
          if (!raw) return;
          var s = JSON.parse(raw);
          var t = s && s.theme;
          var m = s && s.themeMode;
          if (typeof t !== "string") t = "";
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
File logs: /tmp/openclaw-1000/openclaw-2026-08-26.log

Config (cli): ~/.openclaw/openclaw.json
Config (service): ~/.openclaw/openclaw.json

Gateway: bind=loopback (127.0.0.1), port=18789 (env/config)
Probe target: ws://127.0.0.1:18789
Dashboard: http://127.0.0.1:18789/
Probe note: Loopback-only gateway; only local clients can connect.

Runtime: unknown (systemctl --user unavailable: Failed to connect to bus: $DBUS_SESSION_BUS_ADDRESS and $XDG_RUNTIME_DIR not defined (consider using --machine=<user>@.host --user to connect to bus of other user))
Connectivity probe: failed
Probe target: ws://127.0.0.1:18789
  unauthorized: gateway token mismatch (provide gateway auth token)
Capability: unknown

systemd user services unavailable.
systemd user services are unavailable; install/enable systemd or run the gateway under your supervisor.
On a headless server (SSH/no desktop session): run `sudo loginctl enable-linger $(whoami)` to persist your systemd user session across logins.
Also ensure XDG_RUNTIME_DIR is set: `export XDG_RUNTIME_DIR=/run/user/$(id -u)`, then retry.
If you're in a container, run the gateway in the foreground instead of `openclaw gateway`.

Port 18789 is already in use.
- pid 86268 gha: openclaw-gateway (127.0.0.1:18789)
- pid 86268 gha: openclaw-gateway ([::1]:18789)
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
  Connect: ok (66ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

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
NEXT                        LEFT     LAST PASSED UNIT                   ACTIVATES
Wed 2026-08-26 17:55:00 CST 16h left n/a  n/a    fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Wed 2026-08-26 17:55:00 CST
LastTriggerUSec=
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
     Active: active (running) since Wed 2026-08-26 01:13:27 CST; 18min ago
   Main PID: 86268 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 251.6M
        CPU: 47.591s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─86268 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Aug 26 01:31:22 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:22.438+08:00 [ws] closed before connect conn=b3d9713c-26c5-4158-970a-d31c0f8862b3 peer=127.0.0.1:38910->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (provide gateway auth token) phase=auth_credentials_received
Aug 26 01:31:34 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:34.444+08:00 [ws] ⇄ res ✗ status 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=5e6946d5…ad87 id=a0996441…f96f
Aug 26 01:31:34 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:34.456+08:00 [ws] ⇄ res ✗ system-presence 14ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=5e6946d5…ad87 id=56fddd4a…184a
Aug 26 01:31:34 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:34.463+08:00 [ws] ⇄ res ✗ config.get 25ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=5e6946d5…ad87 id=ac0c203c…80f9
Aug 26 01:31:54 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:54.605+08:00 [ws] unauthorized conn=2502a054-a0e9-4cd5-b09f-9a6183db062a peer=127.0.0.1:41592->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=a2744552-ffbf-4bb3-8e5c-a5271b3271cc host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Aug 26 01:31:54 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:54.696+08:00 [ws] closed before connect conn=2502a054-a0e9-4cd5-b09f-9a6183db062a peer=127.0.0.1:41592->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Aug 26 01:31:59 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:59.376+08:00 [ws] unauthorized conn=c17b8151-c2c1-4264-893d-64fd182149af peer=127.0.0.1:58880->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=a3cf232b-e2aa-4233-8bb6-281ce0a768fd host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Aug 26 01:31:59 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:31:59.463+08:00 [ws] closed before connect conn=c17b8151-c2c1-4264-893d-64fd182149af peer=127.0.0.1:58880->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
Aug 26 01:32:04 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:32:04.100+08:00 [ws] unauthorized conn=b3eff669-236d-40fe-9388-d5fc5b86d537 peer=127.0.0.1:58894->127.0.0.1:18789 remote=127.0.0.1 client=cli cli v2026.7.1-2 role=operator scopes=0 auth=token device=no platform=linux instance=a559f80e-072a-4f4e-8e2a-249617df3860 host=127.0.0.1:18789 origin=n/a ua=n/a reason=token_mismatch
Aug 26 01:32:04 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:32:04.186+08:00 [ws] closed before connect conn=b3eff669-236d-40fe-9388-d5fc5b86d537 peer=127.0.0.1:58894->127.0.0.1:18789 remote=127.0.0.1 fwd=n/a origin=n/a host=127.0.0.1:18789 ua=n/a code=1008 reason=unauthorized: gateway token mismatch (set gateway.remote.token to match gateway.auth.token) phase=auth_credentials_received
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-08-25T13:32:04.320172-04:00
next weekday 05:55 ET: 2026-08-26T05:55:00-04:00
next as CST: 2026-08-26T17:55:00+08:00
hours until: 16.38
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
/v1/chat/completions HTTP 404 in 0.0s 'Not Found'
/openai/v1/chat/completions HTTP 404 in 0.0s 'Not Found'
/api/v1/chat/completions HTTP 404 in 0.0s 'Not Found'
/chat/completions HTTP 404 in 0.0s 'Not Found'
PING_RESULT=NO_CHAT_ENDPOINT
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Wed 2026-08-26 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Wed 2026-08-26 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-08-26T05:55:00-04:00
now ET: 2026-08-25T13:32:04.571285-04:00
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
