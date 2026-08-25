# OpenClaw live probe

- generated: 2026-08-25T17:27:32Z UTC / 2026-08-25 13:27 EDT / 2026-08-26 01:27 CST
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
HTTP 200 time=0.002486s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.002588s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":819566,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":34764,"delayP99Ms":20.4,"delayMaxMs":25.3,"utilization":0.004,"cpuCoreRatio":0.006}}
HTTP 200 time=0.004173s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":819582,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":34764,"delayP99Ms":20.4,"delayMaxMs":25.3,"utilization":0.004,"cpuCoreRatio":0.006}}
HTTP 200 time=0.002088s
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
[openclaw] Could not start the CLI.
[openclaw] Reason: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
[openclaw] Debug: set OPENCLAW_DEBUG=1 to include the stack trace.
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw health --verbose
```
[openclaw] Could not start the CLI.
[openclaw] Reason: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
[openclaw] Stack:
[openclaw] GatewayExplicitAuthRequiredError: gateway url override requires explicit credentials
[openclaw] Fix: pass --token or --password *** --url (or gatewayToken in tools).
[openclaw] Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
[openclaw] Config: /home/gha/.openclaw/openclaw.json
[openclaw]     at ensureExplicitGatewayAuth (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:359:8)
[openclaw]     at callGatewayWithScopes (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:643:2)
[openclaw]     at async callGatewayLeastPrivilege (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:744:9)
[openclaw]     at async callGateway (file:///usr/lib/node_modules/openclaw/dist/call-Bj6Erfmh.js:755:9)
[openclaw]     at async file:///usr/lib/node_modules/openclaw/dist/health-p6SutBnt.js:512:18
[openclaw]     at async withProgress (file:///usr/lib/node_modules/openclaw/dist/progress-DXZjrYcT.js:132:10)
[openclaw]     at async healthCommand (file:///usr/lib/node_modules/openclaw/dist/health-p6SutBnt.js:508:13)
[openclaw]     at async Object.runParsedArgs (file:///usr/lib/node_modules/openclaw/dist/route-b75kd5c1.js:313:4)
[openclaw]     at async Object.run (file:///usr/lib/node_modules/openclaw/dist/route-b75kd5c1.js:434:4)
[openclaw]     at async Object.measure (file:///usr/lib/node_modules/openclaw/dist/startup-trace-Bc2ebu8Y.js:425:12)
[openclaw] Try: openclaw doctor
[openclaw] Help: openclaw --help
[exit 1]
```
### openclaw status --deep
```
[openclaw] Could not start the CLI.
[openclaw] Reason: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
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

CLI version: 2026.7.1-2 (/usr/bin/openclaw)
Gateway version: 2026.7.1-2

Runtime: unknown (systemctl --user unavailable: Failed to connect to bus: $DBUS_SESSION_BUS_ADDRESS and $XDG_RUNTIME_DIR not defined (consider using --machine=<user>@.host --user to connect to bus of other user))
Connectivity probe: ok
Capability: connected-no-operator-scope

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
  Connect: ok (49ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

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
GatewayExplicitAuthRequiredError: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
[exit 1]
```
### openclaw cron status
```
GatewayExplicitAuthRequiredError: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
[exit 1]
```
### openclaw cron status --json
```
GatewayExplicitAuthRequiredError: gateway url override requires explicit credentials
Fix: pass --token or --password *** --url (or gatewayToken in tools).
Set OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD alongside OPENCLAW_GATEWAY_URL; config credentials are intentionally not reused.
Config: /home/gha/.openclaw/openclaw.json
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
     Active: active (running) since Wed 2026-08-26 01:13:27 CST; 15min ago
   Main PID: 86268 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 272.2M
        CPU: 46.080s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─86268 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Aug 26 01:17:06 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:17:06.356+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:17:19 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:17:19.724+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.003+08:00 [diagnostic] lane task error: lane=main durationMs=386 error="ProviderAuthError: No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir."
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.009+08:00 [diagnostic] lane task error: lane=session:agent:main:main durationMs=396 error="ProviderAuthError: No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir."
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.033+08:00 [model-fallback/decision] model fallback decision: decision=candidate_failed requested=openai/gpt-5.5 candidate=openai/gpt-5.5 reason=auth next=none detail=No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir.
Aug 26 01:28:21 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:28:21.480+08:00 [ws] ⇄ res ✗ system-presence 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=a20d3f18…1795 id=e4a41f67…3456
Aug 26 01:28:39 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:28:39.595+08:00 [ws] ⇄ res ✗ status 243ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=fac10950…2b65 id=815e24ef…5522
Aug 26 01:28:39 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:28:39.610+08:00 [ws] ⇄ res ✗ system-presence 258ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=fac10950…2b65 id=a7b6a71c…ae16
Aug 26 01:28:39 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:28:39.632+08:00 [ws] ⇄ res ✗ config.get 272ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=fac10950…2b65 id=cad6c79f…e0f4
Aug 26 01:28:39 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:28:39.736+08:00 [ws] ⇄ res ✓ health 340ms cached=true conn=fac10950…2b65 id=f9458f9c…bdc0
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-08-25T13:29:09.560817-04:00
next weekday 05:55 ET: 2026-08-26T05:55:00-04:00
next as CST: 2026-08-26T17:55:00+08:00
hours until: 16.43
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
now ET: 2026-08-25T13:29:09.774139-04:00
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
