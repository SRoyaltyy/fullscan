# OpenClaw live probe

- generated: 2026-10-05T11:06:58Z UTC / 2026-10-05 07:06 EDT / 2026-10-05 19:06 CST
- uid=0 user=root home=/home/gha
- gateway_url=http://127.0.0.1:18789
- token_set=yes

## 1. Port + HTTP health (running process, not disk)

### ss :18789
```
LISTEN 0      511        127.0.0.1:18789      0.0.0.0:*    users:(("openclaw-gatewa",pid=1608979,fd=33))
LISTEN 0      511            [::1]:18789         [::]:*    users:(("openclaw-gatewa",pid=1608979,fd=34))
```
### GET http://127.0.0.1:18789/health
```
{"ok":true,"status":"live"}
HTTP 200 time=0.112525s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.005834s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":1215980458,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":41423,"delayP99Ms":28.7,"delayMaxMs":715.1,"utilization":0.114,"cpuCoreRatio":0.037}}
HTTP 200 time=0.009539s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":1215980491,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":41423,"delayP99Ms":28.7,"delayMaxMs":715.1,"utilization":0.114,"cpuCoreRatio":0.037}}
HTTP 200 time=0.003440s
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
{"object":"list","data":[{"id":"openclaw","object":"model","created":0,"owned_by":"openclaw","permission":[]},{"id":"openclaw/default","object":"model","created":0,"owned_by":"openclaw","permission":[]},{"id":"openclaw/main","object":"model","created":0,"owned_by":"openclaw","permission":[]}]}
HTTP 200 time=0.003601s
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
{
  "ok": true,
  "ts": 1791198450923,
  "durationMs": 54,
  "eventLoop": {
    "degraded": false,
    "reasons": [],
    "intervalMs": 13612,
    "delayP99Ms": 32.8,
    "delayMaxMs": 112.8,
    "utilization": 0.123,
    "cpuCoreRatio": 0.122
  },
  "plugins": {
    "loaded": [
      "alibaba",
      "anthropic",
      "azure-speech",
      "browser",
      "byteplus",
      "canvas",
      "clawrouter",
      "cohere",
      "comfy",
      "copilot-proxy",
      "deepgram",
      "device-pair",
      "document-extract",
      "elevenlabs",
      "fal",
      "file-transfer",
      "github-copilot",
      "google",
      "huggingface",
      "litellm",
      "lmstudio",
      "memory-core",
      "meta",
      "microsoft",
      "microsoft-foundry",
      "minimax",
      "mistral",
      "novita",
      "nvidia",
      "ollama",
      "openai",
      "opencode",
      "opencode-go",
      "openrouter",
      "phone-control",
      "runway",
      "senseaudio",
      "sglang",
      "synthetic",
      "talk-voice",
      "together",
      "tts-local-cli",
      "vllm",
      "volcengine",
      "voyage",
      "vydra",
      "web-readability",
      "xai",
      "xiaomi"
    ],
    "errors": []
  },
  "configReload": {
    "hotReloadStatus": "active"
  },
  "modelPricing": {
    "state": "ok",
    "sources": []
  },
  "channels": {},
  "channelOrder": [],
  "channelLabels": {},
  "heartbeatSeconds": 1800,
  "defaultAgentId": "main",
  "agents": [
```
### openclaw health --verbose
```
Gateway connection:
  Gateway target: ws://127.0.0.1:18789
  Source: local loopback
  Config: /home/gha/.openclaw/openclaw.json
  Bind: loopback
Gateway event loop: ok max=77ms p99=21ms util=0.047 cpu=0.043
Agents: main (default)
Heartbeat interval: 30m (main)
Session store (main): /home/gha/.openclaw/agents/main/sessions/sessions.json (500 entries)
- agent:main:openai:1d64d334-750f-43a3-83dc-4aa0c2c3e7dd (8m ago)
- agent:main:openai:69ac3a6b-42f7-453a-ae00-8e8ef7b8c7ff (10m ago)
- agent:main:openai:5de175bc-0816-419b-a745-95d4734f749b (30m ago)
- agent:main:fullscan-catalyst-catcher-tsla-2f5ce6f087b2 (37m ago)
- agent:main:fullscan-catalyst-step4-tsla-1494ed11618f (43m ago)
[exit 0]
```
### openclaw status --deep
```
OpenClaw status

Overview
┌──────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────┐
│ Item                 │ Value                                                                                         │
├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
│ OS                   │ linux 5.15.0-187-generic (x64) · node 26.7.0                                                  │
│ Dashboard            │ http://127.0.0.1:18789/                                                                       │
│ Tailscale exposure   │ off                                                                                           │
│ Channel              │ stable (default)                                                                              │
│ Update               │ available · npm · npm update 2026.9.8 · deps ok                                               │
│ Gateway              │ local · ws://127.0.0.1:18789 (local loopback) · reachable 428ms · auth token                  │
│ Gateway service      │ systemd user not installed                                                                    │
│ Node service         │ systemd user not installed                                                                    │
│ Agents               │ 1 · 1 bootstrap file present · sessions 500 · default main active 9m ago                      │
│ Memory               │ enabled (plugin memory-core) · not checked                                                    │
│ Plugin compatibility │ none                                                                                          │
│ Probes               │ enabled                                                                                       │
│ Events               │ none                                                                                          │
│ Tasks                │ none                                                                                          │
│ Heartbeat            │ 30m (main)                                                                                    │
│ Last heartbeat       │ skipped · 17m ago ago · unknown                                                               │
│ Sessions             │ 500 active · default grok-4.6 (200k ctx) · ~/.openclaw/agents/main/sessions/sessions.json     │
└──────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────┘

Security audit
Summary: 0 critical · 1 warn · 2 info
  WARN Reverse proxy headers are not trusted
    gateway.bind is loopback and gateway.trustedProxies is empty. If you expose the Control UI through a reverse proxy, configure trusted proxies so local-client c…
    Fix: Set gateway.trustedProxies to your proxy IPs or keep the Control UI local-only.
Full report: openclaw security audit
Deep probe: openclaw security audit --deep

Channels
No channels configured

Sessions
┌────────────────────────────────┬────────┬─────────┬──────────────┬──────────────────┬────────────────────────────────┐
│ Key                            │ Kind   │ Age     │ Model        │ Runtime          │ Tokens                         │
├────────────────────────────────┼────────┼─────────┼──────────────┼──────────────────┼────────────────────────────────┤
│ agent:main:openai:1d64d334-    │ direct │ 9m ago  │ grok-4.6     │ OpenClaw Default │ unknown/256k (?%)              │
│ 750f…                          │        │         │              │                  │                                │
│ agent:main:openai:69ac3a6b-    │ direct │ 11m ago │ grok-4.6     │ OpenClaw Default │ 20k/256k (8%) · 🗄️ 3% cached   │
│ 42f7…                          │        │         │              │                  │                                │
│ agent:main:openai:5de175bc-    │ direct │ 30m ago │ grok-4.6     │ OpenClaw Default │ 20k/256k (8%) · 🗄️ 3% cached   │
│ 0816…                          │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 38m ago │ grok-4.6     │ OpenClaw Default │ 488k/256k (191%)               │
│ ca…                            │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 43m ago │ grok-4.6     │ OpenClaw Default │ 33k/256k (13%) · 🗄️ 2% cached  │
│ st…                            │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 49m ago │ grok-4.6     │ OpenClaw Default │ 59k/256k (23%) · 🗄️ 36% cached │
│ st…                            │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 50m ago │ grok-4.6     │ OpenClaw Default │ 85k/256k (33%) · 🗄️ 69% cached │
│ st…                            │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 50m ago │ grok-4.6     │ OpenClaw Default │ 74k/256k (29%) · 🗄️ 66% cached │
│ ve…                            │        │         │              │                  │                                │
│ agent:main:fullscan-grok_      │ direct │ 1h ago  │ grok-4.6     │ OpenClaw Default │ 69k/256k (27%) · 🗄️ 82% cached │
│ review…                        │        │         │              │                  │                                │
│ agent:main:fullscan-catalyst-  │ direct │ 1h ago  │ grok-4.6     │ OpenClaw Default │ 258k/256k (101%)               │
│ st…                            │        │         │              │                  │                                │
└────────────────────────────────┴────────┴─────────┴──────────────┴──────────────────┴────────────────────────────────┘

Health
┌────────────┬───────────┬─────────────────────────────────────────────────────────────────────────────────────────────┐
│ Item       │ Status    │ Detail                                                                                      │
├────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Gateway    │ reachable │ 20ms                                                                                        │
│ Event loop │ OK        │ healthy · max 93ms · p99 26ms · util 0.153 · cpu 0.148                                      │
└────────────┴───────────┴─────────────────────────────────────────────────────────────────────────────────────────────┘

FAQ: https://docs.openclaw.ai/faq
Troubleshooting: https://docs.openclaw.ai/troubleshooting

Update available (npm 2026.9.8). Run: openclaw update
Next steps:
  Need to share?      openclaw status --all
  Need to debug live? openclaw logs --follow
  Need to test channels? openclaw status --deep
[exit 0]
```
### openclaw gateway status --deep
```
Service: systemd user (disabled)
File logs: /tmp/openclaw-1000/openclaw-2026-10-05.log

Config (cli): ~/.openclaw/openclaw.json
Config (service): ~/.openclaw/openclaw.json

Gateway: bind=loopback (127.0.0.1), port=18789 (env/config)
Probe target: ws://127.0.0.1:18789
Dashboard: http://127.0.0.1:18789/
Probe note: Loopback-only gateway; only local clients can connect.

CLI version: 2026.7.1-2 (/usr/bin/openclaw)
Gateway version: 2026.7.1-2

Runtime: stopped (state inactive, sub dead, last exit 0, reason 0)
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
No cron jobs.
[exit 0]
```
### openclaw cron status
```
{
  "enabled": true,
  "storePath": "/home/gha/.openclaw/cron/jobs.json",
  "storage": "sqlite",
  "sqlitePath": "/home/gha/.openclaw/state/openclaw.sqlite",
  "jobs": 0,
  "nextWakeAtMs": null
}
[exit 0]
```
### openclaw cron status --json
```
{
  "enabled": true,
  "storePath": "/home/gha/.openclaw/cron/jobs.json",
  "storage": "sqlite",
  "sqlitePath": "/home/gha/.openclaw/state/openclaw.sqlite",
  "jobs": 0,
  "nextWakeAtMs": null
}
[exit 0]
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
Tue 2026-10-06 17:55:00 CST 22h left Mon 2026-10-05 17:55:04 CST 1h 13min ago fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Tue 2026-10-06 17:55:00 CST
LastTriggerUSec=Mon 2026-10-05 17:55:04 CST
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
     Active: active (running) since Mon 2026-09-21 17:20:24 CST; 2 weeks 0 days ago
   Main PID: 1608979 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 464.9M
        CPU: 13h 18min 19.753s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─1608979 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Oct 05 18:59:13 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T18:59:13.439+08:00 Hey. I just came online.
Oct 05 18:59:13 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: Who am I? Who are you?
Oct 05 18:59:13 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: I
Oct 05 18:59:13 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T18:59:13.445+08:00 [agents/agent-command] [agent] run chatcmpl_01eefb85-691c-420c-8679-479145522b80 ended with stopReason=stop
Oct 05 19:07:35 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:07:35.451+08:00 [ws] ⇄ res ✓ health 81ms conn=74c8cbcc…8f2f id=d1172523…267a
Oct 05 19:07:51 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:07:51.338+08:00 [ws] ⇄ res ✗ system-presence 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=bc0b9160…06ef id=91a03c67…98b2
Oct 05 19:08:09 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:08:09.799+08:00 [ws] ⇄ res ✗ status 9ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=9d5dc738…0765 id=f6f2a536…1f0d
Oct 05 19:08:09 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:08:09.808+08:00 [ws] ⇄ res ✗ system-presence 38ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=9d5dc738…0765 id=93125f78…327a
Oct 05 19:08:09 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:08:09.824+08:00 [ws] ⇄ res ✗ config.get 54ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=9d5dc738…0765 id=e6fbaa6b…ad4f
Oct 05 19:08:09 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-05T19:08:09.840+08:00 [ws] ⇄ res ✓ health 67ms cached=true conn=9d5dc738…0765 id=00efc7ac…86f9
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-10-05T07:08:38.025557-04:00
next weekday 05:55 ET: 2026-10-06T05:55:00-04:00
next as CST: 2026-10-06T17:55:00+08:00
hours until: 22.77
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
/v1/chat/completions HTTP 200 in 21.4s
content: PONG
model: openclaw/default
PING_RESULT=PONG_OK
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Tue 2026-10-06 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Tue 2026-10-06 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-10-06T05:55:00-04:00
now ET: 2026-10-05T07:08:59.773503-04:00
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
