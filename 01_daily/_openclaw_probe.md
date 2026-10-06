# OpenClaw live probe

- generated: 2026-10-06T11:01:42Z UTC / 2026-10-06 07:01 EDT / 2026-10-06 19:01 CST
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
HTTP 200 time=0.032190s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.001320s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":1302064865,"eventLoop":{"degraded":true,"reasons":["event_loop_delay"],"intervalMs":47582,"delayP99Ms":39.6,"delayMaxMs":1550.8,"utilization":0.133,"cpuCoreRatio":0.135}}
HTTP 200 time=0.006041s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":1302064883,"eventLoop":{"degraded":true,"reasons":["event_loop_delay"],"intervalMs":47582,"delayP99Ms":39.6,"delayMaxMs":1550.8,"utilization":0.133,"cpuCoreRatio":0.135}}
HTTP 200 time=0.002205s
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
HTTP 200 time=0.002642s
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
  "ts": 1791284536836,
  "durationMs": 59,
  "eventLoop": {
    "degraded": false,
    "reasons": [],
    "intervalMs": 21260,
    "delayP99Ms": 20.7,
    "delayMaxMs": 27.8,
    "utilization": 0.021,
    "cpuCoreRatio": 0.041
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
Gateway event loop: ok max=354ms p99=41ms util=0.175 cpu=0.222
Agents: main (default)
Heartbeat interval: 30m (main)
Session store (main): /home/gha/.openclaw/agents/main/sessions/sessions.json (522 entries)
- agent:main:fullscan-catalyst-catcher-tsla-e7a801250fd5 (1m ago)
- agent:main:fullscan-catalyst-step4-tsla-897a6423ff0f (6m ago)
- agent:main:fullscan-catalyst-step2-tsla-d51703cb3b90 (11m ago)
- agent:main:fullscan-catalyst-verdict-tsla-a7f00a39780c (11m ago)
- agent:main:fullscan-catalyst-step1-tsla-395f25267ce0 (11m ago)
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
│ Gateway              │ local · ws://127.0.0.1:18789 (local loopback) · reachable 448ms · auth token                  │
│ Gateway service      │ systemd user not installed                                                                    │
│ Node service         │ systemd user not installed                                                                    │
│ Agents               │ 1 · 1 bootstrap file present · sessions 522 · default main active 1m ago                      │
│ Memory               │ enabled (plugin memory-core) · not checked                                                    │
│ Plugin compatibility │ none                                                                                          │
│ Probes               │ enabled                                                                                       │
│ Events               │ none                                                                                          │
│ Tasks                │ none                                                                                          │
│ Heartbeat            │ 30m (main)                                                                                    │
│ Last heartbeat       │ skipped · 11m ago ago · unknown                                                               │
│ Sessions             │ 522 active · default grok-4.6 (200k ctx) · ~/.openclaw/agents/main/sessions/sessions.json     │
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
┌───────────────────────────────┬────────┬─────────┬──────────────┬──────────────────┬─────────────────────────────────┐
│ Key                           │ Kind   │ Age     │ Model        │ Runtime          │ Tokens                          │
├───────────────────────────────┼────────┼─────────┼──────────────┼──────────────────┼─────────────────────────────────┤
│ agent:main:fullscan-catalyst- │ direct │ 1m ago  │ grok-4.6     │ OpenClaw Default │ 357k/256k (139%)                │
│ ca…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 7m ago  │ grok-4.6     │ OpenClaw Default │ 30k/256k (12%) · 🗄️ 2% cached   │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 11m ago │ grok-4.6     │ OpenClaw Default │ 78k/256k (31%) · 🗄️ 52% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 11m ago │ grok-4.6     │ OpenClaw Default │ 75k/256k (29%) · 🗄️ 76% cached  │
│ ve…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 12m ago │ grok-4.6     │ OpenClaw Default │ 66k/256k (26%) · 🗄️ 82% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 17m ago │ grok-4.6     │ OpenClaw Default │ 122k/256k (48%) · 🗄️ 78% cached │
│ ca…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 25m ago │ grok-4.6     │ OpenClaw Default │ 30k/256k (12%) · 🗄️ 50% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 29m ago │ grok-4.6     │ OpenClaw Default │ 61k/256k (24%) · 🗄️ 60% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 29m ago │ grok-4.6     │ OpenClaw Default │ 72k/256k (28%) · 🗄️ 83% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 30m ago │ grok-4.6     │ OpenClaw Default │ 116k/256k (45%) · 🗄️ 30% cached │
│ ve…                           │        │         │              │                  │                                 │
└───────────────────────────────┴────────┴─────────┴──────────────┴──────────────────┴─────────────────────────────────┘

Health
┌────────────┬───────────┬─────────────────────────────────────────────────────────────────────────────────────────────┐
│ Item       │ Status    │ Detail                                                                                      │
├────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Gateway    │ reachable │ 20ms                                                                                        │
│ Event loop │ OK        │ healthy · max 85ms · p99 39ms · util 0.167 · cpu 0.149                                      │
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
File logs: /tmp/openclaw-1000/openclaw-2026-10-06.log

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
  Connect: ok (84ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

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
NEXT                        LEFT     LAST                        PASSED      UNIT                   ACTIVATES
Wed 2026-10-07 17:55:00 CST 22h left Tue 2026-10-06 17:55:54 CST 1h 7min ago fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Wed 2026-10-07 17:55:00 CST
LastTriggerUSec=Tue 2026-10-06 17:55:54 CST
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
     Active: active (running) since Mon 2026-09-21 17:20:24 CST; 2 weeks 1 day ago
   Main PID: 1608979 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 527.7M
        CPU: 14h 28min 55.769s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─1608979 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Oct 06 19:01:21 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:21.882+08:00 [openai-transport] [responses] error provider=xai api=openai-responses model=grok-4.6 name=Error status=undefined code=undefined type=undefined causeName=undefined causeCode=undefined message=Request was aborted
Oct 06 19:01:22 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:22.681+08:00 [agent/embedded] embedded run failover decision: runId=chatcmpl_f9cdda56-e5bd-45e9-921e-16cda4c7667f stage=assistant decision=surface_error reason=none from=xai/grok-4.6 profile=sha256:2ff640b124a6 rawError=Request was aborted
Oct 06 19:01:25 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:25.341+08:00 [provider-transport-fetch] [model-fetch] start provider=xai api=openai-responses model=grok-4.6 method=POST url=https://api.x.ai/v1/responses timeoutMs=10800000 proxy=none policy=custom
Oct 06 19:01:26 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:26.032+08:00 [provider-transport-fetch] [model-fetch] response provider=xai api=openai-responses model=grok-4.6 status=200 elapsedMs=691 contentType=text/event-stream
Oct 06 19:01:40 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:40.231+08:00 LLM request failed.
Oct 06 19:01:40 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:01:40.252+08:00 [agents/agent-command] [agent] run chatcmpl_f9cdda56-e5bd-45e9-921e-16cda4c7667f ended with stopReason=aborted
Oct 06 19:02:37 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:02:37.563+08:00 [ws] ⇄ res ✗ system-presence 1ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=652d27bd…f57f id=4166f567…2af9
Oct 06 19:02:57 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:02:57.453+08:00 [ws] ⇄ res ✗ status 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=416820b6…3cc6 id=3c3ffda2…a152
Oct 06 19:02:57 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:02:57.465+08:00 [ws] ⇄ res ✗ system-presence 14ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=416820b6…3cc6 id=6f4dc322…4a44
Oct 06 19:02:57 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-10-06T19:02:57.482+08:00 [ws] ⇄ res ✗ config.get 25ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=416820b6…3cc6 id=d079fe43…b2ed
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-10-06T07:03:25.408898-04:00
next weekday 05:55 ET: 2026-10-07T05:55:00-04:00
next as CST: 2026-10-07T17:55:00+08:00
hours until: 22.86
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
/v1/chat/completions HTTP 200 in 31.3s
content: PONG
model: openclaw/default
PING_RESULT=PONG_OK
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Wed 2026-10-07 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Wed 2026-10-07 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-10-07T05:55:00-04:00
now ET: 2026-10-06T07:03:57.078586-04:00
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
