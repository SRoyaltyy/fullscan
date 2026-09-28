# OpenClaw live probe

- generated: 2026-09-28T11:20:22Z UTC / 2026-09-28 07:20 EDT / 2026-09-28 19:20 CST
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
HTTP 200 time=0.033666s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.007937s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":611984461,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":35820,"delayP99Ms":24.4,"delayMaxMs":395.1,"utilization":0.082,"cpuCoreRatio":0.039}}
HTTP 200 time=0.008020s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":611984495,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":35820,"delayP99Ms":24.4,"delayMaxMs":395.1,"utilization":0.082,"cpuCoreRatio":0.039}}
HTTP 200 time=0.003733s
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
HTTP 200 time=0.003351s
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
  "ts": 1790594454496,
  "durationMs": 22,
  "eventLoop": {
    "degraded": false,
    "reasons": [],
    "intervalMs": 7611,
    "delayP99Ms": 32,
    "delayMaxMs": 130,
    "utilization": 0.19,
    "cpuCoreRatio": 0.199
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
Gateway event loop: ok max=54ms p99=20ms util=0.035 cpu=0.034
Agents: main (default)
Heartbeat interval: 30m (main)
Session store (main): /home/gha/.openclaw/agents/main/sessions/sessions.json (508 entries)
- agent:main:fullscan-sector-predict-technology-2026-09-28-139fd185dbbc (9m ago)
- agent:main:fullscan-catalyst-step1-bkr-1c685532c321 (24m ago)
- agent:main:fullscan-catalyst-verdict-bkr-e4c01da337e5 (24m ago)
- agent:main:fullscan-catalyst-step2-bkr-05a6d7ff8a3b (24m ago)
- agent:main:fullscan-catalyst-catcher-slb-023723393165 (25m ago)
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
│ Update               │ available · npm · npm update 2026.9.6 · deps ok                                               │
│ Gateway              │ local · ws://127.0.0.1:18789 (local loopback) · reachable 481ms · auth token                  │
│ Gateway service      │ systemd user not installed                                                                    │
│ Node service         │ systemd user not installed                                                                    │
│ Agents               │ 1 · 1 bootstrap file present · sessions 508 · default main active 10m ago                     │
│ Memory               │ enabled (plugin memory-core) · not checked                                                    │
│ Plugin compatibility │ none                                                                                          │
│ Probes               │ enabled                                                                                       │
│ Events               │ none                                                                                          │
│ Tasks                │ none                                                                                          │
│ Heartbeat            │ 30m (main)                                                                                    │
│ Last heartbeat       │ skipped · just now ago · unknown                                                              │
│ Sessions             │ 508 active · default grok-4.6 (200k ctx) · ~/.openclaw/agents/main/sessions/sessions.json     │
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
│ agent:main:fullscan-sector-   │ direct │ 10m ago │ grok-4.6     │ OpenClaw Default │ 87k/256k (34%) · 🗄️ 49% cached  │
│ pred…                         │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 24m ago │ grok-4.6     │ OpenClaw Default │ 21k/256k (8%) · 🗄️ 2% cached    │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 24m ago │ grok-4.6     │ OpenClaw Default │ 21k/256k (8%) · 🗄️ 2% cached    │
│ ve…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 24m ago │ grok-4.6     │ OpenClaw Default │ unknown/256k (?%)               │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 25m ago │ grok-4.6     │ OpenClaw Default │ 73k/256k (28%) · 🗄️ 85% cached  │
│ ca…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 30m ago │ grok-4.6     │ OpenClaw Default │ 32k/256k (12%) · 🗄️ 50% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 36m ago │ grok-4.6     │ OpenClaw Default │ 115k/256k (45%) · 🗄️ 71% cached │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 36m ago │ grok-4.6     │ OpenClaw Default │ 136k/256k (53%) · 🗄️ 80% cached │
│ ve…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 37m ago │ grok-4.6     │ OpenClaw Default │ 57k/256k (22%) · 🗄️ 73% cached  │
│ st…                           │        │         │              │                  │                                 │
│ agent:main:fullscan-catalyst- │ direct │ 43m ago │ grok-4.6     │ OpenClaw Default │ 83k/256k (32%) · 🗄️ 63% cached  │
│ ca…                           │        │         │              │                  │                                 │
└───────────────────────────────┴────────┴─────────┴──────────────┴──────────────────┴─────────────────────────────────┘

Health
┌────────────┬───────────┬─────────────────────────────────────────────────────────────────────────────────────────────┐
│ Item       │ Status    │ Detail                                                                                      │
├────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Gateway    │ reachable │ 20ms                                                                                        │
│ Event loop │ OK        │ healthy · max 54ms · p99 52ms · util 0.173 · cpu 0.149                                      │
└────────────┴───────────┴─────────────────────────────────────────────────────────────────────────────────────────────┘

FAQ: https://docs.openclaw.ai/faq
Troubleshooting: https://docs.openclaw.ai/troubleshooting

Update available (npm 2026.9.6). Run: openclaw update
Next steps:
  Need to share?      openclaw status --all
  Need to debug live? openclaw logs --follow
  Need to test channels? openclaw status --deep
[exit 0]
```
### openclaw gateway status --deep
```
Service: systemd user (disabled)
File logs: /tmp/openclaw-1000/openclaw-2026-09-28.log

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
  Connect: ok (50ms) · Capability: connect-only · Read probe: limited - missing scope: operator.read

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
Tue 2026-09-29 17:55:00 CST 22h left Mon 2026-09-28 17:55:04 CST 1h 26min ago fullscan-preopen.timer fullscan-preopen.service

1 timers listed.

### fullscan-preopen.timer show
```
Unit=fullscan-preopen.service
NextElapseUSecRealtime=Tue 2026-09-29 17:55:00 CST
LastTriggerUSec=Mon 2026-09-28 17:55:04 CST
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
     Active: active (running) since Mon 2026-09-21 17:20:24 CST; 1 week 0 days ago
   Main PID: 1608979 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 495.0M
        CPU: 6h 44min 1.976s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─1608979 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Sep 28 19:11:42 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 13. Stockanalysis / Public.com premarket (09-28) — LRCX ~$305.75 (~−3%); AMAT ~$470–472 (~−3%) vs 09-25 closes. **Nested semi-eq tape for S2.**
Sep 28 19:11:42 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 14. X search 09-27..09-28 — sparse 09-28 posts; weekend setup: 10Y >5% as the test of AI/tech; Hormuz still headline risk; oil as premium not confirmed structural shutdown. **Color only; did not override Channel 1.**
Sep 28 19:11:42 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 15. Yahoo Finance live blog fetch — **failed**; premarket XLK ~$194.56 −0.87% vs 09-25 $196.27 from Tradesmith/Investing.com search snippets, directionally consistent with Channel 1 XLK PM −1.16%.
Sep 28 19:11:42 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: **Not used as Channel 1 replacements:** Finviz futures board (stale SPX +0.20% / NQ +0.41% vs live ES/NQ vs-prior-close); Finviz WTI −1.59% vs Channel 1 CL=F +4.12%.
Sep 28 19:11:42 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:11:42.371+08:00 [agents/agent-command] [agent] run chatcmpl_11bddd69-8705-43d1-81f7-53f28ac80a75 ended with stopReason=stop
Sep 28 19:20:59 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:20:59.008+08:00 [ws] ⇄ res ✓ health 64ms conn=7822612c…bc12 id=32d1f7d0…409b
Sep 28 19:21:15 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:21:15.199+08:00 [ws] ⇄ res ✗ system-presence 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=2c9439a7…b2d7 id=9dcd9768…a8a3
Sep 28 19:21:34 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:21:34.832+08:00 [ws] ⇄ res ✗ status 2ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=a29fc077…0ce2 id=d8dd67e1…b9e3
Sep 28 19:21:34 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:21:34.842+08:00 [ws] ⇄ res ✗ system-presence 15ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=a29fc077…0ce2 id=6b87d9f0…54d0
Sep 28 19:21:34 iZt4nagf215582ts0wf5jcZ openclaw[1608979]: 2026-09-28T19:21:34.851+08:00 [ws] ⇄ res ✗ config.get 22ms errorCode=INVALID_REQUEST errorMessage=missing scope: operator.read conn=a29fc077…0ce2 id=719263fa…4f37
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-09-28T07:22:02.968596-04:00
next weekday 05:55 ET: 2026-09-29T05:55:00-04:00
next as CST: 2026-09-29T17:55:00+08:00
hours until: 22.55
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
/v1/chat/completions HTTP 200 in 18.1s
content: PONG
model: openclaw/default
PING_RESULT=PONG_OK
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Tue 2026-09-29 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Tue 2026-09-29 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-09-29T05:55:00-04:00
now ET: 2026-09-28T07:22:21.410523-04:00
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
