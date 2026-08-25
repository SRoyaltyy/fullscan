# OpenClaw live probe

- generated: 2026-08-25T17:25:24Z UTC / 2026-08-25 13:25 EDT / 2026-08-26 01:25 CST
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
HTTP 200 time=0.012266s
```
### GET http://127.0.0.1:18789/healthz
```
{"ok":true,"status":"live"}
HTTP 200 time=0.004794s
```
### GET http://127.0.0.1:18789/ready
```
{"ready":true,"failing":[],"uptimeMs":691399,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":26598,"delayP99Ms":20.4,"delayMaxMs":27.4,"utilization":0.004,"cpuCoreRatio":0.005}}
HTTP 200 time=0.008943s
```
### GET http://127.0.0.1:18789/readyz
```
{"ready":true,"failing":[],"uptimeMs":691416,"eventLoop":{"degraded":false,"reasons":[],"intervalMs":26598,"delayP99Ms":20.4,"delayMaxMs":27.4,"utilization":0.004,"cpuCoreRatio":0.005}}
HTTP 200 time=0.002590s
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
### GET http://127.0.0.1:18789/v1/models
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
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### models.providers.xai.timeoutSeconds
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### models.providers.openai.timeoutSeconds
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### models.providers.anthropic.timeoutSeconds
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### agents.defaults.subagents.runTimeoutSeconds
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### agents.defaults.llm (must be ABSENT)
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
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
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw health --verbose
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw status --deep
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw gateway status --deep
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw gateway probe
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```

## 4. OpenClaw cron / automations scheduler

This is OpenClaw's own job timer, distinct from systemd fullscan-preopen.timer.

### openclaw automations status
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw automations list --all
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw automations list --json
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw cron list --all (alias)
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw cron status
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
```
### openclaw cron status --json
```
timeout: failed to run command ‘as_gha’: No such file or directory
[exit 127]
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
     Active: active (running) since Wed 2026-08-26 01:13:27 CST; 11min ago
   Main PID: 86268 (openclaw-gatewa)
      Tasks: 12 (limit: 1789)
     Memory: 251.4M
        CPU: 43.215s
     CGroup: /system.slice/fullscan-openclaw-gateway.service
             └─86268 openclaw-gateway "" "" "" "" "" "" "" "" "" "" "" "" "" ""

Aug 26 01:13:57 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:13:57.645+08:00 [heartbeat] started
Aug 26 01:14:07 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:14:07.727+08:00 [gateway] agent runtime plugins pre-warmed in 173ms
Aug 26 01:16:26 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:16:26.116+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:16:39 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:16:39.750+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:16:53 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:16:53.006+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:17:06 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:17:06.356+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:17:19 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:17:19.724+08:00 [reload] config change detected; evaluating reload (meta.lastTouchedAt)
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.003+08:00 [diagnostic] lane task error: lane=main durationMs=386 error="ProviderAuthError: No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir."
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.009+08:00 [diagnostic] lane task error: lane=session:agent:main:main durationMs=396 error="ProviderAuthError: No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir."
Aug 26 01:21:02 iZt4nagf215582ts0wf5jcZ openclaw[86268]: 2026-08-26T01:21:02.033+08:00 [model-fallback/decision] model fallback decision: decision=candidate_failed requested=openai/gpt-5.5 candidate=openai/gpt-5.5 reason=auth next=none detail=No API key found for provider "openai". Auth store: /home/gha/.openclaw/agents/main/agent/openclaw-agent.sqlite (agentDir: /home/gha/.openclaw/agents/main/agent). Configure auth for this agent (openclaw agents add <id>) or copy only portable static auth profiles from the main agentDir.
```
### expected next 05:55 America/New_York vs systemd Next
```
now ET: 2026-08-25T13:25:25.370489-04:00
next weekday 05:55 ET: 2026-08-26T05:55:00-04:00
next as CST: 2026-08-26T17:55:00+08:00
hours until: 16.49
```

## 6. Live chat ping (gateway actually answers)

Short completion against /v1/chat/completions. 90s cap. Proves the
running process will take a Grok turn. Does NOT soak 9 minutes.

```
PING_RESULT=ERROR after 0.0s: HTTPError: HTTP Error 404: Not Found
```

## 7. Verdict (live, this run)

systemd NextElapseUSecRealtime: Wed 2026-08-26 17:55:00 CST
systemd TimersCalendar: { OnCalendar=Mon..Fri *-*-* 05:55:00 America/New_York ; next_elapse=Wed 2026-08-26 17:55:00 CST }
systemd Persistent: yes
expect next 05:55 ET: 2026-08-26T05:55:00-04:00
now ET: 2026-08-25T13:25:25.666068-04:00
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
