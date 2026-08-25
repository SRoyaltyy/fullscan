#!/usr/bin/env bash
# Self-heal the ECS 05:55 timer. If this job is running because GitHub is
# still the clock, install the timer so tomorrow systemd owns the start.
# Never fails the caller.
set +e

export HOME="${FULLSCAN_HOME:-/home/gha}"
export USER="${FULLSCAN_USER:-gha}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "[ecs-clock] repo=$ROOT uid=$(id -u) user=$(id -un) home=$HOME"

bash "$ROOT/scripts/ensure_openclaw_timeouts.sh"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ecs-clock] no systemctl on this box"
else
  if systemctl is-enabled fullscan-preopen.timer >/dev/null 2>&1; then
    echo "[ecs-clock] fullscan-preopen.timer already enabled"
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
  else
    echo "[ecs-clock] timer NOT enabled — installing so 05:55 ET is the clock"

    if [ "$(id -u)" -eq 0 ]; then
      bash "$ROOT/scripts/install_ecs_preopen.sh"
    elif sudo -n true 2>/dev/null; then
      sudo -E HOME="$HOME" bash "$ROOT/scripts/install_ecs_preopen.sh"
    else
      echo "[ecs-clock] no root/sudo — cannot enable the timer from this job"
      echo "[ecs-clock] GitHub cron 09:55 UTC is the backup clock until someone runs:"
      echo "[ecs-clock]   sudo bash $ROOT/scripts/install_ecs_preopen.sh"
    fi

    if systemctl is-enabled fullscan-preopen.timer >/dev/null 2>&1; then
      echo "[ecs-clock] timer enabled"
    else
      echo "[ecs-clock] WARN: install ran but timer still not enabled"
    fi
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || true
  fi
fi

# Fill blank keys in /home/gha/.fullscan.env from this process env (GH
# secrets on a heal job). Never log values. Never overwrite a non-empty key.
ENVF="${FULLSCAN_ENV:-/home/gha/.fullscan.env}"
python3 - "$ENVF" <<'PY' || true
import os, sys
path = sys.argv[1]
keys = [
    "OPENCLAW_GATEWAY_URL", "OPENCLAW_TOKEN", "OPENCLAW_TIMEOUT",
    "DEEPSEEK_API_KEY", "GITHUB_TOKEN", "DATABASE_URL", "DATABASE_KEY",
    "FRED_API_KEY", "SEARXNG_URL", "FINVIZ_EMAIL", "FINVIZ_PASSWORD",
    "FINVIZ_EXPORT", "FINVIZ_AUTH", "AUTH_TOKEN_FINVIZ",
]
existing = {}
if os.path.isfile(path):
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            raw = line.rstrip("\n")
            if not raw or raw.lstrip().startswith("#") or "=" not in raw:
                continue
            k, _, v = raw.partition("=")
            existing[k] = v
filled = 0
kept = 0
for k in keys:
    incoming = os.environ.get(k)
    if incoming is None or incoming == "":
        continue
    cur = existing.get(k, "")
    if cur:
        kept += 1
        continue
    existing[k] = incoming
    filled += 1
os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
lines = [f"{k}={existing[k]}" for k in keys if k in existing]
# keep any extra keys already in the file
for k, v in existing.items():
    if k not in keys:
        lines.append(f"{k}={v}")
with open(path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + ("\n" if lines else ""))
os.chmod(path, 0o600)
print(f"[ecs-clock] env {path}: filled {filled} empty keys, kept {kept} existing")
PY
if [ "$(id -u)" -eq 0 ]; then
  chown gha:gha "$ENVF" 2>/dev/null || true
  chmod 600 "$ENVF" 2>/dev/null || true
fi

# Land a small file the GH commit step can push even past 09:25, so the
# GIT_DIR / dubious-ownership / $HOME path is proven without rewriting the day.
OUT="${GITHUB_WORKSPACE:-$ROOT}/01_daily/_ecs_clock.md"
mkdir -p "$(dirname "$OUT")"
{
  echo "# ECS clock status"
  echo
  echo "- generated: $(date -u +%Y-%m-%dT%H:%M:%SZ) UTC / $(TZ=America/New_York date '+%F %H:%M %Z')"
  echo "- uid=$(id -u) user=$(id -un) home=$HOME"
  echo "- repo=$ROOT"
  if command -v systemctl >/dev/null 2>&1; then
    echo "- timer: $(systemctl is-enabled fullscan-preopen.timer 2>/dev/null || echo NOT_ENABLED)"
    echo "- service: $(systemctl is-active fullscan-preopen.service 2>/dev/null || echo n/a)"
  else
    echo "- timer: no-systemctl"
  fi
  if command -v ss >/dev/null 2>&1; then
    if ss -ltn 2>/dev/null | grep -q ':18789'; then
      echo "- gateway: 18789 up"
    else
      echo "- gateway: 18789 DOWN"
    fi
  fi
  if command -v openclaw >/dev/null 2>&1; then
    echo "- xai.timeoutSeconds: $(sudo -u gha -H openclaw config get models.providers.xai.timeoutSeconds 2>/dev/null | tr '\n' ' ')"
    echo "- defaults.timeoutSeconds: $(sudo -u gha -H openclaw config get agents.defaults.timeoutSeconds 2>/dev/null | tr '\n' ' ')"
  fi
  echo
  echo '```'
  if command -v systemctl >/dev/null 2>&1; then
    systemctl list-timers --all fullscan-preopen.timer 2>/dev/null || echo "(no timer listed)"
  else
    echo "(no systemctl)"
  fi
  echo '```'
} > "$OUT"
echo "[ecs-clock] wrote $OUT"
exit 0
