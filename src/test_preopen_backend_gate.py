"""Pre-Open backend gate on a runner with no OpenClaw gateway.

2026-11-09 run 42619500477 (dispatched llm_backend=auto on ubuntu) failed
in 0s with "predict needs Grok: no reachable OpenClaw gateway" and no
predict landed until the deepseek-web relay. auto means "Grok then the
free model", so with no gateway it must skip Grok and fall through.
An explicit llm_backend=grok dispatch still fails closed.

This runs the real gate block out of .github/workflows/preopen_all.yml.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows" / "preopen_all.yml"
BEGIN = "# >>> backend gate"
END = "# <<< backend gate"


def _gate_block() -> str:
    text = WF.read_text(encoding="utf-8")
    assert BEGIN in text and END in text, "backend gate markers missing"
    block = text.split(BEGIN, 1)[1].split(END, 1)[0]
    lines = block.splitlines()[1:]  # drop the rest of the marker line
    return "\n".join(ln[10:] if ln.startswith(" " * 10) else ln.lstrip()
                     for ln in lines)


def _run(backend: str, event: str, runner: str = "",
         gw: str = "") -> subprocess.CompletedProcess:
    script = "\n".join([
        "set -e",
        f'BACKEND="{backend}"',
        f'EVENT="{event}"',
        f'RUNNER="{runner}"',
        'export LLM_BACKEND="$BACKEND"',
        f'export OPENCLAW_GATEWAY_URL="{gw}"',
        'export OPENCLAW_TOKEN="tok"',
        'ARGS=(--llm-backend "$BACKEND")',
        'ON_UBUNTU=0; [ "$RUNNER" != "ecs" ] && ON_UBUNTU=1',
        'GW="${OPENCLAW_GATEWAY_URL:-}"; GW_LOOP=0',
        "if [ -z \"$GW\" ] || printf '%s' \"$GW\" | grep -Eq "
        "'127\\.0\\.0\\.1|localhost|\\[::1\\]'; then GW_LOOP=1; fi",
        _gate_block(),
        'echo "ARGS=${ARGS[*]} LLM_BACKEND=$LLM_BACKEND '
        'GW=${OPENCLAW_GATEWAY_URL:-unset} TOKEN=${OPENCLAW_TOKEN:-unset}"',
    ])
    return subprocess.run([shutil.which("bash") or "bash", "-c", script],
                          capture_output=True, text=True, timeout=30)


def test_auto_dispatch_on_ubuntu_falls_through():
    r = _run("auto", "workflow_dispatch", "ubuntu", gw="")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "falls through to the configured free model" in r.stdout
    assert "ARGS=--llm-backend auto LLM_BACKEND=auto" in r.stdout
    assert "GW=unset TOKEN=unset" in r.stdout


def test_auto_with_loopback_gateway_falls_through():
    r = _run("auto", "workflow_dispatch", "ubuntu", gw="http://127.0.0.1:18789")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "GW=unset" in r.stdout


def test_schedule_default_grok_falls_through():
    # Schedule/push carry no input; LLM_BACKEND defaults to 'grok'.
    for event in ("schedule", "push", "workflow_run"):
        r = _run("grok", event, "", gw="")
        assert r.returncode == 0, (event, r.stdout + r.stderr)
        assert "ARGS=--llm-backend auto LLM_BACKEND=auto" in r.stdout


def test_explicit_grok_dispatch_still_fails_closed():
    r = _run("grok", "workflow_dispatch", "ubuntu", gw="")
    assert r.returncode == 1
    assert "predict needs Grok: no reachable OpenClaw gateway" in r.stdout


def test_deepseek_untouched():
    r = _run("deepseek", "workflow_dispatch", "ubuntu", gw="")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "ARGS=--llm-backend deepseek LLM_BACKEND=deepseek" in r.stdout
    assert "falls through" not in r.stdout


def test_ecs_and_real_gateway_untouched():
    r = _run("grok", "workflow_dispatch", "ecs", gw="")
    assert r.returncode == 0 and "LLM_BACKEND=grok" in r.stdout
    r = _run("grok", "workflow_dispatch", "ubuntu", gw="https://gw.example.com")
    assert r.returncode == 0 and "LLM_BACKEND=grok" in r.stdout
    assert "GW=https://gw.example.com" in r.stdout


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
