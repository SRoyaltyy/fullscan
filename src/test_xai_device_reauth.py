"""Parse + health JSON tests for the phone Claw re-auth path."""
from __future__ import annotations

import base64
import importlib.util
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from src.pipeline_health import (
    Check,
    Report,
    reauth_payload_from_report,
    write_reauth_status,
)
import src.pipeline_health as ph
from src.test_pipeline_health import ET


def _load_script():
    path = Path(__file__).resolve().parent.parent / "scripts" / "xai_device_reauth.py"
    spec = importlib.util.spec_from_file_location("xai_device_reauth", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_parse_device_output():
    mod = _load_script()
    blob = """
To authorize, visit:
  https://accounts.x.ai/oauth2/device
and enter code: ABCD-EFGH

Waiting for approval...
"""
    code, uri = mod.parse_device_output(blob)
    assert code == "ABCD-EFGH"
    assert "accounts.x.ai" in uri

    code, uri = mod.parse_device_output(
        "user_code: WXYZ1234\nverification_uri: https://accounts.x.ai/oauth2/device"
    )
    assert code == "WXYZ1234"
    assert uri.startswith("https://accounts.x.ai")

    code, uri = mod.parse_device_output(
        "Open https://accounts.x.ai/oauth2/device?user_code=KQCM-ANGJ"
    )
    assert code == "KQCM-ANGJ"
    assert "user_code=" in uri

    code, _ = mod.parse_device_output("nothing here")
    assert code is None


def test_complete_uri():
    mod = _load_script()
    assert mod.complete_uri("ABCD-EFGH") == (
        "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH"
    )
    printed = "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH"
    assert mod.complete_uri("ABCD-EFGH", printed) == printed
    assert mod.complete_uri(None) == "https://accounts.x.ai/oauth2/device"


def test_reauth_payload_from_report():
    r = Report(job="postclose", date="2026-08-28",
               source_date="2026-08-27", target_date="2026-08-28")
    r.checks = [
        Check(step="runtime.oauth", name="oauth", group="runtime",
              status="FAIL", required=True, detail="expired"),
        Check(step="runtime.pong", name="pong", group="runtime",
              status="OK", required=True, detail="PONG"),
    ]
    p = reauth_payload_from_report(r)
    assert p["status"] == "needs_reauth"
    assert p["oauth"] == "FAIL"
    assert p["pong_ok"] is True


def test_write_reauth_preserves_waiting():
    d = Path(tempfile.mkdtemp())
    prev = ph.REAUTH_JSON
    ph.REAUTH_JSON = d / "_xai_reauth.json"
    try:
        waiting = {
            "status": "waiting",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH",
            "expires_at": (datetime.now(ET) + timedelta(minutes=10)).isoformat(),
            "source": "xai_device_reauth",
        }
        ph.REAUTH_JSON.write_text(json.dumps(waiting), encoding="utf-8")
        r = Report(job="postclose", date="2026-08-28",
                   source_date="2026-08-27", target_date="2026-08-28")
        r.checks = [
            Check(step="runtime.oauth", name="oauth", group="runtime",
                  status="FAIL", required=True, detail="expired"),
        ]
        write_reauth_status(r)
        got = json.loads(ph.REAUTH_JSON.read_text(encoding="utf-8"))
        assert got["status"] == "waiting"
        assert got["user_code"] == "ABCD-EFGH"
    finally:
        ph.REAUTH_JSON = prev


class _Resp:
    def __init__(self, status, data):
        self.status = status
        self._raw = json.dumps(data).encode()

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_publish_contents_puts_waiting_json_without_git():
    mod = _load_script()
    d = Path(tempfile.mkdtemp())
    mod.OUT = d / "_xai_reauth.json"
    payload = {
        "status": "waiting",
        "user_code": "ABCD-EFGH",
        "verification_uri": "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH",
        "expires_at": "2099-01-01T00:00:00-05:00",
    }
    raw = json.dumps(payload, indent=2) + "\n"
    mod.OUT.write_text(raw, encoding="utf-8")
    saved = {k: os.environ.get(k) for k in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY")}
    os.environ["GITHUB_TOKEN"] = "test-token"
    os.environ.pop("GH_TOKEN", None)
    os.environ["GITHUB_REPOSITORY"] = "SRoyaltyy/fullscan"
    calls = []

    def fake_urlopen(req, timeout=30):
        if isinstance(req, str):
            calls.append(("GET", req, None))
            return _Resp(200, {})
        url = req.full_url
        calls.append((req.get_method(), url, req.data))
        if req.get_method() == "GET":
            return _Resp(200, {"sha": "abc123"})
        return _Resp(201, {"content": {"sha": "def456"}})

    def no_git(*args, **kwargs):
        raise AssertionError("contents publish must not git push")

    mod.urllib.request.urlopen = fake_urlopen
    mod.subprocess.run = no_git
    try:
        assert mod.publish_contents("chore: xAI device code ABCD-EFGH") is True
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    methods = [c[0] for c in calls]
    assert methods[:2] == ["GET", "PUT"]
    assert "purge.jsdelivr.net" in calls[2][1]
    put = calls[1]
    assert put[1].endswith("/repos/SRoyaltyy/fullscan/contents/01_daily/_xai_reauth.json")
    sent = json.loads(put[2].decode())
    assert sent["message"] == "chore: xAI device code ABCD-EFGH"
    assert sent["branch"] == "main"
    assert sent["sha"] == "abc123"
    assert base64.b64decode(sent["content"]).decode() == raw
    landed = json.loads(base64.b64decode(sent["content"]).decode())
    assert landed["status"] == "waiting"
    assert landed["user_code"] == "ABCD-EFGH"
    assert "user_code=ABCD-EFGH" in landed["verification_uri"]


def test_publish_contents_skips_without_token():
    mod = _load_script()
    saved = {k: os.environ.get(k) for k in ("GITHUB_TOKEN", "GH_TOKEN")}
    os.environ.pop("GITHUB_TOKEN", None)
    os.environ.pop("GH_TOKEN", None)
    try:
        assert mod.publish_contents("chore: xAI device code") is False
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_daemon_publishes_code_to_main_before_wait():
    """James reads the code from main. It must land before wait_pid."""
    mod = _load_script()
    d = Path(tempfile.mkdtemp())
    mod.OUT = d / "01_daily" / "_xai_reauth.json"
    mod.PID = d / "xai_reauth.pid"
    calls = []
    mod.start_login_pty = lambda: (4242, 99)
    mod.read_until_code = lambda fd, timeout: (
        "ABCD-EFGH", "https://accounts.x.ai/oauth2/device", "enter code: ABCD-EFGH")

    def publish(msg):
        body = json.loads(mod.OUT.read_text(encoding="utf-8"))
        assert body["status"] == "waiting" and body["user_code"] == "ABCD-EFGH"
        assert body["waiter_pid"] == os.getpid()
        assert body["host"] == mod.hostname()
        calls.append("publish")
        return True

    def wait(pid, timeout):
        assert pid == 4242 and timeout == mod.WAIT_S
        calls.append("wait")
        return 124

    mod.publish_contents = publish
    mod.wait_pid = wait
    mod.push = lambda msg: calls.append("push")
    from unittest import mock
    with mock.patch.object(mod.os, "close", lambda fd: None):
        assert mod.daemon(True) == 1
    assert calls[:2] == ["publish", "wait"]
    final = json.loads(mod.OUT.read_text(encoding="utf-8"))
    assert final["status"] == "needs_reauth" and final["user_code"] is None


def _waiting(mod, **kw):
    base = {
        "status": "waiting", "user_code": "ABCD-EFGH",
        "expires_at": (datetime.now(mod.ET) + timedelta(minutes=10)).isoformat(),
        "waiter_pid": os.getpid(), "host": mod.hostname(),
    }
    base.update(kw)
    return base


def test_waiting_live_only_with_live_local_waiter_and_unexpired_code():
    mod = _load_script()
    assert mod.waiting_live(_waiting(mod)) is True
    # dead waiter (GHA reaped it) -> stale
    assert mod.waiting_live(_waiting(mod, waiter_pid=2 ** 22 + 12345)) is False
    # other host
    assert mod.waiting_live(_waiting(mod, host="some-other-box")) is False
    # expired code
    past = (datetime.now(mod.ET) - timedelta(minutes=1)).isoformat()
    assert mod.waiting_live(_waiting(mod, expires_at=past)) is False
    # legacy file without waiter_pid/host (2026-10-15 3CMT-SMT4) -> stale
    legacy = _waiting(mod)
    legacy.pop("waiter_pid"); legacy.pop("host")
    assert mod.waiting_live(legacy) is False
    assert mod.waiting_live(_waiting(mod, expires_at=None)) is False
    assert mod.waiting_live({"status": "ok"}) is False


def test_check_starts_fresh_over_stale_waiting_file():
    mod = _load_script()
    d = Path(tempfile.mkdtemp())
    mod.OUT = d / "_xai_reauth.json"
    mod.PID = d / "xai_reauth.pid"
    mod.OUT.write_text(json.dumps(_waiting(mod, waiter_pid=2 ** 22 + 12345)))
    spawned = []
    mod.models_check = lambda: (1, "expired")
    mod.grok_busy = lambda: False
    mod.push = lambda msg: None
    mod.spawn_daemon = lambda force: spawned.append(force) or 0
    assert mod.check(False) == 0
    assert spawned == [False]


def test_publish_contents_retries_on_conflict():
    mod = _load_script()
    d = Path(tempfile.mkdtemp())
    mod.OUT = d / "_xai_reauth.json"
    mod.OUT.write_text(json.dumps(_waiting(mod)))
    os.environ["GITHUB_TOKEN"] = "t"
    seq = [409, 409, 201]
    puts = []
    mod._contents_sha = lambda url, h: "sha%d" % len(puts)
    def put(url, h, payload):
        puts.append(payload.get("sha"))
        return seq[len(puts) - 1]
    mod._contents_put = put
    mod._purge_jsdelivr = lambda: None
    from unittest import mock
    try:
        with mock.patch.object(mod.time, "sleep", lambda s: None):
            assert mod.publish_contents("chore: xAI device code ABCD-EFGH") is True
    finally:
        os.environ.pop("GITHUB_TOKEN", None)
    assert len(puts) == 3


def test_wait_shorter_than_code_life():
    mod = _load_script()
    assert 15 * 60 <= mod.WAIT_S < 30 * 60


def test_workflow_fails_red_unless_final_status_ok():
    wf = (Path(__file__).resolve().parent.parent / ".github" / "workflows"
          / "xai_reauth.yml").read_text(encoding="utf-8")
    assert 'if [ "$FINAL" != "ok" ]' in wf
    assert "exit 1" in wf.split('if [ "$FINAL" != "ok" ]', 1)[1][:200]
    import re as _re
    tmo = int(_re.search(r"timeout-minutes:\s*(\d+)", wf).group(1))
    mod = _load_script()
    assert tmo * 60 > mod.WAIT_S + 120


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok", t.__name__)
    print(f"{len(tests)} passed")
