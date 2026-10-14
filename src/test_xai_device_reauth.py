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


def test_daemon_publishes_user_code_before_wait_pid():
    """GitHub must see status=waiting before the 12-minute poll.

    The old order wrote the code on the box, git-pushed (invalid
    credentials), then wait_pid. Contents API ran only after that
    returned, often as needs_reauth with user_code cleared.
    """
    mod = _load_script()
    d = Path(tempfile.mkdtemp())
    mod.OUT = d / "01_daily" / "_xai_reauth.json"
    mod.PID = d / "xai_reauth.pid"
    calls = []

    def start():
        return 4242, 99

    def read_until(fd, timeout):
        return (
            "ABCD-EFGH",
            "https://accounts.x.ai/oauth2/device",
            "enter code: ABCD-EFGH",
        )

    def publish(msg):
        body = json.loads(mod.OUT.read_text(encoding="utf-8"))
        assert body["status"] == "waiting"
        assert body["user_code"] == "ABCD-EFGH"
        assert body["verification_uri"].endswith("user_code=ABCD-EFGH")
        assert body.get("expires_at")
        assert "ABCD-EFGH" in msg
        calls.append("publish")
        return True

    def wait(pid, timeout):
        assert pid == 4242
        assert timeout == mod.WAIT_S
        calls.append("wait")
        return 124

    def gitpush(msg):
        calls.append("gitpush")

    mod.start_login_pty = start
    mod.read_until_code = read_until
    mod.publish_contents = publish
    mod.wait_pid = wait
    mod.push = gitpush
    mod.os.close = lambda fd: None
    rc = mod.daemon(True)
    assert rc == 1
    assert calls[:2] == ["publish", "wait"]
    assert calls.index("wait") < calls.index("gitpush")
    final = json.loads(mod.OUT.read_text(encoding="utf-8"))
    assert final["status"] == "needs_reauth"
    assert final["user_code"] is None


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok", t.__name__)
    print(f"{len(tests)} passed")
