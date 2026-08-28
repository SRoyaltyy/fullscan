"""Parse + health JSON tests for the phone Claw re-auth path."""
from __future__ import annotations

import importlib.util
import json
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


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok", t.__name__)
    print(f"{len(tests)} passed")
